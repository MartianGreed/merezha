//! `LISTEN` sessions: one dedicated connection per listener, outside the
//! pool, driven by a task that forwards notifications into a bounded queue.
use crate::{classify, instruments::Telemetry, statement::kernel};
use merezha_domain::Failure;
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::{AsyncMessage, Client, Connection};
use tokio_postgres_rustls::MakeRustlsConnect;

/// Most notifications waiting unread in a [`Listener`]; later ones are
/// dropped until the reader catches up.
pub const MAX_PENDING_NOTIFICATIONS: usize = 64;
const MAX_CHANNELS: usize = 8;
/// Notification payloads are cut to this many bytes.
const MAX_PAYLOAD_BYTES: usize = 1024;

#[derive(Clone)]
pub(crate) struct SessionConfig {
    pg: tokio_postgres::Config,
    tls: Option<MakeRustlsConnect>,
}
impl SessionConfig {
    pub(crate) fn new(pg: tokio_postgres::Config, tls: Option<MakeRustlsConnect>) -> Self {
        Self { pg, tls }
    }
}

/// One received notification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Notification {
    pub channel: String,
    /// At most 1 KiB; longer payloads are cut at a character boundary.
    pub payload: String,
}

/// A session listening on its channels. Dropping it closes the session.
///
/// A session that ended ([`Listener::recv`] returned `None`) or failed
/// [`Listener::check`] is lost; the next [`crate::Database::listen`] on the
/// same channels that succeeds counts one `merezha.db.listener.reconnects`.
pub struct Listener {
    client: Client,
    receiver: tokio::sync::mpsc::Receiver<Notification>,
    driver: tokio::task::JoinHandle<()>,
    telemetry: Telemetry,
    /// The channel set, comma-joined: the reconnect counter's key.
    channels: String,
}
impl std::fmt::Debug for Listener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Listener").finish_non_exhaustive()
    }
}
impl Listener {
    /// The next notification, or `None` once the session ended.
    pub async fn recv(&mut self) -> Option<Notification> {
        let notification = self.receiver.recv().await;
        if notification.is_none() {
            self.telemetry.listener_lost(&self.channels);
        }
        notification
    }
    /// Prove the session still answers: `SELECT 1` before `deadline`. A
    /// half-open connection never ends by itself, so a reader that waits
    /// long between notifications probes it and reconnects on failure.
    /// Fails like [`crate::Database::ping`].
    pub async fn check(&self, deadline: Instant) -> Result<(), Failure> {
        let at = tokio::time::Instant::from_std(deadline);
        let probe = self.telemetry.probe(&self.client, &kernel::LISTENER_CHECK);
        let result = match tokio::time::timeout_at(at, probe).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(classify(&error)),
            Err(_) => Err(Failure::transient("database_timeout")),
        };
        if result.is_err() {
            self.telemetry.listener_lost(&self.channels);
        }
        result
    }
    /// A waiting notification without blocking, for draining a burst.
    pub fn try_recv(&mut self) -> Option<Notification> {
        self.receiver.try_recv().ok()
    }
}
impl Drop for Listener {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

fn valid_channel(name: &str) -> bool {
    (1..=63).contains(&name.len())
        && name
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_lowercase() || b == b'_')
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

pub(crate) async fn open(
    session: &SessionConfig,
    telemetry: &Telemetry,
    channels: &[&str],
    deadline: Instant,
) -> Result<Listener, Failure> {
    if channels.is_empty()
        || channels.len() > MAX_CHANNELS
        || !channels.iter().all(|c| valid_channel(c))
    {
        return Err(Failure::permanent("invalid_input"));
    }
    let key = channels.join(",");
    let result = connect(session, telemetry, channels, key.clone(), deadline).await;
    match &result {
        Ok(_) => telemetry.listener_connected(&key),
        Err(_) => telemetry.listener_lost(&key),
    }
    result
}

async fn connect(
    session: &SessionConfig,
    telemetry: &Telemetry,
    channels: &[&str],
    key: String,
    deadline: Instant,
) -> Result<Listener, Failure> {
    let at = tokio::time::Instant::from_std(deadline);
    let (sender, receiver) = tokio::sync::mpsc::channel(MAX_PENDING_NOTIFICATIONS);
    let connect = async {
        match &session.tls {
            None => {
                let (client, connection) = session
                    .pg
                    .connect(tokio_postgres::NoTls)
                    .await
                    .map_err(|e| classify(&e))?;
                Ok::<_, Failure>((client, tokio::spawn(drive(connection, sender))))
            }
            Some(tls) => {
                let (client, connection) = session
                    .pg
                    .connect(tls.clone())
                    .await
                    .map_err(|e| classify(&e))?;
                Ok((client, tokio::spawn(drive(connection, sender))))
            }
        }
    };
    let (client, driver) = match tokio::time::timeout_at(at, connect).await {
        Ok(result) => result?,
        Err(_) => return Err(Failure::transient("database_timeout")),
    };
    let listener = Listener {
        client,
        receiver,
        driver,
        telemetry: telemetry.clone(),
        channels: key,
    };
    // Channel names are validated identifiers, never values.
    let statements: String = channels
        .iter()
        .map(|channel| format!("LISTEN {channel};"))
        .collect();
    let listen = telemetry.batch_text(&listener.client, &kernel::LISTEN, &statements);
    match tokio::time::timeout_at(at, listen).await {
        Ok(Ok(())) => Ok(listener),
        Ok(Err(error)) => Err(classify(&error)),
        Err(_) => Err(Failure::transient("database_timeout")),
    }
}

/// Poll the connection until it ends, forwarding notifications. A full
/// queue drops the notification: the reader is already due to wake.
async fn drive<S, T>(
    mut connection: Connection<S, T>,
    sender: tokio::sync::mpsc::Sender<Notification>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
            Some(Ok(AsyncMessage::Notification(notification))) => {
                let mut payload = notification.payload().to_owned();
                if payload.len() > MAX_PAYLOAD_BYTES {
                    let mut end = MAX_PAYLOAD_BYTES;
                    while !payload.is_char_boundary(end) {
                        end -= 1;
                    }
                    payload.truncate(end);
                }
                let _ = sender.try_send(Notification {
                    channel: notification.channel().to_owned(),
                    payload,
                });
            }
            Some(Ok(_)) => {}
            Some(Err(_)) | None => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::valid_channel;

    #[test]
    fn channel_names_are_plain_lowercase_identifiers() {
        assert!(valid_channel("merezha_jobs_outbox"));
        assert!(valid_channel("_x1"));
        for bad in ["", "Jobs", "1x", "a-b", "a;b", &"a".repeat(64)] {
            assert!(!valid_channel(bad), "{bad}");
        }
    }
}
