//! The webhook maintenance tasks (NT §2.5, §5.5), on `notify.maintenance`:
//!
//! - [`auto_disable`] (`webhook_auto_disable`, every 5 minutes): an active
//!   endpoint failing since more than `webhooks.auto_disable_after` ago with
//!   at least 10 failed attempts since then is disabled (`failing`), its
//!   queued deliveries are cancelled, an audit row is written, and, in the
//!   same transaction, `notifications.webhook_disabled` is requested for
//!   `webhooks.alert_roles` in the endpoint's tenant, or
//!   `webhooks.application_alert_roles` for an application-level endpoint
//!   (none when the list is empty). The disable is logged
//!   `notification.webhook_endpoint_disabled` either way.
//! - [`retention`] (part of the daily `retention` task): event bodies past
//!   `retention.webhook_deliveries` are nulled (a redelivery then answers
//!   410 `event_body_expired`), events past `retention.deliveries` with
//!   no open delivery are deleted with their deliveries and attempts, audit
//!   rows go after [`AUDIT_RETENTION`], previous secrets whose rotation
//!   window closed are dropped, and, while `secret_key_previous` is set,
//!   secrets still sealed under another key are sealed again under
//!   `secret_key` ([`reseal`]); what remains is the gauge
//!   `merezha.webhook.secrets.previous_key`.
use super::{AUTO_DISABLE_FAILURES, secrets::SecretBox, sql, worker};
use crate::notifier::{Core, request_in};
use merezha_domain::{Failure, Identity};
use merezha_platform::ports::{
    ChannelSelection, Correlation, DedupeKey, NotificationRequest, NotifyError, Recipient, Source,
};
use merezha_postgres::classify;
use serde_json::{Map, Value};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;

/// The topic of the alert (NT §3.3).
pub const WEBHOOK_DISABLED_TOPIC: &str = "notifications.webhook_disabled";
/// How long audit rows are kept: identity's `audit.retention` default.
pub const AUDIT_RETENTION: Duration = Duration::from_secs(400 * 86_400);
const BATCH: i64 = 500;
const MAX_BATCHES: usize = 20;

fn before(now: jiff::Timestamp, by: Duration) -> Result<jiff::Timestamp, Failure> {
    jiff::SignedDuration::try_from(by)
        .ok()
        .and_then(|d| now.checked_sub(d).ok())
        .ok_or_else(|| Failure::permanent("invalid_config"))
}

/// The alert of a disabled endpoint, or `None` when no role is configured
/// for its registry.
fn alert(
    roles: &[String],
    endpoint: Uuid,
    tenant: Option<&str>,
    disabled_at: jiff::Timestamp,
) -> Result<Option<NotificationRequest>, Failure> {
    if roles.is_empty() {
        return Ok(None);
    }
    let roles = roles
        .iter()
        .map(|role| Identity::new(role.as_str()))
        .collect::<Result<Vec<_>, _>>()?;
    let tenant = tenant.map(Identity::new).transpose()?;
    let recipient = match &tenant {
        Some(id) => Recipient::Tenant {
            id: id.clone(),
            roles: Some(roles),
        },
        None => Recipient::Application { roles },
    };
    let mut data = Map::new();
    data.insert("endpoint_id".into(), Value::String(endpoint.to_string()));
    Ok(Some(NotificationRequest {
        topic: Identity::new(WEBHOOK_DISABLED_TOPIC)?,
        recipients: vec![recipient],
        channels: ChannelSelection::TopicDefault,
        template: None,
        data,
        secret_data: Map::new(),
        locale: None,
        dedupe_key: DedupeKey::new(format!("whdisable:{endpoint}:{}", disabled_at.as_second()))?,
        tenant,
        members: Vec::new(),
        not_before: None,
        expires_at: None,
        collapse_key: None,
        correlation: Correlation {
            correlation_id: Identity::new(endpoint.simple().to_string())?,
            causation_id: None,
        },
    }))
}

/// `webhook_auto_disable`: the endpoints it disabled.
pub async fn auto_disable(core: &Arc<Core>, deadline: Instant) -> Result<u64, Failure> {
    let Some(sender) = core.webhooks.clone() else {
        return Ok(0);
    };
    let mut total = 0;
    for _ in 0..MAX_BATCHES {
        let now = core.now();
        let since = before(now, sender.settings.auto_disable_after)?;
        let candidates: Vec<Uuid> = {
            let client = core.database.acquire(deadline).await?;
            core.database
                .query(
                    &client,
                    &sql::AUTO_DISABLE_CANDIDATES,
                    &[&since, &AUTO_DISABLE_FAILURES, &BATCH],
                )
                .await
                .map_err(|e| classify(&e))?
                .iter()
                .map(|row| row.get(0))
                .collect()
        };
        for endpoint in &candidates {
            let endpoint = *endpoint;
            let core = core.clone();
            let settings = sender.settings.clone();
            let db = core.database.clone();
            let cancelled = db
                .clone()
                .transaction(deadline, move |tx| {
                    Box::pin(async move {
                        let Some((tenant, cancelled)) = worker::disable(
                            &db,
                            tx,
                            endpoint,
                            "failing",
                            now,
                            Some(since),
                            worker::SYSTEM_ACTOR,
                        )
                        .await?
                        else {
                            return Ok(None);
                        };
                        let roles = match tenant {
                            Some(_) => &settings.alert_roles,
                            None => &settings.application_alert_roles,
                        };
                        if let Some(request) = alert(roles, endpoint, tenant.as_deref(), now)? {
                            let source = Source::new(crate::NAME)?;
                            match request_in(&core, &source, tx, request).await {
                                Ok(_) => {}
                                // The disable must not wait on the alert's
                                // validity; the refusal is logged
                                // `notification.invalid`.
                                Err(NotifyError::Unavailable) => {
                                    return Err(Failure::transient("unavailable"));
                                }
                                Err(_) => {}
                            }
                        }
                        Ok(Some(cancelled))
                    })
                })
                .await?;
            if let Some(cancelled) = cancelled {
                worker::disabled(&sender, endpoint, "failing", cancelled);
                total += 1;
            }
        }
        if (candidates.len() as i64) < BATCH {
            break;
        }
    }
    Ok(total)
}

/// Run `statement` (`$1` a time, `$2` the batch size) until it changes
/// fewer rows than a batch.
async fn batches(
    core: &Core,
    statement: &merezha_postgres::Statement,
    at: jiff::Timestamp,
    deadline: Instant,
) -> Result<(u64, bool), Failure> {
    let mut total = 0;
    for _ in 0..MAX_BATCHES {
        if Instant::now() + crate::maintenance::RETENTION_MARGIN >= deadline {
            return Ok((total, true));
        }
        let client = core.database.acquire(deadline).await?;
        let changed = core
            .database
            .execute(&client, statement, &[&at, &BATCH])
            .await
            .map_err(|e| classify(&e))?;
        total += changed;
        if changed < BATCH as u64 {
            return Ok((total, false));
        }
    }
    // Every batch came back full: the next `retention` run goes on.
    Ok((total, true))
}

/// The webhook part of `retention`: the rows it changed, and whether a
/// step stopped with work left (the caller then chains another run).
pub async fn retention(core: &Arc<Core>, deadline: Instant) -> Result<(u64, bool), Failure> {
    let now = core.now();
    let retention = &core.settings.retention;
    // Each step runs whatever the earlier ones did, so a failing step never
    // keeps the reseal and its gauges from running; the first failure is
    // the task's outcome, every one is logged.
    let mut first_failure = None;
    let mut total = 0;
    let mut more = false;
    let mut step = |name: &'static str, result: Result<(u64, bool), Failure>| match result {
        Ok((rows, left)) => {
            total += rows;
            more |= left;
        }
        Err(failure) => {
            merezha_telemetry::event!(
                WARN,
                "notification.maintenance_step_failed",
                task = "retention",
                reason = name,
                code = failure.code.as_str()
            );
            first_failure.get_or_insert(failure);
        }
    };
    let at = |by: Duration| before(now, by);
    let bodies = match at(retention.webhook_deliveries) {
        Ok(at) => batches(core, &sql::RETENTION_BODIES, at, deadline).await,
        Err(failure) => Err(failure),
    };
    step("webhook_bodies", bodies);
    let events = match at(retention.deliveries) {
        Ok(at) => batches(core, &sql::RETENTION_EVENTS, at, deadline).await,
        Err(failure) => Err(failure),
    };
    step("webhook_events", events);
    let audit = match at(AUDIT_RETENTION) {
        Ok(at) => batches(core, &sql::RETENTION_AUDIT, at, deadline).await,
        Err(failure) => Err(failure),
    };
    step("webhook_audit", audit);
    step(
        "previous_secrets",
        batches(core, &sql::PREVIOUS_EXPIRE, now, deadline).await,
    );
    step(
        "reseal",
        reseal(core, deadline, ResealBounds::default())
            .await
            .map(|r| (r.resealed, false)),
    );
    match first_failure {
        Some(failure) => Err(failure),
        None => Ok((total, more)),
    }
}

/// How much one [`reseal`] run may do: `batches` pages of `batch` rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResealBounds {
    pub batch: i64,
    pub batches: usize,
}
impl Default for ResealBounds {
    fn default() -> Self {
        Self {
            batch: BATCH,
            batches: MAX_BATCHES,
        }
    }
}

/// What a [`reseal`] run did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Resealed {
    /// Endpoints sealed again under `secret_key`.
    pub resealed: u64,
    /// Endpoints still holding a secret under another key afterwards: the
    /// value of `merezha.webhook.secrets.previous_key`. `secret_key_previous`
    /// may be removed once it is 0. Unreadable endpoints are not counted.
    pub remaining: u64,
    /// Endpoints whose secret neither key opens: the value of
    /// `merezha.webhook.secrets.unreadable`.
    pub unreadable: u64,
}

/// While `secret_key_previous` is set, seal again under `secret_key` the
/// secrets of the endpoints whose `key_id` or `previous_key_id` names
/// another key, at most `bounds` per run; each run picks up the rows the
/// last one left, since resealed rows no longer match. A secret neither
/// key opens is marked unreadable under the current key pair and skipped
/// from then on (its deliveries fail `secret_unreadable`; rotating the
/// endpoint's secret or deleting the endpoint clears it). Then counts what
/// remains and the unreadable rows, and records both gauges.
pub async fn reseal(
    core: &Core,
    deadline: Instant,
    bounds: ResealBounds,
) -> Result<Resealed, Failure> {
    let secrets = SecretBox::new(
        &core.settings.secret_key,
        core.settings.secret_key_previous.as_ref(),
    );
    let current = secrets.key_id().to_vec();
    let pair = secrets.key_pair().to_vec();
    let mut resealed = 0;
    if secrets.has_previous() {
        let mut after = Uuid::nil();
        for _ in 0..bounds.batches {
            let client = core.database.acquire(deadline).await?;
            let rows = core
                .database
                .query(
                    &client,
                    &sql::STALE_SECRETS,
                    &[&after, &current, &bounds.batch, &pair],
                )
                .await
                .map_err(|e| classify(&e))?;
            for row in &rows {
                let id: Uuid = row.get(0);
                after = id;
                let (ct, nonce, key): (Vec<u8>, Vec<u8>, Vec<u8>) =
                    (row.get(1), row.get(2), row.get(3));
                let previous_ct: Option<Vec<u8>> = row.get(4);
                let previous_nonce: Option<Vec<u8>> = row.get(5);
                let previous_key: Option<Vec<u8>> = row.get(6);
                let Ok((secret, _)) = secrets.open(id, &ct, &nonce, Some(&key)) else {
                    // Neither key opens it: skipped by later runs until the
                    // keys change or the secret is rotated.
                    core.database
                        .execute(&client, &sql::MARK_UNREADABLE, &[&id, &pair, &ct])
                        .await
                        .map_err(|e| classify(&e))?;
                    continue;
                };
                let sealed = secrets.seal(id, &secret, core.random.as_ref())?;
                let (prior_ct, prior_nonce) = match (&previous_ct, &previous_nonce) {
                    (Some(pct), Some(pnonce)) => {
                        match secrets.open(id, pct, pnonce, previous_key.as_deref()) {
                            Ok((prior, _)) => {
                                let sealed = secrets.seal(id, &prior, core.random.as_ref())?;
                                (Some(sealed.ciphertext), Some(sealed.nonce))
                            }
                            // A previous secret no key opens is dropped.
                            Err(_) => (None, None),
                        }
                    }
                    _ => (None, None),
                };
                resealed += core
                    .database
                    .execute(
                        &client,
                        &sql::SECRETS_RESEAL,
                        &[
                            &id,
                            &sealed.ciphertext,
                            &sealed.nonce,
                            &prior_ct,
                            &prior_nonce,
                            &ct,
                            &current,
                        ],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
            }
            if (rows.len() as i64) < bounds.batch {
                break;
            }
        }
    }
    let counts = count_secrets(core, deadline).await?;
    let (remaining, unreadable) = (counts.previous_key, counts.unreadable);
    Ok(Resealed {
        resealed,
        remaining,
        unreadable,
    })
}

/// Where the webhook signing secrets stand: what `GET
/// webhook-secrets/status` answers and the two gauges show.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SecretCounts {
    /// The id of the current key (`secret_key`).
    pub current_key_id: [u8; 8],
    pub previous_key_configured: bool,
    /// Endpoints with a secret still sealed under another key, unreadable
    /// ones left out: `secret_key_previous` may go only once this is 0.
    pub previous_key: u64,
    /// Endpoints whose secret neither configured key opened (tried under
    /// exactly these keys): already lost to both, so removing the previous
    /// key loses nothing more of theirs.
    pub unreadable: u64,
}
impl SecretCounts {
    /// Whether `secret_key_previous` can go without losing a secret: no
    /// endpoint holds one only it may open. Unreadable endpoints do not
    /// block it, since neither configured key opens them.
    pub fn previous_key_removable(&self) -> bool {
        self.previous_key == 0
    }
}

/// Count the endpoints still on another key and the unreadable ones,
/// straight from the database, and record both gauges.
pub async fn count_secrets(core: &Core, deadline: Instant) -> Result<SecretCounts, Failure> {
    let secrets = SecretBox::new(
        &core.settings.secret_key,
        core.settings.secret_key_previous.as_ref(),
    );
    let current = secrets.key_id();
    let pair = secrets.key_pair();
    let row = {
        let client = core.database.acquire(deadline).await?;
        core.database
            .query_one(
                &client,
                &sql::SECRET_COUNTS,
                &[&current.to_vec(), &pair.to_vec()],
            )
            .await
            .map_err(|e| classify(&e))?
    };
    let (previous_key, unreadable): (i64, i64) = (row.get(0), row.get(1));
    let counts = SecretCounts {
        current_key_id: current,
        previous_key_configured: secrets.has_previous(),
        previous_key: u64::try_from(previous_key).unwrap_or(0),
        unreadable: u64::try_from(unreadable).unwrap_or(0),
    };
    core.metrics
        .webhook_secrets(counts.previous_key, counts.unreadable);
    Ok(counts)
}

/// How often every instance counts the secrets again for the gauges.
pub const SECRET_COUNT_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Count the secrets again every [`SECRET_COUNT_INTERVAL`] on this
/// instance (the block counts once at start), so every instance's gauges
/// show a count it made; a failed count keeps the last one. The block
/// aborts the task at shutdown.
pub(crate) fn count_secrets_periodically(core: Arc<Core>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(SECRET_COUNT_INTERVAL).await;
            let deadline = Instant::now() + Duration::from_secs(10);
            let _ = count_secrets(&core, deadline).await;
        }
    })
}
