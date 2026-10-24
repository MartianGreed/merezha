//! Shared services handed to every module.
use crate::{
    edge::TraceContext,
    ports::{
        Authenticator, Authorizer, BlobStore, CaptureNotifications, Directory, EmptyDirectory,
        Jobs, JobsHandle, Notifier, NotifierFactory, Outbox, Source, SourceOutbox,
        UnavailableBlobStore, UnavailableJobs, UnavailableOutbox,
    },
};
use merezha_domain::Failure;
use merezha_postgres::Database;
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

/// Time source. Modules take time from here so tests can control it.
pub trait Clock: Send + Sync {
    /// Wall-clock time for timestamps.
    fn now(&self) -> SystemTime;
    /// Monotonic time for deadlines.
    fn monotonic(&self) -> Instant;
}
/// The operating system clocks.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
    fn monotonic(&self) -> Instant {
        Instant::now()
    }
}

/// Cryptographically secure randomness for tokens, keys and identifiers.
pub trait Random: Send + Sync {
    /// Fill `bytes` or fail transient `random_unavailable`; never returns
    /// predictable bytes.
    fn fill(&self, bytes: &mut [u8]) -> Result<(), Failure>;
}
/// The operating system CSPRNG through `getrandom`.
#[derive(Clone, Copy, Debug, Default)]
pub struct OsRandom;
impl Random for OsRandom {
    fn fill(&self, bytes: &mut [u8]) -> Result<(), Failure> {
        getrandom::fill(bytes).map_err(|_| Failure::transient("random_unavailable"))
    }
}
/// 256 random bits as lowercase hex, for session and API key secrets.
pub fn random_token(random: &dyn Random) -> Result<String, Failure> {
    let mut bytes = [0u8; 32];
    random.fill(&mut bytes)?;
    Ok(hex(&bytes))
}
pub(crate) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[usize::from(byte >> 4)] as char);
        out.push(DIGITS[usize::from(byte & 15)] as char);
    }
    out
}

/// A bounded outbound response.
#[derive(Clone, Debug)]
pub struct OutboundResponse {
    pub status: reqwest::StatusCode,
    pub headers: reqwest::header::HeaderMap,
    pub body: Vec<u8>,
}
/// Outbound HTTP: rustls with the platform trust store, no redirects, no
/// automatic retries, a connect timeout and a caller deadline on every call.
#[derive(Clone, Debug)]
pub struct OutboundHttp {
    client: reqwest::Client,
}
impl OutboundHttp {
    pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
    pub fn new() -> Result<Self, Failure> {
        let client = reqwest::Client::builder()
            .connect_timeout(Self::CONNECT_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .user_agent(concat!("merezha/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|_| Failure::permanent("http_client_config"))?;
        Ok(Self { client })
    }
    /// The underlying client, for building requests.
    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }
    /// Send `request`, read at most `max_body` bytes, all before `deadline`.
    /// A `trace` context is propagated as W3C `traceparent`. Failures are
    /// transient `upstream_timeout` / `upstream_unavailable` or permanent
    /// `upstream_body_limit`; the request is never retried here.
    pub async fn send(
        &self,
        mut request: reqwest::Request,
        deadline: Instant,
        max_body: usize,
        trace: Option<&TraceContext>,
    ) -> Result<OutboundResponse, Failure> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Failure::transient("upstream_timeout"));
        }
        *request.timeout_mut() = Some(remaining);
        if let Some(trace) = trace
            && let Ok(value) = reqwest::header::HeaderValue::from_str(&trace.child_header())
        {
            request.headers_mut().insert("traceparent", value);
        }
        let at = tokio::time::Instant::from_std(deadline);
        let exchange = async {
            let mut response = self.client.execute(request).await.map_err(transport)?;
            let status = response.status();
            let headers = response.headers().clone();
            let mut body = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(transport)? {
                if body.len() + chunk.len() > max_body {
                    return Err(Failure::permanent("upstream_body_limit"));
                }
                body.extend_from_slice(&chunk);
            }
            Ok(OutboundResponse {
                status,
                headers,
                body,
            })
        };
        match tokio::time::timeout_at(at, exchange).await {
            Ok(result) => result,
            Err(_) => Err(Failure::transient("upstream_timeout")),
        }
    }
}
fn transport(error: reqwest::Error) -> Failure {
    if error.is_timeout() {
        Failure::transient("upstream_timeout")
    } else {
        Failure::transient("upstream_unavailable")
    }
}

/// Everything modules share. Cheap to clone.
#[derive(Clone)]
pub struct AppContext {
    inner: Arc<Inner>,
    /// The pool, scoped to [`AppContext::module`] so its statements are
    /// labelled with the module (TE §1.3).
    database: Database,
    /// The module this context was handed to, which fixes the sources it
    /// may write as. `None` for the host's own context.
    module: Option<Source>,
    /// Kernel sources the host granted this module besides its own name.
    granted: Vec<Source>,
}
/// Sources only the host's own context binds: HTTP-supplied requests
/// (`api`) and reactions (`application`).
pub const KERNEL_SOURCES: [&str; 2] = ["api", "application"];
/// The only `(source, module)` grants a module may claim through
/// [`crate::Module::extra_sources`]: the notifications block's HTTP send
/// route writes as `api` (NT §3.1). `application` is never granted.
pub const GRANTABLE_SOURCES: [(&str, &str); 1] = [("api", "notifications")];

/// Check that module `name` may claim `extra` sources; fails permanent
/// `source_not_granted` otherwise.
pub fn check_source_grants(name: &str, extra: &[&str]) -> Result<(), Failure> {
    if extra
        .iter()
        .all(|source| GRANTABLE_SOURCES.contains(&(source, name)))
    {
        Ok(())
    } else {
        Err(Failure::permanent("source_not_granted"))
    }
}
struct Inner {
    app_id: String,
    instance_id: String,
    database: Database,
    migrate: bool,
    clock: Arc<dyn Clock>,
    random: Arc<dyn Random>,
    http: OutboundHttp,
    outbox: Arc<dyn Outbox>,
    jobs: Arc<dyn Jobs>,
    authenticator: Option<Arc<dyn Authenticator>>,
    authorizer: Arc<dyn Authorizer>,
    notifications: Arc<dyn NotifierFactory>,
    directory: Arc<dyn Directory>,
    blobs: Arc<dyn BlobStore>,
}
impl std::fmt::Debug for AppContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppContext")
            .field("app_id", &self.inner.app_id)
            .field("database", &self.inner.database)
            .finish_non_exhaustive()
    }
}
impl AppContext {
    /// Start from the disabled-block fallbacks: no authenticator, the given
    /// policy, capture notifications, an empty directory, a refusing outbox,
    /// jobs port and blob store.
    pub fn builder(
        app_id: &str,
        database: Database,
        authorizer: Arc<dyn Authorizer>,
    ) -> AppContextBuilder {
        AppContextBuilder {
            app_id: app_id.to_owned(),
            instance_id: crate::config::default_instance_id(None),
            database,
            migrate: false,
            clock: Arc::new(SystemClock),
            random: Arc::new(OsRandom),
            http: None,
            outbox: Arc::new(UnavailableOutbox),
            jobs: Arc::new(UnavailableJobs),
            authenticator: None,
            authorizer,
            notifications: Arc::new(CaptureNotifications::default()),
            directory: Arc::new(EmptyDirectory),
            blobs: Arc::new(UnavailableBlobStore),
        }
    }
    pub fn app_id(&self) -> &str {
        &self.inner.app_id
    }
    /// `app.instance_id`: this process among the application's instances.
    pub fn instance_id(&self) -> &str {
        &self.inner.instance_id
    }
    /// The pool. A module's context hands it out scoped to the module:
    /// its statements, transactions and listeners carry
    /// `merezha.module = <module>`.
    pub fn database(&self) -> &Database {
        &self.database
    }
    /// `database.migrate`: this host applies schema changes at startup.
    /// Modules with schema work of their own beyond migrations (content
    /// model activation) do it only when this is `true` and otherwise
    /// verify it, reporting not ready while it is pending.
    pub fn migrate(&self) -> bool {
        self.inner.migrate
    }
    pub fn clock(&self) -> &dyn Clock {
        self.inner.clock.as_ref()
    }
    pub fn random(&self) -> &dyn Random {
        self.inner.random.as_ref()
    }
    pub fn http(&self) -> &OutboundHttp {
        &self.inner.http
    }
    /// The module this context belongs to; `None` for the host's own.
    pub fn module(&self) -> Option<&str> {
        self.module.as_ref().map(Source::as_str)
    }
    /// This context as handed to module `name`: its handles can only be
    /// bound to `name`. The host binds each module's context at start.
    pub(crate) fn for_module(&self, name: &'static str, extra: &[&str]) -> Result<Self, Failure> {
        check_source_grants(name, extra)?;
        Ok(Self {
            inner: self.inner.clone(),
            database: self.inner.database.for_module(name),
            module: Some(Source::new(name)?),
            granted: extra
                .iter()
                .map(|source| Source::new(source))
                .collect::<Result<_, _>>()?,
        })
    }
    /// The source a handle may be bound to: a module's context binds its
    /// own name and the kernel sources the host granted it
    /// ([`GRANTABLE_SOURCES`]), so blocks cannot write in each other's
    /// dedupe namespace or claim identity's membership exemption; the host's
    /// context binds any source, [`KERNEL_SOURCES`] included. Fails
    /// permanent `invalid_source` or `source_not_allowed`.
    fn source(&self, source: &str) -> Result<Source, Failure> {
        let source = Source::new(source)?;
        match &self.module {
            Some(own) if *own != source && !self.granted.contains(&source) => {
                Err(Failure::permanent("source_not_allowed"))
            }
            _ => Ok(source),
        }
    }
    /// The outbox bound to `source`. It validates every append before the
    /// implementation sees it. See [`AppContext::module`] for which sources
    /// a context may bind.
    pub fn outbox(&self, source: &str) -> Result<SourceOutbox, Failure> {
        Ok(SourceOutbox::new(
            self.source(source)?,
            self.inner.outbox.clone(),
        ))
    }
    /// The jobs port bound to `source` (AP §B.3), with the same source rule
    /// as [`AppContext::outbox`].
    pub fn jobs(&self, source: &str) -> Result<JobsHandle, Failure> {
        Ok(JobsHandle::new(
            self.source(source)?,
            self.inner.jobs.clone(),
        ))
    }
    pub fn authenticator(&self) -> Option<&Arc<dyn Authenticator>> {
        self.inner.authenticator.as_ref()
    }
    pub fn authorizer(&self) -> &Arc<dyn Authorizer> {
        &self.inner.authorizer
    }
    /// The notifier bound to `source`, with the same source rule as
    /// [`AppContext::outbox`].
    pub fn notifier(&self, source: &str) -> Result<Arc<dyn Notifier>, Failure> {
        Ok(self.inner.notifications.bind(&self.source(source)?))
    }
    /// `false` while notifications are only captured: blocks that must not
    /// lose security mail refuse to start on that.
    pub fn notifications_deliver(&self) -> bool {
        self.inner.notifications.delivers()
    }
    pub fn directory(&self) -> &Arc<dyn Directory> {
        &self.inner.directory
    }
    pub fn blobs(&self) -> &Arc<dyn BlobStore> {
        &self.inner.blobs
    }
}
/// Replaces fallbacks with block implementations.
pub struct AppContextBuilder {
    app_id: String,
    instance_id: String,
    database: Database,
    migrate: bool,
    clock: Arc<dyn Clock>,
    random: Arc<dyn Random>,
    http: Option<OutboundHttp>,
    outbox: Arc<dyn Outbox>,
    jobs: Arc<dyn Jobs>,
    authenticator: Option<Arc<dyn Authenticator>>,
    authorizer: Arc<dyn Authorizer>,
    notifications: Arc<dyn NotifierFactory>,
    directory: Arc<dyn Directory>,
    blobs: Arc<dyn BlobStore>,
}
impl AppContextBuilder {
    pub fn instance_id(mut self, instance_id: &str) -> Self {
        self.instance_id = instance_id.to_owned();
        self
    }
    /// See [`AppContext::migrate`]; `false` by default.
    pub fn migrate(mut self, migrate: bool) -> Self {
        self.migrate = migrate;
        self
    }
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }
    pub fn random(mut self, random: Arc<dyn Random>) -> Self {
        self.random = random;
        self
    }
    pub fn http(mut self, http: OutboundHttp) -> Self {
        self.http = Some(http);
        self
    }
    pub fn outbox(mut self, outbox: Arc<dyn Outbox>) -> Self {
        self.outbox = outbox;
        self
    }
    pub fn jobs(mut self, jobs: Arc<dyn Jobs>) -> Self {
        self.jobs = jobs;
        self
    }
    pub fn authenticator(mut self, authenticator: Arc<dyn Authenticator>) -> Self {
        self.authenticator = Some(authenticator);
        self
    }
    pub fn notifications(mut self, notifications: Arc<dyn NotifierFactory>) -> Self {
        self.notifications = notifications;
        self
    }
    pub fn directory(mut self, directory: Arc<dyn Directory>) -> Self {
        self.directory = directory;
        self
    }
    pub fn blobs(mut self, blobs: Arc<dyn BlobStore>) -> Self {
        self.blobs = blobs;
        self
    }
    pub fn build(self) -> Result<AppContext, Failure> {
        let http = match self.http {
            Some(http) => http,
            None => OutboundHttp::new()?,
        };
        Ok(AppContext {
            module: None,
            granted: Vec::new(),
            database: self.database.clone(),
            inner: Arc::new(Inner {
                app_id: self.app_id,
                instance_id: self.instance_id,
                database: self.database,
                migrate: self.migrate,
                clock: self.clock,
                random: self.random,
                http,
                outbox: self.outbox,
                jobs: self.jobs,
                authenticator: self.authenticator,
                authorizer: self.authorizer,
                notifications: self.notifications,
                directory: self.directory,
                blobs: self.blobs,
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_256_bit_hex_and_distinct() {
        let a = random_token(&OsRandom).unwrap();
        let b = random_token(&OsRandom).unwrap();
        assert_eq!(a.len(), 64);
        assert!(a.bytes().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
    }
    #[tokio::test]
    async fn outbound_calls_respect_deadline_and_body_limit() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut seen = Vec::new();
            for response in [
                "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
                "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
                "",
                "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            ] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buffer = vec![0; 4096];
                let n = socket.read(&mut buffer).await.unwrap();
                seen.push(String::from_utf8_lossy(&buffer[..n]).to_string());
                if response.is_empty() {
                    // Never answer: the caller's deadline must fire.
                    tokio::time::sleep(Duration::from_secs(3)).await;
                } else {
                    socket.write_all(response.as_bytes()).await.unwrap();
                }
            }
            seen
        });
        let http = OutboundHttp::new().unwrap();
        let url = format!("http://{address}/hook");
        let soon = || Instant::now() + Duration::from_secs(5);
        let trace =
            TraceContext::parse("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01").unwrap();
        let ok = http
            .send(
                http.client().get(&url).build().unwrap(),
                soon(),
                16,
                Some(&trace),
            )
            .await
            .unwrap();
        assert_eq!(
            (ok.status.as_u16(), ok.body.as_slice()),
            (200, &b"hello"[..])
        );
        let limited = http
            .send(http.client().get(&url).build().unwrap(), soon(), 4, None)
            .await
            .unwrap_err();
        assert_eq!(limited.code, "upstream_body_limit");
        let started = Instant::now();
        let late = http
            .send(
                http.client().get(&url).build().unwrap(),
                Instant::now() + Duration::from_millis(200),
                16,
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(late.code, "upstream_timeout");
        assert!(started.elapsed() < Duration::from_secs(2));
        // A 503 is returned as is: no automatic retry hides it.
        let unavailable = http
            .send(http.client().get(&url).build().unwrap(), soon(), 16, None)
            .await
            .unwrap();
        assert_eq!(unavailable.status.as_u16(), 503);
        let seen = server.await.unwrap();
        assert_eq!(seen.len(), 4, "exactly one attempt per call");
        assert!(seen[0].contains("traceparent: 00-0af7651916cd43dd8448eb211c80319c-"));
        assert!(
            !seen[0].contains("b7ad6b7169203331"),
            "a child span id is sent"
        );
    }

    #[test]
    fn module_contexts_bind_only_their_own_source() {
        let db = merezha_postgres::Database::connect(&merezha_postgres::DatabaseConfig::new(
            merezha_postgres::Secret::new("postgres://127.0.0.1:1/none"),
        ))
        .unwrap();
        let policy = Arc::new(crate::ports::DenyAll);
        let host = AppContext::builder("tasks", db, policy).build().unwrap();
        assert_eq!(host.module(), None);
        for source in ["identity", "api", "application"] {
            assert!(host.outbox(source).is_ok() && host.notifier(source).is_ok());
        }
        let content = host.for_module("content", &[]).unwrap();
        assert_eq!(content.module(), Some("content"));
        // Its pool handle labels statements with the module (TE §1.3).
        assert_eq!(content.database().module(), "content");
        assert_eq!(host.database().module(), "kernel");
        assert_eq!(
            content.outbox("content").unwrap().source().as_str(),
            "content"
        );
        assert!(content.notifier("content").is_ok());
        for spoof in ["identity", "api", "application", "notifications"] {
            assert_eq!(
                content.outbox(spoof).unwrap_err().code,
                "source_not_allowed"
            );
            assert_eq!(
                content.notifier(spoof).err().map(|f| f.code),
                Some("source_not_allowed".into())
            );
        }
        assert_eq!(content.outbox("Bad").unwrap_err().code, "invalid_source");
        assert_eq!(
            content.jobs("content").unwrap().source().as_str(),
            "content"
        );
        assert_eq!(
            content.jobs("identity").unwrap_err().code,
            "source_not_allowed"
        );
    }

    #[test]
    fn only_notifications_is_granted_the_api_source() {
        let db = merezha_postgres::Database::connect(&merezha_postgres::DatabaseConfig::new(
            merezha_postgres::Secret::new("postgres://127.0.0.1:1/none"),
        ))
        .unwrap();
        let host = AppContext::builder("tasks", db, Arc::new(crate::ports::DenyAll))
            .build()
            .unwrap();
        let notifications = host.for_module("notifications", &["api"]).unwrap();
        for source in ["notifications", "api"] {
            assert!(notifications.outbox(source).is_ok(), "{source}");
            assert!(notifications.notifier(source).is_ok(), "{source}");
        }
        for spoof in ["application", "identity"] {
            assert_eq!(
                notifications.notifier(spoof).err().map(|f| f.code),
                Some("source_not_allowed".into())
            );
        }
        // Without the claim, even notifications binds only its own name.
        let plain = host.for_module("notifications", &[]).unwrap();
        assert_eq!(plain.outbox("api").unwrap_err().code, "source_not_allowed");
        for (module, extra) in [
            ("content", "api"),
            ("identity", "api"),
            ("notifications", "application"),
            ("content", "application"),
        ] {
            assert_eq!(
                host.for_module(module, &[extra]).unwrap_err().code,
                "source_not_granted",
                "{module} {extra}"
            );
        }
    }
}
