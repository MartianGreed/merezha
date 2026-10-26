//! Shared fixtures for the identity PostgreSQL tests: a project on a
//! throwaway database, a fake clock, a capture notifier that keeps the
//! emailed tokens (and writes the flagged outbox record the notifications
//! block would write), and a small browser with a cookie jar.
#![allow(dead_code)]
pub mod idp;

use axum::{
    Router,
    body::Body,
    extract::ConnectInfo,
    http::{Request, Response, StatusCode, header},
};
use merezha_identity::{HostFacts, IdentityHandle, IdentityModule, MIGRATIONS};
use merezha_jobs::{PgJobs, PgOutbox, RelayMode};
use merezha_platform::{
    AppContext, BoxFuture, Clock, ConfigInput, Database, ErrorRegistry, JobsRuntime, Module,
    ProjectConfig, Tx, edge,
    host::configure,
    ports::{
        Address, LoopbackAllowAll, NotificationHandle, NotificationRequest, Notifier,
        NotifierFactory, NotifyError, OnDuplicate, Outbox, OutboxRecord, OutboxTarget, Recipient,
        Source, WebhookEvent, WebhookEventHandle,
    },
};
use merezha_postgres::{MigrationMode, ModuleMigrations, TestDatabase, run_migrations};
use serde_json::{Map, Value, json};
use std::{
    net::{IpAddr, SocketAddr},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use tower::ServiceExt;

pub const APP: &str = "https://app.example";
pub const COOKIE: &str = "__Host-merezha_session";
pub const BINDING: &str = "__Host-merezha_verify";
pub const OAUTH: &str = "__Host-merezha_oauth";
pub const SECRET: &str = "an-identity-secret-key-of-at-least-32-bytes";
pub const SECRET_NEXT: &str = "the-next-identity-secret-key-after-rotation";
/// Every rate-limit table, so tests can raise the defaults.
pub const POLICY_TABLES: [&str; 15] = [
    "sign_in_ip",
    "sign_in_account_ip",
    "sign_in_account",
    "registration_ip",
    "email_address_ip",
    "email_address",
    "email_ip",
    "token_ip",
    "reauth_session",
    "invitation_organization",
    "api_key_invalid_ip",
    "passkey_ip",
    "oauth_ip",
    "oauth_callback_ip",
    "oauth_callback_unknown_ip",
];
/// An identity section: `top` keys, cheap Argon2 at its floor, magic links
/// on, every rate limit at 1000 per hour unless `limits` names it, then
/// `tables`.
pub fn section(top: &str, limits: &[(&str, u32, &str)], tables: &str) -> String {
    let mut text = format!(
        "[modules.identity]\napp_origin = \"{APP}\"\nsecret_key = {{ env = \"IDENTITY_SECRET\" }}\n{top}\n\
         [modules.identity.password]\nargon2_memory_kib = 19456\nargon2_iterations = 2\nhash_concurrency = 4\n\
         [modules.identity.magic_links]\n"
    );
    for table in POLICY_TABLES {
        let (limit, period) = limits
            .iter()
            .find(|(name, _, _)| *name == table)
            .map_or((1000, "1h"), |(_, l, p)| (*l, *p));
        text.push_str(&format!(
            "[modules.identity.rate_limits.{table}]\nlimit = {limit}\nperiod = \"{period}\"\n"
        ));
    }
    text.push_str(tables);
    text
}
/// The default section.
pub fn base() -> String {
    section("", &[], "")
}

/// Connections per world: CI's PostgreSQL allows 100, and a test binary
/// runs its tests, each with its own world and instances, in parallel.
pub const POOL: usize = 8;

pub fn soon() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

/// A clock tests move by hand.
#[derive(Debug)]
pub struct FakeClock(Mutex<SystemTime>);
impl FakeClock {
    pub fn new() -> Arc<Self> {
        // A fixed instant well after the UUIDv7 epoch.
        Arc::new(Self(Mutex::new(
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000),
        )))
    }
    pub fn advance(&self, by: Duration) {
        *self.0.lock().unwrap() += by;
    }
}
impl Clock for FakeClock {
    fn now(&self) -> SystemTime {
        *self.0.lock().unwrap()
    }
    fn monotonic(&self) -> Instant {
        Instant::now()
    }
}

/// One request the identity block made, with its secret data.
#[derive(Clone, Debug)]
pub struct Sent {
    pub topic: String,
    /// The explicit address, or `user:<id>` for user notices.
    pub to: String,
    pub data: Map<String, Value>,
    pub secret: Map<String, Value>,
    pub dedupe: String,
}
impl Sent {
    /// The token in the link's fragment.
    pub fn token(&self) -> String {
        let link = self.secret["link"].as_str().expect("a link");
        link.split_once("#token=")
            .expect("a token fragment")
            .1
            .to_owned()
    }
}

/// The capture notifier: keeps every request, and appends the flagged
/// outbox record the notifications block writes (R3) in the caller's
/// transaction.
#[derive(Default)]
pub struct Mail {
    sent: Mutex<Vec<Sent>>,
    pub fail: AtomicBool,
}
impl Mail {
    pub fn sent(&self) -> Vec<Sent> {
        self.sent.lock().unwrap().clone()
    }
    pub fn clear(&self) {
        self.sent.lock().unwrap().clear();
    }
    /// The requests of `topic` to `to`.
    pub fn to(&self, topic: &str, to: &str) -> Vec<Sent> {
        self.sent()
            .into_iter()
            .filter(|s| s.topic == topic && s.to == to)
            .collect()
    }
    /// The last token mailed on `topic` to `to`.
    pub fn token(&self, topic: &str, to: &str) -> String {
        self.to(topic, to).last().expect("a mail").token()
    }
}
struct MailNotifier(Arc<Mail>);
impl Notifier for MailNotifier {
    fn validate(&self, _: &NotificationRequest) -> Result<(), NotifyError> {
        Ok(())
    }
    fn request<'a>(
        &'a self,
        tx: &'a mut Tx<'_>,
        request: NotificationRequest,
    ) -> BoxFuture<'a, Result<NotificationHandle, NotifyError>> {
        Box::pin(async move {
            if self.0.fail.load(Ordering::SeqCst) {
                return Err(NotifyError::Unavailable);
            }
            let to = match &request.recipients[0] {
                Recipient::Address(Address::Email { address, .. }) => address.expose().to_owned(),
                Recipient::User { id } => format!("user:{}", id.as_str()),
                other => format!("{other:?}"),
            };
            let record = OutboxRecord {
                dedupe_key: request.dedupe_key.clone(),
                target: OutboxTarget::Job {
                    queue: merezha_domain::Identity::new("notify.email").unwrap(),
                    kind: merezha_domain::Identity::new("notify.deliver").unwrap(),
                },
                payload: json!({"topic": request.topic.as_str(), "data": request.data,
                                "secret": request.secret_data}),
                contains_secret: !request.secret_data.is_empty(),
                run_at: None,
                priority: 0,
                context: merezha_platform::ports::MessageContext {
                    correlation_id: request.correlation.correlation_id.clone(),
                    causation_id: None,
                    tenant: None,
                },
            };
            PgOutbox
                .append(tx, "notifications", &[record], OnDuplicate::Ignore)
                .await
                .map_err(|_| NotifyError::Unavailable)?;
            self.0.sent.lock().unwrap().push(Sent {
                topic: request.topic.as_str().to_owned(),
                to,
                data: request.data.clone(),
                secret: request.secret_data.clone(),
                dedupe: request.dedupe_key.as_str().to_owned(),
            });
            Ok(NotificationHandle {
                notification_id: uuid::Uuid::nil(),
                duplicate: false,
            })
        })
    }
    fn emit_webhook<'a>(
        &'a self,
        _: &'a mut Tx<'_>,
        _: WebhookEvent,
    ) -> BoxFuture<'a, Result<WebhookEventHandle, NotifyError>> {
        Box::pin(async { Err(NotifyError::WebhooksDisabled) })
    }
}
pub struct MailFactory(pub Arc<Mail>);
impl NotifierFactory for MailFactory {
    fn bind(&self, _: &Source) -> Arc<dyn Notifier> {
        Arc::new(MailNotifier(self.0.clone()))
    }
}

/// Application permissions of the test catalog.
pub const TASKS: [&str; 4] = [
    "tasks.read",
    "tasks.create",
    "tasks.delete",
    "tasks.admin.purge",
];
/// The test catalog: identity's and the host's permissions, content's,
/// [`TASKS`], and the resource kinds `content_type` and `project`.
pub fn catalogued(mut module: IdentityModule) -> IdentityModule {
    module
        .add_block_permissions(
            "content",
            &["content.entries.read", "content.entries.publish"],
        )
        .unwrap();
    module
        .add_application_permissions(TASKS.iter().map(|t| (*t, "")))
        .unwrap();
    module.add_resource_kinds(&["content_type", "project"]);
    module
}

/// The probe route of [`Instance`]: `{"allowed": bool, "break_glass": bool,
/// "code": ...}` for one check.
fn check_route(
    authorizer: &dyn merezha_platform::ports::Authorizer,
    request: axum::extract::Request,
) -> axum::Json<Value> {
    use merezha_platform::ports::{Allowed, ResourceScope};
    let principal = request
        .extensions()
        .get::<edge::Authenticated>()
        .and_then(|a| a.0.clone());
    let query = request.uri().query().unwrap_or("").to_owned();
    let get = |key: &str| {
        query
            .split('&')
            .filter_map(|p| p.split_once('='))
            .find(|(k, _)| *k == key)
            .map(|(_, v)| merezha_domain::Identity::new(v).unwrap())
    };
    let permission = get("permission").unwrap();
    let scope = match get("scope").as_ref().map(|s| s.as_str()) {
        Some("tenant") => ResourceScope::Tenant(get("tenant").unwrap()),
        Some("resource") => ResourceScope::Resource {
            tenant: get("tenant"),
            kind: get("kind").unwrap(),
            id: get("id").unwrap(),
        },
        _ => ResourceScope::Application,
    };
    let result = authorizer.check(principal.as_deref(), &permission, &scope);
    axum::Json(json!({
        "allowed": result.is_ok(),
        "break_glass": result == Ok(Allowed::BreakGlass),
        "code": result.err().map(|d| d.failure().code),
        "tenant": principal.as_ref().and_then(|p| p.tenant.as_ref().map(|t| t.as_str().to_owned())),
    }))
}

/// A project directory with `merezha.toml` and the identity section.
pub fn project(dir: &Path, section: &str) -> ConfigInput {
    std::fs::write(
        dir.join("merezha.toml"),
        format!(
            "[app]\nid = \"tasks\"\nname = \"Tasks\"\n[database]\nurl = {{ env = \"IDENTITY_DB\" }}\n\
             migrate = true\n[runtime]\nmanifest = \"runtime.toml\"\n{section}"
        ),
    )
    .unwrap();
    ConfigInput {
        path: dir.join("merezha.toml"),
        env: vec![
            ("IDENTITY_DB".into(), "postgres://127.0.0.1:1/unused".into()),
            ("IDENTITY_SECRET".into(), SECRET.into()),
            ("IDENTITY_SECRET_NEXT".into(), SECRET_NEXT.into()),
            ("OAUTH_SECRET".into(), idp::CLIENT_SECRET.into()),
        ],
        overrides: Vec::new(),
    }
}

/// The project, the configured module and its handles.
pub type Configured = (ProjectConfig, Box<dyn Module>, IdentityModuleParts);
/// Configure the identity module alone; `Err` carries `(key, code)`.
pub fn configured(
    section: &str,
    facts: Option<HostFacts>,
) -> Result<Configured, Vec<(String, String)>> {
    let dir = tempfile::tempdir().unwrap();
    let input = project(dir.path(), section);
    let module = catalogued(IdentityModule::new(facts));
    let parts = IdentityModuleParts {
        handle: module.handle(),
        authenticator: module.authenticator(),
    };
    match configure(&input, vec![Box::new(module)]) {
        Ok((config, mut modules)) => {
            assert_eq!(modules.len(), 1, "the identity section is present");
            Ok((config, modules.remove(0), parts))
        }
        Err(errors) => Err(errors
            .0
            .iter()
            .map(|i| (i.key.clone(), i.code.to_owned()))
            .collect()),
    }
}
pub struct IdentityModuleParts {
    pub handle: IdentityHandle,
    pub authenticator: Arc<dyn merezha_platform::ports::Authenticator>,
}

/// A migrated database for `test`.
pub async fn database(test: &TestDatabase, pool: usize) -> Database {
    let mut config = test.config();
    config.pool_size = pool;
    config.statement_timeout = Duration::from_secs(30);
    let db = Database::connect(&config).unwrap();
    run_migrations(
        &db,
        &[
            ModuleMigrations {
                module: "identity",
                migrations: MIGRATIONS,
            },
            ModuleMigrations {
                module: "jobs",
                migrations: PgJobs::migrations(),
            },
        ],
        MigrationMode::Apply,
        soon(),
    )
    .await
    .unwrap();
    db
}

/// One host instance: the identity routes behind the platform edge with the
/// session authenticator.
#[derive(Clone)]
pub struct Instance {
    pub router: Router,
    /// The started block, for its readiness.
    pub module: Arc<dyn Module>,
    pub handle: IdentityHandle,
    pub authorizer: Arc<dyn merezha_platform::ports::Authorizer>,
    pub jobs: PgJobs,
    pub db: Database,
    /// The jobs workers run, so link requests are fulfilled.
    pub workers: bool,
    /// One settle poll at a time, so parallel requests do not crowd the
    /// pool with their waits.
    pub settling: Arc<tokio::sync::Mutex<()>>,
}
impl Instance {
    /// Wait until every queued `identity.link_request` job ran: the request
    /// path only queues it (section 7).
    pub async fn settle(&self) {
        if !self.workers {
            return;
        }
        let _one = self.settling.lock().await;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let client = self.db.acquire(soon()).await.unwrap();
            let open: i64 = client
                .query_one(
                    "SELECT (SELECT count(*) FROM merezha_jobs.outbox
                              WHERE source = 'identity' AND relayed_at IS NULL AND quarantined_at IS NULL)
                          + (SELECT count(*) FROM merezha_jobs.jobs
                              WHERE kind = 'identity.link_request' AND state IN ('available', 'running'))",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            if open == 0 {
                return;
            }
            assert!(Instant::now() < deadline, "link requests never ran");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// A test world: one database, one clock, one mailbox, and instances.
pub struct World {
    pub test: TestDatabase,
    pub db: Database,
    pub clock: Arc<FakeClock>,
    pub mail: Arc<Mail>,
    pub section: String,
    /// Provider endpoint overrides every new instance gets.
    pub endpoints: Mutex<Vec<(String, merezha_identity::oauth::ProviderEndpoints)>>,
}
impl World {
    /// `None` when `MEREZHA_TEST_DATABASE_URL` is unset.
    pub async fn new(section: &str) -> Option<Self> {
        Self::with_pool(
            section,
            POOL,
            Duration::from_secs(5),
            Duration::from_secs(30),
        )
        .await
    }
    /// A world whose pool has `pool` connections, the acquisition timeout
    /// `acquire` and the statement timeout `statement`.
    pub async fn with_pool(
        section: &str,
        pool: usize,
        acquire: Duration,
        statement: Duration,
    ) -> Option<Self> {
        Self::build(section, pool, acquire, statement, None).await
    }
    /// A world whose database records its statements on `meter`.
    pub async fn with_meter(section: &str, meter: &opentelemetry::metrics::Meter) -> Option<Self> {
        Self::build(
            section,
            POOL,
            Duration::from_secs(5),
            Duration::from_secs(30),
            Some(meter),
        )
        .await
    }
    async fn build(
        section: &str,
        pool: usize,
        acquire: Duration,
        statement: Duration,
        meter: Option<&opentelemetry::metrics::Meter>,
    ) -> Option<Self> {
        let test = TestDatabase::create()?;
        let mut config = test.config();
        config.pool_size = pool;
        config.acquire_timeout = acquire;
        config.statement_timeout = statement;
        let db = match meter {
            Some(meter) => Database::connect_with_meter(&config, meter).unwrap(),
            None => Database::connect(&config).unwrap(),
        };
        run_migrations(
            &db,
            &[
                ModuleMigrations {
                    module: "identity",
                    migrations: MIGRATIONS,
                },
                ModuleMigrations {
                    module: "jobs",
                    migrations: PgJobs::migrations(),
                },
            ],
            MigrationMode::Apply,
            soon(),
        )
        .await
        .unwrap();
        Some(Self {
            test,
            db,
            clock: FakeClock::new(),
            mail: Arc::new(Mail::default()),
            section: section.to_owned(),
            endpoints: Mutex::new(Vec::new()),
        })
    }
    /// Start one more instance sharing the database, clock and mailbox.
    pub async fn instance(&self) -> Instance {
        self.instance_with(None).await
    }
    pub async fn instance_with(&self, meter: Option<&opentelemetry::metrics::Meter>) -> Instance {
        self.instance_full(meter, true).await
    }
    /// An instance whose jobs workers run only when `workers`.
    pub async fn instance_full(
        &self,
        meter: Option<&opentelemetry::metrics::Meter>,
        workers: bool,
    ) -> Instance {
        let dir = tempfile::tempdir().unwrap();
        let input = project(dir.path(), &self.section);
        let mut module = catalogued(IdentityModule::new(Some(HostFacts::loopback())));
        for (name, endpoints) in self.endpoints.lock().unwrap().iter() {
            module.override_provider_endpoints(name, endpoints.clone());
        }
        if let Some(meter) = meter {
            module = module.with_meter(meter);
        }
        let handle = module.handle();
        let authenticator = module.authenticator();
        let authorizer = handle.authorizer();
        let (config, mut modules) = configure(&input, vec![Box::new(module)]).unwrap();
        let module = modules.remove(0);
        let jobs = PgJobs::with_mode(
            &config.jobs,
            if workers {
                RelayMode::Listen
            } else {
                RelayMode::Off
            },
        );
        let ports = jobs.ports(&self.db);
        let ctx = AppContext::builder(
            "tasks",
            self.db.clone(),
            Arc::new(LoopbackAllowAll::new("127.0.0.1:1".parse().unwrap()).unwrap()),
        )
        .clock(self.clock.clone())
        .outbox(ports.outbox)
        .jobs(ports.jobs)
        .notifications(Arc::new(MailFactory(self.mail.clone())))
        .build()
        .unwrap();
        module.start(ctx.clone()).await.unwrap();
        if workers {
            jobs.start(ctx.clone()).await.unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            while jobs.ready().await.is_err() {
                assert!(Instant::now() < deadline, "jobs never ready");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        let mut errors = ErrorRegistry::kernel();
        errors.register("identity", module.errors()).unwrap();
        let body_limits: Vec<(axum::http::Method, String, usize)> = module
            .body_limits()
            .into_iter()
            .map(|l| (l.method, format!("/v1/identity{}", l.path), l.limit))
            .collect();
        // A route of another block, to show the edge checks cover it too.
        let probe = axum::routing::any(|request: axum::extract::Request| async move {
            let subject = request
                .extensions()
                .get::<edge::Authenticated>()
                .and_then(|a| a.0.as_ref().map(|p| p.subject.as_str().to_owned()));
            axum::Json(json!({ "subject": subject }))
        });
        // A host route checking one permission through the RBAC
        // `Authorizer`, as AP-C2's enforcement and the admin routes do:
        // `?permission=..&scope=application|tenant|resource&tenant=org_..&kind=..&id=..`.
        let check = {
            let authorizer = authorizer.clone();
            axum::routing::get(move |request: axum::extract::Request| {
                let authorizer = authorizer.clone();
                async move { check_route(authorizer.as_ref(), request) }
            })
        };
        let router = edge::apply(
            Router::new()
                .nest("/v1/identity", module.routes(&ctx))
                .route("/v1/probe/check", check)
                .route("/v1/probe", probe),
            edge::EdgeSettings::new(&config.http, Arc::new(errors))
                .authenticator(Some(authenticator))
                .route_body_limits(body_limits),
        );
        Instance {
            module: Arc::from(module),
            router,
            handle,
            authorizer,
            jobs,
            db: self.db.clone(),
            workers,
            settling: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
    pub async fn scalar(&self, sql: &str) -> i64 {
        let client = self.db.acquire(soon()).await.unwrap();
        client
            .query_one(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{e}: {sql}"))
            .get(0)
    }
    pub async fn execute(&self, sql: &str) {
        let client = self.db.acquire(soon()).await.unwrap();
        client
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{e}: {sql}"));
    }
}

/// A response with its parsed JSON body.
#[derive(Debug)]
pub struct Answer {
    pub status: StatusCode,
    pub headers: axum::http::HeaderMap,
    pub body: Value,
}
impl Answer {
    pub fn code(&self) -> &str {
        self.body["error"]["code"].as_str().unwrap_or("")
    }
    pub fn binding_cookie(&self) -> Option<String> {
        self.headers
            .get_all(header::SET_COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .find(|v| v.starts_with(BINDING) || v.starts_with("merezha_verify="))
            .map(str::to_owned)
    }
    pub fn oauth_cookie(&self) -> Option<String> {
        self.headers
            .get_all(header::SET_COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .find(|v| v.starts_with(OAUTH) || v.starts_with("merezha_oauth="))
            .map(str::to_owned)
    }
    /// The `Location` of a redirect.
    pub fn location(&self) -> String {
        self.headers
            .get(header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned()
    }
    pub fn set_cookie(&self) -> Option<String> {
        self.headers
            .get_all(header::SET_COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .find(|v| v.starts_with(COOKIE) || v.starts_with("merezha_session="))
            .map(str::to_owned)
    }
}

pub async fn read(response: Response<Body>) -> Answer {
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or(Value::String(String::from_utf8_lossy(&bytes).into()))
    };
    Answer {
        status,
        headers,
        body,
    }
}

/// A browser: an address, a cookie jar for the session cookie and the
/// CSRF token of its session. Sends `Origin: APP` unless told otherwise.
#[derive(Clone, Debug)]
pub struct Browser {
    pub ip: IpAddr,
    pub cookie: Option<String>,
    /// The registration binding cookie (pre-hijacking rule).
    pub binding: Option<String>,
    /// The OAuth flow binding cookie.
    pub oauth: Option<String>,
    pub csrf: Option<String>,
    pub bearer: Option<String>,
    pub origin: Option<String>,
    pub user_agent: String,
    /// The `Merezha-Organization` header, when set.
    pub organization: Option<String>,
}
impl Browser {
    pub fn new(ip: &str) -> Self {
        Self {
            ip: ip.parse().unwrap(),
            cookie: None,
            binding: None,
            oauth: None,
            csrf: None,
            bearer: None,
            origin: Some(APP.to_owned()),
            user_agent: "TestBrowser/1.0".into(),
            organization: None,
        }
    }
    pub fn request(&self, method: &str, path: &str, body: Option<&Value>) -> Request<Body> {
        let mut builder = Request::builder()
            .method(method)
            .uri(if path.starts_with("/v1/") {
                path.to_owned()
            } else {
                format!("/v1/identity{path}")
            })
            .header(header::USER_AGENT, &self.user_agent);
        if let Some(origin) = &self.origin {
            builder = builder.header(header::ORIGIN, origin);
        }
        let mut cookies = Vec::new();
        if let Some(cookie) = &self.cookie {
            cookies.push(format!("{COOKIE}={cookie}"));
        }
        if let Some(binding) = &self.binding {
            cookies.push(format!("{BINDING}={binding}"));
        }
        if let Some(oauth) = &self.oauth {
            cookies.push(format!("{OAUTH}={oauth}"));
        }
        if !cookies.is_empty() {
            builder = builder.header(header::COOKIE, cookies.join("; "));
        }
        if let Some(csrf) = &self.csrf {
            builder = builder.header("x-csrf-token", csrf);
        }
        if let Some(bearer) = &self.bearer {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {bearer}"));
        }
        if let Some(org) = &self.organization {
            builder = builder.header("merezha-organization", org);
        }
        let body = match body {
            Some(value) => {
                builder = builder.header(header::CONTENT_TYPE, "application/json");
                Body::from(serde_json::to_vec(value).unwrap())
            }
            None => Body::empty(),
        };
        let mut request = builder.body(body).unwrap();
        request
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::new(self.ip, 40_000)));
        request
    }
    /// Send and keep what the answer sets: the cookie and the CSRF token.
    pub async fn send(
        &mut self,
        instance: &Instance,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> Answer {
        let response = instance
            .router
            .clone()
            .oneshot(self.request(method, path, body.as_ref()))
            .await
            .unwrap();
        let answer = read(response).await;
        instance.settle().await;
        if let Some(set) = answer.binding_cookie() {
            let value = set.split(';').next().unwrap().split_once('=').unwrap().1;
            self.binding = (!value.is_empty()).then(|| value.to_owned());
        }
        if let Some(set) = answer.oauth_cookie() {
            let value = set.split(';').next().unwrap().split_once('=').unwrap().1;
            self.oauth = (!value.is_empty()).then(|| value.to_owned());
        }
        if let Some(set) = answer.set_cookie() {
            let value = set.split(';').next().unwrap().split_once('=').unwrap().1;
            self.cookie = (!value.is_empty()).then(|| value.to_owned());
            if self.cookie.is_none() {
                self.csrf = None;
            }
        }
        if let Some(csrf) = answer.body["csrf_token"].as_str() {
            self.csrf = Some(csrf.to_owned());
        }
        if let Some(token) = answer.body["session"]["token"].as_str() {
            self.bearer = Some(token.to_owned());
        }
        answer
    }
    pub async fn post(&mut self, instance: &Instance, path: &str, body: Value) -> Answer {
        self.send(instance, "POST", path, Some(body)).await
    }
    pub async fn get(&mut self, instance: &Instance, path: &str) -> Answer {
        self.send(instance, "GET", path, None).await
    }
    pub async fn patch(&mut self, instance: &Instance, path: &str, body: Value) -> Answer {
        self.send(instance, "PATCH", path, Some(body)).await
    }
    pub async fn put(&mut self, instance: &Instance, path: &str, body: Value) -> Answer {
        self.send(instance, "PUT", path, Some(body)).await
    }
    pub async fn delete(&mut self, instance: &Instance, path: &str) -> Answer {
        self.send(instance, "DELETE", path, None).await
    }
    pub async fn delete_with(&mut self, instance: &Instance, path: &str, body: Value) -> Answer {
        self.send(instance, "DELETE", path, Some(body)).await
    }
    /// The probe check of `permission` at `scope` (`application`,
    /// `tenant:<org>`, `resource:<org or ->:<kind>:<id>`).
    pub async fn check(&mut self, instance: &Instance, permission: &str, scope: &str) -> Answer {
        let query = match scope.split(':').collect::<Vec<_>>().as_slice() {
            ["application"] => "scope=application".to_owned(),
            ["tenant", org] => format!("scope=tenant&tenant={org}"),
            ["resource", "-", kind, id] => format!("scope=resource&kind={kind}&id={id}"),
            ["resource", org, kind, id] => {
                format!("scope=resource&tenant={org}&kind={kind}&id={id}")
            }
            _ => panic!("scope {scope}"),
        };
        self.get(
            instance,
            &format!("/v1/probe/check?permission={permission}&{query}"),
        )
        .await
    }
    /// Whether the probe allows `permission` at `scope`.
    pub async fn allowed(&mut self, instance: &Instance, permission: &str, scope: &str) -> bool {
        let answer = self.check(instance, permission, scope).await;
        assert_eq!(answer.status, 200, "{answer:?}");
        answer.body["allowed"].as_bool().unwrap()
    }
}

/// Register `email` with `password`, verify through the mailed link and
/// return the signed-in browser.
pub async fn signed_up(world: &World, instance: &Instance, email: &str, password: &str) -> Browser {
    let mut browser = Browser::new("198.51.100.10");
    let answer = browser
        .post(
            instance,
            "/registrations",
            json!({"email": email, "password": password}),
        )
        .await;
    assert_eq!(answer.status, 202, "{answer:?}");
    let token = world.mail.token("identity.verify_email", email);
    let answer = browser
        .post(
            instance,
            "/email-verifications/confirm",
            json!({"token": token}),
        )
        .await;
    assert_eq!(answer.status, 201, "{answer:?}");
    browser
}

pub const PASSWORD: &str = "correct horse battery staple";

/// A browser with a bearer session for `email` (its account registered and
/// verified), so tests need no CSRF dance.
pub async fn user(world: &World, instance: &Instance, email: &str) -> Browser {
    let mut browser = Browser::new("198.51.100.20");
    let answer = browser
        .post(
            instance,
            "/registrations",
            json!({"email": email, "password": PASSWORD}),
        )
        .await;
    assert_eq!(answer.status, 202, "{answer:?}");
    let token = world.mail.token("identity.verify_email", email);
    let answer = browser
        .post(
            instance,
            "/email-verifications/confirm",
            json!({"token": token, "transport": "bearer"}),
        )
        .await;
    assert_eq!(answer.status, 201, "{answer:?}");
    browser.binding = None;
    browser
}
/// Sign `email` in again with a bearer session (a second device).
pub async fn sign_in(instance: &Instance, email: &str) -> Browser {
    let mut browser = Browser::new("198.51.100.21");
    let answer = browser
        .post(
            instance,
            "/sessions",
            json!({"email": email, "password": PASSWORD, "transport": "bearer"}),
        )
        .await;
    assert_eq!(answer.status, 201, "{answer:?}");
    browser
}
/// The id in `answer.body[pointer]`.
pub fn id_at(answer: &Answer, pointer: &str) -> String {
    answer
        .body
        .pointer(pointer)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("{pointer} in {answer:?}"))
        .to_owned()
}
/// Create organization `slug` as `owner`; its id.
pub async fn organization(instance: &Instance, owner: &mut Browser, slug: &str) -> String {
    let answer = owner
        .post(
            instance,
            "/organizations",
            json!({"name": slug, "slug": slug}),
        )
        .await;
    assert_eq!(answer.status, 201, "{answer:?}");
    id_at(&answer, "/organization/id")
}
/// The seeded role ids.
pub const OWNER_ROLE: &str = "rol_00000000000070008000000000000001";
pub const ADMIN_ROLE: &str = "rol_00000000000070008000000000000002";
pub const MEMBER_ROLE: &str = "rol_00000000000070008000000000000003";
pub const PLATFORM_ADMIN_ROLE: &str = "rol_00000000000070008000000000000004";

/// Invite `email` into `org` with `roles` as `inviter`, and accept as
/// `invitee` (signed in with that address); the membership id.
pub async fn join(
    world: &World,
    instance: &Instance,
    inviter: &mut Browser,
    org: &str,
    invitee: &mut Browser,
    email: &str,
    roles: &[&str],
) -> String {
    let answer = inviter
        .post(
            instance,
            &format!("/organizations/{org}/invitations"),
            json!({"email": email, "role_ids": roles}),
        )
        .await;
    assert_eq!(answer.status, 201, "{answer:?}");
    let token = world.mail.token("identity.invitation", email);
    let answer = invitee
        .post(instance, "/invitations/accept", json!({"token": token}))
        .await;
    assert_eq!(answer.status, 201, "{answer:?}");
    id_at(&answer, "/membership/id")
}
/// Grant `platform_admin` to `email` as the CLI does.
pub async fn platform_admin(instance: &Instance, email: &str) {
    let email = merezha_identity::normalize::email(email).unwrap();
    instance.handle.platform_admin(&email, true).await.unwrap();
}

impl World {
    /// Run `sql`; the error's SQLSTATE on failure.
    pub async fn try_execute(&self, sql: &str) -> Result<(), String> {
        let client = self.db.acquire(soon()).await.unwrap();
        client
            .batch_execute(sql)
            .await
            .map_err(|e| e.code().map(|c| c.code().to_owned()).unwrap_or_default())
    }
}
/// The raw UUID hex of a prefixed id.
pub fn hex(id: &str) -> String {
    id.split_once('_').unwrap().1.to_owned()
}
/// The membership id of `email` in `org`, listed by `viewer`.
pub async fn membership_of(
    instance: &Instance,
    viewer: &mut Browser,
    org: &str,
    email: &str,
) -> String {
    let answer = viewer
        .get(instance, &format!("/organizations/{org}/members?limit=100"))
        .await;
    assert_eq!(answer.status, 200, "{answer:?}");
    answer.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["user"]["email"] == email)
        .unwrap_or_else(|| panic!("{email} in {answer:?}"))["id"]
        .as_str()
        .unwrap()
        .to_owned()
}

impl World {
    /// Start the identity block with another `section` on this database
    /// and return its start result.
    pub async fn try_start(&self, section: &str) -> Result<(), merezha_domain::Failure> {
        let dir = tempfile::tempdir().unwrap();
        let input = project(dir.path(), section);
        let module = catalogued(IdentityModule::new(Some(HostFacts::loopback())));
        let (config, mut modules) = configure(&input, vec![Box::new(module)]).unwrap();
        let module = modules.remove(0);
        let jobs = PgJobs::with_mode(&config.jobs, RelayMode::Off);
        let ports = jobs.ports(&self.db);
        let ctx = AppContext::builder(
            "tasks",
            self.db.clone(),
            Arc::new(LoopbackAllowAll::new("127.0.0.1:1".parse().unwrap()).unwrap()),
        )
        .clock(self.clock.clone())
        .outbox(ports.outbox)
        .jobs(ports.jobs)
        .notifications(Arc::new(MailFactory(self.mail.clone())))
        .build()
        .unwrap();
        module.start(ctx).await
    }
}
