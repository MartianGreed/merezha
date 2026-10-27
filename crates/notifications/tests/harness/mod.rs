//! A started notifications block on a throwaway database: the module, the
//! jobs runtime, a test Directory and an authenticator mapping bearer
//! tokens to principals, behind the platform edge.
#![allow(dead_code)]
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use merezha_domain::{Failure, Identity, MessageKind, Metadata};
use merezha_jobs::{PgJobs, RelayMode};
use merezha_notifications::{MIGRATIONS, Notifications, faults::Faults, notifier::Core};
use merezha_platform::{
    AppContext, BoxFuture, ErrorRegistry, JobsRuntime, Module, ProjectConfig, edge,
    ports::{
        AccessDenied, Allowed, Authenticator, Authorizer, Contact, DenyReason, Directory,
        GrantSnapshot, Grants, Notifier, Page, PageRequest, Permission, PermissionSet, Principal,
        PrincipalKind, RequestHead, ResourceScope, RoleKey, Source, TenantId, UserId,
    },
};
use merezha_postgres::{Database, MigrationMode, ModuleMigrations, TestDatabase, run_migrations};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tower::ServiceExt;

/// base64 of 32 bytes.
pub const KEY: &str = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";
const BASE: &str = "[app]\nid = \"tasks\"\nname = \"Tasks\"\n\
    [database]\nurl = { env = \"APP_DATABASE_URL\" }\n\
    [runtime]\nmanifest = \"runtime.toml\"\n\
    [jobs]\npoll_interval_ms = 100\nreaper_interval_ms = 100\n\
    [jobs.relay]\ninterval_ms = 100\n";
/// The minimal section.
pub const SECTION: &str = "[modules.notifications]\nsecret_key = { env = \"NOTIFY_KEY\" }\n";

pub fn soon() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
pub fn id(text: &str) -> Identity {
    Identity::new(text).unwrap()
}

// ----- Directory double -----

/// A Directory over in-memory users, memberships and application roles.
#[derive(Default)]
pub struct People {
    users: Mutex<BTreeMap<String, Contact>>,
    /// tenant -> user -> roles
    tenants: Mutex<BTreeMap<String, BTreeMap<String, Vec<String>>>>,
    /// user -> application roles
    application: Mutex<BTreeMap<String, Vec<String>>>,
    pub calls: AtomicUsize,
    pub failing: AtomicBool,
}
impl People {
    pub fn user(&self, user: &str, email: &str, name: Option<&str>, locale: Option<&str>) {
        self.users.lock().unwrap().insert(
            user.to_owned(),
            Contact {
                user: id(user),
                email: email.to_owned(),
                display_name: name.map(str::to_owned),
                locale: locale.map(str::to_owned),
            },
        );
    }
    pub fn join(&self, tenant: &str, user: &str, roles: &[&str]) {
        self.tenants
            .lock()
            .unwrap()
            .entry(tenant.to_owned())
            .or_default()
            .insert(
                user.to_owned(),
                roles.iter().map(|r| (*r).to_owned()).collect(),
            );
    }
    pub fn grant(&self, user: &str, roles: &[&str]) {
        self.application.lock().unwrap().insert(
            user.to_owned(),
            roles.iter().map(|r| (*r).to_owned()).collect(),
        );
    }
    fn check(&self) -> Result<(), Failure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.failing.load(Ordering::SeqCst) {
            Err(Failure::transient("directory_unavailable"))
        } else {
            Ok(())
        }
    }
    fn page(
        &self,
        members: &BTreeMap<String, Vec<String>>,
        roles: Option<&[RoleKey]>,
        page: &PageRequest,
    ) -> Page<Contact> {
        let users = self.users.lock().unwrap();
        let wanted: Vec<Contact> = members
            .iter()
            .filter(|(user, held)| {
                roles.is_none_or(|roles| roles.iter().any(|r| held.iter().any(|h| h == r.as_str())))
                    && page
                        .after()
                        .is_none_or(|after| user.as_str() > after.as_str())
            })
            .filter_map(|(user, _)| users.get(user).cloned())
            .collect();
        let limit = usize::from(page.limit());
        let more = wanted.len() > limit;
        let items: Vec<Contact> = wanted.into_iter().take(limit).collect();
        let next = more.then(|| items.last().map(|c| c.user.clone())).flatten();
        Page { items, next }
    }
}
impl Directory for People {
    fn users<'a>(&'a self, ids: &'a [UserId]) -> BoxFuture<'a, Result<Vec<Contact>, Failure>> {
        let result = self.check().map(|()| {
            let users = self.users.lock().unwrap();
            ids.iter()
                .filter_map(|id| users.get(id.as_str()).cloned())
                .collect()
        });
        Box::pin(async move { result })
    }
    fn tenant_members<'a>(
        &'a self,
        tenant: &'a TenantId,
        roles: Option<&'a [RoleKey]>,
        page: PageRequest,
    ) -> BoxFuture<'a, Result<Page<Contact>, Failure>> {
        let result = self.check().map(|()| {
            let tenants = self.tenants.lock().unwrap();
            let members = tenants.get(tenant.as_str()).cloned().unwrap_or_default();
            self.page(&members, roles, &page)
        });
        Box::pin(async move { result })
    }
    fn application_members<'a>(
        &'a self,
        roles: Option<&'a [RoleKey]>,
        page: PageRequest,
    ) -> BoxFuture<'a, Result<Page<Contact>, Failure>> {
        let result = self.check().map(|()| {
            let members = self.application.lock().unwrap().clone();
            self.page(&members, roles, &page)
        });
        Box::pin(async move { result })
    }
    fn is_member<'a>(
        &'a self,
        user: &'a UserId,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<bool, Failure>> {
        let result = self.check().map(|()| {
            self.tenants
                .lock()
                .unwrap()
                .get(tenant.as_str())
                .is_some_and(|m| m.contains_key(user.as_str()))
        });
        Box::pin(async move { result })
    }
}

// ----- principals -----

/// RBAC as identity decides it: the principal's own snapshot.
pub struct Grantsheet;
impl merezha_application::Authorizer for Grantsheet {
    fn authorize(
        &self,
        _: MessageKind,
        _: &str,
        _: Option<&str>,
        _: &Value,
        _: &Metadata,
    ) -> Result<(), Failure> {
        Err(Failure::permanent("unauthorized"))
    }
}
impl Authorizer for Grantsheet {
    fn check(
        &self,
        principal: Option<&Principal>,
        permission: &Permission,
        scope: &ResourceScope,
    ) -> Result<Allowed, AccessDenied> {
        match principal {
            None => Err(AccessDenied::new(DenyReason::Unauthenticated)),
            Some(principal) => principal.check(permission, scope),
        }
    }
}

/// A user principal holding `permissions` at application scope.
pub fn user(subject: &str, permissions: &[&str], tenant: Option<&str>) -> Principal {
    principal(subject, PrincipalKind::User, permissions, tenant)
}
pub fn principal(
    subject: &str,
    kind: PrincipalKind,
    permissions: &[&str],
    tenant: Option<&str>,
) -> Principal {
    let set: PermissionSet = permissions.iter().map(|p| id(p)).collect();
    Principal {
        subject: id(subject),
        kind,
        user: (kind == PrincipalKind::User).then(|| id(subject)),
        service_account: None,
        tenant: tenant.map(id),
        grants: Arc::new(GrantSnapshot::empty().with_application(Grants::new(Vec::new(), set))),
        session: None,
    }
}

/// `Authorization: Bearer <token>` names a registered principal.
#[derive(Default)]
pub struct Tokens(Mutex<BTreeMap<String, Arc<Principal>>>);
impl Tokens {
    pub fn add(&self, token: &str, principal: Principal) {
        self.0
            .lock()
            .unwrap()
            .insert(token.to_owned(), Arc::new(principal));
    }
    /// Revoke `token`: it authenticates as `unauthenticated` from now on.
    pub fn revoke(&self, token: &str) {
        self.0.lock().unwrap().remove(token);
    }
}
impl Authenticator for Tokens {
    fn authenticate<'a>(
        &'a self,
        request: &'a RequestHead,
    ) -> BoxFuture<'a, Result<Option<Arc<Principal>>, Failure>> {
        let token = request
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(str::to_owned);
        let result = match token {
            None => Ok(None),
            Some(token) => self
                .0
                .lock()
                .unwrap()
                .get(&token)
                .cloned()
                .map(Some)
                .ok_or_else(|| Failure::permanent("unauthenticated")),
        };
        Box::pin(async move { result })
    }
}

// ----- world and instances -----

pub struct World {
    pub test: TestDatabase,
    pub db: Database,
}
impl World {
    pub async fn new() -> Option<Self> {
        Self::with_pool(16).await
    }
    pub async fn with_pool(pool: usize) -> Option<Self> {
        let test = TestDatabase::create()?;
        let mut config = test.config();
        config.pool_size = pool;
        config.acquire_timeout = Duration::from_secs(2);
        config.statement_timeout = Duration::from_secs(30);
        let db = Database::connect(&config).unwrap();
        run_migrations(
            &db,
            &[
                ModuleMigrations {
                    module: "notifications",
                    migrations: &MIGRATIONS,
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
        Some(Self { test, db })
    }
    /// A pool of `size` connections on the same database.
    pub fn pool(&self, size: usize) -> Database {
        let mut config = self.test.config();
        config.pool_size = size;
        config.acquire_timeout = Duration::from_secs(1);
        Database::connect(&config).unwrap()
    }
    pub async fn scalar(&self, sql: &str) -> i64 {
        let client = self.db.acquire(soon()).await.unwrap();
        client
            .query_one(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{e}: {sql}"))
            .get(0)
    }
    pub async fn text(&self, sql: &str) -> Vec<String> {
        let client = self.db.acquire(soon()).await.unwrap();
        client
            .query(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{e}: {sql}"))
            .iter()
            .map(|r| r.get::<_, Option<String>>(0).unwrap_or_default())
            .collect()
    }
    pub async fn execute(&self, sql: &str) {
        let client = self.db.acquire(soon()).await.unwrap();
        client
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{e}: {sql}"));
    }
}

/// How an instance starts.
pub struct Options {
    pub section: String,
    pub directory: Arc<dyn Directory>,
    pub workers: bool,
    pub heartbeat: bool,
    pub faults: Faults,
    /// The pool the block runs on; the world's by default.
    pub database: Option<Database>,
    /// `false`: the relay runs only on [`Instance::relay_now`].
    pub relay: bool,
    /// An email driver in place of the configured one.
    pub email_driver: Option<Arc<dyn merezha_notifications::drivers::EmailDriver>>,
    /// The host's meters; no-op by default.
    pub meters: Option<merezha_telemetry::Meters>,
    /// More environment variables for the configuration (secrets), such
    /// as a VAPID key.
    pub env: Vec<(String, String)>,
    /// The block's wall clock; the system's by default. The jobs runtime
    /// keeps PostgreSQL's.
    pub clock: Option<Arc<dyn merezha_platform::Clock>>,
    /// Files written beside `merezha.toml`, such as project templates.
    pub files: Vec<(String, String)>,
    /// The resolver of push hosts under the egress guard.
    pub push_lookup: Option<Arc<dyn merezha_notifications::egress::Lookup>>,
    /// Webhook busy hand-off delays (first, longest) in place of 1 s / 30 s.
    pub webhook_busy_delays: Option<(Duration, Duration)>,
}
impl Options {
    pub fn new(section: &str, directory: Arc<dyn Directory>) -> Self {
        Self {
            section: section.to_owned(),
            directory,
            workers: true,
            heartbeat: true,
            faults: Faults::default(),
            database: None,
            relay: true,
            email_driver: None,
            meters: None,
            env: Vec::new(),
            clock: None,
            files: Vec::new(),
            push_lookup: None,
            webhook_busy_delays: None,
        }
    }
}

pub struct Instance {
    pub router: Router,
    pub jobs: PgJobs,
    pub module: Arc<Notifications>,
    pub tokens: Arc<Tokens>,
    pub db: Database,
    _dir: tempfile::TempDir,
}
impl Instance {
    pub async fn start(world: &World, options: Options) -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("merezha.toml"),
            format!("{BASE}{}", options.section),
        )
        .unwrap();
        for (name, text) in &options.files {
            let path = dir.path().join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
        }
        let input = merezha_platform::ConfigInput {
            path: dir.path().join("merezha.toml"),
            env: [
                ("APP_DATABASE_URL".to_owned(), world.test.url().to_owned()),
                ("NOTIFY_KEY".to_owned(), KEY.to_owned()),
            ]
            .into_iter()
            .chain(options.env.iter().cloned())
            .collect(),
            overrides: Vec::new(),
        };
        let config = ProjectConfig::load(&input).unwrap();
        let mut module = Notifications::new(merezha_notifications::HostFacts::load(&input))
            .with_faults(options.faults.clone());
        if let Some(driver) = options.email_driver.clone() {
            module = module.with_email_driver(driver);
        }
        if let Some(lookup) = options.push_lookup.clone() {
            module = module.with_push_lookup(lookup);
        }
        if let Some((first, longest)) = options.webhook_busy_delays {
            module = module.with_webhook_busy_delays(first, longest);
        }
        module
            .configure(&config.modules["notifications"])
            .unwrap_or_else(|issues| panic!("{issues:?}"));
        let module = Arc::new(module);
        let db = options.database.clone().unwrap_or_else(|| world.db.clone());
        let mut jobs = PgJobs::with_mode(
            &config.jobs,
            if options.workers && options.relay {
                RelayMode::Listen
            } else {
                RelayMode::Off
            },
        );
        if !options.heartbeat {
            jobs = jobs.without_heartbeat();
        }
        let ports = jobs.ports(&db);
        let tokens = Arc::new(Tokens::default());
        let mut builder = AppContext::builder("tasks", db.clone(), Arc::new(Grantsheet));
        if let Some(meters) = options.meters.clone() {
            builder = builder.telemetry(meters);
        }
        if let Some(clock) = options.clock.clone() {
            builder = builder.clock(clock);
        }
        let ctx = builder
            .outbox(ports.outbox)
            .jobs(ports.jobs)
            .directory(options.directory.clone())
            .notifications(module.notifier().expect("configured"))
            .authenticator(tokens.clone())
            .build()
            .unwrap();
        module.start(ctx.clone()).await.unwrap();
        if options.workers {
            jobs.start(ctx.clone()).await.unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            while jobs.ready().await.is_err() {
                assert!(Instant::now() < deadline, "jobs never ready");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        let mut errors = ErrorRegistry::kernel();
        errors.register("notifications", module.errors()).unwrap();
        errors.register("jobs", jobs.errors()).unwrap();
        let router = edge::apply(
            Router::new().nest("/v1/notifications", module.routes(&ctx)),
            edge::EdgeSettings::new(&config.http, Arc::new(errors))
                .authenticator(Some(tokens.clone() as Arc<dyn Authenticator>))
                .credential_free_routes(
                    module
                        .credential_free_routes()
                        .into_iter()
                        .map(|r| (r.method, format!("/v1/notifications{}", r.path)))
                        .collect(),
                )
                .route_body_limits(
                    module
                        .body_limits()
                        .into_iter()
                        .map(|r| (r.method, format!("/v1/notifications{}", r.path), r.limit))
                        .collect(),
                ),
        );
        Self {
            router,
            jobs,
            module,
            tokens,
            db,
            _dir: dir,
        }
    }
    pub fn core(&self) -> Arc<Core> {
        self.module.core().expect("started")
    }
    /// The notifier bound to `source`.
    pub fn notifier(&self, source: &str) -> Arc<dyn Notifier> {
        use merezha_platform::ports::NotifierFactory;
        self.module.notifiers().bind(&Source::new(source).unwrap())
    }
    /// One HTTP call; the body parsed as JSON when it is JSON.
    pub async fn call(
        &self,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut request = Request::builder().method(method).uri(path);
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        if body.is_some() {
            request = request.header("content-type", "application/json");
        }
        let request = request
            .body(match body {
                Some(body) => Body::from(body.to_string()),
                None => Body::empty(),
            })
            .unwrap();
        self.raw(request).await
    }
    pub async fn raw(&self, request: Request<Body>) -> (StatusCode, Value) {
        let response = self.router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
        let body = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into()));
        (status, body)
    }
    /// Wait until every notification record was relayed and every
    /// expansion and delivery job finished.
    pub async fn settle(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let client = self.db.acquire(soon()).await.unwrap();
            let open: i64 = client
                .query_one(
                    "SELECT (SELECT count(*) FROM merezha_jobs.outbox \
                       WHERE source IN ('notifications', 'content') AND relayed_at IS NULL \
                       AND quarantined_at IS NULL AND (run_at IS NULL OR run_at <= now())) \
                     + (SELECT count(*) FROM merezha_jobs.jobs WHERE kind IN \
                       ('notify.expand', 'notify.deliver.email', 'notify.deliver.sms', \
                        'notify.deliver.push', 'notify.webhook_event', 'notify.webhook.deliver', \
                        'notify.module_event') \
                       AND state IN ('available', 'running') AND run_at <= now())",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            if open == 0 {
                return;
            }
            assert!(Instant::now() < deadline, "notifications never settled");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    /// Relay what is pending now (with `Options::relay = false`).
    pub async fn relay_now(&self) {
        self.jobs.relay_now().await.unwrap();
    }
    /// Wait until `sql` (one integer) answers `want`.
    pub async fn until(&self, sql: &str, want: i64) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let client = self.db.acquire(soon()).await.unwrap();
            let got: i64 = client.query_one(sql, &[]).await.unwrap().get(0);
            if got == want {
                return;
            }
            assert!(Instant::now() < deadline, "{sql} stayed {got}, not {want}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    pub async fn stop(&self) {
        self.jobs
            .shutdown(Instant::now() + Duration::from_secs(5))
            .await;
    }
}

/// The system clock moved forward by [`Shifted::advance`].
#[derive(Default)]
pub struct Shifted(std::sync::atomic::AtomicU64);
impl Shifted {
    pub fn advance(&self, by: Duration) {
        self.0.fetch_add(by.as_secs(), Ordering::SeqCst);
    }
}
impl merezha_platform::Clock for Shifted {
    fn now(&self) -> std::time::SystemTime {
        std::time::SystemTime::now() + Duration::from_secs(self.0.load(Ordering::SeqCst))
    }
    fn monotonic(&self) -> Instant {
        Instant::now()
    }
}
