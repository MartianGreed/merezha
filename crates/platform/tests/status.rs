//! `GET /v1/admin/status` (TE §5, §8 "Health") against real PostgreSQL:
//! gated by `system.status.read` at application scope with identity, by a
//! loopback peer on a private listener without it; one request at a time;
//! sections embedded as their owners return them; and no planted canary in
//! the body.
use axum::{
    Router,
    body::Body,
    extract::ConnectInfo,
    http::{Request, StatusCode},
};
use merezha_application::Authorizer as ApplicationAuthorizer;
use merezha_domain::{Failure, Identity, MessageKind, Metadata};
use merezha_platform::{
    BoxFuture, Host, HostOptions, HostParts, JobsPorts, JobsRuntime, Migration, Module,
    ProjectConfig, StatusSection, Timeouts,
    ports::{
        AccessDenied, Allowed, Authenticator, Authorizer, Enqueued, GrantSnapshot, Grants,
        JobRegistration, Jobs, LoopbackAllowAll, NewJob, OutboxSummary, Permission, Principal,
        PrincipalKind, QueueStatus, QueueSummary, RequestHead, ResourceScope, Source,
        UnavailableOutbox,
    },
    status::STATUS_PERMISSION,
};
use merezha_postgres::{Database, TestDatabase};
use serde_json::{Value, json};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tower::ServiceExt;

const CANARY: &str = "sTaTuS-cAnArY-77";
const MIGRATIONS: [Migration; 2] = [
    Migration::new(1, "create", "CREATE SCHEMA ledger;"),
    Migration::new(2, "table", "CREATE TABLE ledger.items (id bigint);"),
];

struct Ledger;
impl Module for Ledger {
    fn name(&self) -> &'static str {
        "ledger"
    }
    fn migrations(&self) -> &[Migration] {
        &MIGRATIONS
    }
}

/// A jobs runtime whose `queue_status` returns a fixed document and counts
/// its calls: the endpoint embeds it as returned and queries nothing
/// itself.
#[derive(Default)]
struct FakeJobs {
    reads: AtomicUsize,
}
fn queue_status() -> QueueStatus {
    QueueStatus {
        queues: vec![QueueSummary {
            name: "notify.email".into(),
            ready: 0,
            delayed: 2,
            leased: 1,
            dead: 0,
            oldest_ready_age_s: 0.0,
            concurrency: 8,
            global_concurrency: 10,
        }],
        outbox: OutboxSummary {
            pending: 3,
            oldest_age_s: 1.5,
            unrouted: 0,
            quarantined: Vec::new(),
        },
    }
}
impl Jobs for FakeJobs {
    fn enqueue<'a>(&'a self, _: &'a Source, _: NewJob) -> BoxFuture<'a, Result<Enqueued, Failure>> {
        Box::pin(async { Err(Failure::permanent("jobs_unavailable")) })
    }
    fn cancel<'a>(&'a self, _: &'a Source, _: i64) -> BoxFuture<'a, Result<bool, Failure>> {
        Box::pin(async { Err(Failure::permanent("jobs_unavailable")) })
    }
    fn register(&self, _: &Source, _: JobRegistration) -> Result<(), Failure> {
        Err(Failure::permanent("jobs_unavailable"))
    }
    fn subscribe(&self, _: &Source, _: &Identity, _: &Identity) -> Result<(), Failure> {
        Err(Failure::permanent("jobs_unavailable"))
    }
    fn schedule(
        &self,
        _: &Source,
        _: merezha_platform::ports::ScheduleDeclaration,
    ) -> Result<(), Failure> {
        Err(Failure::permanent("jobs_unavailable"))
    }
    fn queue_status(&self) -> BoxFuture<'_, Result<QueueStatus, Failure>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(queue_status()) })
    }
}
struct FakeRuntime(Arc<FakeJobs>);
impl Module for FakeRuntime {
    fn name(&self) -> &'static str {
        "jobs"
    }
}
impl JobsRuntime for FakeRuntime {
    fn ports(&self, _: &Database) -> JobsPorts {
        JobsPorts {
            outbox: Arc::new(UnavailableOutbox),
            jobs: self.0.clone(),
        }
    }
}

/// The `application` section as AP's `Application::status()` would return
/// it; it can be held open to observe the one-at-a-time rule.
struct ApplicationSection {
    hold: tokio::sync::Semaphore,
    held: AtomicUsize,
}
impl Default for ApplicationSection {
    fn default() -> Self {
        Self {
            hold: tokio::sync::Semaphore::new(0),
            held: AtomicUsize::new(0),
        }
    }
}
impl StatusSection for ApplicationSection {
    fn name(&self) -> &'static str {
        "application"
    }
    fn read(&self) -> BoxFuture<'_, Result<Value, Failure>> {
        Box::pin(async move {
            if self.held.load(Ordering::SeqCst) > 0 {
                let _ = self.hold.acquire().await;
            }
            Ok(json!({"application": "ledger", "head": 7, "projections": []}))
        })
    }
}
/// A section that fails, shown by its code only.
struct Broken;
impl StatusSection for Broken {
    fn name(&self) -> &'static str {
        "catalog"
    }
    fn read(&self) -> BoxFuture<'_, Result<Value, Failure>> {
        Box::pin(async { Err(Failure::transient("internal")) })
    }
}

/// Grants from a `x-test-principal` header: `none`, `tenant` (the
/// permission at tenant scope only), `app` (at application scope) or
/// `admin` (break-glass).
struct HeaderAuthenticator;
impl Authenticator for HeaderAuthenticator {
    fn authenticate<'a>(
        &'a self,
        request: &'a RequestHead,
    ) -> BoxFuture<'a, Result<Option<Arc<Principal>>, Failure>> {
        let which = request
            .headers
            .get("x-test-principal")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        Box::pin(async move {
            let permission: Permission = Identity::new(STATUS_PERMISSION)?;
            let granted = || Grants::new(Vec::new(), [permission.clone()].into_iter().collect());
            let grants = match which.as_deref() {
                None => return Ok(None),
                Some("tenant") => GrantSnapshot::empty().with_tenant(granted()),
                Some("app") => GrantSnapshot::empty().with_application(granted()),
                Some("admin") => GrantSnapshot::empty().with_break_glass(),
                Some(_) => GrantSnapshot::empty(),
            };
            Ok(Some(Arc::new(Principal {
                subject: Identity::new("usr_status")?,
                kind: PrincipalKind::User,
                user: None,
                service_account: None,
                tenant: Some(Identity::new("org_1")?),
                grants: Arc::new(grants),
                session: None,
            })))
        })
    }
}
/// Decides from the principal's snapshot, as identity's authorizer does.
struct GrantPolicy;
impl ApplicationAuthorizer for GrantPolicy {
    fn authorize(
        &self,
        _: MessageKind,
        _: &str,
        _: Option<&str>,
        _: &Value,
        _: &Metadata,
    ) -> Result<(), Failure> {
        Ok(())
    }
}
impl Authorizer for GrantPolicy {
    fn check(
        &self,
        principal: Option<&Principal>,
        permission: &Permission,
        scope: &ResourceScope,
    ) -> Result<Allowed, AccessDenied> {
        match principal {
            Some(principal) => principal.check(permission, scope),
            None => Err(AccessDenied::new(
                merezha_platform::ports::DenyReason::Unauthenticated,
            )),
        }
    }
}

fn config(test: &TestDatabase, extra: &str) -> ProjectConfig {
    let separator = if test.url().contains('?') { '&' } else { '?' };
    // The canary rides in the database URL and an OTLP header secret.
    let url = format!("{}{separator}application_name={CANARY}", test.url());
    ProjectConfig::parse(
        &format!(
            "[app]\nid = \"ledger-app\"\nversion = \"1.4.0\"\ninstance_id = \"ledger-1\"\n[database]\nurl = {{ env = \"LEDGER_DATABASE_URL\" }}\nmigrate = true\npool_size = 2\n[runtime]\nmanifest = \"runtime.toml\"\n[telemetry]\nheaders = {{ authorization = {{ env = \"LEDGER_OTLP\" }} }}\n{extra}"
        ),
        std::env::temp_dir(),
        &[
            ("LEDGER_DATABASE_URL".into(), url),
            ("LEDGER_OTLP".into(), format!("Bearer {CANARY}")),
        ],
        &[],
    )
    .unwrap()
}
fn parts(jobs: &Arc<FakeJobs>, authorizer: Arc<dyn Authorizer>) -> HostParts {
    HostParts {
        routes: Router::new(),
        authorizer,
        timeouts: Timeouts {
            readiness_interval: Duration::from_millis(100),
            ..Timeouts::default()
        },
        jobs: Some(Arc::new(FakeRuntime(jobs.clone()))),
    }
}
fn loopback() -> Arc<dyn Authorizer> {
    Arc::new(LoopbackAllowAll::new("127.0.0.1:0".parse().unwrap()).unwrap())
}
fn status_request(peer: &str, principal: Option<&str>) -> Request<Body> {
    let mut builder = Request::get(format!("/v1/admin/status?token={CANARY}"))
        .header("authorization", format!("Bearer {CANARY}"))
        .header("cookie", format!("session={CANARY}"))
        .header("x-api-key", CANARY);
    if let Some(principal) = principal {
        builder = builder.header("x-test-principal", principal);
    }
    let mut request = builder.body(Body::empty()).unwrap();
    request
        .extensions_mut()
        .insert(ConnectInfo::<SocketAddr>(peer.parse().unwrap()));
    request
}
async fn get(router: &Router, request: Request<Body>) -> (StatusCode, Value, String) {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let cache = response
        .headers()
        .get("cache-control")
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or_default();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(!text.contains(CANARY), "canary in the status body: {text}");
    (status, serde_json::from_str(&text).unwrap(), cache)
}

#[tokio::test]
async fn without_identity_status_is_served_to_loopback_on_a_private_listener_only() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let jobs = Arc::new(FakeJobs::default());
    let application = Arc::new(ApplicationSection::default());
    let options = HostOptions {
        status: vec![application.clone(), Arc::new(Broken)],
        ..HostOptions::default()
    };
    let host = Host::start_with(
        config(&test, ""),
        vec![Box::new(Ledger)],
        parts(&jobs, loopback()),
        options,
    )
    .await
    .unwrap();
    let router = host.router();
    let (status, body, cache) = get(&router, status_request("127.0.0.1:5000", None)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(cache, "no-store");
    assert_eq!(body["service"]["name"], "ledger-app");
    assert_eq!(body["service"]["version"], "1.4.0");
    assert_eq!(body["service"]["instance_id"], "ledger-1");
    assert_eq!(
        body["service"]["host_version"],
        merezha_telemetry::Service::HOST_VERSION
    );
    assert!(
        body["service"]["started_at"]
            .as_str()
            .unwrap()
            .ends_with('Z')
    );
    assert_eq!(body["startup"]["state"], "started");
    assert!(body["startup"]["duration_ms"].is_u64());
    assert_eq!(body["readiness"]["ready"], true);
    assert_eq!(body["readiness"]["draining"], false);
    assert_eq!(body["readiness"]["checks"][0]["name"], "database");
    assert_eq!(body["readiness"]["checks"][0]["code"], "ok");
    assert_eq!(body["database"]["pool"]["max"], 2);
    assert_eq!(
        body["database"]["migrations"],
        json!([
            {"module": "ledger", "applied": 2, "pending": 0},
            {"module": "jobs", "applied": 0, "pending": 0},
        ])
    );
    // AP's documents as returned (R10), read once per request.
    assert_eq!(body["jobs"], serde_json::to_value(queue_status()).unwrap());
    assert_eq!(jobs.reads.load(Ordering::SeqCst), 1);
    assert_eq!(body["application"]["head"], 7);
    assert_eq!(body["catalog"], json!({"code": "internal"}));
    assert_eq!(
        body["modules"],
        json!([
            {"name": "ledger", "ready": true, "code": "ok"},
            {"name": "jobs", "ready": true, "code": "ok"},
        ])
    );

    // A remote peer, even on a private listener, finds nothing.
    let (status, body, _) = get(&router, status_request("192.0.2.9:5000", None)).await;
    assert_eq!(
        (status, body),
        (
            StatusCode::NOT_FOUND,
            json!({"error":{"code":"not_found","classification":"permanent"}})
        )
    );

    // One request at a time: a concurrent one is refused with Retry-After.
    application.held.store(1, Ordering::SeqCst);
    let held = tokio::spawn({
        let router = router.clone();
        async move { get(&router, status_request("127.0.0.1:5000", None)).await.0 }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let response = router
        .clone()
        .oneshot(status_request("127.0.0.1:5000", None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["retry-after"], "1");
    application.hold.add_permits(1);
    assert_eq!(held.await.unwrap(), StatusCode::OK);
    application.held.store(0, Ordering::SeqCst);
    drop(host);

    // A public listener without identity does not serve it at all.
    let host = Host::start(
        config(&test, "[http]\nlisten = \"0.0.0.0:0\"\npublic = true\n"),
        vec![Box::new(Ledger)],
        parts(&jobs, Arc::new(merezha_platform::ports::DenyAll)),
    )
    .await
    .unwrap();
    let (status, _, _) = get(&host.router(), status_request("127.0.0.1:5000", None)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn with_identity_status_needs_the_permission_at_application_scope() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let jobs = Arc::new(FakeJobs::default());
    let options = HostOptions {
        authenticator: Some(Arc::new(HeaderAuthenticator)),
        ..HostOptions::default()
    };
    // Public: identity, not the peer, decides.
    let host = Host::start_with(
        config(&test, "[http]\nlisten = \"0.0.0.0:0\"\npublic = true\n"),
        vec![Box::new(Ledger)],
        parts(&jobs, Arc::new(GrantPolicy)),
        options,
    )
    .await
    .unwrap();
    let router = host.router();
    for (principal, status, code) in [
        (None, StatusCode::UNAUTHORIZED, Some("unauthenticated")),
        (Some("nobody"), StatusCode::FORBIDDEN, Some("unauthorized")),
        // A tenant-scoped grant is not enough.
        (Some("tenant"), StatusCode::FORBIDDEN, Some("unauthorized")),
        (Some("app"), StatusCode::OK, None),
        (Some("admin"), StatusCode::OK, None),
    ] {
        let (got, body, _) = get(&router, status_request("192.0.2.9:5000", principal)).await;
        assert_eq!(got, status, "{principal:?}: {body}");
        match code {
            Some(code) => assert_eq!(
                body,
                json!({"error":{"code":code,"classification":"permanent"}})
            ),
            None => assert_eq!(body["startup"]["state"], "started"),
        }
    }
}

/// Review finding 3: identity's authenticator beside the allow-all policy
/// would switch every permission check off; startup refuses the pairing.
#[tokio::test]
async fn an_authenticator_with_the_allow_all_policy_is_refused() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let jobs = Arc::new(FakeJobs::default());
    let options = HostOptions {
        authenticator: Some(Arc::new(HeaderAuthenticator)),
        ..HostOptions::default()
    };
    let error = Host::start_with(
        config(&test, ""),
        vec![Box::new(Ledger)],
        parts(&jobs, loopback()),
        options,
    )
    .await
    .unwrap_err();
    assert_eq!(error.failure.code, "allow_all_with_authenticator");
    assert!(loopback().allows_all());
    assert!(!GrantPolicy.allows_all());
}
