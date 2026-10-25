//! Route permissions through the platform host (AP §C.4, slice C2): the
//! HTTP adapter's caller checks each route's `permission` or `access` at
//! its scope with the host policy before any guest call, composes tenant
//! streams from the principal, and `merezha run` refuses undeclared routes
//! with the identity block, unknown permissions and tenant queries on
//! unpartitioned projections at startup. In process with the platform's
//! grant evaluation, then through the binary with the identity block's
//! RBAC policy on real PostgreSQL.
mod support;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::Request,
};
use merezha_application::{Application, Guest, GuestSession, config::Config};
use merezha_domain::{Event, Failure, Identity, Operation, Reply};
use merezha_platform::{
    Principal, ResourceScope,
    edge::Authenticated,
    ports::{
        AccessDenied, Allowed, DenyReason, GrantSnapshot, Grants, PermissionSet, PrincipalKind,
    },
};
use merezha_postgres::TestDatabase;
use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use support::Host;
use tower::ServiceExt;

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}

/// Counts every guest call.
#[derive(Clone, Default)]
struct Counting(Arc<AtomicUsize>);
impl Guest for Counting {
    fn services(&self) -> Result<Vec<String>, Failure> {
        Ok(vec!["domain".into()])
    }
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
        Ok(Box::new(self.clone()))
    }
}
impl GuestSession for Counting {
    fn invoke(&self, _: &str, operation: Operation) -> Result<Reply, Failure> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(match operation {
            Operation::Validate { payload, .. } => Reply::Validated { payload },
            Operation::Decide { .. } => Reply::Decided {
                events: vec![Event {
                    tag: id("Done"),
                    schema_version: 1,
                    payload: json!({}),
                }],
            },
            Operation::Project { state, .. } => Reply::Projected { state },
            Operation::Query { state, .. } => Reply::Queried { value: state },
            Operation::Describe {} => unreachable!("a version 1 fake is never described"),
        })
    }
}

/// Decides from the principal's grant snapshot (`GrantSnapshot::evaluate`),
/// as the identity block's RBAC `Authorizer` does, without its audit
/// store. Its payload hook refuses everything, so a test passing proves the
/// platform host never relies on it.
struct GrantPolicy;
impl merezha_application::Authorizer for GrantPolicy {
    fn authorize(
        &self,
        _: merezha_domain::MessageKind,
        _: &str,
        _: Option<&str>,
        _: &Value,
        _: &merezha_domain::Metadata,
    ) -> Result<(), Failure> {
        Ok(())
    }
}
impl merezha_platform::ports::Authorizer for GrantPolicy {
    fn check(
        &self,
        principal: Option<&Principal>,
        permission: &Identity,
        scope: &ResourceScope,
    ) -> Result<Allowed, AccessDenied> {
        match principal {
            Some(principal) => principal.check(permission, scope),
            None => Err(AccessDenied::new(DenyReason::Unauthenticated)),
        }
    }
}

const ROUTES: &str = r#"version = 2
[[permissions]]
name = "tasks.create"
[[permissions]]
name = "tasks.read"
[[permissions]]
name = "tasks.rename"
[[commands]]
tag = "Open"
service = "domain"
aggregate = "Task"
access = "public"
[[commands]]
tag = "Create"
service = "domain"
aggregate = "Task"
permission = "tasks.create"
[[commands]]
tag = "TenantCreate"
service = "domain"
aggregate = "Task"
permission = "tasks.create"
scope = "tenant"
[[commands]]
tag = "Rename"
service = "domain"
aggregate = "Task"
permission = "tasks.rename"
scope = "tenant"
resource = "task"
[[queries]]
tag = "Everything"
service = "domain"
projection = "all"
permission = "tasks.read"
[[projections]]
name = "all"
service = "domain"
initial = 0
"#;

fn set(names: &[&str]) -> PermissionSet {
    names.iter().map(|n| id(n)).collect()
}
fn user(tenant: Option<&str>, grants: GrantSnapshot) -> Option<Arc<Principal>> {
    Some(Arc::new(Principal {
        subject: id("usr_ada"),
        kind: PrincipalKind::User,
        user: Some(id("usr_ada")),
        service_account: None,
        tenant: tenant.map(id),
        grants: Arc::new(grants),
        session: None,
    }))
}

fn router(guest: &Counting) -> Router {
    let app = Application::in_memory(
        Config::parse(ROUTES).unwrap(),
        Arc::new(guest.clone()),
        Arc::new(merezha_application::AllowAll),
    )
    .unwrap();
    merezha_cli::application_http::router(app, Some(Arc::new(GrantPolicy)))
}
async fn call(
    router: &Router,
    principal: Option<Arc<Principal>>,
    path: &str,
    body: Value,
) -> (u16, Value) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .extension(Authenticated(principal))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
fn command(tag: &str, stream: &str, metadata: Value) -> Value {
    json!({"tag": tag, "stream": stream, "payload": {}, "metadata": metadata})
}

/// AP-C2 tests 1, 2 and 7 through the HTTP adapter, every declared route
/// against every caller: `401 unauthenticated` and `403 unauthorized`,
/// `tenant_required` and `actor_mismatch` with zero guest calls; an
/// application-level grant without a tenant passes application routes; a
/// `platform_admin` passes every route; `resource = "task"` checks the
/// stream's resource grant.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_edge_caller_checks_every_route_before_any_guest_call() {
    let org = Some("org_1");
    let callers = [
        ("anonymous", None),
        ("member", user(org, GrantSnapshot::empty())),
        (
            "application grant, no tenant",
            user(
                None,
                GrantSnapshot::empty().with_application(Grants::new(
                    vec![id("support")],
                    set(&["tasks.create", "tasks.read", "tasks.rename"]),
                )),
            ),
        ),
        (
            "tenant owner",
            user(
                org,
                GrantSnapshot::empty().with_tenant(Grants::new(
                    vec![id("owner")],
                    set(&["tasks.create", "tasks.read", "tasks.rename"]),
                )),
            ),
        ),
        (
            "resource grant on t-1",
            user(
                org,
                GrantSnapshot::empty().with_resource(id("task"), id("t-1"), set(&["tasks.rename"])),
            ),
        ),
        (
            "platform_admin",
            user(org, GrantSnapshot::empty().with_break_glass()),
        ),
    ];
    // (route, stream, expected status per caller).
    let table: [(&str, &str, [u16; 6]); 5] = [
        ("Open", "t-1", [200, 200, 200, 200, 200, 200]),
        ("Create", "t-1", [401, 403, 200, 403, 403, 200]),
        ("TenantCreate", "t-1", [401, 403, 403, 200, 403, 200]),
        ("Rename", "t-1", [401, 403, 403, 200, 200, 200]),
        ("Rename", "t-2", [401, 403, 403, 200, 403, 200]),
    ];
    for (tag, stream, expected) in table {
        for ((name, principal), expected) in callers.iter().zip(expected) {
            let guest = Counting::default();
            let router = router(&guest);
            let (status, body) = call(
                &router,
                principal.clone(),
                "/v1/application/commands",
                command(tag, stream, json!({})),
            )
            .await;
            assert_eq!(status, expected, "{tag} {stream} as {name}: {body}");
            let calls = guest.0.load(Ordering::SeqCst);
            match status {
                200 => {
                    assert_eq!(calls, 2, "{tag} as {name}");
                    let composed = body["stream"].as_str().unwrap();
                    if tag == "Open" || tag == "Create" {
                        assert_eq!(composed, format!("Task/{stream}"));
                    } else {
                        assert_eq!(composed, format!("org_1:Task/{stream}"));
                    }
                }
                status => {
                    assert_eq!(calls, 0, "{tag} as {name}: no guest call");
                    let code = body["error"]["code"].as_str().unwrap();
                    let wanted = match (status, name) {
                        (401, _) => "unauthenticated",
                        (403, &"application grant, no tenant") if tag != "Create" => {
                            "tenant_required"
                        }
                        _ => "unauthorized",
                    };
                    assert_eq!(code, wanted, "{tag} as {name}");
                }
            }
        }
    }
    // The application-scope query, the same way.
    for ((name, principal), expected) in callers.iter().zip([401, 403, 200, 403, 403, 200]) {
        let guest = Counting::default();
        let router = router(&guest);
        let (status, body) = call(
            &router,
            principal.clone(),
            "/v1/application/queries",
            json!({"tag": "Everything", "payload": {}}),
        )
        .await;
        assert_eq!(status, expected, "Everything as {name}: {body}");
        assert_eq!(
            guest.0.load(Ordering::SeqCst),
            if status == 200 { 2 } else { 0 }
        );
    }
    // A principal acting for another actor, everywhere.
    let guest = Counting::default();
    let router = router(&guest);
    let (status, body) = call(
        &router,
        callers[5].1.clone(),
        "/v1/application/commands",
        command("Open", "t-1", json!({"actor": "usr_bob"})),
    )
    .await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (403, Some("actor_mismatch"))
    );
    assert_eq!(guest.0.load(Ordering::SeqCst), 0);
}

/// `serve` (no policy) keeps its loopback allow-all: permission routes
/// pass, and a tenant route without a principal is `403 tenant_required`.
#[tokio::test]
async fn serve_keeps_the_local_allow_all() {
    let guest = Counting::default();
    let app = Application::in_memory(
        Config::parse(ROUTES).unwrap(),
        Arc::new(guest.clone()),
        Arc::new(merezha_application::AllowAll),
    )
    .unwrap();
    let router = merezha_cli::application_http::router(app, None);
    let (status, _) = call(
        &router,
        None,
        "/v1/application/commands",
        command("Create", "t-1", json!({})),
    )
    .await;
    assert_eq!(status, 200);
    let (status, body) = call(
        &router,
        None,
        "/v1/application/commands",
        command("TenantCreate", "t-1", json!({})),
    )
    .await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (403, Some("tenant_required"))
    );
}

const SECRET: &str = "a-process-test-identity-secret-of-32-bytes";
const DATABASE_ENV: &str = "MEREZHA_ROUTES_TEST_DATABASE_URL";

fn runtime(dir: &Path) {
    fs::write(dir.join("protocol.wasm"), support::component()).unwrap();
    fs::write(
        dir.join("runtime.toml"),
        "version=1\n[limits]\nmemory_bytes=16777216\nfuel=1000000\nmax_input_bytes=65536\n\
         max_output_bytes=65536\ntimeout_ms=1000\nmax_concurrent_invocations=32\n\
         [[services]]\nid=\"protocol\"\npath=\"protocol.wasm\"\n",
    )
    .unwrap();
}
fn project(dir: &Path, application: &str, identity: bool) -> std::path::PathBuf {
    runtime(dir);
    fs::write(dir.join("application.toml"), application).unwrap();
    let config = dir.join("merezha.toml");
    let identity = if identity {
        "[modules.identity]\napp_origin = \"http://localhost:5173\"\n\
         secret_key = { env = \"IDENTITY_SECRET\" }\ndev_print_links = true\n\
         [modules.identity.password]\nargon2_memory_kib = 19456\n\
         [modules.identity.rbac.application_roles.support]\nname = \"Support\"\npermissions = [\"tasks.add\"]\n"
    } else {
        ""
    };
    fs::write(
        &config,
        format!(
            "[app]\nid = \"route-permissions\"\n[http]\nlisten = \"127.0.0.1:0\"\n\
             [database]\nurl = {{ env = \"{DATABASE_ENV}\" }}\nmigrate = true\npool_size = 24\n\
             [runtime]\nmanifest = \"runtime.toml\"\n[application]\npath = \"application.toml\"\n{identity}"
        ),
    )
    .unwrap();
    config
}

/// One HTTP/1.1 POST: status, raw headers and body.
fn http(
    address: std::net::SocketAddr,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, String, Value) {
    send_http(address, "POST", path, headers, body)
}
/// One HTTP/1.1 request: status, raw headers and body.
fn send_http(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, String, Value) {
    use std::io::{Read, Write};
    let mut stream =
        std::net::TcpStream::connect_timeout(&address, Duration::from_secs(2)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    stream
        .write_all(format!("{head}\r\n{body}").as_bytes())
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    (
        headers.split_whitespace().nth(1).unwrap().parse().unwrap(),
        headers.to_owned(),
        serde_json::from_str(body).unwrap_or(Value::Null),
    )
}

/// A verified account's session: the cookie and the CSRF token.
struct Session {
    cookie: String,
    csrf: String,
}
const ORIGIN: (&str, &str) = ("Origin", "http://localhost:5173");
fn register(host: &Host, address: std::net::SocketAddr, email: &str) -> Session {
    let links = |host: &Host| {
        host.events()
            .into_iter()
            .filter(|e| e["event"] == "identity.dev_link")
            .count()
    };
    let before = links(host);
    let (status, _, _) = http(
        address,
        "/v1/identity/registrations",
        &[ORIGIN],
        &json!({"email": email, "password": "correct horse battery staple"}).to_string(),
    );
    assert_eq!(status, 202);
    let deadline = Instant::now() + Duration::from_secs(10);
    let link = loop {
        let events: Vec<Value> = host
            .events()
            .into_iter()
            .filter(|e| e["event"] == "identity.dev_link")
            .collect();
        if events.len() > before {
            break events[before]["link"].as_str().unwrap().to_owned();
        }
        assert!(Instant::now() < deadline, "identity.dev_link on stderr");
        std::thread::sleep(Duration::from_millis(20));
    };
    let token = link.split_once("#token=").unwrap().1;
    let (status, headers, body) = http(
        address,
        "/v1/identity/email-verifications/confirm",
        &[ORIGIN],
        &json!({ "token": token }).to_string(),
    );
    assert_eq!(status, 201, "{body}");
    let cookie = headers
        .lines()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .starts_with("set-cookie: merezha_session=")
                .then(|| {
                    l.split_once(": ")
                        .unwrap()
                        .1
                        .split(';')
                        .next()
                        .unwrap()
                        .to_owned()
                })
        })
        .expect("the session cookie");
    Session {
        cookie,
        csrf: body["csrf_token"].as_str().unwrap().to_owned(),
    }
}

const APPLICATION: &str = r#"version = 2
[storage]
backend = "postgres"
application = "route-permissions"
[[permissions]]
name = "tasks.add"
[[permissions]]
name = "tasks.read"
[[commands]]
tag = "Add"
service = "protocol"
aggregate = "Counter"
permission = "tasks.add"
scope = "tenant"
[[commands]]
tag = "Rename"
service = "protocol"
aggregate = "Counter"
permission = "tasks.add"
scope = "tenant"
resource = "task"
[[commands]]
tag = "Global"
service = "protocol"
aggregate = "Counter"
permission = "tasks.add"
[[commands]]
tag = "Open"
service = "protocol"
aggregate = "Counter"
access = "public"
[[queries]]
tag = "Total"
service = "protocol"
projection = "total"
permission = "tasks.read"
scope = "tenant"
[[queries]]
tag = "Count"
service = "protocol"
projection = "count"
access = "authenticated"
cross_tenant = true
[[projections]]
name = "total"
service = "protocol"
initial = 0
partition = "tenant"
[[projections]]
name = "count"
service = "protocol"
initial = 0
"#;

/// The whole path with the identity block on real PostgreSQL: anonymous
/// callers get 401 (public routes answer), a member without the grant
/// 403, an organization owner writes and reads its own tenant only, a
/// tenant cannot address another tenant's stream, and a `platform_admin`
/// passes the application route it holds no grant for.
#[test]
fn identity_grants_decide_every_application_route() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path(), APPLICATION, true);
    let mut host = Host::spawn(
        &config,
        &[(DATABASE_ENV, test.url()), ("IDENTITY_SECRET", SECRET)],
        "routes",
    );
    let address = host.ready();
    let command = |tag: &str, stream: &str, metadata: Value| {
        json!({"tag": tag, "stream": stream, "payload": {"amount": 1}, "metadata": metadata})
            .to_string()
    };
    let code = |reply: &(u16, String, Value)| {
        (
            reply.0,
            reply.2["error"]["code"].as_str().map(str::to_owned),
        )
    };
    let commands = "/v1/application/commands";
    let queries = "/v1/application/queries";
    // Anonymous.
    for tag in ["Add", "Global"] {
        assert_eq!(
            code(&http(
                address,
                commands,
                &[ORIGIN],
                &command(tag, "t-1", json!({}))
            )),
            (401, Some("unauthenticated".into())),
            "{tag}"
        );
    }
    let open = http(
        address,
        commands,
        &[ORIGIN],
        &command("Open", "t-1", json!({})),
    );
    assert_eq!(open.0, 200, "{:?}", open.2);
    assert_eq!(
        code(&http(
            address,
            queries,
            &[ORIGIN],
            r#"{"tag":"Count","payload":{}}"#
        )),
        (401, Some("unauthenticated".into()))
    );
    // A member of nothing: authenticated routes answer, grants are missing.
    let ada = register(&host, address, "ada@example.com");
    let ada_headers = [
        ORIGIN,
        ("Cookie", ada.cookie.as_str()),
        ("X-CSRF-Token", ada.csrf.as_str()),
    ];
    assert_eq!(
        http(
            address,
            queries,
            &ada_headers,
            r#"{"tag":"Count","payload":{}}"#
        )
        .0,
        200
    );
    assert_eq!(
        code(&http(
            address,
            commands,
            &ada_headers,
            &command("Global", "t-1", json!({}))
        )),
        (403, Some("unauthorized".into()))
    );
    assert_eq!(
        code(&http(
            address,
            commands,
            &ada_headers,
            &command("Add", "t-1", json!({}))
        )),
        (403, Some("tenant_required".into()))
    );
    assert_eq!(
        code(&http(
            address,
            commands,
            &ada_headers,
            &command("Open", "t-1", json!({"actor": "usr_someone_else"}))
        )),
        (403, Some("actor_mismatch".into()))
    );
    // Owners of two organizations.
    let organization = |headers: &[(&str, &str)], slug: &str| {
        let (status, _, body) = http(
            address,
            "/v1/identity/organizations",
            headers,
            &json!({"name": slug, "slug": slug}).to_string(),
        );
        assert_eq!(status, 201, "{body}");
        body["organization"]["id"].as_str().unwrap().to_owned()
    };
    let acme = organization(&ada_headers, "acme");
    let bob = register(&host, address, "bob@example.com");
    let bob_plain = [
        ORIGIN,
        ("Cookie", bob.cookie.as_str()),
        ("X-CSRF-Token", bob.csrf.as_str()),
    ];
    let beta = organization(&bob_plain, "beta");
    let in_org = |session: &Session, org: &str| -> Vec<(String, String)> {
        vec![
            (ORIGIN.0.into(), ORIGIN.1.into()),
            ("Cookie".into(), session.cookie.clone()),
            ("X-CSRF-Token".into(), session.csrf.clone()),
            ("Merezha-Organization".into(), org.into()),
        ]
    };
    let send = |headers: &[(String, String)], path: &str, body: &str| {
        let headers: Vec<(&str, &str)> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        http(address, path, &headers, body)
    };
    let ada_acme = in_org(&ada, &acme);
    let bob_beta = in_org(&bob, &beta);
    let first = send(&ada_acme, commands, &command("Add", "t-1", json!({})));
    assert_eq!(first.0, 200, "{:?}", first.2);
    assert_eq!(first.2["stream"], format!("{acme}:Counter/t-1"));
    assert_eq!(first.2["version"], 1);
    let other = send(&bob_beta, commands, &command("Add", "t-1", json!({})));
    assert_eq!(other.0, 200, "{:?}", other.2);
    assert_eq!(other.2["stream"], format!("{beta}:Counter/t-1"));
    assert_eq!(other.2["version"], 1, "a distinct history");
    // Naming the other tenant's stream nests it under one's own.
    let named = send(
        &bob_beta,
        commands,
        &command("Add", &format!("{acme}:Counter/t-1"), json!({})),
    );
    assert_eq!(
        named.2["stream"],
        format!("{beta}:Counter/{acme}:Counter/t-1")
    );
    let second = send(&ada_acme, commands, &command("Add", "t-1", json!({})));
    assert_eq!(second.2["version"], 2, "Bob never wrote Ada's stream");
    // Bob cannot stand in Ada's organization.
    let foreign = send(
        &in_org(&bob, &acme),
        commands,
        &command("Add", "t-1", json!({})),
    );
    assert_ne!(foreign.0, 200, "{:?}", foreign.2);
    // A tenant query reads the caller's partition, read-your-write.
    let read = send(
        &ada_acme,
        queries,
        &json!({"tag": "Total", "payload": {}, "min_position": second.2["position"]}).to_string(),
    );
    assert_eq!(read.0, 200, "{:?}", read.2);
    assert!(read.2["checkpoint"].as_u64().unwrap() >= second.2["position"].as_u64().unwrap());
    // Ada holds no application grant; as platform_admin she passes.
    let admin = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args([
            "identity",
            "platform-admin",
            "grant",
            "ada@example.com",
            "--config",
        ])
        .arg(&config)
        .env(DATABASE_ENV, test.url())
        .env("IDENTITY_SECRET", SECRET)
        .output()
        .unwrap();
    assert_eq!(admin.status.code(), Some(0), "{admin:?}");
    let global = http(
        address,
        commands,
        &ada_headers,
        &command("Global", "t-1", json!({})),
    );
    assert_eq!(global.0, 200, "{:?}", global.2);
    assert_eq!(global.2["stream"], "Counter/t-1");
    // A partitioned projection's explicit catch-up has no single state.
    let caught = http(
        address,
        "/v1/application/projections/total/catch-up",
        &ada_headers,
        "",
    );
    assert_eq!(caught.0, 200, "{:?}", caught.2);
    assert!(caught.2.get("state").is_none(), "{:?}", caught.2);
    assert_eq!(caught.2["head"], global.2["position"]);
    assert_eq!(caught.2["checkpoint"], global.2["position"]);
    // Route `resource` kinds are tenant-only: routes check them inside the
    // caller's organization, so an application-level assignment on one
    // could never take effect and is refused. Block kinds still take one.
    let get = |path: &str| send_http(address, "GET", path, &ada_headers, "");
    let roles = get("/v1/identity/application/roles");
    assert_eq!(roles.0, 200, "{:?}", roles.2);
    let support = roles.2["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["key"] == "support")
        .expect("the declared role")["id"]
        .clone();
    let users = get("/v1/identity/application/users?email=ada%40example.com");
    let ada_id = users.2["items"][0]["id"].clone();
    let assign = |kind: &str| {
        http(
            address,
            "/v1/identity/application/role-assignments",
            &ada_headers,
            &json!({"user_id": ada_id, "role_id": support, "resource": {"kind": kind, "id": "t-1"}})
                .to_string(),
        )
    };
    assert_eq!(
        code(&assign("task")),
        (422, Some("resource_kind_tenant_only".into()))
    );
    let content = assign("content_type");
    assert_eq!(content.0, 201, "{:?}", content.2);
    host.stop();
}

/// AP-C2 test 6: `merezha run` lists every access problem of the
/// application at startup and stops with the first, before the database:
/// a route without `permission` or `access` with the identity block, a
/// permission outside the catalog, and a tenant query on an unpartitioned
/// projection.
#[test]
fn startup_refuses_undeclared_unknown_and_unpartitioned_routes() {
    let base = |extra: &str| {
        format!(
            "version = 2\n[storage]\nbackend = \"postgres\"\napplication = \"refused\"\n\
             [[permissions]]\nname = \"tasks.add\"\n{extra}\
             [[projections]]\nname = \"total\"\nservice = \"protocol\"\ninitial = 0\n"
        )
    };
    let undeclared =
        "[[commands]]\ntag = \"Bare\"\nservice = \"protocol\"\naggregate = \"Counter\"\n";
    let unknown = "[[commands]]\ntag = \"Unknown\"\nservice = \"protocol\"\naggregate = \"Counter\"\n\
                   permission = \"tasks.delete\"\n";
    let unpartitioned = "[[queries]]\ntag = \"Total\"\nservice = \"protocol\"\nprojection = \"total\"\n\
                         access = \"authenticated\"\nscope = \"tenant\"\n";
    let unreachable = [
        (DATABASE_ENV, "postgres://nobody:pw@127.0.0.1:1/none"),
        ("IDENTITY_SECRET", SECRET),
    ];
    for (name, extra, identity, code) in [
        ("undeclared", undeclared, true, "route_access_undeclared"),
        ("unknown", unknown, false, "unknown_permission"),
        (
            "unpartitioned",
            unpartitioned,
            false,
            "tenant_query_unpartitioned",
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let config = project(dir.path(), &base(extra), identity);
        let failed = Host::spawn(&config, &unreachable, name).failed();
        assert_eq!(failed["code"], code, "{name}: {failed}");
    }
    // All at once: every issue is listed, the first is the code.
    let dir = tempfile::tempdir().unwrap();
    let config = project(
        dir.path(),
        &base(&format!("{undeclared}{unknown}{unpartitioned}")),
        true,
    );
    let failed = Host::spawn(&config, &unreachable, "all").failed();
    assert_eq!(failed["code"], "tenant_query_unpartitioned", "{failed}");
    assert_eq!(
        failed["issues"],
        json!([
            {"key": "queries.Total", "code": "tenant_query_unpartitioned"},
            {"key": "commands.Bare", "code": "route_access_undeclared"},
            {"key": "commands.Unknown", "code": "unknown_permission"},
        ]),
        "{failed}"
    );
    // Without identity an undeclared route is allowed: it stops on the
    // database instead.
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path(), &base(undeclared), false);
    let failed = Host::spawn(&config, &unreachable, "allowed").failed();
    assert_ne!(failed["code"], "route_access_undeclared");
    // The PR #58 review's PoC: beside tenant routes, an anonymous `List`
    // on an unpartitioned projection would answer every tenant's data
    // (org B's `ssn`); so would one checking a permission every user
    // holds. Both stop startup unless the query opts in.
    let leak = |list: &str, user: Option<&str>, name: &str| {
        let dir = tempfile::tempdir().unwrap();
        let routes = format!(
            "[[permissions]]\nname = \"people.read\"\n\
             [[commands]]\ntag = \"AddPerson\"\nservice = \"protocol\"\naggregate = \"Person\"\n\
             access = \"authenticated\"\nscope = \"tenant\"\n\
             [[queries]]\ntag = \"List\"\nservice = \"protocol\"\nprojection = \"total\"\n{list}\n"
        );
        let config = project(dir.path(), &base(&routes), true);
        if let Some(user) = user {
            let mut text = fs::read_to_string(&config).unwrap();
            text.push_str(&format!(
                "[modules.identity.rbac.builtin_grants]\nuser = [\"{user}\"]\n"
            ));
            fs::write(&config, text).unwrap();
        }
        Host::spawn(&config, &unreachable, name).failed()
    };
    for (list, user) in [
        ("access = \"public\"", None),
        ("permission = \"people.read\"", Some("people.*")),
    ] {
        let failed = leak(list, user, "leak");
        assert_eq!(failed["code"], "cross_tenant_query", "{list}: {failed}");
        let opted = leak(&format!("{list}\ncross_tenant = true"), user, "opted");
        assert_ne!(opted["code"], "cross_tenant_query", "{list}: {opted}");
    }
    // Identity §5: a `builtin_grants.user` grant a tenant route checks is
    // warned about once per route (it holds in every organization).
    let dir = tempfile::tempdir().unwrap();
    let tenant_route = "[[commands]]\ntag = \"Add\"\nservice = \"protocol\"\naggregate = \"Counter\"\n\
                        permission = \"tasks.add\"\nscope = \"tenant\"\n\
                        [[commands]]\ntag = \"Global\"\nservice = \"protocol\"\naggregate = \"Counter\"\n\
                        permission = \"tasks.add\"\n";
    let config = project(dir.path(), &base(tenant_route), true);
    let mut text = fs::read_to_string(&config).unwrap();
    text.push_str("[modules.identity.rbac.builtin_grants]\nuser = [\"tasks.*\"]\n");
    fs::write(&config, text).unwrap();
    let mut host = Host::spawn(&config, &unreachable, "warned");
    host.failed();
    let warned: Vec<Value> = host
        .events()
        .into_iter()
        .filter(|e| e["event"] == "identity.user_grant_tenant_route")
        .collect();
    assert_eq!(warned.len(), 1, "{warned:?}");
    assert_eq!(
        (&warned[0]["permission"], &warned[0]["route"]),
        (&json!("tasks.add"), &json!("commands.Add"))
    );
}
