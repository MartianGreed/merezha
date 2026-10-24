//! The host lifecycle against real PostgreSQL: migrations at startup,
//! readiness, the edge and graceful drain. Each test creates its own database
//! through `MEREZHA_TEST_DATABASE_URL` and skips with a message when unset.
use axum::{
    Extension, Router,
    body::{Body, Bytes},
    http::{Request, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use merezha_domain::Failure;
use merezha_platform::{
    AppContext, BoxFuture, Host, HostParts, Migration, Module, ProjectConfig, Timeouts,
    edge::{ClientInfo, RequestId, TraceContext},
    ports::LoopbackAllowAll,
};
use merezha_postgres::{MigrationMode, ModuleMigrations, TestDatabase, run_migrations};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tower::ServiceExt;

const NOTES: [Migration; 2] = [
    Migration {
        version: 1,
        name: "create_schema",
        sql: "CREATE SCHEMA notes; CREATE TABLE notes.items (id bigint PRIMARY KEY);",
    },
    Migration {
        version: 2,
        name: "seed",
        sql: "INSERT INTO notes.items VALUES (1), (2);",
    },
];

/// A minimal block: one schema, a few routes and observable lifecycle hooks.
#[derive(Clone, Default)]
struct Notes {
    started: Arc<AtomicUsize>,
    stopped: Arc<AtomicUsize>,
    unready: Arc<AtomicBool>,
    fail_start: bool,
    migrations: Option<&'static [Migration]>,
}
impl Module for Notes {
    fn name(&self) -> &'static str {
        "notes"
    }
    fn migrations(&self) -> &[Migration] {
        self.migrations.unwrap_or(&NOTES)
    }
    fn start(&self, _ctx: AppContext) -> BoxFuture<'_, Result<(), Failure>> {
        Box::pin(async move {
            if self.fail_start {
                return Err(Failure::permanent("refused"));
            }
            self.started.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
    fn routes(&self, ctx: &AppContext) -> Router {
        let db = ctx.database().clone();
        Router::new()
            .route(
                "/count",
                get(move || async move {
                    let deadline = Instant::now() + Duration::from_secs(2);
                    let client = match db.acquire(deadline).await {
                        Ok(client) => client,
                        Err(failure) => return merezha_platform::error_response(&failure),
                    };
                    match client
                        .query_one("SELECT count(*) FROM notes.items", &[])
                        .await
                    {
                        Ok(row) => {
                            axum::Json(json!({"count": row.get::<_, i64>(0)})).into_response()
                        }
                        Err(e) => merezha_platform::error_response(&merezha_postgres::classify(&e)),
                    }
                }),
            )
            .route(
                "/whoami",
                get(
                    |Extension(id): Extension<RequestId>,
                     Extension(client): Extension<ClientInfo>,
                     Extension(trace): Extension<TraceContext>| async move {
                        axum::Json(json!({
                            "request_id": id.0,
                            "address": client.address.map(|a| a.to_string()),
                            "proto": client.forwarded_proto,
                            "trace_id": trace.trace_id_hex(),
                        }))
                    },
                ),
            )
            .route("/echo", post(|body: Bytes| async move { body }))
            .route(
                "/slow",
                get(|| async {
                    tokio::time::sleep(Duration::from_millis(400)).await;
                    "done"
                }),
            )
            .route(
                "/refuse",
                get(|| async { merezha_platform::error_response(&Failure::conflict("taken")) }),
            )
    }
    fn ready(&self) -> BoxFuture<'_, Result<(), Failure>> {
        let unready = self.unready.load(Ordering::SeqCst);
        Box::pin(async move {
            if unready {
                Err(Failure::transient("warming_up"))
            } else {
                Ok(())
            }
        })
    }
    fn shutdown(&self, _deadline: Instant) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.stopped.fetch_add(1, Ordering::SeqCst);
        })
    }
}

fn config(test: &TestDatabase, migrate: bool, extra: &str) -> ProjectConfig {
    config_with(test, migrate, extra, &[])
}
fn config_with(
    test: &TestDatabase,
    migrate: bool,
    extra: &str,
    overrides: &[(String, String)],
) -> ProjectConfig {
    let text = format!(
        "[app]\nid = \"notes-app\"\n[database]\nurl = {{ env = \"NOTES_DATABASE_URL\" }}\nmigrate = {migrate}\npool_size = 4\nconnect_timeout_ms = 2000\nacquire_timeout_ms = 1000\n[runtime]\nmanifest = \"runtime.toml\"\n[http]\nbody_limit = 1024\ntrusted_proxies = [\"10.0.0.0/8\"]\n{extra}"
    );
    ProjectConfig::parse(
        &text,
        std::env::temp_dir(),
        &[("NOTES_DATABASE_URL".into(), test.url().into())],
        overrides,
    )
    .unwrap()
}
fn parts() -> HostParts {
    HostParts {
        routes: Router::new().route("/extra", get(|| async { "extra" })),
        authorizer: Arc::new(LoopbackAllowAll::new("127.0.0.1:0".parse().unwrap()).unwrap()),
        timeouts: Timeouts {
            drain: Duration::from_secs(5),
            ..Timeouts::default()
        },
    }
}
async fn call(
    router: &Router,
    request: Request<Body>,
) -> (StatusCode, Value, axum::http::HeaderMap) {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into()));
    (status, body, headers)
}
fn get_request(path: &str) -> Request<Body> {
    Request::get(path).body(Body::empty()).unwrap()
}
async fn ready(router: &Router) -> StatusCode {
    // Readiness shares an evaluation for 250 ms; wait it out.
    tokio::time::sleep(Duration::from_millis(300)).await;
    call(router, get_request("/health/ready")).await.0
}

#[tokio::test]
async fn startup_applies_migrations_and_mounts_modules_behind_the_edge() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let notes = Notes::default();
    let host = Host::start(
        config(&test, true, ""),
        vec![Box::new(notes.clone())],
        parts(),
    )
    .await
    .unwrap();
    assert_eq!(host.migrations().applied.len(), 2);
    assert_eq!(notes.started.load(Ordering::SeqCst), 1);
    let router = host.router();
    assert_eq!(
        call(&router, get_request("/health/live")).await.1,
        json!({"status":"live"})
    );
    let (status, body, _) = call(&router, get_request("/health/ready")).await;
    assert_eq!((status, body), (StatusCode::OK, json!({"status":"ready"})));
    assert_eq!(
        call(&router, get_request("/v1/notes/count")).await.1,
        json!({"count":2})
    );
    assert_eq!(call(&router, get_request("/extra")).await.1, json!("extra"));
    // One error shape for unknown routes, wrong methods, handler failures
    // and extractor rejections, with stable codes.
    for (request, status, code, classification) in [
        (get_request("/nope"), 404, "not_found", "permanent"),
        (
            Request::delete("/v1/notes/count")
                .body(Body::empty())
                .unwrap(),
            405,
            "method_not_allowed",
            "permanent",
        ),
        (get_request("/v1/notes/refuse"), 409, "taken", "conflict"),
        (
            Request::post("/v1/notes/echo")
                .header("content-length", "2048")
                .body(Body::from(vec![b'x'; 2048]))
                .unwrap(),
            413,
            "body_limit",
            "permanent",
        ),
        (
            // No declared length: the limit applies while reading.
            Request::post("/v1/notes/echo")
                .body(Body::from(vec![b'x'; 2048]))
                .unwrap(),
            413,
            "body_limit",
            "permanent",
        ),
    ] {
        let (got, body, headers) = call(&router, request).await;
        assert_eq!(got.as_u16(), status, "{body}");
        assert_eq!(
            body,
            json!({"error":{"code":code,"classification":classification}})
        );
        assert!(headers.contains_key("x-request-id"));
    }
    let (status, body, _) = call(
        &router,
        Request::post("/v1/notes/echo")
            .body(Body::from("small"))
            .unwrap(),
    )
    .await;
    assert_eq!((status, body), (StatusCode::OK, json!("small")));
    // Applying again at the next start is a no-op.
    let again = Host::start(
        config(&test, true, ""),
        vec![Box::new(Notes::default())],
        parts(),
    )
    .await
    .unwrap();
    assert!(again.migrations().applied.is_empty() && again.migrations().is_current());
}

#[tokio::test]
async fn body_limits_above_the_extractor_default_reach_handlers() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    // 4 MiB is above the 2 MiB default of axum's body extractors.
    let config = config_with(
        &test,
        true,
        "",
        &[("http.body_limit".into(), (4 << 20).to_string())],
    );
    let host = Host::start(config, vec![Box::new(Notes::default())], parts())
        .await
        .unwrap();
    let router = host.router();
    let echo = |size: usize| {
        Request::post("/v1/notes/echo")
            .body(Body::from(vec![b'x'; size]))
            .unwrap()
    };
    let response = router.clone().oneshot(echo(3 << 20)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 8 << 20)
        .await
        .unwrap();
    assert_eq!(bytes.len(), 3 << 20);
    let (status, body, _) = call(&router, echo((4 << 20) + 1)).await;
    assert_eq!(
        (status, body),
        (
            StatusCode::PAYLOAD_TOO_LARGE,
            json!({"error":{"code":"body_limit","classification":"permanent"}})
        )
    );
}

#[tokio::test]
async fn request_ids_traces_and_forwarded_headers_follow_the_trusted_proxy_list() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let host = Host::start(
        config(&test, true, ""),
        vec![Box::new(Notes::default())],
        parts(),
    )
    .await
    .unwrap();
    let router = host.router();
    let whoami = |peer: &str| {
        let mut request = Request::get("/v1/notes/whoami")
            .header("x-request-id", "proxy-id-1")
            .header("x-forwarded-for", "203.0.113.7, 10.1.1.1")
            .header("x-forwarded-proto", "https")
            .header(
                "traceparent",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            )
            .body(Body::empty())
            .unwrap();
        request
            .extensions_mut()
            .insert(axum::extract::ConnectInfo::<std::net::SocketAddr>(
                peer.parse().unwrap(),
            ));
        request
    };
    let (_, trusted, headers) = call(&router, whoami("10.0.0.2:5000")).await;
    assert_eq!(trusted["request_id"], "proxy-id-1");
    assert_eq!(trusted["address"], "203.0.113.7");
    assert_eq!(trusted["proto"], "https");
    assert_eq!(trusted["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736");
    assert_eq!(headers["x-request-id"], "proxy-id-1");
    let (_, direct, headers) = call(&router, whoami("198.51.100.4:5000")).await;
    let generated = direct["request_id"].as_str().unwrap();
    assert_ne!(
        generated, "proxy-id-1",
        "an untrusted peer cannot set the id"
    );
    assert_eq!(generated.len(), 32);
    assert_eq!(headers["x-request-id"], generated);
    assert_eq!(direct["address"], "198.51.100.4");
    assert_eq!(direct["proto"], Value::Null);
    // The caller's trace continues whoever sends it.
    assert_eq!(direct["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736");
}

#[tokio::test]
async fn cors_answers_preflight_for_allowed_origins_only() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let config = config(
        &test,
        true,
        "[http.cors]\nallowed_origins = [\"https://app.example\"]\n",
    );
    let host = Host::start(config, vec![Box::new(Notes::default())], parts())
        .await
        .unwrap();
    let router = host.router();
    let preflight = |origin: &str| {
        Request::options("/v1/notes/count")
            .header("origin", origin)
            .header("access-control-request-method", "GET")
            .body(Body::empty())
            .unwrap()
    };
    let (_, _, headers) = call(&router, preflight("https://app.example")).await;
    assert_eq!(
        headers["access-control-allow-origin"],
        "https://app.example"
    );
    let (_, _, headers) = call(&router, preflight("https://evil.example")).await;
    assert!(!headers.contains_key("access-control-allow-origin"));
}

#[tokio::test]
async fn verify_mode_refuses_readiness_until_migrations_are_current() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let host = Host::start(
        config(&test, false, ""),
        vec![Box::new(Notes::default())],
        parts(),
    )
    .await
    .unwrap();
    assert_eq!(host.migrations().pending.len(), 2);
    let router = host.router();
    let (status, body, _) = call(&router, get_request("/health/ready")).await;
    assert_eq!(
        (status, body),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"status":"not_ready"})
        )
    );
    // Another host applies them; readiness re-verifies and recovers.
    let db = merezha_postgres::Database::connect(&test.config()).unwrap();
    run_migrations(
        &db,
        &[ModuleMigrations {
            module: "notes",
            migrations: &NOTES,
        }],
        MigrationMode::Apply,
        Instant::now() + Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(ready(&router).await, StatusCode::OK);
    // A changed migration stops the next startup in either mode.
    const EDITED: [Migration; 1] = [Migration {
        version: 1,
        name: "create_schema",
        sql: "CREATE SCHEMA notes;",
    }];
    for migrate in [false, true] {
        let edited = Notes {
            migrations: Some(&EDITED),
            ..Notes::default()
        };
        let error = Host::start(config(&test, migrate, ""), vec![Box::new(edited)], parts())
            .await
            .unwrap_err();
        assert_eq!(error.failure.code, "migration_checksum_mismatch");
        let migration = error.migration.unwrap();
        assert_eq!(
            (migration.module.as_deref(), migration.version),
            (Some("notes"), Some(1))
        );
    }
}

#[tokio::test]
async fn readiness_follows_the_pool_and_modules() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let notes = Notes::default();
    let host = Host::start(
        config(&test, true, ""),
        vec![Box::new(notes.clone())],
        parts(),
    )
    .await
    .unwrap();
    let router = host.router();
    assert_eq!(ready(&router).await, StatusCode::OK);
    test.admin_execute(&format!(
        "ALTER DATABASE \"{0}\" ALLOW_CONNECTIONS false; SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{0}'",
        test.name()
    ));
    assert_eq!(ready(&router).await, StatusCode::SERVICE_UNAVAILABLE);
    // Liveness does not depend on the database.
    assert_eq!(
        call(&router, get_request("/health/live")).await.0,
        StatusCode::OK
    );
    test.admin_execute(&format!(
        "ALTER DATABASE \"{}\" ALLOW_CONNECTIONS true",
        test.name()
    ));
    let mut recovered = false;
    for _ in 0..10 {
        if ready(&router).await == StatusCode::OK {
            recovered = true;
            break;
        }
    }
    assert!(recovered, "readiness did not recover with the database");
    notes.unready.store(true, Ordering::SeqCst);
    assert_eq!(ready(&router).await, StatusCode::SERVICE_UNAVAILABLE);
    notes.unready.store(false, Ordering::SeqCst);
    assert_eq!(ready(&router).await, StatusCode::OK);
}

#[tokio::test]
async fn startup_fails_cleanly_for_an_unreachable_database_or_a_failing_module() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let failing = Notes {
        fail_start: true,
        ..Notes::default()
    };
    let error = Host::start(config(&test, true, ""), vec![Box::new(failing)], parts())
        .await
        .unwrap_err();
    assert_eq!(
        (error.failure.code.as_str(), error.module),
        ("module_start_failed", Some("notes"))
    );
    test.admin_execute(&format!(
        "ALTER DATABASE \"{}\" ALLOW_CONNECTIONS false",
        test.name()
    ));
    let error = Host::start(
        config(&test, true, ""),
        vec![Box::new(Notes::default())],
        parts(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.failure.code, "database_unavailable");
    test.admin_execute(&format!(
        "ALTER DATABASE \"{}\" ALLOW_CONNECTIONS true",
        test.name()
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_drains_in_flight_requests_then_stops_modules() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let notes = Notes::default();
    let host = Host::start(
        config(&test, true, ""),
        vec![Box::new(notes.clone())],
        parts(),
    )
    .await
    .unwrap();
    let listener = host
        .bind(Some("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(host.serve(listener, async move {
        let _ = stopped.await;
    }));
    let slow = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /v1/notes/slow HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    stop.send(()).unwrap();
    let response = slow.await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with("done"));
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(notes.stopped.load(Ordering::SeqCst), 1);
    assert!(
        tokio::net::TcpStream::connect(address).await.is_err(),
        "the listener closed"
    );
}
