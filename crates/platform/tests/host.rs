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
    Migration::new(
        1,
        "create_schema",
        "CREATE SCHEMA notes; CREATE TABLE notes.items (id bigint PRIMARY KEY);",
    ),
    Migration::new(2, "seed", "INSERT INTO notes.items VALUES (1), (2);"),
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
                    SLOW_ENTERED.fetch_add(1, Ordering::SeqCst);
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
        "[app]\nid = \"notes-app\"\n[database]\nurl = {{ env = \"NOTES_DATABASE_URL\" }}\nmigrate = {migrate}\npool_size = 4\nconnect_timeout_ms = 2000\nacquire_timeout_ms = 1000\n[runtime]\nmanifest = \"runtime.toml\"\n[http]\nbody_limit = 1024\ndrain_delay_ms = 0\ntrusted_proxies = [\"10.0.0.0/8\"]\n{extra}"
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
            // Probes read a cached evaluation; refresh it often in tests.
            readiness_interval: Duration::from_millis(100),
            ..Timeouts::default()
        },
        jobs: None,
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
/// Wait until `/health/ready` answers `status`. Probes read the
/// background evaluation (every 100 ms here) and each check takes up to
/// 1 s against an unreachable database: 20 s is generous on a loaded
/// machine.
async fn ready(router: &Router, status: StatusCode) {
    ready_within(router, status, Duration::from_secs(20)).await;
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
    // Only a trusted caller's trace continues (TE §2.1): an untrusted one
    // gets a new root and no traceparent back.
    assert_ne!(direct["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736");
    assert_eq!(direct["trace_id"].as_str().map(str::len), Some(32));
    assert!(!headers.contains_key("traceparent"));
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
    ready(&router, StatusCode::OK).await;
    // A changed migration stops the next startup in either mode.
    const EDITED: [Migration; 1] = [Migration::new(1, "create_schema", "CREATE SCHEMA notes;")];
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
    ready(&router, StatusCode::OK).await;
    test.admin_execute(&format!(
        "ALTER DATABASE \"{0}\" ALLOW_CONNECTIONS false; SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{0}'",
        test.name()
    ));
    ready(&router, StatusCode::SERVICE_UNAVAILABLE).await;
    // Liveness does not depend on the database.
    assert_eq!(
        call(&router, get_request("/health/live")).await.0,
        StatusCode::OK
    );
    test.admin_execute(&format!(
        "ALTER DATABASE \"{}\" ALLOW_CONNECTIONS true",
        test.name()
    ));
    // Readiness recovers with the database.
    ready(&router, StatusCode::OK).await;
    notes.unready.store(true, Ordering::SeqCst);
    ready(&router, StatusCode::SERVICE_UNAVAILABLE).await;
    notes.unready.store(false, Ordering::SeqCst);
    ready(&router, StatusCode::OK).await;
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
    let entered = SLOW_ENTERED.load(Ordering::SeqCst);
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
    // The request is in flight, inside its handler, when the stop comes.
    let sent = Instant::now();
    while SLOW_ENTERED.load(Ordering::SeqCst) == entered {
        assert!(sent.elapsed() < Duration::from_secs(20), "never in flight");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
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

/// A block that registers its own error codes.
struct Ledger {
    errors: &'static [merezha_platform::ErrorCode],
}
impl Module for Ledger {
    fn name(&self) -> &'static str {
        "ledger"
    }
    fn errors(&self) -> &[merezha_platform::ErrorCode] {
        self.errors
    }
    fn routes(&self, _ctx: &AppContext) -> Router {
        use merezha_platform::{ErrorReply, FieldError};
        Router::new()
            .route(
                "/locked",
                get(|| async {
                    merezha_platform::error_response(&Failure::permanent("entry_locked"))
                }),
            )
            .route(
                "/unregistered",
                get(|| async {
                    merezha_platform::error_response(&Failure::permanent("ledger_odd"))
                }),
            )
            .route(
                "/invalid",
                get(|| async {
                    ErrorReply::new(Failure::permanent("validation_failed"))
                        .details(vec![FieldError::new("/amount", "min").unwrap()])
                }),
            )
            .route(
                "/limited",
                get(|| async {
                    ErrorReply::new(Failure::transient("rate_limited"))
                        .retry_after(Duration::from_secs(7))
                }),
            )
            .route(
                "/busy",
                get(|| async {
                    merezha_platform::error_response(&Failure::transient("overloaded"))
                }),
            )
    }
}
const LEDGER_CODES: &[merezha_platform::ErrorCode] = &[
    merezha_platform::ErrorCode::new("entry_locked", 423),
    merezha_platform::ErrorCode::new("validation_failed", 422),
];

#[tokio::test]
async fn module_error_codes_get_their_registered_status_at_the_edge() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let host = Host::start(
        config(&test, true, ""),
        vec![Box::new(Ledger {
            errors: LEDGER_CODES,
        })],
        parts(),
    )
    .await
    .unwrap();
    assert_eq!(
        host.errors().status_of("entry_locked"),
        Some(StatusCode::LOCKED)
    );
    let router = host.router();
    let (status, body, _) = call(&router, get_request("/v1/ledger/locked")).await;
    assert_eq!(status, 423, "outside the edge it would be 422");
    assert_eq!(
        body,
        json!({"error": {"code": "entry_locked", "classification": "permanent"}})
    );
    let (status, _, _) = call(&router, get_request("/v1/ledger/unregistered")).await;
    assert_eq!(status, 422);
    let (status, body, _) = call(&router, get_request("/v1/ledger/invalid")).await;
    assert_eq!(status, 422);
    assert_eq!(
        body["error"]["details"],
        json!([{"path": "/amount", "code": "min"}])
    );
    let (status, body, headers) = call(&router, get_request("/v1/ledger/limited")).await;
    assert_eq!(
        (status.as_u16(), &headers["retry-after"]),
        (429, &axum::http::HeaderValue::from_static("7"))
    );
    assert_eq!(body["error"]["classification"], "transient");
    let (status, _, headers) = call(&router, get_request("/v1/ledger/busy")).await;
    assert_eq!(
        (status.as_u16(), &headers["retry-after"]),
        (503, &axum::http::HeaderValue::from_static("1"))
    );
    // Edge failures use the same registry.
    let (status, body, _) = call(
        &router,
        Request::post("/v1/ledger/locked")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        (status.as_u16(), body["error"]["code"].as_str()),
        (405, Some("method_not_allowed"))
    );
}

#[tokio::test]
async fn a_code_registered_twice_with_different_statuses_stops_startup() {
    // Registration runs before the pool opens, so no server is needed.
    let config = ProjectConfig::parse(
        "[app]\nid = \"notes-app\"\n[database]\nurl = { env = \"NOTES_DATABASE_URL\" }\n[runtime]\nmanifest = \"runtime.toml\"\n",
        std::env::temp_dir(),
        &[("NOTES_DATABASE_URL".into(), "postgres://127.0.0.1:1/none".into())],
        &[],
    )
    .unwrap();
    const KERNEL_CLASH: &[merezha_platform::ErrorCode] =
        &[merezha_platform::ErrorCode::new("validation_failed", 400)];
    let error = Host::start(
        config.clone(),
        vec![Box::new(Ledger {
            errors: KERNEL_CLASH,
        })],
        parts(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        (error.failure.code.as_str(), error.module),
        ("error_code_conflict", Some("ledger"))
    );
    struct Other;
    impl Module for Other {
        fn name(&self) -> &'static str {
            "other"
        }
        fn errors(&self) -> &[merezha_platform::ErrorCode] {
            const CODES: &[merezha_platform::ErrorCode] =
                &[merezha_platform::ErrorCode::new("entry_locked", 409)];
            CODES
        }
    }
    let error = Host::start(
        config,
        vec![
            Box::new(Ledger {
                errors: LEDGER_CODES,
            }),
            Box::new(Other),
        ],
        parts(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        (error.failure.code.as_str(), error.module),
        ("error_code_conflict", Some("other"))
    );
}

/// Tries to write as other sources from the context the host hands it.
struct Spoofer {
    refused: Arc<std::sync::Mutex<Vec<String>>>,
}
impl Module for Spoofer {
    fn name(&self) -> &'static str {
        "spoofer"
    }
    fn start(&self, ctx: AppContext) -> BoxFuture<'_, Result<(), Failure>> {
        Box::pin(async move {
            assert_eq!(ctx.module(), Some("spoofer"));
            ctx.outbox("spoofer")?;
            ctx.notifier("spoofer")?;
            let mut refused = self.refused.lock().unwrap();
            for source in ["identity", "api", "application"] {
                if let Err(failure) = ctx.outbox(source) {
                    refused.push(format!("outbox:{source}:{}", failure.code));
                }
                if let Err(failure) = ctx.notifier(source) {
                    refused.push(format!("notifier:{source}:{}", failure.code));
                }
            }
            Ok(())
        })
    }
}

#[tokio::test]
async fn modules_cannot_bind_handles_for_another_source() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let refused = Arc::new(std::sync::Mutex::new(Vec::new()));
    Host::start(
        config(&test, true, ""),
        vec![Box::new(Spoofer {
            refused: refused.clone(),
        })],
        parts(),
    )
    .await
    .unwrap();
    let refused = refused.lock().unwrap().clone();
    assert_eq!(refused.len(), 6, "{refused:?}");
    assert!(refused.iter().all(|r| r.ends_with(":source_not_allowed")));
}

/// Claims the `api` source for an HTTP send route, as notifications does.
struct Sender {
    name: &'static str,
    bound: Arc<std::sync::Mutex<Vec<String>>>,
}
impl Module for Sender {
    fn name(&self) -> &'static str {
        self.name
    }
    fn extra_sources(&self) -> &[&'static str] {
        &["api"]
    }
    fn start(&self, ctx: AppContext) -> BoxFuture<'_, Result<(), Failure>> {
        Box::pin(async move {
            let mut bound = self.bound.lock().unwrap();
            for source in ["api", "application", "notifications"] {
                let outcome = match ctx.notifier(source) {
                    Ok(_) => "ok".to_owned(),
                    Err(failure) => failure.code,
                };
                bound.push(format!("{source}:{outcome}"));
            }
            Ok(())
        })
    }
}

#[tokio::test]
async fn only_the_notifications_module_is_granted_the_api_source() {
    let bound = Arc::new(std::sync::Mutex::new(Vec::new()));
    // A claim by any other module stops startup before the pool opens.
    let config = ProjectConfig::parse(
        "[app]\nid = \"notes-app\"\n[database]\nurl = { env = \"NOTES_DATABASE_URL\" }\n[runtime]\nmanifest = \"runtime.toml\"\n",
        std::env::temp_dir(),
        &[("NOTES_DATABASE_URL".into(), "postgres://127.0.0.1:1/none".into())],
        &[],
    )
    .unwrap();
    let error = Host::start(
        config,
        vec![Box::new(Sender {
            name: "content",
            bound: bound.clone(),
        })],
        parts(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        (error.failure.code.as_str(), error.module),
        ("source_not_granted", Some("content"))
    );
    let Some(test) = TestDatabase::create() else {
        return;
    };
    Host::start(
        config_with(&test, true, "", &[]),
        vec![Box::new(Sender {
            name: "notifications",
            bound: bound.clone(),
        })],
        parts(),
    )
    .await
    .unwrap();
    assert_eq!(
        *bound.lock().unwrap(),
        vec![
            "api:ok".to_owned(),
            "application:source_not_allowed".to_owned(),
            "notifications:ok".to_owned()
        ]
    );
}

/// A module whose start takes `delay` and declares `own` as its bound, and
/// records whether its context says the host migrates.
struct Slow {
    delay: Duration,
    own: Option<Duration>,
    migrate: Arc<AtomicBool>,
}
impl Module for Slow {
    fn name(&self) -> &'static str {
        "slow"
    }
    fn start_timeout(&self) -> Option<Duration> {
        self.own
    }
    fn start(&self, ctx: AppContext) -> BoxFuture<'_, Result<(), Failure>> {
        self.migrate.store(ctx.migrate(), Ordering::SeqCst);
        let delay = self.delay;
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            Ok(())
        })
    }
}

#[tokio::test]
async fn a_module_may_declare_its_own_start_bound_and_sees_the_migrate_setting() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let short = || HostParts {
        timeouts: Timeouts {
            module_start: Duration::from_millis(100),
            ..parts().timeouts
        },
        ..parts()
    };
    let slow = |own| Slow {
        delay: Duration::from_millis(400),
        own,
        migrate: Arc::new(AtomicBool::new(false)),
    };
    let error = Host::start(config(&test, true, ""), vec![Box::new(slow(None))], short())
        .await
        .unwrap_err();
    assert_eq!(
        (error.failure.code.as_str(), error.module),
        ("module_start_failed", Some("slow"))
    );
    let module = slow(Some(Duration::from_secs(5)));
    let migrate = module.migrate.clone();
    Host::start(config(&test, true, ""), vec![Box::new(module)], short())
        .await
        .unwrap();
    assert!(migrate.load(Ordering::SeqCst));
    let module = slow(Some(Duration::from_secs(5)));
    let migrate = module.migrate.clone();
    Host::start(config(&test, false, ""), vec![Box::new(module)], short())
        .await
        .unwrap();
    assert!(!migrate.load(Ordering::SeqCst));
}

/// One `Connection: close` GET: the status, the lowercased head and the
/// body.
async fn http_get(address: std::net::SocketAddr, path: &str) -> (u16, String, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, head.to_ascii_lowercase(), body.to_owned())
}

/// `/slow` requests that reached their handler, across the tests.
static SLOW_ENTERED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// A migration slow enough to observe the early listener while it runs.
const SLOW_NOTES: [Migration; 1] = [Migration::new(
    1,
    "slow_schema",
    "SELECT pg_sleep(4); CREATE SCHEMA notes; CREATE TABLE notes.items (id bigint PRIMARY KEY); INSERT INTO notes.items VALUES (1);",
)];

/// TE §8 "Health": while a slow migration runs, `/health/startup` and
/// `/health/ready` answer 503 and application routes `503 starting`;
/// liveness answers throughout; the switch is `startup.completed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_early_listener_answers_health_and_refuses_routes_during_a_slow_migration() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let config = config(&test, true, "");
    let options = merezha_platform::HostOptions::default();
    let early = merezha_platform::EarlyListener::bind(
        &config,
        Some("127.0.0.1:0".parse().unwrap()),
        &options,
    )
    .await
    .unwrap();
    let address = early.address();
    let options = merezha_platform::HostOptions {
        lifecycle: Some(early.lifecycle().clone()),
        ..options
    };
    let notes = Notes {
        migrations: Some(&SLOW_NOTES),
        ..Notes::default()
    };
    let starting = tokio::spawn(Host::start_with(
        config,
        vec![Box::new(notes)],
        parts(),
        options,
    ));
    // Probe once the migration's sleep runs, so every probe below lands
    // inside it.
    let watcher = merezha_postgres::Database::connect(&test.config()).unwrap();
    let client = watcher
        .acquire(Instant::now() + Duration::from_secs(20))
        .await
        .unwrap();
    let began = Instant::now();
    loop {
        let running: i64 = client
            .query_one(
                "SELECT count(*) FROM pg_stat_activity
                  WHERE datname = current_database() AND query LIKE 'SELECT pg_sleep(4)%'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if running == 1 {
            break;
        }
        assert!(
            began.elapsed() < Duration::from_secs(20),
            "the migration never ran"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(client);
    assert!(!starting.is_finished(), "the migration is still running");
    let (status, _, body) = http_get(address, "/health/startup").await;
    assert_eq!((status, body.as_str()), (503, r#"{"status":"starting"}"#));
    assert_eq!(http_get(address, "/health/ready").await.0, 503);
    assert_eq!(http_get(address, "/health/live").await.0, 200);
    let (status, head, body) = http_get(address, "/v1/notes/count").await;
    assert_eq!(status, 503);
    assert!(head.contains("retry-after: 1"), "{head}");
    assert!(head.contains("x-request-id: "), "{head}");
    assert_eq!(
        body,
        r#"{"error":{"classification":"transient","code":"starting"}}"#
    );
    let host = starting.await.unwrap().unwrap();
    assert_eq!(
        http_get(address, "/health/startup").await.0,
        503,
        "starting until the listener switches"
    );
    assert_eq!(
        http_get(address, "/health/ready").await.0,
        503,
        "not ready until the listener switches, though every check passed"
    );
    assert!(host.lifecycle().evaluation().unwrap().passed());
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(early.serve(host, async move {
        let _ = stopped.await;
    }));
    let (status, _, body) = http_get(address, "/health/startup").await;
    assert_eq!((status, body.as_str()), (200, r#"{"status":"started"}"#));
    assert_eq!(http_get(address, "/health/ready").await.0, 200);
    let (status, _, body) = http_get(address, "/v1/notes/count").await;
    assert_eq!((status, body.as_str()), (200, r#"{"count":1}"#));
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

/// TE §8 "Health": SIGTERM flips readiness at once, the listener keeps
/// serving for `http.drain_delay_ms` with liveness at 200, then closes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_flips_readiness_before_the_listener_closes() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let config = config_with(
        &test,
        true,
        "",
        &[("http.drain_delay_ms".into(), "3000".into())],
    );
    assert_eq!(config.http.drain_delay, Duration::from_millis(3000));
    let notes = Notes::default();
    let host = Host::start(config, vec![Box::new(notes.clone())], parts())
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
    assert_eq!(http_get(address, "/health/ready").await.0, 200);
    let signalled = Instant::now();
    stop.send(()).unwrap();
    // Within the delay: no longer ready, still live and still serving.
    while http_get(address, "/health/ready").await.0 != 503 {
        assert!(
            signalled.elapsed() < Duration::from_millis(3000),
            "readiness did not flip within the drain delay"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(http_get(address, "/health/live").await.0, 200);
    assert_eq!(http_get(address, "/v1/notes/count").await.0, 200);
    assert_eq!(notes.stopped.load(Ordering::SeqCst), 0, "modules still run");
    tokio::time::timeout(Duration::from_secs(15), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(signalled.elapsed() >= Duration::from_millis(3000));
    assert_eq!(notes.stopped.load(Ordering::SeqCst), 1);
    assert!(
        tokio::net::TcpStream::connect(address).await.is_err(),
        "the listener closed after the delay"
    );
}

/// `Flaky::mode` values.
const HEALTHY: u8 = 0;
const PANICS: u8 = 1;
const BLOCKS: u8 = 2;
/// A module whose `ready()` answers, panics, or blocks its thread for 3 s
/// before it returns a future.
#[derive(Clone, Default)]
struct Flaky {
    mode: Arc<std::sync::atomic::AtomicU8>,
}
impl Module for Flaky {
    fn name(&self) -> &'static str {
        "flaky"
    }
    fn ready(&self) -> BoxFuture<'_, Result<(), Failure>> {
        match self.mode.load(Ordering::SeqCst) {
            PANICS => panic!("readiness check bug"),
            BLOCKS => std::thread::sleep(Duration::from_secs(3)),
            _ => {}
        }
        Box::pin(async { Ok(()) })
    }
}
/// Poll `/health/ready` through `router` until it answers `status`.
async fn ready_within(router: &Router, status: StatusCode, within: Duration) -> Duration {
    let started = Instant::now();
    loop {
        if call(router, get_request("/health/ready")).await.0 == status {
            return started.elapsed();
        }
        assert!(
            started.elapsed() < within,
            "/health/ready never answered {status}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Review finding 1: a readiness check that panics after a first success
/// turns readiness off at the next refresh (the refresh survives the
/// panic) and on again once it recovers.
#[tokio::test]
async fn a_panicking_readiness_check_turns_readiness_off() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let flaky = Flaky::default();
    let host = Host::start(
        config(&test, true, ""),
        vec![Box::new(Notes::default()), Box::new(flaky.clone())],
        parts(),
    )
    .await
    .unwrap();
    let router = host.router();
    assert_eq!(
        call(&router, get_request("/health/ready")).await.0,
        StatusCode::OK
    );
    flaky.mode.store(PANICS, Ordering::SeqCst);
    ready_within(
        &router,
        StatusCode::SERVICE_UNAVAILABLE,
        Duration::from_secs(10),
    )
    .await;
    let evaluation = host.lifecycle().evaluation().unwrap();
    let codes: Vec<(&str, &str)> = evaluation
        .modules
        .iter()
        .map(|m| (m.name, m.code.as_str()))
        .collect();
    assert_eq!(codes, [("notes", "ok"), ("flaky", "internal")]);
    flaky.mode.store(HEALTHY, Ordering::SeqCst);
    ready_within(&router, StatusCode::OK, Duration::from_secs(10)).await;
}

/// A module whose start takes long, to interrupt it.
struct SlowStart;
impl Module for SlowStart {
    fn name(&self) -> &'static str {
        "slow_start"
    }
    fn start(&self, _ctx: AppContext) -> BoxFuture<'_, Result<(), Failure>> {
        Box::pin(async {
            tokio::time::sleep(Duration::from_secs(8)).await;
            Ok(())
        })
    }
}

/// Review finding 5: a shutdown signal during startup interrupts the
/// module start in progress and shuts the modules already started down.
#[tokio::test]
async fn an_interrupted_startup_stops_the_modules_it_started() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let notes = Notes::default();
    let (signal, interrupt) = tokio::sync::watch::channel(false);
    let options = merezha_platform::HostOptions {
        interrupt: Some(interrupt),
        ..merezha_platform::HostOptions::default()
    };
    let starting = tokio::spawn(Host::start_with(
        config(&test, true, ""),
        vec![Box::new(notes.clone()), Box::new(SlowStart)],
        parts(),
        options.clone(),
    ));
    let deadline = Instant::now() + Duration::from_secs(5);
    while notes.started.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline, "notes never started");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let signalled = Instant::now();
    signal.send(true).unwrap();
    let error = starting.await.unwrap().unwrap_err();
    assert_eq!(error.failure.code, "startup_interrupted");
    // Well before the 8 s start would have ended on its own.
    assert!(signalled.elapsed() < Duration::from_secs(6));
    assert_eq!(
        notes.stopped.load(Ordering::SeqCst),
        1,
        "started modules stop"
    );
    // Already signalled: the next start refuses before touching anything.
    let error = Host::start_with(config(&test, true, ""), vec![], parts(), options)
        .await
        .unwrap_err();
    assert_eq!(error.failure.code, "startup_interrupted");
}

/// Latency and deadline-race tests (issue #37). CI runs them after the
/// parallel suite, alone and one at a time:
/// `cargo test -- --test-threads=1 timing::`.
mod timing {
    use super::*;
    /// Review finding 1: a `ready()` that blocks its thread is bounded by the
    /// check timeout and stalls neither the other checks nor the runtime,
    /// even on a single-threaded runtime.
    #[tokio::test(flavor = "current_thread")]
    async fn a_blocking_readiness_check_stalls_nothing_else() {
        let Some(test) = TestDatabase::create() else {
            return;
        };
        let flaky = Flaky::default();
        let notes = Notes::default();
        let host = Host::start(
            config(&test, true, ""),
            vec![Box::new(notes.clone()), Box::new(flaky.clone())],
            parts(),
        )
        .await
        .unwrap();
        let router = host.router();
        flaky.mode.store(BLOCKS, Ordering::SeqCst);
        let took = ready_within(
            &router,
            StatusCode::SERVICE_UNAVAILABLE,
            Duration::from_secs(3),
        )
        .await;
        assert!(took < Duration::from_millis(2_500), "{took:?}");
        // The runtime still answers at once while the check blocks.
        let started = Instant::now();
        assert_eq!(
            call(&router, get_request("/health/live")).await.0,
            StatusCode::OK
        );
        assert!(started.elapsed() < Duration::from_millis(200));
        // The other module's state still reaches readiness.
        notes.unready.store(true, Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let evaluation = host.lifecycle().evaluation().unwrap();
            let codes: Vec<(&str, &str)> = evaluation
                .modules
                .iter()
                .map(|m| (m.name, m.code.as_str()))
                .collect();
            if codes == [("notes", "warming_up"), ("flaky", "timeout")] {
                break;
            }
            assert!(Instant::now() < deadline, "{codes:?}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        notes.unready.store(false, Ordering::SeqCst);
        flaky.mode.store(HEALTHY, Ordering::SeqCst);
        ready_within(&router, StatusCode::OK, Duration::from_secs(6)).await;
    }
}

/// Provides a notifier factory, as the notifications block does.
struct Provider {
    name: &'static str,
    factory: Arc<merezha_platform::ports::CaptureNotifications>,
}
/// Delivers, unlike the capture fallback.
struct Delivering(Arc<merezha_platform::ports::CaptureNotifications>);
impl merezha_platform::ports::NotifierFactory for Delivering {
    fn bind(
        &self,
        source: &merezha_platform::Source,
    ) -> Arc<dyn merezha_platform::ports::Notifier> {
        self.0.bind(source)
    }
}
impl Module for Provider {
    fn name(&self) -> &'static str {
        self.name
    }
    fn notifier(&self) -> Option<Arc<dyn merezha_platform::ports::NotifierFactory>> {
        Some(Arc::new(Delivering(self.factory.clone())))
    }
}
/// Records whether its context delivers notifications.
struct Requester(Arc<AtomicBool>);
impl Module for Requester {
    fn name(&self) -> &'static str {
        "requester"
    }
    fn start(&self, ctx: AppContext) -> BoxFuture<'_, Result<(), Failure>> {
        self.0.store(ctx.notifications_deliver(), Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn a_module_provides_the_notifier_of_every_context_and_only_one_may() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let factory = Arc::new(merezha_platform::ports::CaptureNotifications::default());
    let delivers = Arc::new(AtomicBool::new(false));
    // The requester starts before the provider and still gets its factory.
    Host::start(
        config(&test, true, ""),
        vec![
            Box::new(Requester(delivers.clone())),
            Box::new(Provider {
                name: "notifications",
                factory: factory.clone(),
            }),
        ],
        parts(),
    )
    .await
    .unwrap();
    assert!(
        delivers.load(Ordering::SeqCst),
        "the provided factory is installed"
    );
    // Without a provider the capture fallback stays, which does not deliver.
    let fallback = Arc::new(AtomicBool::new(true));
    Host::start(
        config(&test, true, ""),
        vec![Box::new(Requester(fallback.clone()))],
        parts(),
    )
    .await
    .unwrap();
    assert!(!fallback.load(Ordering::SeqCst));
    let error = Host::start(
        config(&test, true, ""),
        vec![
            Box::new(Provider {
                name: "notifications",
                factory: factory.clone(),
            }),
            Box::new(Provider {
                name: "other",
                factory,
            }),
        ],
        parts(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        (error.failure.code.as_str(), error.module),
        ("notifier_conflict", Some("other"))
    );
}
