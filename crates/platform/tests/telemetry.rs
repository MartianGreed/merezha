//! The kernel's events through the telemetry harness, against real
//! PostgreSQL: every event the host lifecycle emits is in the catalog at a
//! catalog level with catalog fields, and no planted canary reaches stderr,
//! spans, logs or metrics.
use axum::{Router, routing::get};
use merezha_domain::Failure;
use merezha_platform::{
    AppContext, BoxFuture, Host, HostParts, Migration, Module, ProjectConfig, Timeouts,
    ports::LoopbackAllowAll,
};
use merezha_postgres::TestDatabase;
use merezha_telemetry::testing::Harness;
use std::{
    io::{Read, Write},
    sync::Arc,
    time::Duration,
};

const CANARY: &str = "kErNeL-cAnArY-31";
const MIGRATIONS: [Migration; 1] = [Migration::new(1, "create", "CREATE SCHEMA probe;")];

struct Probe;
impl Module for Probe {
    fn name(&self) -> &'static str {
        "probe"
    }
    fn migrations(&self) -> &[Migration] {
        &MIGRATIONS
    }
    fn start(&self, _ctx: AppContext) -> BoxFuture<'_, Result<(), Failure>> {
        Box::pin(async { Ok(()) })
    }
    fn routes(&self, _ctx: &AppContext) -> Router {
        Router::new().route("/ping", get(|| async { "pong" }))
    }
}

fn config(test: &TestDatabase, migrate: bool) -> ProjectConfig {
    let separator = if test.url().contains('?') { '&' } else { '?' };
    let url = format!("{}{separator}application_name={CANARY}", test.url());
    ProjectConfig::parse(
        &format!(
            "[app]\nid = \"probe-app\"\n[database]\nurl = {{ env = \"PROBE_DATABASE_URL\" }}\nmigrate = {migrate}\npool_size = 2\n[runtime]\nmanifest = \"runtime.toml\"\n[http]\nlisten = \"127.0.0.1:0\"\n"
        ),
        std::env::temp_dir(),
        &[("PROBE_DATABASE_URL".into(), url)],
        &[],
    )
    .unwrap()
}
fn parts() -> HostParts {
    HostParts {
        routes: Router::new(),
        authorizer: Arc::new(LoopbackAllowAll::new("127.0.0.1:0".parse().unwrap()).unwrap()),
        timeouts: Timeouts {
            drain: Duration::from_secs(2),
            ..Timeouts::default()
        },
        jobs: None,
    }
}

#[tokio::test]
async fn the_host_lifecycle_emits_only_catalog_events_and_no_canary() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    // Verify mode first: the pending migration is reported, not applied.
    let pending = Host::start(config(&test, false), vec![Box::new(Probe)], parts())
        .await
        .unwrap();
    drop(pending);
    let host = Host::start(config(&test, true), vec![Box::new(Probe)], parts())
        .await
        .unwrap();
    let listener = host.bind(None).await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(host.serve(listener, async move {
        let _ = stopped.await;
    }));
    let reply = merezha_telemetry::spawn_blocking_in_span(move || {
        let mut socket = std::net::TcpStream::connect(address).unwrap();
        write!(
            socket,
            "GET /v1/probe/ping?secret={CANARY} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {CANARY}\r\nCookie: s={CANARY}\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        let mut reply = String::new();
        socket.read_to_string(&mut reply).unwrap();
        reply
    })
    .await
    .unwrap();
    assert!(reply.starts_with("HTTP/1.1 200"), "{reply}");
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();

    let events: Vec<String> = harness
        .recorded()
        .into_iter()
        .filter_map(|e| e.name)
        .collect();
    for expected in [
        "db.connected",
        "migration.checked",
        "migration.pending",
        "module.started",
        "listener.ready",
        "http.request.completed",
        "listener.draining",
        "module.stopped",
        "listener.stopped",
    ] {
        assert!(
            events.iter().any(|e| e == expected),
            "missing {expected} in {events:?}"
        );
    }
    harness.assert_catalogued();
    harness.assert_no_canary(CANARY);
    let lines = harness.stderr_lines();
    let ready = lines
        .iter()
        .find(|l| l["event"] == "listener.ready")
        .unwrap();
    assert_eq!(ready["address"], address.to_string());
    assert_eq!(ready["app"], "probe-app");
    let pending = lines
        .iter()
        .find(|l| l["event"] == "migration.pending")
        .unwrap();
    assert_eq!(
        (pending["level"].as_str(), pending["module"].as_str()),
        (Some("warn"), Some("probe"))
    );
    assert_eq!(pending["migration_version"], 1);
    let logs = harness.logs();
    let bridged = logs
        .iter()
        .find(|l| l.record.event_name() == Some("migration.pending"))
        .unwrap();
    let keys: Vec<String> = bridged
        .record
        .attributes_iter()
        .map(|(k, _)| k.to_string())
        .collect();
    assert_eq!(keys, ["merezha.module"], "unmapped fields stay on stderr");
}
