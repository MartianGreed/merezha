//! `merezha run` with a PostgreSQL application and `merezha application
//! verify`, as real processes against real PostgreSQL. Tests create their
//! database through `MEREZHA_TEST_DATABASE_URL` and skip with a message when
//! it is unset.
mod support;
use merezha_postgres::TestDatabase;
use serde_json::{Value, json};
use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};
use support::Host;

const DATABASE_ENV: &str = "RUN_TEST_DATABASE_URL";

fn request(address: SocketAddr, path: &str, body: &str) -> (u16, Value) {
    let reply = support::request(address, "POST", path, body);
    (reply.status, reply.body)
}
fn call(address: SocketAddr, method: &str, path: &str, body: &str) -> (u16, Value) {
    let reply = support::request(address, method, path, body);
    (reply.status, reply.body)
}
fn command(stream: &str, key: Option<&str>) -> String {
    json!({"tag":"Add","stream":stream,"payload":{"amount":1},"idempotency_key":key,
           "metadata":{"message_id":"m1","correlation_id":"c1","actor":"alice"}})
    .to_string()
}

/// A project whose application stores in PostgreSQL as `application`.
fn project(dir: &Path, application: &str, limits: &str, migrate: bool) -> PathBuf {
    fs::write(dir.join("protocol.wasm"), support::component()).unwrap();
    fs::write(
        dir.join("runtime.toml"),
        r#"version=1
[limits]
memory_bytes=16777216
fuel=1000000
max_input_bytes=65536
max_output_bytes=65536
timeout_ms=1000
max_concurrent_invocations=32
[[services]]
id="protocol"
path="protocol.wasm"
"#,
    )
    .unwrap();
    fs::write(
        dir.join("application.toml"),
        format!(
            r#"version=1
{limits}
[storage]
backend="postgres"
application="{application}"
[[commands]]
tag="Add"
service="protocol"
aggregate="Counter"
[[queries]]
tag="Total"
service="protocol"
projection="total"
[[projections]]
name="total"
service="protocol"
initial=0
"#
        ),
    )
    .unwrap();
    let path = dir.join("merezha.toml");
    fs::write(
        &path,
        format!(
            r#"[app]
id = "run-postgres"
[http]
listen = "127.0.0.1:0"
[database]
url = {{ env = "{DATABASE_ENV}" }}
migrate = {migrate}
pool_size = 4
connect_timeout_ms = 2000
[runtime]
manifest = "runtime.toml"
[application]
path = "application.toml"
[application.append]
lock_timeout_max_ms = 2000
"#
        ),
    )
    .unwrap();
    path
}
fn verify(config: &Path, url: &str) -> (Option<i32>, Value) {
    let output = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args(["application", "verify", "--config"])
        .arg(config)
        .env(DATABASE_ENV, url)
        .output()
        .unwrap();
    let report = serde_json::from_slice(&output.stdout).unwrap();
    (output.status.code(), report)
}

#[test]
fn two_hosts_on_one_database_both_serve_commands_and_verify_passes() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let configs: Vec<_> = dirs
        .iter()
        .map(|d| project(d.path(), "run-pg", "", true))
        .collect();
    let mut a = Host::spawn(&configs[0], &[(DATABASE_ENV, test.url())], "a");
    let mut b = Host::spawn(&configs[1], &[(DATABASE_ENV, test.url())], "b");
    let (left, right) = (a.listening(), b.listening());
    // Both hosts append to one position space, without gaps.
    let mut positions = Vec::new();
    for i in 0..6 {
        let host = if i % 2 == 0 { left } else { right };
        let (status, receipt) = request(
            host,
            "/v1/application/commands",
            &command(&format!("s{i}"), None),
        );
        assert_eq!(status, 200, "{receipt}");
        positions.push(receipt["position"].as_u64().unwrap());
    }
    positions.sort();
    assert_eq!(positions, (1..=6).collect::<Vec<_>>());
    // A key committed through one host replays through the other.
    let (status, first) = request(
        left,
        "/v1/application/commands",
        &command("keyed", Some("k")),
    );
    assert_eq!(status, 200, "{first}");
    let (status, again) = request(
        right,
        "/v1/application/commands",
        &command("keyed", Some("k")),
    );
    assert_eq!((status, &again), (200, &first));
    // A stale precondition is the documented conflict on either host.
    let stale = json!({"tag":"Add","stream":"s0","payload":{"amount":1},"expected_version":0,
                       "metadata":{"message_id":"m1","correlation_id":"c1"}});
    let (status, body) = request(right, "/v1/application/commands", &stale.to_string());
    assert_eq!(
        (status, body),
        (
            409,
            json!({"error":{"code":"concurrency_conflict","classification":"conflict"}})
        )
    );
    let (status, pair) = request(right, "/v1/application/projections/total/catch-up", "");
    assert_eq!(
        (status, pair["checkpoint"].clone()),
        (200, json!(7)),
        "{pair}"
    );
    let (status, model) = request(
        left,
        "/v1/application/queries",
        r#"{"tag":"Total","payload":{},"metadata":{"message_id":"q1","correlation_id":"c1"}}"#,
    );
    assert_eq!(
        (status, model["checkpoint"].clone()),
        (200, json!(7)),
        "{model}"
    );
    a.stop();
    b.stop();
    let (code, report) = verify(&configs[0], test.url());
    assert_eq!(code, Some(0), "{report}");
    assert_eq!(
        (
            report["ok"].clone(),
            report["head"].clone(),
            report["receipts"].clone()
        ),
        (json!(true), json!(7), json!(1))
    );
    for (config, log) in configs.iter().zip(["a", "b"]) {
        let text = fs::read_to_string(config.with_file_name(format!("{log}.log"))).unwrap();
        assert!(!text.contains(test.url()), "the database URL was logged");
    }
}

#[test]
fn verify_exits_one_on_a_violation_and_two_without_the_schema() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path(), "run-pg", "", true);
    let (code, report) = verify(&config, test.url());
    assert_eq!(code, Some(2), "{report}");
    assert_eq!(report["error"]["code"], "storage_schema");
    let mut host = Host::spawn(&config, &[(DATABASE_ENV, test.url())], "host");
    let address = host.listening();
    let (status, _) = request(address, "/v1/application/commands", &command("one", None));
    assert_eq!(status, 200);
    host.stop();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (client, connection) = merezha_postgres::tokio_postgres::connect(
            test.url(),
            merezha_postgres::tokio_postgres::NoTls,
        )
        .await
        .unwrap();
        let task = tokio::spawn(connection);
        client
            .batch_execute("UPDATE merezha_app.head SET position = 5")
            .await
            .unwrap();
        drop(client);
        let _ = task.await;
    });
    let (code, report) = verify(&config, test.url());
    assert_eq!(code, Some(1), "{report}");
    assert_eq!(report["ok"], false);
    assert_eq!(
        report["violations"][0],
        json!({"check":"head_matches_events","subject":"head"})
    );
    let usage = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args(["application", "verify"])
        .output()
        .unwrap();
    assert_eq!(usage.status.code(), Some(2));
    let body: Value = serde_json::from_slice(&usage.stdout).unwrap();
    assert_eq!(body["error"]["code"], "invalid_arguments");
}

#[test]
fn startup_reports_the_application_storage_code() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    // Without applied migrations the application cannot open.
    let dir = tempfile::tempdir().unwrap();
    let pending = project(dir.path(), "run-pg", "", false);
    assert_eq!(
        Host::spawn(&pending, &[(DATABASE_ENV, test.url())], "pending").failed()["code"],
        "storage_schema"
    );
    // The first start records the identity; another one is refused.
    let dir = tempfile::tempdir().unwrap();
    let first = project(dir.path(), "run-pg", "", true);
    let mut host = Host::spawn(&first, &[(DATABASE_ENV, test.url())], "first");
    host.listening();
    host.stop();
    let dir = tempfile::tempdir().unwrap();
    let other = project(dir.path(), "someone-else", "", true);
    assert_eq!(
        Host::spawn(&other, &[(DATABASE_ENV, test.url())], "other").failed()["code"],
        "storage_configuration"
    );
    // Total-capacity keys fail before the database is touched.
    let dir = tempfile::tempdir().unwrap();
    let capacity = project(dir.path(), "run-pg", "[limits]\nmax_events=10", true);
    assert_eq!(
        Host::spawn(
            &capacity,
            &[(DATABASE_ENV, "postgres://nobody:pw@127.0.0.1:1/none")],
            "capacity"
        )
        .failed()["code"],
        "invalid_limits"
    );
}

#[test]
fn projections_advance_in_the_background_and_the_admin_api_reports_and_rebuilds() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path(), "run-pg", "", true);
    let mut host = Host::spawn(&config, &[(DATABASE_ENV, test.url())], "admin");
    let address = host.ready();
    let mut position = 0;
    for i in 0..5 {
        let (status, receipt) = request(
            address,
            "/v1/application/commands",
            &command(&format!("s{i}"), None),
        );
        assert_eq!(status, 200, "{receipt}");
        position = receipt["position"].as_u64().unwrap();
    }
    // Read-your-write without an explicit catch-up.
    let (status, model) = request(
        address,
        "/v1/application/queries",
        &json!({"tag":"Total","payload":{},"min_position":position,
                "metadata":{"message_id":"q1","correlation_id":"c1"}})
        .to_string(),
    );
    assert_eq!(
        (status, model["checkpoint"].clone()),
        (200, json!(5)),
        "{model}"
    );
    let (status, document) = call(address, "GET", "/v1/application/status", "");
    assert_eq!(status, 200, "{document}");
    assert_eq!(
        (
            document["application"].clone(),
            document["head"].clone(),
            document["projections"][0]["name"].clone(),
            document["projections"][0]["lag_events"].clone(),
            document["reactors"].clone(),
        ),
        (
            json!("run-pg"),
            json!(5),
            json!("total"),
            json!(0),
            json!([])
        )
    );
    assert!(document["instance"]["id"].is_string());
    assert!(matches!(
        document["listener"].as_str(),
        Some("connected" | "reconnecting")
    ));
    let (status, overview) = call(address, "GET", "/v1/application/projections", "");
    assert_eq!(status, 200, "{overview}");
    assert_eq!(overview["head"], 5);
    assert_eq!(
        (
            overview["projections"][0]["checkpoint"].clone(),
            overview["projections"][0]["warm"].clone(),
            overview["projections"][0]["status"].clone(),
        ),
        (json!(5), json!(true), json!("active"))
    );
    // A rebuild is accepted and runs in the background to its swap.
    let (status, accepted) = request(address, "/v1/application/projections/total/rebuild", "{}");
    assert_eq!(
        (status, accepted),
        (
            202,
            json!({"rebuild":{"name":"total","revision":1,"target":5}})
        )
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (status, entry) = call(address, "GET", "/v1/application/projections/total", "");
        assert_eq!(status, 200, "{entry}");
        if entry.get("rebuild").is_none() {
            assert_eq!(
                entry["revisions"],
                json!([{"revision":1,"checkpoint":5,"warm":true}])
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the rebuild never swapped: {entry}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let (status, body) = request(
        address,
        "/v1/application/projections/total/rebuild",
        r#"{"resume":"yes"}"#,
    );
    assert_eq!(
        (status, body["error"]["code"].clone()),
        (400, json!("invalid_input"))
    );
    let (status, _) = call(
        address,
        "DELETE",
        "/v1/application/projections/total/rebuild",
        "",
    );
    assert_eq!(status, 204);
    let (status, body) = call(
        address,
        "DELETE",
        "/v1/application/projections/total/revisions/1",
        "",
    );
    assert_eq!(
        (status, body["error"]["code"].clone()),
        (409, json!("revision_active"))
    );
    let (status, body) = call(address, "GET", "/v1/application/projections/missing", "");
    assert_eq!(
        (status, body["error"]["code"].clone()),
        (404, json!("projection_not_found"))
    );
    let (status, _) = call(address, "GET", "/health/ready", "");
    assert_eq!(status, 200);
    host.stop();
    let text = fs::read_to_string(config.with_file_name("admin.log")).unwrap();
    assert!(!text.contains(test.url()), "the database URL was logged");
}

#[test]
fn startup_refuses_a_guest_runtime_below_the_concurrency_budget() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path(), "run-pg", "[limits]\nmax_concurrent=31", true);
    assert_eq!(
        Host::spawn(&config, &[(DATABASE_ENV, test.url())], "budget").failed()["code"],
        "invalid_concurrency_budget"
    );
    // A pool below the guidance only warns.
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path(), "run-pg", "", true);
    let mut host = Host::spawn(&config, &[(DATABASE_ENV, test.url())], "pool");
    host.ready();
    assert!(
        host.events()
            .iter()
            .any(|e| e["event"] == "db.pool_undersized" && e["pool_size"] == 4),
        "pool warning"
    );
    host.stop();
}
