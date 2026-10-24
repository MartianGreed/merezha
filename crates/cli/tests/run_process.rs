//! `merezha run` as a real process over loopback HTTP against real
//! PostgreSQL. Tests needing a database create one through
//! `MEREZHA_TEST_DATABASE_URL` and skip with a message when it is unset.
mod support;
use merezha_postgres::{Database, TestDatabase};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

const DATABASE_ENV: &str = "RUN_TEST_DATABASE_URL";

struct Host {
    child: Child,
    log: PathBuf,
}
impl Host {
    fn spawn(config: &Path, env: &[(&str, &str)], name: &str) -> Self {
        let log = config.with_file_name(format!("{name}.log"));
        let mut command = Command::new(env!("CARGO_BIN_EXE_merezha"));
        command
            .args(["run", "--config"])
            .arg(config)
            .stdout(Stdio::null())
            .stderr(File::create(&log).unwrap());
        for (key, value) in env {
            command.env(key, value);
        }
        Self {
            child: command.spawn().unwrap(),
            log,
        }
    }
    fn events(&self) -> Vec<Value> {
        fs::read_to_string(&self.log)
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect()
    }
    fn ready(&mut self) -> SocketAddr {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(event) = self
                .events()
                .into_iter()
                .find(|e| e["event"] == "listener.ready")
            {
                return event["address"].as_str().unwrap().parse().unwrap();
            }
            let text = fs::read_to_string(&self.log).unwrap();
            assert!(
                self.child.try_wait().unwrap().is_none(),
                "host exited before readiness: {text}"
            );
            assert!(Instant::now() < deadline, "readiness timeout: {text}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    /// Wait for exit code 2 and return the `startup.failed` event.
    fn failed(&mut self) -> Value {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert_eq!(status.code(), Some(2));
                return self
                    .events()
                    .into_iter()
                    .find(|e| e["event"] == "startup.failed")
                    .expect("startup.failed event");
            }
            assert!(Instant::now() < deadline, "host failure timeout");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    fn stop(&mut self) {
        assert!(
            Command::new("kill")
                .args(["-TERM", &self.child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success(), "graceful host exit: {status}");
                return;
            }
            assert!(Instant::now() < deadline, "host shutdown timeout");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}
impl Drop for Host {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Reply {
    status: u16,
    headers: String,
    body: Value,
}
fn request(address: SocketAddr, method: &str, path: &str, body: &str) -> Reply {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    Reply {
        status: headers.split_whitespace().nth(1).unwrap().parse().unwrap(),
        headers: headers.to_ascii_lowercase(),
        body: serde_json::from_str(body).unwrap_or_else(|_| json!(body)),
    }
}
fn wait_for_ready_status(address: SocketAddr, status: u16) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let reply = request(address, "GET", "/health/ready", "");
        if reply.status == status {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "readiness stayed {} {}",
            reply.status,
            reply.body
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A project directory with the runtime, application and `merezha.toml`.
fn project(dir: &Path, extra: &str) -> PathBuf {
    fs::write(dir.join("protocol.wasm"), support::component()).unwrap();
    fs::write(
        dir.join("echo.wasm"),
        wat::parse_str(include_str!("../../runtime/tests/fixtures/echo.wat")).unwrap(),
    )
    .unwrap();
    fs::write(
        dir.join("runtime.toml"),
        r#"version=1
[limits]
memory_bytes=16777216
fuel=1000000
max_input_bytes=65536
max_output_bytes=65536
timeout_ms=1000
max_concurrent_invocations=4
[[services]]
id="protocol"
path="protocol.wasm"
[[services]]
id="echo"
path="echo.wasm"
"#,
    )
    .unwrap();
    fs::write(
        dir.join("application.toml"),
        r#"version=1
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
revision=1
"#,
    )
    .unwrap();
    let path = dir.join("merezha.toml");
    fs::write(
        &path,
        format!(
            r#"[app]
id = "run-process"
[http]
listen = "127.0.0.1:0"
[database]
url = {{ env = "{DATABASE_ENV}" }}
migrate = true
pool_size = 2
connect_timeout_ms = 2000
[runtime]
manifest = "runtime.toml"
[application]
path = "application.toml"
{extra}"#
        ),
    )
    .unwrap();
    path
}
const COMMAND: &str = r#"{"tag":"Add","stream":"one","payload":{"amount":1},"idempotency_key":"key","metadata":{"message_id":"m1","correlation_id":"c1","actor":"alice"}}"#;

fn ledger_exists(test: &TestDatabase) -> bool {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let db = Database::connect(&test.config()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let client = db.acquire(deadline).await.unwrap();
        client
            .query_one("SELECT to_regclass('merezha.migrations') IS NOT NULL", &[])
            .await
            .unwrap()
            .get(0)
    })
}

#[test]
fn run_serves_health_runtime_and_application_behind_the_edge_and_stops_gracefully() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path(), "");
    let mut host = Host::spawn(&config, &[(DATABASE_ENV, test.url())], "host");
    let address = host.ready();
    assert!(ledger_exists(&test), "migrations ran against PostgreSQL");

    let live = request(address, "GET", "/health/live", "");
    assert_eq!((live.status, live.body), (200, json!({"status":"live"})));
    let ready = request(address, "GET", "/health/ready", "");
    assert_eq!((ready.status, ready.body), (200, json!({"status":"ready"})));
    assert!(ready.headers.contains("x-request-id: "));

    let echo = request(address, "POST", "/v1/services/echo/invoke", "opaque");
    assert_eq!((echo.status, echo.body), (200, json!("opaque")));
    let receipt = request(address, "POST", "/v1/application/commands", COMMAND);
    assert_eq!(receipt.status, 200, "{}", receipt.body);
    assert_eq!(receipt.body["position"], 1);
    // A private listener keeps projection administration open.
    let caught_up = request(
        address,
        "POST",
        "/v1/application/projections/total/catch-up",
        "",
    );
    assert_eq!(
        (caught_up.status, caught_up.body),
        (200, json!({"state":1,"checkpoint":1}))
    );

    // One error shape for kernel, runtime and application failures.
    for (method, path, body, status, code) in [
        ("GET", "/missing", "", 404, "not_found"),
        ("POST", "/v1/services/missing/invoke", "x", 404, "not_found"),
        (
            "POST",
            "/v1/application/commands",
            "{",
            422,
            "validation_failed",
        ),
    ] {
        let reply = request(address, method, path, body);
        assert_eq!(reply.status, status, "{path}: {}", reply.body);
        assert_eq!(reply.body["error"]["code"], code, "{path}");
        assert_eq!(reply.body["error"]["classification"], "permanent", "{path}");
    }

    // Readiness follows the database while liveness does not.
    test.admin_execute(&format!(
        "ALTER DATABASE \"{0}\" ALLOW_CONNECTIONS false; SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{0}'",
        test.name()
    ));
    wait_for_ready_status(address, 503);
    assert_eq!(request(address, "GET", "/health/live", "").status, 200);
    test.admin_execute(&format!(
        "ALTER DATABASE \"{}\" ALLOW_CONNECTIONS true",
        test.name()
    ));
    wait_for_ready_status(address, 200);

    host.stop();
    let events: Vec<String> = host
        .events()
        .iter()
        .filter_map(|e| e["event"].as_str().map(str::to_owned))
        .collect();
    for expected in [
        "db.connected",
        "migration.checked",
        "listener.ready",
        "http.request.completed",
        "listener.draining",
        "listener.stopped",
    ] {
        assert!(
            events.iter().any(|e| e == expected),
            "missing {expected} in {events:?}"
        );
    }
    let log = fs::read_to_string(&host.log).unwrap();
    assert!(!log.contains(test.url()), "the database URL was logged");
}

#[test]
fn a_public_listener_without_identity_denies_application_and_hides_the_raw_runtime() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    const QUERY: &str =
        r#"{"tag":"Total","payload":{},"metadata":{"message_id":"q1","correlation_id":"c1"}}"#;
    // Both stay bound to loopback so the test exposes nothing. A trusted
    // proxy makes a loopback listener reachable from elsewhere, so it is
    // public too.
    for (name, setting) in [
        ("public", ("MEREZHA_HTTP__PUBLIC", "true")),
        ("proxied", ("MEREZHA_HTTP__TRUSTED_PROXIES", "127.0.0.1")),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let config = project(dir.path(), "");
        let mut host = Host::spawn(&config, &[(DATABASE_ENV, test.url()), setting], name);
        let address = host.ready();
        for path in ["/v1/services/echo/invoke", "/v1/reload", "/health"] {
            let method = if path == "/health" { "GET" } else { "POST" };
            let raw = request(address, method, path, "");
            assert_eq!(raw.status, 404, "{name} {path}: {}", raw.body);
        }
        // Messages carry no principal of their own: the policy refuses them.
        // Projection controls are permission checks, and an anonymous caller
        // is unauthenticated.
        for (path, body, status, code) in [
            ("/v1/application/commands", COMMAND, 403, "unauthorized"),
            ("/v1/application/queries", QUERY, 403, "unauthorized"),
            (
                "/v1/application/projections/total/catch-up",
                "",
                401,
                "unauthenticated",
            ),
            (
                "/v1/application/projections/total/rebuild",
                "",
                401,
                "unauthenticated",
            ),
        ] {
            let denied = request(address, "POST", path, body);
            assert_eq!(
                (denied.status, denied.body),
                (
                    status,
                    json!({"error":{"code":code,"classification":"permanent"}})
                ),
                "{name} {path}"
            );
        }
        host.stop();
    }
}

#[test]
fn invalid_configuration_reports_every_issue_without_values() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("merezha.toml");
    fs::write(
        &path,
        r#"[app]
id = "Bad Id"
[http]
listen = "0.0.0.0:8080"
[database]
url = "postgres://user:hunter2@db/app"
colour = "blue"
"#,
    )
    .unwrap();
    let mut host = Host::spawn(&path, &[("MEREZHA_HTTP__BODY_LIMIT", "1")], "invalid");
    let event = host.failed();
    assert_eq!(event["code"], "invalid_config");
    let issues: Vec<(String, String)> = event["issues"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| {
            (
                i["key"].as_str().unwrap().to_owned(),
                i["code"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    for expected in [
        ("app.id", "invalid_app_id"),
        ("http.listen", "public_listen_not_enabled"),
        ("http.body_limit", "out_of_range"),
        ("database.url", "secret_literal"),
        ("database.colour", "unknown_key"),
        ("runtime.manifest", "missing_required"),
    ] {
        assert!(
            issues.contains(&(expected.0.into(), expected.1.into())),
            "missing {expected:?} in {issues:?}"
        );
    }
    assert!(!fs::read_to_string(&host.log).unwrap().contains("hunter2"));
}

#[test]
fn an_application_using_keys_of_later_slices_stops_startup_before_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path(), "");
    let application = dir.path().join("application.toml");
    let text = fs::read_to_string(&application).unwrap().replacen(
        "version=1",
        "version=2\n[limits]\nmax_stream_events=8",
        1,
    );
    fs::write(
        &application,
        format!("{text}[[reactors]]\nname=\"notifier\"\nservice=\"protocol\"\n"),
    )
    .unwrap();
    // The application opens before the pool, so no database is needed.
    let mut host = Host::spawn(
        &config,
        &[(DATABASE_ENV, "postgres://nobody:pw@127.0.0.1:1/none")],
        "unsupported",
    );
    assert_eq!(host.failed()["code"], "unsupported_in_this_build");
}

#[test]
fn an_unreachable_database_stops_startup() {
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path(), "");
    let mut host = Host::spawn(
        &config,
        &[(DATABASE_ENV, "postgres://nobody:pw@127.0.0.1:1/none")],
        "unreachable",
    );
    assert_eq!(host.failed()["code"], "database_unavailable");
    assert!(!fs::read_to_string(&host.log).unwrap().contains(":pw@"));
}

#[test]
fn run_arguments_are_validated() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("arguments.log");
    let status = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args(["run", "--listen", "127.0.0.1:0"])
        .stderr(File::create(&log).unwrap())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(2));
    let text = fs::read_to_string(&log).unwrap();
    assert!(text.contains("invalid_arguments") && text.contains("merezha run --config"));
}
