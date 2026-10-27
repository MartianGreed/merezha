//! `merezha run` with the telemetry pipeline as a real process: startup
//! failures log at error, the startup path exports traces, logs and metrics
//! over OTLP/HTTP to a stub collector, `OTEL_*` variables are ignored, and
//! no planted canary reaches stderr or an exported payload.
mod support;
use merezha_postgres::TestDatabase;
use serde_json::Value;
use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const CANARY: &str = "cAnArY-5e1f";

/// One request the stub collector received.
#[derive(Clone, Debug)]
struct Received {
    path: String,
    /// Lowercased header block.
    headers: String,
    body: Vec<u8>,
}

/// A stub OTLP/HTTP collector: answers every POST with an empty protobuf
/// message and keeps what it received.
struct Collector {
    address: SocketAddr,
    received: Arc<Mutex<Vec<Received>>>,
}
impl Collector {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let received = Arc::new(Mutex::new(Vec::new()));
        let sink = received.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let sink = sink.clone();
                std::thread::spawn(move || serve(stream, &sink));
            }
        });
        Self { address, received }
    }
    fn received(&self) -> Vec<Received> {
        self.received.lock().unwrap().clone()
    }
    fn wait_for(&self, paths: &[&str]) -> Vec<Received> {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let received = self.received();
            if paths.iter().all(|p| received.iter().any(|r| r.path == *p)) {
                return received;
            }
            assert!(Instant::now() < deadline, "collector got {received:?}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}
fn serve(stream: TcpStream, sink: &Mutex<Vec<Received>>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut writer = stream;
    loop {
        let mut head = String::new();
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => return,
                Ok(_) if line == "\r\n" => break,
                Ok(_) => head.push_str(&line),
            }
        }
        let headers = head.to_ascii_lowercase();
        let path = head
            .split_whitespace()
            .nth(1)
            .unwrap_or_default()
            .to_owned();
        let length = headers
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(0);
        let mut body = vec![0; length];
        if reader.read_exact(&mut body).is_err() {
            return;
        }
        sink.lock().unwrap().push(Received {
            path,
            headers,
            body,
        });
        let _ = writer.write_all(
            b"HTTP/1.1 200 OK\r\ncontent-type: application/x-protobuf\r\ncontent-length: 0\r\n\r\n",
        );
    }
}

struct Host {
    child: Child,
    log: PathBuf,
}
impl Host {
    fn spawn(config: &Path, env: &[(&str, &str)]) -> Self {
        let log = config.with_extension("log");
        let mut command = Command::new(env!("CARGO_BIN_EXE_merezha"));
        command
            .args(["run", "--config"])
            .arg(config)
            .env("MEREZHA_HTTP__DRAIN_DELAY_MS", "0")
            .stdout(Stdio::null())
            .stderr(fs::File::create(&log).unwrap());
        for (key, value) in env {
            command.env(key, value);
        }
        Self {
            child: command.spawn().unwrap(),
            log,
        }
    }
    fn stderr(&self) -> String {
        fs::read_to_string(&self.log).unwrap()
    }
    fn events(&self) -> Vec<Value> {
        let stderr = self.stderr();
        if let Err(problems) = merezha_telemetry::testing::check_stderr_catalogued(&stderr) {
            panic!("event catalog: {problems}");
        }
        stderr
            .lines()
            .map(|l| serde_json::from_str(l).expect("every stderr line is JSON"))
            .collect()
    }
    fn event(&self, name: &str) -> Option<Value> {
        self.events().into_iter().find(|e| e["event"] == name)
    }
    fn wait(&mut self) -> i32 {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status.code().unwrap_or(-1);
            }
            assert!(
                Instant::now() < deadline,
                "host did not exit: {}",
                self.stderr()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}
impl Drop for Host {
    /// Kill the host, then check its stderr against the event catalog.
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if !std::thread::panicking()
            && let Ok(text) = fs::read_to_string(&self.log)
            && let Err(problems) = merezha_telemetry::testing::check_stderr_catalogued(&text)
        {
            panic!("event catalog: {problems}");
        }
    }
}

/// Wait for `/health/ready` to answer 200: the jobs runtime reports ready
/// after its first subscription sync, which runs after `listener.ready`.
fn wait_until_ready(address: SocketAddr) {
    let deadline = Instant::now() + support::READINESS_BOUND;
    while support::request(address, "GET", "/health/ready", "").status != 200 {
        assert!(
            Instant::now() < deadline,
            "/health/ready never answered 200"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn write_config(dir: &Path, body: &str) -> PathBuf {
    let path = dir.join("merezha.toml");
    fs::write(&path, body).unwrap();
    path
}

#[test]
fn startup_failures_log_at_error_on_both_sides_of_the_subscriber() {
    let dir = tempfile::tempdir().unwrap();
    // Before the subscriber: the configuration is unreadable.
    let mut host = Host::spawn(&dir.path().join("missing.toml"), &[]);
    assert_eq!(host.wait(), 2);
    let failed = host.event("startup.failed").unwrap();
    assert_eq!(
        (failed["level"].as_str(), failed["code"].as_str()),
        (Some("error"), Some("invalid_config"))
    );
    assert_eq!(failed["issues"][0]["code"], "config_unreadable");
    assert_eq!(failed["service"], "merezha");

    // After the subscriber: the manifest is missing. signals = [] sends
    // nothing to the collector.
    let collector = Collector::start();
    let config = write_config(
        dir.path(),
        &format!(
            r#"[app]
id = "t1"
instance_id = "t1-a:7"
[database]
url = {{ env = "T1_DATABASE_URL" }}
[runtime]
manifest = "missing-runtime.toml"
[telemetry]
endpoint = "http://{}"
signals = []
"#,
            collector.address
        ),
    );
    let mut host = Host::spawn(
        &config,
        &[("T1_DATABASE_URL", "postgres://u@127.0.0.1:1/db")],
    );
    assert_eq!(host.wait(), 2);
    let failed = host.event("startup.failed").unwrap();
    assert_eq!(
        (failed["level"].as_str(), failed["code"].as_str()),
        (Some("error"), Some("invalid_manifest"))
    );
    assert_eq!(
        (failed["service"].as_str(), failed["instance"].as_str()),
        (Some("t1"), Some("t1-a:7"))
    );
    std::thread::sleep(Duration::from_millis(200));
    assert!(collector.received().is_empty(), "signals = [] exported");
}

/// A runtime with the no-import echo component.
fn runtime(dir: &Path) {
    fs::write(
        dir.join("echo.wasm"),
        wat::parse_str(include_str!("../../runtime/tests/fixtures/echo.wat")).unwrap(),
    )
    .unwrap();
    fs::write(
        dir.join("runtime.toml"),
        "version=1\n[limits]\nmemory_bytes=16777216\nfuel=1000000\nmax_input_bytes=65536\nmax_output_bytes=65536\ntimeout_ms=1000\nmax_concurrent_invocations=4\n[[services]]\nid=\"echo\"\npath=\"echo.wasm\"\n",
    )
    .unwrap();
}

#[test]
fn the_startup_path_exports_over_otlp_and_leaks_no_canary() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    runtime(dir.path());
    let collector = Collector::start();
    let config = write_config(
        dir.path(),
        &format!(
            r#"[app]
id = "tasks"
instance_id = "tasks-a:1"
version = "1.4.0"
[http]
listen = "127.0.0.1:0"
[database]
url = {{ env = "T1_DATABASE_URL" }}
migrate = true
pool_size = 2
[runtime]
manifest = "runtime.toml"
[telemetry]
endpoint = "http://{}"
compression = "none"
headers = {{ authorization = {{ env = "T1_OTLP_AUTHORIZATION" }} }}
shutdown_timeout_ms = 5000
[telemetry.traces]
schedule_delay_ms = 100
[telemetry.logs]
schedule_delay_ms = 100
[telemetry.metrics]
interval_ms = 1000
"#,
            collector.address
        ),
    );
    let separator = if test.url().contains('?') { '&' } else { '?' };
    let url = format!("{}{separator}application_name={CANARY}-db", test.url());
    let authorization = format!("Bearer {CANARY}-header");
    let leak = format!("x-leak={CANARY}-otel");
    let mut host = Host::spawn(
        &config,
        &[
            ("T1_DATABASE_URL", url.as_str()),
            ("T1_OTLP_AUTHORIZATION", authorization.as_str()),
            ("OTEL_EXPORTER_OTLP_HEADERS", leak.as_str()),
            ("OTEL_SERVICE_NAME", CANARY),
            ("T1_UNRELATED", CANARY),
        ],
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    let ready = loop {
        if let Some(ready) = host.event("listener.ready") {
            break ready;
        }
        assert!(
            host.child.try_wait().unwrap().is_none(),
            "{}",
            host.stderr()
        );
        assert!(Instant::now() < deadline, "{}", host.stderr());
        std::thread::sleep(Duration::from_millis(20));
    };
    // The same fields as before the telemetry block, plus the instance.
    for field in ["address", "app", "modules", "timestamp_unix_ms", "version"] {
        assert!(
            ready.get(field).is_some(),
            "listener.ready lacks {field}: {ready}"
        );
    }
    assert_eq!(
        (ready["service"].as_str(), ready["instance"].as_str()),
        (Some("tasks"), Some("tasks-a:1"))
    );
    let ignored = host.event("telemetry.otel_env_ignored").unwrap();
    assert_eq!(
        ignored["names"],
        "OTEL_EXPORTER_OTLP_HEADERS,OTEL_SERVICE_NAME"
    );

    let address: SocketAddr = ready["address"].as_str().unwrap().parse().unwrap();
    wait_until_ready(address);
    let mut socket = TcpStream::connect(address).unwrap();
    // One write, so an unread request cannot reset the connection.
    let request = format!(
        "GET /health/live?token={CANARY}-query HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {CANARY}-request\r\nConnection: close\r\n\r\n"
    );
    socket.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    socket.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");

    assert!(
        Command::new("kill")
            .args(["-TERM", &host.child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert_eq!(host.wait(), 0, "{}", host.stderr());

    let received = collector.wait_for(&["/v1/traces", "/v1/logs", "/v1/metrics"]);
    for request in &received {
        assert!(
            request.headers.contains(&format!(
                "authorization: bearer {}-header",
                CANARY.to_ascii_lowercase()
            )),
            "configured headers reach the collector: {}",
            request.headers
        );
        assert!(
            !request.headers.contains("x-leak"),
            "OTEL_EXPORTER_OTLP_HEADERS was read"
        );
        assert!(!request.headers.contains("traceparent"));
        let body = String::from_utf8_lossy(&request.body);
        assert!(!body.contains(CANARY), "{} leaked the canary", request.path);
        for expected in ["tasks", "tasks-a:1", "1.4.0"] {
            assert!(body.contains(expected), "{} lacks {expected}", request.path);
        }
    }
    let traces: String = received
        .iter()
        .filter(|r| r.path == "/v1/traces")
        .map(|r| String::from_utf8_lossy(&r.body).into_owned())
        .collect();
    assert!(traces.contains("startup"), "the startup span is exported");
    let logs: String = received
        .iter()
        .filter(|r| r.path == "/v1/logs")
        .map(|r| String::from_utf8_lossy(&r.body).into_owned())
        .collect();
    assert!(logs.contains("listener.ready") && logs.contains("db.connected"));
    let stderr = host.stderr();
    assert!(!stderr.contains(CANARY), "stderr leaked the canary");
    assert!(!stderr.contains(test.url()), "the database URL was logged");
}

#[test]
fn a_down_collector_costs_at_most_one_warning_per_signal_on_stderr() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    runtime(dir.path());
    let closed = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let config = write_config(
        dir.path(),
        &format!(
            r#"[app]
id = "tasks"
[http]
listen = "127.0.0.1:0"
[database]
url = {{ env = "T1_DATABASE_URL" }}
migrate = true
pool_size = 2
[runtime]
manifest = "runtime.toml"
[telemetry]
endpoint = "http://{closed}"
timeout_ms = 1000
[telemetry.traces]
schedule_delay_ms = 100
[telemetry.logs]
schedule_delay_ms = 100
[telemetry.metrics]
interval_ms = 1000
"#
        ),
    );
    let mut host = Host::spawn(&config, &[("T1_DATABASE_URL", test.url())]);
    let deadline = Instant::now() + Duration::from_secs(30);
    while host.event("listener.ready").is_none() {
        assert!(
            host.child.try_wait().unwrap().is_none(),
            "{}",
            host.stderr()
        );
        assert!(Instant::now() < deadline, "{}", host.stderr());
        std::thread::sleep(Duration::from_millis(20));
    }
    let address: SocketAddr = host.event("listener.ready").unwrap()["address"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    wait_until_ready(address);
    let ready_lines = host.events().len();
    // Keep the pipeline busy: every request makes a server span. Readiness
    // never depends on the collector (TE §5).
    let until = Instant::now() + Duration::from_secs(5);
    while Instant::now() < until {
        assert_eq!(
            support::request(address, "GET", "/v1/nothing", "").status,
            404
        );
        assert_eq!(
            support::request(address, "GET", "/health/ready", "").status,
            200,
            "readiness follows the collector"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    // Request lines are the edge's own (debug for 4xx, TE §4).
    let after: Vec<Value> = host
        .events()
        .split_off(ready_lines)
        .into_iter()
        .filter(|e| e["event"] != "http.request.completed")
        .collect();
    // The first failure may come before readiness (the startup span and
    // logs), and the next warning of that signal waits a minute.
    let warnings = host
        .events()
        .iter()
        .filter(|e| e["event"] == "telemetry.export_failed")
        .count();
    assert!(warnings >= 1, "the failure is reported: {after:?}");
    assert!(warnings <= 3, "at most one warning per signal: {warnings}");
    assert!(after.len() <= 6, "{} lines in 5 s: {after:?}", after.len());
    assert!(
        !after.iter().any(|e| e["event"] == "dependency.log"),
        "exporter internals reached stderr: {after:?}"
    );
}
