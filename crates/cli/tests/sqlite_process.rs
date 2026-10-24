//! Synthetic no-import protocol component, real CLI processes and loopback HTTP.
mod support;
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use support::component;

struct Host {
    child: Child,
    log: PathBuf,
}
impl Host {
    fn spawn(dir: &Path, config: &Path, name: &str) -> Self {
        let log = dir.join(format!("{name}.log"));
        let child = Command::new(env!("CARGO_BIN_EXE_merezha"))
            .args(["serve", "--manifest"])
            .arg(dir.join("runtime.toml"))
            .arg("--application")
            .arg(config)
            .args(["--listen", "127.0.0.1:0"])
            .stdout(Stdio::null())
            .stderr(File::create(&log).unwrap())
            .spawn()
            .unwrap();
        Self { child, log }
    }
    fn ready(&mut self) -> SocketAddr {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let text = fs::read_to_string(&self.log).unwrap();
            for line in text.lines() {
                if let Ok(value) = serde_json::from_str::<Value>(line)
                    && value["event"] == "listener.ready"
                {
                    return value["address"].as_str().unwrap().parse().unwrap();
                }
            }
            assert!(
                self.child.try_wait().unwrap().is_none(),
                "host exited before readiness: {text}"
            );
            assert!(Instant::now() < deadline, "readiness timeout: {text}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn failed(&mut self, code: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert_eq!(status.code(), Some(2));
                assert!(fs::read_to_string(&self.log).unwrap().contains(code));
                return;
            }
            assert!(Instant::now() < deadline, "host failure timeout");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn kill(&mut self) {
        self.child.kill().unwrap();
        assert!(!self.child.wait().unwrap().success());
    }
    fn stop(&mut self) {
        assert!(
            Command::new("kill")
                .args(["-TERM", &self.child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success(), "graceful host exit: {status}");
                return;
            }
            assert!(Instant::now() < deadline, "host shutdown timeout");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
impl Drop for Host {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn post(address: SocketAddr, path: &str, body: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    // One write, as in `support::request`: head and body arrive together.
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    let status = headers.split_whitespace().nth(1).unwrap().parse().unwrap();
    (
        status,
        serde_json::from_str(body).unwrap_or_else(|_| json!(body)),
    )
}
fn config(dir: &Path, database: &Path, initialize: bool, name: &str) -> PathBuf {
    let path = dir.join(format!("{name}.toml"));
    let database = database.to_str().unwrap();
    let text = format!(
        r#"version=1
[storage]
backend="sqlite"
path={}
application="counter-v1"
initialize={initialize}
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
        serde_json::to_string(database).unwrap()
    );
    fs::write(&path, text).unwrap();
    path
}
#[test]
fn owner_host_mixed_2_4_8_clients_wait_for_internal_sql_instead_of_storage_busy() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    fs::write(dir.join("protocol.wasm"), component()).unwrap();
    fs::write(
        dir.join("runtime.toml"),
        r#"version=1
[limits]
memory_bytes=16777216
fuel=1000000
max_input_bytes=65536
max_output_bytes=65536
timeout_ms=1000
max_concurrent_invocations=16
[[services]]
id="protocol"
path="protocol.wasm"
"#,
    )
    .unwrap();
    // Native and application admission both allow 16. These 2/4/8 clients
    // stay inside that configured budget; no global default is enlarged.
    let db = dir.join("application.db");
    let init = config(dir, &db, true, "initialize");
    let mut host = Host::spawn(dir, &init, "mixed-owner-host");
    let address = host.ready();
    let query =
        json!({"tag":"Total","payload":{},"metadata":{"message_id":"q","correlation_id":"c"}})
            .to_string();
    let mut accepted = Vec::new();
    for clients in [2, 4, 8] {
        let results = std::thread::scope(|scope| {
            let mut workers = Vec::new();
            for client in 0..clients {
                let query = &query;
                workers.push(scope.spawn(move || {
                    let mut replies = Vec::new();
                    for request in 0..20 {
                        if request % 2 == 0 {
                            let command = json!({
                                "tag": "Add",
                                "stream": format!("c{clients}-{client}"),
                                "payload": {"amount": 1},
                                "idempotency_key": format!("k{clients}-{client}-{request}"),
                                "metadata": {"message_id": "m", "correlation_id": "c", "actor": "alice"}
                            }).to_string();
                            replies.push((true, post(address, "/v1/application/commands", &command)));
                        } else {
                            replies.push((false, post(address, "/v1/application/queries", query)));
                        }
                    }
                    replies
                }));
            }
            workers
                .into_iter()
                .flat_map(|w| w.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(results.len(), clients * 20);
        for (command, (status, body)) in results {
            assert_eq!(status, 200, "{clients} admitted clients: {body}");
            if command {
                accepted.push(body["position"].as_u64().unwrap());
            } else {
                assert_eq!(body["checkpoint"], 0, "queries must not catch up");
            }
        }
        eprintln!(
            "owner HTTP contention: clients={clients}, replies={}, all HTTP200",
            clients * 20
        );
    }
    accepted.sort_unstable();
    assert_eq!(accepted, (1..=140).collect::<Vec<_>>());
    host.stop();
}
#[test]
fn host_process_restart_owner_exclusion_crash_and_closed_backup_restore() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    fs::write(dir.join("protocol.wasm"), component()).unwrap();
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
    let db = dir.join("application.db");
    let init = config(dir, &db, true, "initialize");
    let reopen = config(dir, &db, false, "reopen");
    let mut host = Host::spawn(dir, &init, "initial-host");
    let address = host.ready();
    let command=json!({"tag":"Add","stream":"one","payload":{"amount":1},"idempotency_key":"key","metadata":{"message_id":"m1","correlation_id":"c1","actor":"alice"}}).to_string();
    let receipt = post(address, "/v1/application/commands", &command);
    assert_eq!(receipt.0, 200);
    assert_eq!(receipt.1["position"], 1);
    assert_eq!(
        post(address, "/v1/application/projections/total/catch-up", "").1,
        json!({"state":1,"checkpoint":1})
    );
    assert_eq!(
        post(address, "/v1/services/echo/invoke", "opaque").1,
        json!("opaque")
    );
    let mut contender = Host::spawn(dir, &reopen, "contender");
    contender.failed("storage_owned");
    drop(contender);
    host.kill();
    drop(host);
    let mut host = Host::spawn(dir, &reopen, "restarted-host");
    let address = host.ready();
    assert_eq!(post(address, "/v1/application/commands", &command), receipt);
    let query =
        json!({"tag":"Total","payload":{},"metadata":{"message_id":"m2","correlation_id":"c1"}})
            .to_string();
    assert_eq!(
        post(address, "/v1/application/queries", &query).1,
        json!({"value":1,"checkpoint":1})
    );
    let mut collision: Value = serde_json::from_str(&command).unwrap();
    collision["stream"] = json!("other");
    assert_eq!(
        post(address, "/v1/application/commands", &collision.to_string()).1["error"]["code"],
        "idempotency_mismatch"
    );
    let mut new: Value = serde_json::from_str(&command).unwrap();
    new["idempotency_key"] = json!("next");
    let second = post(address, "/v1/application/commands", &new.to_string());
    assert_eq!(
        (second.1["version"].as_u64(), second.1["position"].as_u64()),
        (Some(2), Some(2))
    );
    host.stop();
    drop(host);
    let backup = dir.join("closed-backup.db");
    fs::copy(&db, &backup).unwrap();
    let restored = dir.join("restored.db");
    fs::copy(&backup, &restored).unwrap();
    let restore = config(dir, &restored, false, "restore");
    let mut host = Host::spawn(dir, &restore, "restored-host");
    let address = host.ready();
    assert_eq!(post(address, "/v1/application/commands", &command), receipt);
    assert_eq!(
        post(address, "/v1/application/commands", &new.to_string()),
        second
    );
    assert_eq!(
        post(address, "/v1/application/queries", &query).1,
        json!({"value":1,"checkpoint":1})
    );
    assert_eq!(
        post(address, "/v1/application/projections/total/rebuild", "").1["checkpoint"],
        2
    );
    host.stop();
    drop(host);
    // This component emits fixed protocol responses. Arithmetic and failed
    // projection recovery are separately exercised through application guests.
    eprintln!(
        "HTTP hosts: initial SIGKILL, restart SIGTERM exit=0, restore SIGTERM exit=0; contender exit=2; receipts positions=1,2; restored checkpoint=1 then rebuild=2"
    );
}
