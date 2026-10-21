//! Synthetic no-import protocol component, real CLI processes and loopback HTTP.
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

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
    write!(stream,"POST {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",body.len()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    let status = headers.split_whitespace().nth(1).unwrap().parse().unwrap();
    (
        status,
        serde_json::from_str(body).unwrap_or_else(|_| json!(body)),
    )
}
fn component() -> Vec<u8> {
    let replies = [
        (
            "validate",
            json!({"kind":"validated","payload":{"amount":1}}),
        ),
        (
            "decide",
            json!({"kind":"decided","events":[{"tag":"Added","schema_version":1,"payload":{"amount":1}}]}),
        ),
        ("project", json!({"kind":"projected","state":1})),
        ("query", json!({"kind":"queried","value":1})),
    ];
    let mut data = String::new();
    let mut branches = String::new();
    let mut offset = 128;
    let prefix = r#"{"version":1,"body":{"op":""#;
    for (op, reply) in replies {
        let response = json!({"version":1,"outcome":{"status":"ok","reply":reply}}).to_string();
        let escaped: String = response.bytes().map(|b| format!("\\{b:02x}")).collect();
        data.push_str(&format!("(data (i32.const {offset}) \"{escaped}\")"));
        branches.push_str(&format!("local.get $p i32.const {} i32.add i32.load8_u i32.const {} i32.eq if i32.const 4 i32.const {offset} i32.store i32.const 8 i32.const {} i32.store end ",prefix.len(),op.as_bytes()[0],response.len()));
        offset += response.len() + 1;
    }
    wat::parse_str(format!(r#"(component (core module $m
      (memory (export "memory") 2) {data}
      (func (export "realloc") (param i32 i32 i32 i32) (result i32) i32.const 8192)
      (func (export "handle") (param $p i32) (param i32) (result i32)
        i32.const 0 i32.const 0 i32.store {branches} i32.const 0))
      (core instance $i (instantiate $m))
      (type $h (func (param "input" string) (result (result string (error string)))))
      (func $handle (type $h) (canon lift (core func $i "handle") (memory (core memory $i "memory")) (realloc (core func $i "realloc"))))
      (instance $handler (export "handle" (func $handle)))
      (export "merezha:service/handler@0.1.0" (instance $handler)))"#)).unwrap()
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
