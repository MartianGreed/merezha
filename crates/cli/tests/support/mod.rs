//! Shared fixtures for process tests: the protocol component, a `merezha
//! run` host and a loopback HTTP client.
#![allow(dead_code, reason = "each test crate uses a subset")]
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

/// Bound on startup, failed startup and graceful stop.
const PROCESS_BOUND: Duration = Duration::from_secs(20);
/// Bound on `/health/ready` reaching an expected status. Readiness may lag
/// `listener.ready`: the jobs runtime reports ready only after its first
/// subscription sync, which runs in the background.
pub const READINESS_BOUND: Duration = Duration::from_secs(10);

/// A `merezha run --config` process logging JSON events to `<name>.log`
/// beside its configuration. Killed on drop.
pub struct Host {
    pub child: Child,
    pub log: PathBuf,
}
impl Host {
    pub fn spawn(config: &Path, env: &[(&str, &str)], name: &str) -> Self {
        let log = config.with_file_name(format!("{name}.log"));
        let mut command = Command::new(env!("CARGO_BIN_EXE_merezha"));
        command
            .args(["run", "--config"])
            .arg(config)
            // No load balancer to wait for: stop at once on SIGTERM unless
            // a test sets the delay itself.
            .env("MEREZHA_HTTP__DRAIN_DELAY_MS", "0")
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
    pub fn events(&self) -> Vec<Value> {
        fs::read_to_string(&self.log)
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect()
    }
    /// Wait for `listener.ready` and return the bound address. The early
    /// listener answers `/health/*` from then on and every other route
    /// `503 starting` until startup completes.
    pub fn bound(&mut self) -> SocketAddr {
        let deadline = Instant::now() + PROCESS_BOUND;
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
                "host exited before listening: {text}"
            );
            assert!(Instant::now() < deadline, "listening timeout: {text}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    /// Wait for `listener.ready`, then for `/health/startup` to answer 200:
    /// startup completed and every route is mounted. `/health/ready` may
    /// still answer 503.
    pub fn listening(&mut self) -> SocketAddr {
        let address = self.bound();
        let deadline = Instant::now() + PROCESS_BOUND;
        while request(address, "GET", "/health/startup", "").status != 200 {
            let text = fs::read_to_string(&self.log).unwrap();
            assert!(
                self.child.try_wait().unwrap().is_none(),
                "host exited during startup: {text}"
            );
            assert!(Instant::now() < deadline, "startup timeout: {text}");
            std::thread::sleep(Duration::from_millis(20));
        }
        address
    }
    /// Wait for `listener.ready`, then for `/health/ready` to answer 200.
    pub fn ready(&mut self) -> SocketAddr {
        let address = self.listening();
        self.wait_for_readiness(address, 200);
        address
    }
    /// Poll `/health/ready` until it answers `status`, within
    /// [`READINESS_BOUND`]. The failure names the last reply and the failing
    /// checks of the last `health.not_ready` event.
    pub fn wait_for_readiness(&self, address: SocketAddr, status: u16) {
        let deadline = Instant::now() + READINESS_BOUND;
        loop {
            let reply = request(address, "GET", "/health/ready", "");
            if reply.status == status {
                return;
            }
            if Instant::now() >= deadline {
                let not_ready = self
                    .events()
                    .into_iter()
                    .rev()
                    .find(|e| e["event"] == "health.not_ready");
                panic!(
                    "/health/ready did not answer {status} within {READINESS_BOUND:?}: last reply {} {}, checks failing at the last health.not_ready: {}",
                    reply.status,
                    reply.body,
                    not_ready.map_or_else(|| "none logged".to_owned(), |e| e["checks"].to_string())
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    /// Wait for exit code 2 and return the `startup.failed` event.
    pub fn failed(&mut self) -> Value {
        let deadline = Instant::now() + PROCESS_BOUND;
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
    /// SIGTERM, then require a graceful exit 0.
    pub fn stop(&mut self) {
        assert!(
            Command::new("kill")
                .args(["-TERM", &self.child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let deadline = Instant::now() + PROCESS_BOUND;
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

pub struct Reply {
    pub status: u16,
    /// Lowercased.
    pub headers: String,
    /// The JSON body, or the raw text as a JSON string.
    pub body: Value,
}
/// One request on its own `Connection: close` connection, head and body
/// sent in one write. The host may answer before reading a body (an unknown
/// service, a denied message) and then close; a body byte still unread at
/// that close makes the kernel reset the connection, which can discard the
/// reply. One write lands the body with the head, so the server has buffered
/// it by the time it parses the head.
pub fn request(address: SocketAddr, method: &str, path: &str, body: &str) -> Reply {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    Reply {
        status: headers.split_whitespace().nth(1).unwrap().parse().unwrap(),
        headers: headers.to_ascii_lowercase(),
        body: serde_json::from_str(body).unwrap_or_else(|_| json!(body)),
    }
}

/// A no-import component answering the application protocol with fixed
/// replies: `Add` decides one `Added` event, projections count to 1 and the
/// `Total` query returns 1.
pub fn component() -> Vec<u8> {
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
