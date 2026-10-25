//! A service declared `application_protocol = 2` whose guest only speaks
//! version 1 stops startup with `service_protocol_mismatch`, for `serve`
//! and for `merezha run` on PostgreSQL (AP §C.1, slice C1).
mod support;
use merezha_postgres::TestDatabase;
use std::{
    fs::{self, File},
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use support::Host;

const DATABASE_ENV: &str = "MEREZHA_PROTOCOL_TEST_DATABASE_URL";

/// The version 1 protocol component, declared as version 2.
fn runtime(dir: &Path) {
    fs::write(dir.join("protocol.wasm"), support::component()).unwrap();
    fs::write(
        dir.join("runtime.toml"),
        "version=1\n[limits]\nmemory_bytes=16777216\nfuel=1000000\nmax_input_bytes=65536\n\
         max_output_bytes=65536\ntimeout_ms=1000\nmax_concurrent_invocations=32\n\
         [[services]]\nid=\"protocol\"\npath=\"protocol.wasm\"\napplication_protocol=2\n",
    )
    .unwrap();
}
fn application(dir: &Path, storage: &str) {
    fs::write(
        dir.join("application.toml"),
        format!(
            "version=1\n{storage}\n[[commands]]\ntag=\"Add\"\nservice=\"protocol\"\naggregate=\"Counter\"\n"
        ),
    )
    .unwrap();
}

#[test]
fn serve_refuses_a_guest_that_does_not_speak_its_declared_protocol() {
    let dir = tempfile::tempdir().unwrap();
    runtime(dir.path());
    application(dir.path(), "");
    let log = dir.path().join("serve.log");
    let mut child = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args(["serve", "--manifest"])
        .arg(dir.path().join("runtime.toml"))
        .arg("--application")
        .arg(dir.path().join("application.toml"))
        .args(["--listen", "127.0.0.1:0"])
        .stdout(Stdio::null())
        .stderr(File::create(&log).unwrap())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "serve did not stop");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(status.code(), Some(2));
    let failed = fs::read_to_string(&log)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|event| event["event"] == "startup.failed")
        .expect("startup.failed event");
    assert_eq!(failed["code"], "service_protocol_mismatch");
}

#[test]
fn run_refuses_a_guest_that_does_not_speak_its_declared_protocol() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    runtime(dir.path());
    application(
        dir.path(),
        "[storage]\nbackend=\"postgres\"\napplication=\"protocol-run\"",
    );
    let config = dir.path().join("merezha.toml");
    fs::write(
        &config,
        format!(
            "[app]\nid = \"protocol-run\"\n[http]\nlisten = \"127.0.0.1:0\"\n\
             [database]\nurl = {{ env = \"{DATABASE_ENV}\" }}\nmigrate = true\npool_size = 4\n\
             [runtime]\nmanifest = \"runtime.toml\"\n[application]\npath = \"application.toml\"\n"
        ),
    )
    .unwrap();
    let failed = Host::spawn(&config, &[(DATABASE_ENV, test.url())], "run").failed();
    assert_eq!(failed["code"], "service_protocol_mismatch");
}

/// One HTTP/1.1 request with extra headers: status, raw headers and body.
fn call(
    address: std::net::SocketAddr,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, String, serde_json::Value) {
    use std::io::{Read, Write};
    let mut stream =
        std::net::TcpStream::connect_timeout(&address, Duration::from_secs(2)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut head = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
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
        serde_json::from_str(body).unwrap_or(serde_json::Value::Null),
    )
}

/// With the identity block, application commands carry a real session's
/// principal into `command_as`: a body principal is never trusted, another
/// actor is `actor_mismatch`, and a route open to any principal
/// (`access = "authenticated"`, AP-C2) answers.
#[test]
fn identity_sessions_reach_the_trusted_entry_points() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    const SECRET: &str = "a-process-test-identity-secret-of-32-bytes";
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("protocol.wasm"), support::component()).unwrap();
    fs::write(
        dir.path().join("runtime.toml"),
        "version=1\n[limits]\nmemory_bytes=16777216\nfuel=1000000\nmax_input_bytes=65536\n\
         max_output_bytes=65536\ntimeout_ms=1000\nmax_concurrent_invocations=32\n\
         [[services]]\nid=\"protocol\"\npath=\"protocol.wasm\"\n",
    )
    .unwrap();
    // With identity every route declares its access (AP §C.4).
    fs::write(
        dir.path().join("application.toml"),
        "version=2\n[[commands]]\ntag=\"Add\"\nservice=\"protocol\"\naggregate=\"Counter\"\n\
         access=\"authenticated\"\n",
    )
    .unwrap();
    let config = dir.path().join("merezha.toml");
    fs::write(
        &config,
        format!(
            "[app]\nid = \"protocol-identity\"\n[http]\nlisten = \"127.0.0.1:0\"\n\
             [database]\nurl = {{ env = \"{DATABASE_ENV}\" }}\nmigrate = true\n\
             [runtime]\nmanifest = \"runtime.toml\"\n[application]\npath = \"application.toml\"\n\
             [modules.identity]\napp_origin = \"http://localhost:5173\"\n\
             secret_key = {{ env = \"IDENTITY_SECRET\" }}\ndev_print_links = true\n\
             [modules.identity.password]\nargon2_memory_kib = 19456\n"
        ),
    )
    .unwrap();
    let mut host = Host::spawn(
        &config,
        &[(DATABASE_ENV, test.url()), ("IDENTITY_SECRET", SECRET)],
        "identity",
    );
    let address = host.ready();
    let origin = ("Origin", "http://localhost:5173");
    let (status, _, _) = call(
        address,
        "/v1/identity/registrations",
        &[origin],
        r#"{"email":"ada@example.com","password":"correct horse battery staple"}"#,
    );
    assert_eq!(status, 202);
    let deadline = Instant::now() + Duration::from_secs(10);
    let link = loop {
        if let Some(event) = host
            .events()
            .into_iter()
            .find(|e| e["event"] == "identity.dev_link")
        {
            break event["link"].as_str().unwrap().to_owned();
        }
        assert!(Instant::now() < deadline, "identity.dev_link on stderr");
        std::thread::sleep(Duration::from_millis(20));
    };
    let token = link.split_once("#token=").unwrap().1;
    let (status, headers, body) = call(
        address,
        "/v1/identity/email-verifications/confirm",
        &[origin],
        &serde_json::json!({ "token": token }).to_string(),
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
    let csrf = body["csrf_token"].as_str().unwrap().to_owned();
    let session = [origin, ("Cookie", &cookie), ("X-CSRF-Token", &csrf)];
    let command = |metadata: serde_json::Value| {
        serde_json::json!({"tag":"Add","stream":"one","payload":{"amount":1},"metadata":metadata})
            .to_string()
    };
    let code = |reply: (u16, String, serde_json::Value)| {
        (
            reply.0,
            reply.2["error"]["code"].as_str().map(str::to_owned),
        )
    };
    assert_eq!(
        code(call(
            address,
            "/v1/application/commands",
            &session,
            &command(serde_json::json!({
                "principal": {"subject": "usr_root", "kind": "user", "roles": ["admin"]}
            })),
        )),
        (422, Some("principal_not_trusted".into()))
    );
    assert_eq!(
        code(call(
            address,
            "/v1/application/commands",
            &session,
            &command(serde_json::json!({"actor": "usr_someone_else"})),
        )),
        (403, Some("actor_mismatch".into()))
    );
    assert_eq!(
        code(call(
            address,
            "/v1/application/commands",
            &session,
            &command(serde_json::json!({})),
        )),
        (200, None),
    );
    host.stop();
}
