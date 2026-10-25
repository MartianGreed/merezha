//! The identity block through the `merezha` binary: `merezha run` refuses
//! email flows without a delivery path (acceptance 12), serves the block
//! with printed links on loopback, installs the authenticator and the RBAC
//! `Authorizer` in the host; `merezha identity platform-admin`; and `merezha
//! identity password-benchmark`.
mod support;
use merezha_postgres::TestDatabase;
use serde_json::{Value, json};
use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};
use support::Host;

const SECRET: &str = "a-process-test-identity-secret-of-32-bytes";

fn project(dir: &Path, identity: &str) -> PathBuf {
    fs::write(
        dir.join("echo.wasm"),
        wat::parse_str(include_str!("../../runtime/tests/fixtures/echo.wat")).unwrap(),
    )
    .unwrap();
    fs::write(
        dir.join("runtime.toml"),
        "version=1\n[limits]\nmemory_bytes=16777216\nfuel=1000000\nmax_input_bytes=65536\n\
         max_output_bytes=65536\ntimeout_ms=1000\nmax_concurrent_invocations=4\n\
         [[services]]\nid=\"echo\"\npath=\"echo.wasm\"\n",
    )
    .unwrap();
    let path = dir.join("merezha.toml");
    fs::write(
        &path,
        format!(
            "[app]\nid = \"identity-run\"\n[http]\nlisten = \"127.0.0.1:0\"\n\
             [database]\nurl = {{ env = \"IDENTITY_TEST_DATABASE_URL\" }}\nmigrate = true\n\
             [runtime]\nmanifest = \"runtime.toml\"\n\
             [modules.identity]\napp_origin = \"http://localhost:5173\"\n\
             secret_key = {{ env = \"IDENTITY_SECRET\" }}\n{identity}\n\
             [modules.identity.password]\nargon2_memory_kib = 19456\n"
        ),
    )
    .unwrap();
    path
}

struct Reply {
    status: u16,
    headers: String,
    body: Value,
}
fn call(
    address: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> Reply {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
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
    Reply {
        status: headers.split_whitespace().nth(1).unwrap().parse().unwrap(),
        headers: headers.to_owned(),
        body: serde_json::from_str(body).unwrap_or_else(|_| json!(body)),
    }
}

#[test]
fn run_refuses_email_flows_without_a_delivery_path() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path(), "");
    let mut host = Host::spawn(
        &config,
        &[
            ("IDENTITY_TEST_DATABASE_URL", test.url()),
            ("IDENTITY_SECRET", SECRET),
        ],
        "refused",
    );
    let failed = host.failed();
    assert_eq!(failed["code"], "module_start_failed");
    let start = host
        .events()
        .into_iter()
        .find(|e| e["event"] == "module.start_failed")
        .expect("module.start_failed");
    assert_eq!(
        (start["module"].as_str(), start["code"].as_str()),
        (Some("identity"), Some("notifications_required"))
    );
}

#[test]
fn run_serves_identity_with_printed_links_on_loopback() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path(), "dev_print_links = true");
    let mut host = Host::spawn(
        &config,
        &[
            ("IDENTITY_TEST_DATABASE_URL", test.url()),
            ("IDENTITY_SECRET", SECRET),
        ],
        "dev",
    );
    let address = host.ready();
    let origin = [("Origin", "http://localhost:5173")];
    let registered = call(
        address,
        "POST",
        "/v1/identity/registrations",
        &origin,
        &json!({"email": "ada@example.com", "password": "correct horse battery staple"})
            .to_string(),
    );
    assert_eq!(registered.status, 202, "{}", registered.body);
    assert!(
        registered
            .headers
            .to_ascii_lowercase()
            .contains("cache-control: no-store")
    );
    // The stderr writer is asynchronous: wait for the line.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let link = loop {
        if let Some(link) = host
            .events()
            .into_iter()
            .find(|e| e["event"] == "identity.dev_link")
        {
            break link;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "identity.dev_link on stderr"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(link["purpose"], "email_verification");
    let token = link["link"]
        .as_str()
        .unwrap()
        .split_once("#token=")
        .unwrap()
        .1
        .to_owned();
    assert!(
        link["link"]
            .as_str()
            .unwrap()
            .starts_with("http://localhost:5173/auth/verify-email#token=")
    );
    let confirmed = call(
        address,
        "POST",
        "/v1/identity/email-verifications/confirm",
        &origin,
        &json!({"token": token}).to_string(),
    );
    assert_eq!(confirmed.status, 201, "{}", confirmed.body);
    // The loopback development cookie: no prefix, no Secure.
    let cookie_line = confirmed
        .headers
        .lines()
        .find(|l| {
            l.to_ascii_lowercase()
                .starts_with("set-cookie: merezha_session=")
        })
        .expect("the development cookie");
    assert!(!cookie_line.contains("Secure"), "{cookie_line}");
    let cookie = cookie_line
        .split_once(": ")
        .unwrap()
        .1
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let session = call(
        address,
        "GET",
        "/v1/identity/session",
        &[("Cookie", &cookie)],
        "",
    );
    assert_eq!(session.status, 200, "{}", session.body);
    // The edge authenticates every route and the RBAC `Authorizer`
    // decides: a user without `system.status.read` is refused, a platform
    // admin (granted through the CLI) breaks glass, audited.
    let status = call(
        address,
        "GET",
        "/v1/admin/status",
        &[("Cookie", &cookie)],
        "",
    );
    assert_eq!(
        (status.status, status.body["error"]["code"].as_str()),
        (403, Some("unauthorized"))
    );
    let anonymous = call(address, "GET", "/v1/admin/status", &[], "");
    assert_eq!(anonymous.status, 401);
    let admin = |action: &str, email: &str| {
        let output = Command::new(env!("CARGO_BIN_EXE_merezha"))
            .args(["identity", "platform-admin", action, email, "--config"])
            .arg(&config)
            .env("IDENTITY_TEST_DATABASE_URL", test.url())
            .env("IDENTITY_SECRET", SECRET)
            .output()
            .unwrap();
        let report: Value = serde_json::from_slice(&output.stdout).unwrap();
        (output.status.code(), report)
    };
    let (code, report) = admin("grant", "ada@example.com");
    assert_eq!(
        (code, report["action"].as_str()),
        (Some(0), Some("granted")),
        "{report}"
    );
    assert!(report["user_id"].as_str().unwrap().starts_with("usr_"));
    let (code, report) = admin("grant", "ada@example.com");
    assert_eq!(
        (code, report["action"].as_str()),
        (Some(0), Some("unchanged"))
    );
    let (code, report) = admin("grant", "nobody@example.com");
    assert_eq!(
        (code, report["error"]["code"].as_str()),
        (Some(2), Some("not_found"))
    );
    let status = call(
        address,
        "GET",
        "/v1/admin/status",
        &[("Cookie", &cookie)],
        "",
    );
    assert_eq!(status.status, 200, "{}", status.body);
    let projections = call(
        address,
        "GET",
        "/v1/identity/me/permissions",
        &[("Cookie", &cookie)],
        "",
    );
    assert_eq!(projections.body["application"]["break_glass"], true);
    let (code, report) = admin("revoke", "ada@example.com");
    assert_eq!(
        (code, report["action"].as_str()),
        (Some(0), Some("revoked"))
    );
    let status = call(
        address,
        "GET",
        "/v1/admin/status",
        &[("Cookie", &cookie)],
        "",
    );
    assert_eq!(status.status, 403, "revoked on the next request");
    // The raw runtime endpoints are not mounted beside an authenticator.
    for path in ["/v1/services", "/health"] {
        assert_eq!(call(address, "GET", path, &[], "").status, 404, "{path}");
    }
    let reload = call(
        address,
        "POST",
        "/v1/reload",
        &[("Origin", "http://localhost:5173")],
        "{}",
    );
    assert_eq!(reload.status, 404);
    let bogus = call(
        address,
        "GET",
        "/v1/identity/session",
        &[("Authorization", "Bearer mzs_nope")],
        "",
    );
    assert_eq!(bogus.status, 401);
    let text = fs::read_to_string(&host.log).unwrap();
    assert!(!text.contains(SECRET), "the secret key is never logged");
    assert!(
        !text.contains(&cookie[16..]),
        "the session token is never logged"
    );
    host.stop();
}

#[test]
fn password_benchmark_reports_timings() {
    let output = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args([
            "identity",
            "password-benchmark",
            "--memory-kib",
            "19456",
            "--samples",
            "2",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["status"], "ok");
    assert_eq!(report["algorithm"], "argon2id");
    assert_eq!(
        (
            report["argon2_memory_kib"].as_u64(),
            report["samples"].as_u64()
        ),
        (Some(19456), Some(2))
    );
    assert!(report["median_ms"].as_f64().unwrap() > 0.0);
    // The project's parameters, overridable.
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path(), "");
    let output = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args([
            "identity",
            "password-benchmark",
            "--samples",
            "1",
            "--iterations",
            "3",
            "--config",
        ])
        .arg(&config)
        .env(
            "IDENTITY_TEST_DATABASE_URL",
            "postgres://app@127.0.0.1:1/app",
        )
        .env("IDENTITY_SECRET", SECRET)
        .output()
        .unwrap();
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(output.status.code(), Some(0), "{report}");
    assert_eq!(
        (
            report["argon2_memory_kib"].as_u64(),
            report["argon2_iterations"].as_u64()
        ),
        (Some(19456), Some(3))
    );
    let output = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args(["identity", "password-benchmark", "--memory-kib", "8"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["error"]["code"], "out_of_range");
    let output = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args(["identity", "nothing"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
}
