use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use merezha_runtime::Runtime;
use tower::ServiceExt;
const MANIFEST: &str = r#"version=1
[limits]
memory_bytes=16777216
fuel=10000000
max_input_bytes=8
max_output_bytes=8
timeout_ms=100
max_concurrent_invocations=1
[[services]]
id="echo"
path="echo.wasm"
[[pipelines]]
id="twice"
steps=["echo","echo"]
"#;
fn fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("echo.wasm"),
        wat::parse_str(include_str!("../../runtime/tests/fixtures/echo.wat")).unwrap(),
    )
    .unwrap();
    let path = dir.path().join("runtime.toml");
    std::fs::write(&path, MANIFEST).unwrap();
    (dir, path)
}
#[tokio::test]
async fn http_exercises_catalog_invocation_limits_utf8_and_atomic_reload() {
    let (_dir, path) = fixture();
    let rt = Runtime::load(&path).unwrap();
    let app = merezha_cli::app(rt);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    for (uri, body, status, expected) in [
        (
            "/v1/pipelines/twice/invoke",
            " é ".as_bytes().to_vec(),
            200,
            " é ",
        ),
        (
            "/v1/services/echo/invoke",
            b"123456789".to_vec(),
            413,
            "input_limit",
        ),
        ("/v1/services/echo/invoke", vec![0xff], 400, "invalid_input"),
        ("/v1/services/missing/invoke", vec![], 404, "not_found"),
    ] {
        let r = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status().as_u16(), status);
        if status == 200 {
            assert_eq!(r.headers()["x-merezha-generation"], "1");
        }
        let bytes = to_bytes(r.into_body(), 4096).await.unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        if status == 200 {
            assert_eq!(text, expected);
        } else {
            assert!(text.contains(expected));
        }
    }
    let r = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/services")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(r.into_body(), 4096).await.unwrap();
    let c: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        c,
        serde_json::json!({"generation":1,"services":["echo"],"pipelines":["twice"]})
    );
    std::fs::write(&path, "broken").unwrap();
    let r = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/reload")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    let r = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/services/echo/invoke")
                .body(Body::from("still-ok"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.headers()["x-merezha-generation"], "1");
    std::fs::write(&path, MANIFEST).unwrap();
    let r = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/reload")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(r.into_body(), 4096).await.unwrap(),
        "{\"generation\":2}"
    );
}

struct Process(std::process::Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[test]
fn cli_binds_ephemeral_loopback_serves_real_http_and_logs_metadata_only() {
    use std::io::{BufRead, Read, Write};
    let (_dir, path) = fixture();
    let mut process = Process(
        std::process::Command::new(env!("CARGO_BIN_EXE_merezha"))
            .args(["serve", "--manifest"])
            .arg(&path)
            .args(["--listen", "127.0.0.1:0"])
            .stderr(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut logs = std::io::BufReader::new(process.0.stderr.take().unwrap());
    let mut first = String::new();
    logs.read_line(&mut first).unwrap();
    let event: serde_json::Value = serde_json::from_str(&first).unwrap();
    assert_eq!(event["event"], "listener.ready");
    let address = event["address"].as_str().unwrap();
    let mut socket = std::net::TcpStream::connect(address).unwrap();
    socket
        .set_read_timeout(Some(std::time::Duration::from_secs(2)))
        .unwrap();
    socket.write_all(b"POST /v1/pipelines/twice/invoke HTTP/1.1\r\nHost: localhost\r\nContent-Length: 6\r\nConnection: close\r\n\r\ns3cr3t").unwrap();
    let mut response = String::new();
    socket.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.ends_with("s3cr3t"));
    // An oversized body must be rejected before buffering the complete body,
    // even when the client keeps its write side open and stops transmitting.
    let mut socket = std::net::TcpStream::connect(address).unwrap();
    socket
        .set_read_timeout(Some(std::time::Duration::from_secs(2)))
        .unwrap();
    socket.write_all(b"POST /v1/services/echo/invoke HTTP/1.1\r\nHost: localhost\r\nContent-Length: 10000000\r\nConnection: close\r\n\r\n123456789").unwrap();
    let mut oversized = String::new();
    socket.read_to_string(&mut oversized).unwrap();
    assert!(oversized.starts_with("HTTP/1.1 413"), "{oversized}");
    assert!(oversized.contains("input_limit"));
    // A stalled body must not hold admission indefinitely.
    let mut socket = std::net::TcpStream::connect(address).unwrap();
    socket
        .set_read_timeout(Some(std::time::Duration::from_secs(2)))
        .unwrap();
    socket.write_all(b"POST /v1/services/echo/invoke HTTP/1.1\r\nHost: localhost\r\nContent-Length: 6\r\nConnection: close\r\n\r\na").unwrap();
    let mut slow = String::new();
    socket.read_to_string(&mut slow).unwrap();
    assert!(slow.starts_with("HTTP/1.1 504"));
    #[cfg(unix)]
    {
        assert!(
            std::process::Command::new("kill")
                .args(["-TERM", &process.0.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
    }
    #[cfg(not(unix))]
    {
        process.0.kill().unwrap();
    }
    assert!(process.0.wait().unwrap().success());
    let mut rest = String::new();
    logs.read_to_string(&mut rest).unwrap();
    assert!(!rest.contains("s3cr3t"));
    let events: Vec<serde_json::Value> = rest
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert!(
        events
            .iter()
            .any(|e| e["event"] == "invocation.completed" && e["input_bytes"] == 6)
    );
    assert!(events.iter().any(|e| e["event"] == "listener.stopped"));
    let mut stdout = String::new();
    process
        .0
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut stdout)
        .unwrap();
    assert!(stdout.is_empty());
}
#[test]
fn cli_refuses_public_listener_before_opening_manifest() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args([
            "serve",
            "--manifest",
            "missing.toml",
            "--listen",
            "0.0.0.0:0",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("invalid_arguments")
    );
}
