use axum::{
    Router,
    body::{Body, to_bytes},
    http::Request,
};
use merezha_application::{AllowAll, Application, Guest, GuestSession, config::Config};
use merezha_domain::{Event, Failure, Identity, Operation, Reply};
use merezha_runtime::Runtime;
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;
#[derive(Clone)]
struct GuestFixture;
impl Guest for GuestFixture {
    fn services(&self) -> Result<Vec<String>, Failure> {
        Ok(vec!["domain".into()])
    }
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
        Ok(Box::new(self.clone()))
    }
}
impl GuestSession for GuestFixture {
    fn invoke(&self, _: &str, operation: Operation) -> Result<Reply, Failure> {
        Ok(match operation {
            Operation::Validate { payload, .. } => {
                merezha_domain::validate_payload::<u64>(payload)?
            }
            Operation::Decide { payload, .. } => Reply::Decided {
                events: vec![Event {
                    tag: Identity::new("Added").unwrap(),
                    schema_version: 1,
                    payload,
                }],
            },
            Operation::Project { state, events, .. } => Reply::Projected {
                state: json!(state.as_u64().unwrap() + events.len() as u64),
            },
            Operation::Query { state, .. } => Reply::Queried { value: state },
        })
    }
}
fn fixture() -> (tempfile::TempDir, std::path::PathBuf, Router) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("echo.wasm"),
        wat::parse_str(include_str!("../../runtime/tests/fixtures/echo.wat")).unwrap(),
    )
    .unwrap();
    let path = dir.path().join("runtime.toml");
    std::fs::write(
        &path,
        r#"version=1
[limits]
memory_bytes=16777216
fuel=1000000
max_input_bytes=65536
max_output_bytes=65536
timeout_ms=1000
max_concurrent_invocations=4
[[services]]
id="echo"
path="echo.wasm"
"#,
    )
    .unwrap();
    let runtime = Runtime::load(&path).unwrap();
    let config = Config::parse(
        r#"version=1
[[commands]]
tag="Add"
service="domain"
aggregate="Counter"
[[queries]]
tag="Total"
service="domain"
projection="total"
[[projections]]
name="total"
service="domain"
initial=0
"#,
    )
    .unwrap();
    let app = Application::in_memory(config, Arc::new(GuestFixture), Arc::new(AllowAll)).unwrap();
    (dir, path, merezha_cli::app_with_application(runtime, app))
}
async fn post(app: &Router, path: &str, body: String) -> (u16, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 1_048_576).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}
#[tokio::test]
async fn application_http_preserves_raw_routes_and_dispatches_commands_queries_and_projection_controls()
 {
    let (_dir, _path, app) = fixture();
    let cmd=json!({"tag":"Add","stream":"one","payload":1,"idempotency_key":"key","metadata":{"message_id":"m1","correlation_id":"c1","actor":"alice"}}).to_string();
    let (status, receipt) = post(&app, "/v1/application/commands", cmd.clone()).await;
    assert_eq!(status, 200);
    assert_eq!(receipt["version"], 1);
    assert_eq!(post(&app, "/v1/application/commands", cmd).await.1, receipt);
    let query =
        json!({"tag":"Total","payload":0,"metadata":{"message_id":"m2","correlation_id":"c1"}})
            .to_string();
    assert_eq!(
        post(&app, "/v1/application/queries", query.clone()).await.1,
        json!({"value":0,"checkpoint":0})
    );
    assert_eq!(
        post(
            &app,
            "/v1/application/projections/total/catch-up",
            String::new()
        )
        .await
        .1,
        json!({"state":1,"checkpoint":1})
    );
    assert_eq!(
        post(&app, "/v1/application/queries", query).await.1,
        json!({"value":1,"checkpoint":1})
    );
    assert_eq!(
        post(
            &app,
            "/v1/application/projections/total/rebuild",
            String::new()
        )
        .await
        .1,
        json!({"state":1,"checkpoint":1})
    );
    assert_eq!(
        post(&app, "/v1/application/commands", "{}".into()).await.0,
        422
    );
    assert_eq!(
        post(&app, "/v1/application/commands", " ".repeat(65_537))
            .await
            .0,
        413
    );
    assert_eq!(
        post(
            &app,
            "/v1/application/projections/total/rebuild",
            "{}".into()
        )
        .await
        .0,
        413
    );
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/services/echo/invoke")
                .body(Body::from("opaque"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        to_bytes(response.into_body(), 100).await.unwrap().as_ref(),
        b"opaque"
    );
}
#[test]
fn invalid_application_configuration_fails_before_listener_start() {
    let (dir, path, _app) = fixture();
    let config = dir.path().join("invalid.toml");
    std::fs::write(&config, "version=1\nunknown=true").unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args([
            "serve",
            "--manifest",
            path.to_str().unwrap(),
            "--application",
            config.to_str().unwrap(),
            "--listen",
            "127.0.0.1:0",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("invalid_config"));
    assert!(!stderr.contains("listener.ready"));
}
