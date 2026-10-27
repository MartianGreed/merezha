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
            Operation::Describe {} | Operation::React { .. } => {
                unreachable!("a version 1 fake is never described and never reacts")
            }
            Operation::Query { state, .. } => Reply::Queried { value: state },
        })
    }
}
fn application() -> Application {
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
    Application::in_memory(config, Arc::new(GuestFixture), Arc::new(AllowAll)).unwrap()
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
    assert_catalogued(&stderr);
}

/// Every JSON line a `merezha` process wrote to stderr is a catalog event at
/// a catalog level (TE §8).
fn assert_catalogued(stderr: &str) {
    if let Err(problems) = merezha_telemetry::testing::check_stderr_catalogued(stderr) {
        panic!("event catalog: {problems}");
    }
}

/// Allows application messages and decides permissions from the principal's
/// grant snapshot, as the identity block's authorizer will.
struct GrantPolicy;
impl merezha_application::Authorizer for GrantPolicy {
    fn authorize(
        &self,
        _: merezha_domain::MessageKind,
        _: &str,
        _: Option<&str>,
        _: &Value,
        _: &merezha_domain::Metadata,
    ) -> Result<(), Failure> {
        Ok(())
    }
}
impl merezha_platform::ports::Authorizer for GrantPolicy {
    fn check(
        &self,
        principal: Option<&merezha_platform::Principal>,
        permission: &Identity,
        scope: &merezha_platform::ResourceScope,
    ) -> Result<merezha_platform::ports::Allowed, merezha_platform::ports::AccessDenied> {
        use merezha_platform::ports::{AccessDenied, DenyReason};
        match principal {
            Some(principal) => principal.check(permission, scope),
            None => Err(AccessDenied::new(DenyReason::Unauthenticated)),
        }
    }
}

#[tokio::test]
async fn projection_controls_check_the_authenticated_principal_at_application_scope() {
    use merezha_platform::{
        Principal,
        edge::Authenticated,
        ports::{GrantSnapshot, Grants, PermissionSet, PrincipalKind},
    };
    let router = merezha_cli::application_http::router(application(), Some(Arc::new(GrantPolicy)));
    let user = |grants: GrantSnapshot| {
        Arc::new(Principal {
            subject: Identity::new("usr_1").unwrap(),
            kind: PrincipalKind::User,
            user: Some(Identity::new("usr_1").unwrap()),
            service_account: None,
            tenant: Some(Identity::new("org_1").unwrap()),
            grants: Arc::new(grants),
            session: None,
        })
    };
    let grant = |names: &[&str]| -> PermissionSet {
        names.iter().map(|n| Identity::new(*n).unwrap()).collect()
    };
    let call = |principal: Option<Arc<Principal>>, path: &'static str| {
        let router = router.clone();
        async move {
            let response = router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(path)
                        .extension(Authenticated(principal))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = response.status().as_u16();
            let body = to_bytes(response.into_body(), 4096).await.unwrap();
            (status, serde_json::from_slice::<Value>(&body).unwrap())
        }
    };
    const CATCH_UP: &str = "/v1/application/projections/total/catch-up";
    const REBUILD: &str = "/v1/application/projections/total/rebuild";
    let (status, body) = call(None, CATCH_UP).await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (401, Some("unauthenticated"))
    );
    let operator = user(GrantSnapshot::empty().with_application(Grants::new(
        vec![Identity::new("operator").unwrap()],
        grant(&["application.projections.catch_up"]),
    )));
    assert_eq!(call(Some(operator.clone()), CATCH_UP).await.0, 200);
    let (status, body) = call(Some(operator), REBUILD).await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (403, Some("unauthorized"))
    );
    // A tenant grant never satisfies the application-scope check.
    let tenant_admin = user(GrantSnapshot::empty().with_tenant(Grants::new(
        vec![Identity::new("owner").unwrap()],
        grant(&[
            "application.projections.catch_up",
            "application.projections.rebuild",
        ]),
    )));
    assert_eq!(call(Some(tenant_admin), REBUILD).await.0, 403);
}
