//! The edge's principal reaches a version 2 guest through the HTTP
//! adapter's trusted entry points, as its limited projection; a principal
//! in a request body is never trusted (AP §C.2, §C.4, slice C1).
use axum::{
    Router,
    body::{Body, to_bytes},
    http::Request,
};
use merezha_application::{Application, Guest, GuestSession, config::Config};
use merezha_domain::{Event, Failure, Identity, Operation, PROTOCOL_V2, Reply};
use merezha_platform::{
    edge::Authenticated,
    ports::{GrantSnapshot, Grants, LoopbackAllowAll, Principal, PrincipalKind, SessionFacts},
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;

/// A version 2 guest recording the principal each `decide` and `query`
/// receives.
#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Value>>>);
impl Guest for Recorder {
    fn services(&self) -> Result<Vec<String>, Failure> {
        Ok(vec!["domain".into()])
    }
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
        Ok(Box::new(self.clone()))
    }
}
impl GuestSession for Recorder {
    fn protocol(&self, _: &str) -> u16 {
        PROTOCOL_V2
    }
    fn invoke(&self, _: &str, operation: Operation) -> Result<Reply, Failure> {
        let mut seen = self.0.lock().unwrap();
        Ok(match operation {
            Operation::Describe {} => Reply::Described { protocol: 2 },
            Operation::Validate { payload, .. } => Reply::Validated { payload },
            Operation::Decide {
                payload, metadata, ..
            } => {
                seen.push(serde_json::to_value(&metadata).unwrap());
                Reply::Decided {
                    events: vec![Event {
                        tag: Identity::new("Added").unwrap(),
                        schema_version: 1,
                        payload,
                    }],
                }
            }
            Operation::Query { metadata, .. } => {
                seen.push(serde_json::to_value(&metadata).unwrap());
                Reply::Queried { value: json!(0) }
            }
            Operation::Project { state, .. } => Reply::Projected { state },
        })
    }
}
fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}
fn principal() -> Arc<Principal> {
    Arc::new(Principal {
        subject: id("usr_ada"),
        kind: PrincipalKind::User,
        user: Some(id("usr_ada")),
        service_account: None,
        tenant: Some(id("org_1")),
        grants: Arc::new(
            GrantSnapshot::empty()
                .with_application(Grants::new(vec![id("user")], Default::default()))
                .with_tenant(Grants::new(vec![id("member")], Default::default())),
        ),
        session: Some(SessionFacts {
            id: id("ses_0192_secret_ish"),
            method: id("password"),
            authenticated_at: jiff::Timestamp::UNIX_EPOCH,
            transport: merezha_platform::ports::SessionTransport::Cookie,
        }),
    })
}
fn router(recorder: &Recorder, principal: Option<Arc<Principal>>) -> Router {
    let config = Config::parse(
        "version=1\n[[commands]]\ntag=\"Add\"\nservice=\"domain\"\naggregate=\"Counter\"\n\
         [[queries]]\ntag=\"Total\"\nservice=\"domain\"\nprojection=\"total\"\n\
         [[projections]]\nname=\"total\"\nservice=\"domain\"\ninitial=0\n",
    )
    .unwrap();
    let policy = Arc::new(LoopbackAllowAll::new("127.0.0.1:1".parse().unwrap()).unwrap());
    let app = Application::in_memory(config, Arc::new(recorder.clone()), policy.clone()).unwrap();
    merezha_cli::application_http::router(app, Some(policy)).layer(axum::middleware::from_fn(
        move |mut request: axum::extract::Request, next: axum::middleware::Next| {
            request
                .extensions_mut()
                .insert(Authenticated(principal.clone()));
            next.run(request)
        },
    ))
}
async fn post(app: &Router, path: &str, body: Value) -> (u16, Value) {
    send(app, path, body.to_string()).await
}
async fn send(app: &Router, path: &str, body: String) -> (u16, Value) {
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
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn the_edge_principal_reaches_the_guest_as_its_projection() {
    let recorder = Recorder::default();
    let app = router(&recorder, Some(principal()));
    let (status, _) = post(
        &app,
        "/v1/application/commands",
        json!({"tag":"Add","stream":"one","payload":1}),
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = post(
        &app,
        "/v1/application/queries",
        json!({"tag":"Total","payload":{}}),
    )
    .await;
    assert_eq!(status, 200);
    let seen = recorder.0.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    for metadata in seen {
        assert_eq!(metadata["actor"], json!("usr_ada"));
        assert_eq!(
            metadata["principal"],
            json!({"subject":"usr_ada","kind":"user","tenant":"org_1","roles":["user"]})
        );
        assert!(!metadata.to_string().contains("ses_"), "no session facts");
    }
    // Another caller-supplied actor is refused.
    let (status, body) = post(
        &app,
        "/v1/application/commands",
        json!({"tag":"Add","stream":"one","payload":1,"metadata":{"actor":"usr_bob"}}),
    )
    .await;
    assert_eq!(
        (status, body["error"]["code"].clone()),
        (403, json!("actor_mismatch"))
    );
}

#[tokio::test]
async fn a_principal_in_the_body_is_never_trusted() {
    let injected = json!({"subject":"usr_root","kind":"user","roles":["admin"]});
    for principal in [None, Some(principal())] {
        let recorder = Recorder::default();
        let app = router(&recorder, principal);
        let (status, body) = post(
            &app,
            "/v1/application/commands",
            json!({"tag":"Add","stream":"one","payload":1,"metadata":{"principal":injected}}),
        )
        .await;
        assert_eq!(
            (status, body["error"]["code"].clone()),
            (422, json!("principal_not_trusted"))
        );
        let (status, body) = post(
            &app,
            "/v1/application/queries",
            json!({"tag":"Total","payload":{},"metadata":{"principal":injected}}),
        )
        .await;
        assert_eq!(
            (status, body["error"]["code"].clone()),
            (422, json!("principal_not_trusted"))
        );
        assert!(recorder.0.lock().unwrap().is_empty(), "no guest decided");
    }
    // Anonymous requests reach the guest without a principal.
    let recorder = Recorder::default();
    let app = router(&recorder, None);
    let (status, _) = post(
        &app,
        "/v1/application/commands",
        json!({"tag":"Add","stream":"one","payload":1}),
    )
    .await;
    assert_eq!(status, 200);
    assert!(recorder.0.lock().unwrap()[0].get("principal").is_none());
}

/// The guest projection is built only for commands and queries, and its
/// bounds bind only for a version 2 guest: an oversize principal never
/// fails an administrative route.
#[tokio::test]
async fn an_oversize_principal_fails_only_the_version_2_call() {
    let crowded = {
        let mut principal = (*principal()).clone();
        principal.grants = Arc::new(GrantSnapshot::empty().with_application(Grants::new(
            (0..17).map(|i| id(&format!("r{i:02}"))).collect(),
            Default::default(),
        )));
        Arc::new(principal)
    };
    let recorder = Recorder::default();
    let app = router(&recorder, Some(crowded));
    let (status, body) = post(
        &app,
        "/v1/application/commands",
        json!({"tag":"Add","stream":"one","payload":1}),
    )
    .await;
    assert_eq!(
        (status, body["error"]["code"].clone()),
        (422, json!("invalid_principal"))
    );
    let (status, _) = send(
        &app,
        "/v1/application/projections/total/catch-up",
        String::new(),
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = send(
        &app,
        "/v1/application/projections/total/rebuild",
        String::new(),
    )
    .await;
    assert_eq!(status, 200);
    assert!(recorder.0.lock().unwrap().is_empty(), "no guest decided");
}

/// AP-C2: the guest receives the role keys held at the route's scope: the
/// tenant roles on a tenant-scoped route, whose stream is the principal's
/// tenant's, and the application roles on an application route.
#[tokio::test]
async fn the_guest_sees_the_roles_at_the_routes_scope() {
    let recorder = Recorder::default();
    let config = Config::parse(
        "version=2\n[[commands]]\ntag=\"Add\"\nservice=\"domain\"\naggregate=\"Counter\"\naccess=\"authenticated\"\n\
         [[commands]]\ntag=\"TenantAdd\"\nservice=\"domain\"\naggregate=\"Counter\"\naccess=\"authenticated\"\nscope=\"tenant\"\n",
    )
    .unwrap();
    let policy = Arc::new(LoopbackAllowAll::new("127.0.0.1:1".parse().unwrap()).unwrap());
    let app = Application::in_memory(config, Arc::new(recorder.clone()), policy.clone()).unwrap();
    let principal = principal();
    let app =
        merezha_cli::application_http::router(app, Some(policy)).layer(axum::middleware::from_fn(
            move |mut request: axum::extract::Request, next: axum::middleware::Next| {
                request
                    .extensions_mut()
                    .insert(Authenticated(Some(principal.clone())));
                next.run(request)
            },
        ));
    let (status, tenant) = post(
        &app,
        "/v1/application/commands",
        json!({"tag":"TenantAdd","stream":"one","payload":1}),
    )
    .await;
    assert_eq!(
        (status, tenant["stream"].clone()),
        (200, json!("org_1:Counter/one"))
    );
    let (status, _) = post(
        &app,
        "/v1/application/commands",
        json!({"tag":"Add","stream":"one","payload":1}),
    )
    .await;
    assert_eq!(status, 200);
    let roles: Vec<Value> = recorder
        .0
        .lock()
        .unwrap()
        .iter()
        .map(|metadata| metadata["principal"]["roles"].clone())
        .collect();
    assert_eq!(roles, [json!(["member"]), json!(["user"])]);
}
