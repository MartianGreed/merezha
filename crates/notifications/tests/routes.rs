//! The preview routes (NT §4, §7.1): with and without the permission,
//! disabled previews, render failures with details and the sandboxed HTML.
mod support;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use merezha_domain::{Failure, Identity, MessageKind, Metadata};
use merezha_notifications::{Notifications, PREVIEW_CSP, PREVIEW_PERMISSION};
use merezha_platform::{
    AppContext, Module,
    edge::{Authenticated, ClientInfo},
    ports::{
        AccessDenied, Allowed, Authorizer, DenyAll, DenyReason, GrantSnapshot, Grants,
        LoopbackAllowAll, Permission, PermissionSet, Principal, PrincipalKind, ResourceScope,
    },
};
use serde_json::{Value, json};
use std::sync::Arc;
use support::*;
use tower::ServiceExt;

/// RBAC as identity decides it: the principal's own snapshot.
struct Grantsheet;
impl merezha_application::Authorizer for Grantsheet {
    fn authorize(
        &self,
        _: MessageKind,
        _: &str,
        _: Option<&str>,
        _: &Value,
        _: &Metadata,
    ) -> Result<(), Failure> {
        Err(Failure::permanent("unauthorized"))
    }
}
impl Authorizer for Grantsheet {
    fn check(
        &self,
        principal: Option<&Principal>,
        permission: &Permission,
        scope: &ResourceScope,
    ) -> Result<Allowed, AccessDenied> {
        match principal {
            None => Err(AccessDenied::new(DenyReason::Unauthenticated)),
            Some(principal) => principal.check(permission, scope),
        }
    }
}

fn user(permissions: &[&str]) -> Arc<Principal> {
    let set: PermissionSet = permissions
        .iter()
        .map(|p| Identity::new(*p).unwrap())
        .collect();
    Arc::new(Principal {
        subject: Identity::new("usr_ada").unwrap(),
        kind: PrincipalKind::User,
        user: Some(Identity::new("usr_ada").unwrap()),
        service_account: None,
        tenant: None,
        grants: Arc::new(GrantSnapshot::empty().with_application(Grants::new(Vec::new(), set))),
        session: None,
    })
}

fn routes_for(project: &Project, authorizer: Arc<dyn Authorizer>) -> Router {
    let input = project.input();
    let mut module = Notifications::new(merezha_notifications::HostFacts::load(&input));
    let config = merezha_platform::ProjectConfig::load(&input).unwrap();
    module
        .configure(&config.modules["notifications"])
        .unwrap_or_else(|issues| panic!("{issues:?}"));
    let db = merezha_postgres::Database::connect(&merezha_postgres::DatabaseConfig::new(
        merezha_postgres::Secret::new("postgres://127.0.0.1:1/none"),
    ))
    .unwrap();
    let ctx = AppContext::builder("tasks", db, authorizer)
        .build()
        .unwrap();
    module.routes(&ctx)
}

async fn call(
    router: &Router,
    method: &str,
    path: &str,
    principal: Option<Arc<Principal>>,
    body: Option<Value>,
) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
    let mut request = Request::builder().method(method).uri(path);
    if body.is_some() {
        request = request.header("content-type", "application/json");
    }
    let mut request = request
        .body(match body {
            Some(body) => Body::from(body.to_string()),
            None => Body::empty(),
        })
        .unwrap();
    request.extensions_mut().insert(Authenticated(principal));
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    (status, headers, bytes.to_vec())
}
fn json_of(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).unwrap()
}

#[tokio::test]
async fn preview_needs_the_permission_at_application_scope() {
    let project = Project::new(MINIMAL);
    let router = routes_for(&project, Arc::new(Grantsheet));
    let path = "/templates/identity.magic_link/preview";
    let (status, _, body) = call(&router, "POST", path, None, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(json_of(&body)["error"]["code"], "unauthenticated");
    let (status, _, body) = call(&router, "POST", path, Some(user(&["tasks.read"])), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(json_of(&body)["error"]["code"], "unauthorized");
    let (status, _, body) = call(
        &router,
        "GET",
        "/templates",
        Some(user(&["tasks.read"])),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "{}",
        String::from_utf8_lossy(&body)
    );

    let (status, _, body) = call(
        &router,
        "POST",
        path,
        Some(user(&[PREVIEW_PERMISSION])),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let preview = json_of(&body);
    assert_eq!(preview["email"]["subject"], "Your Tasks sign-in link");
    assert!(
        preview["email"]["text"]
            .as_str()
            .unwrap()
            .contains("https://app.example.com/magic?token=preview")
    );
    assert!(preview.get("inbox").is_none());
    let (status, _, body) = call(
        &router,
        "GET",
        "/templates",
        Some(user(&[PREVIEW_PERMISSION])),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let list = json_of(&body);
    assert_eq!(list.as_array().unwrap().len(), 10);
    let changed = list
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "identity.password_changed")
        .unwrap();
    assert_eq!(changed["channels"], json!(["email", "inbox"]));
    assert_eq!(changed["locales"], json!(["en"]));

    // The kernel fallbacks: allow-all on loopback, deny-all elsewhere.
    let open = routes_for(
        &project,
        Arc::new(LoopbackAllowAll::new("127.0.0.1:8080".parse().unwrap()).unwrap()),
    );
    assert_eq!(
        call(&open, "POST", path, None, None).await.0,
        StatusCode::OK
    );
    let closed = routes_for(&project, Arc::new(DenyAll));
    assert_eq!(
        call(&closed, "POST", path, None, None).await.0,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn preview_renders_given_data_and_reports_failures_as_details() {
    let project = Project::new(&format!(
        "{MINIMAL}[modules.notifications.templates]\nlocales = [\"en\", \"fr\"]\n\
         [modules.notifications.topics.\"tasks.assigned\"]\ncategory = \"transactional\"\nchannels = [\"email\", \"inbox\"]\n"
    ))
    .template("tasks.assigned/en/subject.txt", "Task: {{ title }}")
    .template("tasks.assigned/en/body.txt", "{{ title }} for {{ recipient.name }}{% if links.unsubscribe %} {{ links.unsubscribe }}{% endif %}")
    .template("tasks.assigned/en/inbox.toml", "title = \"{{ title }}\"\nbody = \"{{ owner }}\"\n")
    .template("tasks.assigned/fr/subject.txt", "Tâche : {{ title }}")
    .template("tasks.assigned/fr/body.txt", "{{ title }}")
    .template(
        "tasks.assigned/template.toml",
        "description = \"Assigned\"\nrequired = [\"title\"]\n[preview]\ntitle = \"Write\"\nowner = \"Bob\"\n",
    );
    let router = routes_for(&project, Arc::new(Grantsheet));
    let admin = || Some(user(&[PREVIEW_PERMISSION]));
    let path = "/templates/tasks.assigned/preview";
    let (status, _, body) = call(
        &router,
        "POST",
        path,
        admin(),
        Some(json!({"locale": "fr-CA", "data": {"title": "Rapport", "owner": "Eve"}})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let preview = json_of(&body);
    assert_eq!(preview["email"]["subject"], "Tâche : Rapport");
    // The inbox has no French file: it falls back to English.
    assert_eq!(preview["inbox"]["title"], "Rapport");
    assert_eq!(preview["inbox"]["body"], "Eve");

    // Transactional email previews carry an unsubscribe link.
    let (_, _, body) = call(
        &router,
        "POST",
        path,
        admin(),
        Some(json!({"channels": ["email"]})),
    )
    .await;
    let preview = json_of(&body);
    assert!(
        preview["email"]["text"]
            .as_str()
            .unwrap()
            .contains("/v1/notifications/unsubscribe?token=preview")
    );
    assert!(preview.get("inbox").is_none());

    // Strict undefined: a missing variable fails with details, no text echoed.
    let (status, _, body) = call(
        &router,
        "POST",
        path,
        admin(),
        Some(json!({"data": {"title": "Only a title"}})),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let error = json_of(&body);
    assert_eq!(error["error"]["code"], "render_failed");
    assert_eq!(
        error["error"]["details"],
        json!([{"path": "/channels/inbox", "code": "undefined_variable"}])
    );
    assert!(!String::from_utf8_lossy(&body).contains("owner"));

    for (request, status, code) in [
        (json!({"data": {}}), 422, "render_failed"),
        (json!({"data": {"app": "x"}}), 422, "reserved_data_key"),
        (
            json!({"data": {"title": "x".repeat(40_000)}}),
            422,
            "data_too_large",
        ),
        (json!({"channels": ["sms"]}), 422, "channel_not_allowed"),
        (json!({"channels": ["fax"]}), 422, "channel_not_allowed"),
        (json!({"locale": "not a locale"}), 422, "invalid_locale"),
        (json!({"unknown": true}), 400, "invalid_input"),
    ] {
        let (got, _, body) = call(&router, "POST", path, admin(), Some(request.clone())).await;
        assert_eq!(got.as_u16(), status, "{request}");
        assert_eq!(json_of(&body)["error"]["code"], code, "{request}");
    }
    let (status, _, _) = call(
        &router,
        "POST",
        "/templates/tasks.unknown/preview",
        admin(),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn preview_html_is_sandboxed() {
    let project = Project::new(&format!(
        "{MINIMAL}[modules.notifications.templates]\nlocales = [\"en\", \"fr\"]\n"
    ));
    let router = routes_for(&project, Arc::new(Grantsheet));
    let (status, headers, body) = call(
        &router,
        "GET",
        "/templates/identity.verify_email/preview.html?locale=fr",
        Some(user(&[PREVIEW_PERMISSION])),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-security-policy"], PREVIEW_CSP);
    assert_eq!(headers["content-type"], "text/html; charset=utf-8");
    assert_eq!(headers["x-content-type-options"], "nosniff");
    let html = String::from_utf8(body).unwrap();
    assert!(html.contains("<html lang=\"fr\">") && html.contains("Confirmez votre adresse"));
    let (status, _, _) = call(
        &router,
        "GET",
        "/templates/identity.verify_email/preview.html",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn disabled_previews_answer_404() {
    let project = Project::new(&format!(
        "{MINIMAL}[modules.notifications.templates]\npreview = false\n"
    ));
    let router = routes_for(&project, Arc::new(Grantsheet));
    let admin = Some(user(&[PREVIEW_PERMISSION]));
    for (method, path) in [
        ("GET", "/templates"),
        ("POST", "/templates/identity.magic_link/preview"),
        ("GET", "/templates/identity.magic_link/preview.html"),
    ] {
        let (status, _, body) = call(&router, method, path, admin.clone(), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
        assert_eq!(json_of(&body)["error"]["code"], "not_found");
    }
}

#[tokio::test]
async fn captures_are_listed_only_on_loopback_with_a_capture_driver() {
    let project = Project::new(MINIMAL);
    let router = routes_for(&project, Arc::new(Grantsheet));
    let remote = ClientInfo {
        address: Some("203.0.113.9".parse().unwrap()),
        forwarded_proto: None,
        forwarded_host: None,
        via_trusted_proxy: false,
    };
    let request = Request::builder()
        .uri("/dev/captures")
        .extension(remote)
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    // Without any client information (no edge) the route stays closed.
    let request = Request::builder()
        .uri("/dev/captures")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        router.clone().oneshot(request).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
    // Without a capture driver it is closed on loopback too.
    let disabled = routes_for(
        &Project::new(&format!(
            "{MINIMAL}[modules.notifications.email]\ndriver = \"disabled\"\n"
        )),
        Arc::new(Grantsheet),
    );
    let local = ClientInfo {
        address: Some("127.0.0.1".parse().unwrap()),
        forwarded_proto: None,
        forwarded_host: None,
        via_trusted_proxy: false,
    };
    let request = Request::builder()
        .uri("/dev/captures")
        .extension(local)
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        disabled.oneshot(request).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
}
