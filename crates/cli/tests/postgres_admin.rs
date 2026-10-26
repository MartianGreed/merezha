//! The PostgreSQL application's routes and their permissions (AP §D.1), in
//! process against real PostgreSQL. The test creates its database through
//! `MEREZHA_TEST_DATABASE_URL` and skips with a message when it is unset.
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use merezha_application::{
    Application, Guest, GuestSession,
    config::Config,
    postgres::{MIGRATIONS, MODULE, PostgresOptions, PostgresStore},
};
use merezha_domain::{Event, Failure, Identity, Operation, Reply};
use merezha_platform::{
    Principal,
    edge::Authenticated,
    ports::{GrantSnapshot, Grants, PermissionSet, PrincipalKind},
};
use merezha_postgres::{Database, MigrationMode, ModuleMigrations, TestDatabase, run_migrations};
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tower::ServiceExt;

#[derive(Clone)]
struct Counter;
impl Guest for Counter {
    fn services(&self) -> Result<Vec<String>, Failure> {
        Ok(vec!["domain".into()])
    }
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
        Ok(Box::new(self.clone()))
    }
}
impl GuestSession for Counter {
    fn invoke(&self, _: &str, operation: Operation) -> Result<Reply, Failure> {
        Ok(match operation {
            Operation::Validate { payload, .. } => Reply::Validated { payload },
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

/// Allows application messages and decides permissions from the
/// principal's grant snapshot, as the identity block's authorizer does.
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
        principal: Option<&Principal>,
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

const PERMISSIONS: [&str; 4] = [
    "application.admin.read",
    "application.admin.write",
    "application.projections.catch_up",
    "application.projections.rebuild",
];
fn grant(names: &[&str]) -> PermissionSet {
    names.iter().map(|n| Identity::new(*n).unwrap()).collect()
}
fn user(grants: GrantSnapshot) -> Arc<Principal> {
    Arc::new(Principal {
        subject: Identity::new("usr_1").unwrap(),
        kind: PrincipalKind::User,
        user: Some(Identity::new("usr_1").unwrap()),
        service_account: None,
        tenant: Some(Identity::new("org_1").unwrap()),
        grants: Arc::new(grants),
        session: None,
    })
}
fn holding(permission: &str) -> Option<Arc<Principal>> {
    Some(user(GrantSnapshot::empty().with_application(Grants::new(
        vec![Identity::new("operator").unwrap()],
        grant(&[permission]),
    ))))
}

/// Every route of `postgres_routes` against no principal, a principal with
/// each one permission at application scope, and one with all of them at
/// tenant scope only, which never satisfies an application-scope check.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_admin_route_checks_its_permission_at_application_scope() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    let db = Database::connect(&test.config()).unwrap();
    let modules = [ModuleMigrations {
        module: MODULE,
        migrations: MIGRATIONS,
    }];
    run_migrations(&db, &modules, MigrationMode::Apply, deadline)
        .await
        .unwrap();
    let config = Config::parse(
        r#"version=2
[storage]
backend="postgres"
application="admin-test"
[[commands]]
tag="Add"
service="domain"
aggregate="Counter"
access="public"
[[queries]]
tag="Total"
service="domain"
projection="total"
access="public"
[[projections]]
name="total"
service="domain"
initial=0
"#,
    )
    .unwrap();
    let store = PostgresStore::connect(&db, &config, PostgresOptions::default(), deadline)
        .await
        .unwrap();
    let app = Application::with_postgres(
        config,
        Arc::new(Counter),
        Arc::new(GrantPolicy),
        Arc::new(store),
    )
    .unwrap();
    let router = merezha_cli::application_http::postgres_routes(app, Arc::new(GrantPolicy));
    let call = |principal: Option<Arc<Principal>>, method: &'static str, path: &'static str| {
        let router = router.clone();
        async move {
            let body = match path {
                "/commands" => {
                    r#"{"tag":"Add","stream":"a","payload":1,"metadata":{"message_id":"m","correlation_id":"c"}}"#
                }
                "/queries" => {
                    r#"{"tag":"Total","payload":{},"metadata":{"message_id":"m","correlation_id":"c"}}"#
                }
                _ => "",
            };
            let response = router
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .header("content-type", "application/json")
                        .extension(Authenticated(principal))
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = response.status().as_u16();
            let body = to_bytes(response.into_body(), 65_536).await.unwrap();
            (
                status,
                serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null),
            )
        }
    };
    // Route, the permission it needs (none for the public messages) and its
    // status once allowed. The order runs the rebuild before it is abandoned.
    let routes: [(&str, &str, Option<&str>, u16); 9] = [
        ("POST", "/commands", None, 200),
        ("POST", "/queries", None, 200),
        ("GET", "/status", Some("application.admin.read"), 200),
        ("GET", "/projections", Some("application.admin.read"), 200),
        (
            "GET",
            "/projections/total",
            Some("application.admin.read"),
            200,
        ),
        (
            "POST",
            "/projections/total/catch-up",
            Some("application.projections.catch_up"),
            200,
        ),
        (
            "POST",
            "/projections/total/rebuild",
            Some("application.projections.rebuild"),
            202,
        ),
        (
            "DELETE",
            "/projections/total/rebuild",
            Some("application.admin.write"),
            204,
        ),
        // The configured revision: allowed, then refused on its merits.
        (
            "DELETE",
            "/projections/total/revisions/1",
            Some("application.admin.write"),
            409,
        ),
    ];
    let tenant_wide = Some(user(GrantSnapshot::empty().with_tenant(Grants::new(
        vec![Identity::new("owner").unwrap()],
        grant(&PERMISSIONS),
    ))));
    for (method, path, needed, allowed) in routes {
        let mut cells: Vec<(&str, Option<Arc<Principal>>)> = vec![("none", None)];
        cells.extend(PERMISSIONS.iter().map(|p| (*p, holding(p))));
        cells.push(("tenant", tenant_wide.clone()));
        for (cell, principal) in cells {
            let expected = match (needed, cell) {
                (None, _) => allowed,
                (Some(_), "none") => 401,
                (Some(permission), held) if permission == held => allowed,
                _ => 403,
            };
            let (status, body) = call(principal, method, path).await;
            assert_eq!(status, expected, "{method} {path} as {cell}: {body}");
            match expected {
                401 => assert_eq!(body["error"]["code"], "unauthenticated", "{path}"),
                403 => assert_eq!(body["error"]["code"], "unauthorized", "{path}"),
                _ => {}
            }
        }
    }
}
