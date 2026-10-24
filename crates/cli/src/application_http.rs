use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Path, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use merezha_application::Application;
use merezha_domain::{Classification, Failure, Identity};
use merezha_platform::{
    edge::Authenticated,
    ports::{Authorizer, ResourceScope},
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

#[derive(Clone)]
struct Routes {
    application: Application,
    policy: Option<Arc<dyn Authorizer>>,
}
/// Application routes. Projection catch-up and rebuild are administrative:
/// with a `policy`, each needs `application.projections.catch_up` or
/// `application.projections.rebuild` at application scope, checked for the
/// principal the edge authenticated. With a policy (the platform host)
/// errors also take their status from the platform error registry. `serve`
/// passes no policy, keeps them open on its loopback listener and keeps its
/// own error statuses.
pub fn router(application: Application, policy: Option<Arc<dyn Authorizer>>) -> Router {
    Router::new()
        .route("/v1/application/commands", post(command))
        .route("/v1/application/queries", post(query))
        .route(
            "/v1/application/projections/{name}/catch-up",
            post(catch_up),
        )
        .route("/v1/application/projections/{name}/rebuild", post(rebuild))
        .with_state(Routes {
            application,
            policy,
        })
}
enum Action {
    Command,
    Query,
    Project { name: String, rebuild: bool },
}
async fn command(State(app): State<Routes>, request: Request) -> Response {
    dispatch(app, request, Action::Command).await
}
async fn query(State(app): State<Routes>, request: Request) -> Response {
    dispatch(app, request, Action::Query).await
}
async fn catch_up(
    State(app): State<Routes>,
    Path(name): Path<String>,
    request: Request,
) -> Response {
    dispatch(
        app,
        request,
        Action::Project {
            name,
            rebuild: false,
        },
    )
    .await
}
async fn rebuild(
    State(app): State<Routes>,
    Path(name): Path<String>,
    request: Request,
) -> Response {
    dispatch(
        app,
        request,
        Action::Project {
            name,
            rebuild: true,
        },
    )
    .await
}
async fn dispatch(state: Routes, request: Request, action: Action) -> Response {
    let platform = state.policy.is_some();
    let failure_response = |failure: Failure| {
        if platform {
            merezha_platform::error_response(&failure)
        } else {
            failure_response(failure)
        }
    };
    if let (Action::Project { rebuild, .. }, Some(policy)) = (&action, &state.policy) {
        let permission = if *rebuild {
            "application.projections.rebuild"
        } else {
            "application.projections.catch_up"
        };
        let principal = request
            .extensions()
            .get::<Authenticated>()
            .and_then(|authenticated| authenticated.0.clone());
        let checked = Identity::new(permission).and_then(|permission| {
            policy
                .check(
                    principal.as_deref(),
                    &permission,
                    &ResourceScope::Application,
                )
                .map_err(Failure::from)
        });
        if let Err(failure) = checked {
            return failure_response(failure);
        }
    }
    let app = state.application;
    let admission = match app.admit() {
        Ok(a) => a,
        Err(e) => return failure_response(e),
    };
    let maximum = if matches!(action, Action::Project { .. }) {
        0
    } else {
        65_536
    };
    let bytes = match tokio::time::timeout_at(
        tokio::time::Instant::from_std(
            admission
                .deadline()
                .min(std::time::Instant::now() + Duration::from_secs(5)),
        ),
        to_bytes(request.into_body(), maximum),
    )
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(_)) => return failure_response(Failure::permanent("input_limit")),
        Err(_) => return failure_response(Failure::transient("timeout")),
    };
    let result = tokio::task::spawn_blocking(move || -> Result<Value, Failure> {
        let input = std::str::from_utf8(&bytes).map_err(|_| Failure::permanent("invalid_input"))?;
        let result = match action {
            Action::Command => {
                serde_json::to_value(admission.command(merezha_domain::decode(input)?)?)
            }
            Action::Query => serde_json::to_value(admission.query(merezha_domain::decode(input)?)?),
            Action::Project { name, rebuild } => {
                serde_json::to_value(admission.project(&name, rebuild)?)
            }
        };
        result.map_err(|_| Failure::permanent("serialization_failed"))
    })
    .await;
    match result {
        Ok(Ok(value)) => Json(value).into_response(),
        Ok(Err(error)) => failure_response(error),
        Err(_) => failure_response(Failure::transient("internal")),
    }
}
fn failure_response(error: Failure) -> Response {
    let status = match error.code.as_str() {
        "unauthorized" => StatusCode::FORBIDDEN,
        "handler_not_found" | "projection_not_found" => StatusCode::NOT_FOUND,
        "input_limit" | "payload_limit" | "output_limit" => StatusCode::PAYLOAD_TOO_LARGE,
        "timeout" => StatusCode::GATEWAY_TIMEOUT,
        _ => match error.classification {
            Classification::Permanent => StatusCode::UNPROCESSABLE_ENTITY,
            Classification::Conflict => StatusCode::CONFLICT,
            Classification::Transient => StatusCode::TOO_MANY_REQUESTS,
        },
    };
    (status, Json(json!({"error":error}))).into_response()
}
