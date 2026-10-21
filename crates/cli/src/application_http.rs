use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Path, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use merezha_application::Application;
use merezha_domain::{Classification, Failure};
use serde_json::{Value, json};
use std::time::Duration;

pub fn router(application: Application) -> Router {
    Router::new()
        .route("/v1/application/commands", post(command))
        .route("/v1/application/queries", post(query))
        .route(
            "/v1/application/projections/{name}/catch-up",
            post(catch_up),
        )
        .route("/v1/application/projections/{name}/rebuild", post(rebuild))
        .with_state(application)
}
enum Action {
    Command,
    Query,
    Project { name: String, rebuild: bool },
}
async fn command(State(app): State<Application>, request: Request) -> Response {
    dispatch(app, request, Action::Command).await
}
async fn query(State(app): State<Application>, request: Request) -> Response {
    dispatch(app, request, Action::Query).await
}
async fn catch_up(
    State(app): State<Application>,
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
    State(app): State<Application>,
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
async fn dispatch(app: Application, request: Request, action: Action) -> Response {
    let admission = match app.admit() {
        Ok(a) => a,
        Err(e) => return failure(e),
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
        Ok(Err(_)) => return failure(Failure::permanent("input_limit")),
        Err(_) => return failure(Failure::transient("timeout")),
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
        Ok(Err(error)) => failure(error),
        Err(_) => failure(Failure::transient("internal")),
    }
}
fn failure(error: Failure) -> Response {
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
