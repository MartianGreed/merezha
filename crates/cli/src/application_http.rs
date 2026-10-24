use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Path, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
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
    Router::new().nest("/v1/application", routes(application, policy))
}
/// The same routes relative to `/v1/application`, for the platform host,
/// which nests the PostgreSQL application module's routes under its name.
pub fn routes(application: Application, policy: Option<Arc<dyn Authorizer>>) -> Router {
    Router::new()
        .route("/commands", post(command))
        .route("/queries", post(query))
        .route("/projections/{name}/catch-up", post(catch_up))
        .route("/projections/{name}/rebuild", post(rebuild))
        .with_state(Routes {
            application,
            policy,
        })
}
/// The PostgreSQL application's routes: [`routes`] with the background
/// rebuild (`202`) and the admin API of AP §D.1. Reads need
/// `application.admin.read`, revision deletion and abandoning a rebuild
/// `application.admin.write`, at application scope.
pub fn postgres_routes(application: Application, policy: Arc<dyn Authorizer>) -> Router {
    Router::new()
        .route("/commands", post(command))
        .route("/queries", post(query))
        .route("/projections/{name}/catch-up", post(catch_up))
        .route(
            "/projections/{name}/rebuild",
            post(request_rebuild).delete(abandon_rebuild),
        )
        .route("/projections", get(projections))
        .route("/projections/{name}", get(projection))
        .route(
            "/projections/{name}/revisions/{revision}",
            delete(delete_revision),
        )
        .route("/status", get(status))
        .with_state(Routes {
            application,
            policy: Some(policy),
        })
}
enum Action {
    Command,
    Query,
    Project { name: String, rebuild: bool },
    RequestRebuild { name: String },
    AbandonRebuild { name: String },
    Projections,
    Projection { name: String },
    DeleteRevision { name: String, revision: String },
    Status,
}
impl Action {
    /// The permission checked at application scope, for administrative
    /// actions.
    fn permission(&self) -> Option<&'static str> {
        match self {
            Self::Command | Self::Query => None,
            Self::Project { rebuild: false, .. } => Some("application.projections.catch_up"),
            Self::Project { rebuild: true, .. } | Self::RequestRebuild { .. } => {
                Some("application.projections.rebuild")
            }
            Self::AbandonRebuild { .. } | Self::DeleteRevision { .. } => {
                Some("application.admin.write")
            }
            Self::Projections | Self::Projection { .. } | Self::Status => {
                Some("application.admin.read")
            }
        }
    }
    fn body_limit(&self) -> usize {
        match self {
            Self::Command | Self::Query => 65_536,
            Self::RequestRebuild { .. } => 1024,
            _ => 0,
        }
    }
}
/// The `resume` flag of a `POST .../rebuild` body on PostgreSQL: empty,
/// `{}` or `{"resume":bool}`; anything else is `invalid_input`.
fn resume(input: &str) -> Result<bool, Failure> {
    if input.trim().is_empty() {
        return Ok(false);
    }
    let invalid = || Failure::permanent("invalid_input");
    let body: serde_json::Map<String, Value> =
        serde_json::from_str(input).map_err(|_| invalid())?;
    match (body.len(), body.get("resume")) {
        (0, None) => Ok(false),
        (1, Some(Value::Bool(resume))) => Ok(*resume),
        _ => Err(invalid()),
    }
}
async fn request_rebuild(
    State(app): State<Routes>,
    Path(name): Path<String>,
    request: Request,
) -> Response {
    dispatch(app, request, Action::RequestRebuild { name }).await
}
async fn abandon_rebuild(
    State(app): State<Routes>,
    Path(name): Path<String>,
    request: Request,
) -> Response {
    dispatch(app, request, Action::AbandonRebuild { name }).await
}
async fn projections(State(app): State<Routes>, request: Request) -> Response {
    dispatch(app, request, Action::Projections).await
}
async fn projection(
    State(app): State<Routes>,
    Path(name): Path<String>,
    request: Request,
) -> Response {
    dispatch(app, request, Action::Projection { name }).await
}
async fn delete_revision(
    State(app): State<Routes>,
    Path((name, revision)): Path<(String, String)>,
    request: Request,
) -> Response {
    dispatch(app, request, Action::DeleteRevision { name, revision }).await
}
async fn status(State(app): State<Routes>, request: Request) -> Response {
    dispatch(app, request, Action::Status).await
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
    let principal = request
        .extensions()
        .get::<Authenticated>()
        .and_then(|authenticated| authenticated.0.clone());
    let requested_by = principal
        .as_ref()
        .map(|principal| principal.subject.as_str().to_owned());
    if let (Some(permission), Some(policy)) = (action.permission(), &state.policy) {
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
    let maximum = action.body_limit();
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
    let result = merezha_telemetry::spawn_blocking_in_span(
        move || -> Result<(StatusCode, Value), Failure> {
            let input =
                std::str::from_utf8(&bytes).map_err(|_| Failure::permanent("invalid_input"))?;
            let encoded = |value: Result<Value, serde_json::Error>| {
                value.map_err(|_| Failure::permanent("serialization_failed"))
            };
            let ok = StatusCode::OK;
            Ok(match action {
                Action::Command => (
                    ok,
                    encoded(serde_json::to_value(
                        admission.command(merezha_domain::decode(&with_host_ids(input)?)?)?,
                    ))?,
                ),
                Action::Query => (
                    ok,
                    encoded(serde_json::to_value(
                        admission.query(merezha_domain::decode(&with_host_ids(input)?)?)?,
                    ))?,
                ),
                Action::Project { name, rebuild } => (
                    ok,
                    encoded(serde_json::to_value(admission.project(&name, rebuild)?))?,
                ),
                Action::RequestRebuild { name } => {
                    let requested = admission.request_rebuild(
                        &name,
                        resume(input)?,
                        requested_by.as_deref(),
                    )?;
                    (
                        StatusCode::ACCEPTED,
                        json!({"rebuild": encoded(serde_json::to_value(requested))?}),
                    )
                }
                Action::AbandonRebuild { name } => {
                    admission.abandon_rebuild(&name)?;
                    (StatusCode::NO_CONTENT, Value::Null)
                }
                Action::Projections => {
                    (ok, encoded(serde_json::to_value(admission.projections()?))?)
                }
                Action::Projection { name } => (
                    ok,
                    encoded(serde_json::to_value(admission.projection(&name)?))?,
                ),
                Action::DeleteRevision { name, revision } => {
                    let revision = revision
                        .parse()
                        .map_err(|_| Failure::permanent("projection_not_found"))?;
                    admission.delete_revision(&name, revision)?;
                    (StatusCode::NO_CONTENT, Value::Null)
                }
                Action::Status => (ok, encoded(serde_json::to_value(admission.status()?))?),
            })
        },
    )
    .await;
    match result {
        Ok(Ok((StatusCode::NO_CONTENT, _))) => StatusCode::NO_CONTENT.into_response(),
        Ok(Ok((status, value))) => (status, Json(value)).into_response(),
        Ok(Err(error)) => failure_response(error),
        Err(_) => failure_response(Failure::transient("internal")),
    }
}
/// Fill the ids a command or query body leaves out (AP4, TE §2.2) and
/// return the body to decode: a missing `metadata` object, `message_id`
/// (128 random bits in hex) or `correlation_id` (the current trace id as 32
/// lowercase hex, or 128 random bits when no trace is recorded).
/// Caller-supplied ids are kept. Fails `input_limit` past the wire limit
/// and `validation_failed` for a body that is not a JSON object.
pub fn with_host_ids(input: &str) -> Result<String, Failure> {
    if input.len() > merezha_domain::MAX_WIRE_BYTES {
        return Err(Failure::permanent("input_limit"));
    }
    let invalid = || Failure::permanent("validation_failed");
    let mut body: Value = serde_json::from_str(input).map_err(|_| invalid())?;
    let Value::Object(fields) = &mut body else {
        return Err(invalid());
    };
    let metadata = fields
        .entry("metadata")
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    if let Value::Object(metadata) = metadata {
        if !metadata.contains_key("message_id") {
            metadata.insert("message_id".into(), Value::String(random_id()?));
        }
        if !metadata.contains_key("correlation_id") {
            let correlation = match merezha_telemetry::propagation::StoredContext::current() {
                Some(context) => context.span_context().trace_id().to_string(),
                None => random_id()?,
            };
            metadata.insert("correlation_id".into(), Value::String(correlation));
        }
    }
    Ok(body.to_string())
}
/// 128 bits from the CSPRNG as 32 lowercase hex digits.
fn random_id() -> Result<String, Failure> {
    use merezha_platform::Random;
    let mut bytes = [0u8; 16];
    merezha_platform::OsRandom.fill(&mut bytes)?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
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
