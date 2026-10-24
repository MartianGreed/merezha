//! Loopback HTTP transport for the component runtime.
use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Path, Request, State},
    http::{HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use merezha_runtime::{Error, Invocation, Runtime};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;

#[derive(Clone)]
struct AppState {
    runtime: Runtime,
    reload: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    next_id: Arc<AtomicU64>,
}

/// Build the transport. Callers must bind their listener to numeric loopback.
/// The executable enforces that boundary before calling this function.
pub fn app(runtime: Runtime) -> Router {
    let state = AppState {
        runtime,
        reload: Arc::new(Semaphore::new(1)),
        requests: Arc::new(Semaphore::new(128)),
        next_id: Arc::new(AtomicU64::new(1)),
    };
    Router::new()
        .route("/health", get(health))
        .route("/v1/services", get(catalog))
        .route("/v1/services/{id}/invoke", post(service))
        .route("/v1/pipelines/{id}/invoke", post(pipeline))
        .route("/v1/reload", post(reload))
        .fallback(|| async { failure(Error::NotFound) })
        .layer(middleware::from_fn_with_state(state.clone(), admission))
        .with_state(state)
}

/// Opt-in application endpoints composed with the existing raw transport.
pub fn app_with_application(
    runtime: Runtime,
    application: merezha_application::Application,
) -> Router {
    app(runtime).merge(application_http::router(application, None))
}
pub mod application_http;
pub mod notifications;
pub mod run;

/// The blocks the `merezha` binary registers. Each stays disabled until
/// its `[modules.<name>]` section is present. The notifications block reads
/// the facts its cross-section checks need from the same `input`.
pub fn modules(input: &merezha_platform::ConfigInput) -> Vec<Box<dyn merezha_platform::Module>> {
    vec![Box::new(merezha_notifications::Notifications::new(
        merezha_notifications::HostFacts::load(input),
    ))]
}

/// Read, validate and open an application configuration with the runtime as
/// its guest. Blocking: call it from `spawn_blocking`. Fails
/// `invalid_application_config` for an unreadable or oversized file, or the
/// configuration and storage codes of the application layer.
pub fn open_application(
    path: &std::path::Path,
    runtime: Runtime,
    authorizer: Arc<dyn merezha_application::Authorizer>,
) -> Result<merezha_application::Application, merezha_domain::Failure> {
    use merezha_domain::Failure;
    use std::io::Read;
    let invalid = || Failure::permanent("invalid_application_config");
    if !std::fs::metadata(path).map_err(|_| invalid())?.is_file() {
        return Err(invalid());
    }
    let file = std::fs::File::open(path).map_err(|_| invalid())?;
    if !file.metadata().map_err(|_| invalid())?.is_file() {
        return Err(invalid());
    }
    let mut text = String::new();
    file.take(65_537)
        .read_to_string(&mut text)
        .map_err(|_| invalid())?;
    let config = merezha_application::config::Config::parse(&text)?;
    merezha_application::Application::open(config, Arc::new(runtime), authorizer)
}

async fn admission(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let Ok(_permit) = state.requests.try_acquire() else {
        return failure(Error::Overloaded);
    };
    next.run(request).await
}
async fn health(State(state): State<AppState>) -> Response {
    match state.runtime.catalog() {
        Ok(c) => Json(json!({"status":"ok", "generation":c.generation})).into_response(),
        Err(e) => failure(e),
    }
}
async fn catalog(State(state): State<AppState>) -> Response {
    match state.runtime.catalog() {
        Ok(c) => Json(c).into_response(),
        Err(e) => failure(e),
    }
}
async fn service(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Request,
) -> Response {
    let invocation = state.runtime.prepare_service(&id);
    invoke(state, invocation, request).await
}
async fn pipeline(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Request,
) -> Response {
    let invocation = state.runtime.prepare_pipeline(&id);
    invoke(state, invocation, request).await
}
async fn invoke(
    state: AppState,
    invocation: Result<Invocation, Error>,
    request: Request,
) -> Response {
    let invocation = match invocation {
        Ok(i) => i,
        Err(e) => return failure(e),
    };
    let request_id = state.next_id.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    let generation = invocation.generation();
    let deadline = tokio::time::Instant::from_std(invocation.deadline());
    let bytes = match tokio::time::timeout_at(
        deadline,
        to_bytes(request.into_body(), invocation.limits().max_input_bytes),
    )
    .await
    {
        Ok(Ok(b)) => b,
        Ok(Err(_)) => return failure(Error::InputLimit),
        Err(_) => return failure(Error::Timeout),
    };
    let input_bytes = bytes.len();
    let input = match String::from_utf8(bytes.to_vec()) {
        Ok(s) => s,
        Err(_) => return failure(Error::InvalidInput),
    };
    // Admission already owns the permit, including while a blocking task waits
    // for an executor thread. Dropped HTTP callers cannot detach unbounded work.
    let result = tokio::task::spawn_blocking(move || invocation.run(&input)).await;
    let result = match result {
        Ok(result) => result,
        Err(_) => Err(Error::Internal),
    };
    let (code, output_bytes) = match &result {
        Ok(o) => ("ok", o.output.len()),
        Err(e) => (e.code(), 0),
    };
    emit(
        "invocation.completed",
        json!({"request_id":request_id,"generation":generation,"duration_ms":started.elapsed().as_millis(),"input_bytes":input_bytes,"output_bytes":output_bytes,"code":code}),
    );
    match result {
        Ok(output) => {
            let mut response = output.output.into_response();
            response.headers_mut().insert(
                "x-merezha-generation",
                HeaderValue::from_str(&output.generation.to_string())
                    .expect("u64 is a valid header"),
            );
            response
        }
        Err(error) => failure(error),
    }
}
async fn reload(State(state): State<AppState>, request: Request) -> Response {
    let Ok(permit) = state.reload.clone().try_acquire_owned() else {
        return coded(StatusCode::CONFLICT, "reload_busy");
    };
    match tokio::time::timeout(
        std::time::Duration::from_secs(1),
        to_bytes(request.into_body(), 0),
    )
    .await
    {
        Ok(Ok(_)) => {}
        _ => return failure(Error::InvalidInput),
    }
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        state.runtime.reload()
    })
    .await;
    match result {
        Ok(Ok(generation)) => {
            emit(
                "registry.reloaded",
                json!({"generation":generation,"code":"ok"}),
            );
            Json(json!({"generation":generation})).into_response()
        }
        Ok(Err(_)) => {
            emit("registry.reload_failed", json!({"code":"invalid_manifest"}));
            coded(StatusCode::BAD_REQUEST, "invalid_manifest")
        }
        Err(_) => failure(Error::Internal),
    }
}
fn coded(status: StatusCode, code: &str) -> Response {
    (status, Json(json!({"error":{"code":code}}))).into_response()
}
fn failure(error: Error) -> Response {
    let status = match error {
        Error::InvalidInput => StatusCode::BAD_REQUEST,
        Error::NotFound => StatusCode::NOT_FOUND,
        Error::InputLimit | Error::OutputLimit => StatusCode::PAYLOAD_TOO_LARGE,
        Error::Timeout => StatusCode::GATEWAY_TIMEOUT,
        Error::Overloaded => StatusCode::TOO_MANY_REQUESTS,
        Error::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::UNPROCESSABLE_ENTITY,
    };
    coded(status, error.code())
}
/// Emit allowlisted metadata. Never pass guest input/output or error text here.
pub fn emit(event: &str, mut fields: Value) {
    fields["event"] = json!(event);
    fields["timestamp_unix_ms"] = json!(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    );
    fields["level"] = json!("info");
    fields["service"] = json!("merezha");
    fields["version"] = json!(env!("CARGO_PKG_VERSION"));
    eprintln!("{fields}");
}
