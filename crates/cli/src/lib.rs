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
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
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
pub mod application_cli;
pub mod application_http;
pub mod application_module;
pub mod content;
pub mod guest;
pub mod identity;
pub mod notifications;
pub mod reactions;
pub mod run;
pub mod telemetry;

/// The blocks the `merezha` binary registers. Each stays disabled until
/// its `[modules.<name>]` section is present. The notifications and
/// identity blocks read the facts their cross-section checks need from the
/// same `input`.
pub fn modules(input: &merezha_platform::ConfigInput) -> Vec<Box<dyn merezha_platform::Module>> {
    host_blocks(input).0
}

/// [`modules`] and the identity block's handle, whose session
/// authenticator the host installs when `[modules.identity]` is present.
pub fn host_blocks(
    input: &merezha_platform::ConfigInput,
) -> (
    Vec<Box<dyn merezha_platform::Module>>,
    merezha_identity::IdentityHandle,
) {
    let identity = identity_module(input);
    let handle = identity.handle();
    (
        blocks_with(input, merezha_content::ContentModule::new(), identity),
        handle,
    )
}

/// [`modules`] with the given content module, so the caller keeps its
/// [`merezha_content::ContentHandle`] (`merezha content diff`).
pub fn blocks(
    input: &merezha_platform::ConfigInput,
    content: merezha_content::ContentModule,
) -> Vec<Box<dyn merezha_platform::Module>> {
    blocks_with(input, content, identity_module(input))
}

/// The identity block with the full permission catalog of this binary
/// (identity section 5): identity's and the host's own permissions, the
/// content and notifications blocks' lists, content's `content_type`
/// resource kind, and the application's `[[permissions]]` and route
/// `resource` kinds when `input` names an application configuration that
/// reads (AP §C.4; its problems stop startup on their own, listed at once).
pub fn identity_module(input: &merezha_platform::ConfigInput) -> merezha_identity::IdentityModule {
    let mut identity = blocks_catalog(merezha_identity::HostFacts::load(input));
    if let Some(application) = merezha_platform::ProjectConfig::load(input)
        .ok()
        .and_then(|project| project.application)
        .and_then(|section| read_application_document(&section.path).ok())
    {
        add_application(&mut identity, &application);
    }
    identity
}
/// The resource kinds the blocks of this binary declare; a route's
/// `resource` cannot name one (`block_resource_kind`).
pub const BLOCK_RESOURCE_KINDS: &[&str] = &[merezha_content::RESOURCE_KIND];
/// The identity block with the blocks' permissions and resource kinds.
fn blocks_catalog(facts: Option<merezha_identity::HostFacts>) -> merezha_identity::IdentityModule {
    let mut identity = merezha_identity::IdentityModule::new(facts);
    let added = identity
        .add_block_permissions("content", &merezha_content::CONTENT_PERMISSIONS)
        .and_then(|()| {
            identity.add_block_permissions("notifications", &merezha_notifications::PERMISSIONS)
        });
    debug_assert!(added.is_ok(), "block permissions follow the grammar");
    identity.add_resource_kinds(BLOCK_RESOURCE_KINDS);
    identity
}
/// The application's `[[permissions]]` and route `resource` kinds join the
/// catalog. Names off the grammar or in a block's namespace are left out:
/// the application's own validation refuses them (`invalid_permission`,
/// `block_prefix`).
fn add_application(
    identity: &mut merezha_identity::IdentityModule,
    application: &merezha_application::config::Config,
) {
    use merezha_application::config::{BLOCK_PREFIXES, valid_permission};
    let added = identity.add_application_permissions(
        application
            .permissions
            .iter()
            .flatten()
            .filter(|p| {
                let name = p.name.as_str();
                valid_permission(name)
                    && !BLOCK_PREFIXES.contains(&name.split('.').next().unwrap_or(""))
            })
            .map(|p| (p.name.as_str(), p.description.as_str())),
    );
    debug_assert!(added.is_ok(), "filtered application permissions");
    // Routes check resources at tenant scope only, so an application-level
    // grant on these kinds could never take effect.
    let kinds: Vec<&str> = application.resource_kinds().into_iter().collect();
    identity.add_tenant_resource_kinds(&kinds);
}
/// This binary's permission catalog with `application`'s permissions: what
/// a route `permission` must name (AP §C.4, `unknown_permission`).
pub fn permission_catalog(
    application: &merezha_application::config::Config,
) -> merezha_identity::Catalog {
    let mut identity = blocks_catalog(None);
    add_application(&mut identity, application);
    identity.catalog().clone()
}

fn blocks_with(
    input: &merezha_platform::ConfigInput,
    content: merezha_content::ContentModule,
    identity: merezha_identity::IdentityModule,
) -> Vec<Box<dyn merezha_platform::Module>> {
    // Organization content types need identity's organizations (CT-C9);
    // `delivery.public_base_url` may be plain HTTP on a private loopback
    // listener only (issue #46).
    let content = content
        .with_organizations(identity_configured(input))
        .with_private_loopback(private_loopback(input));
    vec![
        Box::new(identity),
        Box::new(content),
        Box::new(merezha_notifications::Notifications::new(
            merezha_notifications::HostFacts::load(input),
        )),
    ]
}

/// Whether `[modules.identity]` is present, which enables the identity
/// block and with it the organizations content's organization types are
/// scoped to. A configuration that does not load enables nothing; the
/// kernel reports why.
fn identity_configured(input: &merezha_platform::ConfigInput) -> bool {
    merezha_platform::ProjectConfig::load(input)
        .is_ok_and(|config| config.modules.contains_key(merezha_identity::NAME))
}

/// Whether the host listens on a private loopback address, by identity's
/// rule ([`merezha_identity::HostFacts::private_loopback`]: a loopback
/// `http.listen`, `http.public = false`, no `http.trusted_proxies`). A
/// configuration that does not load is not private; the kernel reports
/// why.
fn private_loopback(input: &merezha_platform::ConfigInput) -> bool {
    merezha_identity::HostFacts::load(input).is_some_and(|facts| facts.private_loopback())
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
    let config = read_application_config(path)?;
    // Route permissions must name a catalog permission (AP §C.4); `serve`
    // has no identity block, so undeclared routes stay allowed.
    let catalog = permission_catalog(&config);
    let mut issues = config.host_issues(
        &|permission| catalog.contains(permission),
        &|_| false,
        false,
    );
    issues.extend(config.block_kind_issues(BLOCK_RESOURCE_KINDS));
    if let Some(issue) = issues.first() {
        return Err(merezha_domain::Failure::permanent(issue.code));
    }
    merezha_application::Application::open(config, Arc::new(runtime), authorizer)
}
/// Read and validate an application configuration file. Blocking. Fails
/// `invalid_application_config` for an unreadable or oversized file, or the
/// configuration codes of the application layer.
pub fn read_application_config(
    path: &std::path::Path,
) -> Result<merezha_application::config::Config, merezha_domain::Failure> {
    let config = read_application_document(path)?;
    config.validate()?;
    Ok(config)
}
/// [`read_application_config`] without validation, for `merezha run`,
/// which lists every access issue of the document before refusing it.
pub fn read_application_document(
    path: &std::path::Path,
) -> Result<merezha_application::config::Config, merezha_domain::Failure> {
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
    merezha_application::config::Config::decode(&text)
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
    let result = merezha_telemetry::spawn_blocking_in_span(move || invocation.run(&input)).await;
    let result = match result {
        Ok(result) => result,
        Err(_) => Err(Error::Internal),
    };
    let (code, output_bytes) = match &result {
        Ok(o) => ("ok", o.output.len()),
        Err(e) => (e.code(), 0),
    };
    let duration_ms = started.elapsed().as_millis() as u64;
    let (input_bytes, output_bytes) = (input_bytes as u64, output_bytes as u64);
    // Debug for successes and client errors, warn for limits, error when
    // the host broke its own contract (TE §4).
    match code {
        "internal" => merezha_telemetry::event!(
            ERROR,
            "invocation.completed",
            request_id,
            generation,
            duration_ms,
            input_bytes,
            output_bytes,
            code
        ),
        "input_limit" | "output_limit" | "fuel_exhausted" | "memory_limit" | "timeout" | "trap"
        | "overloaded" => merezha_telemetry::event!(
            WARN,
            "invocation.completed",
            request_id,
            generation,
            duration_ms,
            input_bytes,
            output_bytes,
            code
        ),
        _ => merezha_telemetry::event!(
            DEBUG,
            "invocation.completed",
            request_id,
            generation,
            duration_ms,
            input_bytes,
            output_bytes,
            code
        ),
    }
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
    let result = merezha_telemetry::spawn_blocking_in_span(move || {
        let _permit = permit;
        state.runtime.reload()
    })
    .await;
    match result {
        Ok(Ok(generation)) => {
            merezha_telemetry::event!(INFO, "registry.reloaded", generation, code = "ok");
            Json(json!({"generation":generation})).into_response()
        }
        Ok(Err(_)) => {
            merezha_telemetry::event!(WARN, "registry.reload_failed", code = "invalid_manifest");
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
