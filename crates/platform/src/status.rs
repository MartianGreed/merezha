//! `GET /v1/admin/status` (TE §5): the protected diagnostics document.
//!
//! With identity enabled (an [`Authenticator`](crate::ports::Authenticator)
//! is installed), the caller
//! needs [`STATUS_PERMISSION`] at [`ResourceScope::Application`] (R5);
//! `platform_admin` passes as it passes every check. Without identity, the
//! document is served only on a private listener (`http.public = false`, a
//! loopback `http.listen`, no `http.trusted_proxies`: the allow-all trust
//! model) to a loopback TCP peer, and is `404 not_found` otherwise.
//!
//! One request runs at a time; a concurrent one gets `429 rate_limited`
//! with `Retry-After: 1`. Each section is read within 2 s. Responses carry
//! `Cache-Control: no-store`. Every field is a code, a count, a configured
//! name or a timestamp. The `jobs` section is `JobsHandle::queue_status()`
//! as returned, and other subsystems (the application, the catalog,
//! telemetry) add theirs as [`StatusSection`]s: this endpoint never queries
//! their tables itself (R10).
use crate::{
    BoxFuture,
    edge::{Authenticated, ErrorReply, error_response},
    health::{Lifecycle, Readiness},
    ports::{Authorizer, JobsHandle, Permission, ResourceScope},
};
use axum::{
    Router,
    extract::{ConnectInfo, Request, State},
    http::{HeaderValue, header},
    response::{IntoResponse, Response},
    routing::get,
};
use merezha_domain::Failure;
use merezha_postgres::{Database, PendingMigration};
use serde_json::{Map, Value, json};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

/// The route of the status document.
pub const STATUS_PATH: &str = "/v1/admin/status";
/// The `Application`-scope permission the status document requires when
/// identity is enabled (TE §5, R5).
pub const STATUS_PERMISSION: &str = "system.status.read";
/// Bound on reading each section.
pub const SECTION_TIMEOUT: Duration = Duration::from_secs(2);
/// Sections the host writes itself; a [`StatusSection`] cannot take them.
pub const HOST_SECTIONS: [&str; 6] = [
    "service",
    "startup",
    "readiness",
    "database",
    "jobs",
    "modules",
];

/// A section of the status document owned by another subsystem, such as
/// `application` (AP's `Application::status()`), `catalog` (the runtime)
/// or `telemetry` (the exporter accounts). Its document is embedded as
/// returned; it must hold only codes, counts, configured names and
/// timestamps.
pub trait StatusSection: Send + Sync {
    /// The member name, `[a-z][a-z0-9_]*`, not one of [`HOST_SECTIONS`].
    fn name(&self) -> &'static str;
    /// The section document, read within [`SECTION_TIMEOUT`].
    fn read(&self) -> BoxFuture<'_, Result<Value, Failure>>;
}

/// The service identity the document reports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceInfo {
    pub name: String,
    pub version: String,
    pub instance_id: String,
}

/// Everything the status route reads.
pub(crate) struct StatusState {
    pub lifecycle: Arc<Lifecycle>,
    pub readiness: Arc<Readiness>,
    pub database: Database,
    /// Each module with its declared migration count, in start order.
    pub modules: Vec<(&'static str, usize)>,
    /// Migrations pending at startup.
    pub pending: Vec<PendingMigration>,
    pub service: ServiceInfo,
    /// The allow-all trust model: a private listener.
    pub private: bool,
    /// Identity is enabled: an authenticator is installed.
    pub authenticated: bool,
    pub authorizer: Arc<dyn Authorizer>,
    pub jobs: Option<JobsHandle>,
    pub sections: Vec<Arc<dyn StatusSection>>,
    pub gate: Semaphore,
}

/// The status route, outside module prefixes.
pub(crate) fn router(state: Arc<StatusState>) -> Router {
    Router::new()
        .route(STATUS_PATH, get(status))
        .with_state(state)
}

/// Whether the caller may read the document, or the refusal.
fn admit(state: &StatusState, request: &Request) -> Result<(), Failure> {
    if state.authenticated {
        let principal = request
            .extensions()
            .get::<Authenticated>()
            .and_then(|a| a.0.clone());
        let permission = Permission::new(STATUS_PERMISSION)?;
        return state
            .authorizer
            .check(
                principal.as_deref(),
                &permission,
                &ResourceScope::Application,
            )
            .map(|_| ())
            .map_err(|denied| denied.failure());
    }
    let loopback_peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .is_some_and(|peer| peer.0.ip().to_canonical().is_loopback());
    if state.private && loopback_peer {
        Ok(())
    } else {
        Err(Failure::permanent("not_found"))
    }
}

async fn status(State(state): State<Arc<StatusState>>, request: Request) -> Response {
    if let Err(refused) = admit(&state, &request) {
        return error_response(&refused);
    }
    let Ok(_permit) = state.gate.try_acquire() else {
        return ErrorReply::new(Failure::transient("rate_limited"))
            .retry_after(Duration::from_secs(1))
            .into_response();
    };
    let mut document = Map::new();
    document.insert("service".into(), service(&state));
    document.insert("startup".into(), startup(&state.lifecycle));
    document.insert("readiness".into(), readiness(&state.lifecycle));
    document.insert("database".into(), database(&state));
    if let Some(jobs) = &state.jobs {
        let read = async { serde_json::to_value(jobs.queue_status().await?).map_err(internal) };
        document.insert("jobs".into(), bounded(read).await);
    }
    for section in &state.sections {
        let name = section.name();
        if HOST_SECTIONS.contains(&name) || document.contains_key(name) {
            continue;
        }
        document.insert(name.into(), bounded(section.read()).await);
    }
    document.insert("modules".into(), modules(&state.lifecycle));
    let mut response = axum::Json(Value::Object(document)).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn internal(_: serde_json::Error) -> Failure {
    Failure::transient("internal")
}
/// A section read within [`SECTION_TIMEOUT`]; a failure is shown as its
/// code only.
async fn bounded(read: impl Future<Output = Result<Value, Failure>>) -> Value {
    match tokio::time::timeout(SECTION_TIMEOUT, read).await {
        Ok(Ok(value)) => value,
        Ok(Err(failure)) => json!({"code": failure.code}),
        Err(_) => json!({"code": "timeout"}),
    }
}

fn service(state: &StatusState) -> Value {
    json!({
        "name": state.service.name,
        "version": state.service.version,
        "host_version": merezha_telemetry::Service::HOST_VERSION,
        "instance_id": state.service.instance_id,
        "started_at": state.lifecycle.created_at().to_string(),
        "uptime_s": state.lifecycle.uptime().as_secs(),
    })
}
fn startup(lifecycle: &Lifecycle) -> Value {
    match lifecycle.completed() {
        Some(completed) => json!({
            "state": "started",
            "completed_at": completed.at.to_string(),
            "duration_ms": completed.duration.as_millis() as u64,
        }),
        None => json!({"state": "starting", "completed_at": null, "duration_ms": null}),
    }
}
fn readiness(lifecycle: &Lifecycle) -> Value {
    let checks = lifecycle.evaluation().map(|e| e.checks).unwrap_or_default();
    json!({
        "ready": lifecycle.ready(),
        "draining": lifecycle.draining(),
        "checks": checks,
    })
}
fn database(state: &StatusState) -> Value {
    let pool = state.database.status();
    let current = state.readiness.migrations_current();
    let migrations: Vec<Value> = state
        .modules
        .iter()
        .map(|(module, declared)| {
            let pending = if current {
                0
            } else {
                state.pending.iter().filter(|p| p.module == *module).count()
            };
            json!({
                "module": module,
                "applied": declared.saturating_sub(pending),
                "pending": pending,
            })
        })
        .collect();
    json!({
        "pool": {
            "max": pool.max_size,
            "size": pool.size,
            "idle": pool.available,
            "waiting": pool.waiting,
        },
        "migrations": migrations,
    })
}
fn modules(lifecycle: &Lifecycle) -> Value {
    let modules = lifecycle
        .evaluation()
        .map(|e| e.modules)
        .unwrap_or_default();
    json!(modules)
}
