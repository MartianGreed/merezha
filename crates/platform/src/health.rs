//! `/health/live` and `/health/ready`. Responses carry only a status word;
//! failing checks are logged as `health.not_ready` with their names.
use crate::module::Module;
use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use merezha_postgres::{Database, MigrationMode, ModuleMigrations, run_migrations};
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

/// Bound on each readiness check.
pub const CHECK_TIMEOUT: Duration = Duration::from_secs(1);
/// Concurrent probes within this window share one evaluation.
const CACHE_FOR: Duration = Duration::from_millis(250);

/// Inputs to readiness, shared with the host.
pub struct Readiness {
    database: Database,
    modules: Vec<Arc<dyn Module>>,
    migrations_current: AtomicBool,
    draining: AtomicBool,
    last: Mutex<Option<(Instant, bool)>>,
    reported: AtomicBool,
}
impl Readiness {
    pub fn new(
        database: Database,
        modules: Vec<Arc<dyn Module>>,
        migrations_current: bool,
    ) -> Self {
        Self {
            database,
            modules,
            migrations_current: AtomicBool::new(migrations_current),
            draining: AtomicBool::new(false),
            last: Mutex::new(None),
            reported: AtomicBool::new(true),
        }
    }
    /// Report not ready from now on, so balancers stop routing during drain.
    pub fn drain(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }
    /// Evaluate every check, sharing a recent result with concurrent probes.
    pub async fn check(&self) -> bool {
        let mut last = self.last.lock().await;
        if let Some((at, ready)) = *last
            && at.elapsed() < CACHE_FOR
        {
            return ready;
        }
        let failed = self.failed_checks().await;
        let ready = failed.is_empty();
        // Log transitions only, so a polling balancer cannot flood the log.
        if self.reported.swap(ready, Ordering::SeqCst) != ready {
            if ready {
                tracing::info!(event = "health.ready");
            } else {
                tracing::warn!(event = "health.not_ready", checks = ?failed);
            }
        }
        *last = Some((Instant::now(), ready));
        ready
    }
    async fn failed_checks(&self) -> Vec<String> {
        let mut failed = Vec::new();
        if self.draining.load(Ordering::SeqCst) {
            failed.push("draining".to_owned());
        }
        let deadline = Instant::now() + CHECK_TIMEOUT;
        if self.database.ping(deadline).await.is_err() {
            failed.push("database".to_owned());
        } else if !self.migrations_current.load(Ordering::SeqCst) {
            // Another host may have applied them since startup: re-verify.
            let declared: Vec<ModuleMigrations<'_>> = self
                .modules
                .iter()
                .map(|m| ModuleMigrations {
                    module: m.name(),
                    migrations: m.migrations(),
                })
                .collect();
            match run_migrations(&self.database, &declared, MigrationMode::Verify, deadline).await {
                Ok(report) if report.is_current() => {
                    self.migrations_current.store(true, Ordering::SeqCst);
                }
                _ => failed.push("migrations".to_owned()),
            }
        }
        for module in &self.modules {
            let at = tokio::time::Instant::from_std(Instant::now() + CHECK_TIMEOUT);
            match tokio::time::timeout_at(at, module.ready()).await {
                Ok(Ok(())) => {}
                _ => failed.push(format!("module:{}", module.name())),
            }
        }
        failed
    }
}

/// Health routes. Mounted by the host outside module prefixes.
pub fn router(readiness: Arc<Readiness>) -> Router {
    Router::new()
        .route(
            "/health/live",
            get(|| async { Json(json!({"status":"live"})) }),
        )
        .route("/health/ready", get(ready))
        .with_state(readiness)
}
async fn ready(State(readiness): State<Arc<Readiness>>) -> impl IntoResponse {
    if readiness.check().await {
        (StatusCode::OK, Json(json!({"status":"ready"})))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status":"not_ready"})),
        )
    }
}
