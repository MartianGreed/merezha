//! Health (TE §5): `/health/live`, `/health/startup` and `/health/ready`.
//!
//! [`Lifecycle`] holds the host's state: starting or started, draining, and
//! the last readiness evaluation. [`Readiness`] evaluates the checks (the
//! pool answers `SELECT 1`, migrations are current, every module's
//! `ready()`), each within one second, in a background task every
//! [`READINESS_INTERVAL`]; probes read the cached result and never touch
//! the database. Module checks run concurrently, each on a blocking thread
//! of its own, so a check that panics reports `internal` and one that
//! blocks its thread reports `timeout` without stalling the others. An
//! evaluation older than [`staleness_bound`] counts as not ready, so a
//! refresh that stops can never leave readiness at 200. Responses carry
//! only a status word; failing checks are logged as `health.not_ready` and
//! shown by `/v1/admin/status`.
use crate::module::Module;
use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use merezha_postgres::{Database, MigrationMode, ModuleMigrations, run_migrations};
use serde::Serialize;
use serde_json::json;
use std::{
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

/// Bound on each readiness check.
pub const CHECK_TIMEOUT: Duration = Duration::from_secs(1);
/// How often the background task re-evaluates readiness.
pub const READINESS_INTERVAL: Duration = Duration::from_secs(2);
/// The code of a passing check.
const OK: &str = "ok";
/// Slack on top of the longest evaluation in [`staleness_bound`].
const STALENESS_MARGIN: Duration = Duration::from_secs(1);
/// The age past which an evaluation no longer counts and readiness is 503:
/// the refresh `interval` plus the longest evaluation (the database check,
/// the migration re-verification and the concurrent module checks, each
/// bounded by [`CHECK_TIMEOUT`]) plus a margin. With the defaults, 6 s.
pub const fn staleness_bound(interval: Duration) -> Duration {
    interval
        .saturating_add(Duration::from_secs(3 * CHECK_TIMEOUT.as_secs()))
        .saturating_add(STALENESS_MARGIN)
}
/// [`Readiness::check`] calls within this window share one evaluation.
const CACHE_FOR: Duration = Duration::from_millis(250);

/// One readiness check as `/v1/admin/status` shows it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CheckResult {
    /// `database` or `migrations`.
    pub name: &'static str,
    pub ok: bool,
    /// `ok` or the failure code.
    pub code: String,
    pub duration_ms: u64,
    /// RFC 3339, UTC.
    pub checked_at: String,
}
/// One module's readiness contribution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ModuleReadiness {
    pub name: &'static str,
    pub ready: bool,
    /// `ok`, the module's failure code or `timeout`.
    pub code: String,
}
/// The result of one readiness evaluation.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Evaluation {
    pub checks: Vec<CheckResult>,
    pub modules: Vec<ModuleReadiness>,
}
impl Evaluation {
    /// Every check and every module passed.
    pub fn passed(&self) -> bool {
        self.checks.iter().all(|c| c.ok) && self.modules.iter().all(|m| m.ready)
    }
    /// The failing check and module names with their codes, in order.
    fn failures(&self) -> Vec<(String, String)> {
        self.checks
            .iter()
            .filter(|c| !c.ok)
            .map(|c| (c.name.to_owned(), c.code.clone()))
            .chain(
                self.modules
                    .iter()
                    .filter(|m| !m.ready)
                    .map(|m| (format!("module:{}", m.name), m.code.clone())),
            )
            .collect()
    }
}

/// When startup completed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Completed {
    pub at: jiff::Timestamp,
    pub duration: Duration,
}

/// The host's lifecycle state, shared by the early listener, the host,
/// the health routes and the status endpoint.
#[derive(Debug)]
pub struct Lifecycle {
    created: Instant,
    created_at: jiff::Timestamp,
    completed: Mutex<Option<Completed>>,
    started: AtomicBool,
    draining: AtomicBool,
    /// The last evaluation and when it finished.
    evaluation: Mutex<Option<(Instant, Evaluation)>>,
    /// Older evaluations count as not ready ([`staleness_bound`]).
    max_age: Mutex<Duration>,
    /// Whether the last logged evaluation passed; starts `true`, so the
    /// first failure logs `health.not_ready` and a first success logs
    /// nothing.
    reported: AtomicBool,
}
impl Lifecycle {
    /// A host that is starting, from now.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            created: Instant::now(),
            created_at: jiff::Timestamp::now(),
            completed: Mutex::new(None),
            started: AtomicBool::new(false),
            draining: AtomicBool::new(false),
            evaluation: Mutex::new(None),
            max_age: Mutex::new(staleness_bound(READINESS_INTERVAL)),
            reported: AtomicBool::new(true),
        })
    }
    /// When this lifecycle began: the process start in `merezha run`.
    pub fn created_at(&self) -> jiff::Timestamp {
        self.created_at
    }
    pub fn uptime(&self) -> Duration {
        self.created.elapsed()
    }
    /// Startup completed: configuration valid, migrations applied or
    /// verified, the catalog compiled and every module's `start()`
    /// returned.
    pub fn started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }
    pub fn completed(&self) -> Option<Completed> {
        *self
            .completed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
    /// Mark startup complete and log `startup.completed`; later calls do
    /// nothing.
    pub(crate) fn complete(&self) {
        if self.started.swap(true, Ordering::SeqCst) {
            return;
        }
        let duration = self.created.elapsed();
        *self
            .completed
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Completed {
            at: jiff::Timestamp::now(),
            duration,
        });
        merezha_telemetry::event!(
            INFO,
            "startup.completed",
            duration_ms = duration.as_millis() as u64
        );
    }
    pub fn draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }
    /// Report not ready from now on, so balancers stop routing during drain.
    pub fn drain(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }
    /// Started, not draining, and the last evaluation passed and is
    /// younger than [`Lifecycle::max_age`].
    pub fn ready(&self) -> bool {
        let max_age = self.max_age();
        self.started()
            && !self.draining()
            && self
                .evaluation
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_ref()
                .is_some_and(|(at, evaluation)| at.elapsed() <= max_age && evaluation.passed())
    }
    /// The age past which an evaluation no longer counts.
    pub fn max_age(&self) -> Duration {
        *self.max_age.lock().unwrap_or_else(PoisonError::into_inner)
    }
    pub(crate) fn set_max_age(&self, max_age: Duration) {
        *self.max_age.lock().unwrap_or_else(PoisonError::into_inner) = max_age;
    }
    /// Whether the last evaluation is older than [`Lifecycle::max_age`]
    /// (or none ran): the refresh has stopped or stalls.
    pub fn stale(&self) -> bool {
        let max_age = self.max_age();
        self.evaluation
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .is_none_or(|(at, _)| at.elapsed() > max_age)
    }
    /// The last readiness evaluation, if any ran.
    pub fn evaluation(&self) -> Option<Evaluation> {
        self.evaluation
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|(_, evaluation)| evaluation.clone())
    }
    /// Keep `evaluation` and log a transition: `health.not_ready` with the
    /// failing check names and the first code, or `health.ready`.
    fn record(&self, evaluation: Evaluation) {
        self.record_at(Instant::now(), evaluation);
    }
    fn record_at(&self, at: Instant, evaluation: Evaluation) {
        let passed = evaluation.passed();
        let failures = evaluation.failures();
        *self
            .evaluation
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some((at, evaluation));
        if self.reported.swap(passed, Ordering::SeqCst) == passed {
            return;
        }
        if passed {
            merezha_telemetry::event!(INFO, "health.ready");
        } else {
            let checks = failures
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>()
                .join(",");
            let (check, code) = failures
                .first()
                .map(|(name, code)| (name.as_str(), code.as_str()))
                .unwrap_or(("none", "ok"));
            merezha_telemetry::event!(
                WARN,
                "health.not_ready",
                check,
                checks = checks.as_str(),
                code
            );
        }
    }
}

/// The readiness checks of a started host. The host refreshes them every
/// [`READINESS_INTERVAL`] into its [`Lifecycle`].
pub struct Readiness {
    database: Database,
    modules: Vec<Arc<dyn Module>>,
    /// Per module: a check still running on its blocking thread.
    in_flight: Vec<Arc<AtomicBool>>,
    migrations_current: AtomicBool,
    lifecycle: Arc<Lifecycle>,
    last: tokio::sync::Mutex<Option<Instant>>,
}
impl Readiness {
    pub fn new(
        database: Database,
        modules: Vec<Arc<dyn Module>>,
        migrations_current: bool,
        lifecycle: Arc<Lifecycle>,
    ) -> Self {
        Self {
            database,
            in_flight: modules
                .iter()
                .map(|_| Arc::new(AtomicBool::new(false)))
                .collect(),
            modules,
            migrations_current: AtomicBool::new(migrations_current),
            lifecycle,
            last: tokio::sync::Mutex::new(None),
        }
    }
    pub fn lifecycle(&self) -> &Arc<Lifecycle> {
        &self.lifecycle
    }
    /// Report not ready from now on (the host's [`Lifecycle::drain`]).
    pub fn drain(&self) {
        self.lifecycle.drain();
    }
    /// Whether every declared migration is applied, as last verified.
    pub fn migrations_current(&self) -> bool {
        self.migrations_current.load(Ordering::SeqCst)
    }
    /// Evaluate every check now, sharing a result less than 250 ms old with
    /// concurrent callers, and return whether the host is ready. Probes do
    /// not call this; they read [`Lifecycle::ready`].
    pub async fn check(&self) -> bool {
        let mut last = self.last.lock().await;
        if last.is_none_or(|at| at.elapsed() >= CACHE_FOR) {
            self.lifecycle.record(self.evaluate().await);
            *last = Some(Instant::now());
        }
        self.lifecycle.ready()
    }
    async fn evaluate(&self) -> Evaluation {
        let mut checks = Vec::new();
        let started = Instant::now();
        // Module checks start first and run while the database is checked.
        let pending: Vec<_> = self
            .modules
            .iter()
            .zip(&self.in_flight)
            .map(|(module, in_flight)| {
                (
                    module.name(),
                    tokio::spawn(module_check(module.clone(), in_flight.clone())),
                )
            })
            .collect();
        let database =
            tokio::time::timeout(CHECK_TIMEOUT, self.database.ping(started + CHECK_TIMEOUT))
                .await
                .unwrap_or_else(|_| Err(merezha_domain::Failure::transient("timeout")));
        checks.push(result("database", database.as_ref().err(), started));
        if database.is_ok() && !self.migrations_current() {
            // Another host may have applied them since startup: re-verify.
            let started = Instant::now();
            let declared: Vec<ModuleMigrations<'_>> = self
                .modules
                .iter()
                .map(|m| ModuleMigrations {
                    module: m.name(),
                    migrations: m.migrations(),
                })
                .collect();
            let verified = tokio::time::timeout(
                CHECK_TIMEOUT,
                run_migrations(
                    &self.database,
                    &declared,
                    MigrationMode::Verify,
                    started + CHECK_TIMEOUT,
                ),
            )
            .await;
            let failure = match verified {
                Ok(Ok(report)) if report.is_current() => {
                    self.migrations_current.store(true, Ordering::SeqCst);
                    None
                }
                Ok(Ok(_)) => Some(merezha_domain::Failure::transient("migrations_pending")),
                Ok(Err(error)) => Some(error.failure),
                Err(_) => Some(merezha_domain::Failure::transient("timeout")),
            };
            checks.push(result("migrations", failure.as_ref(), started));
        } else if !self.migrations_current() {
            checks.push(CheckResult {
                name: "migrations",
                ok: false,
                code: "migrations_pending".to_owned(),
                duration_ms: 0,
                checked_at: jiff::Timestamp::now().to_string(),
            });
        }
        let mut modules = Vec::new();
        for (name, check) in pending {
            let code = check.await.unwrap_or_else(|_| INTERNAL.to_owned());
            modules.push(ModuleReadiness {
                name,
                ready: code == OK,
                code,
            });
        }
        Evaluation { checks, modules }
    }
}

/// The code of a check that panicked.
const INTERNAL: &str = "internal";
/// The code of a check that did not answer within [`CHECK_TIMEOUT`].
const TIMEOUT: &str = "timeout";
/// Clears a module's in-flight flag when its check ends, unwinding
/// included.
struct InFlight(Arc<AtomicBool>);
impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}
/// One module's `ready()` on a blocking thread of its own, bounded by
/// [`CHECK_TIMEOUT`]: a panic reports `internal`, a check that blocks or
/// takes longer reports `timeout`. While a previous check of the module
/// is still running, no other starts and the result is `timeout`, so a
/// stuck check holds at most one thread.
async fn module_check(module: Arc<dyn Module>, in_flight: Arc<AtomicBool>) -> String {
    if in_flight.swap(true, Ordering::SeqCst) {
        return TIMEOUT.to_owned();
    }
    let guard = InFlight(in_flight);
    let runtime = tokio::runtime::Handle::current();
    let task = merezha_telemetry::spawn_blocking_in_span(move || {
        let _guard = guard;
        runtime.block_on(module.ready())
    });
    match tokio::time::timeout(CHECK_TIMEOUT, task).await {
        Ok(Ok(Ok(()))) => OK.to_owned(),
        Ok(Ok(Err(failure))) => failure.code,
        Ok(Err(_)) => INTERNAL.to_owned(),
        Err(_) => TIMEOUT.to_owned(),
    }
}
fn result(
    name: &'static str,
    failure: Option<&merezha_domain::Failure>,
    started: Instant,
) -> CheckResult {
    CheckResult {
        name,
        ok: failure.is_none(),
        code: failure.map_or_else(|| OK.to_owned(), |f| f.code.clone()),
        duration_ms: started.elapsed().as_millis() as u64,
        checked_at: jiff::Timestamp::now().to_string(),
    }
}

/// Re-evaluate `readiness` every `interval` until the host drains or is
/// dropped. The first evaluation is the caller's.
pub(crate) fn spawn_refresh(readiness: &Arc<Readiness>, interval: Duration) {
    let weak = Arc::downgrade(readiness);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            let Some(readiness) = weak.upgrade() else {
                return;
            };
            if readiness.lifecycle.draining() {
                return;
            }
            // Each refresh runs as a task of its own, so a panic ends that
            // refresh only, never the loop.
            let _ = tokio::spawn(async move { readiness.check().await }).await;
        }
    });
}

/// Health routes, mounted by the host outside module prefixes and served
/// by the early listener from the moment it binds.
pub fn router(lifecycle: Arc<Lifecycle>) -> Router {
    Router::new()
        .route(
            "/health/live",
            get(|| async { Json(json!({"status":"live"})) }),
        )
        .route("/health/startup", get(startup))
        .route("/health/ready", get(ready))
        .with_state(lifecycle)
}
async fn startup(State(lifecycle): State<Arc<Lifecycle>>) -> impl IntoResponse {
    if lifecycle.started() {
        (StatusCode::OK, Json(json!({"status":"started"})))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status":"starting"})),
        )
    }
}
async fn ready(State(lifecycle): State<Arc<Lifecycle>>) -> impl IntoResponse {
    if lifecycle.ready() {
        (StatusCode::OK, Json(json!({"status":"ready"})))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status":"not_ready"})),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn passing() -> Evaluation {
        Evaluation {
            checks: vec![CheckResult {
                name: "database",
                ok: true,
                code: OK.to_owned(),
                duration_ms: 1,
                checked_at: jiff::Timestamp::now().to_string(),
            }],
            modules: Vec::new(),
        }
    }

    #[test]
    fn an_evaluation_past_the_staleness_bound_is_not_ready() {
        assert_eq!(staleness_bound(READINESS_INTERVAL), Duration::from_secs(6));
        let lifecycle = Lifecycle::new();
        lifecycle.record(passing());
        assert!(!lifecycle.ready(), "not started yet");
        lifecycle.complete();
        assert!(lifecycle.ready() && !lifecycle.stale());
        // The refresh stopped: the last passing evaluation ages out.
        let max_age = lifecycle.max_age();
        lifecycle.record_at(
            Instant::now() - max_age - Duration::from_millis(1),
            passing(),
        );
        assert!(!lifecycle.ready() && lifecycle.stale());
        lifecycle.set_max_age(max_age * 2);
        assert!(lifecycle.ready());
        lifecycle.drain();
        assert!(!lifecycle.ready());
    }
}
