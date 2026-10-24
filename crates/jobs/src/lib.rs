//! Jobs and the outbox (AP §B), schema `merezha_jobs`.
//!
//! [`PgJobs`] is the jobs runtime the host installs
//! ([`merezha_platform::JobsRuntime`]): the PostgreSQL [`PgOutbox`] behind
//! the kernel `Outbox` port, the `Jobs` port behind every module's
//! `JobsHandle` (enqueue, cancel, register, subscribe, queue status), the
//! relay that turns committed outbox records into jobs, the workers that
//! lease and run them (dispatch, heartbeat, reaper, retry, drain), retention
//! and the admin API under `/v1/jobs/admin/`.
//!
//! Seam for AP-B3: cron adds `JobsHandle::schedule`, the scheduler tick and
//! the schedule admin routes beside [`PgJobs::admin_router`]'s.
pub mod admin;
mod outbox;
mod relay;
mod retention;
mod retry;
mod routes;
mod schema;
mod validate;
mod worker;

pub use admin::{
    AttemptRecord, BulkReplay, DeadFilter, JobDetail, JobFilter, JobPage, JobSummary,
    REISSUE_CLAIM_TIMEOUT, REISSUE_DEADLINE, Replay, ReplayProgress,
};
pub use outbox::{OUTBOX_CHANNEL, PgOutbox};
pub use relay::{JOBS_CHANNEL, RelayReport};
pub use retention::{RETENTION_INTERVAL, RetentionReport};

use merezha_domain::{Failure, Identity};
use merezha_platform::{
    AppContext, BoxFuture, Database, ErrorCode, JobsPorts, JobsRuntime, Migration, Module,
    config::{JobsConfig, SubscriptionsConfig},
    ports::{Enqueued, JobRegistration, Jobs, NewJob, QueueStatus, Source},
};
use merezha_postgres::{Listener, classify};
use relay::RelaySettings;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use worker::{QueuePlan, Worker, WorkerConfig};

/// Codes this runtime returns, registered at startup (R8).
pub const ERRORS: &[ErrorCode] = &[
    ErrorCode::new("invalid_topic", 422),
    ErrorCode::new("invalid_queue", 422),
    ErrorCode::new("invalid_run_at", 422),
    ErrorCode::new("invalid_event_envelope", 422),
    ErrorCode::new("invalid_record_data", 422),
    ErrorCode::new("job_payload_limit", 422),
    ErrorCode::new("kind_already_registered", 422),
    ErrorCode::new("kind_not_registered", 422),
    ErrorCode::new("too_many_kinds", 422),
    ErrorCode::new("jobs_started", 422),
    ErrorCode::new("not_quarantined", 409),
    ErrorCode::new("not_dead", 409),
    ErrorCode::new("not_cancellable", 409),
    ErrorCode::new("jobs_not_ready", 503),
];
/// Per-instance slots of a queue a registered kind names but `[jobs.queues]`
/// does not configure (AP §D.2).
pub const UNCONFIGURED_CONCURRENCY: u32 = 2;
/// Most registered kinds and subscriptions each.
pub const MAX_KINDS: usize = 1024;
/// Most batches one wake-up relays before it checks for shutdown.
const MAX_BATCHES_PER_WAKE: usize = 64;
/// Bound on enqueue, cancel, requeue and subscription sync.
const OPERATION_DEADLINE: Duration = Duration::from_secs(10);
/// How often an idle `LISTEN` session is probed with `SELECT 1`.
pub const LISTEN_CHECK_INTERVAL: Duration = Duration::from_secs(30);
/// Bound on opening and probing the `LISTEN` session.
const LISTEN_DEADLINE: Duration = Duration::from_secs(5);
/// The process-test hook (feature `test-hooks`): `off` keeps the relay
/// stopped, `interval` relays on the interval only.
pub const TEST_RELAY_ENV: &str = "MEREZHA_JOBS_TEST_RELAY";
/// The process-test hook (feature `test-hooks`): a number of milliseconds
/// registers kind `test.sleep` on queue `default` (source `jobs`), whose
/// handler sleeps that long, ignoring `cancel`, and succeeds.
pub const TEST_HANDLER_ENV: &str = "MEREZHA_JOBS_TEST_HANDLER";

/// How the relay is woken.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelayMode {
    /// `LISTEN merezha_jobs_outbox` plus `jobs.relay.interval_ms`.
    Listen,
    /// The interval only.
    Interval,
    /// Never: records stay pending until [`PgJobs::relay_now`] or another
    /// instance relays them.
    Off,
}

struct Kind {
    source: Source,
    registration: JobRegistration,
}
#[derive(Default)]
struct Registry {
    kinds: BTreeMap<String, Kind>,
    /// `(topic, kind)` to the kind's queue and source.
    subscriptions: BTreeMap<(String, String), (String, String)>,
}

struct Inner {
    config: JobsConfig,
    settings: RelaySettings,
    subscriptions: SubscriptionsConfig,
    listen_check: Duration,
    heartbeat: bool,
    mode: RelayMode,
    database: OnceLock<Database>,
    ctx: OnceLock<AppContext>,
    worker: OnceLock<Arc<Worker>>,
    registry: Mutex<Registry>,
    started: AtomicBool,
    ready: AtomicBool,
    unrouted: AtomicU64,
    stop: tokio::sync::watch::Sender<bool>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// The jobs runtime on PostgreSQL. Cheap to clone; clones share state.
#[derive(Clone)]
pub struct PgJobs {
    inner: Arc<Inner>,
}
impl std::fmt::Debug for PgJobs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgJobs")
            .field("mode", &self.inner.mode)
            .finish_non_exhaustive()
    }
}

/// The `test.sleep` handler of [`TEST_HANDLER_ENV`].
#[cfg(feature = "test-hooks")]
struct Sleep(Duration);
#[cfg(feature = "test-hooks")]
impl merezha_platform::ports::JobHandler for Sleep {
    fn handle<'a>(
        &'a self,
        _: merezha_platform::ports::JobContext,
        _: serde_json::Value,
    ) -> BoxFuture<'a, Result<(), merezha_platform::ports::JobFailure>> {
        Box::pin(async move {
            tokio::time::sleep(self.0).await;
            Ok(())
        })
    }
}
fn test_handler(jobs: &PgJobs) {
    #[cfg(feature = "test-hooks")]
    if let Some(ms) = std::env::var(TEST_HANDLER_ENV)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        && let (Ok(source), Ok(kind), Ok(queue)) = (
            Source::new("jobs"),
            Identity::new("test.sleep"),
            Identity::new("default"),
        )
    {
        let _ = jobs.register(
            &source,
            JobRegistration {
                kind,
                queue,
                handler: Arc::new(Sleep(Duration::from_millis(ms))),
                options: Default::default(),
            },
        );
    }
    let _ = jobs;
}

fn test_mode() -> Option<RelayMode> {
    #[cfg(feature = "test-hooks")]
    match std::env::var(TEST_RELAY_ENV).as_deref() {
        Ok("off") => return Some(RelayMode::Off),
        Ok("interval") => return Some(RelayMode::Interval),
        _ => {}
    }
    None
}

impl PgJobs {
    /// The runtime for `[jobs]`, relaying in [`RelayMode::Listen`].
    pub fn new(config: &JobsConfig) -> Self {
        let jobs = Self::with_mode(config, test_mode().unwrap_or(RelayMode::Listen));
        test_handler(&jobs);
        jobs
    }
    /// The runtime in `mode`. Workers listen on `merezha_jobs` only in
    /// [`RelayMode::Listen`]; otherwise they poll.
    pub fn with_mode(config: &JobsConfig, mode: RelayMode) -> Self {
        Self {
            inner: Arc::new(Inner {
                config: config.clone(),
                settings: RelaySettings::new(config),
                subscriptions: config.subscriptions.clone(),
                listen_check: LISTEN_CHECK_INTERVAL,
                heartbeat: true,
                mode,
                database: OnceLock::new(),
                ctx: OnceLock::new(),
                worker: OnceLock::new(),
                registry: Mutex::new(Registry::default()),
                started: AtomicBool::new(false),
                ready: AtomicBool::new(false),
                unrouted: AtomicU64::new(0),
                stop: tokio::sync::watch::channel(false).0,
                task: Mutex::new(None),
            }),
        }
    }
    /// Probe an idle `LISTEN` session every `interval` instead of
    /// [`LISTEN_CHECK_INTERVAL`]; call before sharing the runtime.
    pub fn with_listen_check(mut self, interval: Duration) -> Self {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.listen_check = interval;
        }
        self
    }
    /// Never extend leases: a test hook for duplicate delivery (AP-B2
    /// test 1), feature `test-hooks`. Call before sharing the runtime.
    #[cfg(feature = "test-hooks")]
    pub fn without_heartbeat(mut self) -> Self {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.heartbeat = false;
        }
        self
    }
    /// Migrations 0001 and 0002 of `merezha_jobs`, under ledger key `jobs`.
    pub fn migrations() -> &'static [Migration] {
        &schema::MIGRATIONS
    }
    fn database(&self) -> Result<&Database, Failure> {
        self.inner
            .database
            .get()
            .ok_or_else(|| Failure::transient("jobs_not_ready"))
    }
    /// Event records relayed without a subscription since this instance
    /// started (D.1 `GET outbox`).
    pub fn unrouted(&self) -> u64 {
        self.inner.unrouted.load(Ordering::Relaxed)
    }
    /// Every registered kind, for the dispatcher (AP-B2).
    pub fn registrations(&self) -> Vec<JobRegistration> {
        self.registry()
            .kinds
            .values()
            .map(|kind| kind.registration.clone())
            .collect()
    }
    /// How this instance runs `queue`: its `[jobs.queues.<name>]`, else
    /// the defaults with [`UNCONFIGURED_CONCURRENCY`] slots.
    fn plan(&self, queue: &str) -> QueuePlan {
        let config = &self.inner.config;
        match config.queues.get(queue) {
            Some(q) => QueuePlan {
                name: queue.to_owned(),
                concurrency: q.concurrency,
                global_concurrency: q.global_concurrency,
                settings: q.settings.clone(),
            },
            None => QueuePlan {
                name: queue.to_owned(),
                concurrency: UNCONFIGURED_CONCURRENCY,
                global_concurrency: 0,
                settings: config.defaults.clone(),
            },
        }
    }
    /// Configured queues and the queues of registered kinds.
    fn plans(&self) -> Vec<QueuePlan> {
        let mut names: std::collections::BTreeSet<String> =
            self.inner.config.queues.keys().cloned().collect();
        names.extend(
            self.registry()
                .kinds
                .values()
                .map(|kind| kind.registration.queue.as_str().to_owned()),
        );
        names.iter().map(|name| self.plan(name)).collect()
    }
    /// One retention pass now (AP §B.9); the workers run one at start and
    /// then every [`RETENTION_INTERVAL`].
    pub async fn purge_expired(&self) -> Result<RetentionReport, Failure> {
        retention::purge(self.database()?, &self.inner.config.retention).await
    }
    /// The jobs admin API (AP §D.1), as [`Module::routes`] mounts it under
    /// `/v1/jobs`, checked by `policy`.
    pub fn admin_router(
        &self,
        policy: Arc<dyn merezha_platform::ports::Authorizer>,
    ) -> axum::Router {
        routes::router(self.clone(), policy)
    }
    /// Build and start the workers once the tables answered.
    fn start_workers(&self, database: &Database) {
        let Some(ctx) = self.inner.ctx.get().cloned() else {
            return;
        };
        let config = &self.inner.config;
        let registered: Vec<(String, Arc<dyn merezha_platform::ports::JobHandler>, String)> = self
            .registry()
            .kinds
            .iter()
            .map(|(name, kind)| {
                (
                    name.clone(),
                    kind.registration.handler.clone(),
                    kind.registration.queue.as_str().to_owned(),
                )
            })
            .collect();
        for queue in registered
            .iter()
            .map(|(_, _, queue)| queue)
            .collect::<std::collections::BTreeSet<_>>()
        {
            if !config.queues.contains_key(queue) {
                tracing::warn!(event = "job.queue_unconfigured", queue = queue.as_str());
            }
        }
        let worker = Worker::new(
            database.clone(),
            ctx,
            WorkerConfig {
                plans: self.plans(),
                defaults: config.defaults.clone(),
                handlers: registered
                    .into_iter()
                    .map(|(name, handler, _)| (name, handler))
                    .collect(),
                poll_interval: config.poll_interval,
                reaper_interval: config.reaper_interval,
                drain_timeout: config.drain_timeout,
                retention: config.retention.clone(),
                heartbeat: self.inner.heartbeat,
                listen: self.inner.mode == RelayMode::Listen,
                listen_check: self.inner.listen_check,
                max_lag: config.readiness_max_lag,
            },
        );
        if self.inner.worker.set(worker.clone()).is_ok() {
            worker.start();
        }
    }
    fn registry(&self) -> std::sync::MutexGuard<'_, Registry> {
        self.inner
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    /// Relay pending records now, batch after batch until one comes back
    /// short (at most 64 batches).
    pub async fn relay_now(&self) -> Result<RelayReport, Failure> {
        let (report, _) =
            relay::drain(self.database()?, &self.inner.settings, MAX_BATCHES_PER_WAKE).await?;
        self.inner
            .unrouted
            .fetch_add(report.unrouted, Ordering::Relaxed);
        Ok(report)
    }
    /// Return a quarantined record to pending once its cause is fixed
    /// (D.1 `POST outbox/{id}/requeue`), with an `admin_actions` row for
    /// `principal`. Fails `not_found`, conflict `secret_purged` for a
    /// flagged record quarantined as `unroutable`, whose payload is gone
    /// (the owning module issues it again), or conflict `not_quarantined`.
    /// A record isolated by its own data error kept its payload and can be
    /// requeued, flagged or not.
    pub async fn requeue_outbox(&self, id: i64, principal: Option<&str>) -> Result<(), Failure> {
        let principal = principal.map(str::to_owned);
        self.database()?
            .transaction(Instant::now() + OPERATION_DEADLINE, move |tx| {
                Box::pin(async move {
                    let requeued = tx
                        .query_opt(
                            "UPDATE merezha_jobs.outbox SET quarantined_at = NULL, quarantine_code = NULL
                              WHERE id = $1 AND quarantined_at IS NOT NULL
                                AND NOT (contains_secret AND payload IS NULL)
                             RETURNING id",
                            &[&id],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                    if requeued.is_none() {
                        let row = tx
                            .query_opt(
                                "SELECT contains_secret AND payload IS NULL, quarantined_at IS NOT NULL
                                   FROM merezha_jobs.outbox WHERE id = $1",
                                &[&id],
                            )
                            .await
                            .map_err(|e| classify(&e))?;
                        return Err(match row.map(|r| (r.get::<_, bool>(0), r.get::<_, bool>(1))) {
                            None => Failure::permanent("not_found"),
                            Some((true, true)) => Failure::conflict("secret_purged"),
                            Some(_) => Failure::conflict("not_quarantined"),
                        });
                    }
                    tx.execute(
                        "INSERT INTO merezha_jobs.admin_actions (principal, action, target, count)
                         VALUES ($1, 'outbox_requeue', $2, 1)",
                        &[&principal, &id.to_string()],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                    tx.execute("SELECT pg_notify('merezha_jobs_outbox', '')", &[])
                        .await
                        .map_err(|e| classify(&e))?;
                    Ok(())
                })
            })
            .await?;
        tracing::info!(
            event = "job.admin_action",
            action = "outbox_requeue",
            count = 1
        );
        Ok(())
    }
    fn launch(&self, ctx: AppContext) -> Result<(), Failure> {
        let database = ctx.database().clone();
        let _ = self.inner.database.set(database.clone());
        let _ = self.inner.ctx.set(ctx);
        if self.inner.started.swap(true, Ordering::SeqCst) {
            return Err(Failure::permanent("jobs_started"));
        }
        let task = tokio::spawn(background(
            self.clone(),
            database,
            self.inner.stop.subscribe(),
        ));
        *self
            .inner
            .task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(task);
        Ok(())
    }
    /// Declare this instance's subscriptions (`last_declared_at = now()`,
    /// reactivated if retired), then retire, under a transaction advisory
    /// lock, every subscription nobody declared for
    /// `jobs.subscriptions.retire_after`. No instance deactivates anything
    /// from its own registry, so during a rolling deploy a subscription any
    /// live version declares keeps receiving events.
    async fn sync_subscriptions(&self, database: &Database) -> Result<(), Failure> {
        let (topics, kinds, queues, sources) = {
            let registry = self.registry();
            let mut columns = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
            for ((topic, kind), (queue, source)) in &registry.subscriptions {
                columns.0.push(topic.clone());
                columns.1.push(kind.clone());
                columns.2.push(queue.clone());
                columns.3.push(source.clone());
            }
            columns
        };
        let retire_after = self.inner.subscriptions.retire_after.as_secs_f64();
        database
            .transaction(Instant::now() + OPERATION_DEADLINE, move |tx| {
                Box::pin(async move {
                    tx.execute(
                        "INSERT INTO merezha_jobs.subscriptions (topic, job_kind, queue, source)
                         SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[])
                         ON CONFLICT (topic, job_kind)
                         DO UPDATE SET queue = excluded.queue, source = excluded.source,
                                       last_declared_at = now(), deactivated_at = NULL",
                        &[&topics, &kinds, &queues, &sources],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                    tx.execute(
                        "SELECT pg_advisory_xact_lock(1297762882, hashtext('subscriptions'))",
                        &[],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                    tx.execute(
                        "UPDATE merezha_jobs.subscriptions SET deactivated_at = now()
                          WHERE deactivated_at IS NULL
                            AND last_declared_at < now() - make_interval(secs => $1)",
                        &[&retire_after],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                    Ok(())
                })
            })
            .await
    }
}

fn classification(failure: &Failure) -> &'static str {
    match failure.classification {
        merezha_domain::Classification::Permanent => "permanent",
        merezha_domain::Classification::Transient => "transient",
        merezha_domain::Classification::Conflict => "conflict",
    }
}

/// One subscription heartbeat; logs `job.subscription_sync_failed` once
/// per failure streak. Returns whether it succeeded.
async fn declare(jobs: &PgJobs, database: &Database, failing: &mut bool) -> bool {
    match jobs.sync_subscriptions(database).await {
        Ok(()) => {
            *failing = false;
            true
        }
        Err(failure) => {
            if !*failing {
                tracing::warn!(
                    event = "job.subscription_sync_failed",
                    code = failure.code.as_str(),
                    classification = classification(&failure)
                );
            }
            *failing = true;
            false
        }
    }
}

/// Wait for a notification; `false` once the session ended. Without a
/// listener it never resolves.
async fn notified(listener: &mut Option<Listener>) -> bool {
    match listener {
        Some(listener) => listener.recv().await.is_some(),
        None => std::future::pending().await,
    }
}

/// Sync subscriptions, then relay on notification and interval until stop.
async fn background(
    jobs: PgJobs,
    database: Database,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let settings = jobs.inner.settings.clone();
    let heartbeat = jobs.inner.subscriptions.heartbeat;
    let check_every = jobs.inner.listen_check;
    let stopped = |stop: &tokio::sync::watch::Receiver<bool>| *stop.borrow();
    // Subscriptions first, so no event relays before its subscribers exist.
    // Pending migrations in verify mode keep this retrying and the runtime
    // not ready.
    let mut failing = false;
    loop {
        if declare(&jobs, &database, &mut failing).await {
            break;
        }
        tokio::select! {
            _ = stop.changed() => return,
            () = tokio::time::sleep(settings.interval) => {}
        }
        if stopped(&stop) {
            return;
        }
    }
    if jobs.inner.config.workers {
        jobs.start_workers(&database);
    }
    jobs.inner.ready.store(true, Ordering::SeqCst);
    let mut next_heartbeat = tokio::time::Instant::now() + heartbeat;
    if jobs.inner.mode == RelayMode::Off {
        loop {
            tokio::select! {
                _ = stop.changed() => return,
                () = tokio::time::sleep_until(next_heartbeat) => {
                    next_heartbeat = tokio::time::Instant::now() + heartbeat;
                    declare(&jobs, &database, &mut failing).await;
                }
            }
            if stopped(&stop) {
                return;
            }
        }
    }
    let mut listener: Option<Listener> = None;
    let mut next_check = tokio::time::Instant::now() + check_every;
    let mut lost = false;
    let mut relay_failing = false;
    loop {
        if stopped(&stop) {
            return;
        }
        if jobs.inner.mode == RelayMode::Listen && listener.is_none() {
            match database
                .listen(&[OUTBOX_CHANNEL], Instant::now() + LISTEN_DEADLINE)
                .await
            {
                Ok(session) => {
                    if lost {
                        tracing::info!(event = "db.listener_restored");
                    }
                    lost = false;
                    listener = Some(session);
                    next_check = tokio::time::Instant::now() + check_every;
                }
                Err(failure) => {
                    if !lost {
                        tracing::warn!(event = "db.listener_lost", code = failure.code.as_str());
                    }
                    lost = true;
                }
            }
        }
        let drained = match relay::drain(&database, &settings, MAX_BATCHES_PER_WAKE).await {
            Ok((report, drained)) => {
                jobs.inner
                    .unrouted
                    .fetch_add(report.unrouted, Ordering::Relaxed);
                relay_failing = false;
                drained
            }
            Err(failure) => {
                // Systemic failures abort the batch and retry at the
                // interval; nothing is quarantined for them.
                if !relay_failing {
                    tracing::error!(
                        event = "outbox.relay_failed",
                        code = failure.code.as_str(),
                        classification = classification(&failure)
                    );
                }
                relay_failing = true;
                true
            }
        };
        if !drained {
            // A backlog never starves the heartbeat.
            if tokio::time::Instant::now() >= next_heartbeat {
                next_heartbeat = tokio::time::Instant::now() + heartbeat;
                declare(&jobs, &database, &mut failing).await;
            }
            continue;
        }
        tokio::select! {
            _ = stop.changed() => return,
            () = tokio::time::sleep_until(next_heartbeat) => {
                next_heartbeat = tokio::time::Instant::now() + heartbeat;
                declare(&jobs, &database, &mut failing).await;
            }
            () = tokio::time::sleep_until(next_check), if listener.is_some() => {
                next_check = tokio::time::Instant::now() + check_every;
                // A half-open session never ends by itself: probe it.
                let probe = match &listener {
                    Some(session) => session.check(Instant::now() + LISTEN_DEADLINE).await,
                    None => Ok(()),
                };
                if let Err(failure) = probe {
                    listener = None;
                    tracing::warn!(event = "db.listener_lost", code = failure.code.as_str());
                    lost = true;
                }
            }
            alive = notified(&mut listener) => {
                if alive {
                    // Coalesce a burst into one pass.
                    while listener.as_mut().and_then(Listener::try_recv).is_some() {}
                } else {
                    listener = None;
                    tracing::warn!(event = "db.listener_lost", code = "database_unavailable");
                    lost = true;
                }
            }
            () = tokio::time::sleep(settings.interval) => {}
        }
    }
}

impl Module for PgJobs {
    fn name(&self) -> &'static str {
        "jobs"
    }
    fn migrations(&self) -> &[Migration] {
        Self::migrations()
    }
    fn errors(&self) -> &[ErrorCode] {
        ERRORS
    }
    /// Starts after every other module: subscriptions are synced, then the
    /// relay runs in the background.
    fn start(&self, ctx: AppContext) -> BoxFuture<'_, Result<(), Failure>> {
        let result = self.launch(ctx);
        Box::pin(async move { result })
    }
    /// The jobs admin API under `/v1/jobs/admin/` (AP §D.1).
    fn routes(&self, ctx: &AppContext) -> axum::Router {
        self.admin_router(ctx.authorizer().clone())
    }
    /// Ready once subscriptions are synced and the workers started
    /// (`jobs.workers = true`), until shutdown; with
    /// `jobs.readiness.max_lag_ms`, not while the backlog is older.
    fn ready(&self) -> BoxFuture<'_, Result<(), Failure>> {
        let worker = self.inner.worker.get();
        let workers = !self.inner.config.workers || worker.is_some();
        let lagging = worker.is_some_and(|w| w.lagging());
        let ready = self.inner.ready.load(Ordering::SeqCst)
            && workers
            && !lagging
            && !*self.inner.stop.borrow();
        Box::pin(async move {
            if ready {
                Ok(())
            } else {
                Err(Failure::transient("jobs_not_ready"))
            }
        })
    }
    /// Stops the relay and drains the workers (AP §B.8): no new leases,
    /// in-flight jobs get `jobs.drain_timeout_ms` (within `deadline`), then
    /// `cancel` and a 2 s grace, then the rest is released with the attempt
    /// refunded. A relay batch in progress finishes or, past `deadline`, is
    /// abandoned and rolls back.
    fn shutdown(&self, deadline: Instant) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.inner.stop.send_replace(true);
            if let Some(worker) = self.inner.worker.get() {
                worker.drain(deadline).await;
            }
            let task = self
                .inner
                .task
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(mut task) = task {
                let at = tokio::time::Instant::from_std(deadline);
                if tokio::time::timeout_at(at, &mut task).await.is_err() {
                    task.abort();
                }
            }
        })
    }
}

impl JobsRuntime for PgJobs {
    fn ports(&self, database: &Database) -> JobsPorts {
        let _ = self.inner.database.set(database.clone());
        JobsPorts {
            outbox: Arc::new(PgOutbox),
            jobs: Arc::new(self.clone()),
        }
    }
}

impl Jobs for PgJobs {
    fn enqueue<'a>(
        &'a self,
        source: &'a Source,
        job: NewJob,
    ) -> BoxFuture<'a, Result<Enqueued, Failure>> {
        Box::pin(async move {
            validate::source(source.as_str())?;
            validate::queue(job.queue.as_str())?;
            validate::run_at(job.run_at, jiff::Timestamp::now())?;
            let payload = validate::job_payload(&job.payload)?;
            let key = format!("{}:{}", source.as_str(), job.idempotency_key.as_str());
            let max_attempts = self.inner.settings.max_attempts(job.queue.as_str());
            let run_at = job.run_at.map(|at| at.to_string());
            let context = job.context;
            let correlation = context.correlation_id.as_str().to_owned();
            let causation = context.causation_id.map(String::from);
            let tenant = context.tenant.map(String::from);
            let queue = job.queue.as_str().to_owned();
            let kind = job.kind.as_str().to_owned();
            let secret = job.contains_secret;
            let priority = job.priority;
            self.database()?
                .transaction(Instant::now() + OPERATION_DEADLINE, move |tx| {
                    Box::pin(async move {
                        let inserted = tx
                            .query_opt(
                                "INSERT INTO merezha_jobs.jobs (queue, kind, idempotency_key, payload,
                                     payload_digest, contains_secret, priority, run_at, max_attempts,
                                     correlation_id, causation_id, tenant)
                                 VALUES ($1, $2, $3, $4::text::json, sha256(convert_to($4::text, 'UTF8')),
                                     $5, $6, coalesce($7::text::timestamptz, now()), $8, $9, $10, $11)
                                 ON CONFLICT (queue, idempotency_key) DO NOTHING
                                 RETURNING id",
                                &[
                                    &queue,
                                    &kind,
                                    &key,
                                    &payload,
                                    &secret,
                                    &priority,
                                    &run_at,
                                    &max_attempts,
                                    &correlation,
                                    &causation,
                                    &tenant,
                                ],
                            )
                            .await
                            .map_err(|e| classify(&e))?;
                        if let Some(row) = inserted {
                            // Wake the queue's dispatchers (AP-B2), on commit.
                            tx.execute("SELECT pg_notify('merezha_jobs', $1)", &[&queue])
                                .await
                                .map_err(|e| classify(&e))?;
                            return Ok(Enqueued {
                                id: row.get(0),
                                existed: false,
                            });
                        }
                        // A new statement sees a conflicting row another
                        // transaction committed meanwhile.
                        let existing = tx
                            .query_opt(
                                "SELECT id, kind = $3 AND payload_digest = sha256(convert_to($4::text, 'UTF8'))
                                   FROM merezha_jobs.jobs WHERE queue = $1 AND idempotency_key = $2",
                                &[&queue, &key, &kind, &payload],
                            )
                            .await
                            .map_err(|e| classify(&e))?;
                        match existing {
                            Some(row) if row.get::<_, bool>(1) => Ok(Enqueued {
                                id: row.get(0),
                                existed: true,
                            }),
                            Some(_) => Err(Failure::conflict("idempotency_mismatch")),
                            None => Err(Failure::conflict("database_conflict")),
                        }
                    })
                })
                .await
        })
    }
    fn cancel<'a>(&'a self, source: &'a Source, id: i64) -> BoxFuture<'a, Result<bool, Failure>> {
        let valid = validate::source(source.as_str());
        let kinds: Vec<String> = self
            .registry()
            .kinds
            .iter()
            .filter(|(_, kind)| kind.source == *source)
            .map(|(name, _)| name.clone())
            .collect();
        Box::pin(async move {
            valid?;
            let client = self
                .database()?
                .acquire(Instant::now() + OPERATION_DEADLINE)
                .await?;
            let cancelled = client
                .execute(
                    "UPDATE merezha_jobs.jobs
                        SET state = 'cancelled', finished_at = now(), updated_at = now(),
                            payload = CASE WHEN contains_secret THEN NULL ELSE payload END
                      WHERE id = $1 AND state = 'available'
                        AND (starts_with(idempotency_key, $2::text || ':') OR kind = ANY($3::text[]))",
                    &[&id, &source.as_str(), &kinds],
                )
                .await
                .map_err(|e| classify(&e))?;
            Ok(cancelled == 1)
        })
    }
    fn register(&self, source: &Source, registration: JobRegistration) -> Result<(), Failure> {
        validate::queue(registration.queue.as_str())?;
        let mut registry = self.registry();
        if self.inner.started.load(Ordering::SeqCst) {
            return Err(Failure::permanent("jobs_started"));
        }
        let kind = registration.kind.as_str().to_owned();
        if registry.kinds.contains_key(&kind) {
            return Err(Failure::permanent("kind_already_registered"));
        }
        if registry.kinds.len() >= MAX_KINDS {
            return Err(Failure::permanent("too_many_kinds"));
        }
        registry.kinds.insert(
            kind,
            Kind {
                source: source.clone(),
                registration,
            },
        );
        Ok(())
    }
    fn queue_status(&self) -> BoxFuture<'_, Result<QueueStatus, Failure>> {
        Box::pin(self.status())
    }
    fn subscribe(&self, source: &Source, topic: &Identity, kind: &Identity) -> Result<(), Failure> {
        validate::topic(topic.as_str())?;
        let mut registry = self.registry();
        if self.inner.started.load(Ordering::SeqCst) {
            return Err(Failure::permanent("jobs_started"));
        }
        let queue = match registry.kinds.get(kind.as_str()) {
            Some(owned) if owned.source == *source => owned.registration.queue.as_str().to_owned(),
            _ => return Err(Failure::permanent("kind_not_registered")),
        };
        let key = (topic.as_str().to_owned(), kind.as_str().to_owned());
        if !registry.subscriptions.contains_key(&key) && registry.subscriptions.len() >= MAX_KINDS {
            return Err(Failure::permanent("too_many_kinds"));
        }
        registry
            .subscriptions
            .insert(key, (queue, source.as_str().to_owned()));
        Ok(())
    }
}
