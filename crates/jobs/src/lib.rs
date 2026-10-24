//! Jobs and the outbox (AP §B), schema `merezha_jobs`.
//!
//! [`PgJobs`] is the jobs runtime the host installs
//! ([`merezha_platform::JobsRuntime`]): the PostgreSQL [`PgOutbox`] behind
//! the kernel `Outbox` port, the `Jobs` port behind every module's
//! `JobsHandle` (enqueue, cancel, register, subscribe), and the relay that
//! turns committed outbox records into jobs.
//!
//! Seams for later slices: leasing, dispatch, retry, drain, retention and
//! the admin API (AP-B2) read the registrations kept here
//! ([`PgJobs::registrations`]) and the tables of migration 0001; cron
//! (AP-B3) adds `JobsHandle::schedule` and the scheduler tick.
mod outbox;
mod relay;
mod schema;
mod validate;

pub use outbox::{OUTBOX_CHANNEL, PgOutbox};
pub use relay::{JOBS_CHANNEL, RelayReport};

use merezha_domain::{Failure, Identity};
use merezha_platform::{
    AppContext, BoxFuture, Database, ErrorCode, JobsPorts, JobsRuntime, Migration, Module,
    config::JobsConfig,
    ports::{Enqueued, JobRegistration, Jobs, NewJob, Source},
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

/// Codes this runtime returns, registered at startup (R8).
pub const ERRORS: &[ErrorCode] = &[
    ErrorCode::new("invalid_topic", 422),
    ErrorCode::new("invalid_queue", 422),
    ErrorCode::new("invalid_run_at", 422),
    ErrorCode::new("invalid_event_envelope", 422),
    ErrorCode::new("job_payload_limit", 422),
    ErrorCode::new("kind_already_registered", 422),
    ErrorCode::new("kind_not_registered", 422),
    ErrorCode::new("too_many_kinds", 422),
    ErrorCode::new("jobs_started", 422),
    ErrorCode::new("not_quarantined", 409),
    ErrorCode::new("jobs_not_ready", 503),
];
/// Most registered kinds and subscriptions each.
pub const MAX_KINDS: usize = 1024;
/// Most batches one wake-up relays before it checks for shutdown.
const MAX_BATCHES_PER_WAKE: usize = 64;
/// Bound on enqueue, cancel, requeue and subscription sync.
const OPERATION_DEADLINE: Duration = Duration::from_secs(10);
/// Bound on opening the `LISTEN` session.
const LISTEN_DEADLINE: Duration = Duration::from_secs(5);
/// The process-test hook (feature `test-hooks`): `off` keeps the relay
/// stopped, `interval` relays on the interval only.
pub const TEST_RELAY_ENV: &str = "MEREZHA_JOBS_TEST_RELAY";

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
    settings: RelaySettings,
    mode: RelayMode,
    database: OnceLock<Database>,
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
        Self::with_mode(config, test_mode().unwrap_or(RelayMode::Listen))
    }
    pub fn with_mode(config: &JobsConfig, mode: RelayMode) -> Self {
        Self {
            inner: Arc::new(Inner {
                settings: RelaySettings::new(config),
                mode,
                database: OnceLock::new(),
                registry: Mutex::new(Registry::default()),
                started: AtomicBool::new(false),
                ready: AtomicBool::new(false),
                unrouted: AtomicU64::new(0),
                stop: tokio::sync::watch::channel(false).0,
                task: Mutex::new(None),
            }),
        }
    }
    /// Migration 0001 of `merezha_jobs`, under ledger key `jobs`.
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
    /// flagged record, whose payload is gone (the owning module issues it
    /// again), or conflict `not_quarantined`.
    pub async fn requeue_outbox(&self, id: i64, principal: Option<&str>) -> Result<(), Failure> {
        let principal = principal.map(str::to_owned);
        self.database()?
            .transaction(Instant::now() + OPERATION_DEADLINE, move |tx| {
                Box::pin(async move {
                    let requeued = tx
                        .query_opt(
                            "UPDATE merezha_jobs.outbox SET quarantined_at = NULL, quarantine_code = NULL
                              WHERE id = $1 AND quarantined_at IS NOT NULL AND NOT contains_secret
                             RETURNING id",
                            &[&id],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                    if requeued.is_none() {
                        let row = tx
                            .query_opt(
                                "SELECT contains_secret, quarantined_at IS NOT NULL
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
    fn launch(&self, database: Database) -> Result<(), Failure> {
        let _ = self.inner.database.set(database.clone());
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
    /// Upsert every declared subscription.
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
        if topics.is_empty() {
            return Ok(());
        }
        let client = database
            .acquire(Instant::now() + OPERATION_DEADLINE)
            .await?;
        client
            .execute(
                "INSERT INTO merezha_jobs.subscriptions (topic, job_kind, queue, source)
                 SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[])
                 ON CONFLICT (topic, job_kind)
                 DO UPDATE SET queue = excluded.queue, source = excluded.source",
                &[&topics, &kinds, &queues, &sources],
            )
            .await
            .map_err(|e| classify(&e))?;
        Ok(())
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
    let stopped = |stop: &tokio::sync::watch::Receiver<bool>| *stop.borrow();
    // Subscriptions first, so no event relays before its subscribers exist.
    // Pending migrations in verify mode keep this retrying and the runtime
    // not ready.
    let mut failing = false;
    loop {
        match jobs.sync_subscriptions(&database).await {
            Ok(()) => break,
            Err(failure) => {
                if !failing && failure.classification != merezha_domain::Classification::Permanent {
                    tracing::error!(event = "db.unavailable", code = failure.code.as_str());
                }
                failing = true;
            }
        }
        tokio::select! {
            _ = stop.changed() => return,
            () = tokio::time::sleep(settings.interval) => {}
        }
        if stopped(&stop) {
            return;
        }
    }
    jobs.inner.ready.store(true, Ordering::SeqCst);
    if jobs.inner.mode == RelayMode::Off {
        let _ = stop.wait_for(|stop| *stop).await;
        return;
    }
    let mut listener: Option<Listener> = None;
    let mut lost = false;
    let mut failing = false;
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
                failing = false;
                drained
            }
            Err(failure) => {
                if !failing {
                    tracing::error!(event = "db.unavailable", code = failure.code.as_str());
                }
                failing = true;
                true
            }
        };
        if !drained {
            continue;
        }
        tokio::select! {
            _ = stop.changed() => return,
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
        let result = self.launch(ctx.database().clone());
        Box::pin(async move { result })
    }
    /// Ready once subscriptions are synced and until shutdown.
    fn ready(&self) -> BoxFuture<'_, Result<(), Failure>> {
        let ready = self.inner.ready.load(Ordering::SeqCst) && !*self.inner.stop.borrow();
        Box::pin(async move {
            if ready {
                Ok(())
            } else {
                Err(Failure::transient("jobs_not_ready"))
            }
        })
    }
    /// Stops the relay. A batch in progress finishes or, past `deadline`,
    /// is abandoned and rolls back.
    fn shutdown(&self, deadline: Instant) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.inner.stop.send_replace(true);
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
    fn cancel(&self, id: i64) -> BoxFuture<'_, Result<bool, Failure>> {
        Box::pin(async move {
            let client = self
                .database()?
                .acquire(Instant::now() + OPERATION_DEADLINE)
                .await?;
            let cancelled = client
                .execute(
                    "UPDATE merezha_jobs.jobs
                        SET state = 'cancelled', finished_at = now(), updated_at = now(),
                            payload = CASE WHEN contains_secret THEN NULL ELSE payload END
                      WHERE id = $1 AND state = 'available'",
                    &[&id],
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
