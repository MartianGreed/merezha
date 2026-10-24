//! Leasing, dispatch, heartbeat, reaper and graceful drain (AP §B.5, §B.6,
//! §B.8). One dispatcher task per queue leases with `FOR UPDATE SKIP
//! LOCKED` only while it has free slots; every write after the lease is
//! fenced on the per-claim `lease_token`, so a handler whose lease was lost
//! (reaped, released by a drain) can never settle the job.
use crate::retry::{self, Outcome, Settlement};
use merezha_domain::{Failure, Identity};
use merezha_platform::{
    AppContext, Database,
    config::{JobDefaults, JobRetention},
    ports::{JobContext, JobHandler},
};
use merezha_postgres::{Listener, classify};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Notify, Semaphore, watch};

/// `'MZJB'`, the advisory lock class of the jobs runtime.
pub(crate) const MZJB: i32 = 1_297_762_882;
/// Bound on one lease, heartbeat, reaper or release statement.
const STATEMENT_DEADLINE: Duration = Duration::from_secs(5);
/// Most jobs one reaper statement reclaims, and statements per tick.
const REAP_BATCH: usize = 100;
const MAX_REAP_BATCHES: usize = 10;
/// Grace after the drain cutoff fires `cancel` (AP §B.8 step 3).
pub(crate) const DRAIN_GRACE: Duration = Duration::from_secs(2);
/// Kept free before the shutdown deadline for the release statement.
const RELEASE_MARGIN: Duration = Duration::from_secs(1);
/// Settle attempts on transient database failures, while the lease is
/// still heartbeated.
const SETTLE_RETRIES: [Duration; 2] = [Duration::from_millis(100), Duration::from_millis(500)];
/// `job.drain_abandoned` lists at most this many ids.
const MAX_LOGGED_IDS: usize = 64;
/// Bound on opening and probing the `LISTEN merezha_jobs` session.
const LISTEN_DEADLINE: Duration = Duration::from_secs(5);

/// One queue as this instance runs it.
#[derive(Clone, Debug)]
pub(crate) struct QueuePlan {
    pub name: String,
    pub concurrency: u32,
    pub global_concurrency: u32,
    pub settings: JobDefaults,
}

/// What a [`Worker`] needs from `[jobs]` and the registry.
pub(crate) struct WorkerConfig {
    pub plans: Vec<QueuePlan>,
    pub defaults: JobDefaults,
    pub handlers: BTreeMap<String, Arc<dyn JobHandler>>,
    pub poll_interval: Duration,
    pub reaper_interval: Duration,
    pub drain_timeout: Duration,
    pub retention: JobRetention,
    pub heartbeat: bool,
    /// Reissue claims older than this go back to `dead`.
    pub claim_timeout: Duration,
    pub listen: bool,
    pub listen_check: Duration,
    pub max_lag: Option<Duration>,
}

const LEASE: &str = "
WITH cap AS (
    SELECT CASE WHEN $6::bigint > 0
                THEN greatest(0, least($3::bigint, $6::bigint - (SELECT count(*) FROM merezha_jobs.jobs
                                                                 WHERE queue = $1 AND state = 'running')))
                ELSE $3::bigint END AS n
), candidate AS (
    SELECT id FROM merezha_jobs.jobs
     WHERE queue = $1 AND state = 'available' AND run_at <= now() AND kind = ANY($2::text[])
     ORDER BY priority DESC, run_at, id
     LIMIT (SELECT n FROM cap)
     FOR UPDATE SKIP LOCKED
), leased AS (
    UPDATE merezha_jobs.jobs j
       SET state = 'running', attempts = j.attempts + 1, lease_token = gen_random_uuid(),
           lease_owner = $4, lease_expires_at = now() + make_interval(secs => $5::float8),
           updated_at = now()
      FROM candidate c WHERE j.id = c.id
    RETURNING j.id, j.kind, j.payload::text AS payload, j.attempts, j.max_attempts,
              j.lease_token, j.idempotency_key, j.correlation_id, j.causation_id, j.tenant
), started AS (
    INSERT INTO merezha_jobs.attempts (lease_token, job_id, attempt, lease_owner, started_at)
    SELECT lease_token, id, attempts, $4, now() FROM leased
)
SELECT id, kind, payload, attempts, max_attempts, lease_token::text, idempotency_key,
       correlation_id, causation_id, tenant
  FROM leased ORDER BY id";

/// One statement settles the job (nulling a flagged payload at a terminal
/// state, R3) and closes its attempt, both fenced on the lease token.
const SETTLE: &str = "
WITH settled AS (
    UPDATE merezha_jobs.jobs
       SET state = $3,
           run_at = CASE WHEN $3 = 'available' THEN now() + make_interval(secs => $4::float8) ELSE run_at END,
           finished_at = CASE WHEN $3 <> 'available' THEN now() END,
           payload = CASE WHEN contains_secret AND $3 <> 'available' THEN NULL ELSE payload END,
           last_error_code = coalesce($5, last_error_code),
           last_error_class = coalesce($6, last_error_class),
           last_error_at = CASE WHEN $5::text IS NOT NULL THEN now() ELSE last_error_at END,
           lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
     WHERE id = $1 AND lease_token = $2::text::uuid AND state = 'running'
    RETURNING id
), closed AS (
    UPDATE merezha_jobs.attempts
       SET finished_at = now(), outcome = CASE WHEN $3 = 'succeeded' THEN 'succeeded' ELSE 'failed' END,
           error_code = $5, error_class = $6
     WHERE lease_token = $2::text::uuid AND EXISTS (SELECT 1 FROM settled)
)
SELECT count(*) FROM settled";

const HEARTBEAT: &str = "
UPDATE merezha_jobs.jobs j SET lease_expires_at = now() + make_interval(secs => h.secs), updated_at = now()
  FROM unnest($1::text[]::uuid[], $2::float8[]) AS h (token, secs)
 WHERE j.lease_token = h.token AND j.state = 'running'
RETURNING j.lease_token::text";

/// Expired leases count as failed attempts (counted at lease time); the
/// backoff has no jitter, enough to stop a job that crashes its process from
/// looping hot.
const REAP: &str = "
WITH settings (queue, base_s, max_s) AS (
    SELECT * FROM unnest($1::text[], $2::float8[], $3::float8[])
), expired AS (
    SELECT j.id, j.lease_token, coalesce(s.base_s, $4::float8) AS base_s,
           coalesce(s.max_s, $5::float8) AS max_s
      FROM merezha_jobs.jobs j LEFT JOIN settings s ON s.queue = j.queue
     WHERE j.state = 'running' AND j.lease_expires_at < now()
     ORDER BY j.lease_expires_at LIMIT 100
     FOR UPDATE OF j SKIP LOCKED
), reaped AS (
    UPDATE merezha_jobs.jobs j
       SET state = CASE WHEN j.attempts >= j.max_attempts THEN 'dead' ELSE 'available' END,
           run_at = CASE WHEN j.attempts >= j.max_attempts THEN j.run_at
                         ELSE now() + make_interval(secs => least(
                             e.base_s * power(2::float8, greatest(j.attempts - 1, 0)), e.max_s)) END,
           finished_at = CASE WHEN j.attempts >= j.max_attempts THEN now() END,
           payload = CASE WHEN j.contains_secret AND j.attempts >= j.max_attempts THEN NULL
                          ELSE j.payload END,
           last_error_code = 'lease_expired', last_error_class = 'transient', last_error_at = now(),
           lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
      FROM expired e WHERE j.id = e.id
    RETURNING j.id, j.queue, j.kind, j.attempts, j.state
), closed AS (
    UPDATE merezha_jobs.attempts a
       SET finished_at = now(), outcome = 'lease_expired', error_code = 'lease_expired',
           error_class = 'transient'
      FROM expired e WHERE a.lease_token = e.lease_token
)
SELECT id, queue, kind, attempts, state FROM reaped ORDER BY id";

/// The drain release (AP §B.8 step 4): back to `available` now with the
/// attempt refunded, fenced on the tokens this instance still holds.
const RELEASE: &str = "
WITH released AS (
    UPDATE merezha_jobs.jobs
       SET state = 'available', run_at = now(), attempts = greatest(attempts - 1, 0),
           lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
     WHERE lease_token = ANY($1::text[]::uuid[]) AND state = 'running'
    RETURNING id
), closed AS (
    UPDATE merezha_jobs.attempts a SET finished_at = now(), outcome = 'released'
      FROM released r
     WHERE a.job_id = r.id AND a.lease_token = ANY($1::text[]::uuid[])
)
SELECT id FROM released ORDER BY id";

/// The oldest due job through `jobs_due`, without scanning the backlog.
pub(crate) const LAG: &str = "
SELECT coalesce(extract(epoch FROM now() - (
           SELECT run_at FROM merezha_jobs.jobs
            WHERE state = 'available' AND run_at <= now()
            ORDER BY run_at LIMIT 1)), 0)::float8";

/// Reissue claims cut between claim and hook (a crash) go back to `dead`.
const REVERT_CLAIMS: &str = "
UPDATE merezha_jobs.jobs
   SET state = 'dead', replays = greatest(replays - 1, 0), reissue_claimed_at = NULL,
       updated_at = now()
 WHERE id IN (SELECT id FROM merezha_jobs.jobs
               WHERE reissue_claimed_at < now() - make_interval(secs => $1::float8)
               ORDER BY reissue_claimed_at LIMIT 100 FOR UPDATE SKIP LOCKED)";

/// A leased job, as the lease statement returns it.
struct Leased {
    id: i64,
    kind: String,
    payload: Option<String>,
    attempt: u32,
    max_attempts: u32,
    token: String,
    idempotency_key: String,
    correlation_id: String,
    causation_id: Option<String>,
    tenant: Option<String>,
}

/// One lease this instance holds.
struct InFlight {
    lease_secs: f64,
    cancel: watch::Sender<bool>,
    /// The heartbeat found the lease gone: the result is discarded.
    lost: bool,
    /// The drain cutoff fired `cancel`: a failure is released, not retried.
    cut: bool,
    /// The handler returned; the entry waits for the drain release.
    done: bool,
}

struct Lane {
    plan: QueuePlan,
    slots: Arc<Semaphore>,
    wake: Notify,
}

/// The dispatchers, heartbeat, reaper and drain of one instance.
pub(crate) struct Worker {
    database: Database,
    ctx: AppContext,
    owner: String,
    kinds: Vec<String>,
    handlers: BTreeMap<String, Arc<dyn JobHandler>>,
    lanes: Vec<Arc<Lane>>,
    defaults: JobDefaults,
    config: WorkerConfig,
    in_flight: Mutex<HashMap<String, InFlight>>,
    changed: Notify,
    stop: watch::Sender<bool>,
    stop_heartbeat: watch::Sender<bool>,
    lagging: AtomicBool,
    failing: Mutex<HashSet<&'static str>>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    dispatchers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
fn deadline() -> Instant {
    Instant::now() + STATEMENT_DEADLINE
}
fn attempt_number(value: i32) -> u32 {
    u32::try_from(value).unwrap_or(0)
}

impl Worker {
    pub(crate) fn new(database: Database, ctx: AppContext, mut config: WorkerConfig) -> Arc<Self> {
        let lanes = std::mem::take(&mut config.plans)
            .into_iter()
            .map(|plan| {
                Arc::new(Lane {
                    slots: Arc::new(Semaphore::new(plan.concurrency as usize)),
                    plan,
                    wake: Notify::new(),
                })
            })
            .collect();
        let handlers = std::mem::take(&mut config.handlers);
        Arc::new(Self {
            database,
            owner: ctx.instance_id().to_owned(),
            ctx,
            kinds: handlers.keys().cloned().collect(),
            handlers,
            lanes,
            defaults: config.defaults.clone(),
            config,
            in_flight: Mutex::new(HashMap::new()),
            changed: Notify::new(),
            stop: watch::channel(false).0,
            stop_heartbeat: watch::channel(false).0,
            lagging: AtomicBool::new(false),
            failing: Mutex::new(HashSet::new()),
            tasks: Mutex::new(Vec::new()),
            dispatchers: Mutex::new(Vec::new()),
        })
    }

    /// Start the dispatchers, the heartbeat, the reaper and the `LISTEN`
    /// session. Without any registered kind nothing can be leased, so only
    /// the reaper runs.
    pub(crate) fn start(self: &Arc<Self>) {
        let mut tasks = lock(&self.tasks);
        if !self.kinds.is_empty() {
            let mut dispatchers = lock(&self.dispatchers);
            for lane in &self.lanes {
                dispatchers.push(tokio::spawn(dispatch(self.clone(), lane.clone())));
            }
            if self.config.heartbeat {
                tasks.push(tokio::spawn(heartbeat(self.clone())));
            }
            if self.config.listen {
                tasks.push(tokio::spawn(listen(self.clone())));
            }
        }
        tasks.push(tokio::spawn(reaper(self.clone())));
        tasks.push(tokio::spawn(retain(self.clone())));
    }

    /// `false` while `jobs.readiness.max_lag_ms` is set and exceeded.
    pub(crate) fn lagging(&self) -> bool {
        self.lagging.load(Ordering::Relaxed)
    }

    /// Log a background failure once per streak, as `job.worker_failed`.
    fn failed(&self, task: &'static str, failure: &Failure) {
        if lock(&self.failing).insert(task) {
            tracing::warn!(
                event = "job.worker_failed",
                task,
                code = failure.code.as_str(),
                classification = retry::class_name(failure.classification)
            );
        }
    }
    fn recovered(&self, task: &'static str) {
        lock(&self.failing).remove(task);
    }

    async fn lease(&self, lane: &Lane, free: usize) -> Result<Vec<Leased>, Failure> {
        let queue = lane.plan.name.clone();
        // Kinds whose handler turned leasing off stay available for other
        // instances (content on an outdated host, CT §2).
        let kinds: Vec<String> = self
            .kinds
            .iter()
            .filter(|kind| self.handlers.get(*kind).is_some_and(|h| h.accepts_leases()))
            .cloned()
            .collect();
        if kinds.is_empty() {
            return Ok(Vec::new());
        }
        let wanted = i64::try_from(free).unwrap_or(i64::MAX);
        let owner = self.owner.clone();
        let lease_secs = lane.plan.settings.lease.as_secs_f64();
        let global = i64::from(lane.plan.global_concurrency);
        self.database
            .transaction(deadline(), move |tx| {
                Box::pin(async move {
                    if global > 0 {
                        tx.execute(
                            "SELECT pg_advisory_xact_lock($1, hashtext($2))",
                            &[&MZJB, &queue],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                    }
                    let rows = tx
                        .query(
                            LEASE,
                            &[&queue, &kinds, &wanted, &owner, &lease_secs, &global],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                    Ok(rows
                        .iter()
                        .map(|row| Leased {
                            id: row.get(0),
                            kind: row.get(1),
                            payload: row.get(2),
                            attempt: attempt_number(row.get(3)),
                            max_attempts: attempt_number(row.get(4)),
                            token: row.get(5),
                            idempotency_key: row.get(6),
                            correlation_id: row.get(7),
                            causation_id: row.get(8),
                            tenant: row.get(9),
                        })
                        .collect())
                })
            })
            .await
    }

    /// The fenced settle; `Ok(false)` when the lease was no longer held.
    async fn settle(&self, id: i64, token: &str, settlement: &Settlement) -> Result<bool, Failure> {
        let state = settlement.state();
        let (delay, code, class) = match settlement {
            Settlement::Succeeded => (0.0, None, None),
            Settlement::Retry { delay, code, class } => {
                (delay.as_secs_f64(), Some(code.clone()), Some(*class))
            }
            Settlement::Dead { code, class } => (0.0, Some(code.clone()), Some(*class)),
        };
        let client = self.database.acquire(deadline()).await?;
        let at = tokio::time::Instant::from_std(deadline());
        let row = tokio::time::timeout_at(
            at,
            client.query_one(SETTLE, &[&id, &token, &state, &delay, &code, &class]),
        )
        .await
        .map_err(|_| Failure::transient("database_timeout"))?
        .map_err(|e| classify(&e))?;
        Ok(row.get::<_, i64>(0) == 1)
    }

    fn track(&self, token: &str, entry: InFlight) {
        lock(&self.in_flight).insert(token.to_owned(), entry);
    }
    /// Whether the drain cutoff fired for `token`; `None` once released.
    fn cut(&self, token: &str) -> Option<bool> {
        lock(&self.in_flight).get(token).map(|entry| entry.cut)
    }
    fn untrack(&self, token: &str) -> Option<InFlight> {
        let entry = lock(&self.in_flight).remove(token);
        self.changed.notify_waiters();
        entry
    }
    /// Leave `token` for the drain release: its handler returned a failure
    /// after the cutoff.
    fn park(&self, token: &str) {
        if let Some(entry) = lock(&self.in_flight).get_mut(token) {
            entry.done = true;
        }
        self.changed.notify_waiters();
    }
    fn running(&self) -> usize {
        lock(&self.in_flight)
            .values()
            .filter(|entry| !entry.done)
            .count()
    }

    /// Hold a fresh lease: from here the heartbeat extends it and a drain
    /// releases it. Returns its `cancel` receiver.
    fn hold(&self, lane: &Lane, job: &Leased) -> watch::Receiver<bool> {
        let (cancel, cancelled) = watch::channel(false);
        self.track(
            &job.token,
            InFlight {
                lease_secs: lane.plan.settings.lease.as_secs_f64(),
                cancel,
                lost: false,
                cut: false,
                done: false,
            },
        );
        cancelled
    }

    /// Run one held job to its settlement.
    async fn run(self: Arc<Self>, lane: Arc<Lane>, job: Leased, cancelled: watch::Receiver<bool>) {
        tracing::debug!(
            event = "job.leased",
            job_id = job.id,
            queue = lane.plan.name.as_str(),
            kind = job.kind.as_str(),
            attempt = job.attempt
        );
        let outcome = self.execute(&lane, &job, cancelled).await;
        if self.cut(&job.token) == Some(true) && !matches!(outcome, Outcome::Succeeded) {
            // Drain cutoff: released with the attempt refunded, not retried.
            self.park(&job.token);
            return;
        }
        let settlement = retry::decide(
            outcome,
            job.attempt,
            job.max_attempts,
            &lane.plan.settings,
            self.ctx.random(),
        );
        let mut result = self.settle(job.id, &job.token, &settlement).await;
        for pause in SETTLE_RETRIES {
            match &result {
                Err(failure)
                    if failure.classification != merezha_domain::Classification::Permanent =>
                {
                    tokio::time::sleep(pause).await;
                    result = self.settle(job.id, &job.token, &settlement).await;
                }
                _ => break,
            }
        }
        let entry = self.untrack(&job.token);
        let queue = lane.plan.name.as_str();
        match result {
            Ok(true) => {
                self.recovered("settle");
                log_settled(&job, queue, &settlement);
            }
            // Released by the drain (no entry) or lost: fenced out.
            Ok(false) if entry.is_some() => tracing::warn!(
                event = "job.lease_lost",
                job_id = job.id,
                queue,
                kind = job.kind.as_str(),
                attempt = job.attempt,
                code = "lease_lost",
                classification = "transient"
            ),
            Ok(false) => {}
            // The lease expires and the reaper decides.
            Err(failure) if entry.is_some() => self.failed("settle", &failure),
            Err(_) => {}
        }
    }

    async fn execute(
        &self,
        lane: &Lane,
        job: &Leased,
        cancelled: watch::Receiver<bool>,
    ) -> Outcome {
        let Some(payload) = job
            .payload
            .as_deref()
            .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok())
        else {
            return Outcome::Undecodable;
        };
        let Some(handler) = self.handlers.get(&job.kind).cloned() else {
            return Outcome::Undecodable;
        };
        // Leased kinds are registered identities; a correlation id written
        // around the checks falls back to one naming the job.
        let (Ok(kind), Ok(correlation_id)) = (
            Identity::new(job.kind.as_str()),
            Identity::new(job.correlation_id.as_str())
                .or_else(|_| Identity::new(format!("job_{}", job.id))),
        ) else {
            return Outcome::Undecodable;
        };
        let deadline = tokio::time::Instant::now() + lane.plan.settings.timeout;
        let ctx = JobContext {
            id: job.id,
            kind,
            attempt: job.attempt,
            max_attempts: job.max_attempts,
            idempotency_key: job.idempotency_key.clone(),
            deadline,
            cancel: cancelled,
            correlation_id,
            causation_id: job
                .causation_id
                .as_deref()
                .and_then(|c| Identity::new(c).ok()),
            tenant: job.tenant.as_deref().and_then(|t| Identity::new(t).ok()),
        };
        // A task of its own, so a panic is caught as `JoinError::is_panic`.
        let mut task = tokio::spawn(async move { handler.handle(ctx, payload).await });
        tokio::select! {
            joined = &mut task => match joined {
                Ok(Ok(())) => Outcome::Succeeded,
                Ok(Err(failure)) => Outcome::Failed(failure),
                Err(error) if error.is_panic() => Outcome::Panicked,
                Err(_) => Outcome::TimedOut,
            },
            () = tokio::time::sleep_until(deadline) => {
                if let Some(entry) = lock(&self.in_flight).get(&job.token) {
                    entry.cancel.send_replace(true);
                }
                task.abort();
                Outcome::TimedOut
            }
        }
    }

    /// Stop leasing, wait for in-flight jobs up to `jobs.drain_timeout_ms`
    /// (within `until`), fire `cancel`, give the grace, then release what
    /// is left with the attempt refunded (AP §B.8).
    pub(crate) async fn drain(&self, until: Instant) {
        self.stop.send_replace(true);
        let release_by = until.checked_sub(RELEASE_MARGIN).unwrap_or(until);
        // A lease in flight when the stop came is held before its
        // dispatcher exits, so the release below covers it.
        let dispatchers: Vec<_> = std::mem::take(&mut *lock(&self.dispatchers));
        for mut dispatcher in dispatchers {
            let at = tokio::time::Instant::from_std(release_by);
            if tokio::time::timeout_at(at, &mut dispatcher).await.is_err() {
                dispatcher.abort();
            }
        }
        let wait_until = (Instant::now() + self.config.drain_timeout)
            .min(release_by.checked_sub(DRAIN_GRACE).unwrap_or(release_by));
        self.wait_idle(wait_until).await;
        if self.running() > 0 {
            for entry in lock(&self.in_flight).values_mut() {
                entry.cut = true;
                entry.cancel.send_replace(true);
            }
            self.wait_idle((Instant::now() + DRAIN_GRACE).min(release_by))
                .await;
        }
        let tokens: Vec<String> = {
            let in_flight = lock(&self.in_flight);
            in_flight.keys().cloned().collect()
        };
        if !tokens.is_empty() {
            match self.release(&tokens).await {
                Ok(ids) => {
                    let logged: Vec<i64> = ids.iter().copied().take(MAX_LOGGED_IDS).collect();
                    tracing::warn!(
                        event = "job.drain_abandoned",
                        count = ids.len(),
                        job_ids = ?logged
                    );
                }
                Err(failure) => self.failed("release", &failure),
            }
            let mut in_flight = lock(&self.in_flight);
            for token in &tokens {
                in_flight.remove(token);
            }
        }
        self.stop_heartbeat.send_replace(true);
        let tasks: Vec<_> = std::mem::take(&mut *lock(&self.tasks));
        for mut task in tasks {
            let at = tokio::time::Instant::from_std(until);
            if tokio::time::timeout_at(at, &mut task).await.is_err() {
                task.abort();
            }
        }
    }

    async fn wait_idle(&self, until: Instant) {
        let at = tokio::time::Instant::from_std(until);
        loop {
            let changed = self.changed.notified();
            if self.running() == 0 || Instant::now() >= until {
                return;
            }
            if tokio::time::timeout_at(at, changed).await.is_err() {
                return;
            }
        }
    }

    async fn release(&self, tokens: &[String]) -> Result<Vec<i64>, Failure> {
        let client = self.database.acquire(deadline()).await?;
        let at = tokio::time::Instant::from_std(deadline());
        let rows = tokio::time::timeout_at(at, client.query(RELEASE, &[&tokens]))
            .await
            .map_err(|_| Failure::transient("database_timeout"))?
            .map_err(|e| classify(&e))?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    /// One heartbeat: extend every held lease; a token missing from the
    /// result lost its lease, so its handler's `cancel` fires.
    async fn beat(&self) -> Result<(), Failure> {
        let (tokens, secs): (Vec<String>, Vec<f64>) = lock(&self.in_flight)
            .iter()
            .filter(|(_, entry)| !entry.lost)
            .map(|(token, entry)| (token.clone(), entry.lease_secs))
            .unzip();
        if tokens.is_empty() {
            return Ok(());
        }
        let client = self.database.acquire(deadline()).await?;
        let at = tokio::time::Instant::from_std(deadline());
        let rows = tokio::time::timeout_at(at, client.query(HEARTBEAT, &[&tokens, &secs]))
            .await
            .map_err(|_| Failure::transient("database_timeout"))?
            .map_err(|e| classify(&e))?;
        let alive: HashSet<String> = rows.iter().map(|row| row.get(0)).collect();
        for token in tokens.iter().filter(|token| !alive.contains(*token)) {
            if let Some(entry) = lock(&self.in_flight).get_mut(token) {
                entry.lost = true;
                entry.cancel.send_replace(true);
            }
        }
        Ok(())
    }

    /// Reclaim expired leases, batch after batch.
    async fn reap(&self) -> Result<usize, Failure> {
        let mut queues = Vec::new();
        let mut bases = Vec::new();
        let mut maxima = Vec::new();
        for lane in &self.lanes {
            queues.push(lane.plan.name.clone());
            bases.push(lane.plan.settings.backoff_base.as_secs_f64());
            maxima.push(lane.plan.settings.backoff_max.as_secs_f64());
        }
        let base = self.defaults.backoff_base.as_secs_f64();
        let max = self.defaults.backoff_max.as_secs_f64();
        let mut total = 0;
        for _ in 0..MAX_REAP_BATCHES {
            let client = self.database.acquire(deadline()).await?;
            let at = tokio::time::Instant::from_std(deadline());
            let rows = tokio::time::timeout_at(
                at,
                client.query(REAP, &[&queues, &bases, &maxima, &base, &max]),
            )
            .await
            .map_err(|_| Failure::transient("database_timeout"))?
            .map_err(|e| classify(&e))?;
            for row in &rows {
                let (id, queue, kind, attempt, state): (i64, String, String, i32, String) =
                    (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
                if state == "dead" {
                    tracing::error!(
                        event = "job.dead_lettered",
                        job_id = id,
                        queue,
                        kind,
                        attempt,
                        code = "lease_expired",
                        classification = "transient"
                    );
                } else {
                    tracing::warn!(
                        event = "job.retry_scheduled",
                        job_id = id,
                        queue,
                        kind,
                        attempt,
                        code = "lease_expired",
                        classification = "transient"
                    );
                }
            }
            total += rows.len();
            if rows.len() < REAP_BATCH {
                break;
            }
        }
        let client = self.database.acquire(deadline()).await?;
        let at = tokio::time::Instant::from_std(deadline());
        let claim_timeout = self.config.claim_timeout.as_secs_f64();
        let reverted =
            tokio::time::timeout_at(at, client.execute(REVERT_CLAIMS, &[&claim_timeout]))
                .await
                .map_err(|_| Failure::transient("database_timeout"))?
                .map_err(|e| classify(&e))?;
        if reverted > 0 {
            tracing::info!(
                event = "job.admin_action",
                action = "reissue_reverted",
                count = reverted
            );
        }
        Ok(total)
    }

    async fn measure_lag(&self, max: Duration) -> Result<(), Failure> {
        let client = self.database.acquire(deadline()).await?;
        let at = tokio::time::Instant::from_std(deadline());
        let row = tokio::time::timeout_at(at, client.query_one(LAG, &[]))
            .await
            .map_err(|_| Failure::transient("database_timeout"))?
            .map_err(|e| classify(&e))?;
        let lag: f64 = row.get(0);
        self.lagging
            .store(lag > max.as_secs_f64(), Ordering::Relaxed);
        Ok(())
    }
}

fn log_settled(job: &Leased, queue: &str, settlement: &Settlement) {
    let kind = job.kind.as_str();
    match settlement {
        Settlement::Succeeded => tracing::debug!(
            event = "job.completed",
            job_id = job.id,
            queue,
            kind,
            attempt = job.attempt
        ),
        Settlement::Retry { code, class, .. } => tracing::warn!(
            event = "job.retry_scheduled",
            job_id = job.id,
            queue,
            kind,
            attempt = job.attempt,
            code = code.as_str(),
            classification = *class
        ),
        Settlement::Dead { code, class } => tracing::error!(
            event = "job.dead_lettered",
            job_id = job.id,
            queue,
            kind,
            attempt = job.attempt,
            code = code.as_str(),
            classification = *class
        ),
    }
}

/// One queue's dispatcher: lease while slots are free, woken by
/// `merezha_jobs` notifications, a slot freeing and the poll interval.
async fn dispatch(worker: Arc<Worker>, lane: Arc<Lane>) {
    let mut stop = worker.stop.subscribe();
    loop {
        if *stop.borrow() {
            return;
        }
        let free = lane.slots.available_permits();
        let mut full = false;
        if free > 0 {
            let permits = lane.slots.clone().try_acquire_many_owned(free as u32);
            if let Ok(mut permits) = permits {
                match worker.lease(&lane, free).await {
                    Ok(jobs) => {
                        worker.recovered("lease");
                        full = jobs.len() == free;
                        let stopping = *stop.borrow();
                        for job in jobs {
                            let cancelled = worker.hold(&lane, &job);
                            if stopping {
                                // Leased as the drain began: never run,
                                // released with the attempt refunded.
                                worker.park(&job.token);
                                continue;
                            }
                            let Some(permit) = permits.split(1) else {
                                // Never happens: one permit per leased job.
                                // Unheld, the lease expires to the reaper.
                                worker.untrack(&job.token);
                                continue;
                            };
                            let worker = worker.clone();
                            let lane = lane.clone();
                            tokio::spawn(async move {
                                let wake = lane.clone();
                                worker.run(lane, job, cancelled).await;
                                drop(permit);
                                wake.wake.notify_one();
                            });
                        }
                    }
                    Err(failure) => worker.failed("lease", &failure),
                }
            }
        }
        if full {
            continue;
        }
        tokio::select! {
            _ = stop.changed() => {}
            () = lane.wake.notified() => {}
            () = tokio::time::sleep(worker.config.poll_interval) => {}
        }
    }
}

/// Extend held leases every third of the shortest lease.
async fn heartbeat(worker: Arc<Worker>) {
    let every = worker
        .lanes
        .iter()
        .map(|lane| lane.plan.settings.lease)
        .min()
        .unwrap_or(worker.defaults.lease)
        / 3;
    let mut stop = worker.stop_heartbeat.subscribe();
    loop {
        tokio::select! {
            _ = stop.changed() => return,
            () = tokio::time::sleep(every) => {}
        }
        match worker.beat().await {
            Ok(()) => worker.recovered("heartbeat"),
            Err(failure) => worker.failed("heartbeat", &failure),
        }
    }
}

/// Reclaim expired leases every `jobs.reaper_interval_ms` and, with
/// `jobs.readiness.max_lag_ms`, measure the backlog for readiness.
async fn reaper(worker: Arc<Worker>) {
    let mut stop = worker.stop.subscribe();
    loop {
        match worker.reap().await {
            Ok(_) => worker.recovered("reaper"),
            Err(failure) => worker.failed("reaper", &failure),
        }
        if let Some(max) = worker.config.max_lag {
            match worker.measure_lag(max).await {
                Ok(()) => worker.recovered("lag"),
                Err(failure) => worker.failed("lag", &failure),
            }
        }
        tokio::select! {
            _ = stop.changed() => return,
            () = tokio::time::sleep(worker.config.reaper_interval) => {}
        }
        if *stop.borrow() {
            return;
        }
    }
}

/// Retention at start and then every [`crate::RETENTION_INTERVAL`].
async fn retain(worker: Arc<Worker>) {
    let mut stop = worker.stop.subscribe();
    loop {
        match crate::retention::purge(&worker.database, &worker.config.retention).await {
            Ok(_) => worker.recovered("retention"),
            Err(failure) => worker.failed("retention", &failure),
        }
        tokio::select! {
            _ = stop.changed() => return,
            () = tokio::time::sleep(crate::RETENTION_INTERVAL) => {}
        }
        if *stop.borrow() {
            return;
        }
    }
}

/// `LISTEN merezha_jobs`: each notification names a queue and wakes its
/// dispatcher. A lost session falls back to the poll interval and
/// reconnects; an idle one is probed.
async fn listen(worker: Arc<Worker>) {
    let mut stop = worker.stop.subscribe();
    let lanes: HashMap<String, Arc<Lane>> = worker
        .lanes
        .iter()
        .map(|lane| (lane.plan.name.clone(), lane.clone()))
        .collect();
    let mut lost = false;
    loop {
        if *stop.borrow() {
            return;
        }
        let mut session: Listener = match worker
            .database
            .listen(&[crate::JOBS_CHANNEL], Instant::now() + LISTEN_DEADLINE)
            .await
        {
            Ok(session) => {
                // Every way out of a session sets `lost` again.
                if lost {
                    tracing::info!(event = "db.listener_restored");
                }
                session
            }
            Err(failure) => {
                if !lost {
                    tracing::warn!(event = "db.listener_lost", code = failure.code.as_str());
                }
                lost = true;
                tokio::select! {
                    _ = stop.changed() => return,
                    () = tokio::time::sleep(worker.config.poll_interval) => {}
                }
                continue;
            }
        };
        // Wake every queue once: notifications sent before the session
        // opened were missed.
        for lane in lanes.values() {
            lane.wake.notify_one();
        }
        loop {
            tokio::select! {
                _ = stop.changed() => return,
                received = session.recv() => match received {
                    Some(notification) => {
                        if let Some(lane) = lanes.get(&notification.payload) {
                            lane.wake.notify_one();
                        }
                    }
                    None => {
                        tracing::warn!(event = "db.listener_lost", code = "database_unavailable");
                        lost = true;
                        break;
                    }
                },
                () = tokio::time::sleep(worker.config.listen_check) => {
                    if let Err(failure) = session.check(Instant::now() + LISTEN_DEADLINE).await {
                        tracing::warn!(event = "db.listener_lost", code = failure.code.as_str());
                        lost = true;
                        break;
                    }
                }
            }
        }
    }
}
