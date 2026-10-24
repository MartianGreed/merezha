//! The jobs port (AP §B.3): enqueue in an own transaction, cancel, handler
//! registration and topic subscriptions. `merezha-jobs` implements it;
//! modules reach it through [`crate::AppContext::jobs`], bound to their
//! name like the outbox.
//!
//! [`Jobs::queue_status`] is the read behind the jobs admin API's
//! `GET queues` and `GET outbox` and TE's status and gauges (R10).
//! [`Jobs::schedule`] declares a module's cron schedules (AP §B.7).
use super::{DedupeKey, JobFailure, MessageContext, Source};
use crate::BoxFuture;
use merezha_domain::{Failure, Identity};
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;

/// A job enqueued outside any caller transaction (R1). The stored
/// idempotency key is `{source}:{idempotency_key}`, the key an outbox `Job`
/// record of the same source and dedupe key relays to, so both paths
/// deduplicate against each other. `Debug` never prints the payload.
#[derive(Clone, PartialEq)]
pub struct NewJob {
    pub queue: Identity,
    pub kind: Identity,
    pub idempotency_key: DedupeKey,
    /// At most 64 KiB encoded, JSON-compatible numbers only.
    pub payload: Value,
    /// R3: the payload is nulled at the job's terminal state.
    pub contains_secret: bool,
    /// At most 365 days ahead; `None` runs now.
    pub run_at: Option<jiff::Timestamp>,
    pub priority: i16,
    pub context: MessageContext,
}
impl std::fmt::Debug for NewJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NewJob")
            .field("queue", &self.queue)
            .field("kind", &self.kind)
            .field("idempotency_key", &self.idempotency_key)
            .field("contains_secret", &self.contains_secret)
            .field("run_at", &self.run_at)
            .field("priority", &self.priority)
            .field("context", &self.context)
            .finish_non_exhaustive()
    }
}

/// The outcome of [`JobsHandle::enqueue`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Enqueued {
    pub id: i64,
    /// The `(queue, idempotency_key)` job existed with the same kind and
    /// payload digest; nothing was written.
    pub existed: bool,
}

/// What a handler sees of the job it runs (AP §B.3).
#[derive(Clone, Debug)]
pub struct JobContext {
    pub id: i64,
    pub kind: Identity,
    pub attempt: u32,
    pub max_attempts: u32,
    /// Carried into the handler so it can deduplicate its effect: delivery
    /// is at least once.
    pub idempotency_key: String,
    pub deadline: tokio::time::Instant,
    /// Turns `true` on lease loss, at the drain cutoff and at `deadline`
    /// (the queue's `timeout_ms`), where the handler is also aborted.
    pub cancel: tokio::sync::watch::Receiver<bool>,
    pub correlation_id: Identity,
    pub causation_id: Option<Identity>,
    pub tenant: Option<Identity>,
}

/// Runs one kind of job. Outcomes drive retry through
/// [`JobFailure`]'s classification (AP §B.6).
pub trait JobHandler: Send + Sync {
    fn handle<'a>(
        &'a self,
        ctx: JobContext,
        payload: Value,
    ) -> BoxFuture<'a, Result<(), JobFailure>>;
    /// Whether this instance may lease jobs of the kind now. Read before
    /// every lease; `false` leaves the kind's jobs available for other
    /// instances. Content turns it off on a host whose model a newer one
    /// superseded (CT §2, mixed model versions).
    fn accepts_leases(&self) -> bool {
        true
    }
}

/// What a reissue hook learns about a dead flagged job whose payload is
/// gone (R3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReissueRequest {
    pub job_id: i64,
    pub kind: Identity,
    pub idempotency_key: String,
    pub correlation_id: Identity,
    pub causation_id: Option<Identity>,
    pub tenant: Option<Identity>,
}
/// Asks the owning module to issue a dead flagged job's effect again.
pub trait Reissue: Send + Sync {
    fn reissue(&self, request: ReissueRequest) -> BoxFuture<'_, Result<(), Failure>>;
}

/// Per-kind options given to [`JobsHandle::register`].
#[derive(Clone, Default)]
pub struct KindOptions {
    /// Replays a dead flagged job whose payload is gone by asking the
    /// owning module to issue the effect again. `None`: such a replay is
    /// refused with `secret_purged`.
    pub reissue: Option<Arc<dyn Reissue>>,
}
impl std::fmt::Debug for KindOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KindOptions")
            .field("reissue", &self.reissue.is_some())
            .finish()
    }
}

/// One kind's handler, as registered.
#[derive(Clone)]
pub struct JobRegistration {
    pub kind: Identity,
    pub queue: Identity,
    pub handler: Arc<dyn JobHandler>,
    pub options: KindOptions,
}
impl std::fmt::Debug for JobRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobRegistration")
            .field("kind", &self.kind)
            .field("queue", &self.queue)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

/// A cron schedule a module declares through [`JobsHandle::schedule`]
/// (AP §B.7), stored with the module's name as its source. Each occurrence
/// enqueues one job of `kind` on `queue` with `payload`. `Debug` never
/// prints the payload.
#[derive(Clone, PartialEq)]
pub struct ScheduleDeclaration {
    /// `[a-z][a-z0-9_.-]{0,63}`, unique across config and modules.
    pub name: String,
    /// Five fields (minute to day of week); see [`crate::cron`].
    pub cron: String,
    /// An IANA zone such as `Europe/Paris`, or `UTC`.
    pub timezone: String,
    pub queue: Identity,
    /// A kind the declaring module registered.
    pub kind: Identity,
    /// At most 64 KiB encoded, JSON-compatible numbers only.
    pub payload: Value,
}
impl std::fmt::Debug for ScheduleDeclaration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScheduleDeclaration")
            .field("name", &self.name)
            .field("cron", &self.cron)
            .field("timezone", &self.timezone)
            .field("queue", &self.queue)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// One queue in [`QueueStatus`], with TE's state names: `ready` and
/// `delayed` are `available` jobs whose `run_at` is past or future,
/// `leased` is `running`, `dead` is `dead`; terminal rows are not counted.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct QueueSummary {
    pub name: String,
    pub ready: u64,
    pub delayed: u64,
    pub leased: u64,
    pub dead: u64,
    /// Age in seconds of the oldest due `available` job, 0 when none
    /// (TE's `merezha.job.lag`).
    pub oldest_ready_age_s: f64,
    /// This instance's per-queue slots.
    pub concurrency: u32,
    /// Cluster-wide cap; 0 is unlimited.
    pub global_concurrency: u32,
}
/// A quarantined outbox record, as `GET outbox` lists it. No payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct QuarantinedRecord {
    pub id: i64,
    pub source: String,
    pub record_kind: String,
    pub quarantine_code: String,
    pub quarantined_at: jiff::Timestamp,
}
/// The outbox in [`QueueStatus`].
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OutboxSummary {
    /// Records not relayed yet, quarantined ones included.
    pub pending: u64,
    /// Age in seconds of the oldest of them, 0 when none.
    pub oldest_age_s: f64,
    /// Event records this instance relayed without a subscription since it
    /// started.
    pub unrouted: u64,
    /// At most [`MAX_QUARANTINED_LISTED`], oldest first.
    pub quarantined: Vec<QuarantinedRecord>,
}
/// Most quarantined records [`OutboxSummary`] lists.
pub const MAX_QUARANTINED_LISTED: usize = 100;
/// Most queues [`QueueStatus`] lists.
pub const MAX_QUEUES_LISTED: usize = 256;
/// Queue and outbox state from the database (AP §D.1 `GET queues` and
/// `GET outbox`), read by TE's status endpoint and gauges (R10). Every
/// field is a count, a configured name, a code or a timestamp.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct QueueStatus {
    /// Configured queues, queues of registered kinds and queues holding
    /// jobs, by name, at most [`MAX_QUEUES_LISTED`].
    pub queues: Vec<QueueSummary>,
    pub outbox: OutboxSummary,
}

/// The jobs runtime as modules use it. Registration and subscriptions are
/// declared before the runtime starts, from [`crate::Module::start`];
/// later calls fail.
pub trait Jobs: Send + Sync {
    /// Own transaction; idempotent on `(queue, {source}:{key})`. The same
    /// key with another kind or payload fails conflict
    /// `idempotency_mismatch`.
    fn enqueue<'a>(
        &'a self,
        source: &'a Source,
        job: NewJob,
    ) -> BoxFuture<'a, Result<Enqueued, Failure>>;
    /// `available` to `cancelled`, nulling a flagged payload, for a job
    /// `source` owns: its key starts with `{source}:` (enqueued or relayed
    /// from its own `Job` records) or its kind is one `source` registered.
    /// `false` when the job is missing, running, terminal or another
    /// module's, without telling those apart.
    fn cancel<'a>(&'a self, source: &'a Source, id: i64) -> BoxFuture<'a, Result<bool, Failure>>;
    /// Declare the handler of a kind. A kind has one owner.
    fn register(&self, source: &Source, registration: JobRegistration) -> Result<(), Failure>;
    /// Fan event records on `topic` out to jobs of `kind`, which `source`
    /// registered; stored in `merezha_jobs.subscriptions` at start.
    fn subscribe(&self, source: &Source, topic: &Identity, kind: &Identity) -> Result<(), Failure>;
    /// Declare a cron schedule for a kind `source` registered; stored in
    /// `merezha_jobs.schedules` at start and re-declared on every
    /// heartbeat, like subscriptions.
    fn schedule(&self, source: &Source, schedule: ScheduleDeclaration) -> Result<(), Failure>;
    /// Queue and outbox state, read in one bounded pass.
    fn queue_status(&self) -> BoxFuture<'_, Result<QueueStatus, Failure>>;
}

/// The jobs port bound to one source, as [`crate::AppContext::jobs`] hands
/// it out.
#[derive(Clone)]
pub struct JobsHandle {
    source: Source,
    inner: Arc<dyn Jobs>,
}
impl std::fmt::Debug for JobsHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobsHandle")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}
impl JobsHandle {
    pub fn new(source: Source, inner: Arc<dyn Jobs>) -> Self {
        Self { source, inner }
    }
    pub fn source(&self) -> &Source {
        &self.source
    }
    /// See [`Jobs::enqueue`].
    pub async fn enqueue(&self, job: NewJob) -> Result<Enqueued, Failure> {
        self.inner.enqueue(&self.source, job).await
    }
    /// See [`Jobs::cancel`].
    pub async fn cancel(&self, id: i64) -> Result<bool, Failure> {
        self.inner.cancel(&self.source, id).await
    }
    /// Declare the handler of `kind` on `queue`; must run before the jobs
    /// runtime starts. Fails permanent `validation_failed` for names that
    /// are not identities, and the implementation's codes.
    pub fn register(
        &self,
        kind: &str,
        queue: &str,
        handler: Arc<dyn JobHandler>,
        options: KindOptions,
    ) -> Result<(), Failure> {
        self.inner.register(
            &self.source,
            JobRegistration {
                kind: Identity::new(kind)?,
                queue: Identity::new(queue)?,
                handler,
                options,
            },
        )
    }
    /// Subscribe this source's registered `kind` to `topic`.
    pub fn subscribe(&self, topic: &str, kind: &str) -> Result<(), Failure> {
        self.inner
            .subscribe(&self.source, &Identity::new(topic)?, &Identity::new(kind)?)
    }
    /// Declare a cron schedule for this source's registered `kind`; must
    /// run before the jobs runtime starts. See [`Jobs::schedule`].
    pub fn schedule(&self, schedule: ScheduleDeclaration) -> Result<(), Failure> {
        self.inner.schedule(&self.source, schedule)
    }
    /// See [`Jobs::queue_status`].
    pub async fn queue_status(&self) -> Result<QueueStatus, Failure> {
        self.inner.queue_status().await
    }
}

/// Fallback without the jobs runtime: every call fails permanent
/// `jobs_unavailable`, so nothing is silently dropped.
#[derive(Debug, Default)]
pub struct UnavailableJobs;
impl Jobs for UnavailableJobs {
    fn enqueue<'a>(&'a self, _: &'a Source, _: NewJob) -> BoxFuture<'a, Result<Enqueued, Failure>> {
        Box::pin(async { Err(Failure::permanent("jobs_unavailable")) })
    }
    fn cancel<'a>(&'a self, _: &'a Source, _: i64) -> BoxFuture<'a, Result<bool, Failure>> {
        Box::pin(async { Err(Failure::permanent("jobs_unavailable")) })
    }
    fn register(&self, _: &Source, _: JobRegistration) -> Result<(), Failure> {
        Err(Failure::permanent("jobs_unavailable"))
    }
    fn subscribe(&self, _: &Source, _: &Identity, _: &Identity) -> Result<(), Failure> {
        Err(Failure::permanent("jobs_unavailable"))
    }
    fn schedule(&self, _: &Source, _: ScheduleDeclaration) -> Result<(), Failure> {
        Err(Failure::permanent("jobs_unavailable"))
    }
    fn queue_status(&self) -> BoxFuture<'_, Result<QueueStatus, Failure>> {
        Box::pin(async { Err(Failure::permanent("jobs_unavailable")) })
    }
}
