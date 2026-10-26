//! Projection runs on PostgreSQL (AP §A.7): live catch-up and resumable
//! rebuild, each under a session advisory lock held on a connection
//! detached from the pool.
//!
//! A run is synchronous: it executes on a blocking thread (the scheduler's
//! `spawn_blocking`, or the HTTP dispatch for explicit catch-ups) and runs
//! each statement with `Handle::block_on`, bounded by the run's hard
//! deadline. The run owns its [`DetachedClient`]; dropping it on any exit
//! (success, failure, a cancelled step, a guest panic) closes the session,
//! which releases the lock and rolls back an open transaction. No pooled
//! session ever takes a session lock.
use super::{
    PostgresStore, decode, encode, invalid, read_error, signed, sql, stored_state, unsigned,
};
use crate::{
    Application,
    application::{ProjectionState, check_value},
    config::{MAX_STATE_BYTES, Projection},
    instrument::{ProjectionRun, RunFailure},
};
use merezha_domain::{Failure, Identity, Operation, Reply, StoredEvent};
use merezha_postgres::DetachedClient;
use merezha_telemetry::{names::dispatch::Trigger, propagation::StoredContext};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Notify, Semaphore, watch};

/// How long a background run waits for another wake after it caught up,
/// before it releases its lock (§A.7 step 4).
pub(crate) const LINGER: Duration = Duration::from_secs(1);
/// Poll interval of a request waiting on another runner's progress (§A.6).
pub(crate) const POLL: Duration = Duration::from_millis(50);
/// An explicit request retries the run lock at most this often while
/// another session holds it.
const RETRY_LOCK: Duration = Duration::from_secs(1);

/// When background live catch-up runs (`[application.projections]
/// catch_up`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CatchUp {
    /// On a local commit, a `merezha_app_events` notification and every
    /// interval.
    #[default]
    Auto,
    /// Only through the admin endpoints. New revisions still warm up and
    /// requested rebuilds still run in the background.
    Manual,
}

/// Background runner settings (`merezha.toml` `[application.projections]`
/// and `[application.background]`, §D.2).
#[derive(Clone, Debug)]
pub struct BackgroundOptions {
    pub catch_up: CatchUp,
    /// The safety-net wake for lost notifications and external restores.
    pub interval: Duration,
    /// How long one background run may keep its lock; every statement of
    /// the run ends 5 s later at the latest.
    pub run_timeout: Duration,
    /// Hold readiness until this instance's configured revisions are warm.
    pub warm_readiness: bool,
    /// Background runs at once on this instance (projection and rebuild),
    /// and separately the explicit catch-ups holding their own lock.
    pub max_concurrent: usize,
    /// `app.instance_id`, reported by [`Application::status`].
    pub instance_id: String,
    /// The reactors' safety-net wake (`[application.reactors]
    /// interval_ms`).
    pub reactor_interval: Duration,
    /// The reactors' equal-jitter retry backoff, from `retry_base_ms` up
    /// to `retry_max_ms` (AP §C.5).
    pub reactor_retry: (Duration, Duration),
    /// Test hooks; production leaves the default (none).
    #[doc(hidden)]
    pub hooks: RunHooks,
}
impl Default for BackgroundOptions {
    fn default() -> Self {
        Self {
            catch_up: CatchUp::Auto,
            interval: Duration::from_secs(5),
            run_timeout: Duration::from_secs(30),
            warm_readiness: true,
            max_concurrent: 2,
            instance_id: String::new(),
            reactor_interval: Duration::from_secs(5),
            reactor_retry: (Duration::from_secs(1), Duration::from_secs(300)),
            hooks: RunHooks::default(),
        }
    }
}
/// Bounds of [`BackgroundOptions`].
pub const INTERVAL: (Duration, Duration) = (Duration::from_millis(100), Duration::from_secs(600));
pub const RUN_TIMEOUT: (Duration, Duration) = (Duration::from_secs(1), Duration::from_secs(300));
pub const MAX_CONCURRENT: (usize, usize) = (1, 16);
/// A `database_timeout` this close to the run's deadline is the deadline.
const DEADLINE_SLACK: Duration = Duration::from_millis(50);
/// A background run's statements end this long after `run_timeout`.
const HARD_MARGIN: Duration = Duration::from_secs(5);
impl BackgroundOptions {
    pub(crate) fn validate(&self) -> Result<(), Failure> {
        let in_range = |value, (min, max)| (min..=max).contains(&value);
        if !in_range(self.interval, INTERVAL)
            || !in_range(self.reactor_interval, INTERVAL)
            || self.reactor_retry.0 > self.reactor_retry.1
            || self.reactor_retry.1 > Duration::from_secs(86_400)
            || !in_range(self.run_timeout, RUN_TIMEOUT)
            || !(MAX_CONCURRENT.0..=MAX_CONCURRENT.1).contains(&self.max_concurrent)
            || self.instance_id.len() > 256
        {
            return Err(Failure::permanent("storage_configuration"));
        }
        Ok(())
    }
    pub(crate) fn hooks(&self) -> RunHooks {
        self.hooks.clone()
    }
}
/// Test hooks for the cancellation and lost-CAS tests.
#[derive(Clone, Debug, Default)]
#[doc(hidden)]
pub struct RunHooks {
    /// A one-shot pause inside a run's commit transaction, after its
    /// writes and before `COMMIT`.
    pub pause: Option<Arc<Pause>>,
    /// Bound every background run's statements by this instead of
    /// `run_timeout + 5 s`.
    pub hard_timeout: Option<Duration>,
    /// Run without the advisory lock, as when a lock session died.
    pub skip_lock: bool,
    /// Start no `LISTEN` session: only local wakes and the interval.
    pub skip_listen: bool,
    /// A one-shot pause of a background run after it took its slot and
    /// before its first step.
    pub before_run: Option<Arc<Pause>>,
    /// A one-shot pause of a rebuild run before it reads its candidate for
    /// the next batch.
    pub rebuild_batch: Option<Arc<Pause>>,
    /// A one-shot failure of a reactor's checkpoint transaction after its
    /// writes and before `COMMIT`, which rolls everything back.
    pub reactor_fail: Option<Arc<FailOnce>>,
}
/// See [`RunHooks::reactor_fail`].
#[derive(Debug, Default)]
#[doc(hidden)]
pub struct FailOnce {
    armed: AtomicBool,
    fired: AtomicBool,
    reactor: Mutex<String>,
}
impl FailOnce {
    /// Fail the next checkpoint transaction of `reactor` with transient
    /// `test_rollback`.
    pub fn arm(&self, reactor: &str) {
        *self.reactor.lock().unwrap_or_else(|e| e.into_inner()) = reactor.to_owned();
        self.fired.store(false, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
    }
    /// Whether it failed a transaction since the last `arm`.
    pub fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }
    pub(crate) fn check(&self, reactor: &str) -> Result<(), Failure> {
        if *self.reactor.lock().unwrap_or_else(|e| e.into_inner()) == reactor
            && self.armed.swap(false, Ordering::SeqCst)
        {
            self.fired.store(true, Ordering::SeqCst);
            return Err(Failure::transient("test_rollback"));
        }
        Ok(())
    }
}
/// See [`RunHooks::pause`].
#[derive(Debug, Default)]
#[doc(hidden)]
pub struct Pause {
    armed: AtomicBool,
    entered: AtomicBool,
    millis: AtomicU64,
    projection: Mutex<String>,
    /// Pause in a server-side `pg_sleep` instead, so the backend is busy
    /// in a statement when the run is cut.
    statement: AtomicBool,
}
impl Pause {
    /// Pause the next commit of `projection` for `duration`.
    pub fn arm(&self, projection: &str, duration: Duration) {
        *self.projection.lock().unwrap_or_else(|e| e.into_inner()) = projection.to_owned();
        self.millis
            .store(duration.as_millis() as u64, Ordering::SeqCst);
        self.statement.store(false, Ordering::SeqCst);
        self.entered.store(false, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
    }
    /// Like [`Pause::arm`], with the backend running `pg_sleep` meanwhile.
    pub fn arm_statement(&self, projection: &str, duration: Duration) {
        self.arm(projection, duration);
        self.statement.store(true, Ordering::SeqCst);
    }
    /// Whether a commit is pausing (or paused) since the last `arm`.
    pub fn entered(&self) -> bool {
        self.entered.load(Ordering::SeqCst)
    }
}

/// The two run kinds and their lock prefixes (`p:` live, `b:` rebuild).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Live,
    Rebuild,
}
impl Kind {
    fn prefix(self) -> &'static str {
        ["p", "b"][self as usize]
    }
}

/// Per configured revision, shared by the scheduler, the runs and the
/// application on this instance.
#[derive(Default)]
pub(crate) struct Lane {
    /// Wakes the live lane; signals coalesce.
    pub live: Notify,
    /// Wakes the rebuild lane.
    pub rebuild: Notify,
    /// Whether the checkpoint reached `warm_position`, as last seen here.
    pub warm: AtomicBool,
    /// Since when this instance holds the live and the rebuild lock.
    held: [Mutex<Option<Instant>>; 2],
    /// Lock attempts that found another session holding it.
    pub contended: [AtomicU64; 2],
}
/// Runtime state of one instance's store: wakes, lanes, budgets.
pub(crate) struct Shared {
    pub options: BackgroundOptions,
    pub lanes: BTreeMap<(String, i32), Lane>,
    /// Bumped on every local projection commit and swap; waiting requests
    /// re-read on it and poll every 50 ms besides.
    pub published: watch::Sender<u64>,
    /// Explicit catch-ups holding their own lock session.
    pub explicit: Arc<Semaphore>,
    /// Background runs in progress.
    pub runs: AtomicUsize,
    /// 0: no listener, 1: connected, 2: reconnecting.
    pub listener: AtomicU8,
    /// Bumped to cancel every in-flight background run step.
    pub cancel: watch::Sender<u64>,
    /// Per configured reactor (AP §C.5).
    pub reactors: BTreeMap<String, super::reactor::ReactorLane>,
}
impl Shared {
    pub(crate) fn new(
        options: BackgroundOptions,
        projections: &[Projection],
        reactors: &[crate::config::Reactor],
    ) -> Self {
        let lanes = projections
            .iter()
            .map(|p| {
                (
                    (p.name.as_str().to_owned(), p.revision as i32),
                    Lane::default(),
                )
            })
            .collect();
        Self {
            explicit: Arc::new(Semaphore::new(options.max_concurrent)),
            options,
            lanes,
            published: watch::Sender::new(0),
            runs: AtomicUsize::new(0),
            listener: AtomicU8::new(0),
            cancel: watch::Sender::new(0),
            reactors: reactors
                .iter()
                .map(|r| (r.name.as_str().to_owned(), Default::default()))
                .collect(),
        }
    }
    pub(crate) fn lane(&self, projection: &Projection) -> Result<&Lane, Failure> {
        self.lanes
            .get(&(
                projection.name.as_str().to_owned(),
                projection.revision as i32,
            ))
            .ok_or_else(|| Failure::permanent("projection_not_found"))
    }
    /// After an `appended` outcome: wake every live lane and every
    /// reactor.
    pub(crate) fn appended(&self) {
        for lane in self.lanes.values() {
            lane.live.notify_one();
        }
        self.wake_reactors();
    }
    pub(crate) fn wake_reactors(&self) {
        for lane in self.reactors.values() {
            lane.wake.notify_one();
        }
    }
    pub(crate) fn publish(&self) {
        self.published.send_modify(|n| *n = n.wrapping_add(1));
    }
}
/// The sources of `merezha.projection.lock.held` and `.contended` (TE
/// §1.3): one series per configured projection, live and rebuild locks
/// together.
impl crate::instrument::LockSource for Shared {
    fn held(&self) -> Vec<(String, f64)> {
        let mut held = Vec::new();
        for ((name, _), lane) in &self.lanes {
            let longest = lane
                .held
                .iter()
                .filter_map(|slot| *slot.lock().unwrap_or_else(|e| e.into_inner()))
                .map(|since| since.elapsed().as_secs_f64())
                .reduce(f64::max);
            if let Some(seconds) = longest {
                held.push((name.clone(), seconds));
            }
        }
        held
    }
    fn contended(&self) -> Vec<(String, u64)> {
        self.lanes
            .iter()
            .map(|((name, _), lane)| {
                (
                    name.clone(),
                    lane.contended[0].load(Ordering::Relaxed)
                        + lane.contended[1].load(Ordering::Relaxed),
                )
            })
            .collect()
    }
    fn reactors_held(&self) -> Vec<(String, f64)> {
        self.reactors
            .iter()
            .filter_map(|(name, lane)| lane.held_for().map(|seconds| (name.clone(), seconds)))
            .collect()
    }
    fn reactors_contended(&self) -> Vec<(String, u64)> {
        self.reactors
            .iter()
            .map(|(name, lane)| (name.clone(), lane.contended.load(Ordering::Relaxed)))
            .collect()
    }
}
/// Clears the lock-held time when the run ends, however it ends.
struct Held<'a>(&'a Mutex<Option<Instant>>);
impl Drop for Held<'_> {
    fn drop(&mut self) {
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }
}

/// How one run is bounded.
pub(crate) struct Bounds {
    /// No batch starts after this.
    pub soft: Instant,
    /// Every statement and guest call ends by this.
    pub hard: Instant,
    /// A background run cut at `hard` fails `run_timeout`, a request
    /// `timeout`.
    pub background: bool,
    pub cancel: Option<watch::Receiver<u64>>,
}
impl Bounds {
    /// A background run started now, cancelled through `cancel`, which the
    /// scheduler subscribed before it checked for shutdown.
    pub(crate) fn background(shared: &Shared, cancel: watch::Receiver<u64>) -> Self {
        let now = Instant::now();
        let mut soft = now + shared.options.run_timeout;
        let mut hard = soft + HARD_MARGIN;
        if let Some(bound) = shared.options.hooks().hard_timeout {
            hard = now + bound;
            soft = soft.min(hard);
        }
        Self {
            soft,
            hard,
            background: true,
            cancel: Some(cancel),
        }
    }
    /// A request bounded by its admitted deadline.
    pub(crate) fn request(deadline: Instant) -> Self {
        Self {
            soft: deadline,
            hard: deadline,
            background: false,
            cancel: None,
        }
    }
    fn cut(&self) -> Failure {
        if self.background {
            Failure::transient("run_timeout")
        } else {
            Failure::transient("timeout")
        }
    }
}

/// What a run did.
#[derive(Debug)]
pub(crate) enum Outcome {
    /// Another session holds the lock; nothing ran.
    Contended,
    /// The run stopped with this served pair; `behind` when the head is
    /// past it (or, for a rebuild, when the candidate is not finished).
    Finished { pair: ProjectionState, behind: bool },
    /// Rebuild only: no running rebuild for this revision.
    Idle,
    /// Rebuild only: the candidate became the served pair.
    Swapped(ProjectionState),
}

/// The served row as a run reads it. A partitioned row's pair has a
/// `null` state; its states are the partitions of `generation`.
struct Row {
    pair: ProjectionState,
    row_revision: i64,
    warm_position: u64,
    generation: i32,
    partitioned: bool,
}
/// The states one partitioned batch produced: `(tenant, encoded state)`.
type Partitions = Vec<(String, String)>;
/// Up to this many partitions go in one sweep statement.
const SWEEP_CHUNK: u64 = 1000;

/// One run's locked session.
struct Session<'a> {
    store: &'a PostgresStore,
    bounds: &'a Bounds,
    client: DetachedClient,
    name: String,
    revision: i32,
    _held: Option<Held<'a>>,
}

impl PostgresStore {
    /// Run `work` on the store's runtime within the run's hard deadline;
    /// a cancellation drops it at once.
    pub(super) fn step<T>(
        &self,
        bounds: &Bounds,
        work: impl Future<Output = Result<T, Failure>>,
    ) -> Result<T, Failure> {
        if Instant::now() >= bounds.hard {
            return Err(bounds.cut());
        }
        let at = tokio::time::Instant::from_std(bounds.hard);
        let cut = bounds.cut();
        let mut cancel = bounds.cancel.clone();
        let result = self.handle.block_on(async move {
            let bounded = async move {
                tokio::time::timeout_at(at, work)
                    .await
                    .unwrap_or_else(|_| Err(cut))
            };
            match cancel.as_mut() {
                Some(cancel) => tokio::select! {
                    biased;
                    _ = cancel.changed() => Err(Failure::transient("cancelled")),
                    result = bounded => result,
                },
                None => bounded.await,
            }
        });
        // The kernel's transaction helper, the session statement timeout and
        // the pool wait share the run's deadline and answer
        // `database_timeout`, `database_busy` or (a commit cut by it)
        // `commit_outcome_unknown` when they reach it first: that is the
        // run's own cut. An unknown projection commit is harmless, since
        // the next run reloads the row and its CAS settles either outcome.
        match result {
            Err(failure)
                if matches!(
                    failure.code.as_str(),
                    "database_timeout" | "database_busy" | "commit_outcome_unknown"
                ) && Instant::now() + DEADLINE_SLACK >= bounds.hard =>
            {
                Err(bounds.cut())
            }
            other => other,
        }
    }
    /// Detach a session, name it `merezha {key}` and try the run lock.
    /// `None` when another session holds it.
    fn lock<'a>(
        &'a self,
        bounds: &'a Bounds,
        projection: &Projection,
        kind: Kind,
    ) -> Result<Option<Session<'a>>, Failure> {
        let lane = self.shared.lane(projection)?;
        let name = projection.name.as_str().to_owned();
        let revision = i32::try_from(projection.revision).map_err(|_| invalid())?;
        let key = format!("{}:{name}:{revision}", kind.prefix());
        let client = self.step(bounds, self.db.detach(bounds.hard))?;
        let label = format!("merezha {key}");
        self.step(bounds, async {
            self.db
                .query_one(&client, &sql::NAME_SESSION, &[&label])
                .await
                .map_err(|e| read_error(&e))
        })?;
        if !self.shared.options.hooks().skip_lock {
            let locked: bool = self
                .step(bounds, async {
                    self.db
                        .query_one(&client, &sql::TRY_LOCK, &[&key])
                        .await
                        .map_err(|e| read_error(&e))
                })?
                .get(0);
            if !locked {
                lane.contended[kind as usize].fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
        }
        let held = &lane.held[kind as usize];
        *held.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
        Ok(Some(Session {
            store: self,
            bounds,
            client,
            name,
            revision,
            _held: Some(Held(held)),
        }))
    }
    /// How long this instance has held each run lock it holds now:
    /// `(kind, projection, revision, age)`, the source of TE's
    /// `merezha.projection.lock.held`. Kinds are `live` and `rebuild`.
    pub fn held_locks(&self) -> Vec<(&'static str, String, u32, Duration)> {
        let mut held = Vec::new();
        for ((name, revision), lane) in &self.shared.lanes {
            for (kind, slot) in ["live", "rebuild"].into_iter().zip(&lane.held) {
                if let Some(since) = *slot.lock().unwrap_or_else(|e| e.into_inner()) {
                    held.push((kind, name.clone(), *revision as u32, since.elapsed()));
                }
            }
        }
        held
    }
    /// Lock attempts per revision that found another session holding the
    /// lock, live and rebuild: TE's `merezha.projection.lock.contended`.
    pub fn contended_locks(&self) -> Vec<(String, u32, u64, u64)> {
        self.shared
            .lanes
            .iter()
            .map(|((name, revision), lane)| {
                (
                    name.clone(),
                    *revision as u32,
                    lane.contended[0].load(Ordering::Relaxed),
                    lane.contended[1].load(Ordering::Relaxed),
                )
            })
            .collect()
    }
}

impl Session<'_> {
    fn row(&self) -> Result<Row, Failure> {
        let row = self.store.step(self.bounds, async {
            self.store
                .db
                .query_opt(
                    &self.client,
                    &sql::PROJECTION,
                    &[&self.name, &self.revision],
                )
                .await
                .map_err(|e| read_error(&e))
        })?;
        let row = row.ok_or_else(invalid)?;
        let text: Option<&str> = row.get(0);
        let partitioned: bool = row.get(5);
        let state = match (text, partitioned) {
            (None, true) => serde_json::Value::Null,
            (Some(text), false) => stored_state(text)?,
            _ => return Err(invalid()),
        };
        Ok(Row {
            pair: ProjectionState {
                state,
                checkpoint: unsigned(row.get(1))?,
            },
            row_revision: row.get(2),
            warm_position: unsigned(row.get(3))?,
            generation: row.get(4),
            partitioned,
        })
    }
    fn head(&self) -> Result<u64, Failure> {
        let row = self.store.step(self.bounds, async {
            self.store
                .db
                .query_one(&self.client, &sql::HEAD, &[])
                .await
                .map_err(|e| read_error(&e))
        })?;
        unsigned(row.get(0))
    }
    /// At most `limit` events after `after`, contiguous, with their stored
    /// trace contexts for span links and their streams' tenants.
    fn feed(&self, after: u64, limit: u64) -> Result<(Feed, Vec<Option<String>>), Failure> {
        let from = signed(after)?;
        let count = signed(limit)?;
        let rows = self.store.step(self.bounds, async {
            self.store
                .db
                .query(&self.client, &sql::READ_ALL, &[&from, &count])
                .await
                .map_err(|e| read_error(&e))
        })?;
        let events = self.store.events(&rows)?;
        if events.is_empty()
            || events
                .iter()
                .zip(after + 1..)
                .any(|(event, expected)| event.position != expected)
        {
            return Err(Failure::transient("event_feed_gap"));
        }
        let tenants = rows.iter().map(|row| row.get(10)).collect();
        Ok(((events, PostgresStore::traces(&rows)), tenants))
    }
    /// One guest `project` call within the run's bounds; `partition` names
    /// the tenant of a partitioned projection's state.
    fn project(
        &self,
        app: &Application,
        projection: &Projection,
        state: serde_json::Value,
        events: Vec<StoredEvent>,
        live: bool,
        partition: Option<Identity>,
    ) -> Result<serde_json::Value, RunFailure> {
        let limits = &app.config().limits;
        let deadline = self
            .bounds
            .hard
            .min(Instant::now() + Duration::from_millis(limits.timeout_ms));
        let session = app.guest().session()?;
        let reply = app
            .call_guest(
                session.as_ref(),
                projection.service.as_str(),
                Operation::Project {
                    projection: projection.name.clone(),
                    state,
                    events,
                    live,
                    partition,
                },
                deadline,
            )
            .map_err(RunFailure::from_guest_call)?;
        let Reply::Projected { state } = reply else {
            return Err(Failure::permanent("invalid_response").into());
        };
        check_value(&state, limits.max_state_bytes)?;
        Ok(state)
    }
    /// The live catch-up commit with row-revision CAS (§A.7).
    fn commit(&mut self, row_revision: i64, pair: &ProjectionState) -> Result<i64, Failure> {
        let text = encode(&pair.state)?;
        let checkpoint = signed(pair.checkpoint)?;
        let (name, revision) = (self.name.clone(), self.revision);
        let sleep = self.store.shared.options.hooks().pause;
        let hard = self.bounds.hard;
        let store = self.store;
        let bounds = self.bounds;
        let db = store.db.clone();
        store.step(
            bounds,
            self.client.transaction(hard, move |tx| {
                Box::pin(async move {
                    let row = db
                        .query_opt(
                            tx,
                            &sql::COMMIT_PROJECTION,
                            &[&name, &revision, &text, &checkpoint, &row_revision],
                        )
                        .await
                        .map_err(|e| read_error(&e))?;
                    pause_in(&db, tx, sleep, &name).await?;
                    row.map(|row| row.get(0))
                        .ok_or_else(|| Failure::conflict("projection_conflict"))
                })
            }),
        )
    }
}
impl Session<'_> {
    /// One partitioned batch (§A.7): the events grouped by their stream's
    /// tenant, each group projected once from its partition at
    /// `generation` (the configured initial state when it has none), with
    /// `partition` set. Events without a tenant only advance the
    /// checkpoint. At most one guest call per event.
    fn project_partitions(
        &self,
        app: &Application,
        projection: &Projection,
        generation: i32,
        events: Vec<StoredEvent>,
        tenants: Vec<Option<String>>,
        live: bool,
    ) -> Result<Partitions, RunFailure> {
        let mut groups: BTreeMap<String, Vec<StoredEvent>> = BTreeMap::new();
        for (event, tenant) in events.into_iter().zip(tenants) {
            if let Some(tenant) = tenant {
                groups.entry(tenant).or_default().push(event);
            }
        }
        if groups.is_empty() {
            return Ok(Vec::new());
        }
        let names: Vec<String> = groups.keys().cloned().collect();
        let rows = self.store.step(self.bounds, async {
            self.store
                .db
                .query(
                    &self.client,
                    &sql::LOAD_PARTITIONS,
                    &[&self.name, &self.revision, &generation, &names],
                )
                .await
                .map_err(|e| read_error(&e))
        })?;
        let mut loaded = BTreeMap::new();
        for row in &rows {
            loaded.insert(row.get::<_, String>(0), stored_state(row.get(1))?);
        }
        let mut partitions = Vec::with_capacity(groups.len());
        for (tenant, events) in groups {
            let state = loaded
                .remove(&tenant)
                .unwrap_or_else(|| projection.initial.clone());
            let partition = Identity::new(tenant.as_str()).map_err(|_| invalid())?;
            let state = self.project(app, projection, state, events, live, Some(partition))?;
            partitions.push((tenant, encode(&state)?));
        }
        Ok(partitions)
    }
    /// The partitioned live commit, CAS first (§A.7): the checkpoint with
    /// row-revision CAS at `generation`, then the batch's partitions. A
    /// lost CAS rolls back before any partition is written, and the row
    /// lock it takes orders the batch against a rebuild swap.
    fn commit_partitions(
        &mut self,
        row_revision: i64,
        generation: i32,
        checkpoint: u64,
        partitions: Partitions,
    ) -> Result<i64, Failure> {
        let checkpoint = signed(checkpoint)?;
        let (name, revision) = (self.name.clone(), self.revision);
        let sleep = self.store.shared.options.hooks().pause;
        let hard = self.bounds.hard;
        let (store, bounds) = (self.store, self.bounds);
        let db = store.db.clone();
        let (tenants, states): (Vec<String>, Vec<String>) = partitions.into_iter().unzip();
        store.step(
            bounds,
            self.client.transaction(hard, move |tx| {
                Box::pin(async move {
                    let row = db
                        .query_opt(
                            tx,
                            &sql::COMMIT_PARTITIONED,
                            &[&name, &revision, &checkpoint, &row_revision, &generation],
                        )
                        .await
                        .map_err(|e| read_error(&e))?
                        .ok_or_else(|| Failure::conflict("projection_conflict"))?;
                    write_partitions(&db, tx, &name, revision, generation, &tenants, &states)
                        .await?;
                    pause_in(&db, tx, sleep, &name).await?;
                    Ok(row.get(0))
                })
            }),
        )
    }
    /// Delete partitions in chunks of 1000 until a chunk comes back short,
    /// within the run's bounds: of generations before `generation` (after
    /// a swap), or with `stale` of every generation but `generation`
    /// (before a rebuild's first batch).
    fn sweep(&self, stale: bool, generation: i32) -> Result<(), Failure> {
        let db = &self.store.db;
        loop {
            let deleted = self.store.step(self.bounds, async {
                let params: [&(dyn merezha_postgres::tokio_postgres::types::ToSql + Sync); 3] =
                    [&self.name, &self.revision, &generation];
                if stale {
                    db.execute(&self.client, &sql::SWEEP_STALE_PARTITIONS, &params)
                        .await
                } else {
                    db.execute(&self.client, &sql::SWEEP_OLD_PARTITIONS, &params)
                        .await
                }
                .map_err(|e| read_error(&e))
            })?;
            if deleted < SWEEP_CHUNK {
                return Ok(());
            }
        }
    }
}
/// Upsert one batch's partitions at `generation` inside its transaction.
async fn write_partitions(
    db: &merezha_postgres::Database,
    tx: &merezha_postgres::Tx<'_>,
    name: &String,
    revision: i32,
    generation: i32,
    tenants: &Vec<String>,
    states: &Vec<String>,
) -> Result<(), Failure> {
    if tenants.is_empty() {
        return Ok(());
    }
    db.execute(
        tx,
        &sql::WRITE_PARTITIONS,
        &[name, &revision, &generation, tenants, states],
    )
    .await
    .map_err(|e| read_error(&e))?;
    Ok(())
}
/// The test hook's pause inside a commit transaction: the transaction
/// stays open, idle or in a `pg_sleep`, so only the run's own bounds can
/// end it.
pub(super) async fn pause_in(
    db: &merezha_postgres::Database,
    tx: &merezha_postgres::Tx<'_>,
    pause: Option<Arc<Pause>>,
    projection: &str,
) -> Result<(), Failure> {
    if let Some(pause) = pause
        && *pause.projection.lock().unwrap_or_else(|e| e.into_inner()) == projection
        && pause.armed.swap(false, Ordering::SeqCst)
    {
        pause.entered.store(true, Ordering::SeqCst);
        let duration = Duration::from_millis(pause.millis.load(Ordering::SeqCst));
        if pause.statement.load(Ordering::SeqCst) {
            let sleep = format!("SELECT pg_sleep({})", duration.as_secs_f64());
            db.batch_text(tx, &sql::COMMIT_DELAY, &sleep)
                .await
                .map_err(|e| read_error(&e))?;
        } else {
            tokio::time::sleep(duration).await;
        }
    }
    Ok(())
}

impl Pause {
    /// The client-side pause, if armed for `projection`.
    pub(crate) async fn hold(pause: Option<Arc<Pause>>, projection: &str) {
        if let Some(pause) = pause
            && *pause.projection.lock().unwrap_or_else(|e| e.into_inner()) == projection
            && pause.armed.swap(false, Ordering::SeqCst)
        {
            pause.entered.store(true, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(pause.millis.load(Ordering::SeqCst))).await;
        }
    }
}

/// Feed events and their stored trace contexts.
type Feed = (Vec<StoredEvent>, Vec<Option<StoredContext>>);
/// A feed batch into the run's span: lag, links.
fn record_batch(run: &mut ProjectionRun<'_>, feed: &Feed) {
    run.batch(
        feed.0.first().map(|e| (e.position, e.accepted_unix_ms)),
        &feed.1,
    );
}

/// Which head a live run works towards.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Until {
    /// The background loop: the head, re-snapshotted while wakes arrive.
    Head,
    /// An explicit request: this position; reaching `soft` first fails
    /// `timeout`.
    Position(u64),
}

/// Live catch-up of `projection` under its `p:` lock. Events up to the
/// revision's `warm_position` go to the guest with `live = false`, later
/// ones with `live = true`; each batch publishes state and checkpoint
/// together. A lost CAS (a rebuild swap, or a run whose lock session died)
/// reloads the row and continues.
pub(crate) fn live(
    app: &Application,
    store: &PostgresStore,
    projection: &Projection,
    bounds: &Bounds,
    until: Until,
) -> Result<Outcome, RunFailure> {
    let Some(mut session) = store.lock(bounds, projection, Kind::Live)? else {
        return Ok(Outcome::Contended);
    };
    let lane = store.shared.lane(projection)?;
    let mut row = session.row()?;
    let from = row.pair.checkpoint;
    note_warm(lane, projection, &row);
    // A scheduler run or an explicit request: either way a root span per
    // stretch of batches up to one target (TE §1.2).
    let trigger = if bounds.background {
        Trigger::Background
    } else {
        Trigger::Admin
    };
    loop {
        let target = match until {
            Until::Head => session.head()?,
            Until::Position(position) => position,
        };
        app.instruments().observe_head(target);
        let mut run = app
            .instruments()
            .catch_up(projection.name.as_str(), trigger);
        let stretch = advance(app, &mut session, projection, &mut row, target, &mut run);
        run.finish(stretch.as_ref().map(|_| ()));
        if let Some(early) = stretch? {
            return match until {
                Until::Position(_) => Err(Failure::transient("timeout").into()),
                Until::Head => Ok(early),
            };
        }
        if let Until::Position(_) = until {
            break;
        }
        // Caught up: stay while wakes keep arriving, within the run.
        if Instant::now() >= bounds.soft
            || !store.step(bounds, async {
                Ok(tokio::time::timeout(LINGER, lane.live.notified())
                    .await
                    .is_ok())
            })?
        {
            break;
        }
    }
    let head = session.head()?;
    if row.pair.checkpoint > from {
        merezha_telemetry::event!(
            DEBUG,
            "projection.caught_up",
            projection = projection.name.as_str(),
            from,
            to = row.pair.checkpoint
        );
    }
    Ok(Outcome::Finished {
        behind: head > row.pair.checkpoint,
        pair: row.pair,
    })
}
/// Batches of a live run up to `target`, inside `run`'s span. `Some` when
/// the run's soft deadline stopped it first, with the pair it reached.
fn advance(
    app: &Application,
    session: &mut Session<'_>,
    projection: &Projection,
    row: &mut Row,
    target: u64,
    run: &mut ProjectionRun<'_>,
) -> Result<Option<Outcome>, RunFailure> {
    let store = session.store;
    let lane = store.shared.lane(projection)?;
    let batch = app.config().limits.batch_size as u64;
    while row.pair.checkpoint < target {
        if Instant::now() >= session.bounds.soft {
            return Ok(Some(Outcome::Finished {
                pair: row.pair.clone(),
                behind: true,
            }));
        }
        let checkpoint = row.pair.checkpoint;
        let live = checkpoint >= row.warm_position;
        let mut limit = batch.min(target - checkpoint);
        if !live {
            limit = limit.min(row.warm_position - checkpoint);
        }
        let (feed, tenants) = session.feed(checkpoint, limit)?;
        record_batch(run, &feed);
        let span = run.span().clone();
        let _entered = span.enter();
        let events = feed.0;
        let (count, last) = (
            events.len(),
            events.last().map_or(checkpoint, |e| e.position),
        );
        let committed = if row.partitioned {
            let partitions = session.project_partitions(
                app,
                projection,
                row.generation,
                events,
                tenants,
                live,
            )?;
            session
                .commit_partitions(row.row_revision, row.generation, last, partitions)
                .map(|revision| {
                    (
                        revision,
                        ProjectionState {
                            state: serde_json::Value::Null,
                            checkpoint: last,
                        },
                    )
                })
        } else {
            let state =
                session.project(app, projection, row.pair.state.clone(), events, live, None)?;
            let pair = ProjectionState {
                state,
                checkpoint: last,
            };
            session
                .commit(row.row_revision, &pair)
                .map(|revision| (revision, pair))
        };
        match committed {
            Ok((row_revision, pair)) => {
                row.row_revision = row_revision;
                row.pair = pair;
                store.shared.publish();
                run.published(count, Some(last));
                note_warm(lane, projection, row);
            }
            Err(failure) if failure.code == "projection_conflict" => *row = session.row()?,
            Err(failure) => return Err(failure.into()),
        }
    }
    Ok(None)
}
/// Record warmth, logging `projection.warmed` once per instance.
fn note_warm(lane: &Lane, projection: &Projection, row: &Row) {
    if row.pair.checkpoint >= row.warm_position && !lane.warm.swap(true, Ordering::SeqCst) {
        merezha_telemetry::event!(
            INFO,
            "projection.warmed",
            projection = projection.name.as_str(),
            revision = projection.revision,
            checkpoint = row.pair.checkpoint
        );
    }
}

/// The rebuild candidate row. A partitioned candidate's state is `null`;
/// its states are the partitions of `generation`, the target.
struct Candidate {
    state: serde_json::Value,
    checkpoint: u64,
    target: u64,
    row_revision: i64,
    running: bool,
    generation: i32,
    /// The request's identity: a rebuild abandoned and requested again is
    /// another candidate.
    id: i64,
}
impl Session<'_> {
    fn candidate(&self) -> Result<Option<Candidate>, Failure> {
        let row = self.store.step(self.bounds, async {
            self.store
                .db
                .query_opt(&self.client, &sql::REBUILD, &[&self.name, &self.revision])
                .await
                .map_err(|e| read_error(&e))
        })?;
        let Some(row) = row else { return Ok(None) };
        let text: Option<&str> = row.get(0);
        let state = match text {
            Some(text) => stored_state(text)?,
            None => serde_json::Value::Null,
        };
        Ok(Some(Candidate {
            state,
            checkpoint: unsigned(row.get(1))?,
            target: unsigned(row.get(2))?,
            row_revision: row.get(3),
            running: row.get::<_, &str>(4) == "running",
            generation: row.get(5),
            id: row.get(6),
        }))
    }
    /// Persist the candidate with CAS on the rebuild row, then, for a
    /// partitioned candidate (`state` `None`), the batch's partitions at the
    /// target `generation`; `None` when the rebuild was abandoned or
    /// changed elsewhere, and then nothing is written.
    fn commit_candidate(
        &mut self,
        id: i64,
        row_revision: i64,
        state: Option<&serde_json::Value>,
        checkpoint: u64,
        generation: i32,
        partitions: Partitions,
    ) -> Result<Option<i64>, Failure> {
        let text = state.map(encode).transpose()?;
        let checkpoint = signed(checkpoint)?;
        let (tenants, states): (Vec<String>, Vec<String>) = partitions.into_iter().unzip();
        let (name, revision) = (self.name.clone(), self.revision);
        let sleep = self.store.shared.options.hooks().pause;
        let hard = self.bounds.hard;
        let (store, bounds) = (self.store, self.bounds);
        let db = store.db.clone();
        store.step(
            bounds,
            self.client.transaction(hard, move |tx| {
                Box::pin(async move {
                    let row = db
                        .query_opt(
                            tx,
                            &sql::COMMIT_CANDIDATE,
                            &[&name, &revision, &text, &checkpoint, &row_revision, &id],
                        )
                        .await
                        .map_err(|e| read_error(&e))?;
                    // CAS first: a lost candidate writes no partition.
                    let Some(row) = row else { return Ok(None) };
                    write_partitions(&db, tx, &name, revision, generation, &tenants, &states)
                        .await?;
                    pause_in(&db, tx, sleep, &name).await?;
                    Ok(Some(row.get(0)))
                })
            }),
        )
    }
    fn fail_candidate(&self, id: i64, row_revision: i64, failure: &Failure) -> Result<(), Failure> {
        self.store.step(self.bounds, async {
            self.store
                .db
                .execute(
                    &self.client,
                    &sql::FAIL_REBUILD,
                    &[
                        &self.name,
                        &self.revision,
                        &row_revision,
                        &failure.code,
                        &id,
                    ],
                )
                .await
                .map_err(|e| read_error(&e))
        })?;
        Ok(())
    }
    /// The swap transaction (§A.7 step 3): lock the served row, take the
    /// finished candidate this run built and publish it as the next
    /// generation, with its generation. `None` when the rebuild was
    /// abandoned meanwhile.
    fn swap(
        &mut self,
        id: i64,
        row_revision: i64,
    ) -> Result<Option<(ProjectionState, i32)>, Failure> {
        let (name, revision) = (self.name.clone(), self.revision);
        let sleep = self.store.shared.options.hooks().pause;
        let hard = self.bounds.hard;
        let (store, bounds) = (self.store, self.bounds);
        let db = store.db.clone();
        let swapped: Option<(Option<String>, i64, i32)> = store.step(
            bounds,
            self.client.transaction(hard, move |tx| {
                Box::pin(async move {
                    db.query_opt(tx, &sql::LOCK_PROJECTION, &[&name, &revision])
                        .await
                        .map_err(|e| read_error(&e))?
                        .ok_or_else(invalid)?;
                    let Some(taken) = db
                        .query_opt(
                            tx,
                            &sql::TAKE_CANDIDATE,
                            &[&name, &revision, &row_revision, &id],
                        )
                        .await
                        .map_err(|e| read_error(&e))?
                    else {
                        return Ok(None);
                    };
                    // NULL for a partitioned candidate, whose row keeps none.
                    let state: Option<String> = taken.get(0);
                    let checkpoint: i64 = taken.get(1);
                    let generation: i32 = taken.get(2);
                    db.query_one(
                        tx,
                        &sql::SWAP,
                        &[&name, &revision, &state, &checkpoint, &generation],
                    )
                    .await
                    .map_err(|e| read_error(&e))?;
                    pause_in(&db, tx, sleep, &name).await?;
                    Ok(Some((state, checkpoint, generation)))
                })
            }),
        )?;
        let Some((state, checkpoint, generation)) = swapped else {
            return Ok(None);
        };
        let state = match state {
            Some(state) => decode(&state, MAX_STATE_BYTES)?,
            None => serde_json::Value::Null,
        };
        Ok(Some((
            ProjectionState {
                state,
                checkpoint: unsigned(checkpoint)?,
            },
            generation,
        )))
    }
}

/// Rebuild of `projection` under its `b:` lock (§A.7): batches with
/// `live = false` from the persisted candidate to the rebuild target,
/// the candidate persisted after each, then the swap. A failed batch marks
/// the rebuild `failed` and leaves the served pair untouched.
pub(crate) fn rebuild(
    app: &Application,
    store: &PostgresStore,
    projection: &Projection,
    bounds: &Bounds,
) -> Result<Outcome, RunFailure> {
    let Some(mut session) = store.lock(bounds, projection, Kind::Rebuild)? else {
        return Ok(Outcome::Contended);
    };
    let mut run = app
        .instruments()
        .catch_up(projection.name.as_str(), Trigger::Rebuild);
    let result = rebuild_batches(app, &mut session, projection, bounds, &mut run);
    run.finish(result.as_ref().map(|_| ()));
    result
}
/// The batches and the swap of one rebuild run, inside `run`'s span.
fn rebuild_batches(
    app: &Application,
    session: &mut Session<'_>,
    projection: &Projection,
    bounds: &Bounds,
    run: &mut ProjectionRun<'_>,
) -> Result<Outcome, RunFailure> {
    let store = session.store;
    let batch = app.config().limits.batch_size as u64;
    let partitioned = projection.partitioned();
    let mut started = None;
    loop {
        let pause = store.shared.options.hooks().rebuild_batch;
        store.step(bounds, async {
            Pause::hold(pause, projection.name.as_str()).await;
            Ok(())
        })?;
        let Some(candidate) = session.candidate()? else {
            return Ok(Outcome::Idle);
        };
        if !candidate.running {
            return Ok(Outcome::Idle);
        }
        // A rebuild abandoned and requested again while this run held the
        // lock is another candidate: stop, and the next run builds it from
        // its start.
        if *started.get_or_insert(candidate.id) != candidate.id {
            return Ok(Outcome::Idle);
        }
        // Before a partitioned candidate's first batch, nothing at its
        // target generation is legitimate yet: clear what an abandoned
        // rebuild or an interrupted sweep left, so absent partitions mean
        // the initial state. Checked on every batch, not only a run's
        // first.
        if partitioned && candidate.checkpoint == 0 {
            let served = session.row()?.generation;
            session.sweep(true, served)?;
        }
        if candidate.checkpoint >= candidate.target {
            #[cfg(test)]
            super::tests::crash_cut("rebuild_before_swap");
            let Some((pair, generation)) = session.swap(candidate.id, candidate.row_revision)?
            else {
                return Ok(Outcome::Idle);
            };
            #[cfg(test)]
            super::tests::crash_cut("rebuild_after_swap");
            run.published(0, Some(pair.checkpoint));
            store.shared.publish();
            store.shared.lane(projection)?.live.notify_one();
            // Older generations are no longer read. A failed sweep leaves
            // rows the next rebuild's first sweep removes.
            if partitioned {
                let _ = session.sweep(false, generation);
            }
            return Ok(Outcome::Swapped(pair));
        }
        if Instant::now() >= bounds.soft {
            return Ok(Outcome::Finished {
                pair: ProjectionState {
                    state: candidate.state,
                    checkpoint: candidate.checkpoint,
                },
                behind: true,
            });
        }
        let limit = batch.min(candidate.target - candidate.checkpoint);
        let (feed, tenants) = session.feed(candidate.checkpoint, limit)?;
        record_batch(run, &feed);
        let span = run.span().clone();
        let _entered = span.enter();
        let events = feed.0;
        let (count, last) = (
            events.len(),
            events.last().map_or(candidate.checkpoint, |e| e.position),
        );
        let projected = if partitioned {
            session
                .project_partitions(
                    app,
                    projection,
                    candidate.generation,
                    events,
                    tenants,
                    false,
                )
                .map(|partitions| (None, partitions))
        } else {
            session
                .project(app, projection, candidate.state, events, false, None)
                .map(|state| (Some(state), Vec::new()))
        };
        let (state, partitions) = match projected {
            Ok(projected) => projected,
            Err(failure) => {
                session.fail_candidate(candidate.id, candidate.row_revision, &failure.failure)?;
                return Err(failure);
            }
        };
        if session
            .commit_candidate(
                candidate.id,
                candidate.row_revision,
                state.as_ref(),
                last,
                candidate.generation,
                partitions,
            )?
            .is_none()
        {
            return Ok(Outcome::Idle);
        }
        run.published(count, None);
    }
}

impl PostgresStore {
    /// Wait for a local projection commit or 50 ms, whichever is first;
    /// `expired()` once `deadline` passed.
    pub(crate) fn wait_published(
        &self,
        published: &mut watch::Receiver<u64>,
        deadline: Instant,
        expired: fn() -> Failure,
    ) -> Result<(), Failure> {
        let now = Instant::now();
        if now >= deadline {
            return Err(expired());
        }
        let wait = POLL.min(deadline - now);
        self.handle.block_on(async {
            let _ = tokio::time::timeout(wait, published.changed()).await;
        });
        // No read is started that could only end at the deadline.
        if Instant::now() >= deadline {
            return Err(expired());
        }
        Ok(())
    }
    /// `POST .../catch-up` on PostgreSQL (§A.7): snapshot the head, run
    /// to it under the `p:` lock, or wait within `deadline` until another
    /// runner reached it. At most `background.max_concurrent` requests
    /// hold their own lock session; the rest wait the same way.
    pub(crate) fn catch_up(
        &self,
        app: &Application,
        projection: &Projection,
        deadline: Instant,
    ) -> Result<ProjectionState, Failure> {
        let target = self.head_at(deadline)?;
        let bounds = Bounds::request(deadline);
        let mut published = self.shared.published.subscribe();
        let mut next_try = Instant::now();
        loop {
            let (pair, _, _) =
                at_deadline(self.served(projection, None, deadline), deadline, timed_out)?;
            if pair.checkpoint >= target {
                return Ok(pair);
            }
            if Instant::now() >= next_try {
                next_try = Instant::now() + RETRY_LOCK;
                if let Ok(_slot) = self.shared.explicit.clone().try_acquire_owned() {
                    let ran = live(app, self, projection, &bounds, Until::Position(target))
                        .map_err(Failure::from)?;
                    if let Outcome::Finished { pair, .. } = ran {
                        return Ok(pair);
                    }
                }
            }
            self.wait_published(&mut published, deadline, timed_out)?;
        }
    }
    /// Synchronous rebuild on PostgreSQL, for embedding callers and
    /// tests: request (or resume) the rebuild, run it under the `b:` lock
    /// within `deadline`, or wait for the runner that holds it. Progress
    /// persists; a later call resumes it.
    pub(crate) fn rebuild_now(
        &self,
        app: &Application,
        projection: &Projection,
        deadline: Instant,
    ) -> Result<ProjectionState, Failure> {
        self.request_rebuild(projection, true, None, deadline)?;
        let bounds = Bounds::request(deadline);
        let mut published = self.shared.published.subscribe();
        let mut next_try = Instant::now();
        loop {
            match at_deadline(
                self.rebuild_status(projection, deadline),
                deadline,
                timed_out,
            )? {
                None => return Ok(self.served(projection, None, deadline)?.0),
                Some(false) => return Err(Failure::conflict("rebuild_failed")),
                Some(true) => {}
            }
            if Instant::now() >= next_try {
                next_try = Instant::now() + RETRY_LOCK;
                if let Ok(_slot) = self.shared.explicit.clone().try_acquire_owned() {
                    match rebuild(app, self, projection, &bounds).map_err(Failure::from)? {
                        Outcome::Swapped(pair) => return Ok(pair),
                        Outcome::Finished { .. } => return Err(Failure::transient("timeout")),
                        Outcome::Idle | Outcome::Contended => {}
                    }
                }
            }
            self.wait_published(&mut published, deadline, timed_out)?;
        }
    }
}
fn timed_out() -> Failure {
    Failure::transient("timeout")
}
/// A read that failed because the request's deadline came (the pool wait
/// or the statement bound reaching it) is the request's `expired()`.
pub(crate) fn at_deadline<T>(
    result: Result<T, Failure>,
    deadline: Instant,
    expired: fn() -> Failure,
) -> Result<T, Failure> {
    match result {
        Err(failure)
            if matches!(
                failure.code.as_str(),
                "timeout" | "database_timeout" | "database_busy"
            ) && Instant::now() + DEADLINE_SLACK >= deadline =>
        {
            Err(expired())
        }
        other => other,
    }
}
