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
use super::{PostgresStore, decode, encode, invalid, read_error, signed, sql, unsigned};
use crate::{
    Application,
    application::{ProjectionState, check_value},
    config::{MAX_STATE_BYTES, Projection},
};
use merezha_domain::{Failure, Operation, Reply, StoredEvent, compatible_json};
use merezha_postgres::DetachedClient;
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
const LINGER: Duration = Duration::from_secs(1);
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
    #[cfg(feature = "test-support")]
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
            #[cfg(feature = "test-support")]
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
            || !in_range(self.run_timeout, RUN_TIMEOUT)
            || !(MAX_CONCURRENT.0..=MAX_CONCURRENT.1).contains(&self.max_concurrent)
            || self.instance_id.len() > 256
        {
            return Err(Failure::permanent("storage_configuration"));
        }
        Ok(())
    }
    pub(crate) fn hooks(&self) -> RunHooks {
        #[cfg(feature = "test-support")]
        return self.hooks.clone();
        #[cfg(not(feature = "test-support"))]
        RunHooks::default()
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
}
/// See [`RunHooks::pause`].
#[derive(Debug, Default)]
#[doc(hidden)]
pub struct Pause {
    armed: AtomicBool,
    entered: AtomicBool,
    millis: AtomicU64,
    projection: Mutex<String>,
}
impl Pause {
    /// Pause the next commit of `projection` for `duration`.
    pub fn arm(&self, projection: &str, duration: Duration) {
        *self.projection.lock().unwrap_or_else(|e| e.into_inner()) = projection.to_owned();
        self.millis
            .store(duration.as_millis() as u64, Ordering::SeqCst);
        self.entered.store(false, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
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
}
impl Shared {
    pub(crate) fn new(options: BackgroundOptions, projections: &[Projection]) -> Self {
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
    /// After an `appended` outcome: wake every live lane.
    pub(crate) fn appended(&self) {
        for lane in self.lanes.values() {
            lane.live.notify_one();
        }
    }
    pub(crate) fn publish(&self) {
        self.published.send_modify(|n| *n = n.wrapping_add(1));
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
    /// A background run started now.
    pub(crate) fn background(shared: &Shared) -> Self {
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
            cancel: Some(shared.cancel.subscribe()),
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

/// The served row as a run reads it.
struct Row {
    pair: ProjectionState,
    row_revision: i64,
    warm_position: u64,
}

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
    fn step<T>(
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
        // `database_timeout` or `database_busy` when they reach it first:
        // that is the run's own cut.
        match result {
            Err(failure)
                if matches!(failure.code.as_str(), "database_timeout" | "database_busy")
                    && Instant::now() + DEADLINE_SLACK >= bounds.hard =>
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
            client
                .query_one(sql::NAME_SESSION, &[&label])
                .await
                .map_err(|e| read_error(&e))
        })?;
        if !self.shared.options.hooks().skip_lock {
            let locked: bool = self
                .step(bounds, async {
                    client
                        .query_one(sql::TRY_LOCK, &[&key])
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
            self.client
                .query_opt(sql::PROJECTION, &[&self.name, &self.revision])
                .await
                .map_err(|e| read_error(&e))
        })?;
        let row = row.ok_or_else(invalid)?;
        let text: Option<&str> = row.get(0);
        let state: serde_json::Value = decode(text.ok_or_else(invalid)?, MAX_STATE_BYTES)?;
        compatible_json(&state).map_err(|_| invalid())?;
        Ok(Row {
            pair: ProjectionState {
                state,
                checkpoint: unsigned(row.get(1))?,
            },
            row_revision: row.get(2),
            warm_position: unsigned(row.get(3))?,
        })
    }
    fn head(&self) -> Result<u64, Failure> {
        let row = self.store.step(self.bounds, async {
            self.client
                .query_one(sql::HEAD, &[])
                .await
                .map_err(|e| read_error(&e))
        })?;
        unsigned(row.get(0))
    }
    /// At most `limit` events after `after`, contiguous.
    fn feed(&self, after: u64, limit: u64) -> Result<Vec<StoredEvent>, Failure> {
        let from = signed(after)?;
        let count = signed(limit)?;
        let rows = self.store.step(self.bounds, async {
            self.client
                .query(&sql::read_all(), &[&from, &count])
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
        Ok(events)
    }
    /// One guest `project` call within the run's bounds.
    fn project(
        &self,
        app: &Application,
        projection: &Projection,
        state: serde_json::Value,
        events: Vec<StoredEvent>,
        live: bool,
    ) -> Result<serde_json::Value, Failure> {
        let limits = &app.config().limits;
        let deadline = self
            .bounds
            .hard
            .min(Instant::now() + Duration::from_millis(limits.timeout_ms));
        let session = app.guest().session()?;
        let reply = session.invoke_until(
            projection.service.as_str(),
            Operation::Project {
                projection: projection.name.clone(),
                state,
                events,
                live,
            },
            deadline,
        )?;
        let Reply::Projected { state } = reply else {
            return Err(Failure::permanent("invalid_response"));
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
        store.step(
            bounds,
            self.client.transaction(hard, move |tx| {
                Box::pin(async move {
                    let row = tx
                        .query_opt(
                            sql::COMMIT_PROJECTION,
                            &[&name, &revision, &text, &checkpoint, &row_revision],
                        )
                        .await
                        .map_err(|e| read_error(&e))?;
                    pause_in(sleep, &name).await;
                    row.map(|row| row.get(0))
                        .ok_or_else(|| Failure::conflict("projection_conflict"))
                })
            }),
        )
    }
}
/// The test hook's pause inside a commit transaction: the transaction
/// stays open, idle, so only the run's own bounds can end it.
async fn pause_in(pause: Option<Arc<Pause>>, projection: &str) {
    if let Some(pause) = pause
        && *pause.projection.lock().unwrap_or_else(|e| e.into_inner()) == projection
        && pause.armed.swap(false, Ordering::SeqCst)
    {
        pause.entered.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(pause.millis.load(Ordering::SeqCst))).await;
    }
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
) -> Result<Outcome, Failure> {
    let Some(mut session) = store.lock(bounds, projection, Kind::Live)? else {
        return Ok(Outcome::Contended);
    };
    let lane = store.shared.lane(projection)?;
    let batch = app.config().limits.batch_size as u64;
    let mut row = session.row()?;
    let from = row.pair.checkpoint;
    note_warm(lane, projection, &row);
    loop {
        let target = match until {
            Until::Head => session.head()?,
            Until::Position(position) => position,
        };
        while row.pair.checkpoint < target {
            if Instant::now() >= bounds.soft {
                return match until {
                    Until::Position(_) => Err(Failure::transient("timeout")),
                    Until::Head => Ok(Outcome::Finished {
                        pair: row.pair,
                        behind: true,
                    }),
                };
            }
            let checkpoint = row.pair.checkpoint;
            let live = checkpoint >= row.warm_position;
            let mut limit = batch.min(target - checkpoint);
            if !live {
                limit = limit.min(row.warm_position - checkpoint);
            }
            let events = session.feed(checkpoint, limit)?;
            let last = events.last().map_or(checkpoint, |e| e.position);
            let state = session.project(app, projection, row.pair.state.clone(), events, live)?;
            let pair = ProjectionState {
                state,
                checkpoint: last,
            };
            match session.commit(row.row_revision, &pair) {
                Ok(row_revision) => {
                    row.row_revision = row_revision;
                    row.pair = pair;
                    store.shared.publish();
                    note_warm(lane, projection, &row);
                }
                Err(failure) if failure.code == "projection_conflict" => row = session.row()?,
                Err(failure) => return Err(failure),
            }
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
        tracing::debug!(
            event = "projection.caught_up",
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
/// Record warmth, logging `projection.warmed` once per instance.
fn note_warm(lane: &Lane, projection: &Projection, row: &Row) {
    if row.pair.checkpoint >= row.warm_position && !lane.warm.swap(true, Ordering::SeqCst) {
        tracing::info!(
            event = "projection.warmed",
            projection = projection.name.as_str(),
            revision = projection.revision,
            checkpoint = row.pair.checkpoint
        );
    }
}

/// The rebuild candidate row.
struct Candidate {
    state: serde_json::Value,
    checkpoint: u64,
    target: u64,
    row_revision: i64,
    running: bool,
}
impl Session<'_> {
    fn candidate(&self) -> Result<Option<Candidate>, Failure> {
        let row = self.store.step(self.bounds, async {
            self.client
                .query_opt(sql::REBUILD, &[&self.name, &self.revision])
                .await
                .map_err(|e| read_error(&e))
        })?;
        let Some(row) = row else { return Ok(None) };
        let text: Option<&str> = row.get(0);
        let state: serde_json::Value = decode(text.ok_or_else(invalid)?, MAX_STATE_BYTES)?;
        compatible_json(&state).map_err(|_| invalid())?;
        Ok(Some(Candidate {
            state,
            checkpoint: unsigned(row.get(1))?,
            target: unsigned(row.get(2))?,
            row_revision: row.get(3),
            running: row.get::<_, &str>(4) == "running",
        }))
    }
    /// Persist the candidate with CAS on the rebuild row; `None` when the
    /// rebuild was abandoned or changed elsewhere.
    fn commit_candidate(
        &mut self,
        row_revision: i64,
        state: &serde_json::Value,
        checkpoint: u64,
    ) -> Result<Option<i64>, Failure> {
        let text = encode(state)?;
        let checkpoint = signed(checkpoint)?;
        let (name, revision) = (self.name.clone(), self.revision);
        let sleep = self.store.shared.options.hooks().pause;
        let hard = self.bounds.hard;
        let (store, bounds) = (self.store, self.bounds);
        store.step(
            bounds,
            self.client.transaction(hard, move |tx| {
                Box::pin(async move {
                    let row = tx
                        .query_opt(
                            sql::COMMIT_CANDIDATE,
                            &[&name, &revision, &text, &checkpoint, &row_revision],
                        )
                        .await
                        .map_err(|e| read_error(&e))?;
                    pause_in(sleep, &name).await;
                    Ok(row.map(|row| row.get(0)))
                })
            }),
        )
    }
    fn fail_candidate(&self, row_revision: i64, failure: &Failure) -> Result<(), Failure> {
        self.store.step(self.bounds, async {
            self.client
                .execute(
                    sql::FAIL_REBUILD,
                    &[&self.name, &self.revision, &row_revision, &failure.code],
                )
                .await
                .map_err(|e| read_error(&e))
        })?;
        Ok(())
    }
    /// The swap transaction (§A.7 step 3): lock the served row, take the
    /// finished candidate this run built and publish it as the next
    /// generation. `None` when the rebuild was abandoned meanwhile.
    fn swap(&mut self, row_revision: i64) -> Result<Option<ProjectionState>, Failure> {
        let (name, revision) = (self.name.clone(), self.revision);
        let sleep = self.store.shared.options.hooks().pause;
        let hard = self.bounds.hard;
        let (store, bounds) = (self.store, self.bounds);
        let swapped: Option<(String, i64)> = store.step(
            bounds,
            self.client.transaction(hard, move |tx| {
                Box::pin(async move {
                    tx.query_opt(sql::LOCK_PROJECTION, &[&name, &revision])
                        .await
                        .map_err(|e| read_error(&e))?
                        .ok_or_else(invalid)?;
                    let Some(taken) = tx
                        .query_opt(sql::TAKE_CANDIDATE, &[&name, &revision, &row_revision])
                        .await
                        .map_err(|e| read_error(&e))?
                    else {
                        return Ok(None);
                    };
                    let state: Option<String> = taken.get(0);
                    let state = state.ok_or_else(invalid)?;
                    let checkpoint: i64 = taken.get(1);
                    let generation: i32 = taken.get(2);
                    tx.query_one(
                        sql::SWAP,
                        &[&name, &revision, &state, &checkpoint, &generation],
                    )
                    .await
                    .map_err(|e| read_error(&e))?;
                    pause_in(sleep, &name).await;
                    Ok(Some((state, checkpoint)))
                })
            }),
        )?;
        let Some((state, checkpoint)) = swapped else {
            return Ok(None);
        };
        Ok(Some(ProjectionState {
            state: decode(&state, MAX_STATE_BYTES)?,
            checkpoint: unsigned(checkpoint)?,
        }))
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
) -> Result<Outcome, Failure> {
    let Some(mut session) = store.lock(bounds, projection, Kind::Rebuild)? else {
        return Ok(Outcome::Contended);
    };
    let batch = app.config().limits.batch_size as u64;
    loop {
        let Some(candidate) = session.candidate()? else {
            return Ok(Outcome::Idle);
        };
        if !candidate.running {
            return Ok(Outcome::Idle);
        }
        if candidate.checkpoint >= candidate.target {
            #[cfg(test)]
            super::tests::crash_cut("rebuild_before_swap");
            let Some(pair) = session.swap(candidate.row_revision)? else {
                return Ok(Outcome::Idle);
            };
            #[cfg(test)]
            super::tests::crash_cut("rebuild_after_swap");
            store.shared.publish();
            store.shared.lane(projection)?.live.notify_one();
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
        let events = session.feed(candidate.checkpoint, limit)?;
        let last = events.last().map_or(candidate.checkpoint, |e| e.position);
        let state = match session.project(app, projection, candidate.state, events, false) {
            Ok(state) => state,
            Err(failure) => {
                session.fail_candidate(candidate.row_revision, &failure)?;
                return Err(failure);
            }
        };
        if session
            .commit_candidate(candidate.row_revision, &state, last)?
            .is_none()
        {
            return Ok(Outcome::Idle);
        }
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
            let (pair, _, _) = at_deadline(self.served(projection, deadline), deadline, timed_out)?;
            if pair.checkpoint >= target {
                return Ok(pair);
            }
            if Instant::now() >= next_try {
                next_try = Instant::now() + RETRY_LOCK;
                if let Ok(_slot) = self.shared.explicit.clone().try_acquire_owned() {
                    let ran = live(app, self, projection, &bounds, Until::Position(target))?;
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
                None => return Ok(self.served(projection, deadline)?.0),
                Some(false) => return Err(Failure::conflict("rebuild_failed")),
                Some(true) => {}
            }
            if Instant::now() >= next_try {
                next_try = Instant::now() + RETRY_LOCK;
                if let Ok(_slot) = self.shared.explicit.clone().try_acquire_owned() {
                    match rebuild(app, self, projection, &bounds)? {
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
