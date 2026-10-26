//! The per-instance projection scheduler (AP §A.7, "Triggers").
//!
//! One async task per configured revision runs live catch-up, and one per
//! revision runs its rebuild; both start at most one run at a time, bounded
//! together by `background.max_concurrent`. Live lanes wake on a local
//! commit, on a `merezha_app_events` notification (one `LISTEN` session,
//! reconnected with backoff from 100 ms to 10 s, waking every lane on
//! reconnect because notifications may have been lost) and every
//! `interval`. Each run executes on a blocking thread.
use super::{
    PostgresStore,
    reactor::{self, Due, ReactorOutcome},
    runner::{self, Bounds, CatchUp, Lane, Outcome, Until},
};
use crate::{
    Application,
    config::{Projection, Reactor},
    instrument::RunFailure,
};
use merezha_domain::Failure;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{sync::watch, task::JoinHandle};
use tracing::instrument::WithSubscriber;

/// The channel the append function notifies with the new head (§A.4).
pub const EVENTS_CHANNEL: &str = "merezha_app_events";
const RECONNECT: (Duration, Duration) = (Duration::from_millis(100), Duration::from_secs(10));
/// A failing lane retries after 1 s, doubling up to 5 min.
const RETRY: (Duration, Duration) = (Duration::from_secs(1), Duration::from_secs(300));
/// How often an idle `LISTEN` session is probed, since a half-open one
/// never ends by itself.
const PROBE: Duration = Duration::from_secs(5);
/// After a lock attempt found another holder, the lane waits this long
/// before the next one: the holder's linger.
const CONTENDED: Duration = runner::LINGER;
/// Bound on the scheduler's own pooled reads.
const READ: Duration = Duration::from_secs(5);

/// The background runners of one instance. Start it once the application
/// is open; stop it with [`Scheduler::shutdown`].
pub struct Scheduler {
    store: Arc<PostgresStore>,
    stop: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}
impl std::fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler").finish_non_exhaustive()
    }
}

/// What every lane task shares.
struct Context {
    app: Application,
    store: Arc<PostgresStore>,
    gate: Arc<tokio::sync::Semaphore>,
    stop: watch::Receiver<bool>,
}

impl Scheduler {
    /// Start the runners of a PostgreSQL `app` on the current runtime:
    /// live catch-up and rebuild lanes per projection revision and one
    /// reactor lane per configured reactor (AP §C.5).
    /// Fails `storage_configuration` for another backend or outside a
    /// runtime, and `invalid_concurrency_budget` when the guest runtime
    /// admits fewer calls than HTTP admission (`limits.max_concurrent`)
    /// plus `background.max_concurrent` (§A.8): background runs would
    /// otherwise make requests fail `overloaded`. The guest keeps refusing
    /// reloads below that budget (`Guest::reserve_concurrency`).
    pub fn start(app: &Application) -> Result<Self, Failure> {
        let store = app
            .postgres()
            .ok_or_else(|| Failure::permanent("storage_configuration"))?;
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| Failure::permanent("storage_configuration"))?;
        let options = &store.shared().options;
        app.guest()
            .reserve_concurrency(app.config().limits.max_concurrent + options.max_concurrent)?;
        let (stop, stopped) = watch::channel(false);
        let context = Arc::new(Context {
            app: app.clone(),
            store: store.clone(),
            gate: Arc::new(tokio::sync::Semaphore::new(options.max_concurrent)),
            stop: stopped,
        });
        // The lanes keep the caller's dispatcher (scoped in tests), so their
        // runs and logs reach the same subscriber; their spans are roots.
        let mut tasks = Vec::new();
        for projection in &app.config().projections {
            tasks.push(
                handle.spawn(
                    live_lane(context.clone(), projection.clone()).with_current_subscriber(),
                ),
            );
            tasks.push(handle.spawn(
                rebuild_lane(context.clone(), projection.clone()).with_current_subscriber(),
            ));
        }
        for reactor in app.config().reactors.iter().flatten() {
            tasks.push(
                handle.spawn(
                    reactor_lane(context.clone(), reactor.clone()).with_current_subscriber(),
                ),
            );
        }
        tasks.push(handle.spawn(interval(context.clone()).with_current_subscriber()));
        if !store.shared().reactors.is_empty() {
            tasks.push(handle.spawn(reactor_interval(context.clone()).with_current_subscriber()));
        }
        if options.catch_up == CatchUp::Auto && !options.hooks().skip_listen {
            store.shared().listener.store(2, Ordering::SeqCst);
            tasks.push(handle.spawn(listen(context).with_current_subscriber()));
        }
        Ok(Self {
            store,
            stop,
            tasks: Mutex::new(tasks),
        })
    }
    /// Ready for traffic: every configured revision is warm on this
    /// instance, or `warm_readiness` is off. Ongoing lag never counts
    /// (R10). Reads only in-process state.
    pub fn ready(&self) -> bool {
        let shared = self.store.shared();
        !shared.options.warm_readiness
            || shared
                .lanes
                .values()
                .all(|lane| lane.warm.load(Ordering::SeqCst))
    }
    /// Stop the lanes, cancel in-flight run steps (their sessions close and
    /// release their locks) and wait for them until `deadline`.
    pub async fn shutdown(&self, deadline: Instant) {
        let _ = self.stop.send(true);
        self.store
            .shared()
            .cancel
            .send_modify(|n| *n = n.wrapping_add(1));
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap_or_else(|e| e.into_inner()));
        let at = tokio::time::Instant::from_std(deadline);
        for task in tasks {
            let _ = tokio::time::timeout_at(at, task).await;
        }
        self.store.shared().listener.store(0, Ordering::SeqCst);
    }
    /// Cancel the steps of every in-flight run without stopping the lanes,
    /// as a lost process would; the lanes run again at once.
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn cancel_runs(&self) {
        self.store
            .shared()
            .cancel
            .send_modify(|n| *n = n.wrapping_add(1));
    }
}
impl Drop for Scheduler {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        for task in self.tasks.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            task.abort();
        }
    }
}

fn stopped(context: &Context) -> bool {
    *context.stop.borrow()
}
/// Wait for `notify` (or `armed`), or return `false` on stop.
async fn wake(context: &Context, notify: &tokio::sync::Notify, armed: bool) -> bool {
    if armed {
        return !stopped(context);
    }
    let mut stop = context.stop.clone();
    tokio::select! {
        _ = stop.wait_for(|stopped| *stopped) => false,
        () = notify.notified() => !stopped(context),
    }
}
/// Sleep until `at`, or return `false` on stop.
async fn sleep_until(context: &Context, at: Instant) -> bool {
    let mut stop = context.stop.clone();
    tokio::select! {
        _ = stop.wait_for(|stopped| *stopped) => false,
        () = tokio::time::sleep_until(at.into()) => !stopped(context),
    }
}
fn backoff(failures: u32) -> Duration {
    let (base, max) = RETRY;
    base.saturating_mul(1u32 << failures.saturating_sub(1).min(16))
        .min(max)
}
/// One run on a blocking thread, holding a background slot.
async fn run(
    context: &Arc<Context>,
    projection: &Projection,
    work: fn(&Application, &PostgresStore, &Projection, &Bounds) -> Result<Outcome, RunFailure>,
) -> Option<Result<Outcome, RunFailure>> {
    // Subscribe first, then check for shutdown: `shutdown` sets `stop`
    // before it cancels, so a run either sees the stop here or the cancel
    // at its first step.
    let shared = context.store.shared();
    let cancel = shared.cancel.subscribe();
    if stopped(context) {
        return None;
    }
    let mut stop = context.stop.clone();
    let slot = tokio::select! {
        _ = stop.wait_for(|stopped| *stopped) => return None,
        slot = context.gate.clone().acquire_owned() => slot.ok()?,
    };
    // Test hook: the window between taking the slot and the first step.
    runner::Pause::hold(shared.options.hooks().before_run, projection.name.as_str()).await;
    shared.runs.fetch_add(1, Ordering::SeqCst);
    let (app, store, projection) = (
        context.app.clone(),
        context.store.clone(),
        projection.clone(),
    );
    let result = merezha_telemetry::spawn_blocking_in_span(move || {
        let bounds = Bounds::background(store.shared(), cancel);
        work(&app, &store, &projection, &bounds)
    })
    .await;
    shared.runs.fetch_sub(1, Ordering::SeqCst);
    drop(slot);
    Some(result.unwrap_or_else(|_| Err(Failure::transient("internal").into())))
}
fn live_work(
    app: &Application,
    store: &PostgresStore,
    projection: &Projection,
    bounds: &Bounds,
) -> Result<Outcome, RunFailure> {
    runner::live(app, store, projection, bounds, Until::Head)
}

/// Live catch-up of one revision. In manual mode it only warms the
/// revision up.
async fn live_lane(context: Arc<Context>, projection: Projection) {
    let store = context.store.clone();
    let Ok(lane) = store.shared().lane(&projection) else {
        return;
    };
    let manual = store.shared().options.catch_up == CatchUp::Manual;
    let (name, revision) = (
        projection.name.as_str().to_owned(),
        projection.revision as i32,
    );
    let mut armed = true;
    let mut failures = 0u32;
    let mut reported = false;
    loop {
        if !wake(&context, &lane.live, armed).await {
            return;
        }
        armed = false;
        // A cheap pooled read first: no lock session when nothing is due.
        let deadline = Instant::now() + READ;
        let due = async {
            let (pair, _, warm) = store.served_row(name.clone(), revision, deadline).await?;
            if pair.checkpoint >= warm {
                lane.warm.store(true, Ordering::SeqCst);
                if manual {
                    return Ok(false);
                }
            }
            Ok::<_, Failure>(store.head_async(deadline).await? > pair.checkpoint)
        };
        match due.await {
            Ok(false) => continue,
            Ok(true) => {}
            Err(failure) => {
                // The interval retries; report once per streak.
                if !reported {
                    report_failure(&projection, &failure.code);
                }
                reported = true;
                continue;
            }
        }
        let Some(result) = run(&context, &projection, live_work).await else {
            return;
        };
        match result {
            Ok(Outcome::Finished { behind, .. }) => {
                failures = 0;
                reported = false;
                armed = behind;
            }
            // Another host runs it and lingers up to 1 s after catching up:
            // wait as long before trying again, so commits on this host do
            // not open a lock session each. Wakes meanwhile coalesce.
            Ok(Outcome::Contended) => {
                if !sleep_until(&context, Instant::now() + CONTENDED).await {
                    return;
                }
            }
            Ok(_) => {}
            Err(run) if run.failure.code == "cancelled" => armed = true,
            Err(run) => {
                failures = failures.saturating_add(1);
                // A guest's own code never reaches the log (TE §1.4 rule 3).
                if !reported {
                    report_failure(&projection, run.log_code());
                }
                reported = true;
                let _ = store
                    .record_failure(name.clone(), revision, &run.failure, Instant::now() + READ)
                    .await;
                if !sleep_until(&context, Instant::now() + backoff(failures)).await {
                    return;
                }
                armed = true;
            }
        }
    }
}
fn report_failure(projection: &Projection, code: &str) {
    merezha_telemetry::event!(
        WARN,
        "projection.failed",
        projection = projection.name.as_str(),
        code
    );
}

/// The resumable rebuild of one revision, from whichever instance gets its
/// `b:` lock.
async fn rebuild_lane(context: Arc<Context>, projection: Projection) {
    let store = context.store.clone();
    let Ok(lane) = store.shared().lane(&projection) else {
        return;
    };
    let (name, revision) = (
        projection.name.as_str().to_owned(),
        projection.revision as i32,
    );
    let mut armed = true;
    let mut failures = 0u32;
    loop {
        if !wake(&context, &lane.rebuild, armed).await {
            return;
        }
        armed = false;
        let running = store
            .rebuild_status_async(name.clone(), revision, Instant::now() + READ)
            .await;
        if !matches!(running, Ok(Some(true))) {
            continue;
        }
        let Some(result) = run(&context, &projection, runner::rebuild).await else {
            return;
        };
        match result {
            Ok(Outcome::Swapped(pair)) => {
                failures = 0;
                merezha_telemetry::event!(
                    INFO,
                    "projection.rebuild_completed",
                    projection = projection.name.as_str(),
                    revision = projection.revision,
                    checkpoint = pair.checkpoint
                );
            }
            // The candidate advanced; carry on in a fresh run.
            Ok(Outcome::Finished { .. }) => {
                failures = 0;
                armed = true;
            }
            // Another host runs it and lingers up to 1 s after catching up:
            // wait as long before trying again, so commits on this host do
            // not open a lock session each. Wakes meanwhile coalesce.
            Ok(Outcome::Contended) => {
                if !sleep_until(&context, Instant::now() + CONTENDED).await {
                    return;
                }
            }
            Ok(_) => {}
            Err(run) if run.failure.code == "cancelled" => armed = true,
            Err(run) => {
                failures = failures.saturating_add(1);
                merezha_telemetry::event!(
                    WARN,
                    "projection.rebuild_failed",
                    projection = projection.name.as_str(),
                    revision = projection.revision,
                    code = run.log_code()
                );
                // A failed batch marked the rebuild failed; anything else
                // (the database) retries with backoff.
                if !sleep_until(&context, Instant::now() + backoff(failures)).await {
                    return;
                }
                armed = true;
            }
        }
    }
}

fn wake_all(lanes: impl Iterator<Item = &'static str>, shared: &runner::Shared) {
    for kind in lanes {
        for lane in shared.lanes.values() {
            notify(lane, kind);
        }
    }
}
fn notify(lane: &Lane, kind: &str) {
    if kind == "live" {
        lane.live.notify_one();
    } else {
        lane.rebuild.notify_one();
    }
}

/// The safety net for lost notifications and external restores.
async fn interval(context: Arc<Context>) {
    let shared = context.store.shared();
    let every = shared.options.interval;
    loop {
        if !sleep_until(&context, Instant::now() + every).await {
            return;
        }
        wake_all(["live", "rebuild"].into_iter(), shared);
    }
}

/// One `LISTEN merezha_app_events` session through the kernel's
/// [`merezha_postgres::Database::listen`], reconnected with backoff.
async fn listen(context: Arc<Context>) {
    let shared = context.store.shared();
    let database = context.store.db.clone();
    let lost = AtomicBool::new(false);
    let mut delay = RECONNECT.0;
    loop {
        if stopped(&context) {
            return;
        }
        let session = database
            .listen(&[EVENTS_CHANNEL], Instant::now() + READ)
            .await;
        let mut listener = match session {
            Ok(listener) => listener,
            Err(failure) => {
                shared.listener.store(2, Ordering::SeqCst);
                if !lost.swap(true, Ordering::SeqCst) {
                    merezha_telemetry::event!(
                        WARN,
                        "db.listener_lost",
                        code = failure.code.as_str()
                    );
                }
                if !sleep_until(&context, Instant::now() + delay).await {
                    return;
                }
                delay = (delay * 2).min(RECONNECT.1);
                continue;
            }
        };
        shared.listener.store(1, Ordering::SeqCst);
        if lost.swap(false, Ordering::SeqCst) {
            merezha_telemetry::event!(INFO, "db.listener_restored");
        }
        delay = RECONNECT.0;
        // Notifications may have been lost while disconnected.
        wake_all(["live"].into_iter(), shared);
        shared.wake_reactors();
        let mut stop = context.stop.clone();
        let code = loop {
            tokio::select! {
                () = async { let _ = stop.wait_for(|stopped| *stopped).await; } => return,
                notification = listener.recv() => match notification {
                    Some(_) => {
                        while listener.try_recv().is_some() {}
                        wake_all(["live"].into_iter(), shared);
                        shared.wake_reactors();
                    }
                    None => break "database_unavailable",
                },
                () = tokio::time::sleep(PROBE) => {
                    if let Err(failure) = listener.check(Instant::now() + READ).await {
                        break if failure.code == "database_timeout" { "database_timeout" } else { "database_unavailable" };
                    }
                }
            }
        };
        drop(listener);
        shared.listener.store(2, Ordering::SeqCst);
        lost.store(true, Ordering::SeqCst);
        merezha_telemetry::event!(WARN, "db.listener_lost", code);
        if !sleep_until(&context, Instant::now() + delay).await {
            return;
        }
    }
}

/// The reactors' safety-net wake (`[application.reactors] interval_ms`).
async fn reactor_interval(context: Arc<Context>) {
    let shared = context.store.shared();
    let every = shared.options.reactor_interval;
    loop {
        if !sleep_until(&context, Instant::now() + every).await {
            return;
        }
        shared.wake_reactors();
    }
}
/// Wait for `notify` until `at`; `false` on stop.
async fn wake_or_until(context: &Context, notify: &tokio::sync::Notify, at: Instant) -> bool {
    let mut stop = context.stop.clone();
    tokio::select! {
        _ = stop.wait_for(|stopped| *stopped) => false,
        () = notify.notified() => !stopped(context),
        () = tokio::time::sleep_until(at.into()) => !stopped(context),
    }
}
/// One reactor run on a blocking thread, holding a background slot.
async fn run_reactor(
    context: &Arc<Context>,
    reactor: &Reactor,
) -> Option<Result<ReactorOutcome, RunFailure>> {
    let shared = context.store.shared();
    let cancel = shared.cancel.subscribe();
    if stopped(context) {
        return None;
    }
    let mut stop = context.stop.clone();
    let slot = tokio::select! {
        _ = stop.wait_for(|stopped| *stopped) => return None,
        slot = context.gate.clone().acquire_owned() => slot.ok()?,
    };
    runner::Pause::hold(shared.options.hooks().before_run, reactor.name.as_str()).await;
    shared.runs.fetch_add(1, Ordering::SeqCst);
    let (app, store, reactor) = (context.app.clone(), context.store.clone(), reactor.clone());
    let result = merezha_telemetry::spawn_blocking_in_span(move || {
        let bounds = Bounds::background(store.shared(), cancel);
        reactor::react(&app, &store, &reactor, &bounds)
    })
    .await;
    shared.runs.fetch_sub(1, Ordering::SeqCst);
    drop(slot);
    Some(result.unwrap_or_else(|_| Err(Failure::transient("internal").into())))
}
/// One reactor (AP §C.5): runs on a local commit, a `merezha_app_events`
/// notification, the reactor interval and its retry time. A paused
/// reactor waits for a wake; a failed one for its `retry_at`.
async fn reactor_lane(context: Arc<Context>, reactor: Reactor) {
    let store = context.store.clone();
    let Ok(lane) = store.reactor_lane(reactor.name.as_str()) else {
        return;
    };
    let name = reactor.name.as_str().to_owned();
    let (base, max) = store.shared().options.reactor_retry;
    let mut armed = true;
    let mut retry: Option<Instant> = None;
    let mut failures = 0u32;
    loop {
        if !armed {
            let woke = match retry {
                Some(at) => wake_or_until(&context, &lane.wake, at).await,
                None => wake(&context, &lane.wake, false).await,
            };
            if !woke {
                return;
            }
        }
        armed = false;
        retry = None;
        // A cheap pooled read first: no lock session when nothing is due.
        match store.reactor_due(&name, Instant::now() + READ).await {
            Ok(Due::Run) => {}
            Ok(Due::Idle) | Ok(Due::Paused) => continue,
            Ok(Due::Later(wait)) => {
                retry = Some(Instant::now() + wait);
                continue;
            }
            Err(_) => {
                failures = failures.saturating_add(1);
                retry = Some(Instant::now() + reactor::backoff(base, max, failures));
                continue;
            }
        }
        let Some(result) = run_reactor(&context, &reactor).await else {
            return;
        };
        match result {
            Ok(ReactorOutcome::Finished { behind }) => {
                failures = 0;
                armed = behind;
            }
            // Another host runs it and lingers up to 1 s after catching
            // up: wait as long before trying again. Wakes meanwhile
            // coalesce.
            Ok(ReactorOutcome::Contended) => {
                if !sleep_until(&context, Instant::now() + CONTENDED).await {
                    return;
                }
            }
            Ok(ReactorOutcome::Waiting(wait)) => {
                failures = 0;
                retry = wait.map(|wait| Instant::now() + wait);
            }
            Err(run) if run.failure.code == "cancelled" => armed = true,
            Err(_) => {
                // Storage or the lock session failed before the batch
                // could record anything: retry with backoff.
                failures = failures.saturating_add(1);
                retry = Some(Instant::now() + reactor::backoff(base, max, failures));
            }
        }
    }
}
