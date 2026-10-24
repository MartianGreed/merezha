//! Background projection runs on PostgreSQL (AP §E, A3 tests): the
//! scheduler and its wakes, the run locks on detached sessions,
//! cancellation, rebuild, revisions and warm readiness, `min_position`,
//! explicit catch-ups and the concurrency budget. Each test creates its own
//! database through `MEREZHA_TEST_DATABASE_URL` and skips when it is unset.
use merezha_application::{
    AllowAll, Application, Guest, GuestSession,
    config::Config,
    postgres::{
        BackgroundOptions, CatchUp, MIGRATIONS, MODULE, Pause, PostgresOptions, PostgresStore,
        RUN_LOCK_CLASS, RunHooks, Scheduler,
    },
};
use merezha_domain::{
    Command, Event, Failure, Identity, Metadata, Operation, Query, Reply, StoredEvent,
};
use merezha_postgres::{
    Database, MigrationMode, ModuleMigrations, TestDatabase, run_migrations, tokio_postgres::Row,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
fn config_with(revision: u32, limits: &str) -> Config {
    Config::parse(&format!(
        r#"version=1
{limits}
[storage]
backend="postgres"
application="bg-test"
[[commands]]
tag="Add"
service="business"
aggregate="Counter"
[[queries]]
tag="Count"
service="view"
projection="count"
[[queries]]
tag="Other"
service="view"
projection="other"
[[projections]]
name="count"
service="view"
revision={revision}
initial={{n=0,cold=false}}
[[projections]]
name="other"
service="view"
initial={{n=0,cold=false}}
"#
    ))
    .unwrap()
}
fn config() -> Config {
    config_with(1, "")
}
fn metadata() -> Metadata {
    Metadata {
        message_id: id("message-1"),
        correlation_id: id("correlation-1"),
        causation_id: None,
        actor: None,
    }
}
/// A command deciding `events` events on `stream`.
fn add(stream: &str, events: usize) -> Command {
    Command {
        tag: id("Add"),
        stream: id(stream),
        payload: json!({ "events": events }),
        metadata: metadata(),
        idempotency_key: None,
        expected_version: None,
    }
}
fn query(tag: &str, min_position: Option<u64>) -> Query {
    Query {
        tag: id(tag),
        payload: json!({}),
        metadata: metadata(),
        min_position,
    }
}
fn code<T: std::fmt::Debug>(result: Result<T, Failure>) -> String {
    result.expect_err("expected a failure").code
}
/// Wait until `ready` holds, within `within`.
fn until(within: Duration, what: &str, mut ready: impl FnMut() -> bool) -> Duration {
    let started = Instant::now();
    while !ready() {
        assert!(started.elapsed() < within, "{what} within {within:?}");
        std::thread::sleep(Duration::from_millis(10));
    }
    started.elapsed()
}

/// The guest. `decide` turns `{"events":n}` into n events. Projections
/// count events into `{"n", "cold"}`, where `cold` sticks once a batch came
/// with `live = false`. Every project call is recorded with its events'
/// positions, the number of calls in flight at once is tracked, and calls
/// can be slowed down, made to fail or to panic.
#[derive(Default)]
struct Probe {
    /// Positions of every project call, per projection, in call order.
    seen: Mutex<Vec<(String, Vec<u64>)>>,
    /// The `live` flag of every project call of `count`, with its positions.
    lives: Mutex<Vec<(Vec<u64>, bool)>>,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    delay_ms: AtomicU64,
    /// Panic once in the next project call of `count`.
    panic_once: AtomicBool,
    /// Fail project calls whose batch contains this position.
    fail_at: AtomicU64,
    budget: Option<usize>,
}
impl Probe {
    fn calls(&self, projection: &str) -> Vec<Vec<u64>> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| p == projection)
            .map(|(_, positions)| positions.clone())
            .collect()
    }
    fn positions(&self, projection: &str) -> Vec<u64> {
        self.calls(projection).concat()
    }
}
#[derive(Clone)]
struct Fake(Arc<Probe>);
impl Guest for Fake {
    fn services(&self) -> Result<Vec<String>, Failure> {
        Ok(vec!["business".into(), "view".into()])
    }
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
        Ok(Box::new(self.clone()))
    }
    fn max_concurrent_invocations(&self) -> Option<usize> {
        self.0.budget
    }
}
struct InFlight<'a>(&'a Probe);
impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}
impl GuestSession for Fake {
    fn invoke(&self, _: &str, operation: Operation) -> Result<Reply, Failure> {
        match operation {
            Operation::Validate { payload, .. } => Ok(Reply::Validated { payload }),
            Operation::Decide { payload, .. } => {
                let n = payload["events"].as_u64().unwrap_or(1) as usize;
                let events = (0..n)
                    .map(|_| Event {
                        tag: id("Added"),
                        schema_version: 1,
                        payload: json!({}),
                    })
                    .collect();
                Ok(Reply::Decided { events })
            }
            Operation::Project {
                projection,
                state,
                events,
                live,
            } => {
                let probe = &self.0;
                let now = probe.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                let _in_flight = InFlight(probe);
                probe.max_in_flight.fetch_max(now, Ordering::SeqCst);
                let positions: Vec<u64> = events.iter().map(|e: &StoredEvent| e.position).collect();
                probe
                    .seen
                    .lock()
                    .unwrap()
                    .push((projection.as_str().to_owned(), positions.clone()));
                if projection.as_str() == "count" {
                    probe.lives.lock().unwrap().push((positions.clone(), live));
                }
                let delay = probe.delay_ms.load(Ordering::SeqCst);
                if delay > 0 {
                    std::thread::sleep(Duration::from_millis(delay));
                }
                if projection.as_str() == "count" && probe.panic_once.swap(false, Ordering::SeqCst)
                {
                    panic!("guest panic (test)");
                }
                let fail = probe.fail_at.load(Ordering::SeqCst);
                if fail > 0 && positions.contains(&fail) {
                    return Err(Failure::permanent("projection_failed"));
                }
                Ok(Reply::Projected {
                    state: json!({
                        "n": state["n"].as_u64().unwrap() + events.len() as u64,
                        "cold": state["cold"].as_bool().unwrap() || !live,
                    }),
                })
            }
            Operation::Query { state, .. } => Ok(Reply::Queried { value: state }),
        }
    }
}

/// A fresh migrated database and the runtime the stores block on.
struct Pg {
    runtime: tokio::runtime::Runtime,
    admin: Database,
    test: TestDatabase,
}
/// One instance: its application, store and (when started) scheduler.
struct Instance {
    app: Application,
    store: Arc<PostgresStore>,
    scheduler: Option<Scheduler>,
}
impl Pg {
    fn new() -> Option<Self> {
        let test = TestDatabase::create()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let admin = Database::connect(&test.config()).unwrap();
        runtime.block_on(async {
            let modules = [ModuleMigrations {
                module: MODULE,
                migrations: MIGRATIONS,
            }];
            run_migrations(&admin, &modules, MigrationMode::Apply, deadline())
                .await
                .unwrap();
        });
        Some(Self {
            runtime,
            admin,
            test,
        })
    }
    fn options(catch_up: CatchUp, interval: Duration, hooks: RunHooks) -> BackgroundOptions {
        BackgroundOptions {
            catch_up,
            interval,
            run_timeout: Duration::from_secs(5),
            instance_id: "test".into(),
            hooks,
            ..BackgroundOptions::default()
        }
    }
    /// An instance with its own pool, as another host.
    fn open(
        &self,
        config: Config,
        probe: &Arc<Probe>,
        background: BackgroundOptions,
    ) -> Result<Instance, Failure> {
        let mut db_config = self.test.config();
        db_config.pool_size = 6;
        // Room for a loaded machine; the tests bound their own waits.
        db_config.acquire_timeout = Duration::from_secs(5);
        let db = Database::connect(&db_config).unwrap();
        let options = PostgresOptions {
            background,
            ..PostgresOptions::default()
        };
        let store = Arc::new(self.runtime.block_on(PostgresStore::connect(
            &db,
            &config,
            options,
            deadline(),
        ))?);
        let app = Application::with_postgres(
            config,
            Arc::new(Fake(probe.clone())),
            Arc::new(AllowAll),
            store.clone(),
        )?;
        Ok(Instance {
            app,
            store,
            scheduler: None,
        })
    }
    /// An instance with a running scheduler.
    fn start(&self, config: Config, probe: &Arc<Probe>, background: BackgroundOptions) -> Instance {
        let mut instance = self.open(config, probe, background).unwrap();
        let _guard = self.runtime.enter();
        instance.scheduler = Some(Scheduler::start(&instance.app).unwrap());
        instance
    }
    fn stop(&self, instance: &Instance) {
        if let Some(scheduler) = &instance.scheduler {
            self.runtime.block_on(scheduler.shutdown(deadline()));
        }
    }
    fn rows(&self, statement: &str) -> Vec<Row> {
        self.runtime.block_on(async {
            let client = self.admin.acquire(deadline()).await.unwrap();
            client.query(statement, &[]).await.unwrap()
        })
    }
    fn count(&self, statement: &str) -> i64 {
        self.rows(statement)[0].get(0)
    }
    fn execute(&self, statement: &str) {
        self.runtime.block_on(async {
            let client = self.admin.acquire(deadline()).await.unwrap();
            client.batch_execute(statement).await.unwrap();
        });
    }
    fn checkpoint(&self, name: &str) -> u64 {
        self.count(&format!(
            "SELECT max(checkpoint) FROM merezha_app.projections WHERE name = '{name}'"
        )) as u64
    }
    /// Backends holding a run lock (`classid = 'MZAP'`) and their names.
    fn lock_holders(&self) -> Vec<(i32, String)> {
        self.rows(&format!(
            "SELECT a.pid, a.application_name FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid \
             WHERE l.locktype = 'advisory' AND l.classid = {RUN_LOCK_CLASS} AND l.granted \
               AND a.datname = current_database()"
        ))
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
    }
    /// No pooled session (named `merezha`) ever holds a run lock.
    fn assert_no_pooled_lock(&self) {
        for (pid, name) in self.lock_holders() {
            assert!(
                name.starts_with("merezha p:") || name.starts_with("merezha b:"),
                "backend {pid} ({name}) holds a run lock"
            );
        }
    }
}

fn fill(instance: &Instance, commands: usize, events: usize) {
    for i in 0..commands {
        instance
            .app
            .command(add(&format!("s{}", i % 8), events))
            .unwrap();
    }
}
fn count_of(instance: &Instance, tag: &str) -> (Value, u64) {
    let result = instance.app.query(query(tag, None)).unwrap();
    (result.value, result.checkpoint)
}

/// A3 test 2 and "two instances never run the same projection at once":
/// every batch is applied once, never by two runs at once.
#[test]
fn two_instances_run_one_live_runner_per_projection_and_apply_each_batch_once() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    probe.delay_ms.store(2, Ordering::SeqCst);
    let mut c = config();
    c.limits.batch_size = 4;
    let interval = Duration::from_millis(200);
    let instances = [
        pg.start(
            c.clone(),
            &probe,
            Pg::options(CatchUp::Auto, interval, RunHooks::default()),
        ),
        pg.start(
            c,
            &probe,
            Pg::options(CatchUp::Auto, interval, RunHooks::default()),
        ),
    ];
    let sampled = AtomicBool::new(true);
    std::thread::scope(|scope| {
        let sampler = scope.spawn(|| {
            let mut samples = 0;
            while sampled.load(Ordering::SeqCst) {
                let holders = pg.lock_holders();
                let live: Vec<_> = holders
                    .iter()
                    .filter(|(_, name)| name == "merezha p:count:1")
                    .collect();
                assert!(live.len() <= 1, "two live runners: {holders:?}");
                pg.assert_no_pooled_lock();
                samples += 1;
            }
            samples
        });
        for i in 0..150 {
            instances[i % 2]
                .app
                .command(add(&format!("s{}", i % 8), 2))
                .unwrap();
        }
        until(
            Duration::from_secs(20),
            "both projections caught up",
            || pg.checkpoint("count") == 300 && pg.checkpoint("other") == 300,
        );
        sampled.store(false, Ordering::SeqCst);
        eprintln!("lock samples: {}", sampler.join().unwrap());
    });
    // Two projections may run at once, one run per projection may not.
    assert!(probe.max_in_flight.load(Ordering::SeqCst) <= 2);
    for name in ["count", "other"] {
        assert_eq!(
            probe.positions(name),
            (1..=300).collect::<Vec<_>>(),
            "{name}: each event applied once, in order"
        );
    }
    for instance in &instances {
        assert_eq!(
            count_of(instance, "Count"),
            (json!({"n":300,"cold":false}), 300)
        );
        pg.stop(instance);
    }
}

/// A3 test 3: killing the lock holder's session mid-run lets the other
/// instance take over; the final state equals a single-instance fold.
#[test]
fn killing_the_lock_holders_session_lets_the_other_instance_take_over() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let mut c = config();
    c.limits.batch_size = 2;
    let writer = pg
        .open(c.clone(), &probe, BackgroundOptions::default())
        .unwrap();
    fill(&writer, 40, 1);
    probe.delay_ms.store(300, Ordering::SeqCst);
    let interval = Duration::from_millis(200);
    let first = pg.start(
        c.clone(),
        &probe,
        Pg::options(CatchUp::Auto, interval, RunHooks::default()),
    );
    until(Duration::from_secs(5), "a live run holds the lock", || {
        pg.lock_holders()
            .iter()
            .any(|(_, n)| n == "merezha p:count:1")
    });
    // The holder reports its lock age (TE's `merezha.projection.lock.held`)
    // once the lock call returned to it; `pg_locks` shows the grant first.
    until(
        Duration::from_secs(5),
        "the holder records its lock",
        || {
            first
                .store
                .held_locks()
                .iter()
                .any(|(kind, name, revision, _)| {
                    (*kind, name.as_str(), *revision) == ("live", "count", 1)
                })
        },
    );
    let second = pg.start(
        c,
        &probe,
        Pg::options(CatchUp::Auto, interval, RunHooks::default()),
    );
    // The other instance counts its contended attempts.
    until(Duration::from_secs(5), "a contended attempt", || {
        second
            .store
            .contended_locks()
            .iter()
            .any(|(name, _, live, _)| name == "count" && *live > 0)
    });
    let before = pg.checkpoint("count");
    assert!(before < 40, "the first run is still mid-way");
    pg.execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE application_name = 'merezha p:count:1' AND datname = current_database()",
    );
    probe.delay_ms.store(0, Ordering::SeqCst);
    until(
        Duration::from_secs(10),
        "the other instance finishes",
        || pg.checkpoint("count") == 40,
    );
    // A batch computed by the killed run was never published, so the
    // published state is exactly the fold, whatever was redelivered.
    for instance in [&first, &second] {
        assert_eq!(
            count_of(instance, "Count"),
            (json!({"n":40,"cold":false}), 40)
        );
    }
    let distinct: BTreeSet<u64> = probe.positions("count").into_iter().collect();
    assert_eq!(distinct, (1..=40).collect());
    pg.assert_no_pooled_lock();
    pg.stop(&first);
    pg.stop(&second);
}

/// The CAS, not the lock, keeps the data correct: a run without the lock
/// (as one whose lock session died) loses its CAS against the run that
/// published first and writes nothing.
#[test]
fn a_run_without_the_lock_loses_its_cas_and_writes_nothing() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let hooks = RunHooks {
        skip_lock: true,
        pause: Some(Arc::new(Pause::default())),
        ..RunHooks::default()
    };
    let pause = hooks.pause.clone().unwrap();
    let options = Pg::options(CatchUp::Manual, Duration::from_secs(600), hooks);
    let a = pg.open(config(), &probe, options.clone()).unwrap();
    let b = pg.open(config(), &probe, options).unwrap();
    fill(&a, 3, 1);
    // b computes and holds its commit open; a publishes meanwhile.
    pause.arm("count", Duration::from_millis(1500));
    let other = b.app.clone();
    let racing = std::thread::spawn(move || other.catch_up("count"));
    until(Duration::from_secs(5), "b pauses in its commit", || {
        pause.entered()
    });
    let published = a.app.catch_up("count").unwrap_or_else(|f| {
        // a's CAS waits on b's row lock; once b commits, a may lose instead.
        assert_eq!(f.code, "projection_conflict");
        a.app.catch_up("count").unwrap()
    });
    let raced = racing.join().unwrap();
    assert_eq!(published.checkpoint, 3);
    // Exactly one of them published the batch; the loser wrote nothing.
    assert_eq!(
        pg.count("SELECT row_revision FROM merezha_app.projections WHERE name = 'count'"),
        1
    );
    match raced {
        Ok(pair) => assert_eq!(pair.checkpoint, 3),
        Err(failure) => assert_eq!(failure.code, "projection_conflict"),
    }
    assert_eq!(count_of(&a, "Count"), (json!({"n":3,"cold":false}), 3));
}

/// Which run the cancellation test cancels, and how.
#[derive(Clone, Copy, Debug)]
enum Cut {
    /// The future is dropped while its commit transaction is open.
    Drop,
    /// The future is dropped while its backend runs a statement: the
    /// session's `client_connection_check_interval` ends it (CT-C2).
    Statement,
    /// The run's hard deadline (`timeout_at`) ends the commit.
    Deadline,
    /// The guest panics inside the run.
    Panic,
}

/// A3 test 8: a live or rebuild run cancelled mid-batch by a dropped
/// future, a `timeout_at` or a guest panic releases its lock at once: the
/// other instance and this one's next run proceed within 1 s, the batch
/// left no rows and no pooled session holds a run lock.
#[test]
fn cancelled_runs_release_their_lock_and_leave_no_rows() {
    for cut in [Cut::Drop, Cut::Statement, Cut::Deadline, Cut::Panic] {
        for rebuild in [false, true] {
            cancelled_run(cut, rebuild);
        }
    }
}
fn cancelled_run(cut: Cut, rebuild: bool) {
    let Some(pg) = Pg::new() else { return };
    let what = format!("{cut:?} {}", if rebuild { "rebuild" } else { "live" });
    let probe = Arc::new(Probe::default());
    let mut c = config();
    c.limits.batch_size = 4;
    let pause = Arc::new(Pause::default());
    let hooks = RunHooks {
        pause: Some(pause.clone()),
        hard_timeout: matches!(cut, Cut::Deadline).then_some(Duration::from_millis(1500)),
        ..RunHooks::default()
    };
    let interval = Duration::from_millis(200);
    let manual = Pg::options(CatchUp::Manual, interval, RunHooks::default());
    let writer = pg.open(c.clone(), &probe, manual.clone()).unwrap();
    fill(&writer, 10, 2);
    if rebuild {
        writer.app.catch_up("count").unwrap();
        probe.seen.lock().unwrap().clear();
    }
    match cut {
        Cut::Drop | Cut::Deadline => pause.arm("count", Duration::from_secs(3)),
        Cut::Statement => pause.arm_statement("count", Duration::from_secs(4)),
        Cut::Panic => probe.panic_once.store(true, Ordering::SeqCst),
    }
    let kind = if rebuild { "b" } else { "p" };
    let lock = format!("merezha {kind}:count:1");
    // Only the instance under test runs live catch-up at first.
    let a = pg.start(
        c.clone(),
        &probe,
        Pg::options(CatchUp::Auto, interval, hooks),
    );
    if rebuild {
        // a's rebuild lane finds it on its interval.
        writer
            .app
            .admit()
            .unwrap()
            .request_rebuild("count", false, None)
            .unwrap();
    }
    let held_by = || {
        pg.lock_holders()
            .into_iter()
            .filter(|(_, name)| *name == lock)
            .map(|(pid, _)| pid)
            .collect::<Vec<_>>()
    };
    let first = match cut {
        Cut::Drop | Cut::Statement | Cut::Deadline => {
            until(
                Duration::from_secs(5),
                &format!("{what}: the commit pauses"),
                || pause.entered(),
            );
            let pid = held_by();
            assert_eq!(pid.len(), 1, "{what}");
            Some(pid[0])
        }
        Cut::Panic => None,
    };
    let progress = || {
        if rebuild {
            pg.count("SELECT coalesce(max(checkpoint), 0) FROM merezha_app.projection_rebuilds")
                as u64
        } else {
            pg.checkpoint("count")
        }
    };
    // Nothing of the paused batch is visible.
    assert_eq!(progress(), 0, "{what}");
    let b = pg.start(c.clone(), &probe, manual.clone());
    if let Cut::Drop | Cut::Statement = cut {
        a.scheduler.as_ref().unwrap().cancel_runs();
    }
    // The lock is released and the work proceeds within 1 s of the cut.
    let cut_at = match cut {
        Cut::Deadline => Duration::from_millis(1700),
        // The server notices the closed socket at its next check.
        Cut::Statement => Duration::from_secs(2),
        _ => Duration::from_millis(1000),
    };
    until(cut_at, &format!("{what}: the lock is released"), || {
        first.is_none_or(|pid| !held_by().contains(&pid))
    });
    if rebuild {
        // b's rebuild lane runs on its interval.
        until(
            Duration::from_secs(3),
            &format!("{what}: the rebuild swaps"),
            || pg.count("SELECT count(*) FROM merezha_app.projection_rebuilds") == 0,
        );
    } else {
        writer.app.command(add("after", 1)).unwrap();
        until(
            Duration::from_secs(3),
            &format!("{what}: live catch-up proceeds"),
            || pg.checkpoint("count") == 21,
        );
    }
    pg.assert_no_pooled_lock();
    // The cut batch was delivered again from the same checkpoint: it had
    // left nothing behind.
    let calls = probe.calls("count");
    eprintln!("{what}: {calls:?}");
    assert!(calls.len() >= 2, "{what}: {calls:?}");
    assert_eq!(calls[0], calls[1], "{what}: the cut batch was redone");
    let expected = if rebuild {
        json!({"n":20,"cold":true})
    } else {
        json!({"n":21,"cold":false})
    };
    assert_eq!(count_of(&writer, "Count").0, expected, "{what}");
    pg.stop(&a);
    pg.stop(&b);
}

/// A3 test 4: a rebuild over 20 000 events while commands continue.
/// Queries serve the old state until the swap and never an empty one; the
/// swap is atomic, and live catch-up carries the new state on.
#[test]
fn a_rebuild_over_20000_events_serves_the_old_state_until_an_atomic_swap() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let c = config_with(1, "[limits]\nbatch_size=64\ntimeout_ms=30000");
    let interval = Duration::from_millis(200);
    let a = pg.start(
        c.clone(),
        &probe,
        Pg::options(CatchUp::Auto, interval, RunHooks::default()),
    );
    for i in 0..313 {
        a.app.command(add(&format!("s{}", i % 8), 64)).unwrap();
    }
    until(Duration::from_secs(60), "live catch-up", || {
        pg.checkpoint("count") == 20_032
    });
    let requested = a
        .app
        .admit()
        .unwrap()
        .request_rebuild("count", false, None)
        .unwrap();
    assert_eq!(requested.target, 20_032);
    assert_eq!(
        code(a.app.admit().unwrap().request_rebuild("count", false, None)),
        "rebuild_in_progress"
    );
    let stop = AtomicBool::new(false);
    // Queries the reader answered from the rebuilt state so far.
    let rebuilt = AtomicUsize::new(0);
    let (old, new) = std::thread::scope(|scope| {
        let reader = scope.spawn(|| {
            let mut old = 0;
            let mut swapped = false;
            while !stop.load(Ordering::SeqCst) {
                let (value, checkpoint) = count_of(&a, "Count");
                // Never empty, always a consistent pair.
                assert_eq!(value["n"], json!(checkpoint), "{value} at {checkpoint}");
                assert!(checkpoint >= 20_000, "{value} at {checkpoint}");
                if value["cold"] == true {
                    swapped = true;
                    rebuilt.fetch_add(1, Ordering::SeqCst);
                } else {
                    assert!(!swapped, "the old state came back after the swap");
                    old += 1;
                }
            }
            (old, rebuilt.load(Ordering::SeqCst))
        });
        for i in 0..20 {
            a.app.command(add(&format!("w{}", i % 4), 1)).unwrap();
            std::thread::sleep(Duration::from_millis(20));
        }
        until(Duration::from_secs(60), "the swap", || {
            pg.count("SELECT count(*) FROM merezha_app.projection_rebuilds") == 0
        });
        until(Duration::from_secs(10), "live carries on", || {
            pg.checkpoint("count") == 20_052
        });
        // However starved the reader, it answers from the swapped state
        // before it stops.
        until(Duration::from_secs(30), "a query after the swap", || {
            rebuilt.load(Ordering::SeqCst) > 0
        });
        stop.store(true, Ordering::SeqCst);
        reader.join().unwrap()
    });
    eprintln!("queries: {old} old, {new} rebuilt");
    assert!(old > 0 && new > 0);
    assert_eq!(
        count_of(&a, "Count"),
        (json!({"n":20_052,"cold":true}), 20_052)
    );
    assert_eq!(
        pg.count("SELECT generation::bigint FROM merezha_app.projections WHERE name = 'count'"),
        2
    );
    pg.stop(&a);
}

/// A failed rebuild leaves the served pair, can resume from its candidate
/// and can be abandoned.
#[test]
fn a_failed_rebuild_keeps_the_served_pair_resumes_and_can_be_abandoned() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let mut c = config();
    c.limits.batch_size = 2;
    let options = Pg::options(
        CatchUp::Manual,
        Duration::from_millis(200),
        RunHooks::default(),
    );
    let a = pg.start(c, &probe, options);
    fill(&a, 6, 1);
    a.app.catch_up("count").unwrap();
    probe.fail_at.store(5, Ordering::SeqCst);
    a.app
        .admit()
        .unwrap()
        .request_rebuild("count", false, None)
        .unwrap();
    until(Duration::from_secs(5), "the rebuild fails", || {
        pg.count("SELECT count(*) FROM merezha_app.projection_rebuilds WHERE status = 'failed'")
            == 1
    });
    let overview = a.app.admit().unwrap().projection("count").unwrap();
    let rebuild = overview.rebuild.clone().unwrap();
    assert_eq!(
        (rebuild.checkpoint, rebuild.target, rebuild.status.as_str()),
        (4, 6, "failed")
    );
    assert_eq!(count_of(&a, "Count"), (json!({"n":6,"cold":false}), 6));
    probe.fail_at.store(0, Ordering::SeqCst);
    probe.seen.lock().unwrap().clear();
    a.app
        .admit()
        .unwrap()
        .request_rebuild("count", true, None)
        .unwrap();
    until(Duration::from_secs(5), "the resumed rebuild swaps", || {
        pg.count("SELECT count(*) FROM merezha_app.projection_rebuilds") == 0
    });
    // Resumed from its persisted candidate, not from the start.
    assert_eq!(probe.positions("count"), vec![5, 6]);
    assert_eq!(count_of(&a, "Count"), (json!({"n":6,"cold":true}), 6));
    // Abandoning a rebuild drops it; the runner stops at its next batch.
    probe.delay_ms.store(200, Ordering::SeqCst);
    a.app
        .admit()
        .unwrap()
        .request_rebuild("count", false, None)
        .unwrap();
    a.app.admit().unwrap().abandon_rebuild("count").unwrap();
    a.app.admit().unwrap().abandon_rebuild("count").unwrap();
    std::thread::sleep(Duration::from_millis(800));
    assert_eq!(
        pg.count("SELECT count(*) FROM merezha_app.projection_rebuilds"),
        0
    );
    assert_eq!(
        pg.count("SELECT generation::bigint FROM merezha_app.projections WHERE name = 'count'"),
        2
    );
    pg.stop(&a);
}

/// A3 test 5: a new revision is not ready (and its queries fail
/// `projection_warming`) until it is warm, while an instance on the old
/// revision keeps serving. Old revision rows go only on request.
#[test]
fn a_new_revision_is_not_ready_until_warm_while_the_old_revision_keeps_serving() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let interval = Duration::from_millis(200);
    let mut old_config = config();
    old_config.limits.batch_size = 2;
    let old = pg.start(
        old_config,
        &probe,
        Pg::options(CatchUp::Auto, interval, RunHooks::default()),
    );
    fill(&old, 20, 1);
    until(
        Duration::from_secs(5),
        "the old revision catches up",
        || old.app.query(query("Count", Some(20))).is_ok(),
    );
    // Every lane checks its revision once at start.
    until(Duration::from_secs(5), "the old instance is ready", || {
        old.scheduler.as_ref().unwrap().ready()
    });
    probe.delay_ms.store(400, Ordering::SeqCst);
    let mut new_config = config_with(2, "");
    new_config.limits.batch_size = 2;
    let new = pg.start(
        new_config.clone(),
        &probe,
        Pg::options(CatchUp::Auto, interval, RunHooks::default()),
    );
    let scheduler = new.scheduler.as_ref().unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert!(!scheduler.ready(), "warming");
    assert_eq!(
        code(new.app.query(query("Count", None))),
        "projection_warming"
    );
    // The old revision keeps serving and advancing meanwhile.
    old.app.command(add("more", 1)).unwrap();
    assert_eq!(
        old.app.query(query("Count", Some(21))).unwrap().value,
        json!({"n":21,"cold":false})
    );
    let warm = new.app.admit().unwrap().projection("count").unwrap();
    assert!(!warm.warm);
    probe.delay_ms.store(0, Ordering::SeqCst);
    until(Duration::from_secs(30), "warm", || scheduler.ready());
    // Up to the warm position the guest saw `live = false`, then `true`.
    let (value, _) = count_of(&new, "Count");
    assert_eq!(value["cold"], true);
    let revisions = new
        .app
        .admit()
        .unwrap()
        .projection("count")
        .unwrap()
        .revisions
        .unwrap();
    assert_eq!(
        revisions.iter().map(|r| r.revision).collect::<Vec<_>>(),
        vec![1, 2]
    );
    // Without warm readiness an instance is ready at once.
    let mut options = Pg::options(CatchUp::Manual, interval, RunHooks::default());
    options.warm_readiness = false;
    let mut third_config = config_with(3, "");
    third_config.limits.batch_size = 2;
    let third = pg.start(third_config, &probe, options);
    assert!(third.scheduler.as_ref().unwrap().ready());
    pg.stop(&third);
    // The configured revision cannot be deleted; an old one can, once no
    // instance serves it.
    pg.stop(&old);
    assert_eq!(
        code(new.app.admit().unwrap().delete_revision("count", 2)),
        "revision_active"
    );
    new.app
        .admit()
        .unwrap()
        .delete_revision("count", 1)
        .unwrap();
    assert_eq!(
        code(new.app.admit().unwrap().delete_revision("count", 1)),
        "projection_not_found"
    );
    // Rows of a projection no longer configured can be cleaned up.
    pg.execute(
        "INSERT INTO merezha_app.projections (name, revision, partitioned, state, warm_position) \
         VALUES ('removed', 1, false, '0', 0)",
    );
    new.app
        .admit()
        .unwrap()
        .delete_revision("removed", 1)
        .unwrap();
    assert_eq!(
        code(new.app.admit().unwrap().delete_revision("removed", 1)),
        "projection_not_found"
    );
    pg.stop(&new);
}

/// A3 test 6: `min_position` reads your write, or fails
/// `projection_behind` at the deadline.
#[test]
fn min_position_reads_your_write_or_fails_projection_behind() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let interval = Duration::from_secs(600);
    let a = pg.start(
        config(),
        &probe,
        Pg::options(CatchUp::Auto, interval, RunHooks::default()),
    );
    for i in 1..=20u64 {
        let receipt = a.app.command(add("rw", 1)).unwrap();
        let result = a.app.query(query("Count", Some(receipt.position))).unwrap();
        assert!(result.checkpoint >= receipt.position);
        assert_eq!(result.value["n"], json!(result.checkpoint), "{i}");
    }
    pg.stop(&a);
    // Nothing runs in manual mode: the query waits out its deadline.
    let manual = Pg::options(CatchUp::Manual, interval, RunHooks::default());
    let short = config_with(1, "[limits]\ntimeout_ms=500");
    let b = pg.start(short, &probe, manual);
    // The write goes through a host with the default 5 s deadline, so a
    // loaded machine cannot cut it; only the query has 500 ms.
    let writer = pg
        .open(config(), &probe, BackgroundOptions::default())
        .unwrap();
    let receipt = writer.app.command(add("rw", 1)).unwrap();
    let started = Instant::now();
    assert_eq!(
        code(b.app.query(query("Count", Some(receipt.position)))),
        "projection_behind"
    );
    assert!(started.elapsed() >= Duration::from_millis(400));
    // The 500 ms deadline, with slack for a loaded runner.
    assert!(started.elapsed() < Duration::from_millis(3500));
    assert!(b.app.query(query("Count", Some(20))).is_ok());
    pg.stop(&b);
}

/// A3 test 7: the guest concurrency budget is checked at start, and
/// `catch_up = "manual"` keeps the request-driven behaviour.
#[test]
fn the_guest_concurrency_budget_is_checked_and_manual_catch_up_keeps_the_old_behaviour() {
    let Some(pg) = Pg::new() else { return };
    let mut c = config();
    c.limits.max_concurrent = 4;
    let interval = Duration::from_millis(200);
    let options = Pg::options(CatchUp::Manual, interval, RunHooks::default());
    // 4 HTTP admissions plus 2 background runs need 6 guest calls.
    let tight = Arc::new(Probe {
        budget: Some(5),
        ..Probe::default()
    });
    let instance = pg.open(c.clone(), &tight, options.clone()).unwrap();
    {
        let _guard = pg.runtime.enter();
        assert_eq!(
            code(Scheduler::start(&instance.app)),
            "invalid_concurrency_budget"
        );
    }
    let probe = Arc::new(Probe {
        budget: Some(6),
        ..Probe::default()
    });
    let manual = pg.start(c, &probe, options);
    fill(&manual, 5, 1);
    std::thread::sleep(Duration::from_millis(1000));
    assert_eq!(pg.checkpoint("count"), 0, "nothing runs without a request");
    assert_eq!(count_of(&manual, "Count"), (json!({"n":0,"cold":false}), 0));
    assert_eq!(manual.app.catch_up("count").unwrap().checkpoint, 5);
    assert_eq!(count_of(&manual, "Count"), (json!({"n":5,"cold":false}), 5));
    let status = manual.app.status().unwrap();
    assert_eq!(status.listener, Some("off"));
    assert_eq!(status.head, 5);
    pg.stop(&manual);
}

/// An explicit catch-up waits on the runner holding the lock within its
/// deadline, fails `timeout` past it with progress kept, and at most
/// `background.max_concurrent` explicit catch-ups hold a lock session.
#[test]
fn explicit_catch_up_waits_on_the_running_runner_within_its_deadline() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let mut c = config_with(1, "[limits]\ntimeout_ms=3000");
    c.limits.batch_size = 2;
    let writer = pg
        .open(c.clone(), &probe, BackgroundOptions::default())
        .unwrap();
    fill(&writer, 20, 1);
    probe.delay_ms.store(50, Ordering::SeqCst);
    let interval = Duration::from_millis(200);
    let runner = pg.start(
        c.clone(),
        &probe,
        Pg::options(CatchUp::Auto, interval, RunHooks::default()),
    );
    until(Duration::from_secs(5), "the runner holds the lock", || {
        pg.lock_holders()
            .iter()
            .any(|(_, n)| n == "merezha p:count:1")
    });
    // Another instance's request waits for that runner instead of running.
    let mut options = Pg::options(
        CatchUp::Manual,
        Duration::from_secs(600),
        RunHooks::default(),
    );
    options.max_concurrent = 1;
    let other = pg.open(c.clone(), &probe, options.clone()).unwrap();
    let started = Instant::now();
    let pair = other.app.catch_up("count").unwrap();
    assert!(pair.checkpoint >= 20);
    assert!(started.elapsed() < Duration::from_secs(6));
    assert!(pg.lock_holders().len() <= 2);
    pg.stop(&runner);
    // Past its deadline a waiting request fails `timeout`; progress stays.
    fill(&writer, 30, 1);
    let short = config_with(1, "[limits]\ntimeout_ms=400\nbatch_size=2");
    let slow = pg.open(short.clone(), &probe, options.clone()).unwrap();
    assert_eq!(code(slow.app.catch_up("count")), "timeout");
    let kept = pg.checkpoint("count");
    assert!(kept > 20, "progress persists: {kept}");
    // With max_concurrent = 1, two explicit catch-ups on two projections
    // hold one lock session at a time; the second waits for a slot.
    let both = pg.open(c, &probe, options).unwrap();
    let sampling = AtomicBool::new(true);
    std::thread::scope(|scope| {
        let sampler = scope.spawn(|| {
            let mut most = 0;
            while sampling.load(Ordering::SeqCst) {
                most = most.max(pg.lock_holders().len());
            }
            most
        });
        let left = scope.spawn(|| both.app.catch_up("count"));
        let right = scope.spawn(|| both.app.catch_up("other"));
        let (left, right) = (left.join().unwrap(), right.join().unwrap());
        sampling.store(false, Ordering::SeqCst);
        assert!(sampler.join().unwrap() <= 1);
        // The waiting one may run out of time while the other works.
        for result in [left, right] {
            if let Err(failure) = result {
                assert_eq!(failure.code, "timeout");
            }
        }
    });
}

/// The status document and the projection overview.
#[test]
fn status_and_overview_report_lag_warmth_and_failures() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let options = Pg::options(
        CatchUp::Auto,
        Duration::from_millis(200),
        RunHooks::default(),
    );
    let a = pg.start(config_with(1, "[limits]\nbatch_size=1"), &probe, options);
    probe.fail_at.store(2, Ordering::SeqCst);
    fill(&a, 3, 1);
    until(Duration::from_secs(5), "the failure is recorded", || {
        pg.count("SELECT count(*) FROM merezha_app.projections WHERE status = 'failing'") == 2
    });
    let status = a.app.status().unwrap();
    assert_eq!(status.application, Some(id("bg-test")));
    assert_eq!(status.head, 3);
    let count = &status.projections[0];
    assert_eq!(
        (
            count.name.as_str(),
            count.checkpoint,
            count.lag_events,
            count.status.as_str()
        ),
        ("count", 1, 2, "failing")
    );
    assert_eq!(count.failure_code.as_deref(), Some("projection_failed"));
    assert!(count.lag_seconds >= 0.0);
    assert_eq!(status.instance.as_ref().unwrap().id, "test");
    let document = serde_json::to_value(&status).unwrap();
    assert_eq!(
        document.as_object().unwrap().keys().collect::<Vec<_>>(),
        vec![
            "application",
            "head",
            "instance",
            "listener",
            "projections",
            "reactors"
        ]
    );
    let overview = a.app.admit().unwrap().projections().unwrap();
    assert_eq!(overview.head, 3);
    let entry = serde_json::to_value(&overview.projections[0]).unwrap();
    assert_eq!(entry["lag"], 2);
    assert_eq!(entry["warm"], true);
    assert!(entry.get("rebuild").is_none() && entry.get("revisions").is_none());
    assert!(entry["updated_at"].as_str().unwrap().ends_with('Z'));
    probe.fail_at.store(0, Ordering::SeqCst);
    until(
        Duration::from_secs(5),
        "recovered after the backoff",
        || pg.checkpoint("count") == 3 && pg.checkpoint("other") == 3,
    );
    let status = a.app.status().unwrap();
    assert!(
        status
            .projections
            .iter()
            .all(|p| p.status == "active" && p.lag_events == 0)
    );
    assert_eq!(status.projections[0].lag_seconds, 0.0);
    pg.stop(&a);
}

/// Review finding 1: while another host holds the lock, local commits do
/// not open a lock session each. After a contended attempt the lane waits
/// the holder's linger (1 s) before trying again.
#[test]
fn local_commits_on_a_host_that_does_not_hold_the_lock_open_few_lock_sessions() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let mut c = config();
    c.limits.batch_size = 1;
    let interval = Duration::from_secs(600);
    let writer = pg
        .open(c.clone(), &probe, BackgroundOptions::default())
        .unwrap();
    fill(&writer, 30, 1);
    // The holder takes 30 batches of 200 ms: a run of about 5 s.
    probe.delay_ms.store(200, Ordering::SeqCst);
    let holder = pg.start(
        c.clone(),
        &probe,
        Pg::options(CatchUp::Auto, interval, RunHooks::default()),
    );
    until(Duration::from_secs(5), "the holder runs", || {
        pg.lock_holders()
            .iter()
            .any(|(_, n)| n == "merezha p:count:1")
    });
    let other = pg.start(
        c,
        &probe,
        Pg::options(CatchUp::Auto, interval, RunHooks::default()),
    );
    let contended = || -> u64 {
        other
            .store
            .contended_locks()
            .iter()
            .filter(|(name, ..)| name == "count")
            .map(|(_, _, live, _)| *live)
            .sum()
    };
    let started = Instant::now();
    // 100 local commits over about 2 s, each a wake of the lane.
    for i in 0..100 {
        other.app.command(add(&format!("o{}", i % 4), 1)).unwrap();
        std::thread::sleep(Duration::from_millis(20));
    }
    let elapsed = started.elapsed();
    let attempts = contended();
    // One attempt at start plus at most one per second of commits.
    let bound = 2 + elapsed.as_secs();
    eprintln!("{attempts} contended attempts during {elapsed:?} of commits");
    assert!(attempts <= bound, "{attempts} lock attempts in {elapsed:?}");
    pg.stop(&holder);
    pg.stop(&other);
}

/// Review finding 3: events up to the revision's `warm_position` reach the
/// guest with `live = false`, later ones with `live = true`, and no batch
/// mixes the two, even when the warm position splits a batch.
#[test]
fn events_after_the_warm_position_are_delivered_live() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let writer = pg
        .open(config(), &probe, BackgroundOptions::default())
        .unwrap();
    fill(&writer, 7, 1);
    let mut c = config_with(2, "");
    c.limits.batch_size = 4;
    // The new revision's row warms from position 7; five more events
    // follow before its scheduler starts, so one batch would span it.
    let mut instance = pg
        .open(
            c,
            &probe,
            Pg::options(CatchUp::Auto, Duration::from_secs(600), RunHooks::default()),
        )
        .unwrap();
    fill(&writer, 5, 1);
    {
        let _guard = pg.runtime.enter();
        instance.scheduler = Some(Scheduler::start(&instance.app).unwrap());
    }
    // Queries fail `projection_warming` until the warm-up is done.
    until(Duration::from_secs(10), "caught up", || {
        instance
            .app
            .query(query("Count", None))
            .is_ok_and(|result| result.checkpoint == 12)
    });
    let lives = probe.lives.lock().unwrap().clone();
    for (positions, live) in &lives {
        assert!(
            positions.iter().all(|p| (*p > 7) == *live),
            "{positions:?} live={live}: {lives:?}"
        );
    }
    let delivered: Vec<u64> = lives.iter().flat_map(|(p, _)| p.clone()).collect();
    assert_eq!(delivered, (1..=12).collect::<Vec<_>>());
    pg.stop(&instance);
}

/// Review finding 4: an explicit catch-up whose lock session dies well
/// before its deadline fails with the session's failure, not `timeout`.
#[test]
fn an_explicit_catch_up_losing_its_session_early_reports_the_real_failure() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let mut c = config_with(1, "[limits]\ntimeout_ms=20000");
    c.limits.batch_size = 1;
    let options = Pg::options(
        CatchUp::Manual,
        Duration::from_secs(600),
        RunHooks::default(),
    );
    let instance = pg.open(c, &probe, options).unwrap();
    fill(&instance, 10, 1);
    probe.delay_ms.store(300, Ordering::SeqCst);
    let started = Instant::now();
    let result = std::thread::scope(|scope| {
        let running = scope.spawn(|| instance.app.catch_up("count"));
        until(
            Duration::from_secs(5),
            "the catch-up holds the lock",
            || {
                pg.lock_holders()
                    .iter()
                    .any(|(_, n)| n == "merezha p:count:1")
            },
        );
        pg.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE application_name = 'merezha p:count:1' AND datname = current_database()",
        );
        running.join().unwrap()
    });
    assert_eq!(code(result), "database_unavailable");
    assert!(started.elapsed() < Duration::from_secs(15));
}

/// Review finding 5: a run that took its slot just as the scheduler shuts
/// down sees the cancellation at its first step and publishes nothing.
#[test]
fn a_run_starting_during_shutdown_is_cancelled_before_it_publishes() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let writer = pg
        .open(config(), &probe, BackgroundOptions::default())
        .unwrap();
    fill(&writer, 4, 1);
    let pause = Arc::new(Pause::default());
    pause.arm("count", Duration::from_millis(800));
    let hooks = RunHooks {
        before_run: Some(pause.clone()),
        ..RunHooks::default()
    };
    let instance = pg.start(
        config(),
        &probe,
        Pg::options(CatchUp::Auto, Duration::from_secs(600), hooks),
    );
    until(Duration::from_secs(5), "a run holds its slot", || {
        pause.entered()
    });
    pg.stop(&instance);
    assert_eq!(pg.checkpoint("count"), 0, "the cancelled run published");
    assert!(
        probe.calls("count").is_empty(),
        "the cancelled run called the guest"
    );
    pg.assert_no_pooled_lock();
}

/// Review finding 6: a `LISTEN` reconnect wakes every live lane, since
/// notifications may have been lost while disconnected.
#[test]
fn a_listen_reconnect_wakes_every_live_lane() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let instance = pg.start(
        config(),
        &probe,
        Pg::options(CatchUp::Auto, Duration::from_secs(600), RunHooks::default()),
    );
    until(Duration::from_secs(5), "the listener connects", || {
        instance.app.status().unwrap().listener == Some("connected")
    });
    // An event committed without a notification, as one lost meanwhile.
    pg.execute(
        "BEGIN; \
         UPDATE merezha_app.head SET position = 1 WHERE id = 1; \
         INSERT INTO merezha_app.streams (stream, version, first_position, last_position) \
              VALUES ('Counter/silent', 1, 1, 1); \
         INSERT INTO merezha_app.events (position, stream, version, tag, schema_version, payload, \
                                         metadata, accepted_unix_ms) \
              VALUES (1, 'Counter/silent', 1, 'Added', 1, '{}', \
                      '{\"message_id\":\"m\",\"correlation_id\":\"c\"}', 0); \
         COMMIT",
    );
    std::thread::sleep(Duration::from_millis(1000));
    assert_eq!(pg.checkpoint("count"), 0, "nothing woke the lanes yet");
    pg.execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE query LIKE 'LISTEN merezha_app_events%' AND datname = current_database()",
    );
    until(
        Duration::from_secs(3),
        "the reconnect wakes both lanes",
        || pg.checkpoint("count") == 1 && pg.checkpoint("other") == 1,
    );
    pg.stop(&instance);
}

/// Latency and deadline-race tests (issue #37). CI runs them after the
/// parallel suite, alone and one at a time:
/// `cargo test -- --test-threads=1 timing::`.
mod timing {
    use super::*;
    /// A3 test 1: without explicit catch-up, projections converge within 1 s
    /// through `NOTIFY`, and within the interval without the `LISTEN` session.
    #[test]
    fn projections_converge_within_a_second_by_notify_and_within_the_interval_without_listen() {
        let Some(pg) = Pg::new() else { return };
        let probe = Arc::new(Probe::default());
        // The writer runs no scheduler, so only the notification wakes the
        // reader (its interval is 10 minutes).
        let writer = pg
            .open(config(), &probe, BackgroundOptions::default())
            .unwrap();
        let never = Duration::from_secs(600);
        let reader = pg.start(
            config(),
            &probe,
            Pg::options(CatchUp::Auto, never, RunHooks::default()),
        );
        until(Duration::from_secs(5), "the listener connects", || {
            reader.app.status().unwrap().listener == Some("connected")
        });
        for round in 1..=3u64 {
            writer.app.command(add("a", 1)).unwrap();
            let took = until(Duration::from_secs(1), "notify convergence", || {
                pg.checkpoint("count") == round
            });
            eprintln!("round {round}: converged in {took:?}");
        }
        // Kill the LISTEN session: the listener reconnects, wakes every lane
        // and converges again.
        pg.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE query LIKE 'LISTEN merezha_app_events%' AND datname = current_database()",
        );
        writer.app.command(add("a", 1)).unwrap();
        until(
            Duration::from_secs(1),
            "convergence after a reconnect",
            || pg.checkpoint("count") == 4,
        );
        until(Duration::from_secs(5), "the listener reconnects", || {
            reader.app.status().unwrap().listener == Some("connected")
        });
        pg.stop(&reader);
        // Without a LISTEN session only the interval wakes another host's runs.
        let hooks = RunHooks {
            skip_listen: true,
            ..RunHooks::default()
        };
        let deaf = pg.start(
            config(),
            &probe,
            Pg::options(CatchUp::Auto, never, hooks.clone()),
        );
        // Its lanes check once at start; ready means they all did.
        until(
            Duration::from_secs(5),
            "the deaf instance's start-up check",
            || deaf.scheduler.as_ref().unwrap().ready(),
        );
        writer.app.command(add("a", 1)).unwrap();
        std::thread::sleep(Duration::from_millis(1500));
        assert_eq!(pg.checkpoint("count"), 4, "nothing woke the deaf instance");
        pg.stop(&deaf);
        let polling = pg.start(
            config(),
            &probe,
            Pg::options(CatchUp::Auto, Duration::from_millis(500), hooks),
        );
        writer.app.command(add("a", 1)).unwrap();
        until(Duration::from_millis(1500), "interval convergence", || {
            pg.checkpoint("count") == 6
        });
        pg.stop(&polling);
    }
}
