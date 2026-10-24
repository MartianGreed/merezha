//! PostgreSQL application storage against a real server (AP §E, A2 tests).
//! Every test creates its own database through `MEREZHA_TEST_DATABASE_URL`
//! and skips with the kernel's message when it is unset. Each instance has
//! its own pool, as another host would.
use merezha_application::{
    AllowAll, Application, Guest, GuestSession,
    config::{Config, StorageConfig},
    postgres::{
        MIGRATIONS, MODULE, PostgresOptions, PostgresStore, TraceContext, TraceSource, verify,
    },
};
use merezha_domain::{
    Command, Event, Failure, Identity, Metadata, Operation, Query, Receipt, Reply, StoredEvent,
};
use merezha_postgres::{
    Database, MigrationMode, ModuleMigrations, PooledClient, TestDatabase, run_migrations,
    tokio_postgres::Row,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Barrier, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
const ROUTES: &str = r#"
[[commands]]
tag="Add"
service="business"
aggregate="Counter"
[[queries]]
tag="Count"
service="view"
projection="count"
[[projections]]
name="count"
service="view"
initial=0
"#;
fn config() -> Config {
    Config::parse(&format!(
        "version=1\n[storage]\nbackend=\"postgres\"\napplication=\"pg-test\"\n{ROUTES}"
    ))
    .unwrap()
}
fn metadata() -> Metadata {
    Metadata {
        message_id: id("message-1"),
        correlation_id: id("correlation-1"),
        causation_id: None,
        actor: Some(id("alice")),
    }
}
fn command(stream: &str, payload: Value, key: Option<&str>) -> Command {
    Command {
        tag: id("Add"),
        stream: id(stream),
        payload,
        metadata: metadata(),
        idempotency_key: key.map(id),
        expected_version: None,
    }
}
/// Slack on top of a deadline in elapsed-time checks. A loaded CI runner
/// can stall a thread well past a deadline; the outcome codes and the
/// committed rows prove the bound, the elapsed time only that nothing
/// waited on something unbounded.
const SLACK: Duration = Duration::from_secs(3);
fn code<T>(result: Result<T, Failure>) -> String {
    result.err().expect("expected a failure").code
}

/// A one-shot rendezvous of `n` callers with a deadline.
struct Gate {
    arrived: Mutex<usize>,
    changed: Condvar,
    n: usize,
}
impl Gate {
    fn new(n: usize) -> Self {
        Self {
            arrived: Mutex::new(0),
            changed: Condvar::new(),
            n,
        }
    }
    fn wait(&self) {
        let mut arrived = self.arrived.lock().unwrap();
        *arrived += 1;
        self.changed.notify_all();
        let (arrived, _) = self
            .changed
            .wait_timeout_while(arrived, Duration::from_secs(10), |a| *a < self.n)
            .unwrap();
        assert!(*arrived >= self.n, "the gate was never filled");
    }
}
/// A decision held until released, for the next decide call.
struct Hold {
    arrived: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}

/// The guest. `decide` turns `{"events":[p, ...]}` into one event per
/// payload, `{"pad":n}` into one event with an `n`-byte string and any other
/// payload into one event carrying it. Projections count events and record
/// what they saw; every operation's JSON is kept when `record` is set.
#[derive(Default)]
struct Probe {
    decisions: AtomicUsize,
    gate: Mutex<Option<Arc<Gate>>>,
    hold: Mutex<Option<Hold>>,
    seen: Mutex<Vec<StoredEvent>>,
    record: AtomicBool,
    inputs: Mutex<Vec<String>>,
}
impl Probe {
    fn hold(&self) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (arrived, arrived_rx) = mpsc::channel();
        let (release_tx, release) = mpsc::channel();
        *self.hold.lock().unwrap() = Some(Hold { arrived, release });
        (arrived_rx, release_tx)
    }
}
#[derive(Clone)]
struct Fake(Arc<Probe>);
fn guest(probe: &Arc<Probe>) -> Arc<dyn Guest> {
    Arc::new(Fake(probe.clone()))
}
impl Guest for Fake {
    fn services(&self) -> Result<Vec<String>, Failure> {
        Ok(vec!["business".into(), "view".into()])
    }
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
        Ok(Box::new(self.clone()))
    }
}
fn added(payload: Value) -> Event {
    Event {
        tag: id("Added"),
        schema_version: 1,
        payload,
    }
}
impl GuestSession for Fake {
    fn invoke(&self, _: &str, operation: Operation) -> Result<Reply, Failure> {
        if self.0.record.load(Ordering::SeqCst) {
            self.0
                .inputs
                .lock()
                .unwrap()
                .push(serde_json::to_string(&operation).unwrap());
        }
        match operation {
            Operation::Validate { payload, .. } => Ok(Reply::Validated { payload }),
            Operation::Decide { payload, .. } => {
                self.0.decisions.fetch_add(1, Ordering::SeqCst);
                let gate = self.0.gate.lock().unwrap().clone();
                if let Some(gate) = gate {
                    gate.wait();
                }
                let hold = self.0.hold.lock().unwrap().take();
                if let Some(hold) = hold {
                    hold.arrived.send(()).unwrap();
                    hold.release.recv_timeout(Duration::from_secs(10)).unwrap();
                }
                let events = if let Some(events) = payload["events"].as_array() {
                    events.iter().cloned().map(added).collect()
                } else if let Some(n) = payload["pad"].as_u64() {
                    vec![added(json!({"pad": "x".repeat(n as usize)}))]
                } else {
                    vec![added(payload)]
                };
                Ok(Reply::Decided { events })
            }
            Operation::Project { state, events, .. } => {
                let count = state.as_u64().unwrap() + events.len() as u64;
                self.0.seen.lock().unwrap().extend(events);
                Ok(Reply::Projected {
                    state: json!(count),
                })
            }
            // `{"len":true}` asks for the state's encoded size instead.
            Operation::Query { state, payload, .. } if payload["len"] == true => {
                Ok(Reply::Queried {
                    value: json!(state.to_string().len()),
                })
            }
            Operation::Query { state, .. } => Ok(Reply::Queried { value: state }),
        }
    }
}

/// A fresh migrated database and the runtime the stores block on.
struct Pg {
    runtime: tokio::runtime::Runtime,
    /// Connections per instance pool: small, so parallel tests stay within
    /// the server's default `max_connections`.
    pool_size: AtomicUsize,
    admin: Database,
    test: TestDatabase,
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
            pool_size: AtomicUsize::new(4),
            admin,
            test,
        })
    }
    fn pool(&self, size: usize) -> Database {
        let mut config = self.test.config();
        config.pool_size = size;
        Database::connect(&config).unwrap()
    }
    fn store(&self, config: &Config, options: PostgresOptions) -> Result<PostgresStore, Failure> {
        let db = self.pool(self.pool_size.load(Ordering::SeqCst));
        self.runtime
            .block_on(PostgresStore::connect(&db, config, options, deadline()))
    }
    fn open_with(
        &self,
        config: Config,
        probe: &Arc<Probe>,
        options: PostgresOptions,
    ) -> Result<Application, Failure> {
        let store = self.store(&config, options)?;
        Application::with_postgres(config, guest(probe), Arc::new(AllowAll), Arc::new(store))
    }
    fn open(&self, config: Config, probe: &Arc<Probe>) -> Application {
        self.open_with(config, probe, PostgresOptions::default())
            .unwrap()
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
    /// A session holding `merezha_app.head FOR UPDATE` until it rolls back.
    /// A session that ran `statements` and stays open, as another host
    /// mid-transaction.
    fn begin(&self, statements: &str) -> PooledClient {
        self.runtime.block_on(async {
            let client = self.admin.acquire(deadline()).await.unwrap();
            client.batch_execute(statements).await.unwrap();
            client
        })
    }
    fn finish(&self, client: PooledClient, statements: &str) {
        self.runtime
            .block_on(client.batch_execute(statements))
            .unwrap();
    }
    /// A session holding `merezha_app.head FOR UPDATE` until it rolls back.
    fn hold_head(&self) -> PooledClient {
        self.begin("BEGIN; SELECT position FROM merezha_app.head WHERE id = 1 FOR UPDATE")
    }
    fn release_head(&self, client: PooledClient) {
        self.finish(client, "ROLLBACK");
    }
    /// Wait until `n` appends wait on a lock.
    fn wait_for_lock_waiters(&self, n: i64) {
        self.wait_for_waiters("merezha_app.append", n);
    }
    /// Wait until `n` sessions running a statement containing `text` wait
    /// on a lock.
    fn wait_for_waiters(&self, text: &str, n: i64) {
        let until = Instant::now() + Duration::from_secs(10);
        let statement = format!(
            "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' \
             AND query LIKE '%{text}%' AND datname = current_database()"
        );
        while self.count(&statement) < n {
            assert!(Instant::now() < until, "no session waited on {text}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn verify(&self) -> merezha_application::postgres::VerifyReport {
        self.runtime
            .block_on(verify(&self.admin, deadline()))
            .unwrap()
    }
}

/// Every receipt names rows that exist with its stream and versions.
fn assert_receipts_match_rows(pg: &Pg, receipts: &[Receipt]) {
    let rows: BTreeMap<i64, (String, i64)> = pg
        .rows("SELECT position, stream, version FROM merezha_app.events")
        .iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2))))
        .collect();
    for receipt in receipts {
        for i in 0..receipt.events as u64 {
            let position = (receipt.position - i) as i64;
            assert_eq!(
                rows.get(&position),
                Some(&(
                    receipt.stream.as_str().to_owned(),
                    (receipt.version - i) as i64
                )),
                "receipt {receipt:?}"
            );
        }
    }
}

#[test]
fn two_instances_racing_on_one_stream_commit_exactly_one_write_per_expected_version() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let instances = [pg.open(config(), &probe), pg.open(config(), &probe)];
    const THREADS: usize = 8;
    const ROUNDS: u64 = 20;
    let barrier = Arc::new(Barrier::new(THREADS));
    let results: Vec<Vec<Result<Receipt, Failure>>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let app = instances[t % 2].clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    (0..ROUNDS)
                        .map(|version| {
                            barrier.wait();
                            let mut c = command("race", json!({"t": t}), None);
                            c.expected_version = Some(version);
                            let result = app.command(c);
                            barrier.wait();
                            result
                        })
                        .collect()
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    for round in 0..ROUNDS as usize {
        let outcomes: Vec<_> = results.iter().map(|r| &r[round]).collect();
        let winners: Vec<_> = outcomes.iter().filter_map(|r| r.as_ref().ok()).collect();
        assert_eq!(winners.len(), 1, "round {round}: {outcomes:?}");
        assert_eq!(winners[0].version, round as u64 + 1);
        assert_eq!(winners[0].position, round as u64 + 1);
        for loser in outcomes.iter().filter_map(|r| r.as_ref().err()) {
            assert_eq!(loser.code, "concurrency_conflict", "round {round}");
        }
    }
    assert!(pg.verify().ok);
}

#[test]
fn positions_stay_contiguous_under_load_from_two_instances_and_a_reader_never_sees_a_gap() {
    let Some(pg) = Pg::new() else { return };
    const THREADS: usize = 16;
    const STREAMS: usize = 64;
    let load = Duration::from_secs(10);
    pg.pool_size.store(THREADS, Ordering::SeqCst);
    let writers = Arc::new(Probe::default());
    let reader = Arc::new(Probe::default());
    let instances = [pg.open(config(), &writers), pg.open(config(), &writers)];
    let view = pg.open(config(), &reader);
    let stop = AtomicBool::new(false);
    // Under a loaded machine (shared CI runners) a command may run out of
    // its deadline. Such outcomes are counted, never ignored silently: the
    // invariants below hold whatever they were, and enough commits must
    // still succeed for the load to mean anything.
    let busy = AtomicUsize::new(0);
    let unknown = AtomicUsize::new(0);
    let read_timeouts = AtomicUsize::new(0);
    let (receipts, reads) = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..2 * THREADS)
            .map(|t| {
                let app = instances[t % 2].clone();
                let stop = &stop;
                let (busy, unknown) = (&busy, &unknown);
                scope.spawn(move || {
                    let mut receipts = Vec::new();
                    let mut n = 0usize;
                    while !stop.load(Ordering::SeqCst) {
                        n += 1;
                        let stream = format!("s{}", (t * 7 + n) % STREAMS);
                        let key = format!("t{t}-{n}");
                        let keyed = n.is_multiple_of(2);
                        match app.command(command(
                            &stream,
                            json!({"t": t, "n": n}),
                            keyed.then_some(key.as_str()),
                        )) {
                            Ok(receipt) => receipts.push(receipt),
                            // May have committed: bounds the head below.
                            Err(f) if f.code == "commit_outcome_unknown" => {
                                unknown.fetch_add(1, Ordering::SeqCst);
                            }
                            // Definitely not committed.
                            Err(f)
                                if matches!(
                                    f.code.as_str(),
                                    "timeout" | "storage_busy" | "database_busy"
                                ) =>
                            {
                                busy.fetch_add(1, Ordering::SeqCst);
                            }
                            Err(f) => assert_eq!(f.code, "concurrency_conflict", "{f:?}"),
                        }
                    }
                    receipts
                })
            })
            .collect();
        let view = &view;
        let stop_ref = &stop;
        let read_timeouts = &read_timeouts;
        let walker = scope.spawn(move || {
            let mut reads = 0;
            while !stop_ref.load(Ordering::SeqCst) {
                match view.catch_up("count") {
                    Ok(_) => reads += 1,
                    Err(f) => {
                        assert_eq!(f.code, "timeout", "{f:?}");
                        read_timeouts.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
            reads
        });
        std::thread::sleep(load);
        stop.store(true, Ordering::SeqCst);
        let receipts: Vec<Receipt> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        (receipts, walker.join().unwrap())
    });
    let caught_up = view.catch_up("count").unwrap();
    let head = pg.count("SELECT position FROM merezha_app.head") as u64;
    assert_eq!(caught_up.checkpoint, head);
    let (busy, unknown) = (busy.into_inner(), unknown.into_inner());
    // One event per accepted command, plus at most one per command whose
    // outcome is unknown.
    assert!(
        receipts.len() as u64 <= head && head <= (receipts.len() + unknown) as u64,
        "{} receipts, {unknown} unknown outcomes, head {head}",
        receipts.len()
    );
    assert!(
        receipts.len() >= 200,
        "only {} commits succeeded ({busy} timed out, {unknown} unknown)",
        receipts.len()
    );
    // The reader never saw a jump: every position it was handed is at most
    // one past the highest it had seen. A catch-up cut by its deadline may
    // hand a batch again, never skip one. It saw every position, and each
    // still exists with the same content.
    let seen = reader.seen.lock().unwrap().clone();
    let mut highest = 0;
    for event in &seen {
        assert!(
            event.position <= highest + 1,
            "the reader jumped from {highest} to {}",
            event.position
        );
        highest = highest.max(event.position);
    }
    let distinct: std::collections::BTreeSet<u64> = seen.iter().map(|e| e.position).collect();
    assert_eq!(distinct, (1..=head).collect());
    let read_timeouts = read_timeouts.into_inner();
    if read_timeouts == 0 {
        // No catch-up was cut, so none was handed a batch twice.
        assert_eq!(
            seen.iter().map(|e| e.position).collect::<Vec<_>>(),
            (1..=head).collect::<Vec<_>>()
        );
    }
    let now: BTreeMap<i64, (String, i64, String)> = pg
        .rows("SELECT position, stream, version, payload::text FROM merezha_app.events")
        .iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2), r.get(3))))
        .collect();
    for event in &seen {
        assert_eq!(
            now.get(&(event.position as i64)),
            Some(&(
                event.stream.as_str().to_owned(),
                event.version as i64,
                event.event.payload.to_string()
            ))
        );
    }
    assert_receipts_match_rows(&pg, &receipts);
    let report = pg.verify();
    assert!(report.ok, "{report:?}");
    assert_eq!(report.events, head);
    eprintln!(
        "load: {head} commits in {load:?} from {} writers, {reads} catch-ups; \
         {busy} commands timed out, {unknown} unknown, {} catch-ups timed out",
        2 * THREADS,
        read_timeouts
    );
}

#[test]
fn a_held_head_lock_fails_appends_storage_busy_and_queued_appends_commit_in_position_order() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let mut short = config();
    short.limits.timeout_ms = 1000;
    let app = pg.open(short, &probe);
    let holder = pg.hold_head();
    let started = Instant::now();
    assert_eq!(
        code(app.command(command("one", json!(1), None))),
        "storage_busy"
    );
    // `storage_busy` (not `timeout`) proves the server gave up first.
    let waited = started.elapsed();
    assert!(waited < Duration::from_millis(1000) + SLACK, "{waited:?}");
    // Readers are not blocked and see nothing new.
    assert_eq!(pg.count("SELECT count(*) FROM merezha_app.events"), 0);
    assert_eq!(pg.count("SELECT position FROM merezha_app.head"), 0);
    // Appends queued behind the holder commit after it, contiguously.
    let patient = pg.open(config(), &probe);
    let (receipts, observations) = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..4)
            .map(|i| {
                let app = patient.clone();
                scope.spawn(move || app.command(command(&format!("q{i}"), json!(i), None)))
            })
            .collect();
        pg.wait_for_lock_waiters(4);
        assert_eq!(pg.count("SELECT count(*) FROM merezha_app.events"), 0);
        let pg = &pg;
        // Commit order equals position order: every snapshot a reader takes
        // while the queue drains shows a prefix 1..=k of the positions.
        let reader = scope.spawn(move || {
            let mut observations = Vec::new();
            let until = Instant::now() + Duration::from_secs(10);
            loop {
                assert!(Instant::now() < until, "queued appends never all committed");
                let seen: Vec<i64> = pg
                    .rows("SELECT position FROM merezha_app.events ORDER BY position")
                    .iter()
                    .map(|r| r.get(0))
                    .collect();
                assert_eq!(seen, (1..=seen.len() as i64).collect::<Vec<_>>());
                let done = seen.len() == 4;
                observations.push(seen.len());
                if done {
                    return observations;
                }
            }
        });
        pg.release_head(holder);
        let receipts = handles
            .into_iter()
            .map(|h| h.join().unwrap().unwrap())
            .collect::<Vec<_>>();
        (receipts, reader.join().unwrap())
    });
    let mut positions: Vec<u64> = receipts.iter().map(|r| r.position).collect();
    positions.sort();
    assert_eq!(positions, vec![1, 2, 3, 4]);
    assert!(observations.windows(2).all(|w| w[0] <= w[1]));
    assert_receipts_match_rows(&pg, &receipts);
    assert!(pg.verify().ok);
}

#[test]
fn the_same_key_on_two_instances_at_once_appends_once_and_both_get_the_same_receipt() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let a = pg.open(config(), &probe);
    let b = pg.open(config(), &probe);
    *probe.gate.lock().unwrap() = Some(Arc::new(Gate::new(2)));
    let (first, second) = std::thread::scope(|scope| {
        let x = scope.spawn(|| a.command(command("one", json!(1), Some("same"))));
        let y = scope.spawn(|| b.command(command("one", json!(1), Some("same"))));
        (x.join().unwrap().unwrap(), y.join().unwrap().unwrap())
    });
    *probe.gate.lock().unwrap() = None;
    assert_eq!(first, second);
    assert_eq!((first.position, first.version, first.events), (1, 1, 1));
    assert_eq!(probe.decisions.load(Ordering::SeqCst), 2, "both decided");
    assert_eq!(pg.count("SELECT count(*) FROM merezha_app.events"), 1);
    assert_eq!(pg.count("SELECT count(*) FROM merezha_app.receipts"), 1);
    // Completed receipts come from the store on either instance: no
    // decision runs again, and a changed payload is refused.
    for app in [&a, &b] {
        assert_eq!(
            app.command(command("one", json!(1), Some("same"))).unwrap(),
            first
        );
        assert_eq!(
            code(app.command(command("one", json!(2), Some("same")))),
            "idempotency_mismatch"
        );
    }
    assert_eq!(probe.decisions.load(Ordering::SeqCst), 2);
}

#[test]
fn an_instance_with_a_full_pending_registry_still_replays_another_instances_receipt() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let mut small = config();
    // Set in code, so not a refused capacity key: one pending claim at most.
    small.limits.max_idempotency = 1;
    let a = pg.open(small.clone(), &probe);
    let b = pg.open(small, &probe);
    let committed = a.command(command("one", json!(1), Some("k"))).unwrap();
    // The registry holds pending claims only: a completed key frees its slot.
    assert_eq!(
        a.command(command("two", json!(1), Some("k2")))
            .unwrap()
            .position,
        2
    );
    let (arrived, release) = probe.hold();
    std::thread::scope(|scope| {
        let pending = scope.spawn(|| b.command(command("three", json!(1), Some("pending"))));
        arrived.recv_timeout(Duration::from_secs(10)).unwrap();
        // b's registry is full, yet a's committed key replays from the store.
        assert_eq!(
            b.command(command("one", json!(1), Some("k"))).unwrap(),
            committed
        );
        assert_eq!(
            code(b.command(command("four", json!(1), Some("new")))),
            "idempotency_capacity"
        );
        release.send(()).unwrap();
        assert_eq!(pending.join().unwrap().unwrap().position, 3);
    });
}

/// Explicit catch-ups on two instances take the run lock (AP-A3): one runs,
/// the other waits for it, so every batch is applied once and no catch-up
/// loses a CAS.
#[test]
fn concurrent_catch_ups_on_two_instances_apply_each_batch_once() {
    let Some(pg) = Pg::new() else { return };
    let writers = Arc::new(Probe::default());
    let views = [Arc::new(Probe::default()), Arc::new(Probe::default())];
    let mut c = config();
    c.limits.batch_size = 4;
    let writer = pg.open(c.clone(), &writers);
    let instances = [pg.open(c.clone(), &views[0]), pg.open(c, &views[1])];
    let stop = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let readers: Vec<_> = instances
            .iter()
            .map(|app| {
                let stop = &stop;
                scope.spawn(move || {
                    while !stop.load(Ordering::SeqCst) {
                        let pair = app.catch_up("count").unwrap();
                        assert!(pair.state == json!(pair.checkpoint));
                    }
                })
            })
            .collect();
        for i in 0..200 {
            writer
                .command(command(&format!("s{}", i % 8), json!(i), None))
                .unwrap();
        }
        stop.store(true, Ordering::SeqCst);
        for reader in readers {
            reader.join().unwrap();
        }
    });
    let pair = instances[0].catch_up("count").unwrap();
    assert_eq!(pair.state, json!(200));
    assert_eq!(pair.checkpoint, 200);
    let mut applied: Vec<u64> = views
        .iter()
        .flat_map(|view| view.seen.lock().unwrap().clone())
        .map(|event| event.position)
        .collect();
    applied.sort();
    assert_eq!(
        applied,
        (1..=200).collect::<Vec<_>>(),
        "each event applied once"
    );
    for app in &instances {
        let model = app
            .query(Query {
                tag: id("Count"),
                payload: json!({}),
                metadata: metadata(),
                min_position: None,
            })
            .unwrap();
        assert_eq!((model.value, model.checkpoint), (json!(200), 200));
    }
}

#[test]
fn terminating_the_append_backend_gives_commit_outcome_unknown_and_the_next_command_works() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let app = pg.open(config(), &probe);
    let holder = pg.hold_head();
    let result = std::thread::scope(|scope| {
        let append = scope.spawn(|| app.command(command("one", json!(1), Some("k"))));
        pg.wait_for_lock_waiters(1);
        pg.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE wait_event_type = 'Lock' AND query LIKE '%merezha_app.append%' \
             AND datname = current_database()",
        );
        append.join().unwrap()
    });
    pg.release_head(holder);
    assert_eq!(code(result), "commit_outcome_unknown");
    assert_eq!(pg.count("SELECT count(*) FROM merezha_app.events"), 0);
    // No poison and no restart: the keyed retry commits once.
    let receipt = app.command(command("one", json!(1), Some("k"))).unwrap();
    assert_eq!((receipt.position, receipt.version), (1, 1));
    assert_eq!(
        app.command(command("one", json!(1), Some("k"))).unwrap(),
        receipt
    );
}

#[test]
fn payload_batch_and_capacity_bounds_hold_on_postgres() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let mut c = config();
    c.limits.max_payload_bytes = 64;
    c.limits.batch_size = 2;
    let app = pg.open(c, &probe);
    assert_eq!(
        code(app.command(command("one", json!({"pad": 80}), Some("big")))),
        "payload_limit"
    );
    assert_eq!(
        code(app.command(command("one", json!({"events": [1, 2, 3]}), Some("many")))),
        "event_batch_limit"
    );
    assert_eq!(
        app.command(command("one", json!({"events": [1, 2]}), None))
            .unwrap()
            .events,
        2
    );
    assert_eq!(pg.count("SELECT count(*) FROM merezha_app.receipts"), 0);
    // Total-capacity keys are an operator disk concern on PostgreSQL.
    for key in [
        "max_events",
        "max_streams",
        "max_store_bytes",
        "max_idempotency",
    ] {
        let text = format!(
            "version=1\n[limits]\n{key}=10\n[storage]\nbackend=\"postgres\"\napplication=\"pg-test\"\n{ROUTES}"
        );
        assert_eq!(code(Config::parse(&text)), "invalid_limits", "{key}");
    }
    let text = format!(
        "version=1\n[storage]\nbackend=\"postgres\"\napplication=\"a\"\ninitialize=true\n{ROUTES}"
    );
    assert_eq!(code(Config::parse(&text)), "invalid_config");
}

#[test]
fn json_documents_round_trip_to_the_events_sqlite_returns() {
    let Some(pg) = Pg::new() else { return };
    let payloads = [
        json!({"text": "nul \u{0000} inside", "escape": "\"\\/\n"}),
        json!({"float": 1.0, "int": 1, "negative": -0.5}),
        json!({"z": 1, "a": {"y": [3, 2, 1], "b": null}}),
        json!({"max": 9_007_199_254_740_991_i64, "min": -9_007_199_254_740_991_i64}),
        json!("plain string"),
        json!([2.5, true, false]),
    ];
    let run = |app: &Application, probe: &Arc<Probe>| {
        for (i, payload) in payloads.iter().enumerate() {
            app.command(command(
                &format!("s{i}"),
                json!({"events": [payload]}),
                None,
            ))
            .unwrap();
        }
        app.catch_up("count").unwrap();
        let mut seen = probe.seen.lock().unwrap().clone();
        for event in &mut seen {
            event.accepted_unix_ms = 0;
        }
        seen
    };
    let postgres = Arc::new(Probe::default());
    let from_postgres = run(&pg.open(config(), &postgres), &postgres);
    let dir = tempfile::tempdir().unwrap();
    let mut sqlite_config = config();
    sqlite_config.storage = StorageConfig::Sqlite {
        path: dir.path().join("round-trip.db"),
        application: id("pg-test"),
        initialize: true,
    };
    let sqlite = Arc::new(Probe::default());
    let app = Application::open(sqlite_config, guest(&sqlite), Arc::new(AllowAll)).unwrap();
    let from_sqlite = run(&app, &sqlite);
    assert_eq!(from_postgres, from_sqlite);
    assert_eq!(
        from_postgres
            .iter()
            .map(|e| &e.event.payload)
            .collect::<Vec<_>>(),
        payloads.iter().collect::<Vec<_>>()
    );
    // The column keeps the exact text the host wrote.
    let stored: Vec<String> = pg
        .rows("SELECT payload::text FROM merezha_app.events ORDER BY position")
        .iter()
        .map(|r| r.get(0))
        .collect();
    let written: Vec<String> = payloads
        .iter()
        .map(|p| serde_json::to_string(p).unwrap())
        .collect();
    assert_eq!(stored, written);
    assert!(stored[0].contains("\\u0000") && stored[1].contains("1.0"));
}

struct FixedTrace(Option<TraceContext>);
impl TraceSource for FixedTrace {
    fn current(&self) -> Option<TraceContext> {
        self.0.clone()
    }
}
#[test]
fn events_carry_the_command_trace_context_and_guests_never_see_it() {
    let Some(pg) = Pg::new() else { return };
    let parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let state = "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE";
    let context = TraceContext::new(parent, Some(state)).unwrap();
    for (bad, tracestate) in [
        ("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7", None),
        (
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            None,
        ),
        (
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            None,
        ),
        (
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            None,
        ),
        (parent, Some("x".repeat(513))),
        (parent, Some("tab\tinside".to_owned())),
    ] {
        assert!(
            TraceContext::new(bad, tracestate.as_deref()).is_none(),
            "{bad}"
        );
    }
    let probe = Arc::new(Probe::default());
    probe.record.store(true, Ordering::SeqCst);
    let traced = pg
        .open_with(
            config(),
            &probe,
            PostgresOptions {
                trace: Some(Arc::new(FixedTrace(Some(context)))),
                ..PostgresOptions::default()
            },
        )
        .unwrap();
    traced
        .command(command("one", json!({"events": [1, 2]}), None))
        .unwrap();
    let untraced = pg
        .open_with(
            config(),
            &probe,
            PostgresOptions {
                trace: Some(Arc::new(FixedTrace(None))),
                ..PostgresOptions::default()
            },
        )
        .unwrap();
    untraced.command(command("one", json!(3), None)).unwrap();
    // A later decision and a catch-up read the traced events back.
    traced.command(command("one", json!(4), None)).unwrap();
    traced.catch_up("count").unwrap();
    let columns: Vec<(Option<String>, Option<String>)> = pg
        .rows("SELECT trace_parent, trace_state FROM merezha_app.events ORDER BY position")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    let traced_pair = (Some(parent.to_owned()), Some(state.to_owned()));
    assert_eq!(
        columns,
        vec![
            traced_pair.clone(),
            traced_pair.clone(),
            (None, None),
            traced_pair
        ]
    );
    let inputs = probe.inputs.lock().unwrap();
    assert!(inputs.iter().any(|i| i.contains("\"history\"")));
    assert!(inputs.iter().any(|i| i.contains("\"events\":[{")));
    for input in inputs.iter() {
        assert!(
            !input.contains("4bf92f3577b34da6") && !input.contains("rojo"),
            "{input}"
        );
    }
    for event in probe.seen.lock().unwrap().iter() {
        let text = serde_json::to_string(event).unwrap();
        assert!(
            !text.contains("4bf92f3577b34da6") && !text.contains("trace"),
            "{text}"
        );
    }
}

#[test]
fn opening_checks_the_schema_the_manifest_and_the_projection_rows() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    drop(pg.open(config(), &probe));
    assert_eq!(
        pg.count("SELECT count(*) FROM merezha_app.manifest WHERE application = 'pg-test'"),
        1
    );
    // Two hosts with the same identity both open; another identity fails.
    let first = pg.open(config(), &probe);
    let second = pg.open(config(), &probe);
    first.command(command("one", json!(1), None)).unwrap();
    second.command(command("two", json!(1), None)).unwrap();
    let mut other = config();
    other.storage = StorageConfig::Postgres {
        application: id("someone-else"),
    };
    assert_eq!(
        code(pg.store(&other, PostgresOptions::default())),
        "storage_configuration"
    );
    // A new projection revision gets its own row, warm from the head.
    let mut revised = config();
    revised.projections[0].revision = 2;
    drop(pg.open(revised.clone(), &probe));
    assert_eq!(
        pg.rows("SELECT checkpoint, warm_position FROM merezha_app.projections WHERE revision = 2")
            .iter()
            .map(|r| (r.get::<_, i64>(0), r.get::<_, i64>(1)))
            .collect::<Vec<_>>(),
        vec![(0, 2)]
    );
    // Partitioning an existing revision needs a new revision.
    pg.execute(
        "UPDATE merezha_app.projections SET partitioned = true, state = NULL WHERE revision = 2",
    );
    assert_eq!(
        code(pg.store(&revised, PostgresOptions::default())),
        "storage_configuration"
    );
    // A store only serves the configuration it was connected for.
    let store = Arc::new(pg.store(&config(), PostgresOptions::default()).unwrap());
    assert_eq!(
        code(Application::with_postgres(
            revised,
            guest(&probe),
            Arc::new(AllowAll),
            store
        )),
        "storage_configuration"
    );
    // The synchronous opener has no pool.
    assert_eq!(
        code(Application::open(
            config(),
            guest(&probe),
            Arc::new(AllowAll)
        )),
        "storage_configuration"
    );
    let bad = PostgresOptions {
        lock_timeout_max: Duration::from_secs(31),
        ..PostgresOptions::default()
    };
    assert_eq!(code(pg.store(&config(), bad)), "storage_configuration");
}

#[test]
fn opening_needs_applied_migrations_and_a_multi_thread_runtime() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let db = Database::connect(&test.config()).unwrap();
    let opened = runtime.block_on(PostgresStore::connect(
        &db,
        &config(),
        PostgresOptions::default(),
        deadline(),
    ));
    assert_eq!(code(opened), "storage_schema");
    let current = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let opened = current.block_on(PostgresStore::connect(
        &db,
        &config(),
        PostgresOptions::default(),
        deadline(),
    ));
    assert_eq!(code(opened), "storage_configuration");
    assert_eq!(
        code(runtime.block_on(verify(&db, deadline()))),
        "storage_schema"
    );
}

#[test]
fn verify_reports_each_broken_invariant() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let app = pg.open(config(), &probe);
    for i in 0..3 {
        app.command(command(
            "one",
            json!({"events": [i, i]}),
            Some(&format!("k{i}")),
        ))
        .unwrap();
    }
    app.command(command("two", json!(1), None)).unwrap();
    app.catch_up("count").unwrap();
    let report = pg.verify();
    assert!(report.ok, "{report:?}");
    assert_eq!(
        (
            report.head,
            report.events,
            report.streams,
            report.receipts,
            report.projections
        ),
        (7, 7, 2, 3, 1)
    );
    // Simulate a bad restore.
    pg.execute(
        "DELETE FROM merezha_app.events WHERE position = 3; \
         UPDATE merezha_app.head SET position = 9; \
         UPDATE merezha_app.streams SET last_position = 8 WHERE stream = 'Counter/two'; \
         UPDATE merezha_app.receipts SET stream = 'Counter/two' WHERE key = 'k0'; \
         UPDATE merezha_app.receipts SET version = version + 1 WHERE key = 'k2'; \
         UPDATE merezha_app.projections SET checkpoint = 10",
    );
    let report = pg.verify();
    assert!(!report.ok);
    let checks: Vec<(&str, Value)> = report
        .violations
        .iter()
        .map(|v| (v.check, serde_json::to_value(&v.subject).unwrap()))
        .collect();
    for expected in [
        ("head_matches_events", json!("head")),
        ("positions_contiguous", json!({"position": 3})),
        (
            "stream_versions_contiguous",
            json!({"stream": "Counter/one"}),
        ),
        ("stream_matches_events", json!({"stream": "Counter/two"})),
        (
            "receipt_matches_events",
            json!({"receipt": {"tag": "Add", "tenant": "", "actor": "alice", "key": "k1"}}),
        ),
        // Events of another stream, and the right positions at wrong versions.
        (
            "receipt_matches_events",
            json!({"receipt": {"tag": "Add", "tenant": "", "actor": "alice", "key": "k0"}}),
        ),
        (
            "receipt_matches_events",
            json!({"receipt": {"tag": "Add", "tenant": "", "actor": "alice", "key": "k2"}}),
        ),
        (
            "checkpoint_within_head",
            json!({"projection": {"name": "count", "revision": 1}}),
        ),
    ] {
        assert!(
            checks.contains(&(expected.0, expected.1.clone())),
            "missing {expected:?} in {checks:?}"
        );
    }
    assert_eq!(report.violation_count as usize, report.violations.len());
}

/// The append of another host committing key `k` for `Counter/one` at
/// version 0, as `fingerprint` binds it, from a session that may hold locks.
fn foreign_append(fingerprint: &str) -> String {
    format!(
        "SELECT * FROM merezha_app.append('Counter/one', NULL, 0, ARRAY['Added'], ARRAY[1], \
         ARRAY['1']::text[]::json[], '{{\"message_id\":\"other\",\"correlation_id\":\"other\"}}'::text::json, \
         0, NULL, NULL, 'Add', '', 'alice', 'k', '{fingerprint}', 1000)"
    )
}

#[test]
fn a_key_committed_between_the_stream_check_and_the_receipt_lookup_replays() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let app = pg.open(config(), &probe);
    // Reads of `streams` block, so the append stops at its stream check
    // while another host commits the same key.
    let other = pg.begin("BEGIN; LOCK TABLE merezha_app.streams IN ACCESS EXCLUSIVE MODE");
    let replayed = std::thread::scope(|scope| {
        let command = scope.spawn(|| app.command(command("one", json!(1), Some("k"))));
        pg.wait_for_lock_waiters(1);
        pg.finish(
            other,
            &format!(
                "{}; COMMIT",
                foreign_append(r#"{"expected_version":null,"payload":1,"stream":"Counter/one"}"#)
            ),
        );
        command.join().unwrap()
    });
    let receipt = replayed.unwrap();
    assert_eq!(
        (receipt.position, receipt.version, receipt.events),
        (1, 1, 1)
    );
    assert_eq!(pg.count("SELECT count(*) FROM merezha_app.events"), 1);
}

#[test]
fn a_key_committed_between_the_receipt_fast_path_and_the_history_load_replays() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let app = pg.open(config(), &probe);
    // The fast path finds no receipt; the history load then waits while
    // another host commits the key, and sees a stale expected version.
    let other = pg.begin("BEGIN; LOCK TABLE merezha_app.events IN ACCESS EXCLUSIVE MODE");
    let replayed = std::thread::scope(|scope| {
        let command = scope.spawn(|| {
            let mut keyed = command("one", json!(1), Some("k"));
            keyed.expected_version = Some(0);
            app.command(keyed)
        });
        pg.wait_for_waiters("FROM merezha_app.events WHERE stream", 1);
        pg.finish(
            other,
            &format!(
                "{}; COMMIT",
                foreign_append(r#"{"expected_version":0,"payload":1,"stream":"Counter/one"}"#)
            ),
        );
        command.join().unwrap()
    });
    let receipt = replayed.unwrap();
    assert_eq!(
        (receipt.position, receipt.version, receipt.events),
        (1, 1, 1)
    );
    assert_eq!(probe.decisions.load(Ordering::SeqCst), 0);
}

#[test]
fn an_append_losing_its_key_to_a_zero_event_command_rolls_back_in_flight() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let app = pg.open(config(), &probe);
    // Receipt inserts block (reads do not), so the append holds the head
    // and its events when a zero-event command, which takes no head lock,
    // commits the same key.
    let other = pg.begin("BEGIN; LOCK TABLE merezha_app.receipts IN SHARE MODE");
    let lost = std::thread::scope(|scope| {
        let command = scope.spawn(|| app.command(command("one", json!(1), Some("k"))));
        pg.wait_for_lock_waiters(1);
        pg.finish(
            other,
            "SELECT * FROM merezha_app.append('Counter/one', NULL, 0, ARRAY[]::text[], \
             ARRAY[]::integer[], ARRAY[]::json[], '{}'::json, 0, NULL, NULL, 'Add', '', 'alice', 'k', \
             'zero', 1000); COMMIT",
        );
        command.join().unwrap()
    });
    assert_eq!(code(lost), "idempotency_in_flight");
    assert_eq!(pg.count("SELECT count(*) FROM merezha_app.events"), 0);
    assert_eq!(pg.count("SELECT position FROM merezha_app.head"), 0);
    assert_eq!(
        pg.count("SELECT count(*) FROM merezha_app.receipts WHERE events = 0"),
        1
    );
}

#[test]
fn a_slow_pool_acquire_still_bounds_the_lock_wait_by_the_time_left() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let mut c = config();
    c.limits.timeout_ms = 3000;
    let db = pg.pool(1);
    let store = pg
        .runtime
        .block_on(PostgresStore::connect(
            &db,
            &c,
            PostgresOptions::default(),
            deadline(),
        ))
        .unwrap();
    let app =
        Application::with_postgres(c, guest(&probe), Arc::new(AllowAll), Arc::new(store)).unwrap();
    let holder = pg.hold_head();
    let (arrived, release) = probe.hold();
    let started = Instant::now();
    let result = std::thread::scope(|scope| {
        let command = scope.spawn(|| app.command(command("one", json!(1), None)));
        arrived.recv_timeout(Duration::from_secs(10)).unwrap();
        // The decision is made; the only pooled session is busy for 1.5 s.
        let busy = pg.runtime.block_on(db.acquire(deadline())).unwrap();
        release.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(1500));
        drop(busy);
        command.join().unwrap()
    });
    // `storage_busy`, not `commit_outcome_unknown`: the lock wait was cut
    // from the time left after the slow acquire, so the server ended the
    // statement before the caller's 3 s deadline.
    assert_eq!(code(result), "storage_busy");
    let waited = started.elapsed();
    assert!(waited < Duration::from_millis(3000) + SLACK, "{waited:?}");
    pg.release_head(holder);
    // Nothing commits later behind the caller's back.
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(pg.count("SELECT count(*) FROM merezha_app.events"), 0);
    assert_eq!(pg.count("SELECT position FROM merezha_app.head"), 0);
}

#[test]
fn an_append_never_waits_past_the_database_statement_timeout() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let mut c = config();
    c.limits.timeout_ms = 30_000;
    let mut database = pg.test.config();
    database.statement_timeout = Duration::from_secs(1);
    let db = Database::connect(&database).unwrap();
    let store = pg
        .runtime
        .block_on(PostgresStore::connect(
            &db,
            &c,
            PostgresOptions::default(),
            deadline(),
        ))
        .unwrap();
    let app =
        Application::with_postgres(c, guest(&probe), Arc::new(AllowAll), Arc::new(store)).unwrap();
    let holder = pg.hold_head();
    let started = Instant::now();
    assert_eq!(
        code(app.command(command("one", json!(1), None))),
        "storage_busy"
    );
    // The 1 s database ceiling ended the wait, far before the 30 s
    // deadline; the lower bound shows the lock wait really ran.
    let waited = started.elapsed();
    assert!(waited < Duration::from_millis(1000) + SLACK, "{waited:?}");
    assert!(waited >= Duration::from_millis(900), "{waited:?}");
    pg.release_head(holder);
    // The pooled session is back at the operator's default and works.
    assert_eq!(
        app.command(command("one", json!(1), None))
            .unwrap()
            .position,
        1
    );
}

#[test]
fn lowering_payload_and_state_limits_keeps_stored_history_readable() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let mut wide = config();
    wide.limits.max_payload_bytes = 1024;
    let app = pg.open(wide, &probe);
    app.command(command("one", json!({"pad": 500}), None))
        .unwrap();
    drop(app);
    // A rolling deploy lowers both limits: stored rows stay readable, new
    // writes obey the new limits.
    let mut narrow = config();
    narrow.limits.max_payload_bytes = 128;
    narrow.limits.max_state_bytes = 128;
    let app = pg.open(narrow, &probe);
    assert_eq!(
        app.command(command("one", json!({"pad": 10}), None))
            .unwrap()
            .version,
        2
    );
    assert_eq!(
        code(app.command(command("one", json!({"pad": 500}), None))),
        "payload_limit"
    );
    assert_eq!(app.catch_up("count").unwrap().checkpoint, 2);
    pg.execute(&format!(
        "UPDATE merezha_app.projections SET state = '\"{}\"'",
        "x".repeat(500)
    ));
    let size = app
        .query(Query {
            tag: id("Count"),
            payload: json!({"len": true}),
            metadata: metadata(),
            min_position: None,
        })
        .unwrap();
    assert_eq!(size.value, json!(502));
}

#[test]
fn a_position_gap_in_the_feed_is_the_transient_event_feed_gap() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let app = pg.open(config(), &probe);
    for i in 0..3 {
        app.command(command("one", json!(i), None)).unwrap();
    }
    pg.execute("DELETE FROM merezha_app.events WHERE position = 2");
    let failure = app.catch_up("count").err().unwrap();
    assert_eq!(
        (failure.code.as_str(), failure.classification),
        ("event_feed_gap", merezha_domain::Classification::Transient)
    );
    assert_eq!(
        app.query(Query {
            tag: id("Count"),
            payload: json!({}),
            metadata: metadata(),
            min_position: None,
        })
        .unwrap()
        .checkpoint,
        0
    );
}

/// Recorded throughput for the PR description (§E, not gated).
#[test]
#[ignore = "measurement: cargo test --test postgres -- --ignored --nocapture"]
fn append_throughput_with_8_concurrent_single_event_writers() {
    let Some(pg) = Pg::new() else { return };
    let probe = Arc::new(Probe::default());
    let app = pg.open(config(), &probe);
    let run = Duration::from_secs(5);
    let stop = AtomicBool::new(false);
    let total: usize = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|t| {
                let app = app.clone();
                let stop = &stop;
                scope.spawn(move || {
                    let mut n = 0;
                    while !stop.load(Ordering::SeqCst) {
                        app.command(command(&format!("w{t}-{}", n % 64), json!(n), None))
                            .unwrap();
                        n += 1;
                    }
                    n
                })
            })
            .collect();
        std::thread::sleep(run);
        stop.store(true, Ordering::SeqCst);
        handles.into_iter().map(|h| h.join().unwrap()).sum()
    });
    eprintln!(
        "throughput: {total} commits in {run:?} = {:.0}/s with 8 single-event writers",
        total as f64 / run.as_secs_f64()
    );
}
