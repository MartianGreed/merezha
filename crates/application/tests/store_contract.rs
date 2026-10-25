//! The portable store contract (AP §A.10). Each `contract_*` function
//! asserts one behaviour every durable backend keeps, through the public
//! `Application` API, against a fixture that creates, reopens and
//! fault-injects one store. "Reopen" is a new `Application` over the same
//! data; an "independent instance" is a second live one, as another host.
//!
//! SQLite runs always (`sqlite::*`). PostgreSQL (`postgres::*`) runs over a
//! fresh `merezha_postgres::TestDatabase` per contract; its constructor
//! returns `None`, and each test returns early, with the kernel's explicit
//! skip message when `MEREZHA_TEST_DATABASE_URL` is unset.
use merezha_application::{
    AllowAll, Application, Authorizer, Guest, GuestSession, ProjectionState,
    config::{Config, StorageConfig},
};
use merezha_domain::{
    Command, Event, Failure, Identity, MessageKind, Metadata, Operation, Query, Receipt, Reply,
    validate_payload,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}
fn metadata(actor: &str) -> Metadata {
    Metadata {
        message_id: id("message-1"),
        correlation_id: id("correlation-1"),
        causation_id: Some(id("cause-1")),
        actor: Some(id(actor)),
        principal: None,
    }
}
fn command(amount: i64, key: Option<&str>) -> Command {
    Command {
        tag: id("Add"),
        stream: id("one"),
        payload: json!({"amount":amount}),
        metadata: metadata("alice"),
        idempotency_key: key.map(id),
        expected_version: None,
    }
}
fn query() -> Query {
    Query {
        tag: id("Total"),
        payload: json!({}),
        metadata: metadata("alice"),
        min_position: None,
    }
}
const CONFIG: &str = r#"
[[commands]]
tag="Add"
service="business"
aggregate="Counter"
[[queries]]
tag="Total"
service="view"
projection="total"
[[projections]]
name="total"
service="view"
initial=0
"#;
fn config() -> Config {
    Config::parse(&format!("version=1\n{CONFIG}")).unwrap()
}
fn pair(state: i64, checkpoint: u64) -> ProjectionState {
    ProjectionState {
        state: json!(state),
        checkpoint,
    }
}
fn allow() -> Arc<dyn Authorizer> {
    Arc::new(AllowAll)
}

/// Guest operations a test can hold mid-call, outside any SQL.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Op {
    Decide,
    Project,
}
/// A one-shot pause armed for the next call of `op`, whichever instance
/// makes it.
struct Pause {
    op: Op,
    arrived: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}
/// The held caller's side: wait for it to arrive, then release it.
struct Held {
    arrived: mpsc::Receiver<()>,
    release: mpsc::Sender<()>,
}
impl Held {
    fn arrived(&self) {
        self.arrived
            .recv_timeout(Duration::from_secs(5))
            .expect("the held call never arrived");
    }
    fn release(self) {
        self.release.send(()).unwrap();
    }
}
#[derive(Default)]
struct Control {
    decisions: AtomicUsize,
    fail: AtomicBool,
    no_events: AtomicBool,
    invalid_tail: AtomicBool,
    fail_projection: AtomicUsize,
    lives: Mutex<Vec<bool>>,
    /// `(version, position, event_id)` of every event projected.
    seen: Mutex<Vec<(u64, u64, String)>>,
    seen_metadata: Mutex<Vec<Metadata>>,
    pause: Mutex<Option<Pause>>,
}
impl Control {
    fn hold(&self, op: Op) -> Held {
        let (arrived_tx, arrived) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        *self.pause.lock().unwrap() = Some(Pause {
            op,
            arrived: arrived_tx,
            release: release_rx,
        });
        Held { arrived, release }
    }
    fn pause_at(&self, op: Op) {
        let taken = {
            let mut slot = self.pause.lock().unwrap();
            if slot.as_ref().is_some_and(|p| p.op == op) {
                slot.take()
            } else {
                None
            }
        };
        if let Some(pause) = taken {
            pause.arrived.send(()).unwrap();
            pause
                .release
                .recv_timeout(Duration::from_secs(5))
                .expect("the held call was never released");
        }
    }
    fn decisions(&self) -> usize {
        self.decisions.load(Ordering::SeqCst)
    }
}
#[derive(Clone)]
struct Fake(Arc<Control>);
fn guest(control: &Arc<Control>) -> Arc<dyn Guest> {
    Arc::new(Fake(control.clone()))
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Add {
    amount: i64,
    #[serde(default)]
    note: Option<String>,
    /// Further events of the same batch.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    then: Vec<i64>,
}
fn added(amount: i64) -> Event {
    Event {
        tag: id("Added"),
        schema_version: 1,
        payload: json!({"amount":amount}),
    }
}
impl Guest for Fake {
    fn services(&self) -> Result<Vec<String>, Failure> {
        Ok(vec!["business".into(), "view".into()])
    }
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
        Ok(Box::new(self.clone()))
    }
}
impl GuestSession for Fake {
    fn invoke(&self, _: &str, operation: Operation) -> Result<Reply, Failure> {
        match operation {
            Operation::Validate {
                kind: MessageKind::Command,
                payload,
                ..
            } => validate_payload::<Add>(payload),
            Operation::Validate { payload, .. } => Ok(Reply::Validated { payload }),
            Operation::Decide {
                payload, history, ..
            } => {
                self.0.decisions.fetch_add(1, Ordering::SeqCst);
                self.0.pause_at(Op::Decide);
                if self.0.fail.load(Ordering::SeqCst) {
                    return Err(Failure::permanent("invariant_violation"));
                }
                if self.0.no_events.load(Ordering::SeqCst) {
                    return Ok(Reply::Decided { events: Vec::new() });
                }
                let add: Add = serde_json::from_value(payload).unwrap();
                let total: i64 = history
                    .iter()
                    .map(|e| e.event.payload["amount"].as_i64().unwrap())
                    .sum();
                if total + add.amount < 0 {
                    return Err(Failure::permanent("invariant_violation"));
                }
                let mut events: Vec<_> = std::iter::once(add.amount)
                    .chain(add.then)
                    .map(added)
                    .collect();
                if self.0.invalid_tail.load(Ordering::SeqCst) {
                    events.push(Event {
                        schema_version: 0,
                        ..added(1)
                    });
                }
                Ok(Reply::Decided { events })
            }
            Operation::Project {
                state,
                events,
                live,
                ..
            } => {
                self.0.lives.lock().unwrap().push(live);
                self.0.pause_at(Op::Project);
                if events
                    .iter()
                    .any(|e| e.position == self.0.fail_projection.load(Ordering::SeqCst) as u64)
                {
                    return Err(Failure::transient("projection_failed"));
                }
                self.0.seen.lock().unwrap().extend(
                    events
                        .iter()
                        .map(|e| (e.version, e.position, e.event_id.as_str().to_owned())),
                );
                self.0
                    .seen_metadata
                    .lock()
                    .unwrap()
                    .extend(events.iter().map(|e| e.metadata.clone()));
                let added: i64 = events
                    .iter()
                    .map(|e| e.event.payload["amount"].as_i64().unwrap())
                    .sum();
                Ok(Reply::Projected {
                    state: json!(state.as_i64().unwrap() + added),
                })
            }
            Operation::Describe {} => unreachable!("a version 1 fake is never described"),
            Operation::Query { state, .. } => Ok(Reply::Queried { value: state }),
        }
    }
}
struct Deny;
impl Authorizer for Deny {
    fn authorize(
        &self,
        _: MessageKind,
        _: &str,
        _: Option<&str>,
        _: &Value,
        _: &Metadata,
    ) -> Result<(), Failure> {
        Err(Failure::permanent("unauthorized"))
    }
}

/// Faults injected inside the store's own transactions.
#[derive(Clone, Copy, Debug)]
enum Fault {
    /// Every receipt insert fails.
    ReceiptInsert,
    /// Every projection checkpoint update fails.
    ProjectionCommit,
}
/// One durable store under test. Each contract gets a fresh fixture.
trait StoreFixture {
    /// Open an application on a fresh, empty store.
    fn create(
        &self,
        config: Config,
        guest: Arc<dyn Guest>,
        authorizer: Arc<dyn Authorizer>,
    ) -> Result<Application, Failure>;
    /// Open a new application over the same durable data.
    fn reopen(
        &self,
        config: Config,
        guest: Arc<dyn Guest>,
        authorizer: Arc<dyn Authorizer>,
    ) -> Result<Application, Failure>;
    /// A second live instance over `app`'s data, with its own connections,
    /// pending claims and projection cache.
    fn independent(&self, app: &Application) -> Result<Application, Failure>;
    fn inject(&self, fault: Fault);
    fn clear(&self, fault: Fault);
    /// SQLite rebuilds synchronously; PostgreSQL runs the background
    /// rebuild to its swap within the request (AP-A3).
    fn rebuild(&self, app: &Application, name: &str) -> Result<ProjectionState, Failure> {
        app.rebuild(name)
    }
    /// Whether a racing second run must skip the run lock to race at all.
    /// PostgreSQL runs hold a session advisory lock, so a second locked
    /// run waits instead; the CAS that keeps the data correct without the
    /// lock is raced by an unlocked run, as one whose lock session died.
    fn locks_runs(&self) -> bool {
        false
    }
}
/// A racing catch-up or rebuild on `app` for the CAS contracts.
fn racing(app: &Application, unlocked: bool, rebuild: bool) -> Result<ProjectionState, Failure> {
    match (unlocked, rebuild) {
        (true, _) => app.project_unlocked("total", rebuild),
        (false, false) => app.catch_up("total"),
        (false, true) => app.rebuild("total"),
    }
}

/// One fresh database per contract, migrated once. Every `Application` gets
/// its own pool, so a reopen or an independent instance is another host.
struct Postgres {
    // Dropped before the database, so no pool outlives it.
    runtime: tokio::runtime::Runtime,
    test: merezha_postgres::TestDatabase,
    last: Mutex<Option<Opened>>,
}
/// What the last application was opened with.
type Opened = (Config, Arc<dyn Guest>, Arc<dyn Authorizer>);
impl Postgres {
    fn new() -> Option<Self> {
        use merezha_application::postgres::{MIGRATIONS, MODULE};
        use merezha_postgres::{Database, MigrationMode, ModuleMigrations, run_migrations};
        let test = merezha_postgres::TestDatabase::create()?;
        // The store blocks on its runtime from caller threads, which needs
        // a multi-thread runtime to keep driving I/O meanwhile.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let db = Database::connect(&test.config()).unwrap();
            let modules = [ModuleMigrations {
                module: MODULE,
                migrations: MIGRATIONS,
            }];
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            run_migrations(&db, &modules, MigrationMode::Apply, deadline)
                .await
                .unwrap();
            db.close();
        });
        Some(Self {
            runtime,
            test,
            last: Mutex::new(None),
        })
    }
    fn open(
        &self,
        mut config: Config,
        guest: Arc<dyn Guest>,
        authorizer: Arc<dyn Authorizer>,
    ) -> Result<Application, Failure> {
        use merezha_application::postgres::{PostgresOptions, PostgresStore};
        config.storage = StorageConfig::Postgres {
            application: id("contract-v1"),
        };
        let db = merezha_postgres::Database::connect(&self.test.config())?;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let store = self.runtime.block_on(PostgresStore::connect(
            &db,
            &config,
            PostgresOptions::default(),
            deadline,
        ))?;
        *self.last.lock().unwrap() = Some((config.clone(), guest.clone(), authorizer.clone()));
        Application::with_postgres(config, guest, authorizer, Arc::new(store))
    }
    fn sql(&self, statement: &str) {
        self.runtime.block_on(async {
            let db = merezha_postgres::Database::connect(&self.test.config()).unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let client = db.acquire(deadline).await.unwrap();
            client.batch_execute(statement).await.unwrap();
        });
    }
}
impl StoreFixture for Postgres {
    fn create(
        &self,
        config: Config,
        guest: Arc<dyn Guest>,
        authorizer: Arc<dyn Authorizer>,
    ) -> Result<Application, Failure> {
        self.open(config, guest, authorizer)
    }
    fn reopen(
        &self,
        config: Config,
        guest: Arc<dyn Guest>,
        authorizer: Arc<dyn Authorizer>,
    ) -> Result<Application, Failure> {
        self.open(config, guest, authorizer)
    }
    /// Another host: its own pool, pending claims and projection cache over
    /// the configuration `app` was opened with.
    fn independent(&self, _: &Application) -> Result<Application, Failure> {
        let (config, guest, authorizer) = self.last.lock().unwrap().clone().unwrap();
        self.open(config, guest, authorizer)
    }
    fn inject(&self, fault: Fault) {
        let target = match fault {
            Fault::ReceiptInsert => "BEFORE INSERT ON merezha_app.receipts",
            Fault::ProjectionCommit => "BEFORE UPDATE OF checkpoint ON merezha_app.projections",
        };
        self.sql(&format!(
            "CREATE FUNCTION contract_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected'; END $$; \
             CREATE TRIGGER contract_fault {target} FOR EACH ROW EXECUTE FUNCTION contract_fault()"
        ));
    }
    fn locks_runs(&self) -> bool {
        true
    }
    fn clear(&self, fault: Fault) {
        let table = match fault {
            Fault::ReceiptInsert => "merezha_app.receipts",
            Fault::ProjectionCommit => "merezha_app.projections",
        };
        self.sql(&format!(
            "DROP TRIGGER contract_fault ON {table}; DROP FUNCTION contract_fault()"
        ));
    }
}

struct Sqlite {
    _dir: tempfile::TempDir,
    path: PathBuf,
}
impl Sqlite {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("contract.db");
        Self { _dir: dir, path }
    }
    fn open(
        &self,
        mut config: Config,
        initialize: bool,
        guest: Arc<dyn Guest>,
        authorizer: Arc<dyn Authorizer>,
    ) -> Result<Application, Failure> {
        config.storage = StorageConfig::Sqlite {
            path: self.path.clone(),
            application: id("contract-v1"),
            initialize,
        };
        Application::open(config, guest, authorizer)
    }
    fn sql(&self, statement: &str) {
        rusqlite::Connection::open(&self.path)
            .unwrap()
            .execute_batch(statement)
            .unwrap();
    }
}
impl StoreFixture for Sqlite {
    fn create(
        &self,
        config: Config,
        guest: Arc<dyn Guest>,
        authorizer: Arc<dyn Authorizer>,
    ) -> Result<Application, Failure> {
        self.open(config, true, guest, authorizer)
    }
    fn reopen(
        &self,
        config: Config,
        guest: Arc<dyn Guest>,
        authorizer: Arc<dyn Authorizer>,
    ) -> Result<Application, Failure> {
        self.open(config, false, guest, authorizer)
    }
    fn independent(&self, app: &Application) -> Result<Application, Failure> {
        app.independent_instance()
    }
    fn inject(&self, fault: Fault) {
        self.sql(match fault {
            Fault::ReceiptInsert => {
                "CREATE TRIGGER contract_fault BEFORE INSERT ON receipts BEGIN SELECT RAISE(ABORT,'injected'); END"
            }
            Fault::ProjectionCommit => {
                "CREATE TRIGGER contract_fault BEFORE UPDATE OF checkpoint ON projections BEGIN SELECT RAISE(ABORT,'injected'); END"
            }
        });
    }
    fn clear(&self, _: Fault) {
        self.sql("DROP TRIGGER contract_fault");
    }
}

/// The checkpoint of an up-to-date catch-up equals the feed head.
fn head(app: &Application) -> u64 {
    app.catch_up("total").unwrap().checkpoint
}

fn contract_reopen_retains_receipt_and_projection(f: &impl StoreFixture) {
    let control = Arc::new(Control::default());
    let app = f.create(config(), guest(&control), allow()).unwrap();
    let receipt = app.command(command(3, Some("restart"))).unwrap();
    app.catch_up("total").unwrap();
    drop(app);
    let app = f.reopen(config(), guest(&control), allow()).unwrap();
    let model = app.query(query()).unwrap();
    assert_eq!((model.value, model.checkpoint), (json!(3), 1));
    assert_eq!(app.command(command(3, Some("restart"))).unwrap(), receipt);
    assert_eq!(control.decisions(), 1);
}

fn contract_keyed_replay_is_scoped_and_canonical_but_still_validates_and_authorizes(
    f: &impl StoreFixture,
) {
    let control = Arc::new(Control::default());
    let app = f.create(config(), guest(&control), allow()).unwrap();
    let first = app.command(command(3, Some("k"))).unwrap();
    let mut bob = command(3, Some("k"));
    bob.metadata.actor = Some(id("bob"));
    assert_eq!(app.command(bob.clone()).unwrap().version, 2);
    control.fail.store(true, Ordering::SeqCst);
    assert_eq!(
        app.command(command(2, Some("retry"))).unwrap_err().code,
        "invariant_violation"
    );
    control.fail.store(false, Ordering::SeqCst);
    assert_eq!(app.command(command(2, Some("retry"))).unwrap().version, 3);
    drop(app);
    let app = f.reopen(config(), guest(&control), allow()).unwrap();
    let mut equivalent = command(3, Some("k"));
    equivalent.payload = json!({"note":null,"amount":3});
    let before = control.decisions();
    assert_eq!(app.command(equivalent).unwrap(), first);
    assert_eq!(app.command(bob).unwrap().version, 2);
    assert_eq!(control.decisions(), before);
    for changed in [
        command(4, Some("k")),
        Command {
            stream: id("other"),
            ..command(3, Some("k"))
        },
        Command {
            expected_version: Some(0),
            ..command(3, Some("k"))
        },
    ] {
        assert_eq!(
            app.command(changed).unwrap_err().code,
            "idempotency_mismatch"
        );
    }
    let mut invalid = command(3, Some("k"));
    invalid.payload = json!({"amount":"bad"});
    assert_eq!(app.command(invalid).unwrap_err().code, "validation_failed");
    drop(app);
    let app = f.reopen(config(), guest(&control), Arc::new(Deny)).unwrap();
    assert_eq!(
        app.command(command(3, Some("k"))).unwrap_err().code,
        "unauthorized"
    );
}

fn contract_zero_event_receipts_survive_reopen(f: &impl StoreFixture) {
    let control = Arc::new(Control::default());
    control.no_events.store(true, Ordering::SeqCst);
    let app = f.create(config(), guest(&control), allow()).unwrap();
    let receipt = app.command(command(3, Some("zero"))).unwrap();
    assert_eq!(
        (receipt.version, receipt.position, receipt.events),
        (0, 0, 0)
    );
    drop(app);
    let app = f.reopen(config(), guest(&control), allow()).unwrap();
    control.no_events.store(false, Ordering::SeqCst);
    assert_eq!(app.command(command(3, Some("zero"))).unwrap(), receipt);
    assert_eq!(control.decisions(), 1);
    assert_eq!(app.command(command(3, None)).unwrap().version, 1);
}

fn contract_one_instance_shares_pending_keys_and_commits_one_of_two_racing_writes(
    f: &impl StoreFixture,
) {
    let control = Arc::new(Control::default());
    let app = f.create(config(), guest(&control), allow()).unwrap();
    let held = control.hold(Op::Decide);
    let worker = app.clone();
    let thread = std::thread::spawn(move || worker.command(command(1, Some("same"))));
    held.arrived();
    assert_eq!(
        app.command(command(1, Some("same"))).unwrap_err().code,
        "idempotency_in_flight"
    );
    assert_eq!(
        app.command(command(2, Some("same"))).unwrap_err().code,
        "idempotency_mismatch"
    );
    held.release();
    let receipt = thread.join().unwrap().unwrap();
    assert_eq!(app.command(command(1, Some("same"))).unwrap(), receipt);
    // Both decide on version 1; only the first commit lands.
    let held = control.hold(Op::Decide);
    let worker = app.clone();
    let thread = std::thread::spawn(move || worker.command(command(1, None)));
    held.arrived();
    let winner = app.command(command(1, None)).unwrap();
    held.release();
    assert_eq!(
        thread.join().unwrap().unwrap_err().code,
        "concurrency_conflict"
    );
    drop(app);
    let app = f.reopen(config(), guest(&control), allow()).unwrap();
    assert_eq!(app.command(command(1, Some("same"))).unwrap(), receipt);
    assert_eq!(control.decisions(), 3);
    assert_eq!(head(&app), winner.position);
}

fn contract_independent_instances_recheck_stream_key_and_projection_revision(
    f: &impl StoreFixture,
) {
    let control = Arc::new(Control::default());
    let a = f.create(config(), guest(&control), allow()).unwrap();
    let b = f.independent(&a).unwrap();
    // Stream: b decides on version 0, a commits version 1 first.
    let held = control.hold(Op::Decide);
    let other = b.clone();
    let thread = std::thread::spawn(move || other.command(command(1, None)));
    held.arrived();
    assert_eq!(a.command(command(1, None)).unwrap().version, 1);
    held.release();
    assert_eq!(
        thread.join().unwrap().unwrap_err().code,
        "concurrency_conflict"
    );
    // Key: b holds its own claim, a commits the same key first; b's commit
    // finds a's receipt in its transaction and returns it.
    let held = control.hold(Op::Decide);
    let other = b.clone();
    let thread = std::thread::spawn(move || other.command(command(2, Some("k"))));
    held.arrived();
    let keyed = a.command(command(2, Some("k"))).unwrap();
    held.release();
    assert_eq!(thread.join().unwrap().unwrap(), keyed);
    // A key b never saw is checked against the store before any decision:
    // a changed payload is refused, the same one replays.
    let late = a.command(command(1, Some("late"))).unwrap();
    let before = control.decisions();
    assert_eq!(
        b.command(command(5, Some("late"))).unwrap_err().code,
        "idempotency_mismatch"
    );
    assert_eq!(b.command(command(1, Some("late"))).unwrap(), late);
    assert_eq!(control.decisions(), before);
    // Projection: b computes from row revision r, a publishes r + 1 first.
    let unlocked = f.locks_runs();
    let held = control.hold(Op::Project);
    let other = b.clone();
    let thread = std::thread::spawn(move || racing(&other, unlocked, false));
    held.arrived();
    assert_eq!(a.catch_up("total").unwrap(), pair(4, 3));
    held.release();
    assert_eq!(
        thread.join().unwrap().unwrap_err().code,
        "projection_conflict"
    );
    // A rebuild publishing at the same checkpoint still changes the row
    // revision, so a candidate computed before it cannot overwrite it.
    let held = control.hold(Op::Project);
    let other = b.clone();
    let thread = std::thread::spawn(move || racing(&other, unlocked, true));
    held.arrived();
    assert_eq!(f.rebuild(&a, "total").unwrap(), pair(4, 3));
    held.release();
    assert_eq!(
        thread.join().unwrap().unwrap_err().code,
        "projection_conflict"
    );
    for app in [&a, &b] {
        let model = app.query(query()).unwrap();
        assert_eq!((model.value, model.checkpoint), (json!(4), 3));
    }
}

fn contract_failed_catch_up_and_rebuild_preserve_the_last_persisted_pair(f: &impl StoreFixture) {
    let control = Arc::new(Control::default());
    let mut c = config();
    c.limits.batch_size = 1;
    let app = f.create(c.clone(), guest(&control), allow()).unwrap();
    for i in 1..=3 {
        app.command(command(i, Some(&format!("k{i}")))).unwrap();
    }
    control.fail_projection.store(2, Ordering::SeqCst);
    assert_eq!(app.catch_up("total").unwrap_err().code, "projection_failed");
    drop(app);
    let app = f.reopen(c.clone(), guest(&control), allow()).unwrap();
    let model = app.query(query()).unwrap();
    assert_eq!((model.value, model.checkpoint), (json!(1), 1));
    control.fail_projection.store(0, Ordering::SeqCst);
    assert_eq!(app.catch_up("total").unwrap().checkpoint, 3);
    control.fail_projection.store(2, Ordering::SeqCst);
    assert_eq!(
        f.rebuild(&app, "total").unwrap_err().code,
        "projection_failed"
    );
    drop(app);
    let app = f.reopen(c, guest(&control), allow()).unwrap();
    let model = app.query(query()).unwrap();
    assert_eq!((model.value, model.checkpoint), (json!(6), 3));
    control.fail_projection.store(0, Ordering::SeqCst);
    assert_eq!(f.rebuild(&app, "total").unwrap(), pair(6, 3));
    assert!(control.lives.lock().unwrap().contains(&false));
}

fn contract_state_limit_holds_across_reopen(f: &impl StoreFixture) {
    let control = Arc::new(Control::default());
    let mut c = config();
    c.limits.max_state_bytes = 1;
    let app = f.create(c.clone(), guest(&control), allow()).unwrap();
    app.command(command(9, Some("k"))).unwrap();
    drop(app);
    let app = f.reopen(c, guest(&control), allow()).unwrap();
    app.command(command(2, Some("next"))).unwrap();
    assert_eq!(app.catch_up("total").unwrap_err().code, "payload_limit");
    assert_eq!(app.query(query()).unwrap().checkpoint, 0);
}

fn contract_history_limit_bounds_each_decision(f: &impl StoreFixture) {
    let control = Arc::new(Control::default());
    let c = Config::parse(&format!(
        "version=2\n[limits]\nmax_stream_events=2\n{CONFIG}"
    ))
    .unwrap();
    let app = f.create(c.clone(), guest(&control), allow()).unwrap();
    for _ in 0..3 {
        app.command(command(1, None)).unwrap();
    }
    // The stream now holds 3 events, more than a decision may receive.
    for _ in 0..2 {
        assert_eq!(
            app.command(command(1, Some("over"))).unwrap_err().code,
            "history_limit"
        );
    }
    assert_eq!(control.decisions(), 3);
    let mut other = command(1, None);
    other.stream = id("two");
    assert_eq!(app.command(other).unwrap().position, 4);
    drop(app);
    let app = f.reopen(c, guest(&control), allow()).unwrap();
    assert_eq!(
        app.command(command(1, Some("over"))).unwrap_err().code,
        "history_limit"
    );
    assert_eq!(head(&app), 4);
}

fn contract_multi_event_batches_keep_contiguous_versions_and_roll_back_an_invalid_tail(
    f: &impl StoreFixture,
) {
    let control = Arc::new(Control::default());
    let app = f.create(config(), guest(&control), allow()).unwrap();
    control.invalid_tail.store(true, Ordering::SeqCst);
    assert_eq!(
        app.command(command(3, Some("batch"))).unwrap_err().code,
        "invalid_event"
    );
    control.invalid_tail.store(false, Ordering::SeqCst);
    assert_eq!(head(&app), 0);
    let mut batch = command(3, Some("batch"));
    batch.payload = json!({"amount":3,"then":[4]});
    let receipt = app.command(batch.clone()).unwrap();
    assert_eq!(
        (receipt.version, receipt.position, receipt.events),
        (2, 2, 2)
    );
    assert_eq!(app.catch_up("total").unwrap(), pair(7, 2));
    assert_eq!(
        *control.seen.lock().unwrap(),
        vec![(1, 1, "event/1".to_owned()), (2, 2, "event/2".to_owned())]
    );
    assert!(
        control
            .seen_metadata
            .lock()
            .unwrap()
            .iter()
            .all(|m| *m == metadata("alice"))
    );
    drop(app);
    let app = f.reopen(config(), guest(&control), allow()).unwrap();
    assert_eq!(app.command(batch).unwrap(), receipt);
    let mut next = command(1, Some("next"));
    next.expected_version = Some(2);
    assert_eq!(app.command(next).unwrap().position, 3);
}

fn contract_statement_failure_rolls_back_events_receipt_position_and_projection_pair(
    f: &impl StoreFixture,
) {
    let control = Arc::new(Control::default());
    let app = f.create(config(), guest(&control), allow()).unwrap();
    f.inject(Fault::ReceiptInsert);
    assert!(app.command(command(3, Some("k"))).is_err());
    f.clear(Fault::ReceiptInsert);
    assert_eq!(head(&app), 0);
    // Neither a receipt nor a stranded claim is left behind.
    let receipt: Receipt = app.command(command(3, Some("k"))).unwrap();
    assert_eq!((receipt.position, receipt.version), (1, 1));
    f.inject(Fault::ProjectionCommit);
    assert!(app.catch_up("total").is_err());
    let model = app.query(query()).unwrap();
    assert_eq!((model.value, model.checkpoint), (json!(0), 0));
    f.clear(Fault::ProjectionCommit);
    drop(app);
    let app = f.reopen(config(), guest(&control), allow()).unwrap();
    assert_eq!(app.command(command(3, Some("k"))).unwrap(), receipt);
    assert_eq!(app.query(query()).unwrap().checkpoint, 0);
    assert_eq!(app.catch_up("total").unwrap(), pair(3, 1));
}

/// One `#[test]` per contract for a backend. `$fixture` is an
/// `Option<impl StoreFixture>`; `None` skips (the backend's own message).
macro_rules! contracts {
    ($backend:ident, $fixture:expr; $($contract:ident),* $(,)?) => {
        mod $backend {
            #[allow(unused_imports)]
            use super::*;
            $(
                #[test]
                fn $contract() {
                    let Some(fixture) = $fixture else { return };
                    super::$contract(&fixture);
                }
            )*
        }
    };
}
contracts!(
    sqlite, Some(Sqlite::new());
    contract_reopen_retains_receipt_and_projection,
    contract_keyed_replay_is_scoped_and_canonical_but_still_validates_and_authorizes,
    contract_zero_event_receipts_survive_reopen,
    contract_one_instance_shares_pending_keys_and_commits_one_of_two_racing_writes,
    contract_independent_instances_recheck_stream_key_and_projection_revision,
    contract_failed_catch_up_and_rebuild_preserve_the_last_persisted_pair,
    contract_state_limit_holds_across_reopen,
    contract_history_limit_bounds_each_decision,
    contract_multi_event_batches_keep_contiguous_versions_and_roll_back_an_invalid_tail,
    contract_statement_failure_rolls_back_events_receipt_position_and_projection_pair,
);

contracts!(
    postgres, Postgres::new();
    contract_reopen_retains_receipt_and_projection,
    contract_keyed_replay_is_scoped_and_canonical_but_still_validates_and_authorizes,
    contract_zero_event_receipts_survive_reopen,
    contract_one_instance_shares_pending_keys_and_commits_one_of_two_racing_writes,
    contract_independent_instances_recheck_stream_key_and_projection_revision,
    contract_failed_catch_up_and_rebuild_preserve_the_last_persisted_pair,
    contract_state_limit_holds_across_reopen,
    contract_history_limit_bounds_each_decision,
    contract_multi_event_batches_keep_contiguous_versions_and_roll_back_an_invalid_tail,
    contract_statement_failure_rolls_back_events_receipt_position_and_projection_pair,
);

/// `max_stream_events` binds the memory backend too.
#[test]
fn memory_history_limit_bounds_each_decision() {
    let control = Arc::new(Control::default());
    let c = Config::parse(&format!(
        "version=2\n[limits]\nmax_stream_events=1\n{CONFIG}"
    ))
    .unwrap();
    let app = Application::in_memory(c, guest(&control), allow()).unwrap();
    app.command(command(1, None)).unwrap();
    app.command(command(1, None)).unwrap();
    assert_eq!(
        app.command(command(1, None)).unwrap_err().code,
        "history_limit"
    );
    assert_eq!(control.decisions(), 2);
}

/// Memory storage has no second instance to open.
#[test]
fn memory_has_no_independent_instance() {
    let control = Arc::new(Control::default());
    let app = Application::in_memory(config(), guest(&control), allow()).unwrap();
    assert_eq!(
        app.independent_instance().err().unwrap().code,
        "storage_configuration"
    );
}
