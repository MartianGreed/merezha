//! PostgreSQL store tests that need crate internals: the crash cuts.
use super::*;
use crate::{AllowAll, Application, Guest, GuestSession};
use merezha_domain::{Command, Operation, Reply};
use merezha_postgres::TestDatabase;
use serde_json::json;
use std::process::{Child, Command as Process, Stdio};

/// Park forever at `point` when `MEREZHA_PG_CRASH_CUT` names it, after
/// writing the marker file, so a parent test can kill the process there.
pub(super) fn crash_cut(point: &str) {
    if std::env::var("MEREZHA_PG_CRASH_CUT").as_deref() == Ok(point) {
        let marker = std::env::var_os("MEREZHA_PG_CRASH_MARKER").expect("test marker");
        std::fs::write(marker, point).expect("write crash cut marker");
        loop {
            std::thread::park_timeout(Duration::from_secs(1));
        }
    }
}

#[derive(Clone, Default)]
struct Counter(Arc<std::sync::atomic::AtomicUsize>);
impl Guest for Counter {
    fn services(&self) -> Result<Vec<String>, Failure> {
        Ok(vec!["counter".into(), "reactor".into()])
    }
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
        Ok(Box::new(self.clone()))
    }
}
impl GuestSession for Counter {
    /// `reactor` speaks version 2; everything else version 1.
    fn protocol(&self, service: &str) -> u16 {
        if service == "reactor" {
            merezha_domain::PROTOCOL_V2
        } else {
            merezha_domain::PROTOCOL_V1
        }
    }
    fn invoke(&self, _: &str, op: Operation) -> Result<Reply, Failure> {
        Ok(match op {
            Operation::Describe {} => Reply::Described { protocol: 2 },
            // Each event: one notification, and one on an unknown topic,
            // which the test port refuses.
            Operation::React { events, .. } => Reply::Reacted {
                reactions: events
                    .iter()
                    .map(|event| {
                        Ok(merezha_domain::Reaction {
                            event_id: event.event_id.clone(),
                            effects: serde_json::from_value(json!([
                                {"effect": "notify", "topic": "tasks.completed",
                                 "recipients": [{"user": {"id": "usr_1"}}]},
                                {"effect": "notify", "topic": "unknown.topic",
                                 "recipients": [{"user": {"id": "usr_1"}}]},
                            ]))
                            .map_err(|_| Failure::permanent("invalid_response"))?,
                        })
                    })
                    .collect::<Result<_, Failure>>()?,
            },
            Operation::Validate { payload, .. } => Reply::Validated { payload },
            Operation::Decide { payload, .. } => {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Reply::Decided {
                    events: vec![Event {
                        tag: Identity::new("Added")?,
                        schema_version: 1,
                        payload,
                    }],
                }
            }
            Operation::Project { state, events, .. } => Reply::Projected {
                state: json!(state.as_u64().unwrap_or(0) + events.len() as u64),
            },
            Operation::Query { state, .. } => Reply::Queried { value: state },
        })
    }
}
/// The test port's table and write, registered like the store's own.
const TEST_REACTIONS: merezha_postgres::Statement = merezha_postgres::Statement::new(
    "app.test.reactions_table",
    "CREATE TABLE",
    "CREATE TABLE IF NOT EXISTS merezha_app.test_reactions (dedupe_key text PRIMARY KEY)",
);
const TEST_REACTION: merezha_postgres::Statement = merezha_postgres::Statement::new(
    "app.test.reaction",
    "INSERT merezha_app.test_reactions",
    "INSERT INTO merezha_app.test_reactions (dedupe_key) VALUES ($1) ON CONFLICT DO NOTHING",
);
/// An effect port refusing topic `unknown.topic`, everyone a member,
/// writing each accepted effect's key in the checkpoint transaction when it
/// has a database.
struct Recording(Option<Database>);
impl Effects for Recording {
    fn check(&self, request: &EffectRequest) -> Result<(), Failure> {
        match &request.body {
            EffectBody::Notify { topic, .. } if topic.as_str() == "unknown.topic" => {
                Err(Failure::permanent("unknown_topic"))
            }
            _ => Ok(()),
        }
    }
    fn is_member<'a>(
        &'a self,
        _: &'a Identity,
        _: &'a Identity,
    ) -> merezha_postgres::BoxFuture<'a, Result<bool, Failure>> {
        Box::pin(async { Ok(true) })
    }
    fn write<'a>(
        &'a self,
        tx: &'a mut merezha_postgres::Tx<'_>,
        request: &'a EffectRequest,
    ) -> merezha_postgres::BoxFuture<'a, Result<(), Failure>> {
        Box::pin(async move {
            if let Some(db) = &self.0 {
                db.execute(tx, &TEST_REACTION, &[&request.dedupe_key])
                    .await
                    .map_err(|e| merezha_postgres::classify(&e))?;
            }
            Ok(())
        })
    }
}
/// [`config`] with a reactor on `Added`, from the beginning.
fn reactor_config() -> Config {
    Config::parse(
        r#"version=2
[storage]
backend="postgres"
application="crash-test"
[[commands]]
tag="Add"
service="counter"
aggregate="Counter"
[[queries]]
tag="Total"
service="counter"
projection="total"
[[projections]]
name="total"
service="counter"
initial=0
[[reactors]]
name="notifier"
service="reactor"
events=["Added"]
effects=["notify"]
start="beginning"
"#,
    )
    .unwrap()
}
fn open_reactor(runtime: &tokio::runtime::Runtime, url: &str) -> Application {
    let db = Database::connect(&merezha_postgres::DatabaseConfig::new(
        merezha_postgres::Secret::new(url),
    ))
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let options = PostgresOptions {
        effects: Some(Arc::new(Recording(Some(db.clone())))),
        ..PostgresOptions::default()
    };
    let store = runtime
        .block_on(PostgresStore::connect(
            &db,
            &reactor_config(),
            options,
            deadline,
        ))
        .unwrap();
    Application::with_postgres(
        reactor_config(),
        Arc::new(Counter::default()),
        Arc::new(AllowAll),
        Arc::new(store),
    )
    .unwrap()
}
fn config() -> Config {
    Config::parse(
        r#"version=1
[storage]
backend="postgres"
application="crash-test"
[[commands]]
tag="Add"
service="counter"
aggregate="Counter"
[[queries]]
tag="Total"
service="counter"
projection="total"
[[projections]]
name="total"
service="counter"
initial=0
"#,
    )
    .unwrap()
}
/// [`config`] with a tenant-scoped command and query, a partitioned
/// projection (AP-C2) and a reactor (AP-C3).
fn tenant_config() -> Config {
    Config::parse(
        r#"version=2
[storage]
backend="postgres"
application="crash-test"
[[commands]]
tag="Add"
service="counter"
aggregate="Counter"
[[commands]]
tag="TenantAdd"
service="counter"
aggregate="Counter"
access="authenticated"
scope="tenant"
[[queries]]
tag="Total"
service="counter"
projection="total"
cross_tenant=true
[[queries]]
tag="Mine"
service="counter"
projection="mine"
access="authenticated"
scope="tenant"
[[projections]]
name="total"
service="counter"
initial=0
[[projections]]
name="mine"
service="counter"
initial=0
partition="tenant"
[[reactors]]
name="notifier"
service="reactor"
events=["Added"]
effects=["notify"]
start="beginning"
"#,
    )
    .unwrap()
}
fn command(key: Option<&str>) -> Command {
    serde_json::from_value(json!({"tag":"Add","stream":"one","payload":3,"idempotency_key":key,"metadata":{"message_id":"m1","correlation_id":"c1","actor":"alice"}})).unwrap()
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
}
fn open(runtime: &tokio::runtime::Runtime, url: &str, guest: Counter) -> Application {
    let db = Database::connect(&merezha_postgres::DatabaseConfig::new(
        merezha_postgres::Secret::new(url),
    ))
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let store = runtime
        .block_on(PostgresStore::connect(
            &db,
            &config(),
            PostgresOptions::default(),
            deadline,
        ))
        .unwrap();
    Application::with_postgres(
        config(),
        Arc::new(guest),
        Arc::new(AllowAll),
        Arc::new(store),
    )
    .unwrap()
}
fn count(runtime: &tokio::runtime::Runtime, url: &str, table: &str) -> i64 {
    runtime.block_on(async {
        let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
            .await
            .unwrap();
        let task = tokio::spawn(connection);
        let n = client
            .query_one(&format!("SELECT count(*) FROM merezha_app.{table}"), &[])
            .await
            .unwrap()
            .get(0);
        drop(client);
        let _ = task.await;
        n
    })
}

struct OwnedChild(Child);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[test]
#[ignore = "subprocess entry point, invoked only by the crash cut test"]
fn crash_worker() {
    let url = std::env::var("MEREZHA_PG_CRASH_DATABASE").expect("child database");
    let runtime = runtime();
    if std::env::var("MEREZHA_PG_CRASH_OP").as_deref() == Ok("react") {
        let _ = open_reactor(&runtime, &url).react_now("notifier");
        panic!("crash cut was not reached");
    }
    let app = open(&runtime, &url, Counter::default());
    if std::env::var("MEREZHA_PG_CRASH_OP").as_deref() == Ok("rebuild") {
        let _ = app.rebuild("total");
    } else {
        let _ = app.command(command(Some("k")));
    }
    panic!("crash cut was not reached");
}
fn migrated(runtime: &tokio::runtime::Runtime, test: &TestDatabase) {
    runtime.block_on(async {
        let db = Database::connect(&test.config()).unwrap();
        let modules = [ModuleMigrations {
            module: MODULE,
            migrations: MIGRATIONS,
        }];
        let deadline = Instant::now() + Duration::from_secs(30);
        run_migrations(&db, &modules, MigrationMode::Apply, deadline)
            .await
            .unwrap();
    });
}
/// Run the crash worker until it parks at `cut`, then kill -9 it.
fn kill_at(test: &TestDatabase, cut: &str, op: &str) {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("cut");
    let mut child = OwnedChild(
        Process::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "postgres::tests::crash_worker",
                "--ignored",
                "--nocapture",
            ])
            .env("MEREZHA_PG_CRASH_DATABASE", test.url())
            .env("MEREZHA_PG_CRASH_CUT", cut)
            .env("MEREZHA_PG_CRASH_OP", op)
            .env("MEREZHA_PG_CRASH_MARKER", &marker)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let until = Instant::now() + Duration::from_secs(20);
    while !marker.exists() {
        assert!(Instant::now() < until, "child failed to reach {cut}");
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "child exited before {cut}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    child.0.kill().unwrap();
    assert!(!child.0.wait().unwrap().success());
}
fn generation(runtime: &tokio::runtime::Runtime, url: &str) -> (i32, i64, i64) {
    runtime.block_on(async {
        let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
            .await
            .unwrap();
        let task = tokio::spawn(connection);
        let row = client
            .query_one(
                "SELECT p.generation, p.checkpoint, (SELECT count(*) FROM merezha_app.projection_rebuilds) \
                 FROM merezha_app.projections p WHERE p.name = 'total'",
                &[],
            )
            .await
            .unwrap();
        drop(client);
        let _ = task.await;
        (row.get(0), row.get(1), row.get(2))
    })
}
/// A3 test 4, crash cuts: kill -9 just before and just after the rebuild
/// swap. Before it, the finished candidate survives and the next rebuild
/// resumes and swaps it; after it, the new generation is served and no
/// rebuild is left.
#[test]
fn killed_rebuild_resumes_before_the_swap_and_keeps_the_result_after_it() {
    for cut in ["rebuild_before_swap", "rebuild_after_swap"] {
        let Some(test) = TestDatabase::create() else {
            return;
        };
        let runtime = runtime();
        migrated(&runtime, &test);
        let app = open(&runtime, test.url(), Counter::default());
        for _ in 0..5 {
            app.command(command(None)).unwrap();
        }
        assert_eq!(app.catch_up("total").unwrap().checkpoint, 5);
        drop(app);
        kill_at(&test, cut, "rebuild");
        let swapped = cut == "rebuild_after_swap";
        assert_eq!(
            generation(&runtime, test.url()),
            if swapped { (2, 5, 0) } else { (1, 5, 1) },
            "{cut}"
        );
        let app = open(&runtime, test.url(), Counter::default());
        if !swapped {
            // The persisted, finished candidate is swapped by the resume.
            let pair = app.rebuild("total").unwrap();
            assert_eq!(
                (pair.state.clone(), pair.checkpoint),
                (json!(5), 5),
                "{cut}"
            );
            assert_eq!(generation(&runtime, test.url()), (2, 5, 0), "{cut}");
        }
        let model = app
            .query(serde_json::from_value(json!({"tag":"Total","payload":{},"metadata":{"message_id":"q","correlation_id":"c"}})).unwrap())
            .unwrap();
        assert_eq!((model.value, model.checkpoint), (json!(5), 5), "{cut}");
    }
}
/// C3 test 2, crash cuts: kill -9 inside the checkpoint transaction, after
/// its writes and before `COMMIT`, and just after it. Before it, nothing
/// was written: the restarted reactor runs the batch again and writes each
/// effect once. After it, the effects, rejections and checkpoint are
/// committed together and nothing is written twice.
#[test]
fn killed_reactor_writes_its_effects_once_at_each_checkpoint_cut() {
    for cut in ["reactor_before_commit", "reactor_after_commit"] {
        let Some(test) = TestDatabase::create() else {
            return;
        };
        let runtime = runtime();
        migrated(&runtime, &test);
        runtime.block_on(async {
            let db = Database::connect(&test.config()).unwrap();
            let client = db
                .acquire(Instant::now() + Duration::from_secs(10))
                .await
                .unwrap();
            db.batch(&client, &TEST_REACTIONS).await.unwrap();
        });
        let app = open_reactor(&runtime, test.url());
        for _ in 0..3 {
            app.command(command(None)).unwrap();
        }
        drop(app);
        kill_at(&test, cut, "react");
        let committed = if cut == "reactor_after_commit" { 3 } else { 0 };
        assert_eq!(
            count(&runtime, test.url(), "test_reactions"),
            committed,
            "{cut}"
        );
        assert_eq!(
            count(&runtime, test.url(), "reactor_rejections"),
            committed,
            "{cut}"
        );
        let app = open_reactor(&runtime, test.url());
        app.react_now("notifier").unwrap();
        app.react_now("notifier").unwrap();
        assert_eq!(count(&runtime, test.url(), "test_reactions"), 3, "{cut}");
        assert_eq!(
            count(&runtime, test.url(), "reactor_rejections"),
            3,
            "{cut}"
        );
        let reactors = app.admit().unwrap().reactors().unwrap();
        assert_eq!(reactors.reactors[0].checkpoint, 3, "{cut}");
    }
}
#[test]
fn killed_process_keeps_history_and_receipts_exact_at_each_append_cut() {
    for cut in ["before_append", "after_append_before_wake"] {
        let Some(test) = TestDatabase::create() else {
            return;
        };
        let runtime = runtime();
        runtime.block_on(async {
            let db = Database::connect(&test.config()).unwrap();
            let modules = [ModuleMigrations {
                module: MODULE,
                migrations: MIGRATIONS,
            }];
            let deadline = Instant::now() + Duration::from_secs(30);
            run_migrations(&db, &modules, MigrationMode::Apply, deadline)
                .await
                .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("cut");
        let mut child = OwnedChild(
            Process::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "postgres::tests::crash_worker",
                    "--ignored",
                    "--nocapture",
                ])
                .env("MEREZHA_PG_CRASH_DATABASE", test.url())
                .env("MEREZHA_PG_CRASH_CUT", cut)
                .env("MEREZHA_PG_CRASH_MARKER", &marker)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let until = Instant::now() + Duration::from_secs(20);
        while !marker.exists() {
            assert!(Instant::now() < until, "child failed to reach {cut}");
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "child exited before {cut}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        child.0.kill().unwrap();
        assert!(!child.0.wait().unwrap().success());
        let committed = i64::from(cut == "after_append_before_wake");
        assert_eq!(count(&runtime, test.url(), "events"), committed, "{cut}");
        assert_eq!(count(&runtime, test.url(), "receipts"), committed, "{cut}");
        // After the restart a keyed retry replays a committed command and
        // commits an uncommitted one; an unkeyed retry is a new command.
        let guest = Counter::default();
        let app = open(&runtime, test.url(), guest.clone());
        let receipt = app.command(command(Some("k"))).unwrap();
        assert_eq!((receipt.position, receipt.version), (1, 1), "{cut}");
        assert_eq!(
            guest.0.load(std::sync::atomic::Ordering::SeqCst),
            usize::from(committed == 0),
            "{cut}"
        );
        assert_eq!(app.command(command(None)).unwrap().position, 2, "{cut}");
        assert_eq!(count(&runtime, test.url(), "events"), 2, "{cut}");
        assert_eq!(count(&runtime, test.url(), "receipts"), 1, "{cut}");
        drop(app);
        let report = runtime
            .block_on(verify(
                &Database::connect(&test.config()).unwrap(),
                Instant::now() + Duration::from_secs(10),
            ))
            .unwrap();
        assert!(report.ok, "{cut}: {report:?}");
    }
}

#[test]
fn an_append_that_answered_keeps_its_outcome_when_only_the_reset_fails_or_is_late() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let runtime = runtime();
    let db = Database::connect(&test.config()).unwrap();
    runtime.block_on(async {
        let modules = [ModuleMigrations {
            module: MODULE,
            migrations: MIGRATIONS,
        }];
        run_migrations(
            &db,
            &modules,
            MigrationMode::Apply,
            Instant::now() + Duration::from_secs(30),
        )
        .await
        .unwrap();
    });
    // Room for the append on a loaded machine, and well under the 5 s
    // session default that bounds the late reset below.
    let mut short = config();
    short.limits.timeout_ms = 2000;
    for (position, reset) in [
        // The reset fails outright.
        (1, "RESET no_such_parameter"),
        // The reset answers only after the caller's deadline: the session
        // default (5 s) bounds the sleep, not the append's 2 s.
        (2, "RESET statement_timeout; SELECT pg_sleep(10)"),
    ] {
        let mut store = runtime
            .block_on(PostgresStore::connect(
                &db,
                &short,
                PostgresOptions::default(),
                Instant::now() + Duration::from_secs(10),
            ))
            .unwrap();
        store.reset_statement = reset;
        let app = Application::with_postgres(
            short.clone(),
            Arc::new(Counter::default()),
            Arc::new(AllowAll),
            Arc::new(store),
        )
        .unwrap();
        let before = db.status().size;
        let started = Instant::now();
        let receipt = app.command(command(None)).unwrap();
        assert_eq!(receipt.position, position, "{reset}");
        // At the caller's deadline, before the late reset ends at the 5 s
        // session default.
        assert!(started.elapsed() < Duration::from_millis(4500), "{reset}");
        // The session whose statement timeout was not restored is closed.
        assert_eq!(db.status().size, before - 1, "{reset}");
    }
    assert_eq!(count(&runtime, test.url(), "events"), 2);
}

/// A counter whose projection fails while `fail` is set, and whose
/// reactor is overloaded while `overloaded` is.
#[derive(Clone, Default)]
struct Flaky {
    counter: Counter,
    fail: Arc<std::sync::atomic::AtomicBool>,
    overloaded: Arc<std::sync::atomic::AtomicBool>,
}
impl Guest for Flaky {
    fn services(&self) -> Result<Vec<String>, Failure> {
        self.counter.services()
    }
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
        Ok(Box::new(self.clone()))
    }
}
impl GuestSession for Flaky {
    fn invoke(&self, service: &str, op: Operation) -> Result<Reply, Failure> {
        if matches!(op, Operation::Project { .. })
            && self.fail.load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(Failure::permanent("guest_error"));
        }
        if matches!(op, Operation::React { .. })
            && self.overloaded.load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(Failure::transient("overloaded"));
        }
        self.counter.invoke(service, op)
    }
    fn protocol(&self, service: &str) -> u16 {
        self.counter.protocol(service)
    }
}

/// TE §1.3 (T4, owed by AP-A2): the store runs registered statements only,
/// the append reports as `app.events.append`, and driving every store path
/// (commands, a replay, reads, a catch-up commit, a paused rebuild, a
/// failing rebuild with resume and abandon, the admin reads, a recorded
/// failure, a tenant command, a partitioned catch-up, query and rebuild,
/// a reactor's failed and committed batches with its admin operations,
/// and `verify`) exercises exactly the statements `STATEMENTS` lists.
#[test]
fn the_store_runs_exactly_its_registered_statements_and_reports_the_append() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    assert_eq!(
        merezha_postgres::check_registry(&[merezha_postgres::KERNEL_STATEMENTS, STATEMENTS]),
        Ok(())
    );
    let harness = merezha_telemetry::testing::Harness::new();
    let runtime = runtime();
    let meter = harness
        .telemetry()
        .meters()
        .meter(merezha_postgres::METER_SCOPE);
    let db = Database::connect_with_meter(&test.config(), &meter)
        .unwrap()
        .for_module(MODULE);
    let deadline = || Instant::now() + Duration::from_secs(30);
    let pause = Arc::new(Pause::default());
    let options = PostgresOptions {
        background: BackgroundOptions {
            catch_up: CatchUp::Manual,
            interval: Duration::from_secs(600),
            hooks: RunHooks {
                pause: Some(pause.clone()),
                ..RunHooks::default()
            },
            ..BackgroundOptions::default()
        },
        effects: Some(Arc::new(Recording(None))),
        ..PostgresOptions::default()
    };
    let guest = Flaky::default();
    let config = tenant_config();
    let projection = config.projections[0].clone();
    let caller = tracing::info_span!(target: "merezha_test", "caller");
    let _entered = caller.enter();
    let modules = [ModuleMigrations {
        module: MODULE,
        migrations: MIGRATIONS,
    }];
    runtime
        .block_on(run_migrations(
            &db,
            &modules,
            MigrationMode::Apply,
            deadline(),
        ))
        .unwrap();
    let store = Arc::new(
        runtime
            .block_on(PostgresStore::connect(&db, &config, options, deadline()))
            .unwrap(),
    );
    let app = Application::with_postgres(
        config.clone(),
        Arc::new(guest.clone()),
        Arc::new(AllowAll),
        store.clone(),
    )
    .unwrap();
    app.command(command(Some("k1"))).unwrap();
    app.command(command(Some("k1"))).unwrap();
    app.command(command(None)).unwrap();
    app.query(
        serde_json::from_value(
            json!({"tag":"Total","payload":{},"metadata":{"message_id":"q","correlation_id":"c"}}),
        )
        .unwrap(),
    )
    .unwrap();
    let stream = Identity::new("one").unwrap();
    store.load(&stream, deadline()).unwrap();
    store.read_all(0, 10, deadline()).unwrap();
    store.head(deadline()).unwrap();
    let reference = ProjectionRef {
        name: &projection.name,
        revision: projection.revision,
    };
    let (pair, row_revision) = store.projection(&reference, deadline()).unwrap();
    store
        .commit_projection(&reference, row_revision, &pair, deadline())
        .unwrap();
    // A rebuild whose commits pause in a server-side sleep.
    pause.arm_statement(projection.name.as_str(), Duration::from_millis(10));
    app.rebuild("total").unwrap();
    // A rebuild that fails, resumed, then abandoned.
    guest.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(app.rebuild("total").unwrap_err().code, "guest_error");
    guest.fail.store(false, std::sync::atomic::Ordering::SeqCst);
    store
        .request_rebuild(&projection, true, None, deadline())
        .unwrap();
    store.rebuild_status(&projection, deadline()).unwrap();
    store.abandon_rebuild(&projection, deadline()).unwrap();
    // A tenant stream, a partitioned catch-up, tenant query and rebuild.
    let member = merezha_domain::Principal::new(
        Identity::new("usr_1").unwrap(),
        merezha_domain::PrincipalKind::User,
        Some(Identity::new("org_1").unwrap()),
        vec![],
    )
    .unwrap();
    let tenant_command: Command = serde_json::from_value(json!({"tag":"TenantAdd","stream":"one","payload":3,"metadata":{"message_id":"m2","correlation_id":"c2"}})).unwrap();
    app.command_as(member.clone(), tenant_command).unwrap();
    app.catch_up("mine").unwrap();
    let mine: merezha_domain::Query = serde_json::from_value(
        json!({"tag":"Mine","payload":{},"metadata":{"message_id":"q2","correlation_id":"c2"}}),
    )
    .unwrap();
    assert_eq!(app.query_as(member, mine).unwrap().value, json!(1));
    app.rebuild("mine").unwrap();
    store.overview(&[&projection], deadline()).unwrap();
    store.revisions(&projection.name, deadline()).unwrap();
    assert_eq!(
        store
            .delete_revision(&projection.name, 99, deadline())
            .unwrap_err()
            .code,
        "projection_not_found"
    );
    runtime
        .block_on(store.record_failure(
            projection.name.as_str().to_owned(),
            projection.revision as i32,
            &Failure::transient("database_timeout"),
            deadline(),
        ))
        .unwrap();
    // Reactors: a failed batch, an operator's skip and retry, a batch with
    // a rejected effect, a pause and resume, and the admin reads.
    guest
        .overloaded
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(matches!(
        app.react_now("notifier").unwrap(),
        ReactorOutcome::Waiting(Some(_))
    ));
    guest
        .overloaded
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let admin = || app.admit().unwrap();
    admin()
        .skip_reactor(
            "notifier",
            crate::status::SkipTo::Position(1),
            Some("usr_1"),
        )
        .unwrap();
    admin().retry_reactor("notifier").unwrap();
    app.react_now("notifier").unwrap();
    admin().set_reactor_active("notifier", false).unwrap();
    admin().set_reactor_active("notifier", true).unwrap();
    assert_eq!(admin().reactors().unwrap().reactors[0].rejected, 2);
    assert_eq!(
        admin()
            .rejections("notifier", 10, None)
            .unwrap()
            .rejections
            .len(),
        2
    );
    runtime
        .block_on(store.reactor_due("notifier", deadline()))
        .unwrap();
    assert!(runtime.block_on(verify(&db, deadline())).unwrap().ok);
    // A missing position makes `verify` list the gap.
    runtime.block_on(async {
        let client = db.acquire(deadline()).await.unwrap();
        client
            .batch_execute(
                "ALTER TABLE merezha_app.events DISABLE TRIGGER USER; \
                 DELETE FROM merezha_app.events WHERE position = 1",
            )
            .await
            .unwrap();
    });
    assert!(!runtime.block_on(verify(&db, deadline())).unwrap().ok);
    drop(_entered);

    let spans: Vec<_> = harness
        .spans()
        .into_iter()
        .filter(|s| s.span_kind == opentelemetry::trace::SpanKind::Client)
        .collect();
    let name = |span: &opentelemetry_sdk::trace::SpanData| {
        span.attributes
            .iter()
            .find(|kv| kv.key.as_str() == "merezha.db.statement")
            .map(|kv| kv.value.to_string())
            .expect("every client span names its statement")
    };
    let seen: std::collections::BTreeSet<String> = spans.iter().map(name).collect();
    let kernel: std::collections::BTreeSet<&str> = merezha_postgres::KERNEL_STATEMENTS
        .iter()
        .map(|s| s.name)
        .collect();
    let store_seen: std::collections::BTreeSet<&str> = seen
        .iter()
        .map(String::as_str)
        .filter(|n| !kernel.contains(n))
        .collect();
    let registered: std::collections::BTreeSet<&str> = STATEMENTS.iter().map(|s| s.name).collect();
    assert_eq!(store_seen, registered);
    let appends = spans
        .iter()
        .filter(|s| name(s) == APPEND_STATEMENT)
        .collect::<Vec<_>>();
    assert_eq!(
        appends.len(),
        3,
        "two commands and the tenant command; the replay reads its receipt instead"
    );
    assert_eq!(appends[0].name, "INSERT merezha_app.events");
    assert_eq!(
        harness.metric("db.client.operation.duration").value(&[
            ("db.operation.name", "INSERT"),
            ("db.collection.name", "merezha_app.events"),
            ("merezha.module", MODULE),
        ]),
        Some(3.0)
    );
}
