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
        Ok(vec!["counter".into()])
    }
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
        Ok(Box::new(self.clone()))
    }
}
impl GuestSession for Counter {
    fn invoke(&self, _: &str, op: Operation) -> Result<Reply, Failure> {
        Ok(match op {
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
[[projections]]
name="total"
service="counter"
initial=0
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
    let app = open(&runtime, &url, Counter::default());
    let _ = app.command(command(Some("k")));
    panic!("crash cut was not reached");
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
    let mut short = config();
    short.limits.timeout_ms = 1000;
    for (position, reset) in [
        // The reset fails outright.
        (1, "RESET no_such_parameter"),
        // The reset answers only after the caller's deadline: the session
        // default (5 s) bounds the sleep, not the append's 1 s.
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
        assert!(started.elapsed() < Duration::from_millis(1500), "{reset}");
        // The session whose statement timeout was not restored is closed.
        assert_eq!(db.status().size, before - 1, "{reset}");
    }
    assert_eq!(count(&runtime, test.url(), "events"), 2);
}
