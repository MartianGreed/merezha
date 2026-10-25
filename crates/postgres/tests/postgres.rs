//! Real PostgreSQL behavior. Each test creates and drops its own database
//! through `MEREZHA_TEST_DATABASE_URL` and skips with a message when unset.
use merezha_postgres::{
    Database, Migration, MigrationMode, ModuleMigrations, TestDatabase, checksum, classify,
    run_migrations,
};
use std::time::{Duration, Instant};

const CREATE: Migration = Migration::new(
    1,
    "create_notes",
    "CREATE TABLE notes (id bigint PRIMARY KEY, body text NOT NULL);",
);
const INDEX: Migration = Migration::new(
    2,
    "index_notes",
    "CREATE INDEX notes_body ON notes (body); INSERT INTO notes VALUES (1, 'seed');",
);

fn soon() -> Instant {
    Instant::now() + Duration::from_secs(20)
}
fn module(migrations: &[Migration]) -> [ModuleMigrations<'_>; 1] {
    [ModuleMigrations {
        module: "notes",
        migrations,
    }]
}
async fn ledger(db: &Database) -> Vec<(String, i32, String, String)> {
    let client = db.acquire(soon()).await.unwrap();
    client
        .query(
            "SELECT module, version, name, checksum FROM merezha.migrations ORDER BY module, version",
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect()
}

#[tokio::test]
async fn fresh_apply_records_ledger_and_rerun_is_idempotent() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = Database::connect(&test.config()).unwrap();
    let report = run_migrations(&db, &module(&[CREATE, INDEX]), MigrationMode::Apply, soon())
        .await
        .unwrap();
    assert_eq!(
        report
            .applied
            .iter()
            .map(|m| (m.module.as_str(), m.version, m.name))
            .collect::<Vec<_>>(),
        vec![("notes", 1, "create_notes"), ("notes", 2, "index_notes")]
    );
    assert!(report.is_current());
    assert_eq!(
        ledger(&db).await,
        vec![
            (
                "notes".into(),
                1,
                "create_notes".into(),
                checksum(CREATE.sql)
            ),
            ("notes".into(), 2, "index_notes".into(), checksum(INDEX.sql)),
        ]
    );
    let again = run_migrations(&db, &module(&[CREATE, INDEX]), MigrationMode::Apply, soon())
        .await
        .unwrap();
    assert!(again.applied.is_empty() && again.is_current());
    let verified = run_migrations(
        &db,
        &module(&[CREATE, INDEX]),
        MigrationMode::Verify,
        soon(),
    )
    .await
    .unwrap();
    assert!(verified.is_current());
    // The seed row was inserted exactly once.
    let client = db.acquire(soon()).await.unwrap();
    let count: i64 = client
        .query_one("SELECT count(*) FROM notes", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);
}

#[tokio::test]
async fn changed_checksum_and_unknown_applied_versions_are_refused() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = Database::connect(&test.config()).unwrap();
    run_migrations(&db, &module(&[CREATE, INDEX]), MigrationMode::Apply, soon())
        .await
        .unwrap();
    let edited = Migration {
        sql: "CREATE INDEX notes_body ON notes (body);",
        ..INDEX
    };
    for mode in [MigrationMode::Apply, MigrationMode::Verify] {
        let error = run_migrations(&db, &module(&[CREATE, edited]), mode, soon())
            .await
            .unwrap_err();
        assert_eq!(error.failure.code, "migration_checksum_mismatch");
        assert_eq!(
            (error.module.as_deref(), error.version),
            (Some("notes"), Some(2))
        );
        let error = run_migrations(&db, &module(&[CREATE]), mode, soon())
            .await
            .unwrap_err();
        assert_eq!(
            (error.failure.code.as_str(), error.version),
            ("unknown_applied_migration", Some(2))
        );
    }
    // Nothing was rewritten by the refused runs.
    assert_eq!(ledger(&db).await[1].3, checksum(INDEX.sql));
}

#[tokio::test]
async fn verify_only_reports_pending_without_writing() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = Database::connect(&test.config()).unwrap();
    // No ledger yet: verify must not create it.
    let report = run_migrations(&db, &module(&[CREATE]), MigrationMode::Verify, soon())
        .await
        .unwrap();
    assert!(!report.is_current());
    assert_eq!(report.pending[0].version, 1);
    let client = db.acquire(soon()).await.unwrap();
    let exists: bool = client
        .query_one("SELECT to_regclass('merezha.migrations') IS NOT NULL", &[])
        .await
        .unwrap()
        .get(0);
    assert!(!exists, "verify created the ledger");
    drop(client);
    run_migrations(&db, &module(&[CREATE]), MigrationMode::Apply, soon())
        .await
        .unwrap();
    let report = run_migrations(
        &db,
        &module(&[CREATE, INDEX]),
        MigrationMode::Verify,
        soon(),
    )
    .await
    .unwrap();
    assert_eq!(
        report.pending.iter().map(|m| m.version).collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(ledger(&db).await.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_runners_are_serialized_by_the_advisory_lock() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    // Not idempotent SQL: two unserialized runners would fail with
    // duplicate_table or a ledger primary key violation.
    const SLOW: Migration = Migration::new(
        1,
        "slow_create",
        "SELECT pg_sleep(0.3); CREATE TABLE once (id int);",
    );
    let mut runners = Vec::new();
    for _ in 0..4 {
        let db = Database::connect(&test.config()).unwrap();
        runners.push(tokio::spawn(async move {
            let started = Instant::now();
            let report = run_migrations(&db, &module(&[SLOW]), MigrationMode::Apply, soon())
                .await
                .unwrap();
            (report.applied.len(), started.elapsed())
        }));
    }
    let mut applied = 0;
    for runner in runners {
        let (count, _) = runner.await.unwrap();
        applied += count;
    }
    assert_eq!(applied, 1, "exactly one runner applies the migration");
    let db = Database::connect(&test.config()).unwrap();
    assert_eq!(ledger(&db).await.len(), 1);
    // The lock died with the detached session: a new apply proceeds at
    // once, where a lock left behind would hold it to its 20 s deadline.
    let started = Instant::now();
    run_migrations(&db, &module(&[SLOW]), MigrationMode::Apply, soon())
        .await
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(10));
}

#[tokio::test]
async fn failing_migration_rolls_back_alone() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = Database::connect(&test.config()).unwrap();
    let broken = Migration::new(
        2,
        "broken",
        "CREATE TABLE half (id int); SELECT * FROM missing_table;",
    );
    let error = run_migrations(
        &db,
        &module(&[CREATE, broken]),
        MigrationMode::Apply,
        soon(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        (error.failure.code.as_str(), error.version),
        ("migration_failed", Some(2))
    );
    assert_eq!(ledger(&db).await.len(), 1);
    let client = db.acquire(soon()).await.unwrap();
    let half: bool = client
        .query_one("SELECT to_regclass('half') IS NOT NULL", &[])
        .await
        .unwrap()
        .get(0);
    assert!(!half, "the failed migration's DDL was rolled back");
}

#[tokio::test]
async fn transactions_commit_roll_back_and_take_statement_timeout_from_the_deadline() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = Database::connect(&test.config()).unwrap();
    db.ping(soon()).await.unwrap();
    db.transaction(soon(), |tx| {
        Box::pin(async move {
            tx.batch_execute("CREATE TABLE items (id int)")
                .await
                .map_err(|e| classify(&e))
        })
    })
    .await
    .unwrap();
    let error = db
        .transaction(soon(), |tx| {
            Box::pin(async move {
                tx.batch_execute("INSERT INTO items VALUES (1)")
                    .await
                    .map_err(|e| classify(&e))?;
                Err::<(), _>(merezha_domain::Failure::conflict("caller_refused"))
            })
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, "caller_refused");
    // The deadline is far below the 5 s default ceiling.
    let timeout: String = db
        .transaction(Instant::now() + Duration::from_millis(1500), |tx| {
            Box::pin(async move {
                Ok(tx
                    .query_one("SHOW statement_timeout", &[])
                    .await
                    .map_err(|e| classify(&e))?
                    .get(0))
            })
        })
        .await
        .unwrap();
    let millis: u64 = timeout.trim_end_matches("ms").parse().unwrap_or(1000);
    assert!(
        timeout.ends_with("ms") && (1..=1500).contains(&millis),
        "{timeout}"
    );
    let started = Instant::now();
    let error = db
        .transaction(Instant::now() + Duration::from_millis(200), |tx| {
            Box::pin(async move {
                tx.batch_execute("SELECT pg_sleep(5)")
                    .await
                    .map_err(|e| classify(&e))
            })
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, "database_timeout");
    // Cut long before the 5 s sleep.
    assert!(started.elapsed() < Duration::from_secs(4));
    let client = db.acquire(soon()).await.unwrap();
    let rows: i64 = client
        .query_one("SELECT count(*) FROM items", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(rows, 0, "the refused transaction rolled back");
    // Pooled sessions keep the configured default outside transactions.
    let default: String = client
        .query_one("SHOW statement_timeout", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(default, "5s");
}

#[tokio::test]
async fn unreachable_database_is_a_transient_failure() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = Database::connect(&test.config()).unwrap();
    db.ping(soon()).await.unwrap();
    test.admin_execute(&format!(
        "ALTER DATABASE \"{0}\" ALLOW_CONNECTIONS false; SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{0}'",
        test.name()
    ));
    let mut failure = None;
    for _ in 0..3 {
        if let Err(error) = db.ping(soon()).await {
            failure = Some(error);
            break;
        }
    }
    let failure = failure.expect("ping kept succeeding after connections were refused");
    assert_eq!(
        failure.classification,
        merezha_domain::Classification::Transient,
        "{}",
        failure.code
    );
    test.admin_execute(&format!(
        "ALTER DATABASE \"{}\" ALLOW_CONNECTIONS true",
        test.name()
    ));
    // Fast recycling discards each dead pooled session after its first
    // failed use, so recovery takes at most one attempt per pool slot.
    let mut recovered = false;
    for _ in 0..=test.config().pool_size {
        if db.ping(soon()).await.is_ok() {
            recovered = true;
            break;
        }
    }
    assert!(recovered, "pool did not recover after connections resumed");
}

#[tokio::test]
async fn verify_reuses_a_pooled_session_without_changing_it() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let mut config = test.config();
    config.pool_size = 1;
    let db = Database::connect(&config).unwrap();
    run_migrations(&db, &module(&[CREATE]), MigrationMode::Apply, soon())
        .await
        .unwrap();
    let pid = |db: Database| async move {
        let client = db.acquire(soon()).await.unwrap();
        client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get::<_, i32>(0)
    };
    let before = pid(db.clone()).await;
    // Readiness polls like this while migrations are pending.
    for _ in 0..5 {
        let report = run_migrations(
            &db,
            &module(&[CREATE, INDEX]),
            MigrationMode::Verify,
            Instant::now() + Duration::from_millis(1500),
        )
        .await
        .unwrap();
        assert_eq!(report.pending.len(), 1);
    }
    assert_eq!(pid(db.clone()).await, before, "verify opened new sessions");
    let client = db.acquire(soon()).await.unwrap();
    let timeout: String = client
        .query_one("SHOW statement_timeout", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(timeout, "5s", "the verify bound leaked into the session");
}

#[tokio::test]
async fn url_options_are_kept_beside_the_statement_timeout() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let separator = if test.url().contains('?') { '&' } else { '?' };
    let mut config = test.config();
    config.url = merezha_postgres::Secret::new(format!(
        "{}{separator}options=-c%20search_path%3Dcustom_schema",
        test.url()
    ));
    let db = Database::connect(&config).unwrap();
    let client = db.acquire(soon()).await.unwrap();
    let path: String = client
        .query_one("SHOW search_path", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(path, "custom_schema");
    let timeout: String = client
        .query_one("SHOW statement_timeout", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(timeout, "5s");
}

const BODY_INDEX: Migration = Migration::non_transactional(
    3,
    "notes_body_concurrently",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS notes_body_cc ON notes (body);",
);
const UNIQUE_BODY: Migration = Migration::non_transactional(
    3,
    "notes_body_unique",
    // A leading comment must not hide the concurrent build from the runner.
    "-- one body per note\n/* rebuilt if invalid */ CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS notes_body_unique ON notes (body)",
);
const DUPLICATES: Migration = Migration::new(
    2,
    "duplicate_bodies",
    "INSERT INTO notes VALUES (1, 'same'), (2, 'same');",
);
/// `Some(valid)` for an existing index, `None` when there is none.
async fn index_valid(db: &Database, name: &str) -> Option<bool> {
    let client = db.acquire(soon()).await.unwrap();
    client
        .query_opt(
            "SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = $1",
            &[&name],
        )
        .await
        .unwrap()
        .map(|row| row.get(0))
}

#[tokio::test]
async fn non_transactional_step_runs_outside_a_transaction_and_is_recorded_after_success() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = Database::connect(&test.config()).unwrap();
    let declared = [CREATE, INDEX, BODY_INDEX];
    // CREATE INDEX CONCURRENTLY fails inside a transaction block, so a
    // success proves the step ran outside one.
    let report = run_migrations(&db, &module(&declared), MigrationMode::Apply, soon())
        .await
        .unwrap();
    assert_eq!(report.applied.len(), 3);
    assert_eq!(index_valid(&db, "notes_body_cc").await, Some(true));
    let rows = ledger(&db).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2].1, 3);
    assert_eq!(
        rows[2].3,
        checksum(BODY_INDEX.sql),
        "checksummed like the others"
    );
    let rerun = run_migrations(&db, &module(&declared), MigrationMode::Apply, soon())
        .await
        .unwrap();
    assert!(rerun.applied.is_empty());
    let verify = run_migrations(&db, &module(&declared), MigrationMode::Verify, soon())
        .await
        .unwrap();
    assert!(verify.is_current());
    // A changed step is drift like any other migration.
    let edited = Migration::non_transactional(
        3,
        "notes_body_concurrently",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS notes_body_cc ON notes (id)",
    );
    let error = run_migrations(
        &db,
        &module(&[CREATE, INDEX, edited]),
        MigrationMode::Verify,
        soon(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.failure.code, "migration_checksum_mismatch");
}

#[tokio::test]
async fn a_step_interrupted_before_its_ledger_row_is_retried_safely() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = Database::connect(&test.config()).unwrap();
    run_migrations(&db, &module(&[CREATE, INDEX]), MigrationMode::Apply, soon())
        .await
        .unwrap();
    // A crash after the statement but before the ledger insert: the index
    // exists, the ledger does not know it.
    let client = db.acquire(soon()).await.unwrap();
    client
        .batch_execute("CREATE INDEX CONCURRENTLY notes_body_cc ON notes (body)")
        .await
        .unwrap();
    drop(client);
    let report = run_migrations(
        &db,
        &module(&[CREATE, INDEX, BODY_INDEX]),
        MigrationMode::Apply,
        soon(),
    )
    .await
    .unwrap();
    assert_eq!(report.applied.len(), 1);
    assert_eq!(ledger(&db).await.len(), 3);
    assert_eq!(index_valid(&db, "notes_body_cc").await, Some(true));
}

#[tokio::test]
async fn a_failed_concurrent_build_is_not_recorded_and_its_invalid_index_is_rebuilt() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = Database::connect(&test.config()).unwrap();
    let declared = [CREATE, DUPLICATES, UNIQUE_BODY];
    let error = run_migrations(&db, &module(&declared), MigrationMode::Apply, soon())
        .await
        .unwrap_err();
    assert_eq!(
        (error.failure.code.as_str(), error.version),
        ("migration_failed", Some(3))
    );
    assert_eq!(ledger(&db).await.len(), 2, "a failed step is not recorded");
    assert_eq!(
        index_valid(&db, "notes_body_unique").await,
        Some(false),
        "the failed concurrent build left an invalid index"
    );
    // Still failing: the invalid index is dropped, rebuilt and fails again.
    let error = run_migrations(&db, &module(&declared), MigrationMode::Apply, soon())
        .await
        .unwrap_err();
    assert_eq!(error.version, Some(3));
    assert_eq!(ledger(&db).await.len(), 2);
    let verify = run_migrations(&db, &module(&declared), MigrationMode::Verify, soon())
        .await
        .unwrap();
    assert_eq!(verify.pending.len(), 1);
    // Fix the data: the retry drops the invalid index and builds a valid one
    // instead of letting IF NOT EXISTS keep the broken one.
    let client = db.acquire(soon()).await.unwrap();
    client
        .batch_execute("DELETE FROM notes WHERE id = 2")
        .await
        .unwrap();
    drop(client);
    let report = run_migrations(&db, &module(&declared), MigrationMode::Apply, soon())
        .await
        .unwrap();
    assert_eq!(report.applied.len(), 1);
    assert_eq!(index_valid(&db, "notes_body_unique").await, Some(true));
    assert_eq!(ledger(&db).await.len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_runners_serialize_non_transactional_steps() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    const SLOW: Migration = Migration::new(
        1,
        "slow_create",
        "SELECT pg_sleep(0.2); CREATE TABLE notes (id bigint PRIMARY KEY, body text NOT NULL);",
    );
    let declared = [SLOW, BODY_INDEX];
    let mut runners = Vec::new();
    for _ in 0..4 {
        let db = Database::connect(&test.config()).unwrap();
        runners.push(tokio::spawn(async move {
            run_migrations(&db, &module(&declared), MigrationMode::Apply, soon())
                .await
                .map(|report| report.applied.len())
        }));
    }
    let mut applied = 0;
    for runner in runners {
        applied += runner.await.unwrap().unwrap();
    }
    assert_eq!(applied, 2, "each migration is applied exactly once");
    let db = Database::connect(&test.config()).unwrap();
    assert_eq!(ledger(&db).await.len(), 2);
    assert_eq!(index_valid(&db, "notes_body_cc").await, Some(true));
}

#[tokio::test]
async fn transactions_hand_out_a_mutable_tx_for_ports() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = Database::connect(&test.config()).unwrap();
    async fn port(tx: &mut merezha_postgres::Tx<'_>) -> Result<i32, merezha_domain::Failure> {
        let row = tx
            .query_one("SELECT 7", &[])
            .await
            .map_err(|e| classify(&e))?;
        Ok(row.get(0))
    }
    let value = db
        .transaction(soon(), |tx| Box::pin(async move { port(tx).await }))
        .await
        .unwrap();
    assert_eq!(value, 7);
}

/// Runs a transaction on either a pooled or a detached connection.
enum Session {
    Pooled(Database),
    Detached(merezha_postgres::DetachedClient),
}
impl Session {
    async fn run<T: Send>(
        &mut self,
        deadline: Instant,
        work: impl for<'t> FnOnce(
            &'t mut merezha_postgres::Tx<'_>,
        )
            -> merezha_postgres::BoxFuture<'t, Result<T, merezha_domain::Failure>>
        + Send,
    ) -> Result<T, merezha_domain::Failure> {
        match self {
            Session::Pooled(db) => db.transaction(deadline, work).await,
            Session::Detached(client) => client.transaction(deadline, work).await,
        }
    }
}

#[tokio::test]
async fn detached_transactions_behave_like_pooled_ones() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = Database::connect(&test.config()).unwrap();
    let setup = db.acquire(soon()).await.unwrap();
    // A deferred constraint trigger that sleeps at commit makes the commit
    // itself outlast the deadline.
    setup
        .batch_execute(
            "CREATE TABLE items (id int);
             CREATE FUNCTION slow_commit() RETURNS trigger LANGUAGE plpgsql AS
               $$ BEGIN PERFORM pg_sleep(3); RETURN NULL; END $$;
             CREATE TABLE slow (id int);
             CREATE CONSTRAINT TRIGGER slow_commit AFTER INSERT ON slow
               DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION slow_commit();",
        )
        .await
        .unwrap();
    drop(setup);
    for detached in [false, true] {
        let mut session = if detached {
            Session::Detached(db.detach(soon()).await.unwrap())
        } else {
            Session::Pooled(db.clone())
        };
        let label = if detached { "detached" } else { "pooled" };
        session
            .run(soon(), |tx| {
                Box::pin(async move {
                    tx.batch_execute("INSERT INTO items VALUES (1)")
                        .await
                        .map_err(|e| classify(&e))
                })
            })
            .await
            .unwrap();
        let error = session
            .run(soon(), |tx| {
                Box::pin(async move {
                    tx.batch_execute("INSERT INTO items VALUES (2)")
                        .await
                        .map_err(|e| classify(&e))?;
                    Err::<(), _>(merezha_domain::Failure::conflict("caller_refused"))
                })
            })
            .await
            .unwrap_err();
        assert_eq!(error.code, "caller_refused", "{label}");
        let timeout: String = session
            .run(Instant::now() + Duration::from_millis(1500), |tx| {
                Box::pin(async move {
                    Ok(tx
                        .query_one("SHOW statement_timeout", &[])
                        .await
                        .map_err(|e| classify(&e))?
                        .get(0))
                })
            })
            .await
            .unwrap();
        let millis: u64 = timeout.trim_end_matches("ms").parse().unwrap_or(0);
        assert!(
            timeout.ends_with("ms") && (1..=1500).contains(&millis),
            "{label}: {timeout}"
        );
        let unique = session
            .run(soon(), |tx| {
                Box::pin(async move {
                    tx.batch_execute(
                        "CREATE TEMP TABLE u (id int PRIMARY KEY); INSERT INTO u VALUES (1), (1)",
                    )
                    .await
                    .map_err(|e| classify(&e))
                })
            })
            .await
            .unwrap_err();
        assert_eq!(
            unique.code, "unique_violation",
            "{label}: same classification"
        );
        let started = Instant::now();
        let late = session
            .run(Instant::now() + Duration::from_millis(200), |tx| {
                Box::pin(async move {
                    tx.batch_execute("SELECT pg_sleep(5)")
                        .await
                        .map_err(|e| classify(&e))
                })
            })
            .await
            .unwrap_err();
        assert_eq!(late.code, "database_timeout", "{label}");
        // Cut long before the 5 s sleep.
        assert!(started.elapsed() < Duration::from_secs(4));
        // A session's state is unknown after a timeout: start a new one.
        let mut session = if detached {
            Session::Detached(db.detach(soon()).await.unwrap())
        } else {
            Session::Pooled(db.clone())
        };
        let started = Instant::now();
        let unknown = session
            .run(Instant::now() + Duration::from_millis(1000), |tx| {
                Box::pin(async move {
                    tx.batch_execute("INSERT INTO slow VALUES (1)")
                        .await
                        .map_err(|e| classify(&e))?;
                    // Start the commit well before the server-side bound.
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    Ok(())
                })
            })
            .await
            .unwrap_err();
        assert_eq!(unknown.code, "commit_outcome_unknown", "{label}");
        // The caller gave up at its deadline, not after the 3 s commit.
        assert!(started.elapsed() < Duration::from_secs(3), "{label}");
    }
    let client = db.acquire(soon()).await.unwrap();
    let rows: i64 = client
        .query_one("SELECT count(*) FROM items", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(rows, 2, "one commit per session kind; refusals rolled back");
}

#[tokio::test]
async fn a_detached_session_keeps_its_lock_across_transactions_and_releases_it_on_drop() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = Database::connect(&test.config()).unwrap();
    let mut holder = db.detach(soon()).await.unwrap();
    let locked: bool = holder
        .query_one("SELECT pg_try_advisory_lock(42)", &[])
        .await
        .unwrap()
        .get(0);
    assert!(locked);
    holder
        .transaction(soon(), |tx| {
            Box::pin(async move { tx.batch_execute("SELECT 1").await.map_err(|e| classify(&e)) })
        })
        .await
        .unwrap();
    let other = db.acquire(soon()).await.unwrap();
    let taken: bool = other
        .query_one("SELECT pg_try_advisory_lock(42)", &[])
        .await
        .unwrap()
        .get(0);
    assert!(!taken, "the detached session still holds the lock");
    drop(holder);
    let mut free = false;
    let closing = Instant::now();
    while closing.elapsed() < Duration::from_secs(10) {
        free = other
            .query_one("SELECT pg_try_advisory_xact_lock(42)", &[])
            .await
            .unwrap()
            .get(0);
        if free {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(free, "dropping the detached client closed its session");
}

#[tokio::test]
async fn detached_sessions_are_bounded_and_leave_their_pool_slot_open() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let mut config = test.config();
    config.pool_size = 1;
    config.max_detached = 1;
    // Long enough for a loaded server to open a connection, short enough
    // to see the bound refuse well before the 20 s deadline.
    config.acquire_timeout = Duration::from_secs(3);
    let db = Database::connect(&config).unwrap();
    let first = db.detach(soon()).await.unwrap();
    // The single pool slot reopened: a pooled client is still available
    // while the detached session lives.
    let pooled = db.acquire(soon()).await.unwrap();
    pooled.simple_query("SELECT 1").await.unwrap();
    drop(pooled);
    let started = Instant::now();
    let error = db.detach(soon()).await.unwrap_err();
    assert_eq!(error.code, "database_busy");
    assert!(started.elapsed() < Duration::from_secs(12));
    // A migration run is a detached session too.
    let error = run_migrations(&db, &module(&[CREATE]), MigrationMode::Apply, soon())
        .await
        .unwrap_err();
    assert_eq!(error.failure.code, "database_busy");
    drop(first);
    let second = db.detach(soon()).await.unwrap();
    drop(second);
    run_migrations(&db, &module(&[CREATE]), MigrationMode::Apply, soon())
        .await
        .unwrap();
    config.max_detached = 0;
    assert_eq!(config.validate(), vec![("max_detached", "out_of_range")]);
}

#[tokio::test]
async fn a_dropped_detached_session_mid_statement_releases_its_lock_within_seconds() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let mut config = test.config();
    config.statement_timeout = Duration::from_secs(60);
    let db = Database::connect(&config).unwrap();
    let holder = db.detach(soon()).await.unwrap();
    let interval: String = holder
        .query_one("SHOW client_connection_check_interval", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(interval, "500ms");
    let busy = tokio::spawn(async move {
        holder
            .batch_execute("SELECT pg_advisory_lock(43); SELECT pg_sleep(30)")
            .await
    });
    let other = db.acquire(soon()).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let held: i64 = other
            .query_one(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND objid = 43",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if held == 1 {
            break;
        }
        assert!(Instant::now() < deadline, "the lock was never taken");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The owner goes away mid-statement, without pg_terminate_backend.
    busy.abort();
    let _ = busy.await;
    // The server checks the connection every 500 ms; 10 s is generous for
    // a loaded one and far below the 30 s sleep.
    let dropped = Instant::now();
    let mut free = false;
    while dropped.elapsed() < Duration::from_secs(10) {
        free = other
            .query_one("SELECT pg_try_advisory_xact_lock(43)", &[])
            .await
            .unwrap()
            .get(0);
        if free {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(free, "the lock outlived its client by more than 10 s");
}

/// Latency and deadline-race tests (issue #37). CI runs them after the
/// parallel suite, alone and one at a time:
/// `cargo test -- --test-threads=1 timing::`.
mod timing {
    use super::*;
    #[tokio::test]
    async fn lock_wait_is_bounded_by_the_deadline() {
        let Some(test) = TestDatabase::create() else {
            return;
        };
        let db = Database::connect(&test.config()).unwrap();
        let holder = db.acquire(soon()).await.unwrap();
        holder
            .execute(
                "SELECT pg_advisory_lock($1)",
                &[&merezha_postgres::MIGRATIONS_LOCK],
            )
            .await
            .unwrap();
        let error = run_migrations(
            &db,
            &module(&[CREATE]),
            MigrationMode::Apply,
            // Room for a loaded server to open the run's session; the
            // race below is the last lock poll against the deadline.
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
        // The poll loop gives up before the run's outer deadline fires.
        assert_eq!(error.failure.code, "migration_lock_timeout", "{error}");
        holder
            .execute(
                "SELECT pg_advisory_unlock($1)",
                &[&merezha_postgres::MIGRATIONS_LOCK],
            )
            .await
            .unwrap();
    }
}
