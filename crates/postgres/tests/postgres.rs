//! Real PostgreSQL behavior. Each test creates and drops its own database
//! through `MEREZHA_TEST_DATABASE_URL` and skips with a message when unset.
use merezha_postgres::{
    Database, Migration, MigrationMode, ModuleMigrations, TestDatabase, checksum, classify,
    run_migrations,
};
use std::time::{Duration, Instant};

const CREATE: Migration = Migration {
    version: 1,
    name: "create_notes",
    sql: "CREATE TABLE notes (id bigint PRIMARY KEY, body text NOT NULL);",
};
const INDEX: Migration = Migration {
    version: 2,
    name: "index_notes",
    sql: "CREATE INDEX notes_body ON notes (body); INSERT INTO notes VALUES (1, 'seed');",
};

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
    const SLOW: Migration = Migration {
        version: 1,
        name: "slow_create",
        sql: "SELECT pg_sleep(0.3); CREATE TABLE once (id int);",
    };
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
    // The lock died with the detached session: a new apply proceeds at once.
    let started = Instant::now();
    run_migrations(&db, &module(&[SLOW]), MigrationMode::Apply, soon())
        .await
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(5));
}

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
        Instant::now() + Duration::from_millis(300),
    )
    .await
    .unwrap_err();
    assert!(
        ["migration_lock_timeout", "migration_timeout"].contains(&error.failure.code.as_str()),
        "{error}"
    );
    holder
        .execute(
            "SELECT pg_advisory_unlock($1)",
            &[&merezha_postgres::MIGRATIONS_LOCK],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn failing_migration_rolls_back_alone() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = Database::connect(&test.config()).unwrap();
    let broken = Migration {
        version: 2,
        name: "broken",
        sql: "CREATE TABLE half (id int); SELECT * FROM missing_table;",
    };
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
    assert!(started.elapsed() < Duration::from_secs(2));
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
