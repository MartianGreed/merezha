//! PostgreSQL telemetry (TE §1.3 "PostgreSQL", §8 "DB spans", T4) against a
//! real server, through the telemetry harness: statement spans and
//! `db.client.operation.duration`, transaction and migration spans, pool
//! gauges, wait time and timeouts, listener reconnects, the pending
//! migrations gauge, and the canary: a unique violation quoting a planted
//! value never puts the value or the SQL text in a span, log or metric.
use merezha_postgres::{
    Database, METER_SCOPE, Migration, MigrationMode, ModuleMigrations, Statement, TestDatabase,
    classify, run_migrations, statement::KERNEL_STATEMENTS,
};
use merezha_telemetry::testing::Harness;
use opentelemetry::trace::{SpanKind, Status};
use opentelemetry_sdk::trace::SpanData;
use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};
use tracing::Instrument;

const CANARY: &str = "pg-cAnArY-7731";

const CREATE: Migration = Migration::new(
    1,
    "create_items",
    "CREATE SCHEMA canary; CREATE TABLE canary.items (id bigserial PRIMARY KEY, body text NOT NULL UNIQUE);",
);
const INSERT: Statement = Statement::new(
    "canary.items.insert",
    "INSERT canary.items",
    "INSERT INTO canary.items (body) VALUES ($1)",
);
const INSERT_TEXT: Statement = Statement::new(
    "canary.items.insert_text",
    "INSERT canary.items",
    "INSERT INTO canary.items (body) VALUES ('<body>')",
);
const MISSING: Statement = Statement::new(
    "canary.missing.read",
    "SELECT canary.missing",
    "SELECT * FROM canary.missing",
);
const SLEEP: Statement = Statement::new("canary.sleep", "SELECT pg_sleep()", "SELECT pg_sleep(5)");

fn soon() -> Instant {
    Instant::now() + Duration::from_secs(20)
}
/// A caller span the span bridge records (a `merezha*` target).
fn caller() -> tracing::Span {
    tracing::info_span!(target: "merezha_test", "caller")
}
fn database(test: &TestDatabase, harness: &Harness) -> Database {
    Database::connect_with_meter(
        &test.config(),
        &harness.telemetry().meters().meter(METER_SCOPE),
    )
    .unwrap()
}
fn attribute(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| kv.value.to_string())
}
fn client_spans(harness: &Harness) -> Vec<SpanData> {
    harness
        .spans()
        .into_iter()
        .filter(|s| s.span_kind == SpanKind::Client)
        .collect()
}
async fn migrate(db: &Database) {
    run_migrations(
        db,
        &[ModuleMigrations {
            module: "canary",
            migrations: &[CREATE],
        }],
        MigrationMode::Apply,
        soon(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn a_unique_violation_carries_its_sqlstate_but_never_the_value_or_the_sql() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test, &harness).for_module("canary");
    migrate(&db).await;
    let client = db.acquire(soon()).await.unwrap();
    db.execute(&client, &INSERT, &[&CANARY])
        .instrument(caller())
        .await
        .unwrap();
    let outcome = async {
        let inserted = db.clone();
        db.transaction(soon(), move |tx| {
            Box::pin(async move {
                inserted
                    .execute(tx, &INSERT, &[&CANARY])
                    .await
                    .map_err(|e| classify(&e))?;
                Ok(())
            })
        })
        .await
    }
    .instrument(caller())
    .await;
    assert_eq!(outcome.unwrap_err().code, "unique_violation");
    // Text built at run time with the value inline fails the same way.
    let text = format!("INSERT INTO canary.items (body) VALUES ('{CANARY}')");
    let inline = db
        .batch_text(&client, &INSERT_TEXT, &text)
        .instrument(caller())
        .await;
    assert_eq!(classify(&inline.unwrap_err()).code, "unique_violation");
    // A statement the server refuses as invalid is a host failure.
    let missing = db.query(&client, &MISSING, &[]).instrument(caller()).await;
    assert_eq!(classify(&missing.unwrap_err()).code, "database_error");
    drop(client);

    let spans = client_spans(&harness);
    let failed: Vec<&SpanData> = spans
        .iter()
        .filter(|s| s.name == "INSERT canary.items")
        .collect();
    assert_eq!(failed.len(), 3, "two parameterized inserts and one inline");
    let violation = failed
        .iter()
        .find(|s| {
            attribute(s, "db.response.status_code").is_some()
                && attribute(s, "merezha.db.statement").as_deref() == Some("canary.items.insert")
        })
        .unwrap();
    for (key, value) in [
        ("db.system.name", "postgresql"),
        ("db.operation.name", "INSERT"),
        ("db.collection.name", "canary.items"),
        ("db.query.summary", "INSERT canary.items"),
        ("merezha.db.statement", "canary.items.insert"),
        ("merezha.module", "canary"),
        ("db.response.status_code", "23505"),
        ("error.type", "unique_violation"),
    ] {
        assert_eq!(attribute(violation, key).as_deref(), Some(value), "{key}");
    }
    // A conflict is the caller's outcome: the span status stays unset.
    assert_eq!(violation.status, Status::Unset);
    let refused = spans
        .iter()
        .find(|s| s.name == "SELECT canary.missing")
        .unwrap();
    assert_eq!(
        attribute(refused, "db.response.status_code").as_deref(),
        Some("42P01")
    );
    assert!(matches!(refused.status, Status::Error { .. }));
    // Statements inside the transaction are children of `transaction canary`,
    // itself a child of the caller.
    let transaction = harness.span("transaction canary");
    assert_eq!(transaction.span_kind, SpanKind::Internal);
    assert!(attribute(&transaction, "merezha.db.pool.wait").is_some());
    assert_eq!(
        attribute(&transaction, "merezha.module").as_deref(),
        Some("canary")
    );
    assert_eq!(violation.parent_span_id, transaction.span_context.span_id());
    let callers: Vec<_> = harness
        .spans()
        .into_iter()
        .filter(|s| s.name == "caller")
        .map(|s| s.span_context.span_id())
        .collect();
    assert!(callers.contains(&transaction.parent_span_id));
    for name in ["BEGIN", "SET"] {
        let span = spans.iter().find(|s| s.name == name).unwrap();
        assert_eq!(
            span.parent_span_id,
            transaction.span_context.span_id(),
            "{name}"
        );
    }

    let duration = harness.metric("db.client.operation.duration");
    assert_eq!(
        duration.value(&[
            ("db.system.name", "postgresql"),
            ("db.operation.name", "INSERT"),
            ("db.collection.name", "canary.items"),
            ("db.response.status_code", "23505"),
            ("error.type", "unique_violation"),
            ("merezha.module", "canary"),
        ]),
        Some(2.0),
        "the insert in the transaction and the inline one"
    );
    let succeeded: Vec<f64> = duration
        .points()
        .iter()
        .filter(|p| {
            p.attributes.get("db.collection.name").map(String::as_str) == Some("canary.items")
                && !p.attributes.contains_key("error.type")
        })
        .map(|p| p.value)
        .collect();
    assert_eq!(
        succeeded,
        vec![1.0],
        "the first insert succeeded: its point has no error attributes"
    );
    for point in duration.points() {
        let keys: BTreeSet<&str> = point.attributes.keys().map(String::as_str).collect();
        let allowed: BTreeSet<&str> =
            merezha_telemetry::names::metric::DB_CLIENT_OPERATION_DURATION
                .attributes
                .iter()
                .copied()
                .collect();
        assert!(keys.is_subset(&allowed), "{keys:?}");
    }

    harness.assert_catalogued();
    harness.assert_no_canary(CANARY);
    for sql in [
        INSERT.sql,
        MISSING.sql,
        text.as_str(),
        "INSERT INTO",
        "VALUES",
    ] {
        harness.assert_no_canary(sql);
    }
}

#[tokio::test]
async fn a_failed_concurrent_index_build_on_planted_duplicates_leaks_nothing() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test, &harness);
    let create = Migration::new(
        1,
        "create",
        "CREATE TABLE dup (body text NOT NULL); INSERT INTO dup VALUES ('pg-cAnArY-7731'), ('pg-cAnArY-7731');",
    );
    let unique = Migration::non_transactional(
        2,
        "unique_body",
        "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS dup_body ON dup (body)",
    );
    let declared = [create, unique];
    let modules = [ModuleMigrations {
        module: "dup",
        migrations: &declared,
    }];
    for _ in 0..2 {
        let error = run_migrations(&db, &modules, MigrationMode::Apply, soon())
            .instrument(caller())
            .await
            .unwrap_err();
        assert_eq!(error.failure.code, "migration_failed");
    }
    let step = client_spans(&harness)
        .into_iter()
        .find(|s| attribute(s, "merezha.db.statement").as_deref() == Some("kernel.migration.step"))
        .unwrap();
    assert_eq!(
        attribute(&step, "db.response.status_code").as_deref(),
        Some("23505")
    );
    let migrate = harness.span("migrate dup");
    assert!(matches!(migrate.status, Status::Error { .. }));
    // The retry dropped the invalid index first, and logged it.
    assert!(
        harness
            .stderr_lines()
            .iter()
            .any(|l| l["event"] == "migration.invalid_index_dropped")
    );
    harness.assert_catalogued();
    harness.assert_no_canary(CANARY);
    harness.assert_no_canary("CREATE UNIQUE INDEX");
}

#[tokio::test]
async fn statements_without_a_caller_span_are_measured_but_open_no_span() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test, &harness);
    db.ping(soon()).await.unwrap();
    db.transaction(soon(), |_| Box::pin(async { Ok(()) }))
        .await
        .unwrap();
    assert!(harness.spans().is_empty(), "{:?}", harness.spans());
    let duration = harness.metric("db.client.operation.duration");
    assert_eq!(
        duration.value(&[
            ("db.operation.name", "SELECT"),
            ("merezha.module", "kernel")
        ]),
        Some(1.0)
    );
    for operation in ["BEGIN", "SET", "COMMIT"] {
        assert_eq!(
            duration.value(&[("db.operation.name", operation)]),
            Some(1.0),
            "{operation}"
        );
    }
}

#[tokio::test]
async fn a_cancelled_statement_counts_as_cancelled() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test, &harness);
    let client = db.acquire(soon()).await.unwrap();
    let sleep = db.query(&client, &SLEEP, &[]).instrument(caller());
    assert!(
        tokio::time::timeout(Duration::from_millis(200), sleep)
            .await
            .is_err()
    );
    let span = harness.span("SELECT pg_sleep()");
    assert_eq!(attribute(&span, "error.type").as_deref(), Some("cancelled"));
    assert_eq!(attribute(&span, "db.collection.name"), None);
    assert_eq!(
        harness
            .metric("db.client.operation.duration")
            .value(&[("error.type", "cancelled")]),
        Some(1.0)
    );
}

#[tokio::test]
async fn pool_gauges_follow_pool_status_and_timeouts_are_counted_and_logged() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let mut config = test.config();
    config.pool_size = 2;
    config.acquire_timeout = Duration::from_millis(300);
    let db =
        Database::connect_with_meter(&config, &harness.telemetry().meters().meter(METER_SCOPE))
            .unwrap();
    let first = db.acquire(soon()).await.unwrap();
    let second = db.acquire(soon()).await.unwrap();
    drop(second);
    let gauge = |name: &str, state: Option<&str>| {
        let mut attributes = vec![("db.client.connection.pool.name", "merezha")];
        if let Some(state) = state {
            attributes.push(("db.client.connection.state", state));
        }
        harness.metric(name).value(&attributes)
    };
    let status = db.status();
    assert_eq!((status.size, status.available), (2, 1));
    assert_eq!(gauge("db.client.connection.count", Some("used")), Some(1.0));
    assert_eq!(gauge("db.client.connection.count", Some("idle")), Some(1.0));
    assert_eq!(gauge("db.client.connection.max", None), Some(2.0));
    assert_eq!(
        gauge("db.client.connection.pending_requests", None),
        Some(0.0)
    );
    // Exhaust the pool: one waiter is pending, then gives up.
    let second = db.acquire(soon()).await.unwrap();
    let waiter = {
        let db = db.clone();
        tokio::spawn(async move { db.acquire(soon()).await.map(drop) })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(db.status().waiting, 1);
    assert_eq!(
        gauge("db.client.connection.pending_requests", None),
        Some(1.0)
    );
    assert_eq!(gauge("db.client.connection.count", Some("used")), Some(2.0));
    assert_eq!(waiter.await.unwrap().unwrap_err().code, "database_busy");
    // A second timeout within 10 s counts but is not logged again.
    assert_eq!(db.acquire(soon()).await.unwrap_err().code, "database_busy");
    assert_eq!(gauge("db.client.connection.timeouts", None), Some(2.0));
    assert_eq!(gauge("db.client.connection.wait_time", None), Some(3.0));
    let logged: Vec<_> = harness
        .stderr_lines()
        .into_iter()
        .filter(|l| l["event"] == "db.pool_timeout")
        .collect();
    assert_eq!(logged.len(), 1);
    assert_eq!(logged[0]["level"], "warn");
    assert_eq!(logged[0]["code"], "database_busy");
    drop((first, second));
    harness.assert_catalogued();
}

#[tokio::test]
async fn a_listener_reconnect_after_a_lost_session_is_counted_once() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test, &harness);
    let reconnects = || {
        harness
            .metric("merezha.db.listener.reconnects")
            .points()
            .iter()
            .map(|p| p.value)
            .sum::<f64>()
    };
    let mut listener = db.listen(&["probe_events"], soon()).await.unwrap();
    // Another session on other channels, and a second one on the same
    // channels while the first is alive, are not reconnects.
    let _other = db.listen(&["probe_other"], soon()).await.unwrap();
    drop(db.listen(&["probe_events"], soon()).await.unwrap());
    listener.check(soon()).await.unwrap();
    assert_eq!(reconnects(), 0.0);
    test.admin_execute(&format!(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = '{}'",
        test.name()
    ));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), listener.recv())
            .await
            .unwrap(),
        None
    );
    let listener = db.listen(&["probe_events"], soon()).await.unwrap();
    assert_eq!(reconnects(), 1.0);
    drop(db.listen(&["probe_events"], soon()).await.unwrap());
    assert_eq!(reconnects(), 1.0, "only the session after the loss counts");
    // A failed probe also marks the session lost.
    test.admin_execute(&format!(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = '{}'",
        test.name()
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(listener.check(soon()).await.is_err());
    drop(db.listen(&["probe_events"], soon()).await.unwrap());
    assert_eq!(reconnects(), 2.0);
}

#[tokio::test]
async fn migrations_trace_each_module_and_report_pending_counts() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test, &harness);
    let notes = [
        Migration::new(1, "create", "CREATE TABLE notes (id int);"),
        Migration::new(2, "index", "CREATE INDEX notes_id ON notes (id);"),
    ];
    let tags = [Migration::new(1, "create", "CREATE TABLE tags (id int);")];
    let modules = [
        ModuleMigrations {
            module: "notes",
            migrations: &notes,
        },
        ModuleMigrations {
            module: "tags",
            migrations: &tags,
        },
    ];
    let pending = |module: &str| {
        harness
            .metric("merezha.db.migrations.pending")
            .value(&[("merezha.module", module)])
    };
    run_migrations(&db, &modules, MigrationMode::Verify, soon())
        .await
        .unwrap();
    assert_eq!((pending("notes"), pending("tags")), (Some(2.0), Some(1.0)));
    run_migrations(&db, &modules, MigrationMode::Apply, soon())
        .instrument(caller())
        .await
        .unwrap();
    assert_eq!((pending("notes"), pending("tags")), (Some(0.0), Some(0.0)));
    let caller_id = harness.span("caller").span_context.span_id();
    for (module, applied) in [("notes", 2), ("tags", 1)] {
        let span = harness.span(&format!("migrate {module}"));
        assert_eq!(span.parent_span_id, caller_id);
        assert_eq!(attribute(&span, "merezha.module").as_deref(), Some(module));
        assert_eq!(span.status, Status::Unset);
        let id = span.span_context.span_id();
        let children: Vec<_> = client_spans(&harness)
            .into_iter()
            .filter(|s| s.parent_span_id == id)
            .collect();
        let bodies = children.iter().filter(|s| s.name == "MIGRATE").count();
        assert_eq!(bodies, applied, "{module}");
        assert!(
            children
                .iter()
                .all(|s| attribute(s, "merezha.module").as_deref() == Some(module))
        );
    }
    let applied: Vec<_> = harness
        .stderr_lines()
        .into_iter()
        .filter(|l| l["event"] == "migration.applied")
        .collect();
    assert_eq!(applied.len(), 3);
    harness.assert_catalogued();
}

/// Every statement the kernel runs is registered: driving every kernel
/// path inside a caller span yields client spans for registered kernel
/// statements only, and every registered kernel statement is exercised.
#[tokio::test]
async fn every_kernel_statement_is_registered_and_exercised() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test, &harness);
    let create = Migration::new(
        1,
        "create",
        "CREATE TABLE dup (body text NOT NULL); INSERT INTO dup VALUES ('a'), ('a');",
    );
    let unique = Migration::non_transactional(
        2,
        "unique_body",
        "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS dup_body ON dup (body)",
    );
    let declared = [create, unique];
    let modules = [ModuleMigrations {
        module: "dup",
        migrations: &declared,
    }];
    async {
        db.ping(soon()).await.unwrap();
        db.transaction(soon(), |_| Box::pin(async { Ok(()) }))
            .await
            .unwrap();
        let mut detached = db.detach(soon()).await.unwrap();
        detached
            .transaction(soon(), |_| Box::pin(async { Ok(()) }))
            .await
            .unwrap();
        drop(detached);
        let listener = db.listen(&["probe_events"], soon()).await.unwrap();
        listener.check(soon()).await.unwrap();
        run_migrations(&db, &modules, MigrationMode::Verify, soon())
            .await
            .unwrap();
        // The failed build leaves an invalid index; the retry drops it.
        for _ in 0..2 {
            run_migrations(&db, &modules, MigrationMode::Apply, soon())
                .await
                .unwrap_err();
        }
    }
    .instrument(caller())
    .await;
    let registered: BTreeSet<&str> = KERNEL_STATEMENTS.iter().map(|s| s.name).collect();
    let seen: BTreeSet<String> = client_spans(&harness)
        .iter()
        .map(|s| attribute(s, "merezha.db.statement").expect("every client span is registered"))
        .collect();
    let seen: BTreeSet<&str> = seen.iter().map(String::as_str).collect();
    assert_eq!(seen, registered);
    let summaries: BTreeSet<&str> = KERNEL_STATEMENTS.iter().map(|s| s.summary).collect();
    for span in client_spans(&harness) {
        assert!(summaries.contains(&*span.name), "{}", span.name);
    }
}

#[tokio::test]
async fn detached_transactions_are_traced_and_failures_set_the_transaction_status() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test, &harness).for_module("probe");
    async {
        let mut detached = db.detach(soon()).await.unwrap();
        let session = db.clone();
        detached
            .transaction(soon(), move |tx| {
                Box::pin(async move {
                    session
                        .query_one(tx, &PROBE, &[])
                        .await
                        .map_err(|e| classify(&e))?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        // A host failure inside the work marks the transaction span.
        let failed = detached
            .transaction(soon(), |_| {
                Box::pin(async { Err::<(), _>(merezha_domain::Failure::transient("probe_lost")) })
            })
            .await;
        assert_eq!(failed.unwrap_err().code, "probe_lost");
        // A domain rejection leaves it unset.
        let rejected = db
            .transaction(soon(), |_| {
                Box::pin(async { Err::<(), _>(merezha_domain::Failure::permanent("rejected")) })
            })
            .await;
        assert_eq!(rejected.unwrap_err().code, "rejected");
    }
    .instrument(caller())
    .await;
    let transactions: Vec<SpanData> = harness
        .spans()
        .into_iter()
        .filter(|s| s.name == "transaction probe")
        .collect();
    assert_eq!(transactions.len(), 3);
    let caller_id = harness.span("caller").span_context.span_id();
    assert!(transactions.iter().all(|t| t.parent_span_id == caller_id));
    let succeeded = &transactions[0];
    assert_eq!(succeeded.status, Status::Unset);
    let children: Vec<String> = client_spans(&harness)
        .into_iter()
        .filter(|s| s.parent_span_id == succeeded.span_context.span_id())
        .map(|s| s.name.to_string())
        .collect();
    assert_eq!(children, ["BEGIN", "SET", "SELECT", "COMMIT"]);
    assert!(matches!(transactions[1].status, Status::Error { .. }));
    assert_eq!(transactions[2].status, Status::Unset);
    // Every statement finished: none is recorded as cancelled.
    let duration = harness.metric("db.client.operation.duration");
    assert_eq!(duration.value(&[("error.type", "cancelled")]), None);
    assert!(
        client_spans(&harness)
            .iter()
            .all(|s| attribute(s, "error.type").is_none())
    );
}

const PROBE: Statement = Statement::new("canary.probe", "SELECT", "SELECT 1");

#[tokio::test]
async fn a_caller_span_the_bridge_does_not_record_opens_no_db_span() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test, &harness);
    let foreign = tracing::info_span!(target: "elsewhere", "foreign");
    let quiet = tracing::debug_span!(target: "merezha_test", "quiet");
    for span in [foreign, quiet] {
        async {
            db.ping(soon()).await.unwrap();
            db.transaction(soon(), |_| Box::pin(async { Ok(()) }))
                .await
                .unwrap();
        }
        .instrument(span)
        .await;
    }
    assert!(harness.spans().is_empty(), "{:?}", harness.spans());
    assert_eq!(
        harness
            .metric("db.client.operation.duration")
            .value(&[("db.operation.name", "SELECT")]),
        Some(2.0)
    );
}

#[tokio::test]
async fn detached_and_listener_sessions_are_reported_as_their_own_pools() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let mut config = test.config();
    config.max_detached = 2;
    config.acquire_timeout = Duration::from_millis(300);
    let db =
        Database::connect_with_meter(&config, &harness.telemetry().meters().meter(METER_SCOPE))
            .unwrap();
    let gauge = |name: &str, pool: &str, state: Option<&str>| {
        let mut attributes = vec![("db.client.connection.pool.name", pool)];
        if let Some(state) = state {
            attributes.push(("db.client.connection.state", state));
        }
        harness.metric(name).value(&attributes)
    };
    let count = "db.client.connection.count";
    assert_eq!(
        gauge("db.client.connection.max", "merezha.detached", None),
        Some(2.0)
    );
    assert_eq!(gauge(count, "merezha.detached", Some("used")), Some(0.0));
    assert_eq!(gauge(count, "merezha.listen", Some("used")), Some(0.0));
    let first = db.detach(soon()).await.unwrap();
    let second = db.detach(soon()).await.unwrap();
    let listener = db.listen(&["probe_events"], soon()).await.unwrap();
    assert_eq!(gauge(count, "merezha.detached", Some("used")), Some(2.0));
    assert_eq!(gauge(count, "merezha.listen", Some("used")), Some(1.0));
    // A third detach waits for a slot, then gives up.
    let waiter = {
        let db = db.clone();
        tokio::spawn(async move { db.detach(soon()).await.map(drop) })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        gauge(
            "db.client.connection.pending_requests",
            "merezha.detached",
            None
        ),
        Some(1.0)
    );
    assert_eq!(waiter.await.unwrap().unwrap_err().code, "database_busy");
    assert_eq!(
        gauge(
            "db.client.connection.pending_requests",
            "merezha.detached",
            None
        ),
        Some(0.0)
    );
    assert_eq!(
        gauge("db.client.connection.timeouts", "merezha.detached", None),
        Some(1.0)
    );
    assert_eq!(
        gauge("db.client.connection.wait_time", "merezha.detached", None),
        Some(2.0)
    );
    drop((first, second, listener));
    assert_eq!(gauge(count, "merezha.detached", Some("used")), Some(0.0));
    assert_eq!(gauge(count, "merezha.listen", Some("used")), Some(0.0));
}

const SELECT_TEXT: Statement = Statement::new(
    "canary.items.select_text",
    "SELECT canary.items",
    "SELECT body FROM canary.items WHERE body = $1 <and conditions built per call>",
);

#[tokio::test]
async fn text_built_at_run_time_runs_with_parameters_under_its_registered_name() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let harness = Harness::new();
    let db = database(&test, &harness).for_module("canary");
    migrate(&db).await;
    let client = db.acquire(soon()).await.unwrap();
    db.execute(&client, &INSERT, &[&CANARY]).await.unwrap();
    // The text varies per call; the value stays a parameter.
    let text = "SELECT body FROM canary.items WHERE body = $1 AND id > $2";
    let rows = db
        .query_text(&client, &SELECT_TEXT, text, &[&CANARY, &0i64])
        .instrument(caller())
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), CANARY);
    let refused = db
        .query_text(&client, &SELECT_TEXT, "SELECT * FROM canary.missing", &[])
        .instrument(caller())
        .await;
    assert_eq!(classify(&refused.unwrap_err()).code, "database_error");
    drop(client);
    let spans: Vec<SpanData> = client_spans(&harness)
        .into_iter()
        .filter(|s| s.name == "SELECT canary.items")
        .collect();
    assert_eq!(spans.len(), 2);
    for span in &spans {
        assert_eq!(
            attribute(span, "merezha.db.statement").as_deref(),
            Some("canary.items.select_text")
        );
        let text = format!("{span:?}");
        assert!(!text.contains("id > $2") && !text.contains("canary.missing"));
        assert!(!text.contains(CANARY));
    }
    let duration = harness.metric("db.client.operation.duration");
    assert_eq!(
        duration.value(&[
            ("db.collection.name", "canary.items"),
            ("db.operation.name", "SELECT"),
            ("merezha.module", "canary"),
        ]),
        Some(1.0)
    );
}
