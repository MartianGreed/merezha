//! The outbox, enqueue and the relay against real PostgreSQL (AP-B1).
//! Skips with a message when `MEREZHA_TEST_DATABASE_URL` is unset.
mod support;
use merezha_domain::Failure;
use merezha_jobs::{PgJobs, PgOutbox, RelayMode};
use merezha_platform::{
    AppContext, BoxFuture, Database, Host, HostParts, JobsRuntime, Module, Timeouts,
    ports::{
        DedupeKey, IntegrationEvent, Jobs, KindOptions, LoopbackAllowAll, NewEvent, NewJob,
        OnDuplicate, Outbox, OutboxTarget, Source,
    },
};
use merezha_postgres::TestDatabase;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use support::*;

/// B1 test 1.
#[tokio::test]
async fn an_append_in_a_rolled_back_transaction_leaves_no_record() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let tasks = outbox("tasks");
    let failed: Result<(), Failure> = db
        .transaction(soon(), move |tx| {
            Box::pin(async move {
                let appended = tasks
                    .append(
                        tx,
                        &[job_record("k1", "default", json!({}))],
                        OnDuplicate::Fail,
                    )
                    .await?;
                assert_eq!(appended.inserted, 1);
                Err(Failure::permanent("use_case_failed"))
            })
        })
        .await;
    assert_eq!(failed.unwrap_err().code, "use_case_failed");
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM merezha_jobs.outbox").await,
        0
    );
}

/// B1 test 2. Each wait bound stays far inside the other wake-up source
/// (a ten-minute interval, no `LISTEN`), so what woke the relay is not in
/// doubt however slow the machine; on an idle one both take well under a
/// second.
#[tokio::test]
async fn committed_records_relay_within_a_second_by_notify_and_within_the_interval_without() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    // A ten-minute interval: only the notification can wake the relay.
    let slow = jobs_config(&test, "[jobs.relay]\ninterval_ms = 600000");
    let listening = started(&db, &slow, RelayMode::Listen).await;
    // Wait for the LISTEN session, so the notification cannot be missed.
    wait_for(
        &db,
        "SELECT count(*) FROM pg_stat_activity
          WHERE datname = current_database() AND query LIKE 'LISTEN merezha_jobs_outbox%'",
        1,
        PATIENCE,
    )
    .await;
    append(
        &db,
        &outbox("tasks"),
        vec![job_record("notified", "default", json!({"n": 1}))],
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
    let took = wait_for(
        &db,
        "SELECT count(*) FROM merezha_jobs.jobs WHERE idempotency_key = 'tasks:notified'",
        1,
        PATIENCE,
    )
    .await;
    assert!(
        took < PATIENCE,
        "only the notification can wake it: {took:?}"
    );
    listening.shutdown(soon()).await;

    // Without notifications, the interval relays.
    let fast = jobs_config(&test, "[jobs.relay]\ninterval_ms = 300");
    let polling = started(&db, &fast, RelayMode::Interval).await;
    append(
        &db,
        &outbox("tasks"),
        vec![job_record("polled", "default", json!({"n": 2}))],
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
    let took = wait_for(
        &db,
        "SELECT count(*) FROM merezha_jobs.jobs WHERE idempotency_key = 'tasks:polled'",
        1,
        PATIENCE,
    )
    .await;
    assert!(
        took < PATIENCE,
        "no LISTEN session: the interval woke it: {took:?}"
    );
    polling.shutdown(soon()).await;
    assert!(polling.ready().await.is_err(), "not ready after shutdown");
}

/// B1 test 4.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_relays_on_two_instances_produce_no_duplicate_jobs() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let migrated = database(&test).await;
    // Room for the handshake below inside one statement.
    let mut patient = test.config();
    patient.statement_timeout = Duration::from_secs(20);
    let first_db = Database::connect(&patient).unwrap();
    let second_db = Database::connect(&patient).unwrap();
    drop(migrated);
    // A handshake on the jobs insert of every relay batch: the first batch
    // to arrive waits (at most 15 s) until a second one arrives, which can
    // only be the other relay's, since the first relay is blocked inside
    // it. So both relays take batches whatever the scheduling, and the
    // exactly-once assertions below run over a real overlap. A sequence
    // counts arrivals because it is visible across open transactions.
    execute(&first_db, "CREATE SEQUENCE public.relay_arrivals")
        .await
        .unwrap();
    execute(
        &first_db,
        "CREATE FUNCTION public.relay_handshake() RETURNS trigger LANGUAGE plpgsql AS $$
         DECLARE
             arrival bigint := nextval('public.relay_arrivals');
             waited integer := 0;
         BEGIN
             IF arrival = 1 THEN
                 WHILE (SELECT last_value FROM public.relay_arrivals) < 2 AND waited < 1500 LOOP
                     PERFORM pg_sleep(0.01);
                     waited := waited + 1;
                 END LOOP;
             END IF;
             RETURN NULL;
         END $$",
    )
    .await
    .unwrap();
    execute(
        &first_db,
        "CREATE TRIGGER relay_handshake AFTER INSERT ON merezha_jobs.jobs
             FOR EACH STATEMENT EXECUTE FUNCTION public.relay_handshake()",
    )
    .await
    .unwrap();
    let config = jobs_config(&test, "[jobs.relay]\nbatch = 7");
    let first = PgJobs::with_mode(&config, RelayMode::Off);
    first.ports(&first_db);
    let second = PgJobs::with_mode(&config, RelayMode::Off);
    second.ports(&second_db);
    let tasks = outbox("tasks");
    for chunk in 0..4 {
        let records = (0..100)
            .map(|n| job_record(&format!("r{chunk}-{n}"), "default", json!({"n": n})))
            .collect();
        append(&first_db, &tasks, records, OnDuplicate::Fail)
            .await
            .unwrap();
    }
    let run = |jobs: PgJobs| {
        tokio::spawn(async move {
            let mut relayed = 0;
            for _ in 0..200 {
                relayed += jobs.relay_now().await.unwrap().relayed;
            }
            relayed
        })
    };
    let (a, b) = (run(first.clone()), run(second.clone()));
    let (a, b) = (a.await.unwrap(), b.await.unwrap());
    assert_eq!(a + b, 400, "every record marked exactly once ({a} + {b})");
    assert!(a > 0 && b > 0, "both relays took batches ({a}, {b})");
    assert_eq!(
        scalar(&first_db, "SELECT count(*) FROM merezha_jobs.jobs").await,
        400
    );
    assert_eq!(
        scalar(
            &first_db,
            "SELECT count(*) FROM (SELECT outbox_id FROM merezha_jobs.jobs GROUP BY outbox_id HAVING count(*) > 1) d"
        )
        .await,
        0
    );
    assert_eq!(
        scalar(
            &first_db,
            "SELECT count(*) FROM merezha_jobs.outbox WHERE relayed_at IS NULL"
        )
        .await,
        0
    );
}

/// B1 test 5.
#[tokio::test]
async fn topic_fan_out_gives_one_job_per_subscription_and_counts_unrouted_events() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = jobs_config(&test, "[jobs.queues.\"notify.expand\"]\nmax_attempts = 3");
    let jobs = PgJobs::with_mode(&config, RelayMode::Off);
    jobs.ports(&db);
    let notifications = handle(&jobs, "notifications");
    notifications
        .register(
            "notify.module_event",
            "notify.expand",
            Arc::new(Noop),
            KindOptions::default(),
        )
        .unwrap();
    notifications
        .subscribe("identity.user_registered", "notify.module_event")
        .unwrap();
    let audit = handle(&jobs, "audit");
    audit
        .register(
            "audit.record",
            "default",
            Arc::new(Noop),
            KindOptions::default(),
        )
        .unwrap();
    audit
        .subscribe("identity.user_registered", "audit.record")
        .unwrap();
    // Subscriptions name kinds their source registered, and close at start.
    assert_eq!(
        audit
            .subscribe("identity.user_registered", "notify.module_event")
            .unwrap_err()
            .code,
        "kind_not_registered"
    );
    assert_eq!(
        audit
            .subscribe("not-a-topic", "audit.record")
            .unwrap_err()
            .code,
        "invalid_topic"
    );
    assert_eq!(
        audit
            .register(
                "audit.record",
                "default",
                Arc::new(Noop),
                KindOptions::default()
            )
            .unwrap_err()
            .code,
        "kind_already_registered"
    );
    jobs.start(context(&db)).await.unwrap();
    wait_ready(&jobs).await;
    assert_eq!(
        audit
            .register(
                "audit.late",
                "default",
                Arc::new(Noop),
                KindOptions::default()
            )
            .unwrap_err()
            .code,
        "jobs_started"
    );
    assert_eq!(jobs.registrations().len(), 2);
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM merezha_jobs.subscriptions").await,
        2
    );

    let registered = event(
        "identity",
        "identity.user_registered",
        "usr_1:registered",
        json!({"user_id": "usr_1"}),
    );
    let unrouted = event(
        "identity",
        "identity.user_disabled",
        "usr_1:disabled",
        json!({"user_id": "usr_1"}),
    );
    append(
        &db,
        &outbox("identity"),
        vec![registered, unrouted],
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
    let report = jobs.relay_now().await.unwrap();
    assert_eq!((report.relayed, report.unrouted), (2, 1));
    assert_eq!(jobs.unrouted(), 1);
    let client = db.acquire(soon()).await.unwrap();
    let rows = client
        .query(
            "SELECT j.queue, j.kind, j.idempotency_key, j.max_attempts, o.id
               FROM merezha_jobs.jobs j JOIN merezha_jobs.outbox o ON o.id = j.outbox_id
              ORDER BY j.kind",
            &[],
        )
        .await
        .unwrap();
    let found: Vec<(String, String, String, i32)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect();
    let outbox_id: i64 = rows[0].get(4);
    assert_eq!(
        found,
        vec![
            (
                "default".into(),
                "audit.record".into(),
                format!("outbox:{outbox_id}:audit.record"),
                10
            ),
            (
                "notify.expand".into(),
                "notify.module_event".into(),
                format!("outbox:{outbox_id}:notify.module_event"),
                3
            ),
        ]
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.outbox WHERE relayed_at IS NOT NULL"
        )
        .await,
        2,
        "the unrouted event is marked relayed too"
    );
}

/// B1 test 6.
#[tokio::test]
async fn duplicates_fail_or_are_ignored_and_enqueue_is_idempotent_per_payload() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let tasks = outbox("tasks");
    let first = append(
        &db,
        &tasks,
        vec![
            job_record("dup", "default", json!({})),
            job_record("dup", "default", json!({})),
        ],
        OnDuplicate::Ignore,
    )
    .await
    .unwrap();
    assert_eq!((first.inserted, first.duplicates), (1, 1));
    let again = append(
        &db,
        &tasks,
        vec![
            job_record("dup", "default", json!({})),
            job_record("new", "default", json!({})),
        ],
        OnDuplicate::Ignore,
    )
    .await
    .unwrap();
    assert_eq!((again.inserted, again.duplicates), (1, 1));
    let refused = append(
        &db,
        &tasks,
        vec![
            job_record("other", "default", json!({})),
            job_record("dup", "default", json!({})),
        ],
        OnDuplicate::Fail,
    )
    .await
    .unwrap_err();
    assert_eq!(refused.code, "outbox_duplicate");
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM merezha_jobs.outbox").await,
        2,
        "the failed call rolled back with its transaction"
    );
    // Another source has its own dedupe namespace.
    let content = append(
        &db,
        &outbox("content"),
        vec![job_record("dup", "default", json!({}))],
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
    assert_eq!(content.inserted, 1);

    let config = jobs_config(&test, "");
    let jobs = PgJobs::with_mode(&config, RelayMode::Off);
    jobs.ports(&db);
    let tasks = handle(&jobs, "tasks");
    let job = |payload: Value| NewJob {
        queue: id("default"),
        kind: id("tasks.remind"),
        idempotency_key: DedupeKey::new("remind:t1").unwrap(),
        payload,
        contains_secret: false,
        run_at: None,
        priority: 0,
        context: message(),
    };
    let created = tasks.enqueue(job(json!({"task": "t1"}))).await.unwrap();
    assert!(!created.existed);
    let replayed = tasks.enqueue(job(json!({"task": "t1"}))).await.unwrap();
    assert_eq!(replayed.id, created.id);
    assert!(replayed.existed);
    assert_eq!(
        tasks
            .enqueue(job(json!({"task": "t2"})))
            .await
            .unwrap_err()
            .code,
        "idempotency_mismatch"
    );
    let mut other_kind = job(json!({"task": "t1"}));
    other_kind.kind = id("tasks.other");
    assert_eq!(
        tasks.enqueue(other_kind).await.unwrap_err().code,
        "idempotency_mismatch"
    );
    // The stored key is source-namespaced, the key a Job record relays to.
    let client = db.acquire(soon()).await.unwrap();
    let row = client
        .query_one(
            "SELECT idempotency_key, max_attempts, state FROM merezha_jobs.jobs WHERE id = $1",
            &[&created.id],
        )
        .await
        .unwrap();
    assert_eq!(
        (
            row.get::<_, String>(0),
            row.get::<_, i32>(1),
            row.get::<_, String>(2)
        ),
        ("tasks:remind:t1".into(), 10, "available".into())
    );
    let mut late = job(json!({}));
    late.idempotency_key = DedupeKey::new("late").unwrap();
    late.run_at = Some(jiff::Timestamp::now() + jiff::SignedDuration::from_hours(24 * 366));
    assert_eq!(
        tasks.enqueue(late).await.unwrap_err().code,
        "invalid_run_at"
    );
    assert!(tasks.cancel(created.id).await.unwrap());
    assert!(
        !tasks.cancel(created.id).await.unwrap(),
        "already cancelled"
    );
    assert!(!tasks.cancel(i64::MAX).await.unwrap(), "missing");
}

/// B1 test 7.
#[tokio::test]
async fn flagged_payloads_are_nulled_at_relay_and_the_checks_refuse_them_after() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let mut secret = job_record(
        "identity.magic_link:tok_1",
        "notify.expand",
        json!({"link": "https://app.example/m/t0k3n"}),
    );
    secret.contains_secret = true;
    let plain = job_record("plain", "notify.expand", json!({"n": 1}));
    append(
        &db,
        &outbox("identity"),
        vec![secret, plain],
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
    let jobs = PgJobs::with_mode(&jobs_config(&test, ""), RelayMode::Off);
    jobs.ports(&db);
    assert_eq!(jobs.relay_now().await.unwrap().relayed, 2);
    let client = db.acquire(soon()).await.unwrap();
    let rows = client
        .query(
            "SELECT o.dedupe_key, o.payload IS NULL, j.contains_secret, j.payload::text, j.id
               FROM merezha_jobs.outbox o JOIN merezha_jobs.jobs j ON j.outbox_id = o.id
              ORDER BY o.id",
            &[],
        )
        .await
        .unwrap();
    let flagged = &rows[0];
    assert_eq!(flagged.get::<_, String>(0), "identity.magic_link:tok_1");
    assert!(
        flagged.get::<_, bool>(1),
        "the relayed flagged record lost its payload"
    );
    assert!(flagged.get::<_, bool>(2), "its job inherits the flag");
    assert_eq!(
        flagged.get::<_, String>(3),
        r#"{"link":"https://app.example/m/t0k3n"}"#,
        "the job keeps the payload until its terminal state"
    );
    assert!(
        !rows[1].get::<_, bool>(1),
        "an unflagged record keeps its payload"
    );
    for refused in [
        "UPDATE merezha_jobs.outbox SET payload = '{}' WHERE contains_secret",
        "INSERT INTO merezha_jobs.outbox (source, dedupe_key, record_kind, queue, job_kind, payload,
             contains_secret, correlation_id, relayed_at)
         VALUES ('identity', 'raw', 'job', 'q', 'k', '{}', true, 'c', now())",
        "UPDATE merezha_jobs.outbox SET payload = NULL WHERE NOT contains_secret",
    ] {
        assert_eq!(
            execute(&db, refused).await.unwrap_err().code,
            "constraint_violation",
            "{refused}"
        );
    }
    // Cancelling is terminal: the flagged job loses its payload too.
    let job_id: i64 = flagged.get(4);
    assert!(handle(&jobs, "identity").cancel(job_id).await.unwrap());
    assert_eq!(
        scalar(
            &db,
            &format!(
                "SELECT count(*) FROM merezha_jobs.jobs WHERE id = {job_id} AND payload IS NULL"
            )
        )
        .await,
        1
    );
    assert_eq!(
        execute(
            &db,
            &format!("UPDATE merezha_jobs.jobs SET payload = '{{}}' WHERE id = {job_id}")
        )
        .await
        .unwrap_err()
        .code,
        "constraint_violation"
    );
}

/// B1 test 8.
#[tokio::test]
async fn a_use_case_writes_its_rows_an_event_and_a_job_in_one_transaction() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    execute(&db, "CREATE TABLE users (id text PRIMARY KEY)")
        .await
        .unwrap();
    let identity = outbox("identity");
    let register = |user: &'static str, fail: bool| {
        let identity = identity.clone();
        db.transaction(soon(), move |tx| {
            Box::pin(async move {
                tx.execute("INSERT INTO users VALUES ($1)", &[&user])
                    .await
                    .map_err(|e| merezha_postgres::classify(&e))?;
                let event = event(
                    "identity",
                    "identity.user_registered",
                    &format!("{user}:registered"),
                    json!({"user_id": user, "method": "password"}),
                );
                let mut mail = job_record(
                    &format!("identity.verify_email:{user}"),
                    "notify.expand",
                    json!({"user_id": user}),
                );
                mail.contains_secret = true;
                identity
                    .append(tx, &[event, mail], OnDuplicate::Ignore)
                    .await?;
                if fail {
                    return Err(Failure::permanent("use_case_failed"));
                }
                Ok(())
            })
        })
    };
    assert_eq!(
        register("usr_rolled_back", true).await.unwrap_err().code,
        "use_case_failed"
    );
    assert_eq!(scalar(&db, "SELECT count(*) FROM users").await, 0);
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM merezha_jobs.outbox").await,
        0
    );
    register("usr_01JB2X6Q9F3W", false).await.unwrap();
    assert_eq!(scalar(&db, "SELECT count(*) FROM users").await, 1);
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM merezha_jobs.outbox").await,
        2
    );

    // The envelope round-trips through the relay into a subscriber's job.
    let jobs = PgJobs::with_mode(&jobs_config(&test, ""), RelayMode::Off);
    jobs.ports(&db);
    let notifications = handle(&jobs, "notifications");
    notifications
        .register(
            "notify.module_event",
            "notify.expand",
            Arc::new(Noop),
            KindOptions::default(),
        )
        .unwrap();
    notifications
        .subscribe("identity.user_registered", "notify.module_event")
        .unwrap();
    jobs.start(context(&db)).await.unwrap();
    wait_ready(&jobs).await;
    jobs.relay_now().await.unwrap();
    let client = db.acquire(soon()).await.unwrap();
    let text: String = client
        .query_one(
            "SELECT payload::text FROM merezha_jobs.jobs WHERE kind = 'notify.module_event'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let delivered: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        delivered,
        json!({"event_id": "identity:usr_01JB2X6Q9F3W:registered", "type": "identity.user_registered",
               "version": 1, "occurred_at": "2026-10-24T09:00:00Z", "source": "identity",
               "tenant": "org_1", "correlation_id": "4bf92f3577b34da6a3ce929d0e0e4736",
               "causation_id": null,
               "payload": {"user_id": "usr_01JB2X6Q9F3W", "method": "password"}})
    );
    let envelope = IntegrationEvent::from_value(&delivered).unwrap();
    assert_eq!(envelope.to_value(), delivered);
    assert_eq!(
        envelope.correlation_id.as_str(),
        "4bf92f3577b34da6a3ce929d0e0e4736"
    );

    // An event record must carry the envelope this source would build.
    let mut forged = event(
        "content",
        "identity.user_registered",
        "usr_2:registered",
        json!({}),
    );
    forged.dedupe_key = DedupeKey::new("usr_2:registered").unwrap();
    let mut bare = event("identity", "identity.user_registered", "usr_3", json!({}));
    bare.payload = json!({"user_id": "usr_3"});
    let mut retopic = event("identity", "identity.user_registered", "usr_4", json!({}));
    retopic.target = OutboxTarget::Event {
        topic: id("identity.user_deleted"),
    };
    for record in [forged, bare, retopic] {
        assert_eq!(
            append(&db, &identity, vec![record], OnDuplicate::Fail)
                .await
                .unwrap_err()
                .code,
            "invalid_event_envelope"
        );
    }
    let mut bad_topic = event("identity", "identity", "usr_5", json!({}));
    bad_topic.target = OutboxTarget::Event {
        topic: id("identity"),
    };
    assert_eq!(
        append(&db, &identity, vec![bad_topic], OnDuplicate::Fail)
            .await
            .unwrap_err()
            .code,
        "invalid_topic"
    );
    for (source, queue, code) in [
        ("identity", "Bad", "invalid_queue"),
        ("outbox", "default", "invalid_source"),
        (&*"s".repeat(33), "default", "invalid_source"),
    ] {
        let direct = PgOutbox;
        let record = job_record("x", queue, json!({}));
        let source = source.to_owned();
        let failure = db
            .transaction(soon(), move |tx| {
                Box::pin(async move {
                    direct
                        .append(tx, &source, &[record], OnDuplicate::Fail)
                        .await
                })
            })
            .await
            .unwrap_err();
        assert_eq!(failure.code, code);
    }
}

/// B1 test 9.
#[tokio::test]
async fn long_keys_fail_at_append_and_unroutable_records_are_quarantined_and_requeued() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    assert_eq!(
        DedupeKey::new("k".repeat(161)).unwrap_err().code,
        "invalid_dedupe_key"
    );
    assert_eq!(
        execute(
            &db,
            &format!(
                "INSERT INTO merezha_jobs.outbox (source, dedupe_key, record_kind, queue, job_kind, payload, correlation_id)
                 VALUES ('tasks', '{}', 'job', 'default', 'k', '{{}}', 'c')",
                "k".repeat(161)
            )
        )
        .await
        .unwrap_err()
        .code,
        "constraint_violation"
    );
    let tasks = outbox("tasks");
    append(
        &db,
        &tasks,
        vec![job_record("before", "default", json!({}))],
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
    // Test hook: records the jobs table would refuse, written past the
    // append checks.
    execute(
        &db,
        "INSERT INTO merezha_jobs.outbox (source, dedupe_key, record_kind, queue, job_kind, payload, contains_secret, correlation_id)
         VALUES ('tasks', 'bad-queue', 'job', 'Bad Queue', 'tasks.remind', '{}', false, 'c'),
                ('tasks', 'bad-secret', 'job', 'Bad Queue', 'tasks.remind', '{\"link\":\"x\"}', true, 'c')",
    )
    .await
    .unwrap();
    append(
        &db,
        &tasks,
        vec![job_record("after", "default", json!({}))],
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
    let jobs = PgJobs::with_mode(&jobs_config(&test, ""), RelayMode::Off);
    jobs.ports(&db);
    let report = jobs.relay_now().await.unwrap();
    assert_eq!((report.relayed, report.quarantined), (2, 2));
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM merezha_jobs.jobs").await,
        2,
        "the rest of the batch relayed"
    );
    let client = db.acquire(soon()).await.unwrap();
    let quarantined = client
        .query(
            "SELECT id, quarantine_code, payload IS NULL FROM merezha_jobs.outbox
              WHERE quarantined_at IS NOT NULL ORDER BY id",
            &[],
        )
        .await
        .unwrap();
    let (plain, flagged): (i64, i64) = (quarantined[0].get(0), quarantined[1].get(0));
    assert_eq!(quarantined[0].get::<_, String>(1), "unroutable");
    assert!(!quarantined[0].get::<_, bool>(2));
    assert!(
        quarantined[1].get::<_, bool>(2),
        "a flagged quarantined record loses its payload"
    );

    jobs.requeue_outbox(plain, Some("usr_admin")).await.unwrap();
    assert_eq!(
        scalar(
            &db,
            &format!("SELECT count(*) FROM merezha_jobs.outbox WHERE id = {plain} AND quarantined_at IS NULL AND relayed_at IS NULL")
        )
        .await,
        1,
        "requeued records are pending again"
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.admin_actions WHERE action = 'outbox_requeue' AND principal = 'usr_admin'"
        )
        .await,
        1
    );
    let refused = |id: i64| {
        let jobs = jobs.clone();
        async move { jobs.requeue_outbox(id, None).await.unwrap_err() }
    };
    let purged = refused(flagged).await;
    assert_eq!(
        (purged.code.as_str(), purged.classification),
        ("secret_purged", merezha_domain::Classification::Conflict)
    );
    assert_eq!(refused(plain).await.code, "not_quarantined");
    assert_eq!(refused(i64::MAX).await.code, "not_found");

    // Any other permanent failure is isolated record by record: an event
    // whose subscription names a queue the jobs table refuses.
    execute(
        &db,
        "INSERT INTO merezha_jobs.subscriptions (topic, job_kind, queue, source)
         VALUES ('tasks.task_broken', 'tasks.fan', 'Bad Queue', 'tasks')",
    )
    .await
    .unwrap();
    append(
        &db,
        &tasks,
        vec![
            job_record("one", "default", json!({})),
            event("tasks", "tasks.task_broken", "broken", json!({})),
            job_record("two", "default", json!({})),
        ],
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
    let report = jobs.relay_now().await.unwrap();
    assert_eq!(
        report.quarantined, 2,
        "the requeued record and the broken event"
    );
    assert_eq!(report.relayed, 2);
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.outbox WHERE dedupe_key = 'broken' AND quarantine_code = 'constraint_violation'"
        )
        .await,
        1
    );
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM merezha_jobs.jobs").await,
        4
    );
}

/// A block on the host: its outbox handle is the PostgreSQL outbox, its
/// registrations and subscriptions are in before the jobs runtime starts.
#[derive(Default)]
struct Notes;
impl Module for Notes {
    fn name(&self) -> &'static str {
        "notes"
    }
    fn start(&self, ctx: AppContext) -> BoxFuture<'_, Result<(), Failure>> {
        Box::pin(async move {
            let jobs = ctx.jobs("notes")?;
            jobs.register(
                "notes.index",
                "default",
                Arc::new(Noop),
                KindOptions::default(),
            )?;
            jobs.subscribe("notes.note_created", "notes.index")?;
            assert_eq!(ctx.jobs("identity").unwrap_err().code, "source_not_allowed");
            let outbox = ctx.outbox("notes")?;
            let record = outbox.event(
                id("notes.note_created"),
                NewEvent {
                    dedupe_key: DedupeKey::new("n1:created")?,
                    version: 1,
                    occurred_at: jiff::Timestamp::now(),
                    payload: json!({"note_id": "n1"}),
                    context: message(),
                },
            );
            ctx.database()
                .transaction(soon(), move |tx| {
                    Box::pin(async move {
                        outbox.append(tx, &[record], OnDuplicate::Fail).await?;
                        Ok(())
                    })
                })
                .await
        })
    }
}

#[tokio::test]
async fn the_host_installs_the_jobs_runtime_after_every_module() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let config = project(&test, "");
    let jobs = Arc::new(PgJobs::with_mode(&config.jobs, RelayMode::Listen));
    let host = Host::start(
        config,
        vec![Box::new(Notes)],
        HostParts {
            routes: axum::Router::new(),
            authorizer: Arc::new(LoopbackAllowAll::new("127.0.0.1:0".parse().unwrap()).unwrap()),
            timeouts: Timeouts::default(),
            jobs: Some(jobs.clone()),
        },
    )
    .await
    .unwrap();
    assert!(
        host.migrations()
            .applied
            .iter()
            .any(|m| m.module == "jobs" && m.version == 1)
    );
    wait_ready(&jobs).await;
    let db = Database::connect(&test.config()).unwrap();
    // The event appended at the notes module's start relays to its own
    // subscription.
    wait_for(
        &db,
        "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notes.index'",
        1,
        PATIENCE,
    )
    .await;
    assert_eq!(
        host.errors()
            .status_of("not_quarantined")
            .map(|s| s.as_u16()),
        Some(409)
    );
}

#[tokio::test]
async fn without_the_runtime_the_jobs_port_refuses() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = Database::connect(&test.config()).unwrap();
    let ctx = context(&db);
    let jobs = ctx.jobs("tasks").unwrap();
    assert_eq!(
        jobs.register("k", "default", Arc::new(Noop), KindOptions::default())
            .unwrap_err()
            .code,
        "jobs_unavailable"
    );
    assert_eq!(jobs.cancel(1).await.unwrap_err().code, "jobs_unavailable");
    let unavailable: &dyn Jobs = &merezha_platform::ports::UnavailableJobs;
    assert_eq!(
        unavailable
            .subscribe(&Source::new("tasks").unwrap(), &id("a.b"), &id("k"))
            .unwrap_err()
            .code,
        "jobs_unavailable"
    );
    assert_eq!(
        jobs.schedule(merezha_platform::ports::ScheduleDeclaration {
            name: "tasks.daily".into(),
            cron: "0 9 * * *".into(),
            timezone: "UTC".into(),
            queue: id("default"),
            kind: id("k"),
            payload: serde_json::json!({}),
        })
        .unwrap_err()
        .code,
        "jobs_unavailable"
    );
}
