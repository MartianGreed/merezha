//! Cancel scoping, systemic relay failures, isolation payloads, the LISTEN
//! session's recovery, enqueue wake-ups, subscription deactivation and
//! sync failure logging, against real PostgreSQL.
mod support;
use merezha_jobs::{JOBS_CHANNEL, PgJobs, RelayMode};
use merezha_platform::{
    JobsRuntime, Module,
    ports::{DedupeKey, KindOptions, NewJob, OnDuplicate},
};
use merezha_postgres::{Database, TestDatabase};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use support::*;

fn new_job(key: &str, payload: Value) -> NewJob {
    NewJob {
        queue: id("default"),
        kind: id("tasks.remind"),
        idempotency_key: DedupeKey::new(key).unwrap(),
        payload,
        contains_secret: true,
        run_at: None,
        priority: 0,
        context: message(),
    }
}
async fn state(db: &Database, job: i64) -> (String, bool) {
    let client = db.acquire(soon()).await.unwrap();
    let row = client
        .query_one(
            "SELECT state, payload IS NULL FROM merezha_jobs.jobs WHERE id = $1",
            &[&job],
        )
        .await
        .unwrap();
    (row.get(0), row.get(1))
}

/// Everything the log subscriber writes, as text.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for Captured {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl Captured {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
    fn install(&self) -> tracing::subscriber::DefaultGuard {
        let writer = self.clone();
        tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .json()
                .with_writer(move || writer.clone())
                .finish(),
        )
    }
}
async fn wait_log(logs: &Captured, needle: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !logs.text().contains(needle) {
        assert!(Instant::now() < deadline, "no {needle} in {}", logs.text());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Review item 1: a module cancels only its own jobs.
#[tokio::test]
async fn cancel_is_scoped_to_the_owning_module() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
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

    let identity = handle(&jobs, "identity");
    let secret = identity
        .enqueue(new_job("identity.magic_link:tok_1", json!({"link": "x"})))
        .await
        .unwrap();
    let tasks = handle(&jobs, "tasks");
    assert!(
        !tasks.cancel(secret.id).await.unwrap(),
        "another module's job is not cancelled"
    );
    assert_eq!(state(&db, secret.id).await, ("available".into(), false));
    assert!(identity.cancel(secret.id).await.unwrap());
    assert_eq!(state(&db, secret.id).await, ("cancelled".into(), true));

    // A fanned-out job belongs to the module that registered its kind.
    append(
        &db,
        &outbox("identity"),
        vec![event(
            "identity",
            "identity.user_registered",
            "usr_1:registered",
            json!({}),
        )],
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
    jobs.relay_now().await.unwrap();
    let fanned = scalar(
        &db,
        "SELECT id FROM merezha_jobs.jobs WHERE kind = 'notify.module_event'",
    )
    .await;
    assert!(!identity.cancel(fanned).await.unwrap());
    assert!(!tasks.cancel(fanned).await.unwrap());
    assert!(notifications.cancel(fanned).await.unwrap());
}

/// Review item 2: a systemic failure quarantines nothing and keeps every
/// payload; the relay resumes once the cause is gone.
#[tokio::test]
async fn a_systemic_relay_failure_quarantines_nothing_and_the_relay_resumes() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    execute(
        &db,
        "CREATE FUNCTION refuse() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'jobs are refused'; END $$",
    )
    .await
    .unwrap();
    execute(
        &db,
        "CREATE TRIGGER refuse BEFORE INSERT ON merezha_jobs.jobs
             FOR EACH ROW EXECUTE FUNCTION refuse()",
    )
    .await
    .unwrap();
    let mut secret = job_record(
        "identity.magic_link:tok_1",
        "notify.expand",
        json!({"link": "x"}),
    );
    secret.contains_secret = true;
    append(
        &db,
        &outbox("identity"),
        vec![secret, job_record("plain", "default", json!({}))],
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
    let jobs = PgJobs::with_mode(&jobs_config(&test, ""), RelayMode::Off);
    jobs.ports(&db);
    let failure = jobs.relay_now().await.unwrap_err();
    assert_eq!(
        failure.classification,
        merezha_domain::Classification::Transient
    );
    let untouched = "SELECT count(*) FROM merezha_jobs.outbox
        WHERE relayed_at IS NULL AND quarantined_at IS NULL AND payload IS NOT NULL";
    assert_eq!(scalar(&db, untouched).await, 2);

    // The background relay logs it and retries at the interval.
    let logs = Captured::default();
    let _guard = logs.install();
    let config = jobs_config(&test, "[jobs.relay]\ninterval_ms = 200");
    let running = started(&db, &config, RelayMode::Interval).await;
    wait_log(&logs, "outbox.relay_failed").await;
    assert_eq!(scalar(&db, untouched).await, 2);
    assert!(running.ready().await.is_ok(), "readiness is unaffected");
    execute(&db, "DROP TRIGGER refuse ON merezha_jobs.jobs")
        .await
        .unwrap();
    wait_for(
        &db,
        "SELECT count(*) FROM merezha_jobs.jobs",
        2,
        Duration::from_secs(3),
    )
    .await;
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.outbox WHERE quarantined_at IS NOT NULL"
        )
        .await,
        0
    );
    running.shutdown(soon()).await;
}

/// Review item 3: a record isolated by its own data error keeps its
/// payload, flagged or not, and relays after a requeue.
#[tokio::test]
async fn an_isolated_record_keeps_its_payload_and_relays_after_requeue() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    execute(
        &db,
        "INSERT INTO merezha_jobs.subscriptions (topic, job_kind, queue, source)
         VALUES ('identity.user_registered', 'audit.record', 'Bad Queue', 'audit')",
    )
    .await
    .unwrap();
    let mut flagged = event(
        "identity",
        "identity.user_registered",
        "usr_1:registered",
        json!({"user_id": "usr_1"}),
    );
    flagged.contains_secret = true;
    append(
        &db,
        &outbox("identity"),
        vec![flagged, job_record("plain", "default", json!({}))],
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
    let jobs = PgJobs::with_mode(&jobs_config(&test, ""), RelayMode::Off);
    jobs.ports(&db);
    let report = jobs.relay_now().await.unwrap();
    assert_eq!((report.relayed, report.quarantined), (1, 1));
    let isolated = scalar(
        &db,
        "SELECT id FROM merezha_jobs.outbox
          WHERE quarantine_code = 'constraint_violation' AND contains_secret AND payload IS NOT NULL",
    )
    .await;
    // Fix the cause, requeue, relay: the job carries the payload.
    execute(
        &db,
        "UPDATE merezha_jobs.subscriptions SET queue = 'default' WHERE job_kind = 'audit.record'",
    )
    .await
    .unwrap();
    jobs.requeue_outbox(isolated, None).await.unwrap();
    assert_eq!(jobs.relay_now().await.unwrap().relayed, 1);
    assert_eq!(
        scalar(
            &db,
            &format!(
                "SELECT count(*) FROM merezha_jobs.jobs
                  WHERE outbox_id = {isolated} AND contains_secret AND payload IS NOT NULL"
            )
        )
        .await,
        1
    );
    assert_eq!(
        scalar(
            &db,
            &format!(
                "SELECT count(*) FROM merezha_jobs.outbox WHERE id = {isolated} AND payload IS NULL"
            )
        )
        .await,
        1,
        "relayed at last, the flagged record loses its payload"
    );
}

const LISTENERS: &str = "SELECT count(*) FROM pg_stat_activity
  WHERE datname = current_database() AND query LIKE 'LISTEN merezha_jobs_outbox%'";

/// Review items 3 and 5: a killed LISTEN session is noticed and reopened,
/// and `Listener::check` fails on a dead session.
#[tokio::test]
async fn a_killed_listen_session_is_reopened_and_checks_fail_on_dead_sessions() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = jobs_config(&test, "[jobs.relay]\ninterval_ms = 600000");
    let jobs = started(&db, &config, RelayMode::Listen).await;
    wait_for(&db, LISTENERS, 1, Duration::from_secs(5)).await;
    let first = scalar(
        &db,
        "SELECT pid::bigint FROM pg_stat_activity
          WHERE datname = current_database() AND query LIKE 'LISTEN merezha_jobs_outbox%'",
    )
    .await;
    execute(&db, &format!("SELECT pg_terminate_backend({first})"))
        .await
        .unwrap();
    wait_for(
        &db,
        &format!(
            "SELECT count(*) FROM pg_stat_activity
              WHERE datname = current_database() AND query LIKE 'LISTEN merezha_jobs_outbox%'
                AND pid <> {first}"
        ),
        1,
        Duration::from_secs(5),
    )
    .await;
    append(
        &db,
        &outbox("tasks"),
        vec![job_record("after-kill", "default", json!({}))],
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
    wait_for(
        &db,
        "SELECT count(*) FROM merezha_jobs.jobs WHERE idempotency_key = 'tasks:after-kill'",
        1,
        Duration::from_secs(1),
    )
    .await;
    jobs.shutdown(soon()).await;

    let session = db.listen(&["probe_channel"], soon()).await.unwrap();
    let pid = scalar(
        &db,
        "SELECT pid::bigint FROM pg_stat_activity
          WHERE datname = current_database() AND query LIKE 'LISTEN probe_channel%'",
    )
    .await;
    session.check(soon()).await.unwrap();
    execute(&db, &format!("SELECT pg_terminate_backend({pid})"))
        .await
        .unwrap();
    // Termination is asynchronous: the probe fails once the backend is gone.
    let deadline = Instant::now() + Duration::from_secs(5);
    while session.check(soon()).await.is_ok() {
        assert!(Instant::now() < deadline, "a dead session passed the check");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Review item 4: enqueue wakes the queue's dispatchers on commit.
#[tokio::test]
async fn enqueue_notifies_the_queue_on_commit() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let jobs = PgJobs::with_mode(&jobs_config(&test, ""), RelayMode::Off);
    jobs.ports(&db);
    let mut listener = db.listen(&[JOBS_CHANNEL], soon()).await.unwrap();
    let tasks = handle(&jobs, "tasks");
    tasks
        .enqueue(new_job("remind:t1", json!({})))
        .await
        .unwrap();
    let woken = tokio::time::timeout(Duration::from_secs(1), listener.recv())
        .await
        .expect("a notification within 1 s")
        .unwrap();
    assert_eq!(
        (woken.channel.as_str(), woken.payload.as_str()),
        (JOBS_CHANNEL, "default")
    );
    // An existing job is not new work.
    assert!(
        tasks
            .enqueue(new_job("remind:t1", json!({})))
            .await
            .unwrap()
            .existed
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(listener.try_recv().is_none());
}

/// Review item 6: subscriptions the running version no longer declares
/// are deactivated, kept, and no longer routed to.
#[tokio::test]
async fn undeclared_subscriptions_are_deactivated_and_no_longer_routed() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let version = |kinds: &[&str]| {
        let jobs = PgJobs::with_mode(&jobs_config(&test, ""), RelayMode::Off);
        jobs.ports(&db);
        let audit = handle(&jobs, "audit");
        for kind in kinds {
            audit
                .register(kind, "default", Arc::new(Noop), KindOptions::default())
                .unwrap();
            audit.subscribe("identity.user_registered", kind).unwrap();
        }
        jobs
    };
    let active = "SELECT count(*) FROM merezha_jobs.subscriptions WHERE deactivated_at IS NULL";
    let old = version(&["audit.record", "audit.legacy"]);
    old.start(context(&db)).await.unwrap();
    wait_ready(&old).await;
    assert_eq!(scalar(&db, active).await, 2);
    let new = version(&["audit.record"]);
    new.start(context(&db)).await.unwrap();
    wait_ready(&new).await;
    assert_eq!(scalar(&db, active).await, 1);
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM merezha_jobs.subscriptions").await,
        2,
        "history is kept"
    );
    append(
        &db,
        &outbox("identity"),
        vec![event(
            "identity",
            "identity.user_registered",
            "usr_1:registered",
            json!({}),
        )],
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
    new.relay_now().await.unwrap();
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'audit.legacy'"
        )
        .await,
        0
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'audit.record'"
        )
        .await,
        1
    );
    // Declaring it again reactivates it.
    let back = version(&["audit.record", "audit.legacy"]);
    back.start(context(&db)).await.unwrap();
    wait_ready(&back).await;
    assert_eq!(scalar(&db, active).await, 2);
}

/// Review item 7: a permanent subscription sync failure is logged.
#[tokio::test]
async fn a_permanent_subscription_sync_failure_is_logged() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    execute(&db, "DROP TABLE merezha_jobs.subscriptions")
        .await
        .unwrap();
    let logs = Captured::default();
    let _guard = logs.install();
    let jobs = PgJobs::with_mode(&jobs_config(&test, ""), RelayMode::Off);
    jobs.ports(&db);
    jobs.start(context(&db)).await.unwrap();
    wait_log(&logs, "job.subscription_sync_failed").await;
    assert!(logs.text().contains("\"classification\":\"permanent\""));
    assert!(
        jobs.ready().await.is_err(),
        "not ready until the sync succeeds"
    );
    jobs.shutdown(soon()).await;
}
