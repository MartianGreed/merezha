//! Cancel scoping, systemic relay failures, isolation payloads, the LISTEN
//! session's recovery, enqueue wake-ups, subscription deactivation and
//! sync failure logging, against real PostgreSQL.
mod support;
use merezha_jobs::{JOBS_CHANNEL, PgJobs, RelayMode};
use merezha_platform::{
    JobsRuntime, Module,
    config::JobsConfig,
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
    let deadline = Instant::now() + PATIENCE;
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
    wait_for(&db, "SELECT count(*) FROM merezha_jobs.jobs", 2, PATIENCE).await;
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
    wait_for(&db, LISTENERS, 1, PATIENCE).await;
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
        PATIENCE,
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
        PATIENCE,
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
    let deadline = Instant::now() + PATIENCE;
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
    let woken = tokio::time::timeout(PATIENCE, listener.recv())
        .await
        .expect("a notification")
        .unwrap();
    assert_eq!(
        (woken.channel.as_str(), woken.payload.as_str()),
        (JOBS_CHANNEL, "default")
    );
    // An existing job is not new work. Notifications arrive in commit
    // order, so a later sentinel arriving first proves none was sent.
    assert!(
        tasks
            .enqueue(new_job("remind:t1", json!({})))
            .await
            .unwrap()
            .existed
    );
    execute(
        &db,
        &format!("SELECT pg_notify('{JOBS_CHANNEL}', 'sentinel')"),
    )
    .await
    .unwrap();
    let next = tokio::time::timeout(PATIENCE, listener.recv())
        .await
        .expect("the sentinel")
        .unwrap();
    assert_eq!(next.payload, "sentinel");
}

/// A runtime of one "version": it declares `kinds` on `topic` for source
/// `audit`, binds to `db` and starts.
async fn version(db: &Database, config: &JobsConfig, kinds: &[&str]) -> PgJobs {
    version_in(db, config, kinds, RelayMode::Off).await
}
async fn version_in(db: &Database, config: &JobsConfig, kinds: &[&str], mode: RelayMode) -> PgJobs {
    let jobs = PgJobs::with_mode(config, mode);
    jobs.ports(db);
    let audit = handle(&jobs, "audit");
    for kind in kinds {
        audit
            .register(kind, "default", Arc::new(Noop), KindOptions::default())
            .unwrap();
        audit.subscribe("identity.user_registered", kind).unwrap();
    }
    jobs.start(context(db)).await.unwrap();
    wait_ready(&jobs).await;
    jobs
}
const ACTIVE: &str = "SELECT count(*) FROM merezha_jobs.subscriptions WHERE deactivated_at IS NULL";
async fn registered(db: &Database, key: &str) {
    append(
        db,
        &outbox("identity"),
        vec![event(
            "identity",
            "identity.user_registered",
            key,
            json!({}),
        )],
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
}

/// Re-review F2: during a rolling deploy, v1 and v2 instances restart in
/// turns; v2's new subscription is never deactivated and gets every event.
/// `retire_after` is long enough that no restart outlives it on a loaded
/// machine: only an instance retiring what its own registry lacks could
/// deactivate it.
#[tokio::test]
async fn alternating_v1_and_v2_restarts_never_retire_the_new_subscription() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = jobs_config(
        &test,
        "[jobs.subscriptions]\nheartbeat = \"200ms\"\nretire_after = \"1h\"",
    );
    let v1 = &["audit.record"][..];
    let v2 = &["audit.record", "audit.trail"][..];
    for round in 0..4 {
        let declared = if round % 2 == 0 { v2 } else { v1 };
        let instance = version(&db, &config, declared).await;
        assert_eq!(scalar(&db, ACTIVE).await, 2, "round {round}");
        registered(&db, &format!("usr_{round}:registered")).await;
        instance.relay_now().await.unwrap();
        instance.shutdown(soon()).await;
    }
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'audit.trail'"
        )
        .await,
        4,
        "no event was lost to v2's subscription"
    );
}

/// Re-review F2: an instance that declares nothing retires nothing.
#[tokio::test]
async fn an_instance_with_an_empty_registry_retires_nothing() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = jobs_config(&test, "");
    let declaring = version(&db, &config, &["audit.record", "audit.trail"]).await;
    let empty = version(&db, &config, &[]).await;
    empty.relay_now().await.unwrap();
    assert_eq!(scalar(&db, ACTIVE).await, 2);
    empty.shutdown(soon()).await;
    declaring.shutdown(soon()).await;
}

/// Wait for a subscription heartbeat of `audit.record`'s declarer that
/// began after now: its retire step, run in the same transaction after the
/// declaration, saw every change committed before this call.
async fn a_later_heartbeat(db: &Database) {
    let since = scalar(
        db,
        "SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::bigint",
    )
    .await;
    wait_for(
        db,
        &format!(
            "SELECT count(*) FROM merezha_jobs.subscriptions
              WHERE job_kind = 'audit.record' AND last_declared_at > to_timestamp({since} / 1e6)"
        ),
        1,
        PATIENCE,
    )
    .await;
}
/// Date `audit.legacy`'s last declaration `ago` back, as if nobody had
/// declared it since, then wait for a heartbeat to judge it.
async fn age_legacy(db: &Database, ago: &str) {
    execute(
        db,
        &format!(
            "UPDATE merezha_jobs.subscriptions SET last_declared_at = now() - interval '{ago}'
              WHERE job_kind = 'audit.legacy'"
        ),
    )
    .await
    .unwrap();
    a_later_heartbeat(db).await;
}

/// Re-review F2: a subscription nobody declared for `retire_after` is
/// retired, kept as history and no longer routed; declaring it again
/// reactivates it. The window is an hour and the undeclared row is dated
/// back explicitly, so no stall of a loaded machine can age a row.
#[tokio::test]
async fn a_subscription_nobody_declares_is_retired_after_the_grace_window() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = jobs_config(
        &test,
        "[jobs.subscriptions]\nheartbeat = \"200ms\"\nretire_after = \"1h\"\n[jobs.relay]\ninterval_ms = 600000",
    );
    let all = ["audit.record", "audit.legacy", "audit.extra"];
    let old = version(&db, &config, &all).await;
    old.shutdown(soon()).await;
    // Both background loops heartbeat: a relaying instance and a held one.
    let new = version_in(&db, &config, &["audit.record"], RelayMode::Interval).await;
    let held = version(&db, &config, &["audit.extra"]).await;
    assert_eq!(
        scalar(&db, ACTIVE).await,
        3,
        "still inside the grace window"
    );
    age_legacy(&db, "59 minutes").await;
    assert_eq!(
        scalar(&db, ACTIVE).await,
        3,
        "a minute inside the grace window"
    );
    age_legacy(&db, "61 minutes").await;
    assert_eq!(scalar(&db, ACTIVE).await, 2, "past the grace window");
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.subscriptions
              WHERE job_kind = 'audit.legacy' AND deactivated_at IS NOT NULL"
        )
        .await,
        1,
        "retired, not deleted"
    );
    // The declared ones stay active across heartbeats.
    a_later_heartbeat(&db).await;
    a_later_heartbeat(&db).await;
    assert_eq!(scalar(&db, ACTIVE).await, 2);
    registered(&db, "usr_1:registered").await;
    new.relay_now().await.unwrap();
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'audit.legacy'"
        )
        .await,
        0
    );
    new.shutdown(soon()).await;
    held.shutdown(soon()).await;
    let back = version(&db, &config, &all).await;
    assert_eq!(scalar(&db, ACTIVE).await, 3);
    back.shutdown(soon()).await;
}

/// Re-review F4: a class-22 data error isolates its record; the rest of
/// the batch relays and the record keeps its payload.
#[tokio::test]
async fn a_data_exception_isolates_only_its_record() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    execute(
        &db,
        "CREATE FUNCTION poison() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF NEW.idempotency_key = 'tasks:poison' THEN
             RAISE EXCEPTION 'bad value' USING ERRCODE = '22023';
           END IF;
           RETURN NEW;
         END $$",
    )
    .await
    .unwrap();
    execute(
        &db,
        "CREATE TRIGGER poison BEFORE INSERT ON merezha_jobs.jobs
             FOR EACH ROW EXECUTE FUNCTION poison()",
    )
    .await
    .unwrap();
    let mut poison = job_record("poison", "default", json!({"secret": "s"}));
    poison.contains_secret = true;
    append(
        &db,
        &outbox("tasks"),
        vec![
            job_record("one", "default", json!({})),
            poison,
            job_record("two", "default", json!({})),
        ],
        OnDuplicate::Fail,
    )
    .await
    .unwrap();
    let jobs = PgJobs::with_mode(&jobs_config(&test, ""), RelayMode::Off);
    jobs.ports(&db);
    let report = jobs.relay_now().await.unwrap();
    assert_eq!((report.relayed, report.quarantined), (2, 1));
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.outbox
              WHERE dedupe_key = 'poison' AND quarantine_code = 'invalid_record_data'
                AND payload IS NOT NULL"
        )
        .await,
        1
    );
}

/// Re-review F4 and F1: a source that prefixes another cannot cancel its
/// jobs, and an invalid source is refused.
#[tokio::test]
async fn cancel_matches_the_whole_source_name() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let jobs = PgJobs::with_mode(&jobs_config(&test, ""), RelayMode::Off);
    jobs.ports(&db);
    let job = handle(&jobs, "tasks")
        .enqueue(new_job("remind:t1", json!({})))
        .await
        .unwrap();
    assert!(!handle(&jobs, "task").cancel(job.id).await.unwrap());
    assert_eq!(
        handle(&jobs, "outbox")
            .cancel(job.id)
            .await
            .unwrap_err()
            .code,
        "invalid_source"
    );
    assert_eq!(state(&db, job.id).await.0, "available");
    assert!(handle(&jobs, "tasks").cancel(job.id).await.unwrap());
}

/// Re-review F4: the idle LISTEN session is probed on its interval.
#[tokio::test]
async fn the_idle_listen_session_is_probed_on_its_interval() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = jobs_config(&test, "[jobs.relay]\ninterval_ms = 600000");
    let jobs =
        PgJobs::with_mode(&config, RelayMode::Listen).with_listen_check(Duration::from_millis(200));
    jobs.ports(&db);
    jobs.start(context(&db)).await.unwrap();
    wait_ready(&jobs).await;
    // The probe's `SELECT 1` replaces `LISTEN` as the session's last query.
    wait_for(
        &db,
        "SELECT count(*) FROM pg_stat_activity
          WHERE datname = current_database() AND query = 'SELECT 1'",
        1,
        PATIENCE,
    )
    .await;
    jobs.shutdown(soon()).await;
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
