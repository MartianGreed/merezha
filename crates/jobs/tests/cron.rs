//! Cron against real PostgreSQL (AP-B3, §B.7): the schedule sync across
//! instances and versions, the scheduler tick, the missed-run policy and
//! module declarations. Skips with a message when
//! `MEREZHA_TEST_DATABASE_URL` is unset.
mod support;
use merezha_jobs::{PgJobs, RelayMode, TickReport};
use merezha_platform::{
    JobsRuntime, Module,
    config::JobsConfig,
    ports::{KindOptions, ScheduleDeclaration},
};
use merezha_postgres::{Database, TestDatabase};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use support::*;

/// A `[[jobs.schedules]]` entry on queue `default`, kind `tasks.remind`.
fn entry(name: &str, cron: &str, timezone: &str) -> String {
    format!(
        "[[jobs.schedules]]\nname = \"{name}\"\ncron = \"{cron}\"\ntimezone = \"{timezone}\"\nqueue = \"default\"\nkind = \"tasks.remind\"\npayload = {{ schedule = \"{name}\" }}\n"
    )
}
/// No workers (jobs stay `available`), a fast heartbeat, and the given
/// scheduler interval.
fn config(test: &TestDatabase, interval_ms: u64, extra: &str) -> JobsConfig {
    jobs_config(
        test,
        &format!(
            "[jobs]\nworkers = false\n[jobs.scheduler]\ninterval_ms = {interval_ms}\n[jobs.subscriptions]\nheartbeat = \"200ms\"\nretire_after = \"6s\"\n{extra}"
        ),
    )
}
async fn instance(db: &Database, config: &JobsConfig) -> PgJobs {
    started(db, config, RelayMode::Off).await
}
async fn text(db: &Database, sql: &str) -> String {
    let client = db.acquire(soon()).await.unwrap();
    client.query_one(sql, &[]).await.unwrap().get(0)
}
/// `next_run_at` of `name`, in epoch seconds.
async fn next_run(db: &Database, name: &str) -> i64 {
    scalar(
        db,
        &format!(
            "SELECT extract(epoch FROM next_run_at)::bigint FROM merezha_jobs.schedules WHERE name = '{name}'"
        ),
    )
    .await
}
async fn db_now(db: &Database) -> i64 {
    scalar(db, "SELECT extract(epoch FROM now())::bigint").await
}
/// Run `sql` (an update of schedule rows) under the schedules advisory
/// lock, as the sync takes it, so it never deadlocks with a heartbeat
/// upserting the same rows in another order.
async fn reschedule(db: &Database, sql: &str) {
    let sql = sql.to_owned();
    db.transaction(soon(), move |tx| {
        Box::pin(async move {
            tx.execute(
                "SELECT pg_advisory_xact_lock(1297762882, hashtext('schedules'))",
                &[],
            )
            .await
            .map_err(|e| merezha_postgres::classify(&e))?;
            tx.execute(sql.as_str(), &[])
                .await
                .map_err(|e| merezha_postgres::classify(&e))?;
            Ok(())
        })
    })
    .await
    .unwrap();
}
const JOBS: &str = "SELECT count(*) FROM merezha_jobs.jobs WHERE schedule_name IS NOT NULL";

/// Brief requirement and AP-B3 test 3: three instances on three pools tick
/// concurrently (their background schedulers every second, plus explicit
/// ticks released together), round after round of due occurrences. Each
/// occurrence enqueues exactly one job, no tick ever finds another
/// instance's job for the occurrence it claimed, and every job carries
/// the occurrence key.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn three_instances_ticking_at_once_enqueue_each_occurrence_exactly_once() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    const SCHEDULES: i64 = 12;
    const ROUNDS: i64 = 10;
    let schedules: String = (0..SCHEDULES)
        .map(|n| entry(&format!("tasks.s{n:02}"), "0 0 1 1 *", "UTC"))
        .collect();
    let config = config(&test, 1_000, &schedules);
    let mut instances = Vec::new();
    for _ in 0..3 {
        let db = database(&test).await;
        let jobs = instance(&db, &config).await;
        instances.push((db, jobs));
    }
    let db = &instances[0].0;
    assert_eq!(
        scalar(db, "SELECT count(*) FROM merezha_jobs.schedules").await,
        SCHEDULES
    );
    let mut explicit = TickReport::default();
    for round in 0..ROUNDS {
        // Each round makes every schedule due at a distinct past minute.
        reschedule(
            db,
            &format!(
                "UPDATE merezha_jobs.schedules
                    SET next_run_at = date_trunc('minute', now()) - make_interval(mins => {})",
                100 - round
            ),
        )
        .await;
        let barrier = Arc::new(tokio::sync::Barrier::new(instances.len()));
        let ticks: Vec<_> = instances
            .iter()
            .map(|(_, jobs)| {
                let jobs = jobs.clone();
                let barrier = barrier.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    jobs.schedule_now().await.unwrap()
                })
            })
            .collect();
        for tick in ticks {
            let report = tick.await.unwrap();
            explicit.fired += report.fired;
            explicit.duplicates += report.duplicates;
        }
        wait_for(
            db,
            "SELECT count(*) FROM merezha_jobs.schedules WHERE next_run_at <= now()",
            0,
            PATIENCE,
        )
        .await;
        assert_eq!(
            scalar(db, JOBS).await,
            SCHEDULES * (round + 1),
            "round {round}"
        );
    }
    assert_eq!(
        explicit.duplicates, 0,
        "no tick found a job another instance enqueued for its occurrence"
    );
    assert!(explicit.fired > 0, "the explicit ticks took part");
    assert_eq!(
        scalar(
            db,
            "SELECT count(*) FROM (SELECT schedule_name, scheduled_for FROM merezha_jobs.jobs
                GROUP BY 1, 2 HAVING count(*) > 1) d"
        )
        .await,
        0,
        "one job per occurrence"
    );
    assert_eq!(
        scalar(
            db,
            "SELECT count(*) FROM merezha_jobs.jobs
              WHERE idempotency_key <> 'cron:' || schedule_name || ':' || extract(epoch FROM scheduled_for)::bigint
                 OR correlation_id <> idempotency_key OR state <> 'available'"
        )
        .await,
        0,
        "every job carries its occurrence key"
    );
    assert_eq!(
        scalar(
            db,
            "SELECT count(*) FROM merezha_jobs.schedules s
              WHERE last_job_id IS DISTINCT FROM (SELECT max(id) FROM merezha_jobs.jobs j
                                                   WHERE j.schedule_name = s.name)"
        )
        .await,
        0,
        "last_job_id names the latest occurrence's job"
    );
    for (_, jobs) in &instances {
        jobs.shutdown(soon()).await;
    }
}

/// Brief requirement and AP-B3 test 5: a schedule only version 2 declares
/// survives version 1 instances restarting during a rolling deploy, is
/// retired (kept, never fired) only once nobody declared it for
/// `retire_after`, and is reactivated from now when declared again.
#[tokio::test]
async fn a_schedule_declared_by_v2_survives_v1_restarts_during_a_rolling_deploy() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let v1 = config(&test, 3_600_000, &entry("tasks.daily", "0 9 * * *", "UTC"));
    let v2 = config(
        &test,
        3_600_000,
        &[
            entry("tasks.daily", "0 9 * * *", "UTC"),
            entry("tasks.digest", "0 18 * * fri", "Europe/Paris"),
        ]
        .concat(),
    );
    const LIVE: &str = "SELECT count(*) FROM merezha_jobs.schedules
        WHERE name = 'tasks.digest' AND enabled AND retired_at IS NULL";
    // Old and new versions overlap, then old instances keep restarting.
    let old = instance(&db, &v1).await;
    let new = instance(&db, &v2).await;
    assert_eq!(scalar(&db, LIVE).await, 1);
    for round in 0..4 {
        old.shutdown(soon()).await;
        let restarted = instance(&db, &v1).await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(scalar(&db, LIVE).await, 1, "round {round}");
        restarted.shutdown(soon()).await;
    }
    let digest_next = next_run(&db, "tasks.digest").await;
    assert!(digest_next > db_now(&db).await);
    // The rollback case: only v1 is left, so nobody declares the digest.
    new.shutdown(soon()).await;
    let only_v1 = instance(&db, &v1).await;
    assert_eq!(scalar(&db, LIVE).await, 1, "still inside the grace window");
    wait_for(&db, LIVE, 0, PATIENCE).await;
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.schedules
              WHERE name = 'tasks.digest' AND retired_at IS NOT NULL"
        )
        .await,
        1,
        "retired, not deleted"
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.schedules WHERE name = 'tasks.daily' AND retired_at IS NULL"
        )
        .await,
        1,
        "the schedule v1 declares stays"
    );
    reschedule(
        &db,
        "UPDATE merezha_jobs.schedules SET next_run_at = now() - interval '1 minute'",
    )
    .await;
    let report = only_v1.schedule_now().await.unwrap();
    assert_eq!(report.fired, 1, "only the live schedule fires");
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.jobs WHERE schedule_name = 'tasks.digest'"
        )
        .await,
        0
    );
    // Declared again: reactivated, its next run recomputed from now.
    let again = instance(&db, &v2).await;
    assert_eq!(scalar(&db, LIVE).await, 1);
    assert!(next_run(&db, "tasks.digest").await > db_now(&db).await);
    again.shutdown(soon()).await;
    only_v1.shutdown(soon()).await;
}

/// Brief requirement and AP-B3 test 4: a downtime spanning several
/// occurrences runs one job, for the oldest missed occurrence, then the
/// normal cadence: the next run is the first occurrence after now.
#[tokio::test]
async fn a_downtime_across_several_occurrences_runs_one_job_then_the_normal_cadence() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = config(
        &test,
        3_600_000,
        &entry("tasks.minutely", "* * * * *", "UTC"),
    );
    let before = instance(&db, &config).await;
    before.shutdown(soon()).await;
    // The host was down for five occurrences and then some.
    reschedule(
        &db,
        "UPDATE merezha_jobs.schedules
            SET next_run_at = date_trunc('minute', now()) - interval '5 minutes'",
    )
    .await;
    let missed = next_run(&db, "tasks.minutely").await;
    // The restarted host syncs (same definition: the due run is kept) and
    // ticks at start.
    let after = instance(&db, &config).await;
    wait_for(&db, JOBS, 1, PATIENCE).await;
    assert_eq!(
        scalar(
            &db,
            "SELECT extract(epoch FROM scheduled_for)::bigint FROM merezha_jobs.jobs"
        )
        .await,
        missed,
        "the one job is for the missed occurrence"
    );
    assert_eq!(
        text(&db, "SELECT idempotency_key FROM merezha_jobs.jobs").await,
        format!("cron:tasks.minutely:{missed}")
    );
    let next = next_run(&db, "tasks.minutely").await;
    let now = db_now(&db).await;
    assert!(
        next > now && next <= now + 60 && next % 60 == 0,
        "the next minute after now, not the next missed one: next {next}, now {now}"
    );
    assert_eq!(after.schedule_now().await.unwrap(), TickReport::default());
    assert_eq!(scalar(&db, JOBS).await, 1, "no burst");
    assert_eq!(
        scalar(
            &db,
            "SELECT extract(epoch FROM last_run_at)::bigint FROM merezha_jobs.schedules"
        )
        .await,
        missed
    );
    after.shutdown(soon()).await;
}

/// AP-B3 test 5: a changed definition recomputes `next_run_at` from now;
/// redeclaring the same definition keeps a due run.
#[tokio::test]
async fn a_changed_definition_recomputes_the_next_run() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let yearly = config(&test, 3_600_000, &entry("tasks.report", "0 0 1 1 *", "UTC"));
    let first = instance(&db, &yearly).await;
    let far = next_run(&db, "tasks.report").await;
    assert!(far - db_now(&db).await > 86_400, "next new year");
    first.shutdown(soon()).await;
    let five = config(
        &test,
        3_600_000,
        &entry("tasks.report", "*/5 * * * *", "UTC"),
    );
    let second = instance(&db, &five).await;
    let near = next_run(&db, "tasks.report").await;
    let now = db_now(&db).await;
    assert!(near > now && near <= now + 300 && near % 300 == 0, "{near}");
    assert_eq!(
        text(&db, "SELECT cron FROM merezha_jobs.schedules").await,
        "*/5 * * * *"
    );
    second.shutdown(soon()).await;
    // The same definition again: the stored (due) run stands.
    reschedule(
        &db,
        "UPDATE merezha_jobs.schedules SET next_run_at = now() - interval '1 hour'",
    )
    .await;
    let due = next_run(&db, "tasks.report").await;
    // Started again with the same definition: its start tick fires the
    // stored due run instead of a recomputed one.
    let third = instance(&db, &five).await;
    wait_for(&db, JOBS, 1, PATIENCE).await;
    assert_eq!(
        scalar(
            &db,
            "SELECT extract(epoch FROM scheduled_for)::bigint FROM merezha_jobs.jobs"
        )
        .await,
        due
    );
    third.shutdown(soon()).await;
}

/// `JobsHandle::schedule`: a module's schedule is validated, refused
/// after start, synced with the module as its source, and its jobs run
/// through the module's handler with the occurrence key.
#[tokio::test]
async fn modules_declare_schedules_whose_jobs_run_through_their_handler() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = jobs_config(
        &test,
        &format!(
            "[jobs.scheduler]\ninterval_ms = 3600000\n[jobs.queues.default]\nconcurrency = 2\n{}",
            entry("tasks.config", "0 9 * * *", "UTC")
        ),
    );
    let seen: Arc<Mutex<Vec<(String, Value)>>> = Arc::default();
    let record = seen.clone();
    let jobs = PgJobs::with_mode(&config, RelayMode::Interval);
    jobs.ports(&db);
    let tasks = handle(&jobs, "tasks");
    tasks
        .register(
            "tasks.digest",
            "default",
            func(move |ctx, payload| {
                record.lock().unwrap().push((ctx.idempotency_key, payload));
                Box::pin(async { Ok(()) })
            }),
            KindOptions::default(),
        )
        .unwrap();
    let declaration = |name: &str, cron: &str, timezone: &str, kind: &str| ScheduleDeclaration {
        name: name.into(),
        cron: cron.into(),
        timezone: timezone.into(),
        queue: id("default"),
        kind: id(kind),
        payload: json!({"scope": "weekly"}),
    };
    for (bad, code) in [
        (
            declaration("Tasks", "0 9 * * *", "UTC", "tasks.digest"),
            "invalid_schedule_name",
        ),
        (
            declaration("tasks.x", "0 0 9 * * *", "UTC", "tasks.digest"),
            "invalid_cron",
        ),
        (
            declaration("tasks.x", "0 9 * * *", "Europe/Atlantis", "tasks.digest"),
            "unknown_timezone",
        ),
        (
            declaration("tasks.x", "0 9 * * *", "UTC", "tasks.other"),
            "kind_not_registered",
        ),
        (
            declaration("tasks.config", "0 9 * * *", "UTC", "tasks.digest"),
            "schedule_already_declared",
        ),
    ] {
        assert_eq!(tasks.schedule(bad).unwrap_err().code, code);
    }
    assert_eq!(
        handle(&jobs, "billing")
            .schedule(declaration("tasks.x", "0 9 * * *", "UTC", "tasks.digest"))
            .unwrap_err()
            .code,
        "kind_not_registered",
        "a module schedules only its own kinds"
    );
    tasks
        .schedule(declaration(
            "tasks.weekly",
            "0 8 * * mon",
            "Europe/Paris",
            "tasks.digest",
        ))
        .unwrap();
    jobs.start(instance_context(&db, "cron-1")).await.unwrap();
    wait_ready(&jobs).await;
    assert_eq!(
        tasks
            .schedule(declaration(
                "tasks.late",
                "0 9 * * *",
                "UTC",
                "tasks.digest"
            ))
            .unwrap_err()
            .code,
        "jobs_started"
    );
    assert_eq!(
        text(
            &db,
            "SELECT string_agg(name || '=' || source || '/' || timezone, ',' ORDER BY name)
               FROM merezha_jobs.schedules"
        )
        .await,
        "tasks.config=config/UTC,tasks.weekly=tasks/Europe/Paris"
    );
    reschedule(
        &db,
        "UPDATE merezha_jobs.schedules SET next_run_at = '2026-10-19T06:00:00Z'
          WHERE name = 'tasks.weekly'",
    )
    .await;
    // The start tick may still be running: whichever tick takes the
    // occurrence, there is one job.
    jobs.schedule_now().await.unwrap();
    wait_for(&db, JOBS, 1, PATIENCE).await;
    let started = std::time::Instant::now();
    while seen.lock().unwrap().is_empty() {
        assert!(started.elapsed() < Duration::from_secs(5), "never ran");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        seen.lock().unwrap()[0],
        (
            "cron:tasks.weekly:1792389600".to_owned(),
            json!({"scope": "weekly"})
        ),
        "Monday 08:00 Paris, keyed by the occurrence"
    );
    jobs.shutdown(soon()).await;
}

/// The schedule admin operations (D.1): list, pause (a paused schedule
/// never fires), resume (from now, so paused occurrences never run) and
/// trigger (keyed `cron:{name}:manual:{unix_ms}`), each audited.
#[tokio::test]
async fn schedules_pause_resume_and_trigger_with_audit_rows() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = config(&test, 3_600_000, &entry("tasks.daily", "0 9 * * *", "UTC"));
    let jobs = instance(&db, &config).await;
    let page = jobs.list_schedules(None, 50).await.unwrap();
    assert_eq!(page.schedules.len(), 1);
    assert_eq!(page.next, None);
    let listed = &page.schedules[0];
    assert_eq!(
        (
            listed.name.as_str(),
            listed.source.as_str(),
            listed.enabled,
            listed.last_job_id
        ),
        ("tasks.daily", "config", true, None)
    );
    let paused = jobs
        .pause_schedule("tasks.daily", Some("sys_ops"))
        .await
        .unwrap();
    assert!(!paused.enabled);
    reschedule(
        &db,
        "UPDATE merezha_jobs.schedules SET next_run_at = now() - interval '1 day'",
    )
    .await;
    assert_eq!(jobs.schedule_now().await.unwrap().fired, 0, "paused");
    let resumed = jobs
        .resume_schedule("tasks.daily", Some("sys_ops"))
        .await
        .unwrap();
    assert!(resumed.enabled);
    assert!(resumed.next_run_at > jiff::Timestamp::now(), "from now");
    assert_eq!(jobs.schedule_now().await.unwrap().fired, 0);
    let triggered = jobs
        .trigger_schedule("tasks.daily", Some("sys_ops"))
        .await
        .unwrap();
    assert!(
        triggered
            .idempotency_key
            .starts_with("cron:tasks.daily:manual:")
    );
    assert_eq!(
        text(
            &db,
            &format!(
                "SELECT idempotency_key || ' ' || payload::text FROM merezha_jobs.jobs WHERE id = {}",
                triggered.job_id
            )
        )
        .await,
        format!("{} {{\"schedule\":\"tasks.daily\"}}", triggered.idempotency_key)
    );
    assert_eq!(
        next_run(&db, "tasks.daily").await,
        resumed.next_run_at.as_second(),
        "a trigger leaves the cadence alone"
    );
    assert_eq!(
        text(
            &db,
            "SELECT string_agg(action || ':' || target || ':' || principal, ',' ORDER BY id)
               FROM merezha_jobs.admin_actions"
        )
        .await,
        "schedule_pause:tasks.daily:sys_ops,schedule_resume:tasks.daily:sys_ops,schedule_trigger:tasks.daily:sys_ops"
    );
    for missing in [
        jobs.pause_schedule("tasks.none", None).await.unwrap_err(),
        jobs.resume_schedule("tasks.none", None).await.unwrap_err(),
        jobs.trigger_schedule("tasks.none", None).await.unwrap_err(),
    ] {
        assert_eq!(missing.code, "not_found");
    }
    jobs.shutdown(soon()).await;
}
