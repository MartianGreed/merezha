//! Leasing, dispatch, heartbeat, reaper, retry, drain and terminal payload
//! nulling against real PostgreSQL (AP-B2). Skips with a message when
//! `MEREZHA_TEST_DATABASE_URL` is unset.
mod support;
use merezha_domain::Failure;
use merezha_jobs::{PgJobs, RelayMode, Replay};
use merezha_platform::{
    BoxFuture, Database, JobsRuntime, Module,
    ports::{JobFailure, KindOptions, Reissue, ReissueRequest},
};
use merezha_postgres::TestDatabase;
use serde_json::json;
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use support::*;

/// Everything the log subscriber writes, as text. Tests that read logs run
/// on a current-thread runtime, so every task logs through it.
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

fn ok() -> BoxFuture<'static, Result<(), JobFailure>> {
    Box::pin(async { Ok(()) })
}
/// Several statements in one transaction.
async fn batch(db: &Database, sql: &'static str) {
    db.transaction(soon(), move |tx| {
        Box::pin(async move {
            tx.batch_execute(sql)
                .await
                .map_err(|e| merezha_postgres::classify(&e))
        })
    })
    .await
    .unwrap();
}
async fn int(db: &Database, sql: &str, id: i64) -> i64 {
    let client = db.acquire(soon()).await.unwrap();
    client.query_one(sql, &[&id]).await.unwrap().get(0)
}
async fn payload_is_null(db: &Database, id: i64) -> bool {
    let client = db.acquire(soon()).await.unwrap();
    client
        .query_one(
            "SELECT payload IS NULL FROM merezha_jobs.jobs WHERE id = $1",
            &[&id],
        )
        .await
        .unwrap()
        .get(0)
}
async fn last_error(db: &Database, id: i64) -> (Option<String>, Option<String>) {
    let client = db.acquire(soon()).await.unwrap();
    let row = client
        .query_one(
            "SELECT last_error_code, last_error_class FROM merezha_jobs.jobs WHERE id = $1",
            &[&id],
        )
        .await
        .unwrap();
    (row.get(0), row.get(1))
}
fn lease_expired(attempt: i32, owner: &str) -> (i32, Option<String>, Option<String>, String) {
    (
        attempt,
        Some("lease_expired".into()),
        Some("lease_expired".into()),
        owner.into(),
    )
}

/// B2 test 1, duplicate delivery: lease 1 s, the first worker's heartbeat
/// disabled, its run outlives the lease. The reaper re-offers the job and
/// a second worker runs it (heartbeated, on a lease no stall can outlive).
/// The first run completes only once the second has begun, and the second
/// only once the test has seen the first completion fenced out by
/// `lease_token` alone: the interleaving is forced, not timed. A handler
/// recording its idempotency key under a unique constraint shows one
/// effect.
#[tokio::test]
async fn a_reaped_lease_runs_again_and_the_first_completion_is_fenced_out() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let logs = Captured::default();
    let _guard = logs.install();
    let db = database(&test).await;
    execute(
        &db,
        "CREATE TABLE public.effects (key text PRIMARY KEY, attempt integer NOT NULL)",
    )
    .await
    .unwrap();
    let queue = |lease_ms: u64| {
        jobs_config(
            &test,
            &format!(
                "[jobs]\npoll_interval_ms = 100\nreaper_interval_ms = 100\n\
                 [jobs.queues.default]\nconcurrency = 1\nlease_ms = {lease_ms}\nbackoff_base_ms = 10"
            ),
        )
    };
    let config = queue(1000);
    let effects = db.clone();
    // Attempt 2 began; the test saw attempt 1 fenced out.
    let second_began = Arc::new(tokio::sync::Notify::new());
    let fenced = Arc::new(tokio::sync::Notify::new());
    let (began, seen) = (second_began.clone(), fenced.clone());
    let handler = func(move |ctx, _| {
        let db = effects.clone();
        let (began, seen) = (began.clone(), seen.clone());
        Box::pin(async move {
            // Both ignore `cancel`.
            if ctx.attempt == 1 {
                let _ = tokio::time::timeout(PATIENCE, began.notified()).await;
            } else {
                began.notify_one();
                let _ = tokio::time::timeout(PATIENCE, seen.notified()).await;
            }
            let client = db.acquire(soon()).await.map_err(JobFailure::from)?;
            client
                .execute(
                    "INSERT INTO public.effects (key, attempt) VALUES ($1, $2)
                     ON CONFLICT (key) DO NOTHING",
                    &[&ctx.idempotency_key, &(ctx.attempt as i32)],
                )
                .await
                .map_err(|e| JobFailure::from(merezha_postgres::classify(&e)))?;
            Ok(())
        })
    });
    let first = workers_with(
        PgJobs::with_mode(&config, RelayMode::Interval).without_heartbeat(),
        &db,
        "first",
        vec![("tasks.effect", handler.clone())],
    )
    .await;
    let id = insert_job(&db, "tasks.effect", "tasks:effect-1", "{}", false).await;
    wait_state(&db, id, "running", PATIENCE).await;
    let second_db = Database::connect(&test.config()).unwrap();
    let second = workers(
        &second_db,
        &queue(PATIENCE.as_millis() as u64 * 4),
        "second",
        vec![("tasks.effect", handler)],
    )
    .await;
    // The first run completes once the second began, inside its lease.
    let deadline = Instant::now() + PATIENCE;
    while !logs.text().contains("job.lease_lost") {
        assert!(
            Instant::now() < deadline,
            "no job.lease_lost: {}",
            logs.text()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(state(&db, id).await, "running", "the second lease holds");
    assert_eq!(
        attempts(&db, id).await,
        vec![lease_expired(1, "first"), (2, None, None, "second".into())],
        "the late completion wrote nothing"
    );
    fenced.notify_one();
    wait_state(&db, id, "succeeded", PATIENCE).await;
    assert_eq!(
        attempts(&db, id).await,
        vec![
            lease_expired(1, "first"),
            (2, Some("succeeded".into()), None, "second".into())
        ]
    );
    // Both runs completed their handler; the key deduplicated the effect.
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM public.effects").await,
        1,
        "one effect"
    );
    first.shutdown(soon()).await;
    second.shutdown(soon()).await;
}

/// Multi-instance leasing: three instances on three pools race for 300
/// jobs and every job runs exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn instances_racing_for_jobs_never_run_one_twice() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    execute(
        &db,
        "CREATE TABLE public.runs (job_id bigint NOT NULL, instance text NOT NULL)",
    )
    .await
    .unwrap();
    let config = jobs_config(
        &test,
        "[jobs]
poll_interval_ms = 100
[jobs.queues.default]
concurrency = 8",
    );
    // Every job waits until each instance has run one, so all three take
    // part in the race whatever the scheduling (one instance could
    // otherwise drain the queue before another polls); an instance can
    // run at most its 8 slots before the others arrive.
    let arrived: Arc<Mutex<BTreeSet<&'static str>>> = Arc::default();
    let mut instances = Vec::new();
    for name in ["a", "b", "c"] {
        // Room for 8 handlers next to the worker's own statements, and a
        // patient acquire: a handler failing on a busy pool would retry
        // and break the one-attempt assertion for a reason unrelated to
        // leasing.
        let mut sized = test.config();
        sized.pool_size = 16;
        sized.acquire_timeout = PATIENCE;
        let pool = Database::connect(&sized).unwrap();
        let recorder = pool.clone();
        let arrived = arrived.clone();
        let handler = func(move |ctx, _| {
            let db = recorder.clone();
            let arrived = arrived.clone();
            Box::pin(async move {
                arrived.lock().unwrap().insert(name);
                let started = Instant::now();
                while arrived.lock().unwrap().len() < 3 && started.elapsed() < PATIENCE {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                let client = db
                    .acquire(Instant::now() + PATIENCE)
                    .await
                    .map_err(JobFailure::from)?;
                client
                    .execute(
                        "INSERT INTO public.runs (job_id, instance) VALUES ($1, $2)",
                        &[&ctx.id, &name],
                    )
                    .await
                    .map_err(|e| JobFailure::from(merezha_postgres::classify(&e)))?;
                Ok(())
            })
        });
        instances.push(workers(&pool, &config, name, vec![("tasks.race", handler)]).await);
    }
    execute(
        &db,
        "INSERT INTO merezha_jobs.jobs (queue, kind, idempotency_key, payload, payload_digest,
             max_attempts, correlation_id)
         SELECT 'default', 'tasks.race', 'tasks:race-' || n, '{}', sha256(convert_to('{}', 'UTF8')), 10, 'c'
           FROM generate_series(1, 300) AS n",
    )
    .await
    .unwrap();
    execute(&db, "SELECT pg_notify('merezha_jobs', 'default')")
        .await
        .unwrap();
    // However slow the machine, the 300 jobs finish: wait for all of them
    // (bounded generously; it returns as soon as they are done), then check
    // exactly once.
    wait_for(
        &db,
        "SELECT count(*) FROM merezha_jobs.jobs WHERE state = 'succeeded'",
        300,
        PATIENCE * 4,
    )
    .await;
    assert_eq!(scalar(&db, "SELECT count(*) FROM public.runs").await, 300);
    assert_eq!(
        scalar(&db, "SELECT count(DISTINCT job_id) FROM public.runs").await,
        300,
        "no job ran twice"
    );
    assert_eq!(
        scalar(&db, "SELECT max(attempts)::bigint FROM merezha_jobs.jobs").await,
        1
    );
    assert_eq!(
        scalar(&db, "SELECT count(DISTINCT instance) FROM public.runs").await,
        3,
        "every instance leased"
    );
    for jobs in instances {
        jobs.shutdown(soon()).await;
    }
}

/// B2 test 2, poison jobs: a panicking handler goes dead after 1 attempt,
/// an always-transient one after `max_attempts` with gaps inside the
/// equal-jitter bounds (base 20 ms), a permanent failure at once, and so
/// does an undecodable payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poison_jobs_are_dead_lettered_by_classification() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = jobs_config(
        &test,
        "[jobs]\npoll_interval_ms = 100\n\
         [jobs.queues.default]\nconcurrency = 4\nmax_attempts = 4\nbackoff_base_ms = 20\nbackoff_max_ms = 1000",
    );
    let decoded = Arc::new(AtomicUsize::new(0));
    let seen = decoded.clone();
    // Each retry's delay, read by the next attempt: the settle statement
    // set `run_at = now() + delay` and the attempt's `finished_at = now()`.
    let delays = Arc::new(Mutex::new(Vec::<f64>::new()));
    let (recorded, reader) = (delays.clone(), db.clone());
    let jobs = workers(
        &db,
        &config,
        "poison",
        vec![
            (
                "tasks.panic",
                func(|_, _| Box::pin(async { panic!("poison") })),
            ),
            (
                "tasks.flaky",
                func(move |ctx, _| {
                    let (recorded, db) = (recorded.clone(), reader.clone());
                    Box::pin(async move {
                        if ctx.attempt > 1 {
                            let client = db.acquire(soon()).await.map_err(JobFailure::from)?;
                            let row = client
                                .query_one(
                                    "SELECT (extract(epoch FROM j.run_at - a.finished_at) * 1000)::float8
                                       FROM merezha_jobs.jobs j JOIN merezha_jobs.attempts a ON a.job_id = j.id
                                      WHERE j.id = $1 AND a.attempt = $2",
                                    &[&ctx.id, &(ctx.attempt as i32 - 1)],
                                )
                                .await
                                .map_err(|e| JobFailure::from(merezha_postgres::classify(&e)))?;
                            recorded.lock().unwrap().push(row.get(0));
                        }
                        Err(JobFailure::transient("provider_busy", None))
                    })
                }),
            ),
            (
                "tasks.permanent",
                func(|_, _| Box::pin(async { Err(JobFailure::permanent("bad_address")) })),
            ),
            (
                "tasks.decode",
                func(move |_, _| {
                    seen.fetch_add(1, Ordering::SeqCst);
                    ok()
                }),
            ),
        ],
    )
    .await;
    let panic = insert_job(&db, "tasks.panic", "tasks:panic", "{}", false).await;
    let flaky = insert_job_on(&db, "default", "tasks.flaky", "tasks:flaky", "{}", false, 4).await;
    let permanent = insert_job(&db, "tasks.permanent", "tasks:permanent", "{}", false).await;
    // 200 nested arrays: valid JSON to PostgreSQL, past serde_json's
    // recursion limit.
    let deep = format!("{}{}", "[".repeat(200), "]".repeat(200));
    let undecodable = insert_job(&db, "tasks.decode", "tasks:decode", &deep, false).await;
    for id in [panic, flaky, permanent, undecodable] {
        wait_state(&db, id, "dead", PATIENCE).await;
    }
    let attempts_of = "SELECT attempts::bigint FROM merezha_jobs.jobs WHERE id = $1";
    assert_eq!(int(&db, attempts_of, panic).await, 1);
    assert_eq!(
        last_error(&db, panic).await,
        (Some("handler_panic".into()), Some("permanent".into()))
    );
    assert_eq!(int(&db, attempts_of, permanent).await, 1);
    assert_eq!(
        last_error(&db, permanent).await,
        (Some("bad_address".into()), Some("permanent".into()))
    );
    assert_eq!(int(&db, attempts_of, undecodable).await, 1);
    assert_eq!(
        last_error(&db, undecodable).await,
        (Some("invalid_job_payload".into()), Some("permanent".into()))
    );
    assert_eq!(decoded.load(Ordering::SeqCst), 0, "the handler never ran");
    assert_eq!(int(&db, attempts_of, flaky).await, 4);
    assert_eq!(
        last_error(&db, flaky).await,
        (Some("provider_busy".into()), Some("transient".into()))
    );
    // Equal jitter: retry i waits between half and all of the capped
    // delay 20 ms * 2^(i-1).
    let delays = delays.lock().unwrap().clone();
    assert_eq!(delays.len(), 3);
    for (index, delay) in delays.iter().enumerate() {
        let full = 20.0 * 2f64.powi(index as i32);
        assert!(
            *delay >= full / 2.0 - 0.01 && *delay <= full + 0.01,
            "retry {index}: {delay} ms outside [{}, {full}]",
            full / 2.0
        );
    }
    let outcomes = attempts(&db, flaky).await;
    assert!(outcomes.iter().all(|a| a.1.as_deref() == Some("failed")));
    assert_eq!(
        outcomes.iter().map(|a| a.0).collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    jobs.shutdown(soon()).await;
}

/// B2 test 4: `global_concurrency = 2` across 2 instances with
/// `concurrency = 4` each: never more than 2 jobs running, sampled in the
/// handlers and in the table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_concurrency_caps_running_jobs_across_instances() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = jobs_config(
        &test,
        "[jobs]\npoll_interval_ms = 100\n\
         [jobs.queues.default]\nconcurrency = 4\nglobal_concurrency = 2",
    );
    let current = Arc::new(AtomicUsize::new(0));
    let highest = Arc::new(AtomicUsize::new(0));
    // Handlers hold their slot until the test has sampled a running job,
    // so the sampling below sees one however the threads are scheduled.
    let sampled_running = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (now, top, seen) = (current.clone(), highest.clone(), sampled_running.clone());
    let handler = func(move |_, _| {
        let (now, top, seen) = (now.clone(), top.clone(), seen.clone());
        Box::pin(async move {
            let running = now.fetch_add(1, Ordering::SeqCst) + 1;
            top.fetch_max(running, Ordering::SeqCst);
            let held = Instant::now();
            while !seen.load(Ordering::SeqCst) && held.elapsed() < PATIENCE {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
            now.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        })
    });
    let second_db = Database::connect(&test.config()).unwrap();
    let a = workers(&db, &config, "a", vec![("tasks.capped", handler.clone())]).await;
    let b = workers(&second_db, &config, "b", vec![("tasks.capped", handler)]).await;
    for n in 0..12 {
        insert_job(
            &db,
            "tasks.capped",
            &format!("tasks:capped-{n}"),
            "{}",
            false,
        )
        .await;
    }
    let started = Instant::now();
    let mut sampled = 0;
    loop {
        let running = scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.jobs WHERE state = 'running'",
        )
        .await;
        sampled = sampled.max(running);
        if running > 0 {
            sampled_running.store(true, Ordering::SeqCst);
        }
        assert!(running <= 2, "{running} running");
        if scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.jobs WHERE state = 'succeeded'",
        )
        .await
            == 12
        {
            break;
        }
        assert!(started.elapsed() < PATIENCE, "jobs never finished");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(highest.load(Ordering::SeqCst) <= 2, "{highest:?}");
    assert!(sampled >= 1);
    a.shutdown(soon()).await;
    b.shutdown(soon()).await;
}

/// B2 tests 5 and 9, drain: shutdown with a handler that outlives
/// `drain_timeout_ms = 1000` releases the job with its attempt refunded,
/// the late completion is fenced, and a fresh instance runs the job. Re-
/// leased, dead-lettered and replayed, the job keeps one attempts row per
/// lease.
#[tokio::test]
async fn drain_releases_in_flight_jobs_with_the_attempt_refunded() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let logs = Captured::default();
    let _guard = logs.install();
    let db = database(&test).await;
    let config = jobs_config(
        &test,
        "[jobs]\npoll_interval_ms = 100\ndrain_timeout_ms = 1000",
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let late = Arc::new(AtomicUsize::new(0));
    // The first run ignores `cancel` and completes only when the test opens
    // the gate, after the release: no timing decides which comes first.
    let gate = Arc::new(tokio::sync::Notify::new());
    let (count, finished, opened) = (calls.clone(), late.clone(), gate.clone());
    let handler = func(move |_, _| {
        let call = count.fetch_add(1, Ordering::SeqCst) + 1;
        let (finished, opened) = (finished.clone(), opened.clone());
        Box::pin(async move {
            match call {
                1 => {
                    opened.notified().await;
                    finished.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
                2 => Err(JobFailure::permanent("provider_rejected")),
                _ => Ok(()),
            }
        })
    });
    let first = workers(&db, &config, "first", vec![("tasks.long", handler.clone())]).await;
    let id = insert_job(&db, "tasks.long", "tasks:long", r#"{"n":1}"#, false).await;
    wait_state(&db, id, "running", PATIENCE).await;
    // The handler runs (a lease landing as the drain began is parked, never
    // run: another test).
    let entered = Instant::now();
    while calls.load(Ordering::SeqCst) == 0 {
        assert!(entered.elapsed() < PATIENCE, "the handler never ran");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let leased_at = Instant::now();
    // A 60 s shutdown budget: a drain that ignored `drain_timeout_ms` and
    // its grace would run to the budget, far past the bound below.
    first
        .shutdown(Instant::now() + Duration::from_secs(60))
        .await;
    let took = leased_at.elapsed();
    assert!(
        took >= Duration::from_millis(2900) && took < Duration::from_secs(30),
        "drain timeout (1 s) plus the 2 s grace, far below the 60 s budget: {took:?}"
    );
    assert!(first.ready().await.is_err());
    assert_eq!(state(&db, id).await, "available");
    let attempts_of = "SELECT attempts::bigint FROM merezha_jobs.jobs WHERE id = $1";
    assert_eq!(
        int(&db, attempts_of, id).await,
        0,
        "the attempt is refunded"
    );
    assert_eq!(
        attempts(&db, id).await,
        vec![(1, Some("released".into()), None, "first".into())]
    );
    assert!(logs.text().contains("job.drain_abandoned"));
    assert!(logs.text().contains(&format!("[{id}]")), "{}", logs.text());

    // The handler completes after the release: fenced out.
    gate.notify_one();
    while late.load(Ordering::SeqCst) == 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(state(&db, id).await, "available");
    assert_eq!(int(&db, attempts_of, id).await, 0);
    assert_eq!(attempts(&db, id).await.len(), 1);

    // A fresh instance runs it: the second call fails permanently.
    let second = workers(&db, &config, "second", vec![("tasks.long", handler)]).await;
    wait_state(&db, id, "dead", PATIENCE).await;
    assert_eq!(int(&db, attempts_of, id).await, 1);
    assert_eq!(
        second.replay(id, Some("usr_ops")).await.unwrap(),
        Replay::Replayed
    );
    wait_state(&db, id, "succeeded", PATIENCE).await;
    assert_eq!(int(&db, attempts_of, id).await, 1);
    assert_eq!(
        int(
            &db,
            "SELECT replays::bigint FROM merezha_jobs.jobs WHERE id = $1",
            id
        )
        .await,
        1
    );
    assert_eq!(
        attempts(&db, id).await,
        vec![
            (1, Some("released".into()), None, "first".into()),
            (
                1,
                Some("failed".into()),
                Some("provider_rejected".into()),
                "second".into()
            ),
            (1, Some("succeeded".into()), None, "second".into()),
        ],
        "one row per lease"
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.admin_actions
              WHERE action = 'replay' AND principal = 'usr_ops'"
        )
        .await,
        1
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    second.shutdown(soon()).await;
}

/// B2 test 7: an instance without the kind's handler never leases the job.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_instance_without_the_handler_never_leases_the_job() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = jobs_config(&test, "[jobs]\npoll_interval_ms = 100");
    let old = workers(
        &db,
        &config,
        "old",
        vec![("tasks.other", func(|_, _| ok()))],
    )
    .await;
    let id = insert_job(&db, "tasks.new", "tasks:new", "{}", false).await;
    // A later job of a kind the old instance handles: the lease that takes
    // it would have taken the earlier job first, had it not filtered kinds.
    let later = insert_job(&db, "tasks.other", "tasks:later", "{}", false).await;
    wait_state(&db, later, "succeeded", PATIENCE).await;
    assert_eq!(state(&db, id).await, "available");
    assert!(attempts(&db, id).await.is_empty());
    let new = workers(&db, &config, "new", vec![("tasks.new", func(|_, _| ok()))]).await;
    wait_state(&db, id, "succeeded", PATIENCE).await;
    assert_eq!(
        attempts(&db, id).await,
        vec![(1, Some("succeeded".into()), None, "new".into())]
    );
    old.shutdown(soon()).await;
    new.shutdown(soon()).await;
}

/// A handler whose leasing can be turned off, like content's on a host
/// whose model was superseded.
struct Gated {
    open: Arc<std::sync::atomic::AtomicBool>,
    runs: Arc<AtomicUsize>,
}
impl merezha_platform::ports::JobHandler for Gated {
    fn handle<'a>(
        &'a self,
        _: merezha_platform::ports::JobContext,
        _: serde_json::Value,
    ) -> BoxFuture<'a, Result<(), JobFailure>> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        ok()
    }
    fn accepts_leases(&self) -> bool {
        self.open.load(Ordering::SeqCst)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_kind_whose_handler_refuses_leases_stays_available() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = jobs_config(&test, "[jobs]\npoll_interval_ms = 100");
    let open = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let runs = Arc::new(AtomicUsize::new(0));
    let gated: Arc<dyn merezha_platform::ports::JobHandler> = Arc::new(Gated {
        open: open.clone(),
        runs: runs.clone(),
    });
    let jobs = workers(
        &db,
        &config,
        "gated",
        vec![("tasks.gated", gated), ("tasks.other", func(|_, _| ok()))],
    )
    .await;
    let gated_id = insert_job(&db, "tasks.gated", "tasks:gated", "{}", false).await;
    let other_id = insert_job(&db, "tasks.other", "tasks:other", "{}", false).await;
    // The other kind still runs; the refused one is never leased: the
    // lease that took the later job would have taken it first.
    wait_state(&db, other_id, "succeeded", PATIENCE).await;
    assert_eq!(state(&db, gated_id).await, "available");
    assert!(attempts(&db, gated_id).await.is_empty());
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    open.store(true, Ordering::SeqCst);
    wait_state(&db, gated_id, "succeeded", PATIENCE).await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    jobs.shutdown(soon()).await;
}

/// Gives every job back once, then stops leasing, like content on a host
/// whose model a newer one superseded mid-run.
struct GivesBack {
    open: Arc<std::sync::atomic::AtomicBool>,
}
impl merezha_platform::ports::JobHandler for GivesBack {
    fn handle<'a>(
        &'a self,
        _: merezha_platform::ports::JobContext,
        _: serde_json::Value,
    ) -> BoxFuture<'a, Result<(), JobFailure>> {
        self.open.store(false, Ordering::SeqCst);
        Box::pin(async { Err(JobFailure::release()) })
    }
    fn accepts_leases(&self) -> bool {
        self.open.load(Ordering::SeqCst)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_released_job_refunds_its_attempt_and_runs_on_another_instance() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = jobs_config(&test, "[jobs]\npoll_interval_ms = 100");
    let open = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let old = workers(
        &db,
        &config,
        "old",
        vec![("tasks.gated", Arc::new(GivesBack { open: open.clone() }))],
    )
    .await;
    let id = insert_job(&db, "tasks.gated", "tasks:gated", "{}", false).await;
    let started = Instant::now();
    while attempts(&db, id)
        .await
        .first()
        .and_then(|a| a.1.clone())
        .is_none()
    {
        assert!(started.elapsed() < PATIENCE, "never released");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(state(&db, id).await, "available");
    assert_eq!(
        int(
            &db,
            "SELECT attempts::int8 FROM merezha_jobs.jobs WHERE id = $1",
            id
        )
        .await,
        0
    );
    assert_eq!(
        attempts(&db, id).await,
        vec![(1, Some("released".into()), None, "old".into())]
    );
    let new = workers(
        &db,
        &config,
        "new",
        vec![("tasks.gated", func(|_, _| ok()))],
    )
    .await;
    wait_state(&db, id, "succeeded", PATIENCE).await;
    assert_eq!(
        attempts(&db, id).await,
        vec![
            (1, Some("released".into()), None, "old".into()),
            (1, Some("succeeded".into()), None, "new".into())
        ]
    );
    old.shutdown(soon()).await;
    new.shutdown(soon()).await;
}

/// Records what the reissue hook was asked.
#[derive(Default)]
struct Reissued(Mutex<Vec<ReissueRequest>>);
impl Reissue for Reissued {
    fn reissue(&self, request: ReissueRequest) -> BoxFuture<'_, Result<(), Failure>> {
        self.0.lock().unwrap().push(request);
        Box::pin(async { Ok(()) })
    }
}

/// B2 test 8, terminal nulling (R3): a flagged job's payload is NULL after
/// success, dead letter (handler and reaper paths) and cancel. Replaying a
/// flagged dead job calls the kind's `reissue` hook, or fails
/// `secret_purged` without one; the admin reads never show a flagged
/// payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flagged_payloads_are_nulled_at_every_terminal_state() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = jobs_config(
        &test,
        "[jobs]\npoll_interval_ms = 100\nreaper_interval_ms = 100\ndrain_timeout_ms = 1000\n\
         [jobs.queues.default]\nconcurrency = 8\nlease_ms = 1000",
    );
    // No heartbeat, so the hanging job's lease expires.
    let jobs = PgJobs::with_mode(&config, RelayMode::Interval).without_heartbeat();
    jobs.ports(&db);
    let tasks = handle(&jobs, "tasks");
    let hook = Arc::new(Reissued::default());
    let failing = || func(|_, _| Box::pin(async { Err(JobFailure::permanent("refused")) }));
    tasks
        .register(
            "tasks.ok",
            "default",
            func(|_, _| ok()),
            KindOptions::default(),
        )
        .unwrap();
    tasks
        .register(
            "tasks.hang",
            "default",
            // Hangs until the drain cancels it: only the reaper ends it.
            func(|ctx, _| {
                Box::pin(async move {
                    let mut cancel = ctx.cancel.clone();
                    let _ = cancel.wait_for(|c| *c).await;
                    Ok(())
                })
            }),
            KindOptions::default(),
        )
        .unwrap();
    tasks
        .register(
            "tasks.reissued",
            "default",
            failing(),
            KindOptions {
                reissue: Some(hook.clone()),
            },
        )
        .unwrap();
    tasks
        .register("tasks.purged", "default", failing(), KindOptions::default())
        .unwrap();
    jobs.start(instance_context(&db, "nulling")).await.unwrap();
    wait_ready(&jobs).await;

    let secret = r#"{"link":"https://app.example/m/t0k3n-s3cr3t"}"#;
    let succeeded = insert_job(&db, "tasks.ok", "tasks:ok", secret, true).await;
    let plain = insert_job(&db, "tasks.ok", "tasks:plain", r#"{"n":1}"#, false).await;
    let reaped = insert_job_on(&db, "default", "tasks.hang", "tasks:hang", secret, true, 1).await;
    let reissued = insert_job(&db, "tasks.reissued", "tasks:reissued", secret, true).await;
    let purged = insert_job(&db, "tasks.purged", "tasks:purged", secret, true).await;
    let waiting = insert_job(&db, "tasks.unhandled", "tasks:waiting", secret, true).await;
    wait_state(&db, succeeded, "succeeded", PATIENCE).await;
    wait_state(&db, plain, "succeeded", PATIENCE).await;
    wait_state(&db, reissued, "dead", PATIENCE).await;
    wait_state(&db, purged, "dead", PATIENCE).await;
    wait_state(&db, reaped, "dead", PATIENCE).await;
    // Not terminal yet, so the row still holds the secret: never shown.
    assert!(!payload_is_null(&db, waiting).await);
    assert_eq!(jobs.job(waiting, true).await.unwrap().payload, None);
    jobs.cancel_job(waiting, Some("usr_ops")).await.unwrap();
    for id in [succeeded, reaped, reissued, purged, waiting] {
        assert!(payload_is_null(&db, id).await, "job {id} kept its secret");
    }
    assert!(!payload_is_null(&db, plain).await);
    assert_eq!(
        last_error(&db, reaped).await,
        (Some("lease_expired".into()), Some("transient".into()))
    );
    assert_eq!(
        attempts(&db, reaped).await,
        vec![lease_expired(1, "nulling")]
    );
    assert_eq!(state(&db, waiting).await, "cancelled");

    // Replay: the hook issues the effect again, or the replay is refused.
    assert_eq!(
        jobs.replay(reissued, Some("usr_ops")).await.unwrap(),
        Replay::Reissued
    );
    let requests = hook.0.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].job_id, reissued);
    assert_eq!(requests[0].idempotency_key, "tasks:reissued");
    assert_eq!(requests[0].kind.as_str(), "tasks.reissued");
    assert_eq!(state(&db, reissued).await, "cancelled");
    let replayed = jobs.replay(purged, Some("usr_ops")).await.unwrap_err();
    assert_eq!(
        (replayed.code.as_str(), replayed.classification),
        ("secret_purged", merezha_domain::Classification::Conflict)
    );
    assert_eq!(state(&db, purged).await, "dead");
    assert_eq!(jobs.replay(plain, None).await.unwrap_err().code, "not_dead");

    // The admin reads never return a flagged payload.
    for id in [succeeded, reaped, reissued, purged, waiting] {
        let detail = jobs.job(id, true).await.unwrap();
        assert!(detail.job.contains_secret);
        assert_eq!(detail.payload, None);
        assert!(!serde_json::to_string(&detail).unwrap().contains("t0k3n"));
    }
    assert_eq!(
        jobs.job(plain, true).await.unwrap().payload,
        Some(json!({"n": 1}))
    );
    assert_eq!(jobs.job(plain, false).await.unwrap().payload, None);
    jobs.shutdown(soon()).await;
}

/// Heartbeats keep a lease alive past `lease_ms`, and a handler running
/// past `timeout_ms` is cancelled and retried as transient `timeout`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeats_extend_leases_and_timeouts_are_transient() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = jobs_config(
        &test,
        "[jobs]\npoll_interval_ms = 100\nreaper_interval_ms = 100\n\
         [jobs.queues.default]\nlease_ms = 3000\ntimeout_ms = 30000\nmax_attempts = 1\n\
         [jobs.queues.short]\ntimeout_ms = 1000\nmax_attempts = 1",
    );
    let jobs = PgJobs::with_mode(&config, RelayMode::Interval);
    jobs.ports(&db);
    let tasks = handle(&jobs, "tasks");
    tasks
        .register(
            "tasks.slow",
            "default",
            func(|_, _| {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_millis(7500)).await;
                    Ok(())
                })
            }),
            KindOptions::default(),
        )
        .unwrap();
    tasks
        .register(
            "tasks.stuck",
            "short",
            func(|ctx, _| {
                Box::pin(async move {
                    let mut cancel = ctx.cancel.clone();
                    let _ = cancel.wait_for(|c| *c).await;
                    Err(JobFailure::transient("cancelled", None))
                })
            }),
            KindOptions::default(),
        )
        .unwrap();
    jobs.start(instance_context(&db, "beating")).await.unwrap();
    wait_ready(&jobs).await;
    // Lease 3 s, heartbeat every second, reaper every 100 ms: the 7.5 s
    // handler (two and a half leases) keeps its one lease.
    let slow = insert_job_on(&db, "default", "tasks.slow", "tasks:slow", "{}", false, 1).await;
    wait_state(&db, slow, "succeeded", PATIENCE).await;
    assert_eq!(
        attempts(&db, slow).await,
        vec![(1, Some("succeeded".into()), None, "beating".into())]
    );
    let stuck = insert_job_on(&db, "short", "tasks.stuck", "tasks:stuck", "{}", false, 1).await;
    wait_state(&db, stuck, "dead", PATIENCE).await;
    assert_eq!(
        last_error(&db, stuck).await,
        (Some("timeout".into()), Some("transient".into()))
    );
    jobs.shutdown(soon()).await;
}

/// The dispatcher wakes on `merezha_jobs` notifications, so with a one
/// minute poll a newly enqueued job still runs long before the next poll.
/// The job is enqueued only once the dispatcher is parked with every other
/// wake consumed (the start poll, the freed slot of a first job, the new
/// `LISTEN` session's wake): only the notification can have run it within
/// `PATIENCE`, half the poll interval.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn notifications_wake_the_dispatcher() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = jobs_config(&test, "[jobs]\npoll_interval_ms = 60000");
    let first = insert_job(&db, "tasks.remind", "tasks:before-start", "{}", false).await;
    let jobs = workers_with(
        PgJobs::with_mode(&config, RelayMode::Listen),
        &db,
        "listening",
        vec![("tasks.remind", func(|_, _| ok()))],
    )
    .await;
    wait_state(&db, first, "succeeded", PATIENCE).await;
    let deadline = Instant::now() + PATIENCE;
    while !jobs.dispatchers_settled() {
        assert!(Instant::now() < deadline, "the dispatcher never settled");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let enqueued = handle(&jobs, "tasks")
        .enqueue(merezha_platform::ports::NewJob {
            queue: id("default"),
            kind: id("tasks.remind"),
            idempotency_key: merezha_platform::ports::DedupeKey::new("r1").unwrap(),
            payload: json!({}),
            contains_secret: false,
            run_at: None,
            priority: 0,
            context: message(),
        })
        .await
        .unwrap();
    wait_state(&db, enqueued.id, "succeeded", PATIENCE).await;
    jobs.shutdown(soon()).await;
}

/// `jobs.readiness.max_lag_ms`: readiness fails while the oldest due job is
/// older, and holds without the key.
#[tokio::test]
async fn backlog_counts_for_readiness_only_with_max_lag() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let id = insert_job(&db, "tasks.nobody", "tasks:old", "{}", false).await;
    execute(
        &db,
        &format!("UPDATE merezha_jobs.jobs SET run_at = now() - interval '1 hour' WHERE id = {id}"),
    )
    .await
    .unwrap();
    let relaxed = workers(
        &db,
        &jobs_config(&test, "[jobs]\nreaper_interval_ms = 100"),
        "relaxed",
        vec![],
    )
    .await;
    // Ready after a whole reaper pass, the one that would have measured
    // the backlog had `max_lag_ms` been set.
    let deadline = Instant::now() + PATIENCE;
    while relaxed.reaper_passes() < 1 {
        assert!(Instant::now() < deadline, "the reaper never ran");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(relaxed.ready().await.is_ok());
    relaxed.shutdown(soon()).await;
    // The strict instance starts with nothing due, so it becomes ready:
    // only a lag measurement can take readiness away afterwards.
    let date = |when: &'static str| {
        format!(
            "UPDATE merezha_jobs.jobs SET run_at = now() {when} interval '1 hour' WHERE id = {id}"
        )
    };
    execute(&db, &date("+")).await.unwrap();
    let strict = PgJobs::with_mode(
        &jobs_config(
            &test,
            "[jobs]\nreaper_interval_ms = 100\n[jobs.readiness]\nmax_lag_ms = 1000",
        ),
        RelayMode::Interval,
    );
    strict.ports(&db);
    strict.start(instance_context(&db, "strict")).await.unwrap();
    let deadline = Instant::now() + PATIENCE;
    while strict.ready().await.is_err() {
        assert!(Instant::now() < deadline, "nothing due, yet never ready");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    execute(&db, &date("-")).await.unwrap();
    while strict.ready().await.is_ok() {
        assert!(Instant::now() < deadline, "an hour of lag is not ready");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    execute(
        &db,
        &format!(
            "UPDATE merezha_jobs.jobs SET state = 'cancelled', finished_at = now() WHERE id = {id}"
        ),
    )
    .await
    .unwrap();
    while strict.ready().await.is_err() {
        assert!(Instant::now() < deadline, "readiness never recovered");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    strict.shutdown(soon()).await;
}

/// Review item 3: a lease that completes after the drain began is held
/// before its dispatcher exits, never run, and released with its attempt
/// refunded. The lease is held back on the queue's advisory lock until the
/// workers report the stop (`workers_stopping`), so the interleaving is
/// forced, not timed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lease_racing_the_drain_is_released_with_its_attempt_refunded() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = jobs_config(
        &test,
        "[jobs]\npoll_interval_ms = 100\n[jobs.queues.default]\nglobal_concurrency = 5",
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    // The lease statement waits on the queue lock for as long as the test
    // holds it: give it more than the default 5 s (lease deadline and
    // statement timeout), so a slow machine cannot time it out before the
    // release.
    let mut patient = test.config();
    patient.statement_timeout = PATIENCE * 2;
    let leasing = Database::connect(&patient).unwrap();
    let jobs = workers_with(
        PgJobs::with_mode(&config, RelayMode::Interval).with_lease_deadline(PATIENCE * 2),
        &leasing,
        "racing",
        vec![(
            "tasks.race",
            func(move |_, _| {
                count.fetch_add(1, Ordering::SeqCst);
                ok()
            }),
        )],
    )
    .await;
    let holder = Database::connect(&test.config()).unwrap();
    let session = holder.detach(soon()).await.unwrap();
    session
        .execute(
            "SELECT pg_advisory_lock(1297762882, hashtext('default'))",
            &[],
        )
        .await
        .unwrap();
    let id = insert_job(&db, "tasks.race", "tasks:race", "{}", false).await;
    // A lease transaction that began waiting before the insert compares
    // `run_at` with its own start time: date the job back so whichever
    // lease gets the lock sees it as due.
    execute(
        &db,
        &format!("UPDATE merezha_jobs.jobs SET run_at = now() - interval '1 hour' WHERE id = {id}"),
    )
    .await
    .unwrap();
    // The dispatcher's lease waits on the queue lock.
    wait_for(
        &db,
        "SELECT count(*) FROM pg_locks
          WHERE locktype = 'advisory' AND NOT granted
            AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
        1,
        PATIENCE,
    )
    .await;
    let draining = jobs.clone();
    let drain = tokio::spawn(async move {
        draining
            .shutdown(Instant::now() + Duration::from_secs(60))
            .await
    });
    let stopping = Instant::now();
    while !jobs.workers_stopping() {
        assert!(stopping.elapsed() < PATIENCE, "the drain never began");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM pg_locks
              WHERE locktype = 'advisory' AND NOT granted
                AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
        )
        .await,
        1,
        "the lease still waits on the queue lock when the drain begins"
    );
    session
        .execute(
            "SELECT pg_advisory_unlock(1297762882, hashtext('default'))",
            &[],
        )
        .await
        .unwrap();
    drain.await.unwrap();
    assert_eq!(state(&db, id).await, "available");
    assert_eq!(
        int(
            &db,
            "SELECT attempts::bigint FROM merezha_jobs.jobs WHERE id = $1",
            id
        )
        .await,
        0
    );
    assert_eq!(
        attempts(&db, id).await,
        vec![(1, Some("released".into()), None, "racing".into())]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0, "never run");
}

/// Review item 4: concurrent lease statements on several instances (one
/// notification wakes them all; a trigger makes each lease slow) still never
/// run more than `global_concurrency` jobs. Without the queue's advisory
/// lock every statement counts zero running jobs and leases its share.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_concurrency_serialises_concurrent_lease_statements() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    batch(
        &db,
        "CREATE FUNCTION public.slow_lease() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF OLD.state = 'available' AND NEW.state = 'running' THEN
                 PERFORM pg_sleep(0.3);
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER slow_lease BEFORE UPDATE ON merezha_jobs.jobs
             FOR EACH ROW EXECUTE FUNCTION public.slow_lease();",
    )
    .await;
    let config = jobs_config(
        &test,
        "[jobs]\npoll_interval_ms = 60000\n\
         [jobs.queues.default]\nconcurrency = 4\nglobal_concurrency = 2",
    );
    let current = Arc::new(AtomicUsize::new(0));
    let highest = Arc::new(AtomicUsize::new(0));
    let (now, top) = (current.clone(), highest.clone());
    let handler = func(move |_, _| {
        let (now, top) = (now.clone(), top.clone());
        Box::pin(async move {
            let running = now.fetch_add(1, Ordering::SeqCst) + 1;
            top.fetch_max(running, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(1000)).await;
            now.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        })
    });
    let mut instances = Vec::new();
    for name in ["a", "b", "c"] {
        let pool = Database::connect(&test.config()).unwrap();
        instances.push(
            workers_with(
                PgJobs::with_mode(&config, RelayMode::Listen),
                &pool,
                name,
                vec![("tasks.capped", handler.clone())],
            )
            .await,
        );
    }
    wait_for(
        &db,
        "SELECT count(*) FROM pg_stat_activity
          WHERE datname = current_database() AND query = 'LISTEN merezha_jobs;'",
        3,
        PATIENCE,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    // One transaction: six jobs and one notification for every dispatcher.
    batch(
        &db,
        "INSERT INTO merezha_jobs.jobs (queue, kind, idempotency_key, payload, payload_digest,
             max_attempts, correlation_id)
         SELECT 'default', 'tasks.capped', 'tasks:burst-' || n, '{}', sha256(convert_to('{}', 'UTF8')), 10, 'c'
           FROM generate_series(1, 6) AS n;
         SELECT pg_notify('merezha_jobs', 'default');",
    )
    .await;
    // Sample for 2.5 s and at least until a job ran (a slow machine leases
    // later, never more at once).
    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(2500) || highest.load(Ordering::SeqCst) == 0 {
        assert!(started.elapsed() < PATIENCE, "no job ever ran");
        let running = scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.jobs WHERE state = 'running'",
        )
        .await;
        assert!(running <= 2, "{running} running");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(highest.load(Ordering::SeqCst) <= 2, "{highest:?}");
    assert!(highest.load(Ordering::SeqCst) >= 1);
    for jobs in instances {
        jobs.shutdown(soon()).await;
    }
}

/// Review item 5: when the heartbeat finds the lease gone, the handler's
/// `cancel` fires and its result is discarded (`job.lease_lost`).
#[tokio::test]
async fn a_lost_heartbeat_fires_cancel_and_discards_the_result() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let logs = Captured::default();
    let _guard = logs.install();
    let db = database(&test).await;
    let config = jobs_config(
        &test,
        "[jobs]\npoll_interval_ms = 100\nreaper_interval_ms = 600000\n\
         [jobs.queues.default]\nlease_ms = 1500\ntimeout_ms = 900000",
    );
    let seen = Arc::new(Mutex::new(None::<Duration>));
    let record = seen.clone();
    let jobs = workers(
        &db,
        &config,
        "losing",
        vec![(
            "tasks.watch",
            func(move |ctx, _| {
                let record = record.clone();
                Box::pin(async move {
                    let started = Instant::now();
                    let mut cancel = ctx.cancel.clone();
                    let _ = tokio::time::timeout(PATIENCE, cancel.wait_for(|c| *c)).await;
                    if *ctx.cancel.borrow() {
                        *record.lock().unwrap() = Some(started.elapsed());
                    }
                    Err(JobFailure::transient("cancelled", None))
                })
            }),
        )],
    )
    .await;
    let id = insert_job(&db, "tasks.watch", "tasks:watch", "{}", false).await;
    wait_state(&db, id, "running", PATIENCE).await;
    // Another holder took the lease (a reap and a re-lease elsewhere).
    execute(
        &db,
        &format!("UPDATE merezha_jobs.jobs SET lease_token = gen_random_uuid() WHERE id = {id}"),
    )
    .await
    .unwrap();
    let deadline = Instant::now() + PATIENCE;
    while !logs.text().contains("job.lease_lost") {
        assert!(
            Instant::now() < deadline,
            "no job.lease_lost: {}",
            logs.text()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // `timeout_ms` is 15 min, so only the lost lease can have fired it.
    let waited = seen.lock().unwrap().expect("cancel fired");
    assert!(waited < PATIENCE, "cancel after {waited:?}");
    // The failure was fenced out: the other holder's lease is untouched.
    assert_eq!(state(&db, id).await, "running");
    assert_eq!(last_error(&db, id).await, (None, None));
    jobs.shutdown(soon()).await;
}

/// Review item 6 (design deviation 4): a handler that fails after the drain
/// cutoff fired `cancel` is released with its attempt refunded, not retried.
#[tokio::test]
async fn a_failure_after_the_drain_cutoff_is_released_not_retried() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = jobs_config(
        &test,
        "[jobs]\npoll_interval_ms = 100\ndrain_timeout_ms = 200",
    );
    let returned = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(AtomicUsize::new(0));
    let (failed, running) = (returned.clone(), entered.clone());
    let jobs = workers(
        &db,
        &config,
        "cut",
        vec![(
            "tasks.cut",
            func(move |ctx, _| {
                let (failed, running) = (failed.clone(), running.clone());
                Box::pin(async move {
                    running.fetch_add(1, Ordering::SeqCst);
                    let mut cancel = ctx.cancel.clone();
                    let _ = cancel.wait_for(|c| *c).await;
                    failed.fetch_add(1, Ordering::SeqCst);
                    Err(JobFailure::transient("cancelled", None))
                })
            }),
        )],
    )
    .await;
    let id = insert_job(&db, "tasks.cut", "tasks:cut", "{}", false).await;
    wait_state(&db, id, "running", PATIENCE).await;
    // The handler runs: a lease landing as the drain began would be parked
    // and never run, which is another test.
    let started = Instant::now();
    while entered.load(Ordering::SeqCst) == 0 {
        assert!(started.elapsed() < PATIENCE, "the handler never ran");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    jobs.shutdown(Instant::now() + PATIENCE).await;
    assert_eq!(
        returned.load(Ordering::SeqCst),
        1,
        "the handler failed after the cutoff, inside the grace"
    );
    assert_eq!(state(&db, id).await, "available");
    assert_eq!(
        int(
            &db,
            "SELECT attempts::bigint FROM merezha_jobs.jobs WHERE id = $1",
            id
        )
        .await,
        0
    );
    assert_eq!(last_error(&db, id).await, (None, None));
    assert_eq!(
        attempts(&db, id).await,
        vec![(1, Some("released".into()), None, "cut".into())]
    );
}
