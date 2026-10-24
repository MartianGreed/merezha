//! Reissue of flagged dead jobs against real PostgreSQL (AP-B2 review item
//! 1): a failing hook returns the job to `dead`, a dropped request cannot
//! leave a claim behind, and a claim cut by a crash is returned to `dead`
//! by the reaper. Skips with a message when `MEREZHA_TEST_DATABASE_URL` is
//! unset.
mod support;
use axum::{Router, body::Body, http::Request};
use merezha_domain::Failure;
use merezha_jobs::{PgJobs, RelayMode};
use merezha_platform::{
    BoxFuture, Database, JobsRuntime, Module,
    ports::{KindOptions, LoopbackAllowAll, Reissue, ReissueRequest},
};
use merezha_postgres::TestDatabase;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use support::*;
use tower::ServiceExt;

/// A hook that waits `delay`, counts its calls and then fails or not.
struct Hook {
    delay: Duration,
    fail: bool,
    calls: AtomicUsize,
}
impl Hook {
    fn new(delay: Duration, fail: bool) -> Arc<Self> {
        Arc::new(Self {
            delay,
            fail,
            calls: AtomicUsize::new(0),
        })
    }
}
impl Reissue for Hook {
    fn reissue(&self, _: ReissueRequest) -> BoxFuture<'_, Result<(), Failure>> {
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(Failure::transient("provider_down"))
            } else {
                Ok(())
            }
        })
    }
}

/// A runtime whose `tasks` module registered `tasks.mail` with `hook`.
async fn runtime(db: &Database, config: &str, test: &TestDatabase, hook: Arc<Hook>) -> PgJobs {
    let jobs = PgJobs::with_mode(&jobs_config(test, config), RelayMode::Interval);
    jobs.ports(db);
    handle(&jobs, "tasks")
        .register(
            "tasks.mail",
            "default",
            func(|_, _| Box::pin(async { Ok(()) })),
            KindOptions {
                reissue: Some(hook),
            },
        )
        .unwrap();
    jobs.start(instance_context(db, "reissue")).await.unwrap();
    wait_ready(&jobs).await;
    jobs
}
/// A flagged job, already dead with its payload nulled.
async fn flagged_dead(db: &Database, key: &str) -> i64 {
    let id = insert_job(db, "tasks.mail", key, r#"{"link":"secret"}"#, true).await;
    execute(
        db,
        &format!(
            "UPDATE merezha_jobs.jobs SET state = 'dead', attempts = 1, finished_at = now(),
                    payload = NULL, last_error_code = 'refused', last_error_class = 'permanent'
              WHERE id = {id}"
        ),
    )
    .await
    .unwrap();
    id
}
/// `(state, replays, claimed)` of job `id`.
async fn claim_of(db: &Database, id: i64) -> (String, i32, bool) {
    let client = db.acquire(soon()).await.unwrap();
    let row = client
        .query_one(
            "SELECT state, replays, reissue_claimed_at IS NOT NULL FROM merezha_jobs.jobs WHERE id = $1",
            &[&id],
        )
        .await
        .unwrap();
    (row.get(0), row.get(1), row.get(2))
}

#[tokio::test]
async fn a_failing_reissue_hook_returns_the_job_to_dead() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let hook = Hook::new(Duration::ZERO, true);
    let jobs = runtime(&db, "[jobs]\nworkers = false", &test, hook.clone()).await;
    let single = flagged_dead(&db, "tasks:single").await;
    let failure = jobs.replay(single, Some("usr_ops")).await.unwrap_err();
    assert_eq!(failure.code, "provider_down");
    assert_eq!(claim_of(&db, single).await, ("dead".into(), 0, false));
    // The claim was audited when it was made.
    assert_eq!(
        scalar(
            &db,
            &format!(
                "SELECT count(*) FROM merezha_jobs.admin_actions
                  WHERE action = 'reissue' AND target = '{single}' AND principal = 'usr_ops'"
            )
        )
        .await,
        1
    );
    let bulk = flagged_dead(&db, "tasks:bulk").await;
    let replayed = jobs
        .replay_dead(
            &merezha_jobs::DeadFilter {
                queue: "default".into(),
                kind: None,
                code: None,
                dead_after: None,
                dead_before: None,
                limit: 10,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!((replayed.reissued, replayed.refused), (0, 2));
    for id in [single, bulk] {
        assert_eq!(claim_of(&db, id).await, ("dead".into(), 0, false));
    }
    assert_eq!(hook.calls.load(Ordering::SeqCst), 3);
    jobs.shutdown(soon()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_bulk_replay_request_leaves_no_claim_behind() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let hook = Hook::new(Duration::from_millis(200), false);
    let jobs = runtime(&db, "[jobs]\nworkers = false", &test, hook.clone()).await;
    let mut ids = Vec::new();
    for n in 0..3 {
        ids.push(flagged_dead(&db, &format!("tasks:drop-{n}")).await);
    }
    let policy = Arc::new(LoopbackAllowAll::new("127.0.0.1:1".parse().unwrap()).unwrap());
    let router = Router::new().nest("/v1/jobs", jobs.admin_router(policy));
    let request = Request::builder()
        .method("POST")
        .uri("/v1/jobs/admin/dead/replay")
        .body(Body::from(r#"{"queue":"default"}"#))
        .unwrap();
    // The client goes away while the first hook runs.
    let dropped = tokio::time::timeout(Duration::from_millis(100), router.oneshot(request)).await;
    assert!(dropped.is_err(), "the request future was dropped mid-run");
    let deadline = Instant::now() + PATIENCE;
    while hook.calls.load(Ordering::SeqCst) < 3 {
        assert!(Instant::now() < deadline, "the detached run stopped");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The last hook returned; its settle clears the claim right after.
    wait_for(
        &db,
        "SELECT count(*) FROM merezha_jobs.jobs WHERE reissue_claimed_at IS NOT NULL",
        0,
        PATIENCE,
    )
    .await;
    for id in ids {
        assert_eq!(claim_of(&db, id).await, ("cancelled".into(), 1, false));
    }
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.jobs WHERE reissue_claimed_at IS NOT NULL"
        )
        .await,
        0
    );
    jobs.shutdown(soon()).await;
}

#[tokio::test]
async fn a_claim_cut_by_a_crash_is_returned_to_dead_by_the_reaper() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let crashed = flagged_dead(&db, "tasks:crashed").await;
    let running = flagged_dead(&db, "tasks:running").await;
    // A crash between claim and hook: claimed two minutes ago, the dead
    // letter past the finished-jobs retention (7 days).
    for (id, age) in [(crashed, "2 minutes"), (running, "1 second")] {
        execute(
            &db,
            &format!(
                "UPDATE merezha_jobs.jobs SET state = 'cancelled', replays = 1,
                        reissue_claimed_at = now() - interval '{age}',
                        finished_at = now() - interval '8 days'
                  WHERE id = {id}"
            ),
        )
        .await
        .unwrap();
    }
    let idle = PgJobs::with_mode(
        &jobs_config(&test, "[jobs]\nworkers = false"),
        RelayMode::Off,
    );
    idle.ports(&db);
    assert_eq!(
        idle.purge_expired().await.unwrap().finished_jobs,
        0,
        "claims are never purged"
    );
    let hook = Hook::new(Duration::ZERO, false);
    let jobs = runtime(&db, "[jobs]\nreaper_interval_ms = 100", &test, hook).await;
    let deadline = Instant::now() + PATIENCE;
    while claim_of(&db, crashed).await.0 != "dead" {
        assert!(Instant::now() < deadline, "the claim was never reverted");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(claim_of(&db, crashed).await, ("dead".into(), 0, false));
    assert_eq!(
        claim_of(&db, running).await,
        ("cancelled".into(), 1, true),
        "a claim younger than the timeout is left to its hook"
    );
    assert_eq!(
        jobs.replay(crashed, None).await.unwrap(),
        merezha_jobs::Replay::Reissued,
        "the reverted job can be replayed"
    );
    jobs.shutdown(soon()).await;
}

/// Everything the log subscriber writes, as text (current-thread tests).
#[derive(Clone, Default)]
struct Captured(Arc<std::sync::Mutex<Vec<u8>>>);
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
}

/// The first call waits for the test to open `gate`, then fails; every
/// later one succeeds at once.
#[derive(Default)]
struct SlowThenFast(AtomicUsize, tokio::sync::Notify);
impl Reissue for SlowThenFast {
    fn reissue(&self, _: ReissueRequest) -> BoxFuture<'_, Result<(), Failure>> {
        let call = self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if call == 0 {
                self.1.notified().await;
                Err(Failure::transient("provider_down"))
            } else {
                Ok(())
            }
        })
    }
}

/// Re-review F1: a slow hook A outlives its claim, the reaper reverts it, a
/// replay B claims and reissues; A's late failure is fenced on its own
/// claim, so it neither kills B's job nor reports `Reissued`. A returns only
/// once the test opens its gate after B, so the interleaving is forced.
#[tokio::test]
async fn a_late_settle_of_a_reverted_claim_is_fenced_out() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let logs = Captured::default();
    let writer = logs.clone();
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .json()
            .with_writer(move || writer.clone())
            .finish(),
    );
    let db = database(&test).await;
    let jobs = PgJobs::with_mode(
        &jobs_config(&test, "[jobs]\nreaper_interval_ms = 100"),
        RelayMode::Interval,
    )
    .with_reissue_timing(
        PATIENCE * 2,
        Duration::from_millis(300),
        Duration::from_secs(30),
    );
    jobs.ports(&db);
    let hook = Arc::new(SlowThenFast::default());
    handle(&jobs, "tasks")
        .register(
            "tasks.mail",
            "default",
            func(|_, _| Box::pin(async { Ok(()) })),
            KindOptions {
                reissue: Some(hook.clone()),
            },
        )
        .unwrap();
    jobs.start(instance_context(&db, "fenced")).await.unwrap();
    wait_ready(&jobs).await;
    let id = flagged_dead(&db, "tasks:fenced").await;
    let first = jobs.clone();
    let slow = tokio::spawn(async move { first.replay(id, Some("usr_a")).await });
    // A's hook runs only once its claim committed: wait for the hook, not
    // for the claimed row, which the reaper may revert before a starved
    // poll sees it.
    let deadline = Instant::now() + PATIENCE;
    while hook.0.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline, "A never claimed");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // The reaper reverts A's claim once it is 300 ms old.
    while claim_of(&db, id).await != ("dead".into(), 0, false) {
        assert!(Instant::now() < deadline, "the claim was never reverted");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        jobs.replay(id, Some("usr_b")).await.unwrap(),
        merezha_jobs::Replay::Reissued
    );
    assert_eq!(claim_of(&db, id).await, ("cancelled".into(), 1, false));
    hook.1.notify_one();
    let late = slow.await.unwrap().unwrap_err();
    assert_eq!(
        (late.code.as_str(), late.classification),
        (
            "reissue_claim_lost",
            merezha_domain::Classification::Conflict
        )
    );
    assert_eq!(
        claim_of(&db, id).await,
        ("cancelled".into(), 1, false),
        "B's reissue stands"
    );
    assert_eq!(hook.0.load(Ordering::SeqCst), 2);
    assert!(
        logs.text().contains("job.reissue_claim_lost"),
        "{}",
        logs.text()
    );
    jobs.shutdown(soon()).await;
}

/// Re-review F2: a bulk replay outlasting the response wait answers `202`
/// with partial counts, and the detached run finishes every row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bulk_replay_past_the_wait_answers_202_and_finishes_detached() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let jobs = PgJobs::with_mode(
        &jobs_config(&test, "[jobs]\nworkers = false"),
        RelayMode::Off,
    )
    .with_reissue_timing(
        Duration::from_secs(5),
        Duration::from_secs(60),
        Duration::from_millis(200),
    );
    jobs.ports(&db);
    let hook = Hook::new(Duration::from_millis(150), false);
    handle(&jobs, "tasks")
        .register(
            "tasks.mail",
            "default",
            func(|_, _| Box::pin(async { Ok(()) })),
            KindOptions {
                reissue: Some(hook.clone()),
            },
        )
        .unwrap();
    jobs.start(instance_context(&db, "partial")).await.unwrap();
    wait_ready(&jobs).await;
    let mut ids = Vec::new();
    for n in 0..4 {
        ids.push(flagged_dead(&db, &format!("tasks:partial-{n}")).await);
    }
    let policy = Arc::new(LoopbackAllowAll::new("127.0.0.1:1".parse().unwrap()).unwrap());
    let router = Router::new().nest("/v1/jobs", jobs.admin_router(policy));
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/jobs/admin/dead/replay")
                .body(Body::from(r#"{"queue":"default"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::ACCEPTED);
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 16)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["complete"], false);
    assert_eq!(body["replayed"], 0);
    assert!(body["reissued"].as_u64().unwrap() < 4, "{body}");
    let deadline = Instant::now() + PATIENCE;
    while hook.calls.load(Ordering::SeqCst) < 4 {
        assert!(Instant::now() < deadline, "the detached run stopped");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The last hook returned; its settle clears the claim right after.
    wait_for(
        &db,
        "SELECT count(*) FROM merezha_jobs.jobs WHERE reissue_claimed_at IS NOT NULL",
        0,
        PATIENCE,
    )
    .await;
    for id in ids {
        assert_eq!(claim_of(&db, id).await, ("cancelled".into(), 1, false));
    }
    jobs.shutdown(soon()).await;
}
