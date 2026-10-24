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
    let deadline = Instant::now() + Duration::from_secs(5);
    while hook.calls.load(Ordering::SeqCst) < 3 {
        assert!(Instant::now() < deadline, "the detached run stopped");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
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
    let deadline = Instant::now() + Duration::from_secs(3);
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
