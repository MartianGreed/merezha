//! The jobs admin API, `queue_status()` and retention against real
//! PostgreSQL (AP-B2, D.1 and B.9). Skips with a message when
//! `MEREZHA_TEST_DATABASE_URL` is unset.
mod support;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use merezha_domain::{Failure, MessageKind, Metadata};
use merezha_jobs::{PgJobs, RelayMode};
use merezha_platform::{
    Database, JobsRuntime, Module,
    edge::Authenticated,
    ports::{
        AccessDenied, Allowed, Authorizer, DenyAll, DenyReason, GrantSnapshot, JobFailure,
        LoopbackAllowAll, Permission, Principal, ResourceScope,
    },
};
use merezha_postgres::TestDatabase;
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc};
use support::*;
use tower::ServiceExt;

fn admin(jobs: &PgJobs, policy: Arc<dyn Authorizer>) -> Router {
    Router::new().nest("/v1/jobs", jobs.admin_router(policy))
}
fn allow_all() -> Arc<dyn Authorizer> {
    Arc::new(LoopbackAllowAll::new("127.0.0.1:1".parse().unwrap()).unwrap())
}
/// One request as `sys_ops`, or anonymous.
async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    body: &str,
    signed_in: bool,
) -> (StatusCode, Value) {
    let mut request = Request::builder().method(method).uri(uri);
    if signed_in {
        let principal = Principal::system("ops", GrantSnapshot::empty()).unwrap();
        request = request.extension(Authenticated(Some(Arc::new(principal))));
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
async fn get(router: &Router, uri: &str) -> (StatusCode, Value) {
    call(router, "GET", uri, "", true).await
}
async fn post(router: &Router, uri: &str, body: &str) -> (StatusCode, Value) {
    call(router, "POST", uri, body, true).await
}
fn code(body: &Value) -> &str {
    body["error"]["code"].as_str().unwrap_or("")
}

/// B2 test 6: dead-letter list, single and bulk replay, purge and cancel,
/// plus the outbox listing and requeue, each write with its audit row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_admin_api_lists_replays_purges_and_cancels_with_audit_rows() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = jobs_config(&test, "[jobs]\npoll_interval_ms = 100");
    let jobs = workers(
        &db,
        &config,
        "admin",
        vec![(
            "tasks.fail",
            func(|_, _| Box::pin(async { Err(JobFailure::permanent("refused")) })),
        )],
    )
    .await;
    let mut dead = Vec::new();
    for n in 0..3 {
        dead.push(
            insert_job(
                &db,
                "tasks.fail",
                &format!("tasks:fail-{n}"),
                r#"{"n":1}"#,
                false,
            )
            .await,
        );
    }
    let waiting = insert_job(&db, "tasks.unhandled", "tasks:waiting", r#"{"n":2}"#, false).await;
    for id in &dead {
        wait_state(&db, *id, "dead", PATIENCE).await;
    }
    // Stop the workers so nothing re-runs what the API replays.
    jobs.shutdown(soon()).await;
    let router = admin(&jobs, allow_all());

    // Dead-letter list, by pages, never with payloads.
    let (status, page) = get(
        &router,
        "/v1/jobs/admin/jobs?queue=default&state=dead&limit=2",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    let listed = page["jobs"].as_array().unwrap();
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().all(|job| job.get("payload").is_none()));
    assert_eq!(listed[0]["id"], dead[0]);
    assert_eq!(listed[0]["state"], "dead");
    assert_eq!(listed[0]["last_error_code"], "refused");
    assert_eq!(listed[0]["last_error_class"], "permanent");
    let next = page["next"].as_str().unwrap().to_owned();
    let (_, rest) = get(
        &router,
        &format!("/v1/jobs/admin/jobs?queue=default&state=dead&limit=2&after={next}"),
    )
    .await;
    assert_eq!(rest["jobs"].as_array().unwrap().len(), 1);
    assert!(rest.get("next").is_none());
    let (_, detail) = get(&router, &format!("/v1/jobs/admin/jobs/{}", dead[0])).await;
    assert_eq!(detail["attempts"], 1);
    assert_eq!(detail["attempt_history"][0]["outcome"], "failed");
    assert_eq!(detail["attempt_history"][0]["error_code"], "refused");
    assert!(detail.get("payload").is_none());
    let (_, shown) = get(
        &router,
        &format!("/v1/jobs/admin/jobs/{}?payload=true", dead[0]),
    )
    .await;
    assert_eq!(shown["payload"], json!({"n": 1}));

    // Queue status: TE's state names.
    let (_, queues) = get(&router, "/v1/jobs/admin/queues").await;
    let default = &queues["queues"][0];
    assert_eq!(default["name"], "default");
    assert_eq!(
        (
            &default["ready"],
            &default["delayed"],
            &default["leased"],
            &default["dead"]
        ),
        (&json!(1), &json!(0), &json!(0), &json!(3))
    );
    assert_eq!(default["concurrency"], 2, "unconfigured queue");
    assert_eq!(default["global_concurrency"], 0);

    // Single replay, then refused on a job that is not dead.
    let (status, replayed) = post(
        &router,
        &format!("/v1/jobs/admin/jobs/{}/replay", dead[0]),
        "",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replayed}");
    assert_eq!(replayed, json!({"id": dead[0], "state": "available"}));
    assert_eq!(state(&db, dead[0]).await, "available");
    assert_eq!(
        scalar(
            &db,
            &format!(
                "SELECT attempts::bigint * 10 + replays FROM merezha_jobs.jobs WHERE id = {}",
                dead[0]
            )
        )
        .await,
        1,
        "attempts 0, replays 1"
    );
    let (status, refused) = post(
        &router,
        &format!("/v1/jobs/admin/jobs/{}/replay", dead[0]),
        "",
    )
    .await;
    assert_eq!((status, code(&refused)), (StatusCode::CONFLICT, "not_dead"));

    // Bulk replay, then purge.
    let (status, bulk) = post(
        &router,
        "/v1/jobs/admin/dead/replay",
        r#"{"queue":"default","kind":"tasks.fail","code":"refused","limit":1}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{bulk}");
    assert_eq!(
        bulk,
        json!({"replayed": 1, "reissued": 0, "refused": 0, "complete": true})
    );
    assert_eq!(state(&db, dead[1]).await, "available");
    let before = jiff::Timestamp::now()
        .saturating_add(jiff::SignedDuration::from_hours(1))
        .unwrap();
    let (status, purged) = post(
        &router,
        "/v1/jobs/admin/dead/purge",
        &json!({"queue": "default", "before": before.to_string()}).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{purged}");
    assert_eq!(purged, json!({"purged": 1}));
    assert_eq!(
        scalar(
            &db,
            &format!(
                "SELECT count(*) FROM merezha_jobs.jobs WHERE id = {}",
                dead[2]
            )
        )
        .await,
        0
    );

    // Cancel.
    let (status, cancelled) = post(
        &router,
        &format!("/v1/jobs/admin/jobs/{waiting}/cancel"),
        "",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{cancelled}");
    assert_eq!(cancelled, json!({"id": waiting, "state": "cancelled"}));
    let (status, again) = post(
        &router,
        &format!("/v1/jobs/admin/jobs/{waiting}/cancel"),
        "",
    )
    .await;
    assert_eq!(
        (status, code(&again)),
        (StatusCode::CONFLICT, "not_cancellable")
    );
    let (status, missing) = post(&router, "/v1/jobs/admin/jobs/999999/cancel", "").await;
    assert_eq!(
        (status, code(&missing)),
        (StatusCode::NOT_FOUND, "not_found")
    );
    let (status, _) = get(&router, "/v1/jobs/admin/jobs/abc").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // The outbox: pending, quarantined, requeue.
    execute(
        &db,
        "INSERT INTO merezha_jobs.outbox (source, dedupe_key, record_kind, queue, job_kind, payload,
             correlation_id, quarantined_at, quarantine_code)
         VALUES ('tasks', 'q1', 'job', 'default', 'tasks.fail', '{}', 'c1', now(), 'constraint_violation')",
    )
    .await
    .unwrap();
    let quarantined = scalar(&db, "SELECT id FROM merezha_jobs.outbox").await;
    let (_, outbox) = get(&router, "/v1/jobs/admin/outbox").await;
    assert_eq!(outbox["pending"], 1);
    assert_eq!(outbox["unrouted"], 0);
    assert_eq!(outbox["quarantined"][0]["id"], quarantined);
    assert_eq!(
        outbox["quarantined"][0]["quarantine_code"],
        "constraint_violation"
    );
    assert_eq!(outbox["quarantined"][0]["record_kind"], "job");
    assert!(outbox["quarantined"][0].get("payload").is_none());
    let (status, requeued) = post(
        &router,
        &format!("/v1/jobs/admin/outbox/{quarantined}/requeue"),
        "",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{requeued}");
    assert_eq!(requeued, json!({"id": quarantined}));

    // Every write left an audit row with the principal.
    let client = db.acquire(soon()).await.unwrap();
    let actions: BTreeSet<String> = client
        .query(
            "SELECT action FROM merezha_jobs.admin_actions WHERE principal = 'sys_ops'",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        actions,
        [
            "bulk_replay",
            "cancel",
            "outbox_requeue",
            "purge_dead",
            "replay"
        ]
        .map(String::from)
        .into()
    );

    // Validation lists every problem without echoing input.
    let (status, invalid) = post(
        &router,
        "/v1/jobs/admin/dead/replay",
        r#"{"limit":5000,"secret_field":1}"#,
    )
    .await;
    assert_eq!(
        (status, code(&invalid)),
        (StatusCode::UNPROCESSABLE_ENTITY, "validation_failed")
    );
    let details = invalid["error"]["details"].as_array().unwrap();
    for expected in [
        json!({"path": "/queue", "code": "required"}),
        json!({"path": "/limit", "code": "out_of_range"}),
        json!({"path": "", "code": "unknown_field"}),
    ] {
        assert!(details.contains(&expected), "{expected} in {invalid}");
    }
    assert!(!invalid.to_string().contains("secret_field"));
    let (status, _) = get(&router, "/v1/jobs/admin/jobs?state=bogus").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let (status, _) = get(&router, "/v1/jobs/admin/jobs?queue=default&queue=other").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let (status, malformed) = post(&router, "/v1/jobs/admin/dead/purge", "not json").await;
    assert_eq!(
        (status, code(&malformed)),
        (StatusCode::BAD_REQUEST, "invalid_input")
    );
}

/// Allows exactly the listed permissions, at application scope only.
struct Only(&'static [&'static str]);
impl merezha_application::Authorizer for Only {
    fn authorize(
        &self,
        _: MessageKind,
        _: &str,
        _: Option<&str>,
        _: &Value,
        _: &Metadata,
    ) -> Result<(), Failure> {
        Err(Failure::permanent("unauthorized"))
    }
}
impl Authorizer for Only {
    fn check(
        &self,
        principal: Option<&Principal>,
        permission: &Permission,
        scope: &ResourceScope,
    ) -> Result<Allowed, AccessDenied> {
        match principal {
            None => Err(AccessDenied::new(DenyReason::Unauthenticated)),
            Some(_)
                if *scope == ResourceScope::Application
                    && self.0.contains(&permission.as_str()) =>
            {
                Ok(Allowed::Grant)
            }
            Some(_) => Err(AccessDenied::new(DenyReason::NoGrant)),
        }
    }
}

/// Reads need `jobs.admin.read`, writes `jobs.admin.write`, payloads
/// `jobs.admin.payload` too, all at application scope (R5).
#[tokio::test]
async fn admin_routes_check_their_permissions_at_application_scope() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let jobs = PgJobs::with_mode(
        &jobs_config(&test, "[jobs]\nworkers = false"),
        RelayMode::Off,
    );
    jobs.ports(&db);
    let id = insert_job(&db, "tasks.unhandled", "tasks:p", r#"{"n":3}"#, false).await;
    let reader = admin(&jobs, Arc::new(Only(&["jobs.admin.read"])));
    assert_eq!(
        get(&reader, "/v1/jobs/admin/queues").await.0,
        StatusCode::OK
    );
    assert_eq!(
        get(&reader, &format!("/v1/jobs/admin/jobs/{id}")).await.0,
        StatusCode::OK
    );
    let (status, denied) = get(&reader, &format!("/v1/jobs/admin/jobs/{id}?payload=true")).await;
    assert_eq!(
        (status, code(&denied)),
        (StatusCode::FORBIDDEN, "unauthorized")
    );
    let (status, denied) = post(&reader, &format!("/v1/jobs/admin/jobs/{id}/cancel"), "").await;
    assert_eq!(
        (status, code(&denied)),
        (StatusCode::FORBIDDEN, "unauthorized")
    );
    assert_eq!(state(&db, id).await, "available");
    let viewer = admin(
        &jobs,
        Arc::new(Only(&["jobs.admin.read", "jobs.admin.payload"])),
    );
    let (status, shown) = get(&viewer, &format!("/v1/jobs/admin/jobs/{id}?payload=true")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(shown["payload"], json!({"n": 3}));
    let writer = admin(&jobs, Arc::new(Only(&["jobs.admin.write"])));
    assert_eq!(
        get(&writer, "/v1/jobs/admin/outbox").await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        post(&writer, &format!("/v1/jobs/admin/jobs/{id}/cancel"), "")
            .await
            .0,
        StatusCode::OK
    );
    let closed = admin(&jobs, Arc::new(DenyAll));
    let (status, anonymous) = call(&closed, "GET", "/v1/jobs/admin/queues", "", false).await;
    assert_eq!(
        (status, code(&anonymous)),
        (StatusCode::UNAUTHORIZED, "unauthenticated")
    );
}

/// `queue_status()` (TE-T5's read): TE's state names per queue, configured
/// and registered queues with their slots, and the outbox backlog.
#[tokio::test]
async fn queue_status_counts_states_per_queue_and_the_outbox() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let config = jobs_config(
        &test,
        "[jobs]\nworkers = false\n[jobs.queues.\"notify.email\"]\nconcurrency = 8\nglobal_concurrency = 3",
    );
    let jobs = PgJobs::with_mode(&config, RelayMode::Off);
    jobs.ports(&db);
    handle(&jobs, "tasks")
        .register(
            "tasks.remind",
            "default",
            Arc::new(Noop),
            Default::default(),
        )
        .unwrap();
    jobs.start(instance_context(&db, "status")).await.unwrap();
    wait_ready(&jobs).await;
    let due = insert_job(&db, "tasks.remind", "tasks:due", "{}", false).await;
    insert_job(&db, "tasks.remind", "tasks:due2", "{}", false).await;
    let later = insert_job(&db, "tasks.remind", "tasks:later", "{}", false).await;
    let leased = insert_job(&db, "tasks.remind", "tasks:leased", "{}", false).await;
    let dead = insert_job(&db, "tasks.remind", "tasks:dead", "{}", false).await;
    for sql in [
        format!("UPDATE merezha_jobs.jobs SET run_at = now() - interval '1 hour' WHERE id = {due}"),
        format!("UPDATE merezha_jobs.jobs SET run_at = now() + interval '1 hour' WHERE id = {later}"),
        format!(
            "UPDATE merezha_jobs.jobs SET state = 'running', attempts = 1, lease_token = gen_random_uuid(),
                 lease_owner = 'x', lease_expires_at = now() + interval '1 hour' WHERE id = {leased}"
        ),
        format!("UPDATE merezha_jobs.jobs SET state = 'dead', finished_at = now() WHERE id = {dead}"),
        "INSERT INTO merezha_jobs.outbox (source, dedupe_key, record_kind, queue, job_kind, payload,
             correlation_id, created_at)
         VALUES ('tasks', 'p1', 'job', 'default', 'tasks.remind', '{}', 'c1', now() - interval '10 minutes')"
            .into(),
    ] {
        execute(&db, &sql).await.unwrap();
    }
    let status = handle(&jobs, "tasks").queue_status().await.unwrap();
    let names: Vec<&str> = status.queues.iter().map(|q| q.name.as_str()).collect();
    assert_eq!(names, ["default", "notify.email"]);
    let default = &status.queues[0];
    assert_eq!(
        (default.ready, default.delayed, default.leased, default.dead),
        (2, 1, 1, 1)
    );
    assert!(default.oldest_ready_age_s >= 3600.0 && default.oldest_ready_age_s < 3700.0);
    assert_eq!((default.concurrency, default.global_concurrency), (2, 0));
    let email = &status.queues[1];
    assert_eq!(
        (
            email.ready,
            email.dead,
            email.concurrency,
            email.global_concurrency
        ),
        (0, 0, 8, 3)
    );
    assert_eq!(status.outbox.pending, 1);
    assert!(status.outbox.oldest_age_s >= 600.0);
    assert!(status.outbox.quarantined.is_empty());
    // The admin route serves the same document.
    let (_, served) = get(&admin(&jobs, allow_all()), "/v1/jobs/admin/queues").await;
    assert_eq!(served["queues"][0]["ready"], 2);
    assert_eq!(served["queues"][1]["name"], "notify.email");
    jobs.shutdown(soon()).await;
}

/// B.9: succeeded and cancelled jobs, dead jobs (with an audit row),
/// relayed and quarantined outbox rows and admin actions past their
/// retention are purged; younger rows stay.
#[tokio::test]
async fn retention_purges_expired_rows_and_keeps_the_rest() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let jobs = PgJobs::with_mode(
        &jobs_config(&test, "[jobs]\nworkers = false"),
        RelayMode::Off,
    );
    jobs.ports(&db);
    let mut ids = Vec::new();
    for (key, state, age) in [
        ("old-ok", "succeeded", "8 days"),
        ("new-ok", "succeeded", "6 days"),
        ("old-cancel", "cancelled", "8 days"),
        ("old-dead", "dead", "31 days"),
        ("new-dead", "dead", "29 days"),
    ] {
        let id = insert_job(&db, "tasks.x", key, "{}", false).await;
        execute(
            &db,
            &format!(
                "UPDATE merezha_jobs.jobs SET state = '{state}', finished_at = now() - interval '{age}'
                  WHERE id = {id}"
            ),
        )
        .await
        .unwrap();
        ids.push(id);
    }
    for sql in [
        "INSERT INTO merezha_jobs.outbox (source, dedupe_key, record_kind, queue, job_kind, payload,
             correlation_id, relayed_at)
         VALUES ('tasks', 'old', 'job', 'default', 'x', '{}', 'c', now() - interval '8 days'),
                ('tasks', 'new', 'job', 'default', 'x', '{}', 'c', now() - interval '6 days')",
        "INSERT INTO merezha_jobs.outbox (source, dedupe_key, record_kind, queue, job_kind, payload,
             correlation_id, quarantined_at, quarantine_code)
         VALUES ('tasks', 'quarantined', 'job', 'default', 'x', '{}', 'c', now() - interval '31 days',
                 'constraint_violation')",
        "INSERT INTO merezha_jobs.admin_actions (at, principal, action, target, count)
         VALUES (now() - interval '366 days', NULL, 'cancel', '1', 1), (now(), NULL, 'cancel', '2', 1)",
    ] {
        execute(&db, sql).await.unwrap();
    }
    let report = jobs.purge_expired().await.unwrap();
    assert_eq!(
        (
            report.finished_jobs,
            report.dead_jobs,
            report.outbox,
            report.audit,
            report.skipped
        ),
        (2, 1, 2, 1, false)
    );
    let left: Vec<i64> = {
        let client = db.acquire(soon()).await.unwrap();
        client
            .query("SELECT id FROM merezha_jobs.jobs ORDER BY id", &[])
            .await
            .unwrap()
            .iter()
            .map(|row| row.get(0))
            .collect()
    };
    assert_eq!(left, vec![ids[1], ids[4]]);
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.outbox WHERE dedupe_key = 'new'"
        )
        .await,
        1
    );
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM merezha_jobs.outbox").await,
        1
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM merezha_jobs.admin_actions
              WHERE action = 'retention_purge' AND target = 'dead' AND count = 1"
        )
        .await,
        1
    );
    // While another instance holds the retention lock, a pass stops.
    let holder = Database::connect(&test.config()).unwrap();
    let mut session = holder.detach(soon()).await.unwrap();
    let racing = jobs.clone();
    session
        .transaction(soon(), move |tx| {
            Box::pin(async move {
                tx.execute(
                    "SELECT pg_advisory_xact_lock(1297762882, hashtext('retention'))",
                    &[],
                )
                .await
                .map_err(|e| merezha_postgres::classify(&e))?;
                let skipped = racing.purge_expired().await?;
                assert!(skipped.skipped);
                Ok(())
            })
        })
        .await
        .unwrap();
}

const DAILY: &str = "[[jobs.schedules]]\nname = \"tasks.daily\"\ncron = \"0 9 * * *\"\nqueue = \"default\"\nkind = \"tasks.remind\"\n";

/// AP-B3: the schedule routes over HTTP. Reads need `jobs.admin.read` and
/// writes `jobs.admin.write`, both at application scope; an anonymous
/// caller on a closed policy gets 401; unknown names and actions are 404;
/// bad query strings are 422 without echoing input.
#[tokio::test]
async fn schedule_routes_check_their_permissions_at_application_scope() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let jobs = PgJobs::with_mode(
        &jobs_config(
            &test,
            &format!("[jobs]\nworkers = false\n[jobs.scheduler]\ninterval_ms = 3600000\n{DAILY}"),
        ),
        RelayMode::Off,
    );
    jobs.ports(&db);
    jobs.start(context(&db)).await.unwrap();
    wait_ready(&jobs).await;
    let reader = admin(&jobs, Arc::new(Only(&["jobs.admin.read"])));
    let writer = admin(&jobs, Arc::new(Only(&["jobs.admin.write"])));
    let (status, listed) = get(&reader, "/v1/jobs/admin/schedules").await;
    assert_eq!(status, StatusCode::OK);
    let entry = &listed["schedules"][0];
    assert_eq!(
        (
            &entry["name"],
            &entry["source"],
            &entry["cron"],
            &entry["timezone"],
            &entry["enabled"]
        ),
        (
            &json!("tasks.daily"),
            &json!("config"),
            &json!("0 9 * * *"),
            &json!("UTC"),
            &json!(true)
        )
    );
    assert!(entry["next_run_at"].is_string() && entry.get("payload").is_none());
    let (status, denied) = post(&reader, "/v1/jobs/admin/schedules/tasks.daily/pause", "").await;
    assert_eq!(
        (status, code(&denied)),
        (StatusCode::FORBIDDEN, "unauthorized")
    );
    assert_eq!(
        get(&writer, "/v1/jobs/admin/schedules").await.0,
        StatusCode::FORBIDDEN
    );
    let (status, paused) = post(&writer, "/v1/jobs/admin/schedules/tasks.daily/pause", "").await;
    assert_eq!(
        (status, &paused["enabled"]),
        (StatusCode::OK, &json!(false))
    );
    let (status, resumed) = post(&writer, "/v1/jobs/admin/schedules/tasks.daily/resume", "").await;
    assert_eq!(
        (status, &resumed["enabled"]),
        (StatusCode::OK, &json!(true))
    );
    let (status, triggered) =
        post(&writer, "/v1/jobs/admin/schedules/tasks.daily/trigger", "").await;
    assert_eq!(status, StatusCode::OK);
    let job = triggered["job_id"].as_i64().unwrap();
    assert_eq!(state(&db, job).await, "available");
    // The permission comes first: a reader learns nothing about names or
    // actions.
    let (status, _) = post(&reader, "/v1/jobs/admin/schedules/tasks.none/delete", "").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // An Idempotency-Key makes the trigger retry-safe; a bad one is 422.
    let keyed = |key: &str| {
        Request::builder()
            .method("POST")
            .uri("/v1/jobs/admin/schedules/tasks.daily/trigger")
            .header("idempotency-key", key)
            .extension(Authenticated(Some(Arc::new(
                Principal::system("ops", GrantSnapshot::empty()).unwrap(),
            ))))
            .body(Body::empty())
            .unwrap()
    };
    let mut answers = Vec::new();
    for key in ["retry-1", "retry-1", "bad key"] {
        let response = writer.clone().oneshot(keyed(key)).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1 << 16).await.unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        answers.push((status, body));
    }
    assert_eq!(answers[0].0, StatusCode::OK);
    assert_eq!(answers[0].1["inserted"], json!(true));
    assert_eq!(answers[1].1["job_id"], answers[0].1["job_id"]);
    assert_eq!(answers[1].1["inserted"], json!(false));
    assert_eq!(
        (answers[2].0, code(&answers[2].1)),
        (StatusCode::UNPROCESSABLE_ENTITY, "validation_failed")
    );
    assert!(!answers[2].1.to_string().contains("bad key"));
    for missing in [
        "/v1/jobs/admin/schedules/tasks.none/pause",
        "/v1/jobs/admin/schedules/Tasks/trigger",
        "/v1/jobs/admin/schedules/tasks.daily/delete",
    ] {
        let (status, body) = post(&writer, missing, "").await;
        assert_eq!(
            (status, code(&body)),
            (StatusCode::NOT_FOUND, "not_found"),
            "{missing}"
        );
    }
    let (status, bad) = get(&reader, "/v1/jobs/admin/schedules?limit=0&after=Bad").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(!bad.to_string().contains("Bad"), "details never echo input");
    let (status, page) = get(
        &reader,
        "/v1/jobs/admin/schedules?limit=1&after=tasks.daily",
    )
    .await;
    assert_eq!((status, &page["schedules"]), (StatusCode::OK, &json!([])));
    let closed = admin(&jobs, Arc::new(DenyAll));
    let (status, anonymous) = call(&closed, "GET", "/v1/jobs/admin/schedules", "", false).await;
    assert_eq!(
        (status, code(&anonymous)),
        (StatusCode::UNAUTHORIZED, "unauthenticated")
    );
    jobs.shutdown(soon()).await;
}

/// Review item 7: every admin route, with a read-only and a write-only
/// principal. Reads need `jobs.admin.read` and nothing else; writes need
/// `jobs.admin.write` and nothing else.
#[tokio::test]
async fn every_admin_route_checks_exactly_its_permission() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let jobs = PgJobs::with_mode(
        &jobs_config(&test, &format!("[jobs]\nworkers = false\n{DAILY}")),
        RelayMode::Off,
    );
    jobs.ports(&db);
    jobs.start(context(&db)).await.unwrap();
    wait_ready(&jobs).await;
    let id = insert_job(&db, "tasks.unhandled", "tasks:perm", "{}", false).await;
    let before = jiff::Timestamp::now().to_string();
    let routes: Vec<(&str, String, String, bool)> = vec![
        ("GET", "/v1/jobs/admin/queues".into(), String::new(), false),
        ("GET", "/v1/jobs/admin/jobs".into(), String::new(), false),
        (
            "GET",
            format!("/v1/jobs/admin/jobs/{id}"),
            String::new(),
            false,
        ),
        ("GET", "/v1/jobs/admin/outbox".into(), String::new(), false),
        (
            "POST",
            format!("/v1/jobs/admin/jobs/{id}/replay"),
            String::new(),
            true,
        ),
        (
            "POST",
            format!("/v1/jobs/admin/jobs/{id}/cancel"),
            String::new(),
            true,
        ),
        (
            "POST",
            "/v1/jobs/admin/dead/replay".into(),
            r#"{"queue":"default"}"#.into(),
            true,
        ),
        (
            "POST",
            "/v1/jobs/admin/dead/purge".into(),
            json!({"queue": "default", "before": before}).to_string(),
            true,
        ),
        (
            "POST",
            "/v1/jobs/admin/outbox/1/requeue".into(),
            String::new(),
            true,
        ),
        (
            "GET",
            "/v1/jobs/admin/schedules".into(),
            String::new(),
            false,
        ),
        (
            "POST",
            "/v1/jobs/admin/schedules/tasks.daily/pause".into(),
            String::new(),
            true,
        ),
        (
            "POST",
            "/v1/jobs/admin/schedules/tasks.daily/resume".into(),
            String::new(),
            true,
        ),
        (
            "POST",
            "/v1/jobs/admin/schedules/tasks.daily/trigger".into(),
            String::new(),
            true,
        ),
    ];
    assert_eq!(routes.len(), 13);
    let reader = admin(&jobs, Arc::new(Only(&["jobs.admin.read"])));
    let writer = admin(&jobs, Arc::new(Only(&["jobs.admin.write"])));
    for (method, uri, body, write) in &routes {
        let (as_reader, _) = call(&reader, method, uri, body, true).await;
        let (as_writer, _) = call(&writer, method, uri, body, true).await;
        let denied = StatusCode::FORBIDDEN;
        if *write {
            assert_eq!(as_reader, denied, "{method} {uri} as reader");
            assert_ne!(as_writer, denied, "{method} {uri} as writer");
        } else {
            assert_ne!(as_reader, denied, "{method} {uri} as reader");
            assert_eq!(as_writer, denied, "{method} {uri} as writer");
        }
    }
}
