//! CT-C3 and C4 with mixed model versions (section 2): a superseded host
//! refuses every write (saves, publishing, deletion, batches) and drains, a pending one refuses without draining, saves
//! racing another host's activation commit before it or are refused, and
//! restore reports what the current model no longer accepts.
mod http;

use axum::http::StatusCode;
use http::*;
use merezha_platform::{
    BoxFuture,
    ports::{JobContext, JobFailure, JobHandler},
};
use merezha_postgres::TestDatabase;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

/// `MODEL` as version 2: the title tightened to 100 characters and a new
/// sortable field, whose derived indexes activation builds first.
fn v2() -> String {
    MODEL
        .replace("version = 1", "version = 2")
        .replace(
            "required = true, max_length = 200, sortable = true",
            "required = true, max_length = 100, sortable = true",
        )
        .replace(
            "  { name = \"sections\"",
            "  { name = \"rank\", type = \"integer\", sortable = true },\n  { name = \"sections\"",
        )
}

fn id_of(reply: &Reply) -> String {
    reply.body["sys"]["id"].as_str().unwrap().to_owned()
}

struct Noop;
impl JobHandler for Noop {
    fn handle<'a>(&'a self, _: JobContext, _: Value) -> BoxFuture<'a, Result<(), JobFailure>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn a_superseded_host_refuses_every_write_and_reports_outdated() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let old = start(&test, "", MODEL).await;
    let id = id_of(
        &old.create("article", json!({"fields": {"title": "Old"}}))
            .await,
    );
    let state = old.handle.state().unwrap();
    let guarded = state.guard_jobs(Arc::new(Noop));
    assert!(guarded.accepts_leases());
    let new = start(&test, "", &v2()).await;
    let before = snapshot(&old.db).await;
    let path = variant("article", &id, "en");
    let body = json!({"fields": {"title": "Late"}});
    let refused = [
        old.post("/v1/content/manage/entries/article")
            .json(&body)
            .send()
            .await,
        old.put(&path).if_match(1).json(&body).send().await,
        old.put(&variant("article", &id, "fr"))
            .header("if-none-match", "*")
            .json(&body)
            .send()
            .await,
        old.patch(&path).if_match(1).json(&body).send().await,
        old.post(&format!("{path}/revisions/1/restore"))
            .if_match(1)
            .send()
            .await,
        old.put("/v1/content/manage/singletons/site_settings/en")
            .header("if-none-match", "*")
            .json(&json!({"fields": {"site_name": "S"}}))
            .send()
            .await,
        // C4: publishing, deletion and batches.
        old.publish("article", &id, "en", 1).await,
        old.unpublish("article", &id, "en", 1).await,
        old.call(axum::http::Method::DELETE, &variant("article", &id, "fr"))
            .if_match(1)
            .send()
            .await,
        old.call(
            axum::http::Method::DELETE,
            &format!("/v1/content/manage/entries/article/{id}"),
        )
        .by(Some("admin"))
        .send()
        .await,
        old.call(
            axum::http::Method::DELETE,
            &format!("/v1/content/manage/entries/article/{id}?purge=true"),
        )
        .by(Some("admin"))
        .send()
        .await,
        old.post(&format!("/v1/content/manage/entries/article/{id}/undelete"))
            .by(Some("admin"))
            .send()
            .await,
        old.post("/v1/content/manage/batch/publish")
            .by(Some("admin"))
            .json(&json!({"items": [{"type": "article", "id": id, "locale": "en", "revision": 1}]}))
            .send()
            .await,
        old.post("/v1/content/manage/batch/unpublish")
            .by(Some("admin"))
            .json(&json!({"items": [{"type": "article", "id": id, "locale": "en"}]}))
            .send()
            .await,
    ];
    for reply in refused {
        assert_eq!(
            (reply.status, reply.code()),
            (StatusCode::SERVICE_UNAVAILABLE, "content_model_superseded"),
            "{:?}",
            reply.body
        );
        assert_eq!(reply.body["error"]["classification"], "transient");
        assert_eq!(reply.headers["retry-after"], "1");
    }
    assert_eq!(snapshot(&old.db).await, before);
    assert!(state.is_outdated());
    assert_eq!(
        state.readiness().await.unwrap_err().code,
        "content_model_outdated"
    );
    assert!(!old.host.readiness().check().await);
    // Its content job kinds stop leasing, and a job it still holds is
    // given back, attempt refunded, without running.
    assert!(!guarded.accepts_leases());
    let (_cancel, cancelled) = tokio::sync::watch::channel(false);
    let job = JobContext {
        id: 1,
        kind: merezha_domain::Identity::new("content.test_job").unwrap(),
        attempt: 1,
        max_attempts: 3,
        idempotency_key: "content:t".into(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(5),
        cancel: cancelled,
        correlation_id: merezha_domain::Identity::new("c1").unwrap(),
        causation_id: None,
        tenant: None,
    };
    assert!(
        guarded
            .handle(job, json!({}))
            .await
            .unwrap_err()
            .is_release()
    );
    // Reads keep the loaded model; the current host writes.
    assert_eq!(old.get(&path).send().await.status, StatusCode::OK);
    let saved = new.put(&path).if_match(1).json(&body).send().await;
    assert_eq!(saved.status, StatusCode::OK, "{:?}", saved.body);
    assert_eq!(saved.body["sys"]["model_version"], 2);
    // A host whose model is not active yet refuses without draining.
    let dir = tempfile::tempdir().unwrap();
    project(
        dir.path(),
        false,
        "",
        &v2().replace("version = 2", "version = 3"),
    );
    let pending = start_in(dir, &test, Arc::new(FixturePolicy)).await;
    let reply = pending.put(&path).if_match(2).json(&body).send().await;
    assert_eq!(
        (reply.status, reply.code()),
        (StatusCode::SERVICE_UNAVAILABLE, "content_model_pending")
    );
    assert_eq!(reply.headers["retry-after"], "1");
    assert!(!pending.handle.state().unwrap().is_outdated());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn saves_around_another_hosts_activation_commit_before_it_or_are_refused() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let old = start(&test, "", MODEL).await;
    // At each save's commit, record which model is active. The save holds
    // its share lock on the v1 row until then, so v2 can never be active.
    execute(
        &old.db,
        "CREATE TABLE public.commits (revision_model int, active_at_commit int);
         CREATE FUNCTION public.log_commit() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO public.commits SELECT NEW.model_version,
             (SELECT version FROM merezha_content.models WHERE state = 'active');
           RETURN NULL;
         END $$;
         CREATE CONSTRAINT TRIGGER log_commit AFTER INSERT ON merezha_content.revisions
           DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.log_commit();",
    )
    .await;
    let stop = Arc::new(AtomicBool::new(false));
    let mut savers = Vec::new();
    for saver in 0..4 {
        let client = old.client.clone();
        let stop = stop.clone();
        savers.push(tokio::spawn(async move {
            let mut results: Vec<(Instant, StatusCode, String)> = Vec::new();
            let mut n = 0;
            while !stop.load(Ordering::SeqCst) {
                n += 1;
                let started = Instant::now();
                let reply = client
                    .post("/v1/content/manage/entries/article")
                    .json(&json!({"fields": {"title": format!("s{saver} n{n}")}}))
                    .send()
                    .await;
                results.push((started, reply.status, reply.code().to_owned()));
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            results
        }));
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    let new = start(&test, "", &v2()).await;
    let activated = Instant::now();
    tokio::time::sleep(Duration::from_millis(400)).await;
    stop.store(true, Ordering::SeqCst);
    let mut committed = 0;
    let mut refused = 0;
    let mut after = 0;
    for saver in savers {
        for (started, status, code) in saver.await.unwrap() {
            match status {
                StatusCode::CREATED => {
                    committed += 1;
                    assert!(
                        started < activated,
                        "a save started after activation committed"
                    );
                }
                StatusCode::SERVICE_UNAVAILABLE => {
                    assert_eq!(code, "content_model_superseded");
                    refused += 1;
                }
                other => panic!("unexpected {other} {code}"),
            }
            if started > activated {
                after += 1;
            }
        }
    }
    assert!(
        committed > 0 && refused > 0 && after > 0,
        "{committed} {refused} {after}"
    );
    let client = old.db.acquire(soon()).await.unwrap();
    let rows: Vec<(i32, i32)> = client
        .query(
            "SELECT revision_model, active_at_commit FROM public.commits",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(rows.len(), committed);
    assert!(rows.iter().all(|row| *row == (1, 1)), "{rows:?}");
    // The new host writes on v2.
    let saved = new
        .post("/v1/content/manage/entries/article")
        .json(&json!({"fields": {"title": "fresh", "rank": 1}}))
        .send()
        .await;
    assert_eq!(saved.status, StatusCode::CREATED);
}

#[tokio::test]
async fn restore_lists_what_the_current_model_no_longer_accepts() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let old = start(&test, "", MODEL).await;
    let long = "x".repeat(150);
    let id = id_of(
        &old.create(
            "article",
            json!({"fields": {"title": long, "slug": "long"}}),
        )
        .await,
    );
    let path = variant("article", &id, "en");
    let short = old
        .put(&path)
        .if_match(1)
        .json(&json!({"fields": {"title": "short"}}))
        .send()
        .await;
    assert_eq!(short.status, StatusCode::OK);
    let coded = id_of(
        &old.create(
            "article",
            json!({"fields": {"title": "A", "legacy_code": "A"}}),
        )
        .await,
    );
    let coded_path = variant("article", &coded, "en");
    assert_eq!(
        old.put(&coded_path)
            .if_match(1)
            .json(&json!({"fields": {"title": "A", "legacy_code": "B"}}))
            .send()
            .await
            .status,
        StatusCode::OK
    );
    let taker = id_of(
        &old.create(
            "article",
            json!({"fields": {"title": "C", "legacy_code": "A"}}),
        )
        .await,
    );
    let new = start(&test, "", &v2()).await;
    let restored = new
        .post(&format!("{path}/revisions/1/restore"))
        .if_match(2)
        .send()
        .await;
    assert_eq!(restored.status, StatusCode::OK, "{:?}", restored.body);
    assert_eq!(
        restored.body["warnings"],
        json!([{"path": "/fields/title", "code": "max_length"}])
    );
    assert_eq!(restored.body["fields"]["title"], long.as_str());
    assert_eq!(restored.body["sys"]["model_version"], 2);
    let item = new.get(&format!("{path}/revisions/3")).send().await;
    assert_eq!(item.body["sys"]["kind"], "restore");
    assert_eq!(item.body["sys"]["restored_from"], 1);
    let first = new.get(&format!("{path}/revisions/1")).send().await;
    assert_eq!(first.body["sys"]["model_version"], 1);
    // A unique conflict fails the restore.
    let before = snapshot(&new.db).await;
    let conflict = new
        .post(&format!("{coded_path}/revisions/1/restore"))
        .if_match(2)
        .send()
        .await;
    assert_eq!(conflict.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        conflict.details(),
        vec![("/fields/legacy_code".to_owned(), "unique".to_owned())]
    );
    assert_eq!(snapshot(&new.db).await, before);
    assert!(!taker.is_empty());
}

/// A job instance (`instance`) whose `content` module registered
/// `content.test_job` through `handler`.
async fn job_instance(
    db: &merezha_platform::Database,
    url: &str,
    instance: &str,
    handler: Arc<dyn JobHandler>,
) -> merezha_jobs::PgJobs {
    use merezha_platform::{JobsRuntime, Module};
    let config = merezha_platform::ProjectConfig::parse(
        "[app]\nid = \"site\"\n[database]\nurl = { env = \"DB\" }\n[runtime]\nmanifest = \"r.toml\"\n\
         [jobs]\npoll_interval_ms = 100\n",
        std::env::temp_dir(),
        &[("DB".into(), url.into())],
        &[],
    )
    .unwrap();
    let jobs = merezha_jobs::PgJobs::with_mode(&config.jobs, merezha_jobs::RelayMode::Interval);
    jobs.ports(db);
    merezha_platform::ports::JobsHandle::new(
        merezha_platform::ports::Source::new("content").unwrap(),
        Arc::new(jobs.clone()),
    )
    .register("content.test_job", "default", handler, Default::default())
    .unwrap();
    let policy = Arc::new(FixturePolicy);
    let ctx = merezha_platform::AppContext::builder("site", db.clone(), policy)
        .instance_id(instance)
        .build()
        .unwrap();
    jobs.start(ctx).await.unwrap();
    jobs
}

/// `(attempt, outcome, lease_owner)` of every lease of job `id`.
async fn job_attempts(
    db: &merezha_platform::Database,
    id: i64,
) -> Vec<(i32, Option<String>, String)> {
    let client = db.acquire(soon()).await.unwrap();
    client
        .query(
            "SELECT attempt, outcome, lease_owner FROM merezha_jobs.attempts \
              WHERE job_id = $1 ORDER BY started_at",
            &[&id],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect()
}

/// Runs the content write check when told to, as a content job would.
struct Writes {
    state: Arc<merezha_content::ContentState>,
    db: merezha_platform::Database,
    started: Arc<tokio::sync::Notify>,
    proceed: Arc<tokio::sync::Notify>,
}
impl JobHandler for Writes {
    fn handle<'a>(&'a self, _: JobContext, _: Value) -> BoxFuture<'a, Result<(), JobFailure>> {
        Box::pin(async move {
            self.started.notify_one();
            self.proceed.notified().await;
            let state = self.state.clone();
            let db = self.db.clone();
            self.db
                .transaction(soon(), move |tx| {
                    Box::pin(async move { state.check_writable(&db, tx).await })
                })
                .await
                .map_err(JobFailure::from)
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_job_leased_before_the_host_turns_outdated_is_released_and_runs_on_a_current_host() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let old = start(&test, "", MODEL).await;
    let old_state = old.handle.state().unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let proceed = Arc::new(tokio::sync::Notify::new());
    let old_jobs = job_instance(
        &old.db,
        test.url(),
        "old",
        old_state.guard_jobs(Arc::new(Writes {
            state: old_state.clone(),
            db: old.db.clone(),
            started: started.clone(),
            proceed: proceed.clone(),
        })),
    )
    .await;
    let id: i64 = old
        .db
        .acquire(soon())
        .await
        .unwrap()
        .query_one(
            "INSERT INTO merezha_jobs.jobs (queue, kind, idempotency_key, payload, payload_digest, \
                 max_attempts, correlation_id) \
             VALUES ('default', 'content.test_job', 'content:t1', '{}'::json, \
                     sha256('{}'::bytea), 3, 'c1') RETURNING id",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    // Leased and running on the v1 host when v2 activates.
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .expect("the old instance leased the job");
    let new = start(&test, "", &v2()).await;
    proceed.notify_one();
    let deadline = Instant::now() + Duration::from_secs(5);
    while job_attempts(&old.db, id)
        .await
        .first()
        .and_then(|a| a.1.clone())
        .is_none()
    {
        assert!(Instant::now() < deadline, "the job was never settled");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(old_state.is_outdated());
    assert_eq!(
        job_attempts(&old.db, id).await,
        vec![(1, Some("released".into()), "old".into())]
    );
    assert_eq!(
        scalar(
            &old.db,
            &format!("SELECT attempts::int8 FROM merezha_jobs.jobs WHERE id = {id}")
        )
        .await,
        0
    );
    // The outdated instance leases it no more; a current one runs it.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(job_attempts(&old.db, id).await.len(), 1);
    let new_state = new.handle.state().unwrap();
    let ready = Arc::new(tokio::sync::Notify::new());
    let go = Arc::new(tokio::sync::Notify::new());
    go.notify_one();
    let new_jobs = job_instance(
        &new.db,
        test.url(),
        "new",
        new_state.guard_jobs(Arc::new(Writes {
            state: new_state.clone(),
            db: new.db.clone(),
            started: ready,
            proceed: go,
        })),
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(5);
    while job_attempts(&old.db, id).await.len() < 2
        || job_attempts(&old.db, id).await[1].1.is_none()
    {
        assert!(Instant::now() < deadline, "the current host never ran it");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        job_attempts(&old.db, id).await,
        vec![
            (1, Some("released".into()), "old".into()),
            (1, Some("succeeded".into()), "new".into())
        ]
    );
    use merezha_platform::Module;
    old_jobs.shutdown(soon()).await;
    new_jobs.shutdown(soon()).await;
}
