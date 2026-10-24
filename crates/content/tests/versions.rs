//! CT-C3 and mixed model versions (section 2): a superseded host refuses
//! every write and drains, a pending one refuses without draining, saves
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
    // Its content job kinds stop leasing.
    assert!(!guarded.accepts_leases());
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
