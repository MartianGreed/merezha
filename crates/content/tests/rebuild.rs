//! CT-C4 and model activation (section 2): a new locale or a changed
//! fallback enqueues the background `published` rebuild, the host is ready
//! while it runs, a host killed mid-rebuild leaves it for another to
//! finish, and a newer activation carries a pending rebuild forward.
mod http;

use axum::http::StatusCode;
use http::*;
use merezha_platform::Module;
use merezha_postgres::TestDatabase;
use serde_json::json;

/// `CHAIN_MODEL` as version 2: `fr-BE` now falls back to `en`, and `de`
/// (falling back to `en`) is new.
fn v2() -> String {
    CHAIN_MODEL.replace("version = 1", "version = 2").replace(
        "{ code = \"fr-BE\", fallback = \"fr\" },",
        "{ code = \"fr-BE\", fallback = \"en\" },\n  { code = \"de\", fallback = \"en\" },",
    )
}

/// `v2` as version 3 with a tightened rule and no locale change.
fn v3() -> String {
    v2().replace("version = 2", "version = 3").replace(
        "{ name = \"title\", type = \"string\", required = true }]",
        "{ name = \"title\", type = \"string\", required = true, max_length = 100 }]",
    )
}

/// `count` published `page` entries straight through SQL, each with its
/// `en` variant, revision and `en` row only (a partial materialization).
async fn seed_pages(db: &merezha_platform::Database, count: i64) {
    execute(
        db,
        &format!(
            "WITH e AS (
               INSERT INTO merezha_content.entries (id, scope, content_type, created_at, created_by)
               SELECT gen_random_uuid(), 'app', 'page', now(), 'sys_test'
                 FROM generate_series(1, {count}) RETURNING id
             ), v AS (
               INSERT INTO merezha_content.variants
                   (entry_id, locale, scope, content_type, draft_revision, draft_data,
                    draft_updated_at, draft_updated_by, published_revision, published_at,
                    first_published_at)
               SELECT id, 'en', 'app', 'page', 1, '{{\"title\":\"P\"}}', now(), 'sys_test', 1,
                      now(), now() FROM e RETURNING entry_id
             ), r AS (
               INSERT INTO merezha_content.revisions
                   (entry_id, locale, revision, model_version, kind, data, created_at, created_by)
               SELECT entry_id, 'en', 1, 1, 'create', '{{\"title\":\"P\"}}', now(), 'sys_test'
                 FROM v
             )
             INSERT INTO merezha_content.published
                 (entry_id, locale, source_locale, scope, content_type, revision, data,
                  published_at, first_published_at)
             SELECT entry_id, 'en', 'en', 'app', 'page', 1, '{{\"title\":\"P\"}}', now(), now()
               FROM v"
        ),
    )
    .await;
}

async fn page_rows(db: &merezha_platform::Database, locale: &str) -> i64 {
    scalar(
        db,
        &format!(
            "SELECT count(*) FROM merezha_content.published \
              WHERE content_type = 'page' AND locale = '{locale}'"
        ),
    )
    .await
}

const REBUILD_STATES: &str = "SELECT coalesce(string_agg(version || ':' || state, ',' \
     ORDER BY version), '') FROM merezha_content.rebuilds";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_rebuild_runs_in_the_background_while_the_host_is_ready() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let old = start_with(&test, NO_WORKERS, "", CHAIN_MODEL).await;
    let id = id_of(
        &old.create("article", json!({"fields": {"title": "Hello"}}))
            .await,
    );
    old.add_variant("article", &id, "fr", json!({"title": "Bonjour"}))
        .await;
    old.publish("article", &id, "en", 1).await;
    old.publish("article", &id, "fr", 1).await;
    // More than two batches of entries with published variants.
    seed_pages(&old.db, 1100).await;
    let generation_before = generation(&old.db, "app").await;
    // Activation completes and the host is ready; nothing has run the job.
    let new = start_with(&test, NO_WORKERS, "", &v2()).await;
    assert!(eventually_ready(&new.host).await);
    assert_eq!(strings(&new.db, REBUILD_STATES).await, ["2:pending"]);
    assert_eq!(page_rows(&new.db, "de").await, 0);
    assert_eq!(
        rows(&new.db, &id).await,
        ["en:en:1", "fr:fr:1", "fr-BE:fr:1"]
    );
    // Writes and publishes work meanwhile, on the new model.
    let reply = new.publish("article", &id, "en", 1).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(
        rows(&new.db, &id).await,
        ["de:en:1", "en:en:1", "fr:fr:1", "fr-BE:en:1"]
    );
    // A host with workers runs it.
    let worker = start(&test, "", &v2()).await;
    wait_for(&worker.db, REBUILD_STATES, "2:done").await;
    assert_eq!(page_rows(&worker.db, "de").await, 1100);
    assert_eq!(page_rows(&worker.db, "fr-BE").await, 1100);
    // Activation bumps once, the publish once, each of the three batches
    // once more.
    assert_eq!(generation(&worker.db, "app").await, generation_before + 5);
    // Fallback rows emit no events of their own.
    assert_eq!(events(&worker.db, "content.entry_published").await.len(), 3);
    // A restart with the same model has nothing to rebuild; a model change
    // without a locale change records no rebuild either.
    start(&test, "", &v2()).await;
    start(&test, "", &v3()).await;
    assert_eq!(strings(&worker.db, REBUILD_STATES).await, ["2:done"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_host_killed_mid_rebuild_leaves_it_for_another_to_finish() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let old = start_with(&test, NO_WORKERS, "", CHAIN_MODEL).await;
    seed_pages(&old.db, 1100).await;
    // Every committed cursor move, and a gate on the second batch.
    execute(
        &old.db,
        "CREATE TABLE public.rebuild_log (n serial, after_entry uuid, state text);
         CREATE FUNCTION public.log_rebuild() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF OLD.after_entry IS NOT NULL AND NEW.state = 'pending' THEN
             PERFORM pg_advisory_xact_lock(4242);
           END IF;
           INSERT INTO public.rebuild_log (after_entry, state) VALUES (NEW.after_entry, NEW.state);
           RETURN NEW;
         END $$;
         CREATE TRIGGER log_rebuild BEFORE UPDATE ON merezha_content.rebuilds
           FOR EACH ROW EXECUTE FUNCTION public.log_rebuild();",
    )
    .await;
    let gate = old.db.acquire(soon()).await.unwrap();
    gate.execute("SELECT pg_advisory_lock(4242)", &[])
        .await
        .unwrap();
    let gate_pid: i32 = gate
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let jobs = "[jobs.queues.content]\nbackoff_base_ms = 1500\n";
    let first = start_with(&test, jobs, "", &v2()).await;
    // The first batch committed; the second waits at its cursor move.
    let stuck = wait_blocked_by(&first.db, gate_pid).await;
    assert_eq!(
        scalar(&first.db, "SELECT count(*) FROM public.rebuild_log").await,
        1
    );
    // Kill its session and stop its jobs runtime.
    execute(&first.db, &format!("SELECT pg_terminate_backend({stuck})")).await;
    first.jobs.shutdown(soon()).await;
    gate.execute("SELECT pg_advisory_unlock(4242)", &[])
        .await
        .unwrap();
    // Another host resumes after the committed cursor.
    let second = start_with(&test, jobs, "", &v2()).await;
    wait_for(&second.db, REBUILD_STATES, "2:done").await;
    assert_eq!(page_rows(&second.db, "de").await, 1100);
    let cursors = strings(
        &second.db,
        "SELECT coalesce(after_entry::text, '-') FROM public.rebuild_log ORDER BY n",
    )
    .await;
    assert_eq!(cursors.len(), 3, "{cursors:?}");
    assert!(
        cursors.windows(2).all(|w| w[0] < w[1]),
        "the cursor only moves forward: {cursors:?}"
    );
    assert_eq!(
        strings(
            &second.db,
            "SELECT string_agg(a.outcome, ',' ORDER BY a.started_at) FROM merezha_jobs.attempts a \
               JOIN merezha_jobs.jobs j ON j.id = a.job_id \
              WHERE j.kind = 'content.published_rebuild'"
        )
        .await,
        ["failed,succeeded"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_newer_activation_carries_a_pending_rebuild_forward() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let old = start_with(&test, NO_WORKERS, "", CHAIN_MODEL).await;
    seed_pages(&old.db, 30).await;
    start_with(&test, NO_WORKERS, "", &v2()).await;
    assert_eq!(strings(&old.db, REBUILD_STATES).await, ["2:pending"]);
    // Version 3 changes no locale, yet takes the pending rebuild over.
    start_with(&test, NO_WORKERS, "", &v3()).await;
    assert_eq!(
        strings(&old.db, REBUILD_STATES).await,
        ["2:superseded,3:pending"]
    );
    assert_eq!(
        strings(
            &old.db,
            "SELECT string_agg(dedupe_key, ',' ORDER BY id) FROM merezha_jobs.outbox \
              WHERE record_kind = 'job'"
        )
        .await,
        ["rebuild:2,rebuild:3"]
    );
    // A current host runs both jobs: version 2's job (its payload names
    // version 2) finds its rebuild superseded and succeeds without work,
    // version 3's rebuilds every entry with `v2`'s locales.
    let worker = start(&test, "", &v3()).await;
    wait_for(&worker.db, REBUILD_STATES, "2:superseded,3:done").await;
    wait_for(
        &worker.db,
        "SELECT string_agg(state, ',' ORDER BY id) FROM merezha_jobs.jobs \
          WHERE kind = 'content.published_rebuild'",
        "succeeded,succeeded",
    )
    .await;
    assert_eq!(page_rows(&worker.db, "de").await, 30);
    assert_eq!(page_rows(&worker.db, "fr-BE").await, 30);
}

/// A session holding advisory lock `key`, and its backend pid.
struct Gate {
    client: merezha_postgres::PooledClient,
    pid: i32,
    key: i64,
}
async fn hold(db: &merezha_platform::Database, key: i64) -> Gate {
    let client = db.acquire(soon()).await.unwrap();
    client
        .execute("SELECT pg_advisory_lock($1)", &[&key])
        .await
        .unwrap();
    let pid = client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    Gate { client, pid, key }
}
impl Gate {
    async fn open(self) {
        self.client
            .execute("SELECT pg_advisory_unlock($1)", &[&self.key])
            .await
            .unwrap();
    }
}

/// The smallest page id, the first entry of the first batch.
async fn first_page(db: &merezha_platform::Database) -> String {
    strings(
        db,
        "SELECT min(id::text) FROM merezha_content.entries WHERE content_type = 'page'",
    )
    .await
    .remove(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_save_referencing_an_entry_of_the_batch_never_waits_for_the_rebuild() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let old = start_with(&test, NO_WORKERS, "", CHAIN_MODEL).await;
    // Two published entries, the referenced author sorting first.
    let mut ids = Vec::new();
    for _ in 0..2 {
        let author = id_of(
            &old.create("author", json!({"fields": {"name": "Ada"}}))
                .await,
        );
        old.publish("author", &author, "en", 1).await;
        ids.push(author);
    }
    ids.sort();
    // The saved article sorts after its target (UUIDv7, created later).
    let target = ids[0].clone();
    let article = id_of(
        &old.create("article", json!({"fields": {"title": "A"}}))
            .await,
    );
    old.publish("article", &article, "en", 1).await;
    let pending = start_with(&test, NO_WORKERS, "", &v2()).await;
    // A save holds its own entry (`FOR SHARE`) and its target (`FOR KEY
    // SHARE`), stopped as it writes the ref.
    execute(
        &old.db,
        &format!(
            "CREATE FUNCTION public.hold_save() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
               IF NEW.target_id = '{target}' THEN PERFORM pg_advisory_xact_lock(71); END IF;
               RETURN NEW;
             END $$;
             CREATE TRIGGER hold_save BEFORE INSERT ON merezha_content.refs
               FOR EACH ROW EXECUTE FUNCTION public.hold_save();"
        ),
    )
    .await;
    let gate = hold(&old.db, 71).await;
    let client = pending.client.clone();
    let path = variant("article", &article, "en");
    let id = target.clone();
    let save = tokio::spawn(async move {
        client
            .put(&path)
            .if_match(1)
            .json(&json!({"fields": {"title": "A", "related": [], "author": {"id": id}}}))
            .send()
            .await
    });
    wait_blocked_by(&old.db, gate.pid).await;
    // The rebuild batch locks every entry `FOR SHARE` and finishes while
    // the save is still open.
    let worker = start(&test, "", &v2()).await;
    wait_for(&worker.db, REBUILD_STATES, "2:done").await;
    gate.open().await;
    let saved = save.await.unwrap();
    assert_eq!(saved.status, StatusCode::OK, "{:?}", saved.body);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_runs_of_one_rebuild_never_repeat_a_batch() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let old = start_with(&test, NO_WORKERS, "", CHAIN_MODEL).await;
    seed_pages(&old.db, 1100).await;
    let first = first_page(&old.db).await;
    execute(
        &old.db,
        &format!(
            "CREATE TABLE public.materialized (entry_id uuid, locale text);
             CREATE TABLE public.cursors (n serial, after_entry uuid);
             CREATE FUNCTION public.log_row() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
               IF NEW.entry_id = '{first}' THEN PERFORM pg_advisory_xact_lock(72); END IF;
               INSERT INTO public.materialized VALUES (NEW.entry_id, NEW.locale);
               RETURN NEW;
             END $$;
             CREATE TRIGGER log_row BEFORE INSERT ON merezha_content.published
               FOR EACH ROW EXECUTE FUNCTION public.log_row();
             CREATE FUNCTION public.log_cursor() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN INSERT INTO public.cursors (after_entry) VALUES (NEW.after_entry); RETURN NEW; END $$;
             CREATE TRIGGER log_cursor BEFORE UPDATE ON merezha_content.rebuilds
               FOR EACH ROW EXECUTE FUNCTION public.log_cursor();"
        ),
    )
    .await;
    let gate = hold(&old.db, 72).await;
    // The activation's job starts the first batch and stops at the gate.
    let worker = start(&test, "", &v2()).await;
    let running = wait_blocked_by(&worker.db, gate.pid).await;
    // A second run of the same rebuild waits on the cursor row.
    execute(
        &worker.db,
        "INSERT INTO merezha_jobs.jobs (queue, kind, idempotency_key, payload, payload_digest, \
             max_attempts, correlation_id) \
         VALUES ('content', 'content.published_rebuild', 'content:test-second', \
                 '{\"version\":2}'::json, sha256('{\"version\":2}'::bytea), 3, 'c2')",
    )
    .await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let on_cursor = scalar(
            &worker.db,
            &format!(
                "SELECT count(*) FROM pg_stat_activity \
                  WHERE {running} = ANY(pg_blocking_pids(pid)) \
                    AND query LIKE '%merezha_content.rebuilds%'"
            ),
        )
        .await;
        if on_cursor > 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the second run never waited on the cursor row"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    gate.open().await;
    wait_for(&worker.db, REBUILD_STATES, "2:done").await;
    wait_for(
        &worker.db,
        "SELECT string_agg(state, ',' ORDER BY id) FROM merezha_jobs.jobs \
          WHERE kind = 'content.published_rebuild'",
        "succeeded,succeeded",
    )
    .await;
    // Every row materialized once, the cursor only moving forward.
    assert_eq!(
        scalar(
            &worker.db,
            "SELECT count(*) FROM (SELECT 1 FROM public.materialized \
              GROUP BY entry_id, locale HAVING count(*) > 1) d"
        )
        .await,
        0
    );
    assert_eq!(
        scalar(&worker.db, "SELECT count(*) FROM public.materialized").await,
        1100 * 4
    );
    let cursors = strings(
        &worker.db,
        "SELECT after_entry::text FROM public.cursors ORDER BY n",
    )
    .await;
    assert_eq!(cursors.len(), 3, "{cursors:?}");
    assert!(cursors.windows(2).all(|w| w[0] < w[1]), "{cursors:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retried_run_finds_its_continuation_and_succeeds() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let old = start_with(&test, NO_WORKERS, "", CHAIN_MODEL).await;
    seed_pages(&old.db, 1100).await;
    // A job timeout no longer than `limits.query_timeout`: every run does
    // one batch, then appends its continuation.
    let tight = "[jobs.queues.content]\ntimeout_ms = 5000\n";
    let worker = start_with(&test, tight, "", &v2()).await;
    wait_for(&worker.db, REBUILD_STATES, "2:done").await;
    let continuations = strings(
        &worker.db,
        "SELECT string_agg(dedupe_key, ',' ORDER BY id) FROM merezha_jobs.outbox \
          WHERE record_kind = 'job' AND dedupe_key LIKE 'rebuild:2:after:%'",
    )
    .await;
    let keys: Vec<&str> = continuations[0].split(',').collect();
    assert_eq!(keys.len(), 2, "{keys:?}");
    // The first job runs again, as after a reaped lease: its continuation
    // is already there, and the run still succeeds.
    let original: i64 = scalar(
        &worker.db,
        "SELECT id FROM merezha_jobs.jobs WHERE idempotency_key = 'content:rebuild:2'",
    )
    .await;
    assert_eq!(keys[0], format!("rebuild:2:after:{original}"));
    execute(
        &worker.db,
        &format!(
            "UPDATE merezha_content.rebuilds SET state = 'pending', after_entry = NULL;
             UPDATE merezha_jobs.jobs SET state = 'available', run_at = now(), finished_at = NULL \
              WHERE id = {original}"
        ),
    )
    .await;
    wait_for(
        &worker.db,
        &format!(
            "SELECT string_agg(a.outcome, ',' ORDER BY a.started_at) FROM merezha_jobs.attempts a \
              WHERE a.job_id = {original}"
        ),
        "succeeded,succeeded",
    )
    .await;
    assert_eq!(
        scalar(
            &worker.db,
            &format!(
                "SELECT count(*) FROM merezha_jobs.outbox WHERE dedupe_key = 'rebuild:2:after:{original}'"
            )
        )
        .await,
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pending_rebuild_without_a_live_job_is_enqueued_again_at_start() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let old = start_with(&test, NO_WORKERS, "", CHAIN_MODEL).await;
    seed_pages(&old.db, 30).await;
    let pending = start_with(&test, NO_WORKERS, "", &v2()).await;
    // Stop both runtimes, then lose the job: its record relayed, any job
    // dead.
    old.jobs.shutdown(soon()).await;
    pending.jobs.shutdown(soon()).await;
    execute(
        &old.db,
        "UPDATE merezha_jobs.outbox SET relayed_at = now() \
          WHERE record_kind = 'job' AND relayed_at IS NULL;
         UPDATE merezha_jobs.jobs SET state = 'dead', finished_at = now() \
          WHERE kind = 'content.published_rebuild'",
    )
    .await;
    assert_eq!(strings(&old.db, REBUILD_STATES).await, ["2:pending"]);
    // The next host of the model enqueues it again, keyed by the cursor.
    let worker = start(&test, "", &v2()).await;
    wait_for(&worker.db, REBUILD_STATES, "2:done").await;
    assert_eq!(page_rows(&worker.db, "de").await, 30);
    assert_eq!(
        strings(
            &worker.db,
            "SELECT state FROM merezha_jobs.jobs WHERE idempotency_key = 'content:rebuild:2:resume:start'"
        )
        .await,
        ["succeeded"]
    );
    // A start with nothing pending enqueues nothing.
    start(&test, "", &v2()).await;
    assert_eq!(
        scalar(
            &worker.db,
            "SELECT count(*) FROM merezha_jobs.jobs WHERE idempotency_key LIKE 'content:rebuild:2:resume:%'"
        )
        .await,
        1
    );
}
