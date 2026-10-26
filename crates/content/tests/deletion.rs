//! CT-C4 deletion over the axum router and a real database: soft delete,
//! `if_unreferenced`, undelete, immediate purge, the trash listing and
//! the `content.trash_purge` job.
mod http;

use axum::http::{Method, StatusCode};
use http::*;
use merezha_postgres::TestDatabase;
use serde_json::{Value, json};
use std::time::{Duration, Instant};

fn entry(content_type: &str, id: &str) -> String {
    format!("/v1/content/manage/entries/{content_type}/{id}")
}

async fn delete(fx: &Fixture, uri: &str, principal: &str) -> Reply {
    fx.call(Method::DELETE, uri)
        .by(Some(principal))
        .send()
        .await
}

async fn undelete(fx: &Fixture, content_type: &str, id: &str) -> Reply {
    fx.post(&format!("{}/undelete", entry(content_type, id)))
        .by(Some("admin"))
        .send()
        .await
}

#[tokio::test]
async fn delete_removes_every_delivery_row_and_emits_one_unpublished_per_live_locale_and_deleted() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    let ada = id_of(
        &fx.create("author", json!({"fields": {"name": "Ada"}}))
            .await,
    );
    fx.publish("author", &ada, "en", 1).await;
    let id = id_of(
        &fx.create(
            "article",
            json!({"fields": {"title": "Hello", "code": "c1", "author": {"id": ada}}}),
        )
        .await,
    );
    fx.add_variant("article", &id, "fr", json!({"title": "Bonjour"}))
        .await;
    fx.add_variant("article", &id, "fr-BE", json!({"title": "Salut"}))
        .await;
    fx.publish("article", &id, "en", 1).await;
    fx.publish("article", &id, "fr", 1).await;
    assert_eq!(
        rows(&fx.db, &id).await,
        ["en:en:1", "fr:fr:1", "fr-BE:fr:1"]
    );
    let generation_before = generation(&fx.db, "app").await;
    // The delete permission is its own.
    let refused = delete(&fx, &entry("article", &id), "editor").await;
    assert_eq!(
        (refused.status, refused.code()),
        (StatusCode::FORBIDDEN, "unauthorized")
    );
    let reply = delete(&fx, &entry("article", &id), "deleter").await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT, "{:?}", reply.body);
    assert!(rows(&fx.db, &id).await.is_empty());
    assert_eq!(
        scalar(
            &fx.db,
            &format!("SELECT count(*) FROM merezha_content.refs WHERE from_entry = '{id}'")
        )
        .await,
        0
    );
    assert_eq!(
        strings(
            &fx.db,
            &format!(
                "SELECT locale || ':' || deleted::text || ':' || \
                        coalesce(published_revision::text, '-') \
                   FROM merezha_content.variants WHERE entry_id = '{id}' ORDER BY locale"
            )
        )
        .await,
        ["en:true:-", "fr:true:-", "fr-BE:true:-"]
    );
    assert_eq!(generation(&fx.db, "app").await, generation_before + 1);
    let unpublished = events(&fx.db, "content.entry_unpublished").await;
    let locales: Vec<&Value> = unpublished
        .iter()
        .map(|e| &e["payload"]["locale"])
        .collect();
    assert_eq!(locales, [&json!("en"), &json!("fr")]);
    assert!(
        unpublished
            .iter()
            .all(|e| e["payload"]["reason"] == "delete")
    );
    let deleted = events(&fx.db, "content.entry_deleted").await;
    assert_eq!(deleted.len(), 1);
    assert_eq!(
        deleted[0]["payload"],
        json!({"scope": "app", "type": "article", "id": id,
               "actor": {"subject": "usr_deleter", "kind": "user"}})
    );
    // Gone from every variant route and list, still in the summary.
    assert_eq!(
        fx.get(&variant("article", &id, "en")).send().await.code(),
        "entry_not_found"
    );
    let list = fx.get("/v1/content/manage/entries/article").send().await;
    assert_eq!(list.body["items"], json!([]));
    let summary = fx.get(&entry("article", &id)).send().await;
    assert!(summary.body["sys"]["deleted_at"].is_string());
    // Deleting again is `entry_not_found`; its unique values are free.
    assert_eq!(
        delete(&fx, &entry("article", &id), "deleter").await.code(),
        "entry_not_found"
    );
    let taken = fx
        .create(
            "article",
            json!({"fields": {"title": "Hello", "code": "c1"}}),
        )
        .await;
    assert_eq!(taken.body["fields"]["slug"], "hello");
}

#[tokio::test]
async fn undelete_restores_drafts_only() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    let ada = id_of(
        &fx.create("author", json!({"fields": {"name": "Ada"}}))
            .await,
    );
    fx.publish("author", &ada, "en", 1).await;
    let id = id_of(
        &fx.create(
            "article",
            json!({"fields": {"title": "Hello", "code": "c1", "author": {"id": ada}}}),
        )
        .await,
    );
    fx.add_variant("article", &id, "fr", json!({"title": "Bonjour"}))
        .await;
    fx.publish("article", &id, "en", 1).await;
    fx.publish("article", &id, "fr", 1).await;
    delete(&fx, &entry("article", &id), "admin").await;
    let reply = undelete(&fx, "article", &id).await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    assert_eq!(reply.body["sys"]["deleted_at"], Value::Null);
    let statuses: Vec<(&str, &str, &Value)> = reply.body["variants"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            (
                v["locale"].as_str().unwrap(),
                v["status"].as_str().unwrap(),
                &v["published_revision"],
            )
        })
        .collect();
    assert_eq!(
        statuses,
        [("en", "draft", &Value::Null), ("fr", "draft", &Value::Null)]
    );
    // Nothing is delivered until it is published again.
    assert!(rows(&fx.db, &id).await.is_empty());
    let read = fx.get(&variant("article", &id, "en")).send().await;
    assert_eq!(read.status, StatusCode::OK);
    assert_eq!(read.body["fields"]["title"], "Hello");
    // The drafts' refs are back: the author lists its referrer.
    let referrers = fx
        .get(&format!("{}/referrers", entry("author", &ada)))
        .send()
        .await;
    assert_eq!(referrers.body["items"][0]["id"], id);
    let undeleted = events(&fx.db, "content.entry_undeleted").await;
    assert_eq!(undeleted.len(), 1);
    assert_eq!(
        undeleted[0]["payload"],
        json!({"scope": "app", "type": "article", "id": id,
               "actor": {"subject": "usr_admin", "kind": "user"}})
    );
    // A live entry is left as it is.
    let again = undelete(&fx, "article", &id).await;
    assert_eq!(again.status, StatusCode::OK);
    assert_eq!(events(&fx.db, "content.entry_undeleted").await.len(), 1);
    // A unique value taken meanwhile fails the undelete with its field.
    delete(&fx, &entry("article", &id), "admin").await;
    fx.create(
        "article",
        json!({"fields": {"title": "Other", "code": "c1"}}),
    )
    .await;
    let before = snapshot(&fx.db).await;
    let refused = undelete(&fx, "article", &id).await;
    assert_eq!(
        (refused.status, refused.code(), refused.details()),
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_failed",
            vec![("/fields/code".to_owned(), "unique".to_owned())]
        )
    );
    assert_eq!(snapshot(&fx.db).await, before);
    // So does a singleton slot taken meanwhile.
    let settings = fx
        .put("/v1/content/manage/singletons/site_settings/en")
        .header("if-none-match", "*")
        .json(&json!({"fields": {"site_name": "One"}}))
        .send()
        .await;
    let first = id_of(&settings);
    delete(&fx, &entry("site_settings", &first), "admin").await;
    let second = fx
        .put("/v1/content/manage/singletons/site_settings/en")
        .header("if-none-match", "*")
        .json(&json!({"fields": {"site_name": "Two"}}))
        .send()
        .await;
    assert_eq!(second.status, StatusCode::CREATED);
    let refused = undelete(&fx, "site_settings", &first).await;
    assert_eq!(
        (refused.status, refused.code()),
        (StatusCode::CONFLICT, "singleton_exists")
    );
}

#[tokio::test]
async fn if_unreferenced_refuses_while_anything_points_at_the_entry() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    let ada = id_of(
        &fx.create("author", json!({"fields": {"name": "Ada"}}))
            .await,
    );
    let article = id_of(
        &fx.create(
            "article",
            json!({"fields": {"title": "Hello", "author": {"id": ada}}}),
        )
        .await,
    );
    let guarded = format!("{}?if_unreferenced=true", entry("author", &ada));
    let before = snapshot(&fx.db).await;
    let refused = delete(&fx, &guarded, "admin").await;
    assert_eq!(
        (refused.status, refused.code()),
        (StatusCode::CONFLICT, "entry_referenced")
    );
    assert_eq!(snapshot(&fx.db).await, before);
    // Trashing the referrer removes its refs; then the author can go.
    delete(&fx, &entry("article", &article), "admin").await;
    // A referrer in another scope counts too, without being listed.
    let foreign = seed_row(&fx.db, "org:acme", "article", r#"{"title":"Theirs"}"#).await;
    execute(
        &fx.db,
        &format!(
            "INSERT INTO merezha_content.refs \
                 (scope, from_entry, from_locale, stage, path, target_kind, target_id) \
             VALUES ('org:acme', '{foreign}', 'en', 'draft', '/fields/author', 'entry', '{ada}')"
        ),
    )
    .await;
    assert_eq!(
        delete(&fx, &guarded, "admin").await.code(),
        "entry_referenced"
    );
    execute(&fx.db, "DELETE FROM merezha_content.refs").await;
    let reply = delete(&fx, &guarded, "admin").await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT, "{:?}", reply.body);
    // Without the flag, references do not block.
    let other = id_of(
        &fx.create("author", json!({"fields": {"name": "Grace"}}))
            .await,
    );
    fx.create(
        "article",
        json!({"fields": {"title": "Two", "author": {"id": other}}}),
    )
    .await;
    let reply = delete(&fx, &entry("author", &other), "admin").await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT);
    // Bad flags are refused before anything is read.
    for query in ["if_unreferenced=yes", "purge=1", "force=true"] {
        let reply = delete(
            &fx,
            &format!("{}?{query}", entry("author", &other)),
            "admin",
        )
        .await;
        assert_eq!(reply.code(), "invalid_query", "{query}");
    }
}

#[tokio::test]
async fn purge_true_hard_deletes_at_once_and_needs_the_purge_permission() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    let id = id_of(
        &fx.create("article", json!({"fields": {"title": "Hello"}}))
            .await,
    );
    fx.add_variant("article", &id, "fr", json!({"title": "Bonjour"}))
        .await;
    fx.publish("article", &id, "fr", 1).await;
    let purge = format!("{}?purge=true", entry("article", &id));
    let before = snapshot(&fx.db).await;
    let refused = delete(&fx, &purge, "deleter").await;
    assert_eq!(
        (refused.status, refused.code()),
        (StatusCode::FORBIDDEN, "unauthorized")
    );
    assert_eq!(snapshot(&fx.db).await, before);
    let reply = delete(&fx, &purge, "purger").await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT, "{:?}", reply.body);
    for table in ["entries", "variants", "revisions", "published", "refs"] {
        assert_eq!(
            scalar(
                &fx.db,
                &format!("SELECT count(*) FROM merezha_content.{table}")
            )
            .await,
            0,
            "{table}"
        );
    }
    assert_eq!(events(&fx.db, "content.entry_unpublished").await.len(), 1);
    assert_eq!(events(&fx.db, "content.entry_deleted").await.len(), 1);
    let purged = events(&fx.db, "content.entry_purged").await;
    assert_eq!(
        purged[0]["payload"],
        json!({"scope": "app", "type": "article", "id": id})
    );
    assert_eq!(
        fx.get(&entry("article", &id)).send().await.code(),
        "entry_not_found"
    );
    // An entry already in the trash purges with `content.entry_purged` only.
    let trashed = id_of(
        &fx.create("article", json!({"fields": {"title": "Old"}}))
            .await,
    );
    delete(&fx, &entry("article", &trashed), "deleter").await;
    let reply = delete(
        &fx,
        &format!("{}?purge=true", entry("article", &trashed)),
        "purger",
    )
    .await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT);
    assert_eq!(events(&fx.db, "content.entry_deleted").await.len(), 2);
    assert_eq!(events(&fx.db, "content.entry_purged").await.len(), 2);
}

#[tokio::test]
async fn the_trash_lists_trashed_entries_newest_first_by_readable_type() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    let mut articles = Vec::new();
    for n in 0..5 {
        let id = id_of(
            &fx.create("article", json!({"fields": {"title": format!("A{n}")}}))
                .await,
        );
        delete(&fx, &entry("article", &id), "deleter").await;
        articles.push(id);
    }
    let ada = id_of(
        &fx.create("author", json!({"fields": {"name": "Ada"}}))
            .await,
    );
    delete(&fx, &entry("author", &ada), "deleter").await;
    fx.create("article", json!({"fields": {"title": "Live"}}))
        .await;
    // Pages of two, newest deletion first, every type the reader reads.
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let uri = match &cursor {
            None => "/v1/content/manage/trash?limit=2".to_owned(),
            Some(c) => format!("/v1/content/manage/trash?limit=2&cursor={c}"),
        };
        let page = fx.get(&uri).by(Some("reader")).send().await;
        assert_eq!(page.status, StatusCode::OK, "{:?}", page.body);
        for item in page.body["items"].as_array().unwrap() {
            seen.push((
                item["id"].as_str().unwrap().to_owned(),
                item["title"].as_str().unwrap().to_owned(),
                item["deleted_by"].as_str().unwrap().to_owned(),
            ));
        }
        match page.body["page"]["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_owned()),
            None => break,
        }
    }
    let mut expected: Vec<(String, String, String)> = articles
        .iter()
        .enumerate()
        .map(|(n, id)| (id.clone(), format!("A{n}"), "usr_deleter".to_owned()))
        .collect();
    expected.push((ada.clone(), "Ada".to_owned(), "usr_deleter".to_owned()));
    expected.reverse();
    assert_eq!(seen, expected);
    // `type` narrows it; a per-type grant sees its type only.
    let authors = fx
        .get("/v1/content/manage/trash?type=author")
        .by(Some("reader"))
        .send()
        .await;
    assert_eq!(authors.body["items"].as_array().unwrap().len(), 1);
    let scoped = fx
        .get("/v1/content/manage/trash")
        .by(Some("article_writer"))
        .send()
        .await;
    assert_eq!(scoped.body["items"].as_array().unwrap().len(), 5);
    let refused = fx
        .get("/v1/content/manage/trash?type=author")
        .by(Some("article_writer"))
        .send()
        .await;
    assert_eq!(refused.code(), "unauthorized");
    for (principal, code) in [
        (None, "unauthenticated"),
        (Some("publisher"), "unauthorized"),
    ] {
        let reply = fx
            .get("/v1/content/manage/trash")
            .by(principal)
            .send()
            .await;
        assert_eq!(reply.code(), code, "{principal:?}");
    }
    // A cursor of another listing, a forged one and unknown keys.
    let first = fx
        .get("/v1/content/manage/trash?limit=1")
        .by(Some("reader"))
        .send()
        .await;
    let next = first.body["page"]["next_cursor"].as_str().unwrap();
    for uri in [
        format!("/v1/content/manage/trash?type=author&cursor={next}"),
        "/v1/content/manage/trash?cursor=abc".to_owned(),
    ] {
        let reply = fx.get(&uri).by(Some("reader")).send().await;
        assert_eq!(reply.code(), "invalid_cursor", "{uri}");
    }
    let reply = fx
        .get("/v1/content/manage/trash?status=any")
        .by(Some("reader"))
        .send()
        .await;
    assert_eq!(reply.code(), "invalid_query");
    let reply = fx
        .get("/v1/content/manage/trash?type=nope")
        .by(Some("reader"))
        .send()
        .await;
    assert_eq!(reply.code(), "content_type_not_found");
}

/// Run one `content.trash_purge` job on the fixture's own jobs runtime and
/// wait for it to succeed.
async fn run_purge_job(fx: &Fixture, key: &str) {
    let id: i64 = fx
        .db
        .acquire(soon())
        .await
        .unwrap()
        .query_one(
            "INSERT INTO merezha_jobs.jobs (queue, kind, idempotency_key, payload, payload_digest, \
                 max_attempts, correlation_id) \
             VALUES ('content', 'content.trash_purge', $1, '{}'::json, \
                     sha256('{}'::bytea), 3, 'c1') RETURNING id",
            &[&key],
        )
        .await
        .unwrap()
        .get(0);
    let deadline = soon();
    loop {
        let state = strings(
            &fx.db,
            &format!("SELECT state FROM merezha_jobs.jobs WHERE id = {id}"),
        )
        .await;
        if state == ["succeeded"] {
            return;
        }
        assert!(Instant::now() < deadline, "the purge job ended {state:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn purge_removes_only_expired_trash() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "trash = { retention = \"30d\" }", CHAIN_MODEL).await;
    let mut ids = Vec::new();
    for n in 0..4 {
        let id = id_of(
            &fx.create("article", json!({"fields": {"title": format!("A{n}")}}))
                .await,
        );
        ids.push(id);
    }
    for id in &ids[..3] {
        delete(&fx, &entry("article", id), "deleter").await;
    }
    // Two expired (31 days), one fresh (29 days), one live.
    execute(
        &fx.db,
        &format!(
            "UPDATE merezha_content.entries SET deleted_at = now() - interval '31 days' \
              WHERE id IN ('{}', '{}');
             UPDATE merezha_content.entries SET deleted_at = now() - interval '29 days' \
              WHERE id = '{}'",
            ids[0], ids[1], ids[2]
        ),
    )
    .await;
    run_purge_job(&fx, "content:test-purge-1").await;
    let left = strings(
        &fx.db,
        "SELECT id::text FROM merezha_content.entries ORDER BY id",
    )
    .await;
    let mut expected = vec![ids[2].clone(), ids[3].clone()];
    expected.sort();
    assert_eq!(left, expected);
    assert_eq!(
        scalar(
            &fx.db,
            &format!(
                "SELECT count(*) FROM merezha_content.revisions \
                  WHERE entry_id IN ('{}', '{}')",
                ids[0], ids[1]
            )
        )
        .await,
        0
    );
    let purged = events(&fx.db, "content.entry_purged").await;
    let mut purged_ids: Vec<&str> = purged
        .iter()
        .map(|e| e["payload"]["id"].as_str().unwrap())
        .collect();
    purged_ids.sort();
    let mut expired = vec![ids[0].as_str(), ids[1].as_str()];
    expired.sort();
    assert_eq!(purged_ids, expired);
    assert_eq!(purged[0]["correlation_id"], "c1");
    // More than one batch: 250 expired entries go in one run.
    execute(
        &fx.db,
        "INSERT INTO merezha_content.entries \
             (id, scope, content_type, created_at, created_by, deleted_at, deleted_by) \
         SELECT gen_random_uuid(), 'app', 'article', now(), 'sys_test', \
                now() - interval '40 days', 'sys_test' FROM generate_series(1, 250)",
    )
    .await;
    run_purge_job(&fx, "content:test-purge-2").await;
    assert_eq!(
        scalar(
            &fx.db,
            "SELECT count(*) FROM merezha_content.entries WHERE deleted_at IS NOT NULL"
        )
        .await,
        1
    );
    assert_eq!(events(&fx.db, "content.entry_purged").await.len(), 252);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_trash_purge_runs_on_its_declared_cron_schedule() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(
        &test,
        "trash = { retention = \"1d\", purge_cron = \"5 4 * * *\" }",
        CHAIN_MODEL,
    )
    .await;
    // Declared by the module, through `JobsHandle::schedule`, and stored
    // by the runtime's first schedule sync.
    let sql = "SELECT source || '|' || cron || '|' || timezone || '|' || queue || '|' || kind \
                 FROM merezha_jobs.schedules WHERE name = 'content.trash_purge'";
    let deadline = soon();
    while strings(&fx.db, sql).await.is_empty() {
        assert!(Instant::now() < deadline, "the schedule was never stored");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        strings(&fx.db, sql).await,
        ["content|5 4 * * *|UTC|content|content.trash_purge"]
    );
    let id = id_of(
        &fx.create("article", json!({"fields": {"title": "Old"}}))
            .await,
    );
    delete(&fx, &entry("article", &id), "deleter").await;
    execute(
        &fx.db,
        "UPDATE merezha_content.entries SET deleted_at = now() - interval '2 days'",
    )
    .await;
    // Make the occurrence due and tick once.
    execute(
        &fx.db,
        "UPDATE merezha_jobs.schedules SET next_run_at = now() - interval '1 minute' \
          WHERE name = 'content.trash_purge'",
    )
    .await;
    fx.jobs.schedule_now().await.unwrap();
    let deadline = soon();
    loop {
        let state = strings(
            &fx.db,
            "SELECT state FROM merezha_jobs.jobs WHERE schedule_name = 'content.trash_purge'",
        )
        .await;
        if state == ["succeeded"] {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the scheduled purge ended {state:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        scalar(&fx.db, "SELECT count(*) FROM merezha_content.entries").await,
        0
    );
    assert_eq!(events(&fx.db, "content.entry_purged").await.len(), 1);
}

#[tokio::test]
async fn a_purge_removes_incoming_refs_so_a_recreated_id_has_no_stale_referrers() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    for (n, immediate) in [(0, true), (1, false)] {
        let target = id_of(
            &fx.create("author", json!({"fields": {"name": format!("Ada {n}")}}))
                .await,
        );
        fx.create(
            "article",
            json!({"fields": {"title": format!("Refers {n}"), "author": {"id": target}}}),
        )
        .await;
        if immediate {
            delete(
                &fx,
                &format!("{}?purge=true", entry("author", &target)),
                "admin",
            )
            .await;
        } else {
            // Through the retention job.
            delete(&fx, &entry("author", &target), "admin").await;
            execute(
                &fx.db,
                &format!(
                    "UPDATE merezha_content.entries SET deleted_at = now() - interval '40 days' \
                      WHERE id = '{target}'"
                ),
            )
            .await;
            run_purge_job(&fx, "content:test-refs").await;
        }
        assert_eq!(
            scalar(
                &fx.db,
                &format!(
                    "SELECT count(*) FROM merezha_content.refs \
                      WHERE target_kind = 'entry' AND target_id = '{target}'"
                )
            )
            .await,
            0,
            "purged {immediate}"
        );
        // The id comes back with a client id, and nothing lists it.
        let again = fx
            .post("/v1/content/manage/entries/author")
            .json(&json!({"id": target, "fields": {"name": "Again"}}))
            .send()
            .await;
        assert_eq!(again.status, StatusCode::CREATED, "{:?}", again.body);
        let referrers = fx
            .get(&format!("{}/referrers", entry("author", &target)))
            .send()
            .await;
        assert_eq!(referrers.body["items"], json!([]), "purged {immediate}");
    }
}

#[tokio::test]
async fn a_self_reference_does_not_block_if_unreferenced() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    let id = id_of(
        &fx.create("article", json!({"fields": {"title": "Me"}}))
            .await,
    );
    let saved = fx
        .put(&variant("article", &id, "en"))
        .if_match(1)
        .json(&json!({"fields": {"title": "Me", "related": [{"id": id}]}}))
        .send()
        .await;
    assert_eq!(saved.status, StatusCode::OK, "{:?}", saved.body);
    let reply = delete(
        &fx,
        &format!("{}?if_unreferenced=true", entry("article", &id)),
        "admin",
    )
    .await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT, "{:?}", reply.body);
}
