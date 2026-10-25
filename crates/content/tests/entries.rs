//! CT-C3 drafts and revisions over the axum router and a real database:
//! saves, preconditions, retention, shared fields, unique fields,
//! singletons, scope predicates, references, referrers and permissions.
mod http;

use axum::http::{Method, StatusCode};
use http::*;
use merezha_platform::ports::DenyAll;
use merezha_postgres::TestDatabase;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

fn id_of(reply: &Reply) -> String {
    reply.body["sys"]["id"].as_str().unwrap().to_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn one_revision_per_save_and_one_winner_among_50_concurrent_saves() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", MODEL).await;
    let created = fx
        .create("article", json!({"fields": {"title": "Hello"}}))
        .await;
    let id = id_of(&created);
    assert_eq!(created.etag(), "\"r1\"");
    assert_eq!(created.body["sys"]["status"], "draft");
    assert_eq!(created.body["sys"]["model_version"], 1);
    assert_eq!(created.body["fields"]["slug"], "hello");
    assert_eq!(
        created.headers["location"],
        format!("/v1/content/manage/entries/article/{id}/en").as_str()
    );
    let path = variant("article", &id, "en");
    for round in 1..=3i64 {
        let mut tasks = Vec::new();
        for i in 0..50 {
            let client = fx.client.clone();
            let path = path.clone();
            tasks.push(tokio::spawn(async move {
                client
                    .put(&path)
                    .if_match(round)
                    .json(&json!({"fields": {"title": format!("t{round} {i}")}}))
                    .send()
                    .await
            }));
        }
        let mut won = 0;
        let mut conflicts = 0;
        for task in tasks {
            let reply = task.await.unwrap();
            match reply.status {
                StatusCode::OK => {
                    won += 1;
                    assert_eq!(reply.etag(), format!("\"r{}\"", round + 1));
                }
                StatusCode::PRECONDITION_FAILED => {
                    assert_eq!(reply.code(), "revision_conflict");
                    conflicts += 1;
                }
                other => panic!("unexpected {other}: {:?}", reply.body),
            }
        }
        assert_eq!((won, conflicts), (1, 49), "round {round}");
        assert_eq!(
            scalar(&fx.db, "SELECT count(*) FROM merezha_content.revisions").await,
            round + 1
        );
        assert_eq!(
            scalar(
                &fx.db,
                "SELECT count(*) FROM merezha_jobs.outbox WHERE topic = 'content.entry_draft_saved'"
            )
            .await,
            round + 1
        );
    }
    let read = fx.get(&path).send().await;
    assert_eq!(read.status, StatusCode::OK);
    assert_eq!(read.etag(), "\"r4\"");
    assert_eq!(read.headers["cache-control"], "private, no-store");
    // The slug a save leaves out is kept.
    assert_eq!(read.body["fields"]["slug"], "hello");
    let event: Value = serde_json::from_str(
        &fx.db
            .acquire(soon())
            .await
            .unwrap()
            .query_one(
                "SELECT payload::text FROM merezha_jobs.outbox \
                  WHERE topic = 'content.entry_draft_saved' ORDER BY id DESC LIMIT 1",
                &[],
            )
            .await
            .unwrap()
            .get::<_, String>(0),
    )
    .unwrap();
    assert_eq!(
        event["payload"],
        json!({"scope": "app", "type": "article", "id": id, "locale": "en", "revision": 4,
               "actor": {"subject": "usr_editor", "kind": "user"}})
    );
    assert_eq!(event["source"], "content");
    assert_eq!(event["tenant"], Value::Null);
}

#[tokio::test]
async fn a_stale_if_match_gives_412_and_writes_nothing_and_a_missing_one_gives_428() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", MODEL).await;
    let id = id_of(
        &fx.create("article", json!({"fields": {"title": "A"}}))
            .await,
    );
    let path = variant("article", &id, "en");
    let saved = fx
        .put(&path)
        .if_match(1)
        .json(&json!({"fields": {"title": "B"}}))
        .send()
        .await;
    assert_eq!(
        (saved.status, saved.etag().as_str()),
        (StatusCode::OK, "\"r2\"")
    );
    let before = snapshot(&fx.db).await;
    let body = json!({"fields": {"title": "C"}});
    for reply in [
        fx.put(&path).if_match(1).json(&body).send().await,
        fx.put(&path)
            .header("if-match", "W/\"r2\"")
            .json(&body)
            .send()
            .await,
        fx.put(&path)
            .header("if-match", "*")
            .json(&body)
            .send()
            .await,
        fx.patch(&path).if_match(1).json(&body).send().await,
        fx.post(&format!("{path}/revisions/1/restore"))
            .if_match(1)
            .send()
            .await,
    ] {
        assert_eq!(
            (reply.status, reply.code()),
            (StatusCode::PRECONDITION_FAILED, "revision_conflict"),
            "{:?}",
            reply.body
        );
    }
    for reply in [
        fx.put(&path).json(&body).send().await,
        fx.patch(&path).json(&body).send().await,
        fx.post(&format!("{path}/revisions/1/restore")).send().await,
    ] {
        assert_eq!(
            (reply.status, reply.code()),
            (StatusCode::PRECONDITION_REQUIRED, "precondition_required"),
        );
    }
    // If-None-Match: * on an existing variant.
    let exists = fx
        .put(&path)
        .header("if-none-match", "*")
        .json(&body)
        .send()
        .await;
    assert_eq!(
        (exists.status, exists.code()),
        (StatusCode::PRECONDITION_FAILED, "variant_exists")
    );
    assert_eq!(snapshot(&fx.db).await, before);
}

#[tokio::test]
async fn retention_keeps_the_newest_revisions_and_never_prunes_the_published_one() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "revisions = { retain = 3 }", MODEL).await;
    let id = id_of(
        &fx.create("article", json!({"fields": {"title": "r1"}}))
            .await,
    );
    let path = variant("article", &id, "en");
    let put = |revision: i64| {
        fx.put(&path)
            .if_match(revision)
            .json(&json!({"fields": {"title": format!("r{}", revision + 1)}}))
            .send()
    };
    assert_eq!(put(1).await.status, StatusCode::OK);
    // Publishing is C4's: point the variant at revision 2 directly.
    execute(
        &fx.db,
        "UPDATE merezha_content.variants SET published_revision = 2, published_at = now(), \
         first_published_at = now()",
    )
    .await;
    for revision in 2..=6 {
        assert_eq!(put(revision).await.status, StatusCode::OK);
    }
    let kept: Vec<i64> = {
        let client = fx.db.acquire(soon()).await.unwrap();
        client
            .query(
                "SELECT revision FROM merezha_content.revisions ORDER BY revision",
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect()
    };
    assert_eq!(kept, vec![2, 5, 6, 7]);
    let history = fx.get(&format!("{path}/revisions?limit=3")).send().await;
    assert_eq!(history.status, StatusCode::OK);
    let items = history.body["items"].as_array().unwrap();
    let numbers: Vec<i64> = items
        .iter()
        .map(|i| i["revision"].as_i64().unwrap())
        .collect();
    assert_eq!(numbers, vec![7, 6, 5]);
    assert_eq!(items[0]["title"], "r7");
    assert_eq!(items[0]["kind"], "save");
    let cursor = history.body["page"]["next_cursor"].as_str().unwrap();
    let rest = fx
        .get(&format!("{path}/revisions?limit=3&cursor={cursor}"))
        .send()
        .await;
    let rest_items = rest.body["items"].as_array().unwrap();
    assert_eq!(rest_items.len(), 1);
    assert_eq!(rest_items[0]["revision"], 2);
    assert_eq!(rest_items[0]["published"], true);
    assert_eq!(rest.body["page"]["next_cursor"], Value::Null);
    let pruned = fx.get(&format!("{path}/revisions/1")).send().await;
    assert_eq!(
        (pruned.status, pruned.code()),
        (StatusCode::NOT_FOUND, "revision_not_found")
    );
    let published = fx.get(&format!("{path}/revisions/2")).send().await;
    assert_eq!(published.status, StatusCode::OK);
    assert_eq!(published.body["fields"]["title"], "r2");
    assert_eq!(published.body["sys"]["published"], true);
    let current = fx.get(&path).send().await;
    assert_eq!(current.body["sys"]["status"], "changed");
    assert_eq!(current.body["sys"]["published_revision"], 2);
}

#[tokio::test]
async fn shared_fields_belong_to_the_default_variant_and_reach_every_variant() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", MODEL).await;
    let ada = id_of(
        &fx.create("author", json!({"fields": {"name": "Ada"}}))
            .await,
    );
    let bob = id_of(
        &fx.create("author", json!({"fields": {"name": "Bob"}}))
            .await,
    );
    let created = fx
        .create(
            "article",
            json!({"fields": {"title": "Hello", "author": {"id": ada}}}),
        )
        .await;
    let id = id_of(&created);
    // The host fills the reference's type.
    assert_eq!(
        created.body["fields"]["author"],
        json!({"id": ada, "type": "author"})
    );
    let fr = variant("article", &id, "fr");
    let refused = fx
        .put(&fr)
        .header("if-none-match", "*")
        .json(&json!({"fields": {"title": "Bonjour", "author": {"id": bob}}}))
        .send()
        .await;
    assert_eq!(refused.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        refused.details(),
        vec![(
            "/fields/author".to_owned(),
            "field_not_localized".to_owned()
        )]
    );
    let french = fx
        .put(&fr)
        .header("if-none-match", "*")
        .json(&json!({"fields": {"title": "Bonjour"}}))
        .send()
        .await;
    assert_eq!(french.status, StatusCode::CREATED, "{:?}", french.body);
    assert_eq!(french.etag(), "\"r1\"");
    assert_eq!(
        french.body["fields"]["author"],
        json!({"id": ada, "type": "author"})
    );
    // A default save copies the shared value into every variant.
    let saved = fx
        .put(&variant("article", &id, "en"))
        .if_match(1)
        .json(&json!({"fields": {"title": "Hello", "author": {"id": bob}}}))
        .send()
        .await;
    assert_eq!(saved.status, StatusCode::OK);
    let french = fx.get(&fr).send().await;
    assert_eq!(french.body["fields"]["author"]["id"], bob.as_str());
    assert_eq!(french.body["sys"]["draft_revision"], 1);
    // The French revision keeps its own fields only.
    let revision = fx.get(&format!("{fr}/revisions/1")).send().await;
    assert_eq!(
        revision.body["fields"],
        json!({"title": "Bonjour", "slug": "bonjour"})
    );
    // A French patch leaves the shared value alone.
    let renamed = fx
        .patch(&fr)
        .if_match(1)
        .json(&json!({"fields": {"title": "Salut"}}))
        .send()
        .await;
    assert_eq!(renamed.status, StatusCode::OK, "{:?}", renamed.body);
    assert_eq!(renamed.body["fields"]["author"]["id"], bob.as_str());
    let revision = fx.get(&format!("{fr}/revisions/2")).send().await;
    assert_eq!(
        revision.body["fields"],
        json!({"title": "Salut", "slug": "bonjour"})
    );
    let patched = fx
        .patch(&fr)
        .if_match(2)
        .json(&json!({"fields": {"author": {"id": ada}}}))
        .send()
        .await;
    assert_eq!(
        patched.details(),
        vec![(
            "/fields/author".to_owned(),
            "field_not_localized".to_owned()
        )]
    );
    let summary = fx
        .get(&format!("/v1/content/manage/entries/article/{id}"))
        .send()
        .await;
    let locales: Vec<&str> = summary.body["variants"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["locale"].as_str().unwrap())
        .collect();
    assert_eq!(locales, vec!["en", "fr"]);
    assert_eq!(summary.body["sys"]["deleted_at"], Value::Null);
}

#[tokio::test]
async fn unique_violations_name_the_field_and_write_nothing() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", MODEL).await;
    let first = id_of(
        &fx.create(
            "article",
            json!({"fields": {"title": "One", "legacy_code": "X"}}),
        )
        .await,
    );
    let before = snapshot(&fx.db).await;
    for (fields, field) in [
        (
            json!({"title": "Two", "legacy_code": "X"}),
            "/fields/legacy_code",
        ),
        (json!({"title": "One"}), "/fields/slug"),
    ] {
        let reply = fx
            .post("/v1/content/manage/entries/article")
            .json(&json!({ "fields": fields }))
            .send()
            .await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(reply.code(), "validation_failed");
        assert_eq!(
            reply.details(),
            vec![(field.to_owned(), "unique".to_owned())]
        );
    }
    assert_eq!(snapshot(&fx.db).await, before);
    let second = id_of(
        &fx.create("article", json!({"fields": {"title": "Two"}}))
            .await,
    );
    let before = snapshot(&fx.db).await;
    let reply = fx
        .put(&variant("article", &second, "en"))
        .if_match(1)
        .json(&json!({"fields": {"title": "Two", "legacy_code": "X"}}))
        .send()
        .await;
    assert_eq!(
        reply.details(),
        vec![("/fields/legacy_code".to_owned(), "unique".to_owned())]
    );
    assert_eq!(snapshot(&fx.db).await, before);
    // Uniqueness is per locale for localized fields.
    let french = fx
        .put(&variant("article", &first, "fr"))
        .header("if-none-match", "*")
        .json(&json!({"fields": {"title": "One", "legacy_code": "X"}}))
        .send()
        .await;
    assert_eq!(french.status, StatusCode::CREATED, "{:?}", french.body);
}

#[tokio::test]
async fn a_second_singleton_fails() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", MODEL).await;
    let en = "/v1/content/manage/singletons/site_settings/en";
    let missing = fx.get(en).send().await;
    assert_eq!(
        (missing.status, missing.code()),
        (StatusCode::NOT_FOUND, "entry_not_found")
    );
    let not_yet = fx
        .put(en)
        .if_match(1)
        .json(&json!({"fields": {}}))
        .send()
        .await;
    assert_eq!(not_yet.code(), "entry_not_found");
    let created = fx
        .put(en)
        .header("if-none-match", "*")
        .json(&json!({"fields": {"site_name": "Site"}}))
        .send()
        .await;
    assert_eq!(created.status, StatusCode::CREATED, "{:?}", created.body);
    let id = id_of(&created);
    let again = fx
        .put(en)
        .header("if-none-match", "*")
        .json(&json!({"fields": {"site_name": "Other"}}))
        .send()
        .await;
    assert_eq!(
        (again.status, again.code()),
        (StatusCode::PRECONDITION_FAILED, "variant_exists")
    );
    let second = fx
        .post("/v1/content/manage/entries/site_settings")
        .json(&json!({"fields": {"site_name": "Second"}}))
        .send()
        .await;
    assert_eq!(
        (second.status, second.code()),
        (StatusCode::CONFLICT, "singleton_exists")
    );
    let fr = fx
        .put("/v1/content/manage/singletons/site_settings/fr")
        .header("if-none-match", "*")
        .json(&json!({"fields": {"site_name": "Site FR"}}))
        .send()
        .await;
    assert_eq!(fr.status, StatusCode::CREATED);
    assert_eq!(id_of(&fr), id);
    let patched = fx
        .patch(en)
        .if_match(1)
        .json(&json!({"fields": {"site_name": "Renamed"}}))
        .send()
        .await;
    assert_eq!(
        (patched.status, patched.etag().as_str()),
        (StatusCode::OK, "\"r2\"")
    );
    let history = fx.get(&format!("{en}/revisions")).send().await;
    assert_eq!(history.body["items"].as_array().unwrap().len(), 2);
    let first = fx.get(&format!("{en}/revisions/1")).send().await;
    assert_eq!(first.body["fields"]["site_name"], "Site");
    let restored = fx
        .post(&format!("{en}/revisions/1/restore"))
        .if_match(2)
        .send()
        .await;
    assert_eq!(restored.status, StatusCode::OK);
    assert_eq!(restored.body["fields"]["site_name"], "Site");
    assert_eq!(restored.body["warnings"], json!([]));
    let read = fx.get(en).send().await;
    assert_eq!(read.etag(), "\"r3\"");
    let collection = fx
        .get("/v1/content/manage/singletons/article/en")
        .send()
        .await;
    assert_eq!(collection.code(), "content_type_not_found");
    assert_eq!(
        scalar(
            &fx.db,
            "SELECT count(*) FROM merezha_content.entries WHERE content_type = 'site_settings'"
        )
        .await,
        1
    );
}

/// Every entry route, as `(method, path after the type, precondition,
/// body)`; `{id}` is replaced. C4 adds publish, unpublish, the variant
/// delete, the entry delete and undelete.
const ENTRY_ROUTES: [(&str, &str, &str, &str); 14] = [
    ("GET", "/{id}", "", ""),
    ("GET", "/{id}/en", "", ""),
    ("PUT", "/{id}/en", "if-match", r#"{"fields":{"title":"x"}}"#),
    (
        "PUT",
        "/{id}/fr",
        "if-none-match",
        r#"{"fields":{"title":"x"}}"#,
    ),
    (
        "PATCH",
        "/{id}/en",
        "if-match",
        r#"{"fields":{"title":"x"}}"#,
    ),
    ("GET", "/{id}/en/revisions", "", ""),
    ("GET", "/{id}/en/revisions/1", "", ""),
    ("POST", "/{id}/en/revisions/1/restore", "if-match", ""),
    ("GET", "/{id}/referrers", "", ""),
    ("POST", "/{id}/en/publish", "if-match", ""),
    ("POST", "/{id}/en/unpublish", "if-match", ""),
    ("DELETE", "/{id}/en", "if-match", ""),
    ("DELETE", "/{id}?if_unreferenced=true", "", ""),
    ("POST", "/{id}/undelete", "", ""),
];

#[tokio::test]
async fn every_entry_route_answers_404_for_another_type_or_scope_and_changes_nothing() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", MODEL).await;
    let own = id_of(
        &fx.create("article", json!({"fields": {"title": "Mine"}}))
            .await,
    );
    let author = id_of(
        &fx.create("author", json!({"fields": {"name": "Ada"}}))
            .await,
    );
    let foreign = seed_row(&fx.db, "org:acme", "article", r#"{"title":"Secret"}"#).await;
    // A ref to `own`, so the guarded delete proves it reached the row
    // without deleting it.
    execute(
        &fx.db,
        &format!(
            "INSERT INTO merezha_content.refs \
                 (scope, from_entry, from_locale, stage, path, target_kind, target_id) \
             VALUES ('app', '{author}', 'en', 'draft', '/fields/x', 'entry', '{own}')"
        ),
    )
    .await;
    // The routes reach the row of the right type and scope.
    for (method, path, condition, body) in ENTRY_ROUTES {
        let mut call = fx
            .call(
                Method::from_bytes(method.as_bytes()).unwrap(),
                &format!(
                    "/v1/content/manage/entries/article{}",
                    path.replace("{id}", &own)
                ),
            )
            .by(Some("admin"));
        if method == "PATCH" {
            call = call.header("content-type", "application/merge-patch+json");
        }
        if condition == "if-none-match" {
            continue;
        }
        if !condition.is_empty() {
            call = call.header(condition, "\"r999\"");
        }
        let reply = call.raw(body.as_bytes()).send().await;
        assert!(
            reply.status == StatusCode::OK
                || [
                    "revision_conflict",
                    "default_variant_required",
                    "entry_referenced"
                ]
                .contains(&reply.code()),
            "{method} {path}: {:?}",
            reply.body
        );
    }
    let before = snapshot(&fx.db).await;
    for wrong in [&author, &foreign] {
        for (method, path, condition, body) in ENTRY_ROUTES {
            let uri = format!(
                "/v1/content/manage/entries/article{}",
                path.replace("{id}", wrong)
            );
            let mut call = fx
                .call(Method::from_bytes(method.as_bytes()).unwrap(), &uri)
                .by(Some("admin"));
            if method == "PATCH" {
                call = call.header("content-type", "application/merge-patch+json");
            }
            match condition {
                "if-match" => call = call.if_match(1),
                "if-none-match" => call = call.header("if-none-match", "*"),
                _ => {}
            }
            let reply = call.raw(body.as_bytes()).send().await;
            assert_eq!(
                (reply.status, reply.code()),
                (StatusCode::NOT_FOUND, "entry_not_found"),
                "{method} {uri}"
            );
        }
    }
    // The purge and the batches refuse them too.
    for wrong in [&author, &foreign] {
        let purge = fx
            .call(
                Method::DELETE,
                &format!("/v1/content/manage/entries/article/{wrong}?purge=true"),
            )
            .by(Some("admin"))
            .send()
            .await;
        assert_eq!(purge.code(), "entry_not_found");
        for (batch, item) in [
            (
                "publish",
                json!({"type": "article", "id": wrong, "locale": "en", "revision": 1}),
            ),
            (
                "unpublish",
                json!({"type": "article", "id": wrong, "locale": "en"}),
            ),
        ] {
            let reply = fx
                .post(&format!("/v1/content/manage/batch/{batch}"))
                .by(Some("admin"))
                .json(&json!({ "items": [item] }))
                .send()
                .await;
            assert_eq!(reply.code(), "entry_not_found", "{batch}");
        }
    }
    assert_eq!(snapshot(&fx.db).await, before);
    let list = fx.get("/v1/content/manage/entries/article").send().await;
    let ids: Vec<&str> = list.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["sys"]["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec![own.as_str()]);
    // A client id taken in another scope is still taken.
    let taken = fx
        .post("/v1/content/manage/entries/article")
        .json(&json!({"id": foreign, "fields": {"title": "Copy"}}))
        .send()
        .await;
    assert_eq!(
        (taken.status, taken.code()),
        (StatusCode::CONFLICT, "entry_exists")
    );
}

#[tokio::test]
async fn references_resolve_against_the_database_and_referrers_list_them() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", MODEL).await;
    let ada = id_of(
        &fx.create("author", json!({"fields": {"name": "Ada"}}))
            .await,
    );
    let trashed = id_of(
        &fx.create("author", json!({"fields": {"name": "Gone"}}))
            .await,
    );
    execute(
        &fx.db,
        &format!(
            "UPDATE merezha_content.entries SET deleted_at = now(), deleted_by = 'sys_test' \
              WHERE id = '{trashed}'"
        ),
    )
    .await;
    let other_scope = seed_row(&fx.db, "org:acme", "author", r#"{"name":"Org"}"#).await;
    let page = id_of(
        &fx.create("article", json!({"fields": {"title": "Page"}}))
            .await,
    );
    let nowhere = "0192f7a0-5d1c-7c3e-9b1a-6f0e2c4d8a11";
    let before = snapshot(&fx.db).await;
    for (target, code) in [
        (nowhere, "reference_not_found"),
        (page.as_str(), "reference_type"),
        (trashed.as_str(), "reference_not_found"),
        // Another scope's entry answers like an absent one (C9): a save
        // never tells that another tenant has the id.
        (other_scope.as_str(), "reference_not_found"),
    ] {
        let reply = fx
            .post("/v1/content/manage/entries/article")
            .json(&json!({"fields": {"title": "Q", "author": {"id": target}}}))
            .send()
            .await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY, "{target}");
        assert_eq!(
            reply.details(),
            vec![("/fields/author".to_owned(), code.to_owned())]
        );
    }
    // Field rules and reference problems come back together.
    let both = fx
        .post("/v1/content/manage/entries/article")
        .json(&json!({"fields": {"title": "Q", "reading_minutes": 0, "author": {"id": nowhere}}}))
        .send()
        .await;
    assert_eq!(
        both.details(),
        vec![
            ("/fields/reading_minutes".to_owned(), "min".to_owned()),
            (
                "/fields/author".to_owned(),
                "reference_not_found".to_owned()
            )
        ]
    );
    assert_eq!(snapshot(&fx.db).await, before);
    let body = json!([{
        "_type": "block", "_key": "b1", "style": "normal",
        "markDefs": [{"_type": "entry_link", "_key": "l1", "id": ada}],
        "children": [{"_type": "span", "_key": "s1", "text": "Ada", "marks": ["l1"]}]
    }]);
    let quoting = fx
        .create(
            "article",
            json!({"fields": {"title": "Quoting", "author": {"id": ada},
                   "related": [{"id": page}, {"id": ada}], "body": body}}),
        )
        .await;
    let quoting_id = id_of(&quoting);
    assert_eq!(
        quoting.body["fields"]["related"],
        json!([{"id": page, "type": "article"}, {"id": ada, "type": "author"}])
    );
    assert_eq!(
        quoting.body["fields"]["body"][0]["markDefs"][0]["type"],
        "author"
    );
    let referrers = format!("/v1/content/manage/entries/author/{ada}/referrers");
    let first = fx.get(&format!("{referrers}?limit=2")).send().await;
    assert_eq!(first.status, StatusCode::OK, "{:?}", first.body);
    let items = first.body["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    let mut paths: Vec<&str> = items.iter().map(|i| i["path"].as_str().unwrap()).collect();
    let cursor = first.body["page"]["next_cursor"]
        .as_str()
        .unwrap()
        .to_owned();
    let second = fx
        .get(&format!("{referrers}?limit=2&cursor={cursor}"))
        .send()
        .await;
    let more = second.body["items"].as_array().unwrap();
    paths.extend(more.iter().map(|i| i["path"].as_str().unwrap()));
    paths.sort();
    assert_eq!(
        paths,
        vec![
            "/fields/author",
            "/fields/body/b1/markDefs/l1",
            "/fields/related/1"
        ]
    );
    assert_eq!(second.body["page"]["next_cursor"], Value::Null);
    assert_eq!(
        items[0],
        json!({"id": quoting_id, "type": "article", "locale": "en", "stage": "draft",
               "path": items[0]["path"], "title": "Quoting"})
    );
    let other = fx
        .get(&format!(
            "{referrers}?limit=2&cursor={cursor}&stage=published"
        ))
        .send()
        .await;
    assert_eq!(other.code(), "invalid_cursor");
    let published = fx.get(&format!("{referrers}?stage=published")).send().await;
    assert_eq!(published.body["items"], json!([]));
    // A save rewrites the variant's draft refs.
    let saved = fx
        .put(&variant("article", &quoting_id, "en"))
        .if_match(1)
        .json(&json!({"fields": {"title": "Quoting", "related": [{"id": page}]}}))
        .send()
        .await;
    assert_eq!(saved.status, StatusCode::OK);
    let after = fx.get(&referrers).send().await;
    assert_eq!(after.body["items"], json!([]));
    assert_eq!(
        scalar(&fx.db, "SELECT count(*) FROM merezha_content.refs").await,
        1
    );
}

#[tokio::test]
async fn routes_check_their_permission_in_the_type_resource_scope() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", MODEL).await;
    let list = "/v1/content/manage/entries/article";
    let body = json!({"fields": {"title": "T"}});
    let cases: [(Option<&str>, Method, &str, StatusCode, &str); 10] = [
        (
            None,
            Method::GET,
            list,
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
        ),
        (
            None,
            Method::POST,
            list,
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
        ),
        (Some("reader"), Method::GET, list, StatusCode::OK, ""),
        (
            Some("reader"),
            Method::POST,
            list,
            StatusCode::FORBIDDEN,
            "unauthorized",
        ),
        (
            Some("article_writer"),
            Method::POST,
            list,
            StatusCode::CREATED,
            "",
        ),
        (
            Some("article_writer"),
            Method::POST,
            "/v1/content/manage/entries/author",
            StatusCode::FORBIDDEN,
            "unauthorized",
        ),
        (
            Some("tenant_editor"),
            Method::GET,
            list,
            StatusCode::FORBIDDEN,
            "unauthorized",
        ),
        (
            Some("nobody"),
            Method::GET,
            list,
            StatusCode::FORBIDDEN,
            "unauthorized",
        ),
        (
            Some("editor"),
            Method::GET,
            "/v1/content/manage/entries/missing",
            StatusCode::NOT_FOUND,
            "content_type_not_found",
        ),
        (
            Some("article_writer"),
            Method::GET,
            "/v1/content/manage/entries/missing",
            StatusCode::FORBIDDEN,
            "unauthorized",
        ),
    ];
    for (principal, method, uri, status, code) in cases {
        let mut call = fx.call(method.clone(), uri).by(principal);
        if method == Method::POST {
            call = call.json(&body);
        }
        let reply = call.send().await;
        assert_eq!(
            reply.status, status,
            "{principal:?} {method} {uri}: {:?}",
            reply.body
        );
        if !code.is_empty() {
            assert_eq!(reply.code(), code);
        }
    }
    // A public listener without identity: the deny-all policy.
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), true, "", MODEL);
    let public = start_in(dir, &test, Arc::new(DenyAll)).await;
    for principal in [None, Some("editor")] {
        let reply = public.get(list).by(principal).send().await;
        let expected = if principal.is_some() {
            (StatusCode::FORBIDDEN, "unauthorized")
        } else {
            (StatusCode::UNAUTHORIZED, "unauthenticated")
        };
        assert_eq!((reply.status, reply.code()), expected);
    }
}

#[tokio::test]
async fn creation_and_bodies_follow_the_request_rules() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(
        &test,
        "limits = { max_request_bytes = 4096, max_entry_bytes = 1024 }",
        MODEL,
    )
    .await;
    let list = "/v1/content/manage/entries/article";
    let id = "0192f7a0-5d1c-7c3e-9b1a-6f0e2c4d8a11";
    let created = fx
        .post(list)
        .json(&json!({"id": id, "locale": "fr", "fields": {"title": "Import"}}))
        .send()
        .await;
    assert_eq!(created.status, StatusCode::CREATED);
    assert_eq!(created.body["sys"]["id"], id);
    assert_eq!(created.body["sys"]["locale"], "fr");
    let cases: Vec<(Vec<u8>, StatusCode, &str)> = vec![
        (
            serde_json::to_vec(&json!({"id": id, "fields": {"title": "Again"}})).unwrap(),
            StatusCode::CONFLICT,
            "entry_exists",
        ),
        (
            serde_json::to_vec(&json!({"locale": "de", "fields": {}})).unwrap(),
            StatusCode::BAD_REQUEST,
            "locale_unknown",
        ),
        (
            br#"{"fields":{},"fields":{}}"#.to_vec(),
            StatusCode::BAD_REQUEST,
            "invalid_json",
        ),
        (
            br#"{"fields":{},"extra":1}"#.to_vec(),
            StatusCode::BAD_REQUEST,
            "invalid_json",
        ),
        (
            b"not json".to_vec(),
            StatusCode::BAD_REQUEST,
            "invalid_json",
        ),
        (
            serde_json::to_vec(&json!({"id": "nope", "fields": {}})).unwrap(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_failed",
        ),
        (
            serde_json::to_vec(&json!({"fields": {"title": "x".repeat(5000)}})).unwrap(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "body_limit",
        ),
        (
            serde_json::to_vec(
                &json!({"fields": {"title": "x".repeat(200), "tags": vec!["t".repeat(40); 20]}}),
            )
            .unwrap(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "body_limit",
        ),
        // Small enough as sent, too large once every item has its `_key`.
        (
            serde_json::to_vec(
                &json!({"fields": {"sections": vec![json!({"_type": "quote", "text": "x"}); 30]}}),
            )
            .unwrap(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "body_limit",
        ),
        (
            serde_json::to_vec(&json!({"fields": {"title": 5, "nope": 1}})).unwrap(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_failed",
        ),
    ];
    for (body, status, code) in cases {
        let reply = fx.post(list).raw(&body).send().await;
        assert_eq!(
            (reply.status, reply.code()),
            (status, code),
            "{}",
            String::from_utf8_lossy(&body[..body.len().min(80)])
        );
    }
    let invalid = fx
        .post(list)
        .json(&json!({"fields": {"title": 5, "nope": 1}}))
        .send()
        .await;
    assert_eq!(
        invalid.details(),
        vec![
            ("/fields".to_owned(), "unknown_field".to_owned()),
            ("/fields/title".to_owned(), "type_mismatch".to_owned())
        ]
    );
    // A non-JSON merge patch is refused before anything is read.
    let wrong_type = fx
        .call(Method::PATCH, &variant("article", id, "fr"))
        .if_match(1)
        .json(&json!({"fields": {}}))
        .send()
        .await;
    assert_eq!(
        (wrong_type.status, wrong_type.code()),
        (StatusCode::UNSUPPORTED_MEDIA_TYPE, "unsupported_media_type")
    );
    let unknown_locale = fx.get(&variant("article", id, "de")).send().await;
    assert_eq!(unknown_locale.code(), "locale_unknown");
    let bad_id = fx.get(&variant("article", "not-a-uuid", "en")).send().await;
    assert_eq!(bad_id.code(), "entry_not_found");
}

#[tokio::test]
async fn patch_merges_and_restore_writes_a_new_revision() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", MODEL).await;
    let created = fx
        .create(
            "article",
            json!({"fields": {"title": "Notes", "category": "news", "tags": ["a", "b"],
                   "sections": [{"_type": "quote", "_key": "q1", "text": "Ship small."}]}}),
        )
        .await;
    let id = id_of(&created);
    let path = variant("article", &id, "en");
    let patched = fx
        .patch(&path)
        .if_match(1)
        .json(&json!({"fields": {"category": null, "tags": ["c"], "score": "4.5"}}))
        .send()
        .await;
    assert_eq!(patched.status, StatusCode::OK, "{:?}", patched.body);
    assert_eq!(
        patched.body["fields"],
        json!({"title": "Notes", "slug": "notes", "score": "4.50", "tags": ["c"],
               "sections": [{"_type": "quote", "_key": "q1", "text": "Ship small."}]})
    );
    // Fields come back in model order, not name order.
    let at = |name: &str| patched.text.find(&format!("\"{name}\":")).unwrap();
    assert!(at("title") < at("slug") && at("slug") < at("score"));
    assert!(at("score") < at("tags") && at("tags") < at("sections"));
    let restored = fx
        .post(&format!("{path}/revisions/1/restore"))
        .if_match(2)
        .send()
        .await;
    assert_eq!(restored.status, StatusCode::OK, "{:?}", restored.body);
    assert_eq!(restored.etag(), "\"r3\"");
    assert_eq!(restored.body["fields"]["category"], "news");
    assert_eq!(restored.body["fields"]["tags"], json!(["a", "b"]));
    assert_eq!(restored.body["warnings"], json!([]));
    let history = fx.get(&format!("{path}/revisions")).send().await;
    let kinds: Vec<(&str, Value)> = history.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| (i["kind"].as_str().unwrap(), i["restored_from"].clone()))
        .collect();
    assert_eq!(
        kinds,
        vec![
            ("restore", json!(1)),
            ("save", Value::Null),
            ("create", Value::Null)
        ]
    );
    let missing = fx
        .post(&format!("{path}/revisions/9/restore"))
        .if_match(3)
        .send()
        .await;
    assert_eq!(missing.code(), "revision_not_found");
    let selected = fx.get(&format!("{path}?fields=title,tags")).send().await;
    assert_eq!(
        selected.body["fields"],
        json!({"title": "Notes", "tags": ["a", "b"]})
    );
    let unknown = fx.get(&format!("{path}?fields=nope")).send().await;
    assert_eq!(unknown.code(), "invalid_query");
}

/// The default variant's `FOR SHARE`, deterministically: a variant save
/// waits for an in-flight default save and copies its committed value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_variant_save_waits_for_an_in_flight_default_save() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", MODEL).await;
    let ada = fx
        .create("author", json!({"fields": {"name": "Ada"}}))
        .await
        .body["sys"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let bob = fx
        .create("author", json!({"fields": {"name": "Bob"}}))
        .await
        .body["sys"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let id = fx
        .create(
            "article",
            json!({"fields": {"title": "T", "author": {"id": ada}}}),
        )
        .await
        .body["sys"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let fr = variant("article", &id, "fr");
    let created = fx
        .put(&fr)
        .header("if-none-match", "*")
        .json(&json!({"fields": {"title": "F"}}))
        .send()
        .await;
    assert_eq!(created.status, StatusCode::CREATED);

    // An in-flight default save: the default row and its copy into `fr`,
    // uncommitted, exactly what Store::save writes for `en`.
    let holder = fx.db.acquire(soon()).await.unwrap();
    let value = json!({"id": bob, "type": "author"}).to_string();
    holder
        .batch_execute(&format!(
            "BEGIN; \
             UPDATE merezha_content.variants \
                SET draft_data = jsonb_set(draft_data, '{{author}}', '{value}'::jsonb), \
                    draft_revision = draft_revision + 1 \
              WHERE entry_id = '{id}' AND locale = 'en'; \
             UPDATE merezha_content.variants \
                SET draft_data = jsonb_set(draft_data, '{{author}}', '{value}'::jsonb) \
              WHERE entry_id = '{id}' AND locale = 'fr';"
        ))
        .await
        .unwrap();
    let pid: i32 = holder
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);

    let save = {
        let client = fx.client.clone();
        let fr = fr.clone();
        tokio::spawn(async move {
            client
                .patch(&fr)
                .if_match(1)
                .json(&json!({"fields": {"title": "F2"}}))
                .send()
                .await
        })
    };
    // Wait until the save blocks on a row lock the holder owns (at the
    // FOR SHARE with the lock, at its own UPDATE without it).
    let watcher = fx.db.acquire(soon()).await.unwrap();
    let mut blocked = false;
    for _ in 0..200 {
        let n: i64 = watcher
            .query_one(
                "SELECT count(*) FROM pg_stat_activity \
                  WHERE wait_event_type = 'Lock' AND $1 = ANY(pg_blocking_pids(pid))",
                &[&pid],
            )
            .await
            .unwrap()
            .get(0);
        if n > 0 {
            blocked = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(blocked, "the variant save never waited on the holder");
    holder.batch_execute("COMMIT").await.unwrap();
    let reply = save.await.unwrap();
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    let fr_author = fx.get(&fr).send().await.body["fields"]["author"]["id"].clone();
    assert_eq!(fr_author, json!(bob), "fr kept a stale shared value");
}

/// The entry-size bound on the fields as sent comes before field rules.
#[tokio::test]
async fn an_oversized_body_is_body_limit_before_field_rules() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", MODEL).await;
    let tags: Vec<String> = (0..20).map(|_| "x".repeat(40)).collect();
    let big = "y".repeat(300_000);
    let reply = fx
        .post("/v1/content/manage/entries/article")
        .json(&json!({"fields": {"title": "T", "reading_minutes": 0, "legacy_code": big, "tags": tags}}))
        .send()
        .await;
    assert_eq!(reply.code(), "body_limit", "{:?}", reply.body);
}

/// Every management route with the permission it needs: `read`,
/// `write`, `publish`, `delete` or `purge` (delete and purge). The
/// destructive ones come last, so the passing round can run them in order.
const MANAGEMENT_ROUTES: [(&str, &str, &str, &str, &str); 25] = [
    ("GET", "/entries/article", "read", "", ""),
    (
        "POST",
        "/entries/article",
        "write",
        "",
        r#"{"fields":{"title":"New"}}"#,
    ),
    ("GET", "/entries/article/{id}", "read", "", ""),
    ("GET", "/entries/article/{id}/referrers", "read", "", ""),
    ("GET", "/entries/article/{id}/en", "read", "", ""),
    (
        "PUT",
        "/entries/article/{id}/en",
        "write",
        "if-match",
        r#"{"fields":{"title":"x"}}"#,
    ),
    (
        "PATCH",
        "/entries/article/{id}/en",
        "write",
        "if-match",
        r#"{"fields":{"title":"x"}}"#,
    ),
    ("GET", "/entries/article/{id}/en/revisions", "read", "", ""),
    (
        "GET",
        "/entries/article/{id}/en/revisions/1",
        "read",
        "",
        "",
    ),
    (
        "POST",
        "/entries/article/{id}/en/revisions/1/restore",
        "write",
        "if-match",
        "",
    ),
    ("GET", "/singletons/site_settings/en", "read", "", ""),
    (
        "PUT",
        "/singletons/site_settings/fr",
        "write",
        "if-none-match",
        r#"{"fields":{"site_name":"S"}}"#,
    ),
    (
        "PATCH",
        "/singletons/site_settings/en",
        "write",
        "if-match",
        r#"{"fields":{"site_name":"x"}}"#,
    ),
    (
        "GET",
        "/singletons/site_settings/en/revisions",
        "read",
        "",
        "",
    ),
    (
        "GET",
        "/singletons/site_settings/en/revisions/1",
        "read",
        "",
        "",
    ),
    (
        "POST",
        "/singletons/site_settings/en/revisions/1/restore",
        "write",
        "if-match",
        "",
    ),
    (
        "POST",
        "/entries/article/{id}/en/publish",
        "publish",
        "if-match",
        "",
    ),
    (
        "POST",
        "/entries/article/{id}/en/unpublish",
        "publish",
        "if-match",
        "",
    ),
    (
        "POST",
        "/singletons/site_settings/en/publish",
        "publish",
        "if-match",
        "",
    ),
    (
        "POST",
        "/singletons/site_settings/en/unpublish",
        "publish",
        "if-match",
        "",
    ),
    (
        "POST",
        "/batch/publish",
        "publish",
        "",
        r#"{"items":[{"type":"article","id":"{id}","locale":"en","revision":999}]}"#,
    ),
    (
        "POST",
        "/batch/unpublish",
        "publish",
        "",
        r#"{"items":[{"type":"article","id":"{id}","locale":"en"}]}"#,
    ),
    ("GET", "/trash?type=article", "read", "", ""),
    (
        "DELETE",
        "/entries/article/{id}/en",
        "write",
        "if-match",
        "",
    ),
    (
        "DELETE",
        "/entries/article/{id}?purge=true",
        "purge",
        "",
        "",
    ),
];

/// The principal each permission class passes with, and whether
/// `principal` holds `class` on articles and the singleton.
fn holder(class: &str) -> &'static str {
    match class {
        "read" => "reader",
        "write" => "editor",
        "publish" => "publisher",
        "delete" => "deleter",
        _ => "purger",
    }
}
fn holds(principal: Option<&str>, class: &str) -> bool {
    matches!(
        (principal, class),
        (Some("reader"), "read") | (Some("publisher"), "publish") | (Some("deleter"), "delete")
    )
}

#[tokio::test]
async fn every_management_route_checks_its_own_permission() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", MODEL).await;
    let id = id_of(
        &fx.create("article", json!({"fields": {"title": "A"}}))
            .await,
    );
    let settings = fx
        .put("/v1/content/manage/singletons/site_settings/en")
        .header("if-none-match", "*")
        .json(&json!({"fields": {"site_name": "S"}}))
        .send()
        .await;
    assert_eq!(settings.status, StatusCode::CREATED);
    let before = snapshot(&fx.db).await;
    let mut routes: Vec<(&str, &str, &str, &str, &str)> = MANAGEMENT_ROUTES.to_vec();
    // The soft delete and undelete, between the variant delete and purge.
    routes.insert(24, ("DELETE", "/entries/article/{id}", "delete", "", ""));
    routes.insert(
        25,
        ("POST", "/entries/article/{id}/undelete", "delete", "", ""),
    );
    for (method, path, class, condition, body) in &routes {
        let uri = format!("/v1/content/manage{}", path.replace("{id}", &id));
        let body = body.replace("{id}", &id);
        for principal in [
            None,
            Some("reader"),
            Some("publisher"),
            Some("deleter"),
            Some("author_writer"),
        ] {
            if holds(principal, class) {
                continue;
            }
            let mut call = fx
                .call(Method::from_bytes(method.as_bytes()).unwrap(), &uri)
                .by(principal);
            if *method == "PATCH" {
                call = call.header("content-type", "application/merge-patch+json");
            }
            match *condition {
                "if-match" => call = call.if_match(1),
                "if-none-match" => call = call.header("if-none-match", "*"),
                _ => {}
            }
            let reply = call.raw(body.as_bytes()).send().await;
            let expected = match principal {
                None => (StatusCode::UNAUTHORIZED, "unauthenticated"),
                Some(_) => (StatusCode::FORBIDDEN, "unauthorized"),
            };
            assert_eq!(
                (reply.status, reply.code()),
                expected,
                "{principal:?} {method} {uri}"
            );
        }
        assert_eq!(snapshot(&fx.db).await, before, "{method} {path}");
    }
    // Each passes with the principal holding its permission.
    for (method, path, class, condition, body) in &routes {
        let uri = format!("/v1/content/manage{}", path.replace("{id}", &id));
        let body = body.replace("{id}", &id);
        let principal = holder(class);
        let mut call = fx
            .call(Method::from_bytes(method.as_bytes()).unwrap(), &uri)
            .by(Some(principal));
        if *method == "PATCH" {
            call = call.header("content-type", "application/merge-patch+json");
        }
        match *condition {
            "if-match" => call = call.header("if-match", "\"r999\""),
            "if-none-match" => call = call.header("if-none-match", "*"),
            _ => {}
        }
        let reply = call.raw(body.as_bytes()).send().await;
        assert!(
            reply.status.is_success()
                || ["revision_conflict", "default_variant_required"].contains(&reply.code()),
            "{principal} {method} {uri}: {:?}",
            reply.body
        );
    }
}

#[tokio::test]
async fn referrers_never_cross_scope() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", MODEL).await;
    let ada = id_of(
        &fx.create("author", json!({"fields": {"name": "Ada"}}))
            .await,
    );
    let own = id_of(
        &fx.create(
            "article",
            json!({"fields": {"title": "Mine", "author": {"id": ada}}}),
        )
        .await,
    );
    // An organization's entry pointing at the application author.
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
    let reply = fx
        .get(&format!(
            "/v1/content/manage/entries/author/{ada}/referrers"
        ))
        .send()
        .await;
    let ids: Vec<&str> = reply.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec![own.as_str()]);
}
