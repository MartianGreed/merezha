//! CT-C4 publishing over the axum router and a real database: publish,
//! unpublish, older revisions, the fallback materialization of
//! `published`, publish-time validation, batches, the variant delete,
//! generations, fault injection and the outbox records of every change.
mod http;

use axum::http::StatusCode;
use http::*;
use merezha_postgres::TestDatabase;
use serde_json::{Value, json};

async fn article(fx: &Fixture, title: &str) -> String {
    id_of(
        &fx.create("article", json!({"fields": {"title": title}}))
            .await,
    )
}

/// One text column of `sql`'s rows.
async fn xmins(fx: &Fixture, sql: &str) -> Vec<String> {
    strings(&fx.db, sql).await
}

#[tokio::test]
async fn publish_makes_the_revision_visible_in_its_locale_and_every_locale_falling_back_to_it() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    let id = article(&fx, "Hello").await;
    assert!(rows(&fx.db, &id).await.is_empty());
    assert_eq!(generation(&fx.db, "app").await, 0);
    let reply = fx.publish("article", &id, "en", 1).await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    assert_eq!(reply.body["sys"]["status"], "published");
    assert_eq!(reply.body["sys"]["published_revision"], 1);
    assert_eq!(reply.etag(), "\"r1\"");
    assert_eq!(reply.headers["cache-control"], "private, no-store");
    let published_at = reply.body["sys"]["published_at"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        reply.body["sys"]["first_published_at"].as_str(),
        Some(published_at.as_str())
    );
    // Its own locale and both locales whose chain reaches it.
    assert_eq!(
        rows(&fx.db, &id).await,
        ["en:en:1", "fr:en:1", "fr-BE:en:1"]
    );
    assert_eq!(
        strings(
            &fx.db,
            &format!(
                "SELECT data->>'title' || ',' || coalesce(slug, '-') FROM merezha_content.published \
                  WHERE entry_id = '{id}' ORDER BY locale"
            )
        )
        .await,
        ["Hello,hello", "Hello,hello", "Hello,hello"]
    );
    // One event, written by the transaction that wrote the rows.
    let published = events(&fx.db, "content.entry_published").await;
    assert_eq!(published.len(), 1);
    assert_eq!(
        published[0]["payload"],
        json!({"scope": "app", "type": "article", "id": id, "locale": "en", "revision": 1,
               "slug": "hello", "published_at": published_at, "first_publish": true,
               "actor": {"subject": "usr_admin", "kind": "user"}})
    );
    // `now()` is the transaction's start: the rows' `published_at` and
    // the record's `created_at` are equal only if one transaction wrote
    // both (the relay later updates the record, so its xmin cannot tell).
    let row_times = xmins(
        &fx,
        &format!(
            "SELECT DISTINCT published_at::text FROM merezha_content.published \
              WHERE entry_id = '{id}'"
        ),
    )
    .await;
    let event_time = xmins(
        &fx,
        "SELECT created_at::text FROM merezha_jobs.outbox WHERE topic = 'content.entry_published'",
    )
    .await;
    assert_eq!(row_times, event_time);
    assert_eq!(generation(&fx.db, "app").await, 1);
    // A newer draft leaves the published rows alone.
    let saved = fx
        .put(&variant("article", &id, "en"))
        .if_match(1)
        .json(&json!({"fields": {"title": "Hello again"}}))
        .send()
        .await;
    assert_eq!(saved.body["sys"]["status"], "changed");
    assert_eq!(
        rows(&fx.db, &id).await,
        ["en:en:1", "fr:en:1", "fr-BE:en:1"]
    );
    // A published `fr` variant takes over `fr` and `fr-BE`.
    fx.add_variant("article", &id, "fr", json!({"title": "Bonjour"}))
        .await;
    let fr = fx.publish("article", &id, "fr", 1).await;
    assert_eq!(fr.status, StatusCode::OK, "{:?}", fr.body);
    assert_eq!(
        rows(&fx.db, &id).await,
        ["en:en:1", "fr:fr:1", "fr-BE:fr:1"]
    );
    // Republishing the default: the draft revision, no longer the first.
    let again = fx.publish("article", &id, "en", 2).await;
    assert_eq!(again.body["sys"]["status"], "published");
    assert_eq!(
        again.body["sys"]["first_published_at"].as_str(),
        Some(published_at.as_str())
    );
    let published = events(&fx.db, "content.entry_published").await;
    assert_eq!(published.len(), 3);
    assert_eq!(published[2]["payload"]["first_publish"], false);
    assert_eq!(published[2]["payload"]["slug"], "hello");
    assert_eq!(published[1]["payload"]["locale"], "fr");
    assert_eq!(
        rows(&fx.db, &id).await,
        ["en:en:2", "fr:fr:1", "fr-BE:fr:1"]
    );
    assert_eq!(generation(&fx.db, "app").await, 3);
    // The singleton routes publish too.
    let settings = fx
        .put("/v1/content/manage/singletons/site_settings/en")
        .header("if-none-match", "*")
        .json(&json!({"fields": {"site_name": "Site"}}))
        .send()
        .await;
    let settings_id = id_of(&settings);
    let reply = fx
        .post("/v1/content/manage/singletons/site_settings/en/publish")
        .by(Some("admin"))
        .if_match(1)
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    assert_eq!(
        rows(&fx.db, &settings_id).await,
        ["en:en:1", "fr:en:1", "fr-BE:en:1"]
    );
    let reply = fx
        .post("/v1/content/manage/singletons/site_settings/en/unpublish")
        .by(Some("admin"))
        .if_match(1)
        .send()
        .await;
    assert_eq!(reply.body["sys"]["status"], "draft");
    assert!(rows(&fx.db, &settings_id).await.is_empty());
}

#[tokio::test]
async fn unpublishing_fr_makes_fr_be_fall_back_to_en() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    let id = article(&fx, "Hello").await;
    fx.add_variant("article", &id, "fr", json!({"title": "Bonjour"}))
        .await;
    fx.publish("article", &id, "en", 1).await;
    fx.publish("article", &id, "fr", 1).await;
    assert_eq!(
        rows(&fx.db, &id).await,
        ["en:en:1", "fr:fr:1", "fr-BE:fr:1"]
    );
    // Unpublish needs `If-Match` and the draft revision.
    let missing = fx
        .post(&format!("{}/unpublish", variant("article", &id, "fr")))
        .by(Some("admin"))
        .send()
        .await;
    assert_eq!(
        (missing.status, missing.code()),
        (StatusCode::PRECONDITION_REQUIRED, "precondition_required")
    );
    let stale = fx.unpublish("article", &id, "fr", 2).await;
    assert_eq!(stale.code(), "revision_conflict");
    let reply = fx.unpublish("article", &id, "fr", 1).await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    assert_eq!(reply.body["sys"]["status"], "draft");
    assert_eq!(reply.body["sys"]["published_revision"], Value::Null);
    assert_eq!(reply.body["sys"]["published_at"], Value::Null);
    assert_eq!(
        rows(&fx.db, &id).await,
        ["en:en:1", "fr:en:1", "fr-BE:en:1"]
    );
    let unpublished = events(&fx.db, "content.entry_unpublished").await;
    assert_eq!(unpublished.len(), 1);
    assert_eq!(
        unpublished[0]["payload"],
        json!({"scope": "app", "type": "article", "id": id, "locale": "fr",
               "actor": {"subject": "usr_admin", "kind": "user"}, "reason": "unpublish"})
    );
    // The published refs of `fr` went with it.
    assert_eq!(
        scalar(
            &fx.db,
            &format!(
                "SELECT count(*) FROM merezha_content.refs WHERE from_entry = '{id}' \
                  AND stage = 'published' AND from_locale = 'fr'"
            )
        )
        .await,
        0
    );
    // A variant that is not published stays as it is, without an event.
    let again = fx.unpublish("article", &id, "fr", 1).await;
    assert_eq!(again.status, StatusCode::OK);
    assert_eq!(events(&fx.db, "content.entry_unpublished").await.len(), 1);
    // Unpublishing the default leaves nothing to deliver.
    fx.unpublish("article", &id, "en", 1).await;
    assert!(rows(&fx.db, &id).await.is_empty());
    assert_eq!(generation(&fx.db, "app").await, 4);
}

/// Which locale serves `locale` when the variants in `published` are.
fn expected_source(locale: &str, published: &[&str]) -> Option<&'static str> {
    let chain: &[&'static str] = match locale {
        "en" => &["en"],
        "fr" => &["fr", "en"],
        _ => &["fr-BE", "fr", "en"],
    };
    chain.iter().copied().find(|l| published.contains(l))
}

#[tokio::test]
async fn a_fallback_matrix_over_three_locales_and_every_publish_state() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    let ada = id_of(
        &fx.create("author", json!({"fields": {"name": "Ada"}}))
            .await,
    );
    fx.publish("author", &ada, "en", 1).await;
    let locales = ["en", "fr", "fr-BE"];
    for mask in 0..8u8 {
        let id = id_of(
            &fx.create(
                "article",
                json!({"fields": {"title": format!("en {mask}"), "author": {"id": ada}}}),
            )
            .await,
        );
        for locale in ["fr", "fr-BE"] {
            fx.add_variant(
                "article",
                &id,
                locale,
                json!({"title": format!("{locale} {mask}")}),
            )
            .await;
        }
        let published: Vec<&str> = locales
            .iter()
            .enumerate()
            .filter(|(bit, _)| mask & (1 << bit) != 0)
            .map(|(_, l)| *l)
            .collect();
        // Publish in reverse chain order, so later publishes re-materialize
        // rows earlier ones wrote.
        for locale in published.iter().rev() {
            let reply = fx.publish("article", &id, locale, 1).await;
            assert_eq!(
                reply.status,
                StatusCode::OK,
                "{mask} {locale}: {:?}",
                reply.body
            );
        }
        // Locale order is the rows' `ORDER BY locale` too.
        let expected: Vec<String> = locales
            .iter()
            .filter_map(|l| expected_source(l, &published).map(|s| format!("{l}:{s}:1")))
            .collect();
        assert_eq!(rows(&fx.db, &id).await, expected, "mask {mask}");
        // Each row serves its source's own fields; the shared author comes
        // from the default variant, and only once that is published.
        for locale in locales {
            let data: Vec<String> = strings(
                &fx.db,
                &format!(
                    "SELECT data::text FROM merezha_content.published \
                      WHERE entry_id = '{id}' AND locale = '{locale}'"
                ),
            )
            .await;
            let Some(source) = expected_source(locale, &published) else {
                assert!(data.is_empty());
                continue;
            };
            let data: Value = serde_json::from_str(&data[0]).unwrap();
            assert_eq!(data["title"], format!("{source} {mask}"), "{mask} {locale}");
            let shared = published.contains(&"en");
            assert_eq!(
                data.get("author").is_some(),
                shared,
                "{mask} {locale}: {data}"
            );
        }
        // Unpublishing every variant again, in chain order, leaves the
        // rows the remaining ones serve at each step.
        let mut remaining = published.clone();
        for locale in published.iter() {
            fx.unpublish("article", &id, locale, 1).await;
            remaining.retain(|l| l != locale);
            let expected: Vec<String> = locales
                .iter()
                .filter_map(|l| expected_source(l, &remaining).map(|s| format!("{l}:{s}:1")))
                .collect();
            assert_eq!(
                rows(&fx.db, &id).await,
                expected,
                "mask {mask} after {locale}"
            );
        }
    }
    // A non-localized type serves every locale from its default variant.
    let page = id_of(
        &fx.create("page", json!({"fields": {"title": "About"}}))
            .await,
    );
    fx.publish("page", &page, "en", 1).await;
    assert_eq!(
        rows(&fx.db, &page).await,
        ["en:en:1", "fr:en:1", "fr-BE:en:1"]
    );
    let other = fx.publish("page", &page, "fr", 1).await;
    assert_eq!(other.code(), "locale_unknown");
}

#[tokio::test]
async fn publish_enforces_required_and_require_published() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    // Drafts may be incomplete; publishing checks `required`.
    let untitled = id_of(&fx.create("article", json!({"fields": {}})).await);
    let before = snapshot(&fx.db).await;
    let reply = fx.publish("article", &untitled, "en", 1).await;
    assert_eq!(
        (reply.status, reply.code()),
        (StatusCode::UNPROCESSABLE_ENTITY, "validation_failed")
    );
    assert_eq!(
        reply.details(),
        [("/fields/title".to_owned(), "required".to_owned())]
    );
    assert_eq!(snapshot(&fx.db).await, before);
    // A non-default variant checks its own required fields.
    let id = article(&fx, "Hello").await;
    fx.add_variant("article", &id, "fr", json!({})).await;
    let fr = fx.publish("article", &id, "fr", 1).await;
    assert_eq!(
        fr.details(),
        [("/fields/title".to_owned(), "required".to_owned())]
    );
    // `require_published`: the shared author must be published in the
    // locale chain. Unpublished targets do not block other references.
    let ada = id_of(
        &fx.create("author", json!({"fields": {"name": "Ada"}}))
            .await,
    );
    let related = article(&fx, "Draft only").await;
    let saved = fx
        .put(&variant("article", &id, "en"))
        .if_match(1)
        .json(&json!({"fields": {"title": "Hello", "author": {"id": ada},
                                  "related": [{"id": related}]}}))
        .send()
        .await;
    assert_eq!(saved.status, StatusCode::OK, "{:?}", saved.body);
    let before = snapshot(&fx.db).await;
    let refused = fx.publish("article", &id, "en", 2).await;
    assert_eq!(refused.code(), "validation_failed");
    assert_eq!(
        refused.details(),
        [(
            "/fields/author".to_owned(),
            "reference_unpublished".to_owned()
        )]
    );
    assert_eq!(snapshot(&fx.db).await, before);
    fx.publish("author", &ada, "en", 1).await;
    let reply = fx.publish("article", &id, "en", 2).await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    // The published refs are the published revision's, both references.
    assert_eq!(
        strings(
            &fx.db,
            &format!(
                "SELECT path FROM merezha_content.refs WHERE from_entry = '{id}' \
                  AND stage = 'published' ORDER BY path"
            )
        )
        .await,
        ["/fields/author", "/fields/related/0"]
    );
    // A reference to a trashed entry fails like at save time.
    fx.call(
        axum::http::Method::DELETE,
        &format!("/v1/content/manage/entries/article/{related}"),
    )
    .by(Some("admin"))
    .send()
    .await;
    let republish = fx.publish("article", &id, "en", 2).await;
    assert_eq!(
        republish.details(),
        [(
            "/fields/related/0".to_owned(),
            "reference_not_found".to_owned()
        )]
    );
    // The fr variant now delivers the shared author with its own title.
    fx.put(&variant("article", &id, "fr"))
        .if_match(1)
        .json(&json!({"fields": {"title": "Bonjour"}}))
        .send()
        .await;
    let fr = fx.publish("article", &id, "fr", 2).await;
    assert_eq!(fr.status, StatusCode::OK, "{:?}", fr.body);
    let data: Value = serde_json::from_str(
        &strings(
            &fx.db,
            &format!(
                "SELECT data::text FROM merezha_content.published \
                  WHERE entry_id = '{id}' AND locale = 'fr-BE'"
            ),
        )
        .await[0],
    )
    .unwrap();
    assert_eq!(data["title"], "Bonjour");
    assert_eq!(data["author"]["id"], ada);
}

#[tokio::test]
async fn an_older_revision_publishes_without_touching_the_draft() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    let id = article(&fx, "One").await;
    for (revision, title) in [(1, "Two"), (2, "Three")] {
        fx.put(&variant("article", &id, "en"))
            .if_match(revision)
            .json(&json!({"fields": {"title": title}}))
            .send()
            .await;
    }
    let path = format!("{}/publish", variant("article", &id, "en"));
    let reply = fx
        .post(&path)
        .by(Some("admin"))
        .if_match(3)
        .json(&json!({"revision": 1}))
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    assert_eq!(reply.body["sys"]["status"], "changed");
    assert_eq!(reply.body["sys"]["draft_revision"], 3);
    assert_eq!(reply.body["sys"]["published_revision"], 1);
    assert_eq!(reply.body["fields"]["title"], "Three");
    assert_eq!(
        rows(&fx.db, &id).await,
        ["en:en:1", "fr:en:1", "fr-BE:en:1"]
    );
    assert_eq!(
        strings(
            &fx.db,
            &format!(
                "SELECT data->>'title' FROM merezha_content.published \
                  WHERE entry_id = '{id}' AND locale = 'en'"
            )
        )
        .await,
        ["One"]
    );
    assert_eq!(
        events(&fx.db, "content.entry_published").await[0]["payload"]["revision"],
        1
    );
    // The draft is untouched: still revision 3, no new revision.
    assert_eq!(
        scalar(
            &fx.db,
            &format!("SELECT count(*) FROM merezha_content.revisions WHERE entry_id = '{id}'")
        )
        .await,
        3
    );
    // The history marks the published revision.
    let history = fx
        .get(&format!("{}/revisions", variant("article", &id, "en")))
        .send()
        .await;
    let marks: Vec<(i64, bool)> = history.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| {
            (
                i["revision"].as_i64().unwrap(),
                i["published"].as_bool().unwrap(),
            )
        })
        .collect();
    assert_eq!(marks, [(3, false), (2, false), (1, true)]);
    // Rolling forward again, then the refusals.
    let reply = fx
        .post(&path)
        .by(Some("admin"))
        .if_match(3)
        .json(&json!({"revision": 2}))
        .send()
        .await;
    assert_eq!(reply.body["sys"]["published_revision"], 2);
    let before = snapshot(&fx.db).await;
    for (if_match, body, code) in [
        (Some(3), json!({"revision": 4}), "revision_not_found"),
        (Some(3), json!({"revision": 0}), "validation_failed"),
        (Some(3), json!({"revision": "1"}), "validation_failed"),
        (Some(3), json!({"other": 1}), "invalid_json"),
        (Some(2), json!({"revision": 1}), "revision_conflict"),
        (None, json!({"revision": 1}), "precondition_required"),
    ] {
        let mut call = fx.post(&path).by(Some("admin")).json(&body);
        if let Some(n) = if_match {
            call = call.if_match(n);
        }
        let reply = call.send().await;
        assert_eq!(reply.code(), code, "{body}");
    }
    // A weak or listed tag never matches.
    for tag in ["W/\"r3\"", "\"r3\", \"r2\"", "*"] {
        let reply = fx
            .post(&path)
            .by(Some("admin"))
            .header("if-match", tag)
            .send()
            .await;
        assert_eq!(reply.code(), "revision_conflict", "{tag}");
    }
    assert_eq!(snapshot(&fx.db).await, before);
}

#[tokio::test]
async fn one_failing_batch_item_rolls_back_the_batch() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    let a = article(&fx, "A").await;
    let b = article(&fx, "B").await;
    let untitled = id_of(&fx.create("article", json!({"fields": {}})).await);
    let before = snapshot(&fx.db).await;
    let batch = |items: Value| {
        fx.post("/v1/content/manage/batch/publish")
            .by(Some("admin"))
            .json(&json!({ "items": items }))
            .send()
    };
    let item = |id: &str, revision: i64| json!({"type": "article", "id": id, "locale": "en", "revision": revision});
    for (items, status, code, details) in [
        (
            json!([item(&a, 1), item(&b, 1), item(&untitled, 2)]),
            StatusCode::PRECONDITION_FAILED,
            "revision_conflict",
            vec![("/items/2", "revision_conflict")],
        ),
        (
            json!([item(&a, 1), item(&untitled, 1), item(&b, 1)]),
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_failed",
            vec![("/items/1/fields/title", "required")],
        ),
        (
            json!([item(&a, 1), {"type": "article", "id": "0192f7a0-5d1c-7c3e-9b1a-6f0e2c4d8a11", "locale": "en", "revision": 1}]),
            StatusCode::NOT_FOUND,
            "entry_not_found",
            vec![("/items/1", "entry_not_found")],
        ),
        (
            json!([item(&a, 1), item(&a, 1)]),
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_failed",
            vec![("/items/1", "duplicate_item")],
        ),
        (
            json!([item(&a, 1), {"type": "article", "id": a, "locale": "de", "revision": 1}]),
            StatusCode::BAD_REQUEST,
            "locale_unknown",
            vec![("/items/1", "locale_unknown")],
        ),
        (
            json!([{"type": "article", "id": a, "locale": "en"}]),
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_failed",
            vec![("/items/0/revision", "required")],
        ),
        (
            json!([]),
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_failed",
            vec![("/items", "min_items")],
        ),
    ] {
        let reply = batch(items.clone()).await;
        let found = reply.details();
        let found: Vec<(&str, &str)> = found
            .iter()
            .map(|(p, c)| (p.as_str(), c.as_str()))
            .collect();
        assert_eq!(
            (reply.status, reply.code(), found),
            (status, code, details),
            "{items}"
        );
        assert_eq!(snapshot(&fx.db).await, before, "{items}");
    }
    // `limits.max_batch` bounds the items.
    let small = start(&test, "limits = { max_batch = 2 }", CHAIN_MODEL).await;
    let reply = small
        .post("/v1/content/manage/batch/publish")
        .by(Some("admin"))
        .json(&json!({"items": [item(&a, 1), item(&b, 1), item(&untitled, 1)]}))
        .send()
        .await;
    assert_eq!(
        reply.details(),
        [("/items".to_owned(), "max_items".to_owned())]
    );
    // Every item is authorized: a publisher of articles only cannot
    // publish an author in the same batch.
    let ada = id_of(
        &fx.create("author", json!({"fields": {"name": "Ada"}}))
            .await,
    );
    let before = snapshot(&fx.db).await;
    let refused = fx
        .post("/v1/content/manage/batch/publish")
        .by(Some("article_publisher"))
        .json(&json!({"items": [item(&a, 1),
                                {"type": "author", "id": ada, "locale": "en", "revision": 1}]}))
        .send()
        .await;
    assert_eq!(
        (refused.status, refused.code()),
        (StatusCode::FORBIDDEN, "unauthorized")
    );
    assert_eq!(snapshot(&fx.db).await, before);
    // A target and its `require_published` referrer in one batch, the
    // referrer first.
    let saved = fx
        .put(&variant("article", &b, "en"))
        .if_match(1)
        .json(&json!({"fields": {"title": "B", "author": {"id": ada}}}))
        .send()
        .await;
    assert_eq!(saved.status, StatusCode::OK);
    let reply = batch(json!([
        item(&b, 2),
        item(&a, 1),
        {"type": "author", "id": ada, "locale": "en", "revision": 1}
    ]))
    .await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    let ids: Vec<&str> = reply.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["sys"]["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, [b.as_str(), a.as_str(), ada.as_str()]);
    assert_eq!(reply.body["items"][0]["sys"]["status"], "published");
    assert_eq!(events(&fx.db, "content.entry_published").await.len(), 3);
    assert_eq!(generation(&fx.db, "app").await, 1);
    // Now that the author is published, the referrer republishes alone.
    let lone = batch(json!([item(&a, 1), item(&b, 2)])).await;
    assert_eq!(lone.status, StatusCode::OK, "{:?}", lone.body);
    // Batch unpublish: all or none too, without revisions.
    let before = snapshot(&fx.db).await;
    let unpublish = |items: Value| {
        fx.post("/v1/content/manage/batch/unpublish")
            .by(Some("admin"))
            .json(&json!({ "items": items }))
            .send()
    };
    let refused = unpublish(json!([
        {"type": "article", "id": a, "locale": "en"},
        {"type": "article", "id": untitled, "locale": "fr"}
    ]))
    .await;
    assert_eq!(
        (refused.code(), refused.details()),
        (
            "entry_not_found",
            vec![("/items/1".to_owned(), "entry_not_found".to_owned())]
        )
    );
    let with_revision = unpublish(json!([
        {"type": "article", "id": a, "locale": "en", "revision": 1}
    ]))
    .await;
    assert_eq!(
        with_revision.details(),
        [("/items/0".to_owned(), "unknown_field".to_owned())]
    );
    assert_eq!(snapshot(&fx.db).await, before);
    let reply = unpublish(json!([
        {"type": "article", "id": a, "locale": "en"},
        {"type": "article", "id": b, "locale": "en"}
    ]))
    .await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    assert!(rows(&fx.db, &a).await.is_empty());
    assert!(rows(&fx.db, &b).await.is_empty());
    assert_eq!(events(&fx.db, "content.entry_unpublished").await.len(), 2);
}

#[tokio::test]
async fn the_variant_delete_removes_a_non_default_variant_with_its_history() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    let id = article(&fx, "Hello").await;
    let other = article(&fx, "Other").await;
    fx.add_variant(
        "article",
        &id,
        "fr",
        json!({"title": "Bonjour", "related": [{"id": other}]}),
    )
    .await;
    fx.add_variant("article", &id, "fr-BE", json!({"title": "Salut"}))
        .await;
    fx.publish("article", &id, "en", 1).await;
    fx.publish("article", &id, "fr", 1).await;
    let path = variant("article", &id, "fr");
    let delete = |path: String| fx.call(axum::http::Method::DELETE, &path);
    let before = snapshot(&fx.db).await;
    let missing = delete(path.clone()).send().await;
    assert_eq!(missing.code(), "precondition_required");
    let stale = delete(path.clone()).if_match(2).send().await;
    assert_eq!(stale.code(), "revision_conflict");
    let default = delete(variant("article", &id, "en"))
        .if_match(1)
        .send()
        .await;
    assert_eq!(
        (default.status, default.code()),
        (StatusCode::CONFLICT, "default_variant_required")
    );
    assert_eq!(snapshot(&fx.db).await, before);
    let reply = delete(path.clone()).if_match(1).send().await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT, "{:?}", reply.body);
    assert_eq!(reply.headers["cache-control"], "private, no-store");
    // `fr` and `fr-BE` (unpublished) now fall back to `en`.
    assert_eq!(
        rows(&fx.db, &id).await,
        ["en:en:1", "fr:en:1", "fr-BE:en:1"]
    );
    for table in ["revisions", "refs"] {
        let column = if table == "refs" {
            "from_locale"
        } else {
            "locale"
        };
        let entry = if table == "refs" {
            "from_entry"
        } else {
            "entry_id"
        };
        assert_eq!(
            scalar(
                &fx.db,
                &format!(
                    "SELECT count(*) FROM merezha_content.{table} \
                      WHERE {entry} = '{id}' AND {column} = 'fr'"
                )
            )
            .await,
            0,
            "{table}"
        );
    }
    assert_eq!(fx.get(&path).send().await.code(), "entry_not_found");
    let unpublished = events(&fx.db, "content.entry_unpublished").await;
    assert_eq!(unpublished.len(), 1);
    assert_eq!(unpublished[0]["payload"]["reason"], "delete");
    assert_eq!(unpublished[0]["payload"]["locale"], "fr");
    // A draft-only variant goes without an event or a generation bump.
    let generation_before = generation(&fx.db, "app").await;
    let reply = delete(variant("article", &id, "fr-BE"))
        .if_match(1)
        .send()
        .await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT);
    assert_eq!(events(&fx.db, "content.entry_unpublished").await.len(), 1);
    assert_eq!(generation(&fx.db, "app").await, generation_before);
    // The locale can be created again from scratch.
    let again = fx
        .add_variant("article", &id, "fr", json!({"title": "Encore"}))
        .await;
    assert_eq!(again.body["sys"]["draft_revision"], 1);
}

#[tokio::test]
async fn a_unique_value_published_elsewhere_fails_the_publish_with_its_field() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    let first = id_of(
        &fx.create(
            "article",
            json!({"fields": {"title": "Same", "code": "x1"}}),
        )
        .await,
    );
    fx.publish("article", &first, "en", 1).await;
    // The draft moves on, which frees the values among drafts only.
    fx.put(&variant("article", &first, "en"))
        .if_match(1)
        .json(&json!({"fields": {"title": "Moved", "slug": "moved", "code": "x2"}}))
        .send()
        .await;
    let second = id_of(
        &fx.create(
            "article",
            json!({"fields": {"title": "Same", "code": "x1"}}),
        )
        .await,
    );
    let before = snapshot(&fx.db).await;
    let reply = fx.publish("article", &second, "en", 1).await;
    assert_eq!(reply.code(), "validation_failed", "{:?}", reply.body);
    let details = reply.details();
    assert_eq!(details.len(), 1);
    assert!(
        details[0] == ("/fields/slug".to_owned(), "unique".to_owned())
            || details[0] == ("/fields/code".to_owned(), "unique".to_owned()),
        "{details:?}"
    );
    assert_eq!(snapshot(&fx.db).await, before);
    // Once the first publishes its new values, the second can publish.
    fx.publish("article", &first, "en", 2).await;
    let reply = fx.publish("article", &second, "en", 1).await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
}

/// A deferred trigger that fails every commit touching `generations`,
/// which every C4 change does after materializing.
async fn fail_commits(fx: &Fixture) {
    execute(
        &fx.db,
        "CREATE FUNCTION public.fail_commit() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'injected'; END $$;
         CREATE CONSTRAINT TRIGGER fail_commit AFTER INSERT OR UPDATE
           ON merezha_content.generations DEFERRABLE INITIALLY DEFERRED
           FOR EACH ROW EXECUTE FUNCTION public.fail_commit();",
    )
    .await;
}

#[tokio::test]
async fn fault_injection_between_materialization_and_commit_leaves_no_rows_and_no_outbox_record() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    let id = article(&fx, "Hello").await;
    let kept = article(&fx, "Kept").await;
    fx.add_variant("article", &kept, "fr", json!({"title": "Gardé"}))
        .await;
    fx.publish("article", &kept, "en", 1).await;
    fx.publish("article", &kept, "fr", 1).await;
    // The rows are written before the commit fails: the trigger runs at
    // commit, after the materialization and the outbox append.
    fail_commits(&fx).await;
    let before = snapshot(&fx.db).await;
    let admin = |method: axum::http::Method, path: String| fx.call(method, &path).by(Some("admin"));
    let replies = [
        fx.publish("article", &id, "en", 1).await,
        fx.unpublish("article", &kept, "fr", 1).await,
        admin(axum::http::Method::DELETE, variant("article", &kept, "fr"))
            .if_match(1)
            .send()
            .await,
        admin(
            axum::http::Method::POST,
            "/v1/content/manage/batch/publish".to_owned(),
        )
        .json(&json!({"items": [{"type": "article", "id": id, "locale": "en", "revision": 1}]}))
        .send()
        .await,
        admin(
            axum::http::Method::DELETE,
            format!("/v1/content/manage/entries/article/{kept}"),
        )
        .send()
        .await,
        admin(
            axum::http::Method::DELETE,
            format!("/v1/content/manage/entries/article/{kept}?purge=true"),
        )
        .send()
        .await,
    ];
    for reply in replies {
        assert!(!reply.status.is_success(), "{:?}", reply.body);
    }
    assert_eq!(snapshot(&fx.db).await, before);
    assert!(rows(&fx.db, &id).await.is_empty());
    assert_eq!(
        rows(&fx.db, &kept).await,
        ["en:en:1", "fr:fr:1", "fr-BE:fr:1"]
    );
    assert_eq!(events(&fx.db, "content.entry_published").await.len(), 2);
}

#[tokio::test]
async fn each_event_lands_as_an_event_record_with_the_changes_dedupe_key() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    let id = article(&fx, "Hello").await;
    fx.add_variant("article", &id, "fr", json!({"title": "Bonjour"}))
        .await;
    fx.publish("article", &id, "en", 1).await;
    fx.publish("article", &id, "fr", 1).await;
    fx.unpublish("article", &id, "fr", 1).await;
    let admin = |method: axum::http::Method, path: String| fx.call(method, &path).by(Some("admin"));
    admin(
        axum::http::Method::DELETE,
        format!("/v1/content/manage/entries/article/{id}"),
    )
    .send()
    .await;
    admin(
        axum::http::Method::POST,
        format!("/v1/content/manage/entries/article/{id}/undelete"),
    )
    .send()
    .await;
    admin(
        axum::http::Method::DELETE,
        format!("/v1/content/manage/entries/article/{id}?purge=true"),
    )
    .send()
    .await;
    let records = strings(
        &fx.db,
        "SELECT record_kind || '|' || coalesce(topic, '') || '|' || dedupe_key || '|' || \
                (queue IS NULL AND job_kind IS NULL)::text || '|' || payload::text \
           FROM merezha_jobs.outbox WHERE source = 'content' \
            AND topic NOT IN ('content.model_activated', 'content.entry_draft_saved') ORDER BY id",
    )
    .await;
    let mut topics = Vec::new();
    let mut keys = std::collections::HashSet::new();
    for record in &records {
        let parts: Vec<&str> = record.splitn(5, '|').collect();
        assert_eq!(parts[0], "event");
        assert_eq!(parts[3], "true");
        let key = uuid::Uuid::parse_str(parts[2]).unwrap();
        assert_eq!(key.get_version_num(), 7, "{record}");
        assert!(keys.insert(key), "one key per event: {record}");
        let envelope: Value = serde_json::from_str(parts[4]).unwrap();
        assert_eq!(envelope["event_id"], format!("content:{}", parts[2]));
        assert_eq!(envelope["type"], parts[1]);
        assert_eq!(envelope["version"], 1);
        assert_eq!(envelope["source"], "content");
        assert_eq!(envelope["payload"]["id"], id);
        topics.push(parts[1].to_owned());
    }
    assert_eq!(
        topics,
        [
            "content.entry_published",
            "content.entry_published",
            "content.entry_unpublished",
            "content.entry_unpublished",
            "content.entry_deleted",
            "content.entry_undeleted",
            // A purge of a live entry deletes it first.
            "content.entry_deleted",
            "content.entry_purged",
        ]
    );
}

#[tokio::test]
async fn event_counts_per_operation() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    let id = article(&fx, "Hello").await;
    let other = article(&fx, "Other").await;
    fx.add_variant("article", &id, "fr", json!({"title": "Bonjour"}))
        .await;
    fx.add_variant("article", &id, "fr-BE", json!({"title": "Salut"}))
        .await;
    let admin = |method: axum::http::Method, path: String| fx.call(method, &path).by(Some("admin"));
    let counts =
        |published: usize, unpublished: usize, deleted: usize, undeleted: usize, purged: usize| {
            let mut out = vec!["content.entry_draft_saved=4".to_owned()];
            for (topic, n) in [
                ("content.entry_deleted", deleted),
                ("content.entry_published", published),
                ("content.entry_purged", purged),
                ("content.entry_undeleted", undeleted),
                ("content.entry_unpublished", unpublished),
            ] {
                if n > 0 {
                    out.push(format!("{topic}={n}"));
                }
            }
            out.sort();
            out
        };
    // Publish: one per variant; a repeat is one more.
    fx.publish("article", &id, "en", 1).await;
    fx.publish("article", &id, "fr", 1).await;
    fx.publish("article", &id, "en", 1).await;
    assert_eq!(event_counts(&fx.db).await, counts(3, 0, 0, 0, 0));
    // Unpublishing a draft-only variant appends nothing.
    fx.unpublish("article", &id, "fr-BE", 1).await;
    assert_eq!(event_counts(&fx.db).await, counts(3, 0, 0, 0, 0));
    // Batch: one per item.
    let batch = admin(
        axum::http::Method::POST,
        "/v1/content/manage/batch/publish".to_owned(),
    )
    .json(&json!({"items": [
        {"type": "article", "id": other, "locale": "en", "revision": 1},
        {"type": "article", "id": id, "locale": "fr-BE", "revision": 1}
    ]}))
    .send()
    .await;
    assert_eq!(batch.status, StatusCode::OK, "{:?}", batch.body);
    assert_eq!(event_counts(&fx.db).await, counts(5, 0, 0, 0, 0));
    // Deleting the published fr-BE variant: one unpublished.
    admin(axum::http::Method::DELETE, variant("article", &id, "fr-BE"))
        .if_match(1)
        .send()
        .await;
    assert_eq!(event_counts(&fx.db).await, counts(5, 1, 0, 0, 0));
    // Delete with en and fr live: two unpublished, one deleted.
    admin(
        axum::http::Method::DELETE,
        format!("/v1/content/manage/entries/article/{id}"),
    )
    .send()
    .await;
    assert_eq!(event_counts(&fx.db).await, counts(5, 3, 1, 0, 0));
    // Undelete: one. Undeleting a live entry: none.
    for _ in 0..2 {
        admin(
            axum::http::Method::POST,
            format!("/v1/content/manage/entries/article/{id}/undelete"),
        )
        .send()
        .await;
    }
    assert_eq!(event_counts(&fx.db).await, counts(5, 3, 1, 1, 0));
    // Purge of a live entry (its drafts only): deleted and purged.
    admin(
        axum::http::Method::DELETE,
        format!("/v1/content/manage/entries/article/{id}?purge=true"),
    )
    .send()
    .await;
    assert_eq!(event_counts(&fx.db).await, counts(5, 3, 2, 1, 1));
    // Purge of the published `other`: one unpublished, deleted, purged.
    admin(
        axum::http::Method::DELETE,
        format!("/v1/content/manage/entries/article/{other}?purge=true"),
    )
    .send()
    .await;
    assert_eq!(event_counts(&fx.db).await, counts(5, 4, 3, 1, 2));
}
