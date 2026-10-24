//! CT-C4 and model activation (section 2): a new locale or a changed
//! fallback rebuilds the `published` fallback rows in batches, readiness
//! stays false while a rebuild is pending, and the next migrating start
//! finishes one a crashed run left.
mod http;

use axum::http::StatusCode;
use http::*;
use merezha_postgres::TestDatabase;
use serde_json::json;
use std::sync::Arc;

/// `CHAIN_MODEL` as version 2: `fr-BE` now falls back to `en`, and `de`
/// (falling back to `en`) is new.
fn v2() -> String {
    CHAIN_MODEL.replace("version = 1", "version = 2").replace(
        "{ code = \"fr-BE\", fallback = \"fr\" },",
        "{ code = \"fr-BE\", fallback = \"en\" },\n  { code = \"de\", fallback = \"en\" },",
    )
}

/// `count` published `page` entries straight through SQL, each with its
/// `en` variant, revision and `en` row only (as v1 materialized a
/// subset), plus one localized article published in `en` and `fr`.
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

#[tokio::test]
async fn a_new_locale_or_a_changed_fallback_rebuilds_the_published_rows_at_activation() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let old = start(&test, "", CHAIN_MODEL).await;
    let id = id_of(
        &old.create("article", json!({"fields": {"title": "Hello"}}))
            .await,
    );
    old.add_variant("article", &id, "fr", json!({"title": "Bonjour"}))
        .await;
    old.publish("article", &id, "en", 1).await;
    old.publish("article", &id, "fr", 1).await;
    assert_eq!(
        rows(&old.db, &id).await,
        ["en:en:1", "fr:fr:1", "fr-BE:fr:1"]
    );
    // More than two batches of entries with published variants.
    seed_pages(&old.db, 1100).await;
    let generation_before = generation(&old.db, "app").await;
    let new = start(&test, "", &v2()).await;
    assert_eq!(
        rows(&new.db, &id).await,
        ["de:en:1", "en:en:1", "fr:fr:1", "fr-BE:en:1"]
    );
    assert_eq!(
        scalar(
            &new.db,
            "SELECT count(*) FROM merezha_content.published WHERE content_type = 'page'"
        )
        .await,
        1100 * 4
    );
    assert_eq!(
        strings(
            &new.db,
            "SELECT version || ':' || state FROM merezha_content.rebuilds"
        )
        .await,
        ["2:done"]
    );
    // Activation bumps once, then every rebuild batch (three) once more.
    assert_eq!(generation(&new.db, "app").await, generation_before + 4);
    // No event of its own: fallback rows emit none.
    assert_eq!(events(&new.db, "content.entry_published").await.len(), 2);
    assert!(eventually_ready(&new.host).await);
    // A restart with the same model has nothing to rebuild.
    let again = start(&test, "", &v2()).await;
    assert_eq!(generation(&again.db, "app").await, generation_before + 4);
    // A model change without a locale change records no rebuild.
    let v3 = v2().replace("version = 2", "version = 3").replace(
        "{ name = \"title\", type = \"string\", required = true }]",
        "{ name = \"title\", type = \"string\", required = true, max_length = 100 }]",
    );
    start(&test, "", &v3).await;
    assert_eq!(
        scalar(&new.db, "SELECT count(*) FROM merezha_content.rebuilds").await,
        1
    );
}

#[tokio::test]
async fn a_pending_rebuild_keeps_readiness_false_and_the_next_migrating_start_finishes_it() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let old = start(&test, "", CHAIN_MODEL).await;
    seed_pages(&old.db, 30).await;
    let new = start(&test, "", &v2()).await;
    assert_eq!(
        scalar(&new.db, "SELECT count(*) FROM merezha_content.published").await,
        120
    );
    // As if the run crashed after its first batches: rows for `de` gone,
    // the rebuild pending with a cursor past some of the entries.
    execute(
        &new.db,
        "DELETE FROM merezha_content.published WHERE locale = 'de';
         UPDATE merezha_content.rebuilds SET state = 'pending',
                after_entry = (SELECT id FROM merezha_content.entries ORDER BY id OFFSET 9 LIMIT 1)",
    )
    .await;
    // A host that does not migrate stays not ready, and changes nothing.
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), false, "", &v2());
    let waiting = start_in(dir, &test, Arc::new(FixturePolicy)).await;
    assert_eq!(
        waiting
            .handle
            .state()
            .unwrap()
            .readiness()
            .await
            .unwrap_err()
            .code,
        "content_rebuild_pending"
    );
    assert!(!waiting.host.readiness().check().await);
    assert_eq!(
        scalar(
            &new.db,
            "SELECT count(*) FROM merezha_content.published WHERE locale = 'de'"
        )
        .await,
        0
    );
    // The next migrating start resumes after the cursor.
    let resumed = start(&test, "", &v2()).await;
    assert_eq!(
        scalar(
            &resumed.db,
            "SELECT count(*) FROM merezha_content.published WHERE locale = 'de'"
        )
        .await,
        20
    );
    assert_eq!(
        strings(&resumed.db, "SELECT state FROM merezha_content.rebuilds").await,
        ["done"]
    );
    assert!(eventually_ready(&resumed.host).await);
    assert!(waiting.handle.state().unwrap().readiness().await.is_ok());
    // Writes work on the rebuilt model.
    let page = id_of(
        &resumed
            .create("page", json!({"fields": {"title": "New"}}))
            .await,
    );
    let reply = resumed.publish("page", &page, "en", 1).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(
        rows(&resumed.db, &page).await,
        ["de:en:1", "en:en:1", "fr:en:1", "fr-BE:en:1"]
    );
}
