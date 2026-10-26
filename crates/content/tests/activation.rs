//! CT-C2 against real PostgreSQL: migration 1, activation, diff
//! classification at startup, derived indexes and the version check.
mod pg;

use merezha_content::{
    ACTIVATION_LOCK, ChangeClass, MIGRATION_1, Status, Verified, derived_indexes,
    lock_model_version, verify,
};
use merezha_postgres::TestDatabase;
use pg::*;
use serde_json::json;
use std::time::Duration;

#[test]
fn migration_1_is_the_design_sql() {
    let design = include_str!("../../../docs/design/content.md");
    let start = design
        .find("### Migration 1: `merezha_content`")
        .expect("section");
    let block = &design[start..];
    let sql = &block[block.find("```sql\n").unwrap() + 7..];
    let sql = &sql[..sql.find("```").unwrap()];
    assert_eq!(MIGRATION_1, sql);
}

#[tokio::test]
async fn migration_1_applies_and_leaves_the_search_path_alone() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let tables = strings(
        &db,
        "SELECT tablename::text FROM pg_tables WHERE schemaname = 'merezha_content' ORDER BY 1",
    )
    .await;
    assert_eq!(
        tables,
        [
            "assets",
            "blobs",
            "entries",
            "generations",
            "locales",
            "models",
            "published",
            "rebuilds",
            "refs",
            "revisions",
            "scopes_deleted",
            "variants"
        ]
    );
    let path = strings(&db, "SHOW search_path").await;
    assert_eq!(path, ["\"$user\", public"]);
}

#[tokio::test]
async fn a_fresh_database_activates_version_1() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let v1 = model(V1);
    let report = run(&db, &v1).await.unwrap();
    assert_eq!(report.plan.status, Status::Fresh);
    assert!(report.activated);
    assert_eq!(models(&db).await, [(1, "active".to_owned())]);
    let client = db.acquire(soon()).await.unwrap();
    let row = client
        .query_one(
            "SELECT fingerprint, definition = $1::text::jsonb FROM merezha_content.models",
            &[&v1.canonical_json()],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), v1.fingerprint());
    assert!(row.get::<_, bool>(1), "definition is the canonical form");
    let locales = client
        .query(
            "SELECT code, fallback, is_default FROM merezha_content.locales ORDER BY code",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| {
            (
                r.get::<_, String>(0),
                r.get::<_, Option<String>>(1),
                r.get::<_, bool>(2),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        locales,
        [
            ("en".to_owned(), None, true),
            ("fr".to_owned(), Some("en".to_owned()), false)
        ]
    );
    let events = activation_events(&db).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["type"], "content.model_activated");
    assert_eq!(events[0]["source"], "content");
    assert_eq!(
        events[0]["payload"],
        json!({"version": 1, "fingerprint": v1.fingerprint(), "previous_version": null,
               "change_classes": ["additive"]})
    );
    // Sortable `reading_minutes` (one index per direction) and unique
    // `slug` and `legacy_code`, on both tables.
    let expected = derived_indexes(&v1);
    assert_eq!(expected.len(), 8);
    let existing = indexes(&db).await;
    for index in &expected {
        assert_eq!(existing.get(&index.name), Some(&true), "{}", index.name);
    }
    assert_eq!(report.created.len(), 8);
    // A second run on the same model changes nothing.
    let again = run(&db, &v1).await.unwrap();
    assert_eq!(again.plan.status, Status::Current);
    assert!(!again.activated && again.created.is_empty() && again.dropped.is_empty());
    assert_eq!(activation_events(&db).await.len(), 1);
}

/// v2 of [`V1`] with one change of each activating class.
const V2: &str = r#"
version = 2
locales = [{ code = "en", default = true }, { code = "fr" }, { code = "fr-BE", fallback = "fr" }]

[components.seo]
fields = [{ name = "title", type = "string", max_length = 60 }]

[types.author]
access = "public"
fields = [{ name = "name", type = "string", required = true }]

[types.article]
access = { permission = "content.delivery.read" }
title_field = "title"
fields = [
  { name = "title", type = "string", required = true, max_length = 10 },
  { name = "slug", type = "slug" },
  { name = "category", type = "enum", values = ["news", "guide", "release"], filterable = true, sortable = true },
  { name = "reading_minutes", type = "integer", min = 1, max = 900 },
  { name = "legacy_code", type = "string" },
  { name = "seo", type = "component", component = "seo" },
  { name = "summary", type = "text" },
  { name = "isbn", type = "string", unique = true, required = true },
]
"#;

#[tokio::test]
async fn additive_tightening_access_and_index_changes_activate() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let (v1, v2) = (model(V1), model(V2));
    run(&db, &v1).await.unwrap();
    // Stored rows: one draft and one published row whose title the new
    // `max_length = 10` rejects, and one that still passes.
    seed(
        &db,
        "article",
        "en",
        1,
        r#"{"title": "A title far longer than ten"}"#,
        true,
    )
    .await;
    seed(&db, "article", "en", 1, r#"{"title": "Short"}"#, false).await;
    execute(
        &db,
        "INSERT INTO merezha_content.generations VALUES ('app', 7)",
    )
    .await;
    let before = indexes(&db).await;
    let report = run(&db, &v2).await.unwrap();
    assert_eq!(report.plan.status, Status::Pending);
    assert!(report.activated);
    let found: Vec<(ChangeClass, &str, &str)> = report
        .plan
        .changes
        .iter()
        .map(|c| (c.class, c.path.as_str(), c.code))
        .collect();
    for expected in [
        (ChangeClass::Additive, "locales.fr-BE", "locale_added"),
        (ChangeClass::Additive, "types.author", "type_added"),
        (
            ChangeClass::Additive,
            "types.article.fields.summary",
            "field_added",
        ),
        (
            ChangeClass::Additive,
            "types.article.fields.isbn",
            "field_added",
        ),
        (
            ChangeClass::Additive,
            "types.article.fields.category.values",
            "rule_relaxed",
        ),
        (
            ChangeClass::Additive,
            "types.article.fields.reading_minutes.max",
            "rule_relaxed",
        ),
        (
            ChangeClass::Tightening,
            "types.article.fields.title.max_length",
            "rule_tightened",
        ),
        (
            ChangeClass::Tightening,
            "components.seo.fields.title.max_length",
            "rule_tightened",
        ),
        (
            ChangeClass::Access,
            "types.article.access",
            "access_changed",
        ),
        (
            ChangeClass::Index,
            "types.article.fields.category.sortable",
            "sortable_changed",
        ),
        (
            ChangeClass::Index,
            "types.article.fields.reading_minutes.sortable",
            "sortable_changed",
        ),
        (
            ChangeClass::Index,
            "types.article.fields.legacy_code.unique",
            "unique_removed",
        ),
    ] {
        assert!(found.contains(&expected), "{expected:?} not in {found:?}");
    }
    assert!(
        found
            .iter()
            .all(|(class, ..)| *class != ChangeClass::Refused)
    );
    assert_eq!(
        models(&db).await,
        [(1, "superseded".to_owned()), (2, "active".to_owned())]
    );
    let events = activation_events(&db).await;
    assert_eq!(events.len(), 2);
    assert_eq!(
        events[1]["payload"],
        json!({"version": 2, "fingerprint": v2.fingerprint(), "previous_version": 1,
               "change_classes": ["additive", "tightening", "access", "index"]})
    );
    assert_eq!(
        strings(
            &db,
            "SELECT code || ':' || coalesce(fallback, '-') FROM merezha_content.locales ORDER BY code"
        )
        .await,
        ["en:-", "fr:en", "fr-BE:fr"]
    );
    assert_eq!(
        scalar_i64(
            &db,
            "SELECT generation FROM merezha_content.generations WHERE scope = 'app'"
        )
        .await,
        8
    );
    // The new `fr-BE` locale needs the `published` rows rebuilt (C4): the
    // same transaction records the rebuild and enqueues its job, which a
    // host's jobs runtime runs in the background.
    assert!(report.rebuild_enqueued);
    assert_eq!(
        strings(
            &db,
            "SELECT version || ':' || state FROM merezha_content.rebuilds"
        )
        .await,
        ["2:pending"]
    );
    assert_eq!(
        strings(
            &db,
            "SELECT dedupe_key || ':' || queue || ':' || job_kind FROM merezha_jobs.outbox \
              WHERE record_kind = 'job'"
        )
        .await,
        ["rebuild:2:content:content.published_rebuild"]
    );
    assert_eq!(
        strings(
            &db,
            "SELECT locale || ':' || source_locale FROM merezha_content.published ORDER BY locale"
        )
        .await,
        ["en:en"]
    );
    // New indexes built (category sort, isbn unique), the dropped ones gone
    // (reading_minutes sort, legacy_code unique), the slug's kept.
    let after = indexes(&db).await;
    let expected: Vec<String> = derived_indexes(&v2).into_iter().map(|i| i.name).collect();
    assert_eq!(after.keys().cloned().collect::<Vec<_>>(), {
        let mut sorted = expected.clone();
        sorted.sort();
        sorted
    });
    assert!(after.values().all(|valid| *valid));
    assert_eq!(report.created.len(), 6);
    assert_eq!(report.dropped.len(), 6);
    for name in &report.dropped {
        assert!(before.contains_key(name) && !after.contains_key(name));
    }
    let tightened: Vec<(&str, &str, u64, u64)> = report
        .tightened
        .iter()
        .map(|t| {
            (
                t.content_type.as_str(),
                t.field.as_str(),
                t.drafts,
                t.published,
            )
        })
        .collect();
    // `isbn` is a new required field: the published row lacks it.
    assert_eq!(
        tightened,
        [("article", "isbn", 0, 1), ("article", "title", 1, 1)]
    );
}

/// [`V1`] with `edit` applied to its source text.
fn v1_with(edit: impl Fn(&str) -> String) -> String {
    edit(V1).replace("version = 1", "version = 2")
}

#[tokio::test]
async fn each_refused_change_fails_with_content_model_breaking_and_changes_nothing() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    run(&db, &model(V1)).await.unwrap();
    let indexes_before = indexes(&db).await;
    let cases: Vec<(&str, String, &str, &str)> = vec![
        (
            "field removed",
            v1_with(|s| s.replace("  { name = \"slug\", type = \"slug\" },\n", "")),
            "types.article.fields.slug",
            "field_removed",
        ),
        (
            "field renamed",
            v1_with(|s| s.replace("name = \"legacy_code\"", "name = \"old_code\"")),
            "types.article.fields.legacy_code",
            "field_removed",
        ),
        (
            "field type changed",
            v1_with(|s| {
                s.replace(
                    "{ name = \"reading_minutes\", type = \"integer\", min = 1, max = 600, sortable = true }",
                    "{ name = \"reading_minutes\", type = \"float\", sortable = true }",
                )
            }),
            "types.article.fields.reading_minutes",
            "field_type_changed",
        ),
        (
            "enum value removed",
            v1_with(|s| s.replace("values = [\"news\", \"guide\"]", "values = [\"news\"]")),
            "types.article.fields.category.values",
            "value_removed",
        ),
        (
            "type removed",
            v1_with(|s| s.replace("[types.article]", "[types.post]")),
            "types.article",
            "type_removed",
        ),
        (
            "localized changed",
            v1_with(|s| {
                s.replace(
                    "access = \"public\"\n",
                    "access = \"public\"\nlocalized = false\n",
                )
            }),
            "types.article.localized",
            "localized_changed",
        ),
        (
            "field localized changed",
            v1_with(|s| {
                s.replace(
                    "{ name = \"slug\", type = \"slug\" }",
                    "{ name = \"slug\", type = \"slug\", localized = false }",
                )
            }),
            "types.article.fields.slug.localized",
            "localized_changed",
        ),
        (
            "scope changed",
            v1_with(|s| {
                s.replace(
                    "access = \"public\"\n",
                    "access = \"public\"\nscope = \"organization\"\n",
                )
            }),
            "types.article.scope",
            "scope_changed",
        ),
        (
            "kind changed",
            v1_with(|s| {
                s.replace(
                    "access = \"public\"\n",
                    "access = \"public\"\nkind = \"singleton\"\n",
                )
            }),
            "types.article.kind",
            "kind_changed",
        ),
        (
            "locale removed",
            v1_with(|s| s.replace(", { code = \"fr\" }", "")),
            "locales.fr",
            "locale_removed",
        ),
        (
            "default locale changed",
            v1_with(|s| {
                s.replace(
                    "[{ code = \"en\", default = true }, { code = \"fr\" }]",
                    "[{ code = \"en\", fallback = \"fr\" }, { code = \"fr\", default = true }]",
                )
            }),
            "locales.en",
            "default_locale_changed",
        ),
        (
            "unique added to an existing field",
            v1_with(|s| s.replace("max_length = 200 }", "max_length = 200, unique = true }")),
            "types.article.fields.title.unique",
            "unique_added",
        ),
        (
            "component removed",
            v1_with(|s| {
                s.replace("[components.seo]\nfields = [{ name = \"title\", type = \"string\", max_length = 70 }]\n", "")
                    .replace("  { name = \"seo\", type = \"component\", component = \"seo\" },\n", "")
            }),
            "components.seo",
            "component_removed",
        ),
    ];
    for (case, source, path, code) in cases {
        let refused = model(&source);
        let error = run(&db, &refused).await.expect_err(case);
        assert_eq!(error.failure.code, "content_model_breaking", "{case}");
        assert!(
            error
                .changes
                .iter()
                .any(|c| c.class == ChangeClass::Refused && c.path == path && c.code == code),
            "{case}: {:?}",
            error.changes
        );
        assert_eq!(models(&db).await, [(1, "active".to_owned())], "{case}");
        assert_eq!(activation_events(&db).await.len(), 1, "{case}");
        assert_eq!(indexes(&db).await, indexes_before, "{case}");
        // A host that only verifies refuses the same model at startup.
        let client = db.acquire(soon()).await.unwrap();
        let error = verify(&db, &**client, &refused).await.expect_err(case);
        assert_eq!(error.failure.code, "content_model_breaking", "{case}");
    }
}

#[tokio::test]
async fn the_same_version_with_a_new_fingerprint_fails() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    run(&db, &model(V1)).await.unwrap();
    let changed = model(&V1.replace("max_length = 200", "max_length = 300"));
    assert_eq!(changed.version(), 1);
    let error = run(&db, &changed).await.unwrap_err();
    assert_eq!(error.failure.code, "content_model_version_unchanged");
    assert_eq!(models(&db).await, [(1, "active".to_owned())]);
    let client = db.acquire(soon()).await.unwrap();
    let error = verify(&db, &**client, &changed).await.unwrap_err();
    assert_eq!(error.failure.code, "content_model_version_unchanged");
}

#[tokio::test]
async fn new_indexes_are_valid_before_the_row_turns_active_and_unneeded_ones_drop_after() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let (v1, v2) = (model(V1), model(V2));
    run(&db, &v1).await.unwrap();
    // Record, at the moment each models row is inserted, which derived
    // indexes exist and are valid.
    execute(
        &db,
        "CREATE TABLE public.seen (version int, valid text[], present text[]);
         CREATE FUNCTION public.seen() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO public.seen SELECT NEW.version,
             coalesce(array_agg(c.relname::text ORDER BY c.relname) FILTER (WHERE i.indisvalid), '{}'),
             coalesce(array_agg(c.relname::text ORDER BY c.relname), '{}')
           FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
           JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = 'merezha_content' AND c.relname LIKE 'content\\_ix\\_%';
           RETURN NEW;
         END $$;
         CREATE TRIGGER seen BEFORE INSERT ON merezha_content.models
           FOR EACH ROW EXECUTE FUNCTION public.seen();",
    )
    .await;
    run(&db, &v2).await.unwrap();
    let client = db.acquire(soon()).await.unwrap();
    let row = client
        .query_one(
            "SELECT valid, present FROM public.seen WHERE version = 2",
            &[],
        )
        .await
        .unwrap();
    let (valid, present): (Vec<String>, Vec<String>) = (row.get(0), row.get(1));
    let new_names: Vec<String> = derived_indexes(&v2).into_iter().map(|i| i.name).collect();
    let old_names: Vec<String> = derived_indexes(&v1).into_iter().map(|i| i.name).collect();
    for name in &new_names {
        assert!(valid.contains(name), "{name} not valid before activation");
    }
    // The isbn unique index, a new `unique` field's, is among them.
    let isbn: Vec<_> = derived_indexes(&v2)
        .into_iter()
        .filter(|i| i.field == "isbn")
        .collect();
    assert_eq!(isbn.len(), 2);
    for index in &isbn {
        assert!(index.create.starts_with("CREATE UNIQUE INDEX CONCURRENTLY"));
        assert!(valid.contains(&index.name));
    }
    // The active model's indexes stayed in place until after activation.
    let unneeded: Vec<&String> = old_names
        .iter()
        .filter(|n| !new_names.contains(n))
        .collect();
    assert_eq!(unneeded.len(), 6);
    let after = indexes(&db).await;
    for name in unneeded {
        assert!(present.contains(name), "{name} dropped before activation");
        assert!(!after.contains_key(name), "{name} kept after activation");
    }
}

#[tokio::test]
async fn an_invalid_index_from_a_crashed_build_is_rebuilt() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let v1 = model(V1);
    run(&db, &v1).await.unwrap();
    seed(&db, "article", "en", 2, r#"{"title": "x"}"#, false).await;
    let index = derived_indexes(&v1)
        .into_iter()
        .find(|i| i.field == "reading_minutes" && i.table == merezha_content::IndexTable::Variants)
        .unwrap();
    // Replace it with an invalid index of the same name, as a concurrent
    // build that failed half-way leaves behind.
    execute(&db, &index.drop_statement()).await;
    let client = db.acquire(soon()).await.unwrap();
    let failed = client
        .batch_execute(&format!(
            "CREATE UNIQUE INDEX CONCURRENTLY {} ON merezha_content.variants ((1))",
            index.name
        ))
        .await;
    assert!(failed.is_err());
    drop(client);
    assert_eq!(indexes(&db).await.get(&index.name), Some(&false));
    // A host that only verifies is not ready.
    let client = db.acquire(soon()).await.unwrap();
    assert_eq!(
        verify(&db, &**client, &v1).await.unwrap(),
        Verified::NotReady("index_build_pending")
    );
    drop(client);
    let report = run(&db, &v1).await.unwrap();
    assert_eq!(report.plan.status, Status::Current);
    assert_eq!(report.created, std::slice::from_ref(&index.name));
    assert_eq!(indexes(&db).await.get(&index.name), Some(&true));
    let definition = strings(
        &db,
        &format!(
            "SELECT pg_get_indexdef('merezha_content.{}'::regclass)",
            index.name
        ),
    )
    .await;
    assert!(definition[0].contains("reading_minutes"), "{definition:?}");
    let client = db.acquire(soon()).await.unwrap();
    assert_eq!(verify(&db, &**client, &v1).await.unwrap(), Verified::Ready);
}

#[tokio::test]
async fn a_build_killed_mid_way_leaves_the_host_not_ready_and_the_next_start_repairs_it() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let (v1, v2) = (model(V1), model(V2));
    run(&db, &v1).await.unwrap();
    // A writer holding locks on both tables makes every concurrent build
    // wait after their catalog entry exists, so killing the backend leaves
    // an invalid index.
    let blocker = db.acquire(soon()).await.unwrap();
    blocker
        .batch_execute("BEGIN; LOCK TABLE merezha_content.variants, merezha_content.published IN ROW EXCLUSIVE MODE")
        .await
        .unwrap();
    let host = {
        let db = db.clone();
        let v2 = v2.clone();
        tokio::spawn(async move { run(&db, &v2).await })
    };
    // Wait until a build waits with its (still invalid) catalog entry in
    // place, so the kill lands mid-build rather than before it began.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let pid = loop {
        let waiting = scalar_i64(
            &db,
            "SELECT coalesce(max(pid), 0)::int8 FROM pg_stat_activity \
             WHERE datname = current_database() AND query LIKE 'CREATE %INDEX CONCURRENTLY%' AND wait_event_type = 'Lock' \
               AND EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
                           WHERE c.relname LIKE 'content\\_ix\\_%' AND NOT i.indisvalid)",
        )
        .await;
        if waiting != 0 {
            break waiting;
        }
        assert!(std::time::Instant::now() < deadline, "no build waited");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    // The host dies mid-build: its task stops and its session drops. No
    // pg_terminate_backend: the detached session's
    // client_connection_check_interval ends the backend, and with it the
    // activation lock, within about half a second. The bound is forty
    // check intervals, so only a backend that never ends fails, however
    // loaded the server is.
    host.abort();
    let _ = host.await;
    let bound = merezha_postgres::CONNECTION_CHECK_INTERVAL * 40;
    let gone = std::time::Instant::now();
    loop {
        let alive = scalar_i64(
            &db,
            &format!("SELECT count(*) FROM pg_stat_activity WHERE pid = {pid}"),
        )
        .await;
        let free = scalar_i64(
            &db,
            &format!("SELECT pg_try_advisory_xact_lock({ACTIVATION_LOCK})::int::int8"),
        )
        .await;
        if alive == 0 && free == 1 {
            break;
        }
        assert!(
            gone.elapsed() < bound,
            "the build backend or the activation lock outlived the host by {bound:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let left = indexes(&db).await;
    assert!(left.values().any(|valid| !valid), "{left:?}");
    assert_eq!(models(&db).await, [(1, "active".to_owned())]);
    // Restarted without migrating, the host is not ready.
    let client = db.acquire(soon()).await.unwrap();
    assert_eq!(
        verify(&db, &**client, &v2).await.unwrap(),
        Verified::NotReady("content_model_pending")
    );
    drop(client);
    blocker.batch_execute("ROLLBACK").await.unwrap();
    drop(blocker);
    // The next start repairs it and activates.
    let report = run(&db, &v2).await.unwrap();
    assert!(report.activated);
    let after = indexes(&db).await;
    assert!(after.values().all(|valid| *valid), "{after:?}");
    let client = db.acquire(soon()).await.unwrap();
    assert_eq!(verify(&db, &**client, &v2).await.unwrap(), Verified::Ready);
}

#[tokio::test]
async fn a_build_failing_three_times_stops_activation_with_index_build_failed() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let (v1, v2) = (model(V1), model(V2));
    run(&db, &v1).await.unwrap();
    // A transient failure (lock_not_available) on every build attempt.
    inject_build_failure(&db, "55P03").await;
    let error = run(&db, &v2).await.unwrap_err();
    assert_eq!(error.failure.code, "index_build_failed");
    let index = error.index.as_ref().unwrap();
    assert_eq!(index.attempts, 3);
    assert_eq!(index.code, "database_busy");
    assert_eq!(build_attempts(&db).await, 3, "exactly three attempts");
    assert_eq!(models(&db).await, [(1, "active".to_owned())]);
    assert_eq!(activation_events(&db).await.len(), 1);
    let client = db.acquire(soon()).await.unwrap();
    // Hosts on the active model keep writing.
    assert_eq!(
        verify(&db, &**client, &v1).await.unwrap(),
        Verified::Ready,
        "the previous model is still served"
    );
    drop(client);
    remove_build_failure(&db).await;
    assert!(run(&db, &v2).await.unwrap().activated);
}

#[tokio::test]
async fn a_unique_violation_stops_activation_at_the_first_attempt() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    run(&db, &model(V1)).await.unwrap();
    inject_build_failure(&db, "23505").await;
    let error = run(&db, &model(V2)).await.unwrap_err();
    assert_eq!(error.failure.code, "index_build_failed");
    assert_eq!(error.index.as_ref().unwrap().code, "unique_violation");
    assert_eq!(error.index.as_ref().unwrap().attempts, 1);
    assert_eq!(build_attempts(&db).await, 1);
    assert_eq!(models(&db).await, [(1, "active".to_owned())]);
}

#[tokio::test]
async fn readiness_stays_false_until_every_expected_index_is_valid() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let v1 = model(V1);
    let client = db.acquire(soon()).await.unwrap();
    assert_eq!(
        verify(&db, &**client, &v1).await.unwrap(),
        Verified::NotReady("content_model_pending"),
        "nothing active yet"
    );
    drop(client);
    run(&db, &v1).await.unwrap();
    for index in derived_indexes(&v1) {
        execute(&db, &index.drop_statement()).await;
        let client = db.acquire(soon()).await.unwrap();
        assert_eq!(
            verify(&db, &**client, &v1).await.unwrap(),
            Verified::NotReady("index_build_pending"),
            "{} missing",
            index.name
        );
        drop(client);
        run(&db, &v1).await.unwrap();
    }
    let client = db.acquire(soon()).await.unwrap();
    assert_eq!(verify(&db, &**client, &v1).await.unwrap(), Verified::Ready);
    // An older host after v2 activated reports outdated.
    drop(client);
    run(&db, &model(V2)).await.unwrap();
    let client = db.acquire(soon()).await.unwrap();
    assert_eq!(
        verify(&db, &**client, &v1).await.unwrap(),
        Verified::NotReady("content_model_outdated")
    );
}

#[tokio::test]
async fn two_hosts_starting_together_activate_once() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    // Two pools, as two hosts have.
    let (a, b) = (database(&test).await, database(&test).await);
    let v1 = model(V1);
    for round in 0..3 {
        let version = round + 1;
        let source = V1.replace("version = 1", &format!("version = {version}"));
        let current = if round == 0 {
            v1.clone()
        } else {
            model(&source)
        };
        let (first, second) = tokio::join!(run(&a, &current), run(&b, &current));
        let (first, second) = (first.unwrap(), second.unwrap());
        assert_eq!(
            [first.activated, second.activated]
                .iter()
                .filter(|a| **a)
                .count(),
            1,
            "round {round}"
        );
        assert_eq!(activation_events(&a).await.len(), version as usize);
        let rows = models(&a).await;
        assert_eq!(rows.len(), version as usize);
        assert_eq!(
            rows.iter().filter(|(_, state)| state == "active").count(),
            1
        );
    }
}

#[tokio::test]
async fn the_write_check_refuses_once_a_newer_model_is_active() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let v1 = model(V1);
    run(&db, &v1).await.unwrap();
    // A write in flight holds the share lock: activation waits for it.
    let mut client = db.acquire(soon()).await.unwrap();
    let mut tx = client.transaction().await.unwrap();
    lock_model_version(&db, &mut tx, 1).await.unwrap();
    let writer: i32 = tx
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    // v2 adds a field and no index, so it reaches the activation commit at once.
    let v2 = model(&V1.replace("version = 1", "version = 2").replace(
        "  { name = \"seo\", type = \"component\", component = \"seo\" },\n",
        "  { name = \"seo\", type = \"component\", component = \"seo\" },\n  { name = \"note\", type = \"text\" },\n",
    ));
    let activation = {
        let db = db.clone();
        tokio::spawn(async move { run(&db, &v2).await })
    };
    // Observed blocked on the write's lock, it cannot finish before the
    // write commits.
    wait_blocked_by(&db, writer).await;
    assert!(!activation.is_finished(), "activation waits for the write");
    assert_eq!(models(&db).await, [(1, "active".to_owned())]);
    tx.commit().await.unwrap();
    assert!(activation.await.unwrap().unwrap().activated);
    // From now on writes validated against v1 are refused.
    let mut tx = client.transaction().await.unwrap();
    let refused = lock_model_version(&db, &mut tx, 1).await.unwrap_err();
    assert_eq!(refused.code, "content_model_superseded");
    assert_eq!(
        refused.classification,
        merezha_domain::Classification::Transient
    );
    drop(tx);
    let mut tx = client.transaction().await.unwrap();
    lock_model_version(&db, &mut tx, 2).await.unwrap();
}

#[tokio::test]
async fn a_range_filter_on_a_sortable_field_uses_its_derived_index() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let v1 = model(V1);
    run(&db, &v1).await.unwrap();
    seed(
        &db,
        "article",
        "en",
        500,
        r#"{"title": "x", "reading_minutes": 7}"#,
        true,
    )
    .await;
    // Distinct values, so a narrow range is selective.
    execute(
        &db,
        "UPDATE merezha_content.published p SET data = jsonb_build_object('title', 'x', 'reading_minutes', n.rank)
           FROM (SELECT entry_id, row_number() OVER (ORDER BY entry_id) AS rank
                   FROM merezha_content.published) n
          WHERE n.entry_id = p.entry_id;
         ANALYZE merezha_content.published",
    )
    .await;
    let index = derived_indexes(&v1)
        .into_iter()
        .find(|i| i.field == "reading_minutes" && i.table == merezha_content::IndexTable::Published)
        .unwrap();
    let client = db.acquire(soon()).await.unwrap();
    client
        .batch_execute("SET enable_seqscan = off")
        .await
        .unwrap();
    let plan: Vec<String> = client
        .query(
            // The shape of a sorted delivery list with a range filter.
            "EXPLAIN SELECT entry_id FROM merezha_content.published \
             WHERE content_type = 'article' AND scope = 'app' AND locale = 'en' \
               AND ((data->>'reading_minutes'))::bigint > 490 \
             ORDER BY ((data->'reading_minutes') IS NULL), ((data->>'reading_minutes'))::bigint, entry_id \
             LIMIT 25",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    client.batch_execute("RESET enable_seqscan").await.unwrap();
    assert!(
        plan.iter().any(|line| line.contains(&index.name)),
        "{plan:#?}"
    );
}

/// The pid of a concurrent index build or drop (a retry drops the invalid
/// index first) waiting on a lock, other than `not`.
async fn waiting_build(db: &merezha_platform::Database, not: i64) -> i64 {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let pid = scalar_i64(
            db,
            &format!(
                "SELECT coalesce(max(pid), 0)::int8 FROM pg_stat_activity \
                 WHERE datname = current_database() AND query LIKE '% INDEX CONCURRENTLY%content\\_ix\\_%' \
                   AND wait_event_type = 'Lock' AND pid <> {not}"
            ),
        )
        .await;
        if pid != 0 {
            return pid;
        }
        if std::time::Instant::now() >= deadline {
            let activity = strings(
                db,
                "SELECT pid || ' ' || coalesce(wait_event_type, '-') || ' ' || coalesce(wait_event, '-') \
                        || ' ' || state || ' ' || left(query, 60) \
                   FROM pg_stat_activity WHERE datname = current_database()",
            )
            .await;
            panic!("no build other than {not} waited: {activity:#?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_lost_session_reconnects_retakes_the_lock_and_keeps_counting_attempts() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let (v1, v2) = (model(V1), model(V2));
    run(&db, &v1).await.unwrap();
    let blocker = db.acquire(soon()).await.unwrap();
    blocker
        .batch_execute("BEGIN; LOCK TABLE merezha_content.variants, merezha_content.published IN ROW EXCLUSIVE MODE")
        .await
        .unwrap();
    // One lost session: the run reopens one, plans again and finishes.
    let activation = {
        let (db, v2) = (db.clone(), v2.clone());
        tokio::spawn(async move { run(&db, &v2).await })
    };
    let first = waiting_build(&db, 0).await;
    execute(&db, &format!("SELECT pg_terminate_backend({first})")).await;
    let second = waiting_build(&db, first).await;
    assert_ne!(first, second, "the build resumed on a new session");
    blocker.batch_execute("ROLLBACK").await.unwrap();
    let report = activation.await.unwrap().unwrap();
    assert!(report.activated);
    assert_eq!(
        models(&db).await,
        [(1, "superseded".to_owned()), (2, "active".to_owned())]
    );
    // Every attempt lost: the third one stops activation.
    let v3 = model(&V2.replace("version = 2", "version = 3").replace(
        "  { name = \"summary\", type = \"text\" },\n",
        "  { name = \"summary\", type = \"text\" },\n  { name = \"rank\", type = \"integer\", sortable = true },\n",
    ));
    blocker
        .batch_execute("BEGIN; LOCK TABLE merezha_content.variants, merezha_content.published IN ROW EXCLUSIVE MODE")
        .await
        .unwrap();
    let activation = {
        let db = db.clone();
        tokio::spawn(async move { run(&db, &v3).await })
    };
    let mut previous = 0;
    for _ in 0..3 {
        let pid = waiting_build(&db, previous).await;
        execute(&db, &format!("SELECT pg_terminate_backend({pid})")).await;
        previous = pid;
    }
    let error = activation.await.unwrap().unwrap_err();
    blocker.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(error.failure.code, "index_build_failed");
    assert_eq!(error.index.as_ref().unwrap().attempts, 3);
    assert_eq!(error.index.as_ref().unwrap().code, "database_unavailable");
    assert_eq!(
        models(&db).await,
        [(1, "superseded".to_owned()), (2, "active".to_owned())]
    );
}

#[tokio::test]
async fn a_failed_drop_after_the_commit_keeps_the_activation_and_the_next_start_drops_it() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let (v1, v2) = (model(V1), model(V2));
    run(&db, &v1).await.unwrap();
    execute(
        &db,
        "CREATE FUNCTION public.fail_drops() RETURNS event_trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF current_query() LIKE 'DROP INDEX CONCURRENTLY IF EXISTS merezha_content.content_ix_%' THEN
             RAISE EXCEPTION 'injected drop failure' USING ERRCODE = '55P03';
           END IF;
         END $$;
         CREATE EVENT TRIGGER fail_drops ON ddl_command_start WHEN TAG IN ('DROP INDEX')
           EXECUTE FUNCTION public.fail_drops();",
    )
    .await;
    let report = run(&db, &v2).await.unwrap();
    assert!(report.activated, "the committed activation stands");
    assert!(report.dropped.is_empty());
    assert_eq!(report.undropped.len(), 6);
    assert_eq!(
        models(&db).await,
        [(1, "superseded".to_owned()), (2, "active".to_owned())]
    );
    let existing = indexes(&db).await;
    for name in &report.undropped {
        assert!(existing.contains_key(name), "{name}");
    }
    execute(&db, "DROP EVENT TRIGGER fail_drops").await;
    let again = run(&db, &v2).await.unwrap();
    assert_eq!(again.plan.status, Status::Current);
    let mut dropped = again.dropped.clone();
    dropped.sort();
    let mut expected = report.undropped.clone();
    expected.sort();
    assert_eq!(dropped, expected);
}

#[tokio::test]
async fn the_write_check_tells_a_pending_model_from_a_superseded_one() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    run(&db, &model(V1)).await.unwrap();
    let mut client = db.acquire(soon()).await.unwrap();
    // A v2 host before v2 is active: pending, not superseded.
    let mut tx = client.transaction().await.unwrap();
    let pending = lock_model_version(&db, &mut tx, 2).await.unwrap_err();
    assert_eq!(pending.code, "content_model_pending");
    assert_eq!(
        pending.classification,
        merezha_domain::Classification::Transient
    );
    drop(tx);
    // After v3 activates, a v2 host is superseded.
    run(&db, &model(&V1.replace("version = 1", "version = 3")))
        .await
        .unwrap();
    let mut tx = client.transaction().await.unwrap();
    assert_eq!(
        lock_model_version(&db, &mut tx, 2).await.unwrap_err().code,
        "content_model_superseded"
    );
}

/// A type with a localized unique field and a shared (`localized = false`)
/// unique field.
const UNIQUE: &str = r#"
version = 1
locales = [{ code = "en", default = true }, { code = "fr" }]

[types.book]
access = "public"
fields = [
  { name = "code", type = "string", unique = true },
  { name = "isbn", type = "string", unique = true, localized = false },
]
"#;

/// Run `sql`; `true` when it succeeded, `false` on a unique violation.
async fn accepted(db: &merezha_platform::Database, sql: &str) -> bool {
    let client = db.acquire(soon()).await.unwrap();
    match client.batch_execute(sql).await {
        Ok(()) => true,
        Err(error) => {
            assert_eq!(
                error.code(),
                Some(&merezha_postgres::tokio_postgres::error::SqlState::UNIQUE_VIOLATION),
                "{error}: {sql}"
            );
            false
        }
    }
}

fn variant(id: &str, locale: &str, data: &str) -> String {
    format!(
        "INSERT INTO merezha_content.variants (entry_id, locale, scope, content_type, draft_revision, \
         draft_data, draft_updated_at, draft_updated_by) \
         VALUES ('{id}', '{locale}', 'app', 'book', 1, '{data}', now(), 'sys_test')"
    )
}

fn published(id: &str, locale: &str, source: &str, data: &str) -> String {
    format!(
        "INSERT INTO merezha_content.published (entry_id, locale, source_locale, scope, content_type, \
         revision, data, published_at, first_published_at) \
         VALUES ('{id}', '{locale}', '{source}', 'app', 'book', 1, '{data}', now(), now())"
    )
}

#[tokio::test]
async fn unique_indexes_ignore_fallback_rows_and_shared_copies() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    run(&db, &model(UNIQUE)).await.unwrap();
    let ids = [
        "00000000-0000-7000-8000-00000000000a",
        "00000000-0000-7000-8000-00000000000b",
        "00000000-0000-7000-8000-00000000000c",
        "00000000-0000-7000-8000-00000000000d",
    ];
    for id in ids {
        execute(
            &db,
            &format!(
                "INSERT INTO merezha_content.entries (id, scope, content_type, created_at, created_by) \
                 VALUES ('{id}', 'app', 'book', now(), 'sys_test')"
            ),
        )
        .await;
    }
    let [a, b, c, d] = ids;
    // Drafts. A localized value is unique per locale.
    assert!(accepted(&db, &variant(a, "en", r#"{"code":"x","isbn":"i1"}"#)).await);
    assert!(!accepted(&db, &variant(b, "en", r#"{"code":"x"}"#)).await);
    assert!(accepted(&db, &variant(b, "fr", r#"{"code":"x"}"#)).await);
    // A's fr variant carries a copy of its shared isbn: no collision.
    assert!(accepted(&db, &variant(a, "fr", r#"{"code":"y","isbn":"i1"}"#)).await);
    // Another entry's default variant with that isbn collides.
    assert!(!accepted(&db, &variant(c, "en", r#"{"isbn":"i1"}"#)).await);
    assert!(accepted(&db, &variant(c, "en", r#"{"isbn":"i2"}"#)).await);
    // Delivery rows. A's fr row falls back to en and repeats code x; C's
    // own fr row with code x does not collide with it.
    assert!(
        accepted(
            &db,
            &published(a, "en", "en", r#"{"code":"x","isbn":"i1"}"#)
        )
        .await
    );
    assert!(
        accepted(
            &db,
            &published(a, "fr", "en", r#"{"code":"x","isbn":"i1"}"#)
        )
        .await
    );
    assert!(
        accepted(
            &db,
            &published(c, "fr", "fr", r#"{"code":"x","isbn":"i2"}"#)
        )
        .await
    );
    // Two own rows of one locale with one value do collide.
    assert!(!accepted(&db, &published(d, "fr", "fr", r#"{"code":"x"}"#)).await);
    // Shared isbn: B's own fr row repeats A's isbn without colliding, a
    // second default row with it does.
    assert!(
        accepted(
            &db,
            &published(b, "fr", "fr", r#"{"code":"z","isbn":"i1"}"#)
        )
        .await
    );
    assert!(!accepted(&db, &published(d, "en", "en", r#"{"isbn":"i1"}"#)).await);
}

#[tokio::test]
async fn the_rebuild_job_is_enqueued_in_the_activation_transaction() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let (v1, v2) = (model(V1), model(V2));
    run(&db, &v1).await.unwrap();
    // A failing commit of the activation leaves neither the rebuild row nor
    // its job.
    execute(
        &db,
        "CREATE FUNCTION public.fail_activation() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'injected'; END $$;
         CREATE CONSTRAINT TRIGGER fail_activation AFTER INSERT ON merezha_content.models
           DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.fail_activation();",
    )
    .await;
    assert!(run(&db, &v2).await.is_err());
    let jobs = "SELECT count(*) FROM merezha_jobs.outbox WHERE record_kind = 'job'";
    assert_eq!(scalar_i64(&db, jobs).await, 0);
    assert_eq!(
        scalar_i64(&db, "SELECT count(*) FROM merezha_content.rebuilds").await,
        0
    );
    execute(
        &db,
        "DROP TRIGGER fail_activation ON merezha_content.models",
    )
    .await;
    let report = run(&db, &v2).await.unwrap();
    assert!(report.rebuild_enqueued);
    // One transaction wrote all three: `now()` is its start.
    assert_eq!(
        strings(
            &db,
            "SELECT (m.activated_at = r.updated_at AND r.updated_at = o.created_at)::text \
               FROM merezha_content.models m, merezha_content.rebuilds r, merezha_jobs.outbox o \
              WHERE m.version = 2 AND r.version = 2 AND o.record_kind = 'job'"
        )
        .await,
        ["true"]
    );
}
