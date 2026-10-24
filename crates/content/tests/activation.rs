//! CT-C2 against real PostgreSQL: migration 1, activation, diff
//! classification at startup, derived indexes and the version check.
mod pg;

use merezha_content::{
    ChangeClass, MIGRATION_1, Status, Verified, derived_indexes, lock_model_version, verify,
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
    // The one difference: the search path ends with the migration's
    // transaction instead of leaking into the session.
    let expected = sql.replace(
        "SET search_path = merezha_content;",
        "SET LOCAL search_path = merezha_content;",
    );
    assert_eq!(MIGRATION_1, expected);
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
            "refs",
            "revisions",
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
    // Sortable `reading_minutes` and unique `slug` and `legacy_code`, on
    // both tables.
    let expected = derived_indexes(&v1);
    assert_eq!(expected.len(), 6);
    let existing = indexes(&db).await;
    for index in &expected {
        assert_eq!(existing.get(&index.name), Some(&true), "{}", index.name);
    }
    assert_eq!(report.created.len(), 6);
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
    assert_eq!(report.created.len(), 4);
    assert_eq!(report.dropped.len(), 4);
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
        let error = verify(&**client, &refused).await.expect_err(case);
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
    let error = verify(&**client, &changed).await.unwrap_err();
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
    assert_eq!(unneeded.len(), 4);
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
        verify(&**client, &v1).await.unwrap(),
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
    assert_eq!(verify(&**client, &v1).await.unwrap(), Verified::Ready);
}

#[tokio::test]
async fn a_build_killed_mid_way_leaves_the_host_not_ready_and_the_next_start_repairs_it() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    let (v1, v2) = (model(V1), model(V2));
    run(&db, &v1).await.unwrap();
    // A writer holding a lock on `published` makes the concurrent builds
    // wait after their catalog entry exists, so killing the backend leaves
    // an invalid index.
    let blocker = db.acquire(soon()).await.unwrap();
    blocker
        .batch_execute("BEGIN; LOCK TABLE merezha_content.published IN ROW EXCLUSIVE MODE")
        .await
        .unwrap();
    let host = {
        let db = db.clone();
        let v2 = v2.clone();
        tokio::spawn(async move { run(&db, &v2).await })
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let pid = loop {
        let waiting = scalar_i64(
            &db,
            "SELECT coalesce(max(pid), 0)::int8 FROM pg_stat_activity \
             WHERE query LIKE 'CREATE %INDEX CONCURRENTLY%' AND wait_event_type = 'Lock'",
        )
        .await;
        if waiting != 0 {
            break waiting;
        }
        assert!(std::time::Instant::now() < deadline, "no build waited");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    // The host dies: its task stops and its build backend is killed.
    host.abort();
    let _ = host.await;
    execute(&db, &format!("SELECT pg_terminate_backend({pid})")).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while scalar_i64(
        &db,
        &format!("SELECT count(*) FROM pg_stat_activity WHERE pid = {pid}"),
    )
    .await
        != 0
    {
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let left = indexes(&db).await;
    assert!(left.values().any(|valid| !valid), "{left:?}");
    assert_eq!(models(&db).await, [(1, "active".to_owned())]);
    // Restarted without migrating, the host is not ready.
    let client = db.acquire(soon()).await.unwrap();
    assert_eq!(
        verify(&**client, &v2).await.unwrap(),
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
    assert_eq!(verify(&**client, &v2).await.unwrap(), Verified::Ready);
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
        verify(&**client, &v1).await.unwrap(),
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
        verify(&**client, &v1).await.unwrap(),
        Verified::NotReady("content_model_pending"),
        "nothing active yet"
    );
    drop(client);
    run(&db, &v1).await.unwrap();
    for index in derived_indexes(&v1) {
        execute(&db, &index.drop_statement()).await;
        let client = db.acquire(soon()).await.unwrap();
        assert_eq!(
            verify(&**client, &v1).await.unwrap(),
            Verified::NotReady("index_build_pending"),
            "{} missing",
            index.name
        );
        drop(client);
        run(&db, &v1).await.unwrap();
    }
    let client = db.acquire(soon()).await.unwrap();
    assert_eq!(verify(&**client, &v1).await.unwrap(), Verified::Ready);
    // An older host after v2 activated reports outdated.
    drop(client);
    run(&db, &model(V2)).await.unwrap();
    let client = db.acquire(soon()).await.unwrap();
    assert_eq!(
        verify(&**client, &v1).await.unwrap(),
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
    lock_model_version(&mut tx, 1).await.unwrap();
    // v2 adds a field and no index, so it reaches the activation commit at once.
    let v2 = model(&V1.replace("version = 1", "version = 2").replace(
        "  { name = \"seo\", type = \"component\", component = \"seo\" },\n",
        "  { name = \"seo\", type = \"component\", component = \"seo\" },\n  { name = \"note\", type = \"text\" },\n",
    ));
    let activation = {
        let db = db.clone();
        tokio::spawn(async move { run(&db, &v2).await })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!activation.is_finished(), "activation waits for the write");
    assert_eq!(models(&db).await, [(1, "active".to_owned())]);
    tx.commit().await.unwrap();
    assert!(activation.await.unwrap().unwrap().activated);
    // From now on writes validated against v1 are refused.
    let mut tx = client.transaction().await.unwrap();
    let refused = lock_model_version(&mut tx, 1).await.unwrap_err();
    assert_eq!(refused.code, "content_model_superseded");
    assert_eq!(
        refused.classification,
        merezha_domain::Classification::Transient
    );
    drop(tx);
    let mut tx = client.transaction().await.unwrap();
    lock_model_version(&mut tx, 2).await.unwrap();
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
