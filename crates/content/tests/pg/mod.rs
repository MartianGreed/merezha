//! Shared fixtures for the content PostgreSQL tests.
#![allow(dead_code)]
use merezha_content::{
    ActivationError, ActivationOptions, ActivationReport, CONTENT_PERMISSIONS, ContentModel,
    MIGRATIONS, ModelOptions, activate,
};
use merezha_jobs::{PgJobs, PgOutbox};
use merezha_platform::{
    Database, OsRandom,
    ports::{Source, SourceOutbox},
};
use merezha_postgres::{MigrationMode, ModuleMigrations, TestDatabase, run_migrations};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

pub fn soon() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

pub fn options() -> ModelOptions<'static> {
    ModelOptions {
        permissions: &CONTENT_PERMISSIONS,
        media: false,
        organizations: true,
    }
}

pub fn model(source: &str) -> ContentModel {
    ContentModel::parse(source, &options()).unwrap_or_else(|e| panic!("{e}\n{source}"))
}

/// Fast retries for tests.
pub fn fast() -> ActivationOptions {
    ActivationOptions {
        timeout: Duration::from_secs(60),
        retry_backoff: Duration::from_millis(10),
    }
}

/// A pool on `test` with migration 1 and the jobs schema applied.
pub async fn database(test: &TestDatabase) -> Database {
    let mut config = test.config();
    config.pool_size = 6;
    config.statement_timeout = Duration::from_secs(30);
    let db = Database::connect(&config).unwrap();
    run_migrations(
        &db,
        &[
            ModuleMigrations {
                module: "content",
                migrations: MIGRATIONS,
            },
            ModuleMigrations {
                module: "jobs",
                migrations: PgJobs::migrations(),
            },
        ],
        MigrationMode::Apply,
        soon(),
    )
    .await
    .unwrap();
    db
}

pub fn outbox() -> SourceOutbox {
    SourceOutbox::new(Source::new("content").unwrap(), Arc::new(PgOutbox))
}

pub async fn run(db: &Database, model: &ContentModel) -> Result<ActivationReport, ActivationError> {
    activate(db, &outbox(), &OsRandom, model, &fast()).await
}

pub async fn execute(db: &Database, sql: &str) {
    let client = db.acquire(soon()).await.unwrap();
    client
        .batch_execute(sql)
        .await
        .unwrap_or_else(|e| panic!("{e}: {sql}"));
}

pub async fn scalar_i64(db: &Database, sql: &str) -> i64 {
    let client = db.acquire(soon()).await.unwrap();
    client
        .query_one(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{e}: {sql}"))
        .get(0)
}

pub async fn strings(db: &Database, sql: &str) -> Vec<String> {
    let client = db.acquire(soon()).await.unwrap();
    client
        .query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{e}: {sql}"))
        .iter()
        .map(|row| row.get(0))
        .collect()
}

/// `(version, state)` of every models row, by version.
pub async fn models(db: &Database) -> Vec<(i32, String)> {
    let client = db.acquire(soon()).await.unwrap();
    client
        .query(
            "SELECT version, state FROM merezha_content.models ORDER BY version",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

/// Derived indexes by name with `indisvalid`.
pub async fn indexes(db: &Database) -> BTreeMap<String, bool> {
    let client = db.acquire(soon()).await.unwrap();
    merezha_content::existing_indexes(db, &**client)
        .await
        .unwrap()
}

/// Payloads of the `content.model_activated` outbox records, oldest first.
pub async fn activation_events(db: &Database) -> Vec<Value> {
    let client = db.acquire(soon()).await.unwrap();
    client
        .query(
            "SELECT payload::text FROM merezha_jobs.outbox \
             WHERE source = 'content' AND topic = 'content.model_activated' ORDER BY id",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| serde_json::from_str(&row.get::<_, String>(0)).unwrap())
        .collect()
}

/// Seed `count` entries of `content_type` in scope `app` with one
/// `locale` variant each holding `data`, and a published row when
/// `published`.
pub async fn seed(
    db: &Database,
    content_type: &str,
    locale: &str,
    count: i64,
    data: &str,
    published: bool,
) {
    let client = db.acquire(soon()).await.unwrap();
    client
        .execute(
            "WITH new AS (
                 INSERT INTO merezha_content.entries (id, scope, content_type, created_at, created_by)
                 SELECT gen_random_uuid(), 'app', $1, now(), 'sys_test' FROM generate_series(1, $2::int8)
                 RETURNING id
             ), drafts AS (
                 INSERT INTO merezha_content.variants
                     (entry_id, locale, scope, content_type, draft_revision, draft_data,
                      draft_updated_at, draft_updated_by, published_revision, published_at,
                      first_published_at)
                 SELECT id, $3, 'app', $1, 1, $4::text::jsonb, now(), 'sys_test',
                        CASE WHEN $5 THEN 1 END, CASE WHEN $5 THEN now() END,
                        CASE WHEN $5 THEN now() END
                   FROM new
                 RETURNING entry_id
             ), history AS (
                 INSERT INTO merezha_content.revisions
                     (entry_id, locale, revision, model_version, kind, data, created_at, created_by)
                 SELECT entry_id, $3, 1, (SELECT max(version) FROM merezha_content.models),
                        'create', $4::text::jsonb, now(), 'sys_test'
                   FROM drafts
             )
             INSERT INTO merezha_content.published
                 (entry_id, locale, source_locale, scope, content_type, revision, data,
                  published_at, first_published_at)
             SELECT entry_id, $3, $3, 'app', $1, 1, $4::text::jsonb, now(), now()
               FROM drafts WHERE $5",
            &[&content_type, &count, &locale, &data, &published],
        )
        .await
        .unwrap();
}

/// Fail every derived index build with SQLSTATE `code`, counting attempts
/// in the sequence `public.build_attempts` (sequences ignore rollbacks).
pub async fn inject_build_failure(db: &Database, code: &str) {
    execute(
        db,
        &format!(
            "CREATE SEQUENCE public.build_attempts;
             CREATE FUNCTION public.fail_builds() RETURNS event_trigger LANGUAGE plpgsql AS $$
             BEGIN
               IF current_query() LIKE 'CREATE %INDEX CONCURRENTLY IF NOT EXISTS content_ix_%' THEN
                 PERFORM nextval('public.build_attempts');
                 RAISE EXCEPTION 'injected build failure' USING ERRCODE = '{code}';
               END IF;
             END $$;
             CREATE EVENT TRIGGER fail_builds ON ddl_command_start WHEN TAG IN ('CREATE INDEX')
               EXECUTE FUNCTION public.fail_builds();"
        ),
    )
    .await;
}

pub async fn build_attempts(db: &Database) -> i64 {
    scalar_i64(
        db,
        "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM public.build_attempts",
    )
    .await
}

pub async fn remove_build_failure(db: &Database) {
    execute(db, "DROP EVENT TRIGGER fail_builds").await;
}

/// v1: two locales, an article with a sortable integer and a unique slug.
pub const V1: &str = r#"
version = 1
locales = [{ code = "en", default = true }, { code = "fr" }]

[components.seo]
fields = [{ name = "title", type = "string", max_length = 70 }]

[types.article]
access = "public"
title_field = "title"
fields = [
  { name = "title", type = "string", required = true, max_length = 200 },
  { name = "slug", type = "slug" },
  { name = "category", type = "enum", values = ["news", "guide"], filterable = true },
  { name = "reading_minutes", type = "integer", min = 1, max = 600, sortable = true },
  { name = "legacy_code", type = "string", unique = true },
  { name = "seo", type = "component", component = "seo" },
]
"#;

/// Wait for a backend of `db`'s database blocked by backend `pid` (as
/// `pg_blocking_pids` reports it); returns the first found. Polls until
/// [`soon`], so only a backend that never blocks fails.
pub async fn wait_blocked_by(db: &Database, pid: i32) -> i32 {
    let deadline = soon();
    loop {
        let client = db.acquire(soon()).await.unwrap();
        let found: Option<i32> = client
            .query_opt(
                "SELECT pid FROM pg_stat_activity \
                  WHERE datname = current_database() AND $1 = ANY(pg_blocking_pids(pid)) LIMIT 1",
                &[&pid],
            )
            .await
            .unwrap()
            .map(|row| row.get(0));
        drop(client);
        if let Some(found) = found {
            return found;
        }
        assert!(
            Instant::now() < deadline,
            "nothing blocked by backend {pid}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
