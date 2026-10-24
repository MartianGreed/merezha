//! Shared fixtures for the jobs PostgreSQL tests.
#![allow(dead_code)]
use merezha_domain::{Failure, Identity};
use merezha_jobs::{PgJobs, PgOutbox, RelayMode};
use merezha_platform::{
    AppContext, BoxFuture, Database, JobsRuntime, Module, ProjectConfig,
    config::JobsConfig,
    ports::{
        DedupeKey, JobContext, JobFailure, JobHandler, JobsHandle, LoopbackAllowAll,
        MessageContext, NewEvent, OnDuplicate, OutboxRecord, OutboxTarget, Source, SourceOutbox,
    },
};
use merezha_postgres::{MigrationMode, ModuleMigrations, TestDatabase, run_migrations};
use serde_json::Value;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

pub fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}
pub fn soon() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
pub fn project(test: &TestDatabase, extra: &str) -> ProjectConfig {
    let text = format!(
        "[app]\nid = \"tasks\"\n[database]\nurl = {{ env = \"JOBS_DATABASE_URL\" }}\nmigrate = true\npool_size = 8\n[runtime]\nmanifest = \"runtime.toml\"\n{extra}"
    );
    ProjectConfig::parse(
        &text,
        std::env::temp_dir(),
        &[("JOBS_DATABASE_URL".into(), test.url().into())],
        &[],
    )
    .unwrap()
}
pub fn jobs_config(test: &TestDatabase, extra: &str) -> JobsConfig {
    project(test, extra).jobs
}
pub async fn database(test: &TestDatabase) -> Database {
    let mut config = test.config();
    config.pool_size = 8;
    let db = Database::connect(&config).unwrap();
    run_migrations(
        &db,
        &[ModuleMigrations {
            module: "jobs",
            migrations: PgJobs::migrations(),
        }],
        MigrationMode::Apply,
        soon(),
    )
    .await
    .unwrap();
    db
}
pub fn context(db: &Database) -> AppContext {
    let policy = Arc::new(LoopbackAllowAll::new("127.0.0.1:1".parse().unwrap()).unwrap());
    AppContext::builder("tasks", db.clone(), policy)
        .build()
        .unwrap()
}
/// A runtime bound to `db` and started, in `mode`.
pub async fn started(db: &Database, config: &JobsConfig, mode: RelayMode) -> PgJobs {
    let jobs = PgJobs::with_mode(config, mode);
    jobs.ports(db);
    jobs.start(context(db)).await.unwrap();
    wait_ready(&jobs).await;
    jobs
}
pub async fn wait_ready(jobs: &PgJobs) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while jobs.ready().await.is_err() {
        assert!(Instant::now() < deadline, "jobs runtime never became ready");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
pub fn message() -> MessageContext {
    MessageContext {
        correlation_id: id("corr-1"),
        causation_id: None,
        tenant: None,
    }
}
pub fn job_record(key: &str, queue: &str, payload: Value) -> OutboxRecord {
    OutboxRecord {
        dedupe_key: DedupeKey::new(key).unwrap(),
        target: OutboxTarget::Job {
            queue: id(queue),
            kind: id("tasks.remind"),
        },
        payload,
        contains_secret: false,
        run_at: None,
        priority: 0,
        context: message(),
    }
}
pub fn event(source: &str, topic: &str, key: &str, payload: Value) -> OutboxRecord {
    OutboxRecord::event(
        &Source::new(source).unwrap(),
        id(topic),
        NewEvent {
            dedupe_key: DedupeKey::new(key).unwrap(),
            version: 1,
            occurred_at: "2026-10-24T09:00:00Z".parse().unwrap(),
            payload,
            context: MessageContext {
                correlation_id: id("4bf92f3577b34da6a3ce929d0e0e4736"),
                causation_id: None,
                tenant: Some(id("org_1")),
            },
        },
    )
}
pub fn outbox(source: &str) -> SourceOutbox {
    SourceOutbox::new(Source::new(source).unwrap(), Arc::new(PgOutbox))
}
pub async fn append(
    db: &Database,
    outbox: &SourceOutbox,
    records: Vec<OutboxRecord>,
    on_duplicate: OnDuplicate,
) -> Result<merezha_platform::ports::Appended, Failure> {
    let outbox = outbox.clone();
    db.transaction(soon(), move |tx| {
        Box::pin(async move { outbox.append(tx, &records, on_duplicate).await })
    })
    .await
}
pub async fn scalar(db: &Database, sql: &str) -> i64 {
    let client = db.acquire(soon()).await.unwrap();
    client.query_one(sql, &[]).await.unwrap().get(0)
}
pub async fn execute(db: &Database, sql: &str) -> Result<u64, Failure> {
    let client = db.acquire(soon()).await.unwrap();
    client
        .execute(sql, &[])
        .await
        .map_err(|e| merezha_postgres::classify(&e))
}
pub async fn wait_for(db: &Database, sql: &str, expected: i64, within: Duration) -> Duration {
    let started = Instant::now();
    loop {
        if scalar(db, sql).await == expected {
            return started.elapsed();
        }
        assert!(
            started.elapsed() < within,
            "`{sql}` never reached {expected} within {within:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub struct Noop;
impl JobHandler for Noop {
    fn handle<'a>(&'a self, _: JobContext, _: Value) -> BoxFuture<'a, Result<(), JobFailure>> {
        Box::pin(async { Ok(()) })
    }
}
pub fn handle(jobs: &PgJobs, source: &str) -> JobsHandle {
    JobsHandle::new(Source::new(source).unwrap(), Arc::new(jobs.clone()))
}

/// A handler from a closure returning a `'static` future.
pub struct Func<F>(pub F);
impl<F> JobHandler for Func<F>
where
    F: Fn(JobContext, Value) -> BoxFuture<'static, Result<(), JobFailure>> + Send + Sync,
{
    fn handle<'a>(
        &'a self,
        ctx: JobContext,
        payload: Value,
    ) -> BoxFuture<'a, Result<(), JobFailure>> {
        (self.0)(ctx, payload)
    }
}
pub fn func<F>(f: F) -> Arc<dyn JobHandler>
where
    F: Fn(JobContext, Value) -> BoxFuture<'static, Result<(), JobFailure>> + Send + Sync + 'static,
{
    Arc::new(Func(f))
}
/// A context for the instance `instance`.
pub fn instance_context(db: &Database, instance: &str) -> AppContext {
    let policy = Arc::new(LoopbackAllowAll::new("127.0.0.1:1".parse().unwrap()).unwrap());
    AppContext::builder("tasks", db.clone(), policy)
        .instance_id(instance)
        .build()
        .unwrap()
}
/// A runtime polling (no `LISTEN`) whose `tasks` module registered
/// `kinds` on queue `default`, started as `instance`.
pub async fn workers(
    db: &Database,
    config: &JobsConfig,
    instance: &str,
    kinds: Vec<(&str, Arc<dyn JobHandler>)>,
) -> PgJobs {
    workers_with(
        PgJobs::with_mode(config, RelayMode::Interval),
        db,
        instance,
        kinds,
    )
    .await
}
pub async fn workers_with(
    jobs: PgJobs,
    db: &Database,
    instance: &str,
    kinds: Vec<(&str, Arc<dyn JobHandler>)>,
) -> PgJobs {
    jobs.ports(db);
    let tasks = handle(&jobs, "tasks");
    for (kind, handler) in kinds {
        tasks
            .register(kind, "default", handler, Default::default())
            .unwrap();
    }
    jobs.start(instance_context(db, instance)).await.unwrap();
    wait_ready(&jobs).await;
    jobs
}
/// Insert one job straight into the table; returns its id.
pub async fn insert_job(db: &Database, kind: &str, key: &str, payload: &str, secret: bool) -> i64 {
    insert_job_on(db, "default", kind, key, payload, secret, 10).await
}
pub async fn insert_job_on(
    db: &Database,
    queue: &str,
    kind: &str,
    key: &str,
    payload: &str,
    secret: bool,
    max_attempts: i32,
) -> i64 {
    let client = db.acquire(soon()).await.unwrap();
    client
        .query_one(
            "INSERT INTO merezha_jobs.jobs (queue, kind, idempotency_key, payload, payload_digest,
                 contains_secret, max_attempts, correlation_id)
             VALUES ($1, $2, $3, $4::text::json, sha256(convert_to($4::text, 'UTF8')), $5, $6, 'corr-1')
             RETURNING id",
            &[&queue, &kind, &key, &payload, &secret, &max_attempts],
        )
        .await
        .unwrap()
        .get(0)
}
/// `state` of job `id`.
pub async fn state(db: &Database, id: i64) -> String {
    let client = db.acquire(soon()).await.unwrap();
    client
        .query_one("SELECT state FROM merezha_jobs.jobs WHERE id = $1", &[&id])
        .await
        .unwrap()
        .get(0)
}
/// Wait until job `id` is in `expected`.
pub async fn wait_state(db: &Database, id: i64, expected: &str, within: Duration) {
    let started = Instant::now();
    loop {
        let now = state(db, id).await;
        if now == expected {
            return;
        }
        assert!(
            started.elapsed() < within,
            "job {id} stayed {now}, never {expected} within {within:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
/// `(attempt, outcome, error_code, lease_owner)` of every lease of job
/// `id`, oldest first.
pub async fn attempts(
    db: &Database,
    id: i64,
) -> Vec<(i32, Option<String>, Option<String>, String)> {
    let client = db.acquire(soon()).await.unwrap();
    client
        .query(
            "SELECT attempt, outcome, error_code, lease_owner FROM merezha_jobs.attempts
              WHERE job_id = $1 ORDER BY started_at",
            &[&id],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
        .collect()
}
