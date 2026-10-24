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
