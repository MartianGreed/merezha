//! The PostgreSQL implementation of the kernel `Outbox` port (AP §B.3).
use crate::validate;
use merezha_domain::Failure;
use merezha_platform::{
    BoxFuture, Tx,
    ports::{Appended, OnDuplicate, Outbox, OutboxRecord, OutboxTarget, validate_append},
};
use merezha_postgres::classify;

/// The channel append notifies, delivered when the caller commits.
pub const OUTBOX_CHANNEL: &str = "merezha_jobs_outbox";

/// One multi-row insert of every record, deduplicated on
/// `(source, dedupe_key)`, then one notification when anything was new.
const APPEND: &str = "
WITH rows AS (
    SELECT * FROM unnest($2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::int2[],
                         $8::text[], $9::text[], $10::bool[], $11::text[], $12::text[], $13::text[])
        WITH ORDINALITY AS r(dedupe_key, record_kind, topic, queue, job_kind, priority, run_at,
                             payload, contains_secret, correlation_id, causation_id, tenant, ord)
), inserted AS (
    INSERT INTO merezha_jobs.outbox (source, dedupe_key, record_kind, topic, queue, job_kind,
                                     priority, run_at, payload, contains_secret, correlation_id,
                                     causation_id, tenant)
    SELECT $1, dedupe_key, record_kind, topic, queue, job_kind, priority, run_at::timestamptz,
           payload::json, contains_secret, correlation_id, causation_id, tenant
      FROM rows ORDER BY ord
    ON CONFLICT (source, dedupe_key) DO NOTHING
    RETURNING id
), notified AS (
    SELECT pg_notify('merezha_jobs_outbox', '') FROM (SELECT 1 FROM inserted LIMIT 1) AS one
)
SELECT (SELECT count(*) FROM inserted), (SELECT count(*) FROM notified)";

/// Appends records inside the caller's transaction. It never commits and
/// never opens a transaction. Besides the kernel's checks it refuses, before
/// the insert: a source outside `[a-z][a-z0-9_]{0,31}` or named `outbox` or
/// `cron` (`invalid_source`), a malformed topic (`invalid_topic`) or queue
/// (`invalid_queue`), a delay over 365 days (`invalid_run_at`), and an event
/// whose payload is not the envelope [`OutboxRecord::event`] builds for
/// this source (`invalid_event_envelope`).
///
/// The trace columns stay empty until the telemetry block fills them from
/// the current span (TE-T5).
#[derive(Clone, Copy, Debug, Default)]
pub struct PgOutbox;

impl Outbox for PgOutbox {
    fn append<'a>(
        &'a self,
        tx: &'a mut Tx<'_>,
        source: &'a str,
        records: &'a [OutboxRecord],
        on_duplicate: OnDuplicate,
    ) -> BoxFuture<'a, Result<Appended, Failure>> {
        Box::pin(async move {
            validate_append(records)?;
            validate::source(source)?;
            let now = jiff::Timestamp::now();
            let mut columns = Columns::default();
            for record in records {
                validate::record(source, record, now)?;
                columns.push(record)?;
            }
            if records.is_empty() {
                return Ok(Appended::default());
            }
            let row = tx
                .query_one(
                    APPEND,
                    &[
                        &source,
                        &columns.dedupe_key,
                        &columns.record_kind,
                        &columns.topic,
                        &columns.queue,
                        &columns.job_kind,
                        &columns.priority,
                        &columns.run_at,
                        &columns.payload,
                        &columns.contains_secret,
                        &columns.correlation_id,
                        &columns.causation_id,
                        &columns.tenant,
                    ],
                )
                .await
                .map_err(|e| classify(&e))?;
            let inserted = usize::try_from(row.get::<_, i64>(0)).unwrap_or(0);
            let duplicates = records.len().saturating_sub(inserted);
            if duplicates > 0 && on_duplicate == OnDuplicate::Fail {
                return Err(Failure::conflict("outbox_duplicate"));
            }
            Ok(Appended {
                inserted,
                duplicates,
            })
        })
    }
}

#[derive(Default)]
struct Columns {
    dedupe_key: Vec<String>,
    record_kind: Vec<&'static str>,
    topic: Vec<Option<String>>,
    queue: Vec<Option<String>>,
    job_kind: Vec<Option<String>>,
    priority: Vec<i16>,
    run_at: Vec<Option<String>>,
    payload: Vec<String>,
    contains_secret: Vec<bool>,
    correlation_id: Vec<String>,
    causation_id: Vec<Option<String>>,
    tenant: Vec<Option<String>>,
}
impl Columns {
    fn push(&mut self, record: &OutboxRecord) -> Result<(), Failure> {
        let owned = |id: &merezha_domain::Identity| id.as_str().to_owned();
        let (kind, topic, queue, job_kind) = match &record.target {
            OutboxTarget::Event { topic } => ("event", Some(owned(topic)), None, None),
            OutboxTarget::Job { queue, kind } => {
                ("job", None, Some(owned(queue)), Some(owned(kind)))
            }
        };
        self.dedupe_key.push(record.dedupe_key.as_str().to_owned());
        self.record_kind.push(kind);
        self.topic.push(topic);
        self.queue.push(queue);
        self.job_kind.push(job_kind);
        self.priority.push(record.priority);
        self.run_at.push(record.run_at.map(|at| at.to_string()));
        self.payload.push(
            serde_json::to_string(&record.payload)
                .map_err(|_| Failure::permanent("outbox_payload_limit"))?,
        );
        self.contains_secret.push(record.contains_secret);
        self.correlation_id
            .push(owned(&record.context.correlation_id));
        self.causation_id
            .push(record.context.causation_id.as_ref().map(owned));
        self.tenant.push(record.context.tenant.as_ref().map(owned));
        Ok(())
    }
}
