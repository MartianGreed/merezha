//! The PostgreSQL implementation of the kernel `Outbox` port (AP §B.3).
use crate::{
    telemetry::{self, Metrics},
    validate,
};
use merezha_domain::Failure;
use merezha_platform::{
    BoxFuture, Tx,
    ports::{Appended, OnDuplicate, Outbox, OutboxRecord, OutboxTarget, validate_append},
};
use merezha_postgres::classify;
use merezha_telemetry::names::jobs::RecordKind;
use std::sync::Arc;

/// The channel append notifies, delivered when the caller commits.
pub const OUTBOX_CHANNEL: &str = "merezha_jobs_outbox";

/// One multi-row insert of every record, deduplicated on
/// `(source, dedupe_key)`, then one notification when anything was new.
const APPEND: &str = "
WITH rows AS (
    SELECT * FROM unnest($2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::int2[],
                         $8::text[], $9::text[], $10::bool[], $11::text[], $12::text[], $13::text[],
                         $14::text[], $15::text[])
        WITH ORDINALITY AS r(dedupe_key, record_kind, topic, queue, job_kind, priority, run_at,
                             payload, contains_secret, correlation_id, causation_id, tenant,
                             trace_parent, trace_state, ord)
), inserted AS (
    INSERT INTO merezha_jobs.outbox (source, dedupe_key, record_kind, topic, queue, job_kind,
                                     priority, run_at, payload, contains_secret, correlation_id,
                                     causation_id, tenant, trace_parent, trace_state)
    SELECT $1, dedupe_key, record_kind, topic, queue, job_kind, priority, run_at::timestamptz,
           payload::json, contains_secret, correlation_id, causation_id, tenant, trace_parent,
           trace_state
      FROM rows ORDER BY ord
    ON CONFLICT (source, dedupe_key) DO NOTHING
    RETURNING id, record_kind
), notified AS (
    SELECT pg_notify('merezha_jobs_outbox', '') FROM (SELECT 1 FROM inserted LIMIT 1) AS one
)
SELECT (SELECT count(*) FROM inserted), (SELECT count(*) FROM notified),
       (SELECT count(*) FROM inserted WHERE record_kind = 'event')";

/// Appends records inside the caller's transaction. It never commits and
/// never opens a transaction. Besides the kernel's checks it refuses, before
/// the insert: a source outside `[a-z][a-z0-9_]{0,31}` or named `outbox` or
/// `cron` (`invalid_source`), a malformed topic (`invalid_topic`) or queue
/// (`invalid_queue`), a delay over 365 days (`invalid_run_at`), and an event
/// whose payload is not the envelope [`OutboxRecord::event`] builds for
/// this source (`invalid_event_envelope`).
///
/// Each record gets a `send {queue}` or `send {topic}` PRODUCER span, a
/// child of the caller's span, whose context (sampled or not) fills the
/// row's `trace_parent`/`trace_state` (TE §2.3, R10). The relay copies it
/// into every job it fans the record out to.
///
/// This form records no metrics; the jobs runtime installs a metered one
/// behind the `Outbox` port, which also counts `merezha.outbox.appended`.
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
        Box::pin(append(tx, source, records, on_duplicate, None))
    }
}

/// [`PgOutbox`] counting `merezha.outbox.appended`: the jobs runtime's
/// `Outbox` port.
#[derive(Clone, Debug)]
pub(crate) struct MeteredOutbox {
    pub metrics: Arc<Metrics>,
}

impl Outbox for MeteredOutbox {
    fn append<'a>(
        &'a self,
        tx: &'a mut Tx<'_>,
        source: &'a str,
        records: &'a [OutboxRecord],
        on_duplicate: OnDuplicate,
    ) -> BoxFuture<'a, Result<Appended, Failure>> {
        Box::pin(append(
            tx,
            source,
            records,
            on_duplicate,
            Some(&self.metrics),
        ))
    }
}

async fn append(
    tx: &mut Tx<'_>,
    source: &str,
    records: &[OutboxRecord],
    on_duplicate: OnDuplicate,
    metrics: Option<&Metrics>,
) -> Result<Appended, Failure> {
    validate_append(records)?;
    validate::source(source)?;
    let now = jiff::Timestamp::now();
    let mut columns = Columns::default();
    for record in records {
        validate::record(source, record, now)?;
    }
    if records.is_empty() {
        return Ok(Appended::default());
    }
    // One PRODUCER span per record, open across the insert.
    let mut spans = Vec::with_capacity(records.len());
    for record in records {
        let span = match &record.target {
            OutboxTarget::Event { topic } => telemetry::send_span(topic.as_str(), None, None),
            OutboxTarget::Job { queue, kind } => {
                telemetry::send_span(queue.as_str(), Some(queue.as_str()), Some(kind.as_str()))
            }
        };
        columns.push(record, telemetry::stored(&span).as_ref())?;
        spans.push(span);
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
                &columns.trace_parent,
                &columns.trace_state,
            ],
        )
        .await
        .map_err(|e| classify(&e))?;
    drop(spans);
    let inserted = usize::try_from(row.get::<_, i64>(0)).unwrap_or(0);
    let duplicates = records.len().saturating_sub(inserted);
    if duplicates > 0 && on_duplicate == OnDuplicate::Fail {
        return Err(Failure::conflict("outbox_duplicate"));
    }
    if let Some(metrics) = metrics {
        // Counted at append: a caller rolling back later still
        // counts (the relay counters count what was committed).
        let events = u64::try_from(row.get::<_, i64>(2)).unwrap_or(0);
        metrics.appended(source, RecordKind::Event, events);
        metrics.appended(
            source,
            RecordKind::Job,
            (inserted as u64).saturating_sub(events),
        );
    }
    Ok(Appended {
        inserted,
        duplicates,
    })
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
    trace_parent: Vec<Option<String>>,
    trace_state: Vec<Option<String>>,
}
impl Columns {
    fn push(
        &mut self,
        record: &OutboxRecord,
        trace: Option<&merezha_telemetry::propagation::StoredContext>,
    ) -> Result<(), Failure> {
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
        let (parent, state) = telemetry::columns(trace);
        self.trace_parent.push(parent);
        self.trace_state.push(state);
        Ok(())
    }
}
