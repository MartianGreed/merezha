//! The relay (AP §B.4): unrelayed outbox rows become jobs in one
//! transaction per batch. Event records fan out to subscriptions, job
//! records go to their queue, records the jobs table would refuse are
//! quarantined, and flagged payloads are nulled as rows are marked (R3).
use crate::telemetry::{self, Metrics, tally};
use merezha_domain::{Classification, Failure};
use merezha_platform::config::JobsConfig;
use merezha_postgres::{Database, Statement, classify};
use merezha_telemetry::event;
use std::time::{Duration, Instant};
use tracing::Instrument;

/// The channel dispatchers listen on (AP-B2); the payload is the queue.
pub const JOBS_CHANNEL: &str = "merezha_jobs";
/// Bound on one relay transaction.
const PASS_DEADLINE: Duration = Duration::from_secs(30);

/// The batch: unrelayed records locked by this transaction. `$2`
/// restricts it to one record when a permanent failure is isolated. An
/// empty batch ends the pass without a span (TE §1.2).
pub(crate) const LOCK: Statement = Statement::new(
    "jobs.relay.lock",
    "SELECT merezha_jobs.outbox",
    "SELECT id FROM merezha_jobs.outbox
 WHERE relayed_at IS NULL AND quarantined_at IS NULL AND ($2::bigint IS NULL OR id = $2::bigint)
 ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED",
);

/// The B.4 statement over the locked batch `$5`. Each record's stored
/// trace context goes to every job it fans out to (TE §2.3).
pub(crate) const RELAY: Statement = Statement::new(
    "jobs.relay.batch",
    "INSERT merezha_jobs.jobs",
    r#"
WITH queues (queue, max_attempts) AS (
    SELECT * FROM unnest($2::text[], $3::int[])
), batch AS (
    SELECT id, source, dedupe_key, record_kind, topic, queue, job_kind, priority, run_at, payload,
           contains_secret, correlation_id, causation_id, tenant, trace_parent, trace_state,
           record_kind = 'event'
             OR (queue ~ '^[a-z][a-z0-9_.-]{0,63}$' AND octet_length(job_kind) BETWEEN 1 AND 128
                 AND octet_length(source || ':' || dedupe_key) <= 200) AS routable
      FROM merezha_jobs.outbox
     WHERE relayed_at IS NULL AND quarantined_at IS NULL AND id = ANY($5::bigint[])
     ORDER BY id
     LIMIT $1
     FOR UPDATE
), fanned AS (
    SELECT b.id, s.queue, s.job_kind AS kind, 'outbox:' || b.id || ':' || s.job_kind AS key,
           b.priority, b.run_at, b.payload, b.contains_secret,
           b.correlation_id, b.causation_id, b.tenant, b.trace_parent, b.trace_state
      FROM batch b JOIN merezha_jobs.subscriptions s
                    ON s.topic = b.topic AND s.deactivated_at IS NULL
     WHERE b.record_kind = 'event'
    UNION ALL
    SELECT b.id, b.queue, b.job_kind, b.source || ':' || b.dedupe_key,
           b.priority, b.run_at, b.payload, b.contains_secret,
           b.correlation_id, b.causation_id, b.tenant, b.trace_parent, b.trace_state
      FROM batch b
     WHERE b.record_kind = 'job' AND b.routable
), inserted AS (
    INSERT INTO merezha_jobs.jobs (queue, kind, idempotency_key, payload, payload_digest, contains_secret,
                                   priority, run_at, max_attempts, outbox_id, correlation_id, causation_id,
                                   tenant, trace_parent, trace_state)
    SELECT f.queue, f.kind, f.key, f.payload, sha256(convert_to(f.payload::text, 'UTF8')), f.contains_secret,
           f.priority, coalesce(f.run_at, now()), coalesce(q.max_attempts, $4::int),
           f.id, f.correlation_id, f.causation_id, f.tenant, f.trace_parent, f.trace_state
      FROM fanned f LEFT JOIN queues q ON q.queue = f.queue
    ON CONFLICT (queue, idempotency_key) DO NOTHING
    RETURNING queue, kind
), marked AS (
    UPDATE merezha_jobs.outbox o
       SET relayed_at = CASE WHEN b.routable THEN now() END,
           quarantined_at = CASE WHEN b.routable THEN NULL ELSE now() END,
           quarantine_code = CASE WHEN b.routable THEN NULL ELSE 'unroutable' END,
           payload = CASE WHEN o.contains_secret THEN NULL ELSE o.payload END
      FROM batch b WHERE o.id = b.id
    RETURNING o.id, o.source, b.routable
)
SELECT (SELECT count(*) FROM marked WHERE routable) AS relayed,
       (SELECT count(*) FROM marked WHERE NOT routable) AS quarantined,
       (SELECT count(*) FROM batch b
         WHERE b.record_kind = 'event'
           AND NOT EXISTS (SELECT 1 FROM fanned f WHERE f.id = b.id)) AS unrouted,
       (SELECT array_agg(DISTINCT queue) FROM inserted) AS queues,
       (SELECT array_agg(id ORDER BY id) FROM marked WHERE NOT routable) AS quarantined_ids,
       (SELECT array_agg(source ORDER BY id) FROM marked WHERE NOT routable) AS quarantined_sources,
       (SELECT array_agg(source ORDER BY id) FROM marked WHERE routable) AS relayed_sources,
       (SELECT array_agg(b.source ORDER BY b.id) FROM batch b
         WHERE b.record_kind = 'event'
           AND NOT EXISTS (SELECT 1 FROM fanned f WHERE f.id = b.id)) AS unrouted_sources,
       (SELECT array_agg(queue ORDER BY queue, kind) FROM inserted) AS job_queues,
       (SELECT array_agg(kind ORDER BY queue, kind) FROM inserted) AS job_kinds
"#,
);

pub(crate) const PENDING_IDS: Statement = Statement::new(
    "jobs.relay.pending",
    "SELECT merezha_jobs.outbox",
    "SELECT id FROM merezha_jobs.outbox
 WHERE relayed_at IS NULL AND quarantined_at IS NULL ORDER BY id LIMIT $1",
);

/// Sets one record aside with the code of the data error it caused alone.
/// Its payload stays, flagged or not, so a requeue can relay it once the
/// cause is fixed; R3 nulls it at the resulting job's terminal state.
pub(crate) const QUARANTINE: Statement = Statement::new(
    "jobs.relay.quarantine",
    "UPDATE merezha_jobs.outbox",
    "UPDATE merezha_jobs.outbox
   SET quarantined_at = now(), quarantine_code = $2
 WHERE id = $1 AND relayed_at IS NULL AND quarantined_at IS NULL
 RETURNING source",
);

/// Wakes the dispatchers of every queue a batch enqueued to, on commit.
pub(crate) const NOTIFY_QUEUES: Statement = Statement::new(
    "jobs.relay.notify",
    "SELECT pg_notify()",
    "SELECT pg_notify('merezha_jobs', q) FROM unnest($1::text[]) AS q",
);

/// What the relay resolves from `[jobs]` once at start.
#[derive(Clone, Debug)]
pub(crate) struct RelaySettings {
    pub batch: i64,
    pub interval: Duration,
    queues: Vec<String>,
    max_attempts: Vec<i32>,
    default_max_attempts: i32,
}
impl RelaySettings {
    pub(crate) fn new(config: &JobsConfig) -> Self {
        let attempts = |n: u32| i32::try_from(n).unwrap_or(100);
        Self {
            batch: i64::from(config.relay.batch),
            interval: config.relay.interval,
            queues: config.queues.keys().cloned().collect(),
            max_attempts: config
                .queues
                .values()
                .map(|q| attempts(q.settings.max_attempts))
                .collect(),
            default_max_attempts: attempts(config.defaults.max_attempts),
        }
    }
    /// `max_attempts` of a job on `queue`: its `[jobs.queues.<name>]`
    /// value, else `[jobs.defaults]`.
    pub(crate) fn max_attempts(&self, queue: &str) -> i32 {
        self.queues
            .iter()
            .position(|q| q == queue)
            .map_or(self.default_max_attempts, |at| self.max_attempts[at])
    }
}

/// One relay pass, or several until a batch came back short.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RelayReport {
    /// Records marked relayed, routed or not.
    pub relayed: u64,
    /// Records set aside.
    pub quarantined: u64,
    /// Event records no subscription took, counted in `relayed`.
    pub unrouted: u64,
}
impl RelayReport {
    fn add(&mut self, other: RelayReport) {
        self.relayed += other.relayed;
        self.quarantined += other.quarantined;
        self.unrouted += other.unrouted;
    }
    fn marked(&self) -> u64 {
        self.relayed + self.quarantined
    }
}

/// Relay failures: a data error the batch's rows caused (SQLSTATE classes
/// 22 and 23) is permanent, so the batch is re-run record by record; any
/// other error, a revoked privilege, a broken trigger or a missing table,
/// is systemic and becomes transient, so the relay retries at the interval
/// and quarantines nothing.
fn relay_failure(error: &merezha_postgres::tokio_postgres::Error) -> Failure {
    let failure = classify(error);
    match error.code().map(|code| &code.code()[..2]) {
        Some("23") => Failure::permanent(&failure.code),
        Some("22") => Failure::permanent("invalid_record_data"),
        _ if failure.classification == Classification::Permanent => {
            Failure::transient(&failure.code)
        }
        _ => failure,
    }
}

fn count(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

/// What one committed pass wrote, per module and queue, for the counters.
#[derive(Default)]
struct Tally {
    relayed: Vec<String>,
    unrouted: Vec<String>,
    /// `(source, code)` of records quarantined as `unroutable`.
    quarantined: Vec<(String, String)>,
    /// `(queue, kind)` of every job inserted.
    jobs: Vec<(String, String)>,
}
impl Tally {
    fn record(&self, metrics: &Metrics) {
        for (module, count) in tally(self.relayed.iter().map(String::as_str)) {
            metrics.relayed(module, count);
        }
        for (module, count) in tally(self.unrouted.iter().map(String::as_str)) {
            metrics.unrouted(module, count);
        }
        for (module, code) in &self.quarantined {
            metrics.quarantined(module, code);
        }
        let mut jobs = std::collections::BTreeMap::new();
        for job in &self.jobs {
            *jobs.entry(job).or_insert(0u64) += 1;
        }
        for ((queue, kind), count) in jobs {
            metrics.enqueued(queue, kind, count);
        }
    }
}

/// One batch transaction, optionally for one record. A batch holding
/// records runs in a `relay outbox` span; its counters are recorded once
/// it committed.
async fn pass(
    database: &Database,
    settings: &RelaySettings,
    only: Option<i64>,
    metrics: &Metrics,
) -> Result<RelayReport, Failure> {
    let settings = settings.clone();
    let db = database.clone();
    let (report, tally) = database
        .transaction(Instant::now() + PASS_DEADLINE, move |tx| {
            Box::pin(async move {
                let ids: Vec<i64> = db
                    .query(tx, &LOCK, &[&settings.batch, &only])
                    .await
                    .map_err(|e| relay_failure(&e))?
                    .iter()
                    .map(|row| row.get(0))
                    .collect();
                if ids.is_empty() {
                    return Ok((RelayReport::default(), Tally::default()));
                }
                let span = telemetry::relay_span();
                let relayed = relay_locked(&db, tx, &settings, &ids)
                    .instrument(span.clone())
                    .await;
                if matches!(&relayed, Err(f) if f.classification != Classification::Permanent) {
                    telemetry::failed(&span);
                }
                relayed
            })
        })
        .await?;
    tally.record(metrics);
    Ok(report)
}

/// The relay statement over the locked batch `ids`.
async fn relay_locked(
    db: &Database,
    tx: &mut merezha_postgres::Tx<'_>,
    settings: &RelaySettings,
    ids: &[i64],
) -> Result<(RelayReport, Tally), Failure> {
    let row = db
        .query_one(
            tx,
            &RELAY,
            &[
                &settings.batch,
                &settings.queues,
                &settings.max_attempts,
                &settings.default_max_attempts,
                &ids,
            ],
        )
        .await
        .map_err(|e| relay_failure(&e))?;
    let queues: Option<Vec<String>> = row.get(3);
    if let Some(queues) = queues.filter(|q| !q.is_empty()) {
        db.execute(tx, &NOTIFY_QUEUES, &[&queues])
            .await
            .map_err(|e| relay_failure(&e))?;
    }
    let quarantined_ids: Option<Vec<i64>> = row.get(4);
    let sources: Option<Vec<String>> = row.get(5);
    let mut tally = Tally::default();
    for (id, source) in quarantined_ids
        .unwrap_or_default()
        .iter()
        .zip(sources.unwrap_or_default())
    {
        quarantined_log(*id, &source, "unroutable");
        tally.quarantined.push((source, "unroutable".to_owned()));
    }
    let strings =
        |at: usize| -> Vec<String> { row.get::<_, Option<Vec<String>>>(at).unwrap_or_default() };
    tally.relayed = strings(6);
    tally.unrouted = strings(7);
    tally.jobs = strings(8).into_iter().zip(strings(9)).collect();
    Ok((
        RelayReport {
            relayed: count(row.get(0)),
            quarantined: count(row.get(1)),
            unrouted: count(row.get(2)),
        },
        tally,
    ))
}

fn quarantined_log(id: i64, source: &str, code: &str) {
    event!(WARN, "outbox.quarantined", outbox_id = id, source, code);
}

/// One batch, and whether it was full. A permanent failure of the whole
/// batch re-runs it one record at a time and quarantines each record that
/// fails alone with its code.
async fn batch(
    database: &Database,
    settings: &RelaySettings,
    metrics: &Metrics,
) -> Result<(RelayReport, bool), Failure> {
    match pass(database, settings, None, metrics).await {
        Err(failure) if failure.classification == Classification::Permanent => {}
        Ok(report) => return Ok((report, report.marked() >= count(settings.batch))),
        Err(failure) => return Err(failure),
    }
    let ids: Vec<i64> = {
        let client = database.acquire(Instant::now() + PASS_DEADLINE).await?;
        database
            .query(&client, &PENDING_IDS, &[&settings.batch])
            .await
            .map_err(|e| relay_failure(&e))?
            .iter()
            .map(|row| row.get(0))
            .collect()
    };
    let full = ids.len() >= usize::try_from(settings.batch).unwrap_or(usize::MAX);
    let mut report = RelayReport::default();
    for id in ids {
        match pass(database, settings, Some(id), metrics).await {
            Ok(one) => report.add(one),
            Err(failure) if failure.classification == Classification::Permanent => {
                let code = failure.code;
                let db = database.clone();
                let set_aside = database
                    .transaction(Instant::now() + PASS_DEADLINE, |tx| {
                        let code = code.clone();
                        Box::pin(async move {
                            let row = db
                                .query_opt(tx, &QUARANTINE, &[&id, &code])
                                .await
                                .map_err(|e| relay_failure(&e))?;
                            Ok(row.map(|row| row.get::<_, String>(0)))
                        })
                    })
                    .await?;
                if let Some(source) = set_aside {
                    quarantined_log(id, &source, &code);
                    metrics.quarantined(&source, &code);
                    report.quarantined += 1;
                }
            }
            Err(failure) => return Err(failure),
        }
    }
    Ok((report, full))
}

/// Batches until one comes back short, at most `max_batches`. Returns the
/// report and whether the outbox was drained.
pub(crate) async fn drain(
    database: &Database,
    settings: &RelaySettings,
    max_batches: usize,
    metrics: &Metrics,
) -> Result<(RelayReport, bool), Failure> {
    let mut total = RelayReport::default();
    let mut drained = false;
    for _ in 0..max_batches {
        let (report, full) = batch(database, settings, metrics).await?;
        total.add(report);
        if !full {
            drained = true;
            break;
        }
    }
    if total.marked() > 0 {
        event!(
            DEBUG,
            "outbox.relayed",
            relayed = total.relayed,
            unrouted = total.unrouted
        );
    }
    Ok((total, drained))
}
