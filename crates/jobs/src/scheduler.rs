//! Cron (AP §B.7): schedules declared in `[[jobs.schedules]]` (source
//! `config`) and by modules through `JobsHandle::schedule` (source = the
//! module name), synced into `merezha_jobs.schedules`, and the scheduler
//! tick that turns each due occurrence into exactly one job.
//!
//! **Sync.** At start and on every `jobs.subscriptions.heartbeat`, each
//! instance upserts the schedules it declares (`last_declared_at = now()`,
//! a retired one reactivated) and retires those nobody declared for
//! `jobs.subscriptions.retire_after`, the rolling-deploy rule
//! subscriptions follow: a schedule a newer version declares survives an
//! older instance's restart. A changed definition digest (cron, zone,
//! queue, kind, payload) recomputes `next_run_at` from now, and so does a
//! reactivation. The whole sync runs in one transaction under
//! `pg_advisory_xact_lock(MZJB, hashtext('schedules'))`; pooled
//! connections take no session locks.
//!
//! **Tick.** Every `jobs.scheduler.interval_ms` (and once at start), any
//! instance may tick: under `pg_try_advisory_xact_lock(MZJB,
//! hashtext('scheduler'))` (an instance that finds it taken skips this
//! tick), it locks due rows with `FOR UPDATE SKIP LOCKED` and, per row,
//! inserts the job keyed `cron:{name}:{epoch seconds of the occurrence}`
//! with `ON CONFLICT DO NOTHING`, then moves `next_run_at` to the first
//! occurrence strictly after `max(occurrence, now)`. A downtime spanning
//! several occurrences therefore runs one job, then the normal cadence
//! (the missed-run policy). Times come from the database clock.
use crate::{admin::audit, relay::RelaySettings};
use merezha_domain::{Failure, compatible_json};
use merezha_platform::{
    config::{JobSchedule, valid_queue_name, valid_schedule_name},
    cron::CronSchedule,
    ports::{MAX_PAYLOAD_BYTES, ScheduleDeclaration},
};
use merezha_postgres::{Database, classify};
use serde::Serialize;
use std::time::{Duration, Instant};

/// Most schedules one instance declares (config and modules together).
pub const MAX_SCHEDULES: usize = 256;
/// Due rows one tick transaction takes.
const TICK_BATCH: i64 = 32;
/// Transactions one tick runs before it waits for the next interval.
const MAX_TICK_BATCHES: usize = 8;
/// Most schedules `GET schedules` lists per page.
pub const MAX_SCHEDULES_LISTED: u32 = 200;
/// Bound on a sync, a tick batch and an admin operation.
const DEADLINE: Duration = Duration::from_secs(10);
/// The source of `[[jobs.schedules]]` rows.
pub const CONFIG_SOURCE: &str = "config";

fn deadline() -> Instant {
    Instant::now() + DEADLINE
}
fn timestamp(micros: i64) -> jiff::Timestamp {
    jiff::Timestamp::from_microsecond(micros).unwrap_or(jiff::Timestamp::UNIX_EPOCH)
}

/// One declared schedule, parsed and ready to sync.
#[derive(Clone, Debug)]
pub(crate) struct Declared {
    pub name: String,
    pub source: String,
    pub schedule: CronSchedule,
    pub queue: String,
    pub kind: String,
    /// The payload as stored, compact JSON.
    pub payload: String,
}
impl Declared {
    /// What the digest covers: everything that decides which jobs run.
    fn definition(&self) -> String {
        let payload: serde_json::Value =
            serde_json::from_str(&self.payload).unwrap_or(serde_json::Value::Null);
        serde_json::json!([
            self.schedule.expression(),
            self.schedule.timezone(),
            self.queue,
            self.kind,
            payload,
        ])
        .to_string()
    }
}

fn payload_text(payload: &serde_json::Value) -> Result<String, Failure> {
    compatible_json(payload)?;
    let text =
        serde_json::to_string(payload).map_err(|_| Failure::permanent("job_payload_limit"))?;
    if text.len() > MAX_PAYLOAD_BYTES {
        return Err(Failure::permanent("job_payload_limit"));
    }
    Ok(text)
}
fn parse(cron: &str, timezone: &str) -> Result<CronSchedule, Failure> {
    CronSchedule::parse(cron, timezone).map_err(|problems| {
        Failure::permanent(problems.first().map_or("invalid_cron", |p| p.code()))
    })
}

/// A `[[jobs.schedules]]` entry; config validation already parsed it.
pub(crate) fn from_config(entry: &JobSchedule) -> Result<Declared, Failure> {
    from_parts(
        CONFIG_SOURCE,
        &entry.name,
        &entry.cron,
        &entry.timezone,
        &entry.queue,
        &entry.kind,
        &entry.payload,
    )
}
/// A module's declaration. Fails permanent `invalid_schedule_name`,
/// `invalid_cron`, `invalid_timezone`, `unknown_timezone`,
/// `invalid_queue`, `invalid_number` or `job_payload_limit`.
pub(crate) fn from_module(
    source: &str,
    schedule: &ScheduleDeclaration,
) -> Result<Declared, Failure> {
    from_parts(
        source,
        &schedule.name,
        &schedule.cron,
        &schedule.timezone,
        schedule.queue.as_str(),
        schedule.kind.as_str(),
        &schedule.payload,
    )
}
fn from_parts(
    source: &str,
    name: &str,
    cron: &str,
    timezone: &str,
    queue: &str,
    kind: &str,
    payload: &serde_json::Value,
) -> Result<Declared, Failure> {
    if !valid_schedule_name(name) {
        return Err(Failure::permanent("invalid_schedule_name"));
    }
    let schedule = parse(cron, timezone)?;
    if !valid_queue_name(queue) {
        return Err(Failure::permanent("invalid_queue"));
    }
    if !(1..=128).contains(&kind.len()) {
        return Err(Failure::permanent("validation_failed"));
    }
    Ok(Declared {
        name: name.to_owned(),
        source: source.to_owned(),
        schedule,
        queue: queue.to_owned(),
        kind: kind.to_owned(),
        payload: payload_text(payload)?,
    })
}

/// Declare `declared` and retire what nobody declared for `retire_after`,
/// in one transaction under the schedules advisory lock.
pub(crate) async fn sync(
    database: &Database,
    declared: Vec<Declared>,
    retire_after: Duration,
) -> Result<(), Failure> {
    let retire_after = retire_after.as_secs_f64();
    database
        .transaction(deadline(), move |tx| {
            Box::pin(async move {
                tx.execute(
                    "SELECT pg_advisory_xact_lock(1297762882, hashtext('schedules'))",
                    &[],
                )
                .await
                .map_err(|e| classify(&e))?;
                let now: i64 = tx
                    .query_one("SELECT (extract(epoch FROM now()) * 1000000)::bigint", &[])
                    .await
                    .map_err(|e| classify(&e))?
                    .get(0);
                let now = timestamp(now);
                let mut columns: [Vec<String>; 9] = Default::default();
                for schedule in &declared {
                    let next = schedule.schedule.next_after(now)?;
                    for (column, value) in columns.iter_mut().zip([
                        schedule.name.clone(),
                        schedule.source.clone(),
                        schedule.schedule.expression().to_owned(),
                        schedule.schedule.timezone().to_owned(),
                        schedule.queue.clone(),
                        schedule.kind.clone(),
                        schedule.payload.clone(),
                        next.to_string(),
                        schedule.definition(),
                    ]) {
                        column.push(value);
                    }
                }
                let [names, sources, crons, zones, queues, kinds, payloads, nexts, definitions] =
                    &columns;
                tx.execute(
                    "INSERT INTO merezha_jobs.schedules
                         (name, source, cron, timezone, queue, kind, payload, next_run_at, definition)
                     SELECT d.name, d.source, d.cron, d.timezone, d.queue, d.kind, d.payload::json,
                            d.next::timestamptz, sha256(convert_to(d.definition, 'UTF8'))
                       FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[],
                                   $6::text[], $7::text[], $8::text[], $9::text[])
                            AS d(name, source, cron, timezone, queue, kind, payload, next, definition)
                     ON CONFLICT (name) DO UPDATE SET
                         source = excluded.source, cron = excluded.cron, timezone = excluded.timezone,
                         queue = excluded.queue, kind = excluded.kind, payload = excluded.payload,
                         next_run_at = CASE
                             WHEN schedules.definition <> excluded.definition
                               OR schedules.retired_at IS NOT NULL
                             THEN excluded.next_run_at ELSE schedules.next_run_at END,
                         updated_at = CASE
                             WHEN schedules.definition <> excluded.definition
                               OR schedules.retired_at IS NOT NULL
                             THEN now() ELSE schedules.updated_at END,
                         definition = excluded.definition,
                         last_declared_at = now(), retired_at = NULL",
                    &[names, sources, crons, zones, queues, kinds, payloads, nexts, definitions],
                )
                .await
                .map_err(|e| classify(&e))?;
                tx.execute(
                    "UPDATE merezha_jobs.schedules SET retired_at = now(), updated_at = now()
                      WHERE retired_at IS NULL
                        AND last_declared_at < now() - make_interval(secs => $1)",
                    &[&retire_after],
                )
                .await
                .map_err(|e| classify(&e))?;
                Ok(())
            })
        })
        .await
}

/// What one scheduler tick did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TickReport {
    /// Occurrences that enqueued a new job.
    pub fired: u64,
    /// Occurrences whose job already existed (nothing enqueued).
    pub duplicates: u64,
    /// Due rows this instance could not schedule: a definition it cannot
    /// parse (written by another version) or one that never fires again.
    pub skipped: u64,
}
impl TickReport {
    fn add(&mut self, other: TickReport) {
        self.fired += other.fired;
        self.duplicates += other.duplicates;
        self.skipped += other.skipped;
    }
}

/// Enqueue every due occurrence: batches of [`TICK_BATCH`] rows, at most
/// [`MAX_TICK_BATCHES`] of them.
pub(crate) async fn tick(
    database: &Database,
    settings: &RelaySettings,
) -> Result<TickReport, Failure> {
    let mut report = TickReport::default();
    for _ in 0..MAX_TICK_BATCHES {
        let (batch, taken) = tick_batch(database, settings).await?;
        report.add(batch);
        if taken < TICK_BATCH as usize || batch.fired + batch.duplicates == 0 {
            break;
        }
    }
    Ok(report)
}

async fn tick_batch(
    database: &Database,
    settings: &RelaySettings,
) -> Result<(TickReport, usize), Failure> {
    let settings = settings.clone();
    database
        .transaction(deadline(), move |tx| {
            Box::pin(async move {
                // One ticking instance at a time; the others find the lock
                // taken and wait for their next interval. A transaction
                // lock, so a cancelled tick releases it with its rollback.
                let ours: bool = tx
                    .query_one(
                        "SELECT pg_try_advisory_xact_lock(1297762882, hashtext('scheduler'))",
                        &[],
                    )
                    .await
                    .map_err(|e| classify(&e))?
                    .get(0);
                if !ours {
                    return Ok((TickReport::default(), 0));
                }
                let rows = tx
                    .query(
                        "SELECT name, cron, timezone, queue, kind, payload::text,
                                (extract(epoch FROM next_run_at) * 1000000)::bigint,
                                (extract(epoch FROM now()) * 1000000)::bigint
                           FROM merezha_jobs.schedules
                          WHERE enabled AND retired_at IS NULL AND next_run_at <= now()
                          ORDER BY next_run_at
                          LIMIT $1
                          FOR UPDATE SKIP LOCKED",
                        &[&TICK_BATCH],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                let mut report = TickReport::default();
                let mut woken: Vec<String> = Vec::new();
                for row in &rows {
                    let name: String = row.get(0);
                    let queue: String = row.get(3);
                    let kind: String = row.get(4);
                    let payload: String = row.get(5);
                    let scheduled = timestamp(row.get(6));
                    let now = timestamp(row.get(7));
                    let next = CronSchedule::parse(row.get(1), row.get(2))
                        .map_err(|_| Failure::permanent("invalid_cron"))
                        .and_then(|schedule| schedule.next_after(scheduled.max(now)));
                    let Ok(next) = next else {
                        report.skipped += 1;
                        tracing::warn!(
                            event = "job.worker_failed",
                            task = "scheduler",
                            code = "invalid_cron",
                            classification = "permanent",
                            schedule = name.as_str()
                        );
                        continue;
                    };
                    let key = format!("cron:{name}:{}", scheduled.as_second());
                    let (id, inserted) = insert_job(
                        tx,
                        &queue,
                        &kind,
                        &key,
                        &payload,
                        settings.max_attempts(&queue),
                        &name,
                        scheduled,
                    )
                    .await?;
                    tx.execute(
                        "UPDATE merezha_jobs.schedules
                            SET last_run_at = next_run_at, last_job_id = $2,
                                next_run_at = $3::text::timestamptz, updated_at = now()
                          WHERE name = $1",
                        &[&name, &id, &next.to_string()],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                    if inserted {
                        report.fired += 1;
                        if !woken.contains(&queue) {
                            woken.push(queue.clone());
                        }
                        tracing::debug!(
                            event = "job.schedule_fired",
                            schedule = name.as_str(),
                            job_id = id,
                            scheduled_for = %scheduled
                        );
                    } else {
                        report.duplicates += 1;
                    }
                }
                for queue in &woken {
                    tx.execute("SELECT pg_notify('merezha_jobs', $1)", &[queue])
                        .await
                        .map_err(|e| classify(&e))?;
                }
                Ok((report, rows.len()))
            })
        })
        .await
}

/// Insert one scheduled job, or find the one with its key. Returns its id
/// and whether this call inserted it.
#[allow(clippy::too_many_arguments)]
async fn insert_job(
    tx: &mut merezha_postgres::Tx<'_>,
    queue: &str,
    kind: &str,
    key: &str,
    payload: &str,
    max_attempts: i32,
    schedule: &str,
    scheduled_for: jiff::Timestamp,
) -> Result<(i64, bool), Failure> {
    let inserted = tx
        .query_opt(
            "INSERT INTO merezha_jobs.jobs (queue, kind, idempotency_key, payload, payload_digest,
                 max_attempts, schedule_name, scheduled_for, correlation_id)
             VALUES ($1, $2, $3, $4::text::json, sha256(convert_to($4::text, 'UTF8')), $5, $6,
                 $7::text::timestamptz, $3)
             ON CONFLICT (queue, idempotency_key) DO NOTHING
             RETURNING id",
            &[
                &queue,
                &kind,
                &key,
                &payload,
                &max_attempts,
                &schedule,
                &scheduled_for.to_string(),
            ],
        )
        .await
        .map_err(|e| classify(&e))?;
    if let Some(row) = inserted {
        return Ok((row.get(0), true));
    }
    // A new statement sees a conflicting row another transaction committed.
    let existing = tx
        .query_opt(
            "SELECT id FROM merezha_jobs.jobs WHERE queue = $1 AND idempotency_key = $2",
            &[&queue, &key],
        )
        .await
        .map_err(|e| classify(&e))?;
    match existing {
        Some(row) => Ok((row.get(0), false)),
        None => Err(Failure::conflict("database_conflict")),
    }
}

/// One schedule as `GET schedules` lists it, without its payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ScheduleSummary {
    pub name: String,
    /// `config` or the declaring module's name.
    pub source: String,
    pub cron: String,
    pub timezone: String,
    pub queue: String,
    pub kind: String,
    /// `false` while paused through the admin API.
    pub enabled: bool,
    /// Set once nobody declared it for `jobs.subscriptions.retire_after`;
    /// a retired schedule never fires until declared again.
    pub retired_at: Option<jiff::Timestamp>,
    pub next_run_at: jiff::Timestamp,
    /// The occurrence the last scheduled job was for.
    pub last_run_at: Option<jiff::Timestamp>,
    pub last_job_id: Option<i64>,
}
/// A page of [`ScheduleSummary`], by name.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SchedulePage {
    pub schedules: Vec<ScheduleSummary>,
    /// The cursor of the next page (the last name), when there may be one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

const SUMMARY: &str = "name, source, cron, timezone, queue, kind, enabled,
    (extract(epoch FROM retired_at) * 1000000)::bigint,
    (extract(epoch FROM next_run_at) * 1000000)::bigint,
    (extract(epoch FROM last_run_at) * 1000000)::bigint, last_job_id";

fn summary(row: &merezha_postgres::tokio_postgres::Row) -> ScheduleSummary {
    ScheduleSummary {
        name: row.get(0),
        source: row.get(1),
        cron: row.get(2),
        timezone: row.get(3),
        queue: row.get(4),
        kind: row.get(5),
        enabled: row.get(6),
        retired_at: row.get::<_, Option<i64>>(7).map(timestamp),
        next_run_at: timestamp(row.get(8)),
        last_run_at: row.get::<_, Option<i64>>(9).map(timestamp),
        last_job_id: row.get(10),
    }
}

/// Schedules by name after `after`, at most `limit` (1 to
/// [`MAX_SCHEDULES_LISTED`]).
pub(crate) async fn list(
    database: &Database,
    after: Option<&str>,
    limit: u32,
) -> Result<SchedulePage, Failure> {
    let limit = i64::from(limit.clamp(1, MAX_SCHEDULES_LISTED));
    let after = after.unwrap_or("").to_owned();
    let client = database.acquire(deadline()).await?;
    let rows = client
        .query(
            &format!(
                "SELECT {SUMMARY} FROM merezha_jobs.schedules WHERE name > $1 ORDER BY name LIMIT $2"
            ),
            &[&after, &limit],
        )
        .await
        .map_err(|e| classify(&e))?;
    let schedules: Vec<ScheduleSummary> = rows.iter().map(summary).collect();
    let next = (schedules.len() as i64 == limit)
        .then(|| schedules.last().map(|s| s.name.clone()))
        .flatten();
    Ok(SchedulePage { schedules, next })
}

fn logged(action: &str, schedule: &str) {
    tracing::info!(event = "job.admin_action", action, count = 1, schedule);
}

/// `POST schedules/{name}/pause`: `enabled = false`, with an audit row.
pub(crate) async fn pause(
    database: &Database,
    name: &str,
    principal: Option<&str>,
) -> Result<ScheduleSummary, Failure> {
    let name = name.to_owned();
    let principal = principal.map(str::to_owned);
    let summary = database
        .transaction(deadline(), move |tx| {
            Box::pin(async move {
                let row = tx
                    .query_opt(
                        &format!(
                            "UPDATE merezha_jobs.schedules SET enabled = false, updated_at = now()
                              WHERE name = $1 RETURNING {SUMMARY}"
                        ),
                        &[&name],
                    )
                    .await
                    .map_err(|e| classify(&e))?
                    .ok_or_else(|| Failure::permanent("not_found"))?;
                audit(tx, principal.as_deref(), "schedule_pause", &name, 1).await?;
                Ok(summary(&row))
            })
        })
        .await?;
    logged("schedule_pause", &summary.name);
    Ok(summary)
}

/// `POST schedules/{name}/resume`: a paused schedule is enabled with
/// `next_run_at` recomputed from now, so the occurrences it missed while
/// paused never run; an enabled one is left as it is. Audited either way.
pub(crate) async fn resume(
    database: &Database,
    name: &str,
    principal: Option<&str>,
) -> Result<ScheduleSummary, Failure> {
    let name = name.to_owned();
    let principal = principal.map(str::to_owned);
    let summary = database
        .transaction(deadline(), move |tx| {
            Box::pin(async move {
                let row = tx
                    .query_opt(
                        "SELECT cron, timezone, enabled,
                                (extract(epoch FROM now()) * 1000000)::bigint
                           FROM merezha_jobs.schedules WHERE name = $1 FOR UPDATE",
                        &[&name],
                    )
                    .await
                    .map_err(|e| classify(&e))?
                    .ok_or_else(|| Failure::permanent("not_found"))?;
                if !row.get::<_, bool>(2) {
                    let next = parse(row.get(0), row.get(1))?.next_after(timestamp(row.get(3)))?;
                    tx.execute(
                        "UPDATE merezha_jobs.schedules
                            SET enabled = true, next_run_at = $2::text::timestamptz, updated_at = now()
                          WHERE name = $1",
                        &[&name, &next.to_string()],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                }
                audit(tx, principal.as_deref(), "schedule_resume", &name, 1).await?;
                let row = tx
                    .query_one(
                        &format!("SELECT {SUMMARY} FROM merezha_jobs.schedules WHERE name = $1"),
                        &[&name],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                Ok(summary(&row))
            })
        })
        .await?;
    logged("schedule_resume", &summary.name);
    Ok(summary)
}

/// What `POST schedules/{name}/trigger` enqueued.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Triggered {
    pub name: String,
    pub job_id: i64,
    /// `cron:{name}:manual:{unix_ms}`.
    pub idempotency_key: String,
}

/// `POST schedules/{name}/trigger`: one job now, keyed
/// `cron:{name}:manual:{unix_ms}`, whether the schedule is paused or
/// retired or not; `next_run_at` is unchanged. Audited.
pub(crate) async fn trigger(
    database: &Database,
    settings: &RelaySettings,
    name: &str,
    principal: Option<&str>,
) -> Result<Triggered, Failure> {
    let name = name.to_owned();
    let principal = principal.map(str::to_owned);
    let settings = settings.clone();
    let triggered = database
        .transaction(deadline(), move |tx| {
            Box::pin(async move {
                let row = tx
                    .query_opt(
                        "SELECT queue, kind, payload::text,
                                (extract(epoch FROM now()) * 1000000)::bigint
                           FROM merezha_jobs.schedules WHERE name = $1",
                        &[&name],
                    )
                    .await
                    .map_err(|e| classify(&e))?
                    .ok_or_else(|| Failure::permanent("not_found"))?;
                let queue: String = row.get(0);
                let kind: String = row.get(1);
                let payload: String = row.get(2);
                let now = timestamp(row.get(3));
                let key = format!("cron:{name}:manual:{}", now.as_millisecond());
                let (job_id, inserted) = insert_job(
                    tx,
                    &queue,
                    &kind,
                    &key,
                    &payload,
                    settings.max_attempts(&queue),
                    &name,
                    now,
                )
                .await?;
                if inserted {
                    tx.execute("SELECT pg_notify('merezha_jobs', $1)", &[&queue])
                        .await
                        .map_err(|e| classify(&e))?;
                }
                audit(tx, principal.as_deref(), "schedule_trigger", &name, 1).await?;
                Ok(Triggered {
                    name,
                    job_id,
                    idempotency_key: key,
                })
            })
        })
        .await?;
    logged("schedule_trigger", &triggered.name);
    Ok(triggered)
}
