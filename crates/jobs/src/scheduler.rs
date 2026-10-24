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

/// One declared schedule, parsed and ready to sync. `Debug` never prints
/// the payload.
#[derive(Clone)]
pub(crate) struct Declared {
    pub name: String,
    pub source: String,
    pub schedule: CronSchedule,
    pub queue: String,
    pub kind: String,
    /// The payload as stored, compact JSON.
    pub payload: String,
}
impl std::fmt::Debug for Declared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Declared")
            .field("name", &self.name)
            .field("source", &self.source)
            .field("schedule", &self.schedule)
            .field("queue", &self.queue)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
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

/// Whether `a` is a strictly greater version than `b`: both dotted
/// numbers (`2`, `1.10.3`; missing parts count as 0), else no precedence.
pub(crate) fn newer(a: Option<&str>, b: Option<&str>) -> bool {
    fn parts(version: &str) -> Option<Vec<u64>> {
        version.split('.').map(|part| part.parse().ok()).collect()
    }
    let (Some(a), Some(b)) = (a.and_then(parts), b.and_then(parts)) else {
        return false;
    };
    let len = a.len().max(b.len());
    let at = |v: &[u64], i: usize| v.get(i).copied().unwrap_or(0);
    (0..len)
        .map(|i| at(&a, i).cmp(&at(&b, i)))
        .find(|order| order.is_ne())
        .is_some_and(|order| order.is_gt())
}

/// How one sync went: the names whose stored definition another
/// declaration kept (logged once per streak by the caller).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SyncReport {
    pub conflicts: Vec<String>,
}

/// The stored side of one declared name, read under the sync lock.
struct Stored {
    same: bool,
    version: Option<String>,
    /// Seconds since anyone declared the stored definition.
    definition_age: f64,
    retired: bool,
}

/// Declare `declared` for an instance of `version` and retire what nobody
/// declared for `retire_after`, in one transaction under the schedules
/// advisory lock. Per name:
///
/// - new: inserted, next run from now;
/// - same definition: refreshed (`last_declared_at`,
///   `definition_declared_at`), the version raised when newer, a retired
///   one reactivated from now;
/// - another definition: it replaces the stored one only when its version
///   is strictly newer, when nobody re-declared the stored one for
///   `2 x heartbeat` (its declarers are gone), or when the row was
///   retired. A due run is kept (it fires once, with the definition current
///   at tick time); otherwise the next run is recomputed from now. Without
///   precedence the stored definition stands, the name stays declared, and
///   it is reported as a conflict.
pub(crate) async fn sync(
    database: &Database,
    declared: Vec<Declared>,
    version: Option<String>,
    heartbeat: Duration,
    retire_after: Duration,
) -> Result<SyncReport, Failure> {
    let retire_after = retire_after.as_secs_f64();
    let grace = (heartbeat * 2).as_secs_f64();
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
                let names: Vec<String> = declared.iter().map(|d| d.name.clone()).collect();
                let definitions: Vec<String> = declared.iter().map(Declared::definition).collect();
                let rows = tx
                    .query(
                        "SELECT d.name, s.definition = sha256(convert_to(d.definition, 'UTF8')),
                                s.declared_version,
                                extract(epoch FROM now() - s.definition_declared_at)::float8,
                                s.retired_at IS NOT NULL
                           FROM unnest($1::text[], $2::text[]) AS d(name, definition)
                           JOIN merezha_jobs.schedules s ON s.name = d.name",
                        &[&names, &definitions],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                let stored: std::collections::BTreeMap<String, Stored> = rows
                    .iter()
                    .map(|row| {
                        (
                            row.get::<_, String>(0),
                            Stored {
                                same: row.get(1),
                                version: row.get(2),
                                definition_age: row.get(3),
                                retired: row.get(4),
                            },
                        )
                    })
                    .collect();
                let mut report = SyncReport::default();
                for (schedule, definition) in declared.iter().zip(&definitions) {
                    let next = schedule.schedule.next_after(now)?.to_string();
                    let row = stored.get(&schedule.name);
                    let replace = match row {
                        None => {
                            tx.execute(
                                "INSERT INTO merezha_jobs.schedules
                                     (name, source, cron, timezone, queue, kind, payload, next_run_at,
                                      definition, declared_version)
                                 VALUES ($1, $2, $3, $4, $5, $6, $7::text::json, $8::text::timestamptz,
                                         sha256(convert_to($9::text, 'UTF8')), $10)
                                 ON CONFLICT (name) DO NOTHING",
                                &[
                                    &schedule.name,
                                    &schedule.source,
                                    &schedule.schedule.expression(),
                                    &schedule.schedule.timezone(),
                                    &schedule.queue,
                                    &schedule.kind,
                                    &schedule.payload,
                                    &next,
                                    definition,
                                    &version,
                                ],
                            )
                            .await
                            .map_err(|e| classify(&e))?;
                            continue;
                        }
                        Some(row) if row.same => {
                            let raised = if row.version.is_none()
                                || newer(version.as_deref(), row.version.as_deref())
                            {
                                version.clone()
                            } else {
                                row.version.clone()
                            };
                            tx.execute(
                                "UPDATE merezha_jobs.schedules
                                    SET source = $2, declared_version = $3,
                                        last_declared_at = now(), definition_declared_at = now(),
                                        next_run_at = CASE WHEN retired_at IS NOT NULL
                                                           THEN $4::text::timestamptz ELSE next_run_at END,
                                        updated_at = CASE WHEN retired_at IS NOT NULL
                                                          THEN now() ELSE updated_at END,
                                        retired_at = NULL
                                  WHERE name = $1",
                                &[&schedule.name, &schedule.source, &raised, &next],
                            )
                            .await
                            .map_err(|e| classify(&e))?;
                            continue;
                        }
                        Some(row) => {
                            row.retired
                                || newer(version.as_deref(), row.version.as_deref())
                                || row.definition_age > grace
                        }
                    };
                    if replace {
                        tx.execute(
                            "UPDATE merezha_jobs.schedules
                                SET source = $2, cron = $3, timezone = $4, queue = $5, kind = $6,
                                    payload = $7::text::json,
                                    definition = sha256(convert_to($9::text, 'UTF8')),
                                    declared_version = $10,
                                    next_run_at = CASE WHEN retired_at IS NULL AND next_run_at <= now()
                                                       THEN next_run_at ELSE $8::text::timestamptz END,
                                    last_declared_at = now(), definition_declared_at = now(),
                                    retired_at = NULL, updated_at = now()
                              WHERE name = $1",
                            &[
                                &schedule.name,
                                &schedule.source,
                                &schedule.schedule.expression(),
                                &schedule.schedule.timezone(),
                                &schedule.queue,
                                &schedule.kind,
                                &schedule.payload,
                                &next,
                                definition,
                                &version,
                            ],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                    } else {
                        // Still declared, so never retired; the stored
                        // definition stands.
                        tx.execute(
                            "UPDATE merezha_jobs.schedules SET last_declared_at = now() WHERE name = $1",
                            &[&schedule.name],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                        report.conflicts.push(schedule.name.clone());
                    }
                }
                tx.execute(
                    "UPDATE merezha_jobs.schedules SET retired_at = now(), updated_at = now()
                      WHERE retired_at IS NULL
                        AND last_declared_at < now() - make_interval(secs => $1)",
                    &[&retire_after],
                )
                .await
                .map_err(|e| classify(&e))?;
                Ok(report)
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
/// [`MAX_TICK_BATCHES`] of them. A due row this version cannot schedule
/// (a definition another version wrote, or one that never fires again) is
/// left due, kept out of this tick's later batches so it cannot starve the
/// rest, and logged once per streak: `unschedulable` holds the names
/// skipped by the previous tick.
pub(crate) async fn tick(
    database: &Database,
    settings: &RelaySettings,
    unschedulable: &std::sync::Mutex<std::collections::BTreeSet<String>>,
) -> Result<TickReport, Failure> {
    let mut report = TickReport::default();
    let mut skipped: Vec<String> = Vec::new();
    for _ in 0..MAX_TICK_BATCHES {
        let (batch, taken, names) = tick_batch(database, settings, skipped.clone()).await?;
        report.add(batch);
        skipped.extend(names);
        // A full batch may hide more due rows behind it; skipped ones are
        // excluded from the next, so every batch makes progress.
        if taken < TICK_BATCH as usize {
            break;
        }
    }
    let mut previous = unschedulable
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for name in skipped.iter().filter(|name| !previous.contains(*name)) {
        tracing::warn!(
            event = "job.worker_failed",
            task = "scheduler",
            code = "invalid_cron",
            classification = "permanent",
            schedule = name.as_str()
        );
    }
    *previous = skipped.into_iter().collect();
    Ok(report)
}

async fn tick_batch(
    database: &Database,
    settings: &RelaySettings,
    excluded: Vec<String>,
) -> Result<(TickReport, usize, Vec<String>), Failure> {
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
                    return Ok((TickReport::default(), 0, Vec::new()));
                }
                let rows = tx
                    .query(
                        "SELECT name, cron, timezone, queue, kind, payload::text,
                                (extract(epoch FROM next_run_at) * 1000000)::bigint,
                                (extract(epoch FROM now()) * 1000000)::bigint
                           FROM merezha_jobs.schedules
                          WHERE enabled AND retired_at IS NULL AND next_run_at <= now()
                            AND NOT (name = ANY($2::text[]))
                          ORDER BY next_run_at
                          LIMIT $1
                          FOR UPDATE SKIP LOCKED",
                        &[&TICK_BATCH, &excluded],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                let mut report = TickReport::default();
                let mut woken: Vec<String> = Vec::new();
                let mut skipped = Vec::new();
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
                        skipped.push(name);
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
                Ok((report, rows.len(), skipped))
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
    /// `cron:{name}:manual:{key}`, the key the caller gave or `unix_ms`.
    pub idempotency_key: String,
    /// `false`: a job with this key existed and was returned instead.
    pub inserted: bool,
}

/// A caller's trigger key (the `Idempotency-Key` header): 1 to 120 bytes
/// of `[A-Za-z0-9._:-]`, so the job key stays within 200 bytes.
pub fn valid_trigger_key(key: &str) -> bool {
    (1..=120).contains(&key.len())
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
}

/// `POST schedules/{name}/trigger`: one job now, keyed
/// `cron:{name}:manual:{key}` with the caller's key (a retry returns the
/// same job, `inserted = false`) or `unix_ms` without one. A paused
/// schedule can be triggered; a retired one fails conflict
/// `schedule_retired`. `next_run_at` is unchanged. Audited.
pub(crate) async fn trigger(
    database: &Database,
    settings: &RelaySettings,
    name: &str,
    key: Option<&str>,
    principal: Option<&str>,
) -> Result<Triggered, Failure> {
    if key.is_some_and(|key| !valid_trigger_key(key)) {
        return Err(Failure::permanent("validation_failed"));
    }
    let given = key.map(str::to_owned);
    let name = name.to_owned();
    let principal = principal.map(str::to_owned);
    let settings = settings.clone();
    let triggered = database
        .transaction(deadline(), move |tx| {
            Box::pin(async move {
                let row = tx
                    .query_opt(
                        "SELECT queue, kind, payload::text,
                                (extract(epoch FROM now()) * 1000000)::bigint, retired_at IS NOT NULL
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
                if row.get::<_, bool>(4) {
                    return Err(Failure::conflict("schedule_retired"));
                }
                let key = match &given {
                    Some(given) => format!("cron:{name}:manual:{given}"),
                    None => format!("cron:{name}:manual:{}", now.as_millisecond()),
                };
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
                    inserted,
                })
            })
        })
        .await?;
    logged("schedule_trigger", &triggered.name);
    Ok(triggered)
}

#[cfg(test)]
mod tests {
    use super::newer;

    #[test]
    fn versions_take_precedence_only_when_both_are_dotted_numbers() {
        for (a, b) in [
            ("2", "1"),
            ("1.10", "1.9"),
            ("1.2.1", "1.2"),
            ("10.0.0", "9.99.99"),
        ] {
            assert!(newer(Some(a), Some(b)), "{a} > {b}");
            assert!(!newer(Some(b), Some(a)), "{b} < {a}");
        }
        for (a, b) in [
            ("1.2", "1.2.0"),
            ("1.2.0", "1.2"),
            ("2.0.0-rc1", "1.0"),
            ("2", "v1"),
            ("", "1"),
        ] {
            assert!(!newer(Some(a), Some(b)), "no precedence: {a} vs {b}");
        }
        assert!(!newer(Some("2"), None));
        assert!(!newer(None, Some("1")));
        assert!(!newer(None, None));
    }
}
