//! The jobs admin operations behind `/v1/jobs/admin/` (AP §D.1) and
//! [`merezha_platform::ports::Jobs::queue_status`]. Every write appends to
//! `merezha_jobs.admin_actions` and logs `job.admin_action`. Payloads are
//! never listed, and a flagged (`contains_secret`) job's payload is never
//! returned (R3).
use crate::PgJobs;
use merezha_domain::{Failure, Identity};
use merezha_platform::ports::{
    MAX_QUARANTINED_LISTED, MAX_QUEUES_LISTED, OutboxSummary, QuarantinedRecord, QueueStatus,
    QueueSummary, Reissue, ReissueRequest,
};
use merezha_postgres::{Tx, classify, tokio_postgres::Row};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

/// Bound on one admin operation.
const ADMIN_DEADLINE: Duration = Duration::from_secs(10);
/// Most jobs one bulk replay touches, and the default.
pub const MAX_BULK_REPLAY: u32 = 1000;
/// Most rows one purge statement deletes, and statements per purge.
const PURGE_BATCH: i64 = 1000;
const MAX_PURGE_BATCHES: usize = 100;
/// Most attempt rows `job` returns.
const MAX_ATTEMPTS_LISTED: i64 = 200;
/// The job states, as the `state` column stores them.
pub const JOB_STATES: [&str; 5] = ["available", "running", "succeeded", "dead", "cancelled"];

fn timestamp(micros: i64) -> jiff::Timestamp {
    jiff::Timestamp::from_microsecond(micros).unwrap_or(jiff::Timestamp::UNIX_EPOCH)
}
fn micros(column: &str) -> String {
    format!("(extract(epoch FROM {column}) * 1000000)::bigint")
}
fn count(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}
fn deadline() -> Instant {
    Instant::now() + ADMIN_DEADLINE
}

/// One job as `GET jobs` lists it: never its payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct JobSummary {
    pub id: i64,
    pub queue: String,
    pub kind: String,
    pub state: String,
    pub attempts: i32,
    pub max_attempts: i32,
    pub run_at: jiff::Timestamp,
    pub last_error_code: Option<String>,
    pub last_error_class: Option<String>,
    pub contains_secret: bool,
    pub created_at: jiff::Timestamp,
    pub finished_at: Option<jiff::Timestamp>,
    pub replays: i32,
}
fn summary_columns() -> String {
    format!(
        "id, queue, kind, state, attempts, max_attempts, {}, last_error_code, last_error_class,
         contains_secret, {}, {}, replays",
        micros("run_at"),
        micros("created_at"),
        micros("finished_at")
    )
}
fn summary(row: &Row) -> JobSummary {
    JobSummary {
        id: row.get(0),
        queue: row.get(1),
        kind: row.get(2),
        state: row.get(3),
        attempts: row.get(4),
        max_attempts: row.get(5),
        run_at: timestamp(row.get(6)),
        last_error_code: row.get(7),
        last_error_class: row.get(8),
        contains_secret: row.get(9),
        created_at: timestamp(row.get(10)),
        finished_at: row.get::<_, Option<i64>>(11).map(timestamp),
        replays: row.get(12),
    }
}

/// One lease of a job, oldest first.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AttemptRecord {
    pub attempt: i32,
    pub started_at: jiff::Timestamp,
    pub finished_at: Option<jiff::Timestamp>,
    /// `succeeded`, `failed`, `lease_expired`, `released`, or none while
    /// the lease is held.
    pub outcome: Option<String>,
    pub error_code: Option<String>,
}

/// `GET jobs/{id}`: the summary, its attempts and, on request with
/// `jobs.admin.payload`, the payload of an unflagged job.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct JobDetail {
    #[serde(flatten)]
    pub job: JobSummary,
    /// Named apart from the `attempts` count of the summary.
    pub attempt_history: Vec<AttemptRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}

/// `GET jobs` filters; `after` is the last id of the previous page.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JobFilter {
    pub queue: Option<String>,
    pub state: Option<String>,
    pub kind: Option<String>,
    /// 1 to 200.
    pub limit: u32,
    pub after: Option<i64>,
}
/// One page of jobs, by id.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct JobPage {
    pub jobs: Vec<JobSummary>,
    /// The cursor of the next page when this one is full.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

/// What a single replay did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Replay {
    /// Back to `available` with `attempts = 0`.
    Replayed,
    /// A flagged dead job's kind reissued its effect; the dead row was
    /// closed as `cancelled` with `replays + 1`.
    Reissued,
}
impl Replay {
    /// The `state` of the D.1 response.
    pub fn state(self) -> &'static str {
        match self {
            Self::Replayed => "available",
            Self::Reissued => "reissued",
        }
    }
}

/// `POST dead/replay` filters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadFilter {
    pub queue: String,
    pub kind: Option<String>,
    pub code: Option<String>,
    /// Dead at or after.
    pub dead_after: Option<jiff::Timestamp>,
    /// Dead before.
    pub dead_before: Option<jiff::Timestamp>,
    /// 1 to [`MAX_BULK_REPLAY`].
    pub limit: u32,
}
/// What a bulk replay did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct BulkReplay {
    pub replayed: u64,
    pub reissued: u64,
    /// Flagged jobs whose kind has no reissue hook on this instance, or
    /// whose hook failed.
    pub refused: u64,
}

/// A flagged dead job claimed for its reissue hook.
struct Claimed {
    id: i64,
    hook: Arc<dyn Reissue>,
    request: ReissueRequest,
}

const CANDIDATE: &str = "id, queue, kind, contains_secret, idempotency_key, correlation_id,
    causation_id, tenant";

async fn audit(
    tx: &mut Tx<'_>,
    principal: Option<&str>,
    action: &str,
    target: &str,
    count: u64,
) -> Result<(), Failure> {
    let count = i32::try_from(count).unwrap_or(i32::MAX);
    tx.execute(
        "INSERT INTO merezha_jobs.admin_actions (principal, action, target, count)
         VALUES ($1, $2, $3, $4)",
        &[&principal, &action, &target, &count],
    )
    .await
    .map_err(|e| classify(&e))?;
    Ok(())
}
/// What an admin write acted on, for `job.admin_action`.
#[derive(Clone, Copy)]
enum Target<'a> {
    Job(i64),
    Queue(&'a str),
}
impl Target<'_> {
    fn audit(self) -> String {
        match self {
            Self::Job(id) => id.to_string(),
            Self::Queue(queue) => queue.to_owned(),
        }
    }
}
fn logged(action: &str, count: u64, target: Target<'_>) {
    match target {
        Target::Job(job_id) => {
            tracing::info!(event = "job.admin_action", action, count, job_id)
        }
        Target::Queue(queue) => {
            tracing::info!(event = "job.admin_action", action, count, queue)
        }
    }
}

impl PgJobs {
    fn hook(&self, kind: &str) -> Option<Arc<dyn Reissue>> {
        self.registrations()
            .into_iter()
            .find(|registration| registration.kind.as_str() == kind)
            .and_then(|registration| registration.options.reissue)
    }
    fn claimable(&self, row: &Row) -> Option<Claimed> {
        let id: i64 = row.get(0);
        let kind: String = row.get(2);
        let hook = self.hook(&kind)?;
        let correlation: String = row.get(5);
        Some(Claimed {
            id,
            hook,
            request: ReissueRequest {
                job_id: id,
                kind: Identity::new(kind).ok()?,
                idempotency_key: row.get(4),
                correlation_id: Identity::new(correlation)
                    .or_else(|_| Identity::new(format!("job_{id}")))
                    .ok()?,
                causation_id: row
                    .get::<_, Option<String>>(6)
                    .and_then(|c| Identity::new(c).ok()),
                tenant: row
                    .get::<_, Option<String>>(7)
                    .and_then(|t| Identity::new(t).ok()),
            },
        })
    }

    /// Call a claimed job's hook; when it fails, the job returns to
    /// `dead` and the hook's failure is returned.
    async fn reissue(&self, claim: Claimed) -> Result<(), Failure> {
        let Err(failure) = claim.hook.reissue(claim.request).await else {
            return Ok(());
        };
        let client = self.database()?.acquire(deadline()).await?;
        client
            .execute(
                "UPDATE merezha_jobs.jobs SET state = 'dead', replays = replays - 1, updated_at = now()
                  WHERE id = $1 AND state = 'cancelled'",
                &[&claim.id],
            )
            .await
            .map_err(|e| classify(&e))?;
        Err(failure)
    }

    /// `GET jobs`: a page of jobs by id, without payloads.
    pub async fn list_jobs(&self, filter: &JobFilter) -> Result<JobPage, Failure> {
        let limit = i64::from(filter.limit.clamp(1, 200));
        let after = filter.after.unwrap_or(0);
        let sql = format!(
            "SELECT {} FROM merezha_jobs.jobs
              WHERE id > $1 AND ($2::text IS NULL OR queue = $2) AND ($3::text IS NULL OR state = $3)
                AND ($4::text IS NULL OR kind = $4)
              ORDER BY id LIMIT $5",
            summary_columns()
        );
        let client = self.database()?.acquire(deadline()).await?;
        let rows = client
            .query(
                &sql,
                &[&after, &filter.queue, &filter.state, &filter.kind, &limit],
            )
            .await
            .map_err(|e| classify(&e))?;
        let jobs: Vec<JobSummary> = rows.iter().map(summary).collect();
        let next = (jobs.len() as i64 == limit)
            .then(|| jobs.last().map(|job| job.id.to_string()))
            .flatten();
        Ok(JobPage { jobs, next })
    }

    /// `GET jobs/{id}`; `payload` adds an unflagged job's payload. Fails
    /// `not_found`.
    pub async fn job(&self, id: i64, payload: bool) -> Result<JobDetail, Failure> {
        let client = self.database()?.acquire(deadline()).await?;
        let row = client
            .query_opt(
                &format!(
                    "SELECT {}, CASE WHEN $2 AND NOT contains_secret THEN payload::text END
                       FROM merezha_jobs.jobs WHERE id = $1",
                    summary_columns()
                ),
                &[&id, &payload],
            )
            .await
            .map_err(|e| classify(&e))?
            .ok_or_else(|| Failure::permanent("not_found"))?;
        let attempts = client
            .query(
                &format!(
                    "SELECT attempt, {}, {}, outcome, error_code FROM merezha_jobs.attempts
                      WHERE job_id = $1 ORDER BY started_at, attempt LIMIT $2",
                    micros("started_at"),
                    micros("finished_at")
                ),
                &[&id, &MAX_ATTEMPTS_LISTED],
            )
            .await
            .map_err(|e| classify(&e))?;
        Ok(JobDetail {
            job: summary(&row),
            attempt_history: attempts
                .iter()
                .map(|a| AttemptRecord {
                    attempt: a.get(0),
                    started_at: timestamp(a.get(1)),
                    finished_at: a.get::<_, Option<i64>>(2).map(timestamp),
                    outcome: a.get(3),
                    error_code: a.get(4),
                })
                .collect(),
            payload: row
                .get::<_, Option<String>>(13)
                .and_then(|text| serde_json::from_str(&text).ok()),
        })
    }

    /// `POST jobs/{id}/replay`: a dead job back to `available` with
    /// `attempts = 0`, `run_at = now()` and `replays + 1`, error fields
    /// kept. A flagged dead job has no payload (R3): its kind's reissue
    /// hook issues the effect again, else conflict `secret_purged`. Fails
    /// `not_found` or conflict `not_dead`.
    pub async fn replay(&self, id: i64, principal: Option<&str>) -> Result<Replay, Failure> {
        let jobs = self.clone();
        let who = principal.map(str::to_owned);
        let claimed = self
            .database()?
            .transaction(deadline(), move |tx| {
                Box::pin(async move {
                    let row = tx
                        .query_opt(
                            &format!(
                                "SELECT {CANDIDATE}, state FROM merezha_jobs.jobs WHERE id = $1 FOR UPDATE"
                            ),
                            &[&id],
                        )
                        .await
                        .map_err(|e| classify(&e))?
                        .ok_or_else(|| Failure::permanent("not_found"))?;
                    if row.get::<_, String>(8) != "dead" {
                        return Err(Failure::conflict("not_dead"));
                    }
                    if !row.get::<_, bool>(3) {
                        let queue: String = row.get(1);
                        tx.execute(
                            "UPDATE merezha_jobs.jobs
                                SET state = 'available', attempts = 0, run_at = now(), finished_at = NULL,
                                    replays = replays + 1, updated_at = now()
                              WHERE id = $1",
                            &[&id],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                        tx.execute("SELECT pg_notify('merezha_jobs', $1)", &[&queue])
                            .await
                            .map_err(|e| classify(&e))?;
                        audit(tx, who.as_deref(), "replay", &id.to_string(), 1).await?;
                        return Ok(None);
                    }
                    let claim = jobs
                        .claimable(&row)
                        .ok_or_else(|| Failure::conflict("secret_purged"))?;
                    tx.execute(
                        "UPDATE merezha_jobs.jobs SET state = 'cancelled', replays = replays + 1,
                                updated_at = now()
                          WHERE id = $1",
                        &[&id],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                    Ok(Some(claim))
                })
            })
            .await?;
        let Some(claim) = claimed else {
            logged("replay", 1, Target::Job(id));
            return Ok(Replay::Replayed);
        };
        self.reissue(claim).await?;
        self.record(principal, "reissue", Target::Job(id), 1)
            .await?;
        Ok(Replay::Reissued)
    }

    async fn record(
        &self,
        principal: Option<&str>,
        action: &'static str,
        target: Target<'_>,
        count: u64,
    ) -> Result<(), Failure> {
        let who = principal.map(str::to_owned);
        let audited = target.audit();
        self.database()?
            .transaction(deadline(), move |tx| {
                Box::pin(async move { audit(tx, who.as_deref(), action, &audited, count).await })
            })
            .await?;
        logged(action, count, target);
        Ok(())
    }

    /// `POST dead/replay`: replay matching dead jobs, oldest id first;
    /// flagged ones go to their kind's reissue hook or are refused.
    pub async fn replay_dead(
        &self,
        filter: &DeadFilter,
        principal: Option<&str>,
    ) -> Result<BulkReplay, Failure> {
        let jobs = self.clone();
        let who = principal.map(str::to_owned);
        let filter = filter.clone();
        let queue = filter.queue.clone();
        let (replayed, refused, claimed) = self
            .database()?
            .transaction(deadline(), move |tx| {
                Box::pin(async move {
                    let after = filter.dead_after.map(|t| t.to_string());
                    let before = filter.dead_before.map(|t| t.to_string());
                    let limit = i64::from(filter.limit.clamp(1, MAX_BULK_REPLAY));
                    let rows = tx
                        .query(
                            &format!(
                                "SELECT {CANDIDATE} FROM merezha_jobs.jobs
                                  WHERE state = 'dead' AND queue = $1
                                    AND ($2::text IS NULL OR kind = $2)
                                    AND ($3::text IS NULL OR last_error_code = $3)
                                    AND ($4::text IS NULL OR finished_at >= $4::text::timestamptz)
                                    AND ($5::text IS NULL OR finished_at < $5::text::timestamptz)
                                  ORDER BY id LIMIT $6 FOR UPDATE SKIP LOCKED"
                            ),
                            &[
                                &filter.queue,
                                &filter.kind,
                                &filter.code,
                                &after,
                                &before,
                                &limit,
                            ],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                    let mut plain = Vec::new();
                    let mut claimed = Vec::new();
                    let mut refused = 0;
                    for row in &rows {
                        if !row.get::<_, bool>(3) {
                            plain.push(row.get::<_, i64>(0));
                        } else if let Some(claim) = jobs.claimable(row) {
                            claimed.push(claim);
                        } else {
                            refused += 1;
                        }
                    }
                    if !plain.is_empty() {
                        tx.execute(
                            "UPDATE merezha_jobs.jobs
                                SET state = 'available', attempts = 0, run_at = now(), finished_at = NULL,
                                    replays = replays + 1, updated_at = now()
                              WHERE id = ANY($1)",
                            &[&plain],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                        tx.execute("SELECT pg_notify('merezha_jobs', $1)", &[&filter.queue])
                            .await
                            .map_err(|e| classify(&e))?;
                    }
                    let ids: Vec<i64> = claimed.iter().map(|claim| claim.id).collect();
                    if !ids.is_empty() {
                        tx.execute(
                            "UPDATE merezha_jobs.jobs SET state = 'cancelled', replays = replays + 1,
                                    updated_at = now()
                              WHERE id = ANY($1)",
                            &[&ids],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                    }
                    let replayed = plain.len() as u64;
                    audit(tx, who.as_deref(), "bulk_replay", &filter.queue, replayed).await?;
                    Ok((replayed, refused, claimed))
                })
            })
            .await?;
        logged("bulk_replay", replayed, Target::Queue(&queue));
        let mut reissued = 0;
        let mut failed = 0;
        for claim in claimed {
            match self.reissue(claim).await {
                Ok(()) => reissued += 1,
                Err(_) => failed += 1,
            }
        }
        if reissued > 0 {
            self.record(principal, "reissue", Target::Queue(&queue), reissued)
                .await?;
        }
        Ok(BulkReplay {
            replayed,
            reissued,
            refused: refused + failed,
        })
    }

    /// `POST dead/purge`: delete the queue's dead jobs that died before
    /// `before`, in batches of 1000. Returns how many.
    pub async fn purge_dead(
        &self,
        queue: &str,
        before: jiff::Timestamp,
        principal: Option<&str>,
    ) -> Result<u64, Failure> {
        let before = before.to_string();
        let mut total = 0;
        for _ in 0..MAX_PURGE_BATCHES {
            let who = principal.map(str::to_owned);
            let queue = queue.to_owned();
            let before = before.clone();
            let deleted = self
                .database()?
                .transaction(deadline(), move |tx| {
                    Box::pin(async move {
                        let deleted = tx
                            .execute(
                                "DELETE FROM merezha_jobs.jobs WHERE id IN (
                                     SELECT id FROM merezha_jobs.jobs
                                      WHERE state = 'dead' AND queue = $1
                                        AND finished_at < $2::text::timestamptz
                                      ORDER BY id LIMIT $3 FOR UPDATE SKIP LOCKED)",
                                &[&queue, &before, &PURGE_BATCH],
                            )
                            .await
                            .map_err(|e| classify(&e))?;
                        if deleted > 0 {
                            audit(tx, who.as_deref(), "purge_dead", &queue, deleted).await?;
                        }
                        Ok(deleted)
                    })
                })
                .await?;
            total += deleted;
            if deleted < PURGE_BATCH as u64 {
                break;
            }
        }
        logged("purge_dead", total, Target::Queue(queue));
        Ok(total)
    }

    /// `POST jobs/{id}/cancel`: an `available` job to `cancelled`, its
    /// flagged payload nulled. Fails `not_found` or conflict
    /// `not_cancellable` when running or terminal.
    pub async fn cancel_job(&self, id: i64, principal: Option<&str>) -> Result<(), Failure> {
        let who = principal.map(str::to_owned);
        self.database()?
            .transaction(deadline(), move |tx| {
                Box::pin(async move {
                    let cancelled = tx
                        .execute(
                            "UPDATE merezha_jobs.jobs
                                SET state = 'cancelled', finished_at = now(), updated_at = now(),
                                    payload = CASE WHEN contains_secret THEN NULL ELSE payload END
                              WHERE id = $1 AND state = 'available'",
                            &[&id],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                    if cancelled == 0 {
                        let exists = tx
                            .query_opt("SELECT 1 FROM merezha_jobs.jobs WHERE id = $1", &[&id])
                            .await
                            .map_err(|e| classify(&e))?;
                        return Err(match exists {
                            None => Failure::permanent("not_found"),
                            Some(_) => Failure::conflict("not_cancellable"),
                        });
                    }
                    audit(tx, who.as_deref(), "cancel", &id.to_string(), 1).await
                })
            })
            .await?;
        logged("cancel", 1, Target::Job(id));
        Ok(())
    }

    /// Queue and outbox state for `GET queues`, `GET outbox` and TE (R10).
    pub(crate) async fn status(&self) -> Result<QueueStatus, Failure> {
        let client = self.database()?.acquire(deadline()).await?;
        let rows = client
            .query(
                "SELECT queue, sum(ready)::bigint, sum(delayed)::bigint, sum(leased)::bigint,
                        sum(dead)::bigint,
                        coalesce(extract(epoch FROM now() - min(oldest)), 0)::float8
                   FROM (SELECT queue, count(*) FILTER (WHERE run_at <= now()) AS ready,
                                count(*) FILTER (WHERE run_at > now()) AS delayed,
                                0::bigint AS leased, 0::bigint AS dead,
                                min(run_at) FILTER (WHERE run_at <= now()) AS oldest
                           FROM merezha_jobs.jobs WHERE state = 'available' GROUP BY queue
                         UNION ALL
                         SELECT queue, 0, 0, count(*), 0, NULL
                           FROM merezha_jobs.jobs WHERE state = 'running' GROUP BY queue
                         UNION ALL
                         SELECT queue, 0, 0, 0, count(*), NULL
                           FROM merezha_jobs.jobs WHERE state = 'dead' GROUP BY queue) AS s
                  GROUP BY queue ORDER BY queue LIMIT $1",
                &[&(MAX_QUEUES_LISTED as i64)],
            )
            .await
            .map_err(|e| classify(&e))?;
        let mut queues: BTreeMap<String, QueueSummary> = BTreeMap::new();
        for plan in self.plans() {
            queues.insert(
                plan.name.clone(),
                QueueSummary {
                    name: plan.name,
                    ready: 0,
                    delayed: 0,
                    leased: 0,
                    dead: 0,
                    oldest_ready_age_s: 0.0,
                    concurrency: plan.concurrency,
                    global_concurrency: plan.global_concurrency,
                },
            );
        }
        for row in &rows {
            let name: String = row.get(0);
            let plan = self.plan(&name);
            let entry = queues.entry(name.clone()).or_insert(QueueSummary {
                name,
                ready: 0,
                delayed: 0,
                leased: 0,
                dead: 0,
                oldest_ready_age_s: 0.0,
                concurrency: plan.concurrency,
                global_concurrency: plan.global_concurrency,
            });
            entry.ready = count(row.get(1));
            entry.delayed = count(row.get(2));
            entry.leased = count(row.get(3));
            entry.dead = count(row.get(4));
            entry.oldest_ready_age_s = row.get::<_, f64>(5).max(0.0);
        }
        let outbox = client
            .query_one(
                "SELECT count(*), coalesce(extract(epoch FROM now() - min(created_at)), 0)::float8
                   FROM merezha_jobs.outbox WHERE relayed_at IS NULL",
                &[],
            )
            .await
            .map_err(|e| classify(&e))?;
        let quarantined = client
            .query(
                &format!(
                    "SELECT id, source, record_kind, quarantine_code, {}
                       FROM merezha_jobs.outbox WHERE quarantined_at IS NOT NULL
                      ORDER BY quarantined_at, id LIMIT $1",
                    micros("quarantined_at")
                ),
                &[&(MAX_QUARANTINED_LISTED as i64)],
            )
            .await
            .map_err(|e| classify(&e))?;
        Ok(QueueStatus {
            queues: queues.into_values().take(MAX_QUEUES_LISTED).collect(),
            outbox: OutboxSummary {
                pending: count(outbox.get(0)),
                oldest_age_s: outbox.get::<_, f64>(1).max(0.0),
                unrouted: self.unrouted(),
                quarantined: quarantined
                    .iter()
                    .map(|row| QuarantinedRecord {
                        id: row.get(0),
                        source: row.get(1),
                        record_kind: row.get(2),
                        quarantine_code: row.get(3),
                        quarantined_at: timestamp(row.get(4)),
                    })
                    .collect(),
            },
        })
    }
}
