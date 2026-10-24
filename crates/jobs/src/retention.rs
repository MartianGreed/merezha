//! Retention (AP §B.9): hourly purges in batches of 1000, each batch's
//! transaction under `pg_try_advisory_xact_lock(MZJB, hashtext('retention'))`
//! so one instance purges at a time. Dead jobs are purged with an
//! `admin_actions` row (F, "Dead letters").
use merezha_domain::Failure;
use merezha_platform::config::JobRetention;
use merezha_postgres::{Database, classify};
use std::time::{Duration, Instant};

/// How often a pass runs.
pub const RETENTION_INTERVAL: Duration = Duration::from_secs(3600);
/// Rows per batch, and batches per kind of row in one pass.
const BATCH: i64 = 1000;
const MAX_BATCHES: usize = 100;
const BATCH_DEADLINE: Duration = Duration::from_secs(30);

/// Rows one pass deleted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetentionReport {
    /// Succeeded and cancelled jobs past `jobs.retention.succeeded`.
    pub finished_jobs: u64,
    /// Dead jobs past `jobs.retention.dead`.
    pub dead_jobs: u64,
    /// Relayed outbox rows past `jobs.retention.outbox`, and quarantined
    /// ones past `jobs.retention.dead`.
    pub outbox: u64,
    /// Admin actions past `jobs.retention.audit`.
    pub audit: u64,
    /// Another instance held the retention lock, so this pass stopped.
    pub skipped: bool,
}

/// One purge: `$1` is the age in seconds, `$2` the batch size.
const PURGES: [(&str, &str); 5] = [
    (
        "finished",
        "DELETE FROM merezha_jobs.jobs WHERE id IN (
             SELECT id FROM merezha_jobs.jobs
              WHERE state IN ('succeeded', 'cancelled') AND reissue_claimed_at IS NULL
                AND finished_at < now() - make_interval(secs => $1::float8)
              ORDER BY finished_at LIMIT $2 FOR UPDATE SKIP LOCKED)",
    ),
    (
        "dead",
        "DELETE FROM merezha_jobs.jobs WHERE id IN (
             SELECT id FROM merezha_jobs.jobs
              WHERE state = 'dead' AND finished_at < now() - make_interval(secs => $1::float8)
              ORDER BY finished_at LIMIT $2 FOR UPDATE SKIP LOCKED)",
    ),
    (
        "relayed",
        "DELETE FROM merezha_jobs.outbox WHERE id IN (
             SELECT id FROM merezha_jobs.outbox
              WHERE relayed_at < now() - make_interval(secs => $1::float8)
              ORDER BY relayed_at LIMIT $2 FOR UPDATE SKIP LOCKED)",
    ),
    (
        "quarantined",
        "DELETE FROM merezha_jobs.outbox WHERE id IN (
             SELECT id FROM merezha_jobs.outbox
              WHERE quarantined_at < now() - make_interval(secs => $1::float8)
              ORDER BY quarantined_at LIMIT $2 FOR UPDATE SKIP LOCKED)",
    ),
    (
        "audit",
        "DELETE FROM merezha_jobs.admin_actions WHERE id IN (
             SELECT id FROM merezha_jobs.admin_actions
              WHERE at < now() - make_interval(secs => $1::float8)
              ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED)",
    ),
];

/// One batch under the try-lock: `None` when another instance holds it.
async fn batch(
    database: &Database,
    name: &'static str,
    sql: &'static str,
    age: f64,
) -> Result<Option<u64>, Failure> {
    database
        .transaction(Instant::now() + BATCH_DEADLINE, move |tx| {
            Box::pin(async move {
                let locked: bool = tx
                    .query_one(
                        "SELECT pg_try_advisory_xact_lock($1, hashtext('retention'))",
                        &[&crate::worker::MZJB],
                    )
                    .await
                    .map_err(|e| classify(&e))?
                    .get(0);
                if !locked {
                    return Ok(None);
                }
                let deleted = tx
                    .execute(sql, &[&age, &BATCH])
                    .await
                    .map_err(|e| classify(&e))?;
                if name == "dead" && deleted > 0 {
                    let count = i32::try_from(deleted).unwrap_or(i32::MAX);
                    tx.execute(
                        "INSERT INTO merezha_jobs.admin_actions (principal, action, target, count)
                         VALUES (NULL, 'retention_purge', 'dead', $1)",
                        &[&count],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                }
                Ok(Some(deleted))
            })
        })
        .await
}

/// One retention pass over every kind of row.
pub(crate) async fn purge(
    database: &Database,
    retention: &JobRetention,
) -> Result<RetentionReport, Failure> {
    let mut report = RetentionReport::default();
    for (name, sql) in PURGES {
        let age = match name {
            "finished" => retention.succeeded,
            "dead" | "quarantined" => retention.dead,
            "relayed" => retention.outbox,
            _ => retention.audit,
        }
        .as_secs_f64();
        for _ in 0..MAX_BATCHES {
            let Some(deleted) = batch(database, name, sql, age).await? else {
                report.skipped = true;
                return Ok(report);
            };
            match name {
                "finished" => report.finished_jobs += deleted,
                "dead" => report.dead_jobs += deleted,
                "relayed" | "quarantined" => report.outbox += deleted,
                _ => report.audit += deleted,
            }
            if deleted < BATCH as u64 {
                break;
            }
        }
    }
    if report.dead_jobs > 0 {
        merezha_telemetry::event!(
            INFO,
            "job.admin_action",
            action = "retention_purge",
            count = report.dead_jobs
        );
    }
    Ok(report)
}
