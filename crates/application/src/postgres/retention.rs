//! Reactor retention (`[application.reactors] retention`, issue #69): an
//! hourly pass deletes `reactor_rejections` and `reactor_skips` rows older
//! than the setting, oldest first, in batches of [`BATCH`] rows, at most
//! [`MAX_BATCHES`] per table and pass. Each batch is its own transaction
//! under `pg_try_advisory_xact_lock(1297760594, hashtext('retention'))`
//! (class `'MZAR'`, apart from the run locks), so
//! one instance purges at a time and a pass never holds row locks for
//! long; the batches skip rows another transaction locked.
use super::{PostgresStore, read_error, sql};
use merezha_domain::Failure;
use merezha_postgres::Statement;
use std::time::{Duration, Instant};

/// How often a pass runs; the scheduler runs one at start too.
pub const RETENTION_INTERVAL: Duration = Duration::from_secs(3_600);
/// Rows per batch.
const BATCH: i64 = 1_000;
/// Batches per table in one pass: a pass deletes at most 100,000 rows of
/// each, and the next pass goes on.
const MAX_BATCHES: usize = 100;
/// Bound of one batch's transaction.
const BATCH_DEADLINE: Duration = Duration::from_secs(30);

/// Rows one retention pass deleted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReactorRetention {
    /// `reactor_rejections` rows past the retention.
    pub rejections: u64,
    /// `reactor_skips` rows past the retention.
    pub skips: u64,
    /// Another instance held the retention lock, so this pass stopped.
    pub skipped: bool,
}

impl PostgresStore {
    /// One retention pass now: rejections and skips recorded more than
    /// `[application.reactors] retention` ago are deleted, in bounded
    /// batches. The scheduler runs it at start and every hour.
    pub async fn purge_reactor_records(&self) -> Result<ReactorRetention, Failure> {
        let age = self.shared().options.reactor_retention.as_secs_f64();
        let mut report = ReactorRetention::default();
        for (table, purge) in [(0, &sql::PURGE_REJECTIONS), (1, &sql::PURGE_SKIPS)] {
            for _ in 0..MAX_BATCHES {
                let Some(rows) = self.purge_batch(purge, age).await? else {
                    report.skipped = true;
                    return Ok(report);
                };
                if table == 0 {
                    report.rejections += rows;
                } else {
                    report.skips += rows;
                }
                if rows < BATCH as u64 {
                    break;
                }
            }
        }
        Ok(report)
    }
    /// One batch under the try-lock: `None` when another instance holds it.
    async fn purge_batch(
        &self,
        purge: &'static Statement,
        age: f64,
    ) -> Result<Option<u64>, Failure> {
        let db = self.db.clone();
        self.db
            .transaction(Instant::now() + BATCH_DEADLINE, move |tx| {
                Box::pin(async move {
                    let locked: bool = db
                        .query_one(tx, &sql::RETENTION_LOCK, &[])
                        .await
                        .map_err(|e| read_error(&e))?
                        .get(0);
                    if !locked {
                        return Ok(None);
                    }
                    let deleted = db
                        .execute(tx, purge, &[&age, &BATCH])
                        .await
                        .map_err(|e| read_error(&e))?;
                    Ok(Some(deleted))
                })
            })
            .await
    }
}
