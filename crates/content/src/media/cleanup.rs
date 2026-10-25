//! `content.blob_cleanup` (section 6): orphaned blobs past
//! `media.orphan_grace`, pending blobs whose upload lease expired, and
//! deletions a crash abandoned are claimed `deleting` (`FOR UPDATE SKIP
//! LOCKED`) and committed; then each object is deleted and its row
//! removed, each step fenced on the claim's delete lease (a worker whose
//! lease expired and was taken over touches nothing). A failed delete
//! restores the row's previous state. At most
//! [`CLEANUP_LIMIT`] blobs per run, declared as the cron schedule of the
//! same name with `media.cleanup_cron` in UTC. Each run first purges the
//! asset trash past `trash.retention`, which orphans the blobs it frees.
use super::{Assets, sql, sql_error, upload::millis};
use crate::store::{Rejection, transaction};
use merezha_platform::{
    BoxFuture,
    ports::{BlobKey, BlobStore, JobContext, JobFailure, JobHandler},
};
use serde_json::Value;
use std::time::Duration;

/// The job kind and schedule name.
pub const BLOB_CLEANUP: &str = "content.blob_cleanup";
/// Most blobs one run deletes.
pub const CLEANUP_LIMIT: usize = 1000;
/// Blobs per claim transaction.
pub const CLEANUP_BATCH: i64 = 100;
/// How long a claimed deletion is leased: past it, a deletion a crash
/// abandoned is claimed again.
pub const DELETE_LEASE: Duration = Duration::from_secs(600);

/// What one cleanup pass did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CleanupReport {
    /// Blobs whose object and row were deleted.
    pub deleted: usize,
    /// Blobs whose object delete failed, restored for a later run.
    pub failed: usize,
    /// Claimed blobs this pass no longer owned when it came to them (its
    /// lease expired and another worker or an upload took the row): left
    /// alone.
    pub lost: usize,
}

/// A blob this worker claimed for deletion: its row, the state and lease
/// to restore after a failed delete, and the delete lease that fences
/// every later step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimedBlob {
    pub scope: String,
    pub sha256: String,
    key: String,
    state: String,
    lease: Option<String>,
    token: String,
}

/// The fenced steps after a claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    Retake,
    Forget,
    Restore,
}

/// What became of one claimed blob.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Swept {
    /// The object and the row are gone.
    Deleted,
    /// The object delete failed; the row is back as it was.
    Failed,
    /// The claim was no longer this worker's: nothing was touched.
    Lost,
}

impl Assets {
    /// Claim and delete up to `limit` due blobs (one pass of
    /// [`BLOB_CLEANUP`]). A batch stops taking its next blob once half the
    /// delete lease has passed: the rest of its claims expire and are
    /// claimed again later, never deleted under a lost lease.
    pub async fn cleanup(&self, limit: usize) -> Result<CleanupReport, Rejection> {
        let mut report = CleanupReport::default();
        loop {
            let done = report.deleted + report.failed + report.lost;
            if done >= limit {
                break;
            }
            let room = (limit - done).min(CLEANUP_BATCH as usize);
            let claimed = self.claim_due(room).await?;
            if claimed.is_empty() {
                break;
            }
            let started = std::time::Instant::now();
            for blob in &claimed {
                if started.elapsed() >= DELETE_LEASE / 2 {
                    report.lost += 1;
                    continue;
                }
                match self.sweep(blob).await? {
                    Swept::Deleted => report.deleted += 1,
                    Swept::Failed => report.failed += 1,
                    Swept::Lost => report.lost += 1,
                }
            }
            if claimed.len() < CLEANUP_BATCH as usize {
                break;
            }
        }
        if report.deleted > 0 || report.failed > 0 {
            merezha_telemetry::event!(
                INFO,
                "content.blob_cleanup",
                driver = self.blobs.driver().as_str(),
                count = report.deleted
            );
        }
        Ok(report)
    }

    /// Claim up to `batch` due blobs `deleting` under a fresh delete lease
    /// (the first step of [`Assets::cleanup`]).
    pub async fn claim_due(&self, batch: usize) -> Result<Vec<ClaimedBlob>, Rejection> {
        let db = self.db().clone();
        let state = self.state().clone();
        let grace = millis(self.settings().orphan_grace);
        let lease = millis(DELETE_LEASE);
        let batch = i64::try_from(batch).unwrap_or(CLEANUP_BATCH);
        transaction(self.db(), self.deadline(), move |tx| {
            Box::pin(async move {
                state.check_writable(tx).await?;
                let rows = db
                    .query(&*tx, &sql::BLOB_CLEANUP_CLAIM, &[&grace, &batch, &lease])
                    .await
                    .map_err(|e| sql_error(&e))?;
                Ok(rows
                    .iter()
                    .map(|row| ClaimedBlob {
                        scope: row.get(0),
                        sha256: row.get(1),
                        key: row.get(2),
                        state: row.get(3),
                        lease: row.get(4),
                        token: row.get(5),
                    })
                    .collect())
            })
        })
        .await
    }

    /// Delete one claimed blob (the second step of [`Assets::cleanup`]):
    /// re-take its row under the claim's token, renewing the lease, then
    /// delete the object (bounded by half the lease) and the row, both
    /// fenced on the renewed token. A lost claim touches nothing.
    pub async fn sweep(&self, blob: &ClaimedBlob) -> Result<Swept, Rejection> {
        let Some(token) = self.fenced(Step::Retake, blob, &blob.token).await? else {
            return Ok(Swept::Lost);
        };
        if self.delete_object(&blob.key).await {
            self.fenced(Step::Forget, blob, &token).await?;
            Ok(Swept::Deleted)
        } else {
            self.fenced(Step::Restore, blob, &token).await?;
            Ok(Swept::Failed)
        }
    }

    /// Delete an object within half the delete lease, so it never runs
    /// past the lease that fences it.
    pub(crate) async fn delete_object(&self, key: &str) -> bool {
        let Ok(key) = BlobKey::new(key.to_owned()) else {
            return false;
        };
        matches!(
            tokio::time::timeout(DELETE_LEASE / 2, self.blobs.delete(&key)).await,
            Ok(Ok(()))
        )
    }

    /// Run one fenced step on `blob` with `token`: the retake returns the
    /// renewed token, the others `Some` when they matched the row.
    async fn fenced(
        &self,
        step: Step,
        blob: &ClaimedBlob,
        token: &str,
    ) -> Result<Option<String>, Rejection> {
        let db = self.db().clone();
        let state = self.state().clone();
        let lease = millis(DELETE_LEASE);
        let (scope, sha256, token) = (blob.scope.clone(), blob.sha256.clone(), token.to_owned());
        let (previous, previous_lease) = (blob.state.clone(), blob.lease.clone());
        transaction(self.db(), self.deadline(), move |tx| {
            Box::pin(async move {
                state.check_writable(tx).await?;
                let changed = match step {
                    Step::Retake => {
                        return Ok(db
                            .query_opt(&*tx, &sql::BLOB_RETAKE, &[&scope, &sha256, &token, &lease])
                            .await
                            .map_err(|e| sql_error(&e))?
                            .map(|row| row.get(0)));
                    }
                    Step::Forget => {
                        db.execute(&*tx, &sql::BLOB_DELETE, &[&scope, &sha256, &token])
                            .await
                    }
                    Step::Restore => {
                        db.execute(
                            &*tx,
                            &sql::BLOB_RESTORE,
                            &[&scope, &sha256, &previous, &previous_lease, &token],
                        )
                        .await
                    }
                }
                .map_err(|e| sql_error(&e))?;
                Ok((changed > 0).then_some(token))
            })
        })
        .await
    }
}

/// The `content.blob_cleanup` handler: purge the expired asset trash in
/// batches, then delete up to [`CLEANUP_LIMIT`] due blobs, stopping early
/// when the job is cancelled or its deadline nears (the next run goes on).
/// Registered through [`crate::ContentState::guard_jobs`].
pub(crate) struct BlobCleanup {
    pub(crate) assets: Assets,
}
impl JobHandler for BlobCleanup {
    fn handle<'a>(
        &'a self,
        ctx: JobContext,
        _payload: Value,
    ) -> BoxFuture<'a, Result<(), JobFailure>> {
        Box::pin(async move {
            let margin = self.assets.state().settings().limits.query_timeout;
            let stop =
                || *ctx.cancel.borrow() || tokio::time::Instant::now() + margin >= ctx.deadline;
            loop {
                if stop() {
                    return Ok(());
                }
                let purged = self
                    .assets
                    .purge_expired()
                    .await
                    .map_err(|r| JobFailure::from(r.failure))?;
                if purged < super::ASSET_PURGE_BATCH as usize {
                    break;
                }
            }
            let mut done = 0;
            while done < CLEANUP_LIMIT && !stop() {
                let room = (CLEANUP_LIMIT - done).min(CLEANUP_BATCH as usize);
                let report = self
                    .assets
                    .cleanup(room)
                    .await
                    .map_err(|r| JobFailure::from(r.failure))?;
                let swept = report.deleted + report.failed + report.lost;
                done += swept;
                if swept < room {
                    break;
                }
            }
            Ok(())
        })
    }
}
