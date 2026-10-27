//! `content.blob_cleanup` (section 6): orphaned blobs past
//! `media.orphan_grace`, pending blobs whose upload lease expired, and
//! deletions a crash abandoned are claimed `deleting` (`FOR UPDATE SKIP
//! LOCKED`) and committed; then each object is deleted and its row
//! removed, each step fenced on the claim's delete lease (a worker whose
//! lease expired and was taken over touches nothing). A refused delete
//! restores the row's previous state; one whose outcome is unknown (cut by
//! the half-lease bound) leaves the row `deleting` until its lease
//! expires. At most
//! [`CLEANUP_LIMIT`] blobs per run, declared as the cron schedule of the
//! same name with `media.cleanup_cron` in UTC. Each run first purges the
//! asset trash past `trash.retention`, which orphans the blobs it frees.
use super::{Assets, sql, sql_error, upload::millis};
use crate::store::{Rejection, transaction};
use merezha_blob::RETRY_TIMEOUT;
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
/// The longest `media.s3.timeout` content accepts. An object delete that
/// gets no answer gives up after at most `RETRY_TIMEOUT`, one backoff (at
/// most a second) and one more `timeout`: with this cap that stays under
/// half the delete lease, the bound on every object delete, so the bound
/// cuts a delete only when the process or the network stalls.
pub const MAX_S3_TIMEOUT: Duration = Duration::from_secs(240);
const _: () =
    assert!(RETRY_TIMEOUT.as_secs() + 1 + MAX_S3_TIMEOUT.as_secs() < DELETE_LEASE.as_secs() / 2);

/// What one cleanup pass did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CleanupReport {
    /// Blobs whose object and row this pass deleted under its own lease
    /// (the `count` of the `content.blob_cleanup` event).
    pub deleted: usize,
    /// Blobs whose object delete failed: refused (the row restored for a
    /// later run) or cut by the half-lease bound (the row left `deleting`
    /// until its lease expires).
    pub failed: usize,
    /// Claimed blobs this pass no longer owned: its lease expired and
    /// another worker or an upload took the row, before the retake (left
    /// alone) or while the object delete ran (the row left to its holder).
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

/// How one object delete ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Deletion {
    /// The store answered: the object is gone.
    Deleted,
    /// The store answered with an error: the object is still there.
    Refused,
    /// The half-lease bound cut it: the request may still land.
    Unknown,
}

/// What became of one claimed blob.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Swept {
    /// The object and the row are gone.
    Deleted,
    /// The object delete failed. Refused: the row is back as it was. Cut
    /// by the half-lease bound, its outcome unknown: the row stays
    /// `deleting` under the renewed lease, so a delete that still lands
    /// does so while the row is fenced, and the next claim after the lease
    /// deletes it again.
    Failed,
    /// The claim was no longer this worker's: at the retake, nothing was
    /// touched; after the object delete, the row (not removed, or not
    /// restored) belongs to whoever took it over.
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
                if started.elapsed() >= self.delete_lease / 2 {
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
        if report.deleted > 0 || report.failed > 0 || report.lost > 0 {
            merezha_telemetry::event!(
                INFO,
                "content.blob_cleanup",
                driver = self.blobs.driver().as_str(),
                count = report.deleted,
                failed = report.failed,
                lost = report.lost
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
        let lease = millis(self.delete_lease);
        let batch = i64::try_from(batch).unwrap_or(CLEANUP_BATCH);
        transaction(self.db(), self.deadline(), move |tx| {
            Box::pin(async move {
                state.check_writable(&db, tx).await?;
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
    /// fenced on the renewed token. A claim lost before the retake touches
    /// nothing; one lost while the object delete ran leaves the row (and
    /// the count) to the worker that holds it now. A delete the bound cut
    /// leaves the row `deleting` under the renewed lease.
    pub async fn sweep(&self, blob: &ClaimedBlob) -> Result<Swept, Rejection> {
        let Some(token) = self.fenced(Step::Retake, blob, &blob.token).await? else {
            return Ok(Swept::Lost);
        };
        let (step, swept) = match self.delete_object(&blob.key).await {
            Deletion::Deleted => (Step::Forget, Swept::Deleted),
            Deletion::Refused => (Step::Restore, Swept::Failed),
            // Restoring now would let an upload reuse the row (`orphaned`
            // means "the object exists") while the cut request may still
            // delete the object.
            Deletion::Unknown => return Ok(Swept::Failed),
        };
        Ok(match self.fenced(step, blob, &token).await? {
            Some(_) => swept,
            None => Swept::Lost,
        })
    }

    /// Delete an object within half the delete lease, so it never runs
    /// past the lease that fences it. The store does not see the fence: a
    /// process stalled for the whole lease before the request leaves can
    /// still delete an object uploaded again meanwhile (the limit of
    /// database-side fencing, content design section 6, issue #49).
    pub(crate) async fn delete_object(&self, key: &str) -> Deletion {
        let Ok(key) = BlobKey::new(key.to_owned()) else {
            return Deletion::Refused;
        };
        match tokio::time::timeout(self.delete_lease / 2, self.blobs.delete(&key)).await {
            Ok(Ok(())) => Deletion::Deleted,
            Ok(Err(_)) => Deletion::Refused,
            Err(_) => Deletion::Unknown,
        }
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
        let lease = millis(self.delete_lease);
        let (scope, sha256, token) = (blob.scope.clone(), blob.sha256.clone(), token.to_owned());
        let (previous, previous_lease) = (blob.state.clone(), blob.lease.clone());
        transaction(self.db(), self.deadline(), move |tx| {
            Box::pin(async move {
                state.check_writable(&db, tx).await?;
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
