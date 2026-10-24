//! The fallback rebuild of `published` (section 2, C4): after an
//! activation adds a locale or changes a fallback, the
//! `content.published_rebuild` job re-materializes the `published` rows of
//! every live entry with a published variant, in the background.
//!
//! The activation transaction records the pending rebuild in
//! `merezha_content.rebuilds` and appends the job. Any current host runs
//! it: batches of [`REBUILD_BATCH`] entries in id order, one transaction
//! each with the model version check, the entries locked, a generation
//! bump for every scope touched and the resume cursor advanced. A run that
//! stops early (cancel, deadline) appends a continuation job; a host that
//! dies mid-batch leaves the cursor where the last commit put it, and the
//! retried job continues there. Readiness never waits for it: until it is
//! done, delivery may serve the previous materialization for the affected
//! locales, as during a rolling deploy.
use crate::{
    publish::{bump_generation, materialize},
    store::{Rejection, Store, sql},
    trash::QUEUE,
};
use merezha_domain::{Failure, Identity};
use merezha_platform::{
    BoxFuture,
    ports::{
        DedupeKey, JobContext, JobFailure, JobHandler, MessageContext, OutboxRecord, OutboxTarget,
    },
};
use serde_json::{Value, json};
use std::collections::BTreeSet;

/// The job kind of the fallback rebuild, on the `content` queue.
pub const REBUILD_JOB: &str = "content.published_rebuild";
/// Entries one rebuild transaction re-materializes.
pub const REBUILD_BATCH: i64 = 500;

/// A rebuild not finished yet, as `merezha content diff` prints it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingRebuild {
    pub version: u32,
    /// The last entry id rebuilt, `None` before the first batch.
    pub after_entry: Option<String>,
}

/// The pending rebuild, if any (`None` before migration 3 too).
pub async fn pending_rebuild(
    client: &impl merezha_postgres::tokio_postgres::GenericClient,
) -> Result<Option<PendingRebuild>, Failure> {
    let exists: bool = client
        .query_one(
            "SELECT to_regclass('merezha_content.rebuilds') IS NOT NULL",
            &[],
        )
        .await
        .map_err(|e| merezha_postgres::classify(&e))?
        .get(0);
    if !exists {
        return Ok(None);
    }
    let row = client
        .query_opt(
            "SELECT version, after_entry::text FROM merezha_content.rebuilds \
              WHERE state = 'pending' ORDER BY version DESC LIMIT 1",
            &[],
        )
        .await
        .map_err(|e| merezha_postgres::classify(&e))?;
    Ok(row.map(|row| PendingRebuild {
        version: u32::try_from(row.get::<_, i32>(0)).unwrap_or(0),
        after_entry: row.get(1),
    }))
}

/// The outbox record of a `content.published_rebuild` job for `version`;
/// `continuation` names the job a stopped run continues.
pub(crate) fn rebuild_job(
    version: u32,
    continuation: Option<i64>,
) -> Result<OutboxRecord, Failure> {
    let key = match continuation {
        None => format!("rebuild:{version}"),
        Some(job) => format!("rebuild:{version}:after:{job}"),
    };
    Ok(OutboxRecord {
        dedupe_key: DedupeKey::new(key.clone())?,
        target: OutboxTarget::Job {
            queue: Identity::new(QUEUE)?,
            kind: Identity::new(REBUILD_JOB)?,
        },
        payload: json!({ "version": version }),
        contains_secret: false,
        run_at: None,
        priority: 0,
        context: MessageContext {
            correlation_id: Identity::new(key)?,
            causation_id: None,
            tenant: None,
        },
    })
}

/// What one rebuild transaction did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Batch {
    /// No rebuild is pending for this host's model.
    Nothing,
    /// `entries` rebuilt; `done` when the last batch.
    Rebuilt { entries: u64, done: bool },
}

impl Store {
    /// One rebuild transaction for this host's model; see the module docs.
    async fn rebuild_batch(&self) -> Result<Batch, Rejection> {
        let work = self.work();
        crate::store::transaction(self.database(), self.deadline(), move |tx| {
            Box::pin(async move {
                work.state.check_writable(tx).await?;
                let version = i32::try_from(work.model.version())
                    .map_err(|_| Failure::permanent("invalid_input"))?;
                let Some(cursor) = tx
                    .query_opt(
                        "SELECT after_entry::text FROM merezha_content.rebuilds \
                          WHERE version = $1 AND state = 'pending' FOR UPDATE",
                        &[&version],
                    )
                    .await
                    .map_err(|e| sql(&e))?
                else {
                    return Ok(Batch::Nothing);
                };
                let after: Option<String> = cursor.get(0);
                let rows = tx
                    .query(
                        "SELECT e.id::text, e.scope, e.content_type FROM merezha_content.entries e \
                          WHERE e.deleted_at IS NULL \
                            AND ($1::text IS NULL OR e.id > $1::text::uuid) \
                            AND EXISTS (SELECT 1 FROM merezha_content.variants v \
                                         WHERE v.entry_id = e.id AND v.scope = e.scope \
                                           AND v.content_type = e.content_type \
                                           AND NOT v.deleted AND v.published_revision IS NOT NULL) \
                          ORDER BY e.id LIMIT $2 FOR UPDATE OF e",
                        &[&after, &REBUILD_BATCH],
                    )
                    .await
                    .map_err(|e| sql(&e))?;
                let mut scopes = BTreeSet::new();
                for row in &rows {
                    let (id, scope, content_type): (String, String, String) =
                        (row.get(0), row.get(1), row.get(2));
                    materialize(tx, &work.model, &work.indexes, &scope, &content_type, &id).await?;
                    scopes.insert(scope);
                }
                for scope in &scopes {
                    bump_generation(tx, scope).await?;
                }
                let last: Option<String> = rows.last().map(|r| r.get(0));
                let done = (rows.len() as i64) < REBUILD_BATCH;
                tx.execute(
                    "UPDATE merezha_content.rebuilds \
                        SET after_entry = $2::text::uuid, updated_at = now(), \
                            state = CASE WHEN $3 THEN 'done' ELSE 'pending' END \
                      WHERE version = $1",
                    &[&version, &last.or(after), &done],
                )
                .await
                .map_err(|e| sql(&e))?;
                Ok(Batch::Rebuilt {
                    entries: rows.len() as u64,
                    done,
                })
            })
        })
        .await
    }

    /// Append the continuation of the stopped job `job`.
    async fn continue_rebuild(&self, job: i64) -> Result<(), Rejection> {
        let work = self.work();
        let record = rebuild_job(work.model.version(), Some(job))?;
        crate::store::transaction(self.database(), self.deadline(), move |tx| {
            Box::pin(async move {
                work.state.check_writable(tx).await?;
                work.append(tx, vec![record]).await
            })
        })
        .await
    }
}

/// The `content.published_rebuild` handler, registered through
/// [`crate::ContentState::guard_jobs`]: an outdated host gives the job
/// back, and a newer model's activation supersedes the rebuild.
pub(crate) struct RebuildPublished {
    pub(crate) store: Store,
}
impl JobHandler for RebuildPublished {
    fn handle<'a>(
        &'a self,
        ctx: JobContext,
        _payload: Value,
    ) -> BoxFuture<'a, Result<(), JobFailure>> {
        Box::pin(async move {
            let version = self.store.model().version();
            let margin = self.store.state().settings().limits.query_timeout;
            let mut entries = 0u64;
            loop {
                if *ctx.cancel.borrow() || tokio::time::Instant::now() + margin >= ctx.deadline {
                    self.store
                        .continue_rebuild(ctx.id)
                        .await
                        .map_err(|r| JobFailure::from(r.failure))?;
                    return Ok(());
                }
                match self
                    .store
                    .rebuild_batch()
                    .await
                    .map_err(|r| JobFailure::from(r.failure))?
                {
                    Batch::Nothing => return Ok(()),
                    Batch::Rebuilt { entries: n, done } => {
                        entries += n;
                        merezha_telemetry::event!(
                            INFO,
                            "content.rebuild_batch",
                            version,
                            entries = n,
                            done
                        );
                        if done {
                            merezha_telemetry::event!(
                                INFO,
                                "content.rebuild_completed",
                                version,
                                entries
                            );
                            return Ok(());
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn job_keys_are_valid_dedupe_keys() {
        use merezha_platform::ports::DedupeKey;
        assert!(DedupeKey::new("rebuild:3").is_ok());
        assert!(DedupeKey::new("rebuild:3:after:42").is_ok());
    }
}
