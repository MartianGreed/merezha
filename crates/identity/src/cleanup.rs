//! The recurring `identity.cleanup` job (section 8), declared through
//! `JobsHandle::schedule`: expired sessions and tokens, drained rate-limit
//! buckets, audit rows past retention, expired invitations, API keys
//! revoked or expired 30 days ago and, only while
//! `require_verified_email = true`, unverified accounts past
//! `unverified_account_ttl` that never signed in. Every step deletes in
//! batches of `cleanup.batch_size` to keep transactions short.
use crate::core::{Actor, Audit, Client, Core, minus, sql};
use jiff::Timestamp;
use merezha_domain::Failure;
use merezha_platform::{
    BoxFuture,
    ports::{JobContext, JobFailure, JobHandler},
};
use std::{
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};
use uuid::Uuid;

/// The job kind and schedule name.
pub const KIND: &str = "identity.cleanup";
/// Its queue.
pub const QUEUE: &str = "identity";
/// Passes per step and run, so one run stays bounded.
const MAX_BATCHES: u32 = 100;
/// How long a revoked or expired key's row stays, so a later use is
/// audited as a use after revocation (section 3).
pub const KEY_RETENTION: Duration = Duration::from_secs(30 * 86_400);
/// Bound on each batch statement.
const BATCH_DEADLINE: Duration = Duration::from_secs(10);

/// The batched delete steps.
#[derive(Clone, Copy, Debug)]
enum Step {
    Sessions,
    Tokens,
    Buckets,
    Audit,
    Invitations,
    Keys,
}

/// What one pass deleted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CleanupReport {
    pub sessions: u64,
    pub tokens: u64,
    pub buckets: u64,
    pub audit_events: u64,
    pub invitations: u64,
    /// API keys revoked or expired more than 30 days ago.
    pub api_keys: u64,
    pub unverified_accounts: u64,
    /// Live sessions after the pass, the `sessions.active` gauge.
    pub active_sessions: u64,
}

impl Core {
    /// One cleanup pass now.
    pub async fn cleanup(self: &Arc<Self>) -> Result<CleanupReport, Failure> {
        let now = self.now();
        let batch = i64::from(self.settings.cleanup_batch);
        let mut report = CleanupReport {
            sessions: self.batched(Step::Sessions, now, batch).await?,
            tokens: self.batched(Step::Tokens, now, batch).await?,
            buckets: self.batched(Step::Buckets, now, batch).await?,
            audit_events: self
                .batched(
                    Step::Audit,
                    minus(now, self.settings.audit_retention),
                    batch,
                )
                .await?,
            invitations: self.batched(Step::Invitations, now, batch).await?,
            api_keys: self
                .batched(Step::Keys, minus(now, KEY_RETENTION), batch)
                .await?,
            ..CleanupReport::default()
        };
        if self.settings.require_verified_email {
            let cutoff = minus(now, self.settings.unverified_account_ttl);
            for _ in 0..MAX_BATCHES {
                let deleted = self.unverified_batch(cutoff, batch).await?;
                report.unverified_accounts += deleted;
                if deleted < batch as u64 {
                    break;
                }
            }
        }
        let client = self.db().acquire(Instant::now() + BATCH_DEADLINE).await?;
        let active: i64 = self
            .db()
            .query_one(&client, &crate::sql::SESSIONS_ACTIVE, &[&now])
            .await
            .map_err(sql)?
            .get(0);
        report.active_sessions = active.max(0) as u64;
        self.metrics.set_active_sessions(report.active_sessions);
        Ok(report)
    }
    async fn batched(&self, step: Step, cutoff: Timestamp, batch: i64) -> Result<u64, Failure> {
        let mut total = 0;
        for _ in 0..MAX_BATCHES {
            let client = self.db().acquire(Instant::now() + BATCH_DEADLINE).await?;
            let params: [&(dyn tokio_postgres::types::ToSql + Sync); 2] = [&cutoff, &batch];
            let deleted = match step {
                Step::Sessions => {
                    self.db()
                        .execute(&client, &crate::sql::CLEANUP_SESSIONS, &params)
                        .await
                }
                Step::Tokens => {
                    self.db()
                        .execute(&client, &crate::sql::CLEANUP_TOKENS, &params)
                        .await
                }
                Step::Buckets => {
                    self.db()
                        .execute(&client, &crate::sql::CLEANUP_BUCKETS, &params)
                        .await
                }
                Step::Audit => {
                    self.db()
                        .execute(&client, &crate::sql::CLEANUP_AUDIT, &params)
                        .await
                }
                Step::Invitations => {
                    self.db()
                        .execute(&client, &crate::sql::CLEANUP_INVITATIONS, &params)
                        .await
                }
                Step::Keys => {
                    self.db()
                        .execute(&client, &crate::sql::CLEANUP_KEYS, &params)
                        .await
                }
            }
            .map_err(sql)?;
            total += deleted;
            if deleted < batch as u64 {
                break;
            }
        }
        Ok(total)
    }
    /// One batch of unverified accounts (section 2): lock the candidates
    /// with `SKIP LOCKED`, then delete in a second statement whose snapshot
    /// postdates the locks and re-checks every condition, so an account that
    /// signed in or verified meanwhile survives. Sign-in and every
    /// attachment lock the user row first. A user with a membership or an
    /// application role is never a candidate, so never an organization
    /// owner.
    async fn unverified_batch(
        self: &Arc<Self>,
        cutoff: Timestamp,
        batch: i64,
    ) -> Result<u64, Failure> {
        let core = self.clone();
        self.db()
            .transaction(Instant::now() + BATCH_DEADLINE, move |tx| {
                Box::pin(async move {
                    let candidates: Vec<Uuid> = core
                        .db()
                        .query(&*tx, &crate::sql::USER_UNVERIFIED_LOCK, &[&cutoff, &batch])
                        .await
                        .map_err(sql)?
                        .iter()
                        .map(|r| r.get(0))
                        .collect();
                    if candidates.is_empty() {
                        return Ok(0);
                    }
                    let deleted: Vec<Uuid> = core
                        .db()
                        .query(
                            &*tx,
                            &crate::sql::USER_UNVERIFIED_DELETE,
                            &[&candidates, &cutoff],
                        )
                        .await
                        .map_err(sql)?
                        .iter()
                        .map(|r| r.get(0))
                        .collect();
                    for id in &deleted {
                        core.audit(
                            tx,
                            Audit::new("user.deleted", Actor::System)
                                .target(*id)
                                .detail("reason", "unverified_expired"),
                            &Client::default(),
                        )
                        .await?;
                    }
                    Ok(deleted.len() as u64)
                })
            })
            .await
    }
}

/// The job handler; it waits for the block to start.
pub(crate) struct CleanupJob {
    pub(crate) core: Arc<OnceLock<Arc<Core>>>,
}
impl JobHandler for CleanupJob {
    fn handle<'a>(
        &'a self,
        _: JobContext,
        _: serde_json::Value,
    ) -> BoxFuture<'a, Result<(), JobFailure>> {
        Box::pin(async move {
            let Some(core) = self.core.get() else {
                return Err(JobFailure::release());
            };
            core.cleanup().await.map(|_| ()).map_err(JobFailure::from)
        })
    }
}
