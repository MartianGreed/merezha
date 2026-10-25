//! Deleted organizations (issue #56, section 8): when identity deletes an
//! organization, content stops serving it at once and purges it.
//!
//! - The kind [`ORGANIZATION_DELETED`] is subscribed to identity's
//!   `identity.organization_deleted` ([`ORGANIZATION_DELETED_TOPIC`]). Its
//!   transaction marks the scope `org:<organization>` deleted in
//!   `merezha_content.scopes_deleted` (under the scope's exclusive
//!   transaction lock) and appends the purge job. From that commit on,
//!   every management, delivery, asset and trash route of the scope
//!   answers 404 `organization_not_found`, and every organization-scope
//!   write takes the scope's shared lock and checks the mark first
//!   ([`guard_write`]), so none commits after it.
//! - The kind [`ORGANIZATION_PURGE`] hard-deletes the scope in bounded
//!   transactions ([`purge_step`]): entries with their variants,
//!   revisions, `published` rows and outgoing refs, the refs of any scope
//!   that point at them, then the assets and the refs to them, then the
//!   blobs, handed to `content.blob_cleanup` (`orphaned`, due at once),
//!   which deletes each object fenced on its delete lease (C7). The last
//!   step removes the scope's generation and marks it `purged`. Every
//!   step is idempotent, so a run killed mid-way is finished by the next
//!   lease on any host; a run that nears its deadline appends its
//!   continuation.
//! - `merezha content purge-organization` runs the same steps
//!   ([`purge_organization`]) for organizations deleted before this, or a
//!   missed event.
//!
//! The mark is kept after the purge: organization ids are never reused,
//! so the scope never answers again. The purge appends no content events:
//! consumers learn of it from `identity.organization_deleted` itself.
use crate::{
    statements,
    store::{Rejection, Store, sql, transaction},
    trash::QUEUE,
};
use merezha_domain::{Failure, Identity};
use merezha_platform::{
    BoxFuture, Database, Tx,
    ports::{
        DedupeKey, IntegrationEvent, JobContext, JobFailure, JobHandler, MessageContext,
        OnDuplicate, OutboxRecord, OutboxTarget, SourceOutbox,
    },
};
use serde_json::{Value, json};
use std::time::{Duration, Instant};

/// The identity event content subscribes to.
pub const ORGANIZATION_DELETED_TOPIC: &str = "identity.organization_deleted";
/// The job kind of that subscription, on the `content` queue: marks the
/// scope deleted and appends the purge.
pub const ORGANIZATION_DELETED: &str = "content.organization_deleted";
/// The purge job kind, on the `content` queue, payload `{"scope"}`.
pub const ORGANIZATION_PURGE: &str = "content.organization_purge";
/// Rows of one kind (entries, assets or blobs) one purge transaction
/// removes or releases.
pub const ORGANIZATION_PURGE_BATCH: i64 = 100;
/// The 404 of every route of a deleted organization's scope.
pub const ORGANIZATION_NOT_FOUND: &str = "organization_not_found";
/// The longest organization id a scope holds (`org:` plus 1 to 128
/// characters of `[A-Za-z0-9._:/-]`, the `entries.scope` check).
pub const MAX_ORGANIZATION_ID: usize = 128;

/// Who marked a scope deleted (`scopes_deleted.source`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeletedBy {
    /// The `identity.organization_deleted` subscription.
    Event,
    /// `merezha content purge-organization`.
    Operator,
}
impl DeletedBy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::Operator => "operator",
        }
    }
}

/// `org:<organization>`, the scope of an organization's content, for an
/// id the scope column accepts; anything else fails permanent
/// `invalid_organization`.
pub fn organization_scope(organization: &str) -> Result<String, Failure> {
    let valid = !organization.is_empty()
        && organization.len() <= MAX_ORGANIZATION_ID
        && organization
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:/-".contains(&b))
        && Identity::new(organization).is_ok();
    if valid {
        Ok(format!("org:{organization}"))
    } else {
        Err(Failure::permanent("invalid_organization"))
    }
}

fn not_found() -> Rejection {
    Rejection::new(Failure::permanent(ORGANIZATION_NOT_FOUND))
}

fn is_organization(scope: &str) -> bool {
    scope.starts_with("org:")
}

/// The start of every organization-scope write transaction, after the
/// model version check: the scope's shared transaction lock, then
/// `organization_not_found` when the scope is deleted. Application scope
/// passes untouched.
pub(crate) async fn guard_write(
    db: &Database,
    tx: &mut Tx<'_>,
    scope: &str,
) -> Result<(), Rejection> {
    if !is_organization(scope) {
        return Ok(());
    }
    db.execute(&*tx, &statements::SCOPE_WRITE_LOCK, &[&scope])
        .await
        .map_err(|e| sql(&e))?;
    // A statement of its own: its snapshot sees a deletion that committed
    // while the lock waited.
    refuse_in(db, tx, scope).await
}

/// `organization_not_found` when `scope` is deleted, inside `tx` (the
/// delivery read's first statement).
pub(crate) async fn refuse_in(
    db: &Database,
    tx: &mut Tx<'_>,
    scope: &str,
) -> Result<(), Rejection> {
    if !is_organization(scope) {
        return Ok(());
    }
    match db
        .query_opt(&*tx, &statements::SCOPE_DELETED, &[&scope])
        .await
        .map_err(|e| sql(&e))?
    {
        Some(_) => Err(not_found()),
        None => Ok(()),
    }
}

impl Store {
    /// `organization_not_found` when `scope` is a deleted organization's:
    /// the check management and asset routes run once authorized, before
    /// they read. One indexed read on an organization scope, none on the
    /// application's.
    pub(crate) async fn refuse_deleted(&self, scope: &str) -> Result<(), Rejection> {
        if !is_organization(scope) {
            return Ok(());
        }
        let db = self.database();
        let deadline = self.deadline();
        let at = tokio::time::Instant::from_std(deadline);
        let read = async {
            let client = db.acquire(deadline).await?;
            db.query_opt(&**client, &statements::SCOPE_DELETED, &[&scope])
                .await
                .map_err(|e| sql(&e))
        };
        match tokio::time::timeout_at(at, read).await {
            Ok(Ok(Some(_))) => Err(not_found()),
            Ok(Ok(None)) => Ok(()),
            Ok(Err(rejection)) => Err(rejection),
            Err(_) => Err(Failure::transient("database_timeout").into()),
        }
    }
}

/// Mark `scope` deleted inside `tx`, under its exclusive transaction lock,
/// which waits for every organization-scope write holding the shared one.
/// `true` when this call marked it, `false` when it already was.
pub async fn mark_deleted(
    db: &Database,
    tx: &mut Tx<'_>,
    scope: &str,
    by: DeletedBy,
) -> Result<bool, Failure> {
    if !is_organization(scope) {
        return Err(Failure::permanent("invalid_organization"));
    }
    db.execute(&*tx, &statements::SCOPE_DELETE_LOCK, &[&scope])
        .await
        .map_err(|e| merezha_postgres::classify(&e))?;
    let marked = db
        .query_opt(
            &*tx,
            &statements::SCOPE_MARK_DELETED,
            &[&scope, &by.as_str()],
        )
        .await
        .map_err(|e| merezha_postgres::classify(&e))?;
    Ok(marked.is_some())
}

/// Whether the database holds migration 4 (`scopes_deleted`), which
/// `merezha content purge-organization` needs before it writes.
pub async fn deletion_ready(db: &Database, deadline: Instant) -> Result<bool, Failure> {
    let client = db.acquire(deadline).await?;
    let row = db
        .query_one(&**client, &statements::SCOPES_DELETED_EXIST, &[])
        .await
        .map_err(|e| merezha_postgres::classify(&e))?;
    Ok(row.get(0))
}

/// What one [`purge_step`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PurgeStep {
    /// Entries hard-deleted (with their variants, revisions, published
    /// rows and refs).
    Entries(usize),
    /// Assets hard-deleted.
    Assets(usize),
    /// Blobs handed to `content.blob_cleanup`.
    Blobs(usize),
    /// Nothing is left: the scope is `purged`. `now` when this step marked
    /// it, `false` when it already was.
    Done { now: bool },
    /// The scope is not marked deleted: nothing was touched.
    NotDeleted,
}

/// One bounded transaction of the purge of the deleted `scope`: the next
/// [`ORGANIZATION_PURGE_BATCH`] entries, else assets, else blobs to
/// release, else the end (the scope's refs and generation removed, the
/// mark `purged`, and with `cleanup` a `content.blob_cleanup` job
/// appended when blob rows are left for it). Idempotent: a step that
/// rolled back or ran twice leaves the same rows.
pub async fn purge_step(
    db: &Database,
    deadline: Instant,
    scope: &str,
    cleanup: Option<&SourceOutbox>,
) -> Result<PurgeStep, Failure> {
    let db_owned = db.clone();
    let scope = scope.to_owned();
    let cleanup = cleanup.cloned();
    let result = transaction(db, deadline, move |tx| {
        Box::pin(async move {
            let db = &db_owned;
            let Some(state) = db
                .query_opt(&*tx, &statements::SCOPE_DELETED, &[&scope])
                .await
                .map_err(|e| sql(&e))?
                .map(|row| row.get::<_, String>(0))
            else {
                return Ok(PurgeStep::NotDeleted);
            };
            if state == "purged" {
                return Ok(PurgeStep::Done { now: false });
            }
            let limit = ORGANIZATION_PURGE_BATCH;
            let ids: Vec<String> = db
                .query(&*tx, &statements::SCOPE_PURGE_ENTRIES, &[&scope, &limit])
                .await
                .map_err(|e| sql(&e))?
                .iter()
                .map(|row| row.get(0))
                .collect();
            if !ids.is_empty() {
                db.execute(&*tx, &statements::REFS_TO_ENTRIES_DELETE, &[&ids])
                    .await
                    .map_err(|e| sql(&e))?;
                return Ok(PurgeStep::Entries(ids.len()));
            }
            let ids: Vec<String> = db
                .query(&*tx, &statements::SCOPE_PURGE_ASSETS, &[&scope, &limit])
                .await
                .map_err(|e| sql(&e))?
                .iter()
                .map(|row| row.get(0))
                .collect();
            if !ids.is_empty() {
                db.execute(&*tx, &statements::REFS_TO_ASSETS_DELETE, &[&ids])
                    .await
                    .map_err(|e| sql(&e))?;
                return Ok(PurgeStep::Assets(ids.len()));
            }
            let released = db
                .execute(&*tx, &statements::SCOPE_RELEASE_BLOBS, &[&scope, &limit])
                .await
                .map_err(|e| sql(&e))?;
            if released > 0 {
                return Ok(PurgeStep::Blobs(usize::try_from(released).unwrap_or(0)));
            }
            db.execute(&*tx, &statements::SCOPE_REFS_DELETE, &[&scope])
                .await
                .map_err(|e| sql(&e))?;
            db.execute(&*tx, &statements::SCOPE_GENERATION_DELETE, &[&scope])
                .await
                .map_err(|e| sql(&e))?;
            let now = db
                .query_opt(&*tx, &statements::SCOPE_PURGED, &[&scope])
                .await
                .map_err(|e| sql(&e))?
                .is_some();
            if let Some(outbox) = &cleanup {
                let left: bool = db
                    .query_one(&*tx, &statements::SCOPE_BLOBS_LEFT, &[&scope])
                    .await
                    .map_err(|e| sql(&e))?
                    .get(0);
                if left {
                    outbox
                        .append(tx, &[cleanup_job(&scope)?], OnDuplicate::Ignore)
                        .await?;
                }
            }
            Ok(PurgeStep::Done { now })
        })
    })
    .await;
    result.map_err(|rejection| rejection.failure)
}

/// What [`purge_organization`] did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PurgeReport {
    /// This call marked the scope deleted (it was live).
    pub marked: bool,
    pub entries: usize,
    pub assets: usize,
    pub blobs: usize,
    /// The scope was `purged` already when the call began its last step.
    pub already_purged: bool,
}

/// Mark the organization `scope` deleted (by `by`) and run every
/// [`purge_step`] to the end, each bounded by `step_timeout`; `progress`
/// sees each step. The operator's purge (`merezha content
/// purge-organization`); safe beside a running purge job, whose steps
/// wait for this one's locks and find less to do.
pub async fn purge_organization(
    db: &Database,
    scope: &str,
    by: DeletedBy,
    cleanup: Option<&SourceOutbox>,
    step_timeout: Duration,
    mut progress: impl FnMut(PurgeStep),
) -> Result<PurgeReport, Failure> {
    let owned = db.clone();
    let mark = scope.to_owned();
    let marked = db
        .transaction(Instant::now() + step_timeout, move |tx| {
            Box::pin(async move { mark_deleted(&owned, tx, &mark, by).await })
        })
        .await?;
    let mut report = PurgeReport {
        marked,
        ..PurgeReport::default()
    };
    loop {
        let step = purge_step(db, Instant::now() + step_timeout, scope, cleanup).await?;
        progress(step);
        match step {
            PurgeStep::Entries(n) => report.entries += n,
            PurgeStep::Assets(n) => report.assets += n,
            PurgeStep::Blobs(n) => report.blobs += n,
            PurgeStep::Done { now } => {
                report.already_purged = !now;
                return Ok(report);
            }
            PurgeStep::NotDeleted => return Err(Failure::transient("organization_not_deleted")),
        }
    }
}

/// The purge job record of `scope`, keyed by the scope's tag (never the
/// organization id); `continuation` names the job a stopped run continues.
pub(crate) fn purge_job(
    scope: &str,
    tenant: Option<Identity>,
    correlation: Option<Identity>,
    continuation: Option<i64>,
) -> Result<OutboxRecord, Failure> {
    let tag = crate::media::scope_tag(scope)?;
    let key = match continuation {
        None => format!("organization_purge:{tag}"),
        Some(job) => format!("organization_purge:{tag}:after:{job}"),
    };
    Ok(OutboxRecord {
        dedupe_key: DedupeKey::new(key.clone())?,
        target: OutboxTarget::Job {
            queue: Identity::new(QUEUE)?,
            kind: Identity::new(ORGANIZATION_PURGE)?,
        },
        payload: json!({ "scope": scope }),
        contains_secret: false,
        run_at: None,
        priority: 0,
        context: MessageContext {
            correlation_id: match correlation {
                Some(correlation) => correlation,
                None => Identity::new(key)?,
            },
            causation_id: None,
            tenant,
        },
    })
}

/// The `content.blob_cleanup` run a finished purge asks for, so the
/// released objects go now rather than at the next `media.cleanup_cron`.
fn cleanup_job(scope: &str) -> Result<OutboxRecord, Rejection> {
    let tag = crate::media::scope_tag(scope)?;
    let key = format!("organization_purge:{tag}:cleanup");
    Ok(OutboxRecord {
        dedupe_key: DedupeKey::new(key.clone())?,
        target: OutboxTarget::Job {
            queue: Identity::new(QUEUE)?,
            kind: Identity::new(crate::media::BLOB_CLEANUP)?,
        },
        payload: json!({}),
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

/// The organization a job's scope names, as the events' tenant.
fn tenant_of(scope: &str) -> Option<Identity> {
    scope
        .strip_prefix("org:")
        .and_then(|org| Identity::new(org).ok())
}

/// The `content.organization_deleted` handler: the scope of the event's
/// `organization_id` is marked deleted and its purge appended in one
/// transaction. A redelivered event finds the mark and appends nothing
/// new (the purge record's key is the scope's).
pub(crate) struct OrganizationDeleted {
    pub(crate) store: Store,
}
impl JobHandler for OrganizationDeleted {
    fn handle<'a>(
        &'a self,
        ctx: JobContext,
        payload: Value,
    ) -> BoxFuture<'a, Result<(), JobFailure>> {
        Box::pin(async move {
            let event = IntegrationEvent::from_value(&payload)?;
            if event.event_type != ORGANIZATION_DELETED_TOPIC {
                return Err(JobFailure::permanent("invalid_job_payload"));
            }
            let organization = event
                .payload
                .get("organization_id")
                .and_then(Value::as_str)
                .ok_or_else(|| JobFailure::permanent("invalid_job_payload"))?;
            let scope = organization_scope(organization)?;
            let tenant = tenant_of(&scope);
            let record = purge_job(
                &scope,
                tenant.clone(),
                Some(ctx.correlation_id.clone()),
                None,
            )?;
            let db = self.store.database().clone();
            let state = self.store.state().clone();
            let outbox = self.store.outbox().clone();
            let marked_scope = scope.clone();
            let marked = transaction(self.store.database(), self.store.deadline(), move |tx| {
                Box::pin(async move {
                    state.check_writable(&db, tx).await?;
                    let marked = mark_deleted(&db, tx, &marked_scope, DeletedBy::Event).await?;
                    outbox.append(tx, &[record], OnDuplicate::Ignore).await?;
                    Ok(marked)
                })
            })
            .await
            .map_err(|r| JobFailure::from(r.failure))?;
            if marked && let Some(tenant) = tenant {
                merezha_telemetry::event!(
                    INFO,
                    "content.organization_deleted",
                    tenant = tenant.as_str(),
                    source = DeletedBy::Event.as_str()
                );
            }
            Ok(())
        })
    }
}

/// The `content.organization_purge` handler: [`purge_step`]s until the
/// scope is `purged`, at least one per run; when the job is cancelled or
/// its deadline nears, its continuation is appended and the next run goes
/// on. Registered through [`crate::ContentState::guard_jobs`].
pub(crate) struct OrganizationPurge {
    pub(crate) store: Store,
}
impl JobHandler for OrganizationPurge {
    fn handle<'a>(
        &'a self,
        ctx: JobContext,
        payload: Value,
    ) -> BoxFuture<'a, Result<(), JobFailure>> {
        Box::pin(async move {
            let scope = payload
                .get("scope")
                .and_then(Value::as_str)
                .and_then(|scope| scope.strip_prefix("org:"))
                .and_then(|org| organization_scope(org).ok())
                .ok_or_else(|| JobFailure::permanent("invalid_job_payload"))?;
            let tenant = tenant_of(&scope);
            let cleanup = self
                .store
                .state()
                .media()
                .and_then(|media| media.assets())
                .map(|_| self.store.outbox().clone());
            let margin = self.store.state().settings().limits.query_timeout;
            let mut report = PurgeReport::default();
            let mut first = true;
            loop {
                if !first
                    && (*ctx.cancel.borrow()
                        || tokio::time::Instant::now() + margin >= ctx.deadline)
                {
                    let record = purge_job(
                        &scope,
                        tenant.clone(),
                        Some(ctx.correlation_id.clone()),
                        Some(ctx.id),
                    )?;
                    let outbox = self.store.outbox().clone();
                    transaction(self.store.database(), self.store.deadline(), move |tx| {
                        Box::pin(async move {
                            // A retried run of the same job finds it there.
                            outbox.append(tx, &[record], OnDuplicate::Ignore).await?;
                            Ok(())
                        })
                    })
                    .await
                    .map_err(|r| JobFailure::from(r.failure))?;
                    return Ok(());
                }
                first = false;
                match purge_step(
                    self.store.database(),
                    self.store.deadline(),
                    &scope,
                    cleanup.as_ref(),
                )
                .await?
                {
                    PurgeStep::Entries(n) => report.entries += n,
                    PurgeStep::Assets(n) => report.assets += n,
                    PurgeStep::Blobs(n) => report.blobs += n,
                    PurgeStep::Done { now } => {
                        if now && let Some(tenant) = &tenant {
                            merezha_telemetry::event!(
                                INFO,
                                "content.organization_purged",
                                tenant = tenant.as_str(),
                                entries = report.entries,
                                assets = report.assets,
                                blobs = report.blobs
                            );
                        }
                        return Ok(());
                    }
                    // Never appended without the mark; nothing to purge.
                    PurgeStep::NotDeleted => {
                        return Err(JobFailure::permanent("organization_not_deleted"));
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn organization_scopes_take_only_what_the_scope_column_accepts() {
        assert_eq!(
            organization_scope("org_0192f5c4e1b07a3c9d2e4f6a8b0c1d2a").unwrap(),
            "org:org_0192f5c4e1b07a3c9d2e4f6a8b0c1d2a"
        );
        assert_eq!(organization_scope("acme").unwrap(), "org:acme");
        for bad in ["", "a b", "é", "x;drop", &"a".repeat(129)] {
            assert_eq!(
                organization_scope(bad).unwrap_err().code,
                "invalid_organization",
                "{bad}"
            );
        }
    }

    #[test]
    fn purge_keys_carry_the_scope_tag_never_the_id() {
        let scope = "org:org_0192f5c4e1b07a3c9d2e4f6a8b0c1d2a";
        let first = purge_job(scope, None, None, None).unwrap();
        let next = purge_job(scope, None, None, Some(42)).unwrap();
        let cleanup = cleanup_job(scope).unwrap();
        for record in [&first, &next, &cleanup] {
            assert!(!record.dedupe_key.as_str().contains("0192f5c4"));
            assert!(
                record
                    .dedupe_key
                    .as_str()
                    .starts_with("organization_purge:org-")
            );
        }
        assert!(next.dedupe_key.as_str().ends_with(":after:42"));
        assert_eq!(first.payload, json!({"scope": scope}));
    }
}
