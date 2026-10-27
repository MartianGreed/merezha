//! Deleted organizations (issue #63, as content does since #60): when
//! identity deletes an organization, the block stops writing for that
//! tenant at once and purges what it holds of it.
//!
//! - The kind [`ORGANIZATION_DELETED_KIND`] is subscribed to identity's
//!   `identity.organization_deleted` ([`ORGANIZATION_DELETED_TOPIC`]). Its
//!   transaction takes the tenant's exclusive transaction lock, records
//!   the tenant in `merezha_notify.tenants_deleted` (migration 7) and
//!   appends the purge job. Every writer of a tenant's rows takes the
//!   tenant's shared lock and checks the mark first (`guard`): the
//!   expansion of a notification of the tenant (nothing is written, the
//!   request is logged `notification.rejected` / `tenant_deleted`), the
//!   webhook fan-out of its events (dropped), a new webhook endpoint or a
//!   test event in its registry and a push subscription made in it (404
//!   `not_found`).
//!   A writer in flight commits before the mark and is purged; none
//!   commits after it.
//! - The kind [`ORGANIZATION_PURGE_KIND`] deletes the tenant's rows in
//!   bounded transactions ([`purge_step`]): webhook events (their
//!   deliveries and attempts cascade), webhook endpoints (an audit row
//!   `delete` by `sys_notifications` each), push subscriptions made in the
//!   tenant, inbox items (raising each user's `purged_through`, as
//!   `inbox_expire` does), and notifications with their deliveries,
//!   attempts and provider events, deliveries locked in id order first as
//!   `retention` does. Pending deliveries go with their notification: a
//!   job that finds its delivery gone finishes. The last step sets
//!   `purged_at`. Every step is idempotent, so a run killed mid-way is
//!   finished by the next lease; a run that nears its deadline appends its
//!   continuation.
//!
//! Audit rows of the tenant's endpoints are kept (`retention` deletes them
//! after 400 days), as are its preferences, which belong to users. The
//! mark is kept: organization ids are never reused.
use crate::{
    notifier::{Core, Slot},
    sql,
    webhooks::worker,
};
use merezha_domain::{Failure, Identity};
use merezha_platform::{
    BoxFuture,
    ports::{
        DedupeKey, IntegrationEvent, JobContext, JobFailure, JobHandler, MessageContext,
        OnDuplicate, OutboxRecord, OutboxTarget,
    },
};
use merezha_postgres::{Database, Tx, classify};
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Instant};
use uuid::Uuid;

/// The identity event the block subscribes to.
pub const ORGANIZATION_DELETED_TOPIC: &str = "identity.organization_deleted";
/// The source that publishes it; an event of another source is refused.
const IDENTITY_SOURCE: &str = "identity";
/// The job kind of that subscription, on `notify.expand`: marks the tenant
/// deleted and appends the purge.
pub const ORGANIZATION_DELETED_KIND: &str = "notify.organization_deleted";
/// The purge job kind, on `notify.expand`, payload `{"tenant"}`.
pub const ORGANIZATION_PURGE_KIND: &str = "notify.organization_purge";
/// Rows of one kind one purge transaction deletes.
pub const PURGE_BATCH: i64 = 100;
/// The topics consumed, for [`merezha_platform::ModuleEvents`].
pub const CONSUMED: [&str; 1] = [ORGANIZATION_DELETED_TOPIC];
/// The `notification.rejected` code of a request for a deleted tenant.
pub const TENANT_DELETED: &str = "tenant_deleted";
/// Longest tenant id the tables store.
const MAX_TENANT: usize = 128;
/// No purge step but a run's first starts closer than this to its
/// deadline; the rest goes to a continuation job.
const PURGE_MARGIN: std::time::Duration = std::time::Duration::from_secs(2);
/// Namespace of the purge job keys, which never carry the tenant id.
const NS_TENANT: Uuid = Uuid::from_u128(0x6d7a_6e74_0003_5000_8000_7465_6e61_6e74);

/// In a writer's transaction: the tenant's shared lock, then whether the
/// tenant was deleted. A writer that gets `true` writes nothing of the
/// tenant. A statement of its own after the lock, so its snapshot sees a
/// mark that committed while the lock waited.
pub(crate) async fn guard(db: &Database, tx: &mut Tx<'_>, tenant: &str) -> Result<bool, Failure> {
    db.query_one(
        tx,
        &sql::TENANT_WRITE_LOCK,
        &[&sql::TENANT_LOCK_CLASS, &tenant],
    )
    .await
    .map_err(|e| classify(&e))?;
    Ok(db
        .query_opt(tx, &sql::TENANT_DELETED, &[&tenant])
        .await
        .map_err(|e| classify(&e))?
        .is_some())
}

fn valid_tenant(tenant: &str) -> bool {
    !tenant.is_empty() && tenant.len() <= MAX_TENANT && Identity::new(tenant).is_ok()
}

/// The purge job record of `tenant`, keyed by a hash of the tenant (never
/// the id); `after` names the job a stopped run continues.
fn purge_job(
    tenant: &str,
    correlation: Identity,
    after: Option<i64>,
) -> Result<OutboxRecord, Failure> {
    let tag = Uuid::new_v5(&NS_TENANT, tenant.as_bytes()).simple();
    let key = match after {
        None => format!("tenant_purge:{tag}"),
        Some(job) => format!("tenant_purge:{tag}:after:{job}"),
    };
    Ok(OutboxRecord {
        dedupe_key: DedupeKey::new(key)?,
        target: OutboxTarget::Job {
            queue: Identity::new(crate::notifier::EXPAND_QUEUE)?,
            kind: Identity::new(ORGANIZATION_PURGE_KIND)?,
        },
        payload: json!({ "tenant": tenant }),
        contains_secret: false,
        run_at: None,
        priority: 0,
        context: MessageContext {
            correlation_id: correlation,
            causation_id: None,
            tenant: Identity::new(tenant).ok(),
        },
    })
}

/// Mark `tenant` deleted and append its purge, in one transaction under
/// the tenant's exclusive lock, which waits for every writer holding the
/// shared one. `true` when this call marked it; a second call (a
/// redelivered event) appends nothing new, the purge record's key being
/// the tenant's.
pub async fn mark_deleted(
    core: &Core,
    tenant: &str,
    correlation: Identity,
    deadline: Instant,
) -> Result<bool, Failure> {
    if !valid_tenant(tenant) {
        return Err(Failure::permanent("invalid_job_payload"));
    }
    let record = purge_job(tenant, correlation, None)?;
    let db = core.database.clone();
    let outbox = core.outbox.clone();
    let now = core.now();
    let tenant = tenant.to_owned();
    db.clone()
        .transaction(deadline, move |tx| {
            Box::pin(async move {
                db.query_one(
                    tx,
                    &sql::TENANT_DELETE_LOCK,
                    &[&sql::TENANT_LOCK_CLASS, &tenant],
                )
                .await
                .map_err(|e| classify(&e))?;
                let marked = db
                    .query_opt(tx, &sql::TENANT_MARK, &[&tenant, &now])
                    .await
                    .map_err(|e| classify(&e))?
                    .is_some();
                outbox.append(tx, &[record], OnDuplicate::Ignore).await?;
                Ok(marked)
            })
        })
        .await
}

/// What one [`purge_step`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PurgeStep {
    WebhookEvents(u64),
    WebhookEndpoints(u64),
    PushSubscriptions(u64),
    InboxItems(u64),
    Notifications(u64),
    /// Nothing is left; `now` when this step recorded the purge.
    Done {
        now: bool,
    },
}

/// What a whole purge deleted, for `notification.organization_purged`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Purged {
    pub webhook_events: u64,
    pub webhook_endpoints: u64,
    pub push_subscriptions: u64,
    pub inbox_items: u64,
    pub notifications: u64,
}
impl Purged {
    fn add(&mut self, step: PurgeStep) {
        match step {
            PurgeStep::WebhookEvents(n) => self.webhook_events += n,
            PurgeStep::WebhookEndpoints(n) => self.webhook_endpoints += n,
            PurgeStep::PushSubscriptions(n) => self.push_subscriptions += n,
            PurgeStep::InboxItems(n) => self.inbox_items += n,
            PurgeStep::Notifications(n) => self.notifications += n,
            PurgeStep::Done { .. } => {}
        }
    }
}

/// One bounded transaction of the purge of the deleted `tenant`: the next
/// [`PURGE_BATCH`] rows of the first kind that has any, else the end
/// (`purged_at`). Idempotent: a step that rolled back or ran twice leaves
/// the same rows.
pub async fn purge_step(
    core: &Core,
    tenant: &str,
    deadline: Instant,
) -> Result<PurgeStep, Failure> {
    let db = core.database.clone();
    let now = core.now();
    let tenant = tenant.to_owned();
    db.clone()
        .transaction(deadline, move |tx| {
            Box::pin(async move {
                let limit = PURGE_BATCH;
                let events = db
                    .execute(tx, &sql::TENANT_PURGE_EVENTS, &[&tenant, &limit])
                    .await
                    .map_err(|e| classify(&e))?;
                if events > 0 {
                    return Ok(PurgeStep::WebhookEvents(events));
                }
                let endpoints = db
                    .query(tx, &sql::TENANT_PURGE_ENDPOINTS, &[&tenant, &limit])
                    .await
                    .map_err(|e| classify(&e))?;
                if !endpoints.is_empty() {
                    let owner = Some(tenant.clone());
                    for row in &endpoints {
                        let (id, revision): (Uuid, i64) = (row.get(0), row.get(1));
                        worker::audit(
                            &db,
                            tx,
                            id,
                            &owner,
                            "delete",
                            None,
                            worker::SYSTEM_ACTOR,
                            revision + 1,
                            now,
                        )
                        .await?;
                    }
                    return Ok(PurgeStep::WebhookEndpoints(endpoints.len() as u64));
                }
                let push = db
                    .execute(tx, &sql::TENANT_PURGE_PUSH, &[&tenant, &limit])
                    .await
                    .map_err(|e| classify(&e))?;
                if push > 0 {
                    return Ok(PurgeStep::PushSubscriptions(push));
                }
                let users: Vec<String> = db
                    .query(tx, &sql::TENANT_INBOX_USERS, &[&tenant, &limit])
                    .await
                    .map_err(|e| classify(&e))?
                    .iter()
                    .map(|row| row.get(0))
                    .collect();
                if !users.is_empty() {
                    // Counters first, in ascending order, as every inbox
                    // writer locks them.
                    db.query(tx, &sql::COUNTERS_LOCK, &[&users])
                        .await
                        .map_err(|e| classify(&e))?;
                    let rows = db
                        .query(
                            tx,
                            &sql::TENANT_PURGE_INBOX,
                            &[&tenant, &users, &(limit * 10)],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                    let mut highest: BTreeMap<String, i64> = BTreeMap::new();
                    for row in &rows {
                        let (user, seq): (String, i64) = (row.get(0), row.get(1));
                        let entry = highest.entry(user).or_insert(seq);
                        *entry = (*entry).max(seq);
                    }
                    let (users, seqs): (Vec<String>, Vec<i64>) = highest.into_iter().unzip();
                    db.execute(tx, &sql::PURGED_THROUGH, &[&users, &seqs])
                        .await
                        .map_err(|e| classify(&e))?;
                    return Ok(PurgeStep::InboxItems(rows.len() as u64));
                }
                let ids: Vec<Uuid> = db
                    .query(tx, &sql::TENANT_NOTIFICATIONS, &[&tenant, &limit])
                    .await
                    .map_err(|e| classify(&e))?
                    .iter()
                    .map(|row| row.get(0))
                    .collect();
                if !ids.is_empty() {
                    // Deliveries first, in id order, as `redeliver` and
                    // `retention` lock them.
                    let deliveries: Vec<Uuid> = db
                        .query(tx, &sql::RETENTION_LOCK_DELIVERIES, &[&ids])
                        .await
                        .map_err(|e| classify(&e))?
                        .iter()
                        .map(|row| row.get(0))
                        .collect();
                    db.execute(tx, &sql::TENANT_PURGE_PROVIDER_EVENTS, &[&deliveries])
                        .await
                        .map_err(|e| classify(&e))?;
                    let deleted = db
                        .execute(tx, &sql::TENANT_PURGE_NOTIFICATIONS, &[&ids])
                        .await
                        .map_err(|e| classify(&e))?;
                    return Ok(PurgeStep::Notifications(deleted));
                }
                let now = db
                    .query_opt(tx, &sql::TENANT_PURGED, &[&tenant, &now])
                    .await
                    .map_err(|e| classify(&e))?
                    .is_some();
                Ok(PurgeStep::Done { now })
            })
        })
        .await
}

/// The tenant an organization event names (its `organization_id`, the
/// same string as `Principal.tenant`).
fn deleted_tenant(payload: &Value) -> Result<String, Failure> {
    let event = IntegrationEvent::from_value(payload)?;
    if event.event_type != ORGANIZATION_DELETED_TOPIC || event.source != IDENTITY_SOURCE {
        return Err(Failure::permanent("invalid_job_payload"));
    }
    event
        .payload
        .get("organization_id")
        .and_then(Value::as_str)
        .filter(|tenant| valid_tenant(tenant))
        .map(str::to_owned)
        .ok_or_else(|| Failure::permanent("invalid_job_payload"))
}

/// `notify.organization_deleted`: see the module docs.
pub struct OrganizationDeleted(pub Slot);
impl JobHandler for OrganizationDeleted {
    fn handle<'a>(
        &'a self,
        ctx: JobContext,
        payload: Value,
    ) -> BoxFuture<'a, Result<(), JobFailure>> {
        Box::pin(async move {
            let Some(core) = self.0.get().cloned() else {
                return Err(JobFailure::transient("unavailable", None));
            };
            let tenant = deleted_tenant(&payload)?;
            let marked = mark_deleted(
                &core,
                &tenant,
                ctx.correlation_id.clone(),
                ctx.deadline.into_std(),
            )
            .await?;
            if marked {
                merezha_telemetry::event!(
                    INFO,
                    "notification.organization_deleted",
                    tenant = tenant.as_str()
                );
            }
            Ok(())
        })
    }
}

/// `notify.organization_purge`: purge steps until done or the deadline is
/// near, then a continuation.
pub struct OrganizationPurge(pub Slot);
impl JobHandler for OrganizationPurge {
    fn handle<'a>(
        &'a self,
        ctx: JobContext,
        payload: Value,
    ) -> BoxFuture<'a, Result<(), JobFailure>> {
        Box::pin(async move {
            let Some(core) = self.0.get().cloned() else {
                return Err(JobFailure::transient("unavailable", None));
            };
            let tenant = payload
                .get("tenant")
                .and_then(Value::as_str)
                .filter(|tenant| valid_tenant(tenant))
                .map(str::to_owned)
                .ok_or_else(|| JobFailure::permanent("invalid_job_payload"))?;
            let deadline = ctx.deadline.into_std();
            let mut purged = Purged::default();
            let mut steps = 0usize;
            loop {
                if steps > 0 && Instant::now() + PURGE_MARGIN >= deadline {
                    // Work left: the next job of the chain goes on.
                    let record = purge_job(&tenant, ctx.correlation_id.clone(), Some(ctx.id))?;
                    let outbox = core.outbox.clone();
                    core.database
                        .transaction(deadline, move |tx| {
                            Box::pin(async move {
                                outbox.append(tx, &[record], OnDuplicate::Ignore).await?;
                                Ok(())
                            })
                        })
                        .await?;
                    return Ok(());
                }
                steps += 1;
                let step = purge_step(&core, &tenant, deadline).await?;
                purged.add(step);
                if let PurgeStep::Done { now } = step {
                    if now {
                        merezha_telemetry::event!(
                            INFO,
                            "notification.organization_purged",
                            tenant = tenant.as_str(),
                            webhook_events = purged.webhook_events,
                            webhook_endpoints = purged.webhook_endpoints,
                            push_subscriptions = purged.push_subscriptions,
                            inbox_items = purged.inbox_items,
                            notifications = purged.notifications
                        );
                    }
                    return Ok(());
                }
            }
        })
    }
}
