//! Maintenance tasks of this slice (NT §2.5), declared as cron schedules on
//! `notify.maintenance` with the skip missed-run policy: `settle_notifications`
//! every minute, `purge_data` every five minutes, `inbox_expire` and
//! (NT-N6) `push_expire` hourly, and (NT-N7) `webhook_auto_disable` every
//! five minutes and `retention` daily. `retention` ([`retention`]) deletes
//! notifications settled more than `retention.deliveries` ago with their
//! deliveries and attempts, provider events received before then, and the
//! captures past the newest `retention.captures` (NT-N8), then runs the
//! webhook part ([`crate::webhooks::tasks::retention`]).
//! Each run works in bounded batches within its deadline.
use crate::{notifier::Core, sql};
use merezha_domain::Failure;
use merezha_postgres::classify;
use std::{collections::BTreeMap, sync::Arc, time::Instant};
use uuid::Uuid;

/// The queue of every maintenance task.
pub const QUEUE: &str = "notify.maintenance";
/// Rows per batch.
const BATCH: i64 = 1000;
/// Users per inbox expiry batch, and items per batch.
const USERS_PER_BATCH: i64 = 100;
/// Batches per run, so a backlog drains over several runs.
const MAX_BATCHES: usize = 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Task {
    SettleNotifications,
    PurgeData,
    InboxExpire,
    PushExpire,
    WebhookAutoDisable,
    Retention,
}
impl Task {
    pub const ALL: [Task; 6] = [
        Task::SettleNotifications,
        Task::PurgeData,
        Task::InboxExpire,
        Task::PushExpire,
        Task::WebhookAutoDisable,
        Task::Retention,
    ];
    pub fn name(self) -> &'static str {
        match self {
            Task::SettleNotifications => "settle_notifications",
            Task::PurgeData => "purge_data",
            Task::InboxExpire => "inbox_expire",
            Task::PushExpire => "push_expire",
            Task::WebhookAutoDisable => "webhook_auto_disable",
            Task::Retention => "retention",
        }
    }
    /// The job kind and the schedule name: `notify.maintenance.<task>`.
    pub fn kind(self) -> String {
        format!("{QUEUE}.{}", self.name())
    }
    pub fn cron(self) -> &'static str {
        match self {
            Task::SettleNotifications => "* * * * *",
            Task::PurgeData => "*/5 * * * *",
            Task::InboxExpire => "17 * * * *",
            Task::PushExpire => "43 * * * *",
            Task::WebhookAutoDisable => "*/5 * * * *",
            Task::Retention => "29 3 * * *",
        }
    }
}

/// Run `task` once; the rows it changed.
pub async fn run(core: &Arc<Core>, task: Task, deadline: Instant) -> Result<u64, Failure> {
    match task {
        Task::SettleNotifications => settle(core, deadline).await,
        Task::PurgeData => purge_data(core, deadline).await,
        Task::InboxExpire => inbox_expire(core, deadline).await,
        // Without a push section the subscriptions are kept: a key id to
        // compare with exists only while push is on.
        Task::PushExpire => match &core.push {
            Some(sender) => {
                crate::push::expire(
                    &core.database,
                    &core.metrics,
                    sender.vapid().key_id(),
                    core.now(),
                    deadline,
                )
                .await
            }
            None => Ok(0),
        },
        Task::WebhookAutoDisable => crate::webhooks::tasks::auto_disable(core, deadline).await,
        Task::Retention => retention(core, deadline).await,
    }
}

/// Notifications per retention batch: each takes its deliveries and
/// attempts with it.
const NOTIFICATIONS_PER_BATCH: i64 = 100;

/// Run `statement` (`$1` the cutoff, `$2` the batch) until a batch comes
/// back short or [`MAX_BATCHES`] ran; the rows deleted.
async fn delete_before(
    core: &Core,
    statement: &merezha_postgres::Statement,
    before: jiff::Timestamp,
    batch: i64,
    deadline: Instant,
) -> Result<u64, Failure> {
    let mut total = 0;
    for _ in 0..MAX_BATCHES {
        let client = core.database.acquire(deadline).await?;
        let changed = core
            .database
            .execute(&client, statement, &[&before, &batch])
            .await
            .map_err(|e| classify(&e))?;
        total += changed;
        if changed < batch as u64 {
            break;
        }
    }
    Ok(total)
}

/// The daily `retention` task: the rows it deleted or changed. Every step
/// runs whatever the earlier ones did; the first failure is the task's
/// outcome and each is logged `notification.maintenance_step_failed`.
pub async fn retention(core: &Arc<Core>, deadline: Instant) -> Result<u64, Failure> {
    let mut first_failure = None;
    let mut total = 0;
    let mut step = |name: &'static str, result: Result<u64, Failure>| match result {
        Ok(rows) => total += rows,
        Err(failure) => {
            merezha_telemetry::event!(
                WARN,
                "notification.maintenance_step_failed",
                task = "retention",
                reason = name,
                code = failure.code.as_str()
            );
            first_failure.get_or_insert(failure);
        }
    };
    let before = jiff::SignedDuration::try_from(core.settings.retention.deliveries)
        .ok()
        .and_then(|keep| core.now().checked_sub(keep).ok())
        .ok_or_else(|| Failure::permanent("invalid_config"));
    let notifications = match &before {
        Ok(at) => {
            delete_before(
                core,
                &sql::RETENTION_NOTIFICATIONS,
                *at,
                NOTIFICATIONS_PER_BATCH,
                deadline,
            )
            .await
        }
        Err(failure) => Err(failure.clone()),
    };
    step("notifications", notifications);
    let events = match &before {
        Ok(at) => delete_before(core, &sql::RETENTION_PROVIDER_EVENTS, *at, BATCH, deadline).await,
        Err(failure) => Err(failure.clone()),
    };
    step("provider_events", events);
    step("captures", trim_captures(core, deadline).await);
    // The webhook part logs its own failed steps.
    match crate::webhooks::tasks::retention(core, deadline).await {
        Ok(rows) => total += rows,
        Err(failure) => {
            first_failure.get_or_insert(failure);
        }
    }
    match first_failure {
        Some(failure) => Err(failure),
        None => Ok(total),
    }
}

/// Keep the newest `retention.captures` captures, as the capture drivers
/// do on every capture, for a lowered setting or a driver switched off.
async fn trim_captures(core: &Core, deadline: Instant) -> Result<u64, Failure> {
    let keep = i64::from(core.settings.retention.captures.max(1));
    let client = core.database.acquire(deadline).await?;
    core.database
        .execute(&client, &crate::drivers::CAPTURE_TRIM, &[&keep])
        .await
        .map_err(|e| classify(&e))
}

/// Set `settled_at` on notifications whose deliveries all settled.
async fn settle(core: &Core, deadline: Instant) -> Result<u64, Failure> {
    let mut total = 0;
    for _ in 0..MAX_BATCHES {
        let client = core.database.acquire(deadline).await?;
        let now = core.now();
        let changed = core
            .database
            .execute(&client, &sql::SETTLE, &[&now, &BATCH])
            .await
            .map_err(|e| classify(&e))?;
        total += changed;
        if changed < BATCH as u64 {
            break;
        }
    }
    Ok(total)
}

/// Null the data of notifications settled more than
/// `retention.data_after_settled` ago (invariant 8), and their deliveries'
/// recipient names.
async fn purge_data(core: &Core, deadline: Instant) -> Result<u64, Failure> {
    let keep = jiff::SignedDuration::try_from(core.settings.retention.data_after_settled)
        .map_err(|_| Failure::permanent("invalid_config"))?;
    let mut total = 0;
    for _ in 0..MAX_BATCHES {
        let now = core.now();
        let before = now
            .checked_sub(keep)
            .map_err(|_| Failure::permanent("invalid_config"))?;
        let db = core.database.clone();
        let purged: Vec<Uuid> = db
            .clone()
            .transaction(deadline, move |tx| {
                Box::pin(async move {
                    let rows = db
                        .query(tx, &sql::PURGE_DATA, &[&now, &before, &BATCH])
                        .await
                        .map_err(|e| classify(&e))?;
                    let ids: Vec<Uuid> = rows.iter().map(|r| r.get(0)).collect();
                    if !ids.is_empty() {
                        db.execute(tx, &sql::PURGE_NAMES, &[&ids])
                            .await
                            .map_err(|e| classify(&e))?;
                    }
                    Ok(ids)
                })
            })
            .await?;
        let count = purged.len() as u64;
        core.metrics.data_purged(count);
        total += count;
        if count < BATCH as u64 {
            break;
        }
    }
    Ok(total)
}

/// Delete inbox items past `expires_at`, or archived more than
/// `inbox.archived_retention` ago, a batch of users at a time in ascending
/// user order, raising each user's `purged_through` in the same
/// transaction (NT §5.4).
async fn inbox_expire(core: &Core, deadline: Instant) -> Result<u64, Failure> {
    let archived = jiff::SignedDuration::try_from(core.settings.inbox.archived_retention)
        .map_err(|_| Failure::permanent("invalid_config"))?;
    let mut total = 0;
    for _ in 0..MAX_BATCHES {
        let now = core.now();
        let archived_before = now
            .checked_sub(archived)
            .map_err(|_| Failure::permanent("invalid_config"))?;
        let db = core.database.clone();
        let deleted: u64 = db
            .clone()
            .transaction(deadline, move |tx| {
                Box::pin(async move {
                    let users: Vec<String> = db
                        .query(
                            tx,
                            &sql::INBOX_EXPIRED_USERS,
                            &[&now, &archived_before, &USERS_PER_BATCH],
                        )
                        .await
                        .map_err(|e| classify(&e))?
                        .iter()
                        .map(|r| r.get(0))
                        .collect();
                    if users.is_empty() {
                        return Ok(0);
                    }
                    // Counters first, in ascending order, as every inbox
                    // writer locks them.
                    db.query(tx, &sql::COUNTERS_LOCK, &[&users])
                        .await
                        .map_err(|e| classify(&e))?;
                    let rows = db
                        .query(
                            tx,
                            &sql::INBOX_EXPIRE,
                            &[&users, &now, &archived_before, &(BATCH * 10)],
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
                    Ok(rows.len() as u64)
                })
            })
            .await?;
        total += deleted;
        if deleted == 0 {
            break;
        }
    }
    Ok(total)
}
