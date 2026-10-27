//! Maintenance tasks of this slice (NT §2.5), declared as cron schedules on
//! `notify.maintenance` with the skip missed-run policy: `settle_notifications`
//! every minute, `purge_data` every five minutes, `inbox_expire` and
//! (NT-N6) `push_expire` hourly, and (NT-N7) `webhook_auto_disable` every
//! five minutes and `retention` daily. `retention` ([`retention`]) deletes
//! notifications settled more than `retention.deliveries` ago with their
//! deliveries and attempts, provider events received before then, and the
//! captures past the newest `retention.captures` (NT-N8), then runs the
//! webhook part ([`crate::webhooks::tasks::retention`]). `reclaim_stuck`
//! (every five minutes, issue #63, [`reclaim_stuck`]) settles the
//! deliveries whose job was lost.
//! Each run works in bounded batches within its deadline.
use crate::{notifier::Core, sql};
use merezha_domain::{Failure, Identity};
use merezha_platform::ports::{DedupeKey, MessageContext, OnDuplicate, OutboxRecord, OutboxTarget};
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
    ReclaimStuck,
}
impl Task {
    pub const ALL: [Task; 7] = [
        Task::SettleNotifications,
        Task::PurgeData,
        Task::InboxExpire,
        Task::PushExpire,
        Task::WebhookAutoDisable,
        Task::Retention,
        Task::ReclaimStuck,
    ];
    pub fn name(self) -> &'static str {
        match self {
            Task::SettleNotifications => "settle_notifications",
            Task::PurgeData => "purge_data",
            Task::InboxExpire => "inbox_expire",
            Task::PushExpire => "push_expire",
            Task::WebhookAutoDisable => "webhook_auto_disable",
            Task::Retention => "retention",
            Task::ReclaimStuck => "reclaim_stuck",
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
            Task::ReclaimStuck => "*/5 * * * *",
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
        Task::ReclaimStuck => reclaim_stuck(core, deadline).await,
    }
}

/// How much one `retention` run does: batches of `notifications`
/// notifications (each with its deliveries and attempts) and of `rows`
/// provider events, one short transaction each, until a batch comes back
/// short, `max_batches` ran, or the job's deadline is
/// [`RETENTION_MARGIN`] away. Work left over is handed to a continuation
/// job ([`retention`]), so a backlog drains run after run instead of once
/// a day.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetentionBounds {
    pub notifications: i64,
    pub rows: i64,
    pub max_batches: usize,
}
impl Default for RetentionBounds {
    fn default() -> Self {
        Self {
            notifications: 100,
            rows: BATCH,
            max_batches: usize::MAX,
        }
    }
}
/// No retention batch but a step's first starts closer than this to the
/// job's deadline.
pub const RETENTION_MARGIN: std::time::Duration = std::time::Duration::from_secs(3);
/// The shortest `notify.maintenance` `timeout_ms` startup accepts: the
/// [`RETENTION_MARGIN`] plus room for batches, so a `retention` run always
/// gets work done before its margin (issue #63).
pub const MIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Whether a step may start batch `batches + 1`: its first batch always
/// runs, so a run with work makes progress whatever its deadline (issue
/// #63); later ones only while the deadline is more than the margin away.
pub(crate) fn may_continue(batches: usize, deadline: Instant) -> bool {
    batches == 0 || Instant::now() + RETENTION_MARGIN < deadline
}

/// Delete notifications settled before `before`, batch by batch (see
/// [`RetentionBounds`]). Each batch locks the candidates' deliveries in id
/// order first, as `redeliver` does before it reopens a notification, then
/// deletes the candidates still settled before `before`: a notification
/// reopened meanwhile is kept, and the two never wait on each other
/// crosswise. The rows deleted, and whether work remains.
async fn delete_notifications(
    core: &Core,
    before: jiff::Timestamp,
    bounds: RetentionBounds,
    deadline: Instant,
) -> Result<(u64, bool), Failure> {
    let mut total = 0;
    let mut batches = 0;
    loop {
        if batches >= bounds.max_batches || !may_continue(batches, deadline) {
            return Ok((total, true));
        }
        batches += 1;
        let db = core.database.clone();
        let limit = bounds.notifications;
        let (found, deleted): (usize, u64) = db
            .clone()
            .transaction(deadline, move |tx| {
                Box::pin(async move {
                    let ids: Vec<Uuid> = db
                        .query(tx, &sql::RETENTION_CANDIDATES, &[&before, &limit])
                        .await
                        .map_err(|e| classify(&e))?
                        .iter()
                        .map(|row| row.get(0))
                        .collect();
                    if ids.is_empty() {
                        return Ok((0, 0));
                    }
                    db.query(tx, &sql::RETENTION_LOCK_DELIVERIES, &[&ids])
                        .await
                        .map_err(|e| classify(&e))?;
                    let deleted = db
                        .execute(tx, &sql::RETENTION_NOTIFICATIONS, &[&ids, &before])
                        .await
                        .map_err(|e| classify(&e))?;
                    Ok((ids.len(), deleted))
                })
            })
            .await?;
        total += deleted;
        if (found as i64) < limit {
            return Ok((total, false));
        }
    }
}

/// Delete provider events received before `before`, batch by batch; the
/// rows deleted, and whether work remains.
async fn delete_provider_events(
    core: &Core,
    before: jiff::Timestamp,
    bounds: RetentionBounds,
    deadline: Instant,
) -> Result<(u64, bool), Failure> {
    let mut total = 0;
    let mut batches = 0;
    loop {
        if batches >= bounds.max_batches || !may_continue(batches, deadline) {
            return Ok((total, true));
        }
        batches += 1;
        let client = core.database.acquire(deadline).await?;
        let deleted = core
            .database
            .execute(
                &client,
                &sql::RETENTION_PROVIDER_EVENTS,
                &[&before, &bounds.rows],
            )
            .await
            .map_err(|e| classify(&e))?;
        total += deleted;
        if (deleted as i64) < bounds.rows {
            return Ok((total, false));
        }
    }
}

/// The `retention` task (daily, and again through continuation jobs while
/// a backlog remains): the rows it deleted or changed. Every step runs
/// whatever the earlier ones did; the first failure is the task's outcome
/// and each is logged `notification.maintenance_step_failed`. When a step
/// stopped with work left and the run changed rows, the run appends one
/// more `notify.maintenance.retention` job (outbox key
/// `retention:more:{ms}`), which runs as soon as a worker is free. A run
/// that changed nothing chains nothing, so a deadline too short for any
/// work cannot keep the maintenance queue busy (issue #63); the next daily
/// run tries again.
pub async fn retention(core: &Arc<Core>, deadline: Instant) -> Result<u64, Failure> {
    let bounds = core.retention;
    let mut first_failure = None;
    let mut total = 0;
    let mut more = false;
    let mut step = |name: &'static str, result: Result<(u64, bool), Failure>| match result {
        Ok((rows, left)) => {
            total += rows;
            more |= left;
        }
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
        Ok(at) => delete_notifications(core, *at, bounds, deadline).await,
        Err(failure) => Err(failure.clone()),
    };
    step("notifications", notifications);
    let events = match &before {
        Ok(at) => delete_provider_events(core, *at, bounds, deadline).await,
        Err(failure) => Err(failure.clone()),
    };
    step("provider_events", events);
    step(
        "captures",
        trim_captures(core, deadline)
            .await
            .map(|rows| (rows, false)),
    );
    // The webhook part logs its own failed steps.
    match crate::webhooks::tasks::retention(core, deadline).await {
        Ok((rows, left)) => {
            total += rows;
            more |= left;
        }
        Err(failure) => {
            first_failure.get_or_insert(failure);
        }
    }
    if more
        && total > 0
        && let Err(failure) = continue_retention(core, deadline).await
    {
        merezha_telemetry::event!(
            WARN,
            "notification.maintenance_step_failed",
            task = "retention",
            reason = "continuation",
            code = failure.code.as_str()
        );
        first_failure.get_or_insert(failure);
    }
    match first_failure {
        Some(failure) => Err(failure),
        None => Ok(total),
    }
}

/// Append one more `notify.maintenance.retention` job.
async fn continue_retention(core: &Core, deadline: Instant) -> Result<(), Failure> {
    let record = OutboxRecord {
        dedupe_key: DedupeKey::new(format!("retention:more:{}", core.now().as_millisecond()))?,
        target: OutboxTarget::Job {
            queue: Identity::new(QUEUE)?,
            kind: Identity::new(Task::Retention.kind())?,
        },
        payload: serde_json::json!({}),
        contains_secret: false,
        run_at: None,
        priority: 0,
        context: MessageContext {
            correlation_id: Identity::new("notify-retention")?,
            causation_id: None,
            tenant: None,
        },
    };
    let outbox = core.outbox.clone();
    core.database
        .transaction(deadline, move |tx| {
            Box::pin(async move {
                outbox.append(tx, &[record], OnDuplicate::Ignore).await?;
                Ok(())
            })
        })
        .await
}

/// A delivery still open whose `run_at` and `updated_at` are both older
/// than this lost its job without a recorded outcome (NT §2.5).
pub const STUCK_AFTER: std::time::Duration = std::time::Duration::from_secs(24 * 3600);
/// Deliveries per `reclaim_stuck` transaction.
const RECLAIM_BATCH: i64 = 100;

/// `reclaim_stuck` (every 5 minutes, NT §2.5): a delivery still `queued`,
/// `sending` or `retrying` whose `run_at` and `updated_at` are both more
/// than [`STUCK_AFTER`] old lost its job (to `handler_panic`, a dead
/// letter the operator never replayed, a lost outbox record) and would
/// stay open forever, its notification never settled and never deleted
/// by `retention` (issue #63). It is settled `job_lost`: `unknown` when
/// its last attempt was still `started` (the send may have gone out, so
/// it is never resent by itself, NT1), else `failed`. Provider events
/// stored `pending` for it apply in the same transaction, as Tx B would
/// have applied them. Webhook deliveries open and due for as long are
/// settled `failed` / `job_lost` the same way. The rows it settled.
pub async fn reclaim_stuck(core: &Core, deadline: Instant) -> Result<u64, Failure> {
    let now = core.now();
    let before = jiff::SignedDuration::try_from(STUCK_AFTER)
        .ok()
        .and_then(|age| now.checked_sub(age).ok())
        .ok_or_else(|| Failure::permanent("invalid_config"))?;
    let mut total = 0;
    for batch in 0..MAX_BATCHES {
        if !may_continue(batch, deadline) {
            break;
        }
        let db = core.database.clone();
        let (found, settled): (usize, Vec<(Uuid, String, String, String)>) = db
            .clone()
            .transaction(deadline, move |tx| {
                Box::pin(async move {
                    let ids: Vec<Uuid> = db
                        .query(tx, &sql::RECLAIM_CANDIDATES, &[&before, &RECLAIM_BATCH])
                        .await
                        .map_err(|e| classify(&e))?
                        .iter()
                        .map(|row| row.get(0))
                        .collect();
                    if ids.is_empty() {
                        return Ok((0, Vec::new()));
                    }
                    let started: Vec<Uuid> = db
                        .query(tx, &sql::RECLAIM_ATTEMPTS, &[&ids, &now])
                        .await
                        .map_err(|e| classify(&e))?
                        .iter()
                        .map(|row| row.get(0))
                        .collect();
                    let rows = db
                        .query(tx, &sql::RECLAIM_SET, &[&ids, &started, &now])
                        .await
                        .map_err(|e| classify(&e))?;
                    let mut settled = Vec::with_capacity(rows.len());
                    for row in &rows {
                        let id: Uuid = row.get(0);
                        crate::brevo::events::apply_pending(&db, tx, id, now).await?;
                        settled.push((id, row.get(1), row.get(2), row.get(3)));
                    }
                    Ok((ids.len(), settled))
                })
            })
            .await?;
        for (id, channel, status, topic) in &settled {
            let Some(channel) = crate::topics::channel(channel) else {
                continue;
            };
            let delivery = id.to_string();
            if status == "unknown" {
                merezha_telemetry::event!(
                    WARN,
                    "notification.ambiguous",
                    channel = channel.as_str(),
                    topic = topic.as_str(),
                    delivery_id = delivery.as_str(),
                    reason = JOB_LOST
                );
                core.metrics.delivery(channel, topic, "unknown", JOB_LOST);
            } else {
                merezha_telemetry::event!(
                    WARN,
                    "notification.failed",
                    channel = channel.as_str(),
                    topic = topic.as_str(),
                    delivery_id = delivery.as_str(),
                    code = JOB_LOST
                );
                core.metrics.delivery(channel, topic, "failed", JOB_LOST);
            }
        }
        total += settled.len() as u64;
        if (found as i64) < RECLAIM_BATCH {
            break;
        }
    }
    for batch in 0..MAX_BATCHES {
        if !may_continue(batch, deadline) {
            break;
        }
        let client = core.database.acquire(deadline).await?;
        let reclaimed = core
            .database
            .query(
                &client,
                &crate::webhooks::sql::RECLAIM,
                &[&before, &RECLAIM_BATCH, &now],
            )
            .await
            .map_err(|e| classify(&e))?
            .len() as u64;
        core.metrics.webhook_delivery("failed", reclaimed);
        total += reclaimed;
        if (reclaimed as i64) < RECLAIM_BATCH {
            break;
        }
    }
    Ok(total)
}
/// The reason of a delivery `reclaim_stuck` settled.
pub const JOB_LOST: &str = "job_lost";

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
