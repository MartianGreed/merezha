//! Maintenance tasks of this slice (NT §2.5), declared as cron schedules on
//! `notify.maintenance` with the skip missed-run policy: `settle_notifications`
//! every minute, `purge_data` every five minutes, `inbox_expire` and
//! (NT-N6) `push_expire` hourly.
//! Each run works in bounded batches within its deadline.
use crate::{notifier::Core, sql};
use merezha_domain::Failure;
use merezha_postgres::classify;
use std::{collections::BTreeMap, time::Instant};
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
}
impl Task {
    pub const ALL: [Task; 4] = [
        Task::SettleNotifications,
        Task::PurgeData,
        Task::InboxExpire,
        Task::PushExpire,
    ];
    pub fn name(self) -> &'static str {
        match self {
            Task::SettleNotifications => "settle_notifications",
            Task::PurgeData => "purge_data",
            Task::InboxExpire => "inbox_expire",
            Task::PushExpire => "push_expire",
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
        }
    }
}

/// Run `task` once; the rows it changed.
pub async fn run(core: &Core, task: Task, deadline: Instant) -> Result<u64, Failure> {
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
    }
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
