//! Every statement the jobs runtime runs, registered with the kernel's
//! statement registry (TE §1.3 "PostgreSQL"): each opens a DB span inside
//! its caller's and times `db.client.operation.duration`, and neither
//! carries the SQL, parameters or values. Each constant sits beside the
//! code that runs it; [`STATEMENTS`] lists them all, and the statements
//! two modules share live here.
use merezha_postgres::Statement;

/// Wakes the queue's dispatchers (AP-B2), delivered on commit.
pub(crate) const NOTIFY_QUEUE: Statement = Statement::new(
    "jobs.queue.notify",
    "SELECT pg_notify()",
    "SELECT pg_notify('merezha_jobs', $1)",
);
/// Wakes the relay, delivered on commit.
pub(crate) const NOTIFY_OUTBOX: Statement = Statement::new(
    "jobs.outbox.notify",
    "SELECT pg_notify()",
    "SELECT pg_notify('merezha_jobs_outbox', '')",
);

/// Every statement of `merezha-jobs`, for `merezha_postgres::check_registry`.
pub const STATEMENTS: &[Statement] = &[
    NOTIFY_QUEUE,
    NOTIFY_OUTBOX,
    // Enqueue, cancel, requeue and subscriptions (`lib.rs`).
    crate::ENQUEUE,
    crate::ENQUEUE_TRACE,
    crate::ENQUEUE_EXISTING,
    crate::CANCEL,
    crate::REQUEUE_OUTBOX,
    crate::REQUEUE_REFUSAL,
    crate::REQUEUE_AUDIT,
    crate::DECLARE_SUBSCRIPTIONS,
    crate::SUBSCRIPTIONS_LOCK,
    crate::RETIRE_SUBSCRIPTIONS,
    // The outbox port.
    crate::outbox::APPEND,
    crate::outbox::TRACE,
    // The relay.
    crate::relay::LOCK,
    crate::relay::RELAY,
    crate::relay::NOTIFY_QUEUES,
    crate::relay::PENDING_IDS,
    crate::relay::QUARANTINE,
    // Workers, heartbeat, reaper and drain.
    crate::worker::QUEUE_LOCK,
    crate::worker::LEASE,
    crate::worker::SETTLE,
    crate::worker::HEARTBEAT,
    crate::worker::REAP,
    crate::worker::RELEASE,
    crate::worker::LAG,
    crate::worker::REVERT_CLAIMS,
    // Schedules and the scheduler.
    crate::scheduler::SCHEDULES_LOCK,
    crate::scheduler::NOW,
    crate::scheduler::STORED,
    crate::scheduler::DECLARE,
    crate::scheduler::REDECLARE,
    crate::scheduler::REPLACE,
    crate::scheduler::KEEP,
    crate::scheduler::RETIRE,
    crate::scheduler::TICK_LOCK,
    crate::scheduler::DUE,
    crate::scheduler::ADVANCE,
    crate::scheduler::INSERT_JOB,
    crate::scheduler::EXISTING_JOB,
    crate::scheduler::LIST,
    crate::scheduler::PAUSE,
    crate::scheduler::RESUME_READ,
    crate::scheduler::RESUME,
    crate::scheduler::SUMMARY,
    crate::scheduler::TRIGGER_READ,
    // Retention.
    crate::retention::LOCK,
    crate::retention::PURGES[0].1,
    crate::retention::PURGES[1].1,
    crate::retention::PURGES[2].1,
    crate::retention::PURGES[3].1,
    crate::retention::PURGES[4].1,
    crate::retention::AUDIT,
    // The admin API and the status reads.
    crate::admin::AUDIT,
    crate::admin::CLAIM_CANDIDATE,
    crate::admin::CLAIM,
    crate::admin::REISSUE_SETTLED,
    crate::admin::REISSUE_REVERTED,
    crate::admin::LIST_JOBS,
    crate::admin::JOB_DETAIL,
    crate::admin::JOB_ATTEMPTS,
    crate::admin::REPLAY_CANDIDATE,
    crate::admin::REPLAY_ONE,
    crate::admin::DEAD_CANDIDATES,
    crate::admin::REPLAY_MANY,
    crate::admin::PURGE_DEAD,
    crate::admin::CANCEL,
    crate::admin::EXISTS,
    crate::admin::QUEUE_COUNTS,
    crate::admin::OUTBOX_BACKLOG,
    crate::admin::QUARANTINED,
];

#[cfg(test)]
mod tests {
    use super::STATEMENTS;

    #[test]
    fn the_statements_are_registered_beside_the_kernel() {
        assert_eq!(
            merezha_postgres::check_registry(&[merezha_postgres::KERNEL_STATEMENTS, STATEMENTS]),
            Ok(())
        );
        for statement in STATEMENTS {
            assert!(statement.name.starts_with("jobs."), "{}", statement.name);
        }
    }
}
