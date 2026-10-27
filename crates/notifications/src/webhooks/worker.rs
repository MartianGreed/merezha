//! The webhook delivery worker (`notify.webhook.deliver` on queue
//! `notify.webhook`, NT §5.1 and §5.5).
//!
//! 0. Take the endpoint's slot of `webhooks.concurrency_per_endpoint` on
//!    this instance (the job carries the endpoint id). A slot that does
//!    not free up in time retries the job without recording an attempt.
//! 1. **Tx A** locks the delivery. Another generation or a settled
//!    delivery is done; a disabled endpoint cancels it; an event whose body
//!    retention removed fails it (`event_body_expired`). A last attempt
//!    still `started` lost its outcome: it is marked `ambiguous` and the
//!    delivery is resent (receivers deduplicate on `webhook-id`). Attempt
//!    `n + 1` is committed as `started`.
//! 2. The secrets are opened, the frozen body signed for this attempt's
//!    timestamp, and the request sent through the guarded client before
//!    `min(webhooks.timeout, deadline - 2 s)`.
//! 3. **Tx B** records the outcome, fenced on the attempt still being
//!    `started`. A 2xx succeeds and clears `failing_since`. A 410 fails the
//!    delivery and disables the endpoint (`gone`), cancelling its queued
//!    deliveries. Any other failure sets `failing_since` if unset and
//!    retries after the next [`super::SCHEDULE`] step (±10 %), which the
//!    job hands to AP as `retry_after`; after attempt 10, or when the job's
//!    attempts run out, the delivery is `failed`.
use super::{Class, DeliverJob, MAX_ATTEMPTS, Sent, WebhookSender, next_delay, signing, sql};
use crate::{faults, handler::Outcome, notifier::Core};
use merezha_domain::Failure;
use merezha_platform::{
    BoxFuture,
    ports::{JobContext, JobFailure, JobHandler},
};
use merezha_postgres::classify;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::Instrument;
use uuid::Uuid;

/// Kept free before the job deadline for Tx B.
const RECORD_MARGIN: Duration = Duration::from_secs(2);
/// A job whose endpoint has no free slot hands the delivery to a new job
/// of the same generation this long after the first such hand-off,
/// doubling per hand-off up to [`BUSY_MAX_DELAY`], with ±50 % jitter.
pub const BUSY_DELAY: Duration = Duration::from_secs(1);
pub const BUSY_MAX_DELAY: Duration = Duration::from_secs(30);
/// A chain of hand-offs ends after this many, or once its first hand-off is
/// [`BUSY_WINDOW`] old: the busy endpoint then costs the delivery a
/// `failed` / `endpoint_busy` attempt on the schedule, so its backoff and
/// final `failed` apply.
pub const MAX_HANDOFFS: u32 = 20;
pub const BUSY_WINDOW: Duration = Duration::from_secs(5 * 60);
/// The code of a delivery whose last job attempt found its endpoint busy.
pub const ENDPOINT_BUSY: &str = "endpoint_busy";
/// The actor of the audit rows the block writes itself.
pub const SYSTEM_ACTOR: &str = "sys_notifications";

/// What Tx A found.
struct Locked {
    attempt: i32,
    /// The attempt's position in its generation (1-based).
    ladder: u32,
    generation: i32,
    url: String,
    /// Ciphertext, nonce and key id.
    secret: super::secrets::StoredSecret,
    previous: Option<super::secrets::StoredSecret>,
    body: Vec<u8>,
}

enum Prepared {
    Finished(Outcome),
    Send(Box<Locked>),
}

/// `notify.webhook.deliver`.
pub struct Handler(pub crate::notifier::Slot);
impl JobHandler for Handler {
    fn handle<'a>(
        &'a self,
        ctx: JobContext,
        payload: serde_json::Value,
    ) -> BoxFuture<'a, Result<(), JobFailure>> {
        Box::pin(async move {
            let Some(core) = self.0.get().cloned() else {
                return Err(JobFailure::transient("unavailable", None));
            };
            let Ok(job) = serde_json::from_value::<DeliverJob>(payload) else {
                return Err(JobFailure::permanent("invalid_job_payload"));
            };
            run(&core, &ctx, job).await.into_result()
        })
    }
}

pub(crate) async fn run(core: &Core, ctx: &JobContext, job: DeliverJob) -> Outcome {
    let span = tracing::info_span!(
        target: merezha_telemetry::TARGET,
        "notify webhook",
        notify.webhook_delivery_id = job.webhook_delivery_id.to_string().as_str(),
        notify.endpoint_id = job.endpoint_id.to_string().as_str(),
        notify.attempt = tracing::field::Empty,
        notify.generation = job.generation,
        http.response.status_code = tracing::field::Empty,
    );
    let outcome = deliver(core, ctx, &job).instrument(span).await;
    match outcome {
        // A retry on the job's last attempt would leave the delivery open
        // behind a dead job: it is settled `failed` with the code instead.
        Outcome::Retry { code, .. } if ctx.attempt >= ctx.max_attempts => {
            settle_exhausted(core, &job, &code).await
        }
        outcome => outcome,
    }
}

/// Settle open delivery `job` `failed` with `code` (an attempt still
/// `started` becomes `ambiguous`): the job's attempts are spent.
async fn settle_exhausted(core: &Core, job: &DeliverJob, code: &str) -> Outcome {
    let valid = !code.is_empty()
        && code.len() <= 64
        && code
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    let code = if valid { code } else { "retries_exhausted" }.to_owned();
    let db = core.database.clone();
    let now = core.now();
    let (id, generation) = (job.webhook_delivery_id, job.generation);
    let settled_code = code.clone();
    let written = db
        .clone()
        .transaction(Instant::now() + Duration::from_secs(10), move |tx| {
            Box::pin(async move {
                let state = db
                    .query_one(tx, &sql::ATTEMPT_STATE, &[&id, &generation])
                    .await
                    .map_err(|e| classify(&e))?;
                let (highest, last): (i32, Option<String>) = (state.get(0), state.get(2));
                if last.as_deref() == Some("started") {
                    db.execute(tx, &sql::ATTEMPT_LOST, &[&id, &highest, &now])
                        .await
                        .map_err(|e| classify(&e))?;
                }
                db.execute(
                    tx,
                    &sql::DELIVERY_SETTLE,
                    &[&id, &generation, &"failed", &settled_code, &now],
                )
                .await
                .map_err(|e| classify(&e))
            })
        })
        .await;
    // When even this fails, the delivery stays open; redeliver accepts it
    // once it has been untouched for an hour.
    if let (Ok(changed), Some(sender)) = (&written, &core.webhooks) {
        sender.metrics.webhook_delivery("failed", *changed);
    }
    Outcome::Permanent(code)
}

/// Hand the delivery to a new job of the same generation after a jittered
/// delay: its endpoint has no free slot on this instance. No attempt is
/// recorded and this job ends, so neither the schedule nor the job's
/// attempts are spent and no queue worker waits.
/// `busy` hand-offs since `since` (block clock) at schedule step `step`;
/// the follow-up runs on the database clock (`db_now`), as AP schedules.
async fn reschedule(
    core: &Core,
    ctx: &JobContext,
    job: &DeliverJob,
    busy: u32,
    since: i64,
    step: i64,
    db_now: jiff::Timestamp,
) -> Outcome {
    let doublings = busy.min(5);
    let (first, cap) = core
        .webhooks
        .as_ref()
        .map_or((BUSY_DELAY, BUSY_MAX_DELAY), |w| w.busy_delays);
    let base = (first * 2u32.pow(doublings)).min(cap);
    let mut bytes = [0u8; 8];
    let share = match core.random.fill(&mut bytes) {
        Ok(()) => u64::from_le_bytes(bytes) as f64 / u64::MAX as f64,
        Err(_) => 0.5,
    };
    let delay = base.mul_f64(0.5 + share);
    let next = DeliverJob {
        busy: busy.saturating_add(1),
        busy_since: Some(since),
        busy_step: Some(step),
        ..job.clone()
    };
    let at = db_now
        .checked_add(jiff::SignedDuration::try_from(delay).unwrap_or_default())
        .ok();
    let record = super::fanout::job_record(&next, &ctx.correlation_id, None).map(|mut record| {
        // Deterministic, so a hand-off replayed after a lost completion
        // dedupes instead of starting a second chain.
        if let Ok(key) = merezha_platform::ports::DedupeKey::new(format!(
            "webhook:{}:g{}:s{}:b{}",
            job.webhook_delivery_id, job.generation, step, next.busy
        )) {
            record.dedupe_key = key;
        }
        record.run_at = at;
        record.context.tenant = ctx.tenant.clone();
        record
    });
    let record = match record {
        Ok(record) => record,
        Err(failure) => return Outcome::retry(&failure.code),
    };
    let outbox = core.outbox.clone();
    let appended = core
        .database
        .transaction(Instant::now() + Duration::from_secs(10), move |tx| {
            Box::pin(async move {
                outbox
                    .append(tx, &[record], merezha_platform::ports::OnDuplicate::Ignore)
                    .await
            })
        })
        .await;
    match appended {
        Ok(_) => Outcome::Done,
        Err(_) => Outcome::Retry {
            after: Some(delay),
            code: ENDPOINT_BUSY.to_owned(),
        },
    }
}

async fn deliver(core: &Core, ctx: &JobContext, job: &DeliverJob) -> Outcome {
    let Some(sender) = core.webhooks.clone() else {
        return Outcome::Permanent(super::code::WEBHOOKS_DISABLED.to_owned());
    };
    let deadline = ctx.deadline.into_std();
    // Never wait for a slot: a queue worker stays free for other endpoints.
    let permit = sender.limiter.try_acquire(job.endpoint_id);
    if permit.is_none() {
        // Where the schedule stands, and the database's clock, which runs
        // the jobs.
        let state = match core.database.acquire(deadline).await {
            Ok(client) => core
                .database
                .query_one(
                    &client,
                    &sql::BUSY_STATE,
                    &[&job.webhook_delivery_id, &job.generation],
                )
                .await
                .map_err(|e| classify(&e)),
            Err(failure) => Err(failure),
        };
        let (step, db_now): (i64, jiff::Timestamp) = match state {
            Ok(row) => (row.get(0), row.get(1)),
            Err(failure) => return Outcome::retry(&failure.code),
        };
        // A chain belongs to one step of the schedule: once an attempt was
        // recorded since it began (a ladder retry runs the same payload),
        // the budget starts again.
        let (busy, since) = match (job.busy_step, job.busy_since) {
            (Some(chain_step), Some(since)) if chain_step == step => (job.busy, since),
            _ => (0, core.now().as_second()),
        };
        let window = i64::try_from(BUSY_WINDOW.as_secs()).unwrap_or(i64::MAX);
        if busy < MAX_HANDOFFS && core.now().as_second().saturating_sub(since) < window {
            return reschedule(core, ctx, job, busy, since, step, db_now).await;
        }
    }
    let id = job.webhook_delivery_id;
    let locked = match prepare(core, &sender, deadline, job).await {
        Ok(Prepared::Send(locked)) => locked,
        Ok(Prepared::Finished(outcome)) => return outcome,
        Err(failure) => return Outcome::retry(&failure.code),
    };
    tracing::Span::current().record("notify.attempt", locked.attempt);
    if permit.is_none() {
        // The hand-off budget is spent: the busy endpoint costs an attempt.
        let busy = Sent {
            class: Class::Busy,
            status: None,
            snippet: None,
        };
        return record(core, &sender, ctx, job, &locked, &busy, Duration::ZERO).await;
    }
    core.faults.hit(faults::AFTER_ATTEMPT_COMMITTED).await;
    let now = core.now();
    let current = sender.secrets.open(
        job.endpoint_id,
        &locked.secret.0,
        &locked.secret.1,
        locked.secret.2.as_deref(),
    );
    let Ok((current, _)) = current else {
        return settle_attempt(
            core,
            job,
            &locked,
            "failed",
            super::secrets::SECRET_UNREADABLE,
        )
        .await;
    };
    // The previous secret signs too while its rotation window is open; one
    // that does not open is left out rather than failing the delivery.
    let previous = locked.previous.as_ref().and_then(|(ct, nonce, key)| {
        sender
            .secrets
            .open(job.endpoint_id, ct, nonce, key.as_deref())
            .ok()
            .map(|(s, _)| s)
    });
    let timestamp = now.as_second();
    let message_id = signing::webhook_id(id);
    let mut secrets = vec![&current];
    secrets.extend(previous.as_ref());
    let signature = signing::header(&secrets, &message_id, timestamp, &locked.body);
    let headers = [
        ("webhook-id", message_id),
        ("webhook-timestamp", timestamp.to_string()),
        ("webhook-signature", signature),
    ];
    let send_by = deadline
        .checked_sub(RECORD_MARGIN)
        .unwrap_or(deadline)
        .min(Instant::now() + sender.settings.timeout);
    let started = Instant::now();
    let sent = sender
        .send(&locked.url, &headers, locked.body.clone(), send_by)
        .await;
    let took = started.elapsed();
    if let Some(status) = sent.status {
        tracing::Span::current().record("http.response.status_code", status);
    }
    core.faults.hit(faults::AFTER_PROVIDER_CALL).await;
    let outcome = record(core, &sender, ctx, job, &locked, &sent, took).await;
    core.faults.hit(faults::AFTER_TX_B).await;
    outcome
}

/// Tx A.
async fn prepare(
    core: &Core,
    sender: &Arc<WebhookSender>,
    deadline: Instant,
    job: &DeliverJob,
) -> Result<Prepared, Failure> {
    let db = core.database.clone();
    let now = core.now();
    let (id, generation) = (job.webhook_delivery_id, job.generation);
    let (prepared, settled) = db
        .clone()
        .transaction(deadline, move |tx| {
            Box::pin(async move {
                let Some(row) = db
                    .query_opt(tx, &sql::DELIVERY_LOCK, &[&id])
                    .await
                    .map_err(|e| classify(&e))?
                else {
                    return Ok((Prepared::Finished(Outcome::Done), None));
                };
                let status: String = row.get(0);
                let row_generation: i32 = row.get(1);
                if row_generation != generation
                    || !matches!(status.as_str(), "queued" | "sending" | "retrying")
                {
                    return Ok((Prepared::Finished(Outcome::Done), None));
                }
                // A deleted organization's deliveries go no further, even
                // before the purge reaches them (issue #63).
                if let Some(tenant) = row.get::<_, Option<String>>(13)
                    && crate::tenants::guard(&db, tx, &tenant).await?
                {
                    let code = crate::policy::reason::TENANT_DELETED;
                    settle_open(&db, tx, id, generation, "cancelled", code, now).await?;
                    return Ok((Prepared::Finished(Outcome::Done), Some("cancelled")));
                }
                let endpoint_status: String = row.get(2);
                if endpoint_status != "active" {
                    let code = super::code::ENDPOINT_DISABLED;
                    settle_open(&db, tx, id, generation, "cancelled", code, now).await?;
                    return Ok((Prepared::Finished(Outcome::Done), Some("cancelled")));
                }
                let Some(body) = row.get::<_, Option<Vec<u8>>>(9) else {
                    let code = super::code::EVENT_BODY_EXPIRED;
                    settle_open(&db, tx, id, generation, "failed", code, now).await?;
                    return Ok((
                        Prepared::Finished(Outcome::Permanent(
                            super::code::EVENT_BODY_EXPIRED.to_owned(),
                        )),
                        Some("failed"),
                    ));
                };
                let state = db
                    .query_one(tx, &sql::ATTEMPT_STATE, &[&id, &generation])
                    .await
                    .map_err(|e| classify(&e))?;
                let (highest, in_generation, last): (i32, i64, Option<String>) =
                    (state.get(0), state.get(1), state.get(2));
                if last.as_deref() == Some("started") {
                    // The previous outcome is unknown (invariant 4): a
                    // webhook is resent, with the same `webhook-id`.
                    db.execute(tx, &sql::ATTEMPT_LOST, &[&id, &highest, &now])
                        .await
                        .map_err(|e| classify(&e))?;
                }
                if in_generation >= i64::from(MAX_ATTEMPTS) {
                    let code = "retries_exhausted";
                    settle_open(&db, tx, id, generation, "failed", code, now).await?;
                    return Ok((
                        Prepared::Finished(Outcome::Permanent("retries_exhausted".into())),
                        Some("failed"),
                    ));
                }
                let attempt = highest + 1;
                db.execute(tx, &sql::ATTEMPT_START, &[&id, &attempt, &generation, &now])
                    .await
                    .map_err(|e| classify(&e))?;
                db.execute(tx, &sql::DELIVERY_SENDING, &[&id, &now])
                    .await
                    .map_err(|e| classify(&e))?;
                let previous_open = row
                    .get::<_, Option<jiff::Timestamp>>(8)
                    .is_some_and(|until| until > now);
                let previous: Option<super::secrets::StoredSecret> = match (row.get(6), row.get(7))
                {
                    (Some(ct), Some(nonce)) if previous_open => Some((ct, nonce, row.get(12))),
                    _ => None,
                };
                Ok((
                    Prepared::Send(Box::new(Locked {
                        attempt,
                        ladder: u32::try_from(in_generation + 1).unwrap_or(MAX_ATTEMPTS),
                        generation,
                        url: row.get(3),
                        secret: (row.get(4), row.get(5), row.get(11)),
                        previous,
                        body,
                    })),
                    None,
                ))
            })
        })
        .await?;
    if let Some(outcome) = settled {
        sender.metrics.webhook_delivery(outcome, 1);
    }
    Ok(prepared)
}

/// Settle open delivery `id` of `generation` without an attempt.
async fn settle_open(
    db: &merezha_postgres::Database,
    tx: &mut merezha_postgres::Tx<'_>,
    id: Uuid,
    generation: i32,
    status: &'static str,
    code: &'static str,
    now: jiff::Timestamp,
) -> Result<u64, Failure> {
    db.execute(
        tx,
        &sql::DELIVERY_SETTLE,
        &[&id, &generation, &status, &code, &now],
    )
    .await
    .map_err(|e| classify(&e))
}

/// Settle the delivery whose attempt could not be sent, recording the
/// attempt `failed` with `code`.
async fn settle_attempt(
    core: &Core,
    job: &DeliverJob,
    locked: &Locked,
    status: &'static str,
    code: &'static str,
) -> Outcome {
    let db = core.database.clone();
    let now = core.now();
    let (id, attempt, generation) = (job.webhook_delivery_id, locked.attempt, locked.generation);
    let deadline = Instant::now() + Duration::from_secs(10);
    let written = db
        .clone()
        .transaction(deadline, move |tx| {
            Box::pin(async move {
                let finished = db
                    .execute(
                        tx,
                        &sql::ATTEMPT_FINISH,
                        &[
                            &id,
                            &attempt,
                            &"failed",
                            &now,
                            &None::<i16>,
                            &None::<i32>,
                            &code,
                            &None::<String>,
                        ],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                if finished == 0 {
                    return Ok(0);
                }
                db.execute(
                    tx,
                    &sql::DELIVERY_RECORD,
                    &[
                        &id,
                        &generation,
                        &status,
                        &None::<i16>,
                        &code,
                        &None::<jiff::Timestamp>,
                        &now,
                    ],
                )
                .await
                .map_err(|e| classify(&e))
            })
        })
        .await;
    match written {
        Ok(0) => Outcome::Done,
        Ok(_) => {
            if let Some(sender) = &core.webhooks {
                sender.metrics.webhook_delivery(status, 1);
            }
            Outcome::Permanent(code.to_owned())
        }
        Err(failure) => Outcome::retry(&failure.code),
    }
}

/// What Tx B writes and the job answers.
struct Step {
    attempt: &'static str,
    status: &'static str,
    code: Option<String>,
    next: Option<Duration>,
    job: Outcome,
}

fn step(sent: &Sent, ladder: u32, exhausted: bool, share: f64) -> Step {
    let code = sent.class.error_code().map(str::to_owned);
    match sent.class {
        Class::Success => Step {
            attempt: "succeeded",
            status: "succeeded",
            code: None,
            next: None,
            job: Outcome::Done,
        },
        Class::Gone => Step {
            attempt: "failed",
            status: "failed",
            code,
            next: None,
            job: Outcome::Permanent("gone".into()),
        },
        class => {
            let attempt = if class == Class::Blocked {
                "blocked"
            } else {
                "failed"
            };
            match next_delay(ladder, share).filter(|_| !exhausted) {
                Some(after) => Step {
                    attempt,
                    status: "retrying",
                    code: code.clone(),
                    next: Some(after),
                    job: Outcome::Retry {
                        after: Some(after),
                        code: code.unwrap_or_else(|| "failed".into()),
                    },
                },
                None => Step {
                    attempt,
                    status: "failed",
                    code,
                    next: None,
                    job: Outcome::Permanent("retries_exhausted".into()),
                },
            }
        }
    }
}

fn plus(at: jiff::Timestamp, by: Duration) -> jiff::Timestamp {
    jiff::SignedDuration::try_from(by)
        .ok()
        .and_then(|d| at.checked_add(d).ok())
        .unwrap_or(jiff::Timestamp::MAX)
}

/// A uniform share in `[0, 1]` from the context's randomness; the middle
/// when it fails.
fn share(core: &Core) -> f64 {
    let mut bytes = [0u8; 8];
    match core.random.fill(&mut bytes) {
        Ok(()) => u64::from_le_bytes(bytes) as f64 / u64::MAX as f64,
        Err(_) => 0.5,
    }
}

/// Tx B.
async fn record(
    core: &Core,
    sender: &Arc<WebhookSender>,
    ctx: &JobContext,
    job: &DeliverJob,
    locked: &Locked,
    sent: &Sent,
    took: Duration,
) -> Outcome {
    let now = core.now();
    let exhausted = ctx.attempt >= ctx.max_attempts;
    let step = step(sent, locked.ladder, exhausted, share(core));
    let db = core.database.clone();
    let (id, endpoint, attempt, generation) = (
        job.webhook_delivery_id,
        job.endpoint_id,
        locked.attempt,
        locked.generation,
    );
    let http_status = sent.status.and_then(|s| i16::try_from(s).ok());
    let duration_ms = i32::try_from(took.as_millis()).unwrap_or(i32::MAX);
    let snippet = sent.snippet.clone();
    let (attempt_outcome, status, code) = (step.attempt, step.status, step.code.clone());
    let next_at = step.next.map(|after| plus(now, after));
    let success = sent.class == Class::Success;
    let busy = sent.class == Class::Busy;
    let gone = sent.class == Class::Gone;
    let deadline = Instant::now() + Duration::from_secs(10);
    let written = db
        .clone()
        .transaction(deadline, move |tx| {
            Box::pin(async move {
                let finished = db
                    .execute(
                        tx,
                        &sql::ATTEMPT_FINISH,
                        &[
                            &id,
                            &attempt,
                            &attempt_outcome,
                            &now,
                            &http_status,
                            &duration_ms,
                            &code,
                            &snippet,
                        ],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                // Another run took the delivery over: it records the outcome.
                if finished == 0 {
                    return Ok(None);
                }
                db.execute(
                    tx,
                    &sql::DELIVERY_RECORD,
                    &[
                        &id,
                        &generation,
                        &status,
                        &http_status,
                        &code,
                        &next_at,
                        &now,
                    ],
                )
                .await
                .map_err(|e| classify(&e))?;
                // A busy endpoint is this instance's limit, not the
                // receiver failing: it leaves `failing_since` alone.
                let health = match (success, busy) {
                    (true, _) => Some(&sql::ENDPOINT_SUCCEEDED),
                    (false, false) => Some(&sql::ENDPOINT_FAILED),
                    (false, true) => None,
                };
                if let Some(health) = health {
                    db.execute(tx, health, &[&endpoint, &now])
                        .await
                        .map_err(|e| classify(&e))?;
                }
                let cancelled = if gone {
                    disable(&db, tx, endpoint, "gone", now, None, SYSTEM_ACTOR).await?
                } else {
                    None
                };
                Ok(Some(cancelled))
            })
        })
        .await;
    let cancelled = match written {
        Err(failure) => return Outcome::retry(&failure.code),
        Ok(None) => return Outcome::Done,
        Ok(Some(cancelled)) => cancelled,
    };
    let metrics = &sender.metrics;
    if !busy {
        metrics.webhook_attempt(sent.class.as_str());
    }
    if matches!(status, "succeeded" | "failed") {
        metrics.webhook_delivery(status, 1);
    }
    if sent.class == Class::Blocked {
        metrics.egress_blocked("webhook");
        merezha_telemetry::event!(
            WARN,
            "notification.egress_blocked",
            endpoint_id = endpoint.to_string().as_str(),
            target = "webhook",
            reason = crate::egress::EGRESS_BLOCKED
        );
    }
    if let Some((_, cancelled)) = cancelled {
        disabled(sender, endpoint, "gone", cancelled);
    }
    step.job
}

/// Disable active `endpoint` for `reason` in `tx`, cancel its queued
/// deliveries and write the audit row. With `failing_before`, only while
/// the endpoint has been failing since before it. `None` when the
/// endpoint was not active (or no longer failing); else its tenant and the
/// deliveries cancelled.
pub(crate) async fn disable(
    db: &merezha_postgres::Database,
    tx: &mut merezha_postgres::Tx<'_>,
    endpoint: Uuid,
    reason: &'static str,
    now: jiff::Timestamp,
    failing_before: Option<jiff::Timestamp>,
    actor: &str,
) -> Result<Option<(Option<String>, u64)>, Failure> {
    let Some(row) = db
        .query_opt(
            tx,
            &sql::ENDPOINT_DISABLE,
            &[&endpoint, &reason, &now, &failing_before],
        )
        .await
        .map_err(|e| classify(&e))?
    else {
        return Ok(None);
    };
    let (tenant, revision): (Option<String>, i64) = (row.get(0), row.get(1));
    let cancelled = db
        .execute(tx, &sql::DELIVERIES_CANCEL, &[&endpoint, &now])
        .await
        .map_err(|e| classify(&e))?;
    audit(
        db,
        tx,
        endpoint,
        &tenant,
        "disable",
        Some(reason),
        actor,
        revision,
        now,
    )
    .await?;
    Ok(Some((tenant, cancelled)))
}

/// Count and log a disabled endpoint and its cancelled deliveries.
pub(crate) fn disabled(
    sender: &WebhookSender,
    endpoint: Uuid,
    reason: &'static str,
    cancelled: u64,
) {
    sender.metrics.webhook_endpoint_disabled(reason);
    sender.metrics.webhook_delivery("cancelled", cancelled);
    merezha_telemetry::event!(
        WARN,
        "notification.webhook_endpoint_disabled",
        endpoint_id = endpoint.to_string().as_str(),
        target = "webhook",
        reason
    );
}

/// One audit row: never the secret, never the URL.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn audit(
    db: &merezha_postgres::Database,
    tx: &mut merezha_postgres::Tx<'_>,
    endpoint: Uuid,
    tenant: &Option<String>,
    action: &'static str,
    reason: Option<&'static str>,
    actor: &str,
    revision: i64,
    now: jiff::Timestamp,
) -> Result<(), Failure> {
    db.execute(
        tx,
        &sql::AUDIT_INSERT,
        &[&endpoint, tenant, &action, &reason, &actor, &revision, &now],
    )
    .await
    .map_err(|e| classify(&e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sent(class: Class) -> Sent {
        Sent {
            class,
            status: None,
            snippet: None,
        }
    }

    #[test]
    fn outcomes_follow_the_schedule_until_attempt_ten() {
        let ok = step(&sent(Class::Success), 1, false, 0.5);
        assert_eq!((ok.status, ok.job), ("succeeded", Outcome::Done));
        let gone = step(&sent(Class::Gone), 1, false, 0.5);
        assert_eq!(gone.status, "failed");
        let first = step(&sent(Class::ServerError), 1, false, 0.5);
        assert_eq!(first.status, "retrying");
        assert_eq!(
            first.job,
            Outcome::Retry {
                after: Some(Duration::from_secs(5)),
                code: "http_server_error".into()
            }
        );
        let ninth = step(&sent(Class::Redirect), 9, false, 1.0);
        assert_eq!(ninth.next, Some(super::super::MAX_STEP));
        let blocked = step(&sent(Class::Blocked), 2, false, 0.5);
        assert_eq!((blocked.attempt, blocked.status), ("blocked", "retrying"));
        let tenth = step(&sent(Class::Timeout), 10, false, 0.5);
        assert_eq!(
            (tenth.status, tenth.job),
            ("failed", Outcome::Permanent("retries_exhausted".into()))
        );
        let out_of_jobs = step(&sent(Class::Network), 3, true, 0.5);
        assert_eq!(out_of_jobs.status, "failed");
    }
}
