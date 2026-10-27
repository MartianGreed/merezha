//! The common delivery worker of email, SMS and push (NT §5.1).
//!
//! 1. Tx A locks the delivery. A settled delivery or another generation is
//!    done; past `expires_at` it is `expired`; suppressions and preferences
//!    are checked again (`skipped`); a last attempt still `started` takes
//!    the channel's ambiguity path (email resends with the same idempotency
//!    key, SMS becomes `unknown` and is never resent, NT1). Otherwise
//!    attempt `n + 1` is committed as `started` and the delivery is
//!    `sending`.
//! 2. The stored data, the job's `secret_data` and the template render the
//!    message on every attempt.
//! 3. The driver sends it before `min(channel timeout, deadline - 2 s)`.
//! 4. Tx B records the outcome.
//!
//! Drivers are the capture drivers, Brevo email (NT-N3) and Brevo SMS
//! (NT-N4), whose call deadline is `brevo.timeout`; an account problem
//! (`provider_account`) is logged at error level and counted, a rate limit
//! at warn. An accepted SMS counts its segments.
//!
//! Tx A also fences the job chain (issue #63): a job whose key names
//! another attempt than the generation's last `provider_account` attempt
//! (a generation's first job names none) was handed over and finishes
//! without a send.
//!
//! Push (NT-N6) runs the same steps: Tx A also reads the delivery's
//! subscription (gone, moved to another user, expired or out of the
//! tenant scope: `skipped` / `no_subscription`; made with another VAPID key
//! than this instance's: retried `vapid_key_mismatch` without an attempt,
//! so an instance holding that key sends it, issue #66, and skipped on the
//! job's last attempt), the
//! request goes through [`crate::push::PushSender`], and Tx B refreshes the
//! subscription, counts a failure, or deletes it on 404 or 410
//! (`subscription_gone`). An ambiguous push is resent: the payload carries
//! the delivery id for client-side deduplication.
use crate::{
    brevo::events,
    drivers::{EmailMessage, SendOutcome, SmsMessage},
    expand, faults,
    handler::Outcome,
    ids,
    keys::Unsubscribe,
    notifier::Core,
    payload::DeliverPayload,
    policy::{self, UserPreferences, reason},
    sql,
    templates::{RenderContext, Rendered},
    topics::Category,
};
use merezha_domain::{Failure, Identity};
use merezha_platform::ports::{
    Channel, DedupeKey, EmailAddress, JobContext, MessageContext, OnDuplicate, OutboxRecord,
    OutboxTarget, PhoneNumber,
};
use merezha_postgres::classify;
use serde_json::{Map, Value};
use std::time::{Duration, Instant};
use tracing::Instrument;
use uuid::Uuid;

/// The provider deadline without a provider setting (capture).
const CHANNEL_TIMEOUT: Duration = Duration::from_secs(10);
/// Kept free before the job deadline for Tx B.
const RECORD_MARGIN: Duration = Duration::from_secs(2);

/// What Tx A found.
struct Locked {
    attempt: i32,
    generation: i32,
    user: Option<String>,
    address: Option<String>,
    name: Option<String>,
    locale: String,
    notification: Uuid,
    topic: String,
    template: String,
    category: Category,
    data: Option<Map<String, Value>>,
    has_secret: bool,
    expires_at: Option<jiff::Timestamp>,
    requested_at: jiff::Timestamp,
    timezone: Option<String>,
    /// When Tx A started this attempt.
    started_at: jiff::Timestamp,
    /// The first attempt of this generation Brevo may have processed (an
    /// `ambiguous` one): the start of its idempotency window (email).
    first_uncertain: Option<jiff::Timestamp>,
    /// The start of the current run of `provider_account` answers.
    account_streak_since: Option<jiff::Timestamp>,
    correlation_id: Option<String>,
    tenant: Option<String>,
    /// The subscription of a push delivery, read in Tx A.
    push: Option<crate::push::PushTarget>,
}

enum Prepared {
    Finished(Outcome),
    Send(Box<Locked>),
}

fn category(text: &str) -> Category {
    match text {
        "security" => Category::Security,
        "marketing" => Category::Marketing,
        _ => Category::Transactional,
    }
}

pub(crate) async fn run(
    core: &Core,
    channel: Channel,
    ctx: &JobContext,
    deadline: Instant,
    payload: DeliverPayload,
) -> Outcome {
    let span = tracing::info_span!(
        target: merezha_telemetry::TARGET,
        "notify delivery",
        otel.name = format!("notify {}", channel.as_str()).as_str(),
        notify.channel = channel.as_str(),
        notify.delivery_id = payload.delivery_id.to_string().as_str(),
        notify.attempt = tracing::field::Empty,
        notify.generation = payload.generation,
        notify.locale = tracing::field::Empty,
        notify.outcome = tracing::field::Empty,
    );
    deliver(core, channel, ctx, deadline, payload)
        .instrument(span)
        .await
}

async fn deliver(
    core: &Core,
    channel: Channel,
    ctx: &JobContext,
    deadline: Instant,
    payload: DeliverPayload,
) -> Outcome {
    let id = payload.delivery_id;
    let job = JobRun {
        generation: payload.generation,
        after_attempt: keyed_attempt(&ctx.idempotency_key).or(payload.after_attempt),
        last: ctx.attempt >= ctx.max_attempts,
    };
    let prepared = match prepare(core, channel, deadline, id, job).await {
        Ok(prepared) => prepared,
        Err(failure) => return Outcome::retry(&failure.code),
    };
    let locked = match prepared {
        Prepared::Finished(outcome) => return outcome,
        Prepared::Send(locked) => locked,
    };
    let span = tracing::Span::current();
    span.record("notify.attempt", locked.attempt);
    span.record("notify.locale", locked.locale.as_str());
    core.faults.hit(faults::AFTER_ATTEMPT_COMMITTED).await;
    let Some(data) = &locked.data else {
        return finish(core, channel, id, &locked, "failed", reason::DATA_PURGED).await;
    };
    let empty = Map::new();
    let secret = match (&payload.secret_data, locked.has_secret) {
        (Some(secret), _) => secret,
        (None, false) => &empty,
        (None, true) => {
            return finish(core, channel, id, &locked, "failed", reason::SECRET_PURGED).await;
        }
    };
    let unsubscribe = unsubscribe_link(core, channel, &locked);
    let context = RenderContext {
        topic: &locked.topic,
        category: locked.category,
        notification_id: locked.notification,
        data,
        secret,
        recipient_name: locked.name.as_deref(),
        timezone: locked.timezone.as_deref(),
        unsubscribe: unsubscribe.as_deref(),
    };
    let rendered = core
        .registry
        .render(&locked.template, channel, &locked.locale, &context);
    // `min(channel timeout, deadline - 2 s)` (NT §5.1 step 3).
    let send_by = |timeout: Option<Duration>| {
        deadline
            .checked_sub(RECORD_MARGIN)
            .unwrap_or(deadline)
            .min(Instant::now() + timeout.unwrap_or(CHANNEL_TIMEOUT))
    };
    let started = Instant::now();
    let (outcome, provider) = match (channel, rendered) {
        (Channel::Email, Ok(Rendered::Email(content))) => {
            let (Some(driver), Some(address)) = (&core.email, &locked.address) else {
                return finish(
                    core,
                    channel,
                    id,
                    &locked,
                    "failed",
                    reason::CHANNEL_DISABLED,
                )
                .await;
            };
            let Ok(to) = EmailAddress::new(address.clone()) else {
                return finish(
                    core,
                    channel,
                    id,
                    &locked,
                    "failed",
                    reason::INVALID_ADDRESS,
                )
                .await;
            };
            let message = EmailMessage {
                delivery_id: id,
                idempotency_key: ids::idempotency_key(id, locked.generation),
                topic: locked.topic.clone(),
                to,
                to_name: locked.name.clone(),
                content,
                unsubscribe,
            };
            (
                driver.send(&message, send_by(driver.timeout())).await,
                driver.provider(),
            )
        }
        (Channel::Push, Ok(Rendered::Push(content))) => {
            let (Some(sender), Some(target)) = (&core.push, &locked.push) else {
                return finish(
                    core,
                    channel,
                    id,
                    &locked,
                    "failed",
                    reason::CHANNEL_DISABLED,
                )
                .await;
            };
            let message = crate::push::PushMessage {
                delivery: id,
                notification: locked.notification,
                topic: &locked.topic,
                content: &content,
                target,
            };
            (
                sender
                    .send(
                        &message,
                        send_by(Some(crate::push::PUSH_TIMEOUT)),
                        ctx.attempt,
                    )
                    .await,
                crate::drivers::Provider::Webpush,
            )
        }
        (Channel::Sms, Ok(Rendered::Sms(content))) => {
            let (Some(driver), Some(address)) = (&core.sms, &locked.address) else {
                return finish(
                    core,
                    channel,
                    id,
                    &locked,
                    "failed",
                    reason::CHANNEL_DISABLED,
                )
                .await;
            };
            let Ok(to) = PhoneNumber::new(address.clone()) else {
                return finish(
                    core,
                    channel,
                    id,
                    &locked,
                    "failed",
                    reason::INVALID_ADDRESS,
                )
                .await;
            };
            let (encoding, segments) = (content.encoding, content.segments);
            let message = SmsMessage {
                delivery_id: id,
                topic: locked.topic.clone(),
                to,
                content,
                marketing: locked.category == Category::Marketing,
            };
            let outcome = driver.send(&message, send_by(driver.timeout())).await;
            if matches!(outcome, SendOutcome::Accepted { .. }) {
                core.metrics.sms_segments(encoding.as_str(), segments);
            }
            (outcome, driver.provider())
        }
        _ => {
            core.metrics.render_failure(channel, &locked.topic);
            return finish(core, channel, id, &locked, "failed", reason::RENDER_FAILED).await;
        }
    };
    let took = started.elapsed();
    core.faults.hit(faults::AFTER_PROVIDER_CALL).await;
    let result = record(
        core,
        channel,
        ctx,
        id,
        &locked,
        &outcome,
        provider,
        took,
        payload.secret_data.as_ref(),
    )
    .await;
    core.faults.hit(faults::AFTER_TX_B).await;
    result
}

/// The unsubscribe link of a non-security email (NT §6), when the block
/// has a public base URL to put in it.
fn unsubscribe_link(core: &Core, channel: Channel, locked: &Locked) -> Option<String> {
    if channel != Channel::Email || locked.category == Category::Security {
        return None;
    }
    let base = core.settings.public_base_url.as_ref()?;
    let claims = Unsubscribe {
        u: locked.user.clone(),
        a: match locked.user {
            Some(_) => None,
            None => locked.address.as_ref().map(|a| a.to_lowercase()),
        },
        t: locked.topic.clone(),
        c: "email".into(),
        iat: core.now().as_second(),
    };
    let token = core.keys.unsubscribe_token(&claims);
    Some(format!("{base}/v1/notifications/unsubscribe?token={token}"))
}

/// What Tx A needs of the job: the generation it sends, the attempt its
/// chain continues after (issue #63), and whether it is the job's last
/// attempt.
#[derive(Clone, Copy, Debug)]
struct JobRun {
    generation: i32,
    after_attempt: Option<i32>,
    last: bool,
}

/// The attempt a continuation job names in its key
/// (`notifications:deliver:{id}:g{generation}:a{attempt}`): the fence of
/// its chain (issue #63). A generation's first job has no `:a` part. The
/// key carries it, not the payload, so a continuation queued by any
/// version is fenced and no older binary refuses its payload.
fn keyed_attempt(key: &str) -> Option<i32> {
    let (_, attempt) = key.rsplit_once(":a")?;
    attempt.parse().ok().filter(|attempt| *attempt > 0)
}

/// The transient code of a push job that leased a delivery whose
/// subscription was made with another VAPID key than this instance's
/// (issue #66): during a rolling key rotation an instance holding that key
/// sends it. Tx A starts no attempt for it; on the job's last attempt the
/// delivery is `skipped` / `no_subscription`.
pub const VAPID_KEY_MISMATCH: &str = "vapid_key_mismatch";

/// Tx A.
async fn prepare(
    core: &Core,
    channel: Channel,
    deadline: Instant,
    id: Uuid,
    job: JobRun,
) -> Result<Prepared, Failure> {
    let generation = job.generation;
    let db = core.database.clone();
    let now = core.now();
    let settings = core.settings.clone();
    let push_key = core
        .push
        .as_ref()
        .filter(|_| channel == Channel::Push)
        .map(|sender| sender.vapid().key_id().to_owned());
    let (prepared, logged) = db
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
                // Another job carries the generation on: this one ran again
                // after handing the delivery over (issue #63).
                let chain: Option<i32> = db
                    .query_one(tx, &sql::ATTEMPT_CHAIN, &[&id, &generation])
                    .await
                    .map_err(|e| classify(&e))?
                    .get(0);
                if chain != job.after_attempt {
                    return Ok((Prepared::Finished(Outcome::Done), None));
                }
                let user: Option<String> = row.get(4);
                let address: Option<String> = row.get(5);
                let topic: String = row.get(9);
                // A deleted organization's deliveries are cancelled before
                // the purge reaches them (issue #63).
                if let Some(tenant) = row.get::<_, Option<String>>(18)
                    && crate::tenants::guard(&db, tx, &tenant).await?
                {
                    let why = reason::TENANT_DELETED;
                    db.execute(
                        tx,
                        &sql::DELIVERY_SET,
                        &[
                            &id,
                            &generation,
                            &"cancelled",
                            &why,
                            &now,
                            &None::<jiff::Timestamp>,
                            &None::<i32>,
                        ],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                    events::apply_pending(&db, tx, id, now).await?;
                    return Ok((
                        Prepared::Finished(Outcome::Done),
                        Some(("cancelled", why, topic)),
                    ));
                }
                let category_text: String = row.get(11);
                let data: Option<Value> = row.get(12);
                let expires_at: Option<jiff::Timestamp> = row.get(14);
                let set = |status: &'static str, why: &'static str| (status, why);
                let mut decided = None;
                if expires_at.is_some_and(|at| at <= now) {
                    decided = Some(set("expired", reason::EXPIRED));
                }
                let topic_settings = settings.topics.get(&topic);
                if decided.is_none()
                    && let (Some(user), Some(topic_settings)) = (&user, topic_settings)
                    && topic_settings.category != Category::Security
                {
                    let rows = db
                        .query(
                            tx,
                            &sql::PREFERENCES_FOR_USER,
                            &[user, &channel.as_str(), &topic],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                    let mut found = UserPreferences::default();
                    for row in rows {
                        let (t, enabled): (String, bool) = (row.get(0), row.get(1));
                        if t == "*" {
                            found.any = Some(enabled);
                        } else {
                            found.topic = Some(enabled);
                        }
                    }
                    if let Err(why) = policy::user_allows(topic_settings, channel, found) {
                        decided = Some(set("skipped", why));
                    }
                }
                if decided.is_none()
                    && let Some(address) = &address
                {
                    let rows = db
                        .query(
                            tx,
                            &sql::SUPPRESSIONS_FOR_ADDRESS,
                            &[&channel.as_str(), &address.to_lowercase(), &topic],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                    let blocked = rows.iter().any(|row| {
                        let (t, r): (Option<String>, String) = (row.get(0), row.get(1));
                        policy::suppression_blocks(
                            &r,
                            t.as_deref(),
                            &topic,
                            category(&category_text),
                        )
                    });
                    if blocked {
                        decided = Some(set("skipped", reason::SUPPRESSED));
                    }
                }
                let mut push = None;
                if decided.is_none()
                    && let Some(key_id) = &push_key
                {
                    match crate::push::load_target(&db, tx, id, now).await? {
                        Some(target) if target.key_id == *key_id => push = Some(target),
                        // Made with another VAPID key: during a rolling
                        // rotation an instance holding it sends the push
                        // (issue #66); no attempt is started.
                        Some(_) if !job.last => {
                            // The chain is alive: `reclaim_stuck` must not
                            // take it for a lost one.
                            db.execute(tx, &sql::DELIVERY_TOUCH, &[&id, &now])
                                .await
                                .map_err(|e| classify(&e))?;
                            return Ok((
                                Prepared::Finished(Outcome::retry(VAPID_KEY_MISMATCH)),
                                Some(("retrying", VAPID_KEY_MISMATCH, topic)),
                            ));
                        }
                        _ => decided = Some(set("skipped", reason::NO_SUBSCRIPTION)),
                    }
                }
                if let Some((status, why)) = decided {
                    db.execute(
                        tx,
                        &sql::DELIVERY_SET,
                        &[
                            &id,
                            &generation,
                            &status,
                            &why,
                            &now,
                            &None::<jiff::Timestamp>,
                            &None::<i32>,
                        ],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                    events::apply_pending(&db, tx, id, now).await?;
                    return Ok((
                        Prepared::Finished(Outcome::Done),
                        Some((status, why, topic)),
                    ));
                }
                let attempt_count: i32 = row.get(2);
                let last = db
                    .query_opt(tx, &sql::LAST_ATTEMPT, &[&id])
                    .await
                    .map_err(|e| classify(&e))?;
                let mut next = attempt_count + 1;
                if let Some(last) = last {
                    let (number, outcome): (i32, String) = (last.get(0), last.get(1));
                    next = next.max(number + 1);
                    if outcome == "started" {
                        // The previous outcome is unknown (invariant 4).
                        db.execute(
                            tx,
                            &sql::ATTEMPT_FINISH,
                            &[
                                &id,
                                &number,
                                &"ambiguous",
                                &now,
                                &"lost_outcome",
                                &None::<i32>,
                            ],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                        if channel == Channel::Sms {
                            // NT1: an ambiguous SMS is never resent.
                            db.execute(
                                tx,
                                &sql::DELIVERY_SET,
                                &[
                                    &id,
                                    &generation,
                                    &"unknown",
                                    &"ambiguous",
                                    &now,
                                    &None::<jiff::Timestamp>,
                                    &None::<i32>,
                                ],
                            )
                            .await
                            .map_err(|e| classify(&e))?;
                            events::apply_pending(&db, tx, id, now).await?;
                            return Ok((
                                Prepared::Finished(Outcome::Done),
                                Some(("unknown", "ambiguous", topic)),
                            ));
                        }
                    }
                }
                let history = db
                    .query_one(tx, &sql::ATTEMPT_HISTORY, &[&id, &generation])
                    .await
                    .map_err(|e| classify(&e))?;
                let first_uncertain: Option<jiff::Timestamp> = history.get(0);
                let account_streak_since: Option<jiff::Timestamp> = history.get(1);
                // Past the margin of Brevo's idempotency window a resend
                // could deliver twice: the outcome stays unknown (NT §5.1).
                if channel == Channel::Email
                    && first_uncertain.is_some_and(|first| beyond_window(first, now))
                {
                    db.execute(
                        tx,
                        &sql::DELIVERY_SET,
                        &[
                            &id,
                            &generation,
                            &"unknown",
                            &reason::IDEMPOTENCY_WINDOW,
                            &now,
                            &None::<jiff::Timestamp>,
                            &None::<i32>,
                        ],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                    events::apply_pending(&db, tx, id, now).await?;
                    return Ok((
                        Prepared::Finished(Outcome::Done),
                        Some(("unknown", reason::IDEMPOTENCY_WINDOW, topic)),
                    ));
                }
                db.execute(tx, &sql::ATTEMPT_START, &[&id, &next, &generation, &now])
                    .await
                    .map_err(|e| classify(&e))?;
                db.execute(tx, &sql::DELIVERY_SENDING, &[&id, &next, &now])
                    .await
                    .map_err(|e| classify(&e))?;
                let user_for_zone = user.clone();
                let timezone = match &user_for_zone {
                    Some(user) => db
                        .query_opt(tx, &sql::TIMEZONE_FOR_USER, &[user])
                        .await
                        .map_err(|e| classify(&e))?
                        .and_then(|r| r.get::<_, Option<String>>(0)),
                    None => None,
                };
                Ok((
                    Prepared::Send(Box::new(Locked {
                        attempt: next,
                        generation,
                        user,
                        address,
                        name: row.get(6),
                        locale: row.get(7),
                        notification: row.get(8),
                        topic,
                        template: row.get(10),
                        category: category(&category_text),
                        data: match data {
                            Some(Value::Object(map)) => Some(map),
                            _ => None,
                        },
                        has_secret: row.get(13),
                        expires_at,
                        requested_at: row.get(15),
                        timezone,
                        started_at: now,
                        first_uncertain,
                        account_streak_since,
                        correlation_id: row.get(17),
                        tenant: row.get(18),
                        push,
                    })),
                    None,
                ))
            })
        })
        .await?;
    match logged {
        Some(("retrying", why, topic)) => merezha_telemetry::event!(
            WARN,
            "notification.retry_scheduled",
            channel = channel.as_str(),
            topic = topic.as_str(),
            delivery_id = id.to_string().as_str(),
            code = why
        ),
        Some((status, why, topic)) => log_settled(core, channel, id, &topic, status, why, None),
        None => {}
    }
    Ok(prepared)
}

fn log_settled(
    core: &Core,
    channel: Channel,
    id: Uuid,
    topic: &str,
    status: &'static str,
    why: &str,
    attempt: Option<i32>,
) {
    let delivery = id.to_string();
    match status {
        "skipped" => merezha_telemetry::event!(
            DEBUG,
            "notification.skipped",
            channel = channel.as_str(),
            topic,
            delivery_id = delivery.as_str(),
            attempt,
            reason = why
        ),
        "expired" => merezha_telemetry::event!(
            WARN,
            "notification.expired",
            channel = channel.as_str(),
            topic,
            delivery_id = delivery.as_str(),
            attempt,
            reason = why
        ),
        "unknown" => merezha_telemetry::event!(
            WARN,
            "notification.ambiguous",
            channel = channel.as_str(),
            topic,
            delivery_id = delivery.as_str(),
            attempt,
            reason = why
        ),
        "failed" => merezha_telemetry::event!(
            WARN,
            "notification.failed",
            channel = channel.as_str(),
            topic,
            delivery_id = delivery.as_str(),
            code = why
        ),
        _ => {}
    }
    core.metrics.delivery(channel, topic, status, why);
}

/// Settle a delivery whose attempt could not reach the provider; the
/// attempt is recorded `permanent` with the reason.
async fn finish(
    core: &Core,
    channel: Channel,
    id: Uuid,
    locked: &Locked,
    status: &'static str,
    why: &'static str,
) -> Outcome {
    let db = core.database.clone();
    let now = core.now();
    let (attempt, generation) = (locked.attempt, locked.generation);
    let deadline = Instant::now() + CHANNEL_TIMEOUT;
    let written = db
        .clone()
        .transaction(deadline, move |tx| {
            Box::pin(async move {
                let changed = db
                    .execute(
                        tx,
                        &sql::DELIVERY_SET,
                        &[
                            &id,
                            &generation,
                            &status,
                            &why,
                            &now,
                            &None::<jiff::Timestamp>,
                            &Some(attempt),
                        ],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                if changed > 0 {
                    db.execute(
                        tx,
                        &sql::ATTEMPT_FINISH,
                        &[&id, &attempt, &"permanent", &now, &why, &None::<i32>],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                    events::apply_pending(&db, tx, id, now).await?;
                }
                Ok(changed)
            })
        })
        .await;
    match written {
        // Another run took the delivery over.
        Ok(0) => Outcome::Done,
        Ok(_) => {
            tracing::Span::current().record("notify.outcome", "permanent");
            log_settled(core, channel, id, &locked.topic, status, why, Some(attempt));
            Outcome::Permanent(why.to_owned())
        }
        Err(failure) => Outcome::retry(&failure.code),
    }
}

/// Whether a resend at `at` falls past the margin of the idempotency
/// window opened by an attempt Brevo may have processed at `first`.
fn beyond_window(first: jiff::Timestamp, at: jiff::Timestamp) -> bool {
    jiff::SignedDuration::try_from(IDEMPOTENCY_MARGIN)
        .ok()
        .and_then(|margin| first.checked_add(margin).ok())
        .is_some_and(|limit| at > limit)
}
/// A resend after an ambiguous email attempt must start within this much of
/// it: Brevo holds an `idempotencyKey` 30 minutes, minus a 5-minute margin.
pub const IDEMPOTENCY_MARGIN: Duration = Duration::from_secs(25 * 60);
/// How long a run of `provider_account` answers may last when the
/// notification has no `expires_at`.
pub const ACCOUNT_STREAK_LIMIT: Duration = Duration::from_secs(24 * 3600);

fn plus(at: jiff::Timestamp, by: Duration) -> jiff::Timestamp {
    jiff::SignedDuration::try_from(by)
        .ok()
        .and_then(|d| at.checked_add(d).ok())
        .unwrap_or(jiff::Timestamp::MAX)
}

/// Tx B: the provider's outcome.
#[allow(clippy::too_many_arguments)]
async fn record(
    core: &Core,
    channel: Channel,
    ctx: &JobContext,
    id: Uuid,
    locked: &Locked,
    outcome: &SendOutcome,
    provider: crate::drivers::Provider,
    took: Duration,
    secret: Option<&Map<String, Value>>,
) -> Outcome {
    let now = core.now();
    let exhausted = ctx.attempt >= ctx.max_attempts;
    // The idempotency window opened by the first attempt Brevo may have
    // processed: an earlier ambiguous one, or this one.
    let first_uncertain = locked
        .first_uncertain
        .or_else(|| matches!(outcome, SendOutcome::Ambiguous { .. }).then_some(locked.started_at));
    let retry = |code: &str, after: Option<Duration>, attempt_outcome: &'static str| {
        let next = plus(now, after.unwrap_or_default());
        let late = locked.expires_at.is_some_and(|at| next > at);
        if late {
            Step::settled(attempt_outcome, code, "expired", reason::EXPIRED, after)
        } else if channel == Channel::Email
            && first_uncertain.is_some_and(|first| beyond_window(first, next))
        {
            Step::settled(
                attempt_outcome,
                code,
                "unknown",
                reason::IDEMPOTENCY_WINDOW,
                after,
            )
        } else if exhausted {
            Step {
                attempt: attempt_outcome,
                code: Some(code.to_owned()),
                status: "failed",
                reason: reason::RETRIES_EXHAUSTED.to_owned(),
                after,
                job: Outcome::Permanent(reason::RETRIES_EXHAUSTED.to_owned()),
                continue_at: None,
            }
        } else {
            Step {
                attempt: attempt_outcome,
                code: Some(code.to_owned()),
                status: "retrying",
                reason: code.to_owned(),
                after,
                job: Outcome::Retry {
                    after,
                    code: code.to_owned(),
                },
                continue_at: None,
            }
        }
    };
    let step = match outcome {
        SendOutcome::Accepted { .. } => Step::accepted("accepted"),
        SendOutcome::Duplicate { .. } => Step::accepted("duplicate"),
        SendOutcome::Permanent { code } => Step {
            attempt: "permanent",
            code: Some(code.clone()),
            status: "failed",
            reason: code.clone(),
            after: None,
            job: Outcome::Permanent(code.clone()),
            continue_at: None,
        },
        // NT1: an ambiguous SMS is never resent.
        SendOutcome::Ambiguous { code } if channel == Channel::Sms => Step {
            attempt: "ambiguous",
            code: Some(code.clone()),
            status: "unknown",
            reason: "ambiguous".into(),
            after: None,
            job: Outcome::Done,
            continue_at: None,
        },
        // An account problem consumes no attempt: the delivery waits for an
        // operator until its deadline (NT §5.2).
        SendOutcome::Transient { code, retry_after } if code == crate::brevo::PROVIDER_ACCOUNT => {
            let after = retry_after
                .unwrap_or(crate::brevo::ACCOUNT_DELAY)
                .max(crate::brevo::ACCOUNT_DELAY)
                .min(
                    core.settings
                        .provider_retry_cap
                        .max(crate::brevo::ACCOUNT_DELAY),
                );
            let next = plus(now, after);
            let since = locked.account_streak_since.unwrap_or(locked.started_at);
            let deadline = locked
                .expires_at
                .unwrap_or_else(|| plus(since, ACCOUNT_STREAK_LIMIT));
            if next > deadline {
                Step {
                    attempt: "transient",
                    code: Some(code.clone()),
                    status: "failed",
                    reason: reason::PROVIDER_ACCOUNT_UNRESOLVED.to_owned(),
                    after: None,
                    job: Outcome::Permanent(reason::PROVIDER_ACCOUNT_UNRESOLVED.to_owned()),
                    continue_at: None,
                }
            } else if channel == Channel::Email
                && first_uncertain.is_some_and(|first| beyond_window(first, next))
            {
                Step::settled(
                    "transient",
                    code,
                    "unknown",
                    reason::IDEMPOTENCY_WINDOW,
                    Some(after),
                )
            } else {
                Step {
                    attempt: "transient",
                    code: Some(code.clone()),
                    status: "retrying",
                    reason: code.clone(),
                    after: Some(after),
                    job: Outcome::Done,
                    continue_at: Some(next),
                }
            }
        }
        // Email resends with the same idempotency key.
        SendOutcome::Ambiguous { code } => retry(code, None, "ambiguous"),
        SendOutcome::Transient { code, retry_after } => retry(code, *retry_after, "transient"),
        SendOutcome::RateLimited { retry_after } => {
            retry("rate_limited", *retry_after, "rate_limited")
        }
    };
    let message_id = match outcome {
        SendOutcome::Accepted { message_id, .. } => message_id.clone(),
        _ => None,
    };
    // The next attempt of an account problem is a new job of the same
    // generation, so it starts with a fresh attempt budget.
    let continuation = match (step.continue_at, expand::delivery_queue(channel)) {
        (Some(at), Some((queue, kind))) => {
            let correlation_id = locked
                .correlation_id
                .as_deref()
                .and_then(|c| Identity::new(c).ok())
                .map_or_else(
                    || Identity::new(locked.notification.simple().to_string()),
                    Ok,
                );
            let record = (|| {
                Ok::<_, Failure>(OutboxRecord {
                    dedupe_key: DedupeKey::new(format!(
                        "deliver:{id}:g{}:a{}",
                        locked.generation, locked.attempt
                    ))?,
                    target: OutboxTarget::Job {
                        queue: Identity::new(queue)?,
                        kind: Identity::new(kind)?,
                    },
                    payload: DeliverPayload {
                        delivery_id: id,
                        generation: locked.generation,
                        // The fence is the attempt in the record's key
                        // (`…:a{attempt}`), never the payload: a payload
                        // field would be refused by older binaries
                        // during a rollout (issue #63).
                        after_attempt: None,
                        secret_data: secret.cloned(),
                    }
                    .to_value(),
                    contains_secret: secret.is_some(),
                    run_at: Some(at),
                    priority: 0,
                    context: MessageContext {
                        correlation_id: correlation_id?,
                        causation_id: None,
                        tenant: locked.tenant.as_deref().and_then(|t| Identity::new(t).ok()),
                    },
                })
            })();
            match record {
                Ok(record) => Some(record),
                Err(failure) => return Outcome::retry(&failure.code),
            }
        }
        _ => None,
    };
    let db = core.database.clone();
    let outbox = core.outbox.clone();
    let (attempt, generation) = (locked.attempt, locked.generation);
    let provider_name = provider.as_str();
    let after_ms = step
        .after
        .map(|d| i32::try_from(d.as_millis()).unwrap_or(i32::MAX));
    let status = step.status;
    let reason_text = step.reason.clone();
    let code = step.code.clone();
    let attempt_outcome = step.attempt;
    let run_at = step.after.map(|after| plus(now, after));
    let deadline = Instant::now() + CHANNEL_TIMEOUT;
    let subscription = locked.push.as_ref().map(|t| t.subscription);
    let sent = outcome.clone();
    let written = db
        .clone()
        .transaction(deadline, move |tx| {
            Box::pin(async move {
                let changed = if status == "accepted" {
                    db.execute(
                        tx,
                        &sql::DELIVERY_ACCEPT,
                        &[
                            &id,
                            &generation,
                            &provider_name,
                            &message_id,
                            &now,
                            &attempt,
                        ],
                    )
                    .await
                    .map_err(|e| classify(&e))?
                } else {
                    let why: Option<&str> = (!reason_text.is_empty()).then_some(&reason_text);
                    db.execute(
                        tx,
                        &sql::DELIVERY_SET,
                        &[
                            &id,
                            &generation,
                            &status,
                            &why,
                            &now,
                            &run_at,
                            &Some(attempt),
                        ],
                    )
                    .await
                    .map_err(|e| classify(&e))?
                };
                let mut gone = false;
                if changed > 0 {
                    db.execute(
                        tx,
                        &sql::ATTEMPT_FINISH,
                        &[&id, &attempt, &attempt_outcome, &now, &code, &after_ms],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                    if message_id.is_some() {
                        db.execute(tx, &sql::ATTEMPT_MESSAGE_ID, &[&id, &attempt, &message_id])
                            .await
                            .map_err(|e| classify(&e))?;
                    }
                    if let Some(subscription) = subscription {
                        gone =
                            crate::push::after_attempt(&db, tx, subscription, &sent, now).await?;
                    }
                    if let Some(record) = continuation {
                        outbox
                            .append(tx, &[record], OnDuplicate::Ignore)
                            .await
                            .map_err(|e| Failure::new(&e.code, e.classification))?;
                    }
                    // Events that arrived while the worker owned the
                    // delivery apply now that its outcome is recorded.
                    if !matches!(status, "queued" | "sending" | "retrying") {
                        events::apply_pending(&db, tx, id, now).await?;
                    }
                }
                Ok((changed, gone))
            })
        })
        .await;
    match written {
        Err(failure) => return Outcome::retry(&failure.code),
        // Another run took the delivery over: it records the outcome.
        Ok((0, _)) => return Outcome::Done,
        Ok((_, gone)) => {
            if gone {
                crate::push::removed(&core.metrics, "gone", 1);
            }
        }
    }
    tracing::Span::current().record("notify.outcome", attempt_outcome);
    core.metrics
        .attempt(channel, provider_name, attempt_outcome, took);
    match outcome {
        // Mail stays queued while an operator fixes the key or the
        // credits; one error per run of refusals, every refusal counted.
        SendOutcome::Transient { code, .. } if code == crate::brevo::PROVIDER_ACCOUNT => {
            if locked.account_streak_since.is_none() {
                merezha_telemetry::event!(
                    ERROR,
                    "notification.provider_account_error",
                    provider = provider_name,
                    channel = channel.as_str(),
                    code = code.as_str()
                );
            }
            core.metrics.provider_account_error(provider_name, channel);
        }
        SendOutcome::RateLimited { .. } => merezha_telemetry::event!(
            WARN,
            "notification.provider_rate_limited",
            provider = provider_name,
            channel = channel.as_str(),
            code = "rate_limited"
        ),
        _ => {}
    }
    let delivery = id.to_string();
    match status {
        "accepted" => {
            merezha_telemetry::event!(
                DEBUG,
                "notification.accepted",
                channel = channel.as_str(),
                topic = locked.topic.as_str(),
                delivery_id = delivery.as_str(),
                attempt
            );
            core.metrics
                .delivery(channel, &locked.topic, "accepted", "");
            let since = jiff::Timestamp::now().duration_since(locked.requested_at);
            core.metrics
                .latency(channel, Duration::try_from(since).unwrap_or_default());
        }
        "retrying" => merezha_telemetry::event!(
            WARN,
            "notification.retry_scheduled",
            channel = channel.as_str(),
            topic = locked.topic.as_str(),
            delivery_id = delivery.as_str(),
            attempt,
            code = step.reason.as_str()
        ),
        settled => log_settled(
            core,
            channel,
            id,
            &locked.topic,
            settled,
            &step.reason,
            Some(attempt),
        ),
    }
    step.job
}

/// One recorded outcome.
struct Step {
    attempt: &'static str,
    code: Option<String>,
    status: &'static str,
    reason: String,
    after: Option<Duration>,
    job: Outcome,
    /// The next attempt is a new job of the same generation at this time.
    continue_at: Option<jiff::Timestamp>,
}
impl Step {
    fn accepted(attempt: &'static str) -> Self {
        Self {
            attempt,
            code: None,
            status: "accepted",
            reason: String::new(),
            after: None,
            job: Outcome::Done,
            continue_at: None,
        }
    }
    /// Settled without another attempt (`expired`, `unknown`).
    fn settled(
        attempt: &'static str,
        code: &str,
        status: &'static str,
        why: &str,
        after: Option<Duration>,
    ) -> Self {
        Self {
            attempt,
            code: Some(code.to_owned()),
            status,
            reason: why.to_owned(),
            after,
            job: Outcome::Done,
            continue_at: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::keyed_attempt;

    #[test]
    fn a_continuation_key_names_the_attempt_it_continues_after() {
        let id = "0c7d1a2e-3b4f-5a6b-8c9d-aaaaaaaaaaaa";
        assert_eq!(
            keyed_attempt(&format!("notifications:deliver:{id}:g2:a7")),
            Some(7)
        );
        assert_eq!(
            keyed_attempt(&format!("notifications:deliver:{id}:g0")),
            None
        );
        assert_eq!(keyed_attempt(""), None);
        assert_eq!(keyed_attempt("notifications:deliver:x:g0:a0"), None);
        assert_eq!(keyed_attempt("notifications:deliver:x:g0:ab"), None);
    }
}
