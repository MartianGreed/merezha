//! The common delivery worker of email and SMS (NT §5.1).
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
//! This slice delivers through the capture drivers; the Brevo drivers of
//! NT-N3 and NT-N4 implement the same traits.
use crate::{
    drivers::{EmailMessage, SendOutcome, SmsMessage},
    faults,
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
use merezha_domain::Failure;
use merezha_platform::ports::{Channel, EmailAddress, JobContext, PhoneNumber};
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
    let prepared = match prepare(core, channel, deadline, id, payload.generation).await {
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
    let send_by = deadline
        .checked_sub(RECORD_MARGIN)
        .unwrap_or(deadline)
        .min(Instant::now() + CHANNEL_TIMEOUT);
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
            (driver.send(&message, send_by).await, driver.provider())
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
            let message = SmsMessage {
                delivery_id: id,
                topic: locked.topic.clone(),
                to,
                content,
                marketing: locked.category == Category::Marketing,
            };
            (driver.send(&message, send_by).await, driver.provider())
        }
        _ => {
            core.metrics.render_failure(channel, &locked.topic);
            return finish(core, channel, id, &locked, "failed", reason::RENDER_FAILED).await;
        }
    };
    let took = started.elapsed();
    core.faults.hit(faults::AFTER_PROVIDER_CALL).await;
    let result = record(core, channel, ctx, id, &locked, &outcome, provider, took).await;
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

/// Tx A.
async fn prepare(
    core: &Core,
    channel: Channel,
    deadline: Instant,
    id: Uuid,
    generation: i32,
) -> Result<Prepared, Failure> {
    let db = core.database.clone();
    let now = core.now();
    let settings = core.settings.clone();
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
                let user: Option<String> = row.get(4);
                let address: Option<String> = row.get(5);
                let topic: String = row.get(9);
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
                            return Ok((
                                Prepared::Finished(Outcome::Done),
                                Some(("unknown", "ambiguous", topic)),
                            ));
                        }
                    }
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
                    })),
                    None,
                ))
            })
        })
        .await?;
    if let Some((status, why, topic)) = logged {
        log_settled(core, channel, id, &topic, status, why, None);
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
) -> Outcome {
    let now = core.now();
    let exhausted = ctx.attempt >= ctx.max_attempts;
    let retry = |code: &str, after: Option<Duration>, attempt_outcome: &'static str| {
        let late = locked.expires_at.is_some_and(|at| {
            let wait =
                jiff::SignedDuration::try_from(after.unwrap_or_default()).unwrap_or_default();
            now.checked_add(wait).is_ok_and(|next| next > at)
        });
        if late {
            Step {
                attempt: attempt_outcome,
                code: Some(code.to_owned()),
                status: "expired",
                reason: reason::EXPIRED.to_owned(),
                after,
                job: Outcome::Done,
            }
        } else if exhausted {
            Step {
                attempt: attempt_outcome,
                code: Some(code.to_owned()),
                status: "failed",
                reason: reason::RETRIES_EXHAUSTED.to_owned(),
                after,
                job: Outcome::Permanent(reason::RETRIES_EXHAUSTED.to_owned()),
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
        },
        // NT1: an ambiguous SMS is never resent.
        SendOutcome::Ambiguous { code } if channel == Channel::Sms => Step {
            attempt: "ambiguous",
            code: Some(code.clone()),
            status: "unknown",
            reason: "ambiguous".into(),
            after: None,
            job: Outcome::Done,
        },
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
    let db = core.database.clone();
    let (attempt, generation) = (locked.attempt, locked.generation);
    let provider_name = provider.as_str();
    let after_ms = step
        .after
        .map(|d| i32::try_from(d.as_millis()).unwrap_or(i32::MAX));
    let status = step.status;
    let reason_text = step.reason.clone();
    let code = step.code.clone();
    let attempt_outcome = step.attempt;
    let run_at = step.after.and_then(|after| {
        jiff::SignedDuration::try_from(after)
            .ok()
            .and_then(|d| now.checked_add(d).ok())
    });
    let deadline = Instant::now() + CHANNEL_TIMEOUT;
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
                if changed > 0 {
                    db.execute(
                        tx,
                        &sql::ATTEMPT_FINISH,
                        &[&id, &attempt, &attempt_outcome, &now, &code, &after_ms],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                }
                Ok(changed)
            })
        })
        .await;
    match written {
        Err(failure) => return Outcome::retry(&failure.code),
        // Another run took the delivery over: it records the outcome.
        Ok(0) => return Outcome::Done,
        Ok(_) => {}
    }
    tracing::Span::current().record("notify.outcome", attempt_outcome);
    core.metrics
        .attempt(channel, provider_name, attempt_outcome, took);
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
        }
    }
}
