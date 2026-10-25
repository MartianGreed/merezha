//! `POST /v1/notifications/providers/brevo/email` (NT §5.2 "Status
//! webhooks"): Brevo's transactional email events.
//!
//! Brevo does not sign its webhooks. The route is credential-free at the
//! edge (`Module::credential_free_routes`), so Brevo's bearer is never read
//! as a session; the handler checks the optional source CIDRs against the
//! client address the edge resolved through `http.trusted_proxies`, then
//! compares `Authorization: Bearer` with `brevo.webhook_token` in constant
//! time. A failed check answers 401 and stores nothing.
//!
//! An authenticated request, an object or an array of at most
//! [`MAX_EVENTS`] events in at most [`MAX_BODY`] bytes, is applied in one
//! transaction: each event is normalized, correlated by `X-Mailin-custom`
//! (`mzd:<delivery_id>`) and then by message id, deduplicated on
//! `sha256(provider|channel|message id|event|ts_event)` and applied once:
//!
//! | Event | Effect |
//! | --- | --- |
//! | `delivered` | `accepted` or `unknown` to `delivered` |
//! | `hard_bounce`, `invalid_email` | `bounced`, plus a `hard_bounce` or `invalid` suppression |
//! | `spam` | `complained`, plus a `spam_complaint` suppression |
//! | `blocked`, `error` | `failed` |
//! | `unsubscribed` | an `unsubscribed` suppression for the delivery's topic |
//! | `request`, `soft_bounce`, `deferred` | recorded only |
//!
//! Opens and clicks are dropped unstored (NT3). Uncorrelated events are
//! stored with `delivery_id` NULL and still answered 200, so Brevo stops
//! retrying; their suppressions use the event's own address. No log line
//! or span carries an address, subject, reason text or message id.
use crate::{http::Ctx, notifier::Core, sql};
use axum::{
    Extension,
    body::Body,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use merezha_domain::Failure;
use merezha_platform::{ErrorReply, edge::ClientInfo, ports::EmailAddress};
use merezha_postgres::classify;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};
use subtle::ConstantTimeEq;
use uuid::Uuid;

/// The route, relative to `/v1/notifications`.
pub const PATH: &str = "/providers/brevo/email";
/// Most bytes of one callback body (NT §5.2).
pub const MAX_BODY: usize = 1024 * 1024;
/// Most events in one callback request, so its transaction stays bounded.
pub const MAX_EVENTS: usize = 2000;
/// The `X-Mailin-custom` value of a delivery is this prefix and its id.
pub const CUSTOM_PREFIX: &str = "mzd:";
/// Most characters of Brevo's reason text kept in `provider_events.reason`
/// (never logged).
const MAX_REASON: usize = 200;
const DEADLINE: Duration = Duration::from_secs(10);
const PROVIDER: &str = "brevo";
const CHANNEL: &str = "email";

/// A normalized Brevo transactional email event (NT §5.2 step 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmailEvent {
    Request,
    Delivered,
    Deferred,
    SoftBounce,
    HardBounce,
    InvalidEmail,
    Blocked,
    Spam,
    Unsubscribed,
    Error,
}
impl EmailEvent {
    /// The stored name (`provider_events.event`, `notify.event`).
    pub fn as_str(self) -> &'static str {
        match self {
            EmailEvent::Request => "request",
            EmailEvent::Delivered => "delivered",
            EmailEvent::Deferred => "deferred",
            EmailEvent::SoftBounce => "soft_bounce",
            EmailEvent::HardBounce => "hard_bounce",
            EmailEvent::InvalidEmail => "invalid_email",
            EmailEvent::Blocked => "blocked",
            EmailEvent::Spam => "spam",
            EmailEvent::Unsubscribed => "unsubscribed",
            EmailEvent::Error => "error",
        }
    }
    /// The delivery status this event moves to and the statuses it may
    /// move from; `None` records the event only.
    ///
    /// A bounce or a spam complaint can follow `delivered` (an asynchronous
    /// bounce, a complaint after reading), so `delivered` is left for them;
    /// a delivery still `queued`, `sending` or `retrying` is never moved,
    /// its worker records the outcome.
    pub fn transition(
        self,
    ) -> Option<(&'static str, Option<&'static str>, &'static [&'static str])> {
        match self {
            EmailEvent::Delivered => Some(("delivered", None, &["accepted", "unknown"])),
            EmailEvent::HardBounce => Some((
                "bounced",
                Some("hard_bounce"),
                &["accepted", "unknown", "delivered"],
            )),
            EmailEvent::InvalidEmail => Some((
                "bounced",
                Some("invalid_email"),
                &["accepted", "unknown", "delivered"],
            )),
            EmailEvent::Spam => Some(("complained", Some("spam"), &["accepted", "delivered"])),
            EmailEvent::Blocked => Some(("failed", Some("blocked"), &["accepted", "unknown"])),
            EmailEvent::Error => Some(("failed", Some("provider_error"), &["accepted", "unknown"])),
            _ => None,
        }
    }
    /// The suppression reason this event writes, and whether it is scoped
    /// to the delivery's topic.
    pub fn suppression(self) -> Option<(&'static str, bool)> {
        match self {
            EmailEvent::HardBounce => Some(("hard_bounce", false)),
            EmailEvent::InvalidEmail => Some(("invalid", false)),
            EmailEvent::Spam => Some(("spam_complaint", false)),
            EmailEvent::Unsubscribed => Some(("unsubscribed", true)),
            _ => None,
        }
    }
}

/// What a raw `event` name is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Normalized {
    Event(EmailEvent),
    /// Opens, clicks and proxy opens: dropped, the block stores no
    /// engagement data (NT3).
    Engagement,
    Unknown,
}
/// Brevo's payload names (`hard_bounce`), with the camel-case names of the
/// webhook registration (`hardBounce`) accepted as aliases.
pub fn normalize(name: &str) -> Normalized {
    use EmailEvent::*;
    Normalized::Event(match name {
        "request" | "sent" => Request,
        "delivered" => Delivered,
        "deferred" => Deferred,
        "soft_bounce" | "softBounce" => SoftBounce,
        "hard_bounce" | "hardBounce" => HardBounce,
        "invalid_email" | "invalid" => InvalidEmail,
        "blocked" => Blocked,
        "spam" | "complaint" => Spam,
        "unsubscribed" | "unsubscribe" => Unsubscribed,
        "error" => Error,
        "opened" | "unique_opened" | "uniqueOpened" | "click" | "clicked" | "proxy_open"
        | "unique_proxy_open" => return Normalized::Engagement,
        _ => return Normalized::Unknown,
    })
}

/// One parsed event. `Debug` shows no address, message id or reason.
#[derive(Clone, PartialEq, Eq)]
pub struct Parsed {
    pub event: EmailEvent,
    pub delivery: Option<Uuid>,
    pub message_id: Option<String>,
    pub email: Option<String>,
    pub reason: Option<String>,
    pub occurred_at: Option<jiff::Timestamp>,
    pub key: String,
}
impl std::fmt::Debug for Parsed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Parsed")
            .field("event", &self.event)
            .field("delivery", &self.delivery)
            .finish_non_exhaustive()
    }
}

fn text(value: &Value, key: &str, max: usize) -> Option<String> {
    match value.get(key)? {
        Value::String(s) if !s.is_empty() => Some(s.chars().take(max).collect()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}
fn integer(value: &Value, key: &str) -> Option<i64> {
    match value.get(key)? {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// `sha256(provider|channel|message id|event|ts_event)`, hex (NT §5.2).
pub fn event_key(message_id: &str, event: EmailEvent, ts_event: &str) -> String {
    let digest = Sha256::digest(format!(
        "{PROVIDER}|{CHANNEL}|{message_id}|{}|{ts_event}",
        event.as_str()
    ));
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// One event of a callback body, or `None` for a dropped, unknown or
/// malformed one.
pub fn parse(value: &Value) -> Option<Parsed> {
    let Normalized::Event(event) = normalize(value.get("event")?.as_str()?) else {
        return None;
    };
    let custom =
        text(value, "X-Mailin-custom", 200).or_else(|| text(value, "x-mailin-custom", 200));
    let delivery = custom
        .as_deref()
        .and_then(|c| c.strip_prefix(CUSTOM_PREFIX))
        .and_then(|id| Uuid::parse_str(id.trim()).ok());
    let message_id = text(value, "message-id", 256).or_else(|| text(value, "messageId", 256));
    let email = text(value, "email", 320)
        .and_then(|e| EmailAddress::new(e).ok())
        .map(|e| e.expose().to_lowercase());
    let reason = text(value, "reason", MAX_REASON).map(|r| {
        r.chars()
            .map(|c| if c.is_control() { ' ' } else { c })
            .collect::<String>()
    });
    let occurred_at = integer(value, "ts_epoch")
        .and_then(|ms| jiff::Timestamp::from_millisecond(ms).ok())
        .or_else(|| {
            integer(value, "ts_event")
                .or_else(|| integer(value, "ts"))
                .and_then(|s| jiff::Timestamp::from_second(s).ok())
        });
    let ts = text(value, "ts_event", 32)
        .or_else(|| text(value, "ts_epoch", 32))
        .or_else(|| text(value, "ts", 32))
        .unwrap_or_default();
    let key_id = message_id
        .clone()
        .or_else(|| delivery.map(|d| d.to_string()))
        .unwrap_or_default();
    Some(Parsed {
        event,
        delivery,
        key: event_key(&key_id, event, &ts),
        message_id,
        email,
        reason,
        occurred_at,
    })
}

/// Why a callback was refused, the `code` of
/// `notification.provider_event_rejected`.
fn authorize(
    ctx: &Ctx,
    headers: &HeaderMap,
    client: Option<&ClientInfo>,
) -> Result<(), &'static str> {
    let Some(brevo) = &ctx.checked.settings.brevo else {
        return Err("not_configured");
    };
    let Some(token) = &brevo.webhook_token else {
        return Err("not_configured");
    };
    if !brevo.webhook_allowed_cidrs.is_empty() {
        let address = client.and_then(|c| c.address);
        let allowed = address.is_some_and(|ip| {
            brevo
                .webhook_allowed_cidrs
                .iter()
                .any(|cidr| cidr.contains(ip))
        });
        if !allowed {
            return Err("source_not_allowed");
        }
    }
    let presented = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.split_once(' ')
                .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
                .map(|(_, t)| t.trim())
        })
        .ok_or("missing_token")?;
    // Digests compare in constant time whatever the lengths.
    let want = Sha256::digest(token.expose().as_bytes());
    let got = Sha256::digest(presented.as_bytes());
    if bool::from(want.as_slice().ct_eq(got.as_slice())) {
        Ok(())
    } else {
        Err("bad_token")
    }
}

fn rejected(code: &'static str) {
    merezha_telemetry::event!(
        WARN,
        "notification.provider_event_rejected",
        provider = PROVIDER,
        channel = CHANNEL,
        code
    );
}

/// The route handler.
pub(crate) async fn callback(
    axum::extract::State(ctx): axum::extract::State<Ctx>,
    client: Option<Extension<ClientInfo>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    match authorize(&ctx, &headers, client.as_ref().map(|c| &c.0)) {
        Ok(()) => {}
        Err("not_configured") => {
            return ErrorReply::new(Failure::permanent("not_found")).into_response();
        }
        Err(code) => {
            rejected(code);
            return ErrorReply::new(Failure::permanent("unauthenticated")).into_response();
        }
    }
    let bytes = match axum::body::to_bytes(body, MAX_BODY).await {
        Ok(bytes) => bytes,
        Err(_) => return ErrorReply::new(Failure::permanent("body_limit")).into_response(),
    };
    let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
        return ErrorReply::new(Failure::permanent("invalid_input")).into_response();
    };
    let items = match value {
        Value::Array(items) => items,
        object @ Value::Object(_) => vec![object],
        _ => return ErrorReply::new(Failure::permanent("invalid_input")).into_response(),
    };
    if items.len() > MAX_EVENTS {
        return ErrorReply::new(Failure::permanent("body_limit")).into_response();
    }
    let mut events: Vec<Parsed> = items.iter().filter_map(parse).collect();
    // One lock order for concurrent callbacks; stable, so a delivery's
    // events keep their order.
    events.sort_by(|a, b| {
        (a.delivery, a.message_id.as_deref()).cmp(&(b.delivery, b.message_id.as_deref()))
    });
    let core = match ctx.core() {
        Ok(core) => core,
        Err(reply) => return reply.into_response(),
    };
    match apply(&core, events).await {
        Ok(applied) => {
            for (event, correlated) in applied {
                core.metrics
                    .provider_event(PROVIDER, CHANNEL, event.as_str(), correlated);
                if correlated {
                    merezha_telemetry::event!(
                        DEBUG,
                        "notification.provider_event_applied",
                        provider = PROVIDER,
                        channel = CHANNEL,
                        event = event.as_str()
                    );
                } else {
                    merezha_telemetry::event!(
                        INFO,
                        "notification.provider_event_uncorrelated",
                        provider = PROVIDER,
                        channel = CHANNEL,
                        event = event.as_str()
                    );
                }
            }
            (StatusCode::OK, axum::Json(json!({}))).into_response()
        }
        Err(failure) => ErrorReply::new(failure).into_response(),
    }
}

/// Fresh suppression ids.
fn new_id(core: &Core) -> Result<Uuid, Failure> {
    let mut bytes = [0u8; 16];
    core.random.fill(&mut bytes)?;
    Ok(uuid::Builder::from_random_bytes(bytes).into_uuid())
}

/// The one transaction of a callback. Returns the newly stored events and
/// whether each was correlated.
async fn apply(core: &Core, events: Vec<Parsed>) -> Result<Vec<(EmailEvent, bool)>, Failure> {
    let mut ids = Vec::with_capacity(events.len());
    for event in &events {
        ids.push(if event.event.suppression().is_some() {
            Some(new_id(core)?)
        } else {
            None
        });
    }
    let db = core.database.clone();
    let now = core.now();
    db.clone()
        .transaction(Instant::now() + DEADLINE, move |tx| {
            Box::pin(async move {
                let mut applied = Vec::new();
                for (event, suppression_id) in events.into_iter().zip(ids) {
                    // Correlate: our header first, then Brevo's message id.
                    let mut row = None;
                    if let Some(id) = event.delivery {
                        row = db
                            .query_opt(tx, &sql::EVENT_DELIVERY_BY_ID, &[&id, &CHANNEL])
                            .await
                            .map_err(|e| classify(&e))?;
                    }
                    if row.is_none()
                        && let Some(message_id) = &event.message_id
                    {
                        row = db
                            .query_opt(
                                tx,
                                &sql::EVENT_DELIVERY_BY_MESSAGE,
                                &[&PROVIDER, message_id, &CHANNEL],
                            )
                            .await
                            .map_err(|e| classify(&e))?;
                    }
                    let delivery: Option<(Uuid, String, Option<String>, String)> =
                        row.map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)));
                    let inserted = db
                        .query_opt(
                            tx,
                            &sql::PROVIDER_EVENT_INSERT,
                            &[
                                &PROVIDER,
                                &CHANNEL,
                                &event.event.as_str(),
                                &event.key,
                                &delivery.as_ref().map(|d| d.0),
                                &event.message_id,
                                &event.reason,
                                &event.occurred_at,
                                &now,
                            ],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                    if inserted.is_none() {
                        // Already applied: Brevo retried or batched it twice.
                        continue;
                    }
                    if let (Some((id, status, _, _)), Some((to, why, from))) =
                        (&delivery, event.event.transition())
                        && from.contains(&status.as_str())
                    {
                        let from: Vec<&str> = from.to_vec();
                        db.execute(tx, &sql::DELIVERY_EVENT_SET, &[id, &to, &why, &now, &from])
                            .await
                            .map_err(|e| classify(&e))?;
                    }
                    if let (Some((reason_text, topic_scoped)), Some(suppression_id)) =
                        (event.event.suppression(), suppression_id)
                    {
                        let address = delivery
                            .as_ref()
                            .and_then(|d| d.2.as_ref().map(|a| a.to_lowercase()))
                            .or_else(|| event.email.clone());
                        let topic = if topic_scoped {
                            delivery.as_ref().map(|d| d.3.clone())
                        } else {
                            None
                        };
                        // A topic-scoped suppression needs the topic.
                        if let Some(address) = address
                            && (!topic_scoped || topic.is_some())
                        {
                            db.execute(
                                tx,
                                &sql::SUPPRESSION_INSERT,
                                &[
                                    &suppression_id,
                                    &CHANNEL,
                                    &address,
                                    &topic,
                                    &reason_text,
                                    &delivery.as_ref().map(|d| d.0),
                                    &now,
                                ],
                            )
                            .await
                            .map_err(|e| classify(&e))?;
                        }
                    }
                    applied.push((event.event, delivery.is_some()));
                }
                Ok(applied)
            })
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_are_normalized_and_engagement_is_dropped() {
        assert_eq!(
            normalize("hard_bounce"),
            Normalized::Event(EmailEvent::HardBounce)
        );
        assert_eq!(
            normalize("hardBounce"),
            Normalized::Event(EmailEvent::HardBounce)
        );
        assert_eq!(
            normalize("invalid_email"),
            Normalized::Event(EmailEvent::InvalidEmail)
        );
        assert_eq!(normalize("opened"), Normalized::Engagement);
        assert_eq!(normalize("click"), Normalized::Engagement);
        assert_eq!(normalize("unique_proxy_open"), Normalized::Engagement);
        assert_eq!(normalize("whatever"), Normalized::Unknown);
        assert!(parse(&json!({"event": "opened", "email": "a@b.c"})).is_none());
    }

    #[test]
    fn a_payload_is_correlated_by_its_custom_header_and_keyed_by_message_and_time() {
        let id = Uuid::from_u128(7);
        let event = json!({
            "event": "hard_bounce", "email": "Ada@Example.com", "message-id": "<m1@relay>",
            "X-Mailin-custom": format!("mzd:{id}"), "ts_event": 1_700_000_000,
            "ts_epoch": 1_700_000_000_123_i64, "reason": "550 user unknown\r\n",
            "subject": "Hello",
        });
        let parsed = parse(&event).unwrap();
        assert_eq!(parsed.delivery, Some(id));
        assert_eq!(parsed.email.as_deref(), Some("ada@example.com"));
        assert_eq!(parsed.reason.as_deref(), Some("550 user unknown  "));
        assert_eq!(
            parsed.occurred_at,
            Some(jiff::Timestamp::from_millisecond(1_700_000_000_123).unwrap())
        );
        assert_eq!(
            parsed.key,
            event_key("<m1@relay>", EmailEvent::HardBounce, "1700000000")
        );
        assert_ne!(
            parsed.key,
            event_key("<m1@relay>", EmailEvent::Delivered, "1700000000")
        );
        let debug = format!("{parsed:?}");
        assert!(
            !debug.contains("example") && !debug.contains("m1@relay"),
            "{debug}"
        );
        // A foreign custom header does not correlate.
        let foreign = parse(&json!({"event": "delivered", "X-Mailin-custom": "order-17"})).unwrap();
        assert_eq!(foreign.delivery, None);
    }
}
