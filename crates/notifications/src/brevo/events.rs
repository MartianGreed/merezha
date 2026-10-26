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
//! `sha256(provider|channel|message id|event|ts_event)` (the delivery and
//! the address stand in for a missing message id) and applied once.
//! Deliveries are resolved first and locked in ascending id order. An
//! event about a delivery still `queued`, `sending` or `retrying` is
//! stored `pending` and applied by the worker's Tx B ([`apply_pending`]):
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
//! stored with `delivery_id` NULL and no effect at all (no status change,
//! no suppression: a forged or foreign event names any address it likes),
//! and still answered 200, so Brevo stops retrying. Suppressions always use
//! the delivery's own address. No log line or span carries an address,
//! subject, reason text or message id.
//!
//! The SMS status callback ([`super::sms_status`], NT-N4) runs its events
//! through the same transaction (`apply`) and source check
//! (`source_allowed`), with its own event set ([`StatusEvent`]).
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

/// The status changes `(to, reason, from)` and suppressions `(reason,
/// topic_scoped)` of one normalized provider event, for the shared
/// callback transaction (`apply`).
pub trait StatusEvent: Copy + Send + Sync + 'static {
    /// The stored name (`provider_events.event`, `notify.event`).
    fn name(self) -> &'static str;
    /// The delivery status this event moves to, its reason and the
    /// statuses it may move from; `None` records the event only.
    fn status_change(self) -> Transition;
    /// The suppression reason this event writes, and whether it is scoped
    /// to the delivery's topic.
    fn suppression_reason(self) -> Option<(&'static str, bool)>;
}
/// `(to, reason, from)` of a status change.
pub type Transition = Option<(&'static str, Option<&'static str>, &'static [&'static str])>;

impl StatusEvent for EmailEvent {
    fn name(self) -> &'static str {
        self.as_str()
    }
    fn status_change(self) -> Transition {
        self.transition()
    }
    fn suppression_reason(self) -> Option<(&'static str, bool)> {
        self.suppression()
    }
}

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
    pub fn transition(self) -> Transition {
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
pub struct Parsed<E = EmailEvent> {
    pub event: E,
    pub delivery: Option<Uuid>,
    pub message_id: Option<String>,
    pub email: Option<String>,
    pub reason: Option<String>,
    pub occurred_at: Option<jiff::Timestamp>,
    pub key: String,
}
impl<E: std::fmt::Debug> std::fmt::Debug for Parsed<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Parsed")
            .field("event", &self.event)
            .field("delivery", &self.delivery)
            .finish_non_exhaustive()
    }
}

pub(crate) fn text(value: &Value, key: &str, max: usize) -> Option<String> {
    match value.get(key)? {
        Value::String(s) if !s.is_empty() => Some(s.chars().take(max).collect()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}
pub(crate) fn integer(value: &Value, key: &str) -> Option<i64> {
    match value.get(key)? {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// `sha256(provider|channel|message id|event|ts_event)`, hex (NT §5.2).
pub fn event_key(message_id: &str, event: EmailEvent, ts_event: &str) -> String {
    channel_event_key(CHANNEL, message_id, event.as_str(), ts_event)
}
/// `sha256(brevo|channel|message id|event|ts_event)`, hex.
pub(crate) fn channel_event_key(
    channel: &str,
    message_id: &str,
    event: &str,
    ts_event: &str,
) -> String {
    let digest = Sha256::digest(format!(
        "{PROVIDER}|{channel}|{message_id}|{event}|{ts_event}"
    ));
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// Brevo's reason text, at most [`MAX_REASON`] characters with control
/// characters replaced (never logged).
pub(crate) fn reason_text(value: &Value, key: &str) -> Option<String> {
    text(value, key, MAX_REASON).map(|r| {
        r.chars()
            .map(|c| if c.is_control() { ' ' } else { c })
            .collect::<String>()
    })
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
    let reason = reason_text(value, "reason");
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
    // Without Brevo's message id, the delivery and the address tell two
    // events of the same name and time apart.
    let key_id = message_id.clone().unwrap_or_else(|| {
        format!(
            "-|{}|{}",
            delivery.map(|d| d.to_string()).unwrap_or_default(),
            email.as_deref().unwrap_or_default()
        )
    });
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

/// Whether the client address the edge resolved may call a Brevo
/// callback: always without `brevo.webhook_allowed_cidrs`, else only from
/// inside one of them (a request without a resolved address is refused).
pub(crate) fn source_allowed(
    brevo: &crate::config::BrevoSettings,
    client: Option<&ClientInfo>,
) -> bool {
    brevo.webhook_allowed_cidrs.is_empty()
        || client.and_then(|c| c.address).is_some_and(|ip| {
            brevo
                .webhook_allowed_cidrs
                .iter()
                .any(|cidr| cidr.contains(ip))
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
    if !source_allowed(brevo, client) {
        return Err("source_not_allowed");
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

pub(crate) fn rejected(channel: &'static str, code: &'static str) {
    merezha_telemetry::event!(
        WARN,
        "notification.provider_event_rejected",
        provider = PROVIDER,
        channel,
        code
    );
}

/// Count and log the stored events of one callback.
pub(crate) fn report<E: StatusEvent>(core: &Core, channel: &'static str, applied: &[(E, bool)]) {
    for (event, correlated) in applied {
        core.metrics
            .provider_event(PROVIDER, channel, event.name(), *correlated);
        if *correlated {
            merezha_telemetry::event!(
                DEBUG,
                "notification.provider_event_applied",
                provider = PROVIDER,
                channel,
                event = event.name()
            );
        } else {
            merezha_telemetry::event!(
                INFO,
                "notification.provider_event_uncorrelated",
                provider = PROVIDER,
                channel,
                event = event.name()
            );
        }
    }
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
            rejected(CHANNEL, code);
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
    let events: Vec<Parsed> = items.iter().filter_map(parse).collect();
    let core = match ctx.core() {
        Ok(core) => core,
        Err(reply) => return reply.into_response(),
    };
    match apply(&core, CHANNEL, true, events).await {
        Ok(applied) => {
            report(&core, CHANNEL, &applied);
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

/// The statuses a worker still owns: an event about such a delivery waits,
/// `pending`, until the worker's Tx B applies it.
const OPEN: [&str; 3] = ["queued", "sending", "retrying"];

/// Apply a status change to a locked delivery: one conditional update, a
/// no-op unless the delivery is in a status the event may leave.
async fn transition(
    db: &merezha_postgres::Database,
    tx: &mut merezha_postgres::Tx<'_>,
    delivery: Uuid,
    change: Transition,
    now: jiff::Timestamp,
) -> Result<(), Failure> {
    if let Some((to, why, from)) = change {
        let from: Vec<&str> = from.to_vec();
        db.execute(
            tx,
            &sql::DELIVERY_EVENT_SET,
            &[&delivery, &to, &why, &now, &from],
        )
        .await
        .map_err(|e| classify(&e))?;
    }
    Ok(())
}

/// The events stored `pending` for `delivery`, applied in arrival order
/// and cleared. The delivery worker calls it in Tx B, after the outcome is
/// recorded, with the delivery locked.
pub(crate) async fn apply_pending(
    db: &merezha_postgres::Database,
    tx: &mut merezha_postgres::Tx<'_>,
    delivery: Uuid,
    now: jiff::Timestamp,
) -> Result<usize, Failure> {
    let rows = db
        .query(tx, &sql::EVENTS_TAKE_PENDING, &[&delivery])
        .await
        .map_err(|e| classify(&e))?;
    let mut events: Vec<(i64, String, String)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    events.sort();
    for (_, name, channel) in &events {
        transition(db, tx, delivery, stored_transition(channel, name), now).await?;
    }
    Ok(events.len())
}

/// The status change of a stored event, by its channel's event set.
fn stored_transition(channel: &str, name: &str) -> Transition {
    match channel {
        super::sms_status::CHANNEL => {
            super::sms_status::normalize(name).and_then(|e| e.transition())
        }
        _ => match normalize(name) {
            Normalized::Event(event) => event.transition(),
            _ => None,
        },
    }
}

/// The one transaction of a callback on `channel`. Returns the newly
/// stored events and whether each was correlated.
///
/// Every event is first resolved to its delivery (our header or the
/// signed path, then, when `by_message`, Brevo's message id) without a
/// lock; the events are then applied in ascending
/// delivery order, so two concurrent callbacks lock the same deliveries in
/// the same order. Only a correlated event has effects: an uncorrelated one
/// (no delivery of ours, whatever address it names) is stored and nothing
/// else, so a forged or foreign event cannot suppress an address.
pub(crate) async fn apply<E: StatusEvent>(
    core: &Core,
    channel: &'static str,
    by_message: bool,
    events: Vec<Parsed<E>>,
) -> Result<Vec<(E, bool)>, Failure> {
    let mut ids = Vec::with_capacity(events.len());
    for event in &events {
        ids.push(if event.event.suppression_reason().is_some() {
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
                let mut resolved = Vec::with_capacity(events.len());
                for (event, suppression_id) in events.into_iter().zip(ids) {
                    let mut found: Option<Uuid> = None;
                    if let Some(id) = event.delivery {
                        found = db
                            .query_opt(tx, &sql::EVENT_RESOLVE_BY_ID, &[&id, &channel])
                            .await
                            .map_err(|e| classify(&e))?
                            .map(|r| r.get(0));
                    }
                    if found.is_none()
                        && by_message
                        && let Some(message_id) = &event.message_id
                    {
                        found = db
                            .query_opt(
                                tx,
                                &sql::EVENT_RESOLVE_BY_MESSAGE,
                                &[&PROVIDER, message_id, &channel],
                            )
                            .await
                            .map_err(|e| classify(&e))?
                            .map(|r| r.get(0));
                    }
                    resolved.push((found, event, suppression_id));
                }
                // Ascending delivery ids, uncorrelated last; stable, so a
                // delivery's events keep their order.
                resolved.sort_by_key(|(id, _, _)| (id.is_none(), *id));
                let mut applied = Vec::new();
                for (found, event, suppression_id) in resolved {
                    let delivery: Option<(Uuid, String, Option<String>, String)> = match found {
                        Some(id) => db
                            .query_opt(tx, &sql::EVENT_DELIVERY_BY_ID, &[&id, &channel])
                            .await
                            .map_err(|e| classify(&e))?
                            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3))),
                        None => None,
                    };
                    // A delivery its worker still owns gets the event later.
                    let pending = delivery.as_ref().is_some_and(|d| {
                        OPEN.contains(&d.1.as_str()) && event.event.status_change().is_some()
                    });
                    let inserted = db
                        .query_opt(
                            tx,
                            &sql::PROVIDER_EVENT_INSERT,
                            &[
                                &PROVIDER,
                                &channel,
                                &event.event.name(),
                                &event.key,
                                &delivery.as_ref().map(|d| d.0),
                                &event.message_id,
                                &event.reason,
                                &event.occurred_at,
                                &now,
                                &pending,
                            ],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                    if inserted.is_none() {
                        // Already stored: Brevo retried or batched it twice.
                        continue;
                    }
                    let Some((id, _, address, topic)) = &delivery else {
                        applied.push((event.event, false));
                        continue;
                    };
                    if !pending {
                        transition(&db, tx, *id, event.event.status_change(), now).await?;
                    }
                    // The address is ours: the delivery's, never the event's.
                    if let (
                        Some((reason_text, topic_scoped)),
                        Some(suppression_id),
                        Some(address),
                    ) = (event.event.suppression_reason(), suppression_id, address)
                    {
                        let topic = topic_scoped.then(|| topic.clone());
                        db.execute(
                            tx,
                            &sql::SUPPRESSION_INSERT,
                            &[
                                &suppression_id,
                                &channel,
                                &address.to_lowercase(),
                                &topic,
                                &reason_text,
                                &Some(*id),
                                &now,
                            ],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                    }
                    applied.push((event.event, true));
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
        // Without a message id the delivery and the address key the event.
        let bare = |email: &str, custom: Option<Uuid>| {
            let mut event = json!({"event": "hard_bounce", "email": email, "ts_event": 5});
            if let Some(id) = custom {
                event["X-Mailin-custom"] = json!(format!("mzd:{id}"));
            }
            parse(&event).unwrap().key
        };
        assert_ne!(bare("a@example.org", None), bare("b@example.org", None));
        assert_ne!(
            bare("a@example.org", Some(Uuid::from_u128(1))),
            bare("a@example.org", Some(Uuid::from_u128(2)))
        );
        assert_eq!(bare("a@example.org", None), bare("A@example.org", None));
        // A foreign custom header does not correlate.
        let foreign = parse(&json!({"event": "delivered", "X-Mailin-custom": "order-17"})).unwrap();
        assert_eq!(foreign.delivery, None);
    }
}
