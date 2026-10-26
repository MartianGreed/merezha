//! `POST /v1/notifications/providers/brevo/sms/{delivery_id}/{mac}` (NT
//! §5.2 "Status webhooks", slice NT-N4): Brevo's report about one SMS.
//!
//! Brevo neither signs its webhooks nor lets a per-message `webUrl` carry
//! a header, so the URL proves itself: its path holds the delivery id and
//! `mac = base64url(HMAC-SHA256(k_sms, delivery_id))[..22]`, which the
//! handler recomputes and compares in constant time (current key, then
//! `secret_key_previous`). The route is credential-free at the edge
//! (`Module::credential_free_routes`); the optional source CIDRs are
//! checked first, as for email. A failed check answers 401, stores nothing
//! and logs `notification.provider_event_rejected` (`source_not_allowed`,
//! `bad_mac`). Without the Brevo SMS driver the route is 404.
//!
//! The body, an event or an array of at most [`MAX_EVENTS`], goes through
//! the email callback's transaction (`events::apply`): the event
//! is correlated by the signed path id only (never by a message id, so a
//! URL for one delivery cannot report on another), deduplicated on
//! `sha256(brevo|sms|message id|event|ts_event)` and applied once. The
//! signed id survives a redelivery, so a report whose `messageId` is not
//! the current generation's moves no status (NT1: a late `rejected` about
//! the first SMS must not fail, and so invite a third send of, the
//! second); its suppression still applies to the delivery's own number:
//!
//! | `msg_status` | Effect |
//! | --- | --- |
//! | `delivered` | `accepted` or `unknown` to `delivered` |
//! | `hard_bounce` | `accepted` or `unknown` to `bounced`, plus a `hard_bounce` suppression of the number |
//! | `rejected` | `accepted` or `unknown` to `failed` / `provider_rejected` |
//! | `blacklisted`, `unsubscribed` (a STOP reply) | an `unsubscribed` suppression of the number for every SMS topic |
//! | `sent`, `accepted`, `soft_bounce` | recorded only |
//!
//! Replies are dropped unstored: their text is the recipient's message.
//! Suppressions use the delivery's own number; no log line, span or metric
//! carries a number, a message id or Brevo's description.
use super::events::{self, Parsed, StatusEvent, Transition};
use crate::http::Ctx;
use axum::{
    Extension,
    body::Body,
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use merezha_domain::Failure;
use merezha_platform::{ErrorReply, edge::ClientInfo, ports::PhoneNumber};
use serde_json::{Value, json};
use uuid::Uuid;

/// The route, relative to `/v1/notifications`.
pub const PATH: &str = "/providers/brevo/sms/{delivery_id}/{mac}";
/// The path before the delivery id and the MAC.
pub const PREFIX: &str = "/providers/brevo/sms";
/// The `provider_events.channel` of these events.
pub const CHANNEL: &str = "sms";
/// Most bytes of one callback body: one message's reports.
pub const MAX_BODY: usize = 64 * 1024;
/// Most events in one callback body.
pub const MAX_EVENTS: usize = 100;

/// A normalized Brevo transactional SMS status (NT §5.2 step 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SmsEvent {
    Sent,
    Delivered,
    SoftBounce,
    HardBounce,
    Blacklisted,
    Rejected,
    Unsubscribed,
}
impl SmsEvent {
    pub fn as_str(self) -> &'static str {
        match self {
            SmsEvent::Sent => "sent",
            SmsEvent::Delivered => "delivered",
            SmsEvent::SoftBounce => "soft_bounce",
            SmsEvent::HardBounce => "hard_bounce",
            SmsEvent::Blacklisted => "blacklisted",
            SmsEvent::Rejected => "rejected",
            SmsEvent::Unsubscribed => "unsubscribed",
        }
    }
    /// See [`StatusEvent::status_change`]. A delivery still `queued`,
    /// `sending` or `retrying` is never moved; its worker records the
    /// outcome and applies the event after it.
    pub fn transition(self) -> Transition {
        match self {
            SmsEvent::Delivered => Some(("delivered", None, &["accepted", "unknown"])),
            SmsEvent::HardBounce => {
                Some(("bounced", Some("hard_bounce"), &["accepted", "unknown"]))
            }
            SmsEvent::Rejected => Some((
                "failed",
                Some("provider_rejected"),
                &["accepted", "unknown"],
            )),
            _ => None,
        }
    }
    /// See [`StatusEvent::suppression_reason`]: an opt-out covers every
    /// non-security SMS topic of the number.
    pub fn suppression(self) -> Option<(&'static str, bool)> {
        match self {
            SmsEvent::HardBounce => Some(("hard_bounce", false)),
            SmsEvent::Blacklisted | SmsEvent::Unsubscribed => Some(("unsubscribed", false)),
            _ => None,
        }
    }
}
impl StatusEvent for SmsEvent {
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

/// Brevo's `msg_status` (or `event`) name, with the camel-case spellings
/// accepted; `None` for a reply (dropped unstored) or an unknown name.
pub fn normalize(name: &str) -> Option<SmsEvent> {
    use SmsEvent::*;
    Some(match name {
        "sent" | "accepted" | "request" => Sent,
        "delivered" => Delivered,
        "soft_bounce" | "softBounce" | "softBounces" => SoftBounce,
        "hard_bounce" | "hardBounce" | "hardBounces" => HardBounce,
        "blacklisted" | "blocked" => Blacklisted,
        "rejected" => Rejected,
        "unsubscribed" | "unsubscribe" | "stop" => Unsubscribed,
        _ => return None,
    })
}

/// One event of a callback body about `delivery` (the signed path id), or
/// `None` for a reply, an unknown or a malformed one.
pub fn parse(value: &Value, delivery: Uuid) -> Option<Parsed<SmsEvent>> {
    let name = value
        .get("msg_status")
        .or_else(|| value.get("event"))?
        .as_str()?;
    let event = normalize(name)?;
    let message_id = events::text(value, "messageId", 256)
        .or_else(|| events::text(value, "message-id", 256))
        .or_else(|| events::text(value, "message_id", 256));
    let number = events::text(value, "to", 32)
        .or_else(|| events::text(value, "recipient", 32))
        .map(|n| {
            if n.starts_with('+') {
                n
            } else {
                format!("+{n}")
            }
        })
        .and_then(|n| PhoneNumber::new(n).ok())
        .map(|n| n.expose().to_owned());
    let occurred_at = events::integer(value, "ts_event")
        .or_else(|| events::integer(value, "ts"))
        .and_then(|s| jiff::Timestamp::from_second(s).ok());
    let ts = events::text(value, "ts_event", 32)
        .or_else(|| events::text(value, "ts", 32))
        .or_else(|| events::text(value, "date", 32))
        .unwrap_or_default();
    // Without Brevo's message id, the delivery and the number key the
    // event, as for email.
    let key_id = message_id
        .clone()
        .unwrap_or_else(|| format!("-|{delivery}|{}", number.as_deref().unwrap_or_default()));
    Some(Parsed {
        event,
        delivery: Some(delivery),
        key: events::channel_event_key(CHANNEL, &key_id, event.as_str(), &ts),
        message_id,
        email: None,
        reason: events::reason_text(value, "description"),
        occurred_at,
    })
}

fn unauthenticated(code: &'static str) -> Response {
    events::rejected(CHANNEL, code);
    ErrorReply::new(Failure::permanent("unauthenticated")).into_response()
}

/// The route handler.
pub(crate) async fn callback(
    axum::extract::State(ctx): axum::extract::State<Ctx>,
    Path((delivery, mac)): Path<(String, String)>,
    client: Option<Extension<ClientInfo>>,
    body: Body,
) -> Response {
    let settings = &ctx.checked.settings;
    let Some(brevo) = settings
        .brevo
        .as_ref()
        .filter(|_| settings.sms.driver == crate::config::Driver::Brevo)
    else {
        return ErrorReply::new(Failure::permanent("not_found")).into_response();
    };
    if !events::source_allowed(brevo, client.as_ref().map(|c| &c.0)) {
        return unauthenticated("source_not_allowed");
    }
    let core = match ctx.core() {
        Ok(core) => core,
        Err(reply) => return reply.into_response(),
    };
    // Only the canonical hyphenated form is ever signed.
    let delivery = Uuid::parse_str(&delivery)
        .ok()
        .filter(|id| id.hyphenated().to_string() == delivery);
    let Some(delivery) = delivery.filter(|id| core.keys.verify_sms_status_mac(*id, &mac)) else {
        return unauthenticated("bad_mac");
    };
    let bytes = match axum::body::to_bytes(body, MAX_BODY).await {
        Ok(bytes) => bytes,
        Err(_) => return ErrorReply::new(Failure::permanent("body_limit")).into_response(),
    };
    let items = match serde_json::from_slice::<Value>(&bytes) {
        Ok(Value::Array(items)) => items,
        Ok(object @ Value::Object(_)) => vec![object],
        _ => return ErrorReply::new(Failure::permanent("invalid_input")).into_response(),
    };
    if items.len() > MAX_EVENTS {
        return ErrorReply::new(Failure::permanent("body_limit")).into_response();
    }
    let parsed: Vec<Parsed<SmsEvent>> = items.iter().filter_map(|v| parse(v, delivery)).collect();
    match events::apply(&core, CHANNEL, false, parsed).await {
        Ok(applied) => {
            events::report(&core, CHANNEL, &applied);
            (StatusCode::OK, axum::Json(json!({}))).into_response()
        }
        Err(failure) => ErrorReply::new(failure).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_are_normalized_and_replies_dropped() {
        assert_eq!(normalize("delivered"), Some(SmsEvent::Delivered));
        assert_eq!(normalize("hardBounce"), Some(SmsEvent::HardBounce));
        assert_eq!(normalize("unsubscribed"), Some(SmsEvent::Unsubscribed));
        assert_eq!(normalize("replied"), None);
        assert_eq!(normalize("whatever"), None);
        let id = Uuid::from_u128(3);
        assert!(parse(&json!({"msg_status": "replied", "reply": "hi"}), id).is_none());
        assert_eq!(
            SmsEvent::Blacklisted.suppression(),
            Some(("unsubscribed", false))
        );
        assert_eq!(SmsEvent::SoftBounce.transition(), None);
    }

    #[test]
    fn a_report_is_keyed_by_message_and_time_and_hides_the_number() {
        let id = Uuid::from_u128(3);
        let event = json!({
            "msg_status": "delivered", "to": "33612345678", "messageId": 1511882900176220_i64,
            "ts_event": 1_700_000_000, "description": "delivered\n", "reply": "secret text",
        });
        let parsed = parse(&event, id).unwrap();
        assert_eq!(parsed.delivery, Some(id));
        assert_eq!(parsed.message_id.as_deref(), Some("1511882900176220"));
        assert_eq!(parsed.reason.as_deref(), Some("delivered "));
        assert_eq!(
            parsed.key,
            events::channel_event_key(CHANNEL, "1511882900176220", "delivered", "1700000000")
        );
        assert_ne!(
            parsed.key,
            events::channel_event_key("email", "1511882900176220", "delivered", "1700000000")
        );
        let debug = format!("{parsed:?}");
        assert!(
            !debug.contains("612345678") && !debug.contains("15118829"),
            "{debug}"
        );
        // Without a message id the delivery and the number key it.
        let bare = |to: &str, delivery: u128| {
            parse(
                &json!({"msg_status": "hard_bounce", "to": to, "ts_event": 5}),
                Uuid::from_u128(delivery),
            )
            .unwrap()
            .key
        };
        assert_ne!(bare("33611111111", 1), bare("33622222222", 1));
        assert_ne!(bare("33611111111", 1), bare("33611111111", 2));
        assert_eq!(bare("33611111111", 1), bare("+33611111111", 1));
    }
}
