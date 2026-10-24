//! Delivery drivers for the email and SMS channels (NT §5.1, §8).
//!
//! A driver takes one rendered message for one target and reports a
//! [`SendOutcome`]; the delivery worker (NT-N2/N3) records the attempt and
//! maps the outcome onto the delivery state machine. This slice ships the
//! capture drivers: they keep the rendered message in
//! `merezha_notify.captures` (the newest `retention.captures` rows) and
//! accept it with provider `capture`, so the whole pipeline runs without a
//! provider. Brevo (NT-N3, NT-N4) implements the same traits.
use crate::templates::{RenderedEmail, RenderedSms};
use merezha_domain::Failure;
use merezha_platform::{
    BoxFuture,
    ports::{Channel, EmailAddress, PhoneNumber},
};
use merezha_postgres::{Database, Statement, classify};
use serde_json::{Value, json};
use std::time::{Duration, Instant, SystemTime};
use uuid::Uuid;

/// The `deliveries.provider` values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Provider {
    Brevo,
    Webpush,
    Inbox,
    Capture,
}
impl Provider {
    pub fn as_str(self) -> &'static str {
        match self {
            Provider::Brevo => "brevo",
            Provider::Webpush => "webpush",
            Provider::Inbox => "inbox",
            Provider::Capture => "capture",
        }
    }
}

/// One provider call's result, the attempt outcomes of NT §2.4.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SendOutcome {
    Accepted {
        provider: Provider,
        message_id: Option<String>,
    },
    /// The provider had already processed this idempotency key.
    Duplicate {
        provider: Provider,
    },
    Transient {
        code: String,
        retry_after: Option<Duration>,
    },
    RateLimited {
        retry_after: Option<Duration>,
    },
    Permanent {
        code: String,
    },
    /// The request may or may not have been processed.
    Ambiguous {
        code: String,
    },
}
impl SendOutcome {
    fn transient(failure: &Failure) -> Self {
        SendOutcome::Transient {
            code: failure.code.clone(),
            retry_after: None,
        }
    }
}

/// One email to one recipient. `Debug` shows ids only.
#[derive(Clone, PartialEq, Eq)]
pub struct EmailMessage {
    pub delivery_id: Uuid,
    /// Stable across the attempts of one generation (NT §2.5).
    pub idempotency_key: Uuid,
    pub topic: String,
    pub to: EmailAddress,
    pub to_name: Option<String>,
    pub content: RenderedEmail,
    /// Sent as `List-Unsubscribe` for non-security topics.
    pub unsubscribe: Option<String>,
}
impl std::fmt::Debug for EmailMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmailMessage")
            .field("delivery_id", &self.delivery_id)
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}
/// One SMS to one number. `Debug` shows ids only.
#[derive(Clone, PartialEq, Eq)]
pub struct SmsMessage {
    pub delivery_id: Uuid,
    pub topic: String,
    pub to: PhoneNumber,
    pub content: RenderedSms,
    pub marketing: bool,
}
impl std::fmt::Debug for SmsMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SmsMessage")
            .field("delivery_id", &self.delivery_id)
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}

/// Sends email. Contract: one recipient per call, never retried inside the
/// driver, finished before `deadline`; the outcome classifies every result.
pub trait EmailDriver: Send + Sync {
    fn provider(&self) -> Provider;
    fn send<'a>(
        &'a self,
        message: &'a EmailMessage,
        deadline: Instant,
    ) -> BoxFuture<'a, SendOutcome>;
}
/// Sends SMS, with the [`EmailDriver`] contract.
pub trait SmsDriver: Send + Sync {
    fn provider(&self) -> Provider;
    fn send<'a>(&'a self, message: &'a SmsMessage, deadline: Instant)
    -> BoxFuture<'a, SendOutcome>;
}

/// A captured message, as `GET /v1/notifications/dev/captures` lists it.
/// It holds rendered content, secret data included: development only.
#[derive(Clone, PartialEq)]
pub struct Capture {
    pub id: i64,
    pub delivery_id: Uuid,
    pub channel: String,
    pub target: String,
    pub rendered: Value,
    pub captured_at: jiff::Timestamp,
}
impl std::fmt::Debug for Capture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Capture")
            .field("id", &self.id)
            .field("delivery_id", &self.delivery_id)
            .field("channel", &self.channel)
            .finish_non_exhaustive()
    }
}
impl Capture {
    pub fn to_json(&self) -> Value {
        json!({
            "delivery_id": self.delivery_id.to_string(),
            "channel": self.channel,
            "target": self.target,
            "rendered": self.rendered,
            "captured_at": self.captured_at.to_string(),
        })
    }
}

/// Most captures listed at once.
pub const MAX_CAPTURE_PAGE: u32 = 100;

/// One captured message, unless its delivery and key were captured.
const CAPTURE_INSERT: Statement = Statement::new(
    "notify.capture.insert",
    "INSERT merezha_notify.captures",
    "INSERT INTO merezha_notify.captures \
     (delivery_id, channel, target, rendered, captured_at) \
     VALUES ($1::text::uuid, $2, $3, $4::text::jsonb, $5) \
     ON CONFLICT DO NOTHING RETURNING id",
);
/// Keep the newest `$1` captures.
const CAPTURE_TRIM: Statement = Statement::new(
    "notify.capture.trim",
    "DELETE merezha_notify.captures",
    "DELETE FROM merezha_notify.captures WHERE id <= \
     (SELECT id FROM merezha_notify.captures ORDER BY id DESC \
      OFFSET $1 LIMIT 1)",
);
const CAPTURE_LIST: Statement = Statement::new(
    "notify.capture.list",
    "SELECT merezha_notify.captures",
    "SELECT id, delivery_id::text, channel, target, rendered::text, captured_at \
     FROM merezha_notify.captures \
     WHERE $1::text IS NULL OR channel = $1 \
     ORDER BY id DESC LIMIT $2",
);
/// Every statement the notifications block runs.
pub const STATEMENTS: &[Statement] = &[CAPTURE_INSERT, CAPTURE_TRIM, CAPTURE_LIST];

/// The capture driver for email and SMS (NT §8).
#[derive(Clone, Debug)]
pub struct CaptureDriver {
    database: Database,
    keep: u32,
}
impl CaptureDriver {
    /// Keep the newest `keep` captures (`retention.captures`).
    pub fn new(database: Database, keep: u32) -> Self {
        Self {
            database,
            keep: keep.max(1),
        }
    }

    /// Store one rendered message and trim to the newest `keep` rows, in one
    /// transaction. A second capture with the same delivery and
    /// idempotency key is `Duplicate`, as a provider with idempotency keys
    /// answers.
    pub async fn capture(
        &self,
        delivery_id: Uuid,
        channel: Channel,
        target: &str,
        idempotency_key: Option<Uuid>,
        mut rendered: Value,
        deadline: Instant,
    ) -> SendOutcome {
        if let (Some(key), Value::Object(map)) = (idempotency_key, &mut rendered) {
            map.insert("idempotency_key".into(), Value::String(key.to_string()));
        }
        let rendered = rendered.to_string();
        let target = target.to_owned();
        let keep = i64::from(self.keep);
        let db = self.database.clone();
        let result = self
            .database
            .transaction(deadline, move |tx| {
                Box::pin(async move {
                    let id = delivery_id.to_string();
                    let row = db
                        .query_opt(
                            tx,
                            &CAPTURE_INSERT,
                            &[
                                &id,
                                &channel.as_str(),
                                &target,
                                &rendered,
                                &SystemTime::now(),
                            ],
                        )
                        .await
                        .map_err(|e| classify(&e))?;
                    let Some(row) = row else {
                        return Ok(None);
                    };
                    db.execute(tx, &CAPTURE_TRIM, &[&keep])
                        .await
                        .map_err(|e| classify(&e))?;
                    Ok(Some(row.get::<_, i64>(0)))
                })
            })
            .await;
        match result {
            Ok(Some(id)) => SendOutcome::Accepted {
                provider: Provider::Capture,
                message_id: Some(format!("capture:{id}")),
            },
            Ok(None) => SendOutcome::Duplicate {
                provider: Provider::Capture,
            },
            Err(failure) if failure.code == "commit_outcome_unknown" => {
                SendOutcome::Ambiguous { code: failure.code }
            }
            Err(failure) => SendOutcome::transient(&failure),
        }
    }

    /// The newest captures first, optionally for one channel, at most
    /// `limit` (1 to [`MAX_CAPTURE_PAGE`]).
    pub async fn list(
        &self,
        channel: Option<Channel>,
        limit: u32,
        deadline: Instant,
    ) -> Result<Vec<Capture>, Failure> {
        let limit = i64::from(limit.clamp(1, MAX_CAPTURE_PAGE));
        let channel = channel.map(Channel::as_str);
        let client = self.database.acquire(deadline).await?;
        let rows = self
            .database
            .query(&client, &CAPTURE_LIST, &[&channel, &limit])
            .await
            .map_err(|e| classify(&e))?;
        rows.iter()
            .map(|row| {
                let delivery_id: String = row.get(1);
                let rendered: String = row.get(4);
                let captured_at: SystemTime = row.get(5);
                Ok(Capture {
                    id: row.get(0),
                    delivery_id: Uuid::parse_str(&delivery_id)
                        .map_err(|_| Failure::permanent("database_error"))?,
                    channel: row.get(2),
                    target: row.get(3),
                    rendered: serde_json::from_str(&rendered)
                        .map_err(|_| Failure::permanent("database_error"))?,
                    captured_at: jiff::Timestamp::try_from(captured_at)
                        .map_err(|_| Failure::permanent("database_error"))?,
                })
            })
            .collect()
    }
}

impl EmailDriver for CaptureDriver {
    fn provider(&self) -> Provider {
        Provider::Capture
    }
    fn send<'a>(
        &'a self,
        message: &'a EmailMessage,
        deadline: Instant,
    ) -> BoxFuture<'a, SendOutcome> {
        let mut rendered = crate::templates::Rendered::Email(message.content.clone()).to_json();
        if let Value::Object(map) = &mut rendered {
            map.insert("topic".into(), Value::String(message.topic.clone()));
            map.insert("to_name".into(), json!(message.to_name));
            map.insert("unsubscribe".into(), json!(message.unsubscribe));
        }
        Box::pin(self.capture(
            message.delivery_id,
            Channel::Email,
            message.to.expose(),
            Some(message.idempotency_key),
            rendered,
            deadline,
        ))
    }
}
impl SmsDriver for CaptureDriver {
    fn provider(&self) -> Provider {
        Provider::Capture
    }
    fn send<'a>(
        &'a self,
        message: &'a SmsMessage,
        deadline: Instant,
    ) -> BoxFuture<'a, SendOutcome> {
        let mut rendered = crate::templates::Rendered::Sms(message.content.clone()).to_json();
        if let Value::Object(map) = &mut rendered {
            map.insert("topic".into(), Value::String(message.topic.clone()));
            map.insert("marketing".into(), Value::Bool(message.marketing));
        }
        Box::pin(self.capture(
            message.delivery_id,
            Channel::Sms,
            message.to.expose(),
            None,
            rendered,
            deadline,
        ))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_capture_statements_are_registered_beside_the_kernel() {
        assert_eq!(
            merezha_postgres::check_registry(&[
                merezha_postgres::KERNEL_STATEMENTS,
                super::STATEMENTS
            ]),
            Ok(())
        );
    }
    /// No source of the block hands SQL to the driver.
    #[test]
    fn the_block_hands_no_sql_to_the_driver() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let found = merezha_postgres::statement::scan_sources(&dir, &[]).unwrap();
        assert!(found.is_empty(), "unregistered SQL: {found:?}");
    }
}
