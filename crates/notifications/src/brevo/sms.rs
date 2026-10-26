//! Brevo transactional SMS (NT §5.2, slice NT-N4): the request body and
//! the SMS driver over the shared [`BrevoClient`] and classifier.
//!
//! `POST /v3/transactionalSMS/send` carries one number, written without
//! its `+`, the configured `sender` (checked at startup), the rendered
//! content (already cut to `sms.max_segments` by the template registry,
//! with `organisation_prefix` counted), `type: "marketing"` for marketing
//! topics, `unicodeEnabled` when the content is outside GSM-7, the topic
//! and `mzd:<delivery_id>` as tags, and, with `public_base_url`, the
//! per-message status URL `webUrl` signed with `k_sms`
//! ([`crate::keys::Keys::sms_status_mac`]). Brevo documents no SMS
//! idempotency, so the worker never resends an ambiguous SMS (NT1); the
//! driver only classifies. A `duplicate_parameter` answer cannot be an
//! idempotency hit here and is a rejection.
use super::{BrevoClient, classify, classify_failure, events::CUSTOM_PREFIX};
use crate::{
    drivers::{Provider, SendOutcome, SmsDriver, SmsMessage},
    keys::Keys,
    templates::Encoding,
};
use merezha_platform::BoxFuture;
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// `POST /v3/transactionalSMS/send`; the deprecated `/v3/transactionalSMS/sms`
/// is not used.
pub const SMS_PATH: &str = "/v3/transactionalSMS/send";

/// The status URL Brevo calls for one delivery:
/// `{public_base_url}/v1/notifications/providers/brevo/sms/{id}/{mac}`.
pub fn status_url(public_base_url: &str, delivery: Uuid, keys: &Keys) -> String {
    format!(
        "{public_base_url}/v1/notifications{}/{delivery}/{}",
        super::sms_status::PREFIX,
        keys.sms_status_mac(delivery)
    )
}

/// The `POST /v3/transactionalSMS/send` body of one message (NT §5.2).
pub fn sms_body(
    message: &SmsMessage,
    sender: &str,
    organisation_prefix: Option<&str>,
    web_url: Option<&str>,
) -> Value {
    let number = message.to.expose();
    let mut body = json!({
        "sender": sender,
        "recipient": number.strip_prefix('+').unwrap_or(number),
        "content": message.content.text,
        "type": if message.marketing { "marketing" } else { "transactional" },
        "tag": [message.topic, format!("{CUSTOM_PREFIX}{}", message.delivery_id)],
        "unicodeEnabled": message.content.encoding == Encoding::Ucs2,
    });
    if let Some(prefix) = organisation_prefix {
        body["organisationPrefix"] = Value::String(prefix.to_owned());
    }
    if let Some(url) = web_url {
        body["webUrl"] = Value::String(url.to_owned());
    }
    body
}

impl BrevoClient {
    /// Send one SMS body (see [`sms_body`]) and classify the result.
    pub async fn send_sms(&self, body: &Value, deadline: Instant) -> SendOutcome {
        match self.post(SMS_PATH, "send_sms", body, deadline).await {
            Ok(response) => match classify(&response) {
                // No idempotency key was sent: a duplicate is a refusal.
                SendOutcome::Duplicate { .. } => SendOutcome::Permanent {
                    code: "provider_rejected".into(),
                },
                outcome => outcome,
            },
            Err(failure) => classify_failure(&failure),
        }
    }
}

/// The Brevo SMS driver (`sms.driver = "brevo"`). `Debug` shows no key.
#[derive(Clone)]
pub struct BrevoSms {
    client: BrevoClient,
    sender: String,
    organisation_prefix: Option<String>,
    public_base_url: Option<String>,
    keys: Keys,
}
impl std::fmt::Debug for BrevoSms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrevoSms")
            .field("client", &self.client)
            .field("sender", &self.sender)
            .finish_non_exhaustive()
    }
}
impl BrevoSms {
    /// The driver of `settings` on `client`; `keys` sign the status URL.
    pub fn new(client: BrevoClient, settings: &crate::Settings, keys: Keys) -> Self {
        Self {
            client,
            sender: settings.sms.sender.clone(),
            organisation_prefix: settings.sms.organisation_prefix.clone(),
            public_base_url: settings.public_base_url.clone(),
            keys,
        }
    }
    /// The body this driver sends for `message`.
    pub fn body(&self, message: &SmsMessage) -> Value {
        let web_url = self
            .public_base_url
            .as_deref()
            .map(|base| status_url(base, message.delivery_id, &self.keys));
        sms_body(
            message,
            &self.sender,
            self.organisation_prefix.as_deref(),
            web_url.as_deref(),
        )
    }
}
impl SmsDriver for BrevoSms {
    fn provider(&self) -> Provider {
        Provider::Brevo
    }
    fn timeout(&self) -> Option<Duration> {
        Some(self.client.timeout())
    }
    fn send<'a>(
        &'a self,
        message: &'a SmsMessage,
        deadline: Instant,
    ) -> BoxFuture<'a, SendOutcome> {
        let body = self.body(message);
        Box::pin(async move { self.client.send_sms(&body, deadline).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::templates::{RenderedSms, segments};
    use merezha_platform::ports::PhoneNumber;

    fn message(text: &str, marketing: bool) -> SmsMessage {
        let (encoding, parts) = segments(text);
        SmsMessage {
            delivery_id: Uuid::from_u128(9),
            topic: "tasks.code".into(),
            to: PhoneNumber::new("+33612345678").unwrap(),
            content: RenderedSms {
                text: text.into(),
                encoding,
                segments: parts,
            },
            marketing,
        }
    }

    #[test]
    fn the_body_has_the_number_without_plus_and_the_encoding_flag() {
        let keys = Keys::new(&crate::config::KeyMaterial::for_tests(vec![7; 32]), None);
        let url = status_url("https://api.acme.test", Uuid::from_u128(9), &keys);
        let body = sms_body(&message("Code 1234", false), "Acme", None, Some(&url));
        assert_eq!(
            body,
            json!({
                "sender": "Acme", "recipient": "33612345678", "content": "Code 1234",
                "type": "transactional", "unicodeEnabled": false,
                "tag": ["tasks.code", "mzd:00000000-0000-0000-0000-000000000009"],
                "webUrl": url,
            })
        );
        let mac = keys.sms_status_mac(Uuid::from_u128(9));
        assert_eq!(
            url,
            format!(
                "https://api.acme.test/v1/notifications/providers/brevo/sms/\
                 00000000-0000-0000-0000-000000000009/{mac}"
            )
        );
        let unicode = sms_body(&message("Код 1234", true), "Acme", Some("ACME"), None);
        assert_eq!(unicode["unicodeEnabled"], true);
        assert_eq!(unicode["type"], "marketing");
        assert_eq!(unicode["organisationPrefix"], "ACME");
        assert!(unicode.get("webUrl").is_none());
    }

    #[test]
    fn the_driver_debug_shows_no_key_or_number() {
        let settings = crate::config::BrevoSettings {
            api_key: Some(merezha_postgres::Secret::new("xkeysib-very-secret")),
            base_url: "https://api.brevo.com".into(),
            timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
            sandbox: false,
            webhook_token: None,
            webhook_allowed_cidrs: Vec::new(),
        };
        let client =
            BrevoClient::new(&settings, &merezha_platform::OutboundHttp::new().unwrap()).unwrap();
        let driver = BrevoSms {
            client,
            sender: "Acme".into(),
            organisation_prefix: None,
            public_base_url: None,
            keys: Keys::new(&crate::config::KeyMaterial::for_tests(vec![7; 32]), None),
        };
        let debug = format!("{driver:?} {:?}", message("x", false));
        assert!(
            !debug.contains("very-secret") && !debug.contains("612345678"),
            "{debug}"
        );
    }
}
