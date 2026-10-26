//! Brevo transactional email (NT §5.2, slice NT-N3): the client over the
//! kernel's outbound HTTP client, the response classifier, the email
//! driver, the webhook registration call and, in [`events`], the email
//! status callback.
//!
//! Every request goes to `brevo.base_url` with `api-key`, `accept` and
//! `content-type: application/json`, before a per-call deadline, and is
//! never retried here: the delivery worker owns retries and idempotency.
//! The API key lives in a [`Secret`] and never reaches a log, a span or a
//! `Debug` output. From Brevo's `{ code, message }` error body only `code`
//! is read; `message` is never stored or logged.
pub mod events;

use crate::{
    config::{BrevoSettings, Mailbox},
    drivers::{EmailDriver, EmailMessage, Provider, SendOutcome},
};
use merezha_domain::Failure;
use merezha_platform::{BoxFuture, OutboundFailure, OutboundHttp, OutboundResponse};
use merezha_postgres::Secret;
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use tracing::Instrument;

/// `POST /v3/smtp/email`.
pub const EMAIL_PATH: &str = "/v3/smtp/email";
/// `POST /v3/webhooks`.
pub const WEBHOOKS_PATH: &str = "/v3/webhooks";
/// Brevo's limit on `sender.name` and `to[].name`, in characters.
pub const MAX_NAME_CHARS: usize = 70;
/// Most bytes of a Brevo response the client reads.
const MAX_RESPONSE: usize = 64 * 1024;
/// The class minimum of `duplicate_request` (NT §5.2).
pub const DUPLICATE_REQUEST_DELAY: Duration = Duration::from_secs(60);
/// The class minimum of an account problem (NT §5.2).
pub const ACCOUNT_DELAY: Duration = Duration::from_secs(300);
/// The longest provider-asked delay the client passes on.
pub const MAX_PROVIDER_DELAY: Duration = Duration::from_secs(3600);
/// The transient code of an account problem (key, credits, permission).
pub const PROVIDER_ACCOUNT: &str = "provider_account";
/// The transient code of a Brevo 5xx.
pub const PROVIDER_UNAVAILABLE: &str = "provider_unavailable";
/// The events `brevo register-webhook` subscribes to (NT §5.2). Opens and
/// clicks are not among them: the block tracks no engagement (NT3).
pub const WEBHOOK_EVENTS: [&str; 8] = [
    "delivered",
    "hardBounce",
    "softBounce",
    "blocked",
    "spam",
    "invalid",
    "deferred",
    "unsubscribed",
];

/// Brevo error codes that get their own `provider_rejected_<code>` reason;
/// any other rejection is `provider_rejected` (a closed set, NT §5.2).
const REJECTED_CODES: [&str; 5] = [
    "invalid_parameter",
    "missing_parameter",
    "out_of_range",
    "document_not_found",
    "not_acceptable",
];
/// Brevo error codes of an account problem.
const ACCOUNT_CODES: [&str; 4] = [
    "unauthorized",
    "not_enough_credits",
    "permission_denied",
    "account_under_validation",
];

/// The Brevo API client. Cheap to clone; `Debug` never shows the key.
#[derive(Clone)]
pub struct BrevoClient {
    http: OutboundHttp,
    base_url: String,
    /// The host of `base_url`, the `server.address` of outbound spans.
    host: String,
    api_key: Secret,
    timeout: Duration,
}
impl std::fmt::Debug for BrevoClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrevoClient")
            .field("base_url", &self.base_url)
            .field("api_key", &self.api_key)
            .finish_non_exhaustive()
    }
}
impl BrevoClient {
    /// The client of `[modules.notifications.brevo]`: the kernel's
    /// outbound client `http` when `connect_timeout` is its default,
    /// otherwise the same client with the configured connect timeout.
    pub fn new(settings: &BrevoSettings, http: &OutboundHttp) -> Result<Self, Failure> {
        let api_key = settings
            .api_key
            .clone()
            .ok_or_else(|| Failure::permanent("invalid_config"))?;
        let http = if settings.connect_timeout == OutboundHttp::CONNECT_TIMEOUT {
            http.clone()
        } else {
            OutboundHttp::with_connect_timeout(settings.connect_timeout)?
        };
        let host = url::Url::parse(&settings.base_url)
            .ok()
            .and_then(|u| u.host_str().map(str::to_owned))
            .unwrap_or_default();
        Ok(Self {
            http,
            base_url: settings.base_url.clone(),
            host,
            api_key,
            timeout: settings.timeout,
        })
    }
    /// `brevo.timeout`, the most one call may take.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// One JSON `POST` to `path` before `min(deadline, now + timeout)`,
    /// inside an outbound span (origin only, never the full URL).
    async fn post(
        &self,
        path: &'static str,
        operation: &'static str,
        body: &Value,
        deadline: Instant,
    ) -> Result<OutboundResponse, OutboundFailure> {
        let span = tracing::info_span!(
            target: merezha_telemetry::TARGET,
            "brevo call",
            otel.name = format!("POST {path}").as_str(),
            otel.kind = "client",
            http.request.method = "POST",
            server.address = self.host.as_str(),
            http.response.status_code = tracing::field::Empty,
            merezha.provider = "brevo",
            merezha.provider.operation = operation,
        );
        let deadline = deadline.min(Instant::now() + self.timeout);
        let request = self
            .http
            .client()
            .post(format!("{}{path}", self.base_url))
            .header(
                "api-key",
                api_key_header(&self.api_key).map_err(|failure| OutboundFailure {
                    failure,
                    maybe_sent: false,
                })?,
            )
            .header("accept", "application/json")
            .header("content-type", "application/json")
            .body(body.to_string())
            .build()
            .map_err(|_| OutboundFailure {
                failure: Failure::permanent("invalid_request"),
                maybe_sent: false,
            })?;
        async {
            // Brevo is a third party: no `traceparent` (TE §2.1).
            let result = self
                .http
                .exchange(request, deadline, MAX_RESPONSE, None)
                .await;
            if let Ok(response) = &result {
                tracing::Span::current()
                    .record("http.response.status_code", response.status.as_u16());
            }
            result
        }
        .instrument(span)
        .await
    }

    /// Send one email body (see [`email_body`]) and classify the result.
    pub async fn send_email(&self, body: &Value, deadline: Instant) -> SendOutcome {
        match self.post(EMAIL_PATH, "send_email", body, deadline).await {
            Ok(response) => classify(&response),
            Err(failure) => classify_failure(&failure),
        }
    }

    /// `POST /v3/webhooks`: the account-level transactional webhook to
    /// `url`, authenticated by `Authorization: Bearer <token>`, for
    /// [`WEBHOOK_EVENTS`]. Returns Brevo's webhook id. Failures are
    /// `provider_account` (key or permission), `provider_rejected[_code]`,
    /// `provider_unavailable` or the transport's code.
    pub async fn register_webhook(
        &self,
        url: &str,
        token: &Secret,
        deadline: Instant,
    ) -> Result<i64, Failure> {
        let body = json!({
            "type": "transactional",
            "url": url,
            "description": "merezha notifications: email status",
            "events": WEBHOOK_EVENTS,
            "auth": { "type": "bearer", "token": token.expose() },
        });
        let response = self
            .post(WEBHOOKS_PATH, "register_webhook", &body, deadline)
            .await
            .map_err(|e| e.failure)?;
        match classify(&response) {
            SendOutcome::Accepted { .. } => serde_json::from_slice::<Value>(&response.body)
                .ok()
                .and_then(|v| v.get("id").and_then(Value::as_i64))
                .ok_or_else(|| Failure::transient("provider_invalid_response")),
            SendOutcome::Permanent { code } => Err(Failure::permanent(&code)),
            SendOutcome::Duplicate { .. } => Err(Failure::permanent("provider_rejected")),
            SendOutcome::Transient { code, .. } | SendOutcome::Ambiguous { code } => {
                Err(Failure::transient(&code))
            }
            SendOutcome::RateLimited { .. } => Err(Failure::transient("rate_limited")),
        }
    }
}

/// The failure code of a registration without a Brevo email driver.
pub const NOT_CONFIGURED: &str = "brevo_not_configured";

/// The callback's full path, which a registered webhook URL must have.
pub const CALLBACK_PATH: &str = "/v1/notifications/providers/brevo/email";

/// What `register-webhook` created.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Registered {
    /// Brevo's webhook id.
    pub id: i64,
    pub url: String,
    /// `host_override` when `--allow-host` let a host other than
    /// `public_base_url`'s through.
    pub warnings: Vec<&'static str>,
}

/// `merezha notifications brevo register-webhook`: create the account-level
/// webhook of `settings` (a Brevo email driver with its token) to `url`,
/// or by default to `public_base_url` + [`CALLBACK_PATH`].
///
/// A given `url` must have the callback path, no query, and the host and
/// port of `public_base_url`, so the bearer token is never handed to
/// another server by a typo; `allow_host` lets another host through with
/// the `host_override` warning. Fails permanent `brevo_not_configured`,
/// `missing_public_base_url`, `invalid_url`, `callback_path_mismatch` or
/// `callback_host_mismatch`, or with [`BrevoClient::register_webhook`]'s
/// codes.
pub async fn register_webhook(
    settings: &crate::Settings,
    url: Option<&str>,
    allow_host: bool,
    http: &OutboundHttp,
    deadline: Instant,
) -> Result<Registered, Failure> {
    let (Some(brevo), crate::config::Driver::Brevo) = (&settings.brevo, settings.email.driver)
    else {
        return Err(Failure::permanent(NOT_CONFIGURED));
    };
    let Some(token) = &brevo.webhook_token else {
        return Err(Failure::permanent(NOT_CONFIGURED));
    };
    let url = match (url, &settings.public_base_url) {
        (Some(url), _) => url.to_owned(),
        (None, Some(base)) => format!("{base}{CALLBACK_PATH}"),
        (None, None) => return Err(Failure::permanent("missing_public_base_url")),
    };
    let warnings = check_callback_url(&url, settings.public_base_url.as_deref(), allow_host)?;
    let client = BrevoClient::new(brevo, http)?;
    let id = client.register_webhook(&url, token, deadline).await?;
    Ok(Registered { id, url, warnings })
}

/// The checks of [`register_webhook`] on its URL.
pub fn check_callback_url(
    url: &str,
    public_base_url: Option<&str>,
    allow_host: bool,
) -> Result<Vec<&'static str>, Failure> {
    if !callback_url_allowed(url) {
        return Err(Failure::permanent("invalid_url"));
    }
    let parsed = url::Url::parse(url).map_err(|_| Failure::permanent("invalid_url"))?;
    if parsed.path() != CALLBACK_PATH || parsed.query().is_some() {
        return Err(Failure::permanent("callback_path_mismatch"));
    }
    let origin = |u: &url::Url| {
        (
            u.host_str().map(str::to_ascii_lowercase),
            u.port_or_known_default(),
        )
    };
    let same_host = public_base_url
        .and_then(|base| url::Url::parse(base).ok())
        .is_some_and(|base| origin(&base) == origin(&parsed));
    match (same_host, allow_host) {
        (true, _) => Ok(Vec::new()),
        (false, true) => Ok(vec!["host_override"]),
        (false, false) => Err(Failure::permanent("callback_host_mismatch")),
    }
}

/// The `api-key` header value, marked sensitive so the HTTP stack never
/// shows it in a `Debug` output or caches it for header compression.
pub fn api_key_header(key: &Secret) -> Result<axum::http::HeaderValue, Failure> {
    let mut value = axum::http::HeaderValue::from_str(key.expose())
        .map_err(|_| Failure::permanent("invalid_config"))?;
    value.set_sensitive(true);
    Ok(value)
}

/// Whether `text` may be the callback URL Brevo calls: `https`, or `http`
/// to loopback (development), without credentials or fragment, at most
/// 2,048 bytes.
pub fn callback_url_allowed(text: &str) -> bool {
    let Ok(url) = url::Url::parse(text) else {
        return false;
    };
    let loopback = match url.host() {
        Some(url::Host::Domain(host)) => host == "localhost",
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => return false,
    };
    (url.scheme() == "https" || (url.scheme() == "http" && loopback))
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
        && text.len() <= 2048
}

/// Cut `name` to Brevo's 70 characters, at a character boundary.
pub fn cut_name(name: &str) -> String {
    name.chars().take(MAX_NAME_CHARS).collect()
}

fn mailbox(mailbox: &Mailbox) -> Value {
    let mut value = json!({ "email": mailbox.address });
    if let Some(name) = mailbox.name.as_deref().filter(|n| !n.is_empty()) {
        value["name"] = Value::String(cut_name(name));
    }
    value
}

/// The `POST /v3/smtp/email` body of one message (NT §5.2): exactly one
/// recipient, the topic as tag, `idempotencyKey` and `X-Mailin-custom` in
/// `headers`, the RFC 8058 `List-Unsubscribe` pair when the message has an
/// unsubscribe link (never for security topics), and `X-Sib-Sandbox:
/// drop` in sandbox mode.
pub fn email_body(
    message: &EmailMessage,
    from: &Mailbox,
    reply_to: Option<&Mailbox>,
    sandbox: bool,
) -> Value {
    let mut to = json!({ "email": message.to.expose() });
    if let Some(name) = message.to_name.as_deref().filter(|n| !n.is_empty()) {
        to["name"] = Value::String(cut_name(name));
    }
    let mut headers = serde_json::Map::new();
    headers.insert(
        "idempotencyKey".into(),
        Value::String(message.idempotency_key.to_string()),
    );
    headers.insert(
        "X-Mailin-custom".into(),
        Value::String(format!("{}{}", events::CUSTOM_PREFIX, message.delivery_id)),
    );
    if let Some(link) = &message.unsubscribe {
        headers.insert(
            "List-Unsubscribe".into(),
            Value::String(format!("<{link}>")),
        );
        headers.insert(
            "List-Unsubscribe-Post".into(),
            Value::String("List-Unsubscribe=One-Click".into()),
        );
    }
    if sandbox {
        headers.insert("X-Sib-Sandbox".into(), Value::String("drop".into()));
    }
    let mut body = json!({
        "sender": mailbox(from),
        "to": [to],
        "subject": message.content.subject,
        "textContent": message.content.text,
        "tags": [message.topic],
        "headers": headers,
    });
    if let Some(html) = &message.content.html {
        body["htmlContent"] = Value::String(html.clone());
    }
    if let Some(reply_to) = reply_to {
        body["replyTo"] = mailbox(reply_to);
    }
    body
}

/// A delay header in whole seconds (`Retry-After`, `x-sib-ratelimit-reset`).
fn seconds(response: &OutboundResponse, name: &str) -> Option<Duration> {
    response
        .headers
        .get(name)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}
/// `max(Retry-After, x-sib-ratelimit-reset)`, at most an hour.
fn provider_delay(response: &OutboundResponse) -> Option<Duration> {
    let asked = seconds(response, "retry-after").max(seconds(response, "x-sib-ratelimit-reset"));
    asked.map(|d| d.min(MAX_PROVIDER_DELAY))
}
fn at_least(response: &OutboundResponse, floor: Duration) -> Option<Duration> {
    Some(provider_delay(response).map_or(floor, |d| d.max(floor)))
}

/// Brevo's error `code`, lower snake case and short, or `None`. The
/// `message` beside it is never read.
fn error_code(body: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(body).ok()?;
    let code = value.get("code")?.as_str()?;
    (code.len() <= 40
        && !code.is_empty()
        && code.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'))
    .then(|| code.to_owned())
}

/// The class of one Brevo response (NT §5.2 classification table).
pub fn classify(response: &OutboundResponse) -> SendOutcome {
    let status = response.status.as_u16();
    let code = error_code(&response.body);
    let code = code.as_deref();
    match status {
        200..=299 => SendOutcome::Accepted {
            provider: Provider::Brevo,
            message_id: serde_json::from_slice::<Value>(&response.body)
                .ok()
                .and_then(|v| match v.get("messageId") {
                    Some(Value::String(id)) => Some(id.clone()),
                    Some(Value::Number(id)) => Some(id.to_string()),
                    _ => None,
                })
                .filter(|id| !id.is_empty() && id.len() <= 256),
        },
        400 if code == Some("duplicate_parameter") => SendOutcome::Duplicate {
            provider: Provider::Brevo,
        },
        400 if code == Some("duplicate_request") => SendOutcome::Transient {
            code: "duplicate_request".into(),
            retry_after: at_least(response, DUPLICATE_REQUEST_DELAY),
        },
        401..=403 => account(response),
        _ if code.is_some_and(|c| ACCOUNT_CODES.contains(&c)) => account(response),
        408 | 425 | 429 => SendOutcome::RateLimited {
            retry_after: provider_delay(response),
        },
        400..=499 => SendOutcome::Permanent {
            code: match code {
                Some(code) if REJECTED_CODES.contains(&code) => {
                    format!("provider_rejected_{code}")
                }
                _ => "provider_rejected".into(),
            },
        },
        _ => SendOutcome::Transient {
            code: PROVIDER_UNAVAILABLE.into(),
            retry_after: provider_delay(response),
        },
    }
}
fn account(response: &OutboundResponse) -> SendOutcome {
    SendOutcome::Transient {
        code: PROVIDER_ACCOUNT.into(),
        retry_after: at_least(response, ACCOUNT_DELAY),
    }
}

/// The class of a failed exchange: transient before the request could
/// reach Brevo, ambiguous once it may have (the email worker resends with
/// the same `idempotencyKey`; SMS never resends, NT1).
pub fn classify_failure(failure: &OutboundFailure) -> SendOutcome {
    if failure.maybe_sent {
        SendOutcome::Ambiguous {
            code: failure.failure.code.clone(),
        }
    } else {
        SendOutcome::Transient {
            code: failure.failure.code.clone(),
            retry_after: None,
        }
    }
}

/// The Brevo email driver (`email.driver = "brevo"`).
#[derive(Clone, Debug)]
pub struct BrevoEmail {
    client: BrevoClient,
    from: Mailbox,
    reply_to: Option<Mailbox>,
    sandbox: bool,
}
impl BrevoEmail {
    pub fn new(
        client: BrevoClient,
        from: Mailbox,
        reply_to: Option<Mailbox>,
        sandbox: bool,
    ) -> Self {
        Self {
            client,
            from,
            reply_to,
            sandbox,
        }
    }
}
impl EmailDriver for BrevoEmail {
    fn provider(&self) -> Provider {
        Provider::Brevo
    }
    fn timeout(&self) -> Option<Duration> {
        Some(self.client.timeout())
    }
    fn send<'a>(
        &'a self,
        message: &'a EmailMessage,
        deadline: Instant,
    ) -> BoxFuture<'a, SendOutcome> {
        let body = email_body(message, &self.from, self.reply_to.as_ref(), self.sandbox);
        Box::pin(async move { self.client.send_email(&body, deadline).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, StatusCode};

    fn response(status: u16, headers: &[(&'static str, &str)], body: &str) -> OutboundResponse {
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            map.insert(*name, value.parse().unwrap());
        }
        OutboundResponse {
            status: StatusCode::from_u16(status).unwrap(),
            headers: map,
            body: body.as_bytes().to_vec(),
        }
    }
    fn transient(code: &str, after: Option<u64>) -> SendOutcome {
        SendOutcome::Transient {
            code: code.into(),
            retry_after: after.map(Duration::from_secs),
        }
    }

    #[test]
    fn responses_are_classified_as_the_design_table_says() {
        let accepted = |id: Option<&str>| SendOutcome::Accepted {
            provider: Provider::Brevo,
            message_id: id.map(str::to_owned),
        };
        let cases = [
            (
                response(201, &[], r#"{"messageId":"<m1@relay>"}"#),
                accepted(Some("<m1@relay>")),
            ),
            (response(202, &[], ""), accepted(None)),
            (
                response(400, &[], r#"{"code":"duplicate_parameter","message":"x"}"#),
                SendOutcome::Duplicate {
                    provider: Provider::Brevo,
                },
            ),
            (
                response(400, &[], r#"{"code":"duplicate_request"}"#),
                transient("duplicate_request", Some(60)),
            ),
            (
                response(400, &[], r#"{"code":"invalid_parameter"}"#),
                SendOutcome::Permanent {
                    code: "provider_rejected_invalid_parameter".into(),
                },
            ),
            (
                response(400, &[], r#"{"code":"something_new"}"#),
                SendOutcome::Permanent {
                    code: "provider_rejected".into(),
                },
            ),
            (
                response(404, &[], "not json"),
                SendOutcome::Permanent {
                    code: "provider_rejected".into(),
                },
            ),
            (
                response(401, &[], r#"{"code":"unauthorized"}"#),
                transient("provider_account", Some(300)),
            ),
            (
                response(402, &[], ""),
                transient("provider_account", Some(300)),
            ),
            (
                response(400, &[], r#"{"code":"not_enough_credits"}"#),
                transient("provider_account", Some(300)),
            ),
            (
                response(403, &[("retry-after", "900")], ""),
                transient("provider_account", Some(900)),
            ),
            (
                response(429, &[("x-sib-ratelimit-reset", "7")], ""),
                SendOutcome::RateLimited {
                    retry_after: Some(Duration::from_secs(7)),
                },
            ),
            (
                response(
                    429,
                    &[("retry-after", "9"), ("x-sib-ratelimit-reset", "3")],
                    "",
                ),
                SendOutcome::RateLimited {
                    retry_after: Some(Duration::from_secs(9)),
                },
            ),
            (
                response(408, &[], ""),
                SendOutcome::RateLimited { retry_after: None },
            ),
            (
                response(425, &[("retry-after", "99999")], ""),
                SendOutcome::RateLimited {
                    retry_after: Some(MAX_PROVIDER_DELAY),
                },
            ),
            (
                response(503, &[], ""),
                transient("provider_unavailable", None),
            ),
            (
                response(500, &[("retry-after", "2")], r#"{"code":"internal_error"}"#),
                transient("provider_unavailable", Some(2)),
            ),
        ];
        for (response, want) in cases {
            assert_eq!(classify(&response), want, "{}", response.status);
        }
    }

    #[test]
    fn transport_failures_are_ambiguous_only_once_maybe_sent() {
        let failure = |maybe_sent| OutboundFailure {
            failure: Failure::transient("upstream_timeout"),
            maybe_sent,
        };
        assert_eq!(
            classify_failure(&failure(false)),
            transient("upstream_timeout", None)
        );
        assert_eq!(
            classify_failure(&failure(true)),
            SendOutcome::Ambiguous {
                code: "upstream_timeout".into()
            }
        );
    }

    #[test]
    fn the_error_message_is_never_read() {
        let response = response(
            400,
            &[],
            r#"{"code":"Invalid Code ada@example.com","message":"ada@example.com"}"#,
        );
        assert_eq!(
            classify(&response),
            SendOutcome::Permanent {
                code: "provider_rejected".into()
            }
        );
        assert_eq!(cut_name(&"é".repeat(90)).chars().count(), 70);
    }

    #[test]
    fn a_callback_url_needs_the_path_and_the_public_host_unless_overridden() {
        let base = Some("https://api.acme.test");
        let ok = "https://api.acme.test/v1/notifications/providers/brevo/email";
        assert_eq!(check_callback_url(ok, base, false), Ok(Vec::new()));
        let code = |url: &str, base: Option<&str>, allow: bool| {
            check_callback_url(url, base, allow).map_err(|f| f.code)
        };
        assert_eq!(
            code("https://api.acme.test/v1/notifications/other", base, true),
            Err("callback_path_mismatch".into())
        );
        assert_eq!(
            code(&format!("{ok}?x=1"), base, true),
            Err("callback_path_mismatch".into())
        );
        let elsewhere = "https://evil.test/v1/notifications/providers/brevo/email";
        assert_eq!(
            code(elsewhere, base, false),
            Err("callback_host_mismatch".into())
        );
        assert_eq!(
            code(
                "https://api.acme.test:8443/v1/notifications/providers/brevo/email",
                base,
                false
            ),
            Err("callback_host_mismatch".into())
        );
        assert_eq!(
            code(elsewhere, None, false),
            Err("callback_host_mismatch".into())
        );
        assert_eq!(code(elsewhere, base, true), Ok(vec!["host_override"]));
        assert_eq!(
            code(
                "http://evil.test/v1/notifications/providers/brevo/email",
                base,
                true
            ),
            Err("invalid_url".into())
        );
    }

    #[test]
    fn the_client_debug_redacts_the_key() {
        let settings = BrevoSettings {
            api_key: Some(Secret::new("xkeysib-very-secret")),
            base_url: "https://api.brevo.com".into(),
            timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
            sandbox: false,
            webhook_token: None,
            webhook_allowed_cidrs: Vec::new(),
        };
        let client = BrevoClient::new(&settings, &OutboundHttp::new().unwrap()).unwrap();
        let debug = format!("{client:?}");
        assert!(!debug.contains("very-secret"), "{debug}");
        let header = api_key_header(settings.api_key.as_ref().unwrap()).unwrap();
        assert!(header.is_sensitive());
        assert!(!format!("{header:?}").contains("very-secret"));
        assert_eq!(client.host, "api.brevo.com");
    }
}
