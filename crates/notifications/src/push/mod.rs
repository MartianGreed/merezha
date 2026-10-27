//! Web Push (NT §5.3, slice NT-N6): VAPID keys, subscriptions, the push
//! request and its classification.
//!
//! - **Keys.** [`Vapid`] holds the P-256 key of `push.vapid_private_key`
//!   (a PKCS#8 PEM or a base64url 32-byte scalar), its public key and
//!   `key_id`, the first 16 hex of SHA-256 of the uncompressed public key.
//!   The VAPID JWT (RFC 8292) is signed for 12 h with `aud` the endpoint's
//!   origin and cached per origin for an hour, so `exp` is never more than
//!   12 h out (the RFC caps it at 24 h). [`generate`] backs `merezha
//!   notifications vapid generate`.
//! - **Endpoints.** [`check_endpoint`] is the strict host allowlist, applied
//!   at subscribe and again at send: `https` on port 443, a host equal to or
//!   a dot-suffix of a `push.allowed_hosts` entry, no userinfo, no
//!   fragment. With `egress.allow_private_networks` (development and tests
//!   only, refused with `http.public = true`) `http` and other ports pass
//!   too; the host allowlist still applies.
//! - **Sending.** [`PushSender`] encrypts the payload with RFC 8291
//!   `aes128gcm` through `web-push-native`, signs the request, and sends it
//!   through the egress-guarded client ([`crate::egress`]) with `TTL`,
//!   `Urgency` and, with a collapse key, `Topic`. The payload is the JSON of
//!   NT §5.3, at most [`MAX_PUSH_PAYLOAD_BYTES`]: the body is cut at a
//!   character boundary when needed.
//! - **Outcomes.** 201 (any 2xx) is accepted with `Location` as the message
//!   id; 404 and 410 fail the delivery `subscription_gone` and delete the
//!   subscription in Tx B; 400 and 413 are permanent; 401 and 403 are
//!   transient `push_auth` (logged at error level) for at most 3 attempts;
//!   429 waits for `Retry-After`; 5xx and network errors are retried. A
//!   destination the egress guard refuses is permanent `egress_blocked`.
//!
//! Endpoints, `p256dh`, `auth`, payloads and the VAPID key never reach a
//! log, a span or a `Debug` output.
pub(crate) mod routes;
pub mod sql;

use crate::{
    config::{PushSettings, Settings},
    drivers::{Provider, SendOutcome},
    egress::{self, EgressPolicy, Lookup},
    metrics::Metrics,
    templates::{MAX_PUSH_PAYLOAD_BYTES, RenderedPush},
};
use base64::Engine;
use merezha_domain::Failure;
use merezha_postgres::{Database, Session, Statement, classify};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use uuid::Uuid;
use web_push_native::jwt_simple::{
    algorithms::{ECDSAP256KeyPairLike, ECDSAP256PublicKeyLike, ES256KeyPair},
    claims::Claims,
};

/// The delivery failure and removal reason of a subscription the push
/// service no longer knows (404 or 410).
pub const SUBSCRIPTION_GONE: &str = "subscription_gone";
/// 413 from the push service, or a payload that does not fit
/// [`MAX_PUSH_PAYLOAD_BYTES`] even with an empty body and no tag.
pub const PAYLOAD_TOO_LARGE: &str = "push_payload_too_large";
/// 401 or 403 from the push service: the VAPID key or JWT was refused.
pub const PUSH_AUTH: &str = "push_auth";
/// `push_auth` is retried this many attempts at most (NT §5.3).
pub const PUSH_AUTH_MAX_ATTEMPTS: u32 = 3;
/// A subscription endpoint outside the allowlist, at subscribe or send.
pub const HOST_NOT_ALLOWED: &str = "push_host_not_allowed";
/// A malformed subscription.
pub const SUBSCRIPTION_INVALID: &str = "subscription_invalid";
/// `GET push/vapid-public-key` and the subscription routes without a
/// `[modules.notifications.push]` section.
pub const PUSH_DISABLED: &str = "push_disabled";
/// Lifetime of a signed VAPID JWT.
pub const JWT_LIFETIME: Duration = Duration::from_secs(12 * 3600);
/// A cached JWT is reused for at most this long, so it always has more than
/// 11 h left.
pub const JWT_REUSE: Duration = Duration::from_secs(3600);
/// Origins whose JWT is cached; the cache is cleared when full.
const MAX_CACHED_ORIGINS: usize = 256;
/// The deadline of one push request without a smaller job deadline.
pub const PUSH_TIMEOUT: Duration = Duration::from_secs(10);
/// Most bytes of an endpoint URL.
pub const MAX_ENDPOINT: usize = 2048;
/// Most characters of a `Location` header kept as the message id.
const MAX_MESSAGE_ID: usize = 256;
/// The longest `Retry-After` honoured; the sender also caps it at
/// `push.ttl`, past which the push service would drop the message anyway.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(86_400);
/// `push_expire`: consecutive failures and the success window after which
/// a subscription is deleted (NT §5.3).
pub const MAX_CONSECUTIVE_FAILURES: i32 = 10;
pub const FAILURE_WINDOW: Duration = Duration::from_secs(30 * 86_400);
/// `push_expire` deletes a subscription of another VAPID key only once it
/// has gone unused this long. During a rolling key rotation the instances
/// on the old key and those on the new one each refresh `last_used_at` of
/// the subscriptions they target, so neither deletes the other's; once
/// every instance runs the new key, the old subscriptions stop being used
/// and go after this grace.
pub const KEY_CHANGE_GRACE: Duration = Duration::from_secs(7 * 86_400);

fn b64url(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}
/// base64url with or without padding, as browsers and libraries emit it.
pub(crate) fn b64url_decode(text: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(text.trim_end_matches('='))
        .ok()
}

/// The VAPID key of `push.vapid_private_key`: a PKCS#8 PEM, or base64url
/// of the 32-byte P-256 scalar. `None` when it is neither, or not a valid
/// scalar.
pub fn parse_private_key(text: &str) -> Option<ES256KeyPair> {
    let text = text.trim();
    if text.starts_with("-----BEGIN PRIVATE KEY-----") {
        if !text.ends_with("-----END PRIVATE KEY-----") {
            return None;
        }
        return ES256KeyPair::from_pem(text).ok();
    }
    let bytes = b64url_decode(text)?;
    if bytes.len() != 32 {
        return None;
    }
    ES256KeyPair::from_bytes(&bytes).ok()
}

/// `key_id`: the first 16 hex of SHA-256 of the uncompressed public key.
pub fn key_id(public_key: &[u8]) -> String {
    let digest = Sha256::digest(public_key);
    let mut out = String::with_capacity(16);
    for byte in &digest[..8] {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// A new VAPID key pair, as `merezha notifications vapid generate` prints
/// it. `Debug` shows the public half only.
pub struct GeneratedKeys {
    /// base64url of the 32-byte scalar: the value of the secret.
    pub private_key: String,
    /// The same key as a PKCS#8 PEM.
    pub private_key_pem: String,
    /// base64url of the 65-byte uncompressed point (the browser's
    /// `applicationServerKey`).
    pub public_key: String,
    pub key_id: String,
}
impl std::fmt::Debug for GeneratedKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GeneratedKeys")
            .field("public_key", &self.public_key)
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}
/// Generate a VAPID key pair from the operating system's randomness.
pub fn generate() -> Result<GeneratedKeys, Failure> {
    let key = ES256KeyPair::generate();
    let public = key.public_key().public_key().to_bytes_uncompressed();
    Ok(GeneratedKeys {
        private_key: b64url(&key.to_bytes()),
        private_key_pem: key
            .to_pem()
            .map_err(|_| Failure::permanent("vapid_key_encoding"))?,
        public_key: b64url(&public),
        key_id: key_id(&public),
    })
}

/// The application server's VAPID identity (RFC 8292). `Debug` shows the
/// key id only.
pub struct Vapid {
    key: ES256KeyPair,
    public_key: Vec<u8>,
    key_id: String,
    subject: String,
    /// Signed JWTs by origin, with the instant they were signed.
    tokens: Mutex<HashMap<String, (String, Instant)>>,
}
impl std::fmt::Debug for Vapid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vapid")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}
impl Vapid {
    pub fn new(push: &PushSettings) -> Result<Self, Failure> {
        let key = parse_private_key(push.vapid_private_key.expose())
            .ok_or_else(|| Failure::permanent("invalid_vapid_key"))?;
        let public_key = key.public_key().public_key().to_bytes_uncompressed();
        Ok(Self {
            key_id: key_id(&public_key),
            key,
            public_key,
            subject: push.vapid_subject.clone(),
            tokens: Mutex::new(HashMap::new()),
        })
    }
    /// base64url of the uncompressed public key.
    pub fn public_key(&self) -> String {
        b64url(&self.public_key)
    }
    pub fn key_id(&self) -> &str {
        &self.key_id
    }
    /// The `Authorization: vapid t=<jwt>, k=<public key>` value for a push
    /// service `origin` (`https://fcm.googleapis.com`).
    pub fn authorization(&self, origin: &str) -> Result<String, Failure> {
        let now = Instant::now();
        if let Ok(tokens) = self.tokens.lock()
            && let Some((token, signed)) = tokens.get(origin)
            && now.duration_since(*signed) < JWT_REUSE
        {
            return Ok(format!("vapid t={token}, k={}", self.public_key()));
        }
        let claims = Claims::create(JWT_LIFETIME.into())
            .with_audience(origin)
            .with_subject(&self.subject);
        let token = self
            .key
            .sign(claims)
            .map_err(|_| Failure::permanent("vapid_sign_failed"))?;
        if let Ok(mut tokens) = self.tokens.lock() {
            if tokens.len() >= MAX_CACHED_ORIGINS {
                tokens.clear();
            }
            tokens.insert(origin.to_owned(), (token.clone(), now));
        }
        Ok(format!("vapid t={token}, k={}", self.public_key()))
    }
}

/// An endpoint that passed [`check_endpoint`].
#[derive(Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub url: url::Url,
    /// `https://fcm.googleapis.com`: the JWT audience and the only part of
    /// the endpoint the API shows.
    pub origin: String,
}
impl std::fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Endpoint")
            .field("origin", &self.origin)
            .finish_non_exhaustive()
    }
}

/// The strict allowlist of NT §5.3: `Err` is [`SUBSCRIPTION_INVALID`] for
/// a malformed endpoint (including a host name that is not a valid DNS
/// name) and [`HOST_NOT_ALLOWED`] for a scheme, port or host
/// outside the rule. `development` is `egress.allow_private_networks`: it
/// also lets `http` and other ports through.
pub fn check_endpoint(
    text: &str,
    allowed_hosts: &[String],
    development: bool,
) -> Result<Endpoint, &'static str> {
    if text.is_empty() || text.len() > MAX_ENDPOINT {
        return Err(SUBSCRIPTION_INVALID);
    }
    let url = url::Url::parse(text).map_err(|_| SUBSCRIPTION_INVALID)?;
    if url.as_str().len() > MAX_ENDPOINT {
        return Err(SUBSCRIPTION_INVALID);
    }
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err(SUBSCRIPTION_INVALID);
    }
    let Some(host) = url.host() else {
        return Err(SUBSCRIPTION_INVALID);
    };
    if let url::Host::Domain(name) = &host
        && !valid_host(name)
    {
        return Err(SUBSCRIPTION_INVALID);
    }
    let secure = url.scheme() == "https" && url.port().is_none_or(|p| p == 443);
    let relaxed = development && matches!(url.scheme(), "https" | "http");
    if !secure && !relaxed {
        return Err(HOST_NOT_ALLOWED);
    }
    let allowed = match &host {
        url::Host::Domain(name) => allowed_hosts.iter().any(|entry| {
            *name == entry.as_str()
                || name
                    .strip_suffix(entry.as_str())
                    .is_some_and(|rest| rest.ends_with('.') && rest.len() > 1)
        }),
        // An address literal only matches an entry naming that address.
        url::Host::Ipv4(v4) => allowed_hosts.iter().any(|e| *e == v4.to_string()),
        url::Host::Ipv6(_) => false,
    };
    if !allowed {
        return Err(HOST_NOT_ALLOWED);
    }
    let origin = url.origin().ascii_serialization();
    Ok(Endpoint { url, origin })
}

/// A DNS host name: at most 253 bytes, labels of 1 to 63 bytes (so no
/// empty label, as in `a..b` or a trailing dot).
fn valid_host(name: &str) -> bool {
    name.len() <= MAX_HOST
        && name
            .split('.')
            .all(|label| (1..=MAX_LABEL).contains(&label.len()))
}
/// Most bytes of a host name, and of one of its labels (RFC 1035).
const MAX_HOST: usize = 253;
const MAX_LABEL: usize = 63;

/// The subscription and notification fields of one push delivery, read in
/// Tx A. `Debug` shows the subscription id only.
#[derive(Clone)]
pub struct PushTarget {
    pub subscription: Uuid,
    pub endpoint: String,
    pub p256dh: Vec<u8>,
    pub auth: Vec<u8>,
    pub collapse_key: Option<String>,
    pub priority: String,
    /// The VAPID key id the subscription was made with.
    pub key_id: String,
}
impl std::fmt::Debug for PushTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PushTarget")
            .field("subscription", &self.subscription)
            .finish_non_exhaustive()
    }
}

/// The target of push delivery `delivery` (Tx A), unless its subscription
/// is gone, belongs to another user now, expired, or is outside the
/// notification's tenant scope. The caller compares its VAPID key.
pub(crate) async fn load_target(
    db: &Database,
    session: &impl Session,
    delivery: Uuid,
    now: jiff::Timestamp,
) -> Result<Option<PushTarget>, Failure> {
    let row = db
        .query_opt(session, &sql::TARGET, &[&delivery, &now])
        .await
        .map_err(|e| classify(&e))?;
    Ok(row.map(|row| PushTarget {
        subscription: row.get(0),
        endpoint: row.get(1),
        p256dh: row.get(2),
        auth: row.get(3),
        collapse_key: row.get(4),
        priority: row.get(5),
        key_id: row.get(6),
    }))
}

/// Tx B for a push attempt: an accepted push refreshes the subscription, a
/// gone one is deleted, any other outcome counts a failure. `true` when the
/// subscription was deleted.
pub(crate) async fn after_attempt(
    db: &Database,
    session: &impl Session,
    subscription: Uuid,
    outcome: &SendOutcome,
    now: jiff::Timestamp,
) -> Result<bool, Failure> {
    let (statement, gone): (&Statement, bool) = match outcome {
        SendOutcome::Accepted { .. } | SendOutcome::Duplicate { .. } => (&sql::SUCCEEDED, false),
        SendOutcome::Permanent { code } if code == SUBSCRIPTION_GONE => (&sql::GONE, true),
        _ => (&sql::FAILED, false),
    };
    let changed = if gone {
        db.execute(session, statement, &[&subscription])
            .await
            .map_err(|e| classify(&e))?
    } else {
        db.execute(session, statement, &[&subscription, &now])
            .await
            .map_err(|e| classify(&e))?
    };
    Ok(gone && changed > 0)
}

/// Count and log removed subscriptions; `reason` is one of `gone`,
/// `expired`, `failures`, `user`, `key_changed`.
pub(crate) fn removed(metrics: &Metrics, reason: &'static str, count: u64) {
    if count == 0 {
        return;
    }
    metrics.push_subscriptions_removed(reason, count);
    merezha_telemetry::event!(
        INFO,
        "notification.push_subscription_removed",
        target = "push",
        reason
    );
}

/// The push payload of NT §5.3, never more than [`MAX_PUSH_PAYLOAD_BYTES`]:
/// `body` is cut at a character boundary when the whole message would not
/// fit. The render refuses a message whose other fields, with the worst
/// case of a collapse key, are too large; should the fields still not fit
/// with an empty body, `tag` is dropped, and `None` means nothing fits
/// (the delivery fails `push_payload_too_large`).
pub fn payload(
    delivery: Uuid,
    notification: Uuid,
    topic: &str,
    content: &RenderedPush,
    tag: Option<&str>,
) -> Option<Vec<u8>> {
    let make = |body: &str, tag: Option<&str>| {
        serde_json::json!({
            "id": delivery.to_string(),
            "notification_id": notification.to_string(),
            "topic": topic,
            "title": content.title,
            "body": body,
            "url": content.url,
            "tag": tag,
        })
        .to_string()
    };
    let fit = |tag: Option<&str>| -> Option<String> {
        let full = make(&content.body, tag);
        if full.len() <= MAX_PUSH_PAYLOAD_BYTES {
            return Some(full);
        }
        if make("", tag).len() > MAX_PUSH_PAYLOAD_BYTES {
            return None;
        }
        // The longest prefix that fits; the empty one does.
        let ends: Vec<usize> = content
            .body
            .char_indices()
            .map(|(i, _)| i)
            .chain(std::iter::once(content.body.len()))
            .collect();
        let (mut low, mut high) = (0usize, ends.len() - 1);
        while low < high {
            let middle = (low + high).div_ceil(2);
            if make(&content.body[..ends[middle]], tag).len() <= MAX_PUSH_PAYLOAD_BYTES {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        Some(make(&content.body[..ends[low]], tag))
    };
    fit(tag)
        .or_else(|| tag.and_then(|_| fit(None)))
        .map(String::into_bytes)
}

/// `Topic`: the first 32 base64url characters of SHA-256 of the collapse
/// key (RFC 8030 allows at most 32 from the URL-safe alphabet).
pub fn topic_header(collapse_key: &str) -> String {
    let mut topic = b64url(&Sha256::digest(collapse_key.as_bytes()));
    topic.truncate(32);
    topic
}
/// `Urgency` from the topic priority.
pub fn urgency(priority: &str) -> &'static str {
    match priority {
        "low" => "low",
        "high" => "high",
        _ => "normal",
    }
}

/// `Retry-After` as seconds or an HTTP date, at most `max_wait` and never
/// more than a day.
fn retry_after(headers: &reqwest::header::HeaderMap, max_wait: Duration) -> Option<Duration> {
    let text = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    let wait = match text.parse::<u64>() {
        Ok(seconds) => Duration::from_secs(seconds),
        Err(_) => {
            let at = jiff::fmt::rfc2822::parse(text).ok()?.timestamp();
            Duration::try_from(at.duration_since(jiff::Timestamp::now())).unwrap_or_default()
        }
    };
    Some(wait.min(max_wait).min(MAX_RETRY_AFTER))
}

/// The outcome of a push service response (RFC 8030, NT §5.3) on attempt
/// `attempt` of the job; a `Retry-After` waits at most `max_wait` (the
/// push TTL).
pub fn classify_response(
    status: reqwest::StatusCode,
    headers: &reqwest::header::HeaderMap,
    attempt: u32,
    max_wait: Duration,
) -> SendOutcome {
    let permanent = |code: &str| SendOutcome::Permanent {
        code: code.to_owned(),
    };
    match status.as_u16() {
        200..=299 => SendOutcome::Accepted {
            provider: Provider::Webpush,
            message_id: headers
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .filter(|v| !v.is_empty() && v.chars().count() <= MAX_MESSAGE_ID)
                .map(str::to_owned),
        },
        404 | 410 => permanent(SUBSCRIPTION_GONE),
        413 => permanent(PAYLOAD_TOO_LARGE),
        401 | 403 if attempt >= PUSH_AUTH_MAX_ATTEMPTS => permanent(PUSH_AUTH),
        401 | 403 => SendOutcome::Transient {
            code: PUSH_AUTH.to_owned(),
            retry_after: None,
        },
        429 => SendOutcome::RateLimited {
            retry_after: retry_after(headers, max_wait),
        },
        500..=599 => SendOutcome::Transient {
            code: "push_unavailable".to_owned(),
            retry_after: retry_after(headers, max_wait),
        },
        300..=399 => permanent("push_redirected"),
        _ => permanent("push_rejected"),
    }
}

/// One push to send. `Debug` shows ids only.
pub struct PushMessage<'a> {
    pub delivery: Uuid,
    pub notification: Uuid,
    pub topic: &'a str,
    pub content: &'a RenderedPush,
    pub target: &'a PushTarget,
}

/// Sends Web Push requests through the guarded client.
pub struct PushSender {
    client: reqwest::Client,
    policy: Arc<EgressPolicy>,
    vapid: Arc<Vapid>,
    ttl: Duration,
    allowed_hosts: Vec<String>,
    development: bool,
    metrics: Metrics,
}
impl std::fmt::Debug for PushSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PushSender")
            .field("vapid", &self.vapid)
            .finish_non_exhaustive()
    }
}
impl PushSender {
    /// The sender of `settings.push`; `lookup` resolves names under the
    /// egress guard.
    pub fn new(
        settings: &Settings,
        push: &PushSettings,
        no_egress: bool,
        lookup: Arc<dyn Lookup>,
        metrics: Metrics,
    ) -> Result<Self, Failure> {
        let policy = Arc::new(EgressPolicy::new(&settings.egress, no_egress));
        Ok(Self {
            client: egress::client(policy.clone(), lookup)?,
            policy,
            vapid: Arc::new(Vapid::new(push)?),
            ttl: push.ttl,
            allowed_hosts: push.allowed_hosts.clone(),
            development: settings.egress.allow_private_networks,
            metrics,
        })
    }
    pub fn vapid(&self) -> &Vapid {
        &self.vapid
    }
    /// The endpoint rule this sender applies at send (and the routes at
    /// subscribe).
    pub fn check(&self, endpoint: &str) -> Result<Endpoint, &'static str> {
        check_endpoint(endpoint, &self.allowed_hosts, self.development)
    }

    fn blocked(&self) -> SendOutcome {
        self.metrics.egress_blocked("push");
        merezha_telemetry::event!(
            WARN,
            "notification.egress_blocked",
            target = "push",
            reason = egress::EGRESS_BLOCKED
        );
        SendOutcome::Permanent {
            code: egress::EGRESS_BLOCKED.to_owned(),
        }
    }

    /// Send `message` before `deadline`, on job attempt `attempt`. Never
    /// retried here; the outcome classifies every result.
    pub async fn send(
        &self,
        message: &PushMessage<'_>,
        deadline: Instant,
        attempt: u32,
    ) -> SendOutcome {
        let permanent = |code: &str| SendOutcome::Permanent {
            code: code.to_owned(),
        };
        // The allowlist again: an entry may have been removed since.
        let endpoint = match self.check(&message.target.endpoint) {
            Ok(endpoint) => endpoint,
            Err(code) => return permanent(code),
        };
        let Some(host) = endpoint.url.host() else {
            return permanent(SUBSCRIPTION_INVALID);
        };
        if self.policy.check_host(&host).is_err() {
            return self.blocked();
        }
        let authorization = match self.vapid.authorization(&endpoint.origin) {
            Ok(value) => value,
            Err(failure) => return permanent(&failure.code),
        };
        let keys = (
            web_push_native::p256::PublicKey::from_sec1_bytes(&message.target.p256dh),
            message.target.auth.len() == 16,
        );
        let (Ok(public), true) = keys else {
            return permanent(SUBSCRIPTION_INVALID);
        };
        let auth = web_push_native::Auth::clone_from_slice(&message.target.auth);
        let Some(plaintext) = payload(
            message.delivery,
            message.notification,
            message.topic,
            message.content,
            message.target.collapse_key.as_deref(),
        ) else {
            return permanent(PAYLOAD_TOO_LARGE);
        };
        let Ok(body) = web_push_native::encrypt(plaintext, &public, &auth) else {
            return permanent("push_encryption_failed");
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return SendOutcome::Transient {
                code: "push_timeout".to_owned(),
                retry_after: None,
            };
        }
        let mut request = self
            .client
            .post(endpoint.url.clone())
            .header("TTL", self.ttl.as_secs().to_string())
            .header("Urgency", urgency(&message.target.priority))
            .header(reqwest::header::CONTENT_ENCODING, "aes128gcm")
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .header(reqwest::header::AUTHORIZATION, authorization)
            .timeout(remaining)
            .body(body);
        if let Some(key) = &message.target.collapse_key {
            request = request.header("Topic", topic_header(key));
        }
        let sent = tokio::time::timeout_at(deadline.into(), request.send()).await;
        let outcome = match sent {
            Err(_) => SendOutcome::Ambiguous {
                code: "push_timeout".to_owned(),
            },
            Ok(Err(error)) if egress::is_blocked(&error) => return self.blocked(),
            // Nothing reached the push service: safe to resend.
            Ok(Err(error)) if error.is_connect() => SendOutcome::Transient {
                code: "push_unavailable".to_owned(),
                retry_after: None,
            },
            Ok(Err(error)) if error.is_timeout() => SendOutcome::Ambiguous {
                code: "push_timeout".to_owned(),
            },
            Ok(Err(_)) => SendOutcome::Ambiguous {
                code: "push_unavailable".to_owned(),
            },
            // The body is never read: push services answer with no content
            // that matters, and dropping the response bounds the read.
            Ok(Ok(response)) => {
                classify_response(response.status(), response.headers(), attempt, self.ttl)
            }
        };
        if let SendOutcome::Transient { code, .. } | SendOutcome::Permanent { code } = &outcome
            && code == PUSH_AUTH
        {
            merezha_telemetry::event!(
                ERROR,
                "notification.provider_account_error",
                provider = Provider::Webpush.as_str(),
                channel = "push",
                code = PUSH_AUTH
            );
            self.metrics.provider_account_error(
                Provider::Webpush.as_str(),
                merezha_platform::ports::Channel::Push,
            );
        }
        // The worker logs rate limits for every channel.
        outcome
    }
}

/// `push_expire` (hourly): delete subscriptions past `expires_at`, made
/// with another VAPID key and unused for [`KEY_CHANGE_GRACE`], or failing
/// 10 times in a row with no success in 30 days, in bounded batches.
pub(crate) async fn expire(
    db: &Database,
    metrics: &Metrics,
    key_id: &str,
    now: jiff::Timestamp,
    deadline: Instant,
) -> Result<u64, Failure> {
    const BATCH: i64 = 1000;
    const MAX_BATCHES: usize = 20;
    let window = jiff::SignedDuration::try_from(FAILURE_WINDOW)
        .map_err(|_| Failure::permanent("invalid_config"))?;
    let since = now
        .checked_sub(window)
        .map_err(|_| Failure::permanent("invalid_config"))?;
    let unused_since = jiff::SignedDuration::try_from(KEY_CHANGE_GRACE)
        .ok()
        .and_then(|grace| now.checked_sub(grace).ok())
        .ok_or_else(|| Failure::permanent("invalid_config"))?;
    let mut total = 0;
    for _ in 0..MAX_BATCHES {
        let client = db.acquire(deadline).await?;
        let rows = db
            .query(
                &client,
                &sql::EXPIRE,
                &[
                    &now,
                    &key_id,
                    &MAX_CONSECUTIVE_FAILURES,
                    &since,
                    &BATCH,
                    &unused_since,
                ],
            )
            .await
            .map_err(|e| classify(&e))?;
        let mut counts: [(&'static str, u64); 3] =
            [("expired", 0), ("key_changed", 0), ("failures", 0)];
        for row in &rows {
            let why: String = row.get(0);
            if let Some(entry) = counts.iter_mut().find(|(name, _)| *name == why) {
                entry.1 += 1;
            }
        }
        for (reason, count) in counts {
            removed(metrics, reason, count);
        }
        total += rows.len() as u64;
        if (rows.len() as i64) < BATCH {
            break;
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hosts() -> Vec<String> {
        crate::config::DEFAULT_PUSH_HOSTS
            .iter()
            .map(|h| (*h).to_owned())
            .collect()
    }

    #[test]
    fn endpoints_follow_the_strict_allowlist() {
        let ok = |url: &str| check_endpoint(url, &hosts(), false).map(|e| e.origin);
        assert_eq!(
            ok("https://fcm.googleapis.com/fcm/send/abc"),
            Ok("https://fcm.googleapis.com".to_owned())
        );
        assert_eq!(
            ok("https://wns2-par02p.notify.windows.com/w/?token=x"),
            Ok("https://wns2-par02p.notify.windows.com".to_owned())
        );
        assert!(ok("https://fcm.googleapis.com:443/x").is_ok());
        for refused in [
            "https://evil.com/fcm.googleapis.com",
            "https://fcm.googleapis.com.evil.com/x",
            "https://xfcm.googleapis.com/x",
            "http://fcm.googleapis.com/x",
            "https://fcm.googleapis.com:8443/x",
            "https://127.0.0.1/x",
            "https://[::1]/x",
        ] {
            assert_eq!(ok(refused), Err(HOST_NOT_ALLOWED), "{refused}");
        }
        for invalid in [
            "",
            "not a url",
            "https://user:pw@fcm.googleapis.com/x",
            "https://fcm.googleapis.com/x#frag",
            "mailto:x@y",
        ] {
            assert_eq!(ok(invalid), Err(SUBSCRIPTION_INVALID), "{invalid}");
        }
        let long = format!("https://fcm.googleapis.com/{}", "a".repeat(MAX_ENDPOINT));
        assert_eq!(ok(&long), Err(SUBSCRIPTION_INVALID));
    }

    /// Host names follow the DNS rules (labels of 1 to 63 bytes, 253 in
    /// all), so an over-long or empty label is `subscription_invalid`, not
    /// a database constraint violation.
    #[test]
    fn endpoint_host_names_must_be_valid_dns_names() {
        let ok = |url: &str| check_endpoint(url, &hosts(), false).map(|e| e.origin);
        let label = |n: usize| "a".repeat(n);
        for invalid in [
            format!("https://{}.fcm.googleapis.com/x", label(400)),
            format!("https://{}.fcm.googleapis.com/x", label(64)),
            "https://a..fcm.googleapis.com/x".to_owned(),
            "https://fcm.googleapis.com./x".to_owned(),
            // 254 bytes in all, every label short enough.
            format!(
                "https://{}.fcm.googleapis.com/x",
                [label(60), label(60), label(60), label(52)].join(".")
            ),
        ] {
            assert_eq!(ok(&invalid), Err(SUBSCRIPTION_INVALID), "{invalid}");
        }
        // 253 bytes, labels of 63: the longest valid name.
        let longest = format!(
            "https://{}.fcm.googleapis.com/x",
            [label(63), label(63), label(63), label(42)].join(".")
        );
        assert!(ok(&longest).is_ok(), "{longest}");
        assert!(ok(&format!("https://{}.fcm.googleapis.com/x", label(63))).is_ok());
        // Development lifts the scheme and port rule, never the host rule.
        let dev = vec!["127.0.0.1".to_owned(), "push.test".to_owned()];
        assert_eq!(
            check_endpoint("http://127.0.0.1:4000/s/1", &dev, true).map(|e| e.origin),
            Ok("http://127.0.0.1:4000".to_owned())
        );
        assert!(check_endpoint("http://push.test:4000/s/1", &dev, true).is_ok());
        assert_eq!(
            check_endpoint("http://127.0.0.1:4000/s/1", &dev, false),
            Err(HOST_NOT_ALLOWED)
        );
        assert_eq!(
            check_endpoint("http://10.0.0.5:4000/s/1", &dev, true),
            Err(HOST_NOT_ALLOWED)
        );
    }

    #[test]
    fn a_long_body_is_cut_at_a_character_boundary_to_fit() {
        let content = RenderedPush {
            title: "Ttl".into(),
            body: "é".repeat(4000),
            url: Some("https://app.example.com/t/1".into()),
        };
        let bytes = payload(
            Uuid::nil(),
            Uuid::nil(),
            "tasks.assigned",
            &content,
            Some("k"),
        )
        .unwrap();
        assert!(bytes.len() <= MAX_PUSH_PAYLOAD_BYTES, "{}", bytes.len());
        assert!(bytes.len() > MAX_PUSH_PAYLOAD_BYTES - 8, "{}", bytes.len());
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let body = value["body"].as_str().unwrap();
        assert!(body.chars().all(|c| c == 'é') && body.len() < content.body.len());
        assert_eq!(value["tag"], "k");
        assert_eq!(value["url"], "https://app.example.com/t/1");
        let short = RenderedPush {
            body: "hello".into(),
            ..content
        };
        let value: serde_json::Value =
            serde_json::from_slice(&payload(Uuid::nil(), Uuid::nil(), "t", &short, None).unwrap())
                .unwrap();
        assert_eq!(
            (value["body"].as_str(), value["tag"].is_null()),
            (Some("hello"), true)
        );
    }

    /// The payload never exceeds the limit, even when the collapse key is
    /// made of characters JSON escapes to six bytes each: past an empty
    /// body the tag is dropped, and when nothing fits there is no payload.
    #[test]
    fn the_payload_never_exceeds_the_limit_whatever_the_tag() {
        let content = RenderedPush {
            title: "T".into(),
            body: "some body".into(),
            url: Some(format!("https://app.example.com/{}", "u".repeat(2700))),
        };
        let tag = "\u{1}".repeat(merezha_platform::ports::MAX_COLLAPSE_KEY);
        let bytes = payload(Uuid::nil(), Uuid::nil(), "t", &content, Some(&tag)).unwrap();
        assert!(bytes.len() <= MAX_PUSH_PAYLOAD_BYTES, "{}", bytes.len());
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            (value["tag"].is_null(), value["body"].as_str()),
            (true, Some("some body"))
        );
        // A tag that fits is kept, with the body cut instead.
        let cut = RenderedPush {
            body: "b".repeat(400),
            url: Some(format!("https://app.example.com/{}", "u".repeat(2300))),
            ..content.clone()
        };
        let bytes = payload(Uuid::nil(), Uuid::nil(), "t", &cut, Some(&tag)).unwrap();
        assert!(bytes.len() <= MAX_PUSH_PAYLOAD_BYTES, "{}", bytes.len());
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["tag"], json_tag(&tag));
        assert!(value["body"].as_str().unwrap().len() < 400);
        // Fields that never fit give no payload.
        let huge = RenderedPush {
            url: Some(format!("https://app.example.com/{}", "u".repeat(3000))),
            ..content
        };
        assert_eq!(
            payload(Uuid::nil(), Uuid::nil(), "t", &huge, Some(&tag)),
            None
        );
    }
    fn json_tag(tag: &str) -> serde_json::Value {
        serde_json::Value::String(tag.to_owned())
    }

    #[test]
    fn keys_parse_from_both_forms_and_ids_are_stable() {
        let generated = generate().unwrap();
        let raw = parse_private_key(&generated.private_key).unwrap();
        let pem = parse_private_key(&generated.private_key_pem).unwrap();
        assert_eq!(raw.to_bytes(), pem.to_bytes());
        let public = b64url_decode(&generated.public_key).unwrap();
        assert_eq!((public.len(), public[0]), (65, 4));
        assert_eq!(generated.key_id, key_id(&public));
        assert_eq!(generated.key_id.len(), 16);
        assert!(!format!("{generated:?}").contains(&generated.private_key));
        // The zero scalar is no key.
        assert!(parse_private_key(&b64url(&[0u8; 32])).is_none());
        assert!(parse_private_key(&b64url(&[1u8; 31])).is_none());
        assert_eq!(topic_header("tasks:42").len(), 32);
        assert_eq!(topic_header("tasks:42"), topic_header("tasks:42"));
    }

    const DAY: Duration = Duration::from_secs(86_400);

    #[test]
    fn responses_are_classified_as_rfc_8030_says() {
        use reqwest::{StatusCode, header::HeaderMap};
        let none = HeaderMap::new();
        let class = |status: u16, attempt| {
            classify_response(StatusCode::from_u16(status).unwrap(), &none, attempt, DAY)
        };
        assert!(matches!(class(201, 1), SendOutcome::Accepted { .. }));
        for gone in [404, 410] {
            assert_eq!(
                class(gone, 1),
                SendOutcome::Permanent {
                    code: SUBSCRIPTION_GONE.into()
                }
            );
        }
        assert!(matches!(class(400, 1), SendOutcome::Permanent { .. }));
        assert!(matches!(class(413, 1), SendOutcome::Permanent { .. }));
        assert!(
            matches!(class(401, 2), SendOutcome::Transient { ref code, .. } if code == PUSH_AUTH)
        );
        assert_eq!(
            class(403, 3),
            SendOutcome::Permanent {
                code: PUSH_AUTH.into()
            }
        );
        assert!(matches!(class(503, 1), SendOutcome::Transient { .. }));
        assert!(matches!(class(302, 1), SendOutcome::Permanent { .. }));
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", "7".parse().unwrap());
        assert_eq!(
            classify_response(StatusCode::TOO_MANY_REQUESTS, &headers, 1, DAY),
            SendOutcome::RateLimited {
                retry_after: Some(Duration::from_secs(7))
            }
        );
        // `Retry-After` waits at most the push TTL (then at most a day).
        headers.insert("retry-after", "100000".parse().unwrap());
        let ttl = Duration::from_secs(60);
        assert_eq!(
            classify_response(StatusCode::TOO_MANY_REQUESTS, &headers, 1, ttl),
            SendOutcome::RateLimited {
                retry_after: Some(ttl)
            }
        );
        assert!(matches!(
            classify_response(StatusCode::SERVICE_UNAVAILABLE, &headers, 1, ttl),
            SendOutcome::Transient { retry_after: Some(wait), .. } if wait == ttl
        ));
        assert_eq!(
            classify_response(StatusCode::TOO_MANY_REQUESTS, &headers, 1, 2 * DAY),
            SendOutcome::RateLimited {
                retry_after: Some(DAY)
            }
        );
    }
}
