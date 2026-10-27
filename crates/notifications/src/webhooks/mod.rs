//! Outgoing webhooks (NT §5.5, slice NT-N7): the endpoint registries, the
//! fan-out of webhook events, the signed delivery worker and its retry
//! schedule, auto-disable, and the audit rows of endpoint changes.
//!
//! - **Registries.** Endpoints are registered per tenant, or at
//!   application level (NT6), through [`routes`]. A secret is
//!   `whsec_` plus base64 of 32 random bytes ([`signing`]); it is shown
//!   only at creation and rotation and stored sealed ([`secrets`]).
//! - **Fan-out** ([`fanout`], `notify.webhook_event` on `notify.expand`):
//!   one transaction stores the event with its frozen envelope
//!   `{"type","timestamp","data"}` and one delivery per matching active
//!   endpoint of the event's registry, each with its job record
//!   (`notify.webhook.deliver` on `notify.webhook`). A tenant event
//!   reaches only its tenant's endpoints, a tenantless event only the
//!   application-level ones.
//! - **Delivery** ([`worker`]): Tx A locks the delivery and commits a
//!   `started` attempt, the request is signed (Standard Webhooks, two
//!   signatures during a rotation window) over the frozen body and sent
//!   through the egress-guarded client ([`crate::egress`]), and Tx B
//!   records the outcome. Any 2xx succeeds; a 410 disables the endpoint
//!   (`gone`) and cancels its queued deliveries; every other failure
//!   (3xx included: redirects are never followed; a refused destination
//!   counts too) is retried on [`SCHEDULE`]. A crash between the call and
//!   Tx B resends with the same `webhook-id`.
//! - **Auto-disable** ([`tasks`]): an endpoint failing for
//!   `webhooks.auto_disable_after` with at least 10 failed attempts in that
//!   window is disabled (`failing`) and its tenant's `alert_roles` (or the
//!   `application_alert_roles`) get `notifications.webhook_disabled`.
//!
//! Signing secrets never reach a log, a span, an error, a `Debug` output
//! or an answer other than create and rotate; URLs never reach telemetry
//! (the endpoint id stands for them).
pub mod fanout;
pub mod routes;
pub mod secrets;
pub mod signing;
pub mod sql;
pub mod target;
pub mod tasks;
pub mod worker;

use crate::{
    config::{Settings, WebhookSettings},
    egress::{self, EgressPolicy, GuardedResolver, Lookup},
    metrics::Metrics,
};
use merezha_domain::Failure;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

/// Queue and kind of webhook deliveries (NT §2.5).
pub const DELIVER_QUEUE: &str = "notify.webhook";
pub const DELIVER_KIND: &str = "notify.webhook.deliver";
/// The `user-agent` of every webhook request.
pub const USER_AGENT: &str = "Merezha-Webhooks/1";
/// The built-in event type of `POST …/webhooks/{id}/test`.
pub const TEST_EVENT_TYPE: &str = "webhook.test";
/// The rotation window when `keep_previous_hours` is not given, and its
/// bound.
pub const DEFAULT_KEEP_PREVIOUS_HOURS: u32 = 24;
pub const MAX_KEEP_PREVIOUS_HOURS: u32 = 72;
/// Most bytes of a response body kept as `response_snippet`.
pub const MAX_SNIPPET: usize = 1024;
/// Attempts of one delivery generation (Standard Webhooks schedule).
pub const MAX_ATTEMPTS: u32 = 10;
/// Failed attempts within `auto_disable_after` that disable an endpoint.
pub const AUTO_DISABLE_FAILURES: i64 = 10;
/// Most event-type filters of one endpoint.
pub const MAX_FILTERS: usize = 64;

/// Error codes of the webhook routes and worker.
pub mod code {
    pub const URL_REJECTED: &str = "webhook_url_rejected";
    pub const ENDPOINT_LIMIT: &str = "webhook_endpoint_limit";
    pub const ENDPOINT_DISABLED: &str = "endpoint_disabled";
    pub const EVENT_BODY_EXPIRED: &str = "event_body_expired";
    pub const UNKNOWN_EVENT_TYPE: &str = "unknown_event_type";
    pub const WEBHOOKS_DISABLED: &str = "webhooks_disabled";
}

/// The delay before attempt `n + 1` once attempt `n` failed, `n` from 1
/// to 9: attempts run at 0, +5 s, +5 min, +30 min, +2 h, +5 h, +10 h,
/// +14 h, +20 h and +24 h (Standard Webhooks), about 3 days in all.
pub const SCHEDULE: [Duration; 9] = [
    Duration::from_secs(5),
    Duration::from_secs(5 * 60),
    Duration::from_secs(30 * 60),
    Duration::from_secs(2 * 3600),
    Duration::from_secs(5 * 3600),
    Duration::from_secs(10 * 3600),
    Duration::from_secs(14 * 3600),
    Duration::from_secs(20 * 3600),
    Duration::from_secs(24 * 3600),
];
/// The longest step, which the `notify.webhook` queue's `backoff_max_ms =
/// 86400000` must allow (R1).
pub const MAX_STEP: Duration = Duration::from_secs(24 * 3600);

/// The delay after failed attempt `attempt` (1-based, within its
/// generation) with ±10 % jitter from `share` in `[0, 1]`, never past
/// [`MAX_STEP`]; `None` once [`MAX_ATTEMPTS`] attempts failed.
pub fn next_delay(attempt: u32, share: f64) -> Option<Duration> {
    let step = SCHEDULE.get(usize::try_from(attempt).ok()?.checked_sub(1)?)?;
    let factor = 0.9 + 0.2 * share.clamp(0.0, 1.0);
    Some(step.mul_f64(factor).min(MAX_STEP))
}

/// The `notify.webhook.deliver` payload. `Debug` shows ids only, which is
/// all it holds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliverJob {
    #[serde(with = "crate::payload::uuid_text")]
    pub webhook_delivery_id: Uuid,
    /// Read before the delivery, so the per-endpoint limit is taken before
    /// an attempt is recorded.
    #[serde(with = "crate::payload::uuid_text")]
    pub endpoint_id: Uuid,
    pub generation: i32,
    /// How many jobs before this one found the endpoint busy and handed
    /// the delivery on (0 for the first job of a generation).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub busy: u32,
    /// Unix seconds of the chain's first hand-off (block clock).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub busy_since: Option<i64>,
    /// How many attempts the generation had when the chain began: a chain
    /// is bounded per step of the schedule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub busy_step: Option<i64>,
}
fn is_zero(n: &u32) -> bool {
    *n == 0
}
impl DeliverJob {
    pub fn to_value(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }
}

/// The id of the delivery of `event` to `endpoint`: `UUIDv5(event id,
/// endpoint id)`, so a second fan-out cannot create a second delivery.
pub fn delivery_id(event: Uuid, endpoint: Uuid) -> Uuid {
    Uuid::new_v5(&event, endpoint.as_bytes())
}

/// The `notify.class` of an attempt (TE's 8 classes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    Success,
    Redirect,
    ClientError,
    Gone,
    ServerError,
    Timeout,
    Network,
    Blocked,
    /// No request: the endpoint's slots stayed busy past the hand-off
    /// budget. Not one of TE's classes: it is not counted as an attempt
    /// class.
    Busy,
}
impl Class {
    pub fn of_status(status: u16) -> Self {
        match status {
            200..=299 => Class::Success,
            300..=399 => Class::Redirect,
            410 => Class::Gone,
            500..=599 => Class::ServerError,
            _ => Class::ClientError,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Class::Success => "2xx",
            Class::Redirect => "3xx",
            Class::ClientError => "4xx",
            Class::Gone => "410",
            Class::ServerError => "5xx",
            Class::Timeout => "timeout",
            Class::Network => "network",
            Class::Blocked => "blocked",
            Class::Busy => "busy",
        }
    }
    /// The attempt's `error_code` (none on success).
    pub fn error_code(self) -> Option<&'static str> {
        match self {
            Class::Success => None,
            Class::Redirect => Some("redirected"),
            Class::ClientError => Some("http_client_error"),
            Class::Gone => Some("gone"),
            Class::ServerError => Some("http_server_error"),
            Class::Timeout => Some("timeout"),
            Class::Network => Some("network"),
            Class::Blocked => Some(egress::EGRESS_BLOCKED),
            Class::Busy => Some("endpoint_busy"),
        }
    }
}

/// What one request gave.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sent {
    pub class: Class,
    pub status: Option<u16>,
    /// At most [`MAX_SNIPPET`] bytes of the body, lossy UTF-8 without
    /// control characters.
    pub snippet: Option<String>,
}

/// Bounded per-endpoint request limits: at most `concurrency_per_endpoint`
/// requests per endpoint on this instance. A job never waits for a slot
/// ([`Limiter::try_acquire`]); without one it hands the delivery to a later
/// job. The map keeps at most [`Limiter::MAX_ENDPOINTS`] endpoints; when
/// full it drops the idle ones, and should every one be busy, the request
/// runs unlimited rather than grow the map.
pub struct Limiter {
    per_endpoint: usize,
    endpoints: Mutex<HashMap<Uuid, Arc<Semaphore>>>,
}
impl std::fmt::Debug for Limiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Limiter")
            .field("per_endpoint", &self.per_endpoint)
            .finish_non_exhaustive()
    }
}
impl Limiter {
    pub const MAX_ENDPOINTS: usize = 4096;
    pub fn new(per_endpoint: u32) -> Self {
        Self {
            per_endpoint: per_endpoint.max(1) as usize,
            endpoints: Mutex::new(HashMap::new()),
        }
    }
    fn semaphore(&self, endpoint: Uuid) -> Arc<Semaphore> {
        let Ok(mut endpoints) = self.endpoints.lock() else {
            return Arc::new(Semaphore::new(self.per_endpoint));
        };
        if let Some(found) = endpoints.get(&endpoint) {
            return found.clone();
        }
        if endpoints.len() >= Self::MAX_ENDPOINTS {
            // Held by a permit or a waiter when shared.
            endpoints.retain(|_, semaphore| Arc::strong_count(semaphore) > 1);
        }
        let semaphore = Arc::new(Semaphore::new(self.per_endpoint));
        if endpoints.len() < Self::MAX_ENDPOINTS {
            endpoints.insert(endpoint, semaphore.clone());
        }
        semaphore
    }
    /// A permit for `endpoint` if one is free now.
    pub fn try_acquire(&self, endpoint: Uuid) -> Option<OwnedSemaphorePermit> {
        self.semaphore(endpoint).try_acquire_owned().ok()
    }
}

/// Sends webhook requests through the egress-guarded client and checks
/// endpoint URLs. Built at start with `[modules.notifications.webhooks]`.
pub struct WebhookSender {
    client: reqwest::Client,
    policy: Arc<EgressPolicy>,
    resolver: GuardedResolver,
    pub(crate) secrets: secrets::SecretBox,
    pub(crate) limiter: Limiter,
    pub(crate) settings: WebhookSettings,
    pub(crate) metrics: Metrics,
}
impl std::fmt::Debug for WebhookSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookSender").finish_non_exhaustive()
    }
}
impl WebhookSender {
    /// The sender of `settings.webhooks`; `lookup` resolves names under the
    /// egress guard.
    pub fn new(
        settings: &Settings,
        webhooks: &WebhookSettings,
        no_egress: bool,
        lookup: Arc<dyn Lookup>,
        metrics: Metrics,
    ) -> Result<Self, Failure> {
        let policy = Arc::new(EgressPolicy::new(&settings.egress, no_egress));
        Ok(Self {
            client: egress::client_with(policy.clone(), lookup.clone(), webhooks.connect_timeout)?,
            resolver: GuardedResolver::new(policy.clone(), lookup),
            policy,
            secrets: secrets::SecretBox::new(
                &settings.secret_key,
                settings.secret_key_previous.as_ref(),
            ),
            limiter: Limiter::new(webhooks.concurrency_per_endpoint),
            settings: webhooks.clone(),
            metrics,
        })
    }
    /// Take one of `endpoint`'s slots, as a request would (feature
    /// `testkit`): while held, jobs for the endpoint find it busy.
    #[cfg(feature = "testkit")]
    pub fn occupy(&self, endpoint: Uuid) -> Option<OwnedSemaphorePermit> {
        self.limiter.try_acquire(endpoint)
    }
    /// The registration check of `url` (NT §5.5).
    pub async fn check_url(&self, url: &str) -> Result<url::Url, &'static str> {
        target::check(url, self.settings.allow_http, &self.policy, &self.resolver).await
    }

    /// POST `body` to `url` with `headers` before `deadline`. Never
    /// retried here and never follows a redirect.
    pub async fn send(
        &self,
        url: &str,
        headers: &[(&'static str, String)],
        body: Vec<u8>,
        deadline: Instant,
    ) -> Sent {
        let failed = |class: Class| Sent {
            class,
            status: None,
            snippet: None,
        };
        // The static rules again (`allow_http` may have been turned off)
        // and the IP literal, which never reaches the resolver.
        let Ok(url) = target::parse(url, self.settings.allow_http) else {
            return failed(Class::Blocked);
        };
        match url.host() {
            Some(host) if self.policy.check_host(&host).is_ok() => {}
            _ => return failed(Class::Blocked),
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return failed(Class::Timeout);
        }
        let mut request = self
            .client
            .post(url)
            .timeout(remaining)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::USER_AGENT, USER_AGENT)
            .body(body);
        for (name, value) in headers {
            request = request.header(*name, value.as_str());
        }
        let response = match tokio::time::timeout_at(deadline.into(), request.send()).await {
            Err(_) => return failed(Class::Timeout),
            Ok(Err(error)) if egress::is_blocked(&error) => return failed(Class::Blocked),
            Ok(Err(error)) if error.is_timeout() => return failed(Class::Timeout),
            Ok(Err(_)) => return failed(Class::Network),
            Ok(Ok(response)) => response,
        };
        let status = response.status().as_u16();
        let snippet = read_snippet(response, deadline).await;
        Sent {
            class: Class::of_status(status),
            status: Some(status),
            snippet,
        }
    }
}

/// At most [`MAX_SNIPPET`] bytes of `response`, then the connection is
/// dropped: lossy UTF-8 with control characters removed, `None` when
/// empty.
async fn read_snippet(mut response: reqwest::Response, deadline: Instant) -> Option<String> {
    let mut bytes = Vec::new();
    while bytes.len() < MAX_SNIPPET {
        match tokio::time::timeout_at(deadline.into(), response.chunk()).await {
            Ok(Ok(Some(chunk))) => {
                let room = MAX_SNIPPET - bytes.len();
                bytes.extend_from_slice(&chunk[..chunk.len().min(room)]);
            }
            _ => break,
        }
    }
    drop(response);
    let text: String = String::from_utf8_lossy(&bytes)
        .chars()
        .filter(|c| !c.is_control())
        .collect();
    let mut text = text;
    while text.len() > MAX_SNIPPET {
        text.pop();
    }
    (!text.is_empty()).then_some(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_schedule_is_the_standard_webhooks_ladder_with_bounded_jitter() {
        let total: Duration = SCHEDULE.iter().sum();
        assert!(
            total > Duration::from_secs(3 * 86_400) && total < Duration::from_secs(4 * 86_400),
            "{total:?}"
        );
        assert_eq!(next_delay(1, 0.5), Some(Duration::from_secs(5)));
        assert_eq!(next_delay(2, 0.0), Some(Duration::from_secs(270)));
        assert_eq!(next_delay(2, 1.0), Some(Duration::from_secs(330)));
        // The 24-hour step keeps its lower jitter and never exceeds 24 h.
        assert_eq!(next_delay(9, 0.0), Some(Duration::from_secs(77_760)));
        assert_eq!(next_delay(9, 1.0), Some(MAX_STEP));
        assert_eq!(next_delay(10, 0.5), None);
        assert_eq!(next_delay(0, 0.5), None);
    }

    #[test]
    fn statuses_are_classified() {
        assert_eq!(Class::of_status(204), Class::Success);
        assert_eq!(Class::of_status(302), Class::Redirect);
        assert_eq!(Class::of_status(410), Class::Gone);
        assert_eq!(Class::of_status(404), Class::ClientError);
        assert_eq!(Class::of_status(503), Class::ServerError);
        assert_eq!(Class::Blocked.error_code(), Some("egress_blocked"));
    }

    #[test]
    fn the_limiter_holds_concurrency_per_endpoint() {
        let limiter = Limiter::new(2);
        let (a, b) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let first = limiter.try_acquire(a);
        let second = limiter.try_acquire(a);
        assert!(first.is_some() && second.is_some());
        assert!(limiter.try_acquire(a).is_none(), "no third slot");
        assert!(limiter.try_acquire(b).is_some(), "another endpoint");
        drop(first);
        assert!(limiter.try_acquire(a).is_some());
    }
}
