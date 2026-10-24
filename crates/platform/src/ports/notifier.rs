//! The `Notifier` port (NT §3.1, R2): notification requests and webhook
//! events written inside the caller's transaction, and the capture
//! fallback used without the notifications block.
use super::{DedupeKey, RoleKey, Source, TenantId, UserId};
use crate::BoxFuture;
use merezha_domain::{Classification, Failure, Identity};
use merezha_postgres::Tx;
use serde_json::{Map, Value};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

/// A topic declared in `[modules.notifications.topics]`, such as
/// `identity.magic_link`.
pub type TopicName = Identity;
pub type TemplateName = Identity;
/// A webhook event type from the catalog, such as `tasks.task_completed`.
pub type EventType = Identity;
pub type Timestamp = jiff::Timestamp;
pub use uuid::Uuid;

/// Most recipients in one request, before expansion.
pub const MAX_RECIPIENTS: usize = 100;
/// Longest push collapse key.
pub const MAX_COLLAPSE_KEY: usize = 64;

/// A BCP 47 language tag such as `fr` or `pt-BR`: 2 to 35 bytes of ASCII
/// letters, digits and `-`, starting with a letter.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Locale(String);
impl Locale {
    /// Fails permanent `invalid_locale`.
    pub fn new(value: impl Into<String>) -> Result<Self, Failure> {
        let value = value.into();
        let valid = (2..=35).contains(&value.len())
            && value.as_bytes()[0].is_ascii_alphabetic()
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-');
        valid
            .then_some(Self(value))
            .ok_or_else(|| Failure::permanent("invalid_locale"))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An email address: personal data, so `Debug` never shows it.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct EmailAddress(String);
impl EmailAddress {
    /// A single `local@domain` of at most 254 bytes without spaces or
    /// control characters; fails permanent `invalid_recipient`. Providers
    /// apply their own stricter rules later.
    pub fn new(value: impl Into<String>) -> Result<Self, Failure> {
        let value = value.into();
        let valid = value.len() <= 254
            && value.split_once('@').is_some_and(|(local, domain)| {
                !local.is_empty() && !domain.is_empty() && !domain.contains('@')
            })
            && !value.chars().any(|c| c.is_whitespace() || c.is_control());
        valid
            .then_some(Self(value))
            .ok_or_else(|| Failure::permanent("invalid_recipient"))
    }
    pub fn expose(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Debug for EmailAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EmailAddress(<redacted>)")
    }
}

/// An E.164 phone number such as `+33612345678`: personal data, so `Debug`
/// never shows it.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PhoneNumber(String);
impl PhoneNumber {
    /// `+` and 2 to 15 digits, the first not zero; fails permanent
    /// `invalid_recipient`.
    pub fn new(value: impl Into<String>) -> Result<Self, Failure> {
        let value = value.into();
        let valid = value.strip_prefix('+').is_some_and(|digits| {
            (2..=15).contains(&digits.len())
                && !digits.starts_with('0')
                && digits.bytes().all(|b| b.is_ascii_digit())
        });
        valid
            .then_some(Self(value))
            .ok_or_else(|| Failure::permanent("invalid_recipient"))
    }
    pub fn expose(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Debug for PhoneNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PhoneNumber(<redacted>)")
    }
}

/// Who a notification is for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Recipient {
    User {
        id: UserId,
    },
    /// Members of the request's tenant, optionally with one of `roles`.
    Tenant {
        id: TenantId,
        roles: Option<Vec<RoleKey>>,
    },
    /// Holders of application-scope roles (R5); at least one role.
    Application {
        roles: Vec<RoleKey>,
    },
    Address(Address),
}
impl Recipient {
    pub fn kind(&self) -> &'static str {
        match self {
            Recipient::User { .. } => "user",
            Recipient::Tenant { .. } => "tenant",
            Recipient::Application { .. } => "application",
            Recipient::Address(Address::Email { .. }) => "email_address",
            Recipient::Address(Address::Phone { .. }) => "phone_address",
        }
    }
}
/// An explicit address. `Debug` never shows the address or the name.
#[derive(Clone, PartialEq, Eq)]
pub enum Address {
    Email {
        address: EmailAddress,
        name: Option<String>,
        user: Option<UserId>,
        locale: Option<Locale>,
    },
    Phone {
        e164: PhoneNumber,
        user: Option<UserId>,
        locale: Option<Locale>,
    },
}
impl std::fmt::Debug for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Address::Email { user, locale, .. } => f
                .debug_struct("Email")
                .field("user", user)
                .field("locale", locale)
                .finish_non_exhaustive(),
            Address::Phone { user, locale, .. } => f
                .debug_struct("Phone")
                .field("user", user)
                .field("locale", locale)
                .finish_non_exhaustive(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Channel {
    Email,
    Sms,
    Push,
    Inbox,
}
impl Channel {
    pub fn as_str(self) -> &'static str {
        match self {
            Channel::Email => "email",
            Channel::Sms => "sms",
            Channel::Push => "push",
            Channel::Inbox => "inbox",
        }
    }
}
/// The topic's default channels, or a subset of the topic's channels.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChannelSelection {
    TopicDefault,
    Only(Vec<Channel>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Correlation {
    pub correlation_id: Identity,
    pub causation_id: Option<Identity>,
}

/// A request to notify. `Debug` shows neither `data`, `secret_data` nor
/// addresses.
#[derive(Clone, PartialEq)]
pub struct NotificationRequest {
    pub topic: TopicName,
    /// 1 to [`MAX_RECIPIENTS`] before expansion.
    pub recipients: Vec<Recipient>,
    pub channels: ChannelSelection,
    /// Defaults to the topic's template.
    pub template: Option<TemplateName>,
    pub data: Map<String, Value>,
    /// Values such as sign-in links and codes. Rendered into the message
    /// under the reserved `secret` key, never stored in the inbox, never
    /// logged, never exported to telemetry, and nulled at the terminal state
    /// (R2, R3).
    pub secret_data: Map<String, Value>,
    /// Overrides the recipient's locale.
    pub locale: Option<Locale>,
    pub dedupe_key: DedupeKey,
    /// Tenant context: inbox scoping and the membership check.
    pub tenant: Option<TenantId>,
    /// `User` recipients the caller confirmed as members of `tenant` through
    /// `Directory::is_member` before it opened its transaction (NT §3.2).
    /// For sources `application` and `api` with a tenant, every `User`
    /// recipient must be listed, else `recipient_not_in_tenant`. Identity
    /// leaves it empty: an invitation targets a non-member.
    pub members: Vec<UserId>,
    pub not_before: Option<Timestamp>,
    /// Not sent after this instant.
    pub expires_at: Option<Timestamp>,
    /// Push `Topic` header, at most [`MAX_COLLAPSE_KEY`] bytes.
    pub collapse_key: Option<String>,
    pub correlation: Correlation,
}
impl std::fmt::Debug for NotificationRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NotificationRequest")
            .field("topic", &self.topic)
            .field("recipients", &self.recipients)
            .field("channels", &self.channels)
            .field("template", &self.template)
            .field("dedupe_key", &self.dedupe_key)
            .field("tenant", &self.tenant)
            .field("has_secret_data", &!self.secret_data.is_empty())
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NotificationHandle {
    pub notification_id: Uuid,
    pub duplicate: bool,
}

/// Longest event id: a source (at most 64 bytes), `:` and an outbox dedupe
/// key, as integration events are named `{source}:{dedupe_key}`.
pub const MAX_EVENT_ID_BYTES: usize = 64 + 1 + super::MAX_DEDUPE_KEY_BYTES;

/// The id of the event a webhook announces, such as
/// `identity:usr_01JB2X6Q9F3W:registered`: 1 to [`MAX_EVENT_ID_BYTES`] bytes
/// of `[A-Za-z0-9._:/-]`. It deduplicates webhook events.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId(String);
impl EventId {
    /// Fails permanent `invalid_event_id`.
    pub fn new(value: impl Into<String>) -> Result<Self, Failure> {
        let value = value.into();
        let valid = (1..=MAX_EVENT_ID_BYTES).contains(&value.len())
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._:/-".contains(&b));
        valid
            .then_some(Self(value))
            .ok_or_else(|| Failure::permanent("invalid_event_id"))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An event for registered webhook endpoints. `Debug` hides `data`.
#[derive(Clone, PartialEq)]
pub struct WebhookEvent {
    /// `None`: the application-level endpoints (NT6).
    pub tenant: Option<TenantId>,
    pub event_type: EventType,
    /// The caller's event id; the dedupe key.
    pub source_event_id: EventId,
    pub occurred_at: Timestamp,
    pub data: Value,
}
impl std::fmt::Debug for WebhookEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookEvent")
            .field("tenant", &self.tenant)
            .field("event_type", &self.event_type)
            .field("source_event_id", &self.source_event_id)
            .field("occurred_at", &self.occurred_at)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WebhookEventHandle {
    pub webhook_event_id: Uuid,
    pub duplicate: bool,
}

/// Why a request was refused. Codes are unprefixed (R8); all are permanent
/// except `unavailable`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotifyError {
    UnknownTopic,
    UnknownTemplate,
    ChannelNotAllowed,
    TooManyRecipients,
    InvalidRecipient,
    RecipientNotInTenant,
    DataTooLarge,
    ReservedDataKey,
    RenderFailed {
        channel: Channel,
    },
    UnknownEventType,
    WebhooksDisabled,
    /// The outbox write failed (transient).
    Unavailable,
}
impl NotifyError {
    pub fn code(&self) -> &'static str {
        match self {
            NotifyError::UnknownTopic => "unknown_topic",
            NotifyError::UnknownTemplate => "unknown_template",
            NotifyError::ChannelNotAllowed => "channel_not_allowed",
            NotifyError::TooManyRecipients => "too_many_recipients",
            NotifyError::InvalidRecipient => "invalid_recipient",
            NotifyError::RecipientNotInTenant => "recipient_not_in_tenant",
            NotifyError::DataTooLarge => "data_too_large",
            NotifyError::ReservedDataKey => "reserved_data_key",
            NotifyError::RenderFailed { .. } => "render_failed",
            NotifyError::UnknownEventType => "unknown_event_type",
            NotifyError::WebhooksDisabled => "webhooks_disabled",
            NotifyError::Unavailable => "unavailable",
        }
    }
    pub fn classification(&self) -> Classification {
        match self {
            NotifyError::Unavailable => Classification::Transient,
            _ => Classification::Permanent,
        }
    }
}
impl From<NotifyError> for Failure {
    fn from(error: NotifyError) -> Self {
        Failure::new(error.code(), error.classification())
    }
}

/// Requests notifications and webhook events (NT §3.1, R2).
///
/// Contract: `validate` is a pure check against the request and the
/// block's own declarations (topics, templates, channels, limits), with no
/// I/O. `request` and `emit_webhook` write one outbox append in the caller's
/// transaction and never commit it. No method takes another pooled
/// connection while the caller's transaction is open (R4): callers resolve
/// recipients and memberships through the [`super::Directory`] before they
/// open their transaction and pass the result in. Delivery happens after
/// commit, at least once, deduplicated by `dedupe_key`. `secret_data` never
/// reaches a log, the inbox or telemetry.
pub trait Notifier: Send + Sync {
    fn validate(&self, request: &NotificationRequest) -> Result<(), NotifyError>;
    fn request<'a>(
        &'a self,
        tx: &'a mut Tx<'_>,
        request: NotificationRequest,
    ) -> BoxFuture<'a, Result<NotificationHandle, NotifyError>>;
    /// Queues an event for its tenant's endpoints, or the application-level
    /// ones without a tenant. Modules never call it for their own domain
    /// events (R11); reactions and the notifications block do.
    fn emit_webhook<'a>(
        &'a self,
        tx: &'a mut Tx<'_>,
        event: WebhookEvent,
    ) -> BoxFuture<'a, Result<WebhookEventHandle, NotifyError>>;
}

/// Sources whose tenant requests must list every user recipient in
/// [`NotificationRequest::members`] (R4).
pub const MEMBERSHIP_CHECKED_SOURCES: [&str; 2] = ["application", "api"];

/// The membership rule of R4 for `source`, from the request alone.
pub fn check_members(source: &Source, request: &NotificationRequest) -> Result<(), NotifyError> {
    if request.tenant.is_none() || !MEMBERSHIP_CHECKED_SOURCES.contains(&source.as_str()) {
        return Ok(());
    }
    let outsider = request
        .recipients
        .iter()
        .any(|r| matches!(r, Recipient::User { id } if !request.members.contains(id)));
    if outsider {
        Err(NotifyError::RecipientNotInTenant)
    } else {
        Ok(())
    }
}

/// Hands out notifiers bound to their caller's source (`identity`, `api`,
/// `application`, ...), as `AppContext::notifier(source)` requires; the
/// source names the dedupe namespace and the membership rule (R4).
pub trait NotifierFactory: Send + Sync {
    fn bind(&self, source: &Source) -> Arc<dyn Notifier>;
    /// `false` only for the capture fallback, so blocks that must not lose
    /// security mail (identity) can refuse to start without the block.
    fn delivers(&self) -> bool {
        true
    }
}

/// What the capture fallback keeps: no data, no secret data, no address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapturedNotification {
    pub source: Source,
    /// `notification` or `webhook`.
    pub kind: &'static str,
    /// The topic or the webhook event type.
    pub name: Identity,
    pub recipient_kinds: Vec<&'static str>,
    /// The request's dedupe key or the webhook's source event id.
    pub dedupe_key: String,
    pub had_secret_data: bool,
}

/// Fallback without the notifications block: validates structurally, logs a
/// redacted `notification.captured` warning, keeps the last
/// [`CaptureNotifications::CAPACITY`] redacted records and delivers nothing.
#[derive(Clone, Debug, Default)]
pub struct CaptureNotifications {
    captured: Arc<Mutex<VecDeque<CapturedNotification>>>,
}
impl CaptureNotifications {
    pub const CAPACITY: usize = 64;
    pub fn captured(&self) -> Vec<CapturedNotification> {
        self.captured
            .lock()
            .map(|c| c.iter().cloned().collect())
            .unwrap_or_default()
    }
}
impl CaptureNotifications {
    /// The concrete capture notifier for `source`.
    pub fn capture_notifier(&self, source: &Source) -> CaptureNotifier {
        CaptureNotifier {
            source: source.clone(),
            captured: self.captured.clone(),
        }
    }
}
impl NotifierFactory for CaptureNotifications {
    fn bind(&self, source: &Source) -> Arc<dyn Notifier> {
        Arc::new(self.capture_notifier(source))
    }
    fn delivers(&self) -> bool {
        false
    }
}
/// One source's capture notifier; see [`CaptureNotifications`].
#[derive(Debug)]
pub struct CaptureNotifier {
    source: Source,
    captured: Arc<Mutex<VecDeque<CapturedNotification>>>,
}
impl CaptureNotifier {
    /// What `request` does, without the transaction it never writes to:
    /// validate, apply the membership rule, keep the redacted record.
    pub fn capture(&self, request: NotificationRequest) -> Result<NotificationHandle, NotifyError> {
        self.validate(&request)?;
        check_members(&self.source, &request)?;
        self.keep(CapturedNotification {
            source: self.source.clone(),
            kind: "notification",
            name: request.topic,
            recipient_kinds: request.recipients.iter().map(Recipient::kind).collect(),
            dedupe_key: request.dedupe_key.as_str().to_owned(),
            had_secret_data: !request.secret_data.is_empty(),
        });
        Ok(NotificationHandle {
            notification_id: random_id(),
            duplicate: false,
        })
    }
    fn keep(&self, record: CapturedNotification) {
        tracing::warn!(
            event = "notification.captured",
            source = record.source.as_str(),
            kind = record.kind,
            topic = record.name.as_str(),
            recipients = ?record.recipient_kinds,
            delivered = false
        );
        if let Ok(mut captured) = self.captured.lock() {
            if captured.len() == CaptureNotifications::CAPACITY {
                captured.pop_front();
            }
            captured.push_back(record);
        }
    }
}
/// A random version 4 id; the capture sink stores nothing to look it up by.
fn random_id() -> Uuid {
    let mut bytes = [0u8; 16];
    if getrandom::fill(&mut bytes).is_err() {
        // Not a secret: fall back to a time-derived value.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        bytes = nanos.to_be_bytes();
    }
    uuid::Builder::from_random_bytes(bytes).into_uuid()
}
impl Notifier for CaptureNotifier {
    fn validate(&self, request: &NotificationRequest) -> Result<(), NotifyError> {
        if request.recipients.len() > MAX_RECIPIENTS {
            return Err(NotifyError::TooManyRecipients);
        }
        let empty_roles = request
            .recipients
            .iter()
            .any(|r| matches!(r, Recipient::Application { roles } if roles.is_empty()));
        let other_tenant = request.recipients.iter().any(
            |r| matches!(r, Recipient::Tenant { id, .. } if Some(id) != request.tenant.as_ref()),
        );
        if request.recipients.is_empty() || empty_roles || other_tenant {
            return Err(NotifyError::InvalidRecipient);
        }
        if request.data.contains_key("secret") {
            return Err(NotifyError::ReservedDataKey);
        }
        if request
            .collapse_key
            .as_ref()
            .is_some_and(|k| k.len() > MAX_COLLAPSE_KEY)
        {
            return Err(NotifyError::DataTooLarge);
        }
        Ok(())
    }
    fn request<'a>(
        &'a self,
        _: &'a mut Tx<'_>,
        request: NotificationRequest,
    ) -> BoxFuture<'a, Result<NotificationHandle, NotifyError>> {
        let checked = self.capture(request);
        Box::pin(async move { checked })
    }
    fn emit_webhook<'a>(
        &'a self,
        _: &'a mut Tx<'_>,
        event: WebhookEvent,
    ) -> BoxFuture<'a, Result<WebhookEventHandle, NotifyError>> {
        self.keep(CapturedNotification {
            source: self.source.clone(),
            kind: "webhook",
            name: event.event_type,
            recipient_kinds: Vec::new(),
            dedupe_key: event.source_event_id.as_str().to_owned(),
            had_secret_data: false,
        });
        Box::pin(async {
            Ok(WebhookEventHandle {
                webhook_event_id: random_id(),
                duplicate: false,
            })
        })
    }
}
