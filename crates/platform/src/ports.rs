//! Cross-module ports. Blocks implement them; other blocks and the host use
//! them through [`crate::AppContext`]. A disabled block leaves a defined
//! fallback that never silently drops a security-relevant effect.
use crate::BoxFuture;
use merezha_application::Authorizer as ApplicationAuthorizer;
use merezha_domain::{Failure, Identity, MessageKind, Metadata};
use merezha_postgres::Transaction;
use serde_json::Value;
use std::{
    collections::{BTreeSet, VecDeque},
    net::SocketAddr,
    sync::Mutex,
};

/// What kind of party a principal is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrincipalKind {
    User,
    ApiKey,
    /// The host itself, for internal work such as jobs.
    System,
}

/// An authenticated party, established by the host and never by a guest.
/// Roles and permissions are resolved by the authenticator, so policy checks
/// need no I/O. `session` names a server-side session, never its secret.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    pub subject: Identity,
    pub kind: PrincipalKind,
    pub tenant: Option<Identity>,
    pub roles: BTreeSet<Identity>,
    pub permissions: BTreeSet<Identity>,
    pub session: Option<Identity>,
}

/// Resolves a request to a principal.
///
/// Contract: return `Ok(None)` when the request carries no credentials,
/// `Err` with permanent `unauthenticated` when credentials are present but
/// invalid, expired or revoked, and a transient failure when the backing store
/// is unavailable. Never log or echo credentials. Implementations bound their
/// own I/O; the edge applies the request deadline on top.
pub trait Authenticator: Send + Sync {
    fn authenticate<'a>(
        &'a self,
        request: &'a axum::http::request::Parts,
    ) -> BoxFuture<'a, Result<Option<Principal>, Failure>>;
}

/// The resource a permission is checked against.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResourceScope {
    /// Host-wide administration.
    Global,
    /// Anything inside one tenant.
    Tenant(Identity),
    /// One resource, optionally inside a tenant.
    Resource {
        tenant: Option<Identity>,
        kind: Identity,
        id: Identity,
    },
}

/// Permission policy over principals, extending the application's
/// command/query [`merezha_application::Authorizer`].
///
/// Contract: synchronous and I/O-free, decided from the principal's resolved
/// grants. Deny with permanent `unauthenticated` when a principal is required
/// but absent and `unauthorized` when the grant is missing. A tenant-scoped
/// check must fail for a principal of another tenant.
pub trait Authorizer: ApplicationAuthorizer {
    fn check(
        &self,
        principal: Option<&Principal>,
        permission: &Identity,
        scope: &ResourceScope,
    ) -> Result<(), Failure>;
}

/// Fallback policy without the identity block. Constructible only for a
/// loopback listener: it is a trusted local demo policy, not security.
#[derive(Debug)]
pub struct LoopbackAllowAll(());
impl LoopbackAllowAll {
    /// Fails `allow_all_requires_loopback` for any non-loopback address.
    pub fn new(listen: SocketAddr) -> Result<Self, Failure> {
        if listen.ip().is_loopback() {
            Ok(Self(()))
        } else {
            Err(Failure::permanent("allow_all_requires_loopback"))
        }
    }
}
impl ApplicationAuthorizer for LoopbackAllowAll {
    fn authorize(
        &self,
        kind: MessageKind,
        tag: &str,
        stream: Option<&str>,
        payload: &Value,
        metadata: &Metadata,
    ) -> Result<(), Failure> {
        merezha_application::AllowAll.authorize(kind, tag, stream, payload, metadata)
    }
}
impl Authorizer for LoopbackAllowAll {
    fn check(&self, _: Option<&Principal>, _: &Identity, _: &ResourceScope) -> Result<(), Failure> {
        Ok(())
    }
}
/// Fallback policy for a public listener without the identity block.
#[derive(Debug, Default)]
pub struct DenyAll;
impl ApplicationAuthorizer for DenyAll {
    fn authorize(
        &self,
        _: MessageKind,
        _: &str,
        _: Option<&str>,
        _: &Value,
        _: &Metadata,
    ) -> Result<(), Failure> {
        Err(Failure::permanent("unauthorized"))
    }
}
impl Authorizer for DenyAll {
    fn check(&self, _: Option<&Principal>, _: &Identity, _: &ResourceScope) -> Result<(), Failure> {
        Err(Failure::permanent("unauthorized"))
    }
}

/// Who a notification is for. An address is personal data: `Debug` and
/// logs never show it.
#[derive(Clone, PartialEq, Eq)]
pub enum Recipient {
    User(Identity),
    Organization(Identity),
    Address { channel: Identity, address: String },
}
impl std::fmt::Debug for Recipient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Recipient::User(id) => f.debug_tuple("User").field(id).finish(),
            Recipient::Organization(id) => f.debug_tuple("Organization").field(id).finish(),
            Recipient::Address { channel, .. } => f
                .debug_struct("Address")
                .field("channel", channel)
                .field("address", &"<redacted>")
                .finish(),
        }
    }
}
impl Recipient {
    fn kind(&self) -> &'static str {
        match self {
            Recipient::User(_) => "user",
            Recipient::Organization(_) => "organization",
            Recipient::Address { .. } => "address",
        }
    }
}
/// A request to notify on named channels with a template and its data.
#[derive(Clone, PartialEq)]
pub struct NotificationRequest {
    pub recipient: Recipient,
    /// Channel names such as `email`, `sms`, `push`, `inbox`, `webhook`.
    pub channels: Vec<Identity>,
    pub template: Identity,
    /// Template data; may contain personal data and is never logged.
    pub data: Value,
    /// Deduplicates retried requests.
    pub idempotency_key: Identity,
}
impl std::fmt::Debug for NotificationRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NotificationRequest")
            .field("recipient", &self.recipient)
            .field("channels", &self.channels)
            .field("template", &self.template)
            .field("idempotency_key", &self.idempotency_key)
            .finish_non_exhaustive()
    }
}

/// Requests notifications.
///
/// Contract: accept or refuse the request durably; delivery happens later
/// and at least once, deduplicated by `idempotency_key`. Refuse unknown
/// templates or channels with a permanent failure. Never drop an accepted
/// request silently.
pub trait Notifier: Send + Sync {
    fn notify<'a>(&'a self, request: NotificationRequest) -> BoxFuture<'a, Result<(), Failure>>;
}

/// The redacted record a [`CaptureNotifier`] keeps.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapturedNotification {
    pub recipient_kind: &'static str,
    pub channels: Vec<Identity>,
    pub template: Identity,
    pub idempotency_key: Identity,
}
/// Fallback without the notifications block: logs a redacted
/// `notification.captured` warning and keeps the last 64 redacted records.
/// Nothing is delivered.
#[derive(Debug, Default)]
pub struct CaptureNotifier {
    captured: Mutex<VecDeque<CapturedNotification>>,
}
impl CaptureNotifier {
    pub const CAPACITY: usize = 64;
    pub fn captured(&self) -> Vec<CapturedNotification> {
        self.captured
            .lock()
            .map(|c| c.iter().cloned().collect())
            .unwrap_or_default()
    }
}
impl Notifier for CaptureNotifier {
    fn notify<'a>(&'a self, request: NotificationRequest) -> BoxFuture<'a, Result<(), Failure>> {
        let record = CapturedNotification {
            recipient_kind: request.recipient.kind(),
            channels: request.channels,
            template: request.template,
            idempotency_key: request.idempotency_key,
        };
        tracing::warn!(
            event = "notification.captured",
            recipient_kind = record.recipient_kind,
            template = record.template.as_str(),
            channels = ?record.channels.iter().map(Identity::as_str).collect::<Vec<_>>(),
            delivered = false
        );
        if let Ok(mut captured) = self.captured.lock() {
            if captured.len() == Self::CAPACITY {
                captured.pop_front();
            }
            captured.push_back(record);
        }
        Box::pin(async { Ok(()) })
    }
}

/// Whether an outbox record announces a fact or requests an effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutboxKind {
    IntegrationEvent,
    EffectRequest,
}
/// One outbox record, delivered at least once after its transaction commits.
#[derive(Clone, Debug, PartialEq)]
pub struct OutboxMessage {
    pub kind: OutboxKind,
    /// Event or effect name, e.g. `identity.user_registered`.
    pub name: Identity,
    /// Unique per logical message; consumers deduplicate on it.
    pub idempotency_key: Identity,
    pub payload: Value,
}
/// Appends to the outbox inside the caller's transaction, so the record
/// commits or rolls back with the caller's own writes.
///
/// Contract: write only through `transaction`; never commit it and never
/// deliver before commit. Refuse oversized payloads with a permanent failure.
pub trait Outbox: Send + Sync {
    fn append<'a>(
        &'a self,
        transaction: &'a Transaction<'a>,
        message: OutboxMessage,
    ) -> BoxFuture<'a, Result<(), Failure>>;
}
/// Fallback without the jobs block: refuses with `outbox_unavailable` so the
/// caller's transaction rolls back instead of losing the record.
#[derive(Debug, Default)]
pub struct UnavailableOutbox;
impl Outbox for UnavailableOutbox {
    fn append<'a>(
        &'a self,
        _: &'a Transaction<'a>,
        _: OutboxMessage,
    ) -> BoxFuture<'a, Result<(), Failure>> {
        Box::pin(async { Err(Failure::permanent("outbox_unavailable")) })
    }
}

/// A stored object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Blob {
    pub content_type: String,
    pub bytes: Vec<u8>,
}
/// Media storage by key.
///
/// Contract: `put` replaces atomically, `get` returns `None` for a missing
/// key, `delete` reports whether a key existed. Keys are validated
/// identities, never paths taken from users. Implementations bound object
/// size and fail permanent `blob_too_large` above it.
pub trait BlobStore: Send + Sync {
    fn put<'a>(&'a self, key: &'a Identity, blob: Blob) -> BoxFuture<'a, Result<(), Failure>>;
    fn get<'a>(&'a self, key: &'a Identity) -> BoxFuture<'a, Result<Option<Blob>, Failure>>;
    fn delete<'a>(&'a self, key: &'a Identity) -> BoxFuture<'a, Result<bool, Failure>>;
}
/// Fallback without the content block: every operation fails
/// `blob_store_unavailable`.
#[derive(Debug, Default)]
pub struct UnavailableBlobStore;
impl BlobStore for UnavailableBlobStore {
    fn put<'a>(&'a self, _: &'a Identity, _: Blob) -> BoxFuture<'a, Result<(), Failure>> {
        Box::pin(async { Err(Failure::permanent("blob_store_unavailable")) })
    }
    fn get<'a>(&'a self, _: &'a Identity) -> BoxFuture<'a, Result<Option<Blob>, Failure>> {
        Box::pin(async { Err(Failure::permanent("blob_store_unavailable")) })
    }
    fn delete<'a>(&'a self, _: &'a Identity) -> BoxFuture<'a, Result<bool, Failure>> {
        Box::pin(async { Err(Failure::permanent("blob_store_unavailable")) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: &str) -> Identity {
        Identity::new(value).unwrap()
    }
    #[test]
    fn allow_all_exists_only_on_loopback() {
        let local = LoopbackAllowAll::new("127.0.0.1:1".parse().unwrap()).unwrap();
        assert!(
            local
                .check(None, &id("tasks.create"), &ResourceScope::Global)
                .is_ok()
        );
        assert!(LoopbackAllowAll::new("[::1]:1".parse().unwrap()).is_ok());
        assert_eq!(
            LoopbackAllowAll::new("0.0.0.0:1".parse().unwrap())
                .unwrap_err()
                .code,
            "allow_all_requires_loopback"
        );
        assert_eq!(
            DenyAll
                .check(None, &id("tasks.create"), &ResourceScope::Global)
                .unwrap_err()
                .code,
            "unauthorized"
        );
    }
    #[tokio::test]
    async fn capture_notifier_keeps_a_bounded_redacted_record() {
        let notifier = CaptureNotifier::default();
        for n in 0..70 {
            let request = NotificationRequest {
                recipient: Recipient::Address {
                    channel: id("email"),
                    address: "ada@example.com".into(),
                },
                channels: vec![id("email")],
                template: id("welcome"),
                data: serde_json::json!({"code": "123456"}),
                idempotency_key: id(&format!("k{n}")),
            };
            assert!(!format!("{request:?}").contains("ada@example.com"));
            assert!(!format!("{request:?}").contains("123456"));
            notifier.notify(request).await.unwrap();
        }
        let captured = notifier.captured();
        assert_eq!(captured.len(), CaptureNotifier::CAPACITY);
        assert_eq!(captured[0].idempotency_key, id("k6"));
        assert_eq!(captured[0].recipient_kind, "address");
    }
    #[tokio::test]
    async fn unavailable_blob_store_refuses() {
        let store = UnavailableBlobStore;
        assert_eq!(
            store.get(&id("a")).await.unwrap_err().code,
            "blob_store_unavailable"
        );
    }
}
