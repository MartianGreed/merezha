//! Guest-safe domain definitions and the application protocol messages,
//! versions 1 and 2. This crate depends only on Serde and JSON, never on
//! the host runtime.
//!
//! Version 2 adds the host-established [`Principal`] to [`Metadata`], the
//! [`Operation::Describe`] handshake (answered by [`handle`] itself) and the
//! projection `partition`. Version 1 JSON is byte-identical to what it was
//! before version 2 existed: every addition is omitted when absent.
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

pub const PROTOCOL_V1: u16 = 1;
pub const PROTOCOL_V2: u16 = 2;
/// The newest protocol this SDK speaks; [`handle`] answers `describe` with it.
pub const LATEST_PROTOCOL: u16 = PROTOCOL_V2;
#[deprecated(note = "use PROTOCOL_V1")]
pub const PROTOCOL_VERSION: u16 = PROTOCOL_V1;
/// Most role keys a [`Principal`] carries.
pub const MAX_PRINCIPAL_ROLES: usize = 16;
/// Longest role key a [`Principal`] carries, in bytes.
pub const MAX_ROLE_BYTES: usize = 64;
pub const MAX_WIRE_BYTES: usize = 1_048_576;
pub const MAX_EVENTS: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Classification {
    Permanent,
    Transient,
    Conflict,
}

/// Codes are bounded identifiers. Error text and guest payloads are never logs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Failure {
    pub code: String,
    pub classification: Classification,
}
impl Failure {
    pub fn new(code: &str, classification: Classification) -> Self {
        Self {
            code: if valid_name(code) {
                code.to_owned()
            } else {
                "invalid_error".into()
            },
            classification,
        }
    }
    pub fn permanent(code: &str) -> Self {
        Self::new(code, Classification::Permanent)
    }
    pub fn conflict(code: &str) -> Self {
        Self::new(code, Classification::Conflict)
    }
    pub fn transient(code: &str) -> Self {
        Self::new(code, Classification::Transient)
    }
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.code)
    }
}
impl std::error::Error for Failure {}

/// Portable identity, validated on construction and deserialization.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Identity(String);
impl Identity {
    pub fn new(value: impl Into<String>) -> Result<Self, Failure> {
        Self::try_from(value.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for Identity {
    type Error = Failure;
    fn try_from(value: String) -> Result<Self, Failure> {
        if valid_name(&value) {
            Ok(Self(value))
        } else {
            Err(Failure::permanent("validation_failed"))
        }
    }
}
impl From<Identity> for String {
    fn from(value: Identity) -> String {
        value.0
    }
}
pub fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-:/".contains(&b))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    pub message_id: Identity,
    pub correlation_id: Identity,
    #[serde(default)]
    pub causation_id: Option<Identity>,
    #[serde(default)]
    pub actor: Option<Identity>,
    /// Version 2 only, set by the host and never by a caller: who the host
    /// authenticated. When present, `actor` equals its subject. Omitted
    /// when absent, so version 1 metadata keeps its exact encoding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<Principal>,
}
impl Metadata {
    /// The host-established principal, `None` for anonymous calls and for
    /// every version 1 request.
    pub fn principal(&self) -> Option<&Principal> {
        self.principal.as_ref()
    }
    /// The principal's subject, or `unauthenticated`.
    pub fn require_subject(&self) -> Result<&Identity, Failure> {
        self.principal
            .as_ref()
            .map(|principal| &principal.subject)
            .ok_or_else(|| Failure::permanent("unauthenticated"))
    }
}

/// What kind of party a [`Principal`] is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    User,
    ApiKey,
    /// The host itself (`sys_<name>` subjects), such as a reactor.
    System,
}

/// The guest's view of an authenticated party (protocol version 2): what
/// the host established, limited to what ownership rules need. Guests
/// never authenticate and never see sessions, grants or secrets.
///
/// `roles` holds the role keys at the route's scope, at most
/// [`MAX_PRINCIPAL_ROLES`] of at most [`MAX_ROLE_BYTES`] bytes, sorted and
/// deduplicated. Stored event metadata keeps no roles, so an empty list is
/// omitted from JSON, and so is a missing tenant.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "PrincipalWire")]
pub struct Principal {
    pub subject: Identity,
    pub kind: PrincipalKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<Identity>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub roles: Vec<Identity>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PrincipalWire {
    subject: Identity,
    kind: PrincipalKind,
    #[serde(default)]
    tenant: Option<Identity>,
    #[serde(default)]
    roles: Vec<Identity>,
}
impl TryFrom<PrincipalWire> for Principal {
    type Error = Failure;
    fn try_from(wire: PrincipalWire) -> Result<Self, Failure> {
        Self::new(wire.subject, wire.kind, wire.tenant, wire.roles)
    }
}
impl Principal {
    /// Sorts and deduplicates `roles`. Fails `invalid_principal` past
    /// [`MAX_PRINCIPAL_ROLES`] roles or a role longer than
    /// [`MAX_ROLE_BYTES`].
    pub fn new(
        subject: Identity,
        kind: PrincipalKind,
        tenant: Option<Identity>,
        mut roles: Vec<Identity>,
    ) -> Result<Self, Failure> {
        roles.sort();
        roles.dedup();
        let principal = Self {
            subject,
            kind,
            tenant,
            roles,
        };
        principal.check()?;
        Ok(principal)
    }
    /// The bounds [`Principal::new`] enforces, for a value built from its
    /// public fields: `invalid_principal` unless the roles are within
    /// bounds, sorted and deduplicated.
    pub fn check(&self) -> Result<(), Failure> {
        if self.roles.len() > MAX_PRINCIPAL_ROLES
            || self.roles.iter().any(|r| r.as_str().len() > MAX_ROLE_BYTES)
            || self.roles.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(Failure::permanent("invalid_principal"));
        }
        Ok(())
    }
    /// Whether the principal holds role `role` at the route's scope.
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r.as_str() == role)
    }
    /// The principal's tenant, or `tenant_required`.
    pub fn require_tenant(&self) -> Result<&Identity, Failure> {
        self.tenant
            .as_ref()
            .ok_or_else(|| Failure::permanent("tenant_required"))
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    Command,
    Query,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub tag: Identity,
    pub schema_version: u16,
    pub payload: Value,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredEvent {
    pub event_id: Identity,
    pub stream: Identity,
    pub version: u64,
    pub position: u64,
    pub event: Event,
    pub metadata: Metadata,
    pub accepted_unix_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: u16,
    pub body: Operation,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    Validate {
        kind: MessageKind,
        tag: Identity,
        payload: Value,
    },
    Decide {
        tag: Identity,
        stream: Identity,
        payload: Value,
        history: Vec<StoredEvent>,
        metadata: Metadata,
    },
    Project {
        projection: Identity,
        state: Value,
        events: Vec<StoredEvent>,
        live: bool,
        /// Version 2 only: the tenant of a partitioned projection's batch.
        /// Omitted when absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        partition: Option<Identity>,
    },
    Query {
        tag: Identity,
        payload: Value,
        state: Value,
        metadata: Metadata,
    },
    /// Version 2 only: the host asks which protocol the guest speaks.
    /// [`handle`] answers it with [`Reply::Described`] without calling the
    /// guest's dispatcher.
    Describe {},
    /// Version 2 only: a reactor's batch of events, in position order, the
    /// ones whose tag the reactor's `events` filter keeps. The guest answers
    /// [`Reply::Reacted`] with exactly one [`Reaction`] per event, in order.
    /// Reactors never see projection state.
    React {
        reactor: Identity,
        events: Vec<StoredEvent>,
    },
}
impl Operation {
    /// Whether this operation can travel in a request of `version`:
    /// version 1 carries no `describe`, no `react`, no `partition` and no
    /// principal, neither in decide or query metadata nor in any event's
    /// metadata.
    pub fn fits(&self, version: u16) -> bool {
        match version {
            PROTOCOL_V2 => true,
            PROTOCOL_V1 => {
                let clean =
                    |events: &[StoredEvent]| events.iter().all(|e| e.metadata.principal.is_none());
                match self {
                    Self::Validate { .. } => true,
                    Self::Decide {
                        history, metadata, ..
                    } => metadata.principal.is_none() && clean(history),
                    Self::Project {
                        events, partition, ..
                    } => partition.is_none() && clean(events),
                    Self::Query { metadata, .. } => metadata.principal.is_none(),
                    Self::Describe {} | Self::React { .. } => false,
                }
            }
            _ => false,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Reply {
    Validated {
        payload: Value,
    },
    Decided {
        events: Vec<Event>,
    },
    Projected {
        state: Value,
    },
    Queried {
        value: Value,
    },
    /// The answer to [`Operation::Describe`].
    Described {
        protocol: u16,
    },
    /// The answer to [`Operation::React`]: one reaction per input event,
    /// in order.
    Reacted {
        reactions: Vec<Reaction>,
    },
}

/// Most effects one event's [`Reaction`] may carry.
pub const MAX_EFFECTS_PER_EVENT: usize = 8;
/// Most effects one [`Reply::Reacted`] may carry in total.
pub const MAX_EFFECTS_PER_BATCH: usize = 64;
/// Most recipients one [`Effect::Notify`] may name.
pub const MAX_EFFECT_RECIPIENTS: usize = 100;
/// The longest `delay_ms` an effect may ask for: 30 days.
pub const MAX_EFFECT_DELAY_MS: u64 = 2_592_000_000;

/// What a reactor asks the host to do about one event (AP §C.2). Guests
/// never name the dedupe key, the tenant or the source event: the host
/// derives them from the event.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reaction {
    /// The [`StoredEvent::event_id`] this reaction answers.
    pub event_id: Identity,
    /// At most [`MAX_EFFECTS_PER_EVENT`], in order; an effect's index in
    /// this list keys it.
    pub effects: Vec<Effect>,
}
/// One effect, in the notifications block's vocabulary (R4, NT §3.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "effect", rename_all = "snake_case", deny_unknown_fields)]
pub enum Effect {
    /// A notification on a declared topic.
    Notify {
        topic: Identity,
        /// 1 to [`MAX_EFFECT_RECIPIENTS`].
        recipients: Vec<Recipient>,
        /// A subset of the topic's channels; the topic's defaults when
        /// absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        channels: Option<Vec<Channel>>,
        #[serde(default)]
        data: serde_json::Map<String, Value>,
        /// 0 to [`MAX_EFFECT_DELAY_MS`].
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delay_ms: Option<u64>,
    },
    /// An event for the registered webhook endpoints of the event's tenant,
    /// or the application-level ones for an event without tenant. Guests
    /// never name a URL.
    Webhook { event_type: Identity, data: Value },
    /// A command the host runs later as the reactor's system principal,
    /// on the event's tenant.
    Command {
        tag: Identity,
        /// The local stream id; the host composes the stream.
        stream: Identity,
        payload: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_version: Option<u64>,
        /// 0 to [`MAX_EFFECT_DELAY_MS`].
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delay_ms: Option<u64>,
    },
}
impl Effect {
    /// `notify`, `webhook` or `command`.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Notify { .. } => "notify",
            Self::Webhook { .. } => "webhook",
            Self::Command { .. } => "command",
        }
    }
}
/// Who a notify effect is for. `{"user":{"id"}}`, `{"tenant":{"id",
/// "roles"?}}`, `{"application":{"roles"}}` or `{"address":{"email"}}` /
/// `{"address":{"phone"}}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Recipient {
    User {
        id: Identity,
    },
    /// Members of the event's tenant, optionally only those with one of
    /// `roles`.
    Tenant {
        id: Identity,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        roles: Option<Vec<Identity>>,
    },
    /// Holders of application-scope roles, at least one.
    Application {
        roles: Vec<Identity>,
    },
    /// An explicit address, only for a reactor with `allow_addresses`.
    Address(Address),
}
/// An explicit address: personal data, so `Debug` never shows it.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Address {
    Email(EmailAddress),
    Phone(PhoneAddress),
}
/// `{"email": "..."}`.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmailAddress {
    pub email: String,
}
/// `{"phone": "+33..."}`, E.164.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhoneAddress {
    pub phone: String,
}
impl std::fmt::Debug for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Email(_) => f.write_str("Email(<redacted>)"),
            Self::Phone(_) => f.write_str("Phone(<redacted>)"),
        }
    }
}
/// A delivery channel of a notify effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Channel {
    Email,
    Sms,
    Push,
    Inbox,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    pub version: u16,
    pub outcome: Outcome,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum Outcome {
    Ok { reply: Reply },
    Err { error: Failure },
}
impl Request {
    /// A version 1 request.
    pub fn new(body: Operation) -> Self {
        Self::with_version(PROTOCOL_V1, body)
    }
    /// A request of `version`. The caller makes `body` fit it
    /// ([`Operation::fits`]); [`Request::decode`] refuses one that does not.
    pub fn with_version(version: u16, body: Operation) -> Self {
        Self { version, body }
    }
    /// Decodes a version 1 or 2 request. Any other version, and a version 1
    /// request carrying a version 2 addition, fails `protocol_version`.
    pub fn decode(input: &str) -> Result<Self, Failure> {
        let request: Self = decode(input)?;
        if !request.body.fits(request.version) {
            return Err(Failure::permanent("protocol_version"));
        }
        Ok(request)
    }
}
impl Response {
    /// A version 1 response.
    pub fn new(result: Result<Reply, Failure>) -> Self {
        Self::new_with_version(PROTOCOL_V1, result)
    }
    /// A response of `version`, which must echo the request's.
    pub fn new_with_version(version: u16, result: Result<Reply, Failure>) -> Self {
        Self {
            version,
            outcome: match result {
                Ok(reply) => Outcome::Ok { reply },
                Err(error) => Outcome::Err { error },
            },
        }
    }
    /// Encodes a version 1 response.
    pub fn encode(result: Result<Reply, Failure>) -> Result<String, Failure> {
        encode(&Self::new(result))
    }
    /// Decodes a version 1 response.
    pub fn decode(input: &str) -> Result<Reply, Failure> {
        Self::decode_version(input, PROTOCOL_V1)
    }
    /// Decodes a response that must echo `expected`, the request's version:
    /// `protocol_version` otherwise, `invalid_response` for a failure whose
    /// code is not a valid name. For hosts.
    pub fn decode_version(input: &str, expected: u16) -> Result<Reply, Failure> {
        let response: Self = decode(input)?;
        if response.version != expected {
            return Err(Failure::permanent("protocol_version"));
        }
        match response.outcome {
            Outcome::Ok { reply } => Ok(reply),
            Outcome::Err { error } if valid_name(&error.code) => Err(error),
            _ => Err(Failure::permanent("invalid_response")),
        }
    }
}

/// Typed decode followed by encode is the validated idempotency payload.
pub fn validate_payload<T: DeserializeOwned + Serialize>(payload: Value) -> Result<Reply, Failure> {
    compatible_json(&payload)?;
    let typed: T =
        serde_json::from_value(payload).map_err(|_| Failure::permanent("validation_failed"))?;
    let payload =
        serde_json::to_value(typed).map_err(|_| Failure::permanent("serialization_failed"))?;
    compatible_json(&payload)?;
    Ok(Reply::Validated { payload })
}

/// Integral numbers stay inside the IEEE-754 safe-integer range; decimal
/// fractions use Serde JSON's binary64 representation, not arbitrary precision.
pub fn compatible_json(value: &Value) -> Result<(), Failure> {
    match value {
        Value::Number(n) => {
            let v = n
                .as_f64()
                .ok_or_else(|| Failure::permanent("invalid_number"))?;
            if !v.is_finite() || (v.fract() == 0.0 && v.abs() > 9_007_199_254_740_991.0) {
                return Err(Failure::permanent("invalid_number"));
            }
        }
        Value::Array(values) => {
            for value in values {
                compatible_json(value)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                compatible_json(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}
pub fn decode<T: DeserializeOwned>(input: &str) -> Result<T, Failure> {
    if input.len() > MAX_WIRE_BYTES {
        return Err(Failure::permanent("input_limit"));
    }
    serde_json::from_str(input).map_err(|_| Failure::permanent("validation_failed"))
}
pub fn encode<T: Serialize>(value: &T) -> Result<String, Failure> {
    let output =
        serde_json::to_string(value).map_err(|_| Failure::permanent("serialization_failed"))?;
    if output.len() > MAX_WIRE_BYTES {
        return Err(Failure::permanent("output_limit"));
    }
    Ok(output)
}

/// Pure business decisions and deterministic evolution belong in the guest.
pub trait Decider {
    type State;
    type Command: DeserializeOwned + Serialize;
    type Event: DeserializeOwned + Serialize;
    fn initial(&self) -> Self::State;
    fn decide(
        &self,
        state: &Self::State,
        command: Self::Command,
    ) -> Result<Vec<Self::Event>, Failure>;
    fn evolve(&self, state: Self::State, event: Self::Event) -> Result<Self::State, Failure>;
}
pub fn rehydrate<D: Decider>(
    decider: &D,
    events: impl IntoIterator<Item = D::Event>,
) -> Result<D::State, Failure> {
    events
        .into_iter()
        .try_fold(decider.initial(), |state, event| {
            decider.evolve(state, event)
        })
}

/// Calls a guest dispatcher and encodes classified failures as successful ABI
/// strings. ABI Err remains reserved for transport/component failures.
///
/// The response echoes the request's version (version 1 when the input
/// names no supported version). `describe` is answered here with
/// [`LATEST_PROTOCOL`], without calling `dispatch`.
pub fn handle(
    input: &str,
    dispatch: impl FnOnce(Operation) -> Result<Reply, Failure>,
) -> Result<String, String> {
    let (version, result) = match Request::decode(input) {
        Ok(Request {
            version,
            body: Operation::Describe {},
        }) => (
            version,
            Ok(Reply::Described {
                protocol: LATEST_PROTOCOL,
            }),
        ),
        Ok(request) => (request.version, dispatch(request.body)),
        Err(failure) => (requested_version(input), Err(failure)),
    };
    encode(&Response::new_with_version(version, result)).map_err(|failure| failure.code)
}
/// The supported version a request that failed to decode names, so its
/// error travels in the version the host expects; version 1 otherwise.
fn requested_version(input: &str) -> u16 {
    #[derive(Deserialize)]
    struct Version {
        version: u16,
    }
    match decode::<Version>(input) {
        Ok(Version {
            version: PROTOCOL_V2,
        }) => PROTOCOL_V2,
        _ => PROTOCOL_V1,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Command {
    pub tag: Identity,
    pub stream: Identity,
    pub payload: Value,
    pub metadata: Metadata,
    #[serde(default)]
    pub idempotency_key: Option<Identity>,
    #[serde(default)]
    pub expected_version: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub tag: Identity,
    pub payload: Value,
    pub metadata: Metadata,
    /// Read-your-writes: the host waits, within the request deadline, until
    /// the projection's checkpoint reaches this position (a command
    /// receipt's `position`), else fails `projection_behind`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_position: Option<u64>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub stream: Identity,
    pub version: u64,
    pub position: u64,
    pub events: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn numeric_compatibility_has_explicit_safe_integer_boundaries() {
        assert!(compatible_json(&json!(9_007_199_254_740_991_u64)).is_ok());
        assert!(compatible_json(&json!(9_007_199_254_740_992_u64)).is_err());
        assert!(compatible_json(&json!(-9_007_199_254_740_992_i64)).is_err());
        assert!(compatible_json(&json!({"fraction":0.1})).is_ok());
        assert!(validate_payload::<u64>(json!(1.0)).is_err());
    }
    struct Counter;
    impl Decider for Counter {
        type State = i64;
        type Command = i64;
        type Event = i64;
        fn initial(&self) -> i64 {
            0
        }
        fn decide(&self, state: &i64, command: i64) -> Result<Vec<i64>, Failure> {
            if state + command < 0 {
                Err(Failure::permanent("invariant_violation"))
            } else {
                Ok(vec![command])
            }
        }
        fn evolve(&self, state: i64, event: i64) -> Result<i64, Failure> {
            Ok(state + event)
        }
    }
    #[test]
    fn identity_cannot_bypass_validation() {
        assert!(Identity::new("").is_err());
        assert!(serde_json::from_str::<Identity>("\"bad space\"").is_err());
        assert!(Identity::new("invoice-1").is_ok());
    }
    #[test]
    fn decisions_reject_invariants_and_replay_deterministically() {
        let state = rehydrate(&Counter, [10, -3]).unwrap();
        assert_eq!(state, 7);
        assert!(Counter.decide(&state, -8).is_err());
        assert_eq!(rehydrate(&Counter, [10, -3, 2]).unwrap(), 9);
    }
    #[test]
    fn versions_errors_and_typed_validation_cross_the_wire() {
        assert!(
            Request::decode(
                r#"{"version":3,"body":{"op":"validate","kind":"command","tag":"Add","payload":1}}"#
            )
            .is_err()
        );
        assert_eq!(
            Response::decode(&Response::encode(Err(Failure::conflict("conflict"))).unwrap()),
            Err(Failure::conflict("conflict"))
        );
        assert!(validate_payload::<u32>(json!(-1)).is_err());
        assert_eq!(
            validate_payload::<u32>(json!(3)).unwrap(),
            Reply::Validated { payload: json!(3) }
        );
    }
}
