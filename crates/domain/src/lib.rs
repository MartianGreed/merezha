//! Guest-safe domain definitions and version 1 application messages.
//! This crate depends only on Serde and JSON, never on the host runtime.
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

pub const PROTOCOL_VERSION: u16 = 1;
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
    },
    Query {
        tag: Identity,
        payload: Value,
        state: Value,
        metadata: Metadata,
    },
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Reply {
    Validated { payload: Value },
    Decided { events: Vec<Event> },
    Projected { state: Value },
    Queried { value: Value },
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
    pub fn new(body: Operation) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            body,
        }
    }
    pub fn decode(input: &str) -> Result<Self, Failure> {
        let request: Self = decode(input)?;
        if request.version != PROTOCOL_VERSION {
            return Err(Failure::permanent("protocol_version"));
        }
        Ok(request)
    }
}
impl Response {
    pub fn new(result: Result<Reply, Failure>) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            outcome: match result {
                Ok(reply) => Outcome::Ok { reply },
                Err(error) => Outcome::Err { error },
            },
        }
    }
    pub fn encode(result: Result<Reply, Failure>) -> Result<String, Failure> {
        encode(&Self::new(result))
    }
    pub fn decode(input: &str) -> Result<Reply, Failure> {
        let response: Self = decode(input)?;
        if response.version != PROTOCOL_VERSION {
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
pub fn handle(
    input: &str,
    dispatch: impl FnOnce(Operation) -> Result<Reply, Failure>,
) -> Result<String, String> {
    let result = Request::decode(input).and_then(|request| dispatch(request.body));
    Response::encode(result).map_err(|failure| failure.code)
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
                r#"{"version":2,"body":{"op":"validate","kind":"command","tag":"Add","payload":1}}"#
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
