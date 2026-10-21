//! `json-envelope`: wraps a JSON object in a versioned envelope.
//!
//! Input: any JSON object. An optional string `kind` (1 to 64 characters
//! from `a-z`, `0-9`, `.`, `_`, `-`; default `document`) is moved out of
//! the data into the envelope. The output is
//! `{"data":{…},"kind":…,"meta":{"data_bytes":…,"digest":"fnv1a64:…","fields":…},"version":1}`,
//! where `data_bytes` and `digest` describe the serialized `data` with keys
//! in sorted order. The digest identifies content; it is not a security
//! checksum. See `docs/services/README.md` for the full contract.

use merezha_service_kit::{ErrorCode, Limits, Object, Service, ServiceError, Value};

/// Identity and work bounds of the service.
pub const SERVICE: Service = Service {
    name: "json-envelope",
    limits: Limits {
        max_input_bytes: 256 * 1024,
        max_depth: 32,
        max_fields: 256,
    },
};

/// Envelope format version written to `version`.
pub const VERSION: u64 = 1;

const DEFAULT_KIND: &str = "document";
const MAX_KIND_BYTES: usize = 64;

/// 64-bit FNV-1a hash of `bytes`.
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    bytes.iter().fold(OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
    })
}

/// The `handle` export: wraps the given JSON object.
///
/// # Errors
///
/// Returns a JSON error document when the input is malformed or exceeds
/// [`SERVICE`]'s limits, or `kind` is not a valid kind name.
pub fn handle(input: &str) -> Result<String, String> {
    SERVICE.run(input, wrap)
}

fn wrap(mut data: Object) -> Result<Value, ServiceError> {
    let kind =
        validate_kind(SERVICE.optional_str(&data, "kind")?.unwrap_or(DEFAULT_KIND))?.to_owned();
    data.remove("kind");
    let fields = data.len();
    let data = Value::Object(data);
    let serialized = data.to_string();

    let mut meta = Object::new();
    meta.insert("data_bytes".to_owned(), Value::from(serialized.len()));
    meta.insert(
        "digest".to_owned(),
        Value::String(format!("fnv1a64:{:016x}", fnv1a64(serialized.as_bytes()))),
    );
    meta.insert("fields".to_owned(), Value::from(fields));

    let mut envelope = Object::new();
    envelope.insert("data".to_owned(), data);
    envelope.insert("kind".to_owned(), Value::String(kind));
    envelope.insert("meta".to_owned(), Value::Object(meta));
    envelope.insert("version".to_owned(), Value::from(VERSION));
    Ok(Value::Object(envelope))
}

fn validate_kind(kind: &str) -> Result<&str, ServiceError> {
    let allowed = |byte: u8| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
    };
    if (1..=MAX_KIND_BYTES).contains(&kind.len()) && kind.bytes().all(allowed) {
        Ok(kind)
    } else {
        Err(SERVICE
            .error(
                ErrorCode::InvalidValue,
                format!(
                    "field `kind` is {kind:?}; expected 1 to {MAX_KIND_BYTES} characters from a-z, 0-9, `.`, `_` and `-`"
                ),
            )
            .with_field("kind"))
    }
}

#[cfg(target_arch = "wasm32")]
mod component {
    wit_bindgen::generate!({ path: "../../wit", world: "service" });

    struct Component;

    impl exports::merezha::service::handler::Guest for Component {
        fn handle(input: String) -> Result<String, String> {
            super::handle(&input)
        }
    }

    export!(Component);
}
