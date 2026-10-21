//! `json-envelope`: wraps a JSON object in a versioned envelope.
//!
//! Input: any JSON object. An optional string `kind` (1 to 64 characters
//! from `a-z`, `0-9`, `.`, `_`, `-`; default `document`) is moved out of
//! the data into the envelope. The output is
//! `{"data":{…},"kind":…,"meta":{"data_bytes":…,"digest":"fnv1a64:…","fields":…},"version":1}`,
//! where `data_bytes` and `digest` describe `data` exactly as emitted: top-level
//! keys sorted, every value as received without insignificant whitespace. The digest identifies content; it is not a security
//! checksum. See `docs/services/README.md` for the full contract.

use merezha_service_kit::{Document, ErrorCode, Limits, Service, ServiceError};

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

fn wrap(mut data: Document) -> Result<Document, ServiceError> {
    let kind = SERVICE
        .optional_str(&data, "kind")?
        .unwrap_or_else(|| DEFAULT_KIND.to_owned());
    validate_kind(&kind)?;
    data.remove("kind");
    let serialized = data.to_json();

    let mut meta = Document::new();
    meta.insert_u64("data_bytes", serialized.len() as u64);
    meta.insert_str(
        "digest",
        &format!("fnv1a64:{:016x}", fnv1a64(serialized.as_bytes())),
    );
    meta.insert_u64("fields", data.len() as u64);

    let mut envelope = Document::new();
    envelope.insert_document("data", &data);
    envelope.insert_str("kind", &kind);
    envelope.insert_document("meta", &meta);
    envelope.insert_u64("version", VERSION);
    Ok(envelope)
}

fn validate_kind(kind: &str) -> Result<(), ServiceError> {
    let allowed = |byte: u8| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
    };
    if (1..=MAX_KIND_BYTES).contains(&kind.len()) && kind.bytes().all(allowed) {
        Ok(())
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
