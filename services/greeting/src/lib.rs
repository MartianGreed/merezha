//! `greeting`: adds a localized greeting for a name to a JSON object.
//!
//! Input: an object with a string `name`, or `text` when `name` is absent,
//! and an optional string `locale` (`en`, the default, `fr`, `es`, `de` or
//! `uk`). The service writes `greeting` and `greeting_style` and copies every
//! other field unchanged; an existing `greeting` is replaced.
//!
//! The default build is the casual revision. Building with the `formal`
//! feature produces the formal revision, a drop-in replacement with the same
//! contract and different wording.
//! See `docs/services/README.md` for the full contract.

use merezha_service_kit::{ErrorCode, Limits, Object, Service, ServiceError, Value};

/// Identity and work bounds of the service.
pub const SERVICE: Service = Service {
    name: "greeting",
    limits: Limits {
        max_input_bytes: 32 * 1024,
        max_depth: 16,
        max_fields: 64,
    },
};

/// Longest accepted name, in Unicode scalar values after trimming.
pub const MAX_NAME_CHARS: usize = 128;

/// The value written to `greeting_style` by this build.
pub const STYLE: &str = if cfg!(feature = "formal") {
    "formal"
} else {
    "casual"
};

/// Supported locales and their greeting as `(prefix, suffix)` around the name.
const LOCALES: [(&str, (&str, &str)); 5] = if cfg!(feature = "formal") {
    [
        ("en", ("Good day, ", ".")),
        ("fr", ("Bien le bonjour, ", ".")),
        ("es", ("Buenos días, ", ".")),
        ("de", ("Guten Tag, ", ".")),
        ("uk", ("Добрий день, ", ".")),
    ]
} else {
    [
        ("en", ("Hello, ", "!")),
        ("fr", ("Bonjour, ", " !")),
        ("es", ("¡Hola, ", "!")),
        ("de", ("Hallo, ", "!")),
        ("uk", ("Привіт, ", "!")),
    ]
};

/// The `handle` export: adds `greeting` and `greeting_style`.
///
/// # Errors
///
/// Returns a JSON error document when the input is malformed or exceeds
/// [`SERVICE`]'s limits, no usable name is present, the name is blank,
/// longer than [`MAX_NAME_CHARS`] or contains control characters, or the
/// locale is not supported.
pub fn handle(input: &str) -> Result<String, String> {
    SERVICE.run(input, greet)
}

fn greet(mut object: Object) -> Result<Value, ServiceError> {
    let (field, raw_name) = match SERVICE.optional_str(&object, "name")? {
        Some(name) => ("name", name),
        None => match SERVICE.optional_str(&object, "text")? {
            Some(text) => ("text", text),
            None => {
                return Err(SERVICE
                    .error(
                        ErrorCode::MissingField,
                        "field `name` (or `text`) is required and must be a string",
                    )
                    .with_field("name"));
            }
        },
    };
    let name = validate_name(field, raw_name)?;
    let (prefix, suffix) = locale(SERVICE.optional_str(&object, "locale")?.unwrap_or("en"))?;
    let greeting = format!("{prefix}{name}{suffix}");
    object.insert("greeting".to_owned(), Value::String(greeting));
    object.insert("greeting_style".to_owned(), Value::from(STYLE));
    Ok(Value::Object(object))
}

fn validate_name<'a>(field: &str, raw: &'a str) -> Result<&'a str, ServiceError> {
    let name = raw.trim();
    let invalid = |code, message: String| Err(SERVICE.error(code, message).with_field(field));
    if name.is_empty() {
        return invalid(
            ErrorCode::InvalidValue,
            format!("field `{field}` is blank; provide a name to greet"),
        );
    }
    let length = name.chars().count();
    if length > MAX_NAME_CHARS {
        return invalid(
            ErrorCode::LimitExceeded,
            format!(
                "field `{field}` has {length} characters; greeting accepts at most {MAX_NAME_CHARS}"
            ),
        );
    }
    if name.chars().any(char::is_control) {
        return invalid(
            ErrorCode::InvalidValue,
            format!(
                "field `{field}` contains control characters; normalize it first, for example with text-normalize"
            ),
        );
    }
    Ok(name)
}

fn locale(code: &str) -> Result<(&'static str, &'static str), ServiceError> {
    LOCALES
        .iter()
        .find(|(candidate, _)| *candidate == code)
        .map(|(_, parts)| *parts)
        .ok_or_else(|| {
            let expected: Vec<String> = LOCALES
                .iter()
                .map(|(code, _)| format!("{code:?}"))
                .collect();
            SERVICE
                .error(
                    ErrorCode::InvalidValue,
                    format!(
                        "field `locale` is {code:?}; expected one of {}",
                        expected.join(", ")
                    ),
                )
                .with_field("locale")
        })
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
