//! `text-normalize`: cleans the `text` field of a JSON object.
//!
//! Input: an object with a required string `text` and an optional string
//! `case` (`keep`, the default, `lower`, `upper` or `title`). The service
//! drops control characters, trims the text, collapses every run of Unicode
//! whitespace into one space and applies the case. It replaces `text` with
//! the result and copies every other field, including `case`, unchanged.
//! See `docs/services/README.md` for the full contract.

use merezha_service_kit::{Document, ErrorCode, Limits, Service, ServiceError};

/// Identity and work bounds of the service.
pub const SERVICE: Service = Service {
    name: "text-normalize",
    limits: Limits {
        max_input_bytes: 32 * 1024,
        max_depth: 16,
        max_fields: 64,
    },
};

/// The `handle` export: normalizes `text` in the given JSON object.
///
/// # Errors
///
/// Returns a JSON error document when the input is malformed or exceeds
/// [`SERVICE`]'s limits, `text` is missing, not a string or empty after
/// normalization, or `case` is not supported.
pub fn handle(input: &str) -> Result<String, String> {
    SERVICE.run(input, normalize)
}

#[derive(Debug, Clone, Copy)]
enum Case {
    Keep,
    Lower,
    Upper,
    Title,
}

impl Case {
    const NAMES: [(&'static str, Case); 4] = [
        ("keep", Case::Keep),
        ("lower", Case::Lower),
        ("upper", Case::Upper),
        ("title", Case::Title),
    ];

    fn parse(name: Option<&str>) -> Result<Self, ServiceError> {
        let Some(name) = name else {
            return Ok(Self::Keep);
        };
        Self::NAMES
            .iter()
            .find(|(candidate, _)| *candidate == name)
            .map(|(_, case)| *case)
            .ok_or_else(|| {
                let expected: Vec<String> = Self::NAMES
                    .iter()
                    .map(|(name, _)| format!("{name:?}"))
                    .collect();
                SERVICE
                    .error(
                        ErrorCode::InvalidValue,
                        format!(
                            "field `case` is {name:?}; expected one of {}",
                            expected.join(", ")
                        ),
                    )
                    .with_field("case")
            })
    }

    fn apply(self, word: &str, output: &mut String) {
        match self {
            Self::Keep => output.push_str(word),
            Self::Lower => output.extend(word.chars().flat_map(char::to_lowercase)),
            Self::Upper => output.extend(word.chars().flat_map(char::to_uppercase)),
            Self::Title => {
                let mut chars = word.chars();
                if let Some(first) = chars.next() {
                    output.extend(first.to_uppercase());
                    output.extend(chars.flat_map(char::to_lowercase));
                }
            }
        }
    }
}

fn normalize(mut document: Document) -> Result<Document, ServiceError> {
    let case = Case::parse(SERVICE.optional_str(&document, "case")?.as_deref())?;
    let text = normalize_text(&SERVICE.required_str(&document, "text")?, case);
    if text.is_empty() {
        return Err(SERVICE
            .error(
                ErrorCode::InvalidValue,
                "field `text` is empty after normalization; it must contain at least one visible character",
            )
            .with_field("text"));
    }
    document.insert_str("text", &text);
    Ok(document)
}

fn normalize_text(text: &str, case: Case) -> String {
    let visible: String = text
        .chars()
        .filter(|character| character.is_whitespace() || !character.is_control())
        .collect();
    let mut output = String::with_capacity(visible.len());
    for word in visible.split_whitespace() {
        if !output.is_empty() {
            output.push(' ');
        }
        case.apply(word, &mut output);
    }
    output
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
