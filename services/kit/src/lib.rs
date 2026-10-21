//! Shared building blocks for Merezha example services.
//!
//! Every service receives one UTF-8 string and returns one, as defined by
//! `merezha:service/handler@0.1.0`. The example services agree on a JSON
//! convention on top of that contract:
//!
//! - the input is a JSON object, checked against the service's [`Limits`]
//!   before any service-specific work starts;
//! - a service decodes only its documented fields and copies every other
//!   field as JSON text, without interpreting it, so data survives a
//!   multi-step pipeline exactly (see [`Document`]);
//! - a failure is returned as a JSON error document (see [`ServiceError`])
//!   naming the service, a stable code, the offending field and a message
//!   that says what was expected.

use std::collections::BTreeMap;

use serde_json::value::{RawValue, to_raw_value};

/// Stable, machine-readable failure classes shared by the example services.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// The input is not valid JSON.
    InvalidJson,
    /// The input or a field has the wrong JSON type.
    InvalidType,
    /// A required field is absent.
    MissingField,
    /// A field has the right type but an unsupported value.
    InvalidValue,
    /// The input exceeds a size, depth or count bound of the service.
    LimitExceeded,
}

impl ErrorCode {
    /// The code as it appears in the error document.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidJson => "invalid_json",
            Self::InvalidType => "invalid_type",
            Self::MissingField => "missing_field",
            Self::InvalidValue => "invalid_value",
            Self::LimitExceeded => "limit_exceeded",
        }
    }
}

/// An actionable guest error.
///
/// [`ServiceError::to_json`] renders it as
/// `{"error":{"code":…,"field":…,"message":…,"service":…}}`; `field` is
/// omitted when the error concerns the whole input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceError {
    service: &'static str,
    code: ErrorCode,
    field: Option<String>,
    message: String,
}

impl ServiceError {
    /// Creates an error for the whole input of `service`.
    pub fn new(service: &'static str, code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            service,
            code,
            field: None,
            message: message.into(),
        }
    }

    /// Names the input field the error concerns.
    #[must_use]
    pub fn with_field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }

    /// The service that reported the error.
    pub fn service(&self) -> &'static str {
        self.service
    }

    /// The stable error class.
    pub fn code(&self) -> ErrorCode {
        self.code
    }

    /// The input field the error concerns, if any.
    pub fn field(&self) -> Option<&str> {
        self.field.as_deref()
    }

    /// The human-readable explanation.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Renders the JSON error document returned through `handle`'s `err` case.
    pub fn to_json(&self) -> String {
        let mut error = Document::new();
        error.insert_str("service", self.service);
        error.insert_str("code", self.code.as_str());
        if let Some(field) = &self.field {
            error.insert_str("field", field);
        }
        error.insert_str("message", &self.message);
        let mut document = Document::new();
        document.insert_document("error", &error);
        document.to_json()
    }
}

/// A JSON object whose field values are kept as JSON text.
///
/// Values are never decoded into a generic JSON tree, so a field a service
/// does not own comes out with the same numbers, strings, nested key order
/// and duplicate keys it went in with. Only insignificant whitespace between
/// tokens is removed. Top-level keys are unique and sorted; a repeated
/// top-level key keeps its last value.
#[derive(Debug, Clone, Default)]
pub struct Document {
    fields: BTreeMap<String, Box<RawValue>>,
}

impl Document {
    /// An empty object.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of top-level fields.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Whether the object has no fields.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// The JSON text of `key`, if present.
    pub fn get(&self, key: &str) -> Option<&RawValue> {
        self.fields.get(key).map(AsRef::as_ref)
    }

    /// Removes `key` and returns its JSON text.
    pub fn remove(&mut self, key: &str) -> Option<Box<RawValue>> {
        self.fields.remove(key)
    }

    /// Sets `key` to the JSON string `value`.
    pub fn insert_str(&mut self, key: impl Into<String>, value: &str) {
        let value = to_raw_value(value).expect("a string always serializes to JSON");
        self.fields.insert(key.into(), value);
    }

    /// Sets `key` to the JSON number `value`.
    pub fn insert_u64(&mut self, key: impl Into<String>, value: u64) {
        let value = to_raw_value(&value).expect("an integer always serializes to JSON");
        self.fields.insert(key.into(), value);
    }

    /// Sets `key` to the object `value`.
    pub fn insert_document(&mut self, key: impl Into<String>, value: &Document) {
        let value = RawValue::from_string(value.to_json())
            .expect("a document always serializes to valid JSON");
        self.fields.insert(key.into(), value);
    }

    /// Serializes the object compactly, with top-level keys sorted.
    pub fn to_json(&self) -> String {
        let mut json = String::from("{");
        for (index, (key, value)) in self.fields.iter().enumerate() {
            if index > 0 {
                json.push(',');
            }
            json.push_str(
                &to_raw_value(key)
                    .expect("a key always serializes")
                    .to_string(),
            );
            json.push(':');
            push_compact(value.get(), &mut json);
        }
        json.push('}');
        json
    }
}

/// Work bounds a service enforces on its own input, independently of the
/// host's invocation limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Maximum input size in bytes.
    pub max_input_bytes: usize,
    /// Maximum container nesting; the top-level object counts as depth 1.
    pub max_depth: usize,
    /// Maximum number of top-level fields.
    pub max_fields: usize,
}

/// A service's identity and bounds, and the entry point that applies them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Service {
    /// Name reported in error documents.
    pub name: &'static str,
    /// Bounds checked before the service's transformation runs.
    pub limits: Limits,
}

impl Service {
    /// Runs one invocation: bounded parsing, `transform`, then serialization.
    ///
    /// This is the body of the component's `handle` export. Errors from any
    /// stage come back as [`ServiceError::to_json`] documents.
    pub fn run(
        &self,
        input: &str,
        transform: impl FnOnce(Document) -> Result<Document, ServiceError>,
    ) -> Result<String, String> {
        self.parse_object(input)
            .and_then(transform)
            .map(|output| output.to_json())
            .map_err(|error| error.to_json())
    }

    /// Parses `input` as a JSON object within this service's [`Limits`].
    ///
    /// # Errors
    ///
    /// `limit_exceeded` when the input is too large, nests too deeply or has
    /// too many fields, checked in that order; `invalid_json` when it does
    /// not parse; `invalid_type` when it is not an object.
    pub fn parse_object(&self, input: &str) -> Result<Document, ServiceError> {
        let limits = self.limits;
        if input.len() > limits.max_input_bytes {
            return Err(self.error(
                ErrorCode::LimitExceeded,
                format!(
                    "input is {} bytes; {} accepts at most {} bytes",
                    input.len(),
                    self.name,
                    limits.max_input_bytes
                ),
            ));
        }
        if nesting_depth(input) > limits.max_depth {
            return Err(self.depth_error());
        }
        let raw: Box<RawValue> =
            serde_json::from_str(input).map_err(|error| self.json_error(&error))?;
        let kind = kind(&raw);
        if kind != "an object" {
            return Err(self.error(
                ErrorCode::InvalidType,
                format!("input must be a JSON object, got {kind}"),
            ));
        }
        let fields: BTreeMap<String, Box<RawValue>> =
            serde_json::from_str(raw.get()).map_err(|error| self.json_error(&error))?;
        if fields.len() > limits.max_fields {
            return Err(self.error(
                ErrorCode::LimitExceeded,
                format!(
                    "input has {} top-level fields; {} accepts at most {}",
                    fields.len(),
                    self.name,
                    limits.max_fields
                ),
            ));
        }
        Ok(Document { fields })
    }

    /// Returns the string in `field`.
    ///
    /// # Errors
    ///
    /// `missing_field` when absent, `invalid_type` when not a string.
    pub fn required_str(&self, document: &Document, field: &str) -> Result<String, ServiceError> {
        self.optional_str(document, field)?.ok_or_else(|| {
            self.error(
                ErrorCode::MissingField,
                format!("field `{field}` is required and must be a string"),
            )
            .with_field(field)
        })
    }

    /// Returns the string in `field`, or `None` when the field is absent.
    ///
    /// # Errors
    ///
    /// `invalid_type` when present but not a string, `null` included;
    /// `invalid_value` when its escapes do not form valid Unicode.
    pub fn optional_str(
        &self,
        document: &Document,
        field: &str,
    ) -> Result<Option<String>, ServiceError> {
        let Some(raw) = document.get(field) else {
            return Ok(None);
        };
        let kind = kind(raw);
        if kind != "a string" {
            return Err(self
                .error(
                    ErrorCode::InvalidType,
                    format!("field `{field}` must be a string, got {kind}"),
                )
                .with_field(field));
        }
        serde_json::from_str(raw.get()).map(Some).map_err(|error| {
            self.error(
                ErrorCode::InvalidValue,
                format!("field `{field}` is not a valid Unicode string: {error}"),
            )
            .with_field(field)
        })
    }

    /// Creates an error attributed to this service.
    pub fn error(&self, code: ErrorCode, message: impl Into<String>) -> ServiceError {
        ServiceError::new(self.name, code, message)
    }

    fn json_error(&self, error: &serde_json::Error) -> ServiceError {
        let reason = error.to_string();
        if reason.starts_with("recursion limit exceeded") {
            self.depth_error()
        } else {
            self.error(
                ErrorCode::InvalidJson,
                format!("input is not valid JSON: {reason}"),
            )
        }
    }

    fn depth_error(&self) -> ServiceError {
        self.error(
            ErrorCode::LimitExceeded,
            format!(
                "input nests deeper than {} levels; {} accepts at most {}",
                self.limits.max_depth, self.name, self.limits.max_depth
            ),
        )
    }
}

/// The JSON type name of `value`, for error messages.
pub fn kind(value: &RawValue) -> &'static str {
    match value.get().trim_start().as_bytes().first() {
        Some(b'{') => "an object",
        Some(b'[') => "an array",
        Some(b'"') => "a string",
        Some(b't' | b'f') => "a boolean",
        Some(b'n') => "null",
        _ => "a number",
    }
}

/// Deepest container nesting in `json`, scanning tokens outside strings.
/// Linear and iterative, so hostile nesting is bounded before parsing.
fn nesting_depth(json: &str) -> usize {
    let (mut depth, mut deepest) = (0usize, 0usize);
    let (mut in_string, mut escaped) = (false, false);
    for byte in json.bytes() {
        if in_string {
            match (escaped, byte) {
                (true, _) => escaped = false,
                (false, b'\\') => escaped = true,
                (false, b'"') => in_string = false,
                _ => {}
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                deepest = deepest.max(depth);
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    deepest
}

/// Appends `json` without insignificant whitespace between tokens.
fn push_compact(json: &str, output: &mut String) {
    let (mut in_string, mut escaped) = (false, false);
    for character in json.chars() {
        if in_string {
            output.push(character);
            match (escaped, character) {
                (true, _) => escaped = false,
                (false, '\\') => escaped = true,
                (false, '"') => in_string = false,
                _ => {}
            }
        } else if character == '"' {
            in_string = true;
            output.push(character);
        } else if !matches!(character, ' ' | '\t' | '\n' | '\r') {
            output.push(character);
        }
    }
}
