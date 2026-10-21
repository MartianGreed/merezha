//! Shared building blocks for Merezha example services.
//!
//! Every service receives one UTF-8 string and returns one, as defined by
//! `merezha:service/handler@0.1.0`. The example services agree on a JSON
//! convention on top of that contract:
//!
//! - the input is a JSON object, checked against the service's [`Limits`]
//!   before any service-specific work starts;
//! - a service reads and writes only its documented fields and copies every
//!   other field unchanged, so data survives a multi-step pipeline;
//! - a failure is returned as a JSON error document (see [`ServiceError`])
//!   naming the service, a stable code, the offending field and a message
//!   that says what was expected.

pub use serde_json::{Map, Value};

/// A JSON object as received or produced by a service.
pub type Object = Map<String, Value>;

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
        let mut error = Object::new();
        error.insert("service".to_owned(), Value::from(self.service));
        error.insert("code".to_owned(), Value::from(self.code.as_str()));
        if let Some(field) = &self.field {
            error.insert("field".to_owned(), Value::from(field.as_str()));
        }
        error.insert("message".to_owned(), Value::from(self.message.as_str()));
        let mut document = Object::new();
        document.insert("error".to_owned(), Value::Object(error));
        Value::Object(document).to_string()
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
        transform: impl FnOnce(Object) -> Result<Value, ServiceError>,
    ) -> Result<String, String> {
        self.parse_object(input)
            .and_then(transform)
            .map(|output| output.to_string())
            .map_err(|error| error.to_json())
    }

    /// Parses `input` as a JSON object within this service's [`Limits`].
    ///
    /// # Errors
    ///
    /// `limit_exceeded` when the input is too large, nests too deeply or has
    /// too many fields; `invalid_json` when it does not parse;
    /// `invalid_type` when it is not an object.
    pub fn parse_object(&self, input: &str) -> Result<Object, ServiceError> {
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
        let value: Value = serde_json::from_str(input).map_err(|error| {
            let reason = error.to_string();
            if reason.starts_with("recursion limit exceeded") {
                self.depth_error()
            } else {
                self.error(
                    ErrorCode::InvalidJson,
                    format!("input is not valid JSON: {reason}"),
                )
            }
        })?;
        let object = match value {
            Value::Object(object) => object,
            other => {
                return Err(self.error(
                    ErrorCode::InvalidType,
                    format!("input must be a JSON object, got {}", kind(&other)),
                ));
            }
        };
        if object.len() > limits.max_fields {
            return Err(self.error(
                ErrorCode::LimitExceeded,
                format!(
                    "input has {} top-level fields; {} accepts at most {}",
                    object.len(),
                    self.name,
                    limits.max_fields
                ),
            ));
        }
        if nesting_exceeds(&object, limits.max_depth) {
            return Err(self.depth_error());
        }
        Ok(object)
    }

    /// Returns the string in `field`.
    ///
    /// # Errors
    ///
    /// `missing_field` when absent, `invalid_type` when not a string.
    pub fn required_str<'a>(
        &self,
        object: &'a Object,
        field: &str,
    ) -> Result<&'a str, ServiceError> {
        self.optional_str(object, field)?.ok_or_else(|| {
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
    /// `invalid_type` when present but not a string; `null` is not accepted.
    pub fn optional_str<'a>(
        &self,
        object: &'a Object,
        field: &str,
    ) -> Result<Option<&'a str>, ServiceError> {
        match object.get(field) {
            None => Ok(None),
            Some(Value::String(text)) => Ok(Some(text)),
            Some(other) => Err(self
                .error(
                    ErrorCode::InvalidType,
                    format!("field `{field}` must be a string, got {}", kind(other)),
                )
                .with_field(field)),
        }
    }

    /// Creates an error attributed to this service.
    pub fn error(&self, code: ErrorCode, message: impl Into<String>) -> ServiceError {
        ServiceError::new(self.name, code, message)
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
pub fn kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// Whether any container below `object` sits deeper than `max_depth`,
/// counting `object` itself as depth 1. Iterative, so hostile nesting cannot
/// exhaust the guest stack here.
fn nesting_exceeds(object: &Object, max_depth: usize) -> bool {
    let mut pending: Vec<(&Value, usize)> = object.values().map(|value| (value, 2)).collect();
    while let Some((value, depth)) = pending.pop() {
        let children: &mut dyn Iterator<Item = &Value> = match value {
            Value::Array(items) => &mut items.iter(),
            Value::Object(fields) => &mut fields.values(),
            _ => continue,
        };
        if depth > max_depth {
            return true;
        }
        pending.extend(children.map(|child| (child, depth + 1)));
    }
    1 > max_depth
}
