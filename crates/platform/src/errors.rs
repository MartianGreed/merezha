//! The error registry (R8): every error code has one HTTP status in every
//! module. The kernel registers the canonical codes; modules declare theirs
//! through [`crate::Module::errors`] and the host registers them at startup,
//! refusing a code registered twice with different statuses.
//!
//! Unregistered codes map by classification: permanent 422, conflict 409,
//! transient 503.
use axum::http::StatusCode;
use merezha_domain::{Classification, Failure};
use serde::Serialize;
use std::{collections::BTreeMap, sync::OnceLock};

/// Most codes the registry holds.
pub const MAX_CODES: usize = 4096;
/// Most field errors in one error body.
pub const MAX_DETAILS: usize = 100;
/// Longest field error path.
pub const MAX_DETAIL_PATH: usize = 256;

/// A code and its HTTP status, as a module declares it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ErrorCode {
    /// Unprefixed lower snake case, `[a-z][a-z0-9_]*`, at most 64 bytes.
    pub code: &'static str,
    /// 400 to 599.
    pub status: u16,
}
impl ErrorCode {
    pub const fn new(code: &'static str, status: u16) -> Self {
        Self { code, status }
    }
}

/// The kernel's canonical codes. `rate_limited` and `overloaded` responses
/// carry `Retry-After`, as does every other 429 and 503.
pub const KERNEL_CODES: &[ErrorCode] = &[
    ErrorCode::new("invalid_input", 400),
    ErrorCode::new("unauthenticated", 401),
    ErrorCode::new("unauthorized", 403),
    ErrorCode::new("actor_mismatch", 403),
    ErrorCode::new("tenant_required", 403),
    ErrorCode::new("not_found", 404),
    ErrorCode::new("handler_not_found", 404),
    ErrorCode::new("projection_not_found", 404),
    ErrorCode::new("method_not_allowed", 405),
    ErrorCode::new("secret_purged", 409),
    ErrorCode::new("revision_conflict", 412),
    ErrorCode::new("body_limit", 413),
    ErrorCode::new("input_limit", 413),
    ErrorCode::new("payload_limit", 413),
    ErrorCode::new("unsupported_media_type", 415),
    ErrorCode::new("validation_failed", 422),
    ErrorCode::new("rate_limited", 429),
    ErrorCode::new("database_busy", 429),
    ErrorCode::new("overloaded", 503),
    ErrorCode::new("timeout", 504),
    ErrorCode::new("upstream_timeout", 504),
];

/// Why a registration was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistrationError {
    pub owner: String,
    pub code: String,
    /// `invalid_error_code`, `invalid_error_status`, `error_status_conflict`
    /// or `too_many_error_codes`.
    pub reason: &'static str,
}
impl std::fmt::Display for RegistrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({}: {})", self.reason, self.owner, self.code)
    }
}
impl std::error::Error for RegistrationError {}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Entry {
    status: StatusCode,
    owner: String,
}

/// Code to HTTP status, one status per code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorRegistry {
    codes: BTreeMap<String, Entry>,
}
fn valid_code(code: &str) -> bool {
    crate::config::valid_module_name(code)
}
impl ErrorRegistry {
    /// The kernel's codes only.
    pub fn kernel() -> Self {
        let mut registry = Self {
            codes: BTreeMap::new(),
        };
        // The kernel list is valid and conflict-free by construction; the
        // unit tests assert it.
        let _ = registry.register("kernel", KERNEL_CODES);
        registry
    }
    /// Register `owner`'s codes. All or nothing: every problem is returned
    /// and nothing is registered when any code is invalid or already has a
    /// different status. Registering a code again with the same status is
    /// allowed, so modules may list canonical codes they return.
    pub fn register(
        &mut self,
        owner: &str,
        codes: &[ErrorCode],
    ) -> Result<(), Vec<RegistrationError>> {
        let mut staged: BTreeMap<String, Entry> = BTreeMap::new();
        let mut errors = Vec::new();
        for declared in codes {
            let refuse = |reason| RegistrationError {
                owner: owner.to_owned(),
                code: declared.code.to_owned(),
                reason,
            };
            if !valid_code(declared.code) {
                errors.push(refuse("invalid_error_code"));
                continue;
            }
            let Some(status) = StatusCode::from_u16(declared.status)
                .ok()
                .filter(|s| s.is_client_error() || s.is_server_error())
            else {
                errors.push(refuse("invalid_error_status"));
                continue;
            };
            let existing = self
                .codes
                .get(declared.code)
                .or_else(|| staged.get(declared.code));
            match existing {
                Some(entry) if entry.status != status => {
                    errors.push(refuse("error_status_conflict"));
                }
                Some(_) => {}
                None => {
                    staged.insert(
                        declared.code.to_owned(),
                        Entry {
                            status,
                            owner: owner.to_owned(),
                        },
                    );
                }
            }
        }
        if self.codes.len() + staged.len() > MAX_CODES {
            errors.push(RegistrationError {
                owner: owner.to_owned(),
                code: String::new(),
                reason: "too_many_error_codes",
            });
        }
        if !errors.is_empty() {
            return Err(errors);
        }
        self.codes.extend(staged);
        Ok(())
    }
    /// The registered status of `code`.
    pub fn status_of(&self, code: &str) -> Option<StatusCode> {
        self.codes.get(code).map(|e| e.status)
    }
    /// Who registered `code` first.
    pub fn owner_of(&self, code: &str) -> Option<&str> {
        self.codes.get(code).map(|e| e.owner.as_str())
    }
    /// The status for `failure`: its registered status, else permanent 422,
    /// conflict 409 and transient 503.
    pub fn status(&self, failure: &Failure) -> StatusCode {
        self.status_of(&failure.code)
            .unwrap_or(match failure.classification {
                Classification::Permanent => StatusCode::UNPROCESSABLE_ENTITY,
                Classification::Conflict => StatusCode::CONFLICT,
                Classification::Transient => StatusCode::SERVICE_UNAVAILABLE,
            })
    }
    pub fn len(&self) -> usize {
        self.codes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.codes.is_empty()
    }
}
impl Default for ErrorRegistry {
    fn default() -> Self {
        Self::kernel()
    }
}

/// The shared kernel-only registry, for responses built outside a host.
pub fn kernel_registry() -> &'static ErrorRegistry {
    static KERNEL: OnceLock<ErrorRegistry> = OnceLock::new();
    KERNEL.get_or_init(ErrorRegistry::kernel)
}

/// Whether responses with `status` carry `Retry-After`.
pub fn needs_retry_after(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
    )
}

/// One field-level validation error in an error body's `details`: a JSON
/// pointer and a code, never user input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldError {
    pub path: String,
    pub code: String,
}
impl FieldError {
    /// `path` is a JSON pointer (`""` or starting with `/`) of at most 256
    /// printable ASCII bytes; `code` is lower snake case. Fails permanent
    /// `invalid_error_detail`.
    pub fn new(path: impl Into<String>, code: &str) -> Result<Self, Failure> {
        let path = path.into();
        let valid_path = path.len() <= MAX_DETAIL_PATH
            && (path.is_empty() || path.starts_with('/'))
            && path.bytes().all(|b| b.is_ascii_graphic());
        if valid_path && valid_code(code) {
            Ok(Self {
                path,
                code: code.to_owned(),
            })
        } else {
            Err(Failure::permanent("invalid_error_detail"))
        }
    }
    pub(crate) fn is_valid(&self) -> bool {
        Self::new(self.path.clone(), &self.code).is_ok()
    }
}

/// The error body member: `{"code","classification","details"?}`.
#[derive(Serialize)]
pub(crate) struct ErrorBody<'a> {
    pub code: &'a str,
    pub classification: Classification,
    #[serde(skip_serializing_if = "<[FieldError]>::is_empty")]
    pub details: &'a [FieldError],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernel_codes_have_their_canonical_statuses() {
        let registry = ErrorRegistry::kernel();
        assert_eq!(registry.len(), KERNEL_CODES.len(), "no kernel duplicates");
        for (code, status) in [
            ("unauthenticated", 401),
            ("unauthorized", 403),
            ("actor_mismatch", 403),
            ("tenant_required", 403),
            ("validation_failed", 422),
            ("revision_conflict", 412),
            ("body_limit", 413),
            ("rate_limited", 429),
            ("overloaded", 503),
            ("secret_purged", 409),
        ] {
            assert_eq!(
                registry.status_of(code).map(|s| s.as_u16()),
                Some(status),
                "{code}"
            );
        }
        assert_eq!(registry.status(&Failure::permanent("unregistered")), 422);
        assert_eq!(registry.status(&Failure::conflict("unregistered")), 409);
        assert_eq!(registry.status(&Failure::transient("unregistered")), 503);
        // The registered status wins over the classification.
        assert_eq!(registry.status(&Failure::transient("rate_limited")), 429);
    }

    #[test]
    fn registrations_are_all_or_nothing_with_one_status_per_code() {
        let mut registry = ErrorRegistry::kernel();
        registry
            .register(
                "identity",
                &[
                    ErrorCode::new("email_taken", 409),
                    ErrorCode::new("unauthorized", 403),
                    ErrorCode::new("invalid_credentials", 401),
                ],
            )
            .unwrap();
        assert_eq!(
            registry.status_of("email_taken"),
            Some(StatusCode::CONFLICT)
        );
        assert_eq!(registry.owner_of("unauthorized"), Some("kernel"));
        let errors = registry
            .register(
                "content",
                &[
                    ErrorCode::new("entry_locked", 423),
                    ErrorCode::new("email_taken", 422),
                    ErrorCode::new("validation_failed", 400),
                    ErrorCode::new("Bad-Code", 400),
                    ErrorCode::new("teapot", 200),
                ],
            )
            .unwrap_err();
        let reasons: Vec<_> = errors.iter().map(|e| (e.code.as_str(), e.reason)).collect();
        assert_eq!(
            reasons,
            vec![
                ("email_taken", "error_status_conflict"),
                ("validation_failed", "error_status_conflict"),
                ("Bad-Code", "invalid_error_code"),
                ("teapot", "invalid_error_status"),
            ]
        );
        assert_eq!(
            registry.status_of("entry_locked"),
            None,
            "nothing was registered"
        );
        // Duplicates inside one declaration conflict too.
        let errors = registry
            .register(
                "jobs",
                &[
                    ErrorCode::new("job_gone", 404),
                    ErrorCode::new("job_gone", 410),
                ],
            )
            .unwrap_err();
        assert_eq!(errors[0].reason, "error_status_conflict");
    }

    #[test]
    fn field_errors_are_pointers_and_codes() {
        assert!(FieldError::new("/fields/title", "required").is_ok());
        assert!(FieldError::new("", "invalid").is_ok());
        for (path, code) in [
            ("fields/title", "required"),
            ("/a b", "required"),
            ("/a", "Required"),
            ("/a", ""),
        ] {
            assert_eq!(
                FieldError::new(path, code).unwrap_err().code,
                "invalid_error_detail"
            );
        }
        assert!(FieldError::new("/".repeat(257), "x").is_err());
    }
}
