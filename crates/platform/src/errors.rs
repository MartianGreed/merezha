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

/// Every code the kernel emits, with its status: the canonical R8 codes,
/// the PostgreSQL, application, runtime and port codes, and the startup
/// codes that never reach HTTP but stay reserved so no module reuses them
/// with another meaning. A module may list one again with the same status,
/// never with another. `rate_limited`, `database_busy`, `overloaded` and
/// every other 429 and 503 response carry `Retry-After`.
pub const KERNEL_CODES: &[ErrorCode] = &[
    ErrorCode::new("invalid_input", 400),
    ErrorCode::new("unauthenticated", 401),
    ErrorCode::new("unauthorized", 403),
    ErrorCode::new("actor_mismatch", 403),
    ErrorCode::new("tenant_required", 403),
    ErrorCode::new("not_found", 404),
    ErrorCode::new("handler_not_found", 404),
    ErrorCode::new("projection_not_found", 404),
    ErrorCode::new("unknown_service", 404),
    ErrorCode::new("blob_not_found", 404),
    ErrorCode::new("method_not_allowed", 405),
    ErrorCode::new("secret_purged", 409),
    ErrorCode::new("conflict", 409),
    ErrorCode::new("database_conflict", 409),
    ErrorCode::new("unique_violation", 409),
    ErrorCode::new("concurrency_conflict", 409),
    ErrorCode::new("idempotency_in_flight", 409),
    ErrorCode::new("idempotency_mismatch", 409),
    ErrorCode::new("projection_conflict", 409),
    ErrorCode::new("projection_in_flight", 409),
    ErrorCode::new("storage_owned", 409),
    ErrorCode::new("outbox_duplicate", 409),
    ErrorCode::new("revision_conflict", 412),
    ErrorCode::new("body_limit", 413),
    ErrorCode::new("input_limit", 413),
    ErrorCode::new("payload_limit", 413),
    ErrorCode::new("output_limit", 413),
    ErrorCode::new("unsupported_media_type", 415),
    ErrorCode::new("validation_failed", 422),
    ErrorCode::new("invalid_number", 422),
    ErrorCode::new("invalid_response", 422),
    ErrorCode::new("invariant_violation", 422),
    ErrorCode::new("protocol_version", 422),
    ErrorCode::new("serialization_failed", 422),
    ErrorCode::new("constraint_violation", 422),
    ErrorCode::new("database_error", 422),
    ErrorCode::new("event_batch_limit", 422),
    ErrorCode::new("history_limit", 422),
    ErrorCode::new("invalid_batch", 422),
    ErrorCode::new("invalid_event", 422),
    ErrorCode::new("invalid_event_feed", 422),
    ErrorCode::new("config_limit", 422),
    ErrorCode::new("invalid_dedupe_key", 422),
    ErrorCode::new("invalid_event_id", 422),
    ErrorCode::new("invalid_error_detail", 422),
    ErrorCode::new("invalid_job_payload", 422),
    ErrorCode::new("invalid_locale", 422),
    ErrorCode::new("invalid_page", 422),
    ErrorCode::new("invalid_recipient", 422),
    ErrorCode::new("invalid_source", 422),
    ErrorCode::new("source_not_allowed", 422),
    ErrorCode::new("source_not_granted", 422),
    ErrorCode::new("invalid_system_principal", 422),
    ErrorCode::new("too_many_ids", 422),
    ErrorCode::new("outbox_unavailable", 422),
    ErrorCode::new("outbox_batch_limit", 422),
    ErrorCode::new("outbox_payload_limit", 422),
    ErrorCode::new("blob_store_unavailable", 422),
    ErrorCode::new("invalid_blob_key", 422),
    ErrorCode::new("upstream_body_limit", 422),
    ErrorCode::new("guest_error", 422),
    ErrorCode::new("fuel_exhausted", 422),
    ErrorCode::new("memory_limit", 422),
    ErrorCode::new("trap", 422),
    ErrorCode::new("unknown_topic", 422),
    ErrorCode::new("unknown_template", 422),
    ErrorCode::new("channel_not_allowed", 422),
    ErrorCode::new("too_many_recipients", 422),
    ErrorCode::new("recipient_not_in_tenant", 422),
    ErrorCode::new("data_too_large", 422),
    ErrorCode::new("reserved_data_key", 422),
    ErrorCode::new("render_failed", 422),
    ErrorCode::new("unknown_event_type", 422),
    ErrorCode::new("webhooks_disabled", 422),
    ErrorCode::new("storage_configuration", 422),
    ErrorCode::new("storage_invalid", 422),
    ErrorCode::new("storage_lock", 422),
    ErrorCode::new("storage_open", 422),
    ErrorCode::new("storage_path", 422),
    ErrorCode::new("storage_schema", 422),
    ErrorCode::new("invalid_config", 422),
    ErrorCode::new("invalid_limits", 422),
    ErrorCode::new("invalid_routes", 422),
    ErrorCode::new("invalid_manifest", 422),
    ErrorCode::new("invalid_application_config", 422),
    ErrorCode::new("invalid_arguments", 422),
    ErrorCode::new("invalid_database_config", 422),
    ErrorCode::new("tls_ca_file", 422),
    ErrorCode::new("tls_config", 422),
    ErrorCode::new("invalid_migrations", 422),
    ErrorCode::new("unknown_applied_migration", 422),
    ErrorCode::new("migration_checksum_mismatch", 422),
    ErrorCode::new("migration_name_mismatch", 422),
    ErrorCode::new("migration_out_of_order", 422),
    ErrorCode::new("migration_failed", 422),
    ErrorCode::new("allow_all_requires_loopback", 422),
    ErrorCode::new("error_code_conflict", 422),
    ErrorCode::new("http_client_config", 422),
    ErrorCode::new("module_start_failed", 422),
    ErrorCode::new("telemetry_initialized", 422),
    ErrorCode::new("rate_limited", 429),
    ErrorCode::new("database_busy", 429),
    ErrorCode::new("overloaded", 503),
    ErrorCode::new("unavailable", 503),
    ErrorCode::new("internal", 503),
    ErrorCode::new("database_unavailable", 503),
    ErrorCode::new("database_timeout", 503),
    ErrorCode::new("database_closed", 503),
    ErrorCode::new("commit_outcome_unknown", 503),
    ErrorCode::new("upstream_unavailable", 503),
    ErrorCode::new("random_unavailable", 503),
    ErrorCode::new("idempotency_capacity", 503),
    ErrorCode::new("storage_busy", 503),
    ErrorCode::new("storage_full", 503),
    ErrorCode::new("storage_reopen_required", 503),
    ErrorCode::new("storage_sync", 503),
    ErrorCode::new("storage_unavailable", 503),
    ErrorCode::new("store_capacity", 503),
    ErrorCode::new("store_unavailable", 503),
    ErrorCode::new("migration_lock_timeout", 503),
    ErrorCode::new("migration_timeout", 503),
    ErrorCode::new("listener_failed", 503),
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

    /// `text` without the items gated by `#[cfg(test)]`: a `;`- or
    /// `,`-terminated item, or one whose body is a balanced `{ }` block,
    /// such as `mod tests { .. }`. Strings and char literals are skipped
    /// while counting braces.
    fn strip_test_items(text: &str) -> String {
        const GATE: &str = "#[cfg(test)]";
        let bytes = text.as_bytes();
        let mut out = String::with_capacity(text.len());
        let mut from = 0;
        while let Some(found) = text[from..].find(GATE) {
            let start = from + found;
            out.push_str(&text[from..start]);
            let mut index = start + GATE.len();
            let mut depth = 0usize;
            while index < bytes.len() {
                match bytes[index] {
                    b'"' => {
                        index += 1;
                        while index < bytes.len() && bytes[index] != b'"' {
                            if bytes[index] == b'\\' {
                                index += 1;
                            }
                            index += 1;
                        }
                    }
                    b'\'' if bytes.get(index + 2) == Some(&b'\'') => index += 2,
                    b'{' => depth += 1,
                    b'}' if depth == 0 => break,
                    b'}' => {
                        depth -= 1;
                        if depth == 0 {
                            index += 1;
                            break;
                        }
                    }
                    b';' | b',' if depth == 0 => {
                        index += 1;
                        break;
                    }
                    _ => {}
                }
                index += 1;
            }
            from = index.min(bytes.len());
        }
        out.push_str(&text[from..]);
        out
    }

    /// Every code literal `text` constructs.
    fn codes_in(text: &str) -> std::collections::BTreeSet<String> {
        const CALLS: [&str; 12] = [
            "Failure::permanent(\"",
            "Failure::transient(\"",
            "Failure::conflict(\"",
            "Failure::new(\"",
            "MigrationError::at(\"",
            "StartupError::new(\"",
            "JobFailure::permanent(\"",
            "JobFailure::transient(\"",
            "BlobError::Invalid(\"",
            "BlobError::Rejected(\"",
            "BlobError::Unavailable(\"",
            "failed(\"",
        ];
        let mut found = std::collections::BTreeSet::new();
        // Multi-line calls: collapse whitespace and join the argument onto
        // the call.
        let code = text
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .replace("( \"", "(\"");
        for call in CALLS {
            for (at, _) in code.match_indices(call) {
                let rest = &code[at + call.len()..];
                if let Some(end) = rest.find('"') {
                    found.insert(rest[..end].to_owned());
                }
            }
        }
        // `code()` tables of the runtime, the notifier and the edge.
        for (at, _) in code.match_indices("=> \"") {
            let rest = &code[at + 4..];
            if let Some(end) = rest.find('"') {
                let literal = &rest[..end];
                if crate::config::valid_module_name(literal) {
                    found.insert(literal.to_owned());
                }
            }
        }
        found
    }

    /// Every code literal the kernel crates construct outside test items.
    fn emitted_codes() -> std::collections::BTreeSet<String> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let mut found = std::collections::BTreeSet::new();
        let mut stack: Vec<std::path::PathBuf> = [
            "postgres",
            "platform",
            "application",
            "domain",
            "cli",
            "runtime",
        ]
        .iter()
        .map(|c| root.join(c).join("src"))
        .collect();
        while let Some(path) = stack.pop() {
            if path.is_dir() {
                for entry in std::fs::read_dir(&path).unwrap() {
                    stack.push(entry.unwrap().path());
                }
                continue;
            }
            if path.extension().is_none_or(|e| e != "rs") {
                continue;
            }
            let text = std::fs::read_to_string(&path).unwrap();
            found.extend(codes_in(&strip_test_items(&text)));
        }
        found
    }

    #[test]
    fn the_scanner_sees_codes_after_early_test_items_and_skips_test_code() {
        let source = r#"
#[cfg(test)]
use std::sync::Mutex;
struct Slot {
    #[cfg(test)]
    waiting: Mutex<usize>,
    open: bool,
}
fn busy() -> Failure {
    Failure::transient("late_production_code")
}
fn start() -> StartupError {
    StartupError::new(
        "startup_code",
    )
}
#[cfg(test)]
mod tests {
    fn helper() { let _ = Failure::permanent("test_only_code"); let _ = "}"; }
}
fn after() -> Failure { Failure::conflict("after_tests") }
"#;
        let codes = codes_in(&strip_test_items(source));
        for expected in ["late_production_code", "startup_code", "after_tests"] {
            assert!(codes.contains(expected), "{expected} in {codes:?}");
        }
        assert!(!codes.contains("test_only_code"), "{codes:?}");
    }

    #[test]
    fn every_code_the_kernel_emits_is_registered_and_cannot_be_redefined() {
        let emitted = emitted_codes();
        assert!(emitted.contains("database_unavailable") && emitted.contains("output_limit"));
        let registry = ErrorRegistry::kernel();
        let known_non_codes = [
            // `=> "..."` literals that are not error codes.
            "default",
            "file",
            "json",
            "text",
            "user",
            "tenant",
            "application",
            "email_address",
            "phone_address",
            "email",
            "sms",
            "push",
            "inbox",
            "kernel",
            "apply",
            "verify",
            // Configuration issue codes from `config::failed`, not failures.
            "config_unreadable",
            "config_too_large",
        ];
        let missing: Vec<_> = emitted
            .iter()
            .filter(|c| registry.status_of(c).is_none() && !known_non_codes.contains(&c.as_str()))
            .collect();
        assert!(missing.is_empty(), "unregistered kernel codes: {missing:?}");
        for code in KERNEL_CODES {
            let other = if code.status == 400 { 418 } else { 400 };
            let mut module = ErrorRegistry::kernel();
            let leaked: &'static str = Box::leak(code.code.to_owned().into_boxed_str());
            let refused = module
                .register("rogue", &[ErrorCode::new(leaked, other)])
                .unwrap_err();
            assert_eq!(refused[0].reason, "error_status_conflict", "{}", code.code);
            assert!(module.register("polite", &[*code]).is_ok());
        }
        assert_eq!(
            registry.status_of("output_limit"),
            Some(StatusCode::PAYLOAD_TOO_LARGE)
        );
        assert_eq!(
            registry.status_of("database_unavailable"),
            Some(StatusCode::SERVICE_UNAVAILABLE)
        );
    }
}
