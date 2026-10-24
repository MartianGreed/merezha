//! The content block (`merezha-content`).
//!
//! CT-C1, the model: `content.toml` parsing and validation with every
//! problem reported at once, the canonical JSON form and its SHA-256
//! fingerprint, entry field value validation with JSON-pointer details,
//! Portable Text validation and slug generation.
//!
//! CT-C2, schema and activation: migration 1 of `merezha_content`
//! ([`MIGRATIONS`]), the classified diff between the active model and the
//! file ([`diff`], [`Plan`]), derived indexes ([`derived_indexes`]), model
//! activation under an advisory lock with its `content.model_activated`
//! event ([`activate`]), readiness, the model version check every content
//! write starts with ([`ContentState::check_writable`]) and the platform
//! [`ContentModule`].
//!
//! Entry routes and writes are C3 onward. They take the loaded model and
//! the write check from [`ContentHandle::state`], map unique violations
//! back to fields through [`DerivedIndex::field`], and resolve
//! [`Validated::references`] against the database.
mod activation;
mod canonical;
mod diff;
mod indexes;
mod issues;
mod model;
mod module;
mod parse;
mod rich_text;
mod scalar;
mod schema;
mod slug;
mod value;

pub use activation::{
    ACTIVATION_LOCK, ACTIVATION_TIMEOUT, ActivationError, ActivationOptions, ActivationReport,
    ActiveModel, BUILD_ATTEMPTS, IndexFailure, MODEL_ACTIVATED, Plan, Status, StoredModel,
    TIGHTENING_SCAN_LIMIT, TightenedCount, Verified, activate, existing_indexes, missing_indexes,
    read_active, unneeded_indexes, verify,
};
pub use diff::{Change, ChangeClass, change_classes, diff, diff_models};
pub use indexes::{
    DerivedIndex, INDEX_PREFIX, IndexPurpose, IndexTable, SCHEMA, derived_indexes, drop_statement,
    is_derived_name,
};
pub use issues::{MAX_ISSUES, MODEL_INVALID, ModelError, ModelIssue};
pub use model::*;
pub use module::{
    ContentHandle, ContentModule, ContentState, DEFAULT_MODEL_PATH, LoadError, NAME, PUBLISHES,
    load_model, lock_model_version, resolve_model_path,
};
pub use parse::{
    CONTENT_PERMISSIONS, MAX_DESCRIPTION, MAX_MODEL_BYTES, MAX_PATTERN_BYTES, ModelOptions,
    PATTERN_SIZE_LIMIT, PermissionCatalog,
};
pub use rich_text::{MAX_HREF_BYTES, MAX_MARK_DEFS, MAX_SPAN_MARKS, MAX_SPANS};
pub use schema::{MIGRATION_1, MIGRATIONS};
pub use slug::slugify;
pub use value::{
    GENERATED_KEY_LENGTH, KeySource, MAX_ALT_LENGTH, MAX_DETAIL_PATH, RandomKeys, ReferenceTarget,
    Stage, ValidateOptions, Validated, ValueError, ValueReference, valid_key,
};

use merezha_platform::ErrorCode;

/// The codes this crate returns, registered in the kernel error registry
/// by [`ContentModule`] (R8). `content_model_superseded` reaches HTTP from
/// every write of C3 onward, as do `validation_failed`, `invalid_json` and
/// `content_type_not_found`. The model, index and activation codes are
/// startup and readiness codes (section 5).
pub const ERRORS: &[ErrorCode] = &[
    ErrorCode::new(MODEL_INVALID, 422),
    ErrorCode::new("validation_failed", 422),
    ErrorCode::new("invalid_json", 400),
    ErrorCode::new("content_type_not_found", 404),
    ErrorCode::new("content_model_superseded", 503),
    ErrorCode::new("content_model_version_unchanged", 422),
    ErrorCode::new("content_model_breaking", 422),
    ErrorCode::new("content_model_outdated", 503),
    ErrorCode::new("content_model_pending", 503),
    ErrorCode::new("content_model_conflict", 503),
    ErrorCode::new("content_not_configured", 422),
    ErrorCode::new("content_activation_timeout", 503),
    ErrorCode::new("index_build_pending", 503),
    ErrorCode::new("index_build_failed", 422),
];

/// Every detail code field validation reports (section 2).
pub const DETAIL_CODES: [&str; 26] = [
    "required",
    "type_mismatch",
    "min_length",
    "max_length",
    "pattern",
    "min",
    "max",
    "precision",
    "scale",
    "not_in_enum",
    "min_items",
    "max_items",
    "unknown_field",
    "field_not_localized",
    "duplicate_key",
    "invalid_character",
    "datetime_precision",
    "reference_not_found",
    "reference_type",
    "reference_scope",
    "reference_unpublished",
    "asset_not_found",
    "asset_type",
    "rich_text_invalid",
    "unique",
    "json_too_large",
];
