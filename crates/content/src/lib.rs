//! The content block's model (CT-C1): `content.toml` parsing and
//! validation with every problem reported at once, the canonical JSON form
//! and its SHA-256 fingerprint, entry field value validation with
//! JSON-pointer details, Portable Text validation and slug generation.
//!
//! This crate has no database code yet. The storage, activation, routes and
//! the platform `Module` arrive in later slices (C2 onwards) and build on
//! [`ContentModel`]: C2 stores [`ContentModel::canonical_json`] and
//! [`ContentModel::fingerprint`] in `merezha_content.models` and diffs
//! canonical forms; C3 resolves [`Validated::references`] against the
//! database and enforces `unique` through indexes.
mod canonical;
mod issues;
mod model;
mod parse;
mod rich_text;
mod scalar;
mod slug;
mod value;

pub use issues::{MAX_ISSUES, MODEL_INVALID, ModelError, ModelIssue};
pub use model::*;
pub use parse::{
    CONTENT_PERMISSIONS, MAX_DESCRIPTION, MAX_MODEL_BYTES, MAX_PATTERN_BYTES, ModelOptions,
    PATTERN_SIZE_LIMIT, PermissionCatalog,
};
pub use rich_text::{MAX_HREF_BYTES, MAX_MARK_DEFS, MAX_SPAN_MARKS, MAX_SPANS};
pub use slug::slugify;
pub use value::{
    GENERATED_KEY_LENGTH, KeySource, MAX_ALT_LENGTH, MAX_DETAIL_PATH, RandomKeys, ReferenceTarget,
    Stage, ValidateOptions, Validated, ValueError, ValueReference, valid_key,
};

use merezha_platform::ErrorCode;

/// The codes this crate returns, for the kernel error registry (R8).
/// `content_model_invalid` is a startup code; `validation_failed`,
/// `invalid_json` and `content_type_not_found` reach HTTP from the write
/// routes of later slices.
pub const ERRORS: &[ErrorCode] = &[
    ErrorCode::new(MODEL_INVALID, 422),
    ErrorCode::new("validation_failed", 422),
    ErrorCode::new("invalid_json", 400),
    ErrorCode::new("content_type_not_found", 404),
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
