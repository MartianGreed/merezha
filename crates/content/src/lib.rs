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
//! CT-C3, drafts and revisions: the management routes under
//! `/v1/content/manage` ([`ContentModule`]'s router): entry creation,
//! variant reads, `PUT`, `PATCH`, revision history, restore with
//! retention, singletons, shared-field copies, the filtered and sorted
//! list with keyset cursors, unique violations mapped to their field
//! through [`DerivedIndex::field`], reference resolution and referrers
//! ([`Store`]). Every write transaction starts with
//! [`ContentState::check_writable`] and every statement carries the
//! route's scope and content type.
//!
//! CT-C4, publishing and deletion: publish, unpublish, publishing an older
//! revision, batch publish and unpublish ([`Store::publish`]), the
//! `published` rows materialized per locale with fallback, generations,
//! the variant delete, soft delete, undelete, immediate purge, the trash
//! and the `content.trash_purge` job ([`Store::delete_entry`]), each with
//! its outbox events in the change's transaction, and the fallback
//! rebuild after a locale or fallback change ([`activate`]). Delivery is
//! C5 and assets C7.
mod activation;
mod canonical;
mod diff;
mod http;
mod indexes;
mod issues;
mod json;
mod manage;
mod model;
mod module;
mod parse;
mod publish;
mod query;
mod refs;
mod rich_text;
mod scalar;
mod schema;
mod settings;
mod slug;
mod store;
mod trash;
mod value;

pub use activation::{
    ACTIVATION_LOCK, ACTIVATION_TIMEOUT, ActivationError, ActivationOptions, ActivationReport,
    ActiveModel, BUILD_ATTEMPTS, IndexFailure, MODEL_ACTIVATED, Plan, Status, StoredModel,
    TIGHTENING_SCAN_LIMIT, TightenedCount, Verified, activate, existing_indexes, missing_indexes,
    read_active, unneeded_indexes, verify,
};
pub use diff::{Change, ChangeClass, change_classes, diff, diff_models};
pub use http::{
    ENTRIES_DELETE, ENTRIES_PUBLISH, ENTRIES_PURGE, ENTRIES_READ, ENTRIES_WRITE, NO_STORE,
    RESOURCE_KIND,
};
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
pub use publish::{Expect, Item, PUBLISHED, REBUILD_BATCH, UNPUBLISHED};
pub use query::{MAX_CURSOR_BYTES, MAX_FILTER_VALUES, MAX_QUERY_BYTES};
pub use rich_text::{MAX_HREF_BYTES, MAX_MARK_DEFS, MAX_SPAN_MARKS, MAX_SPANS};
pub use schema::{MIGRATION_1, MIGRATION_2, MIGRATION_3, MIGRATIONS};
pub use settings::{ContentSettings, DEFAULT_PURGE_CRON, DEFAULT_TRASH_RETENTION, Limits, Trash};
pub use slug::slugify;
pub use store::{
    Actor, Content, DRAFT_SAVED, EntrySummary, Page, Referrer, Rejection, RevisionItem,
    RevisionView, Saved, Store, Target, VariantView, WriteKind,
};
pub use trash::{
    DELETED, DeleteOptions, PURGE_BATCH, PURGE_SUBJECT, PURGED, QUEUE, TRASH_PURGE, TrashItem,
    UNDELETED,
};
pub use value::{
    GENERATED_KEY_LENGTH, KeySource, MAX_ALT_LENGTH, MAX_DETAIL_PATH, RandomKeys, ReferenceTarget,
    Stage, ValidateOptions, Validated, ValueError, ValueReference, valid_key,
};

use merezha_platform::ErrorCode;

/// The codes this crate returns, registered in the kernel error registry
/// by [`ContentModule`] (R8). `content_model_superseded` and
/// `content_model_pending` reach HTTP from every write, as do the entry
/// codes of section 5. The model, index and activation codes are startup
/// and readiness codes. `revision_conflict`, `body_limit`,
/// `unsupported_media_type`, `unauthenticated`, `unauthorized`,
/// `tenant_required` and `timeout` are the kernel's.
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
    ErrorCode::new("entry_not_found", 404),
    ErrorCode::new("revision_not_found", 404),
    ErrorCode::new("locale_unknown", 400),
    ErrorCode::new("invalid_query", 400),
    ErrorCode::new("invalid_filter", 400),
    ErrorCode::new("invalid_sort", 400),
    ErrorCode::new("invalid_cursor", 400),
    ErrorCode::new("variant_exists", 412),
    ErrorCode::new("precondition_required", 428),
    ErrorCode::new("entry_exists", 409),
    ErrorCode::new("singleton_exists", 409),
    ErrorCode::new("entry_referenced", 409),
    ErrorCode::new("default_variant_required", 409),
    ErrorCode::new("content_rebuild_pending", 503),
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
