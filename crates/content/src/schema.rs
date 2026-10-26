//! Migration 1 of `merezha_content` (section 3), applied by the kernel
//! migration runner under the ledger key `content`.
use merezha_platform::Migration;

/// The schema's tables and static indexes, exactly as written in section 3.
/// Its `SET LOCAL search_path` ends with the migration's transaction.
pub const MIGRATION_1: &str = include_str!("migrations/0001_merezha_content.sql");

/// Migration 2, a declared non-transactional step (R15): the drafts
/// index whose `entry_id` follows `draft_updated_at` in either direction,
/// so the default `-sys.updated_at` list and `sys.updated_at` are one scan
/// each (C3). `variants_list` stays for the C2 schema.
pub const MIGRATION_2: &str = "CREATE INDEX CONCURRENTLY IF NOT EXISTS variants_by_update \
     ON merezha_content.variants (scope, content_type, locale, draft_updated_at DESC, entry_id DESC) \
     WHERE NOT deleted";

/// Migration 3 (C4): `merezha_content.rebuilds`, the pending fallback
/// rebuilds of `published` after a locale or fallback change, with their
/// resume cursors.
pub const MIGRATION_3: &str = include_str!("migrations/0003_rebuilds.sql");

/// Migration 4 (issue #56): `merezha_content.scopes_deleted`, the
/// organization scopes identity deleted (or an operator purged), which
/// content refuses from then on while their rows are purged.
pub const MIGRATION_4: &str = include_str!("migrations/0004_scopes_deleted.sql");

/// The content block's migrations, in order.
pub const MIGRATIONS: &[Migration] = &[
    Migration::new(1, "merezha_content", MIGRATION_1),
    Migration::non_transactional(2, "variants_by_update", MIGRATION_2),
    Migration::new(3, "rebuilds", MIGRATION_3),
    Migration::new(4, "scopes_deleted", MIGRATION_4),
];
