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

/// The content block's migrations, in order.
pub const MIGRATIONS: &[Migration] = &[
    Migration::new(1, "merezha_content", MIGRATION_1),
    Migration::non_transactional(2, "variants_by_update", MIGRATION_2),
];
