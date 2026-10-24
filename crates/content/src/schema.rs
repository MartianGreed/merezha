//! Migration 1 of `merezha_content` (section 3), applied by the kernel
//! migration runner under the ledger key `content`.
use merezha_platform::Migration;

/// The schema's tables and static indexes, as written in section 3. Its
/// `SET LOCAL search_path` ends with the migration's transaction.
pub const MIGRATION_1: &str = include_str!("migrations/0001_merezha_content.sql");

/// The content block's migrations, in order.
pub const MIGRATIONS: &[Migration] = &[Migration::new(1, "merezha_content", MIGRATION_1)];
