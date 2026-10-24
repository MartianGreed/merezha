//! Derived indexes (section 3): the partial B-tree and unique indexes a
//! model needs beyond the static ones of migration 1, named
//! `content_ix_<16 hex of the definition's hash>` so the set a model expects
//! can be compared with `pg_index` by name alone.
use crate::model::{ContentModel, ContentType, Field, FieldKind};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

/// Every derived index name starts with this; nothing else in the schema
/// does, so an index with this prefix that the active model does not
/// expect is dropped.
pub const INDEX_PREFIX: &str = "content_ix_";
/// The content schema.
pub const SCHEMA: &str = "merezha_content";

/// The table a derived index is built on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IndexTable {
    /// Drafts: `variants.draft_data`, rows not deleted.
    Variants,
    /// Delivery rows: `published.data`.
    Published,
}
impl IndexTable {
    pub fn as_str(self) -> &'static str {
        match self {
            IndexTable::Variants => "variants",
            IndexTable::Published => "published",
        }
    }
}

/// Why a derived index exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IndexPurpose {
    /// A `sortable` field: range filters and keyset sorts.
    Sort,
    /// A `unique` field, slugs included: unique per type, scope and locale
    /// (per type and scope for shared fields).
    Unique,
}
impl IndexPurpose {
    pub fn as_str(self) -> &'static str {
        match self {
            IndexPurpose::Sort => "sort",
            IndexPurpose::Unique => "unique",
        }
    }
}

/// One index the model needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DerivedIndex {
    /// `content_ix_` plus 16 hex characters.
    pub name: String,
    pub table: IndexTable,
    pub purpose: IndexPurpose,
    pub content_type: String,
    /// The top-level field the index covers. A unique violation on this
    /// index is a `unique` detail at `/fields/<field>`.
    pub field: String,
    /// The `CREATE [UNIQUE] INDEX CONCURRENTLY IF NOT EXISTS` statement.
    pub create: String,
}
impl DerivedIndex {
    /// `DROP INDEX CONCURRENTLY IF EXISTS` for this index.
    pub fn drop_statement(&self) -> String {
        drop_statement(&self.name)
    }
}

/// `DROP INDEX CONCURRENTLY IF EXISTS merezha_content.<name>` for a
/// derived index name. Names come from `pg_class` or [`derived_indexes`]
/// and are checked to be `content_ix_` plus lowercase hex.
pub fn drop_statement(name: &str) -> String {
    debug_assert!(is_derived_name(name));
    format!("DROP INDEX CONCURRENTLY IF EXISTS {SCHEMA}.{name}")
}

/// `content_ix_` followed by exactly 16 lowercase hex digits.
pub fn is_derived_name(name: &str) -> bool {
    name.strip_prefix(INDEX_PREFIX).is_some_and(|hex| {
        hex.len() == 16 && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    })
}

/// Every derived index `model` needs, in type, field and table order: for
/// each `sortable` field a partial B-tree on `variants` and `published`,
/// for each `unique` field a partial unique index on both.
///
/// ```
/// use merezha_content::{ContentModel, ModelOptions, derived_indexes};
/// let options = ModelOptions { permissions: &["tasks.read"], media: false, organizations: false };
/// let model = ContentModel::parse(r#"
/// version = 1
/// locales = [{ code = "en", default = true }]
/// [types.article]
/// access = "public"
/// fields = [{ name = "reading_minutes", type = "integer", sortable = true }]
/// "#, &options).unwrap();
/// let indexes = derived_indexes(&model);
/// assert_eq!(indexes.len(), 2);
/// assert!(indexes[1].create.ends_with(
///     "ON merezha_content.published (scope, locale, ((data->'reading_minutes') IS NULL), \
///      (((data->>'reading_minutes'))::bigint), entry_id) WHERE content_type = 'article'"));
/// ```
pub fn derived_indexes(model: &ContentModel) -> Vec<DerivedIndex> {
    let default_locale = model.default_locale().code.as_str();
    let mut out = Vec::new();
    for content_type in model.types().values() {
        for field in &content_type.fields {
            if field.sortable {
                for table in [IndexTable::Variants, IndexTable::Published] {
                    out.push(sort_index(content_type, field, table));
                }
            }
            if field.unique {
                for table in [IndexTable::Variants, IndexTable::Published] {
                    out.push(unique_index(content_type, field, table, default_locale));
                }
            }
        }
    }
    out
}

fn data_column(table: IndexTable) -> &'static str {
    match table {
        IndexTable::Variants => "draft_data",
        IndexTable::Published => "data",
    }
}

fn cast(kind: &FieldKind) -> &'static str {
    match kind {
        FieldKind::Integer(_) => "::bigint",
        FieldKind::Float(_) => "::float8",
        FieldKind::Decimal(_) => "::numeric",
        // Text for the rest, `datetime` included: a text to timestamptz cast
        // is not immutable, and the stored UTC form sorts in time order.
        _ => "",
    }
}

fn sort_index(content_type: &ContentType, field: &Field, table: IndexTable) -> DerivedIndex {
    let data = data_column(table);
    let key = literal(&field.name);
    let value = match cast(&field.kind) {
        "" => format!("(({data}->>{key}))"),
        cast => format!("((({data}->>{key})){cast})"),
    };
    let columns = format!("(scope, locale, (({data}->{key}) IS NULL), {value}, entry_id)");
    let mut predicate = format!("content_type = {}", literal(&content_type.name));
    if table == IndexTable::Variants {
        predicate.push_str(" AND NOT deleted");
    }
    index(
        content_type,
        field,
        table,
        IndexPurpose::Sort,
        &columns,
        &predicate,
    )
}

fn unique_index(
    content_type: &ContentType,
    field: &Field,
    table: IndexTable,
    default_locale: &str,
) -> DerivedIndex {
    let data = data_column(table);
    let value = format!("(({data}->>{}))", literal(&field.name));
    let mut predicate = format!("content_type = {}", literal(&content_type.name));
    if table == IndexTable::Variants {
        predicate.push_str(" AND NOT deleted");
    }
    // A shared field (and every field of a non-localized type) belongs to
    // the default variant: unique per type and scope, checked on the
    // default locale's rows only, since the other variants carry copies.
    let columns = if field.localized {
        format!("(scope, locale, {value})")
    } else {
        let _ = write!(predicate, " AND locale = {}", literal(default_locale));
        format!("(scope, {value})")
    };
    if table == IndexTable::Published {
        // Fallback rows repeat another locale's value; only own rows count.
        predicate.push_str(" AND source_locale = locale");
    }
    index(
        content_type,
        field,
        table,
        IndexPurpose::Unique,
        &columns,
        &predicate,
    )
}

fn index(
    content_type: &ContentType,
    field: &Field,
    table: IndexTable,
    purpose: IndexPurpose,
    columns: &str,
    predicate: &str,
) -> DerivedIndex {
    let unique = if purpose == IndexPurpose::Unique {
        "UNIQUE "
    } else {
        ""
    };
    let body = format!("ON {SCHEMA}.{} {columns} WHERE {predicate}", table.as_str());
    let digest = Sha256::digest(format!("{unique}INDEX {body}").as_bytes());
    let mut name = String::from(INDEX_PREFIX);
    for byte in &digest[..8] {
        let _ = write!(name, "{byte:02x}");
    }
    let create = format!("CREATE {unique}INDEX CONCURRENTLY IF NOT EXISTS {name} {body}");
    DerivedIndex {
        name,
        table,
        purpose,
        content_type: content_type.name.clone(),
        field: field.name.clone(),
        create,
    }
}

/// A SQL string literal. Type, field and locale names are already limited
/// to `[A-Za-z0-9_-]` by the model; quotes are doubled regardless.
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_names_are_recognized_exactly() {
        assert!(is_derived_name("content_ix_3f9a0c1d2e4b5a69"));
        for name in [
            "content_ix_3f9a0c1d2e4b5a6",
            "content_ix_3f9a0c1d2e4b5a690",
            "content_ix_3F9A0C1D2E4B5A69",
            "content_ix_3f9a0c1d2e4b5a6;",
            "variants_list",
        ] {
            assert!(!is_derived_name(name), "{name}");
        }
    }
    #[test]
    fn literals_double_quotes() {
        assert_eq!(literal("a'b"), "'a''b'");
    }
}
