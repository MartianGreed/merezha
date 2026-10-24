//! The registered statements of delivery, preview and population (C5),
//! each a [`Statement`] run through the TE-T4 registry, so every one has
//! its DB span and `db.client.operation.duration` series. [`STATEMENTS`]
//! lists them for [`merezha_postgres::check_registry`]. The statements of
//! C2 to C4 still use the driver directly (issue #41).
//!
//! Every statement on `published` and `variants` carries the scope and
//! the content type (section 3, scope predicates): a single value for the
//! route's own type, or the arrays of the types and scopes the caller may
//! read for population.
use merezha_postgres::Statement;

/// `to_char` of a `timestamptz` column as RFC 3339 UTC with milliseconds,
/// as [`crate::store::ts`] writes it.
macro_rules! ts {
    ($column:literal) => {
        concat!(
            "to_char(",
            $column,
            " AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')"
        )
    };
}

/// The columns of one delivery document, in the order
/// `Doc::from_row` (`crate::populate`) reads them: id, type, source locale,
/// revision, data, `published_at`, `first_published_at`, scope.
macro_rules! published_columns {
    () => {
        concat!(
            "p.entry_id::text, p.content_type, p.source_locale, p.revision, p.data::text, ",
            ts!("p.published_at"),
            ", ",
            ts!("p.first_published_at"),
            ", p.scope"
        )
    };
}
/// The same columns from a variant: its own locale is the source and its
/// draft revision the revision.
macro_rules! variant_columns {
    () => {
        concat!(
            "v.entry_id::text, v.content_type, v.locale, v.draft_revision, v.draft_data::text, ",
            ts!("v.published_at"),
            ", ",
            ts!("v.first_published_at"),
            ", v.scope"
        )
    };
}
/// Variants with their live entry, joined on the copied scope and type.
macro_rules! live_variants {
    () => {
        "merezha_content.variants v \
         JOIN merezha_content.entries e \
           ON e.id = v.entry_id AND e.scope = v.scope AND e.content_type = v.content_type"
    };
}

/// The generation of each scope a published response depends on: the
/// whole read of the 304 path.
pub const GENERATIONS: Statement = Statement::new(
    "content.delivery.generations",
    "SELECT merezha_content.generations",
    "SELECT scope, generation FROM merezha_content.generations WHERE scope = ANY($1::text[])",
);

/// One published entry by id in a locale; `$5` is false when only the
/// locale's own variant may serve (`fallback=false` on a localized type).
pub const PUBLISHED_ENTRY: Statement = Statement::new(
    "content.delivery.entry",
    "SELECT merezha_content.published",
    concat!(
        "SELECT ",
        published_columns!(),
        " FROM merezha_content.published p \
          WHERE p.scope = $1 AND p.content_type = $2 AND p.locale = $3 \
            AND p.entry_id = $4::text::uuid AND ($5 OR p.source_locale = p.locale)"
    ),
);

/// The scope's published singleton in a locale.
pub const PUBLISHED_SINGLETON: Statement = Statement::new(
    "content.delivery.singleton",
    "SELECT merezha_content.published",
    concat!(
        "SELECT ",
        published_columns!(),
        " FROM merezha_content.published p \
          WHERE p.scope = $1 AND p.content_type = $2 AND p.locale = $3 \
            AND ($4 OR p.source_locale = p.locale) \
          ORDER BY p.entry_id LIMIT 1"
    ),
);

/// A published entry by slug: an own-locale slug first, then fallback
/// rows in chain order (`$6`, which starts with the locale itself), then
/// the entry id.
pub const PUBLISHED_SLUG: Statement = Statement::new(
    "content.delivery.slug",
    "SELECT merezha_content.published",
    concat!(
        "SELECT ",
        published_columns!(),
        " FROM merezha_content.published p \
          WHERE p.scope = $1 AND p.content_type = $2 AND p.locale = $3 AND p.slug = $4 \
            AND ($5 OR p.source_locale = p.locale) \
          ORDER BY array_position($6::text[], p.source_locale), p.entry_id \
          LIMIT 1"
    ),
);

/// The published list of a type, built per request by
/// `crate::delivery`: filters, sort keys and the cursor condition vary,
/// every value is a parameter and the type an identifier literal (the
/// derived indexes are partial on it).
pub const PUBLISHED_LIST: Statement = Statement::new(
    "content.delivery.list",
    "SELECT merezha_content.published",
    "SELECT <document columns>, <sort key texts> FROM merezha_content.published p \
      WHERE p.scope = $1 AND p.content_type = '<type>' AND p.locale = $2 \
        [AND p.source_locale = p.locale] <filters> <after the cursor> \
      ORDER BY <sort keys>, p.entry_id LIMIT <page size + 1>",
);

/// One draft by id, the first live variant of the chain `$4`.
pub const PREVIEW_ENTRY: Statement = Statement::new(
    "content.preview.entry",
    "SELECT merezha_content.variants",
    concat!(
        "SELECT ",
        variant_columns!(),
        " FROM ",
        live_variants!(),
        " WHERE v.scope = $1 AND v.content_type = $2 AND v.entry_id = $3::text::uuid \
            AND v.locale = ANY($4::text[]) AND NOT v.deleted AND e.deleted_at IS NULL \
          ORDER BY array_position($4::text[], v.locale) LIMIT 1"
    ),
);

/// The scope's live singleton draft through the chain `$3`.
pub const PREVIEW_SINGLETON: Statement = Statement::new(
    "content.preview.singleton",
    "SELECT merezha_content.variants",
    concat!(
        "SELECT ",
        variant_columns!(),
        " FROM ",
        live_variants!(),
        " WHERE v.scope = $1 AND v.content_type = $2 AND e.singleton \
            AND v.locale = ANY($3::text[]) AND NOT v.deleted AND e.deleted_at IS NULL \
          ORDER BY array_position($3::text[], v.locale), v.entry_id LIMIT 1"
    ),
);

/// A draft by slug: each candidate entry resolved through the chain `$3`
/// first, then the chain order (the locale's own variant first).
pub const PREVIEW_SLUG: Statement = Statement::new(
    "content.preview.slug",
    "SELECT merezha_content.variants",
    concat!(
        "SELECT v.entry_id, v.content_type, v.locale, v.draft_revision, v.draft_data, \
                v.published_at, v.first_published_at, v.scope FROM (\
           SELECT DISTINCT ON (v.entry_id) ",
        variant_columns!(),
        ", v.draft_slug FROM ",
        live_variants!(),
        " WHERE v.scope = $1 AND v.content_type = $2 AND v.locale = ANY($3::text[]) \
              AND NOT v.deleted AND e.deleted_at IS NULL \
              AND v.entry_id IN (SELECT s.entry_id FROM merezha_content.variants s \
                                  WHERE s.scope = $1 AND s.content_type = $2 \
                                    AND s.draft_slug = $4 AND s.locale = ANY($3::text[]) \
                                    AND NOT s.deleted) \
            ORDER BY v.entry_id, array_position($3::text[], v.locale)\
         ) AS v (entry_id, content_type, locale, draft_revision, draft_data, \
                 published_at, first_published_at, scope, draft_slug) \
         WHERE v.draft_slug = $4 \
         ORDER BY array_position($3::text[], v.locale), v.entry_id \
         LIMIT 1"
    ),
);

/// The preview list of a type, built per request: the live variants of
/// the chain resolved per entry (`DISTINCT ON`), then the filters, sort
/// keys and cursor condition over the resolved variants.
pub const PREVIEW_LIST: Statement = Statement::new(
    "content.preview.list",
    "SELECT merezha_content.variants",
    "SELECT <document columns>, <sort key texts> FROM (SELECT DISTINCT ON (v.entry_id) ... \
       FROM merezha_content.variants v JOIN merezha_content.entries e ... \
      WHERE v.scope = $1 AND v.content_type = '<type>' AND v.locale = ANY($2::text[]) \
        AND NOT v.deleted AND e.deleted_at IS NULL \
      ORDER BY v.entry_id, array_position($2::text[], v.locale)) v \
     WHERE true <filters> <after the cursor> ORDER BY <sort keys>, v.entry_id LIMIT <n>",
);

/// One population level, published stage: the referenced entries among
/// the readable types `$4` and their scopes `$3`, in locale `$2`. `$5`
/// allows fallback rows; `$6` lists the localized types it applies to.
pub const POPULATE_PUBLISHED: Statement = Statement::new(
    "content.populate.published",
    "SELECT merezha_content.published",
    concat!(
        "SELECT ",
        published_columns!(),
        " FROM merezha_content.published p \
          WHERE p.entry_id = ANY($1::text[]::uuid[]) AND p.locale = $2 \
            AND p.scope = ANY($3::text[]) AND p.content_type = ANY($4::text[]) \
            AND ($5 OR p.source_locale = p.locale OR NOT (p.content_type = ANY($6::text[])))"
    ),
);

/// One population level, draft stage: each referenced entry's first live
/// variant of the chain `$2`; without fallback (`$5` false) a localized
/// type (`$6`) serves only the requested locale `$7`.
pub const POPULATE_DRAFT: Statement = Statement::new(
    "content.populate.draft",
    "SELECT merezha_content.variants",
    concat!(
        "SELECT DISTINCT ON (v.entry_id) ",
        variant_columns!(),
        " FROM ",
        live_variants!(),
        " WHERE v.entry_id = ANY($1::text[]::uuid[]) AND v.locale = ANY($2::text[]) \
            AND v.scope = ANY($3::text[]) AND v.content_type = ANY($4::text[]) \
            AND NOT v.deleted AND e.deleted_at IS NULL \
            AND ($5 OR v.locale = $7 OR NOT (v.content_type = ANY($6::text[]))) \
          ORDER BY v.entry_id, array_position($2::text[], v.locale)"
    ),
);

/// Which referenced entries a response may show, published stage: the
/// predicates of [`POPULATE_PUBLISHED`], ids only.
pub const VISIBLE_PUBLISHED: Statement = Statement::new(
    "content.populate.visible_published",
    "SELECT merezha_content.published",
    "SELECT DISTINCT p.entry_id::text, p.content_type, p.scope FROM merezha_content.published p \
      WHERE p.entry_id = ANY($1::text[]::uuid[]) AND p.locale = $2 \
        AND p.scope = ANY($3::text[]) AND p.content_type = ANY($4::text[]) \
        AND ($5 OR p.source_locale = p.locale OR NOT (p.content_type = ANY($6::text[])))",
);

/// Which referenced entries a response may show, draft stage: the
/// predicates of [`POPULATE_DRAFT`], ids only.
pub const VISIBLE_DRAFT: Statement = Statement::new(
    "content.populate.visible_draft",
    "SELECT merezha_content.variants",
    concat!(
        "SELECT DISTINCT v.entry_id::text, v.content_type, v.scope FROM ",
        live_variants!(),
        " WHERE v.entry_id = ANY($1::text[]::uuid[]) AND v.locale = ANY($2::text[]) \
            AND v.scope = ANY($3::text[]) AND v.content_type = ANY($4::text[]) \
            AND NOT v.deleted AND e.deleted_at IS NULL \
            AND ($5 OR v.locale = $7 OR NOT (v.content_type = ANY($6::text[])))"
    ),
);

/// `EXPLAIN (COSTS OFF)` of a delivery list, for index tests and
/// operators (`ContentState::explain_delivery`).
pub const EXPLAIN_LIST: Statement = Statement::new(
    "content.delivery.explain",
    "EXPLAIN",
    "EXPLAIN (COSTS OFF) <a delivery list>",
);

/// Every statement of this module.
pub const STATEMENTS: &[Statement] = &[
    GENERATIONS,
    PUBLISHED_ENTRY,
    PUBLISHED_SINGLETON,
    PUBLISHED_SLUG,
    PUBLISHED_LIST,
    PREVIEW_ENTRY,
    PREVIEW_SINGLETON,
    PREVIEW_SLUG,
    PREVIEW_LIST,
    POPULATE_PUBLISHED,
    POPULATE_DRAFT,
    VISIBLE_PUBLISHED,
    VISIBLE_DRAFT,
    EXPLAIN_LIST,
];

/// The variant columns of the preview list's resolved subquery, the same
/// as [`PREVIEW_ENTRY`]'s.
pub(crate) const PREVIEW_COLUMNS: &str = variant_columns!();
/// The document columns of the published list.
pub(crate) const PUBLISHED_COLUMNS: &str = published_columns!();
/// Live variants, for the preview list's subquery.
pub(crate) const LIVE_VARIANTS: &str = live_variants!();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_statements_register_beside_the_kernels() {
        merezha_postgres::check_registry(&[merezha_postgres::KERNEL_STATEMENTS, STATEMENTS])
            .unwrap();
        for statement in STATEMENTS {
            assert!(statement.name.starts_with("content."), "{}", statement.name);
        }
    }

    /// The C5 sources hand no SQL to the driver outside the registry.
    #[test]
    fn delivery_sources_have_no_unregistered_sql() {
        for (name, source) in [
            ("delivery.rs", include_str!("delivery.rs")),
            ("populate.rs", include_str!("populate.rs")),
            ("statements.rs", include_str!("statements.rs")),
        ] {
            assert_eq!(
                merezha_postgres::statement::unregistered_calls(source),
                Vec::<(usize, &str)>::new(),
                "{name}"
            );
        }
    }
}
