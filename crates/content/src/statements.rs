//! Every statement of the content block outside [`crate::media`] (whose
//! own are in `media::sql`): model activation and derived indexes (C2),
//! entries, drafts, revisions and references (C3), publishing, deletion,
//! the trash and the fallback rebuild (C4), delivery, preview and
//! population (C5), and `merezha content diff`'s read-only transaction.
//! Each is a [`Statement`] run through the TE-T4 registry, so every one
//! has its DB span and `db.client.operation.duration` series.
//! [`STATEMENTS`] lists them for [`merezha_postgres::check_registry`].
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

// ----- model activation and derived indexes (C2) -----
pub(crate) const MODELS_EXIST: Statement = Statement::new(
    "content.models.exist",
    "SELECT to_regclass()",
    "SELECT to_regclass('merezha_content.models') IS NOT NULL",
);
pub(crate) const MODEL_ACTIVE: Statement = Statement::new(
    "content.models.active",
    "SELECT merezha_content.models",
    "SELECT version, fingerprint, definition::text FROM merezha_content.models \
             WHERE state = 'active'",
);
pub(crate) const DERIVED_INDEXES: Statement = Statement::new(
    "content.indexes.list",
    "SELECT pg_index",
    "SELECT c.relname::text, i.indisvalid FROM pg_index i \
             JOIN pg_class c ON c.oid = i.indexrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'merezha_content' AND c.relname LIKE 'content\\_ix\\_%'",
);
/// Text built at run time; `sql` is its shape.
pub(crate) const SESSION_TIMEOUT: Statement = Statement::new(
    "content.activation.timeout",
    "SET",
    "SET statement_timeout = <milliseconds>",
);
/// Text built at run time; `sql` is its shape.
pub(crate) const INDEX_DROP: Statement = Statement::new(
    "content.indexes.drop",
    "DROP INDEX",
    "DROP INDEX CONCURRENTLY IF EXISTS merezha_content.<derived index>",
);
pub(crate) const ACTIVATION_TRY_LOCK: Statement = Statement::new(
    "content.activation.lock",
    "SELECT pg_try_advisory_lock()",
    "SELECT pg_try_advisory_lock($1)",
);
pub(crate) const INDEX_INVALID: Statement = Statement::new(
    "content.indexes.invalid",
    "SELECT pg_index",
    "SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = 'merezha_content' AND c.relname = $1 AND NOT i.indisvalid",
);
/// Text built at run time; `sql` is its shape.
pub(crate) const INDEX_CREATE: Statement = Statement::new(
    "content.indexes.create",
    "CREATE INDEX",
    "CREATE [UNIQUE] INDEX CONCURRENTLY IF NOT EXISTS <derived index> ON merezha_content.<variants or published> <definition>",
);
pub(crate) const MODEL_ACTIVE_LOCK: Statement = Statement::new(
    "content.models.lock_active",
    "SELECT merezha_content.models",
    "SELECT version FROM merezha_content.models \
                         WHERE state = 'active' FOR UPDATE",
);
pub(crate) const MODEL_SUPERSEDE: Statement = Statement::new(
    "content.models.supersede",
    "UPDATE merezha_content.models",
    "UPDATE merezha_content.models SET state = 'superseded' WHERE state = 'active'",
);
pub(crate) const MODEL_INSERT: Statement = Statement::new(
    "content.models.insert",
    "INSERT merezha_content.models",
    "INSERT INTO merezha_content.models \
                     (version, fingerprint, definition, state, activated_at) \
                     VALUES ($1, $2, $3::text::jsonb, 'active', now())",
);
pub(crate) const LOCALE_INSERT: Statement = Statement::new(
    "content.locales.insert",
    "INSERT merezha_content.locales",
    "INSERT INTO merezha_content.locales (code, is_default) VALUES ($1, $2) \
                         ON CONFLICT (code) DO NOTHING",
);
pub(crate) const LOCALE_FALLBACK: Statement = Statement::new(
    "content.locales.fallback",
    "UPDATE merezha_content.locales",
    "UPDATE merezha_content.locales SET fallback = $2 \
                         WHERE code = $1 AND fallback IS DISTINCT FROM $2",
);
pub(crate) const REBUILD_SUPERSEDE: Statement = Statement::new(
    "content.rebuilds.supersede",
    "UPDATE merezha_content.rebuilds",
    "UPDATE merezha_content.rebuilds \
                                SET state = 'superseded', updated_at = now() \
                              WHERE state = 'pending'",
);
pub(crate) const REBUILD_INSERT: Statement = Statement::new(
    "content.rebuilds.insert",
    "INSERT merezha_content.rebuilds",
    "INSERT INTO merezha_content.rebuilds (version, state, updated_at) \
                             VALUES ($1, 'pending', now())",
);
pub(crate) const GENERATIONS_BUMP_ALL: Statement = Statement::new(
    "content.generations.bump_all",
    "UPDATE merezha_content.generations",
    "UPDATE merezha_content.generations SET generation = generation + 1",
);
pub(crate) const TIGHTENED_PUBLISHED: Statement = Statement::new(
    "content.activation.tightened_published",
    "SELECT merezha_content.published",
    "SELECT entry_id::text, locale, source_locale, data::text FROM merezha_content.published \
                 WHERE content_type = $1 AND source_locale = locale \
                   AND (entry_id, locale) > ($2::text::uuid, $3) \
                 ORDER BY entry_id, locale LIMIT $4",
);
pub(crate) const TIGHTENED_DRAFTS: Statement = Statement::new(
    "content.activation.tightened_drafts",
    "SELECT merezha_content.variants",
    "SELECT entry_id::text, locale, locale, draft_data::text FROM merezha_content.variants \
                 WHERE content_type = $1 AND NOT deleted AND (entry_id, locale) > ($2::text::uuid, $3) \
                 ORDER BY entry_id, locale LIMIT $4",
);

// ----- the model version check and readiness (C2) -----
pub(crate) const MODEL_ACTIVE_VERSION: Statement = Statement::new(
    "content.models.active_version",
    "SELECT merezha_content.models",
    "SELECT version FROM merezha_content.models WHERE state = 'active'",
);
pub(crate) const MODEL_VERSION_LOCK: Statement = Statement::new(
    "content.models.lock_version",
    "SELECT merezha_content.models",
    "SELECT 1 FROM merezha_content.models WHERE version = $1 AND state = 'active' FOR SHARE",
);

// ----- publishing and materialization (C4) -----
pub(crate) const PUBLISHED_REVISIONS: Statement = Statement::new(
    "content.publish.revisions",
    "SELECT merezha_content.variants",
    "SELECT v.locale, r.data::text FROM merezha_content.variants v \
               JOIN merezha_content.revisions r \
                 ON r.entry_id = v.entry_id AND r.locale = v.locale \
                AND r.revision = v.published_revision \
              WHERE v.entry_id = $1::text::uuid AND v.scope = $2 AND v.content_type = $3 \
                AND NOT v.deleted AND v.published_revision IS NOT NULL",
);
pub(crate) const PUBLISHED_DELETE: Statement = Statement::new(
    "content.published.delete",
    "DELETE merezha_content.published",
    "DELETE FROM merezha_content.published \
          WHERE entry_id = $1::text::uuid AND scope = $2 AND content_type = $3",
);
pub(crate) const PUBLISHED_INSERT: Statement = Statement::new(
    "content.published.insert",
    "INSERT merezha_content.published",
    "INSERT INTO merezha_content.published \
             (entry_id, locale, source_locale, scope, content_type, revision, slug, data, \
              published_at, first_published_at) \
         SELECT $1::text::uuid, t.locale, t.source, $2, $3, v.published_revision, t.slug, \
                t.data::jsonb, coalesce(v.published_at, now()), \
                coalesce(v.first_published_at, v.published_at, now()) \
           FROM unnest($4::text[], $5::text[], $6::text[], $7::text[]) AS t(locale, source, slug, data) \
           JOIN merezha_content.variants v \
             ON v.entry_id = $1::text::uuid AND v.locale = t.source \
            AND v.scope = $2 AND v.content_type = $3",
);
pub(crate) const GENERATION_BUMP: Statement = Statement::new(
    "content.generations.bump",
    "INSERT merezha_content.generations",
    "INSERT INTO merezha_content.generations (scope, generation) VALUES ($1, 1) \
         ON CONFLICT (scope) DO UPDATE SET generation = merezha_content.generations.generation + 1",
);
pub(crate) const ENTRY_LOCK: Statement = Statement::new(
    "content.entries.lock",
    "SELECT merezha_content.entries",
    "SELECT 1 FROM merezha_content.entries \
          WHERE id = $1::text::uuid AND scope = $2 AND content_type = $3 \
            AND deleted_at IS NULL FOR UPDATE",
);
pub(crate) const VARIANT_LOCK: Statement = Statement::new(
    "content.variants.lock",
    "SELECT merezha_content.variants",
    "SELECT draft_revision, published_revision, first_published_at IS NULL \
               FROM merezha_content.variants \
              WHERE entry_id = $1::text::uuid AND locale = $2 AND scope = $3 \
                AND content_type = $4 AND NOT deleted FOR UPDATE",
);
pub(crate) const REFS_DELETE_STAGE: Statement = Statement::new(
    "content.refs.delete_stage",
    "DELETE merezha_content.refs",
    "DELETE FROM merezha_content.refs \
          WHERE from_entry = $1::text::uuid AND from_locale = $2 AND stage = $3 AND scope = $4",
);
pub(crate) const REFS_INSERT: Statement = Statement::new(
    "content.refs.insert",
    "INSERT merezha_content.refs",
    "INSERT INTO merezha_content.refs \
             (scope, from_entry, from_locale, stage, path, target_kind, target_id) \
         SELECT $1, $2::text::uuid, $3, $4, path, kind, target::uuid \
           FROM unnest($5::text[], $6::text[], $7::text[]) AS t(path, kind, target) \
         ON CONFLICT DO NOTHING",
);
pub(crate) const REQUIRED_TARGET_SHARE: Statement = Statement::new(
    "content.publish.required_lock",
    "SELECT merezha_content.entries",
    "SELECT 1 FROM merezha_content.entries \
                  WHERE id = $1::text::uuid AND (scope = $2 OR scope = 'app') FOR SHARE",
);
pub(crate) const REQUIRED_PUBLISHED: Statement = Statement::new(
    "content.publish.required_published",
    "SELECT merezha_content.published",
    "SELECT 1 FROM merezha_content.published \
                      WHERE entry_id = $1::text::uuid AND locale = $2 \
                        AND (scope = $3 OR scope = 'app') \
                        AND (cardinality($4::text[]) = 0 OR content_type = ANY($4::text[]))",
);
pub(crate) const REVISION_DATA: Statement = Statement::new(
    "content.revisions.data",
    "SELECT merezha_content.revisions",
    "SELECT r.data::text FROM merezha_content.revisions r \
                   JOIN merezha_content.variants v \
                     ON v.entry_id = r.entry_id AND v.locale = r.locale \
                  WHERE r.entry_id = $1::text::uuid AND r.locale = $2 \
                    AND v.scope = $3 AND v.content_type = $4 AND r.revision = $5",
);
pub(crate) const VARIANT_PUBLISH: Statement = Statement::new(
    "content.variants.publish",
    "UPDATE merezha_content.variants",
    "UPDATE merezha_content.variants \
                SET published_revision = $5, published_at = now(), \
                    first_published_at = coalesce(first_published_at, now()) \
              WHERE entry_id = $1::text::uuid AND locale = $2 AND scope = $3 \
                AND content_type = $4 AND NOT deleted",
);
pub(crate) const PUBLISHED_SLUG_READ: Statement = Statement::new(
    "content.published.slug",
    "SELECT merezha_content.published",
    "SELECT slug FROM merezha_content.published \
                  WHERE entry_id = $1::text::uuid AND locale = $2 AND scope = $3 \
                    AND content_type = $4",
);
pub(crate) const VARIANT_WITHDRAW: Statement = Statement::new(
    "content.variants.withdraw",
    "UPDATE merezha_content.variants",
    "UPDATE merezha_content.variants SET published_revision = NULL, published_at = NULL \
              WHERE entry_id = $1::text::uuid AND locale = $2 AND scope = $3 \
                AND content_type = $4 AND NOT deleted",
);
pub(crate) const REFS_DELETE_VARIANT: Statement = Statement::new(
    "content.refs.delete_variant",
    "DELETE merezha_content.refs",
    "DELETE FROM merezha_content.refs \
              WHERE from_entry = $1::text::uuid AND from_locale = $2 AND scope = $3",
);
pub(crate) const VARIANT_DELETE: Statement = Statement::new(
    "content.variants.delete",
    "DELETE merezha_content.variants",
    "DELETE FROM merezha_content.variants \
              WHERE entry_id = $1::text::uuid AND locale = $2 AND scope = $3 \
                AND content_type = $4",
);

// ----- entries, drafts, revisions and referrers (C3) -----
/// Text built at run time; `sql` is its shape.
pub(crate) const ENTRY_SUMMARY: Statement = Statement::new(
    "content.entries.summary",
    "SELECT merezha_content.entries",
    "SELECT id::text, content_type, <created_at text>, <deleted_at text> FROM merezha_content.entries WHERE id = $1::text::uuid AND scope = $2 AND content_type = $3",
);
/// Text built at run time; `sql` is its shape.
pub(crate) const ENTRY_VARIANTS: Statement = Statement::new(
    "content.variants.of_entry",
    "SELECT merezha_content.variants",
    "SELECT locale, draft_revision, published_revision, <draft_updated_at text> FROM merezha_content.variants WHERE entry_id = $1::text::uuid AND scope = $2 AND content_type = $3",
);
pub(crate) const SINGLETON_ID: Statement = Statement::new(
    "content.entries.singleton",
    "SELECT merezha_content.entries",
    "SELECT id::text FROM merezha_content.entries \
                          WHERE scope = $1 AND content_type = $2 AND singleton \
                            AND deleted_at IS NULL",
);
/// Text built at run time; `sql` is its shape.
pub(crate) const REVISION_LIST: Statement = Statement::new(
    "content.revisions.list",
    "SELECT merezha_content.revisions",
    "SELECT r.revision, r.kind, r.restored_from, r.model_version, <created_at text>, r.created_by, r.data->>$5, <published> FROM merezha_content.revisions r JOIN merezha_content.variants v ... WHERE ... AND ($6::int8 IS NULL OR r.revision < $6) ORDER BY r.revision DESC LIMIT $7",
);
/// Text built at run time; `sql` is its shape.
pub(crate) const REVISION_READ: Statement = Statement::new(
    "content.revisions.read",
    "SELECT merezha_content.revisions",
    "SELECT <the revision list columns>, r.data::text FROM merezha_content.revisions r JOIN merezha_content.variants v ... WHERE ... AND r.revision = $6",
);
pub(crate) const ENTRY_EXISTS: Statement = Statement::new(
    "content.entries.exists",
    "SELECT merezha_content.entries",
    "SELECT 1 FROM merezha_content.entries \
                      WHERE id = $1::text::uuid AND scope = $2 AND content_type = $3",
);
/// Text built at run time; `sql` is its shape.
pub(crate) const REFERRERS: Statement = Statement::new(
    "content.refs.referrers",
    "SELECT merezha_content.refs",
    "SELECT f.from_entry::text, e.content_type, f.from_locale, f.stage, f.path, CASE e.content_type WHEN '<type>' THEN <stage data> ->> '<title field>' ... END FROM merezha_content.refs f JOIN merezha_content.entries e ... WHERE f.target_kind = 'entry' AND f.target_id = $1::text::uuid ... ORDER BY f.from_entry, f.from_locale, f.path LIMIT $7",
);
/// Text built at run time; `sql` is its shape.
pub(crate) const MANAGE_LIST: Statement = Statement::new(
    "content.manage.list",
    "SELECT merezha_content.variants",
    "SELECT <variant columns><sort key texts> FROM merezha_content.variants v JOIN merezha_content.entries e ... JOIN merezha_content.revisions r ... WHERE v.scope = $1 AND v.content_type = '<type>' AND v.locale = $2 AND NOT v.deleted AND e.deleted_at IS NULL <status> <filters> <after the cursor> ORDER BY <sort keys> LIMIT $<n>",
);
pub(crate) const VARIANT_EXISTS: Statement = Statement::new(
    "content.variants.exists",
    "SELECT merezha_content.variants",
    "SELECT 1 FROM merezha_content.variants \
          WHERE entry_id = $1::text::uuid AND locale = $2 AND scope = $3 \
            AND content_type = $4 AND NOT deleted",
);
/// Text built at run time; `sql` is its shape.
pub(crate) const VARIANT_VIEW: Statement = Statement::new(
    "content.variants.view",
    "SELECT merezha_content.variants",
    "SELECT <variant columns> FROM merezha_content.variants v JOIN merezha_content.entries e ... JOIN merezha_content.revisions r ... WHERE v.entry_id = $1::text::uuid AND v.locale = $2 AND v.scope = $3 AND v.content_type = $4 [AND NOT v.deleted AND e.deleted_at IS NULL]",
);
pub(crate) const ENTRY_INSERT: Statement = Statement::new(
    "content.entries.insert",
    "INSERT merezha_content.entries",
    "INSERT INTO merezha_content.entries \
                         (id, scope, content_type, singleton, created_at, created_by) \
                     VALUES ($1::text::uuid, $2, $3, $4, now(), $5)",
);
pub(crate) const ENTRY_SHARE: Statement = Statement::new(
    "content.entries.share",
    "SELECT merezha_content.entries",
    "SELECT 1 FROM merezha_content.entries \
                      WHERE id = $1::text::uuid AND scope = $2 AND content_type = $3 \
                        AND deleted_at IS NULL FOR SHARE",
);
pub(crate) const DRAFT_CURRENT: Statement = Statement::new(
    "content.variants.draft",
    "SELECT merezha_content.variants",
    "SELECT draft_revision, draft_data::text FROM merezha_content.variants \
                  WHERE entry_id = $1::text::uuid AND locale = $2 AND scope = $3 \
                    AND content_type = $4 AND NOT deleted",
);
pub(crate) const DEFAULT_DRAFT_SHARE: Statement = Statement::new(
    "content.variants.default_share",
    "SELECT merezha_content.variants",
    "SELECT draft_data::text FROM merezha_content.variants \
                      WHERE entry_id = $1::text::uuid AND locale = $2 AND scope = $3 \
                        AND content_type = $4 AND NOT deleted FOR SHARE",
);
pub(crate) const VARIANT_INSERT: Statement = Statement::new(
    "content.variants.insert",
    "INSERT merezha_content.variants",
    "INSERT INTO merezha_content.variants \
                         (entry_id, locale, scope, content_type, draft_revision, draft_data, \
                          draft_slug, draft_updated_at, draft_updated_by) \
                     VALUES ($1::text::uuid, $2, $3, $4, 1, $5::text::jsonb, $6, now(), $7)",
);
pub(crate) const DRAFT_UPDATE: Statement = Statement::new(
    "content.variants.save",
    "UPDATE merezha_content.variants",
    "UPDATE merezha_content.variants \
                            SET draft_revision = draft_revision + 1, \
                                draft_data = $6::text::jsonb, draft_slug = $7, \
                                draft_updated_at = now(), draft_updated_by = $8 \
                          WHERE entry_id = $1::text::uuid AND locale = $2 AND scope = $3 \
                            AND content_type = $4 AND draft_revision = $5 AND NOT deleted \
                         RETURNING draft_revision",
);
pub(crate) const REVISION_INSERT: Statement = Statement::new(
    "content.revisions.insert",
    "INSERT merezha_content.revisions",
    "INSERT INTO merezha_content.revisions \
                 (entry_id, locale, revision, model_version, kind, restored_from, data, \
                  created_at, created_by) \
             VALUES ($1::text::uuid, $2, $3, $4, $5, $6, $7::text::jsonb, now(), $8)",
);
pub(crate) const REVISION_PRUNE: Statement = Statement::new(
    "content.revisions.prune",
    "DELETE merezha_content.revisions",
    "DELETE FROM merezha_content.revisions r USING merezha_content.variants v \
              WHERE r.entry_id = $1::text::uuid AND r.locale = $2 \
                AND v.entry_id = r.entry_id AND v.locale = r.locale \
                AND v.scope = $3 AND v.content_type = $4 \
                AND r.revision <= v.draft_revision - $5 \
                AND r.revision IS DISTINCT FROM v.published_revision",
);
pub(crate) const REFS_DELETE_DRAFT: Statement = Statement::new(
    "content.refs.delete_draft",
    "DELETE merezha_content.refs",
    "DELETE FROM merezha_content.refs \
              WHERE from_entry = $1::text::uuid AND from_locale = $2 AND stage = 'draft' \
                AND scope = $3",
);
pub(crate) const REFS_INSERT_DRAFT: Statement = Statement::new(
    "content.refs.insert_draft",
    "INSERT merezha_content.refs",
    "INSERT INTO merezha_content.refs \
                     (scope, from_entry, from_locale, stage, path, target_kind, target_id) \
                 SELECT $1, $2::text::uuid, $3, 'draft', path, kind, target::uuid \
                   FROM unnest($4::text[], $5::text[], $6::text[]) AS t(path, kind, target) \
                 ON CONFLICT DO NOTHING",
);
pub(crate) const SHARED_COPY: Statement = Statement::new(
    "content.variants.copy_shared",
    "UPDATE merezha_content.variants",
    "UPDATE merezha_content.variants \
                    SET draft_data = (draft_data - $5::text[]) || $6::text::jsonb, \
                        draft_slug = CASE WHEN $7 THEN $8 ELSE draft_slug END \
                  WHERE entry_id = $1::text::uuid AND scope = $2 AND content_type = $3 \
                    AND locale <> $4 AND NOT deleted",
);

// ----- deletion, the trash and its purge (C4) -----
pub(crate) const ENTRY_LOCK_ANY: Statement = Statement::new(
    "content.trash.lock_live",
    "SELECT merezha_content.entries",
    "SELECT deleted_at IS NULL FROM merezha_content.entries \
                  WHERE id = $1::text::uuid AND scope = $2 AND content_type = $3 FOR UPDATE",
);
pub(crate) const ENTRY_REFERENCED: Statement = Statement::new(
    "content.trash.referenced",
    "SELECT merezha_content.refs",
    "SELECT 1 FROM merezha_content.refs \
                      WHERE target_kind = 'entry' AND target_id = $1::text::uuid \
                        AND from_entry <> $1::text::uuid LIMIT 1",
);
pub(crate) const PUBLISHED_LOCALES: Statement = Statement::new(
    "content.trash.published_locales",
    "SELECT merezha_content.variants",
    "SELECT locale FROM merezha_content.variants \
                      WHERE entry_id = $1::text::uuid AND scope = $2 AND content_type = $3 \
                        AND NOT deleted AND published_revision IS NOT NULL",
);
pub(crate) const ENTRY_DELETE: Statement = Statement::new(
    "content.entries.delete",
    "DELETE merezha_content.entries",
    "DELETE FROM merezha_content.entries \
                  WHERE id = $1::text::uuid AND scope = $2 AND content_type = $3",
);
pub(crate) const REFS_TO_ENTRY_DELETE: Statement = Statement::new(
    "content.refs.delete_to_entry",
    "DELETE merezha_content.refs",
    "DELETE FROM merezha_content.refs \
                  WHERE target_kind = 'entry' AND target_id = $1::text::uuid",
);
pub(crate) const ENTRY_TRASH: Statement = Statement::new(
    "content.entries.trash",
    "UPDATE merezha_content.entries",
    "UPDATE merezha_content.entries SET deleted_at = now(), deleted_by = $4 \
                  WHERE id = $1::text::uuid AND scope = $2 AND content_type = $3",
);
pub(crate) const VARIANTS_TRASH: Statement = Statement::new(
    "content.variants.trash",
    "UPDATE merezha_content.variants",
    "UPDATE merezha_content.variants \
                    SET deleted = true, published_revision = NULL, published_at = NULL \
                  WHERE entry_id = $1::text::uuid AND scope = $2 AND content_type = $3",
);
pub(crate) const REFS_FROM_ENTRY_DELETE: Statement = Statement::new(
    "content.refs.delete_from_entry",
    "DELETE merezha_content.refs",
    "DELETE FROM merezha_content.refs WHERE from_entry = $1::text::uuid AND scope = $2",
);
pub(crate) const ENTRY_LOCK_TRASHED: Statement = Statement::new(
    "content.trash.lock_trashed",
    "SELECT merezha_content.entries",
    "SELECT deleted_at IS NOT NULL FROM merezha_content.entries \
                  WHERE id = $1::text::uuid AND scope = $2 AND content_type = $3 FOR UPDATE",
);
pub(crate) const ENTRY_RESTORE: Statement = Statement::new(
    "content.entries.restore",
    "UPDATE merezha_content.entries",
    "UPDATE merezha_content.entries SET deleted_at = NULL, deleted_by = NULL \
              WHERE id = $1::text::uuid AND scope = $2 AND content_type = $3",
);
pub(crate) const VARIANTS_RESTORE: Statement = Statement::new(
    "content.variants.restore",
    "UPDATE merezha_content.variants",
    "UPDATE merezha_content.variants SET deleted = false \
                  WHERE entry_id = $1::text::uuid AND scope = $2 AND content_type = $3 \
                 RETURNING locale, draft_data::text",
);
pub(crate) const TRASH_EXPIRED: Statement = Statement::new(
    "content.trash.expired",
    "SELECT merezha_content.entries",
    "SELECT id::text, scope, content_type FROM merezha_content.entries \
                  WHERE deleted_at < now() - $1::bigint * interval '1 second' \
                  ORDER BY deleted_at, id LIMIT $2 FOR UPDATE SKIP LOCKED",
);
pub(crate) const ENTRIES_PURGE: Statement = Statement::new(
    "content.entries.purge",
    "DELETE merezha_content.entries",
    "DELETE FROM merezha_content.entries e \
              USING unnest($1::text[], $2::text[], $3::text[]) AS t(id, scope, content_type) \
              WHERE e.id = t.id::uuid AND e.scope = t.scope AND e.content_type = t.content_type \
                AND e.deleted_at IS NOT NULL",
);
pub(crate) const REFS_TO_ENTRIES_DELETE: Statement = Statement::new(
    "content.refs.delete_to_entries",
    "DELETE merezha_content.refs",
    "DELETE FROM merezha_content.refs \
              WHERE target_kind = 'entry' AND target_id = ANY($1::text[]::uuid[])",
);
/// Text built at run time; `sql` is its shape.
pub(crate) const TRASH_LIST: Statement = Statement::new(
    "content.trash.list",
    "SELECT merezha_content.entries",
    "SELECT e.id::text, e.content_type, <deleted_at text>, e.deleted_by, CASE e.content_type WHEN '<type>' THEN v.draft_data ->> '<title field>' ... END, <deleted_at in microseconds> FROM merezha_content.entries e LEFT JOIN merezha_content.variants v ... WHERE e.deleted_at IS NOT NULL ... ORDER BY e.deleted_at DESC, e.id DESC LIMIT $6",
);

// ----- the fallback rebuild of `published` (C4) -----
pub(crate) const REBUILDS_EXIST: Statement = Statement::new(
    "content.rebuilds.exist",
    "SELECT to_regclass()",
    "SELECT to_regclass('merezha_content.rebuilds') IS NOT NULL",
);
pub(crate) const REBUILD_PENDING: Statement = Statement::new(
    "content.rebuilds.pending",
    "SELECT merezha_content.rebuilds",
    "SELECT version, after_entry::text FROM merezha_content.rebuilds \
              WHERE state = 'pending' ORDER BY version DESC LIMIT 1",
);
pub(crate) const REBUILD_CURSOR: Statement = Statement::new(
    "content.rebuilds.cursor",
    "SELECT merezha_content.rebuilds",
    "SELECT after_entry::text FROM merezha_content.rebuilds \
                          WHERE version = $1 AND state = 'pending' FOR UPDATE",
);
pub(crate) const REBUILD_ENTRIES: Statement = Statement::new(
    "content.rebuilds.entries",
    "SELECT merezha_content.entries",
    "SELECT e.id::text, e.scope, e.content_type FROM merezha_content.entries e \
                          WHERE e.deleted_at IS NULL \
                            AND ($1::text IS NULL OR e.id > $1::text::uuid) \
                            AND EXISTS (SELECT 1 FROM merezha_content.variants v \
                                         WHERE v.entry_id = e.id AND v.scope = e.scope \
                                           AND v.content_type = e.content_type \
                                           AND NOT v.deleted AND v.published_revision IS NOT NULL) \
                          ORDER BY e.id LIMIT $2 FOR SHARE OF e",
);
pub(crate) const REBUILD_ADVANCE: Statement = Statement::new(
    "content.rebuilds.advance",
    "UPDATE merezha_content.rebuilds",
    "UPDATE merezha_content.rebuilds \
                        SET after_entry = $2::text::uuid, updated_at = now(), \
                            state = CASE WHEN $3 THEN 'done' ELSE 'pending' END \
                      WHERE version = $1",
);

// ----- reference resolution (C3) -----
pub(crate) const REF_ENTRIES: Statement = Statement::new(
    "content.refs.entries",
    "SELECT merezha_content.entries",
    "SELECT id::text, scope, content_type, deleted_at IS NOT NULL \
                   FROM merezha_content.entries \
                  WHERE id = ANY($1::text[]::uuid[]) AND (scope = $2 OR scope = 'app') \
                  ORDER BY id FOR KEY SHARE",
);
pub(crate) const REF_LIVE_ENTRIES: Statement = Statement::new(
    "content.refs.live_entries",
    "SELECT merezha_content.entries",
    "SELECT id::text FROM merezha_content.entries \
                      WHERE id = ANY($1::text[]::uuid[]) AND deleted_at IS NULL",
);
pub(crate) const REF_ASSETS: Statement = Statement::new(
    "content.refs.assets",
    "SELECT merezha_content.assets",
    "SELECT id::text, content_type FROM merezha_content.assets \
                  WHERE id = ANY($1::text[]::uuid[]) AND (scope = $2 OR scope = 'app') \
                    AND deleted_at IS NULL ORDER BY id FOR KEY SHARE",
);

// ----- the management list (C3) -----
/// Text built at run time; `sql` is its shape.
pub(crate) const MANAGE_EXPLAIN: Statement = Statement::new(
    "content.manage.explain",
    "EXPLAIN",
    "EXPLAIN (COSTS OFF) <an entry list>",
);

// ----- `merezha content diff` -----

/// The read-only transaction `merezha content diff` reads the model, the
/// derived indexes and the pending rebuild in.
pub const DIFF_READ_ONLY: Statement =
    Statement::new("content.diff.read_only", "SET", "SET TRANSACTION READ ONLY");

/// Every content statement outside [`crate::media`], whose statements
/// are [`crate::media::STATEMENTS`].
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
    MODELS_EXIST,
    MODEL_ACTIVE,
    DERIVED_INDEXES,
    SESSION_TIMEOUT,
    INDEX_DROP,
    ACTIVATION_TRY_LOCK,
    INDEX_INVALID,
    INDEX_CREATE,
    MODEL_ACTIVE_LOCK,
    MODEL_SUPERSEDE,
    MODEL_INSERT,
    LOCALE_INSERT,
    LOCALE_FALLBACK,
    REBUILD_SUPERSEDE,
    REBUILD_INSERT,
    GENERATIONS_BUMP_ALL,
    MODEL_ACTIVE_VERSION,
    MODEL_VERSION_LOCK,
    PUBLISHED_REVISIONS,
    PUBLISHED_DELETE,
    PUBLISHED_INSERT,
    GENERATION_BUMP,
    ENTRY_LOCK,
    VARIANT_LOCK,
    REFS_DELETE_STAGE,
    REFS_INSERT,
    REQUIRED_TARGET_SHARE,
    REQUIRED_PUBLISHED,
    REVISION_DATA,
    VARIANT_PUBLISH,
    PUBLISHED_SLUG_READ,
    VARIANT_WITHDRAW,
    REFS_DELETE_VARIANT,
    VARIANT_DELETE,
    ENTRY_SUMMARY,
    ENTRY_VARIANTS,
    SINGLETON_ID,
    REVISION_LIST,
    REVISION_READ,
    ENTRY_EXISTS,
    REFERRERS,
    MANAGE_LIST,
    VARIANT_EXISTS,
    VARIANT_VIEW,
    ENTRY_INSERT,
    ENTRY_SHARE,
    DRAFT_CURRENT,
    DEFAULT_DRAFT_SHARE,
    VARIANT_INSERT,
    DRAFT_UPDATE,
    REVISION_INSERT,
    REVISION_PRUNE,
    REFS_DELETE_DRAFT,
    REFS_INSERT_DRAFT,
    SHARED_COPY,
    ENTRY_LOCK_ANY,
    ENTRY_REFERENCED,
    PUBLISHED_LOCALES,
    ENTRY_DELETE,
    REFS_TO_ENTRY_DELETE,
    ENTRY_TRASH,
    VARIANTS_TRASH,
    REFS_FROM_ENTRY_DELETE,
    ENTRY_LOCK_TRASHED,
    ENTRY_RESTORE,
    VARIANTS_RESTORE,
    TRASH_EXPIRED,
    ENTRIES_PURGE,
    REFS_TO_ENTRIES_DELETE,
    TRASH_LIST,
    REBUILDS_EXIST,
    REBUILD_PENDING,
    REBUILD_CURSOR,
    REBUILD_ENTRIES,
    REBUILD_ADVANCE,
    REF_ENTRIES,
    REF_LIVE_ENTRIES,
    REF_ASSETS,
    MANAGE_EXPLAIN,
    TIGHTENED_PUBLISHED,
    TIGHTENED_DRAFTS,
    DIFF_READ_ONLY,
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
}
