//! Publishing (section 4): publish, unpublish, publishing an older
//! revision, batch publish and unpublish, the variant delete, and the
//! materialization of the `published` rows delivery reads.
//!
//! Every operation is one transaction that starts with the model version
//! check, locks the entry `FOR UPDATE` and then the variant under the
//! route's scope predicates, compares the draft revision with `If-Match`,
//! rewrites the variant's `published` refs, re-materializes the entry's
//! `published` rows for every configured locale, bumps the scope's
//! generation and appends its events. Nothing commits half-way: a batch
//! publishes or unpublishes all of its items or none.
use crate::{
    model::{ContentModel, ContentType},
    refs,
    store::{
        Actor, Rejection, Store, Target, VariantView, Work, field_references, parse_object,
        read_view, shared_fields, slug_field, sql, write_error,
    },
    value::{RandomKeys, ReferenceTarget, Stage, ValidateOptions, ValueError},
};
use merezha_domain::{Classification, Failure};
use merezha_platform::{FieldError, Tx, errors::MAX_DETAILS, ports::OutboxRecord};
use serde_json::{Map, Value, json};
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

/// The event every publish appends (section 7).
pub const PUBLISHED: &str = "content.entry_published";
/// The event every unpublish appends, and a delete per published locale.
pub const UNPUBLISHED: &str = "content.entry_unpublished";

/// How an operation meets the draft revision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expect {
    /// `If-Match` or a batch item's `revision`: `Some(n)` must be the
    /// draft revision, `None` (a tag naming no revision) never is.
    Draft(Option<i64>),
    /// Batch unpublish items carry no revision.
    Any,
}

/// One variant to publish or unpublish.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Item {
    pub target: Target,
    pub id: Uuid,
    pub locale: String,
    pub expect: Expect,
    /// The revision to publish, the draft's when `None` (`{"revision": n}`
    /// rolls the published version back without touching the draft).
    pub revision: Option<i64>,
}

/// A reference whose target must be published in the referrer's locale
/// chain (`require_published`), checked once every item of the
/// transaction is materialized.
struct Required {
    item: usize,
    path: String,
    target: Uuid,
    locale: String,
    scope: String,
    /// The reference's allowed types; empty allows every type.
    types: Vec<String>,
}

/// The locales `locale` falls back through, itself first and the default
/// locale last. A non-localized type is served from its default variant
/// in every locale.
pub(crate) fn chain<'m>(
    model: &'m ContentModel,
    content_type: &ContentType,
    locale: &'m str,
) -> Vec<&'m str> {
    let default = model.default_locale().code.as_str();
    if !content_type.localized {
        return vec![default];
    }
    let mut out: Vec<&str> = Vec::new();
    let mut next = Some(locale);
    while let Some(code) = next {
        if out.contains(&code) || out.len() > model.locales().len() {
            break;
        }
        out.push(code);
        next = model
            .locales()
            .iter()
            .find(|l| l.code == code)
            .and_then(|l| l.fallback.as_deref());
    }
    if !out.contains(&default) {
        out.push(default);
    }
    out
}

/// Recompute every `published` row of one entry from its published
/// variants: for each configured locale, the first published variant of
/// its chain, with the default variant's published shared fields added
/// to another locale's own ones. Locales whose chain reaches no published
/// variant get no row. Returns the number of rows written.
///
/// The caller holds the entry's lock and has run the model version check.
pub(crate) async fn materialize(
    tx: &mut Tx<'_>,
    model: &ContentModel,
    indexes: &[crate::indexes::DerivedIndex],
    scope: &str,
    type_name: &str,
    id: &str,
) -> Result<usize, Rejection> {
    let Some(content_type) = model.content_type(type_name) else {
        return Err(Failure::permanent("content_type_not_found").into());
    };
    let rows = tx
        .query(
            "SELECT v.locale, r.data::text FROM merezha_content.variants v \
               JOIN merezha_content.revisions r \
                 ON r.entry_id = v.entry_id AND r.locale = v.locale \
                AND r.revision = v.published_revision \
              WHERE v.entry_id = $1::text::uuid AND v.scope = $2 AND v.content_type = $3 \
                AND NOT v.deleted AND v.published_revision IS NOT NULL",
            &[&id, &scope, &type_name],
        )
        .await
        .map_err(|e| sql(&e))?;
    let mut published: HashMap<String, Map<String, Value>> = HashMap::new();
    for row in &rows {
        published.insert(row.get(0), parse_object(row.get(1))?);
    }
    let default = model.default_locale().code.clone();
    let shared = shared_fields(content_type);
    let shared_values: Map<String, Value> = published
        .get(&default)
        .map(|data| {
            data.iter()
                .filter(|(k, _)| shared.contains(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        })
        .unwrap_or_default();
    let slug = slug_field(content_type).map(|f| f.name.clone());
    let (mut locales, mut sources, mut slugs, mut datas) = (vec![], vec![], vec![], vec![]);
    for locale in model.locales() {
        let Some(source) = chain(model, content_type, &locale.code)
            .into_iter()
            .find(|code| published.contains_key(*code))
        else {
            continue;
        };
        let mut data = published[source].clone();
        if source != default {
            data.extend(shared_values.clone());
        }
        slugs.push(
            slug.as_ref()
                .and_then(|f| data.get(f))
                .and_then(Value::as_str)
                .map(str::to_owned),
        );
        locales.push(locale.code.clone());
        sources.push(source.to_owned());
        datas.push(Value::Object(data).to_string());
    }
    tx.execute(
        "DELETE FROM merezha_content.published \
          WHERE entry_id = $1::text::uuid AND scope = $2 AND content_type = $3",
        &[&id, &scope, &type_name],
    )
    .await
    .map_err(|e| sql(&e))?;
    if locales.is_empty() {
        return Ok(0);
    }
    tx.execute(
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
        &[&id, &scope, &type_name, &locales, &sources, &slugs, &datas],
    )
    .await
    .map_err(|e| published_error(&e, indexes, slug.as_deref()))?;
    Ok(locales.len())
}

/// A unique violation on `published` names its field: a derived index
/// through [`write_error`], the static own-slug index through the slug.
fn published_error(
    error: &merezha_postgres::tokio_postgres::Error,
    indexes: &[crate::indexes::DerivedIndex],
    slug: Option<&str>,
) -> Rejection {
    let constraint = error.as_db_error().and_then(|d| d.constraint());
    if constraint == Some("published_own_slug")
        && let Some(slug) = slug
        && let Ok(detail) = FieldError::new(format!("/fields/{slug}"), "unique")
    {
        return Rejection::invalid(vec![detail]);
    }
    write_error(error, indexes)
}

/// Bump the generation of `scope`, creating its row.
pub(crate) async fn bump_generation(tx: &mut Tx<'_>, scope: &str) -> Result<(), Rejection> {
    tx.execute(
        "INSERT INTO merezha_content.generations (scope, generation) VALUES ($1, 1) \
         ON CONFLICT (scope) DO UPDATE SET generation = merezha_content.generations.generation + 1",
        &[&scope],
    )
    .await
    .map_err(|e| sql(&e))?;
    Ok(())
}

/// Lock a live entry `FOR UPDATE` under the scope predicates.
pub(crate) async fn lock_entry(
    tx: &mut Tx<'_>,
    target: &Target,
    id: &str,
) -> Result<(), Rejection> {
    tx.query_opt(
        "SELECT 1 FROM merezha_content.entries \
          WHERE id = $1::text::uuid AND scope = $2 AND content_type = $3 \
            AND deleted_at IS NULL FOR UPDATE",
        &[&id, &target.scope, &target.content_type],
    )
    .await
    .map_err(|e| sql(&e))?
    .map(|_| ())
    .ok_or_else(Rejection::not_found)
}

/// The locked variant: its draft revision, published revision and whether
/// it was ever published.
struct Locked {
    draft: i64,
    published: Option<i64>,
    never_published: bool,
}

async fn lock_variant(
    tx: &mut Tx<'_>,
    target: &Target,
    id: &str,
    locale: &str,
    expect: Expect,
) -> Result<Locked, Rejection> {
    let row = tx
        .query_opt(
            "SELECT draft_revision, published_revision, first_published_at IS NULL \
               FROM merezha_content.variants \
              WHERE entry_id = $1::text::uuid AND locale = $2 AND scope = $3 \
                AND content_type = $4 AND NOT deleted FOR UPDATE",
            &[&id, &locale, &target.scope, &target.content_type],
        )
        .await
        .map_err(|e| sql(&e))?
        .ok_or_else(Rejection::not_found)?;
    let locked = Locked {
        draft: row.get(0),
        published: row.get(1),
        never_published: row.get(2),
    };
    if let Expect::Draft(expected) = expect
        && expected != Some(locked.draft)
    {
        return Err(Rejection::code(
            "revision_conflict",
            Classification::Conflict,
        ));
    }
    Ok(locked)
}

/// Replace the variant's `stage` refs with `rows`.
pub(crate) async fn write_refs(
    tx: &mut Tx<'_>,
    scope: &str,
    id: &str,
    locale: &str,
    stage: &str,
    rows: &[(String, &'static str, Uuid)],
) -> Result<(), Rejection> {
    tx.execute(
        "DELETE FROM merezha_content.refs \
          WHERE from_entry = $1::text::uuid AND from_locale = $2 AND stage = $3 AND scope = $4",
        &[&id, &locale, &stage, &scope],
    )
    .await
    .map_err(|e| sql(&e))?;
    if rows.is_empty() {
        return Ok(());
    }
    let paths: Vec<String> = rows.iter().map(|r| r.0.clone()).collect();
    let kinds: Vec<String> = rows.iter().map(|r| r.1.to_owned()).collect();
    let targets: Vec<String> = rows.iter().map(|r| r.2.hyphenated().to_string()).collect();
    tx.execute(
        "INSERT INTO merezha_content.refs \
             (scope, from_entry, from_locale, stage, path, target_kind, target_id) \
         SELECT $1, $2::text::uuid, $3, $4, path, kind, target::uuid \
           FROM unnest($5::text[], $6::text[], $7::text[]) AS t(path, kind, target) \
         ON CONFLICT DO NOTHING",
        &[&scope, &id, &locale, &stage, &paths, &kinds, &targets],
    )
    .await
    .map_err(|e| sql(&e))?;
    Ok(())
}

/// A failure of batch item `index`: field details are prefixed with
/// `/items/<index>`, any other failure gains one detail there.
pub(crate) fn in_item(index: usize, batch: bool, mut rejection: Rejection) -> Rejection {
    if !batch {
        return rejection;
    }
    let prefix = format!("/items/{index}");
    if rejection.details.is_empty() {
        if let Ok(detail) = FieldError::new(prefix, &rejection.failure.code) {
            rejection.details.push(detail);
        }
    } else {
        rejection.details = rejection
            .details
            .into_iter()
            .filter_map(|d| FieldError::new(format!("{prefix}{}", d.path), &d.code).ok())
            .collect();
    }
    rejection
}

impl Work {
    /// Publish `items` in this transaction; see [`Store::publish`].
    async fn publish(
        &self,
        tx: &mut Tx<'_>,
        items: &[Item],
        actor: &Actor,
        batch: bool,
    ) -> Result<Vec<VariantView>, Rejection> {
        self.state.check_writable(tx).await?;
        lock_entries(tx, items).await?;
        let mut required = Vec::new();
        let mut records = Vec::new();
        let mut views = Vec::with_capacity(items.len());
        let mut scopes = BTreeSet::new();
        for (index, item) in items.iter().enumerate() {
            let (view, record) = self
                .publish_one(tx, index, item, actor, &mut required)
                .await
                .map_err(|r| in_item(index, batch, r))?;
            scopes.insert(item.target.scope.clone());
            records.push(record);
            views.push(view);
        }
        // `require_published`, against the rows this transaction wrote.
        let mut problems: Vec<(usize, FieldError)> = Vec::new();
        // Share-lock the targets first, in id order: an unpublish or
        // delete of one (`FOR UPDATE`) in flight is waited for and then
        // seen, and one arriving later waits for this commit. Two publishes
        // that each require the other may still deadlock; PostgreSQL aborts
        // one with `database_conflict` (409, conflict, retryable).
        let mut targets: Vec<(Uuid, &str)> = required
            .iter()
            .map(|n| (n.target, n.scope.as_str()))
            .collect();
        targets.sort();
        targets.dedup();
        for (target, scope) in targets {
            tx.query_opt(
                "SELECT 1 FROM merezha_content.entries \
                  WHERE id = $1::text::uuid AND (scope = $2 OR scope = 'app') FOR SHARE",
                &[&target.hyphenated().to_string(), &scope],
            )
            .await
            .map_err(|e| sql(&e))?;
        }
        for need in &required {
            let target = need.target.hyphenated().to_string();
            let found = tx
                .query_opt(
                    "SELECT 1 FROM merezha_content.published \
                      WHERE entry_id = $1::text::uuid AND locale = $2 \
                        AND (scope = $3 OR scope = 'app') \
                        AND (cardinality($4::text[]) = 0 OR content_type = ANY($4::text[]))",
                    &[&target, &need.locale, &need.scope, &need.types],
                )
                .await
                .map_err(|e| sql(&e))?;
            if found.is_none()
                && let Ok(detail) = FieldError::new(need.path.clone(), "reference_unpublished")
            {
                problems.push((need.item, detail));
            }
        }
        if let Some((first, _)) = problems.first() {
            let first = *first;
            let details = problems
                .into_iter()
                .filter(|(i, _)| *i == first)
                .map(|(_, d)| d)
                .collect();
            return Err(in_item(first, batch, Rejection::invalid(details)));
        }
        for scope in &scopes {
            bump_generation(tx, scope).await?;
        }
        self.append(tx, records).await?;
        Ok(views)
    }

    async fn publish_one(
        &self,
        tx: &mut Tx<'_>,
        index: usize,
        item: &Item,
        actor: &Actor,
        required: &mut Vec<Required>,
    ) -> Result<(VariantView, OutboxRecord), Rejection> {
        let model = self.model.clone();
        let target = &item.target;
        let Some(content_type) = model.content_type(&target.content_type) else {
            return Err(Failure::permanent("content_type_not_found").into());
        };
        let id = item.id.hyphenated().to_string();
        let locale = item.locale.as_str();
        lock_entry(tx, target, &id).await?;
        let locked = lock_variant(tx, target, &id, locale, item.expect).await?;
        let revision = item.revision.unwrap_or(locked.draft);
        if revision < 1 || revision > locked.draft {
            return Err(Failure::permanent("revision_not_found").into());
        }
        let data = tx
            .query_opt(
                "SELECT r.data::text FROM merezha_content.revisions r \
                   JOIN merezha_content.variants v \
                     ON v.entry_id = r.entry_id AND v.locale = r.locale \
                  WHERE r.entry_id = $1::text::uuid AND r.locale = $2 \
                    AND v.scope = $3 AND v.content_type = $4 AND r.revision = $5",
                &[&id, &locale, &target.scope, &target.content_type, &revision],
            )
            .await
            .map_err(|e| sql(&e))?
            .ok_or_else(|| Rejection::new(Failure::permanent("revision_not_found")))?;
        let data = Value::Object(parse_object(data.get(0))?);
        // Publish-time validation against the active model: every rule,
        // `required` included, and the references.
        let owns_shared = locale == model.default_locale().code || !content_type.localized;
        let options = ValidateOptions {
            stage: Stage::Publish,
            default_variant: owns_shared,
            new_variant: false,
        };
        let mut keys = RandomKeys(self.ctx.random());
        let (references, mut problems) =
            match model.validate_fields(&target.content_type, &data, &options, &mut keys) {
                Ok(valid) => (valid.references, Vec::new()),
                Err(ValueError::Failed(failure)) => return Err(failure.into()),
                Err(ValueError::Invalid(details)) => (
                    field_references(&model, &target.content_type, &data, &options, &mut keys)?,
                    details,
                ),
            };
        let resolution = refs::resolve(tx, &target.scope, &references).await?;
        problems.extend(resolution.problems);
        if !problems.is_empty() {
            problems.truncate(MAX_DETAILS);
            return Err(Rejection::invalid(problems));
        }
        for reference in &references {
            if let ReferenceTarget::Entry {
                id: to,
                require_published: true,
                allowed,
                ..
            } = &reference.target
            {
                required.push(Required {
                    item: index,
                    path: reference.path.clone(),
                    target: *to,
                    locale: locale.to_owned(),
                    scope: target.scope.clone(),
                    types: allowed.clone(),
                });
            }
        }
        tx.execute(
            "UPDATE merezha_content.variants \
                SET published_revision = $5, published_at = now(), \
                    first_published_at = coalesce(first_published_at, now()) \
              WHERE entry_id = $1::text::uuid AND locale = $2 AND scope = $3 \
                AND content_type = $4 AND NOT deleted",
            &[&id, &locale, &target.scope, &target.content_type, &revision],
        )
        .await
        .map_err(|e| sql(&e))?;
        write_refs(
            tx,
            &target.scope,
            &id,
            locale,
            "published",
            &resolution.rows,
        )
        .await?;
        materialize(
            tx,
            &model,
            &self.indexes,
            &target.scope,
            &target.content_type,
            &id,
        )
        .await?;
        let slug: Option<String> = tx
            .query_opt(
                "SELECT slug FROM merezha_content.published \
                  WHERE entry_id = $1::text::uuid AND locale = $2 AND scope = $3 \
                    AND content_type = $4",
                &[&id, &locale, &target.scope, &target.content_type],
            )
            .await
            .map_err(|e| sql(&e))?
            .and_then(|row| row.get(0));
        let view = read_view(tx, target, item.id, locale, true).await?;
        let record = self.event(
            target,
            PUBLISHED,
            json!({
                "scope": target.scope,
                "type": target.content_type,
                "id": id,
                "locale": locale,
                "revision": revision,
                "slug": slug,
                "published_at": view.published_at,
                "first_publish": locked.never_published,
                "actor": actor.json(),
            }),
            actor,
        )?;
        Ok((view, record))
    }

    /// Unpublish `items` in this transaction; see [`Store::unpublish`].
    async fn unpublish(
        &self,
        tx: &mut Tx<'_>,
        items: &[Item],
        actor: &Actor,
        batch: bool,
    ) -> Result<Vec<VariantView>, Rejection> {
        self.state.check_writable(tx).await?;
        lock_entries(tx, items).await?;
        let mut records = Vec::new();
        let mut views = Vec::with_capacity(items.len());
        let mut scopes = BTreeSet::new();
        for (index, item) in items.iter().enumerate() {
            let target = &item.target;
            let id = item.id.hyphenated().to_string();
            let locale = item.locale.as_str();
            let changed = async {
                lock_entry(tx, target, &id).await?;
                let locked = lock_variant(tx, target, &id, locale, item.expect).await?;
                if locked.published.is_none() {
                    return Ok(false);
                }
                self.withdraw(tx, target, &id, locale).await?;
                Ok::<_, Rejection>(true)
            }
            .await
            .map_err(|r| in_item(index, batch, r))?;
            if changed {
                scopes.insert(target.scope.clone());
                records.push(self.unpublished(target, &id, locale, "unpublish", actor)?);
            }
            views.push(read_view(tx, target, item.id, locale, true).await?);
        }
        for scope in &scopes {
            bump_generation(tx, scope).await?;
        }
        self.append(tx, records).await?;
        Ok(views)
    }

    /// Clear a locked variant's published pointer and refs and
    /// re-materialize its entry: a locale that fell back to it falls back
    /// further or disappears.
    async fn withdraw(
        &self,
        tx: &mut Tx<'_>,
        target: &Target,
        id: &str,
        locale: &str,
    ) -> Result<(), Rejection> {
        tx.execute(
            "UPDATE merezha_content.variants SET published_revision = NULL, published_at = NULL \
              WHERE entry_id = $1::text::uuid AND locale = $2 AND scope = $3 \
                AND content_type = $4 AND NOT deleted",
            &[&id, &locale, &target.scope, &target.content_type],
        )
        .await
        .map_err(|e| sql(&e))?;
        write_refs(tx, &target.scope, id, locale, "published", &[]).await?;
        materialize(
            tx,
            &self.model,
            &self.indexes,
            &target.scope,
            &target.content_type,
            id,
        )
        .await?;
        Ok(())
    }

    /// `content.entry_unpublished` for one locale.
    pub(crate) fn unpublished(
        &self,
        target: &Target,
        id: &str,
        locale: &str,
        reason: &str,
        actor: &Actor,
    ) -> Result<OutboxRecord, Rejection> {
        self.event(
            target,
            UNPUBLISHED,
            json!({
                "scope": target.scope,
                "type": target.content_type,
                "id": id,
                "locale": locale,
                "actor": actor.json(),
                "reason": reason,
            }),
            actor,
        )
    }

    /// `DELETE /entries/{type}/{id}/{locale}`: a non-default variant with
    /// its revisions and refs; see [`Store::delete_variant`].
    async fn delete_variant(
        &self,
        tx: &mut Tx<'_>,
        target: &Target,
        id: Uuid,
        locale: &str,
        expected: Option<i64>,
        actor: &Actor,
    ) -> Result<(), Rejection> {
        self.state.check_writable(tx).await?;
        let id = id.hyphenated().to_string();
        lock_entry(tx, target, &id).await?;
        if locale == self.model.default_locale().code {
            return Err(Rejection::code(
                "default_variant_required",
                Classification::Conflict,
            ));
        }
        let locked = lock_variant(tx, target, &id, locale, Expect::Draft(expected)).await?;
        tx.execute(
            "DELETE FROM merezha_content.refs \
              WHERE from_entry = $1::text::uuid AND from_locale = $2 AND scope = $3",
            &[&id, &locale, &target.scope],
        )
        .await
        .map_err(|e| sql(&e))?;
        // Its revisions go with it (`ON DELETE CASCADE`).
        tx.execute(
            "DELETE FROM merezha_content.variants \
              WHERE entry_id = $1::text::uuid AND locale = $2 AND scope = $3 \
                AND content_type = $4",
            &[&id, &locale, &target.scope, &target.content_type],
        )
        .await
        .map_err(|e| sql(&e))?;
        if locked.published.is_some() {
            materialize(
                tx,
                &self.model,
                &self.indexes,
                &target.scope,
                &target.content_type,
                &id,
            )
            .await?;
            bump_generation(tx, &target.scope).await?;
            let record = self.unpublished(target, &id, locale, "delete", actor)?;
            self.append(tx, vec![record]).await?;
        }
        Ok(())
    }
}

/// Lock every entry of `items` in id order, so two batches over the same
/// entries never deadlock.
async fn lock_entries(tx: &mut Tx<'_>, items: &[Item]) -> Result<(), Rejection> {
    if items.len() < 2 {
        return Ok(());
    }
    let mut order: Vec<(Uuid, &Target)> = items.iter().map(|i| (i.id, &i.target)).collect();
    order.sort_by_key(|(id, _)| *id);
    order.dedup_by_key(|(id, _)| *id);
    for (id, target) in order {
        // A missing entry is reported by its item, with its index.
        tx.query_opt(
            "SELECT 1 FROM merezha_content.entries \
              WHERE id = $1::text::uuid AND scope = $2 AND content_type = $3 \
                AND deleted_at IS NULL FOR UPDATE",
            &[
                &id.hyphenated().to_string(),
                &target.scope,
                &target.content_type,
            ],
        )
        .await
        .map_err(|e| sql(&e))?;
    }
    Ok(())
}

impl Store {
    /// Publish `items` in one transaction, all or none (section 4). Each
    /// item's draft revision must be its `expect`; it publishes `revision`
    /// (the draft's by default) after publish-time validation against the
    /// active model, rewrites the variant's `published` refs and
    /// re-materializes the entry's `published` rows. `require_published`
    /// references are checked once every item is materialized, so a batch
    /// may publish a target with its referrers. The transaction bumps
    /// each scope's generation and appends one `content.entry_published`
    /// per item. With `batch`, a failure names its item in `details`
    /// (`/items/<n>...`).
    pub async fn publish(
        &self,
        items: Vec<Item>,
        actor: &Actor,
        batch: bool,
    ) -> Result<Vec<VariantView>, Rejection> {
        let work = self.work();
        let actor = actor.clone();
        let deadline = self.batch_deadline(items.len());
        crate::store::transaction(self.database(), deadline, move |tx| {
            Box::pin(async move { work.publish(tx, &items, &actor, batch).await })
        })
        .await
    }

    /// Unpublish `items` in one transaction, all or none: clear each
    /// published pointer and its refs, re-materialize, bump each scope's
    /// generation and append one `content.entry_unpublished` (reason
    /// `unpublish`) per item that was published. An item that is not
    /// published is left as it is, without an event.
    pub async fn unpublish(
        &self,
        items: Vec<Item>,
        actor: &Actor,
        batch: bool,
    ) -> Result<Vec<VariantView>, Rejection> {
        let work = self.work();
        let actor = actor.clone();
        let deadline = self.batch_deadline(items.len());
        crate::store::transaction(self.database(), deadline, move |tx| {
            Box::pin(async move { work.unpublish(tx, &items, &actor, batch).await })
        })
        .await
    }

    /// Remove a non-default variant with its revisions and refs (the
    /// default one goes only with the entry, `default_variant_required`).
    /// A published variant's locale falls back further; its removal bumps
    /// the generation and appends `content.entry_unpublished` with reason
    /// `delete`.
    pub async fn delete_variant(
        &self,
        target: &Target,
        id: Uuid,
        locale: &str,
        expected: Option<i64>,
        actor: &Actor,
    ) -> Result<(), Rejection> {
        let work = self.work();
        let target = target.clone();
        let locale = locale.to_owned();
        let actor = actor.clone();
        crate::store::transaction(self.database(), self.deadline(), move |tx| {
            Box::pin(async move {
                work.delete_variant(tx, &target, id, &locale, expected, &actor)
                    .await
            })
        })
        .await
    }
}
