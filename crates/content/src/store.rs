//! Drafts and revisions (section 3 storage, section 4 saving, revisions,
//! localization): every statement on `entries`, `variants` and
//! `revisions` carries the scope and content type the route resolved, and
//! every write transaction starts with the model version check
//! ([`ContentState::check_writable`]) and compares and swaps
//! `draft_revision`. The same transaction writes the revision, the draft
//! refs, retention pruning, shared-field copies and the
//! `content.entry_draft_saved` outbox record.
use crate::{
    indexes::{DerivedIndex, derived_indexes},
    json::{encoded_len, merge_patch},
    model::{ContentModel, ContentType, FieldKind},
    module::ContentState,
    refs,
    value::{RandomKeys, Stage, ValidateOptions, ValueError, ValueReference},
};
use merezha_domain::{Classification, Failure, Identity};
use merezha_platform::{
    AppContext, BoxFuture, Database, FieldError, Tx,
    errors::MAX_DETAILS,
    ports::{
        DedupeKey, MAX_RECORDS_PER_APPEND, MessageContext, NewEvent, OnDuplicate, OutboxRecord,
        SourceOutbox,
    },
};
use merezha_postgres::{classify, tokio_postgres};
use serde_json::{Map, Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};
use uuid::Uuid;

/// Batch items per `limits.query_timeout` of a batch's transaction.
pub const BATCH_ITEMS_PER_TIMEOUT: usize = 50;

/// The event every draft save appends (section 7).
pub const DRAFT_SAVED: &str = "content.entry_draft_saved";

/// A failure with optional field-level details, as the API answers it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rejection {
    pub failure: Failure,
    pub details: Vec<FieldError>,
}
impl Rejection {
    pub fn new(failure: Failure) -> Self {
        Self {
            failure,
            details: Vec::new(),
        }
    }
    pub(crate) fn code(code: &str, classification: Classification) -> Self {
        Self::new(Failure::new(code, classification))
    }
    pub(crate) fn not_found() -> Self {
        Self::new(Failure::permanent(ENTRY_NOT_FOUND))
    }
    pub(crate) fn invalid(details: Vec<FieldError>) -> Self {
        let mut details = details;
        details.truncate(MAX_DETAILS);
        Self {
            failure: Failure::permanent("validation_failed"),
            details,
        }
    }
}
impl From<Failure> for Rejection {
    fn from(failure: Failure) -> Self {
        Self::new(failure)
    }
}

pub(crate) const ENTRY_NOT_FOUND: &str = "entry_not_found";

/// What a route resolved before touching the database: the type from the
/// path and the scope from the type's declaration and the principal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    pub content_type: String,
    /// `app` or `org:<organization id>`.
    pub scope: String,
    /// The organization for organization scope, the events' tenant.
    pub tenant: Option<Identity>,
}

/// Who writes: the principal's subject and kind, `anonymous` without one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Actor {
    pub subject: String,
    pub kind: String,
    /// The request id, the events' correlation id.
    pub correlation: Option<String>,
}
impl Actor {
    /// The events' `actor`: `{"subject", "kind"}`.
    pub(crate) fn json(&self) -> Value {
        json!({"subject": self.subject, "kind": self.kind})
    }
}

/// How a save meets the variant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteKind {
    /// A new entry with its first variant (`POST`, or the first singleton
    /// `PUT`).
    NewEntry { id: Uuid, singleton: bool },
    /// A new variant of an existing entry (`If-None-Match: *`).
    NewVariant,
    /// A new draft revision of an existing variant (`If-Match`): the
    /// expected draft revision, `None` for an `If-Match` that names none.
    /// `none_match` is an `If-None-Match: *` sent alongside, which an
    /// existing variant always fails.
    Update {
        expected: Option<i64>,
        none_match: bool,
    },
}

/// What a save writes.
#[derive(Clone, Debug, PartialEq)]
pub enum Content {
    /// `PUT` and creation: the variant's fields.
    Replace(Map<String, Value>),
    /// `PATCH`: an RFC 7396 merge patch over the variant's own fields.
    Patch(Value),
    /// Restore: revision `n`'s data as a new draft revision.
    Restore(i64),
}

/// One variant as the management API returns it.
#[derive(Clone, Debug, PartialEq)]
pub struct VariantView {
    pub id: String,
    pub content_type: String,
    pub locale: String,
    pub draft_revision: i64,
    pub published_revision: Option<i64>,
    pub model_version: i32,
    pub created_at: String,
    pub created_by: String,
    pub updated_at: String,
    pub updated_by: String,
    pub published_at: Option<String>,
    pub first_published_at: Option<String>,
    pub fields: Map<String, Value>,
}
impl VariantView {
    /// `draft`, `published` or `changed` (section 1).
    pub fn status(&self) -> &'static str {
        status(self.draft_revision, self.published_revision)
    }
}

pub(crate) fn status(draft: i64, published: Option<i64>) -> &'static str {
    match published {
        None => "draft",
        Some(p) if p == draft => "published",
        Some(_) => "changed",
    }
}

/// A save's outcome.
#[derive(Clone, Debug, PartialEq)]
pub struct Saved {
    pub view: VariantView,
    /// The variant (and for [`WriteKind::NewEntry`] the entry) was created.
    pub created: bool,
    /// Rules a restored revision no longer passes; it was restored anyway.
    pub warnings: Vec<FieldError>,
}

/// `GET /entries/{type}/{id}`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntrySummary {
    pub id: String,
    pub content_type: String,
    pub created_at: String,
    pub deleted_at: Option<String>,
    /// `(locale, draft_revision, published_revision, updated_at)` in model
    /// locale order.
    pub variants: Vec<(String, i64, Option<i64>, String)>,
}

/// One row of the revision history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RevisionItem {
    pub revision: i64,
    pub kind: String,
    pub restored_from: Option<i64>,
    pub model_version: i32,
    pub created_at: String,
    pub created_by: String,
    pub title: Option<String>,
    pub published: bool,
}

/// One revision with its stored data.
#[derive(Clone, Debug, PartialEq)]
pub struct RevisionView {
    pub item: RevisionItem,
    pub id: String,
    pub content_type: String,
    pub locale: String,
    pub fields: Map<String, Value>,
}

/// One referrer of an entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Referrer {
    pub id: String,
    pub content_type: String,
    pub locale: String,
    pub stage: String,
    pub path: String,
    pub title: Option<String>,
}

/// A page of a keyset listing: the items and the sort key of the last
/// one when another page follows.
#[derive(Clone, Debug, PartialEq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next: Option<(Vec<Option<String>>, String)>,
}

/// `to_char` of a `timestamptz` as RFC 3339 UTC with milliseconds.
pub(crate) fn ts(column: &str) -> String {
    format!("to_char({column} AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')")
}

/// The variant columns [`view`] reads, from [`VARIANT_FROM`].
pub(crate) fn variant_columns() -> String {
    format!(
        "v.entry_id::text, v.content_type, v.locale, v.draft_revision, v.published_revision, \
         v.draft_data::text, {}, e.created_by, {}, v.draft_updated_by, {}, {}, r.model_version",
        ts("e.created_at"),
        ts("v.draft_updated_at"),
        ts("v.published_at"),
        ts("v.first_published_at"),
    )
}
/// Variants with their entry and draft revision, joined on the copied
/// scope and type columns.
pub(crate) const VARIANT_FROM: &str = "merezha_content.variants v \
     JOIN merezha_content.entries e \
       ON e.id = v.entry_id AND e.scope = v.scope AND e.content_type = v.content_type \
     JOIN merezha_content.revisions r \
       ON r.entry_id = v.entry_id AND r.locale = v.locale AND r.revision = v.draft_revision";
/// Number of [`variant_columns`].
pub(crate) const VARIANT_COLUMN_COUNT: usize = 13;

pub(crate) fn view(row: &tokio_postgres::Row) -> Result<VariantView, Failure> {
    let data: String = row.get(5);
    let fields = match serde_json::from_str(&data) {
        Ok(Value::Object(fields)) => fields,
        _ => return Err(Failure::transient("internal")),
    };
    Ok(VariantView {
        id: row.get(0),
        content_type: row.get(1),
        locale: row.get(2),
        draft_revision: row.get(3),
        published_revision: row.get(4),
        fields,
        created_at: row.get(6),
        created_by: row.get(7),
        updated_at: row.get(8),
        updated_by: row.get(9),
        published_at: row.get(10),
        first_published_at: row.get(11),
        model_version: row.get(12),
    })
}

/// Lock that survives a poisoned holder; nothing guarded here can be
/// left half-written.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// [`Database::transaction`] for work that rejects with details: the
/// details travel beside the failure, which rolls the transaction back.
pub(crate) async fn transaction<T, F>(
    db: &Database,
    deadline: Instant,
    work: F,
) -> Result<T, Rejection>
where
    T: Send + 'static,
    F: for<'t> FnOnce(&'t mut Tx<'_>) -> BoxFuture<'t, Result<T, Rejection>> + Send,
{
    let slot: Arc<Mutex<Option<Rejection>>> = Arc::new(Mutex::new(None));
    let kept = slot.clone();
    let result = db
        .transaction(deadline, move |tx| {
            let work = work(tx);
            Box::pin(async move {
                work.await.map_err(|rejection| {
                    let failure = rejection.failure.clone();
                    *lock(&kept) = Some(rejection);
                    failure
                })
            })
        })
        .await;
    match result {
        Ok(value) => Ok(value),
        Err(failure) => match lock(&slot).take() {
            Some(rejection) if rejection.failure == failure => Err(rejection),
            _ => Err(Rejection::new(failure)),
        },
    }
}

pub(crate) fn sql(error: &tokio_postgres::Error) -> Rejection {
    Rejection::new(classify(error))
}

/// A failed write statement: unique violations map to their conflict, or
/// through the derived index name to a `unique` detail on the field.
pub(crate) fn write_error(error: &tokio_postgres::Error, indexes: &[DerivedIndex]) -> Rejection {
    let db = error.as_db_error();
    if error.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION)
        && let Some(constraint) = db.and_then(|d| d.constraint())
    {
        match constraint {
            "entries_pkey" => return Rejection::code("entry_exists", Classification::Conflict),
            "entries_one_singleton" => {
                return Rejection::code("singleton_exists", Classification::Conflict);
            }
            "variants_pkey" => return Rejection::code("variant_exists", Classification::Conflict),
            name => {
                if let Some(index) = indexes.iter().find(|i| i.name == name)
                    && let Ok(detail) =
                        FieldError::new(format!("/fields/{}", index.field), "unique")
                {
                    return Rejection::invalid(vec![detail]);
                }
            }
        }
    }
    sql(error)
}

/// The entry and asset store behind the management routes.
#[derive(Clone)]
pub struct Store {
    state: Arc<ContentState>,
    ctx: AppContext,
    outbox: SourceOutbox,
    indexes: Arc<Vec<DerivedIndex>>,
}
impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

/// Fields `content_type` keeps on the default variant only.
pub(crate) fn shared_fields(content_type: &ContentType) -> Vec<String> {
    if !content_type.localized {
        return Vec::new();
    }
    content_type
        .fields
        .iter()
        .filter(|f| !f.localized)
        .map(|f| f.name.clone())
        .collect()
}

/// The first top-level slug field, the one `draft_slug` mirrors.
pub(crate) fn slug_field(content_type: &ContentType) -> Option<&crate::model::Field> {
    content_type
        .fields
        .iter()
        .find(|f| matches!(f.kind, FieldKind::Slug(_)))
}

pub(crate) fn parse_object(text: &str) -> Result<Map<String, Value>, Rejection> {
    match serde_json::from_str(text) {
        Ok(Value::Object(map)) => Ok(map),
        _ => Err(Rejection::new(Failure::transient("internal"))),
    }
}

pub(crate) fn new_id(ctx: &AppContext) -> Result<Uuid, Failure> {
    let mut random = [0u8; 10];
    ctx.random().fill(&mut random)?;
    let millis = u64::try_from(jiff::Timestamp::now().as_millisecond()).unwrap_or(0);
    Ok(uuid::Builder::from_unix_timestamp_millis(millis, &random).into_uuid())
}

/// A context for the work inside one transaction.
pub(crate) struct Work {
    pub(crate) model: Arc<ContentModel>,
    pub(crate) state: Arc<ContentState>,
    pub(crate) ctx: AppContext,
    pub(crate) outbox: SourceOutbox,
    pub(crate) indexes: Arc<Vec<DerivedIndex>>,
    pub(crate) retain: i64,
    pub(crate) max_entry_bytes: usize,
}

impl Store {
    pub(crate) fn new(state: Arc<ContentState>, ctx: AppContext, outbox: SourceOutbox) -> Self {
        let indexes = Arc::new(derived_indexes(state.model()));
        Self {
            state,
            ctx,
            outbox,
            indexes,
        }
    }
    pub fn model(&self) -> &Arc<ContentModel> {
        self.state.model()
    }
    pub fn state(&self) -> &Arc<ContentState> {
        &self.state
    }
    pub(crate) fn deadline(&self) -> Instant {
        Instant::now() + self.state.settings().limits.query_timeout
    }
    /// The transaction deadline of a batch of `items`: one
    /// `limits.query_timeout` per [`BATCH_ITEMS_PER_TIMEOUT`] items started,
    /// so a full batch is not held to one request's bound. Each statement
    /// stays bounded by `database.statement_timeout_ms`.
    pub(crate) fn batch_deadline(&self, items: usize) -> Instant {
        let periods = u32::try_from(items.div_ceil(BATCH_ITEMS_PER_TIMEOUT).max(1)).unwrap_or(1);
        Instant::now() + self.state.settings().limits.query_timeout * periods
    }
    pub(crate) fn database(&self) -> &Database {
        self.ctx.database()
    }
    pub(crate) fn work(&self) -> Work {
        Work {
            model: self.state.model().clone(),
            state: self.state.clone(),
            ctx: self.ctx.clone(),
            outbox: self.outbox.clone(),
            indexes: self.indexes.clone(),
            retain: i64::from(self.state.settings().revisions_retain),
            max_entry_bytes: self.state.settings().limits.max_entry_bytes,
        }
    }

    /// A fresh UUIDv7 entry id.
    pub fn new_entry_id(&self) -> Result<Uuid, Failure> {
        new_id(&self.ctx)
    }

    /// Save one variant: create it, replace or patch its fields, or
    /// restore a revision, in one transaction that starts with the model
    /// version check.
    pub async fn save(
        &self,
        target: &Target,
        id: Uuid,
        locale: &str,
        kind: WriteKind,
        content: Content,
        actor: &Actor,
    ) -> Result<Saved, Rejection> {
        let work = self.work();
        let target = target.clone();
        let locale = locale.to_owned();
        let actor = actor.clone();
        transaction(self.database(), self.deadline(), move |tx| {
            Box::pin(async move {
                work.save(tx, &target, id, &locale, kind, content, &actor)
                    .await
            })
        })
        .await
    }

    /// One variant, not deleted.
    pub async fn variant(
        &self,
        target: &Target,
        id: Uuid,
        locale: &str,
    ) -> Result<VariantView, Rejection> {
        let target = target.clone();
        let locale = locale.to_owned();
        transaction(self.database(), self.deadline(), move |tx| {
            Box::pin(async move { read_view(tx, &target, id, &locale, true).await })
        })
        .await
    }

    /// The entry with its variants, trashed or not.
    pub async fn summary(&self, target: &Target, id: Uuid) -> Result<EntrySummary, Rejection> {
        let target = target.clone();
        let order: Vec<String> = self
            .model()
            .locales()
            .iter()
            .map(|l| l.code.clone())
            .collect();
        let id_text = id.hyphenated().to_string();
        transaction(self.database(), self.deadline(), move |tx| {
            Box::pin(async move {
                let row = tx
                    .query_opt(
                        &format!(
                            "SELECT id::text, content_type, {}, {} FROM merezha_content.entries \
                              WHERE id = $1::text::uuid AND scope = $2 AND content_type = $3",
                            ts("created_at"),
                            ts("deleted_at")
                        ),
                        &[&id_text, &target.scope, &target.content_type],
                    )
                    .await
                    .map_err(|e| sql(&e))?
                    .ok_or_else(Rejection::not_found)?;
                let rows = tx
                    .query(
                        &format!(
                            "SELECT locale, draft_revision, published_revision, {} \
                               FROM merezha_content.variants \
                              WHERE entry_id = $1::text::uuid AND scope = $2 AND content_type = $3",
                            ts("draft_updated_at")
                        ),
                        &[&id_text, &target.scope, &target.content_type],
                    )
                    .await
                    .map_err(|e| sql(&e))?;
                let mut variants: Vec<(String, i64, Option<i64>, String)> = rows
                    .iter()
                    .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
                    .collect();
                variants.sort_by_key(|v| order.iter().position(|l| *l == v.0));
                Ok(EntrySummary {
                    id: row.get(0),
                    content_type: row.get(1),
                    created_at: row.get(2),
                    deleted_at: row.get(3),
                    variants,
                })
            })
        })
        .await
    }

    /// The id of the scope's singleton entry of `target`'s type.
    pub async fn singleton(&self, target: &Target) -> Result<Option<Uuid>, Rejection> {
        let target = target.clone();
        transaction(self.database(), self.deadline(), move |tx| {
            Box::pin(async move {
                let row = tx
                    .query_opt(
                        "SELECT id::text FROM merezha_content.entries \
                          WHERE scope = $1 AND content_type = $2 AND singleton \
                            AND deleted_at IS NULL",
                        &[&target.scope, &target.content_type],
                    )
                    .await
                    .map_err(|e| sql(&e))?;
                Ok(row.and_then(|r| Uuid::try_parse(r.get::<_, &str>(0)).ok()))
            })
        })
        .await
    }

    /// Revision history, newest first, after revision `before` when given.
    pub async fn revisions(
        &self,
        target: &Target,
        id: Uuid,
        locale: &str,
        before: Option<i64>,
        limit: u32,
    ) -> Result<Page<RevisionItem>, Rejection> {
        let target = target.clone();
        let locale = locale.to_owned();
        let title = self
            .model()
            .content_type(&target.content_type)
            .and_then(|t| t.title_field.clone());
        let id_text = id.hyphenated().to_string();
        let fetch = i64::from(limit) + 1;
        transaction(self.database(), self.deadline(), move |tx| {
            Box::pin(async move {
                variant_exists(tx, &target, &id_text, &locale).await?;
                let rows = tx
                    .query(
                        &format!(
                            "SELECT r.revision, r.kind, r.restored_from, r.model_version, {}, \
                                    r.created_by, r.data->>$5, \
                                    coalesce(r.revision = v.published_revision, false) \
                               FROM merezha_content.revisions r \
                               JOIN merezha_content.variants v \
                                 ON v.entry_id = r.entry_id AND v.locale = r.locale \
                              WHERE r.entry_id = $1::text::uuid AND r.locale = $2 \
                                AND v.scope = $3 AND v.content_type = $4 AND NOT v.deleted \
                                AND ($6::int8 IS NULL OR r.revision < $6) \
                              ORDER BY r.revision DESC LIMIT $7",
                            ts("r.created_at")
                        ),
                        &[
                            &id_text,
                            &locale,
                            &target.scope,
                            &target.content_type,
                            &title,
                            &before,
                            &fetch,
                        ],
                    )
                    .await
                    .map_err(|e| sql(&e))?;
                let mut items: Vec<RevisionItem> = rows.iter().map(revision_item).collect();
                let next = if items.len() > limit as usize {
                    items.truncate(limit as usize);
                    items
                        .last()
                        .map(|i| (vec![Some(i.revision.to_string())], id_text.clone()))
                } else {
                    None
                };
                Ok(Page { items, next })
            })
        })
        .await
    }

    /// Revision `n` of a variant: `revision_not_found` when it was pruned
    /// or never existed.
    pub async fn revision(
        &self,
        target: &Target,
        id: Uuid,
        locale: &str,
        n: i64,
    ) -> Result<RevisionView, Rejection> {
        let target = target.clone();
        let locale = locale.to_owned();
        let title = self
            .model()
            .content_type(&target.content_type)
            .and_then(|t| t.title_field.clone());
        let id_text = id.hyphenated().to_string();
        transaction(self.database(), self.deadline(), move |tx| {
            Box::pin(async move {
                variant_exists(tx, &target, &id_text, &locale).await?;
                let row = tx
                    .query_opt(
                        &format!(
                            "SELECT r.revision, r.kind, r.restored_from, r.model_version, {}, \
                                    r.created_by, r.data->>$5, \
                                    coalesce(r.revision = v.published_revision, false), \
                                    r.data::text \
                               FROM merezha_content.revisions r \
                               JOIN merezha_content.variants v \
                                 ON v.entry_id = r.entry_id AND v.locale = r.locale \
                              WHERE r.entry_id = $1::text::uuid AND r.locale = $2 \
                                AND v.scope = $3 AND v.content_type = $4 AND NOT v.deleted \
                                AND r.revision = $6",
                            ts("r.created_at")
                        ),
                        &[
                            &id_text,
                            &locale,
                            &target.scope,
                            &target.content_type,
                            &title,
                            &n,
                        ],
                    )
                    .await
                    .map_err(|e| sql(&e))?
                    .ok_or_else(|| Rejection::new(Failure::permanent("revision_not_found")))?;
                Ok(RevisionView {
                    item: revision_item(&row),
                    fields: parse_object(row.get(8))?,
                    id: id_text,
                    content_type: target.content_type.clone(),
                    locale,
                })
            })
        })
        .await
    }

    /// Entries whose `stage` refs point at the entry, in the entry's scope,
    /// ordered by `(entry, locale, path)` after `after`.
    pub async fn referrers(
        &self,
        target: &Target,
        id: Uuid,
        stage: &str,
        after: Option<(String, String, String)>,
        limit: u32,
    ) -> Result<Page<Referrer>, Rejection> {
        let target = target.clone();
        let stage = stage.to_owned();
        let id_text = id.hyphenated().to_string();
        let fetch = i64::from(limit) + 1;
        let title = self.title_case();
        transaction(self.database(), self.deadline(), move |tx| {
            Box::pin(async move {
                tx.query_opt(
                    "SELECT 1 FROM merezha_content.entries \
                      WHERE id = $1::text::uuid AND scope = $2 AND content_type = $3",
                    &[&id_text, &target.scope, &target.content_type],
                )
                .await
                .map_err(|e| sql(&e))?
                .ok_or_else(Rejection::not_found)?;
                let (entry, locale, path) = match after {
                    Some((e, l, p)) => (Some(e), Some(l), Some(p)),
                    None => (None, None, None),
                };
                let rows = tx
                    .query(
                        &format!(
                            "SELECT f.from_entry::text, e.content_type, f.from_locale, f.stage, \
                                    f.path, {title} \
                               FROM merezha_content.refs f \
                               JOIN merezha_content.entries e \
                                 ON e.id = f.from_entry AND e.scope = f.scope \
                               LEFT JOIN merezha_content.variants v \
                                 ON v.entry_id = f.from_entry AND v.locale = f.from_locale \
                                AND v.scope = f.scope \
                               LEFT JOIN merezha_content.published p \
                                 ON p.entry_id = f.from_entry AND p.locale = f.from_locale \
                                AND p.scope = f.scope \
                              WHERE f.target_kind = 'entry' AND f.target_id = $1::text::uuid \
                                AND f.scope = $2 AND f.stage = $3 AND e.deleted_at IS NULL \
                                AND ($4::text IS NULL OR (f.from_entry, f.from_locale, f.path) \
                                     > ($4::text::uuid, $5::text, $6::text)) \
                              ORDER BY f.from_entry, f.from_locale, f.path LIMIT $7"
                        ),
                        &[
                            &id_text,
                            &target.scope,
                            &stage,
                            &entry,
                            &locale,
                            &path,
                            &fetch,
                        ],
                    )
                    .await
                    .map_err(|e| sql(&e))?;
                let mut items: Vec<Referrer> = rows
                    .iter()
                    .map(|r| Referrer {
                        id: r.get(0),
                        content_type: r.get(1),
                        locale: r.get(2),
                        stage: r.get(3),
                        path: r.get(4),
                        title: r.get(5),
                    })
                    .collect();
                let next = if items.len() > limit as usize {
                    items.truncate(limit as usize);
                    items.last().map(|i| {
                        (
                            vec![Some(i.locale.clone()), Some(i.path.clone())],
                            i.id.clone(),
                        )
                    })
                } else {
                    None
                };
                Ok(Page { items, next })
            })
        })
        .await
    }

    /// `CASE e.content_type WHEN '<type>' THEN <stage data> ->> '<title>'
    /// ... END`, from the model; names are `[a-z0-9_]`.
    fn title_case(&self) -> String {
        let mut arms = String::new();
        for content_type in self.model().types().values() {
            if let Some(title) = &content_type.title_field {
                arms.push_str(&format!(
                    " WHEN '{}' THEN (CASE WHEN f.stage = 'draft' THEN v.draft_data \
                     ELSE p.data END) ->> '{}'",
                    content_type.name, title
                ));
            }
        }
        if arms.is_empty() {
            "NULL::text".to_owned()
        } else {
            format!("CASE e.content_type{arms} END")
        }
    }

    /// Run a list statement built by [`crate::query`].
    pub(crate) async fn list(
        &self,
        statement: String,
        params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
        keys: usize,
        limit: u32,
    ) -> Result<Page<VariantView>, Rejection> {
        transaction(self.database(), self.deadline(), move |tx| {
            Box::pin(async move {
                let refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
                    .iter()
                    .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
                    .collect();
                // A data exception (class 22) can only come from a cursor
                // value cast back to its key's type.
                let rows = tx.query(&statement, &refs).await.map_err(|e| {
                    if e.code().is_some_and(|c| c.code().starts_with("22")) {
                        Rejection {
                            failure: Failure::permanent("invalid_cursor"),
                            details: vec![crate::query::detail("/cursor", "invalid_value")],
                        }
                    } else {
                        sql(&e)
                    }
                })?;
                let mut items = Vec::with_capacity(rows.len());
                let mut last_keys = Vec::new();
                for (index, row) in rows.iter().enumerate() {
                    if index == limit as usize {
                        break;
                    }
                    items.push(view(row)?);
                    last_keys = (0..keys)
                        .map(|k| row.get::<_, Option<String>>(VARIANT_COLUMN_COUNT + k))
                        .collect();
                }
                let next = if rows.len() > limit as usize {
                    items.last().map(|v| (last_keys, v.id.clone()))
                } else {
                    None
                };
                Ok(Page { items, next })
            })
        })
        .await
    }
}

fn revision_item(row: &tokio_postgres::Row) -> RevisionItem {
    RevisionItem {
        revision: row.get(0),
        kind: row.get(1),
        restored_from: row.get(2),
        model_version: row.get(3),
        created_at: row.get(4),
        created_by: row.get(5),
        title: row.get(6),
        published: row.get(7),
    }
}

async fn variant_exists(
    tx: &mut Tx<'_>,
    target: &Target,
    id: &str,
    locale: &str,
) -> Result<(), Rejection> {
    tx.query_opt(
        "SELECT 1 FROM merezha_content.variants \
          WHERE entry_id = $1::text::uuid AND locale = $2 AND scope = $3 \
            AND content_type = $4 AND NOT deleted",
        &[&id, &locale, &target.scope, &target.content_type],
    )
    .await
    .map_err(|e| sql(&e))?
    .map(|_| ())
    .ok_or_else(Rejection::not_found)
}

pub(crate) async fn read_view(
    tx: &mut Tx<'_>,
    target: &Target,
    id: Uuid,
    locale: &str,
    live: bool,
) -> Result<VariantView, Rejection> {
    let id_text = id.hyphenated().to_string();
    let live = if live {
        " AND NOT v.deleted AND e.deleted_at IS NULL"
    } else {
        ""
    };
    let row = tx
        .query_opt(
            &format!(
                "SELECT {} FROM {VARIANT_FROM} \
                  WHERE v.entry_id = $1::text::uuid AND v.locale = $2 \
                    AND v.scope = $3 AND v.content_type = $4{live}",
                variant_columns()
            ),
            &[&id_text, &locale, &target.scope, &target.content_type],
        )
        .await
        .map_err(|e| sql(&e))?
        .ok_or_else(Rejection::not_found)?;
    Ok(view(&row)?)
}

impl Work {
    #[allow(clippy::too_many_arguments)]
    async fn save(
        &self,
        tx: &mut Tx<'_>,
        target: &Target,
        id: Uuid,
        locale: &str,
        kind: WriteKind,
        content: Content,
        actor: &Actor,
    ) -> Result<Saved, Rejection> {
        // Section 2: nothing validated against a superseded model commits.
        self.state.check_writable(tx).await?;
        let model = self.model.clone();
        let Some(content_type) = model.content_type(&target.content_type) else {
            return Err(Failure::permanent("content_type_not_found").into());
        };
        let default_locale = model.default_locale().code.clone();
        let owns_shared = locale == default_locale || !content_type.localized;
        let shared = shared_fields(content_type);
        let id_text = id.hyphenated().to_string();
        let scope = target.scope.as_str();
        let type_name = target.content_type.as_str();

        match &kind {
            WriteKind::NewEntry { singleton, .. } => {
                tx.execute(
                    "INSERT INTO merezha_content.entries \
                         (id, scope, content_type, singleton, created_at, created_by) \
                     VALUES ($1::text::uuid, $2, $3, $4, now(), $5)",
                    &[&id_text, &scope, &type_name, singleton, &actor.subject],
                )
                .await
                .map_err(|e| write_error(&e, &self.indexes))?;
            }
            WriteKind::NewVariant | WriteKind::Update { .. } => {
                tx.query_opt(
                    "SELECT 1 FROM merezha_content.entries \
                      WHERE id = $1::text::uuid AND scope = $2 AND content_type = $3 \
                        AND deleted_at IS NULL FOR SHARE",
                    &[&id_text, &scope, &type_name],
                )
                .await
                .map_err(|e| sql(&e))?
                .ok_or_else(Rejection::not_found)?;
            }
        }
        // The current draft, under the same predicates.
        let current = tx
            .query_opt(
                "SELECT draft_revision, draft_data::text FROM merezha_content.variants \
                  WHERE entry_id = $1::text::uuid AND locale = $2 AND scope = $3 \
                    AND content_type = $4 AND NOT deleted",
                &[&id_text, &locale, &scope, &type_name],
            )
            .await
            .map_err(|e| sql(&e))?;
        let current: Option<(i64, Map<String, Value>)> = match current {
            Some(row) => Some((row.get(0), parse_object(row.get(1))?)),
            None => None,
        };
        let expected = match (&kind, &current) {
            (WriteKind::Update { .. }, None) => return Err(Rejection::not_found()),
            (
                WriteKind::Update {
                    none_match: true, ..
                },
                Some(_),
            ) => {
                return Err(Rejection::code("variant_exists", Classification::Conflict));
            }
            (WriteKind::Update { expected, .. }, Some((revision, _))) => {
                if *expected != Some(*revision) {
                    return Err(Rejection::code(
                        "revision_conflict",
                        Classification::Conflict,
                    ));
                }
                Some(*revision)
            }
            (_, Some(_)) => {
                return Err(Rejection::code("variant_exists", Classification::Conflict));
            }
            (_, None) => None,
        };
        // The variant's own fields: shared ones belong to the default.
        let own_current: Option<Map<String, Value>> = current.map(|(_, mut data)| {
            if !owns_shared {
                data.retain(|k, _| !shared.contains(k));
            }
            data
        });
        // The default variant's shared values, share-locked: a default save
        // committing meanwhile would otherwise copy its values into this
        // variant and be overwritten by the stale ones read here. Locking
        // the default row before this variant's keeps one lock order.
        let shared_values: Map<String, Value> = if !owns_shared && !shared.is_empty() {
            let row = tx
                .query_opt(
                    "SELECT draft_data::text FROM merezha_content.variants \
                      WHERE entry_id = $1::text::uuid AND locale = $2 AND scope = $3 \
                        AND content_type = $4 AND NOT deleted FOR SHARE",
                    &[&id_text, &default_locale, &scope, &type_name],
                )
                .await
                .map_err(|e| sql(&e))?;
            match row {
                Some(row) => {
                    let mut data = parse_object(row.get(0))?;
                    data.retain(|k, _| shared.contains(k));
                    data
                }
                None => Map::new(),
            }
        } else {
            Map::new()
        };
        let restoring = matches!(content, Content::Restore(_));
        let (revision_kind, restored_from) = match (&content, &kind) {
            (Content::Restore(n), _) => ("restore", Some(*n)),
            (_, WriteKind::Update { .. }) => ("save", None),
            _ => ("create", None),
        };
        let input: Value = match content {
            Content::Replace(mut fields) => {
                // The host never changes an existing slug: a save that leaves
                // it out keeps it.
                if let Some(own) = &own_current {
                    for field in &content_type.fields {
                        let owned = owns_shared || field.localized;
                        if owned
                            && matches!(field.kind, FieldKind::Slug(_))
                            && !fields.contains_key(&field.name)
                            && let Some(value) = own.get(&field.name)
                        {
                            fields.insert(field.name.clone(), value.clone());
                        }
                    }
                }
                Value::Object(fields)
            }
            Content::Patch(patch) => {
                let mut base = Value::Object(own_current.clone().unwrap_or_default());
                merge_patch(&mut base, &patch);
                base
            }
            Content::Restore(n) => {
                let row = tx
                    .query_opt(
                        "SELECT r.data::text FROM merezha_content.revisions r \
                           JOIN merezha_content.variants v \
                             ON v.entry_id = r.entry_id AND v.locale = r.locale \
                          WHERE r.entry_id = $1::text::uuid AND r.locale = $2 \
                            AND v.scope = $3 AND v.content_type = $4 AND r.revision = $5",
                        &[&id_text, &locale, &scope, &type_name, &n],
                    )
                    .await
                    .map_err(|e| sql(&e))?
                    .ok_or_else(|| Rejection::new(Failure::permanent("revision_not_found")))?;
                Value::Object(parse_object(row.get(0))?)
            }
        };
        if !input.is_object() {
            return Err(Failure::permanent("invalid_json").into());
        }
        if encoded_len(&input) > self.max_entry_bytes {
            return Err(Failure::permanent("body_limit").into());
        }
        let options = ValidateOptions {
            stage: Stage::Draft,
            default_variant: owns_shared,
            new_variant: expected.is_none(),
        };
        let mut keys = RandomKeys(self.ctx.random());
        let (mut own, references, mut problems) = match model
            .validate_fields(type_name, &input, &options, &mut keys)
        {
            Ok(valid) => (valid.fields, valid.references, Vec::new()),
            Err(ValueError::Failed(failure)) => return Err(failure.into()),
            Err(ValueError::Invalid(details)) => {
                let references = field_references(&model, type_name, &input, &options, &mut keys)?;
                let own = input.as_object().cloned().unwrap_or_default();
                (own, references, details)
            }
        };
        let resolution = refs::resolve(tx, scope, &references).await?;
        problems.extend(resolution.problems);
        if !problems.is_empty() && !restoring {
            return Err(Rejection::invalid(problems));
        }
        problems.truncate(MAX_DETAILS);
        for (path, target_id, found_type) in &resolution.types {
            refs::fill_type(&mut own, path, target_id, found_type);
        }
        let own_value = Value::Object(own);
        if encoded_len(&own_value) > self.max_entry_bytes {
            return Err(Failure::permanent("body_limit").into());
        }
        let Value::Object(own) = own_value else {
            return Err(Failure::transient("internal").into());
        };
        let mut draft = own.clone();
        if !owns_shared {
            draft.extend(shared_values);
        }
        let slug = slug_field(content_type);
        let draft_slug: Option<String> = slug
            .and_then(|f| draft.get(&f.name))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let draft_text = Value::Object(draft).to_string();
        let own_text = Value::Object(own.clone()).to_string();

        let revision: i64 = match expected {
            None => {
                tx.execute(
                    "INSERT INTO merezha_content.variants \
                         (entry_id, locale, scope, content_type, draft_revision, draft_data, \
                          draft_slug, draft_updated_at, draft_updated_by) \
                     VALUES ($1::text::uuid, $2, $3, $4, 1, $5::text::jsonb, $6, now(), $7)",
                    &[
                        &id_text,
                        &locale,
                        &scope,
                        &type_name,
                        &draft_text,
                        &draft_slug,
                        &actor.subject,
                    ],
                )
                .await
                .map_err(|e| write_error(&e, &self.indexes))?;
                1
            }
            Some(expected) => {
                let row = tx
                    .query_opt(
                        "UPDATE merezha_content.variants \
                            SET draft_revision = draft_revision + 1, \
                                draft_data = $6::text::jsonb, draft_slug = $7, \
                                draft_updated_at = now(), draft_updated_by = $8 \
                          WHERE entry_id = $1::text::uuid AND locale = $2 AND scope = $3 \
                            AND content_type = $4 AND draft_revision = $5 AND NOT deleted \
                         RETURNING draft_revision",
                        &[
                            &id_text,
                            &locale,
                            &scope,
                            &type_name,
                            &expected,
                            &draft_text,
                            &draft_slug,
                            &actor.subject,
                        ],
                    )
                    .await
                    .map_err(|e| write_error(&e, &self.indexes))?;
                match row {
                    Some(row) => row.get(0),
                    // A concurrent save won the compare and swap.
                    None => {
                        return Err(Rejection::code(
                            "revision_conflict",
                            Classification::Conflict,
                        ));
                    }
                }
            }
        };
        let model_version =
            i32::try_from(model.version()).map_err(|_| Failure::permanent("invalid_input"))?;
        tx.execute(
            "INSERT INTO merezha_content.revisions \
                 (entry_id, locale, revision, model_version, kind, restored_from, data, \
                  created_at, created_by) \
             VALUES ($1::text::uuid, $2, $3, $4, $5, $6, $7::text::jsonb, now(), $8)",
            &[
                &id_text,
                &locale,
                &revision,
                &model_version,
                &revision_kind,
                &restored_from,
                &own_text,
                &actor.subject,
            ],
        )
        .await
        .map_err(|e| sql(&e))?;
        // Retention: the newest `revisions.retain` plus the published one.
        tx.execute(
            "DELETE FROM merezha_content.revisions r USING merezha_content.variants v \
              WHERE r.entry_id = $1::text::uuid AND r.locale = $2 \
                AND v.entry_id = r.entry_id AND v.locale = r.locale \
                AND v.scope = $3 AND v.content_type = $4 \
                AND r.revision <= v.draft_revision - $5 \
                AND r.revision IS DISTINCT FROM v.published_revision",
            &[&id_text, &locale, &scope, &type_name, &self.retain],
        )
        .await
        .map_err(|e| sql(&e))?;
        // The variant's draft refs.
        tx.execute(
            "DELETE FROM merezha_content.refs \
              WHERE from_entry = $1::text::uuid AND from_locale = $2 AND stage = 'draft' \
                AND scope = $3",
            &[&id_text, &locale, &scope],
        )
        .await
        .map_err(|e| sql(&e))?;
        if !resolution.rows.is_empty() {
            let paths: Vec<String> = resolution.rows.iter().map(|r| r.0.clone()).collect();
            let kinds: Vec<String> = resolution.rows.iter().map(|r| r.1.to_owned()).collect();
            let targets: Vec<String> = resolution
                .rows
                .iter()
                .map(|r| r.2.hyphenated().to_string())
                .collect();
            tx.execute(
                "INSERT INTO merezha_content.refs \
                     (scope, from_entry, from_locale, stage, path, target_kind, target_id) \
                 SELECT $1, $2::text::uuid, $3, 'draft', path, kind, target::uuid \
                   FROM unnest($4::text[], $5::text[], $6::text[]) AS t(path, kind, target) \
                 ON CONFLICT DO NOTHING",
                &[&scope, &id_text, &locale, &paths, &kinds, &targets],
            )
            .await
            .map_err(|e| sql(&e))?;
        }
        // Shared values travel from the default variant to the others.
        if content_type.localized && owns_shared && !shared.is_empty() {
            let values: Map<String, Value> = own
                .iter()
                .filter(|(k, _)| shared.contains(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let values = Value::Object(values).to_string();
            let slug_shared = slug.is_some_and(|f| !f.localized);
            tx.execute(
                "UPDATE merezha_content.variants \
                    SET draft_data = (draft_data - $5::text[]) || $6::text::jsonb, \
                        draft_slug = CASE WHEN $7 THEN $8 ELSE draft_slug END \
                  WHERE entry_id = $1::text::uuid AND scope = $2 AND content_type = $3 \
                    AND locale <> $4 AND NOT deleted",
                &[
                    &id_text,
                    &scope,
                    &type_name,
                    &default_locale,
                    &shared,
                    &values,
                    &slug_shared,
                    &draft_slug,
                ],
            )
            .await
            .map_err(|e| write_error(&e, &self.indexes))?;
        }
        self.append_event(tx, target, &id_text, locale, revision, actor)
            .await?;
        let view = read_view(tx, target, id, locale, true).await?;
        Ok(Saved {
            view,
            created: expected.is_none(),
            warnings: if restoring { problems } else { Vec::new() },
        })
    }

    async fn append_event(
        &self,
        tx: &mut Tx<'_>,
        target: &Target,
        id: &str,
        locale: &str,
        revision: i64,
        actor: &Actor,
    ) -> Result<(), Rejection> {
        let payload = json!({
            "scope": target.scope,
            "type": target.content_type,
            "id": id,
            "locale": locale,
            "revision": revision,
            "actor": actor.json(),
        });
        let record = self.event(target, DRAFT_SAVED, payload, actor)?;
        self.append(tx, vec![record]).await
    }

    /// One event record of section 7 for `target`: a fresh UUIDv7 event id
    /// as its dedupe key, the actor's request id (else the event id) as
    /// its correlation, and the organization as its tenant.
    pub(crate) fn event(
        &self,
        target: &Target,
        topic: &str,
        payload: Value,
        actor: &Actor,
    ) -> Result<OutboxRecord, Rejection> {
        let event_id = new_id(&self.ctx)?.to_string();
        let correlation = actor
            .correlation
            .as_deref()
            .and_then(|c| Identity::new(c).ok())
            .map_or_else(|| Identity::new(event_id.clone()), Ok)?;
        Ok(self.outbox.event(
            Identity::new(topic)?,
            NewEvent {
                dedupe_key: DedupeKey::new(event_id)?,
                version: 1,
                occurred_at: jiff::Timestamp::now(),
                payload,
                context: MessageContext {
                    correlation_id: correlation,
                    causation_id: None,
                    tenant: target.tenant.clone(),
                },
            },
        ))
    }

    /// Append `records` in the transaction, in chunks the outbox accepts.
    pub(crate) async fn append(
        &self,
        tx: &mut Tx<'_>,
        records: Vec<OutboxRecord>,
    ) -> Result<(), Rejection> {
        for chunk in records.chunks(MAX_RECORDS_PER_APPEND) {
            self.outbox.append(tx, chunk, OnDuplicate::Fail).await?;
        }
        Ok(())
    }
}

/// The references of the fields that pass on their own, when the whole
/// set does not: a restore still records them, and a refused save reports
/// reference problems with the field ones.
pub(crate) fn field_references(
    model: &ContentModel,
    content_type: &str,
    input: &Value,
    options: &ValidateOptions,
    keys: &mut RandomKeys<'_>,
) -> Result<Vec<ValueReference>, Rejection> {
    let mut out = Vec::new();
    let Some(object) = input.as_object() else {
        return Ok(out);
    };
    for (name, value) in object {
        let mut single = Map::new();
        single.insert(name.clone(), value.clone());
        match model.validate_fields(content_type, &Value::Object(single), options, keys) {
            Ok(valid) => out.extend(valid.references),
            Err(ValueError::Failed(failure)) => return Err(failure.into()),
            Err(ValueError::Invalid(_)) => {}
        }
    }
    Ok(out)
}
