//! Deletion (section 4, soft delete and purge): moving an entry to the
//! trash, undelete, immediate purge, the trash listing and the
//! `content.trash_purge` job that hard-deletes expired trash.
//!
//! Every write is one transaction that starts with the model version
//! check and locks the entry `FOR UPDATE` under the route's scope
//! predicates. Deleting removes the entry's `published` rows and refs,
//! bumps the scope's generation and appends one
//! `content.entry_unpublished` (reason `delete`) per published locale
//! plus one `content.entry_deleted`.
use crate::statements;
use crate::{
    publish::{bump_generation, write_refs},
    refs,
    store::{
        Actor, EntrySummary, Page, Rejection, Store, Target, Work, field_references, parse_object,
        shared_fields, sql, transaction, ts, write_error,
    },
    value::{RandomKeys, Stage, ValidateOptions, ValueError},
};
use merezha_domain::{Classification, Failure, Identity};
use merezha_platform::{
    BoxFuture, Tx,
    ports::{JobContext, JobFailure, JobHandler},
};
use serde_json::{Value, json};
use std::time::Duration;
use uuid::Uuid;

/// The event a soft delete appends once (section 7).
pub const DELETED: &str = "content.entry_deleted";
/// The event an undelete appends.
pub const UNDELETED: &str = "content.entry_undeleted";
/// The event a purge appends per entry, immediate or by retention.
pub const PURGED: &str = "content.entry_purged";
/// The job kind of the trash purge, on the `content` queue.
pub const TRASH_PURGE: &str = "content.trash_purge";
/// The queue of content's job kinds (`[jobs.queues.content]`).
pub const QUEUE: &str = "content";
/// Entries one purge transaction removes.
pub const PURGE_BATCH: usize = 100;
/// The system subject of the purge job (R13).
pub const PURGE_SUBJECT: &str = "sys_content_trash";

/// One trashed entry, as `GET /trash` lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrashItem {
    pub id: String,
    pub content_type: String,
    pub deleted_at: String,
    pub deleted_by: Option<String>,
    pub title: Option<String>,
}

/// How `DELETE /entries/{type}/{id}` deletes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeleteOptions {
    /// Fail `entry_referenced` while any ref, in any scope, points at it.
    pub if_unreferenced: bool,
    /// Hard-delete at once, trashed or not (`content.entries.purge`).
    pub purge: bool,
}

impl Work {
    async fn delete_entry(
        &self,
        tx: &mut Tx<'_>,
        target: &Target,
        id: Uuid,
        options: DeleteOptions,
        actor: &Actor,
    ) -> Result<(), Rejection> {
        let db = self.ctx.database();
        self.state.check_writable(db, tx).await?;
        crate::organizations::guard_write(db, tx, &target.scope).await?;
        let id = id.hyphenated().to_string();
        let live: bool = db
            .query_opt(
                &*tx,
                &statements::ENTRY_LOCK_ANY,
                &[&id, &target.scope, &target.content_type],
            )
            .await
            .map_err(|e| sql(&e))?
            .map(|row| row.get(0))
            // Only a purge reaches an entry already in the trash.
            .filter(|live| *live || options.purge)
            .ok_or_else(Rejection::not_found)?;
        if options.if_unreferenced {
            // Referrers in every scope count, without being returned.
            let referenced = db
                .query_opt(&*tx, &statements::ENTRY_REFERENCED, &[&id])
                .await
                .map_err(|e| sql(&e))?;
            if referenced.is_some() {
                return Err(Rejection::code(
                    "entry_referenced",
                    Classification::Conflict,
                ));
            }
        }
        let mut records = Vec::new();
        if live {
            let rows = db
                .query(
                    &*tx,
                    &statements::PUBLISHED_LOCALES,
                    &[&id, &target.scope, &target.content_type],
                )
                .await
                .map_err(|e| sql(&e))?;
            let mut locales: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
            let order = self.model.locales();
            locales.sort_by_key(|l| order.iter().position(|o| o.code == *l));
            for locale in &locales {
                records.push(self.unpublished(target, &id, locale, "delete", actor)?);
            }
            records.push(self.entry_event(target, &id, DELETED, Some(actor), actor)?);
            bump_generation(db, tx, &target.scope).await?;
        }
        if options.purge {
            // Variants, revisions, published rows and outgoing refs
            // cascade; incoming refs go too, in every scope, so an id
            // created again does not inherit stale referrers.
            db.execute(
                &*tx,
                &statements::ENTRY_DELETE,
                &[&id, &target.scope, &target.content_type],
            )
            .await
            .map_err(|e| sql(&e))?;
            db.execute(&*tx, &statements::REFS_TO_ENTRY_DELETE, &[&id])
                .await
                .map_err(|e| sql(&e))?;
            records.push(self.entry_event(target, &id, PURGED, None, actor)?);
        } else {
            db.execute(
                &*tx,
                &statements::ENTRY_TRASH,
                &[&id, &target.scope, &target.content_type, &actor.subject],
            )
            .await
            .map_err(|e| sql(&e))?;
            // Deleted variants leave every unique index (they skip
            // `deleted`), which frees their values.
            db.execute(
                &*tx,
                &statements::VARIANTS_TRASH,
                &[&id, &target.scope, &target.content_type],
            )
            .await
            .map_err(|e| sql(&e))?;
            db.execute(
                &*tx,
                &statements::PUBLISHED_DELETE,
                &[&id, &target.scope, &target.content_type],
            )
            .await
            .map_err(|e| sql(&e))?;
            db.execute(
                &*tx,
                &statements::REFS_FROM_ENTRY_DELETE,
                &[&id, &target.scope],
            )
            .await
            .map_err(|e| sql(&e))?;
        }
        self.append(tx, records).await
    }

    /// `content.entry_deleted`, `content.entry_undeleted` (with an actor)
    /// or `content.entry_purged` (without).
    fn entry_event(
        &self,
        target: &Target,
        id: &str,
        topic: &str,
        with_actor: Option<&Actor>,
        actor: &Actor,
    ) -> Result<merezha_platform::ports::OutboxRecord, Rejection> {
        let mut payload = json!({
            "scope": target.scope,
            "type": target.content_type,
            "id": id,
        });
        if let (Some(with), Value::Object(map)) = (with_actor, &mut payload) {
            map.insert("actor".into(), with.json());
        }
        self.event(target, topic, payload, actor)
    }

    async fn undelete(
        &self,
        tx: &mut Tx<'_>,
        target: &Target,
        id: Uuid,
        actor: &Actor,
    ) -> Result<(), Rejection> {
        let db = self.ctx.database();
        self.state.check_writable(db, tx).await?;
        crate::organizations::guard_write(db, tx, &target.scope).await?;
        let id = id.hyphenated().to_string();
        let trashed: bool = db
            .query_opt(
                &*tx,
                &statements::ENTRY_LOCK_TRASHED,
                &[&id, &target.scope, &target.content_type],
            )
            .await
            .map_err(|e| sql(&e))?
            .map(|row| row.get(0))
            .ok_or_else(Rejection::not_found)?;
        if !trashed {
            // Already live: nothing to restore.
            return Ok(());
        }
        db.execute(
            &*tx,
            &statements::ENTRY_RESTORE,
            &[&id, &target.scope, &target.content_type],
        )
        .await
        .map_err(|e| write_error(&e, &self.indexes))?;
        // Drafts only: the published pointers were cleared at delete time.
        // A unique value taken meanwhile fails with its field.
        let rows = db
            .query(
                &*tx,
                &statements::VARIANTS_RESTORE,
                &[&id, &target.scope, &target.content_type],
            )
            .await
            .map_err(|e| write_error(&e, &self.indexes))?;
        // The drafts' refs were removed with the delete: record them again.
        let Some(content_type) = self.model.content_type(&target.content_type) else {
            return Err(Failure::permanent("content_type_not_found").into());
        };
        let default = self.model.default_locale().code.clone();
        let shared = shared_fields(content_type);
        for row in &rows {
            let locale: String = row.get(0);
            let mut data = parse_object(row.get(1))?;
            let owns_shared = locale == default || !content_type.localized;
            if !owns_shared {
                data.retain(|k, _| !shared.contains(k));
            }
            let data = Value::Object(data);
            let options = ValidateOptions {
                stage: Stage::Draft,
                default_variant: owns_shared,
                new_variant: false,
            };
            let mut keys = RandomKeys(self.ctx.random());
            let references =
                match self
                    .model
                    .validate_fields(&target.content_type, &data, &options, &mut keys)
                {
                    Ok(valid) => valid.references,
                    Err(ValueError::Failed(failure)) => return Err(failure.into()),
                    Err(ValueError::Invalid(_)) => field_references(
                        &self.model,
                        &target.content_type,
                        &data,
                        &options,
                        &mut keys,
                    )?,
                };
            let resolution = refs::resolve(db, tx, &target.scope, &references).await?;
            write_refs(
                db,
                tx,
                &target.scope,
                &id,
                &locale,
                "draft",
                &resolution.rows,
            )
            .await?;
        }
        let record = self.entry_event(target, &id, UNDELETED, Some(actor), actor)?;
        self.append(tx, vec![record]).await
    }

    /// Hard-delete up to [`PURGE_BATCH`] entries trashed before `retention`
    /// ago; returns how many.
    async fn purge_expired(
        &self,
        tx: &mut Tx<'_>,
        retention: Duration,
        actor: &Actor,
    ) -> Result<usize, Rejection> {
        let db = self.ctx.database();
        self.state.check_writable(db, tx).await?;
        let seconds = i64::try_from(retention.as_secs()).unwrap_or(i64::MAX);
        let limit = PURGE_BATCH as i64;
        let rows = db
            .query(&*tx, &statements::TRASH_EXPIRED, &[&seconds, &limit])
            .await
            .map_err(|e| sql(&e))?;
        if rows.is_empty() {
            return Ok(0);
        }
        let ids: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
        let scopes: Vec<String> = rows.iter().map(|r| r.get(1)).collect();
        let types: Vec<String> = rows.iter().map(|r| r.get(2)).collect();
        db.execute(&*tx, &statements::ENTRIES_PURGE, &[&ids, &scopes, &types])
            .await
            .map_err(|e| sql(&e))?;
        db.execute(&*tx, &statements::REFS_TO_ENTRIES_DELETE, &[&ids])
            .await
            .map_err(|e| sql(&e))?;
        let mut records = Vec::with_capacity(ids.len());
        for ((id, scope), content_type) in ids.iter().zip(&scopes).zip(&types) {
            let target = Target {
                content_type: content_type.clone(),
                scope: scope.clone(),
                tenant: scope
                    .strip_prefix("org:")
                    .and_then(|t| Identity::new(t).ok()),
            };
            records.push(self.entry_event(&target, id, PURGED, None, actor)?);
        }
        self.append(tx, records).await?;
        Ok(ids.len())
    }
}

impl Store {
    /// Move an entry to the trash (section 4): `deleted_at`, every variant
    /// `deleted` with its published pointer cleared, its `published` rows
    /// and refs removed, the scope's generation bumped, and one
    /// `content.entry_unpublished` per published locale plus one
    /// `content.entry_deleted` appended. With `purge`, the entry (trashed
    /// or not) is hard-deleted instead and `content.entry_purged` follows.
    pub async fn delete_entry(
        &self,
        target: &Target,
        id: Uuid,
        options: DeleteOptions,
        actor: &Actor,
    ) -> Result<(), Rejection> {
        let work = self.work();
        let target = target.clone();
        let actor = actor.clone();
        transaction(self.database(), self.deadline(), move |tx| {
            Box::pin(async move { work.delete_entry(tx, &target, id, options, &actor).await })
        })
        .await
    }

    /// Return a trashed entry as drafts: published versions stay gone, the
    /// drafts' refs are recorded again, and a unique value or singleton
    /// slot taken meanwhile fails it (`validation_failed` with a `unique`
    /// detail, `singleton_exists`). Appends `content.entry_undeleted`. A
    /// live entry is left as it is.
    pub async fn undelete(
        &self,
        target: &Target,
        id: Uuid,
        actor: &Actor,
    ) -> Result<EntrySummary, Rejection> {
        let work = self.work();
        let moved = target.clone();
        let actor = actor.clone();
        transaction(self.database(), self.deadline(), move |tx| {
            Box::pin(async move { work.undelete(tx, &moved, id, &actor).await })
        })
        .await?;
        self.summary(target, id).await
    }

    /// Trashed entries of the `(scope, type)` pairs in `allowed`, newest
    /// deletion first, after `after` (`deleted_at` in microseconds and the
    /// id) when given. Titles come from the default variant's draft.
    pub async fn trash(
        &self,
        allowed: Vec<(String, String)>,
        after: Option<(String, String)>,
        limit: u32,
    ) -> Result<Page<TrashItem>, Rejection> {
        let mut arms = String::new();
        for content_type in self.model().types().values() {
            if let Some(title) = &content_type.title_field {
                arms.push_str(&format!(
                    " WHEN '{}' THEN v.draft_data ->> '{}'",
                    content_type.name, title
                ));
            }
        }
        let title = if arms.is_empty() {
            "NULL::text".to_owned()
        } else {
            format!("CASE e.content_type{arms} END")
        };
        let default = self.model().default_locale().code.clone();
        let (scopes, types): (Vec<String>, Vec<String>) = allowed.into_iter().unzip();
        let (at, id) = match after {
            Some((at, id)) => (Some(at), Some(id)),
            None => (None, None),
        };
        let fetch = i64::from(limit) + 1;
        let statement = format!(
            "SELECT e.id::text, e.content_type, {}, e.deleted_by, {title}, \
                    to_char(e.deleted_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') \
               FROM merezha_content.entries e \
               LEFT JOIN merezha_content.variants v \
                 ON v.entry_id = e.id AND v.locale = $1 AND v.scope = e.scope \
                AND v.content_type = e.content_type \
              WHERE e.deleted_at IS NOT NULL \
                AND (e.scope, e.content_type) IN \
                    (SELECT * FROM unnest($2::text[], $3::text[])) \
                AND ($4::text IS NULL OR (e.deleted_at, e.id) < ($4::timestamptz, $5::text::uuid)) \
              ORDER BY e.deleted_at DESC, e.id DESC LIMIT $6",
            ts("e.deleted_at")
        );
        let db = self.database().clone();
        transaction(self.database(), self.deadline(), move |tx| {
            Box::pin(async move {
                let db = &db;
                let rows = db
                    .query_text(
                        &*tx,
                        &statements::TRASH_LIST,
                        &statement,
                        &[&default, &scopes, &types, &at, &id, &fetch],
                    )
                    .await
                    .map_err(|e| sql(&e))?;
                let mut items = Vec::with_capacity(rows.len());
                let mut last = None;
                for (index, row) in rows.iter().enumerate() {
                    if index == limit as usize {
                        break;
                    }
                    items.push(TrashItem {
                        id: row.get(0),
                        content_type: row.get(1),
                        deleted_at: row.get(2),
                        deleted_by: row.get(3),
                        title: row.get(4),
                    });
                    last = Some((row.get::<_, String>(5), row.get::<_, String>(0)));
                }
                let next = if rows.len() > limit as usize {
                    last.map(|(at, id)| (vec![Some(at)], id))
                } else {
                    None
                };
                Ok(Page { items, next })
            })
        })
        .await
    }

    /// One purge transaction: up to [`PURGE_BATCH`] entries trashed longer
    /// than `trash.retention`, hard-deleted with their variants,
    /// revisions, published rows and refs, one `content.entry_purged`
    /// each. Rows another purge holds are skipped. Returns how many.
    pub async fn purge_expired(&self, actor: &Actor) -> Result<usize, Rejection> {
        let work = self.work();
        let retention = self.state().settings().trash.retention;
        let actor = actor.clone();
        transaction(self.database(), self.deadline(), move |tx| {
            Box::pin(async move { work.purge_expired(tx, retention, &actor).await })
        })
        .await
    }
}

/// The `content.trash_purge` handler: purge batches until no expired
/// trash is left, the job is cancelled or its deadline nears. Registered
/// through [`crate::ContentState::guard_jobs`], so an outdated host gives
/// the job back.
pub(crate) struct TrashPurge {
    pub(crate) store: Store,
}
impl JobHandler for TrashPurge {
    fn handle<'a>(
        &'a self,
        ctx: JobContext,
        _payload: Value,
    ) -> BoxFuture<'a, Result<(), JobFailure>> {
        Box::pin(async move {
            let actor = Actor {
                subject: PURGE_SUBJECT.to_owned(),
                kind: "system".to_owned(),
                correlation: Some(ctx.correlation_id.as_str().to_owned()),
            };
            let margin = self.store.state().settings().limits.query_timeout;
            loop {
                if *ctx.cancel.borrow() || tokio::time::Instant::now() + margin >= ctx.deadline {
                    // The next run continues where this one stopped.
                    return Ok(());
                }
                let purged = self
                    .store
                    .purge_expired(&actor)
                    .await
                    .map_err(|r| JobFailure::from(r.failure))?;
                if purged < PURGE_BATCH {
                    return Ok(());
                }
            }
        })
    }
}
