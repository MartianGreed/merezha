//! Assets (CT-C7, section 6): uploads streamed through a spool file with
//! sniffing, hashing and dimensions, content-addressed blobs deduplicated
//! per scope, the asset management routes, proxy and redirect delivery,
//! deletion with `force` and `purge`, the asset trash purge and the
//! `content.blob_cleanup` job.
//!
//! Every statement here is a registered [`STATEMENTS`] constant. Every
//! write transaction starts with [`ContentState::check_writable`]; asset,
//! blob and ref statements filter on `scope` (only `asset_in_use` counts
//! referrers in every scope). Management works in the scope its path
//! names (`/manage/assets` or `/o/{organization}/manage/assets`); delivery serves an
//! organization's assets under `/v1/content/o/{organization}` (C9), each
//! scope with its own `org-<hash>` key tag ([`blob_key`]), so identical
//! bytes in two organizations are two blobs and two objects.
mod cleanup;
mod http;
mod serve;
mod settings;
mod sniff;
mod sql;
mod upload;

pub use cleanup::{
    BLOB_CLEANUP, CLEANUP_BATCH, CLEANUP_LIMIT, ClaimedBlob, CleanupReport, DELETE_LEASE, Swept,
};
pub use settings::{
    DEFAULT_ALLOWED_TYPES, DEFAULT_ASSET_CACHE_CONTROL, DEFAULT_CLEANUP_CRON, DEFAULT_INLINE_TYPES,
    DEFAULT_MAX_UPLOAD_BYTES, MediaSettings, Serve,
};
pub use sniff::{MAX_FILENAME_BYTES, MAX_IMAGE_SIDE, SNIFF_BYTES, TEXT_TYPES};
pub use sql::STATEMENTS;
pub use upload::{CLAIM_RETRY, SPOOL_MAX_AGE, UPLOAD_IDLE, UPLOAD_LEASE};

pub(crate) use http::{body_limits, router};
pub(crate) use upload::clear_spool;

/// The `content.blob_cleanup` handler over `assets`.
pub(crate) fn cleanup_handler(assets: Assets) -> cleanup::BlobCleanup {
    cleanup::BlobCleanup { assets }
}

use crate::{
    module::ContentState,
    store::{Actor, Rejection, Store, Target, transaction},
};
use merezha_blob::ObjectBlobStore;
use merezha_domain::{Classification, Failure, Identity};
use merezha_platform::{
    AppContext, Database, ErrorCode, Tx,
    ports::{BlobKey, SourceOutbox},
};
use merezha_postgres::{classify, tokio_postgres::Row};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fmt::Write as _,
    sync::{Arc, Mutex, OnceLock},
    time::Instant,
};
use tokio::sync::Semaphore;

/// Permission to list assets and read any asset file through management.
pub const ASSETS_READ: &str = "content.assets.read";
/// Permission to upload assets and edit their metadata.
pub const ASSETS_WRITE: &str = "content.assets.write";
/// Permission to delete, undelete and purge assets, `force` included.
pub const ASSETS_DELETE: &str = "content.assets.delete";
/// The event an upload appends.
pub const ASSET_UPLOADED: &str = "content.asset_uploaded";
/// The event a metadata patch appends.
pub const ASSET_UPDATED: &str = "content.asset_updated";
/// The event a delete (soft or purge) appends.
pub const ASSET_DELETED: &str = "content.asset_deleted";
/// Assets per asset trash purge transaction.
pub const ASSET_PURGE_BATCH: i64 = 100;

/// The asset codes of section 5 (R8), registered by the content module
/// with `merezha_blob::ERRORS`. `overloaded`, `body_limit`,
/// `unsupported_media_type` and `blob_not_found` are the kernel's.
pub const ERRORS: &[ErrorCode] = &[
    ErrorCode::new("asset_not_found", 404),
    ErrorCode::new("asset_in_use", 409),
    ErrorCode::new("asset_too_large", 413),
    ErrorCode::new("media_type_rejected", 415),
    ErrorCode::new("upload_incomplete", 400),
    ErrorCode::new("spool_unavailable", 503),
    ErrorCode::new("media_config_invalid", 422),
];

pub(crate) const ASSET_NOT_FOUND: &str = "asset_not_found";

/// The media runtime of a host with `[modules.content.media]`: its
/// settings, the blob store (opened once, at startup), the upload slots
/// and the asset service the routes and jobs share.
pub struct Media {
    settings: MediaSettings,
    store: Mutex<Option<Arc<ObjectBlobStore>>>,
    uploads: Arc<Semaphore>,
    assets: OnceLock<Assets>,
}
impl std::fmt::Debug for Media {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Media")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}
impl Media {
    pub(crate) fn new(settings: MediaSettings) -> Self {
        let slots = settings.max_concurrent_uploads as usize;
        Self {
            settings,
            store: Mutex::new(None),
            uploads: Arc::new(Semaphore::new(slots)),
            assets: OnceLock::new(),
        }
    }
    pub fn settings(&self) -> &MediaSettings {
        &self.settings
    }
    /// The blob store, opened on first use: the local root is created
    /// 0700 if missing and refused when open to group or others; nothing
    /// contacts a bucket. Puts are capped at `max_upload_bytes`.
    pub(crate) fn open(&self) -> Result<Arc<ObjectBlobStore>, Failure> {
        let mut slot = self
            .store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(store) = slot.as_ref() {
            return Ok(store.clone());
        }
        let store = Arc::new(
            ObjectBlobStore::open(&self.settings.driver)?
                .with_max_bytes(self.settings.max_upload_bytes),
        );
        *slot = Some(store.clone());
        Ok(store)
    }
    /// The opened store, `None` before startup opened it.
    pub fn store(&self) -> Option<Arc<ObjectBlobStore>> {
        self.store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    /// The asset service, once the module started.
    pub fn assets(&self) -> Option<&Assets> {
        self.assets.get()
    }
    pub(crate) fn set_assets(&self, assets: Assets) {
        let _ = self.assets.set(assets);
    }
}

/// The storage key of a blob: `content/<scope tag>/sha256/<2>/<2>/<hex>`,
/// where the tag is `app` or `org-` plus 32 hex characters of the
/// organization id's SHA-256, so ids stay out of bucket listings. Built
/// from the scope and the hash the host computed, never from client
/// input.
pub fn blob_key(scope: &str, sha256: &str) -> Result<BlobKey, Failure> {
    let tag = match scope.strip_prefix("org:") {
        None if scope == "app" => "app".to_owned(),
        Some(org) => format!("org-{}", &hex(&Sha256::digest(org.as_bytes()))[..32]),
        None => return Err(Failure::permanent("invalid_input")),
    };
    if sha256.len() != 64 || !sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Failure::permanent("invalid_input"));
    }
    BlobKey::new(format!(
        "content/{tag}/sha256/{}/{}/{sha256}",
        &sha256[..2],
        &sha256[2..4]
    ))
    .map_err(Failure::from)
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::with_capacity(64), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

/// One asset row with its blob's storage key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssetView {
    pub id: String,
    pub filename: String,
    pub content_type: String,
    pub size: i64,
    pub sha256: String,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub title: Option<String>,
    /// `{"x": .., "y": ..}` as stored.
    pub focal_point: Option<String>,
    pub created_at: String,
    pub created_by: String,
    pub updated_at: String,
    pub deleted_at: Option<String>,
    pub storage_key: String,
    /// `created_at` with microseconds, the list cursor's key.
    pub(crate) cursor_at: String,
    /// `app` or `org:<organization id>` (C9).
    pub scope: String,
}
impl AssetView {
    fn from_row(row: &Row) -> Self {
        Self {
            id: row.get(0),
            filename: row.get(1),
            content_type: row.get(2),
            size: row.get(3),
            sha256: row.get(4),
            width: row.get(5),
            height: row.get(6),
            title: row.get(7),
            focal_point: row.get(8),
            created_at: row.get(9),
            created_by: row.get(10),
            updated_at: row.get(11),
            deleted_at: row.get(12),
            storage_key: row.get(13),
            cursor_at: row.get(14),
            scope: row.get(15),
        }
    }
    /// The delivery path of the asset's bytes: under
    /// `/v1/content/o/{organization}` for an organization's asset.
    pub fn url(&self) -> String {
        match self.scope.strip_prefix("org:") {
            Some(org) => format!("/v1/content/o/{org}/assets/{}/{}", self.id, self.filename),
            None => format!("/v1/content/assets/{}/{}", self.id, self.filename),
        }
    }
    /// The management path of the asset: under
    /// `/v1/content/o/{organization}` for an organization's asset.
    pub fn manage_url(&self) -> String {
        match self.scope.strip_prefix("org:") {
            Some(org) => format!("/v1/content/o/{org}/manage/assets/{}", self.id),
            None => format!("/v1/content/manage/assets/{}", self.id),
        }
    }
    /// The asset JSON of section 6, in its documented order, with `usage`
    /// when given.
    pub fn json(&self, usage: Option<Usage>) -> String {
        use crate::json::{optional, string};
        let number = |n: Option<i32>| n.map_or_else(|| "null".to_owned(), |n| n.to_string());
        let mut out = format!(
            "{{\"id\":{},\"filename\":{},\"content_type\":{},\"size\":{},\"sha256\":{},\
             \"width\":{},\"height\":{},\"title\":{},\"focal_point\":{},\"created_at\":{},\
             \"created_by\":{},\"updated_at\":{},\"deleted_at\":{},\"url\":{}",
            string(&self.id),
            string(&self.filename),
            string(&self.content_type),
            self.size,
            string(&self.sha256),
            number(self.width),
            number(self.height),
            optional(self.title.as_deref()),
            self.focal_point.as_deref().unwrap_or("null"),
            string(&self.created_at),
            string(&self.created_by),
            string(&self.updated_at),
            optional(self.deleted_at.as_deref()),
            string(&self.url()),
        );
        if let Some(Usage {
            draft,
            published,
            elsewhere,
        }) = usage
        {
            let _ = write!(
                out,
                ",\"usage\":{{\"draft\":{draft},\"published\":{published},\
                 \"elsewhere\":{elsewhere}}}"
            );
        }
        out.push('}');
        out
    }
}

/// The refs pointing at an asset (`GET /manage/assets/{id}`): `draft` and
/// `published` from entries of the asset's own scope, and `elsewhere`
/// from every other scope, both stages (organization entries using an
/// application asset; an organization's asset has none). A delete without
/// `force` is refused (`asset_in_use`) while any of the three is not 0;
/// the referrers of other scopes are counted, never named.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Usage {
    pub draft: i64,
    pub published: i64,
    pub elsewhere: i64,
}

/// The filters of `GET /assets`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AssetFilter {
    /// An exact type.
    pub content_type: Option<String>,
    /// A type prefix such as `image/`, from `content_type=image/*`.
    pub type_prefix: Option<String>,
    pub filename_prefix: Option<String>,
}

/// What a delete does.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AssetDelete {
    /// Delete although refs point at the asset; they stay in place.
    pub force: bool,
    /// Hard-delete at once, trashed or not.
    pub purge: bool,
}

/// The asset service behind the routes and the jobs.
#[derive(Clone)]
pub struct Assets {
    pub(crate) store: Store,
    pub(crate) media: Arc<Media>,
    pub(crate) blobs: Arc<ObjectBlobStore>,
}
impl std::fmt::Debug for Assets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Assets").finish_non_exhaustive()
    }
}

pub(crate) fn sql_error(error: &merezha_postgres::tokio_postgres::Error) -> Rejection {
    Rejection::new(classify(error))
}

pub(crate) fn not_found() -> Rejection {
    Rejection::new(Failure::permanent(ASSET_NOT_FOUND))
}

/// The scope a management request's path names (section 8, C9): an
/// organization's (`org:<id>`, its events' tenant), or the application's.
pub(crate) fn asset_target(tenant: Option<Identity>) -> Target {
    match tenant {
        Some(tenant) => Target {
            content_type: String::new(),
            scope: format!("org:{}", tenant.as_str()),
            tenant: Some(tenant),
        },
        None => Target {
            content_type: String::new(),
            scope: "app".to_owned(),
            tenant: None,
        },
    }
}

impl Assets {
    pub(crate) fn new(
        state: Arc<ContentState>,
        ctx: AppContext,
        outbox: SourceOutbox,
        media: Arc<Media>,
        blobs: Arc<ObjectBlobStore>,
    ) -> Self {
        Self {
            store: Store::new(state, ctx, outbox),
            media,
            blobs,
        }
    }
    pub fn settings(&self) -> &MediaSettings {
        &self.media.settings
    }
    pub(crate) fn state(&self) -> &Arc<ContentState> {
        self.store.state()
    }
    pub(crate) fn db(&self) -> &Database {
        self.store.database()
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.store.deadline()
    }

    /// One asset of `scope` in any state (management reads), with its
    /// [`Usage`].
    pub async fn get(&self, scope: &str, id: &str) -> Result<(AssetView, Usage), Rejection> {
        let db = self.db().clone();
        let (scope, id) = (scope.to_owned(), id.to_owned());
        let client = db.acquire(self.deadline()).await?;
        let row = db
            .query_opt(&client, &sql::ASSET_GET, &[&id, &scope])
            .await
            .map_err(|e| sql_error(&e))?
            .ok_or_else(not_found)?;
        let usage = db
            .query_one(&client, &sql::ASSET_USAGE, &[&id, &scope])
            .await
            .map_err(|e| sql_error(&e))?;
        let usage = Usage {
            draft: usage.get(0),
            published: usage.get(1),
            elsewhere: usage.get(2),
        };
        Ok((AssetView::from_row(&row), usage))
    }

    /// A live asset of `scope`, `None` when absent or trashed.
    pub(crate) async fn live(&self, scope: &str, id: &str) -> Result<Option<AssetView>, Rejection> {
        let db = self.db().clone();
        let client = db.acquire(self.deadline()).await?;
        let row = db
            .query_opt(&client, &sql::ASSET_GET, &[&id, &scope])
            .await
            .map_err(|e| sql_error(&e))?;
        Ok(row
            .map(|row| AssetView::from_row(&row))
            .filter(|asset| asset.deleted_at.is_none()))
    }

    /// Live assets of `scope`, newest first, after `after` (`created_at`
    /// with microseconds, id); one row more than `limit` tells whether a
    /// next page exists.
    pub async fn list(
        &self,
        scope: &str,
        filter: &AssetFilter,
        after: Option<(String, String)>,
        limit: u32,
    ) -> Result<(Vec<AssetView>, bool), Rejection> {
        let db = self.db().clone();
        let client = db.acquire(self.deadline()).await?;
        let (at, id) = match after {
            Some((at, id)) => (Some(at), Some(id)),
            None => (None, None),
        };
        let fetch = i64::from(limit) + 1;
        let rows = db
            .query(
                &client,
                &sql::ASSET_LIST,
                &[
                    &scope,
                    &filter.content_type,
                    &filter.type_prefix,
                    &filter.filename_prefix,
                    &at,
                    &id,
                    &fetch,
                ],
            )
            .await
            .map_err(|e| {
                // A cursor time PostgreSQL refuses is the cursor's fault.
                if e.code().is_some_and(|c| c.code().starts_with("22")) {
                    Rejection {
                        failure: Failure::permanent("invalid_cursor"),
                        details: vec![crate::query::detail("/cursor", "invalid_value")],
                    }
                } else {
                    sql_error(&e)
                }
            })?;
        let more = rows.len() > limit as usize;
        Ok((
            rows.iter()
                .take(limit as usize)
                .map(AssetView::from_row)
                .collect(),
            more,
        ))
    }

    /// Merge-patch `title` and `focal_point` of a live asset of `target`'s
    /// scope (`None` keeps a member, `Some(None)` clears it), bump the
    /// scope's generation and append `content.asset_updated` with the
    /// target's organization as its tenant.
    pub async fn patch(
        &self,
        target: &Target,
        id: &str,
        title: Option<Option<String>>,
        focal_point: Option<Option<String>>,
        actor: &Actor,
    ) -> Result<AssetView, Rejection> {
        let db = self.db().clone();
        let work = self.store.work();
        let (target, id, actor) = (target.clone(), id.to_owned(), actor.clone());
        let scope = target.scope.clone();
        transaction(self.db(), self.deadline(), move |tx| {
            Box::pin(async move {
                work.state.check_writable(&db, tx).await?;
                lock_live(&db, tx, &scope, &id).await?;
                let (set_title, set_focal) = (title.is_some(), focal_point.is_some());
                let (title, focal_point) = (title.flatten(), focal_point.flatten());
                db.execute(
                    &*tx,
                    &sql::ASSET_PATCH,
                    &[&id, &scope, &title, &focal_point, &set_title, &set_focal],
                )
                .await
                .map_err(|e| sql_error(&e))?;
                bump_generation(&db, tx, &scope).await?;
                let record = work.event(
                    &target,
                    ASSET_UPDATED,
                    json!({"scope": scope, "id": id, "actor": actor.json()}),
                    &actor,
                )?;
                work.append(tx, vec![record]).await?;
                let row = db
                    .query_one(&*tx, &sql::ASSET_GET, &[&id, &scope])
                    .await
                    .map_err(|e| sql_error(&e))?;
                Ok(AssetView::from_row(&row))
            })
        })
        .await
    }

    /// Delete an asset of `target`'s scope (section 6): `asset_in_use`
    /// while refs in any scope point at it, unless `force`; soft by
    /// default, at once with `purge` (trashed assets included). Appends
    /// `content.asset_deleted`, with the target's organization as its
    /// tenant, and bumps the generation when a live asset goes; a purge
    /// marks the blob `orphaned` when no other asset uses it.
    pub async fn delete(
        &self,
        target: &Target,
        id: &str,
        options: AssetDelete,
        actor: &Actor,
    ) -> Result<(), Rejection> {
        let db = self.db().clone();
        let work = self.store.work();
        let (target, id, actor) = (target.clone(), id.to_owned(), actor.clone());
        let scope = target.scope.clone();
        transaction(self.db(), self.deadline(), move |tx| {
            Box::pin(async move {
                work.state.check_writable(&db, tx).await?;
                let row = db
                    .query_opt(&*tx, &sql::ASSET_LOCK, &[&id, &scope])
                    .await
                    .map_err(|e| sql_error(&e))?
                    .ok_or_else(not_found)?;
                let asset = AssetView::from_row(&row);
                let live = asset.deleted_at.is_none();
                if !live && !options.purge {
                    return Err(not_found());
                }
                if !options.force {
                    let referenced: bool = db
                        .query_one(&*tx, &sql::ASSET_REFERENCED, &[&id])
                        .await
                        .map_err(|e| sql_error(&e))?
                        .get(0);
                    if referenced {
                        return Err(Rejection::code("asset_in_use", Classification::Conflict));
                    }
                }
                if options.purge {
                    db.execute(&*tx, &sql::ASSET_PURGE, &[&id, &scope])
                        .await
                        .map_err(|e| sql_error(&e))?;
                    orphan_if_unused(&db, tx, &scope, &asset.sha256).await?;
                } else {
                    db.execute(&*tx, &sql::ASSET_TRASH, &[&id, &scope])
                        .await
                        .map_err(|e| sql_error(&e))?;
                }
                if live {
                    bump_generation(&db, tx, &scope).await?;
                    let record = work.event(
                        &target,
                        ASSET_DELETED,
                        json!({"scope": scope, "id": id, "actor": actor.json(),
                               "forced": options.force}),
                        &actor,
                    )?;
                    work.append(tx, vec![record]).await?;
                }
                Ok(())
            })
        })
        .await
    }

    /// Bring a trashed asset back; a live one answers as it is. Delivery
    /// may serve it again, so the generation is bumped.
    pub async fn undelete(&self, scope: &str, id: &str) -> Result<AssetView, Rejection> {
        let db = self.db().clone();
        let work = self.store.work();
        let (scope, id) = (scope.to_owned(), id.to_owned());
        transaction(self.db(), self.deadline(), move |tx| {
            Box::pin(async move {
                work.state.check_writable(&db, tx).await?;
                db.query_opt(&*tx, &sql::ASSET_LOCK, &[&id, &scope])
                    .await
                    .map_err(|e| sql_error(&e))?
                    .ok_or_else(not_found)?;
                let changed = db
                    .execute(&*tx, &sql::ASSET_UNDELETE, &[&id, &scope])
                    .await
                    .map_err(|e| sql_error(&e))?;
                if changed > 0 {
                    bump_generation(&db, tx, &scope).await?;
                }
                let row = db
                    .query_one(&*tx, &sql::ASSET_GET, &[&id, &scope])
                    .await
                    .map_err(|e| sql_error(&e))?;
                Ok(AssetView::from_row(&row))
            })
        })
        .await
    }

    /// One asset trash purge transaction: up to [`ASSET_PURGE_BATCH`]
    /// assets trashed longer than `trash.retention`, each blob no other
    /// asset uses marked `orphaned` in the same transaction. Returns how
    /// many went.
    pub async fn purge_expired(&self) -> Result<usize, Rejection> {
        let db = self.db().clone();
        let work = self.store.work();
        let retention =
            i64::try_from(self.state().settings().trash.retention.as_millis()).unwrap_or(i64::MAX);
        transaction(self.db(), self.deadline(), move |tx| {
            Box::pin(async move {
                work.state.check_writable(&db, tx).await?;
                let rows = db
                    .query(
                        &*tx,
                        &sql::ASSET_PURGE_EXPIRED,
                        &[&retention, &ASSET_PURGE_BATCH],
                    )
                    .await
                    .map_err(|e| sql_error(&e))?;
                let mut blobs: Vec<(String, String)> =
                    rows.iter().map(|row| (row.get(0), row.get(1))).collect();
                blobs.sort();
                blobs.dedup();
                for (scope, sha256) in &blobs {
                    orphan_if_unused(&db, tx, scope, sha256).await?;
                }
                Ok(rows.len())
            })
        })
        .await
    }
}

/// Lock a live asset `FOR UPDATE`, `asset_not_found` otherwise.
async fn lock_live(db: &Database, tx: &mut Tx<'_>, scope: &str, id: &str) -> Result<(), Rejection> {
    let row = db
        .query_opt(&*tx, &sql::ASSET_LOCK, &[&id, &scope])
        .await
        .map_err(|e| sql_error(&e))?
        .ok_or_else(not_found)?;
    if AssetView::from_row(&row).deleted_at.is_some() {
        return Err(not_found());
    }
    Ok(())
}

/// After an asset row went: lock its blob, then (a new statement, which
/// sees every asset committed while it waited) mark it `orphaned` when no
/// asset row uses it any more. An upload's final transaction locks the
/// same row, so the two serialize and an asset never ends on an orphaned
/// blob.
pub(crate) async fn orphan_if_unused(
    db: &Database,
    tx: &mut Tx<'_>,
    scope: &str,
    sha256: &str,
) -> Result<(), Rejection> {
    let Some(_) = db
        .query_opt(&*tx, &sql::BLOB_LOCK, &[&scope, &sha256])
        .await
        .map_err(|e| sql_error(&e))?
    else {
        return Ok(());
    };
    let used: bool = db
        .query_one(&*tx, &sql::BLOB_IN_USE, &[&scope, &sha256])
        .await
        .map_err(|e| sql_error(&e))?
        .get(0);
    if !used {
        db.execute(&*tx, &sql::BLOB_ORPHAN, &[&scope, &sha256])
            .await
            .map_err(|e| sql_error(&e))?;
    }
    Ok(())
}

pub(crate) async fn bump_generation(
    db: &Database,
    tx: &mut Tx<'_>,
    scope: &str,
) -> Result<(), Rejection> {
    db.execute(&*tx, &sql::GENERATION_BUMP, &[&scope])
        .await
        .map_err(|e| sql_error(&e))?;
    Ok(())
}

/// The `{"x", "y"}` focal point of a patch: both in 0 to 1.
pub(crate) fn focal_point(value: &Value) -> Option<String> {
    let object = value.as_object()?;
    if object.len() != 2 {
        return None;
    }
    let coordinate = |name: &str| {
        object
            .get(name)
            .and_then(Value::as_f64)
            .filter(|v| (0.0..=1.0).contains(v))
    };
    Some(json!({"x": coordinate("x")?, "y": coordinate("y")?}).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_keys_are_content_addressed_per_scope() {
        let sha = "9f2c".repeat(16);
        assert_eq!(
            blob_key("app", &sha).unwrap().as_str(),
            format!("content/app/sha256/9f/2c/{sha}")
        );
        let org = blob_key("org:acme", &sha).unwrap();
        let tag = &hex(&Sha256::digest(b"acme"))[..32];
        assert_eq!(
            org.as_str(),
            format!("content/org-{tag}/sha256/9f/2c/{sha}")
        );
        assert!(!org.as_str().contains("acme"));
        for (scope, sha) in [
            ("app", "xyz"),
            ("other", sha.as_str()),
            ("app", &"G".repeat(64)),
        ] {
            assert!(blob_key(scope, sha).is_err(), "{scope} {sha}");
        }
    }

    #[test]
    fn focal_points_are_two_coordinates_in_the_unit_square() {
        assert_eq!(
            focal_point(&json!({"x": 0.5, "y": 0.35})).as_deref(),
            Some(r#"{"x":0.5,"y":0.35}"#)
        );
        for bad in [
            json!({"x": 1.5, "y": 0}),
            json!({"x": 0.5}),
            json!({"x": 0.5, "y": 0.5, "z": 1}),
            json!({"x": "0.5", "y": 0.5}),
            json!([0.5, 0.5]),
        ] {
            assert_eq!(focal_point(&bad), None, "{bad}");
        }
    }

    #[test]
    fn the_statements_are_registered_beside_the_kernel() {
        assert_eq!(
            merezha_postgres::check_registry(&[
                merezha_postgres::KERNEL_STATEMENTS,
                crate::STATEMENTS,
                STATEMENTS,
            ]),
            Ok(())
        );
    }
}
