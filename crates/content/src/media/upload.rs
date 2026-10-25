//! The upload flow (section 6): the body streams into a spool file while
//! it is counted and hashed, the bytes are sniffed against the declared
//! type, raster dimensions read, the blob claimed in its own transaction,
//! put from the spool file, and the asset inserted with its event in the
//! transaction that flips the blob to `stored`.
//!
//! Exactly one of an upload and the cleanup job wins a blob: cleanup
//! deletes an object only after committing `deleting`, and the final
//! transaction flips the locked row to `stored` only from `stored`,
//! `orphaned`, or `pending` after its own put. A row that moved meanwhile
//! sends the upload back to the claim with the same spool file.
use super::{ASSET_UPLOADED, AssetView, Assets, app_target, blob_key, hex, sniff, sql, sql_error};
use crate::store::{Actor, Rejection, new_id, transaction};
use axum::body::Body;
use futures_util::StreamExt;
use merezha_domain::Failure;
use merezha_platform::ports::{BlobAttributes, BlobBody, BlobKey, BlobStore};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{path::Path, time::Duration};
use tokio::io::AsyncWriteExt;

/// How long a claimed `pending` blob is leased to its upload; cleanup
/// takes it (and its object) once the lease expired, as after a crash
/// between the claim and the final transaction.
pub const UPLOAD_LEASE: Duration = Duration::from_secs(3600);
/// Longest wait for the next bytes of an upload body.
pub const UPLOAD_IDLE: Duration = Duration::from_secs(30);
/// How long a claim waits for a blob another worker is deleting.
pub const CLAIM_RETRY: Duration = Duration::from_secs(2);
/// The put must end this long before its lease does, so the final
/// transaction runs while the claim is still its own.
const PUT_MARGIN: Duration = Duration::from_secs(120);
/// Claims of one upload whose blob moved under it before it gives up.
const CLAIM_ATTEMPTS: usize = 3;
/// Startup removes spool files older than this, which no live upload
/// holds (an upload's whole body must arrive within its lease).
pub const SPOOL_MAX_AGE: Duration = Duration::from_secs(3600);
/// Spool file names start with this; startup clears old ones.
pub(crate) const SPOOL_PREFIX: &str = "merezha-upload-";

/// A validated upload request.
#[derive(Clone, Debug)]
pub(crate) struct Upload {
    /// The declared type, in `allowed_types`.
    pub(crate) content_type: String,
    /// The stored filename.
    pub(crate) filename: String,
    pub(crate) title: Option<String>,
}

/// How an upload met its blob row.
#[derive(Debug, PartialEq, Eq)]
enum Claim {
    /// It inserted the `pending` row; the lease names the claim.
    Inserted(String),
    /// Another upload of the same bytes holds it `pending`: both put.
    Joined,
    /// `stored` or `orphaned`: the object exists, no put.
    Exists,
}

fn too_large() -> Rejection {
    Rejection::new(Failure::permanent("asset_too_large"))
}
fn rejected_type() -> Rejection {
    Rejection::new(Failure::permanent("media_type_rejected"))
}
fn spool_unavailable() -> Rejection {
    Rejection::new(Failure::transient("spool_unavailable"))
}
fn unavailable() -> Rejection {
    Rejection::new(Failure::transient(merezha_blob::BACKEND_UNAVAILABLE))
}

/// The spooled body: its file (removed when this drops, on every exit
/// path), size, SHA-256 and the head the signature check reads.
struct Spooled {
    path: tempfile::TempPath,
    size: u64,
    sha256: String,
    head: Vec<u8>,
    utf8: bool,
}

impl Assets {
    /// Store one upload (section 6, steps 3 to 8) and return the asset.
    /// The caller checked the permission, the admission slot, the declared
    /// length and type and the filename.
    pub(crate) async fn upload(
        &self,
        upload: Upload,
        body: Body,
        actor: &Actor,
    ) -> Result<AssetView, Rejection> {
        let spooled = self.spool(body, &upload.content_type).await?;
        if !sniff::matches(&upload.content_type, &spooled.head, spooled.utf8) {
            return Err(rejected_type());
        }
        let dimensions = sniff::dimensions(&spooled.path, &upload.content_type)
            .await
            .map_err(|()| rejected_type())?;
        let scope = app_target().scope;
        let key = blob_key(&scope, &spooled.sha256)?;
        let disposition = if self.settings().inline(&upload.content_type) {
            "inline"
        } else {
            // Shared by every asset of these bytes: no filename.
            "attachment"
        };
        let attrs = BlobAttributes {
            content_type: upload.content_type.clone(),
            content_disposition: disposition.to_owned(),
            cache_control: None,
        };
        for _ in 0..CLAIM_ATTEMPTS {
            let claim = self
                .claim(&scope, &spooled, &key, &upload.content_type)
                .await?;
            let put = !matches!(claim, Claim::Exists);
            if put && let Err(rejection) = self.put(&key, &spooled, &attrs).await {
                self.give_up(&scope, &spooled.sha256, &key, &claim).await;
                return Err(rejection);
            }
            match self
                .finish(&scope, &spooled, &upload, dimensions, put, actor)
                .await
            {
                Ok(Some(asset)) => return Ok(asset),
                // Cleanup took the row meanwhile: claim again.
                Ok(None) => continue,
                Err(rejection) => {
                    self.give_up(&scope, &spooled.sha256, &key, &claim).await;
                    return Err(rejection);
                }
            }
        }
        Err(unavailable())
    }

    /// Stream `body` into a fresh spool file, counting bytes against
    /// `max_upload_bytes`, hashing them and keeping the first
    /// [`sniff::SNIFF_BYTES`]. Text types are checked as UTF-8 on the way.
    async fn spool(&self, body: Body, content_type: &str) -> Result<Spooled, Rejection> {
        let settings = self.settings();
        let max = settings.max_upload_bytes;
        let dir = settings.spool_dir.clone();
        let file = merezha_telemetry::spawn_blocking_in_span(move || {
            tempfile::Builder::new()
                .prefix(SPOOL_PREFIX)
                .tempfile_in(dir)
        })
        .await
        .map_err(|_| spool_unavailable())?
        .map_err(|_| spool_unavailable())?;
        let (file, path) = file.into_parts();
        let mut file = tokio::fs::File::from_std(file);
        let text = sniff::TEXT_TYPES.contains(&content_type);
        let mut utf8 = sniff::Utf8Check::default();
        let mut hasher = Sha256::new();
        let mut head = Vec::with_capacity(sniff::SNIFF_BYTES);
        let mut size = 0u64;
        let deadline = tokio::time::Instant::now() + UPLOAD_LEASE;
        let mut stream = body.into_data_stream();
        loop {
            let wait = deadline.min(tokio::time::Instant::now() + UPLOAD_IDLE);
            let next = tokio::time::timeout_at(wait, stream.next())
                .await
                .map_err(|_| Rejection::new(Failure::permanent("upload_incomplete")))?;
            let Some(chunk) = next else { break };
            let chunk = chunk.map_err(|error| {
                if merezha_platform::edge::is_body_limit(&error) {
                    too_large()
                } else {
                    Rejection::new(Failure::permanent("upload_incomplete"))
                }
            })?;
            size += chunk.len() as u64;
            if size > max {
                return Err(too_large());
            }
            hasher.update(&chunk);
            if head.len() < sniff::SNIFF_BYTES {
                let take = (sniff::SNIFF_BYTES - head.len()).min(chunk.len());
                head.extend_from_slice(&chunk[..take]);
            }
            if text {
                utf8.feed(&chunk);
            }
            file.write_all(&chunk)
                .await
                .map_err(|_| spool_unavailable())?;
        }
        file.flush().await.map_err(|_| spool_unavailable())?;
        drop(file);
        Ok(Spooled {
            path,
            size,
            sha256: hex(&hasher.finalize()),
            head,
            utf8: text && utf8.valid(),
        })
    }

    /// Step 6: insert the `pending` row with its lease, or meet the
    /// existing one. A row being deleted is waited for up to
    /// [`CLAIM_RETRY`], then `blob_unavailable`.
    async fn claim(
        &self,
        scope: &str,
        spooled: &Spooled,
        key: &BlobKey,
        content_type: &str,
    ) -> Result<Claim, Rejection> {
        let started = std::time::Instant::now();
        let lease = millis(UPLOAD_LEASE);
        let size = i64::try_from(spooled.size).map_err(|_| too_large())?;
        loop {
            let db = self.db().clone();
            let state = self.state().clone();
            let (scope, sha256, key, content_type) = (
                scope.to_owned(),
                spooled.sha256.clone(),
                key.as_str().to_owned(),
                content_type.to_owned(),
            );
            let claim = transaction(self.db(), self.deadline(), move |tx| {
                Box::pin(async move {
                    state.check_writable(&db, tx).await?;
                    if let Some(row) = db
                        .query_opt(
                            &*tx,
                            &sql::BLOB_CLAIM,
                            &[&scope, &sha256, &key, &size, &content_type, &lease],
                        )
                        .await
                        .map_err(|e| sql_error(&e))?
                    {
                        return Ok(Some(Claim::Inserted(row.get(0))));
                    }
                    let Some(row) = db
                        .query_opt(&*tx, &sql::BLOB_LOCK, &[&scope, &sha256])
                        .await
                        .map_err(|e| sql_error(&e))?
                    else {
                        // Deleted since the insert met it: try again.
                        return Ok(None);
                    };
                    let expired: bool = row.get(1);
                    match row.get::<_, &str>(0) {
                        "stored" | "orphaned" => Ok(Some(Claim::Exists)),
                        "pending" => {
                            db.execute(&*tx, &sql::BLOB_EXTEND, &[&scope, &sha256, &lease])
                                .await
                                .map_err(|e| sql_error(&e))?;
                            Ok(Some(Claim::Joined))
                        }
                        // A deletion whose worker died: this upload takes
                        // the row over (its own claim) and puts again.
                        "deleting" if expired => Ok(db
                            .query_opt(&*tx, &sql::BLOB_TAKEOVER, &[&scope, &sha256, &lease])
                            .await
                            .map_err(|e| sql_error(&e))?
                            .map(|row| Claim::Inserted(row.get(0)))),
                        _ => Ok(None),
                    }
                })
            })
            .await?;
            if let Some(claim) = claim {
                return Ok(claim);
            }
            if started.elapsed() >= CLAIM_RETRY {
                return Err(unavailable());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Step 7: put the spool file, bounded to end well within the lease.
    async fn put(
        &self,
        key: &BlobKey,
        spooled: &Spooled,
        attrs: &BlobAttributes,
    ) -> Result<(), Rejection> {
        let body = BlobBody::File {
            path: spooled.path.to_path_buf(),
            len: spooled.size,
        };
        match tokio::time::timeout(UPLOAD_LEASE - PUT_MARGIN, self.blobs.put(key, body, attrs))
            .await
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => Err(Rejection::new(Failure::from(error))),
            Err(_) => Err(unavailable()),
        }
    }

    /// Step 8: flip the blob to `stored` (guarded) and insert the asset
    /// with `content.asset_uploaded`, in one transaction. `None` when the
    /// blob row moved since the claim (cleanup took it).
    async fn finish(
        &self,
        scope: &str,
        spooled: &Spooled,
        upload: &Upload,
        dimensions: Option<(i32, i32)>,
        put: bool,
        actor: &Actor,
    ) -> Result<Option<AssetView>, Rejection> {
        let db = self.db().clone();
        let work = self.store.work();
        let id = new_id(&work.ctx)?.hyphenated().to_string();
        let size = i64::try_from(spooled.size).map_err(|_| too_large())?;
        let (scope, sha256, upload, actor) = (
            scope.to_owned(),
            spooled.sha256.clone(),
            upload.clone(),
            actor.clone(),
        );
        let (width, height) = (dimensions.map(|d| d.0), dimensions.map(|d| d.1));
        transaction(self.db(), self.deadline(), move |tx| {
            Box::pin(async move {
                work.state.check_writable(&db, tx).await?;
                let stored = db
                    .query_opt(&*tx, &sql::BLOB_STORE, &[&scope, &sha256, &put])
                    .await
                    .map_err(|e| sql_error(&e))?;
                if stored.is_none() {
                    return Ok(None);
                }
                db.execute(
                    &*tx,
                    &sql::ASSET_INSERT,
                    &[
                        &id,
                        &scope,
                        &sha256,
                        &upload.filename,
                        &upload.content_type,
                        &size,
                        &width,
                        &height,
                        &upload.title,
                        &actor.subject,
                    ],
                )
                .await
                .map_err(|e| sql_error(&e))?;
                let record = work.event(
                    &app_target(),
                    ASSET_UPLOADED,
                    json!({"scope": scope, "id": id, "content_type": upload.content_type,
                           "size": size, "sha256": sha256, "actor": actor.json()}),
                    &actor,
                )?;
                work.append(tx, vec![record]).await?;
                let row = db
                    .query_one(&*tx, &sql::ASSET_GET, &[&id, &scope])
                    .await
                    .map_err(|e| sql_error(&e))?;
                Ok(Some(AssetView::from_row(&row)))
            })
        })
        .await
    }

    /// After a failed put or final transaction: an upload that inserted its
    /// blob row (or took it over) removes the row and the object while the
    /// lease is still its own, so a refused upload leaves nothing; both
    /// steps are fenced on the delete lease the release returns. When the
    /// object delete fails the row goes back to `pending` with a lease
    /// already over, so the next upload of these bytes joins it and puts
    /// again (and cleanup may take it). A joined claim, a lost race or a
    /// failing database leave the row to cleanup once its lease expires.
    async fn give_up(&self, scope: &str, sha256: &str, key: &BlobKey, claim: &Claim) {
        let Claim::Inserted(lease) = claim else {
            return;
        };
        let Ok(Some(token)) = self
            .give_up_step(scope, sha256, GiveUp::Release, lease)
            .await
        else {
            return;
        };
        let next = if self.delete_object(key.as_str()).await {
            GiveUp::Forget
        } else {
            GiveUp::Abandon
        };
        let _ = self.give_up_step(scope, sha256, next, &token).await;
    }

    /// One fenced step of a give-up in its own transaction, with the model
    /// version check. The release returns the delete lease (the token of
    /// the next step); the others `Some` when they matched the row.
    async fn give_up_step(
        &self,
        scope: &str,
        sha256: &str,
        step: GiveUp,
        token: &str,
    ) -> Result<Option<String>, Rejection> {
        let db = self.db().clone();
        let state = self.state().clone();
        let delete_lease = millis(super::DELETE_LEASE);
        let (scope, sha256, token) = (scope.to_owned(), sha256.to_owned(), token.to_owned());
        transaction(self.db(), self.deadline(), move |tx| {
            Box::pin(async move {
                state.check_writable(&db, tx).await?;
                let changed = match step {
                    GiveUp::Release => {
                        return Ok(db
                            .query_opt(
                                &*tx,
                                &sql::BLOB_RELEASE,
                                &[&scope, &sha256, &token, &delete_lease],
                            )
                            .await
                            .map_err(|e| sql_error(&e))?
                            .map(|row| row.get(0)));
                    }
                    GiveUp::Forget => {
                        db.execute(&*tx, &sql::BLOB_DELETE, &[&scope, &sha256, &token])
                            .await
                    }
                    GiveUp::Abandon => {
                        db.execute(&*tx, &sql::BLOB_ABANDON, &[&scope, &sha256, &token])
                            .await
                    }
                }
                .map_err(|e| sql_error(&e))?;
                Ok((changed > 0).then_some(token))
            })
        })
        .await
    }
}

/// The steps of a give-up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GiveUp {
    /// The own `pending` claim to `deleting`.
    Release,
    /// The row, once its object is gone.
    Forget,
    /// Back to `pending`, its lease over, after a failed object delete.
    Abandon,
}

pub(crate) fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

/// Remove spool files older than `age` from `dir` (startup, section 6).
/// Only files this host names [`SPOOL_PREFIX`] are touched. Returns how
/// many went.
pub(crate) fn clear_spool(dir: &Path, age: Duration) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut cleared = 0;
    for entry in entries.flatten().take(100_000) {
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with(SPOOL_PREFIX) {
            continue;
        }
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        let old = metadata
            .modified()
            .ok()
            .and_then(|m| m.elapsed().ok())
            .is_some_and(|elapsed| elapsed >= age);
        if metadata.is_file() && old && std::fs::remove_file(entry.path()).is_ok() {
            cleared += 1;
        }
    }
    cleared
}
