//! The registered statements of assets and blobs (TE-T4): every statement
//! of [`crate::media`] is one of these constants, run through
//! `Database::query` and its siblings. [`STATEMENTS`] lists them for
//! `check_registry`. Statements on `assets`, `blobs` and `refs` filter on
//! `scope`; only [`ASSET_REFERENCED`] counts referrers in every scope.
use merezha_postgres::Statement;

/// The asset columns [`super::AssetView::from_row`] reads, from `assets a`
/// joined with its blob `b`.
macro_rules! asset_columns {
    () => {
        "a.id::text, a.filename, a.content_type, a.size, a.sha256, a.width, a.height, a.title, \
         a.focal_point::text, \
         to_char(a.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), \
         a.created_by, \
         to_char(a.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), \
         to_char(a.deleted_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), \
         b.storage_key, \
         to_char(a.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'), \
         a.scope"
    };
}

/// Claim a blob for an upload: a `pending` row with its lease, or nothing
/// when the row exists. Returns the lease as text, which identifies the
/// claim. No conflict target: the row's primary key and its
/// `storage_key` (a function of it) conflict together, and either one,
/// with an identical upload in flight, must yield nothing rather than a
/// unique violation.
pub(crate) const BLOB_CLAIM: Statement = Statement::new(
    "content.blobs.claim",
    "INSERT merezha_content.blobs",
    "INSERT INTO merezha_content.blobs \
         (scope, sha256, storage_key, size, content_type, state, pending_until, created_at) \
     VALUES ($1, $2, $3, $4, $5, 'pending', now() + $6::bigint * interval '1 millisecond', now()) \
     ON CONFLICT DO NOTHING \
     RETURNING pending_until::text",
);
/// The existing blob row, locked.
pub(crate) const BLOB_LOCK: Statement = Statement::new(
    "content.blobs.lock",
    "SELECT merezha_content.blobs",
    "SELECT state, coalesce(pending_until < now(), false) \
       FROM merezha_content.blobs WHERE scope = $1 AND sha256 = $2 FOR UPDATE",
);
/// An upload takes over a deletion whose lease expired (its worker died):
/// back to `pending` under this upload's lease, which names the claim.
pub(crate) const BLOB_TAKEOVER: Statement = Statement::new(
    "content.blobs.takeover",
    "UPDATE merezha_content.blobs",
    "UPDATE merezha_content.blobs \
        SET state = 'pending', pending_until = now() + $3::bigint * interval '1 millisecond' \
      WHERE scope = $1 AND sha256 = $2 AND state = 'deleting' AND pending_until < now() \
     RETURNING pending_until::text",
);
/// Extend a pending blob's lease for a second upload of the same bytes.
pub(crate) const BLOB_EXTEND: Statement = Statement::new(
    "content.blobs.extend",
    "UPDATE merezha_content.blobs",
    "UPDATE merezha_content.blobs \
        SET pending_until = greatest(pending_until, now() + $3::bigint * interval '1 millisecond') \
      WHERE scope = $1 AND sha256 = $2 AND state = 'pending'",
);
/// The guarded flip to `stored` in the transaction that inserts the asset:
/// from `stored` or `orphaned` (the object exists), or from `pending` when
/// this upload put the object (`$3`). A row being deleted or gone matches
/// nothing.
pub(crate) const BLOB_STORE: Statement = Statement::new(
    "content.blobs.store",
    "UPDATE merezha_content.blobs",
    "UPDATE merezha_content.blobs \
        SET state = 'stored', pending_until = NULL, orphaned_at = NULL \
      WHERE scope = $1 AND sha256 = $2 \
        AND (state IN ('stored', 'orphaned') OR (state = 'pending' AND $3::boolean)) \
     RETURNING 1",
);
/// Give up an upload's own claim: to `deleting` with a delete lease, only
/// while the lease is still the one this upload took. Returns the delete
/// lease, the token that fences the object delete.
pub(crate) const BLOB_RELEASE: Statement = Statement::new(
    "content.blobs.release",
    "UPDATE merezha_content.blobs",
    "UPDATE merezha_content.blobs \
        SET state = 'deleting', pending_until = now() + $4::bigint * interval '1 millisecond' \
      WHERE scope = $1 AND sha256 = $2 AND state = 'pending' \
        AND pending_until = $3::text::timestamptz \
     RETURNING pending_until::text",
);
/// Cleanup's claim (section 6): orphaned blobs past the grace, pending
/// blobs past their lease and deletions a crash abandoned, `FOR UPDATE
/// SKIP LOCKED`, set `deleting` with a delete lease. Returns each with its
/// previous state and lease, to restore after a failed delete, and its
/// delete lease, the token that fences every later step.
pub(crate) const BLOB_CLEANUP_CLAIM: Statement = Statement::new(
    "content.blobs.cleanup_claim",
    "UPDATE merezha_content.blobs",
    "WITH due AS ( \
         SELECT scope, sha256, state, pending_until FROM merezha_content.blobs \
          WHERE (state = 'orphaned' AND orphaned_at < now() - $1::bigint * interval '1 millisecond') \
             OR (state IN ('pending', 'deleting') AND pending_until < now()) \
          ORDER BY created_at LIMIT $2 FOR UPDATE SKIP LOCKED) \
     UPDATE merezha_content.blobs b \
        SET state = 'deleting', pending_until = now() + $3::bigint * interval '1 millisecond' \
       FROM due \
      WHERE b.scope = due.scope AND b.sha256 = due.sha256 \
     RETURNING b.scope, b.sha256, b.storage_key, due.state, due.pending_until::text, \
               b.pending_until::text",
);
/// Re-take a claimed deletion right before its object delete: only while
/// its lease is still the token `$3`, renewed to a fresh lease (the new
/// token). A worker whose lease expired and was claimed again, or whose
/// row went, gets nothing and leaves the object alone.
pub(crate) const BLOB_RETAKE: Statement = Statement::new(
    "content.blobs.retake",
    "UPDATE merezha_content.blobs",
    "UPDATE merezha_content.blobs \
        SET pending_until = now() + $4::bigint * interval '1 millisecond' \
      WHERE scope = $1 AND sha256 = $2 AND state = 'deleting' \
        AND pending_until = $3::text::timestamptz \
     RETURNING pending_until::text",
);
/// Remove a blob row once its object is deleted, fenced on the token.
pub(crate) const BLOB_DELETE: Statement = Statement::new(
    "content.blobs.delete",
    "DELETE merezha_content.blobs",
    "DELETE FROM merezha_content.blobs \
      WHERE scope = $1 AND sha256 = $2 AND state = 'deleting' \
        AND pending_until = $3::text::timestamptz",
);
/// A failed object delete restores the previous state and lease, fenced
/// on the token (`$5`).
pub(crate) const BLOB_RESTORE: Statement = Statement::new(
    "content.blobs.restore",
    "UPDATE merezha_content.blobs",
    "UPDATE merezha_content.blobs SET state = $3, pending_until = $4::text::timestamptz \
      WHERE scope = $1 AND sha256 = $2 AND state = 'deleting' \
        AND pending_until = $5::text::timestamptz",
);
/// An upload's give-up whose object delete failed: back to `pending` with
/// a lease already over, fenced on the token, so the next upload of these
/// bytes joins and puts again, and cleanup may take it meanwhile.
pub(crate) const BLOB_ABANDON: Statement = Statement::new(
    "content.blobs.abandon",
    "UPDATE merezha_content.blobs",
    "UPDATE merezha_content.blobs SET state = 'pending', pending_until = now() \
      WHERE scope = $1 AND sha256 = $2 AND state = 'deleting' \
        AND pending_until = $3::text::timestamptz",
);
/// Whether any asset row, trashed ones included, still uses a blob.
pub(crate) const BLOB_IN_USE: Statement = Statement::new(
    "content.blobs.in_use",
    "SELECT merezha_content.assets",
    "SELECT EXISTS (SELECT 1 FROM merezha_content.assets WHERE scope = $1 AND sha256 = $2)",
);
/// A stored blob no asset uses any more.
pub(crate) const BLOB_ORPHAN: Statement = Statement::new(
    "content.blobs.orphan",
    "UPDATE merezha_content.blobs",
    "UPDATE merezha_content.blobs SET state = 'orphaned', orphaned_at = now() \
      WHERE scope = $1 AND sha256 = $2 AND state = 'stored'",
);
/// The new asset row.
pub(crate) const ASSET_INSERT: Statement = Statement::new(
    "content.assets.insert",
    "INSERT merezha_content.assets",
    "INSERT INTO merezha_content.assets \
         (id, scope, sha256, filename, content_type, size, width, height, title, \
          created_at, created_by, updated_at) \
     VALUES ($1::text::uuid, $2, $3, $4, $5, $6, $7, $8, $9, now(), $10, now())",
);
/// One asset of the scope, in any state.
pub(crate) const ASSET_GET: Statement = Statement::new(
    "content.assets.get",
    "SELECT merezha_content.assets",
    concat!(
        "SELECT ",
        asset_columns!(),
        " FROM merezha_content.assets a \
           JOIN merezha_content.blobs b ON b.scope = a.scope AND b.sha256 = a.sha256 \
          WHERE a.id = $1::text::uuid AND a.scope = $2"
    ),
);
/// One asset of the scope, locked `FOR UPDATE` (deletes, patches): a save
/// referencing it holds `FOR KEY SHARE` until it commits.
pub(crate) const ASSET_LOCK: Statement = Statement::new(
    "content.assets.lock",
    "SELECT merezha_content.assets",
    concat!(
        "SELECT ",
        asset_columns!(),
        " FROM merezha_content.assets a \
           JOIN merezha_content.blobs b ON b.scope = a.scope AND b.sha256 = a.sha256 \
          WHERE a.id = $1::text::uuid AND a.scope = $2 FOR UPDATE OF a"
    ),
);
/// Live assets of the scope, newest first, filtered by exact type, type
/// prefix (`image/`) and filename prefix, after a keyset cursor.
pub(crate) const ASSET_LIST: Statement = Statement::new(
    "content.assets.list",
    "SELECT merezha_content.assets",
    concat!(
        "SELECT ",
        asset_columns!(),
        " FROM merezha_content.assets a \
           JOIN merezha_content.blobs b ON b.scope = a.scope AND b.sha256 = a.sha256 \
          WHERE a.scope = $1 AND a.deleted_at IS NULL \
            AND ($2::text IS NULL OR a.content_type = $2) \
            AND ($3::text IS NULL OR starts_with(a.content_type, $3)) \
            AND ($4::text IS NULL OR starts_with(a.filename, $4)) \
            AND ($5::text IS NULL OR a.created_at < $5::text::timestamptz \
                 OR (a.created_at = $5::text::timestamptz AND a.id > $6::text::uuid)) \
          ORDER BY a.created_at DESC, a.id \
          LIMIT $7"
    ),
);
/// Draft and published refs to an asset recorded in its scope.
pub(crate) const ASSET_USAGE: Statement = Statement::new(
    "content.assets.usage",
    "SELECT merezha_content.refs",
    "SELECT count(*) FILTER (WHERE stage = 'draft'), count(*) FILTER (WHERE stage = 'published') \
       FROM merezha_content.refs \
      WHERE target_kind = 'asset' AND target_id = $1::text::uuid AND scope = $2",
);
/// Whether anything in any scope references an asset (`asset_in_use`),
/// without returning the referrers.
pub(crate) const ASSET_REFERENCED: Statement = Statement::new(
    "content.assets.referenced",
    "SELECT merezha_content.refs",
    "SELECT EXISTS (SELECT 1 FROM merezha_content.refs \
                     WHERE target_kind = 'asset' AND target_id = $1::text::uuid)",
);
/// The metadata patch of a live asset: `$5` and `$6` say whether the
/// patch sets `title` and `focal_point`.
pub(crate) const ASSET_PATCH: Statement = Statement::new(
    "content.assets.patch",
    "UPDATE merezha_content.assets",
    "UPDATE merezha_content.assets \
        SET title = CASE WHEN $5::boolean THEN $3 ELSE title END, \
            focal_point = CASE WHEN $6::boolean THEN $4::text::jsonb ELSE focal_point END, \
            updated_at = now() \
      WHERE id = $1::text::uuid AND scope = $2 AND deleted_at IS NULL",
);
/// Move a live asset to the trash.
pub(crate) const ASSET_TRASH: Statement = Statement::new(
    "content.assets.trash",
    "UPDATE merezha_content.assets",
    "UPDATE merezha_content.assets SET deleted_at = now(), updated_at = now() \
      WHERE id = $1::text::uuid AND scope = $2 AND deleted_at IS NULL",
);
/// Bring a trashed asset back.
pub(crate) const ASSET_UNDELETE: Statement = Statement::new(
    "content.assets.undelete",
    "UPDATE merezha_content.assets",
    "UPDATE merezha_content.assets SET deleted_at = NULL, updated_at = now() \
      WHERE id = $1::text::uuid AND scope = $2 AND deleted_at IS NOT NULL",
);
/// Hard-delete one asset; returns its blob.
pub(crate) const ASSET_PURGE: Statement = Statement::new(
    "content.assets.purge",
    "DELETE merezha_content.assets",
    "DELETE FROM merezha_content.assets WHERE id = $1::text::uuid AND scope = $2 \
     RETURNING sha256",
);
/// Hard-delete up to `$2` assets trashed longer than `$1` ms, skipping rows
/// another purge holds; returns each one's scope and blob.
pub(crate) const ASSET_PURGE_EXPIRED: Statement = Statement::new(
    "content.assets.purge_expired",
    "DELETE merezha_content.assets",
    "DELETE FROM merezha_content.assets WHERE id IN ( \
         SELECT id FROM merezha_content.assets \
          WHERE deleted_at < now() - $1::bigint * interval '1 millisecond' \
          ORDER BY deleted_at LIMIT $2 FOR UPDATE SKIP LOCKED) \
     RETURNING scope, sha256",
);
/// The types of live entries whose published variants reference an asset
/// in its scope: delivery serves it to a caller who may read one of them.
pub(crate) const ASSET_PUBLISHED_TYPES: Statement = Statement::new(
    "content.assets.published_types",
    "SELECT merezha_content.refs",
    "SELECT DISTINCT e.content_type \
       FROM merezha_content.refs r \
       JOIN merezha_content.entries e ON e.id = r.from_entry AND e.scope = r.scope \
      WHERE r.target_kind = 'asset' AND r.target_id = $1::text::uuid AND r.stage = 'published' \
        AND r.scope = $2 AND e.deleted_at IS NULL \
      ORDER BY 1",
);
/// The scope's generation, bumped by every change delivery sees.
pub(crate) const GENERATION_BUMP: Statement = Statement::new(
    "content.assets.generation_bump",
    "INSERT merezha_content.generations",
    "INSERT INTO merezha_content.generations (scope, generation) VALUES ($1, 1) \
     ON CONFLICT (scope) DO UPDATE SET generation = merezha_content.generations.generation + 1",
);

/// Every statement of assets and blobs, for `check_registry`.
pub const STATEMENTS: &[Statement] = &[
    BLOB_CLAIM,
    BLOB_LOCK,
    BLOB_EXTEND,
    BLOB_STORE,
    BLOB_TAKEOVER,
    BLOB_RELEASE,
    BLOB_CLEANUP_CLAIM,
    BLOB_RETAKE,
    BLOB_DELETE,
    BLOB_RESTORE,
    BLOB_ABANDON,
    BLOB_IN_USE,
    BLOB_ORPHAN,
    ASSET_INSERT,
    ASSET_GET,
    ASSET_LOCK,
    ASSET_LIST,
    ASSET_USAGE,
    ASSET_REFERENCED,
    ASSET_PATCH,
    ASSET_TRASH,
    ASSET_UNDELETE,
    ASSET_PURGE,
    ASSET_PURGE_EXPIRED,
    ASSET_PUBLISHED_TYPES,
    GENERATION_BUMP,
];
