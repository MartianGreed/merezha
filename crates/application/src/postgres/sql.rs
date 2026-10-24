//! Every statement the PostgreSQL store runs. Names follow TE's statement
//! naming (`app.<table>.<operation>`); the `Statement` registry of TE-T4
//! takes them over.

/// The registered name of the append statement (TE §1.3, AP §A.5).
pub const APPEND_NAME: &str = "app.events.append";

/// One command commit, run as a single autocommit statement (§A.4).
pub const APPEND: &str = "SELECT outcome, r_version, r_position, r_events FROM merezha_app.append(\
    $1, $2, $3, $4, $5, $6::text[]::json[], $7::text::json, $8, $9, $10, $11, $12, $13, $14, $15, $16)";

/// The stored receipt for a key, outside any write: `merezha_app.lookup_receipt`
/// plus the receipt's stream.
pub const RECEIPT: &str = "SELECT CASE WHEN r.fingerprint = sha256(convert_to($5, 'UTF8')) \
        THEN 'replayed' ELSE 'mismatch' END, r.version, r.position, r.events, r.stream \
    FROM merezha_app.receipts r \
    WHERE r.tag = $1 AND r.tenant = $2 AND r.actor = $3 AND r.key = $4";

const EVENT_COLUMNS: &str = "position, stream, version, tag, schema_version, payload::text, metadata::text, accepted_unix_ms";

/// A stream's history in version order, one row over the history limit.
pub fn load() -> String {
    format!(
        "SELECT {EVENT_COLUMNS} FROM merezha_app.events WHERE stream = $1 ORDER BY version LIMIT $2"
    )
}
/// The feed after a position.
pub fn read_all() -> String {
    format!(
        "SELECT {EVENT_COLUMNS} FROM merezha_app.events WHERE position > $1 ORDER BY position LIMIT $2"
    )
}

pub const HEAD: &str = "SELECT position FROM merezha_app.head WHERE id = 1";

pub const PROJECTION: &str = "SELECT state::text, checkpoint, row_revision \
    FROM merezha_app.projections WHERE name = $1 AND revision = $2";

/// Catch-up commit with row-revision CAS (§A.7). Zero rows: the row moved
/// on, or the checkpoint is beyond the head.
pub const COMMIT_PROJECTION: &str = "UPDATE merezha_app.projections \
    SET state = $3::text::json, checkpoint = $4, row_revision = row_revision + 1, \
        status = 'active', failure_code = NULL, failure_class = NULL, failures = 0, updated_at = now() \
    WHERE name = $1 AND revision = $2 AND row_revision = $5 \
      AND $4 <= (SELECT position FROM merezha_app.head WHERE id = 1) \
    RETURNING row_revision";

/// First start on an empty schema records the identity; later starts
/// re-read and compare it (§A.6).
pub const INSERT_MANIFEST: &str = "INSERT INTO merezha_app.manifest (id, application, format) \
    VALUES (1, $1, 1) ON CONFLICT (id) DO NOTHING";
pub const MANIFEST: &str = "SELECT application, format FROM merezha_app.manifest WHERE id = 1";

/// A configured projection revision without a row starts at checkpoint 0
/// with `warm_position` at the current head (AP-A3 warms it).
pub const INSERT_PROJECTION: &str = "INSERT INTO merezha_app.projections \
    (name, revision, partitioned, state, warm_position) \
    SELECT $1, $2, false, $3::text::json, h.position FROM merezha_app.head h WHERE h.id = 1 \
    ON CONFLICT (name, revision) DO NOTHING";
pub const PROJECTION_PARTITIONED: &str =
    "SELECT partitioned FROM merezha_app.projections WHERE name = $1 AND revision = $2";
