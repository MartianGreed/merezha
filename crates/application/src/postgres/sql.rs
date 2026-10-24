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

/// The served pair, its row revision and the position the revision is
/// warm from (§A.7).
pub const PROJECTION: &str = "SELECT state::text, checkpoint, row_revision, warm_position \
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
/// with `warm_position` at the current head, and warms up in the
/// background. The `NOT EXISTS` read keeps a starting host from waiting on
/// a run's open commit of an existing row, which `ON CONFLICT` alone
/// would.
pub const INSERT_PROJECTION: &str = "INSERT INTO merezha_app.projections \
    (name, revision, partitioned, state, warm_position) \
    SELECT $1, $2, false, $3::text::json, h.position FROM merezha_app.head h WHERE h.id = 1 \
       AND NOT EXISTS (SELECT 1 FROM merezha_app.projections p WHERE p.name = $1 AND p.revision = $2) \
    ON CONFLICT (name, revision) DO NOTHING";
pub const PROJECTION_PARTITIONED: &str =
    "SELECT partitioned FROM merezha_app.projections WHERE name = $1 AND revision = $2";

/// The run lock classid, `'MZAP'` (§A.7). Operators find holders in
/// `pg_locks` with `classid = 1297760592`.
pub const LOCK_CLASS: i32 = 1_297_760_592;
/// A session advisory lock on a detached session; `$1` is the run key
/// (`p:{name}:{revision}` or `b:{name}:{revision}`).
pub const TRY_LOCK: &str = "SELECT pg_try_advisory_lock(1297760592, hashtext($1))";
/// Names the detached session `merezha {key}` for `pg_stat_activity`.
pub const NAME_SESSION: &str = "SELECT set_config('application_name', $1, false)";

/// Failure bookkeeping of a background live run; the served pair and its
/// row revision are untouched.
pub const RECORD_FAILURE: &str = "UPDATE merezha_app.projections \
    SET status = 'failing', failure_code = $3, failure_class = $4, failures = failures + 1, \
        updated_at = now() \
    WHERE name = $1 AND revision = $2";

/// A new rebuild: the next generation, the configured initial candidate at
/// checkpoint 0 and the head as its target. Nothing when one exists.
pub const INSERT_REBUILD: &str = "INSERT INTO merezha_app.projection_rebuilds \
        (name, revision, generation, state, checkpoint, target, requested_by) \
    SELECT p.name, p.revision, p.generation + 1, $3::text::json, 0, h.position, $4 \
      FROM merezha_app.projections p, merezha_app.head h \
     WHERE p.name = $1 AND p.revision = $2 AND h.id = 1 \
    ON CONFLICT (name, revision) DO NOTHING RETURNING target";
/// Resume: a failed (or still running) rebuild runs again from its
/// persisted candidate.
pub const RESUME_REBUILD: &str = "UPDATE merezha_app.projection_rebuilds \
    SET status = 'running', failure_code = NULL \
    WHERE name = $1 AND revision = $2 RETURNING target";
pub const DELETE_REBUILD: &str =
    "DELETE FROM merezha_app.projection_rebuilds WHERE name = $1 AND revision = $2";
pub const REBUILD: &str = "SELECT state::text, checkpoint, target, row_revision, status \
    FROM merezha_app.projection_rebuilds WHERE name = $1 AND revision = $2";
pub const REBUILD_STATUS: &str =
    "SELECT status FROM merezha_app.projection_rebuilds WHERE name = $1 AND revision = $2";
/// Persist the candidate after a batch, with CAS on the rebuild row. Zero
/// rows: the rebuild was abandoned, failed or resumed elsewhere.
pub const COMMIT_CANDIDATE: &str = "UPDATE merezha_app.projection_rebuilds \
    SET state = $3::text::json, checkpoint = $4, row_revision = row_revision + 1 \
    WHERE name = $1 AND revision = $2 AND row_revision = $5 AND status = 'running' \
    RETURNING row_revision";
pub const FAIL_REBUILD: &str = "UPDATE merezha_app.projection_rebuilds \
    SET status = 'failed', failure_code = $4 \
    WHERE name = $1 AND revision = $2 AND row_revision = $3 AND status = 'running'";
/// Swap step 1: the served row, locked so a live batch orders against it.
pub const LOCK_PROJECTION: &str = "SELECT row_revision FROM merezha_app.projections \
    WHERE name = $1 AND revision = $2 FOR UPDATE";
/// Swap step 2: take the finished candidate, only if it is still the one
/// this run built.
pub const TAKE_CANDIDATE: &str = "DELETE FROM merezha_app.projection_rebuilds \
    WHERE name = $1 AND revision = $2 AND row_revision = $3 AND status = 'running' \
      AND checkpoint >= target \
    RETURNING state::text, checkpoint, generation";
/// Swap step 3: publish the candidate as the next generation. The
/// checkpoint may move back to the rebuild target.
pub const SWAP: &str = "UPDATE merezha_app.projections \
    SET state = $3::text::json, checkpoint = $4, generation = $5, row_revision = row_revision + 1, \
        status = 'active', failure_code = NULL, failure_class = NULL, failures = 0, updated_at = now() \
    WHERE name = $1 AND revision = $2 RETURNING row_revision";

/// Every revision row of one projection name.
pub const REVISIONS: &str = "SELECT revision, checkpoint, warm_position \
    FROM merezha_app.projections WHERE name = $1 ORDER BY revision";
pub const DELETE_REVISION: &str =
    "DELETE FROM merezha_app.projections WHERE name = $1 AND revision = $2";
/// The admin document of configured revisions (`$1`, `$2` zipped), with
/// their rebuilds and the accepted time of the first unprocessed event.
pub const OVERVIEW: &str = "SELECT p.name, p.revision, p.partitioned, p.checkpoint, p.warm_position, \
        p.status, p.failure_code, \
        to_char(p.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), \
        r.checkpoint, r.target, r.status, e.accepted_unix_ms \
    FROM unnest($1::text[], $2::int[]) AS c (name, revision) \
    JOIN merezha_app.projections p ON (p.name, p.revision) = (c.name, c.revision) \
    LEFT JOIN merezha_app.projection_rebuilds r ON (r.name, r.revision) = (p.name, p.revision) \
    LEFT JOIN merezha_app.events e ON e.position = p.checkpoint + 1 \
    ORDER BY p.name";
