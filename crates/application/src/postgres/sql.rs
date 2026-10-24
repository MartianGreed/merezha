//! Every statement the PostgreSQL store runs, registered with
//! `merezha-postgres` (TE §1.3, T4): names are `app.<table>.<operation>`,
//! [`STATEMENTS`] lists them all.
use merezha_postgres::Statement;

/// One command commit, run as a single autocommit statement (§A.4). TE's
/// `db.client.operation.duration` reports it as `app.events.append`.
pub const APPEND: Statement = Statement::new(
    "app.events.append",
    "INSERT merezha_app.events",
    "SELECT outcome, r_version, r_position, r_events FROM merezha_app.append(\
    $1, $2, $3, $4, $5, $6::text[]::json[], $7::text::json, $8, $9, $10, $11, $12, $13, $14, $15, $16)",
);
/// The append's session bound, pipelined before it; the milliseconds are
/// computed from the deadline.
pub const APPEND_TIMEOUT: Statement = Statement::new(
    "app.session.timeout",
    "SET",
    "SET statement_timeout = <milliseconds>",
);
/// The session default again, pipelined after the append.
pub const RESET_TIMEOUT: Statement =
    Statement::new("app.session.reset", "RESET", "RESET statement_timeout");

/// The stored receipt for a key, outside any write: `merezha_app.lookup_receipt`
/// plus the receipt's stream.
pub const RECEIPT: Statement = Statement::new(
    "app.receipts.lookup",
    "SELECT merezha_app.receipts",
    "SELECT CASE WHEN r.fingerprint = sha256(convert_to($5, 'UTF8')) \
        THEN 'replayed' ELSE 'mismatch' END, r.version, r.position, r.events, r.stream \
    FROM merezha_app.receipts r \
    WHERE r.tag = $1 AND r.tenant = $2 AND r.actor = $3 AND r.key = $4",
);

macro_rules! event_columns {
    () => {
        "position, stream, version, tag, schema_version, payload::text, metadata::text, accepted_unix_ms"
    };
}

/// A stream's history in version order, one row over the history limit.
pub const LOAD: Statement = Statement::new(
    "app.events.load",
    "SELECT merezha_app.events",
    concat!(
        "SELECT ",
        event_columns!(),
        " FROM merezha_app.events WHERE stream = $1 ORDER BY version LIMIT $2"
    ),
);
/// The feed after a position, with each event's stored trace context
/// (host-only, for span links; never part of a `StoredEvent`).
pub const READ_ALL: Statement = Statement::new(
    "app.events.read_all",
    "SELECT merezha_app.events",
    concat!(
        "SELECT ",
        event_columns!(),
        ", trace_parent, trace_state FROM merezha_app.events WHERE position > $1 ORDER BY position LIMIT $2"
    ),
);

pub const HEAD: Statement = Statement::new(
    "app.head.read",
    "SELECT merezha_app.head",
    "SELECT position FROM merezha_app.head WHERE id = 1",
);

/// The served pair, its row revision and the position the revision is
/// warm from (§A.7).
pub const PROJECTION: Statement = Statement::new(
    "app.projections.read",
    "SELECT merezha_app.projections",
    "SELECT state::text, checkpoint, row_revision, warm_position \
    FROM merezha_app.projections WHERE name = $1 AND revision = $2",
);

/// Catch-up commit with row-revision CAS (§A.7). Zero rows: the row moved
/// on, or the checkpoint is beyond the head.
pub const COMMIT_PROJECTION: Statement = Statement::new(
    "app.projections.commit",
    "UPDATE merezha_app.projections",
    "UPDATE merezha_app.projections \
    SET state = $3::text::json, checkpoint = $4, row_revision = row_revision + 1, \
        status = 'active', failure_code = NULL, failure_class = NULL, failures = 0, updated_at = now() \
    WHERE name = $1 AND revision = $2 AND row_revision = $5 \
      AND $4 <= (SELECT position FROM merezha_app.head WHERE id = 1) \
    RETURNING row_revision",
);

/// First start on an empty schema records the identity; later starts
/// re-read and compare it (§A.6).
pub const INSERT_MANIFEST: Statement = Statement::new(
    "app.manifest.insert",
    "INSERT merezha_app.manifest",
    "INSERT INTO merezha_app.manifest (id, application, format) \
    VALUES (1, $1, 1) ON CONFLICT (id) DO NOTHING",
);
pub const MANIFEST: Statement = Statement::new(
    "app.manifest.read",
    "SELECT merezha_app.manifest",
    "SELECT application, format FROM merezha_app.manifest WHERE id = 1",
);

/// A configured projection revision without a row starts at checkpoint 0
/// with `warm_position` at the current head, and warms up in the
/// background. The `NOT EXISTS` read keeps a starting host from waiting on
/// a run's open commit of an existing row, which `ON CONFLICT` alone
/// would.
pub const INSERT_PROJECTION: Statement = Statement::new(
    "app.projections.insert",
    "INSERT merezha_app.projections",
    "INSERT INTO merezha_app.projections \
    (name, revision, partitioned, state, warm_position) \
    SELECT $1, $2, false, $3::text::json, h.position FROM merezha_app.head h WHERE h.id = 1 \
       AND NOT EXISTS (SELECT 1 FROM merezha_app.projections p WHERE p.name = $1 AND p.revision = $2) \
    ON CONFLICT (name, revision) DO NOTHING",
);
pub const PROJECTION_PARTITIONED: Statement = Statement::new(
    "app.projections.partitioned",
    "SELECT merezha_app.projections",
    "SELECT partitioned FROM merezha_app.projections WHERE name = $1 AND revision = $2",
);

/// The run lock classid, `'MZAP'` (§A.7). Operators find holders in
/// `pg_locks` with `classid = 1297760592`.
pub const LOCK_CLASS: i32 = 1_297_760_592;
/// A session advisory lock on a detached session; `$1` is the run key
/// (`p:{name}:{revision}` or `b:{name}:{revision}`).
pub const TRY_LOCK: Statement = Statement::new(
    "app.run.try_lock",
    "SELECT pg_try_advisory_lock()",
    "SELECT pg_try_advisory_lock(1297760592, hashtext($1))",
);
/// Names the detached session `merezha {key}` for `pg_stat_activity`.
pub const NAME_SESSION: Statement = Statement::new(
    "app.run.name_session",
    "SELECT set_config()",
    "SELECT set_config('application_name', $1, false)",
);

/// Failure bookkeeping of a background live run; the served pair and its
/// row revision are untouched.
pub const RECORD_FAILURE: Statement = Statement::new(
    "app.projections.record_failure",
    "UPDATE merezha_app.projections",
    "UPDATE merezha_app.projections \
    SET status = 'failing', failure_code = $3, failure_class = $4, failures = failures + 1, \
        updated_at = now() \
    WHERE name = $1 AND revision = $2",
);

/// A new rebuild: the next generation, the configured initial candidate at
/// checkpoint 0 and the head as its target. Nothing when one exists.
pub const INSERT_REBUILD: Statement = Statement::new(
    "app.rebuilds.insert",
    "INSERT merezha_app.projection_rebuilds",
    "INSERT INTO merezha_app.projection_rebuilds \
        (name, revision, generation, state, checkpoint, target, requested_by) \
    SELECT p.name, p.revision, p.generation + 1, $3::text::json, 0, h.position, $4 \
      FROM merezha_app.projections p, merezha_app.head h \
     WHERE p.name = $1 AND p.revision = $2 AND h.id = 1 \
    ON CONFLICT (name, revision) DO NOTHING RETURNING target",
);
/// Resume: a failed (or still running) rebuild runs again from its
/// persisted candidate.
pub const RESUME_REBUILD: Statement = Statement::new(
    "app.rebuilds.resume",
    "UPDATE merezha_app.projection_rebuilds",
    "UPDATE merezha_app.projection_rebuilds \
    SET status = 'running', failure_code = NULL \
    WHERE name = $1 AND revision = $2 RETURNING target",
);
pub const DELETE_REBUILD: Statement = Statement::new(
    "app.rebuilds.delete",
    "DELETE merezha_app.projection_rebuilds",
    "DELETE FROM merezha_app.projection_rebuilds WHERE name = $1 AND revision = $2",
);
pub const REBUILD: Statement = Statement::new(
    "app.rebuilds.read",
    "SELECT merezha_app.projection_rebuilds",
    "SELECT state::text, checkpoint, target, row_revision, status \
    FROM merezha_app.projection_rebuilds WHERE name = $1 AND revision = $2",
);
pub const REBUILD_STATUS: Statement = Statement::new(
    "app.rebuilds.status",
    "SELECT merezha_app.projection_rebuilds",
    "SELECT status FROM merezha_app.projection_rebuilds WHERE name = $1 AND revision = $2",
);
/// Persist the candidate after a batch, with CAS on the rebuild row. Zero
/// rows: the rebuild was abandoned, failed or resumed elsewhere.
pub const COMMIT_CANDIDATE: Statement = Statement::new(
    "app.rebuilds.commit",
    "UPDATE merezha_app.projection_rebuilds",
    "UPDATE merezha_app.projection_rebuilds \
    SET state = $3::text::json, checkpoint = $4, row_revision = row_revision + 1 \
    WHERE name = $1 AND revision = $2 AND row_revision = $5 AND status = 'running' \
    RETURNING row_revision",
);
pub const FAIL_REBUILD: Statement = Statement::new(
    "app.rebuilds.fail",
    "UPDATE merezha_app.projection_rebuilds",
    "UPDATE merezha_app.projection_rebuilds \
    SET status = 'failed', failure_code = $4 \
    WHERE name = $1 AND revision = $2 AND row_revision = $3 AND status = 'running'",
);
/// Swap step 1: the served row, locked so a live batch orders against it.
pub const LOCK_PROJECTION: Statement = Statement::new(
    "app.projections.lock",
    "SELECT merezha_app.projections",
    "SELECT row_revision FROM merezha_app.projections \
    WHERE name = $1 AND revision = $2 FOR UPDATE",
);
/// Swap step 2: take the finished candidate, only if it is still the one
/// this run built.
pub const TAKE_CANDIDATE: Statement = Statement::new(
    "app.rebuilds.take",
    "DELETE merezha_app.projection_rebuilds",
    "DELETE FROM merezha_app.projection_rebuilds \
    WHERE name = $1 AND revision = $2 AND row_revision = $3 AND status = 'running' \
      AND checkpoint >= target \
    RETURNING state::text, checkpoint, generation",
);
/// Swap step 3: publish the candidate as the next generation. The
/// checkpoint may move back to the rebuild target.
pub const SWAP: Statement = Statement::new(
    "app.projections.swap",
    "UPDATE merezha_app.projections",
    "UPDATE merezha_app.projections \
    SET state = $3::text::json, checkpoint = $4, generation = $5, row_revision = row_revision + 1, \
        status = 'active', failure_code = NULL, failure_class = NULL, failures = 0, updated_at = now() \
    WHERE name = $1 AND revision = $2 RETURNING row_revision",
);

/// Every revision row of one projection name.
pub const REVISIONS: Statement = Statement::new(
    "app.projections.revisions",
    "SELECT merezha_app.projections",
    "SELECT revision, checkpoint, warm_position \
    FROM merezha_app.projections WHERE name = $1 ORDER BY revision",
);
pub const DELETE_REVISION: Statement = Statement::new(
    "app.projections.delete",
    "DELETE merezha_app.projections",
    "DELETE FROM merezha_app.projections WHERE name = $1 AND revision = $2",
);
/// The admin document of configured revisions (`$1`, `$2` zipped), with
/// their rebuilds and the accepted time of the first unprocessed event.
pub const OVERVIEW: Statement = Statement::new(
    "app.projections.overview",
    "SELECT merezha_app.projections",
    "SELECT p.name, p.revision, p.partitioned, p.checkpoint, p.warm_position, \
        p.status, p.failure_code, \
        to_char(p.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), \
        r.checkpoint, r.target, r.status, e.accepted_unix_ms \
    FROM unnest($1::text[], $2::int[]) AS c (name, revision) \
    JOIN merezha_app.projections p ON (p.name, p.revision) = (c.name, c.revision) \
    LEFT JOIN merezha_app.projection_rebuilds r ON (r.name, r.revision) = (p.name, p.revision) \
    LEFT JOIN merezha_app.events e ON e.position = p.checkpoint + 1 \
    ORDER BY p.name",
);

/// `SET TRANSACTION` of the admin overview's snapshot.
pub const ADMIN_SNAPSHOT: Statement = Statement::new(
    "app.admin.snapshot",
    "SET",
    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
);
/// The test hook delaying a catch-up commit (`hooks().commit_delay`); the
/// seconds are computed.
pub const COMMIT_DELAY: Statement = Statement::new(
    "app.test.commit_delay",
    "SELECT pg_sleep()",
    "SELECT pg_sleep(<seconds>)",
);

/// Every statement the store runs, the verification's included.
pub const STATEMENTS: &[Statement] = &[
    APPEND,
    APPEND_TIMEOUT,
    RESET_TIMEOUT,
    RECEIPT,
    LOAD,
    READ_ALL,
    HEAD,
    PROJECTION,
    COMMIT_PROJECTION,
    INSERT_MANIFEST,
    MANIFEST,
    INSERT_PROJECTION,
    PROJECTION_PARTITIONED,
    TRY_LOCK,
    NAME_SESSION,
    RECORD_FAILURE,
    INSERT_REBUILD,
    RESUME_REBUILD,
    DELETE_REBUILD,
    REBUILD,
    REBUILD_STATUS,
    COMMIT_CANDIDATE,
    FAIL_REBUILD,
    LOCK_PROJECTION,
    TAKE_CANDIDATE,
    SWAP,
    REVISIONS,
    DELETE_REVISION,
    OVERVIEW,
    ADMIN_SNAPSHOT,
    COMMIT_DELAY,
    super::verify::SNAPSHOT,
    super::verify::POSITIONS,
    super::verify::GAP_COUNT,
    super::verify::GAPS,
    super::verify::STREAMS,
    super::verify::RECEIPTS,
    super::verify::CHECKPOINTS,
];

#[cfg(test)]
mod tests {
    use super::STATEMENTS;

    #[test]
    fn the_registry_is_unique_beside_the_kernel() {
        assert_eq!(
            merezha_postgres::check_registry(&[merezha_postgres::KERNEL_STATEMENTS, STATEMENTS]),
            Ok(())
        );
    }
    /// No source of the PostgreSQL store hands SQL to the driver: every
    /// file of `src/postgres/` is scanned except the crash-cut tests.
    #[test]
    fn store_sources_hand_no_sql_to_the_driver() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/postgres");
        let found = merezha_postgres::statement::scan_sources(&dir, &["tests.rs"]).unwrap();
        assert!(found.is_empty(), "unregistered SQL: {found:?}");
    }
}
