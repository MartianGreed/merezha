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
/// (host-only, for span links; never part of a `StoredEvent`) and its
/// stream's tenant (partitioned projections, AP-C2).
pub const READ_ALL: Statement = Statement::new(
    "app.events.read_all",
    "SELECT merezha_app.events",
    concat!(
        "SELECT ",
        event_columns!(),
        ", trace_parent, trace_state, tenant FROM merezha_app.events WHERE position > $1 ORDER BY position LIMIT $2"
    ),
);

pub const HEAD: Statement = Statement::new(
    "app.head.read",
    "SELECT merezha_app.head",
    "SELECT position FROM merezha_app.head WHERE id = 1",
);

/// The served pair (no state when partitioned), its row revision, the
/// position the revision is warm from (§A.7), its generation and whether
/// it is partitioned.
pub const PROJECTION: Statement = Statement::new(
    "app.projections.read",
    "SELECT merezha_app.projections",
    "SELECT state::text, checkpoint, row_revision, warm_position, generation, partitioned \
    FROM merezha_app.projections WHERE name = $1 AND revision = $2",
);
/// A tenant's partition at the served generation, in one statement with
/// the row, so no rebuild swap falls between the two reads (§A.6). No
/// partition row: the configured initial state.
pub const PARTITION: Statement = Statement::new(
    "app.partitions.read",
    "SELECT merezha_app.projection_partitions",
    "SELECT p.checkpoint, p.warm_position, pp.state::text, p.partitioned \
    FROM merezha_app.projections p LEFT JOIN merezha_app.projection_partitions pp \
      ON (pp.name, pp.revision, pp.generation, pp.tenant) = (p.name, p.revision, p.generation, $3) \
    WHERE p.name = $1 AND p.revision = $2",
);
/// The partitions of the tenants one batch touches, at one generation.
pub const LOAD_PARTITIONS: Statement = Statement::new(
    "app.partitions.load",
    "SELECT merezha_app.projection_partitions",
    "SELECT tenant, state::text FROM merezha_app.projection_partitions \
    WHERE name = $1 AND revision = $2 AND generation = $3 AND tenant = ANY($4::text[])",
);
/// The partitioned catch-up commit, CAS first (§A.7): the checkpoint with
/// row-revision CAS at the generation the batch loaded. Zero rows: the
/// batch writes no partition.
pub const COMMIT_PARTITIONED: Statement = Statement::new(
    "app.projections.commit_partitioned",
    "UPDATE merezha_app.projections",
    "UPDATE merezha_app.projections \
    SET checkpoint = $3, row_revision = row_revision + 1, \
        status = 'active', failure_code = NULL, failure_class = NULL, failures = 0, updated_at = now() \
    WHERE name = $1 AND revision = $2 AND row_revision = $4 AND generation = $5 AND partitioned \
      AND $3 <= (SELECT position FROM merezha_app.head WHERE id = 1) \
    RETURNING row_revision",
);
/// The partitions one batch produced (`$4` tenants and `$5` states
/// zipped), at one generation, after its CAS.
pub const WRITE_PARTITIONS: Statement = Statement::new(
    "app.partitions.write",
    "INSERT merezha_app.projection_partitions",
    "INSERT INTO merezha_app.projection_partitions (name, revision, generation, tenant, state) \
    SELECT $1, $2, $3, u.tenant, u.state::json FROM unnest($4::text[], $5::text[]) AS u (tenant, state) \
    ON CONFLICT (name, revision, generation, tenant) DO UPDATE SET state = EXCLUDED.state",
);
/// After a swap: up to 1000 partitions of generations before the served
/// one (§A.7 step 3). Queries join on the served generation, so none reads
/// a half-deleted one.
pub const SWEEP_OLD_PARTITIONS: Statement = Statement::new(
    "app.partitions.sweep_old",
    "DELETE merezha_app.projection_partitions",
    "DELETE FROM merezha_app.projection_partitions \
    WHERE (name, revision, generation, tenant) IN ( \
        SELECT name, revision, generation, tenant FROM merezha_app.projection_partitions \
        WHERE name = $1 AND revision = $2 AND generation < $3 LIMIT 1000)",
);
/// Before a rebuild's first batch: up to 1000 partitions of any generation
/// other than the served one, left by an abandoned rebuild or an
/// interrupted sweep.
pub const SWEEP_STALE_PARTITIONS: Statement = Statement::new(
    "app.partitions.sweep_stale",
    "DELETE merezha_app.projection_partitions",
    "DELETE FROM merezha_app.projection_partitions \
    WHERE (name, revision, generation, tenant) IN ( \
        SELECT name, revision, generation, tenant FROM merezha_app.projection_partitions \
        WHERE name = $1 AND revision = $2 AND generation <> $3 LIMIT 1000)",
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
/// background; a partitioned one (`$4`) has no state (`$3` NULL). The `NOT
/// EXISTS` read keeps a starting host from waiting on a run's open commit
/// of an existing row, which `ON CONFLICT` alone would.
pub const INSERT_PROJECTION: Statement = Statement::new(
    "app.projections.insert",
    "INSERT merezha_app.projections",
    "INSERT INTO merezha_app.projections \
    (name, revision, partitioned, state, warm_position) \
    SELECT $1, $2, $4, $3::text::json, h.position FROM merezha_app.head h WHERE h.id = 1 \
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
/// (`p:{name}:{revision}`, `b:{name}:{revision}` or `r:{reactor}`).
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
    "SELECT state::text, checkpoint, target, row_revision, status, generation, candidate \
    FROM merezha_app.projection_rebuilds WHERE name = $1 AND revision = $2",
);
pub const REBUILD_STATUS: Statement = Statement::new(
    "app.rebuilds.status",
    "SELECT merezha_app.projection_rebuilds",
    "SELECT status FROM merezha_app.projection_rebuilds WHERE name = $1 AND revision = $2",
);
/// Persist the candidate after a batch, with CAS on the rebuild row and
/// its candidate id (`$6`). Zero rows: the rebuild was abandoned (and
/// perhaps requested again), failed or resumed elsewhere.
pub const COMMIT_CANDIDATE: Statement = Statement::new(
    "app.rebuilds.commit",
    "UPDATE merezha_app.projection_rebuilds",
    "UPDATE merezha_app.projection_rebuilds \
    SET state = $3::text::json, checkpoint = $4, row_revision = row_revision + 1 \
    WHERE name = $1 AND revision = $2 AND row_revision = $5 AND candidate = $6 AND status = 'running' \
    RETURNING row_revision",
);
pub const FAIL_REBUILD: Statement = Statement::new(
    "app.rebuilds.fail",
    "UPDATE merezha_app.projection_rebuilds",
    "UPDATE merezha_app.projection_rebuilds \
    SET status = 'failed', failure_code = $4 \
    WHERE name = $1 AND revision = $2 AND row_revision = $3 AND candidate = $5 AND status = 'running'",
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
    WHERE name = $1 AND revision = $2 AND row_revision = $3 AND candidate = $4 AND status = 'running' \
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

/// A configured reactor without a row starts at the head (`$2` false) or
/// at the beginning (`$2` true), once: a later start keeps the row, even
/// after the reactor left the configuration. The `NOT EXISTS` read keeps a
/// starting host from waiting on a run's open checkpoint transaction.
pub const INSERT_REACTOR: Statement = Statement::new(
    "app.reactors.insert",
    "INSERT merezha_app.reactors",
    "INSERT INTO merezha_app.reactors (name, checkpoint, start_position) \
    SELECT $1, p.position, p.position \
      FROM (SELECT CASE WHEN $2 THEN 0 ELSE h.position END AS position \
              FROM merezha_app.head h WHERE h.id = 1) p \
     WHERE NOT EXISTS (SELECT 1 FROM merezha_app.reactors r WHERE r.name = $1) \
    ON CONFLICT (name) DO NOTHING",
);
/// A reactor's row, with the milliseconds until `retry_at` (0 when due or
/// unset).
pub const REACTOR: Statement = Statement::new(
    "app.reactors.read",
    "SELECT merezha_app.reactors",
    "SELECT checkpoint, row_revision, status, halted_position, failures, \
        greatest(0, ceil(extract(epoch FROM coalesce(retry_at, now()) - now()) * 1000))::bigint \
    FROM merezha_app.reactors WHERE name = $1",
);
/// Checkpoint transaction step 1 (AP §C.5): the row, locked, so the batch
/// orders against admin skips and pauses and other runs.
pub const LOCK_REACTOR: Statement = Statement::new(
    "app.reactors.lock",
    "SELECT merezha_app.reactors",
    "SELECT checkpoint, row_revision, status FROM merezha_app.reactors WHERE name = $1 FOR UPDATE",
);
/// The batch's rejected effects (`$2` positions, `$3` indexes, `$4`
/// effect types, `$5` codes zipped), inside its checkpoint transaction.
pub const INSERT_REJECTIONS: Statement = Statement::new(
    "app.reactor_rejections.insert",
    "INSERT merezha_app.reactor_rejections",
    "INSERT INTO merezha_app.reactor_rejections (name, position, index, effect, code) \
    SELECT $1, u.position, u.index, u.effect, u.code \
      FROM unnest($2::bigint[], $3::smallint[], $4::text[], $5::text[]) AS u (position, index, effect, code) \
    ON CONFLICT DO NOTHING",
);
/// Skipped events `$6` to `$2`: `on_error = "skip"` in the checkpoint
/// transaction, or an operator's skip (`$5` the principal).
pub const INSERT_SKIP: Statement = Statement::new(
    "app.reactor_skips.insert",
    "INSERT merezha_app.reactor_skips",
    "INSERT INTO merezha_app.reactor_skips (name, position, reason, failure_code, principal, first_position) \
    VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
);
/// Checkpoint transaction, last step: advance past the batch and clear
/// every failure mark; a halted reactor that succeeds is active again.
pub const ADVANCE_REACTOR: Statement = Statement::new(
    "app.reactors.advance",
    "UPDATE merezha_app.reactors",
    "UPDATE merezha_app.reactors \
    SET checkpoint = $2, row_revision = row_revision + 1, status = 'active', halted_position = NULL, \
        failures = 0, failure_code = NULL, failure_class = NULL, retry_at = NULL, updated_at = now() \
    WHERE name = $1 RETURNING row_revision",
);
/// A failed batch (`$5` halts at that position when set): the checkpoint
/// stays, `failures` grows and the next attempt waits `$4` ms. A paused
/// reactor stays paused.
pub const REACTOR_FAILURE: Statement = Statement::new(
    "app.reactors.record_failure",
    "UPDATE merezha_app.reactors",
    "UPDATE merezha_app.reactors \
    SET failure_code = $2, failure_class = $3, failures = failures + 1, \
        retry_at = now() + $4::bigint * interval '1 millisecond', \
        status = CASE WHEN status = 'paused' THEN 'paused' WHEN $5::bigint IS NULL THEN status ELSE 'halted' END, \
        halted_position = CASE WHEN status = 'paused' THEN NULL WHEN $5::bigint IS NULL THEN halted_position ELSE $5 END, \
        updated_at = now() \
    WHERE name = $1 AND checkpoint = $6 RETURNING failures",
);
/// `POST reactors/{name}/pause` and `/resume`: resume makes the reactor
/// active and due now; a halted reactor resumed retries at once.
pub const SET_REACTOR_STATUS: Statement = Statement::new(
    "app.reactors.set_status",
    "UPDATE merezha_app.reactors",
    "UPDATE merezha_app.reactors \
    SET status = $2, halted_position = NULL, row_revision = row_revision + 1, \
        retry_at = CASE WHEN $2 = 'active' THEN NULL ELSE retry_at END, updated_at = now() \
    WHERE name = $1 RETURNING status",
);
/// `POST reactors/{name}/retry`: due now, whatever its backoff.
pub const RETRY_REACTOR: Statement = Statement::new(
    "app.reactors.retry",
    "UPDATE merezha_app.reactors",
    "UPDATE merezha_app.reactors SET retry_at = NULL, updated_at = now() \
    WHERE name = $1 RETURNING status",
);
/// An operator's skip, inside its transaction after `LOCK_REACTOR`: the
/// checkpoint moves to `$2`, an active or halted reactor becomes active.
pub const SKIP_REACTOR: Statement = Statement::new(
    "app.reactors.skip",
    "UPDATE merezha_app.reactors",
    "UPDATE merezha_app.reactors \
    SET checkpoint = $2, row_revision = row_revision + 1, \
        status = CASE WHEN status = 'paused' THEN 'paused' ELSE 'active' END, halted_position = NULL, \
        failures = 0, failure_code = NULL, failure_class = NULL, retry_at = NULL, updated_at = now() \
    WHERE name = $1 AND $2 <= (SELECT position FROM merezha_app.head WHERE id = 1) \
    RETURNING checkpoint",
);
/// The configured reactors (`$1`), in one snapshot with the head: each
/// row and the accepted time of its first unprocessed event; with `$2 > 0`
/// also its rejections, counted up to `$2` (only `GET reactors` counts:
/// the lag sampler and the status document pass 0).
pub const REACTORS_OVERVIEW: Statement = Statement::new(
    "app.reactors.overview",
    "SELECT merezha_app.reactors",
    "SELECT r.name, r.checkpoint, r.status, r.halted_position, r.failure_code, r.failures, \
        to_char(r.retry_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), \
        CASE WHEN $2::bigint > 0 THEN (SELECT count(*) FROM (SELECT 1 FROM merezha_app.reactor_rejections j \
            WHERE j.name = r.name LIMIT $2::bigint) capped) END, \
        e.accepted_unix_ms \
    FROM unnest($1::text[]) AS c (name) \
    JOIN merezha_app.reactors r ON r.name = c.name \
    LEFT JOIN merezha_app.events e ON e.position = r.checkpoint + 1 \
    ORDER BY r.name",
);
/// One page of a reactor's rejections after the cursor `($2, $3)`, oldest
/// first, one row more than `$4` to tell whether another page follows.
pub const REJECTIONS: Statement = Statement::new(
    "app.reactor_rejections.list",
    "SELECT merezha_app.reactor_rejections",
    "SELECT position, index, effect, code, \
        to_char(rejected_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') \
    FROM merezha_app.reactor_rejections \
    WHERE name = $1 AND (position, index) > ($2, $3::smallint) \
    ORDER BY position, index LIMIT $4",
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
    PARTITION,
    LOAD_PARTITIONS,
    COMMIT_PARTITIONED,
    WRITE_PARTITIONS,
    SWEEP_OLD_PARTITIONS,
    SWEEP_STALE_PARTITIONS,
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
    INSERT_REACTOR,
    REACTOR,
    LOCK_REACTOR,
    INSERT_REJECTIONS,
    INSERT_SKIP,
    ADVANCE_REACTOR,
    REACTOR_FAILURE,
    SET_REACTOR_STATUS,
    RETRY_REACTOR,
    SKIP_REACTOR,
    REACTORS_OVERVIEW,
    REJECTIONS,
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
}
