# Application platform: PostgreSQL core, jobs and outbox, guest protocol 2

Status: proposed 2026-10-23, aligned 2026-10-24 with
[cross-block decisions](cross-block-decisions.md) (cited as R1–R15, AP1–AP4; NT, TE, ID and K as
there). Conforms to [framework architecture](../architecture/framework.md), sections "Application
core on PostgreSQL", "Guests and the blocks" and "Jobs and the outbox". It extends
[application.md](../architecture/application.md) and [contract.md](../architecture/contract.md). If
this design conflicts with cross-block-decisions.md or framework.md, they win in that order and this
design is amended.

It owns the outbox and jobs contract (R1), reactions (R4), route permissions (R6) and the event
envelope (R11). Telemetry names come from TE (R9), the effect vocabulary from NT §3.

The three parts depend on each other. The PostgreSQL store (A) supplies the event feed and the
transaction that reactions (C) write into. The outbox (B) is the only way out for effects. Protocol
2 (C) adds the principal that route permissions and tenant streams rely on.

## Decision summary

| Topic | Decision |
| --- | --- |
| Global positions | One `merezha_app.head` counter row, locked `FOR UPDATE` inside the append. Positions have no gaps, and commit order equals position order. The append is a single `plpgsql` function call in autocommit mode, so the lock is held for one statement plus the WAL flush. |
| Sync vs async | `Application` stays synchronous. `PostgresStore` holds a `tokio::runtime::Handle` and calls `block_on` on blocking-pool threads only. Jobs, relay, LISTEN and runners are async-native. |
| Idempotency across instances | Receipts are rows in the append transaction, keyed `(tag, tenant, actor, key)` and bound to a SHA-256 fingerprint. Each instance keeps only an in-process pending registry. A concurrent duplicate on another instance replays the stored receipt after commit. |
| Projections | A JSON state and checkpoint row with `row_revision` compare-and-swap. A session advisory lock per projection revision, held on a connection detached from the pool, so a cancelled run releases it. Background catch-up runs on in-process wake, `LISTEN merezha_app_events` and an interval. Rebuild runs in the background, can resume, and swaps atomically. |
| Projection revisions | A new `revision` is a new row that builds in the background. An instance is ready only when its revisions are warm. Rolling deploys keep the old revision serving. |
| Tenancy | Tenant streams are `"{tenant}:{aggregate}/{stream}"`, composed by the host from the principal. Clients cannot name another tenant's stream. Tenant-scoped queries read tenant-partitioned projections. |
| Outbox | `merezha_jobs.outbox`, appended by every module inside its own transaction (R1). A relay turns records into jobs. Dedup is `(source, dedupe_key)`, keys ≤ 160 bytes (R4a). The relay quarantines a record it cannot route instead of rolling back its batch. `contains_secret` payloads are nulled at relay and at the job's terminal state (R3). |
| Jobs | Lease with `FOR UPDATE SKIP LOCKED` and a per-claim `lease_token` fence. Heartbeats, a reaper, equal-jitter exponential backoff and classification-driven retry. Queue settings live only under `[jobs.queues.<name>]`. Dead letters stay in the same table (`state = 'dead'`). Cron uses `croner =4.0.2` with its `jiff` feature (R14). |
| Protocol 2 | Opt in per service with `application_protocol = 2` in the runtime manifest. Adds `metadata.principal`, the `react` and `describe` ops and `project.partition`. The host downgrades metadata for version 1 guests. |
| Reactions | Batched `react` in NT's effect vocabulary (R4). The checkpoint transaction calls `Notifier::request`/`emit_webhook` and appends command effects, keyed `reaction:{reactor}:{event_id}:{index}`. A malformed reply halts; a rejected effect is recorded and skipped. No rebuild, no replay. |
| Trace context | `trace_parent`/`trace_state` columns on events, outbox and jobs (R10), filled from the current span; job retries continue the original trace. |
| SQLite | Stays a development and test store. No import into PostgreSQL (AP2). |

---

# A. Application core on PostgreSQL

## A.1 What the current code constrains

Read from `crates/application`:

- `Application`/`Admission` are synchronous. Each command calls the guest twice (`validate`,
  `decide`) through `GuestSession::invoke_until`, which runs Wasmtime synchronously under fuel and a
  deadline. `application_http.rs` already runs every dispatch inside `spawn_blocking`, with the
  admission attached.
- The store already sees deadlines. The memory and SQLite stores implement `load/head/read_all`.
  SQLite adds `commit_command` (events and receipt in one transaction) and
  `projection`/`commit_projection` (row-revision CAS).
- Feed consumers check contiguity: `project()` fails `invalid_event_feed` when `event.position !=
  checkpoint + 1`, and SQLite recovery rejects position gaps. So positions must never have gaps. A
  `BIGSERIAL` with an advisory lock, as in Structure `eventsourcing-pg`, cannot be used.
- Event ids are `event/{position}`, derived from the position.
- Idempotency keeps an in-process `BTreeMap<Key, Record>`. SQLite preloads every completed receipt
  into it at open, which only works because SQLite caps receipts at 1024. PostgreSQL has no such
  cap, so the map must only hold pending claims.
- The SQLite owner lock (`.owner` sidecar) and the configuration fingerprint enforce a single host.
  PostgreSQL must allow several hosts on one database.
- Every SQLite store method runs SQL only and never calls the guest. The same rule holds here: no
  SQL transaction is open during guest execution.

## A.2 Sync or async: keep `Application` synchronous

Options:

1. Make `Application` async. Every guest call would still need `spawn_blocking`, because Wasmtime
   runs synchronously here (fuel plus a deadline, with `Session::invoke_service_until`). That means
   2 hops per command and 1 per projection batch. The public traits `Guest`, `GuestSession`,
   `Authorizer` and `EventStore` would all change, as would about 30 synchronous tests and the
   SQLite store's condvar slot. All of that buys nothing, because the dispatch is already off the
   async workers.
2. **Keep it synchronous and block inside blocking threads (chosen).** `PostgresStore` holds a
   `deadpool_postgres::Pool` and a `tokio::runtime::Handle`. Each store method runs
   `handle.block_on(tokio::time::timeout_at(deadline, async { ... }))`.

Rules for option 2:

- `Application` methods are called only from `spawn_blocking` or from non-runtime threads. HTTP
  already does this. Background runners and the command-effect job handler call `spawn_blocking`.
  `Handle::block_on` panics on an async worker thread, so a misuse fails loudly. A test drives every
  production entry point (HTTP, runners, the command-effect handler) on a multi-thread runtime to
  prove each one goes through `spawn_blocking`.
- Blocking threads are bounded. HTTP admission (`limits.max_concurrent` ≤ 64), background runners
  (`application.background.max_concurrent`, default 2) and the `application` job queue concurrency
  (default 4) limit how many blocking threads wait on PostgreSQL at once. That is far below tokio's
  512 default.
- The deadline covers everything. Pool acquisition, every statement and commit run under
  `timeout_at(admission.deadline)`. The pool also sets a session `statement_timeout` equal to the
  hard ceiling (30 s) through the kernel's connection setup. The append function bounds lock waiting
  with `lock_timeout` (A.4).
- Timeout during the append statement returns `commit_outcome_unknown` (transient). Unlike SQLite,
  nothing is poisoned. PostgreSQL keeps the transaction atomic, and a keyed retry replays the stored
  receipt. An unkeyed retry may duplicate, which stays the caller's decision as today.

An async `Application` can still come later. The internal store trait (A.3) is the seam.

## A.3 Internal structure

```
crates/application/src/
  application.rs     Storage enum gains Postgres(Arc<PostgresStore>)
  durable.rs         pub(crate) trait DurableStore (implemented by SqliteStore, PostgresStore)
  postgres/mod.rs    PostgresStore: pool, handle, limits, manifest check
  postgres/sql.rs    statements and the append function call
  postgres/runner.rs projection catch-up, rebuild, reactor runners (async loops)
  postgres/migrations/0001_application.sql
```

`postgres` is a cargo feature of `merezha-application`, on in `merezha-cli`. It pulls in
`merezha-postgres` (pool, migrations, test harness) and `tokio` (the runtime handle), and nothing
else. It is not a separate crate, because it needs the crate-private `Key`, `Record` and
`ProjectionState` internals and the `Storage` enum.

**Dependency direction (decided in AP-A2).** `merezha-platform` already depends on
`merezha-application`: the kernel `Authorizer` extends the application one (`ports/access.rs`). So
`merezha-application` never depends on `merezha-platform`, with or without the `postgres` feature;
that would be a cycle. Whatever the application core later needs from the platform ports
(`Notifier`, `Outbox`, `Principal`, `ResourceScope` for C2 and C3) goes through small traits defined
in `merezha-application` and implemented by adapters in `merezha-platform` or `merezha-cli`. A2
needs none. The host side lives in `merezha-cli`: `run` adds the PostgreSQL application as the
host-provided module `application` (a reserved name no block can register), so the kernel host
applies or verifies its migrations with every module's, then starts it on the project pool and nests
its routes under `/v1/application`.

```rust
pub(crate) trait DurableStore: Send + Sync {
    fn load(&self, stream: &Identity, deadline: Instant) -> Result<Vec<StoredEvent>, Failure>;
    fn head(&self, deadline: Instant) -> Result<u64, Failure>;
    fn read_all(&self, after: u64, limit: usize, deadline: Instant) -> Result<Vec<StoredEvent>, Failure>;
    /// Fast-path replay before the guest decides; None when absent.
    fn receipt(&self, key: &Key, fingerprint: &str, deadline: Instant) -> Result<Option<Receipt>, Failure>;
    fn commit_command(&self, c: CommitCommand<'_>, deadline: Instant) -> Result<Receipt, Failure>;
    fn projection(&self, p: &ProjectionRef<'_>, deadline: Instant) -> Result<(ProjectionState, i64), Failure>;
    fn commit_projection(&self, p: &ProjectionRef<'_>, row_revision: i64,
                         state: &ProjectionState, deadline: Instant) -> Result<i64, Failure>;
}
pub(crate) struct CommitCommand<'a> {
    pub stream: &'a Identity, pub tenant: Option<&'a Identity>, pub expected: u64,
    pub events: Vec<Event>, pub metadata: &'a Metadata, pub claim: Option<(&'a Key, &'a str)>,
}
```

`receipt()` returns `idempotency_mismatch` when the fingerprint differs. `Key` gains `tenant:
Option<Identity>`, serialized with `skip_serializing_if = "Option::is_none"`, so existing SQLite
receipt keys keep encoding byte for byte. PostgreSQL-only operations (partitions, rebuild staging,
reactors, advisory locks) are inherent methods of `PostgresStore` used by `postgres/runner.rs`.

Opening: `PostgresStore::connect(db: &Database, config: &Config, options: PostgresOptions,
deadline: Instant) -> impl Future` runs in the async startup (the module's `start`). It verifies that migrations are current (the kernel runner), checks or inserts the
manifest row, and syncs projection and reactor rows. Then `Application::with_postgres(config, guest,
authorizer, store)` constructs synchronously.

## A.4 Schema `merezha_app` (migration 0001)

`json` is used for every guest-originated document, not `jsonb`. `json` keeps the exact text: number
spelling, key order, and the `\u0000` escapes that `jsonb` rejects. Rows round-trip to the same
`StoredEvent` the SQLite store returns. Admin SQL can still use `->`/`->>`.

```sql
CREATE SCHEMA merezha_app;

CREATE TABLE merezha_app.manifest (
    id smallint PRIMARY KEY CHECK (id = 1),
    application text NOT NULL CHECK (application ~ '^[A-Za-z0-9._:/-]{1,128}$'),
    format smallint NOT NULL CHECK (format = 1),
    created_at timestamptz NOT NULL DEFAULT now());

-- The single append serialization point; position = highest committed position.
CREATE TABLE merezha_app.head (
    id smallint PRIMARY KEY CHECK (id = 1),
    position bigint NOT NULL CHECK (position >= 0));
INSERT INTO merezha_app.head (id, position) VALUES (1, 0);

CREATE TABLE merezha_app.streams (
    stream text PRIMARY KEY CHECK (octet_length(stream) BETWEEN 1 AND 128),
    tenant text NULL CHECK (tenant ~ '^[A-Za-z0-9_-]{1,40}$'),
    version bigint NOT NULL CHECK (version > 0),
    first_position bigint NOT NULL CHECK (first_position > 0),
    last_position bigint NOT NULL CHECK (last_position >= first_position),
    CHECK (tenant IS NULL OR starts_with(stream, tenant || ':')));

CREATE TABLE merezha_app.events (
    position bigint PRIMARY KEY CHECK (position > 0),           -- event_id = 'event/' || position
    stream text NOT NULL REFERENCES merezha_app.streams (stream),
    tenant text NULL,
    version bigint NOT NULL CHECK (version > 0),
    tag text NOT NULL CHECK (octet_length(tag) BETWEEN 1 AND 128),
    schema_version integer NOT NULL CHECK (schema_version BETWEEN 1 AND 65535),
    payload json NOT NULL,
    metadata json NOT NULL,
    accepted_unix_ms bigint NOT NULL CHECK (accepted_unix_ms >= 0),
    trace_parent text NULL CHECK (octet_length(trace_parent) = 55),     -- host-only (R10)
    trace_state text NULL CHECK (octet_length(trace_state) <= 512),
    UNIQUE (stream, version));

-- '' stands for "absent": Identity values are never empty, so it cannot collide.
CREATE TABLE merezha_app.receipts (
    tag text NOT NULL, tenant text NOT NULL DEFAULT '', actor text NOT NULL DEFAULT '', key text NOT NULL,
    fingerprint bytea NOT NULL CHECK (octet_length(fingerprint) = 32),   -- sha256 of canonical JSON
    stream text NOT NULL,
    version bigint NOT NULL CHECK (version >= 0),
    position bigint NOT NULL CHECK (position >= 0),
    events integer NOT NULL CHECK (events BETWEEN 0 AND 64),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tag, tenant, actor, key));

CREATE TABLE merezha_app.projections (
    name text NOT NULL,
    revision integer NOT NULL CHECK (revision > 0),
    partitioned boolean NOT NULL,
    generation integer NOT NULL DEFAULT 1 CHECK (generation > 0),
    state json NULL,                                            -- NULL when partitioned
    checkpoint bigint NOT NULL DEFAULT 0 CHECK (checkpoint >= 0),
    row_revision bigint NOT NULL DEFAULT 0 CHECK (row_revision >= 0),
    warm_position bigint NOT NULL CHECK (warm_position >= 0),
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'failing')),
    failure_code text NULL, failure_class text NULL, failures integer NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, revision),
    CHECK (partitioned = (state IS NULL)));

-- Migration 0002 (slice C2). Absent row = configured initial state.
CREATE TABLE merezha_app.projection_partitions (
    name text NOT NULL, revision integer NOT NULL, generation integer NOT NULL, tenant text NOT NULL,
    state json NOT NULL,
    PRIMARY KEY (name, revision, generation, tenant),
    FOREIGN KEY (name, revision) REFERENCES merezha_app.projections ON DELETE CASCADE);

-- At most one rebuild per projection revision; deleted by the swap transaction.
CREATE TABLE merezha_app.projection_rebuilds (
    name text NOT NULL, revision integer NOT NULL,
    generation integer NOT NULL,                                -- target generation (partitioned)
    state json NULL,                                            -- candidate (unpartitioned)
    checkpoint bigint NOT NULL DEFAULT 0,
    target bigint NOT NULL,                                     -- head snapshot when requested
    row_revision bigint NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'failed')),
    failure_code text NULL, requested_by text NULL, requested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, revision),
    FOREIGN KEY (name, revision) REFERENCES merezha_app.projections ON DELETE CASCADE);

-- Migration 0003 (slice C3).
CREATE TABLE merezha_app.reactors (
    name text PRIMARY KEY,
    checkpoint bigint NOT NULL CHECK (checkpoint >= 0),
    start_position bigint NOT NULL CHECK (start_position >= 0),
    row_revision bigint NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused', 'halted')),
    halted_position bigint NULL,
    failure_code text NULL, failure_class text NULL, failures integer NOT NULL DEFAULT 0,
    retry_at timestamptz NULL, updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((status = 'halted') = (halted_position IS NOT NULL)));

CREATE TABLE merezha_app.reactor_skips (
    name text NOT NULL REFERENCES merezha_app.reactors (name),
    position bigint NOT NULL,
    reason text NOT NULL CHECK (reason IN ('admin', 'on_error_skip', 'skip_to_head')),
    failure_code text NULL, principal text NULL, skipped_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, position));

-- Well-formed effects that failed validation (R4); written in the checkpoint transaction.
CREATE TABLE merezha_app.reactor_rejections (
    name text NOT NULL REFERENCES merezha_app.reactors (name),
    position bigint NOT NULL, index smallint NOT NULL CHECK (index BETWEEN 0 AND 7),
    effect text NOT NULL CHECK (effect IN ('notify', 'webhook', 'command')),
    code text NOT NULL, rejected_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, position, index));
```

`projection_partitions` ships in migration 0002 (slice C2), and `reactors`, `reactor_skips` and
`reactor_rejections` in migration 0003 (slice C3). They are shown here so the full schema reads in
one place. Migrations are forward-only, as the kernel requires.

### Append function (migration 0001)

```sql
CREATE FUNCTION merezha_app.lookup_receipt(p_tag text, p_tenant text, p_actor text, p_key text,
                                           p_digest bytea)
RETURNS TABLE (outcome text, r_version bigint, r_position bigint, r_events integer)
LANGUAGE sql SET search_path = '' AS $$
    SELECT CASE WHEN r.fingerprint = p_digest THEN 'replayed' ELSE 'mismatch' END,
           r.version, r.position, r.events
      FROM merezha_app.receipts r
     WHERE r.tag = p_tag AND r.tenant = p_tenant AND r.actor = p_actor AND r.key = p_key $$;

CREATE FUNCTION merezha_app.append(
    p_stream text, p_tenant text,                  -- p_tenant NULL for application streams
    p_expected bigint,
    p_tags text[], p_schema_versions integer[], p_payloads json[],
    p_metadata json, p_accepted_unix_ms bigint,
    p_trace_parent text, p_trace_state text,       -- current command span; NULL when not recording
    p_key_tag text, p_key_tenant text, p_key_actor text, p_key text,   -- all NULL without a key
    p_fingerprint text,                            -- canonical JSON {stream, payload, expected_version}
    p_lock_timeout_ms integer
) RETURNS TABLE (outcome text, r_version bigint, r_position bigint, r_events integer)
LANGUAGE plpgsql SET search_path = '' AS $$
DECLARE
    v_n integer := coalesce(cardinality(p_tags), 0);
    v_digest bytea;
    v_base bigint;
    v_current bigint;
BEGIN
    IF p_key IS NOT NULL THEN
        v_digest := sha256(convert_to(p_fingerprint, 'UTF8'));
    END IF;
    -- Fail fast without the lock when the precondition is already stale. The
    -- stream check runs before the receipt lookup: a same-key commit landing
    -- between the two is then found by the lookup, and one landing before the
    -- check by the lookup of the conflict branch, so it replays either way.
    SELECT coalesce(max(s.version), 0) INTO v_current FROM merezha_app.streams s WHERE s.stream = p_stream;
    IF v_current <> p_expected THEN
        IF p_key IS NOT NULL THEN
            RETURN QUERY SELECT * FROM merezha_app.lookup_receipt(p_key_tag, p_key_tenant, p_key_actor, p_key, v_digest);
            IF FOUND THEN RETURN; END IF;
        END IF;
        RETURN QUERY SELECT 'conflict'::text, v_current, 0::bigint, 0; RETURN;
    END IF;
    IF p_key IS NOT NULL THEN
        RETURN QUERY SELECT * FROM merezha_app.lookup_receipt(p_key_tag, p_key_tenant, p_key_actor, p_key, v_digest);
        IF FOUND THEN RETURN; END IF;
    END IF;

    IF v_n > 0 THEN
        PERFORM set_config('lock_timeout', p_lock_timeout_ms::text || 'ms', true);
        -- Serialization point, held until commit. Every later statement takes a fresh
        -- READ COMMITTED snapshot and so sees every earlier committed append.
        SELECT h.position INTO v_base FROM merezha_app.head h WHERE h.id = 1 FOR UPDATE;
        IF p_key IS NOT NULL THEN
            RETURN QUERY SELECT * FROM merezha_app.lookup_receipt(p_key_tag, p_key_tenant, p_key_actor, p_key, v_digest);
            IF FOUND THEN RETURN; END IF;
        END IF;
        SELECT coalesce(max(s.version), 0) INTO v_current FROM merezha_app.streams s WHERE s.stream = p_stream;
        IF v_current <> p_expected THEN
            IF p_key IS NOT NULL THEN
                RETURN QUERY SELECT * FROM merezha_app.lookup_receipt(p_key_tag, p_key_tenant, p_key_actor, p_key, v_digest);
                IF FOUND THEN RETURN; END IF;
            END IF;
            RETURN QUERY SELECT 'conflict'::text, v_current, v_base, 0; RETURN;
        END IF;
        UPDATE merezha_app.head SET position = v_base + v_n WHERE id = 1;
        INSERT INTO merezha_app.streams (stream, tenant, version, first_position, last_position)
             VALUES (p_stream, p_tenant, p_expected + v_n, v_base + 1, v_base + v_n)
        ON CONFLICT (stream) DO UPDATE SET version = EXCLUDED.version, last_position = EXCLUDED.last_position;
        INSERT INTO merezha_app.events (position, stream, tenant, version, tag, schema_version, payload,
                                        metadata, accepted_unix_ms, trace_parent, trace_state)
        SELECT v_base + i, p_stream, p_tenant, p_expected + i, p_tags[i], p_schema_versions[i], p_payloads[i],
               p_metadata, p_accepted_unix_ms, p_trace_parent, p_trace_state
          FROM generate_subscripts(p_tags, 1) AS i;
        PERFORM pg_notify('merezha_app_events', (v_base + v_n)::text);
    ELSE
        SELECT h.position INTO v_base FROM merezha_app.head h WHERE h.id = 1;
    END IF;

    IF p_key IS NOT NULL THEN
        INSERT INTO merezha_app.receipts (tag, tenant, actor, key, fingerprint, stream, version, position, events)
             VALUES (p_key_tag, p_key_tenant, p_key_actor, p_key, v_digest, p_stream,
                     p_expected + v_n, v_base + v_n, v_n)
        ON CONFLICT DO NOTHING;
        IF NOT FOUND THEN
            IF v_n > 0 THEN   -- a zero-event command with the same key won; undo our events
                RAISE EXCEPTION USING ERRCODE = 'MZ001', MESSAGE = 'idempotency_in_flight';
            END IF;
            RETURN QUERY SELECT * FROM merezha_app.lookup_receipt(p_key_tag, p_key_tenant, p_key_actor, p_key, v_digest);
            RETURN;
        END IF;
    END IF;
    RETURN QUERY SELECT 'appended'::text, p_expected + v_n, v_base + v_n, v_n;
END $$;
```

The host calls it as one extended-protocol statement **outside an explicit transaction**, so
PostgreSQL commits it implicitly at Sync. One round trip covers lock, inserts and commit. Mapping:
`appended` and `replayed` return a `Receipt`. `mismatch` maps to `idempotency_mismatch` (conflict)
and `conflict` to `concurrency_conflict`. SQLSTATE `55P03` (lock timeout) maps to `storage_busy`
(transient), and `MZ001` to `idempotency_in_flight` (conflict). The host validates payload bounds,
`schema_version > 0`, `compatible_json` and batch size before the call, exactly as
`SqliteStore::commit_command` does. The `UNIQUE (stream, version)` and `head` CHECK constraints are
a backstop only.

`p_lock_timeout_ms` is `min(remaining deadline − 50 ms, application.append.lock_timeout_max_ms)`,
computed after the pooled session is acquired and the statement prepared, so a slow acquire shortens
the lock wait instead of outliving the caller. With less than 50 ms left at that point the host fails
`timeout` without sending the append. The append is pipelined in one round trip between `SET
statement_timeout = min(remaining − 25 ms, database.statement_timeout_ms)` and `RESET
statement_timeout`, so the server ends the statement (a definite rollback, `timeout`) before the
caller's deadline and never past the operator's ceiling; the lock wait is a further 25 ms shorter, so
a held lock answers `storage_busy`. Each of the three statements is bounded on its own: an append that
answered keeps its known outcome even when only the reset fails or comes late. A session whose reset
failed or whose outcome is unknown is closed rather than returned to the pool.

**Same-key races.** The stream fail-fast check runs before the receipt lookup, and every `conflict`
branch with a key looks the receipt up again before answering. A same-key commit by another
instance at any point of the call therefore replays instead of surfacing as a conflict. One level
up, a keyed command whose explicit `expected_version` turns stale after the fast-path replay checks
`receipt()` once more before failing `concurrency_conflict`. Stored events and projection states are
read up to the hard ceilings (65,536 and 262,144 bytes); the configured `max_payload_bytes` and
`max_state_bytes` bind writes only, so lowering them in a rolling deploy keeps history readable.

The trace columns hold the command span's context (TE §2.2). They are not part of `StoredEvent`
and never reach guests; `read_all` returns them only for span links. The store reads the context through
`postgres::TraceSource::current()` on the committing thread, so it sees the command span; TE-T3
implements the source from the current span, and without one the columns stay NULL. A malformed
context is stored as NULL rather than failing the command.

## A.5 Contiguous positions with several instances

**Why there are no gaps.** The position is derived from `head.position`, which changes only inside a
transaction that also inserts the events. A rollback discards both. Nothing else allocates
positions.

**Why commit order equals position order.** Every appender holds the `head` row lock until its
commit finishes. PostgreSQL makes a transaction visible in the proc array before it releases its
locks. So when appender N+1 acquires the row, appender N is already visible, and N+1 inserts
positions above N's. A reader that sees position P has seen every committed position below P.
`read_all` keeps the `position == checkpoint + 1` check as defense. In PostgreSQL mode a gap becomes
the transient `event_feed_gap`, logged as TE's `projection.failed`, not a permanent failure.

**Alternatives considered:**

| Option | Gaps | Commit order | Throughput | Verdict |
| --- | --- | --- | --- | --- |
| `BIGSERIAL` with `pg_advisory_xact_lock` (Structure) | yes (rollbacks consume values) | yes | same as chosen | rejected: gaps break `invalid_event_feed` and the receipt contract |
| `SERIALIZABLE` with `max(position)+1` | no | yes | collapses under contention (serialization failures, retries) | rejected |
| Post-commit sequencer / commit-order table (writers insert unpositioned rows; one leader assigns positions) | no | yes | append parallelism and group commit preserved | rejected: the `Receipt.position` returned by `command` would no longer be known at commit, and it needs leader election |
| **Counter row locked inside a single-statement autocommit function** | no | yes | ≈ 1 / (function time + WAL flush) | **chosen** |

**Throughput analysis.** All appends in one database are serialized for roughly the server time of
the function (about 0.1–0.4 ms for ≤ 16 events) plus the commit WAL flush. The flush is about
0.05–0.5 ms on local NVMe and about 1–3 ms on network block storage with `synchronous_commit = on`.
Because the lock is held through the flush, commits never share one flush, so group commit does not
help. Expected ceiling: **about 300–2000 command commits per second per application**, each carrying
up to `batch_size` events. The owner accepted it (AP1). The design keeps the hold time minimal:

- The receipt and version pre-checks run before the lock. Stale commands fail without queueing.
- Guest execution, validation, serialization and the history load happen before the call. Only one
  statement runs while the lock is held.
- Receipts with zero events never take the lock.

The append is the registered statement `app.events.append`, so TE's `db.client.operation.duration`
times it, and `storage_busy` shows as `merezha.outcome = failed` on `merezha.command.duration` (TE
§1.3). If an application ever needs more, the escape hatch is per-tenant feeds: several `head` rows
and a partitioned position space. That is out of scope (F).

**Concurrent same-stream writers on two instances.** Both pass the pre-check. The second waits on
`head`, re-reads the stream version after acquiring it, sees `expected + n` and returns `conflict`.
The `UNIQUE (stream, version)` constraint is never the mechanism.

**Same key on two instances.** Both run the guest, because each instance's pending registry is
local. The second waits on `head`, then finds the receipt and returns `replayed` with the identical
receipt. Within one instance the existing `idempotency_in_flight` behaviour stays the same. Guests
are pure, so the duplicate decision costs only CPU.

## A.6 Commands, queries and limits in PostgreSQL mode

Command flow (bold marks a change from today):

1. Admission and route lookup. **The HTTP adapter fills a missing `correlation_id` (the trace id)
   and `message_id` (128 random bits, hex), as TE §2.2 specifies (AP4). The route permission check
   (C.4) runs before any guest call.**
2. Stream composition: `{aggregate}/{stream}`, or **`{tenant}:{aggregate}/{stream}`** for
   tenant-scoped routes.
3. Guest `validate`, then the `Authorizer::authorize` payload hook (unchanged).
4. Idempotency: in-process pending claim, then **`store.receipt()` fast-path replay**.
5. `load(stream)`: `SELECT ... WHERE stream = $1 ORDER BY version LIMIT max_stream_events + 1`. More
   rows fail `history_limit`.
6. Guest `decide`, then the deadline and batch checks.
7. `merezha_app.append(...)`, then **an in-process wake to the local projection and reactor
   runners**.

Queries read one row: `projections (name, revision)`, or for tenant-scoped queries the partition at
the current generation in one statement, so no rebuild swap can fall between the two reads:

```sql
SELECT p.checkpoint, p.warm_position, pp.state     -- pp.state NULL: absent row, initial state
  FROM merezha_app.projections p LEFT JOIN merezha_app.projection_partitions pp
    ON (pp.name, pp.revision, pp.generation, pp.tenant) = (p.name, p.revision, p.generation, $3)
 WHERE p.name = $1 AND p.revision = $2;
```

New optional `Query.min_position: Option<u64>`
(`serde(default)`): when set, the host waits until the projection checkpoint reaches that position.
It is woken by local commits and polls every 50 ms, bounded by the admitted deadline. If the
deadline passes, it fails `projection_behind` (transient, HTTP 503). This gives read-your-writes
after a command receipt.

Limits (application config version 2, A.9):

| Key | PostgreSQL meaning | Default | Max |
| --- | --- | ---: | ---: |
| `max_stream_events` (new, all backends) | per-stream history ceiling, because the whole history goes to the guest | 4096 | 4096 |
| `max_payload_bytes`, `max_state_bytes`, `batch_size`, `max_concurrent`, `timeout_ms` | unchanged | unchanged | unchanged |
| `max_events`, `max_streams`, `max_store_bytes`, `max_idempotency` | **rejected** with `invalid_limits` when present, because total capacity is an operator disk concern | n/a | n/a |

Internally `Limits` tracks which keys were present (`Option` fields with accessor defaults), so
memory and SQLite keep their current semantics. The runtime manifest's `max_input_bytes` must still
fit the full history. That check stays where it is today, in the guest call.

Configuration drift. Unlike SQLite, PostgreSQL does not fingerprint the whole configuration, because
rolling deploys change routes. It checks:

- the manifest `application` identity (mismatch fails `storage_configuration`);
- projection `(name, revision)` rows, with a new revision handled as in A.7;
- the `partitioned` flag of an existing `(name, revision)` (a change fails `storage_configuration`;
  changing it requires a new revision).

`initialize` is rejected for PostgreSQL. The first start on a migrated empty schema inserts the
manifest row in a transaction guarded by `INSERT ... ON CONFLICT DO NOTHING`, then re-reads and
compares it.

## A.7 Projections: state, CAS, locks, background catch-up

**Locks.** Each run (live, rebuild, reactor, explicit catch-up) holds a session advisory lock on its
own connection, **detached from the pool** as kernel `migrate.rs` does (`db.acquire` then
`deadpool_postgres::Object::take`). The run sets `application_name = 'merezha {key}'` on that
session and issues all its reads and commits there through K2's detached-connection transaction
helper, which applies the deadline-bound `SET LOCAL statement_timeout` and maps an interrupted
commit to `commit_outcome_unknown` exactly as `Database::transaction` does:

```sql
SELECT pg_try_advisory_lock(1297760592 /* 'MZAP' */, hashtext('p:' || $1 || ':' || $2));  -- live catch-up
SELECT pg_try_advisory_lock(1297760592, hashtext('b:' || $1 || ':' || $2));               -- rebuild
SELECT pg_try_advisory_lock(1297760592, hashtext('r:' || $1));                            -- reactor
```

There is no unlock: the run owns the client, and dropping it closes the session, which releases the
lock and rolls back any open transaction, on success, error, panic or a cancelling `timeout_at`
alike. The kernel pool recycles with `RecyclingMethod::Fast`, which resets nothing,
so a session lock left on a pooled connection would stop that projection on every instance: pooled
connections take only `pg_advisory_xact_lock` (B.5, B.9). Transaction-scoped locks were rejected for
runs, because they keep a transaction open across guest calls and exclude only per batch. A run costs
one connection setup; the linger in step 4 below keeps that near one per projection per
`run_timeout_ms` under load. A hash collision only adds needless mutual exclusion.

**Lock age.** Every statement of a run is awaited under its hard deadline (`run_timeout_ms` + 5 s)
and every guest call has its own, so only a wedged process holds a lock longer; a run cut at the
deadline logs `projection.failed` with code `run_timeout`. A stuck holder shows as
`merezha.projection.lag.age` (or `merezha.reactor.lag.age`) above `run_timeout_ms` while other
instances keep counting `merezha.projection.lock.contended`; the holder reports its lock age in
`merezha.projection.lock.held` (reactors: `merezha.reactor.lock.contended` and
`merezha.reactor.lock.held`), and TE's alert rules key on those. Operators find the holder in `pg_locks`
(`classid = 1297760592`) and end it with `pg_terminate_backend`.

**The advisory lock limits duplicate work. It is not what keeps the data correct.** If the lock
connection dies, another instance may start, and the first instance's next CAS fails
`projection_conflict`. The runner then reloads.

**Catch-up commit (unpartitioned):**

```sql
UPDATE merezha_app.projections
   SET state = $3, checkpoint = $4, row_revision = row_revision + 1,
       status = 'active', failure_code = NULL, failure_class = NULL, failures = 0, updated_at = now()
 WHERE name = $1 AND revision = $2 AND row_revision = $5
   AND $4 <= (SELECT position FROM merezha_app.head WHERE id = 1)
RETURNING row_revision;
```

Zero rows means `projection_conflict`. Batch semantics are unchanged: each successful batch
publishes state and checkpoint together, and a failed batch leaves both untouched.

**Partitioned commit.** One transaction per batch:

1. Read up to `batch_size` events after the checkpoint.
2. Group them by `events.tenant`. Events without a tenant only advance the checkpoint.
3. Load each touched partition of the current generation (or the initial state), and call the guest
   once per tenant group with `project { partition: tenant, ... }`.
4. Commit, CAS first, so a lost CAS writes nothing and its row lock orders the batch against a
   rebuild swap, which locks the same row:

   ```sql
   BEGIN;
   -- the CAS UPDATE above with state left NULL, plus AND generation = $6 (the generation loaded);
   -- zero rows: ROLLBACK, projection_conflict, before any partition write
   INSERT INTO merezha_app.projection_partitions (name, revision, generation, tenant, state)
   SELECT $1, $2, $6, u.tenant, u.state FROM unnest($7::text[], $8::json[]) AS u (tenant, state)
   ON CONFLICT (name, revision, generation, tenant) DO UPDATE SET state = EXCLUDED.state;
   COMMIT;
   ```

At most `batch_size` guest calls run per batch.

**Triggers.** Each instance runs one async `ProjectionScheduler`:

- **Local wake.** After every `appended` outcome, `Application` signals a `tokio::sync::Notify` per
  projection and reactor. Signals coalesce.
- **LISTEN.** `merezha-postgres` exposes one shared `Listener`: a dedicated non-pooled
  `tokio_postgres` connection that drives `poll_message` and fans out `AsyncMessage::Notification`
  by channel. The application subscribes to `merezha_app_events`. On reconnect, with exponential
  backoff from 100 ms to 10 s, the listener fires a wake to every subscriber, because notifications
  may have been lost.
- **Interval.** `application.projections.interval_ms`, default 5000, is the safety net for lost
  notifications and for external restores.

For each wake, the scheduler starts at most one run per projection on this instance, bounded
globally by `application.background.max_concurrent`. A run:

1. `spawn_blocking`;
2. detach a connection and `pg_try_advisory_lock` on it; if held elsewhere, drop it and return
   quietly;
3. snapshot the head;
4. loop over batches until the checkpoint reaches the snapshot, then take a new snapshot while wakes
   keep arriving within 1 s, until `run_timeout_ms` (default 30000) expires;
5. drop the connection, which releases the lock.

If the head moved during the run, the scheduler re-arms. Failures record `status = 'failing'` and
`failure_code`, and retry with backoff from 1 s to 5 min. The data is not touched, and queries keep
serving the last good pair.

**Explicit catch-up.** `POST .../catch-up` keeps its request and response shape (`{state,
checkpoint}` for unpartitioned projections, `{checkpoint, head}` for partitioned ones). It tries the
lock. If another runner holds it, the request waits within its deadline until `checkpoint >= head
snapshot`, then reads the row. At most `background.max_concurrent` explicit catch-ups hold their own
lock connection at once; the rest wait on the running one the same way, so detached connections stay
within the A.8 budget. If the deadline passes, it fails `timeout`, and progress stays
persisted. `projection_in_flight` remains only for memory and SQLite.

**Rebuild.** PostgreSQL histories are unbounded, so rebuild is a background operation that can
resume:

1. `POST .../rebuild` inserts a `projection_rebuilds` row with `target = head`, `generation =
   current + 1`, the initial candidate state and checkpoint 0. It returns `202 {"rebuild": {"name",
   "revision", "target"}}`. If a rebuild already exists, it returns `409 rebuild_in_progress`.
2. The rebuild runner (lock `b:`) processes batches with `live = false`. It persists the candidate
   after each batch, with CAS on `projection_rebuilds.row_revision`; partitioned rebuilds write
   `projection_partitions` rows at the target generation. Live catch-up keeps running under its own
   lock.
3. Swap transaction:

   ```sql
   SELECT row_revision FROM merezha_app.projections
    WHERE name = $1 AND revision = $2 FOR UPDATE;
   UPDATE merezha_app.projections
      SET state = $candidate, checkpoint = $candidate_checkpoint,
          generation = $target_generation, row_revision = row_revision + 1, updated_at = now()
    WHERE name = $1 AND revision = $2;
   DELETE FROM merezha_app.projection_rebuilds WHERE name = $1 AND revision = $2;
   ```

   The checkpoint may move backwards to the rebuild target. Live catch-up then carries the new state
   from there with `live = true`. A live runner holding the old `row_revision` fails CAS once and
   reloads. Old-generation partitions are deleted after the swap in chunks of 1000; queries join on
   the current generation (A.6), so none reads a half-deleted generation.
4. A failed batch sets `projection_rebuilds.status = 'failed'`. The live model is untouched. `POST
   .../rebuild` with `{"resume": true}` retries, and `DELETE .../rebuild` abandons the rebuild.

Memory and SQLite keep the synchronous rebuild contract unchanged.

**Revisions and warm-up.** At startup, for each configured projection without a `(name, revision)`
row, the host inserts one with `warm_position = current head` and builds it in the background with
`live = false` until the checkpoint reaches `warm_position`, then `live = true`. Queries on a
revision that is not yet warm fail `projection_warming` (503). With
`application.projections.warm_readiness = true` (default), `/health/ready` reports not ready until
all of this instance's revisions are warm. This one-time warm-up is the only projection input to
readiness; ongoing lag never is (R10). During a rolling deploy, old instances keep serving the
old revision. Old revision rows stay until `DELETE
/v1/application/projections/{name}/revisions/{revision}` removes them.

## A.8 Bounds summary (PostgreSQL)

| Resource | Bound |
| --- | --- |
| Events per command | `batch_size` ≤ 64; payload ≤ `max_payload_bytes` |
| History per decision | `max_stream_events` ≤ 4096 rows; guest input bytes |
| Feed read | `batch_size` rows per statement |
| Projection state | `max_state_bytes` ≤ 256 KiB per row or partition |
| Partitions touched per batch | ≤ `batch_size` |
| Background runs per instance | `application.background.max_concurrent` (default 2, max 16) |
| Advisory-lock connections | one detached connection per active run, outside the pool (`take` shrinks the pool, which reopens a slot): server connections ≤ pool size + `background.max_concurrent` + 1 (`Listener`) + at most `background.max_concurrent` explicit catch-ups |
| Pool size guidance | ≥ `limits.max_concurrent` + `background.max_concurrent` + jobs needs (B.10) + 2; a startup warning when below |
| Guest runtime concurrency | Startup validation: runtime `max_concurrent_invocations` ≥ `limits.max_concurrent` + `background.max_concurrent`, otherwise background runs make HTTP fail `overloaded`. Fails with `invalid_concurrency_budget`. |
| NOTIFY payload | a decimal position (≤ 20 bytes) |

## A.9 Configuration (application.toml)

```toml
version = 2                         # 1 still accepted; 2 enables the keys marked v2

[storage]
backend = "postgres"                # memory | sqlite | postgres
application = "task-board"          # identity stored in merezha_app.manifest
# no url here: the pool comes from merezha.toml [database]

[limits]
max_stream_events = 4096            # v2
batch_size = 32
```

`merezha.toml` locates this file through K's `[application] path` and the runtime manifest through
K's `[runtime] manifest` (R12). Runtime tuning goes in `merezha.toml` (section D.2).

## A.10 How the SQLite contracts map

The portable assertions move into `crates/application/tests/store_contract.rs`, a set of functions
`fn contract_*(fixture: &impl StoreFixture)`. They run against SQLite always and against PostgreSQL
when `MEREZHA_TEST_DATABASE_URL` is set. A missing variable gives an explicit skip message, as the
kernel requires.

| Existing test | PostgreSQL mapping |
| --- | --- |
| `restart_retains_receipt_and_projection` | contract: "reopen" = a new `Application` over the same database |
| `sqlite_replays_canonical_scoped_receipts_but_still_validates_and_authorizes` | contract |
| `sqlite_zero_event_receipts_and_capacity_survive_reopen` | zero-event part: contract. Capacity part: replaced by "PostgreSQL rejects capacity keys" |
| `sqlite_clones_share_pending_keys_and_only_one_racing_stream_write_commits` | contract, plus a two-instance variant (E, A2 test 4) |
| `sqlite_failed_catchup_and_rebuild_preserve_the_last_persisted_pair` | contract (rebuild via background API in PostgreSQL) |
| `sqlite_event_stream_byte_and_state_limits_remain_finite_across_restart` | state limit: contract; total capacity: n/a; `history_limit`: new |
| `multi_event_batches_retain_contiguous_versions_and_rollback_invalid_tail` | contract |
| `independent_connections_recheck_stream_key_and_projection_row_revision` | contract with two stores on separate pools |
| `statement_failure_rolls_back_events_receipt_position_and_projection_pair` | contract: the same trigger-injection technique (`CREATE TRIGGER ... RAISE`) on `merezha_app.receipts` and `projections` |
| `busy_deadline_capacity_and_commit_failure_do_not_strand_a_transaction` | PostgreSQL: external `SELECT ... FROM head FOR UPDATE` gives `storage_busy` within the deadline; `pg_terminate_backend` during the append gives `commit_outcome_unknown`, and the next call works with no poison |
| `killed_process_recovers_each_command_transaction_cut...`, `killed_rebuild_publishes_both...` | PostgreSQL process tests with `MEREZHA_PG_CRASH_CUT` (test feature): `before_append`, `after_append_before_wake`, `projection_after_commit`, `rebuild_before_swap`, `rebuild_after_swap` |
| `reopen_rejects_schema_configuration_and_recovered_bound_drift_without_reset` | PostgreSQL: migration checksum drift (kernel), manifest identity mismatch, `partitioned` flag drift |
| `recovered_event_invariants_and_receipt_counts_are_checked_before_readiness` | replaced by `merezha application verify` (A.11), which is not run at startup because it is O(history) |
| `closed_backup_restore_retains_exact_events_metadata_and_projection` | PostgreSQL: `pg_dump --schema=merezha_app` then restore into a fresh database; exact `StoredEvent` equality |
| Owner lock, connection slot/condvar, init/symlink/hardlink, page budget, header/btree corruption | SQLite-only. No PostgreSQL analogue; they stay in `sqlite.rs` |
| `cli/tests/sqlite_process.rs` owner exclusion | inverted: two hosts on one database must both serve (E, A2 test 9) |

## A.11 Verification

`merezha application verify --config merezha.toml` is read-only. It runs in bounded SQL chunks of
10,000 positions:

- positions are contiguous: `position = lag(position) + 1` over a window;
- per-stream versions are contiguous, and `streams.version`/`last_position` match the events;
- `head.position = max(position)`;
- every receipt with `events > 0` points at events of its stream;
- every checkpoint is ≤ head.

It prints a JSON report and exits non-zero on any violation. Run it after restores.

**No SQLite import (AP2).** SQLite stays a development and test store. A PostgreSQL application
starts empty; development data is recreated by its seed commands, not copied.

---

# B. Jobs and the outbox (`merezha-jobs`, schema `merezha_jobs`)

## B.1 Model

- **Outbox records** are appended by every module inside its own transaction (R1): use cases,
  NT's expansion (its delivery jobs), the reactor checkpoint. There are two kinds. `event` is an
  integration event on a topic (B.3 envelope), fanned out to subscriptions. `job` targets one queue
  and kind directly, for example command effects and delivery jobs.
- **The relay** moves unrelayed outbox rows into `jobs` in a single transaction: insert the jobs
  with `ON CONFLICT DO NOTHING`, then mark the rows relayed.

  Callers could insert jobs directly, but the relay is kept for three reasons. The caller's
  transaction then touches only an append-only table, never the hot, update-heavy `jobs` indexes.
  Fan-out uses subscriptions stored in the database, so they are the same across versions during
  rolling deploys. And "crash between commit and relay" becomes a plain restart case.
- **Jobs** are leased by workers. Delivery is at least once. Handlers must tolerate duplicates, and
  every job carries its idempotency key into the handler.

Jobs are ordered by `priority DESC, run_at, id`. There is no per-key FIFO ordering (F).

## B.2 Schema (migration 0001)

```sql
CREATE SCHEMA merezha_jobs;

CREATE TABLE merezha_jobs.outbox (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source text NOT NULL CHECK (source ~ '^[a-z][a-z0-9_]{0,31}$'),           -- module name
    dedupe_key text NOT NULL CHECK (octet_length(dedupe_key) BETWEEN 1 AND 160),   -- R4a
    record_kind text NOT NULL CHECK (record_kind IN ('event', 'job')),
    topic text NULL CHECK (topic ~ '^[a-z][a-z0-9_]{0,31}\.[a-z][a-z0-9_]{0,63}$'),   -- R11
    queue text NULL, job_kind text NULL,
    priority smallint NOT NULL DEFAULT 0,
    run_at timestamptz NULL,
    payload json NULL,
    contains_secret boolean NOT NULL DEFAULT false,                          -- R3
    correlation_id text NOT NULL, causation_id text NULL, tenant text NULL,
    trace_parent text NULL, trace_state text NULL,                           -- R10
    created_at timestamptz NOT NULL DEFAULT now(),
    relayed_at timestamptz NULL,
    quarantined_at timestamptz NULL, quarantine_code text NULL,              -- B.4
    UNIQUE (source, dedupe_key),
    CHECK ((quarantined_at IS NULL) = (quarantine_code IS NULL)),
    CHECK (relayed_at IS NULL OR quarantined_at IS NULL),
    -- R3: nulled once relayed or quarantined as unroutable; kept when isolated (B.4)
    CHECK ((payload IS NULL) = (contains_secret
        AND (relayed_at IS NOT NULL OR quarantine_code IS NOT DISTINCT FROM 'unroutable'))),
    CHECK ((record_kind = 'event' AND topic IS NOT NULL AND queue IS NULL AND job_kind IS NULL)
        OR (record_kind = 'job' AND topic IS NULL AND queue IS NOT NULL AND job_kind IS NOT NULL)));
CREATE INDEX outbox_pending ON merezha_jobs.outbox (id) WHERE relayed_at IS NULL AND quarantined_at IS NULL;
CREATE INDEX outbox_relayed ON merezha_jobs.outbox (relayed_at) WHERE relayed_at IS NOT NULL;

CREATE TABLE merezha_jobs.subscriptions (
    topic text NOT NULL, job_kind text NOT NULL, queue text NOT NULL, source text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_declared_at timestamptz NOT NULL DEFAULT now(),   -- refreshed by every declaring instance
    deactivated_at timestamptz NULL,      -- retired: nobody declared it for retire_after
    PRIMARY KEY (topic, job_kind));

CREATE TABLE merezha_jobs.jobs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue text NOT NULL CHECK (queue ~ '^[a-z][a-z0-9_.-]{0,63}$'),
    kind text NOT NULL CHECK (octet_length(kind) BETWEEN 1 AND 128),
    idempotency_key text NOT NULL CHECK (octet_length(idempotency_key) BETWEEN 1 AND 200),
    payload json NULL,
    payload_digest bytea NOT NULL CHECK (octet_length(payload_digest) = 32),
    contains_secret boolean NOT NULL DEFAULT false,
    state text NOT NULL DEFAULT 'available'
        CHECK (state IN ('available', 'running', 'succeeded', 'dead', 'cancelled')),
    priority smallint NOT NULL DEFAULT 0,
    run_at timestamptz NOT NULL DEFAULT now(),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 100),
    lease_token uuid NULL, lease_owner text NULL, lease_expires_at timestamptz NULL,
    last_error_code text NULL, last_error_at timestamptz NULL,
    last_error_class text NULL CHECK (last_error_class IN ('permanent', 'transient', 'conflict')),
    outbox_id bigint NULL, schedule_name text NULL, scheduled_for timestamptz NULL,
    correlation_id text NOT NULL, causation_id text NULL, tenant text NULL,
    trace_parent text NULL, trace_state text NULL,
    replays integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz NULL,
    UNIQUE (queue, idempotency_key),
    CHECK ((state = 'running') = (lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)),
    -- R3: a flagged job loses its payload exactly at the terminal state
    CHECK ((payload IS NULL) = (contains_secret AND state IN ('succeeded', 'dead', 'cancelled'))),
    CHECK (attempts <= max_attempts OR state IN ('dead', 'cancelled')));
CREATE INDEX jobs_dispatch ON merezha_jobs.jobs (queue, priority DESC, run_at, id) WHERE state = 'available';
CREATE INDEX jobs_leases   ON merezha_jobs.jobs (lease_expires_at)                  WHERE state = 'running';
CREATE INDEX jobs_running  ON merezha_jobs.jobs (queue)                             WHERE state = 'running';
CREATE INDEX jobs_dead     ON merezha_jobs.jobs (queue, finished_at, id)            WHERE state = 'dead';
CREATE INDEX jobs_finished ON merezha_jobs.jobs (finished_at) WHERE state IN ('succeeded', 'cancelled');

-- Keyed by lease: a released or replayed job reuses attempt numbers.
CREATE TABLE merezha_jobs.attempts (
    lease_token uuid PRIMARY KEY,
    job_id bigint NOT NULL REFERENCES merezha_jobs.jobs (id) ON DELETE CASCADE,
    attempt integer NOT NULL, lease_owner text NOT NULL,
    started_at timestamptz NOT NULL, finished_at timestamptz NULL,
    outcome text NULL CHECK (outcome IN ('succeeded', 'failed', 'lease_expired', 'released')),
    error_code text NULL, error_class text NULL);
CREATE INDEX attempts_job ON merezha_jobs.attempts (job_id, started_at);

CREATE TABLE merezha_jobs.schedules (
    name text PRIMARY KEY CHECK (name ~ '^[a-z][a-z0-9_.-]{0,63}$'),
    source text NOT NULL,                                       -- 'config' or a module name
    cron text NOT NULL, timezone text NOT NULL,
    queue text NOT NULL, kind text NOT NULL, payload json NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    next_run_at timestamptz NOT NULL, last_run_at timestamptz NULL, last_job_id bigint NULL,
    definition bytea NOT NULL,                                  -- sha256 of the declared definition
    updated_at timestamptz NOT NULL DEFAULT now());

CREATE TABLE merezha_jobs.admin_actions (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    at timestamptz NOT NULL DEFAULT now(),
    principal text NULL,
    action text NOT NULL,             -- replay | bulk_replay | cancel | purge_dead | schedule_pause | ...
    target text NOT NULL, count integer NOT NULL);
```

Dead letters stay in `jobs` with `state = 'dead'`, rather than moving to a separate table as in
Structure. A separate table would free the `(queue, idempotency_key)` slot, so a re-delivery could
create a duplicate job while the dead one sits unexamined. Partial indexes keep dead rows out of the
dispatch path.

## B.3 Outbox port (in `merezha-platform`, implemented by `merezha-jobs`)

```rust
pub struct OutboxRecord {
    pub dedupe_key: String,                 // ≤ 160 bytes (R4a), stable per logical effect
    pub target: OutboxTarget,
    pub payload: serde_json::Value,         // ≤ 65_536 encoded bytes, compatible_json
    pub contains_secret: bool,              // R3: payload nulled at relay, and at the job's terminal state
    pub run_at: Option<jiff::Timestamp>,
    pub priority: i16,
    pub context: MessageContext,            // correlation_id, causation_id, tenant
}
pub enum OutboxTarget {
    Event { topic: Identity },              // payload is an IntegrationEvent envelope
    Job { queue: Identity, kind: Identity },
}
pub enum OnDuplicate { Ignore, Fail }
pub struct Appended { pub inserted: usize, pub duplicates: usize }

pub trait Outbox: Send + Sync {
    /// Appends inside the caller's open transaction. Never commits, never opens a
    /// transaction, and issues NOTIFY merezha_jobs_outbox (delivered on commit).
    /// Stores the current span's trace context (TE's `send {queue}` producer span).
    fn append<'a>(
        &'a self,
        tx: &'a mut Tx<'_>,
        source: &'a str,
        records: &'a [OutboxRecord],
        on_duplicate: OnDuplicate,
    ) -> BoxFuture<'a, Result<Appended, Failure>>;
}
```

`Tx` is the `merezha-postgres` transaction; NT's `Notifier` takes the same type, so one transaction
carries the caller's rows, its outbox records and its notification requests. `source` is fixed per
module: the `AppContext` hands each module an `Outbox` wrapper bound to its name, so blocks cannot
spoof each other's dedupe namespace. The implementation issues one multi-row `INSERT ... SELECT *
FROM unnest(...) ON CONFLICT (source, dedupe_key) DO NOTHING RETURNING id`. With `Fail`, any
duplicate returns `outbox_duplicate` (conflict), and the caller's transaction is expected to roll
back. At most 256 records per call. A key over 160 bytes fails `invalid_dedupe_key` before the
INSERT, so `source:dedupe_key` (≤ 193 bytes) always fits the job key (R4a). Attempts, backoff and
concurrency are not record fields: they
come from `[jobs.queues.<name>]` at relay time (R1).

**Integration event envelope (R11).** An `Event` record's payload is the envelope, extending ID §8's
with `source`, `tenant` and `causation_id`:

```json
{"event_id": "identity:usr_01JB2X6Q9F3W:registered", "type": "identity.user_registered", "version": 1,
 "occurred_at": "2026-10-24T09:00:00Z", "source": "identity", "tenant": null,
 "correlation_id": "4bf92f3577b34da6a3ce929d0e0e4736", "causation_id": null,
 "payload": {"user_id": "usr_01JB2X6Q9F3W", "method": "password"}}
```

`type` equals the topic, `<module>.<aggregate>_<past verb>`; `event_id` is `{source}:{dedupe_key}`;
payloads carry ids, not personal data. A breaking change publishes `version + 1` beside the old one
for one release, its dedupe key suffixed `:v{n}`. `OutboxRecord::event(topic, IntegrationEvent)` builds the envelope. NT's webhook
fan-out consumes module events through `JobsHandle::subscribe`; modules never call `emit_webhook`
for their own domain events.

Other entry points, all on `JobsHandle` in `AppContext`:

```rust
impl JobsHandle {
    /// Own transaction, for callers without one (R1); idempotent on (queue, idempotency_key).
    /// Returns the id and whether it already existed. Same key + different payload digest ->
    /// Err(conflict "idempotency_mismatch").
    pub async fn enqueue(&self, job: NewJob) -> Result<Enqueued, Failure>;   // NewJob carries contains_secret
    pub async fn cancel(&self, id: i64) -> Result<bool, Failure>;   // available -> cancelled, own jobs only
    pub fn register(&self, kind: &str, queue: &str, handler: Arc<dyn JobHandler>, options: KindOptions);
    pub fn subscribe(&self, topic: &str, kind: &str);               // upserts merezha_jobs.subscriptions at start
    pub fn schedule(&self, schedule: ScheduleDeclaration);          // module-declared cron, synced at start
    pub async fn queue_status(&self) -> Result<QueueStatus, Failure>; // D.1 `GET queues`; read by TE (R10)
}
pub struct KindOptions {
    /// Replays a dead flagged job whose payload is gone by asking the owning module to issue
    /// the effect again. None: replay is refused (identity's token mail, whose token may be
    /// superseded, registers none).
    pub reissue: Option<Arc<dyn Reissue>>,
}
pub trait JobHandler: Send + Sync {
    fn handle<'a>(&'a self, ctx: JobContext, payload: serde_json::Value)
        -> BoxFuture<'a, Result<(), JobFailure>>;
}
pub struct JobContext {
    pub id: i64, pub kind: Identity, pub attempt: u32, pub max_attempts: u32,
    pub idempotency_key: String, pub deadline: tokio::time::Instant,
    pub cancel: tokio::sync::watch::Receiver<bool>,     // true on lease loss or drain cutoff
    pub correlation_id: Identity, pub causation_id: Option<Identity>, pub tenant: Option<Identity>,
}
pub struct JobFailure { pub code: String, pub classification: Classification, pub retry_after: Option<Duration> }
```

NT maps its handler outcomes onto this (R1): `Done` is `Ok(())`, `Retry { after, code }` is
`transient` with `retry_after = after`, `Permanent { code }` is `permanent`.

`cancel` only cancels a job the calling module owns: its key starts with `{source}:` (enqueued, or
relayed from the module's own `Job` records) or its kind is one the module registered. Any other id
returns `false`, whether the job exists or not. `enqueue` notifies `merezha_jobs` with the queue
inside its transaction, as the relay does. Subscriptions are declared, never deactivated by an instance's own registry, so rolling deploys
lose no events. Every instance upserts the subscriptions it declares at start and then every
`jobs.subscriptions.heartbeat` (default 5 min), setting `last_declared_at = now()` and
`deactivated_at = NULL`. A subscription is retired (`deactivated_at` set, the row kept) only when its
`last_declared_at` is older than `jobs.subscriptions.retire_after` (default 1 h, at least three
heartbeats); any instance runs the retirement on its heartbeat under
`pg_advisory_xact_lock(MZJB, hashtext('subscriptions'))`. The relay routes to every subscription
with `deactivated_at IS NULL`. During a deploy's grace window a subscription that any live version
declares keeps receiving events, so a restarting old instance never cuts off a new version's
subscription, and an instance that declares nothing retires nothing early.

`register` must run before `start`. A job whose kind has no handler on an instance is never leased
by that instance, because the lease query filters `kind = ANY($registered)`. During a rolling
deploy, old instances therefore leave new kinds alone.

## B.4 Relay

Woken by `LISTEN merezha_jobs_outbox` through the shared `Listener`, plus an interval
(`jobs.relay.interval_ms`, default 1000). It loops until a batch comes back short.

```sql
BEGIN;
WITH queues (queue, max_attempts) AS (
    SELECT * FROM unnest($2::text[], $3::int[])          -- resolved [jobs.queues.<name>] max_attempts
), batch AS (
    SELECT id, source, dedupe_key, record_kind, topic, queue, job_kind, priority, run_at, payload,
           contains_secret, correlation_id, causation_id, tenant, trace_parent, trace_state,
           record_kind = 'event'                           -- mirrors the jobs CHECKs (B.2)
             OR (queue ~ '^[a-z][a-z0-9_.-]{0,63}$' AND octet_length(job_kind) BETWEEN 1 AND 128
                 AND octet_length(source || ':' || dedupe_key) <= 200) AS routable
      FROM merezha_jobs.outbox
     WHERE relayed_at IS NULL AND quarantined_at IS NULL
     ORDER BY id
     LIMIT $1                                              -- jobs.relay.batch, default 100, max 1000
     FOR UPDATE SKIP LOCKED
), fanned AS (
    SELECT b.id, s.queue, s.job_kind AS kind, 'outbox:' || b.id || ':' || s.job_kind AS key,
           b.priority, b.run_at, b.payload, b.contains_secret,
           b.correlation_id, b.causation_id, b.tenant, b.trace_parent, b.trace_state
      FROM batch b JOIN merezha_jobs.subscriptions s ON s.topic = b.topic
     WHERE b.record_kind = 'event'
    UNION ALL
    SELECT b.id, b.queue, b.job_kind, b.source || ':' || b.dedupe_key,
           b.priority, b.run_at, b.payload, b.contains_secret,
           b.correlation_id, b.causation_id, b.tenant, b.trace_parent, b.trace_state
      FROM batch b
     WHERE b.record_kind = 'job' AND b.routable
), inserted AS (
    INSERT INTO merezha_jobs.jobs (queue, kind, idempotency_key, payload, payload_digest, contains_secret,
                                   priority, run_at, max_attempts, outbox_id, correlation_id, causation_id,
                                   tenant, trace_parent, trace_state)
    SELECT f.queue, f.kind, f.key, f.payload, sha256(convert_to(f.payload::text, 'UTF8')), f.contains_secret,
           f.priority, coalesce(f.run_at, now()), coalesce(q.max_attempts, $4 /* [jobs.defaults] */),
           f.id, f.correlation_id, f.causation_id, f.tenant, f.trace_parent, f.trace_state
      FROM fanned f LEFT JOIN queues q ON q.queue = f.queue
    ON CONFLICT (queue, idempotency_key) DO NOTHING
    RETURNING queue
), marked AS (
    UPDATE merezha_jobs.outbox o
       SET relayed_at = CASE WHEN b.routable THEN now() END,
           quarantined_at = CASE WHEN b.routable THEN NULL ELSE now() END,
           quarantine_code = CASE WHEN b.routable THEN NULL ELSE 'unroutable' END,
           payload = CASE WHEN o.contains_secret THEN NULL ELSE o.payload END   -- R3
      FROM batch b WHERE o.id = b.id
    RETURNING o.id, b.routable
)
SELECT (SELECT count(*) FROM marked WHERE routable) AS relayed,
       (SELECT count(*) FROM marked WHERE NOT routable) AS quarantined,
       (SELECT count(*) FROM batch b
         WHERE b.record_kind = 'event'
           AND NOT EXISTS (SELECT 1 FROM fanned f WHERE f.id = b.id)) AS unrouted,
       (SELECT array_agg(DISTINCT queue) FROM inserted) AS queues;
-- host: SELECT pg_notify('merezha_jobs', q) FROM unnest($queues::text[]) AS q;
COMMIT;
```

Data-modifying CTEs run exactly once and share the statement's snapshot, so `inserted` copies the
payload that `marked` nulls. An event with no subscription is marked relayed and counted in
`unrouted` (D.1 `GET outbox`). `SKIP LOCKED` lets several relays run on several instances, and the
job `ON CONFLICT` makes re-relaying harmless.

**Quarantine.** `routable` sets aside a job record the jobs table would refuse, so the rest of the
batch relays; a flagged record quarantined this way (`unroutable`) loses its payload (R3). Only a
data error attributable to a record, SQLSTATE classes 22 and 23, re-runs the batch one record at a
time and quarantines the record that fails alone with its error code (`constraint_violation`,
`invalid_record_data`). Such a record keeps its payload, flagged or not, so a requeue relays it once
the cause is fixed; R3 still nulls the resulting job's payload at its terminal state. Any other
error (a revoked privilege, a failing trigger, a missing table) is systemic: the batch aborts as a
transient relay failure, logged as `outbox.relay_failed` and retried at the interval, nothing is
quarantined and no payload is touched. Readiness follows the existing rules. Each quarantine counts in `merezha.outbox.quarantined` and logs `outbox.quarantined`;
quarantined rows also count in `merezha.outbox.pending` and `merezha.outbox.lag` until requeued
(D.1).

**Crash between commit and relay.** The row stays `relayed_at IS NULL`. Any instance's relay picks
it up at the next notification or interval after restart. Relay and mark happen in one transaction,
so a crash inside the relay rolls both back.

## B.5 Leasing, heartbeat, reaper

Each instance runs one dispatcher task per queue. It has `free` slots, the per-instance
`concurrency` minus jobs in flight. Wakeups come from `LISTEN merezha_jobs` (payload = queue), a
slot freeing, or `jobs.poll_interval_ms` (default 1000). It leases only when `free > 0`:

```sql
BEGIN;
-- only when the queue has global_concurrency > 0:
SELECT pg_advisory_xact_lock(1297762882 /* 'MZJB' */, hashtext($1));
WITH cap AS (
    SELECT CASE WHEN $6 > 0
                THEN greatest(0, least($3, $6 - (SELECT count(*) FROM merezha_jobs.jobs
                                                  WHERE queue = $1 AND state = 'running')))
                ELSE $3 END AS n
), candidate AS (
    SELECT id FROM merezha_jobs.jobs
     WHERE queue = $1 AND state = 'available' AND run_at <= now() AND kind = ANY($2)
     ORDER BY priority DESC, run_at, id
     LIMIT (SELECT n FROM cap)
     FOR UPDATE SKIP LOCKED
), leased AS (
    UPDATE merezha_jobs.jobs j
       SET state = 'running', attempts = j.attempts + 1, lease_token = gen_random_uuid(),
           lease_owner = $4, lease_expires_at = now() + make_interval(secs => $5 / 1000.0), updated_at = now()
      FROM candidate c WHERE j.id = c.id
    RETURNING j.id, j.kind, j.payload, j.attempts, j.max_attempts, j.lease_token, j.idempotency_key,
              j.correlation_id, j.causation_id, j.tenant, j.trace_parent, j.trace_state
), started AS (
    INSERT INTO merezha_jobs.attempts (lease_token, job_id, attempt, lease_owner, started_at)
    SELECT lease_token, id, attempts, $4, now() FROM leased
)
SELECT * FROM leased;
COMMIT;
```

`lease_owner` is `app.instance_id` (R12; default hostname plus process id, ≤ 128 bytes).
`gen_random_uuid()` is built in from PostgreSQL 13, below the 14 floor (R14). Each attempt runs in
TE's `process {queue}` span, a child of the stored context, so retries stay in one trace (TE §2.3).
**Every write afterwards is fenced on `lease_token`.**

**Success and failure.** With `$3` the next state and `$4` the delay (B.6), one statement settles
the job, nulls a flagged payload at a terminal state (R3) and closes the attempt:

```sql
WITH settled AS (
    UPDATE merezha_jobs.jobs
       SET state = $3, run_at = CASE WHEN $3 = 'available' THEN now() + $4 ELSE run_at END,
           finished_at = CASE WHEN $3 <> 'available' THEN now() END,
           payload = CASE WHEN contains_secret AND $3 <> 'available' THEN NULL ELSE payload END,
           last_error_code = coalesce($5, last_error_code), last_error_class = coalesce($6, last_error_class),
           last_error_at = CASE WHEN $5 IS NOT NULL THEN now() ELSE last_error_at END,
           lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
     WHERE id = $1 AND lease_token = $2
    RETURNING id
)
UPDATE merezha_jobs.attempts
   SET finished_at = now(), outcome = CASE WHEN $3 = 'succeeded' THEN 'succeeded' ELSE 'failed' END,
       error_code = $5, error_class = $6
 WHERE lease_token = $2 AND EXISTS (SELECT 1 FROM settled);
```

- **Heartbeat:** every `lease_ms / 3`, one statement for all of the instance's in-flight jobs:

  ```sql
  UPDATE merezha_jobs.jobs SET lease_expires_at = now() + $2
   WHERE lease_token = ANY($1) AND state = 'running'
  RETURNING lease_token;
  ```

  A token missing from the result means the lease was lost. The handler's `cancel` token fires, and
  its eventual result is discarded with `job.lease_lost` logged.
- **Reaper** (`jobs.reaper_interval_ms`, default 5000, any instance, `SKIP LOCKED`):

  ```sql
  WITH expired AS (
      SELECT id, lease_token FROM merezha_jobs.jobs
       WHERE state = 'running' AND lease_expires_at < now()
       ORDER BY lease_expires_at LIMIT 100 FOR UPDATE SKIP LOCKED
  ), reaped AS (
      UPDATE merezha_jobs.jobs j
         SET state = CASE WHEN j.attempts >= j.max_attempts THEN 'dead' ELSE 'available' END,
             run_at = now() + least(make_interval(secs => $1 * power(2, greatest(j.attempts - 1, 0))),
                                    make_interval(secs => $2)),
             finished_at = CASE WHEN j.attempts >= j.max_attempts THEN now() END,
             payload = CASE WHEN j.contains_secret AND j.attempts >= j.max_attempts THEN NULL
                            ELSE j.payload END,
             last_error_code = 'lease_expired', last_error_class = 'transient', last_error_at = now(),
             lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
        FROM expired e WHERE j.id = e.id
      RETURNING j.id
  )
  UPDATE merezha_jobs.attempts a
     SET finished_at = now(), outcome = 'lease_expired', error_code = 'lease_expired'
    FROM expired e WHERE a.lease_token = e.lease_token;
  ```

An expired lease counts as a failed attempt, because the attempt was already counted at lease time.
Backoff on reap has no jitter, which is enough to stop a job that crashes its process from looping
hot.

**Per-queue concurrency.** `concurrency` is per instance and enforced by the slot count.
`global_concurrency` is optional and cluster-wide, enforced by the transaction advisory lock plus a
count of running jobs. Use it for provider-rate-limited queues such as Brevo. Expired leases that
are not yet reaped count as running until the reaper runs, which errs on the safe side.

## B.6 Retry policy

| Handler outcome | Next state |
| --- | --- |
| `Ok(())` | `succeeded` |
| `permanent` | `dead` immediately |
| `transient` or `conflict`, `attempts < max_attempts` | `available`, `run_at = now() + delay` |
| `transient` or `conflict`, `attempts = max_attempts` | `dead` |
| Handler panic (`JoinError::is_panic`) | `dead`, code `handler_panic` (treated as poison) |
| Payload fails to decode | `dead`, code `invalid_job_payload` |
| Handler timeout (`timeout_ms`) | transient `timeout` |
| Lease lost | no write (fenced); the reaper or the new holder decides |
| Drain cutoff | `released` (B.8), attempt refunded |

Delay uses equal jitter, so it never collapses to zero:

```
base_delay = min(backoff_max, backoff_base * 2^(attempt - 1))
delay      = base_delay / 2 + uniform(0, base_delay / 2)       // CSPRNG from AppContext
delay      = max(delay, retry_after.min(backoff_max)) if the handler supplied retry_after
```

Defaults are `backoff_base = 1 s`, `backoff_max = 1 h` and `max_attempts = 10`: at most 511 s
(1 + 2 + … + 256, about 8.5 min) of delay before the dead letter. They are overridden only under
`[jobs.queues.<name>]` (R1). `retry_after` lets a module run its own ladder under the cap;
`notify.webhook` sets `backoff_max_ms = 86400000` for NT's 24-hour step.

## B.7 Delayed and cron jobs

Delayed jobs set `run_at` (a `jiff::Timestamp`), either through `NewJob.run_at`/`delay` or
`OutboxRecord.run_at`. The ceiling is 365 days.

**Cron:** `croner = { version = "=4.0.2", default-features = false, features = ["jiff"] }`, so cron
needs no `chrono` (R14). Checked with `cargo info` on 2026-10-24: croner 4.0.2 is MIT, and its `jiff`
feature depends on `jiff >= 0.2.35` (`alloc`), which the workspace pin `=0.2.38` satisfies; it
implements croner's `CronDateTime` for `jiff::Zoned`. The workspace jiff pin enables `tzdb-bundle-always`
(`jiff-tzdb` 0.1.9, Unlicense OR MIT) so `TimeZone::get` finds IANA zones on Linux, where
`tzdb-bundle-platform` bundles nothing and `tzdb-zoneinfo` is off. Croner was
chosen over `cron 0.17.0` because it handles DST gaps and overlaps OCPS/Vixie-style, supports `L`,
`W` and `#`, and makes seconds configurable. The parser is
`CronParser::builder().seconds(Seconds::Disallowed).build()`, so only 5-field expressions are
accepted and the finest granularity is one minute. `find_next_occurrence(&now.to_zoned(tz), false)`
gives the next run as a `Zoned`, stored as its `jiff::Timestamp`. Pinning `=` follows the workspace
style (`wasmtime`, `rusqlite`).

Schedules are declared in `merezha.toml` (`[[jobs.schedules]]`, source `config`) and by modules
(`JobsHandle::schedule`, source = the module name). They are upserted at start by name. A changed
`definition` digest recomputes `next_run_at` from now. Config-sourced rows missing from the current
config are set `enabled = false`, never deleted. At most 256 schedules.

The scheduler tick (`jobs.scheduler.interval_ms`, default 10 000) runs on any instance:

```sql
BEGIN;
SELECT * FROM merezha_jobs.schedules WHERE enabled AND next_run_at <= now()
 ORDER BY next_run_at LIMIT 32 FOR UPDATE SKIP LOCKED;
-- per row: INSERT INTO jobs (..., idempotency_key = 'cron:' || name || ':' || extract(epoch FROM next_run_at)::bigint,
--          max_attempts = the queue's, schedule_name, scheduled_for = next_run_at) ON CONFLICT DO NOTHING;
-- per row: UPDATE schedules SET last_run_at = next_run_at, last_job_id = $id,
--          next_run_at = $next_after_max_of_scheduled_and_now WHERE name = $name;
COMMIT;
```

Missed runs follow Structure's policy. One job runs for the due occurrence, and the next occurrence
is computed strictly after `max(scheduled, now)`, so downtime never causes a burst.

## B.8 Graceful drain

The platform calls `shutdown()` after readiness turns false and HTTP stops accepting:

1. Stop the dispatchers, the relay, the scheduler and the reaper. No new leases are taken.
2. Keep heartbeating in-flight jobs. Wait up to `jobs.drain_timeout_ms` (default 20 000, which must
   be below the platform's module drain budget).
3. Fire `cancel` for every still-running handler and give a 2 s grace.
4. Release the remaining jobs in one fenced statement:

   ```sql
   WITH released AS (
       UPDATE merezha_jobs.jobs
          SET state = 'available', run_at = now(), attempts = attempts - 1,
              lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
        WHERE lease_token = ANY($1)
       RETURNING id
   )
   UPDATE merezha_jobs.attempts SET finished_at = now(), outcome = 'released'
    WHERE lease_token = ANY($1);
   ```

   Late handler writes are fenced out. The error log `job.drain_abandoned` lists the job ids.
5. Close the listener subscription.

Releasing refunds the attempt, so deploys never push jobs toward a dead letter.

## B.9 Retention

Purges run hourly, on any instance, in batches of 1000, each batch's transaction under
`pg_try_advisory_xact_lock(MZJB, hashtext('retention'))` (pooled connections take no session locks,
A.7). Quarantined outbox rows follow `jobs.retention.dead`:

| Kept rows | Default | Key |
| --- | --- | --- |
| Succeeded and cancelled jobs | 7 days | `jobs.retention.succeeded` |
| Dead jobs | 30 days (only when not replayed) | `jobs.retention.dead` |
| Relayed outbox rows | 7 days | `jobs.retention.outbox` |
| Admin actions | 365 days | `jobs.retention.audit` |

The dedupe window equals retention. Producers whose keys must stay unique for longer must check
their own state. Reactions do, through reactor checkpoints (C.5). Most secrets do not wait for
retention: flagged payloads are NULL once relayed, quarantined as `unroutable` or terminal (R3).
The exception is a flagged record isolated by its own data error (B.4): it keeps its payload until
it is requeued and relayed, or until `jobs.retention.dead` purges the quarantined row.

## B.10 Telemetry, readiness, pool

This design defines no metric or log names (R9): jobs use TE §1.3's `merezha.job.*` instruments and
state names, and TE §4's `job.*` and `reactor.*` log events (`job.lease_lost`,
`job.drain_abandoned`, `job.admin_action`, `reactor.halted`, `reactor.effect_rejected` among them),
whose `names.rs` rows land in the emitting slice. Logs never carry payloads. Queue and outbox gauges belong to
TE-T5, over `JobsHandle::queue_status()`.

Readiness: migrations are current and the dispatchers are started. Job backlog is not a readiness
input unless `jobs.readiness.max_lag_ms` is set; it is unset by default (R10).

Pool needs: 1 connection per active lease, heartbeat or relay statement. They are short, so `2 +
number of queues` is enough headroom. Handlers that need a database connection take their own.

---

# C. Guest protocol 2, route permissions, reactions

## C.1 Declaring the protocol

The runtime manifest's `[[services]]` gains an optional field. The manifest stays at `version = 1`,
because the addition is backwards compatible:

```toml
[[services]]
id = "task-domain"
path = "components/task-domain.wasm"
application_protocol = 2          # 1 (default) or 2; any other value: invalid_manifest
```

- `merezha_runtime::Catalog` gains `protocols: BTreeMap<String, u16>`, and
  `Session::protocol(service) -> u16` reads the session's pinned generation. The runtime itself
  still never interprets payloads. A reload can change the version, and each dispatch uses the
  version of the generation it pinned.
- At startup, and on the first dispatch after each reload (cached per generation and service), the
  host sends `{"version":2,"body":{"op":"describe"}}` to every service declared as protocol 2. The
  expected reply is `{"kind":"described","protocol":2}`. A version 1 guest returns
  `protocol_version`, and a startup mismatch fails with `service_protocol_mismatch`. After a reload,
  dispatch to that service fails with the same permanent code, and the previous history is
  untouched.
- Host requirements: services named by `[[reactors]]` must be protocol 2
  (`reactor_requires_protocol_2` at startup). Tenant scopes and partitioned projections work with
  version 1 guests too, because the host enforces them and a version 1 guest simply never sees the
  principal.

## C.2 Wire changes

The request and response envelopes keep their shape. `version` is 1 or 2, and the response must echo
the request's version, otherwise the host fails `protocol_version`.

**Metadata (v2):**

```json
{
  "message_id": "9f2c41d07a5e4b3c8d1e6f0a2b3c4d5e",
  "correlation_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "causation_id": "event/41",
  "actor": "usr_01JB2X6Q9F3W",
  "principal": {
    "subject": "usr_01JB2X6Q9F3W",
    "kind": "user",
    "tenant": "org_01JB2X7",
    "roles": ["member"]
  }
}
```

- `principal` is optional, set only by the host from K's `Principal` (R13). `kind` is `user`,
  `api_key` or `system`; `tenant` is optional. `roles` holds the role keys at the route's scope, at
  most 16 identities of at most 64 bytes, sorted and deduplicated. The grant snapshot and
  `SessionFacts` are never sent.
- When a principal is present, `actor` equals `principal.subject`. System subjects are `sys_<name>`;
  a reactor is `sys_reactor_<name>` (R13). A client-supplied `actor` that differs fails
  `actor_mismatch` (403, R6).
- Missing `correlation_id` and `message_id` are filled by the HTTP adapter (AP4, A.6).
- Version 1 guests receive metadata with `principal` removed: in decide/query metadata and in every
  `StoredEvent.metadata` of `history` and `events`. The version 1 SDK's `deny_unknown_fields` would
  otherwise reject it. That makes the host downgrade mandatory and covered by tests.
- Stored event metadata keeps `{subject, kind, tenant}` and **drops `roles`**, so audit data stays
  minimal (F). With `#[serde(default, skip_serializing_if = "Vec::is_empty")]`, the roles field
  disappears from stored JSON.

**Operations, v2 additions:**

| Op | v2 change |
| --- | --- |
| `validate` | unchanged; no metadata. Permission checks have already run. |
| `decide` | `metadata.principal` |
| `project` | optional `partition` (tenant id) for partitioned projections; event metadata as stored |
| `query` | `metadata.principal`; `state` is the tenant partition for tenant-scoped routes |
| `react` | new, below |
| `describe` | new; answered automatically by the SDK |

**`react`** carries events only. Reactors cannot read projection state (AP3); NT resolves recipients.

```json
{"version":2,"body":{"op":"react","reactor":"task-notifier","events":[ <StoredEvent>, ... ]}}
```

```json
{"version":2,"outcome":{"status":"ok","reply":{"kind":"reacted","reactions":[
  {"event_id":"event/41","effects":[
    {"effect":"notify","topic":"tasks.completed","recipients":[{"user":{"id":"usr_01JB2X6Q9F3W"}}],
     "channels":["inbox","email"],"data":{"title":"Write docs"}},
    {"effect":"webhook","event_type":"tasks.task_completed","data":{"task_id":"t-1"}},
    {"effect":"command","tag":"ArchiveTask","stream":"t-1","payload":{},"delay_ms":604800000}
  ]},
  {"event_id":"event/42","effects":[]}
]}}}
```

Effect schema, NT's vocabulary (R4, NT §3.3), every type `deny_unknown_fields`:

| `effect` | Fields |
| --- | --- |
| `notify` | `topic` (declared in NT); `recipients`, 1–100, each `{"user":{"id"}}`, `{"tenant":{"id","roles"?}}`, `{"application":{"roles"}}` or, only with `allow_addresses = true`, `{"address":{"email"}}` / `{"address":{"phone"}}`; optional `channels` ⊆ the topic's (`email`, `sms`, `push`, `inbox`); `data` object; optional `delay_ms` |
| `webhook` | `event_type` (declared in NT's catalog, `<module>.<aggregate>_<past verb>`); `data` object. No URL: NT delivers to the registered endpoints of the event's tenant, or to application-level endpoints for an event without tenant (R11). |
| `command` | `tag`, `stream` (local id; the host composes it), `payload`, optional `expected_version`, optional `delay_ms` |

`delay_ms` is 0 to 2,592,000,000 (30 days). Guests never set the dedupe key, the tenant or the
source event id; the host derives them from the event.

The reply is checked in two tiers (R4):

- **Malformed** (`invalid_response`; the reactor halts, C.5): the reply does not decode, the version is wrong, there is
  not exactly one reaction per input event in order with matching `event_id`, an event has more than
  8 effects or the batch more than 64, or an effect exceeds `max_payload_bytes` or fails
  `compatible_json`.
- **Rejected** (recorded in `reactor_rejections`, the reactor advances): a well-formed effect whose
  type is not in the reactor's `effects`, a command tag outside `commands`, an `address` recipient
  without `allow_addresses`, a `tenant` recipient other than the event's tenant, a `tenant`
  recipient on an event without tenant (`tenant_required`), or any `NotifyError` from NT,
  including `recipient_not_in_tenant`.

## C.3 SDK changes

**`crates/domain` (`merezha-domain`):**

- Constants: `PROTOCOL_V1 = 1`, `PROTOCOL_V2 = 2`, `LATEST_PROTOCOL = 2`. `PROTOCOL_VERSION` remains
  `1` with `#[deprecated(note = "use PROTOCOL_V1")]`, so existing hosts and tests keep their
  meaning.
- `Request::new(op)` still builds version 1. New `Request::with_version(v, op)`. `Request::decode`
  accepts 1 or 2. Under version 1 it rejects `React`, `Describe`, `project.partition` and a present
  `metadata.principal` with `protocol_version`.
- `Response::decode` keeps v1. New `Response::decode_version(input, expected)` for the host.
  `Response::new_with_version(v, result)`.
- `handle(input, dispatch)` keeps its signature. It echoes the request version in the response and
  answers `describe` itself with `Reply::Described { protocol: 2 }` without calling `dispatch`.
- New types:

  ```rust
  pub struct Principal {
      pub subject: Identity,
      pub kind: PrincipalKind,
      pub tenant: Option<Identity>,
      pub roles: Vec<Identity>,
  }
  pub enum PrincipalKind { User, ApiKey, System }
  #[serde(tag = "effect", rename_all = "snake_case", deny_unknown_fields)]
  pub enum Effect {
      Notify { topic: Identity, recipients: Vec<Recipient>, channels: Option<Vec<Channel>>,
               data: Map<String, Value>, delay_ms: Option<u64> },
      Webhook { event_type: Identity, data: Value },
      Command { tag: Identity, stream: Identity, payload: Value,
                expected_version: Option<u64>, delay_ms: Option<u64> },
  }
  pub enum Recipient { User { id: Identity }, Tenant { id: Identity, roles: Option<Vec<Identity>> },
                       Application { roles: Vec<Identity> }, Address(Address) }
  pub enum Channel { Email, Sms, Push, Inbox }
  pub struct Reaction { pub event_id: Identity, pub effects: Vec<Effect> }
  ```

  `Operation::React { reactor, events }`, `Operation::Describe`, `Operation::Project { ..,
  partition: Option<Identity> }` (with `serde(default)`), `Reply::Reacted { reactions }` and
  `Reply::Described { protocol }`. `Metadata.principal: Option<Principal>` uses `serde(default,
  skip_serializing_if = "Option::is_none")`, so version 1 JSON is byte-identical.
- Helpers: `Metadata::principal()`, `Metadata::require_subject() -> Result<&Identity, Failure>`
  (`unauthenticated`), `Principal::has_role(&str)` and `Principal::require_tenant()`.
- Recompiling against the new SDK is a source break for guests that match `Operation` exhaustively,
  because of the new variants. This is acceptable at 0.x, and the changelog shows the one-line
  `_ =>` arm. Guests still built on the old SDK keep working as version 1.

**`services/kit`: no change.** It is the JSON helper for raw `merezha:service/handler` services
(normalize, greet), which protocol 2 does not touch. The application examples change instead:

- `task-events`: `TaskCreated` gets schema version 2 with `owner: Option<Identity>`, and decoding
  accepts versions 1 and 2.
- `task-domain` (`application_protocol = 2`): `CreateTask` records `owner = principal.subject`.
  `RenameTask` and `CompleteTask` require `subject == owner` or role `admin`, else `not_owner`
  (permanent). Version 1 history without an owner stays open to any member.
- New `services/task-notifier`: a reactor. `TaskCompleted` notifies the owner on topic
  `tasks.completed` (`inbox`, `email`) and requests an `ArchiveTask` command with `delay_ms = 7
  days`. It has contract tests like task-domain's.
- `scenario`: runs against PostgreSQL with two principals in two tenants and checks isolation,
  ownership and the reaction outcome.

## C.4 Route permissions and tenant scope (application.toml version 2)

```toml
version = 2

[[permissions]]
name = "tasks.create"
description = "Create tasks"

[[permissions]]
name = "tasks.read"
description = "Read tasks"

[[commands]]
tag = "CreateTask"
service = "task-domain"
aggregate = "Task"
permission = "tasks.create"      # or: access = "authenticated" | "public" (xor permission)
scope = "tenant"                 # application (default) | tenant
resource = "task"                # optional, commands only: resource kind from ID

[[queries]]
tag = "ListTasks"
service = "task-view"
projection = "tasks"
permission = "tasks.read"
scope = "tenant"

[[projections]]
name = "tasks"
service = "task-view"
initial = { tasks = {} }
revision = 2
partition = "tenant"             # none (default) | tenant
```

This design owns the syntax and its enforcement (R6, slice C2); identity ships no route adapter.
The keys sit on `[[commands]]`/`[[queries]]` because commands are tags dispatched through
`/v1/application/commands`. They map to K's `ResourceScope`: `Application`,
`Tenant(principal.tenant)`, or `Resource { tenant, kind, id }` with `id` the local stream id.

Validation, all failing startup with every error listed at once:

- Each route sets exactly one of `permission` or `access`. When identity is enabled (its
  `[modules.identity]` section is present, R12), a route with neither fails
  `route_access_undeclared`. Without identity, the edge keeps `AllowAll` on
  loopback only, as framework.md states.
- A `permission` must be in the catalog: the union of `[[permissions]]` here and the block
  permissions from `merezha-platform`. Application permission names must not use a block prefix
  (`identity.`, `content.`, `notifications.`, `jobs.`, `application.`, `system.`).
- A tenant-scoped query must reference a `partition = "tenant"` projection. Application-scoped
  queries must not reference one. `resource` requires `scope = "tenant"`.
- With any tenant scope, aggregate names must not contain `:`.
- The composed stream must fit 128 bytes: `len(tenant) ≤ 40` + 1 + `len(aggregate)` + 1 +
  `len(stream)`. A stream id that is too long fails `validation_failed` per request.

Enforcement order for a command:

1. The edge `Authenticator` resolves the `Principal`, or none.
2. `access = "public"` passes. `authenticated` requires a principal (else `unauthenticated`, 401).
   `permission` requires a principal, then `Authorizer::check(principal, permission, scope)` with
   `ResourceScope::Tenant(principal.tenant)` (or `Resource`) for tenant routes (no tenant fails
   `tenant_required`, 403) or `ResourceScope::Application` otherwise. `platform_admin` satisfies
   every check (R5). A denial fails `unauthorized` (403).
3. The host composes `"{tenant}:{aggregate}/{stream}"` from **`principal.tenant`**. The request body
   only ever names the local stream id, so cross-tenant addressing is impossible by construction.
   The `streams` CHECK constraint backs this up.
4. Guest `validate`, the payload-level `Authorizer::authorize`, idempotency with `Key.tenant =
   principal.tenant`, `decide`, then append with `p_tenant`.

All of this runs before the guest validates and before any receipt is replayed, as framework.md
requires. Unauthorized callers cost no guest fuel.

The trusted entry point is `Admission::command_as(principal, command)` and `query_as(principal,
query)`. The existing `command`/`query` methods mean "anonymous" and reject a `metadata.principal`
supplied in the body with `principal_not_trusted`. That way a caller can never inject a principal
through JSON.

Error codes (R8) use K's canonical names, `unauthenticated` 401 and `unauthorized`, `tenant_required`
and `actor_mismatch` 403, and this design registers its own in K's error registry at startup:
`projection_warming` and `projection_behind` 503 with `Retry-After: 1`; `rebuild_in_progress`,
`revision_active`, `not_dead`, `not_cancellable` and `secret_purged` 409. Unregistered permanent
codes (`principal_not_trusted`, `not_owner`) stay 422. Field-level validation errors use the
optional `details` of FW's error shape.

## C.5 Reactors (application.toml version 2)

```toml
[[reactors]]
name = "task-notifier"             # identity ≤ 48 bytes (keeps derived keys ≤ 128)
service = "task-notifier"          # must be application_protocol = 2
events = ["TaskCompleted"]         # tag filter applied by the host; [] = all
effects = ["notify", "command"]    # allowed effect types
commands = ["ArchiveTask"]         # allowed command tags for command effects
permissions = ["tasks.archive"]    # granted to sys_reactor_task-notifier
allow_addresses = false            # address recipients permitted? (default false, R4)
start = "head"                     # head (default) | beginning; used only when the row is created
on_error = "halt"                  # halt (default) | skip
```

Runner: the same scheduler, triggers, `r:` advisory lock and bounds as projections. Per batch:

1. Read up to `batch_size` events after the checkpoint.
2. Keep those whose tag is in `events`. If none match, CAS-advance the checkpoint with no guest
   call.
3. Call the guest `react` outside SQL, check the reply (C.2) and run NT's pure
   `Notifier::validate` on each effect, so the transaction only writes.
4. Before any transaction (R4), for each `notify` effect on a tenant event, call
   `Directory::is_member(user, tenant)` for its `user` recipients (pairs deduplicated per batch) and
   fill `NotificationRequest.members` with the confirmed users; `request` rejects a user left out
   with `recipient_not_in_tenant`. A Directory failure is transient (failure table).
5. Run one transaction on the run's detached connection (A.7); no call in it reaches another port:

   ```sql
   BEGIN;
   SELECT checkpoint, row_revision, status FROM merezha_app.reactors WHERE name = $1 FOR UPDATE;
   -- abort unless row_revision = $expected AND checkpoint = $batch_start AND status <> 'paused'
   -- per accepted effect, in order, on this transaction:
   --   notify  -> Notifier::request(tx, NotificationRequest { .. })
   --   webhook -> Notifier::emit_webhook(tx, WebhookEvent { .. })
   --   command -> Outbox::append(tx, "application", &[record], OnDuplicate::Ignore)
   -- a NotifyError other than Unavailable from request/emit_webhook rejects that effect
   INSERT INTO merezha_app.reactor_rejections (name, position, index, effect, code)
   SELECT $1, * FROM unnest($r_positions::bigint[], $r_indexes::smallint[], $r_effects::text[],
                            $r_codes::text[])
   ON CONFLICT DO NOTHING;
   UPDATE merezha_app.reactors
      SET checkpoint = $batch_end, row_revision = row_revision + 1, status = 'active',
          halted_position = NULL, failures = 0, failure_code = NULL, failure_class = NULL,
          retry_at = NULL, updated_at = now()
    WHERE name = $1;
   COMMIT;
   ```

AP never writes NT's jobs (R4); NT's records commit or roll back with the checkpoint.

| Effect | Call | Built from the event |
| --- | --- | --- |
| notify | `Notifier::request` | the event's tenant; `not_before = now() + delay_ms`. Without the notifications block, the capture sink logs a redacted record. |
| webhook | `Notifier::emit_webhook` | the event's tenant, or application level (R11); `source_event_id` = the dedupe key; `occurred_at` = `accepted_unix_ms` |
| command | `Outbox::append`, job `application`/`application.command` | `run_at = now() + delay_ms`; handler below |

Every effect uses dedupe key `reaction:{reactor}:{event_id}:{index}` and source `application` (R4),
`index` being the effect's zero-based position: framework.md's `(event_id, index)` plus the reactor,
since two reactors can react to one event. The context is the event's correlation id,
`causation_id = event_id` and the stream's tenant.

**Tenant safety of effects.** On a tenant event, `user` recipients must be members (step 4 fills
`members` from `Directory::is_member`, and NT refuses any user outside it), a `tenant` recipient
must name that tenant, and webhooks reach only its
endpoints. `application` recipients resolve through `Directory::application_members`. Command
effects run as `{subject: "sys_reactor_{name}", kind: system, tenant: event tenant, roles: []}`
(R13), holding only the reactor's `permissions`, so tenant routes compose the same tenant. Events
without tenant accept `user`, `application` and allowed `address` recipients (NT §3.3),
application-level webhooks and application-scoped command routes.

**`application.command` handler.** It runs `Admission::command_as` inside `spawn_blocking` with:

- `idempotency_key = "rx:{reactor}:{position}:{index}"`, ≤ 128 bytes;
- `message_id` equal to the same string;
- `causation_id = event_id`;
- `correlation_id` from the event.

Result mapping:

| Command result | Job outcome |
| --- | --- |
| `Ok` or a replayed receipt | succeeded |
| `concurrency_conflict` | conflict: retry (the guest decides again on fresh history) |
| other conflict or transient | retry |
| permanent, including `idempotency_mismatch` and `unauthorized` | dead letter |

Duplicate job delivery is harmless, because the receipt replays.

**Failure semantics** (R4): a broken guest stops the reactor, a bad effect does not.

| Failure | Behaviour |
| --- | --- |
| Transient guest failure (`timeout`, `overloaded`, `trap` classified transient), storage failure, a Directory failure in step 4, or `NotifyError::Unavailable` | Checkpoint not advanced; `failures += 1`; `retry_at = now() + equal-jitter backoff(1 s .. 5 min)`; status stays `active` |
| Permanent guest failure or malformed reply on a batch of more than one event | Retry the same range with batch size 1 to isolate the poison event |
| Malformed reply on a single event (any `on_error`), or permanent guest failure with `on_error = "halt"` | `status = 'halted'`, `halted_position = position`, error log `reactor.halted`. The runner retries at `retry_at` with the same backoff, so a fixed guest and a reload resume it; an admin can `retry` now or `skip`. |
| Permanent guest failure on a single event, `on_error = "skip"` | Insert `reactor_skips (reason = 'on_error_skip')` and advance the checkpoint past it in the same transaction |
| Well-formed effect rejected (C.2) | Row in `reactor_rejections`, log `reactor.effect_rejected`; the other effects are written and the checkpoint advances |
| CAS mismatch (lock lost) | Reload and continue; the dedupe keys absorb any overlap |

**Replay and rebuild.**

- Reactors have **no rebuild** and are never driven by projection catch-up or rebuild. Projection
  rebuilds never touch `merezha_app.reactors`. They read the feed only. So rebuilding a projection
  cannot re-fire a reaction.
- Each event is reacted to at most once per reactor checkpoint advance. Checkpoint, outbox rows and
  NT's requests commit together. A crash before commit re-runs the batch, and nothing was written. A
  crash after commit has nothing to redo. Effects are delivered at least once by jobs.
- A new reactor starts at `head` by default, so adding a reactor never emails the whole history.
  `start = "beginning"` must be opted into explicitly.
- Removing a reactor from config leaves its row. Re-adding it resumes from the old checkpoint, and
  the admin listing shows the gap (`head − checkpoint`). An operator can `skip` to head (`reason =
  'skip_to_head'`) before re-enabling.
- There is no admin rewind. Should a checkpoint ever move backwards (a restore from backup), the
  outbox `(source, dedupe_key)` uniqueness, which NT's requests also go through, suppresses
  duplicates within outbox retention (7 days). Older duplicates are possible, so the operations
  guide states that a restore must keep `merezha_jobs`, `merezha_notify` and `merezha_app` from the
  same snapshot.
- Memory and SQLite backends reject `[[reactors]]` with `reactors_require_postgres`, because there
  is no outbox there.

**Bounds.** At most 32 reactors and `batch_size` events per call. Effect limits are in C.2. A single
reactor runs on at most one instance at a time (advisory lock).

---

# D. Admin HTTP API and configuration

## D.1 Admin API

Errors use FW's shape `{"error":{"code","classification","details"?}}` (R8). List endpoints take
`limit` (default 50, max 200) and an opaque `after` cursor (the last id). With identity, admin
routes require the named permission at `ResourceScope::Application` (R5); without it, they exist
only on loopback (`AllowAll`). Durations are in seconds (R9).

**Application** (`/v1/application/`; permissions `application.admin.read` and
`application.admin.write`):

| Method and path | Response |
| --- | --- |
| `POST projections/{name}/catch-up` | unchanged: `{state, checkpoint}` (partitioned: `{checkpoint, head}`) |
| `POST projections/{name}/rebuild` | memory/SQLite: unchanged synchronous. PostgreSQL: `202 {"rebuild":{"name","revision","target"}}`; body `{}` or `{"resume":true}` |
| `DELETE projections/{name}/rebuild` | `204`; abandons a failed or running rebuild (the runner stops at the next batch) |
| `GET projections` | `{"head":N,"projections":[{"name","revision","partitioned","checkpoint","lag","warm","status","failure_code","rebuild":{"checkpoint","target","status"}?,"updated_at"}]}` |
| `GET projections/{name}` | one entry, plus `"revisions":[{"revision","checkpoint","warm"}]` |
| `DELETE projections/{name}/revisions/{revision}` | `204`; refused (`409 revision_active`) for the configured revision |
| `GET reactors` | `{"head":N,"reactors":[{"name","checkpoint","lag","status","halted_position","failure_code","failures","retry_at","rejected"}]}` |
| `GET reactors/{name}/rejections` | `{"rejections":[{"position","index","effect","code","rejected_at"}],"next":cursor?}` |
| `POST reactors/{name}/pause` and `/resume` | `{"name","status"}` |
| `POST reactors/{name}/retry` | retries a halted reactor now |
| `POST reactors/{name}/skip` | body `{"position":P}` with `P = checkpoint + 1` (exactly the stuck event) or `{"to":"head"}`; records `reactor_skips` with the principal; `{"name","checkpoint"}` |
| `GET status` | `{"application","head","projections":[{"name","checkpoint","lag_events","lag_seconds","status","failure_code"}],"reactors":[{"name","status","lag_events"}],"instance":{"id","background_runs"},"listener"}`, where `listener` is `connected` or `reconnecting` |

`GET status` serves `Application::status()` and `GET queues`/`GET outbox` serve
`JobsHandle::queue_status()`; TE's `/v1/admin/status` embeds the same documents (R10). Writes need
`application.admin.write`.

**Jobs** (`/v1/jobs/admin/`; permissions `jobs.admin.read` and `jobs.admin.write`):

| Method and path | Response |
| --- | --- |
| `GET queues` | `{"queues":[{"name","ready","delayed","leased","dead","oldest_ready_age_s","concurrency","global_concurrency"}]}`, TE's state names |
| `GET jobs?queue=&state=&kind=&limit=&after=` | `{"jobs":[{"id","queue","kind","state","attempts","max_attempts","run_at","last_error_code","last_error_class","contains_secret","created_at","finished_at","replays"}],"next":cursor?}`. Payloads are never listed. |
| `GET jobs/{id}` | as above plus `"attempts":[{"attempt","started_at","finished_at","outcome","error_code"}]`. `?payload=true` adds `payload` and needs `jobs.admin.payload`; a flagged job's payload is never returned. |
| `POST jobs/{id}/replay` | dead to `available`: `attempts = 0`, `run_at = now()`, `replays += 1`, error fields kept; `{"id","state"}`. A flagged dead job has no payload (R3): the kind's `reissue` hook asks the owning module to issue it again (`"state":"reissued"`), else `409 secret_purged`. `409 not_dead` otherwise. |
| `POST dead/replay` | body `{"queue", "kind"?, "code"?, "dead_after"?, "dead_before"?, "limit": ≤1000}`; `{"replayed":N,"reissued":R,"refused":M}` |
| `POST dead/purge` | body `{"queue", "before"}`; `{"purged":N}` |
| `POST jobs/{id}/cancel` | `available` to `cancelled` (a flagged payload is nulled); `409 not_cancellable` when running or terminal |
| `GET outbox` | `{"pending","oldest_age_s","unrouted","quarantined":[{"id","source","record_kind","quarantine_code","quarantined_at"}]}` (`unrouted`: this instance's count since start; at most 100 quarantined rows, oldest first) |
| `POST outbox/{id}/requeue` | a quarantined record back to pending once the cause is fixed; `{"id"}`. `409 secret_purged` for a flagged record quarantined as `unroutable`, whose payload is gone (R4a): the owning module issues it again. |
| `GET schedules` | `{"schedules":[{"name","source","cron","timezone","enabled","next_run_at","last_run_at","last_job_id"}]}` |
| `POST schedules/{name}/pause`, `/resume`, `/trigger` | `trigger` enqueues a job keyed `cron:{name}:manual:{unix_ms}` |

Every write appends to `merezha_jobs.admin_actions` and emits the log event `job.admin_action`.
Replay starts a new trace that links to the stored context (TE §2.3).

## D.2 Configuration keys (`merezha.toml`)

Each key can be overridden with `MEREZHA_<SECTION>__<KEY>`, for example
`MEREZHA_JOBS__POLL_INTERVAL_MS`. Durations use K's type (R12): `_ms` keys take integer
milliseconds or a string such as `"5s"`, retention keys a string such as `"7d"`. `[jobs]` is K's
top-level section, with no `enabled` key; jobs run whenever the database is configured.

```toml
[runtime]
manifest = "runtime.toml"    # K's key
[application]
path = "application.toml"    # K's key

[application.projections]
catch_up = "auto"            # auto | manual (manual: only the admin endpoints run catch-up)
interval_ms = 5000           # 100 ..= 600000
run_timeout_ms = 30000       # 1000 ..= 300000, per background run
warm_readiness = true

[application.reactors]
interval_ms = 5000
retry_base_ms = 1000
retry_max_ms = 300000

[application.background]
max_concurrent = 2           # 1 ..= 16, shared by projection, rebuild and reactor runs

[application.append]
lock_timeout_max_ms = 5000   # 10 ..= 30000

[jobs]
workers = true               # false: enqueue, relay and outbox only (an "api" role)
poll_interval_ms = 1000      # 100 ..= 60000
reaper_interval_ms = 5000
drain_timeout_ms = 20000

[jobs.relay]
interval_ms = 1000
batch = 100                  # 1 ..= 1000

[jobs.scheduler]
interval_ms = 10000

[jobs.defaults]
max_attempts = 10            # 1 ..= 100
backoff_base_ms = 1000
backoff_max_ms = 3600000
lease_ms = 30000             # 1000 ..= 600000; heartbeat = lease_ms / 3
timeout_ms = 30000           # 1000 ..= 900000

[jobs.queues.default]
concurrency = 8              # per instance; total across queues ≤ 256
global_concurrency = 0       # 0 = unlimited
# max_attempts, backoff_base_ms, backoff_max_ms, lease_ms, timeout_ms may override defaults

[jobs.queues.application]
concurrency = 4

[jobs.queues."notify.email"]   # NT §7.2's values
concurrency = 8
max_attempts = 10
backoff_base_ms = 5000

[jobs.queues."notify.webhook"]
concurrency = 16
max_attempts = 10
backoff_base_ms = 1000
backoff_max_ms = 86400000    # NT's 24-hour step (R1)

[jobs.subscriptions]
heartbeat = "5m"             # every instance re-declares its subscriptions
retire_after = "1h"          # retire one nobody declared for this long; >= 3 x heartbeat

[jobs.retention]
succeeded = "7d"
dead = "30d"
outbox = "7d"
audit = "365d"

[jobs.readiness]
# max_lag_ms unset: backlog is not a readiness input (R10)

[[jobs.schedules]]
name = "tasks.reminders"
cron = "0 9 * * mon-fri"
timezone = "Europe/Paris"
queue = "default"
kind = "tasks.send_reminders"
payload = {}
```

NT's other queues are configured the same way (NT §7.2). A queue referenced by a registered kind
but missing from config gets the defaults with `concurrency = 2`, and a startup warning names it.
Unknown keys fail startup, as the kernel requires.

---

# E. Delivery slices

Every slice is one pull request. Each ships with tests against real PostgreSQL through
`merezha_postgres::TestDatabase` (an explicit skip when `MEREZHA_TEST_DATABASE_URL` is unset; CI
sets it with a PostgreSQL 16 service container), documentation exercised as written, an
independent review and green CI. Process tests spawn the real `merezha run` binary on ephemeral
loopback ports, as `sqlite_process.rs` does, and crash points use the test-only
`MEREZHA_PG_CRASH_CUT` hook. Every slice adds the TE `names.rs` rows for the names it emits (R9)
and registers its error codes (R8).

Order follows cross-block-decisions.md: K2 first; stage 1 opens with A1 and B1 beside TE-T1, then
A2, B2, B3 (ID PR 1 needs it) and A3; stage 3 runs C1 to C3. There is no A4 (AP2).

| Slice | Scope | Depends on |
| --- | --- | --- |
| A1 | `DurableStore` seam, `Key.tenant`, `Limits` presence and `max_stream_events`, config version 2 parsing (permissions, scope and reactors rejected `unsupported_in_this_build`), `store_contract.rs` on SQLite | K2 |
| A2 | Migration 0001 `merezha_app` with the trace columns, `PostgresStore`, `block_on` integration, error mapping and registration, manifest, `merezha run` wiring, `merezha application verify` | A1 |
| A3 | Scheduler, `Listener`, advisory locks, background rebuild, revisions and warm readiness, `min_position`, application admin API and `Application::status()` | A2 |
| B1 | `merezha-jobs`, migration 0001 `merezha_jobs` with `contains_secret` and the trace columns, `Outbox` port and envelope, `JobsHandle` enqueue/cancel/subscribe/register, relay | K2 |
| B2 | Dispatcher, leasing, heartbeat, reaper, retry, global concurrency, drain, retention, terminal payload nulling, jobs admin API with reissue, `queue_status()` for TE-T5 | B1 |
| B3 | Cron (`croner` with `jiff`), schedules sync, scheduler tick, schedule admin | B2 |
| C1 | SDK protocol 2, `application_protocol`, `describe` check, version 1 downgrade, `command_as`/`query_as`, `task-domain` v2 | A2 |
| C2 | Route permission syntax and enforcement (R6), tenant streams, partitioned projections (migration 0002) | C1, ID PR 2 |
| C3 | Reactors (migration 0003), `react` runner, Notifier calls in the checkpoint transaction, command effects, rejections, `application.command` handler, reactor admin, `task-notifier` | C2, B2, NT-N2, NT-N8 (reaction part, same stage) |

**A1 acceptance.** Every existing test passes unchanged. The receipt key encoding is
byte-identical (golden test). Every portable assertion in A.10 runs through `store_contract.rs`
on SQLite.

**A2 tests.**

1. The full contract suite on PostgreSQL.
2. *Position contiguity under load.* Two instances on separate pools, 16 threads each, 64 streams,
   10 s. `verify` passes, every successful receipt matches its rows, and a concurrent reader
   walking `read_all` never sees a jump or a position that later disappears.
3. *Scripted interleaving.* A third connection holds `head FOR UPDATE`. Appends block and fail
   `storage_busy` within their deadline, and a reader sees nothing new. After release, queued
   appends commit in position order.
4. *Cross-instance idempotency.* The same key from two instances at once gives one append and
   identical receipts in both responses.
5. *Crash cuts.* kill -9 at `before_append` and `after_append_before_wake`. History and receipts
   are exact after restart. A keyed retry replays; an unkeyed retry is a new command.
6. *Connection kill during append.* `pg_terminate_backend` gives `commit_outcome_unknown`, and the
   next command succeeds without a restart.
7. *Bounds.* `history_limit`, payload and batch limits, and `invalid_limits` for capacity keys.
8. *`json` round trip.* `\u0000` escapes, `1.0` vs `1`, key order, the safe-integer edges.
9. *Two hosts.* Two `merezha run` processes on one database both serve commands (the inverse of
   SQLite owner exclusion).

10. *Trace context.* Events carry the command span's `trace_parent`/`trace_state`; no guest input
    and no `StoredEvent` contains them.

Acceptance: all ten pass, TE's `db.client.operation.duration` reports `app.events.append`, and
recorded throughput for 8 concurrent single-event writers on the CI container is in the PR
description (not gated).

**A3 tests.**

1. Without explicit catch-up, projections converge within 1 s via NOTIFY, and within
   `interval_ms` when the LISTEN connection is killed.
2. Two instances run exactly one live runner per projection. A guest call counter shows each batch
   applied once, and `task-view` redelivery detection stays quiet.
3. Killing the lock holder's connection mid-run lets the other instance take over. The first fails
   `projection_conflict`, and the final state equals a single-instance fold.
4. A rebuild over 20 000 events while commands continue: queries serve the old state and the swap
   is atomic. kill -9 at `rebuild_before_swap` and `rebuild_after_swap` resumes or keeps the
   result.
5. A new revision is not ready until warm, while an old-revision instance keeps serving.
6. `min_position` gives read-your-write, or `projection_behind` at the deadline.
7. The guest concurrency budget check, and `catch_up = "manual"` keeps the old behaviour.
8. *Cancellation mid-batch.* A test hook cancels live, rebuild and reactor runs by dropping the
   future during the commit, by a `timeout_at`, and by a guest panic. Each time another instance
   and this one's next run take the lock within 1 s and proceed, the batch left no rows, and no
   pooled session holds an `MZAP` lock (`pg_locks`). A partitioned batch losing its CAS writes none.

`application.md` gains a PostgreSQL section, exercised as written.

**B1 tests.**

1. An outbox append in a caller transaction that rolls back leaves no record.
2. After commit, a job appears within 1 s via NOTIFY, and within the interval with NOTIFY off.
3. *Crash between commit and relay.* Commit outbox rows with the relay disabled (test hook), kill
   -9, restart: each record is relayed exactly once.
4. Two relays on two instances produce no duplicate jobs.
5. Topic fan-out to 2 subscriptions gives 2 jobs, and an unrouted event is counted.
6. `OnDuplicate::Fail` and `Ignore`. Enqueue with the same key and payload returns the existing
   id; with a different payload it fails `idempotency_mismatch`.
7. *Secrets (R3).* A flagged record's payload is NULL once relayed, its job carries the flag, and
   the CHECK constraints refuse a non-NULL flagged payload after relay.
8. *In-transaction use.* An identity-style use case appends an event record and a `Job` record in
   one transaction with its own rows; a rollback leaves none of them. The event envelope round-trips.
9. *Keys and quarantine.* A 161-byte `dedupe_key` fails `invalid_dedupe_key` at `append`. A batch
   holding one unroutable record (inserted by a test hook) relays the rest and quarantines it;
   `requeue` returns it, or `409 secret_purged` when flagged.
   A systemic failure (a trigger refusing every job insert) quarantines nothing and leaves every
   payload intact, and the relay resumes once it is gone; a record isolated by its own data error
   keeps its payload and relays after a requeue.

**B2 tests.**

1. *Duplicate delivery.* Lease 1 s, heartbeat disabled (test hook), the handler sleeps 3 s. The
   reaper re-offers the job and a second worker runs it. The first completion is fenced out by
   `lease_token`. A handler recording its idempotency key under a unique constraint shows one
   effect.
2. *Poison jobs.* A panicking handler goes dead after 1 attempt. An always-transient handler goes
   dead after `max_attempts`, with gaps inside the equal-jitter bounds (base 20 ms). A permanent
   failure goes dead at once, and so does an undecodable payload.
3. *Process crash mid-job.* kill -9 while the handler runs. The reaper re-offers the job after
   lease expiry, and the attempt is counted.
4. `global_concurrency = 2` across 2 instances × concurrency 4: never more than 2 jobs running
   (sampled).
5. *Drain.* SIGTERM with a 10 s handler and `drain_timeout_ms = 1000` releases the job with the
   attempt refunded. The late completion is fenced, and a fresh instance runs the job.
6. Dead-letter list, single and bulk replay, purge and cancel, each with audit rows.
7. An instance without the kind's handler never leases the job.
8. *Terminal nulling.* A flagged job's payload is NULL after success, dead letter (handler and
   reaper paths) and cancel. Replay of a flagged dead job calls the kind's `reissue` hook, or
   returns `409 secret_purged` without one; the admin API never shows a flagged payload.
9. A drained job, re-leased and replayed, keeps one attempts row per lease.
10. *Trace.* A retried job's attempt spans share the producer's trace; a replay starts a new root.

**B3 tests.**

1. Parse errors list every problem, and seconds fields are rejected.
2. Next occurrences across the Europe/Paris DST gap and overlap match croner's documented
   behaviour.
3. Two instances fire one job per occurrence.
4. Downtime across 5 occurrences gives one job, then the normal cadence.
5. A changed definition recomputes `next_run_at`, and a config schedule removed from config is
   disabled.

**C1 tests.**

1. Every version 1 contract test and example passes unchanged, including the task-board scenario
   on version 1 guests.
2. A version 1 guest never receives `principal` in any operation, including history written by
   version 2 commands.
3. A version 2 guest sees the principal in decide and query.
4. A mismatched declaration fails startup. A reload to a mismatched guest fails dispatch with
   `service_protocol_mismatch`.
5. SDK golden JSON exists for every new type, and `describe` is answered automatically.

**C2 tests.**

1. An unauthenticated request gets 401 with zero guest fuel spent.
2. An unauthorized principal gets 403 before validation, including on a keyed replay.
3. A tenant A principal cannot read or write tenant B: the same local stream id gives distinct
   histories, and queries see only their own partition.
4. Two-tenant concurrent load keeps partitions independent, and `verify` passes.
5. Partitioned rebuild swaps generations atomically and deletes the old ones. Tenant queries looping
   through the swap and the deletion never return the initial state for a tenant that has one.
6. `route_access_undeclared`, an unknown permission, and a tenant query on an unpartitioned
   projection each fail startup.
7. A client-supplied `actor` that differs from the principal gets `403 actor_mismatch`. An
   application-scoped route passes with an application-level grant and no tenant; `platform_admin`
   passes every route; `resource = "task"` checks the stream's resource grant.

**C3 tests.**

1. Completing a task gives exactly one notification request (through `Notifier::request`) and one
   delayed command job, both keyed `reaction:task-notifier:{event_id}:{index}`.
2. *Crash between reactor commit and relay* (kill -9): the effects are relayed once. *Crash before
   the reactor commit:* the batch re-runs, and neither the outbox nor NT holds a duplicate. A forced
   rollback of the checkpoint transaction leaves no NT rows.
3. *Duplicate delivery* of a command effect (forced lease expiry) gives one append, and the receipt
   replays.
4. *A projection rebuild never re-fires.* Rebuilding `tasks` after 100 reactions leaves the outbox
   count and the reactor checkpoint unchanged.
5. *Poison event.* A guest permanently failing one event in a batch of 16 is isolated by
   bisection. The reactor halts at exactly that position, resumes by itself after a fixed guest is
   reloaded, and `skip` also resumes it. `on_error = "skip"` records the skip. A malformed reply
   halts under either setting.
6. A transient failure retries with backoff without advancing. A rejected effect (unknown topic,
   address recipient without `allow_addresses`, command tag outside `commands`) is listed in
   `rejections`, its sibling effects are written, and the checkpoint advances.
7. Two instances never run the same reactor at once.
8. `start = "head"`: no history from before the reactor existed fires.
9. Tenant safety: a user recipient outside the tenant is rejected with `recipient_not_in_tenant`,
   a command effect runs as `sys_reactor_task-notifier` and composes the event's tenant, and a
   webhook effect on an event without tenant goes to application-level endpoints.
10. Memory and SQLite reject reactors at startup.
11. A test Directory that fails if called while a checkpoint transaction is open is never tripped;
    a Directory outage retries the batch without advancing.

# F. Decisions

Owner answers and internal decisions recorded in
[cross-block-decisions.md](cross-block-decisions.md), "Owner answers"; that file is authoritative.

| Topic | Decision | Applied in |
| --- | --- | --- |
| AP1 append throughput | About 300–2000 commits per second per application is acceptable; per-tenant feeds stay out of scope. | A.5 |
| AP2 SQLite import | None. SQLite stays a development and test store; slice A4 is dropped. | A.11, E |
| AP3 reactors and projections | Reactors cannot read projection state. NT resolves recipients. | C.2 |
| AP4 message ids | The HTTP adapter fills in missing correlation and message ids. | A.6, C.2 |
| Receipts | Kept forever. | A.4 |
| Event metadata | Roles are not stored; `{subject, kind, tenant}` only. | C.2 |
| Job ordering | No per-key FIFO. | B.1 |
| Rebuild | Asynchronous (`202`) on PostgreSQL; memory and SQLite stay synchronous. | A.7, D.1 |
| Reactor defaults | Start at `head`; `on_error = "halt"`. | C.5 |
| Partitions | By tenant only. | C.4 |
| Dead letters | Kept 30 days when not replayed, then purged with an audit row. | B.9 |
| API role | `jobs.workers = false` is part of this program. | D.2 |
| Webhook effects | Registered endpoints only; guests never name URLs. | C.2 |
| PostgreSQL floor | 14 minimum, CI on 16 (P2). | B.5, E |
