# Application protocol and SDK

`merezha-domain` is the guest-safe Rust SDK. It uses Serde and JSON and has no
Wasmtime dependency. A separate workspace can depend on it through
`merezha-domain = { path = "../../crates/domain" }`, adjusted for its crate.
It provides validated `Identity`, classified `Failure`, a pure `Decider` with
`initial`, `decide` and `evolve`, and deterministic `rehydrate`.

Application components use the existing no-import `merezha:service@0.1.0` ABI.
Call `merezha_domain::handle(input, dispatch)` from the exported handler. The
dispatcher matches `Operation` and returns `Result<Reply, Failure>`. Classified
application failures are encoded inside ABI `Ok`; ABI `Err` denotes a component
or transport failure. Unknown operations, fields and protocol versions fail.

Each request has `version: 1` and a `body` tagged by `op`:

| Operation | Input | Required reply |
| --- | --- | --- |
| `validate` | `kind`, `tag`, `payload` | `validated` with normalized `payload` |
| `decide` | `tag`, `stream`, `payload`, `history`, `metadata` | `decided` with `events` |
| `project` | `projection`, `state`, `events`, `live` | `projected` with new `state` |
| `query` | `tag`, `payload`, `state`, `metadata` | `queried` with `value` |

Use `validate_payload::<YourType>` to decode and re-encode typed commands and
queries, with `#[serde(deny_unknown_fields)]` on domain payload types. Perform
business validation in the guest too. During `decide`, decode history event
payloads, verify event tag/schema, rehydrate and call the decider. Emit `Event`
with `tag`, positive `schema_version` and JSON `payload`. The host assigns event
identity, stream versions, global positions, acceptance time and metadata.

Responses have `version: 1` and `outcome` tagged by `status`, either `ok` with
`reply` tagged by `kind`, or `err` with an `error` containing `code` and
`classification`. Classifications are `permanent`, `transient`, and `conflict`.
Identifiers and error codes contain 1 to 128 ASCII alphanumeric characters or
`._-:/`. No error carries arbitrary guest text.

Metadata has required `message_id` and `correlation_id` and optional
`causation_id` and `actor`. Metadata is caller-supplied context, not proof of
authentication. `Authorizer` is an embedding-host policy port; `AllowAll` is an
explicit choice for trusted local demos.

JSON uses the locked Serde JSON i64/u64/f64 number representation. Integral
values outside ±9,007,199,254,740,991 are rejected in application values.
Fractions use binary64 rounding; decimal lexical precision is not preserved.
Encode money and arbitrary precision values as validated decimal strings.
Objects are compared after typed guest validation with recursively sorted keys.
Array order matters. `1` and `1.0` can remain distinct unless the typed guest
normalizes them to the same representation. Duplicate object keys use Serde's
last-value behavior; callers must not depend on duplicate-key spelling. Raw
service strings remain opaque and retain their previous behavior.

The host config public types are in `merezha_application::config`; guest and
authorization ports are `Guest`, `GuestSession` and `Authorizer`. `Application`
accepts these ports and a replaceable `EventStore`, or constructs the bounded
`MemoryEventStore` through `Application::in_memory`. A dispatch pins its runtime
catalog generation. Each guest call still creates a fresh bounded instance.
`Application::open` selects the configured memory or SQLite backend. Custom
`EventStore` ports remain explicitly process-local for receipts and projections;
they cannot supply the shared durable transaction by themselves.

## Local application server

Keep the existing runtime manifest and add a separate application TOML file.
The services named here must exist in the runtime catalog at startup.

```toml
version = 1
[[commands]]
tag = "CreateTask"
service = "task-domain"
aggregate = "Task"
[[queries]]
tag = "GetTask"
service = "task-view"
projection = "tasks"
[[projections]]
name = "tasks"
service = "task-view"
initial = { tasks = {} }
```

```sh
cargo run --locked -p merezha-cli -- serve --manifest runtime.toml --application application.toml --listen 127.0.0.1:0
```

Read the ephemeral port from stderr's `listener.ready`. POST JSON to
`/v1/application/commands` with `tag`, `stream`, `payload`, `metadata`, optional
`idempotency_key` and optional `expected_version`. The host namespaces the stream
as `aggregate/stream`. Configured aggregate labels cannot contain `/`, which
prevents two aggregate/stream pairs from aliasing one history. The composed
stream must still fit the 128-byte identity limit. It returns
`{stream, version, position, events}`. Query
JSON at `/v1/application/queries` has `tag`, `payload`, `metadata` and returns
`{value, checkpoint}`. Both accept at most 64 KiB of request bytes. Errors have
`{error: {code, classification}}` without guest text.

POST an empty body to `/v1/application/projections/tasks/catch-up` or
`/v1/application/projections/tasks/rebuild` to receive `{state, checkpoint}`.
Commands leave projections stale until catch-up or rebuild. Queries receive
only the read-model snapshot and cannot append events, even if the guest
returns a command-shaped reply. Runtime raw endpoints and reload remain
available. Config changes require restart; runtime reload changes guest
artifacts only. A removed service causes dispatch failure, preserving history.
Changing guest event schemas needs compatible guest decoding; no host upcaster
or migration is provided.

## Consistency and limits

Typed guest validation and host authorization run before every command,
including cached replays, and every query. The CLI selects `AllowAll` for this
trusted loopback use. An embedding host supplies real policy and establishes
actor identity. Projection controls and raw invocation/reload also assume a
trusted local administrator; do not expose this server externally.

Idempotency is scoped by command tag, actor and key, bound to the validated
payload, target stream and explicit expected-version precondition. Success
replays the original receipt. A changed payload/target/precondition gives
`idempotency_mismatch`; a pending identical claim gives `idempotency_in_flight`.
Failed commands release the claim. In memory mode, completed records are retained
until restart; capacity is rejected rather than evicting replay protection.
No-key commands can be repeated, and an invariant failure can be retried after
state changes. Memory mode provides process-local suppression. Neither backend
provides external exactly-once delivery.

Expected-version append commits an entire event batch atomically. Per-stream
versions and global positions start at one. Stored records carry host-assigned
event ID, acceptance timestamp and original message metadata. The event store
in memory mode is authoritative only for this process lifetime. Restart loses
events, idempotency and projections in that mode. SQLite mode below persists
them. No outbox, inbox, snapshots or simultaneous host ownership is included.

Catch-up snapshots the feed head and processes ordered batches up to that
position. Each successful batch swaps read-model state and checkpoint together.
A failed batch leaves both unchanged and can be retried. Rebuild starts from
configured initial state, uses `live=false`, and swaps its complete replacement
only after all batches succeed. Failed rebuild preserves the prior model.
Concurrent projection runs fail `projection_in_flight`; queries can also return
that conflict while the model is locked. Guest code cannot access external
effects through this no-import ABI.

Optional `[limits]` settings, all restart-only and nonsecret, have defaults and
hard ceilings below. Counts also bound native collection overhead; the byte
limit counts encoded accepted events, not process RSS. The embedding application
owns sizing and adapter selection. No setting makes the host a native OS sandbox.

| Setting | Default | Maximum |
| --- | ---: | ---: |
| `max_events` | 4096 | 4096 |
| `max_streams` | 256 | 256 |
| `max_store_bytes` | 8388608 | 8388608 |
| `max_idempotency` | 1024 | 1024 |
| `max_payload_bytes` | 16384 | 65536 |
| `max_state_bytes` | 65536 | 262144 |
| `batch_size` | 16 | 64 |
| `max_concurrent` | 16 | 64 |
| `timeout_ms` | 5000 | 30000 |

Every limit must be positive. Registries allow at most 64 command tags, 64
query tags and 32 projections. Command/query tags must be disjoint and unique;
projection names must be unique, and queries must reference known projections.
Unknown config fields, invalid identities, missing services and oversized config
or initial state fail startup before listening. Request body reads and all guest
calls share the admitted deadline. Native ports and policies are trusted code
and must implement their bounded synchronous contract. HTTP caller cancellation
does not detach admission from a still-running blocking dispatch.

Full history is sent to the guest on every decision, so the runtime input byte
and fuel limits can reject a stream before store capacity. No compaction or
snapshot escape hatch is hidden in the in-memory adapter. Read models can be
rebuilt while authoritative history is present, using a finite pinned feed head;
there is no recovery after process loss in this adapter.

The host boundary scenario runs without external services or secrets:

```sh
cargo test --locked -p merezha-domain -p merezha-application
cargo test --locked -p merezha-cli --test application_http
```

These exercise replay, invariants, metadata, concurrency, capacity, failed
catch-up/rebuild, actual no-import component protocol execution and HTTP wiring.
The application component test uses fixed protocol fixtures; independent business
and projection components and their command-driven scenario are owned by the
separate services contribution. [Feature parity](structure-parity.md) records
the delivered subset and remaining framework capabilities.

## Owned SQLite storage

Storage belongs to the embedding host. The CLI calls `Application::open` before
binding its listener. Memory remains the default. For a fresh database, add:

```toml
[storage]
backend = "sqlite"
path = "/absolute/owned/persistent/counter.db"
application = "counter-v1"
initialize = true
```

The parent directory must already exist and be writable. Initialization uses
exclusive file creation and a version 1 schema transaction. After successful
initialization, change `initialize` to `false`, its default, before the next
start. Normal open never creates a missing database. Initialization never adopts
or overwrites an existing file. An interrupted initialization can leave an
uninitialized file; reopen rejects it. Preserve that file for diagnosis and
initialize a different fresh path. There is no automatic destructive repair.

One logical application and one live host own each database. Clones share its
backend and admission/claim registries. An OS lock on the canonical database's
`.owner` sidecar rejects a second opener with `storage_owned` before readiness.
Process exit or SIGKILL releases the lock. The empty sidecar stays on disk and
must never be unlinked or replaced while a host might run. A separate sidecar
avoids interference with SQLite's own native locks. Symlink aliases resolve to
the same canonical path; hard-linked database/lock files are rejected on Unix.

Use an owned directory on a persistent local filesystem with reliable native
locking and fsync. Network filesystems, SQLite URI options, `:memory:`, relative
paths, renaming/replacing live files and noncooperating writers are unsupported.
The lock coordinates Merezha hosts, not arbitrary SQL clients. Do not edit the
database directly. Keep the directory private to its owner; no encryption or
authentication layer is added. Linux and macOS are the supported host targets.

The adapter owns four STRICT tables: manifest, events, receipts and projections.
It stores application identity, canonical configuration, fingerprint format 1,
`application_id`, `user_version` and exact schema definitions. Verified reopen
checks schema drift, `quick_check`, event/stream numbering, payloads, retained
capacities, receipt bindings and projection pairs before readiness. Changed
application identity, routes, limits, initial states or projection revisions
fail with `storage_configuration`. Every projection has a positive `revision`
field, default 1, owned by the application. Change it when guest projection
semantics change. Runtime reload cannot determine semantic compatibility;
operators must keep replacement guests compatible with persisted events/models.
No schema upgrade, data deletion or implicit projection reset occurs. Unsupported
or drifted schemas fail with `storage_schema`; invalid retained data fails
startup. Future schema upgrades need a separately reviewed migration.

Validation and authorization still precede replay. Pending keys exist only in
the live owner's bounded registry. After guest decision, a short `BEGIN IMMEDIATE`
transaction rechecks expected stream version, durable key and capacities, then
inserts the entire numbered event batch and completed canonical receipt before
one commit. No guest code runs inside SQL. Zero-event keyed commands persist a
receipt too. No pending key is written to disk, so a crash cannot strand one.
Rollback consumes no versions or global positions. Accepted IDs, metadata,
timestamps and receipts retain their original values across restart.

Projection SQL commits replace JSON state and checkpoint together, comparing an
independent row revision and incrementing it on every successful publication.
Checkpoint equality alone is insufficient because rebuild can publish at the
same head. Guest computation runs outside SQL. Catch-up retains earlier
successful batches if a later one fails; rebuild publishes once after all its
batches succeed. Queries read one persisted pair and never catch up implicitly.
Concurrent projection work in one application still fails `projection_in_flight`;
a stale SQL candidate fails `projection_conflict`.

SQLite uses DELETE rollback journals, `synchronous=EXTRA`, a 1024 KiB page cache,
in-memory temporary storage, no mmap and immediate busy failure. Core journal,
synchronous, page-size and main-page-limit settings are checked on open. The
4096-byte main-page budget is the rounded-down value of:

```text
4 * (max_store_bytes + max_idempotency * (max_payload_bytes + 2048)
     + projection_count * (max_state_bytes + 1024) + 1048576)
```

With default limits and one projection this is 113512448 bytes, 27713 pages.
It includes conservative row/index overhead and retained fingerprint/receipt
space. Reserve at least a further main-file budget plus 1 MiB for the rollback
journal, and a separate allowance for backups. `journal_size_limit=0` removes
retained journals after transactions; it is not a peak-journal quota. No WAL or
background checkpoint worker runs. Disk quotas/free-space monitoring belong to
the host operator. Restart never reclaims event or receipt capacity; there is
no TTL, eviction, automatic retention or snapshot compaction.

Connection contention and SQLite busy/locked states fail `storage_busy` without
sleeping. The admitted deadline is checked before storage work and before
commit; a progress callback checks it every 1000 SQLite VM operations. Fixed
count/byte ceilings bound native scans and decoding. SQLite row and SQL text
lengths are limited to 1 MiB and 64 KiB respectively. OS filesystem/fsync stalls
cannot be preempted by the VM callback; this is not a hard real-time guarantee.
The HTTP host keeps admission attached to its blocking dispatch. `storage_full`
is a capacity failure. Statement failures roll back their transaction. A commit
error reports `commit_outcome_unknown` and blocks further storage use with
`storage_reopen_required`; stop the host and reopen to reconcile. Never infer
rollback from a lost response or timeout. A keyed retry after verified reopen
replays any committed receipt. An unkeyed retry can duplicate accepted effects
and must be a caller decision. No business decision is automatically rerun.
If cleanup cannot restore autocommit, the backend also blocks use until reopen.

For backup, stop the owner and wait for its exit. After a crash, first perform a
successful verified reopen and close to recover any hot journal. Then copy the
closed main database and keep the matching application config. To restore,
copy the backup to a new owned local path, use the same application identity,
routes, limits and revisions, and open with `initialize=false`. Do not copy a
live database, remove an active journal, or copy a live `.owner` sidecar.
Verified reopen is required before serving the restored store. Recovery tests
compare exact events/metadata, original receipts and persisted checkpoints.

The host pins [rusqlite 0.40.2](https://docs.rs/crate/rusqlite/0.40.2), MIT,
with default features disabled and `bundled`, `hooks`, `limits`, `fallible_uint`.
The locked `libsqlite3-sys` 0.38.2 is MIT and bundles SQLite 3.53.2, which is
[public domain](https://sqlite.org/copyright.html). Bundled compilation requires
a C compiler and archiver; it uses prebuilt bindings, without runtime system
SQLite, libclang, TLS or a database server. The wrappers declare no numeric
`rust-version` in these releases; upstream's policy is stable Rust at release.
This configuration is compiled and tested with Rust 1.98.0, edition 2024.
The guest SDK remains database-free and wasm32-compatible. Native ownership uses
[standard-library file locks](https://doc.rust-lang.org/std/fs/struct.File.html#method.try_lock),
stable since Rust 1.89. Transaction/error and journal policies follow
[SQLite transactions](https://sqlite.org/lang_transaction.html) and
[pragma documentation](https://sqlite.org/pragma.html).

```sh
cargo test --locked -p merezha-application
cargo test --locked -p merezha-cli --test sqlite_process -- --nocapture
```

The first covers reopen, receipt/event rollback, recovered-data bounds,
independent-connection CAS, forced transaction/rebuild interruption and failure
recovery. The second starts real no-import CLI hosts over ephemeral loopback
HTTP, checks owner exclusion, restart, raw service behavior and closed restore.
Its component has fixed protocol replies; arithmetic and failed guest projection
recovery are tested separately through application guest fixtures.
