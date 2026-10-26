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

Each request has `version` 1 or 2 and a `body` tagged by `op`:

| Operation | Input | Required reply |
| --- | --- | --- |
| `validate` | `kind`, `tag`, `payload` | `validated` with normalized `payload` |
| `decide` | `tag`, `stream`, `payload`, `history`, `metadata` | `decided` with `events` |
| `project` | `projection`, `state`, `events`, `live`; version 2: optional `partition` | `projected` with new `state` |
| `query` | `tag`, `payload`, `state`, `metadata` | `queried` with `value` |
| `describe` | version 2 only, no fields | `described` with `protocol` (answered by the SDK) |

Use `validate_payload::<YourType>` to decode and re-encode typed commands and
queries, with `#[serde(deny_unknown_fields)]` on domain payload types. Perform
business validation in the guest too. During `decide`, decode history event
payloads, verify event tag/schema, rehydrate and call the decider. Emit `Event`
with `tag`, positive `schema_version` and JSON `payload`. The host assigns event
identity, stream versions, global positions, acceptance time and metadata.

Responses echo the request's `version` and have `outcome` tagged by `status`, either `ok` with
`reply` tagged by `kind`, or `err` with an `error` containing `code` and
`classification`. Classifications are `permanent`, `transient`, and `conflict`.
Identifiers and error codes contain 1 to 128 ASCII alphanumeric characters or
`._-:/`. No error carries arbitrary guest text.

Metadata has required `message_id` and `correlation_id` and optional
`causation_id` and `actor`. Metadata is caller-supplied context, not proof of
authentication. `Authorizer` is an embedding-host policy port; `AllowAll` is an
explicit choice for trusted local demos.

### Protocol version 2

A service opts in with `application_protocol = 2` on its `[[services]]` entry
in the runtime manifest ([contract](contract.md)); the default is 1. The host
sends version 2 requests only to such services, and expects the response to
echo the version (`protocol_version` otherwise). Version 2 adds:

- `metadata.principal`, set only by the host: `{subject, kind, tenant?,
  roles?}` with `kind` one of `user`, `api_key` or `system` and at most 16
  sorted role keys of at most 64 bytes, the roles held at the route's scope
  (checked before each call to a version 2 guest, `invalid_principal`).
  When it is present, `actor` is its subject. It never carries sessions,
  grants or secrets. Guests use it for ownership rules
  (`Metadata::principal`, `Metadata::require_subject`, `Principal::has_role`,
  `Principal::require_tenant`); they never authenticate.
- `describe`: at startup, and on the first call after each reload (once per
  catalog generation and service), the host sends
  `{"version":2,"body":{"op":"describe"}}` to every version 2 service and
  expects `{"kind":"described","protocol":2}`. `merezha_domain::handle`
  answers it without calling the dispatcher. A guest that does not answer it
  (a guest built on the version 1 SDK answers `protocol_version`) fails
  startup with `service_protocol_mismatch`; after a reload, calls to that
  service fail with the same permanent code and the history is untouched.
- `project.partition` for partitioned projections (route permissions and
  tenant scopes, a later slice; the host sends none yet).

The host downgrades every request to a version 1 service: no `principal` in
decide or query metadata nor in any history or projection event's metadata,
no `partition`, no `describe`. A version 1 request carrying any of them fails
`protocol_version` in the SDK, so guests built on the version 1 SDK keep
working unchanged, and version 1 JSON is byte-identical to what it was before
version 2. Rebuilding a guest on the new SDK is a source break only for code
that matches `Operation` exhaustively (add a `Describe {}` arm) or builds
`Metadata` or `Operation::Project` literals (add `principal: None` or
`partition: None`).

`Admission::command` and `query` (and `Application::command`, `query`) are
anonymous: a `metadata.principal` supplied by the caller fails
`principal_not_trusted`. The trusted entry points are `command_as(principal,
command)` and `query_as(principal, query)`, called by the host with the
principal it established; a caller-supplied `actor` other than the
principal's subject fails `actor_mismatch`. Stored events keep the principal
as `{subject, kind, tenant}` and drop its roles.

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

`version = 1` is the original format and stays accepted. `version = 2` adds
`[limits] max_stream_events` (below), route permissions and tenancy (next
section) and `[[reactors]]` ([reactors](#reactors-version-2-postgresql),
PostgreSQL only). A version 1 file using any version 2 key fails
`invalid_config`, as an unknown key did before.

### Route permissions and tenant streams (version 2)

```toml
version = 2
[[permissions]]
name = "tasks.create"            # joins the identity block's permission catalog
description = "Create tasks"

[[commands]]
tag = "CreateTask"
service = "task-domain"
aggregate = "Task"
permission = "tasks.create"      # or access = "public" | "authenticated"
scope = "tenant"                 # application (default) | tenant
resource = "task"                # optional, tenant commands: checked on the stream

[[queries]]
tag = "ListTasks"
service = "task-view"
projection = "tasks"
permission = "tasks.read"
scope = "tenant"                 # reads the caller's partition

[[projections]]
name = "tasks"
service = "task-view"
initial = { tasks = {} }
partition = "tenant"             # none (default) | tenant; PostgreSQL only
```

Every route is checked before any guest call and before a stored receipt is
replayed, so a refused caller costs no guest fuel. `access = "public"` admits
anyone; `authenticated` needs a principal (`401 unauthenticated`); a
`permission` is checked for the principal at the route's scope: `Application`,
`Tenant(principal.tenant)`, or for a command with `resource` the resource
`{tenant, kind, id}` whose id is the local stream id (`403 unauthorized`). A
tenant route without an active tenant fails `403 tenant_required`. A tenant
command writes the stream `{tenant}:{aggregate}/{stream}` of the principal's
tenant: the request only names the local stream id, so a tenant can never
write another tenant's stream (naming one nests it under its own), and its
receipts are keyed by its tenant too. A tenant query reads the tenant's
partition of a `partition = "tenant"` projection, whose state is kept per
tenant (the configured initial state for a tenant without events).

The checks run through a host-supplied `Caller`: `Application::command_as` and
`query_as` take one (the platform passes the edge's principal and its policy,
`merezha_cli::application_http::EdgeCaller`); `command` and `query` are
anonymous (`Anonymous`: public and undeclared routes only). `serve` uses the
explicit local `AllowAll`, which admits every permission but has no tenant.
A route declaring neither `permission` nor `access` keeps the version 1
behaviour under `serve` and embedding hosts; the platform host serves it only
on a private loopback listener without identity, and refuses it at startup
with the identity block (`route_access_undeclared`).

The document is checked as a whole, every problem listed at once by `merezha
run` (`startup.failed` with `issues: [{key, code}]`): `route_access_conflict`
(both `permission` and `access`), `invalid_permission`, `block_prefix` (an
application permission in a block namespace: `identity.`, `content.`,
`notifications.`, `jobs.`, `application.`, `system.`), `duplicate_permission`,
`unknown_permission` (a route permission outside `[[permissions]]` and the
blocks' catalog), `resource_requires_tenant`, `tenant_query_unpartitioned`,
`partitioned_query_unscoped`, `tenant_aggregate` (with any tenant scope, an
aggregate containing `:`), `partitions_require_postgres` (memory and SQLite
keep one state per projection) and `cross_tenant_query`: with any
tenant-scoped route, an application-scoped query on an unpartitioned
projection that anonymous callers or every principal may call returns every
tenant's data, so it must opt in with `cross_tenant = true`
(`cross_tenant_scoped` on a tenant query). The rule cannot see permissions
granted at runtime through application roles: a permission held by most users
through application roles exposes every tenant; use `scope = "tenant"`.
`block_resource_kind` refuses a `resource` naming a block's resource kind
(content's `content_type`). `access = "authenticated"` checks
no permission, so it ignores an API key's permission list. Changing `partition` needs a new projection
`revision` (`storage_configuration` otherwise).

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

Idempotency is scoped by command tag, the stream's tenant, actor and key, bound
to the validated payload, target stream and explicit expected-version
precondition. A principal's commands are scoped by its subject apart from
anonymous ones, so an anonymous caller naming a principal's subject as its
`actor` never replays that principal's receipt. Success
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
| `max_stream_events` (version 2) | 4096 | 4096 |

`max_stream_events` bounds one stream's history, which goes whole to the guest on
every decision: a command whose stream holds more events fails `history_limit`
(422) before the guest decides, on every backend, and a keyed retry is not left
in flight. Memory and SQLite also stop at `max_events`, whichever is lower.
The host records which `[limits]` keys the file wrote. PostgreSQL storage has
no total capacity and refuses a written `max_events`,
`max_streams`, `max_store_bytes` or `max_idempotency` with `invalid_limits`.

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

The parent directory must already exist and be writable. Initialization locks
the intended canonical path before exclusive file creation and a version 1
schema transaction. Ordinary owner-lock failure leaves no new database. After successful
initialization, change `initialize` to `false`, its default, before the next
start. Normal open never creates a missing database. Initialization never adopts
or overwrites an existing file. Any existing entry, including a dangling symlink,
is a permanent `storage_open` initialization error before owner-sidecar creation
or acquisition, even when an existing database has a live owner. This refusal
does not change existing data, links or sidecar contents.
An interrupted initialization can leave an
uninitialized file; reopen rejects it. Preserve that file for diagnosis and
initialize a different fresh path. There is no automatic destructive repair.

One logical application and one live host own each database. Clones share its
backend and admission/claim registries. An OS lock on the canonical database's
`.owner` sidecar rejects a second opener with `storage_owned` before readiness.
The last logical backend owner closes its SQL connection and explicitly unlocks
the sidecar, even if a spawning child temporarily inherited a descriptor.
Backend clones retain ownership until the last clone ends. Process exit or
SIGKILL also releases the lock. The empty sidecar stays on disk and
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
fail with `storage_configuration`. The stored text is the configuration at
version 1, so changing only `version = 1` to `version = 2` keeps a store
opening; a `max_stream_events` other than its default is part of the text. Every projection has a positive `revision`
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
Concurrent catch-up/rebuild work in one application still fails
`projection_in_flight`; a stale SQL candidate fails `projection_conflict`.
SQLite queries can read the last committed pair while a guest computes a new one.

SQLite uses DELETE rollback journals, `synchronous=EXTRA`, a 1024 KiB page cache,
in-memory temporary storage, no mmap and immediate external busy failure. Core journal,
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

Requests wait for the internal connection through a condition variable, bounded
by their original admitted deadline. Expiration reports `timeout`; an expired
waiter forwards a wakeup if a connection is available for another caller. No SQL or
guest code runs while holding the connection-availability mutex. External
SQLite busy/locked states still fail `storage_busy` immediately. The admitted
deadline is checked before storage work and before
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
These poisoned-store failures remain HTTP 429. The operator must restart;
automatic listener shutdown and health/readiness changes are deferred.
Detected corrupt databases, invalid stored types and invalid UTF-8 report
permanent `storage_invalid`.

For diagnosis after a crash, stop the owner and copy the closed database and any
matching rollback journal before probing it. Normal reopen may roll back a hot
journal and change the original files even if verification later rejects them.
Interrupted bootstrap files are never reinitialized: preserve the diagnostic
copy and initialize a different fresh path. A bootstrap that committed can pass
normal verified reopen.

For backup, stop the owner and wait for its exit. After a crash, perform a
successful verified reopen and close to recover any hot journal. Then copy the
closed main database and keep the matching application config. To restore,
copy the backup to a new owned local path, use the same application identity,
routes, limits and revisions, and open with `initialize=false`. Do not copy a
live database, remove an active journal, or copy a live `.owner` sidecar.
Verified reopen is required before serving the restored store. Recovery tests
compare exact events/metadata, original receipts and persisted checkpoints.

The host pins [rusqlite 0.40.2](https://docs.rs/crate/rusqlite/0.40.2), MIT,
with default features disabled and `bundled`, `hooks`, `limits`, `fallible_uint`.
`rusqlite` still enables the transitive `libsqlite3-sys` default build features,
including `min_sqlite_version_3_34_1`, `pkg-config` and `vcpkg`. They are build
configuration dependencies; bundled mode compiles the pinned SQLite source.
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

`tests/store_contract.rs` holds the portable store contract: reopen, scoped
canonical replay, zero-event receipts, pending keys and racing writes, two
independent instances rechecking stream, key and projection row revision, failed
catch-up and rebuild, the state and history limits, multi-event batches and
statement-failure rollback. It runs on SQLite (`sqlite::contract_*`) and, when
`MEREZHA_TEST_DATABASE_URL` is set, on PostgreSQL (`postgres::contract_*`). The
SQLite second instance comes from the test-only `test-support` feature, which
shares the owner lock because the owner gate forbids two hosts on one file; the
PostgreSQL one is another application on its own pool.

The first command covers reopen, receipt/event rollback, recovered-data bounds,
independent-connection CAS, forced transaction/rebuild interruption and failure
recovery. The second starts real no-import CLI hosts over ephemeral loopback
HTTP, checks owner exclusion, restart, raw service behavior and closed restore.
Its component has fixed protocol replies; arithmetic and failed guest projection
recovery are tested separately through application guest fixtures.

## PostgreSQL storage

`backend = "postgres"` keeps the application in the `merezha_app` schema of the
project database, for `merezha run` only: `serve` has no pool and refuses it
with `storage_configuration`. Several hosts may serve one database; see
[platform: application storage](platform.md#application-storage) for the
configuration, the error codes and `merezha application verify`. An embedding
host connects the store on its runtime and constructs the application
synchronously:

```rust,ignore
let store = PostgresStore::connect(&db, &config, PostgresOptions::default(), deadline).await?;
let app = Application::with_postgres(config, guest, authorizer, Arc::new(store))?;
// Projections advance in the background from here on.
let scheduler = Scheduler::start(&app)?;
// Then call `app` only from spawn_blocking or a thread outside the runtime.
// ...
scheduler.shutdown(deadline).await;
```

The store blocks on the runtime handle it was connected on, so it needs a
multi-thread runtime and must not be called on an async worker thread. The
in-process registry keeps pending idempotency claims only; completed receipts
are read back from the database.

Projections on PostgreSQL differ from the consistency rules above in four
ways ([platform: projections in the background](platform.md#projections-in-the-background)):

- The `Scheduler` (`PostgresOptions::background`) catches every projection
  up in the background after commits on any host, one host at a time per
  projection under a session advisory lock. `catch_up(name)` runs to the head
  it read or waits for the host that holds the lock; it never fails
  `projection_in_flight`.
- `rebuild(name)` requests a resumable rebuild and runs it to its atomic swap
  within the call; `Admission::request_rebuild` only requests it, for the
  `202` admin route. Queries serve the previous state until the swap.
- `Query.min_position` (a receipt's `position`) waits within the deadline
  until the projection reached it, else fails `projection_behind`. Memory and
  SQLite fail it at once when the checkpoint is behind.
- A projection `revision` new to the database warms up in the background;
  its queries fail `projection_warming` and `Scheduler::ready` is false until
  it reaches the head it was created at.
- A `partition = "tenant"` projection keeps one state per tenant and
  generation (migration 0002, `merezha_app.projection_partitions`). Each batch
  groups its events by their stream's tenant and calls the guest once per
  tenant with `project { partition }` (version 2 guests see it; version 1
  guests get the same per-tenant batches without it); events of application
  streams only advance the checkpoint. A batch commits CAS first, so a run
  that lost its row revision writes no partition. A rebuild writes the next
  generation beside the served one, swaps it in with the row, then deletes
  older generations in chunks of 1000; queries read the partition of the
  served generation in the same statement as the row, so none sees a
  half-built or half-deleted generation. Its explicit catch-up answers
  `{checkpoint, head}` and `catch_up`/`rebuild` return a `null` state.

`Application::status()` returns the status document on every backend:
head, per-projection checkpoint, lag and failure, plus on PostgreSQL the
instance's background runs, listener state and reactors.

### Reactors (version 2, PostgreSQL)

A reactor turns committed events into effects: notifications, webhook events
and later commands, in the notifications block's vocabulary
([design](../design/application-platform.md) §C.2, §C.5). It is a version 2
service answering `react`; it sees events only, never projection state.

```toml
[[reactors]]
name = "task-notifier"             # 1 to 48 bytes of [A-Za-z0-9._-]
service = "task-notifier"          # application_protocol = 2 (reactor_requires_protocol_2)
events = ["TaskCompleted"]         # tag filter applied by the host; [] = every event
effects = ["notify", "command"]    # allowed effect types; others are rejected
commands = ["ArchiveTask"]         # allowed command tags (each a [[commands]] route)
permissions = ["tasks.archive"]    # held by sys_reactor_task-notifier
allow_addresses = false            # explicit email or phone recipients
start = "head"                     # head (default) | beginning; read only when the row is created
on_error = "halt"                  # halt (default) | skip
```

```json
{"version":2,"body":{"op":"react","reactor":"task-notifier","events":[ <StoredEvent>, ... ]}}
{"version":2,"outcome":{"status":"ok","reply":{"kind":"reacted","reactions":[
  {"event_id":"event/41","effects":[
    {"effect":"notify","topic":"tasks.completed","recipients":[{"user":{"id":"usr_01JB2X6Q9F3W"}}],
     "channels":["inbox","email"],"data":{"title":"Write docs"}},
    {"effect":"webhook","event_type":"tasks.task_completed","data":{"task_id":"t-1"}},
    {"effect":"command","tag":"ArchiveTask","stream":"t-1","payload":{},"delay_ms":604800000}]}]}}}
```

The SDK types are `Operation::React`, `Reply::Reacted`, `Reaction`, `Effect`,
`Recipient` (`user`, `tenant`, `application`, `address`), `Address` and
`Channel`. Guests never name the dedupe key, the tenant or the source event:
every effect is keyed `reaction:{reactor}:{event_id}:{index}` (source
`application`), takes the event's stream tenant and correlation id, and has
the event as its causation.

The reply is checked in two tiers. A *malformed* reply (`invalid_response`:
not `reacted`, not exactly one reaction per event in order, more than 8
effects per event or 64 per batch, an effect over `max_payload_bytes`)
halts the reactor whatever `on_error` says. A *rejected* effect is recorded
in `merezha_app.reactor_rejections`, logged as `reactor.effect_rejected`,
and its siblings are written: `effect_not_allowed`, `command_not_allowed`,
`address_not_allowed`, `invalid_recipient` (a `tenant` recipient naming
another tenant, or an `application` recipient on a tenant event, since
application roles span every tenant), `tenant_required` (a `tenant` recipient on an event without
tenant), `invalid_delay` (over 30 days), and every refusal of the
notifications block (`unknown_topic`, `recipient_not_in_tenant`, ...).

A batch reads up to `batch_size` events after the reactor's checkpoint,
calls the guest outside SQL with the events its filter keeps, asks the
Directory which `user` recipients belong to the event's tenant, then writes
every accepted effect, its rejections and the new checkpoint in one
transaction on the run's locked session. The host's
`postgres::Effects` port writes them (the platform's `Notifier` and
`Outbox`), so notifications, outbox records and the checkpoint commit or
roll back together. A command effect becomes a job
`application`/`application.command` that runs the command as
`sys_reactor_{name}` (kind `system`, the event's tenant, the reactor's
`permissions` only), with idempotency key and message id
`rx:{reactor}:{position}:{index}`: a duplicate delivery replays the receipt.
`Application::command_effect` is that handler's entry point. An embedding
host passes its port in `PostgresOptions::effects`; a store whose
configuration declares reactors fails `reactor_effects_unavailable` without
one, and the `Scheduler` runs one lane per reactor beside the projection
lanes, under the advisory lock `r:{name}`, so one host at a time runs it.

| Failure | What happens |
| --- | --- |
| Transient (guest `timeout` or `overloaded`, storage, the Directory, the notifier's `unavailable`) | the checkpoint stays; `failures + 1` and a retry after an equal-jitter backoff (`[application.reactors] retry_base_ms` to `retry_max_ms`) |
| The guest's transient failure on a batch of several events, three times in a row | the same range again, one event at a time (`reactor.isolating`): the events before the failing one commit, and it becomes `checkpoint + 1`, which `skip` accepts |
| Permanent guest failure or malformed reply on a batch of several events | the same range again, one event at a time, isolating the poison event |
| Malformed reply on one event, or a permanent failure with `on_error = "halt"` | `halted` at that position (`reactor.halted`); retried at its retry time, so a fixed guest resumes it; an operator can `retry` now or `skip` |
| Permanent guest failure on one event with `on_error = "skip"` | a `reactor_skips` row (`on_error_skip`) and the checkpoint past it, in one transaction |

Reactors never re-fire: projection catch-up and rebuild never touch their
rows, a new reactor starts at the head unless it says `start = "beginning"`,
and a reactor removed from configuration keeps its row and resumes from it
when re-added (the admin listing shows the gap). Memory and SQLite refuse
`[[reactors]]` with `reactors_require_postgres`, since they have no outbox.

```sh
cargo test --locked -p merezha-application --test postgres --test store_contract --test background --test tenancy --test reactors
cargo test --locked -p merezha-application --test route_access
cargo test --locked -p merezha-cli --test postgres_process --test route_permissions --test reactors
```
