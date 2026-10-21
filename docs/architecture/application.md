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
as `aggregate/stream`. It returns `{stream, version, position, events}`. Query
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
Failed commands release the claim. Completed records are retained until
restart; capacity is rejected rather than silently evicting replay protection.
No-key commands can be repeated, and an invariant failure can be retried after
state changes. This is process-local suppression, not external exactly-once
delivery or durable command processing.

Expected-version append commits an entire event batch atomically. Per-stream
versions and global positions start at one. Stored records carry host-assigned
event ID, acceptance timestamp and original message metadata. The event store
is authoritative only for this process lifetime. Restart loses events,
idempotency and projections. No outbox, inbox, snapshots, durable adapter or
cross-process coordination is included.

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
