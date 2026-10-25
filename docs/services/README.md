# Example services

Four small, stateless Wasm components show how independently built services
plug into the Merezha host through `merezha:service/handler@0.1.0`
([`wit/service.wit`](../../wit/service.wit), host contract in
[`docs/architecture/contract.md`](../architecture/contract.md)). The host
knows nothing about them; the manifest names them and orders them into a
pipeline. Two application components build a task board on the same ABI,
using the application protocol of `merezha-domain`; see
[Task board application](#task-board-application).

| Component | Built from | Does |
| --- | --- | --- |
| `text-normalize.wasm` | `services/text-normalize` | Cleans whitespace, control characters and letter case of `text` |
| `greeting.wasm` | `services/greeting` | Adds a localized casual greeting for `name` (or `text`) |
| `greeting-formal.wasm` | `services/greeting`, feature `formal` | Same contract, formal wording; used to demonstrate a replacement |
| `json-envelope.wasm` | `services/json-envelope` | Wraps the object in a versioned envelope with a content digest |
| `task-domain.wasm` | `services/task-domain` | Task board decider: validates and decides `CreateTask`, `RenameTask`, `CompleteTask` |
| `task-view.wasm` | `services/task-view` | Task board read model and the `GetTask`, `ListTasks`, `TaskSummary` queries |
| `task-view-faulty.wasm` | `services/task-view`, feature `faulty` | Fails every projection batch containing `TaskRenamed`; used to demonstrate a failed catch-up |

The services live in their own Cargo workspace, `services/`, with its own
`Cargo.lock`. Host and service dependencies never mix, with one deliberate
exception: the application components use the host workspace's guest SDK,
`crates/domain` (`merezha-domain`), by path. It depends only on Serde and
Serde JSON, never on the host runtime, and its versions resolve through the
services lock.

## Prerequisites

- Rust through rustup (tested with 1.98.0) and the core module target:
  `rustup target add wasm32-unknown-unknown`.
- `curl` and `bash` for the demo.

No other tool is installed globally. The componentization step uses the
`merezha-componentize` tool from the services workspace, pinned by its lock.

## Build

```sh
scripts/build-services.sh
```

This writes the seven components to `examples/welcome/components/`, which is
ignored by Git. Pass another directory as the first argument to build
elsewhere. Each service compiles to a `wasm32-unknown-unknown` core module
carrying the component type that `wit-bindgen` embeds. `merezha-componentize`
then encodes it as a component and fails the build unless the component
imports nothing and exports exactly `merezha:service/handler@0.1.0` with the
types of the canonical `wit/service.wit`, which it embeds at build time.

Why not `wasm32-wasip2`: the host links no WASI or other imports. A Rust
standard-library build for `wasm32-wasip2` always imports `wasi:cli`,
`wasi:io` and `wasi:clocks`, even when the service never calls them, so the
host would reject it. A standard-library build for `wasm32-unknown-unknown`
imports nothing. Services keep plain safe Rust; a panic becomes a trap that
the host classifies.

## Check

```sh
scripts/check-services.sh
```

Runs the demo's response-assertion tests (`scripts/test-demo-assertions.sh`),
`cargo fmt --check`, Clippy with warnings denied for native and component
builds, the native contract tests for both greeting and both task-view
revisions (doctests included) and a full component build in a temporary
directory. The application scenario against the real host is
`scripts/task-board-demo.sh` ([below](#scenario)); CI runs both
(`.github/workflows/services.yml`).

## Demo

```sh
scripts/demo.sh
```

The demo builds the components and the host (`merezha-cli`, or the binary
named by `MEREZHA_BIN`). It copies the example into a private temporary
directory and starts the host there on an ephemeral `127.0.0.1` port. Then it
checks each response against `examples/welcome/expected/`:

1. Stage 1 (`stage-1.toml`): pipeline `welcome` runs `normalize` then
   `greet`. `{"text":"  ada \t LOVELACE\n","case":"title","locale":"fr",…}`
   becomes `"text":"Ada Lovelace"` with `"greeting":"Bonjour, Ada Lovelace !"`,
   and `request_id` and `kind` survive.
2. A malformed call to `normalize` returns HTTP 422 `guest_error`.
3. Stage 2 adds `envelope` and appends it to the pipeline (reload, generation 2).
   A payload carrying `18446744073709551617`, a 34-digit decimal and an object
   keyed `$serde_json::private::Number` comes out with all three unchanged.
4. Stage 3 replaces `greet` with the formal revision without restarting the
   host (generation 3): `"greeting":"Bien le bonjour, Ada Lovelace."`.
5. `invalid-missing-service.toml` is rejected with HTTP 400
   `invalid_manifest`; generation 3 keeps serving the same output.
6. Stage 4 removes `normalize` (generation 4). Calling it returns HTTP 404
   `not_found`, and the shorter pipeline greets an already clean name.

Set `KEEP_SCRATCH=1` to keep the temporary directory and the host log.
The script waits up to `MEREZHA_STARTUP_SECONDS` (default 120) for the host to
listen while the host process is alive; a debug host compiles the catalog
noticeably slower than a release one. The stage manifests are portable:
their component paths are relative, so
`merezha serve --manifest examples/welcome/stage-2.toml` works directly after
a build.

## JSON convention

Every service takes a JSON object and returns a JSON object. It reads and
writes only the fields listed below and copies every other field unchanged.
That is how `request_id` reaches the end of the pipeline. Output keys are
sorted and unique; a repeated top-level key keeps its last value.

Fields a service does not own are copied as JSON text and never decoded.
Numbers of any size or precision, strings, nested key order, repeated nested
keys and keys that look like library internals come out exactly as they went
in; only whitespace between tokens is removed. Because they are not decoded,
these fields are checked for JSON syntax only: an unpaired `\ud800` escape,
for example, passes through unchanged, while the same escape in a field the
service reads is an `invalid_value` error.
Before any service-specific work, each service checks its own bounds,
independently of the host's limits:

| Service | Max input | Max nesting | Max top-level fields |
| --- | --- | --- | --- |
| `text-normalize` | 32 KiB | 16 | 64 |
| `greeting` | 32 KiB | 16 | 64 |
| `json-envelope` | 256 KiB | 32 | 256 |

Nesting counts the top-level object as level 1. Work after these checks is
linear in the input size.

### Errors

A failed call returns `handle`'s `err` case with a JSON document:

```json
{"error":{"code":"invalid_value","field":"locale","message":"field `locale` is \"pt\"; expected one of \"en\", \"fr\", \"es\", \"de\", \"uk\"","service":"greeting"}}
```

| Code | Meaning |
| --- | --- |
| `invalid_json` | The input does not parse as JSON |
| `invalid_type` | The input is not an object, or a field has the wrong JSON type (`null` included) |
| `missing_field` | A required field is absent |
| `invalid_value` | A field has the right type but an unsupported value |
| `limit_exceeded` | The input or a field exceeds a bound above |

`field` is omitted when the error concerns the whole input. The host's HTTP
API reports these as `422 guest_error` and deliberately omits the guest's
text. The full document is what the component returns to library callers and
what the native tests assert.

### `text-normalize`

| Field | In/out | Contract |
| --- | --- | --- |
| `text` | required string, replaced | Control characters removed, trimmed, Unicode whitespace runs collapsed to one space, then cased; must not end up empty |
| `case` | optional string, copied | `keep` (default), `lower`, `upper` or `title` (first letter of each word upper case, the rest lower case), using Unicode case mapping |

### `greeting`

| Field | In/out | Contract |
| --- | --- | --- |
| `name` | optional string, copied | The name to greet, trimmed; 1 to 128 characters, no control characters |
| `text` | optional string, copied | Used as the name when `name` is absent, so the service can follow `text-normalize` |
| `locale` | optional string, copied | `en` (default), `fr`, `es`, `de` or `uk` |
| `greeting` | written | Casual: `Hello, Ada!`, `Bonjour, Ada !`, `¡Hola, Ada!`, `Hallo, Ada!`, `Привіт, Ada!`. Formal: `Good day, Ada.`, `Bien le bonjour, Ada.`, `Buenos días, Ada.`, `Guten Tag, Ada.`, `Добрий день, Ada.` An existing value is replaced |
| `greeting_style` | written | `casual` or `formal`, identifying the component revision that answered |

At least one of `name` and `text` is required. The service rejects names with
control characters and points to `text-normalize`, so a pipeline that skips
normalization fails visibly instead of greeting `ada \t LOVELACE`.

### `json-envelope`

| Field | In/out | Contract |
| --- | --- | --- |
| `kind` | optional string, moved | 1 to 64 characters from `a-z`, `0-9`, `.`, `_`, `-`; default `document` |
| everything else | moved into `data` | Unchanged |

Output: `{"data":{…},"kind":…,"meta":{"data_bytes":…,"digest":"fnv1a64:<16 hex digits>","fields":…},"version":1}`.
`data_bytes` and `digest` describe `data` exactly as emitted (top-level keys
sorted, values as received without whitespace between tokens), and `fields`
counts its top-level fields. The FNV-1a digest identifies
content for deduplication and tests. It is not a security checksum.

## Adding a service

1. Add a crate under `services/` with `crate-type = ["cdylib", "rlib"]`, a
   `merezha-service-kit` dependency and `wit-bindgen` for `wasm32` only, as
   in `services/greeting/Cargo.toml`.
2. Implement `pub fn handle(input: &str) -> Result<String, String>` with
   `SERVICE.run`, and export it in a `#[cfg(target_arch = "wasm32")]` module
   with `wit_bindgen::generate!({ path: "../../wit", world: "service" })`.
3. Add native contract tests, a `build` line in `scripts/build-services.sh`
   and a `[[services]]` entry in a manifest, then reload the host.

An application component follows the same steps with a `merezha-domain`
dependency instead of the kit: its `handle` calls
`merezha_domain::handle(input, dispatch)` and matches the protocol's
`Operation`, as `services/task-domain` and `services/task-view` do. Route its
messages in an application TOML file; changing routes needs a host restart,
while replacing the component only needs a reload.

## Task board application

The task board shows Structure-style application building blocks running as
independently built components: a decider (`task-domain`) owns every
business rule, a projection (`task-view`) owns the read model and its
queries, and a shared schema crate (`services/task-events`) defines the
events both understand. The host owns routing, validation order,
idempotency, the event history, versions, checkpoints and the read model's
storage, as described in
[`docs/architecture/application.md`](../architecture/application.md). The
components never see one another and keep no state between calls; the host
passes the stream history or the read model's state into each call.

Files in `examples/task-board/`:

| File | Purpose |
| --- | --- |
| `runtime.toml` | Catalog: `task-domain` (`application_protocol = 2`), `task-view`, and the stateless `normalize` and `greet` services with the `welcome` pipeline |
| `runtime-faulty.toml` | Same catalog with the faulty `task-view` revision |
| `application.toml` | Command, query and projection routes and the store limits |
| `fixtures/tasks.json` | The three tasks the scenario creates first |
| `expected/*.json` | Read-model observations the scenario and the native tests compare against |

Run it by hand after building into the example's own directory:

```sh
scripts/build-services.sh examples/task-board/components
cargo run --locked -p merezha-cli -- serve --manifest examples/task-board/runtime.toml \
  --application examples/task-board/application.toml --listen 127.0.0.1:0
```

Read the address from the `listener.ready` event, then send commands and
queries as JSON and drive the projection explicitly:

```sh
curl --fail-with-body -H 'Content-Type: application/json' --data-binary \
  '{"tag":"CreateTask","stream":"task-1","payload":{"title":"Write the guide"},"metadata":{"message_id":"m-1","correlation_id":"c-1","actor":"ada"}}' \
  http://127.0.0.1:PORT/v1/application/commands
curl --fail-with-body -X POST http://127.0.0.1:PORT/v1/application/projections/tasks/catch-up
curl --fail-with-body -H 'Content-Type: application/json' --data-binary \
  '{"tag":"GetTask","payload":{"task_id":"Task/task-1"},"metadata":{"message_id":"m-2","correlation_id":"c-1"}}' \
  http://127.0.0.1:PORT/v1/application/queries
```

The command answers a receipt, `{"stream":"Task/task-1","version":1,"position":1,"events":1}`;
the query answers `{"value":{…},"checkpoint":1}`. Commands do not update the
read model by themselves: until a catch-up, queries answer from the last
checkpoint.

### Commands

`task-domain` answers `validate` and `decide`. Each task is one stream of
aggregate `Task`; the host names it `Task/<stream>` and passes that name to
`decide`, which records it in every event as `task_id`.

| Command | Payload | Accepted when | Emits |
| --- | --- | --- | --- |
| `CreateTask` | `{"title":…}` | The task has no history | `TaskCreated` `{task_id,title,owner?}` |
| `RenameTask` | `{"title":…}` | The task is open and the title changes | `TaskRenamed` `{task_id,title}` |
| `CompleteTask` | `{}` | The task is open | `TaskCompleted` `{task_id}` |

A title is 1 to 120 Unicode scalar values without control characters.
Validation trims surrounding whitespace and returns that canonical payload,
so `" Write docs "` and `"Write docs"` are the same command, including for
idempotency. Unknown fields are rejected. Every event has schema version 1,
except a `TaskCreated` that records an owner, which has schema version 2.

`task-domain` speaks [application protocol](../architecture/application.md#protocol-version-2)
version 2, declared in `runtime.toml`. When the host established a principal
(`metadata.principal`), `CreateTask` records its subject as `owner`, and
`RenameTask` and `CompleteTask` on an owned task require that subject or the
role `admin`, else `not_owner`. A task created without a principal, which
includes every task of the local demos (`serve` authenticates nobody) and all
history written before version 2, has no owner and accepts every caller.
Clients cannot set the owner: it is not part of any payload.

`decide` checks the history before using it: every event belongs to the
addressed stream, versions run 1, 2, 3… and each event can follow the
previous state. Then it rehydrates the task and decides. A rejection emits no
event, so the host appends nothing.

### Queries

`task-view` answers `validate` and `query` for these tags and `project` for
projection `tasks`.

| Query | Payload | Answer `value` |
| --- | --- | --- |
| `GetTask` | `{"task_id":"Task/task-1"}` | `{"task_id","title","status","version"}`; `status` is `open` or `completed` |
| `ListTasks` | `{"status"?,"after"?,"limit"?}` | `{"tasks":[…],"next"?}` in id order after `after`; `limit` 1 to 100, default 50; `next` is present only when more tasks match |
| `TaskSummary` | `{}` | `{"total","open","completed"}` |

The read model's state is `{"tasks":{"Task/task-1":{"title","status","version"}}}`,
starting from `{"tasks":{}}`. `project` folds a batch in global order and
returns the new state, or fails without returning any state. An event whose
version the row already reflects is skipped, so a redelivered batch changes
nothing; an event that does not directly follow the row's version, or cannot
follow its status, fails the batch with `projection_gap`. The view has no
side effects, so a rebuild (`live: false`) is handled like live delivery.

### Failure codes

The components answer failures inside the protocol response, as a code and
a classification. The host maps them to HTTP: `permanent` to 422,
`conflict` to 409 and `transient` to 429, with
`{"error":{"code":…,"classification":…}}` (see
[`docs/architecture/application.md`](../architecture/application.md) for
host codes such as `handler_not_found`, `idempotency_mismatch` and
`concurrency_conflict`).

| Code | Class | From | Meaning |
| --- | --- | --- | --- |
| `validation_failed` | permanent | both | The payload does not have the message's shape |
| `invalid_title` | permanent | domain | Blank, too long or contains control characters |
| `invalid_limit` | permanent | view | `ListTasks` limit outside 1 to 100 |
| `unknown_command`, `unknown_query` | permanent | both | The tag is not this component's |
| `task_exists` | permanent | domain | `CreateTask` on a task with history |
| `task_not_found` | permanent | both | Rename or completion of a task without history; `GetTask` of an unknown task |
| `title_unchanged` | permanent | domain | `RenameTask` to the current title |
| `task_completed` | permanent | domain | `RenameTask` after completion |
| `task_already_completed` | permanent | domain | `CompleteTask` twice |
| `not_owner` | permanent | domain | `RenameTask` or `CompleteTask` on an owned task by neither its owner nor an `admin` |
| `corrupt_history` | permanent | domain | The history breaks the checks above |
| `unknown_event`, `unsupported_schema_version`, `invalid_event` | permanent | both | A recorded event this schema does not define (an owned `TaskCreated` must be schema version 2, an unowned one version 1) |
| `projection_gap` | permanent | view | A batch skips or contradicts a task's versions |
| `invalid_state`, `unknown_projection` | permanent | view | The state or projection name is not this component's |
| `projection_failed` | transient | faulty view | The faulty revision met `TaskRenamed` |
| `unsupported_operation` | permanent | both | `project`/`query` sent to the domain, `decide` to the view |

### Scenario

```sh
scripts/task-board-demo.sh
```

The script builds the components, the host (or uses `MEREZHA_BIN`) and the
scenario driver `services/scenario`, copies the example into a private
temporary directory and starts the host there on an ephemeral `127.0.0.1`
port with a fresh in-memory store. The driver then checks, in order:

1. The catalog lists the application and stateless services, and the
   `welcome` pipeline answers exactly `examples/welcome/expected/stage-1-welcome.json`.
2. Three `CreateTask` commands from `fixtures/tasks.json` get versions 1 and
   positions 1 to 3. A query before catch-up still shows the empty read model
   at checkpoint 0; catch-up then matches `expected/catch-up-created.json`.
3. A rename and a completion are accepted at positions 4 and 5.
4. Completing twice, renaming after completion, creating an existing task,
   renaming an unknown task, renaming to the current title (known only by
   replaying the earlier rename), an invalid title, an unknown field and an
   unknown tag are all rejected. The next accepted command lands at position
   6, so none of them appended an event.
5. Idempotency: the same key and payload replay the same receipt, also with
   a title that only differs in surrounding whitespace; a changed payload
   is `idempotency_mismatch`; another actor using the same key runs its own
   command.
6. A stale `expected_version` fails with `concurrency_conflict`; retrying the
   same key with the current version succeeds, so the failed claim was
   released, and a repeat replays it.
7. Six concurrent renames of one task with the same expected version: one
   is accepted at version 3, five fail with `concurrency_conflict`.
8. Six concurrent duplicates of one key: one execution at position 10; the
   others replay its receipt or answer `idempotency_in_flight`.
9. Catch-up to checkpoint 10; `TaskSummary`, `GetTask`, a paged `ListTasks`
   and two query rejections.
10. A reload installs the faulty view. A rename is still accepted (position
    11), but catch-up fails twice with transient `projection_failed` and
    queries keep answering from checkpoint 10.
11. A reload restores the correct view; catch-up resumes from checkpoint 10
    to 11 and shows the new title.
12. Rebuild replays the whole history into exactly the caught-up state,
    `expected/rebuild-final.json` (the concurrent winner's title filled in).
13. After three queries the next command lands at position 12: queries
    appended nothing. The `welcome` pipeline still answers as in step 1.

Set `KEEP_SCRATCH=1` to keep the temporary directory and the host log.
The script waits up to `MEREZHA_STARTUP_SECONDS` (default 120) for the host to
listen while the host process is alive; a debug host compiles the catalog
noticeably slower than a release one.

### Local task board with durable storage

To keep a task board between runs, serve it with the host's SQLite storage:

```sh
scripts/task-board-local.sh
```

The script builds the components and a release host, writes the manifests
into `.merezha/task-board` (git-ignored, and kept by `cargo clean`) and serves
on `127.0.0.1:8787` until Ctrl-C. The first run initializes
`.merezha/task-board/task-board.sqlite`, private to your user; later runs
reopen it. Pass another directory as the first argument, or set
`MEREZHA_LISTEN` to another numeric loopback address such as `127.0.0.1:9000`. Delete the directory to start
over; reopen also refuses the database once `examples/task-board/application.toml`
changes its routes or limits, so that needs a fresh directory too. Only one host
can own the database at a time.

From a second terminal, the `task-board` client sends commands and queries
and prints the host's answers:

```sh
alias task-board='cargo run --manifest-path services/Cargo.toml --locked -q -p merezha-task-board-cli --'
task-board create groceries "Buy milk"
task-board create report "Write the report"
task-board complete groceries
task-board list open
task-board summary
task-board get groceries
task-board rename report "Write the final report"
```

Each `TASK` (here `groceries` and `report`) is the stream of one task, so
its `task_id` is `Task/groceries`; it may use ASCII letters, digits and `._-:/`. Queries catch the read model up first;
`task-board catch-up` and `task-board rebuild` run those operations alone. A
rejected command such as a second `complete groceries` prints the host's
error, for example `task_already_completed`, and exits with status 1. Set
`MEREZHA_ADDRESS` to reach another address and `MEREZHA_ACTOR` to record
another actor than `local`.

Stop the host, or kill it, and start the script again: the tasks and their
full history are still there, the read model resumes from its last saved
checkpoint, and the domain rules still see every earlier event.

```sh
scripts/task-board-restart-demo.sh
```

checks exactly that on a private scratch directory and an ephemeral port. It
creates two tasks and completes one, kills the host with `SIGKILL`, starts it
again on the same database, then checks the summary, the task states, that
completing or creating again is still rejected, a rename at version 2, a
rebuild replaying all four events and a graceful stop with status 0. It
accepts `MEREZHA_BIN`, `KEEP_SCRATCH` and `MEREZHA_STARTUP_SECONDS` like the
scenario. See [owned SQLite storage](../architecture/application.md#owned-sqlite-storage)
for ownership, recovery and backup rules.

### JSON numbers

The application protocol decodes values with Serde JSON's `i64`/`u64`/`f64`
representation. The host and SDK reject integral values outside
±9,007,199,254,740,991 and round fractions to binary64; decimal precision is
not preserved. The task board carries only strings and small integers
(versions, positions, counts and a `limit` of at most 100), so it is not
affected; an application that needs exact decimals should carry them as
validated strings. The stateless services keep their exact behaviour: fields
they do not own still pass through as raw JSON text, numbers included.

### Limits

Everything the task board stores is bounded and lives in host memory only;
nothing survives a host restart and nothing is durable. The example's
`application.toml` spells out the host's default limits: a 5 second deadline
per dispatch, at most 4096 events, 256 streams, 8 MiB of stored events, 1024 idempotency records, 16 KiB
per payload, 64 KiB of read-model state, 16 events per catch-up batch and 16
concurrent dispatches. The host refuses work that would exceed them instead
of evicting history. The components add their own bounds: titles of at most
120 characters and `ListTasks` pages of at most 100 tasks. Each call also runs
under the manifest's guest limits; `runtime.toml` allows 256 KiB of input and
output per call so that a stream's history or a projection batch with its
state fits.

Idempotency records keep a command's receipt only while the host runs; they
suppress duplicate execution of one key, which is not exactly-once delivery
of effects outside the store.

### What the example covers from Structure

| Structure package | Shown here | Not shown here |
| --- | --- | --- |
| `domain` | Decider (`initial`/`decide`/`evolve`), validated identities, an event schema, classified failures | Value objects, repositories |
| `cqrs` | One handler per tag, typed validation before the handler, idempotency by tag, actor and key with replay, mismatch, in-flight and released failed claims | Authorization beyond the host's allow-all demo policy, timeouts per dispatch |
| `eventsourcing` | Expected-version appends, per-stream versions, global positions, rehydration, checkpointed catch-up, failed batches that keep the checkpoint, rebuild | Snapshots, upcasters (schema version 2 is rejected), outbox/inbox, history import, partitions |
| `viewmodel` | One projection as the single writer of its read model, eventually consistent queries | SQL tables, generated migrations, typed stores |
| `fixtures` | A scenario that creates its data only through commands, on an isolated store | Fixture catalogs, run identifiers, the fixtures CLI |

The complete map of all 27 Structure packages, with the delivered subsets and
what remains, is [`docs/architecture/structure-parity.md`](../architecture/structure-parity.md).

## Limitations

- The digest is not cryptographic, and services keep no state between calls
  by design. The host gives every call a fresh instance; the task board's
  history and read model live in the host and are passed in on each call.
- Greetings cover five locales. Title case is per word and has no
  language-specific rules, such as Dutch `IJ` or Irish prefixes.
- The demos need a runnable `merezha-cli` from the host workspace.
- The task board's store is the host's bounded in-memory store: history,
  read model and idempotency records disappear when the host stops. See
  [Limits](#limits).
