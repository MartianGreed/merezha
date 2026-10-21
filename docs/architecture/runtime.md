# Runtime decisions and operating boundary

Merezha loads component services, invokes them in fresh stores, and composes
successful string results through named pipelines. The host owns the catalog
and execution policy. Guest components own application behavior. The transport
is a trusted local administration interface.

See [the versioned contract](contract.md) for the WIT, complete manifest, CLI,
URLs, error codes, ranges and reload recipe.

## Ownership and activation

The library is independent of HTTP. A runtime owns one Wasmtime engine, an epoch
ticker, an immutable active registry behind an `RwLock`, a reload mutex, and an
atomic admission counter. Preparing an invocation captures its registry and
reserves a slot. The reservation lives until completion or drop, including body
reception and blocking executor scheduling. There is no invocation wait queue.

Reload holds the mutex for reading, validation, compilation and publication.
It constructs a complete candidate without changing the active registry, then
publishes one new `Arc` and generation. A failure changes neither. Old admitted
pipelines retain their entire old snapshot, including limits and all steps.
This prevents stale concurrent loads from overwriting a later generation.

Paths resolve under the startup manifest's canonical parent. Traversal, absolute
paths, symlinks escaping the root, nonregular files, unknown TOML fields,
duplicate IDs, invalid references and ranges all fail before activation.
Artifacts must have the binary component header. Only portable Wasm is compiled;
there is no native artifact deserialization or persistent compilation cache.
The empty linker rejects imports. ABI lookup and bounded instantiation happen
before publication. Validation does not call the application handler.

The administrator must publish immutable artifact files and the manifest with
atomic renames. This version does not defend against a malicious local user
swapping filesystem paths between metadata checks and open, or mixing artifacts
while the load reads multiple files. Such a user already controls this process's
local administration inputs. It does not watch files or retry failed loads.

## Invocation budgets

Each pipeline holds one admission slot and one monotonic deadline. Every step
gets a fresh store. Remaining fuel, including canonical ABI work and guest
cleanup, carries into the next store. Time does not reset between steps.
The epoch thread checks every two milliseconds; scheduling delays can make
wall-clock interruption late. A final deadline check prevents a late success.
Fuel is deterministic work accounting, not a CPU rate limit.

One store permits one linear memory, at most 32 core instances, 16 tables, and
10000 elements per table. The memory cap is 64 KiB through 256 MiB. Initial
memory and growth use the same limiter. Growth failure traps and is classified
as `memory_limit`. Threads, shared-memory execution, memory64 and host imports
are unavailable. No guest inherits filesystem, environment or network access.

Loader bounds are 64 KiB manifest, 64 services, 64 pipelines, 16 steps per
pipeline, 16 MiB component bytes, and 64 MiB total component bytes per candidate.
Compilation is serialized and performed away from Tokio workers. These byte and
count limits bound inputs, not native compiler CPU time or process RSS. Old
registries can remain alive while at most 64 admitted invocations reference
them. Host compilation, code memory, network buffers and runtime overhead need
OS controls before use with hostile tenants. There is no production density or
cross-platform performance claim in this change.

## Canonical ABI allocation

Wasmtime is pinned to 49.0.3. Its `TypedFunc::call` runs guest post-return before
returning to the caller. A `WasmStr` cursor would then point at bytes the guest
may already have overwritten. An ordinary `String` lift would copy a
potentially oversized result before the host could check its configured limit.

`bounded.rs` uses a small, documented unsafe `ComponentType`/`Lift` wrapper.
Its ABI and type check delegate exactly to `String`. It checks the canonical
memory32 UTF-8 length before delegating the owned lift to Wasmtime. The owned
copy therefore exists before guest cleanup, and never exceeds the configured
output cap. Pointer, alignment and UTF-8 validation remain Wasmtime's work.
Both success and guest-error strings use this bound.

The loader rejects UTF-16, compact UTF-16 and GC canonical options throughout
the component. The engine disables memory64. A thread-local scoped limit is
valid because calls are synchronous, have no host imports or reentrancy, and
never migrate between threads. Its guard restores the previous value even on
unwind. This wrapper depends on hidden Wasmtime ABI types. Any Wasmtime update
requires reviewing it against the locked source and rerunning oversized-return,
invalid pointer/UTF-8, concurrent-call and cleanup-overwrite tests. There are no
raw pointer dereferences or transmutations in this wrapper.

Primary implementation references are Wasmtime's tagged
[typed call and lift implementation](https://github.com/bytecodealliance/wasmtime/blob/v49.0.3/crates/wasmtime/src/runtime/component/func/typed.rs),
[automatic post-return](https://github.com/bytecodealliance/wasmtime/blob/v49.0.3/crates/wasmtime/src/runtime/component/func.rs), and
[store resource limits](https://github.com/bytecodealliance/wasmtime/blob/v49.0.3/crates/wasmtime/src/runtime/limits.rs).
The installed 49.0.3 sources were checked directly against Cargo.lock.

## Local transport and lifecycle

The executable accepts numeric loopback addresses only. Port zero is supported.
It validates the catalog before opening a listener and reports the actual
address as a JSON `listener.ready` event on stderr. `/health` reflects an active
registry; it does not probe databases or external applications.

The HTTP router admits at most 128 active requests. Guest admission is separately
bounded by the active manifest. Request bodies are byte-limited and share the
invocation deadline. Reload has one admission permit and rejects concurrent
requests with `reload_busy`; its body must be empty. There are no automatic
retries. Client disconnect can leave a bounded blocking call running until its
fuel/deadline ends, retaining its slot. It cannot launch unlimited detached calls.

SIGINT or SIGTERM stops accepting and drains admitted HTTP work. Guest execution
has a maximum 30-second deadline. Native compilation is not forcibly cancelled.
The ticker stops and joins when the last runtime or admitted invocation drops.
No guest process, background job, database or persistent state is created.

Lifecycle and completed-execution events contain Unix UTC timestamps, service
version, event name, generation, request ID, elapsed time, byte counts and stable
error codes. They never contain guest input, output, guest-error text, artifact
contents or environment values. HTTP errors also contain only stable codes.
Detailed typed library load errors are available to library callers. The CLI
reports sanitized startup and reload classifications.

Loopback is not authentication. Any local process can invoke and reload. HTTP
connection count and native process RSS are not hard tenant quotas. Keep the
listener private; do not tunnel it or treat it as a production multi-tenant
service. Authentication, remote administration, OS sandboxing, TLS ingress,
metrics exporters, persistent compile caches and production SLOs are follow-up
work outside this local foundation.

## Research boundary

This component-native route deliberately does not execute unchanged Node apps.
The earlier Node/WASIX experiments remain evidence for a separate compatibility
backend. PostgreSQL, compilers, package installation and developer tools remain
external. This runtime has no database client, environment manager, WASIX layer,
Node shim or engine-specific application logic.

[Research coverage](research-coverage.md) records all fifteen input documents and
what each contributed. Their historical measurements were reviewed, not rerun
or promoted to current host benchmarks.
