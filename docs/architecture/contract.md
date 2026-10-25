# Component and local host contract

Contract checkpoint for issue #3 and the independent service examples in issue #4.
The host implementation follows this contract.

## Component

The authoritative interface is [`wit/service.wit`](../../wit/service.wit).
Export `merezha:service/handler@0.1.0`, with `handle(input: string) ->
result<string, string>`. World `service` has no imports. Services receive opaque
UTF-8 strings, conventionally JSON, and return opaque UTF-8 strings. The host
does not interpret JSON or embed service behavior. Pipelines pass each successful
result to the next step in order and stop on the first error.

Components must be portable binary WebAssembly components using memory32 and
UTF-8 linear canonical ABI. UTF-16, compact UTF-16 and GC canonical options
are rejected to bound host result allocation before guest cleanup. No precompiled native
artifacts or Wasmtime deserialization are accepted. No WASI, filesystem,
environment, network, clock, random or other host imports are linked.

## Manifest version 1

```toml
version = 1

[limits]
memory_bytes = 16777216
fuel = 10000000
max_input_bytes = 65536
max_output_bytes = 65536
timeout_ms = 1000
max_concurrent_invocations = 8

[[services]]
id = "normalize"
path = "components/normalize.wasm"

[[services]]
id = "render"
path = "components/render.wasm"

[[pipelines]]
id = "document"
steps = ["normalize", "render"]
```

All limit fields and `version` are required. `services` and `pipelines` default
to empty lists. Unknown keys are errors at every level. IDs contain only ASCII
letters, digits, `_` or `-`, are 1 through 64 bytes long, and must be unique
within their respective namespace. Service and pipeline namespaces are separate.
Paths are relative to the manifest's parent directory, cannot contain parent
traversal, and must resolve to regular files within that directory. Pipeline
steps refer to existing services. Repeated steps are allowed.

A service may set `application_protocol = 1` (the default) or `2`; any other
value is an invalid manifest. It names the [application protocol](application.md)
version the host speaks to the service. The runtime only records it per
generation (`Catalog::protocols`, `Session::protocol`); raw invocations and
pipelines never interpret it, and `/v1/services` does not list it. On the
loopback `serve` listener, `/v1/services/{id}/invoke` lets a local caller
hand-craft any version 2 request, principal included: the guarantee that only
the host sets the principal covers the application path
(`/v1/application/...`), not raw invocation.

| Setting | Range and meaning |
| --- | --- |
| `memory_bytes` | 65536 through 268435456, multiple of 65536, per fresh store |
| `fuel` | 1 through 1000000000, shared across the complete invocation |
| `max_input_bytes` | 1 through 1048576, initial input and every step input |
| `max_output_bytes` | 1 through 1048576, every successful or error output |
| `timeout_ms` | 1 through 30000, whole invocation, including instantiation |
| `max_concurrent_invocations` | 1 through 64, admission shared by clones of one runtime, no waiting queue |

Hard loader limits are 65536 manifest bytes, 64 services, 64 pipelines, 16 steps
per pipeline, 16777216 bytes per component, and 67108864 component bytes per
registry load. Each separately loaded library runtime has its own admission
counter. The CLI loads one runtime, so its configured limit covers all CLI
invocations. A lower concurrency limit after reload rejects new work until
existing work falls below that limit. Memory limits bound guest linear memory;
the host process and native compilation require separate OS resource controls
for hostile multi-tenant use.

## CLI and HTTP

The workspace will provide library `merezha-runtime`, package `merezha-cli`, and
binary `merezha`.

```sh
cargo run --locked -p merezha-cli -- serve --manifest /path/to/runtime.toml --listen 127.0.0.1:0
```

`--listen` defaults to `127.0.0.1:8080`; port zero chooses an ephemeral port.
Only numeric loopback addresses are accepted. A JSON `listener.ready` event on
stderr reports the actual `address`. Metadata events never include payloads,
guest error text, environment contents or artifact bytes.

| Request | Success |
| --- | --- |
| `GET /health` | `{"status":"ok","generation":1}` |
| `GET /v1/services` | `{"generation":1,"services":["normalize","render"],"pipelines":["document"]}` |
| `POST /v1/services/normalize/invoke` | Raw UTF-8 successful result, HTTP 200 |
| `POST /v1/pipelines/document/invoke` | Raw UTF-8 final result, HTTP 200 |
| `POST /v1/reload` | `{"generation":2}`, reloads the startup manifest path |

Send invocation payload bytes directly with `curl --data-binary`, not a JSON
wrapper or a JSON-encoded string. Content type may be `application/json` or
`text/plain`. Success is `text/plain; charset=utf-8` and includes header
`x-merezha-generation` identifying the snapshot used by the whole invocation.
The JSON listing is sorted. Reload accepts an empty body only.

```sh
curl --fail-with-body -H 'Content-Type: application/json' \
  --data-binary '{"name":"Ada"}' http://127.0.0.1:PORT/v1/pipelines/document/invoke
curl --fail-with-body -X POST http://127.0.0.1:PORT/v1/reload
```

Errors use `{"error":{"code":"CODE"}}`. Stable codes are `invalid_input`
400, `not_found` 404, `guest_error` 422, `input_limit` and `output_limit` 413,
`fuel_exhausted` and `memory_limit` 422, `timeout` 504, `trap` 422, `overloaded`
429, `invalid_manifest` 400, `reload_busy` 409, and `internal` 500. Guest error
text is deliberately omitted from HTTP errors. Library errors preserve their
classification. Transport body rejection also uses `input_limit` 413.

## Reload and isolation

The initial generation is 1. Each successful reload increments it, even when
the manifest is unchanged. Reload is explicit; there is no file watcher.
Replace artifact files and the manifest atomically before calling reload.
Services can be added, removed or replaced without rebuilding the host.
Reload requests are serialized; the HTTP endpoint rejects concurrent reloads
with `reload_busy`. Parsing, reference validation, byte bounds, component
compilation, import checks, ABI validation and bounded instantiation all precede
publication. Failure keeps the active generation and service registry unchanged.

Each invocation captures one immutable registry snapshot before admission and
holds it through its entire pipeline. Already admitted work finishes on its old
snapshot after reload. Every step gets a fresh store with empty capabilities;
guest mutable state does not persist between steps or requests. Fuel remaining
after a step carries into the next step, and all steps share the same monotonic
deadline. There are no automatic retries.

This listener is for trusted local users. Loopback is not authentication.
Local processes can invoke services and request reload. Browser-originated
simple requests and foreign `Host` headers are also accepted; there is no
Origin/Host validation or DNS-rebinding defense in this local-only interface.
Remote access, multi-tenant administration, unchanged Node application
compatibility, WASIX, PostgreSQL and development tool execution remain outside
this component host.
