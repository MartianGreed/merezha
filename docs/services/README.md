# Example services

Four small, stateless Wasm components show how independently built services
plug into the Merezha host through `merezha:service/handler@0.1.0`
([`wit/service.wit`](../../wit/service.wit), host contract in
[`docs/architecture/contract.md`](../architecture/contract.md)). The host
knows nothing about them; the manifest names them and orders them into a
pipeline.

| Component | Built from | Does |
| --- | --- | --- |
| `text-normalize.wasm` | `services/text-normalize` | Cleans whitespace, control characters and letter case of `text` |
| `greeting.wasm` | `services/greeting` | Adds a localized casual greeting for `name` (or `text`) |
| `greeting-formal.wasm` | `services/greeting`, feature `formal` | Same contract, formal wording; used to demonstrate a replacement |
| `json-envelope.wasm` | `services/json-envelope` | Wraps the object in a versioned envelope with a content digest |

The services live in their own Cargo workspace, `services/`, with its own
`Cargo.lock`. Host and service dependencies never mix.

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

This writes the four components to `examples/welcome/components/`, which is
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

Runs `cargo fmt --check`, Clippy with warnings denied for native and
component builds, the native contract tests for both greeting revisions
(doctests included) and a full component build in a temporary directory.

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

Set `KEEP_SCRATCH=1` to keep the temporary directory and the host log. The
stage manifests are portable: their component paths are relative, so
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

## Limitations

- The digest is not cryptographic, and services keep no state between calls
  by design. The host gives every call a fresh instance.
- Greetings cover five locales. Title case is per word and has no
  language-specific rules, such as Dutch `IJ` or Irish prefixes.
- The demo needs a runnable `merezha-cli` from the host workspace.
