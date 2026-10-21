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
authorization ports are `Guest`, `GuestSession` and `Authorizer`. Application
orchestration and local transports are the next checkpoint, not implemented by
the SDK checkpoint alone.
