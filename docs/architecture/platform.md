# Platform kernel

Status: stage K of the [backend framework program](framework.md). Two crates
form the kernel every block builds on:

- `merezha-postgres` (`crates/postgres`): the PostgreSQL pool, deadline-scoped
  transactions, the module migration runner and the `TestDatabase` harness.
- `merezha-platform` (`crates/platform`): `ProjectConfig` for `merezha.toml`,
  the `Module` trait, `AppContext`, the cross-module ports, the HTTP edge,
  health endpoints, structured logging and the host lifecycle.

`merezha run --config merezha.toml` (`crates/cli/src/run.rs`) puts them
together with the existing runtime and application layer. `merezha serve`
is unchanged.

## Running a project

```sh
cargo run --locked -p merezha-cli -- run --config merezha.toml
```

A minimal `merezha.toml`, with paths relative to the file:

```toml
[app]
id = "tasks"

[database]
url = { env = "DATABASE_URL" }
migrate = true

[runtime]
manifest = "runtime.toml"

[application]
path = "application.toml"
```

Startup runs in this order and stops at the first failure with exit code 2
and one `startup.failed` event carrying a stable `code`:

1. Load the configuration. All problems are reported together as
   `{"event":"startup.failed","code":"invalid_config","issues":[{"key","code","origin"}]}`.
   Issues name settings and layers, never values.
2. Install the log subscriber from `[telemetry]`.
3. Load the runtime manifest (`invalid_manifest`).
4. Open the optional application with its configured storage (memory or
   SQLite, as with `serve`).
5. Open the pool and prove it with `SELECT 1` (`database_unavailable`).
6. Apply pending migrations when `database.migrate = true`, otherwise verify
   them. A changed checksum, unknown applied version or out-of-order version
   stops startup in both modes. Pending migrations in verify mode do not stop
   startup; readiness stays false until they are applied.
7. Start modules in registration order, each within 10 s
   (`module_start_failed`; modules already started are shut down).
8. Bind `http.listen` and log `listener.ready` with the bound address.

SIGINT or SIGTERM starts a graceful stop: readiness turns false, the listener
stops accepting, in-flight requests drain for up to 30 s, modules shut down in
reverse order within 10 s, the pool closes, `listener.stopped` is logged and
the process exits 0. A serving failure exits 1.

### Routes

| Path | Owner | Mounted when |
| --- | --- | --- |
| `GET /health/live` | kernel | always |
| `GET /health/ready` | kernel | always |
| `/v1/<module>/...` | each enabled module | its `[modules.<module>]` section exists |
| `/v1/application/...` | application layer | `[application]` is set |
| `/health`, `/v1/services/...`, `/v1/pipelines/...`, `/v1/reload` | raw runtime | the listener is private |

Without the identity block there is no authentication. A listener is private
when `http.public` is false and `http.listen` is a loopback address. A private
listener keeps the explicit allow-all policy and the raw runtime endpoints, as
`serve` does. A public listener gets the deny-all policy, so every application
command and query fails `403 unauthorized`, and the raw runtime endpoints are
not mounted. A deployment exposes nothing unauthenticated by accident.

## Configuration reference

Layers, lowest to highest precedence: code defaults, the file,
`MEREZHA_<SECTION>__<KEY>` environment variables, then command-line flags.
Unknown sections and keys fail. Every setting needs a restart; nothing is
reloaded at runtime. The raw runtime's `/v1/reload` still rereads its own
manifest.

Environment variables use `__` between segments: `MEREZHA_HTTP__LISTEN`,
`MEREZHA_HTTP__CORS__ALLOWED_ORIGINS`, `MEREZHA_MODULES__GREETER__GREETING`.
Variables starting with `MEREZHA_` but without `__`, such as
`MEREZHA_TEST_DATABASE_URL`, are not configuration. Lists are comma
separated, booleans are `true` or `false`, and module values are parsed as
TOML literals (`"\"hi\""`, `3`, `true`), falling back to a plain string.

Flags: `--listen ADDRESS` sets `http.listen`; `--set SECTION.KEY=VALUE` sets
any non-secret setting (at most 64).

Secret settings accept `{ env = "NAME" }` or `{ file = "PATH" }` in the file,
never a literal string (`secret_literal`). A `MEREZHA_` environment variable may
carry the secret itself, so `MEREZHA_DATABASE__URL=postgres://...` works. A
flag never can (`secret_on_command_line`). Secret files are read once, at most
64 KiB, with one trailing newline removed. `Debug` output and errors never
contain secret values.

### `[app]`

| Key | Type | Default | Secret | Notes |
| --- | --- | --- | --- | --- |
| `id` | string | required | no | `[a-z][a-z0-9-]*`, at most 63 bytes |
| `name` | string | `id` | no | 1 to 128 bytes |

### `[http]`

| Key | Type | Default | Secret | Notes |
| --- | --- | --- | --- | --- |
| `listen` | socket address | `127.0.0.1:8080` | no | a non-loopback address needs `public = true` |
| `public` | boolean | `false` | no | see [Routes](#routes) |
| `trusted_proxies` | list of CIDR or addresses | `[]` | no | at most 64; IPv4 and IPv6 |
| `body_limit` | integer, bytes | `1048576` | no | 1024 to 67108864 |

### `[http.cors]`

CORS is off while `allowed_origins` is empty.

| Key | Type | Default | Secret | Notes |
| --- | --- | --- | --- | --- |
| `allowed_origins` | list of strings | `[]` | no | exact lowercase `http(s)://host[:port]`, or `["*"]` alone; at most 64 |
| `allow_credentials` | boolean | `false` | no | refused with `*` |
| `max_age_seconds` | integer | `600` | no | 0 to 86400 |

Allowed methods are GET, POST, PUT, PATCH and DELETE. Allowed request
headers are `content-type`, `authorization`, `x-request-id` and
`traceparent`. `x-request-id` is exposed to scripts.

### `[database]`

| Key | Type | Default | Secret | Notes |
| --- | --- | --- | --- | --- |
| `url` | libpq URL or key/value string | required | yes | |
| `pool_size` | integer | `16` | no | 1 to 256 connections |
| `connect_timeout_ms` | integer | `5000` | no | 100 to 60000; also bounds pool creation and recycling |
| `acquire_timeout_ms` | integer | `2000` | no | 10 to 60000; wait for a pooled connection |
| `statement_timeout_ms` | integer | `5000` | no | 10 to 300000; session default and ceiling for transactions |
| `migrate` | boolean | `false` | no | apply pending migrations at startup instead of verifying |
| `tls` | `disable` or `require` | `disable` | no | `require` verifies the chain and host name |
| `tls_ca_file` | path | none | no | extra PEM roots, at most 1 MiB; only with `tls = "require"` |

With TLS the platform trust store is used through `rustls-platform-verifier`,
plus `tls_ca_file` when set. There is no mode that skips verification.

### `[runtime]`

| Key | Type | Default | Secret | Notes |
| --- | --- | --- | --- | --- |
| `manifest` | path | required | no | the [runtime manifest](contract.md) |

### `[application]`

Optional. Without it no application endpoints are mounted.

| Key | Type | Default | Secret | Notes |
| --- | --- | --- | --- | --- |
| `path` | path | required in the section | no | the [application configuration](application.md); its SQLite `path` keeps `serve`'s meaning |

### `[telemetry]`

| Key | Type | Default | Secret | Notes |
| --- | --- | --- | --- | --- |
| `log_level` | `error`, `warn`, `info`, `debug`, `trace` | `info` | no | filters the log output only |
| `format` | `json` or `text` | `json` | no | written to stderr |

### `[modules.<name>]`

Each table is opaque to the kernel and validated by the module of that name
through `Module::configure`. Module issues are reported with kernel issues. A
section without a registered module fails `unknown_module`. A registered
module without a section stays disabled. `ModuleSection::secret` resolves
secrets with the kernel rules.

### Configuration error codes

`config_unreadable`, `config_too_large` (256 KiB), `invalid_toml` (reported as
`line N`, never with the text), `unknown_section`, `unknown_key`,
`expected_table`, `expected_string`, `expected_boolean`, `expected_integer`,
`expected_string_list`, `too_many_items`, `out_of_range`, `invalid_choice`,
`invalid_path`, `missing_required`, `invalid_app_id`, `invalid_name`,
`invalid_address`, `public_listen_not_enabled`, `invalid_cidr`,
`invalid_origin`, `invalid_wildcard_origin`, `invalid_database_url`,
`tls_ca_without_tls`, `secret_literal`, `secret_on_command_line`,
`secret_unavailable`, `secret_too_large`, `invalid_secret_reference`,
`invalid_module_name`, `unknown_module`, `invalid_module_registration`.

## The HTTP edge

`edge::apply` wraps the composed router. For every request it:

- takes the client address from the socket, or from `X-Forwarded-For` when
  the peer is in `http.trusted_proxies` (the rightmost hop that is not a
  trusted proxy). `X-Forwarded-Proto` and `X-Forwarded-Host` are honoured
  only from trusted peers. All `X-Forwarded-*` and `Forwarded` headers are
  removed before handlers run; handlers read `ClientInfo`.
- accepts `X-Request-Id` only from a trusted proxy (1 to 128 bytes of
  `[A-Za-z0-9._:/-]`), otherwise generates 128 random bits, and returns it in
  the response.
- continues a valid W3C `traceparent` with a new span id or starts a new
  trace. Handlers read `TraceContext`; `OutboundHttp::send` propagates it.
- refuses bodies over `http.body_limit`: a larger `Content-Length` fails at
  once and an undeclared body fails while it is read.
- applies CORS and the optional `Authenticator`.
- logs one `http.request` event with method, matched route, status, duration,
  request id and trace id. It never logs paths, query strings, headers or
  bodies.

Every error response has one shape:

```json
{"error":{"code":"not_found","classification":"permanent"}}
```

`edge::error_response(&Failure)` produces it with a status from
`edge::status_for`: fixed codes first (`not_found` 404, `unauthenticated` 401,
`unauthorized` 403, `body_limit` 413, `upstream_timeout` 504, ...), then by
classification (permanent 422, conflict 409, transient 503). A final layer
normalizes anything else: the raw runtime's `{"error":{"code"}}` documents
gain the classification implied by their status, and plain-text rejections
from extractors become a code derived from the status. The status never
changes. JSON objects without an `error` member, such as readiness, pass
through. `serve` keeps its existing error bodies.

## Health

`GET /health/live` answers `{"status":"live"}` while the process serves.

`GET /health/ready` answers `200 {"status":"ready"}` or
`503 {"status":"not_ready"}`. It checks, each bounded by 1 s: not draining,
the pool answers `SELECT 1`, migrations are current, and every module's
`ready()`. If migrations were pending at startup, readiness verifies them
again until another host has applied them. Concurrent probes within 250 ms
share one evaluation. The response never names the failing check; the log
gets `health.not_ready` with check names on each transition.

## Logging

`telemetry::init` installs a `tracing` subscriber that writes JSON lines (or
text) to stderr. Each event has a stable `event` field and allowlisted
fields only:

`startup.failed`, `runtime.loaded`, `database.connected`,
`database.unavailable`, `migrations.checked`, `migrations.refused`,
`migration.applied`, `module.started`, `module.start_failed`, `module.stopped`,
`module.shutdown_timeout`, `listener.ready`, `listener.draining`,
`listener.drain_timeout`, `listener.stopped`, `http.request`, `health.ready`,
`health.not_ready`, `notification.captured`.

The telemetry block will pass its OTLP layers as `extra` to `telemetry::init`.
They see every event regardless of `log_level`. The raw runtime still writes
its own `invocation.completed` and `registry.*` JSON lines.

## Module contract

`Module` (`crates/platform/src/module.rs`) is what every block implements:

| Method | Contract |
| --- | --- |
| `name()` | `[a-z][a-z0-9_]*`, at most 64 bytes, unique, not `application`, `services`, `pipelines`, `reload` or `health`. Names the section, route prefix and migration ledger key; never changes. |
| `configure(&ModuleSection)` | validate and keep `[modules.<name>]`; return every issue at once. Default: only an empty section. |
| `migrations()` | ordered forward-only `Migration { version, name, sql }` for its own schema |
| `events()` | `ModuleEvents { publishes, consumes }`, logged at start |
| `start(AppContext)` | start background work within the start timeout |
| `routes(&AppContext)` | an axum `Router` nested under `/v1/<name>` |
| `ready()` | readiness contribution, answered within 1 s |
| `shutdown(deadline)` | stop and drain background work after the listener drained |

`host::configure(&ConfigInput, modules)` loads the configuration and lets the
registered modules validate their sections. `Host::start`, `Host::bind` and
`Host::serve` run the lifecycle above. `cli::run::run(input, modules,
shutdown)` is the same entry point the binary uses, for embedding hosts that
register blocks.

## AppContext

`AppContext` (`crates/platform/src/context.rs`) is cheap to clone and holds:

- `database()`: the `Database` pool.
- `clock()`: `Clock` with wall-clock `now()` and monotonic `monotonic()`;
  `SystemClock` by default, replaceable in tests.
- `random()`: `Random`, the operating system CSPRNG through `getrandom`;
  `context::random_token` makes 256-bit hex secrets.
- `http()`: `OutboundHttp`, a reqwest client with rustls and the platform
  trust store, a 5 s connect timeout, no redirects and no retries. `send`
  takes a deadline and a response size limit and fails `upstream_timeout`,
  `upstream_unavailable` or `upstream_body_limit`.
- `outbox()`, `authenticator()`, `authorizer()`, `notifier()`, `blobs()`: the
  ports below.

`AppContext::builder` starts from the disabled-block fallbacks; blocks replace
them through the builder.

## Ports

Defined in `crates/platform/src/ports.rs`:

| Port | Contract | Fallback without its block |
| --- | --- | --- |
| `Authenticator` | request parts to `Option<Principal>`; `Ok(None)` without credentials, permanent `unauthenticated` for bad ones, transient when its store is down | none: no principal |
| `Principal` | `subject`, `kind` (`User`, `ApiKey`, `System`), `tenant`, `roles`, `permissions`, `session` (an id, never a secret) | |
| `Authorizer` | extends the application `Authorizer` with `check(principal, permission, ResourceScope)`; synchronous and I/O-free | `LoopbackAllowAll` on a private listener (constructible only for loopback addresses), `DenyAll` otherwise |
| `Notifier` | durably accept or refuse a `NotificationRequest` (recipient, channels, template, data, idempotency key); deliver at least once later | `CaptureNotifier`: logs a redacted `notification.captured` warning and keeps the last 64 redacted records |
| `Outbox` | append an `OutboxMessage` inside the caller's `Transaction`; never commit it | `UnavailableOutbox`: fails `outbox_unavailable`, so the caller's transaction rolls back |
| `BlobStore` | `put`, `get`, `delete` by validated key, bounded size | `UnavailableBlobStore`: fails `blob_store_unavailable` |

## PostgreSQL

`Database::connect(&DatabaseConfig)` builds a `deadpool-postgres` pool. Each
connection sets `application_name = merezha`, the configured
`statement_timeout` and TCP keepalives. Connections open lazily and are
recycled with the fast method, so a broken session is discarded after its
first failed use.

- `acquire(deadline)` waits at most `acquire_timeout` and never past the
  deadline (`database_busy`, `database_closed`, `database_unavailable`).
- `transaction(deadline, work)` runs `work` in one transaction with
  `SET LOCAL statement_timeout` set to the time left, capped by
  `statement_timeout`. It commits on `Ok` and rolls back otherwise. A failed
  or interrupted commit returns `commit_outcome_unknown`.
- `classify(&tokio_postgres::Error)` maps driver errors to stable codes:
  `database_conflict` and `unique_violation` (conflict), `database_timeout`,
  `database_busy` and `database_unavailable` (transient),
  `constraint_violation` and `database_error` (permanent). Server messages
  and SQL never leave it.

### Migrations

`run_migrations(&db, &[ModuleMigrations], mode, deadline)`:

- validates declarations first: module names `[a-z0-9_]`, strictly
  increasing positive versions, names `[a-z0-9_]`, non-empty SQL up to
  1 MiB, at most 1024 migrations per module and 64 modules
  (`invalid_migrations`);
- in `Apply` mode, takes the session advisory lock `0x6d6572657a686131`
  (`merezha1`) on a connection removed from the pool, so concurrent hosts
  serialize and the lock dies with the session. The wait is bounded by the
  deadline (`migration_lock_timeout`, `migration_timeout`);
- creates `merezha.migrations (module, version, name, checksum, applied_at)`
  when missing. Verify mode never creates it;
- refuses before applying anything when an applied version is not declared
  (`unknown_applied_migration`), its SHA-256 checksum or name differs
  (`migration_checksum_mismatch`, `migration_name_mismatch`), or a pending
  version sorts below an applied one (`migration_out_of_order`);
- applies each pending migration and its ledger row in one transaction. A
  failing migration rolls back alone (`migration_failed`); earlier ones stay.

Migrations are forward-only and additive by default. A migration's SQL must
not manage transactions, so statements such as
`CREATE INDEX CONCURRENTLY` need a later dedicated mechanism.

### Test harness

`TestDatabase::create()` reads `MEREZHA_TEST_DATABASE_URL`, a `postgres://`
URL for a role with `CREATEDB`, creates `merezha_test_<pid>_<n>_<nanos>`, and
drops it `WITH (FORCE)` on drop. Without the variable it prints
`skipped: set MEREZHA_TEST_DATABASE_URL to run PostgreSQL tests` and returns
`None`. With `MEREZHA_TEST_REQUIRE_DATABASE=1` a missing variable fails the
test instead; CI sets both.

## Local PostgreSQL

### Contremaitre

`.contremaitre.yaml` declares a managed PostgreSQL 17 service for this
repository. With the Contremaitre hub, Apple container and HTTPS forwarding
installed:

```sh
contremaitre ensure --json
contremaitre proxy postgres 15432:5432 &
export MEREZHA_TEST_DATABASE_URL="postgres://app:$(contremaitre exec postgres -- printenv POSTGRES_PASSWORD)@127.0.0.1:15432/app"
cargo test --locked --workspace
```

The managed `app` role can create databases, so the same URL serves
`TestDatabase` and, as `DATABASE_URL`, a local `merezha run`. Credentials are
generated per environment. `contremaitre down` stops the environment and
keeps its data.

### Throwaway server

With the Homebrew PostgreSQL 17 binaries and no Contremaitre:

```sh
dir=$(mktemp -d)
LC_ALL=C initdb -D "$dir/data" -U postgres -A trust --no-locale -E UTF8 >/dev/null
LC_ALL=C pg_ctl -D "$dir/data" -l "$dir/log" -o "-p 54329 -k $dir -c listen_addresses=127.0.0.1" -w start
export MEREZHA_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:54329/postgres
cargo test --locked --workspace
pg_ctl -D "$dir/data" -m fast stop && rm -rf "$dir"
```

Trust authentication is acceptable only for a disposable loopback server.
`LC_ALL=C` lets the server start from shells without a valid locale.

### CI

`.github/workflows/host.yml` runs the Linux job with a `postgres:17` service
container and sets `MEREZHA_TEST_DATABASE_URL` and
`MEREZHA_TEST_REQUIRE_DATABASE=1`, so the PostgreSQL suites cannot silently
skip. GitHub's macOS runners cannot start service containers. The macOS job
runs the same checks, and its PostgreSQL tests print the skip message.

## Dependencies

Checked with `cargo info` on 2026-10-23. All support Rust 1.98.0.

| Crate | Version | MSRV | License | Features | Used by |
| --- | --- | --- | --- | --- | --- |
| `tokio-postgres` | 0.7.18 | 1.85 | MIT OR Apache-2.0 | `runtime` only | postgres |
| `deadpool-postgres` | 0.14.2 | 1.85 | MIT OR Apache-2.0 | `rt_tokio_1` only | postgres |
| `tokio-postgres-rustls` | 0.14.0 | unset | MIT | `aws-lc-rs` | postgres |
| `rustls` | 0.23.46 | 1.71 | Apache-2.0 OR ISC OR MIT | `aws_lc_rs`, `std`, `tls12` | postgres |
| `rustls-platform-verifier` | 0.7.1 | 1.85 | MIT OR Apache-2.0 | default | postgres |
| `sha2` | 0.10.9 | unset | MIT OR Apache-2.0 | none | postgres (0.10 is already locked by Wasmtime; 0.11 needs 1.88 and would duplicate it) |
| `reqwest` | 0.13.5 | 1.85.0 | MIT OR Apache-2.0 | `rustls` only | platform |
| `tracing` | 0.1.44 | 1.65.0 | MIT | `std`, `attributes` | postgres, platform, cli |
| `tracing-subscriber` | 0.3.23 | 1.65.0 | MIT | `std`, `fmt`, `json`, `registry` | platform |
| `tower-http` | 0.7.1 | 1.65 | MIT | `cors` only | platform |
| `http-body-util` | 0.1.5 | 1.61 | MIT | default | platform |
| `getrandom` | 0.4.3 | 1.85 | MIT OR Apache-2.0 | none | platform |

The aws-lc-rs provider is shared by the database TLS connector and reqwest,
so one crypto provider is compiled.

## Limits and follow-ups

- Modules start even when migrations are pending in verify mode; readiness
  stays false. A module that needs its tables at start should check them.
- `Timeouts` (migrate 300 s, module start 10 s, drain 30 s, module shutdown
  10 s) are code defaults, not configuration yet.
- There is no request deadline at the edge yet; handlers bound their own
  work.
- The application layer still uses memory or SQLite storage. PostgreSQL
  storage is stage 1.
