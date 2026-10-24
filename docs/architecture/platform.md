# Platform kernel

Status: stages K and K2 of the [backend framework program](framework.md). K2
aligned the kernel contracts with the block designs, as listed in
[cross-block decisions](../design/cross-block-decisions.md). Two crates form the
kernel every block builds on:

- `merezha-postgres` (`crates/postgres`): the PostgreSQL pool, deadline-scoped
  transactions, the module migration runner and the `TestDatabase` harness.
- `merezha-platform` (`crates/platform`): `ProjectConfig` for `merezha.toml`,
  the `Module` trait, `AppContext`, the cross-module ports, the HTTP edge,
  health endpoints, structured logging and the host lifecycle.

`merezha-jobs` (`crates/jobs`, AP-B1) is the jobs runtime the host installs
beside them: the PostgreSQL outbox, the jobs port and the relay (see
[Jobs and the outbox](#jobs-and-the-outbox)).

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
5. Register every enabled module's error codes (`error_code_conflict`, see
   [Errors](#errors)), then open the pool and prove it with `SELECT 1`
   (`database_unavailable`).
6. Apply pending migrations when `database.migrate = true`, otherwise verify
   them. A changed checksum, unknown applied version or out-of-order version
   stops startup in both modes. Pending migrations in verify mode do not stop
   startup; readiness stays false until they are applied.
7. Start modules in registration order, each within 10 s
   (`module_start_failed`; modules already started are shut down). The
   jobs runtime starts last, once every module declared its job handlers
   and subscriptions, and stops first.
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
when `http.public` is false, `http.listen` is a loopback address and
`http.trusted_proxies` is empty; a trusted proxy means requests arrive from
elsewhere even on loopback. A private listener keeps the explicit allow-all
policy and the raw runtime endpoints, as `serve` does. Any other listener gets
the deny-all policy, and the raw runtime endpoints are not mounted. Every
application command and query then fails `403 unauthorized`. Catch-up and
rebuild are checked through the policy as the permissions
`application.projections.catch_up` and `application.projections.rebuild` at
`ResourceScope::Application`, for the principal the edge authenticated: none
until the identity block is enabled, so they fail `401 unauthenticated`, and
`403 unauthorized` for a principal without the grant. A deployment exposes
nothing unauthenticated by accident.

## Configuration reference

Layers, lowest to highest precedence: code defaults, the file,
`MEREZHA_<SECTION>__<KEY>` environment variables, then command-line flags.
Unknown sections and keys fail. Every setting needs a restart; nothing is
reloaded at runtime. The raw runtime's `/v1/reload` still rereads its own
manifest.

Environment variables use `__` between segments: `MEREZHA_HTTP__LISTEN`,
`MEREZHA_HTTP__CORS__ALLOWED_ORIGINS`, `MEREZHA_JOBS__QUEUES__DEFAULT__CONCURRENCY`,
`MEREZHA_MODULES__GREETER__GREETING`. Segments are lowercased, so a queue
whose name contains a dot (`notify.email`) is set in the file or with a
quoted flag segment, not from the environment.
`[[jobs.schedules]]` has no environment or flag form.
Variables starting with `MEREZHA_` but without `__`, such as
`MEREZHA_TEST_DATABASE_URL`, are not configuration. Lists are comma
separated, booleans are `true` or `false`, and module values are parsed as
TOML literals (`"\"hi\""`, `3`, `true`), falling back to a plain string.

Flags: `--listen ADDRESS` sets `http.listen`; `--set SECTION.KEY=VALUE` sets
any non-secret setting (at most 64), nested ones included
(`--set jobs.relay.batch=250`). A double-quoted segment keeps its dots, as in
TOML: `--set 'jobs.queues."notify.email".concurrency=5'`.

Durations (R12) are a string with one unit, `"500ms"`, `"30s"`, `"15m"`,
`"2h"` or `"7d"`. Keys ending `_ms` also take integer milliseconds, in the file
or as a plain number from the environment or a flag. Other duration keys,
such as the `[jobs.retention]` ones, take strings only. A malformed value is
`expected_duration`.

Every key below validates at startup. Keys marked *reserved* have no effect
yet: the jobs runtime (AP-B1 onward), the PostgreSQL application core (AP-A2
onward) and the telemetry block (TE-T1 onward) give them their behaviour.
Configuring them now fails early on typos instead of being ignored.

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
| `instance_id` | string | `<host>-<pid>` | no | the one instance id (job leases, `service.instance.id`); 1 to 128 bytes of `[A-Za-z0-9._:-]`. The host comes from `HOSTNAME`, `/proc/sys/kernel/hostname` or `/etc/hostname`, else `localhost` |
| `version` | string | none | no | the application's version, TE's `service.version`; 1 to 64 printable bytes |

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
| `connect_timeout_ms` | duration | `5000` | no | 100 ms to 60 s; also bounds pool creation and recycling |
| `acquire_timeout_ms` | duration | `2000` | no | 10 ms to 60 s; wait for a pooled connection |
| `statement_timeout_ms` | duration | `5000` | no | 10 ms to 300 s; session default and ceiling for transactions |
| `migrate` | boolean | `false` | no | apply pending migrations at startup instead of verifying |
| `max_detached` | integer | `8` | no | 1 to 64 sessions detached from the pool at once (`Database::detach`, the migration run included); the server sees at most `pool_size + max_detached` connections from this host, plus listeners blocks open. With `[application]`, it must be at least `application.background.max_concurrent + 1`, so one session stays free for migrations and explicit catch-ups (`detached_sessions_exhausted`, reported on both keys) |
| `tls` | `disable` or `require` | `disable` | no | `require` verifies the chain and host name; decides over the URL's `sslmode` |
| `tls_ca_file` | path | none | no | extra PEM roots, at most 1 MiB; only with `tls = "require"` |

With TLS the platform trust store is used through `rustls-platform-verifier`,
plus `tls_ca_file` when set. There is no mode that skips verification. A URL
`sslmode=require` with `tls = "disable"` fails `sslmode_requires_tls` rather
than connecting in plaintext, and `sslmode=disable` with `tls = "require"`
fails `sslmode_conflicts_with_tls`. `sslmode=prefer` follows `tls`. The URL's
`options` are kept; the kernel appends its `statement_timeout` to them.

### `[runtime]`

| Key | Type | Default | Secret | Notes |
| --- | --- | --- | --- | --- |
| `manifest` | path | required | no | the [runtime manifest](contract.md) |

### `[application]`

Optional. Without it no application endpoints are mounted. Any
`[application.*]` table enables the section, so `path` is then required.

| Key | Type | Default | Secret | Notes |
| --- | --- | --- | --- | --- |
| `path` | path | required in the section | no | the [application configuration](application.md); its SQLite `path` keeps `serve`'s meaning |

The PostgreSQL application core's tables (AP §D.2), all *reserved*:

| Table | Key | Type | Default | Notes |
| --- | --- | --- | --- | --- |
| `[application.projections]` | `catch_up` | `auto` or `manual` | `auto` | `manual`: only the admin endpoints run catch-up |
| | `interval_ms` | duration | `5000` | 100 ms to 10 min |
| | `run_timeout_ms` | duration | `30000` | 1 s to 5 min, per background run |
| | `warm_readiness` | boolean | `true` | |
| `[application.reactors]` | `interval_ms` | duration | `5000` | 100 ms to 10 min |
| | `retry_base_ms` | duration | `1000` | 100 ms to 10 min; at most `retry_max_ms` (`retry_base_exceeds_max`) |
| | `retry_max_ms` | duration | `300000` | 1 s to 1 day |
| `[application.background]` | `max_concurrent` | integer | `2` | 1 to 16, shared by projection, rebuild and reactor runs; at most `database.max_detached - 1` |
| `[application.append]` | `lock_timeout_max_ms` | duration | `5000` | 10 ms to 30 s |

### `[jobs]`

Always present: jobs run whenever the database is configured, and there is
no `enabled` key (R12). AP-B1 gives `[jobs.relay]`, `[jobs.subscriptions]` and `max_attempts`
(`[jobs.defaults]` and per queue) their behaviour: the relay and
`enqueue` stamp each job with its queue's `max_attempts`. The other keys
are *reserved* until leasing and dispatch (AP-B2) and cron (AP-B3) ship.

| Table | Key | Type | Default | Notes |
| --- | --- | --- | --- | --- |
| `[jobs]` | `workers` | boolean | `true` | `false`: enqueue, relay and outbox only |
| | `poll_interval_ms` | duration | `1000` | 100 ms to 1 min |
| | `reaper_interval_ms` | duration | `5000` | 100 ms to 10 min |
| | `drain_timeout_ms` | duration | `20000` | 0 to 10 min |
| `[jobs.relay]` | `interval_ms` | duration | `1000` | 100 ms to 10 min |
| | `batch` | integer | `100` | 1 to 1000 |
| `[jobs.scheduler]` | `interval_ms` | duration | `10000` | 1 s to 1 h |
| `[jobs.defaults]` | `max_attempts` | integer | `10` | 1 to 100 |
| | `backoff_base_ms` | duration | `1000` | 10 ms to 1 day; at most `backoff_max_ms` (`backoff_base_exceeds_max`) |
| | `backoff_max_ms` | duration | `3600000` | 10 ms to 1 day |
| | `lease_ms` | duration | `30000` | 1 s to 10 min |
| | `timeout_ms` | duration | `30000` | 1 s to 15 min |
| `[jobs.queues.<name>]` | `concurrency` | integer | `2` | per instance, 1 to 256; at most 256 across queues (`concurrency_total_exceeded`) |
| | `global_concurrency` | integer | `0` | 0 is unlimited |
| | `max_attempts`, `backoff_base_ms`, `backoff_max_ms`, `lease_ms`, `timeout_ms` | as `[jobs.defaults]` | inherited | queue settings live only here (R1) |
| `[jobs.retention]` | `succeeded`, `dead`, `outbox`, `audit` | duration string | `7d`, `30d`, `7d`, `365d` | 1 h to 3650 d |
| `[jobs.subscriptions]` | `heartbeat` | duration string | `5m` | 100 ms to 1 d; every instance re-declares its subscriptions this often |
| | `retire_after` | duration string | `1h` | 300 ms to 30 d, at least three heartbeats (`retire_after_too_short`); a subscription nobody declared for this long is retired |
| `[jobs.readiness]` | `max_lag_ms` | duration | unset | 1 s to 1 day; unset, backlog is no readiness input (R10) |

Queue names are dot-separated `[a-z][a-z0-9_]*` segments, at most 64 bytes
(`invalid_queue_name`), at most 64 queues. `[[jobs.schedules]]` entries take
`name` (required, unique), `cron` (required, 5 or 6 fields), `timezone`
(default `UTC`), `queue` and `kind` (required) and `payload` (a table, default
`{}`, no dates, at most 64 KiB as JSON), at most 256 entries. The cron
expression and time zone are checked for shape only; the scheduler parses them
when it ships.

### `[telemetry]`

`log_level` and `format` drive the stderr log stream today. The other keys
are the telemetry block's (TE §3) and are *reserved*: they validate now, and
no OTLP pipeline runs yet.

| Key | Type | Default | Secret | Notes |
| --- | --- | --- | --- | --- |
| `log_level` | `error`, `warn`, `info`, `debug`, `trace` | `info` | no | filters the log output only |
| `format` | `json` or `text` | `json` | no | written to stderr |
| `log_buffer_lines` | integer | `8192` | no | 64 to 1048576 |
| `endpoint` | URL | `http://127.0.0.1:4318` | no | `http` or `https`, no userinfo, query or fragment |
| `protocol` | `http/protobuf` or `grpc` | `http/protobuf` | no | |
| `compression` | `gzip` or `none` | `gzip` | no | |
| `timeout_ms` | duration | `10000` | no | 1 s to 60 s |
| `headers` | table of secrets | `{}` | yes | lowercase header names, not `content-type` or `te`; at most 32. From the environment, `_` in the name becomes `-` |
| `ca_file` | path | none | no | |
| `allow_plaintext_headers` | boolean | `false` | no | without it, headers with a non-loopback `http://` endpoint fail `plaintext_headers_refused` |
| `shutdown_timeout_ms` | duration | `5000` | no | 100 ms to 60 s |
| `signals` | list of `traces`, `metrics`, `logs` | all three | no | `[]` turns OTLP export off |

| Table | Keys |
| --- | --- |
| `[telemetry.resource]` | `service_name` (default `app.id`), `service_version` (default `app.version`), `service_namespace`, `environment`: 1 to 128 printable bytes, `""` for unset; `attributes`: at most 16 strings whose keys start with `cloud.`, `container.`, `deployment.`, `host.`, `k8s.` or `os.` |
| `[telemetry.traces]` | `sampler` (`parent_based_ratio`, `always_off`), `ratio` (0.0 to 1.0, default 1.0), `trust_inbound` (`trusted_proxies`, `always`, `never`), `endpoint`, `max_queue` (2048), `max_batch` (512, at most `max_queue`), `schedule_delay_ms` (5000) |
| `[telemetry.metrics]` | `interval_ms` (30000, at least 1 s), `temporality` (`cumulative`, `delta`), `sample_interval_ms` (15000), `endpoint` |
| `[telemetry.logs]` | `level` (`info`), `endpoint`, `max_queue`, `max_batch`, `schedule_delay_ms` (1000) |
| `[telemetry.privacy]` | `record_tenant_id` (`true`), `record_subject_id` (`false`), `record_client_address` (`false`) |
| `[telemetry.propagation]` | `outbound_hosts`: at most 64 exact lowercase host names |

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
`sslmode_requires_tls`, `sslmode_conflicts_with_tls`, `tls_ca_without_tls`, `secret_literal`, `secret_on_command_line`,
`secret_unavailable`, `secret_too_large`, `invalid_secret_reference`,
`invalid_module_name`, `unknown_module`, `invalid_module_registration`,
`expected_duration`, `expected_number`, `expected_table_array`,
`invalid_instance_id`, `invalid_version`, `retry_base_exceeds_max`,
`backoff_base_exceeds_max`, `retire_after_too_short`, `invalid_queue_name`,
`concurrency_total_exceeded`, `invalid_cron`, `invalid_timezone`,
`invalid_kind`, `invalid_payload`, `duplicate_name`, `invalid_endpoint`,
`invalid_header_name`, `invalid_signal`, `duplicate_signal`, `invalid_label`,
`invalid_resource_attribute`, `batch_exceeds_queue`, `invalid_host`,
`plaintext_headers_refused`, `detached_sessions_exhausted`.

## The HTTP edge

`edge::apply` wraps the composed router. For every request it:

- takes the client address from the socket, or from `X-Forwarded-For` when
  the peer is in `http.trusted_proxies`. Every header line is read, joined in
  order, and walked from the right: trusted hops are skipped and the first
  untrusted hop is the client. An unparseable hop ends the walk at the last
  trusted hop, and a chain over 4 KiB is ignored. `X-Forwarded-Proto` and
  `X-Forwarded-Host` are honoured only from trusted peers, using the last
  value. All `X-Forwarded-*` and `Forwarded` headers are
  removed before handlers run; handlers read `ClientInfo`.
- accepts `X-Request-Id` only from a trusted proxy (1 to 128 bytes of
  `[A-Za-z0-9._:/-]`), otherwise generates 128 random bits, and returns it in
  the response.
- continues a valid W3C `traceparent` with a new span id or starts a new
  trace. Handlers read `TraceContext`; `OutboundHttp::send` propagates it.
- refuses bodies over `http.body_limit`: a larger `Content-Length` fails at
  once and an undeclared body fails while it is read. The same limit replaces
  the 2 MiB default of axum's `Bytes` and `Json` extractors.
- applies CORS and the optional `Authenticator`.
- logs one `http.request.completed` event with method, matched route, status, duration,
  request id and trace id. It never logs paths, query strings, headers or
  bodies.

Every error response has one shape (R8):

```json
{"error":{"classification":"permanent","code":"validation_failed","details":[{"path":"/email","code":"invalid"}]}}
```

`details` is optional and carries field-level validation errors only: at
most 100 `{path, code}` entries, `path` a JSON pointer of printable ASCII,
`code` lower snake case, never user input. Without details the body is byte
for byte the K body, `{"error":{"classification":..,"code":..}}`.

`edge::error_response(&Failure)` builds the plain body;
`ErrorReply::new(failure).details(..).retry_after(..)` adds details and an
explicit `Retry-After`. Every 429 and 503 error response carries
`Retry-After`, one second unless set. Behind the edge the status of an
`ErrorReply` comes from the host's error registry; outside a host
(`edge::status_for`) from the kernel codes alone.

A final layer normalizes anything else: the raw runtime's `{"error":{"code"}}`
documents gain the classification implied by their status, and plain-text
rejections from extractors become a code derived from the status. Their
status never changes. JSON objects without an `error` member, such as
readiness, pass through. `serve` keeps its existing error bodies and statuses:
its application routes have no policy and keep their own mapping.

### Errors

`ErrorRegistry` (`crates/platform/src/errors.rs`) maps each code to one HTTP
status in every module. Codes are unprefixed lower snake case. The kernel
registers:

| Status | Codes |
| --- | --- |
| 400 | `invalid_input` |
| 401 | `unauthenticated` |
| 403 | `unauthorized`, `actor_mismatch`, `tenant_required` |
| 404 | `not_found`, `handler_not_found`, `projection_not_found`, `unknown_service`, `blob_not_found` |
| 405 | `method_not_allowed` |
| 409 | `secret_purged` (R4a), `conflict`, `database_conflict`, `unique_violation`, `concurrency_conflict`, `idempotency_in_flight`, `idempotency_mismatch`, `projection_conflict`, `projection_in_flight`, `storage_owned`, `outbox_duplicate` |
| 412 | `revision_conflict` |
| 413 | `body_limit`, `input_limit`, `payload_limit`, `output_limit` |
| 415 | `unsupported_media_type` |
| 422 | `validation_failed` and every other permanent code the kernel emits (domain, application, port and startup codes) |
| 429 | `rate_limited`, `database_busy` (with `Retry-After`) |
| 503 | `overloaded` and every transient code the kernel emits (`database_unavailable`, `database_timeout`, `commit_outcome_unknown`, `internal`, `storage_*`, ...), with `Retry-After` |
| 504 | `timeout`, `upstream_timeout` |

`errors::KERNEL_CODES` is the complete list. A unit test scans the kernel
crates' sources, minus the items gated by `#[cfg(test)]`, for every code they
construct (`Failure::*`, `StartupError::new`, `MigrationError::at`,
`JobFailure::*`, `BlobError::*` and the `code()` tables) and fails when one is
missing,
so no module can register a kernel code, `database_unavailable` say, with
another status.

An unregistered code maps by classification: permanent 422, conflict 409,
transient 503. Modules declare `Module::errors() -> &[ErrorCode]`;
`Host::start` registers them, before it opens the pool, in registration
order. Registering a code again with the same status is accepted. A
different status, an invalid code or a status outside 400 to 599 stops
startup with `error_code_conflict` naming the module, and each problem is
logged as `error.registration_refused`. `overloaded` moved from 429 to 503
with K2.

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
fields only. Names follow the telemetry catalog (TE §1, §4): one area, one
dot, `<area>.<noun>_<verb>`.

`startup.failed`, `runtime.loaded`, `error.registration_refused`,
`db.connected`, `db.unavailable`, `migration.checked`, `migration.pending`,
`migration.applied`, `migration.checksum_mismatch`, `migration.refused`,
`migration.invalid_index_dropped`, `module.started`, `module.start_failed`,
`module.stopped`, `module.shutdown_timeout`, `listener.ready`,
`listener.draining`, `listener.drain_timeout`, `listener.stopped`,
`module.source_refused`,
`http.request.completed`, `health.ready`, `health.not_ready`,
`notification.captured`, and from the jobs runtime `outbox.relayed`,
`outbox.quarantined`, `outbox.relay_failed`, `job.admin_action`,
`job.subscription_sync_failed`, `db.listener_lost`,
`db.listener_restored`.

K2 renamed `database.connected`, `database.unavailable`,
`migrations.checked`, `migrations.refused` and `http.request` to the catalog
names above.

The telemetry block will pass its OTLP layers as `extra` to `telemetry::init`.
They see every event regardless of `log_level`. The raw runtime still writes
its own `invocation.completed` and `registry.*` JSON lines.

## Module contract

`Module` (`crates/platform/src/module.rs`) is what every block implements:

| Method | Contract |
| --- | --- |
| `name()` | `[a-z][a-z0-9_]*`, at most 64 bytes, unique, not `application`, `api`, `services`, `pipelines`, `reload`, `health`, `jobs` (the jobs runtime), `outbox` or `cron` (job key prefixes the relay and the scheduler mint). Names the section, route prefix and migration ledger key; never changes. |
| `configure(&ModuleSection)` | validate and keep `[modules.<name>]`; return every issue at once. Default: only an empty section. |
| `migrations()` | ordered forward-only `Migration::new(version, name, sql)` or `Migration::non_transactional(..)` for its own schema |
| `errors()` | `ErrorCode { code, status }` it returns, registered at startup; default none |
| `extra_sources()` | kernel sources it writes as besides its name; only `api` for `notifications` is granted |
| `events()` | `ModuleEvents { publishes, consumes }`, logged at start |
| `start(AppContext)` | start background work within the start timeout |
| `routes(&AppContext)` | an axum `Router` nested under `/v1/<name>` |
| `ready()` | readiness contribution, answered within 1 s |
| `shutdown(deadline)` | stop and drain background work after the listener drained |

`HostParts::jobs` takes the `JobsRuntime`, a `Module` named `jobs` that
`[jobs]` configures instead of a `[modules.jobs]` section.
`JobsRuntime::ports(&Database)` returns the `Outbox` and `Jobs`
implementations the host installs in every context before any module
starts. `None` keeps the refusing fallbacks. `merezha run` always installs
`merezha_jobs::PgJobs`.

`host::configure(&ConfigInput, modules)` loads the configuration and lets the
registered modules validate their sections. `Host::start`, `Host::bind` and
`Host::serve` run the lifecycle above. `cli::run::run(input, modules,
shutdown)` is the same entry point the binary uses, for embedding hosts that
register blocks.

## AppContext

`AppContext` (`crates/platform/src/context.rs`) is cheap to clone and holds:

- `app_id()` and `instance_id()` (`app.instance_id`).
- `database()`: the `Database` pool.
- `clock()`: `Clock` with wall-clock `now()` and monotonic `monotonic()`;
  `SystemClock` by default, replaceable in tests.
- `random()`: `Random`, the operating system CSPRNG through `getrandom`;
  `context::random_token` makes 256-bit hex secrets.
- `http()`: `OutboundHttp`, a reqwest client with rustls and the platform
  trust store, a 5 s connect timeout, no redirects and no retries. `send`
  takes a deadline and a response size limit and fails `upstream_timeout`,
  `upstream_unavailable` or `upstream_body_limit`.
- `outbox(source)`: a `SourceOutbox` bound to `source`; it validates every
  append before the implementation runs.
- `jobs(source)`: a `JobsHandle` bound to `source`, with the same rule:
  `enqueue`, `cancel`, `register` and `subscribe` (AP §B.3).
- `notifier(source)`: a `Notifier` bound to `source`; `notifications_deliver()`
  is `false` while only the capture fallback is installed.
- `module()`: the module the context belongs to. The host hands each module
  a context bound to its own name, in `start` and `routes`; such a context
  binds handles for that name only (`source_not_allowed` otherwise), so blocks
  cannot write in each other's dedupe namespace or claim identity's
  membership exemption. The kernel sources `api` and `application` are also
  reserved module names. A module may claim more through
  `Module::extra_sources()`, and the host grants only the pairs in
  `context::GRANTABLE_SOURCES`: `notifications` may claim `api` for its HTTP
  send route (NT §3.1). Any other claim stops startup with
  `source_not_granted`, before the pool opens, and logs
  `module.source_refused`. `application` is never granted: only the host's
  own context binds it.
- `directory()`, `authenticator()`, `authorizer()`, `blobs()`: the ports below.

`AppContext::builder` starts from the disabled-block fallbacks; blocks replace
them through the builder (`outbox`, `jobs`, `notifications`, `directory`,
`authenticator`, `blobs`).

## Ports

Defined in `crates/platform/src/ports/`. Ports that write inside the
caller's transaction take `&mut Tx<'_>` and never commit it. No port method
waits on a second pooled connection while a caller's transaction is open
(R4): callers read the `Directory` before they open their transaction and
pass what they learned in (`NotificationRequest::members`).

| Port | Contract | Fallback without its block |
| --- | --- | --- |
| `Authenticator` | `RequestHead` (request parts) to `Option<Arc<Principal>>`; `Ok(None)` without credentials, permanent `unauthenticated` for bad ones, transient when its store is down; loads every grant into the snapshot | none: no principal |
| `Principal` | `subject` (`usr_..`, `key_..`, or `sys_<name>` for system principals, `Principal::system`), `kind` (`User`, `ApiKey`, `System`), `user`, `service_account`, `tenant`, `grants: Arc<GrantSnapshot>`, `session: Option<SessionFacts>` | |
| `GrantSnapshot` | break-glass, application grants, application-level resource grants, tenant grants, resource grants in the tenant and an API key limit; `evaluate` and `Principal::check` decide synchronously in identity's order (ID §5) and return `Allowed::{Grant, BreakGlass}` or `AccessDenied { reason }` | |
| `ResourceScope` | `Application`, `Tenant(id)`, `Resource { tenant: Option<id>, kind, id }`; a resource without a tenant is application-level (R5). A tenant grant never satisfies an application check | |
| `Authorizer` | extends the application `Authorizer` with `check(Option<&Principal>, permission, scope) -> Result<Allowed, AccessDenied>`; synchronous and I/O-free. Without a principal it denies `Unauthenticated` (401), otherwise `unauthorized` (403) | `LoopbackAllowAll` on a private listener (constructible only for loopback addresses), `DenyAll` otherwise (401 anonymous, 403 with a principal) |
| `Outbox` | `append(tx, source, &[OutboxRecord], OnDuplicate) -> Appended` (AP §B.3); records carry a `DedupeKey` (at most 160 bytes, R4a; `DedupeKey::api(subject, key)` namespaces HTTP keys), an `OutboxTarget` (`Event { topic }` or `Job { queue, kind }`), a JSON payload of at most 64 KiB, `contains_secret`, `run_at`, `priority` and a `MessageContext`; at most 256 records per call. `OutboxRecord::event(source, topic, NewEvent)` (or `SourceOutbox::event`) builds an event record whose payload is the `IntegrationEvent` envelope (R11). `JobFailure` carries the code, classification and `retry_after` job handlers report | `UnavailableOutbox`: fails `outbox_unavailable`, so the caller's transaction rolls back. `merezha_jobs::PgOutbox` is the PostgreSQL implementation |
| `Jobs` | through `JobsHandle`: `enqueue(NewJob) -> Enqueued` in its own transaction, idempotent on the queue and `{source}:{key}`; `cancel(id)`; `register(kind, queue, Arc<dyn JobHandler>, KindOptions)` and `subscribe(topic, kind)` before the jobs runtime starts. `JobHandler`, `JobContext`, `KindOptions` and `Reissue` are AP §B.3's; `NewJob` hides its payload from `Debug` | `UnavailableJobs`: every call fails `jobs_unavailable` |
| `Notifier` | `validate(&request)` (pure), `request(tx, NotificationRequest)` and `emit_webhook(tx, WebhookEvent)` (NT §3.1); a webhook's `source_event_id` is an `EventId` of up to 225 bytes, room for `{source}:{dedupe_key}`. Requests carry topic, 1 to 100 recipients (`User`, `Tenant`, `Application { roles }`, `Address`), channels, template, `data`, `secret_data`, locale, dedupe key, tenant, `members`, `not_before`, `expires_at`, collapse key and correlation. For sources `application` and `api` with a tenant every user recipient must be in `members` (`recipient_not_in_tenant`). `Debug` never shows data, secret data or addresses. `NotifierFactory::bind(source)` hands out bound notifiers | `CaptureNotifications`: validates structurally, logs a redacted `notification.captured` warning, keeps the last 64 redacted records (never `data`, `secret_data` or addresses) and delivers nothing |
| `Directory` | `users(ids)` (at most 100) to verified `Contact`s, `tenant_members(tenant, roles?, PageRequest)` and `application_members(roles?, PageRequest)` (pages of at most 500, keyset by user id), `is_member(user, tenant)` (R7) | `EmptyDirectory`: resolves nothing; nobody is a member |
| `BlobStore` | `put(key, BlobBody, attrs) -> BlobStat` streaming, `get(key, range?) -> BlobRead` streaming, `head`, idempotent `delete`, `signed_get_url(key, ttl) -> Option<String>` (CT §6); `BlobKey` is 1 to 1024 bytes of `[A-Za-z0-9._-]` segments; `BlobError` maps to classified failures | `UnavailableBlobStore`: fails permanent `blob_store_unavailable` |

## Notifications block

`merezha run` registers the notifications block (`crates/notifications`,
[design](../design/notifications.md)). It stays off until
`[modules.notifications]` is present. Slice NT-N1 ships its configuration,
templates, previews and capture drivers; the `Notifier` (requests from other
blocks), expansion, delivery workers and the other channels arrive with
NT-N2 to NT-N8, so until then other blocks still get the kernel's capture
fallback and nothing is sent.

```toml
[modules.notifications]
secret_key = { env = "MEREZHA_NOTIFY_SECRET_KEY" } # base64 of at least 32 bytes

[modules.notifications.templates]
locales = ["en", "fr"]
```

Check a project without starting it:

```sh
cargo run --locked -p merezha-cli -- notifications check --config merezha.toml
```

It loads the whole configuration as `merezha run` does and prints one JSON
object on stdout: `status` `ok` with the topic and template counts, the
locales and the channel drivers, and exit code 0; or `status` `invalid` with
`issues: [{key, code, origin}]` and exit code 2. `merezha run` refuses to start on the same issues
(`invalid_config`). Issues name settings and template files, never values.

### What starts with the block

- Migration 1 (`core`) creates the schema `merezha_notify` with the core
  tables of NT §2.4: `notifications`, `deliveries`, `delivery_attempts`,
  `provider_events`, `preferences`, `user_settings`, `suppressions` and
  `captures`. Its checksum is pinned by a test; later slices add migrations.
- The block claims the `api` source (`Module::extra_sources`) for its HTTP
  send route.
- Routes under `/v1/notifications/`:

  | Method and path | Access | Answer |
  | --- | --- | --- |
  | `GET templates` | `notifications.templates.preview` at `Application` | `[{name, description, locales, channels}]` |
  | `POST templates/{name}/preview` | same | `{locale?, data?, channels?}` renders every channel: `{email: {subject, html, text}, sms: {text, encoding, segments}, push: {title, body, url}, inbox: {title, body, link, payload}}`. Without `data`, the template's preview data. `data.secret` is the preview's secret data. 422 `render_failed` with `details: [{path: "/channels/<channel>", code}]`, `reserved_data_key`, `data_too_large`, `channel_not_allowed`, `invalid_locale`; 400 `invalid_input`; 404 for an unknown template |
  | `GET templates/{name}/preview.html?locale=` | same | the HTML email with `Content-Security-Policy: default-src 'none'; style-src 'unsafe-inline'; img-src https: data:; sandbox` |
  | `GET dev/captures?channel=email\|sms&limit=1..100` | a loopback peer (not through a trusted proxy) while a capture driver is active | the newest captured messages, `[{delivery_id, channel, target, rendered, captured_at}]`; 404 otherwise |

  The three template routes answer 404 when `templates.preview = false`.
  Without the identity block the kernel policy decides: allowed on a private
  loopback listener, 401 elsewhere.
- The capture drivers (`email.driver = "capture"`, the default, and
  `sms.driver = "capture"`) keep each rendered message in
  `merezha_notify.captures`, the newest `retention.captures` rows, and accept
  it with provider `capture`. A second capture with the same delivery and
  idempotency key answers duplicate, as Brevo does. Captures hold rendered
  secret data, which is why `http.public = true` refuses a capture driver.

### Templates

Project files live under `templates.dir` (default `notifications/templates`,
relative to `merezha.toml`) and replace the embedded `en` and `fr` defaults
of the built-in topics file by file (NT §4 shows the layout).
`template.toml` takes `description`, `required` (data keys) and `[preview]`
data; its `[preview.secret]` table is the preview's secret data. Locale
directories must be in `templates.locales`. Every topic needs its template
in the default locale, so with a default locale other than `en` or `fr` the
project provides the built-in templates in it too. Rendering uses strict undefined,
HTML escaping for `.html` files only, `templates.fuel` and the output limits
of NT §4. The filters are `url` (`https:`, or `http:` to loopback with
`templates.allow_http_links`), `datetime(format)` (a jiff `strftime` format,
default `%Y-%m-%d %H:%M %Z`, in `recipient.timezone`) and `truncate_chars(n)`
(the first `n` characters).

Startup renders every template's preview in every listed locale and fails on
the first problem of each file, reported together. Template issue keys are
`modules.notifications.templates."<name>/<locale>/<file>"` or
`modules.notifications.templates."<name>".preview.<locale>.<channel>`.

### Settings

Keys and defaults are NT §7.2. Bounds and checks the block adds:

| Key | Bounds |
| --- | --- |
| `secret_key`, `secret_key_previous` | secrets; base64 (standard or URL-safe) of 32 to 1024 bytes (`invalid_secret_key`) |
| `public_base_url`, `templates.app_base_url`, `links.*` | `https`, or `http` to a loopback host; no userinfo or fragment (`invalid_url`) |
| `limits` | `max_recipients` 1 to 100, `max_targets` 1 to 100000, `max_marketing_targets` 1 to 1000 and at most `max_targets`, `max_data_bytes` 1024 to 49152 |
| `retention` | durations 1 h to 3650 d; `captures` 1 to 100000 |
| `templates` | `locales` at most 32 of `^[a-z]{2,3}(-[A-Z]{2})?$`, `default_locale` among them (`default_locale_not_listed`); `app_name` defaults to `app.name`; `fuel` 1000 to 10000000 |
| `email.from`, `email.reply_to` | `Name <local@domain>` or `local@domain` (`invalid_sender`); `from` required with `driver = "brevo"` |
| `sms` | `sender` required unless disabled: at most 11 alphanumerics or 15 digits; `max_segments` 1 to 10 |
| `brevo` | required with a `brevo` driver; `api_key` required then, `webhook_token` (at least 32 characters, `secret_too_short`) with the email driver; `timeout` 1 s to 60 s, `connect_timeout` 100 ms to 30 s |
| `push` | `vapid_private_key` a PKCS#8 PEM or base64url of 32 bytes (`invalid_vapid_key`), `vapid_subject` `mailto:` or `https:`; `ttl` 0 to 28 d; `allowed_hosts` at most 64 lowercase host names; `max_subscriptions_per_user` 1 to 100 |
| `inbox` | durations 1 d to 3650 d |
| `sse` | `heartbeat` 1 s to 5 min, `reconcile_interval` 1 s to 10 min, `replay_max` 1 to 10000, `max_streams_per_user` 1 to 64, `max_streams` 1 to 100000, `max_lifetime` 1 min to 1 d, `reauth_interval` 10 s to 1 h |
| `webhooks` | `max_endpoints_per_tenant` 1 to 1000, `max_payload_bytes` 1024 to 1048576, `timeout` 1 s to 60 s, `concurrency_per_endpoint` 1 to 64, `auto_disable_after` 1 h to 30 d; `event_types` names follow R11, at most 256 |
| `topics."<name>"` | names follow R11 (`invalid_topic_name`); `category` and `channels` required for project topics; built-ins keep their category (`category_fixed`); security topics are never `user_configurable` (`security_not_configurable`) |
| `quiet_hours` | refused in v1 (`unsupported_section`) |

Checks across sections, reported with the rest: a declared topic channel
without its driver or section (`channel_unavailable`; built-in topics keep
their channels and are filtered at delivery), `default_channels` outside
`channels`, `http.public = true` with a capture driver,
`allow_private_networks`, `webhooks.allow_http`, `allow_http_links`, a
non-https `brevo.base_url` or `preview` without `preview_in_public`
(`refused_when_public`), a missing `public_base_url` with Brevo SMS or with
Brevo email and a non-security email topic, a `notify.email`,
`notify.sms` or `notify.webhook` `lease_ms` under three provider timeouts
(`lease_too_short`), a `notify.webhook` `backoff_max_ms` under 24 h
(`backoff_max_too_short`), and with `MEREZHA_NOTIFY_NO_EGRESS=1` a
non-loopback `brevo.base_url` (`egress_not_loopback`).

`Module::configure` sees only the module's section, so the binary hands
the block the facts these checks need (`HostFacts::load`: the configuration
directory, `http.public`, `app.name`, `[jobs]` and
`MEREZHA_NOTIFY_NO_EGRESS`) when it registers it. When the kernel sections
do not load, the cross checks that need them are skipped; startup fails on
the kernel issues anyway.

## PostgreSQL

`Database::connect(&DatabaseConfig)` builds a `deadpool-postgres` pool. Each
connection sets `application_name = merezha`, the configured
`statement_timeout` and TCP keepalives. Connections open lazily and are
recycled with the fast method, so a broken session is discarded after its
first failed use.

- `acquire(deadline)` waits at most `acquire_timeout` and never past the
  deadline (`database_busy`, `database_closed`, `database_unavailable`).
- `transaction(deadline, work)` runs `work` with a `&mut Tx<'_>` in one
  transaction with `SET LOCAL statement_timeout` set to the time left, capped
  by `statement_timeout`. It commits on `Ok` and rolls back otherwise. A
  failed or interrupted commit returns `commit_outcome_unknown`. `Tx` is the
  type the in-transaction ports take.
- `detach(deadline)` takes a connection out of the pool as a
  `DetachedClient`, at most `max_detached` at once (it waits within
  `acquire_timeout`, then fails `database_busy`), for sessions that hold a session advisory lock
  (projection, reactor and content activation runs). It derefs to the driver
  client for session statements, and `DetachedClient::transaction(deadline,
  work)` has exactly the contract of `Database::transaction`. Dropping it
  closes the session, which releases its locks; after `database_timeout` or
  `commit_outcome_unknown` drop it rather than reuse it.
- `classify(&tokio_postgres::Error)` maps driver errors to stable codes:
  `database_conflict` and `unique_violation` (conflict), `database_timeout`,
  `database_busy` and `database_unavailable` (transient),
  `constraint_violation` and `database_error` (permanent). Server messages
  and SQL never leave it.

- `listen(channels, deadline)` opens a dedicated session outside the
  pool and the detached budget that `LISTEN`s on up to 8 channels. It hands
  out notifications as wake-up signals: at most 64 wait unread and later
  ones are dropped until the reader catches up. `Listener::recv` returns
  `None` once the session ended, and the caller reconnects;
  `Listener::check(deadline)` runs `SELECT 1` to detect a half-open
  session. The jobs relay
  holds one per host.

### Migrations

`run_migrations(&db, &[ModuleMigrations], mode, deadline)`:

- validates declarations first: module names `[a-z0-9_]`, strictly
  increasing positive versions, names `[a-z0-9_]`, non-empty SQL up to
  1 MiB, at most 1024 migrations per module and 64 modules
  (`invalid_migrations`);
- in `Apply` mode, takes the session advisory lock `0x6d6572657a686131`
  (`merezha1`) on a connection removed from the pool, so concurrent hosts
  serialize and the lock dies with the session. Waiting runners poll
  `pg_try_advisory_lock` every 50 ms instead of blocking in
  `pg_advisory_lock`: a blocked call holds a snapshot, which a
  `CREATE INDEX CONCURRENTLY` in the lock holder would wait for, a deadlock.
  The wait is bounded by the deadline (`migration_lock_timeout`,
  `migration_timeout`);
- creates `merezha.migrations (module, version, name, checksum, applied_at)`
  when missing. Verify mode never creates it and reads in one read-only
  transaction on a pooled session, so repeated readiness probes open no new
  connections;
- refuses before applying anything when an applied version is not declared
  (`unknown_applied_migration`), its SHA-256 checksum or name differs
  (`migration_checksum_mismatch`, `migration_name_mismatch`), or a pending
  version sorts below an applied one (`migration_out_of_order`);
- applies each pending transactional migration and its ledger row in one
  transaction. A failing migration rolls back alone (`migration_failed`);
  earlier ones stay.

Migrations are forward-only and additive by default. A transactional
migration's SQL must not manage transactions. Statements PostgreSQL refuses
inside a transaction, such as `CREATE INDEX CONCURRENTLY`, are declared as a
non-transactional step (R15):

```rust
const BY_TITLE: Migration = Migration::non_transactional(
    3,
    "entries_by_title",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS entries_by_title ON content.entries (title)",
);
```

- It holds exactly one statement (quotes, dollar quotes and comments are
  skipped when counting), checked with the other declarations
  (`invalid_migrations`). Comments before or between keywords do not hide a
  concurrent index build from the rules below.
- It runs outside any transaction, on the same session and under the same
  advisory lock as the rest of the run, and is checksummed like any
  migration.
- Its ledger row is written only after the statement succeeded. A failure
  leaves no row, so the next apply runs the step again; so does a crash
  between the statement and the row.
- It must be safe to repeat. A `CREATE [UNIQUE] INDEX CONCURRENTLY` must say
  `IF NOT EXISTS` and name its index without spaces in quoted names. Before
  running it, the runner drops an invalid index of that name on that table,
  left by a failed concurrent build, with `DROP INDEX CONCURRENTLY`, and logs
  `migration.invalid_index_dropped`, so the retry builds a valid index
  instead of keeping the broken one. Other statements, such as
  `ALTER TYPE ... ADD VALUE IF NOT EXISTS`, are the author's to make
  repeatable.

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

With the Homebrew PostgreSQL 17 binaries and no Contremaitre. The
`postgresql@17` formula is keg-only, so its binaries are not on `PATH`; with
another installation set `pg` to the directory holding `initdb` and `pg_ctl`.

```sh
pg=$(brew --prefix postgresql@17)/bin
dir=$(mktemp -d)
LC_ALL=C "$pg/initdb" -D "$dir/data" -U postgres -A trust --no-locale -E UTF8 >/dev/null
LC_ALL=C "$pg/pg_ctl" -D "$dir/data" -l "$dir/log" -o "-p 54329 -k $dir -c listen_addresses=127.0.0.1" -w start
export MEREZHA_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:54329/postgres
cargo test --locked --workspace
"$pg/pg_ctl" -D "$dir/data" -m fast stop && rm -rf "$dir"
```

Trust authentication is acceptable only for a disposable loopback server.
`LC_ALL=C` lets the server start from shells without a valid locale.

### CI

`.github/workflows/host.yml` runs the Linux job with a `postgres:17` service
container and sets `MEREZHA_TEST_DATABASE_URL` and
`MEREZHA_TEST_REQUIRE_DATABASE=1`, so the PostgreSQL suites cannot silently
skip. GitHub's macOS runners cannot start service containers. The macOS job
runs the same checks, and its PostgreSQL tests print the skip message.

## Jobs and the outbox

`merezha-jobs` implements AP §B for AP-B1. Its migration 0001 (ledger key
`jobs`) creates schema `merezha_jobs`: `outbox`, `subscriptions`, `jobs`,
`attempts`, `schedules` and `admin_actions`, with AP §B.2's constraints,
including the R3 rule that a flagged (`contains_secret`) record has no
payload once relayed or quarantined and a flagged job none at a terminal
state, and the R10 trace columns.

- **Append.** `PgOutbox::append` inserts every record of a call in one
  statement inside the caller's transaction, deduplicated on
  `(source, dedupe_key)`, and notifies `merezha_jobs_outbox`, delivered on
  commit. Before writing it refuses what the tables would refuse at relay:
  sources outside `[a-z][a-z0-9_]{0,31}` or named `outbox` or `cron`
  (`invalid_source`), topics other than `<module>.<name>` (`invalid_topic`),
  queue names `[jobs.queues]` would refuse (`invalid_queue`), delays over
  365 days (`invalid_run_at`), and an event record whose payload is not the
  envelope `OutboxRecord::event` builds for that source
  (`invalid_event_envelope`). Keys over 160 bytes never reach it
  (`invalid_dedupe_key`).
- **Enqueue.** `JobsHandle::enqueue` writes one job in its own transaction
  under the key `{source}:{key}`, the key a `Job` record of the same source
  and dedupe key relays to. The same key with the same kind and payload
  returns the existing id (`existed`); another kind or payload fails
  `idempotency_mismatch`. A new job notifies `merezha_jobs` with its queue
  on commit. `cancel` moves an `available` job to `cancelled` and nulls a
  flagged payload, for jobs the calling module owns only: the key starts
  with `{source}:` or the kind is one it registered. Any other id returns
  `false`, whether the job exists or not.
- **Register and subscribe.** Modules declare handlers and subscriptions
  from `Module::start`; each kind has one owner (`kind_already_registered`),
  a module subscribes only kinds it registered (`kind_not_registered`), and
  later calls fail `jobs_started`. At start, before its first relay pass,
  and then every `jobs.subscriptions.heartbeat`, each instance upserts the
  subscriptions it declares (`last_declared_at = now()`, reactivating a
  retired one) and retires, under a transaction advisory lock, those nobody
  declared for `jobs.subscriptions.retire_after`; retired rows stay as
  history and are not routed to. No instance deactivates anything from its
  own registry, so during a rolling deploy every live version's
  subscriptions keep receiving events. The runtime reports ready once the
  first sync succeeded, so pending migrations in verify mode keep it not
  ready instead of failing startup; each failure streak logs
  `job.subscription_sync_failed`.
- **Relay.** One background task per host, woken by `LISTEN
  merezha_jobs_outbox`, by `jobs.relay.interval_ms` and once at start, so
  records committed before a crash relay after the restart. Each batch of
  at most `jobs.relay.batch` records is one transaction (AP §B.4): event
  records fan out to one job per subscription (key `outbox:{id}:{kind}`),
  job records become one job (key `{source}:{dedupe_key}`), conflicting
  jobs are skipped, rows are marked relayed and flagged payloads nulled,
  and `merezha_jobs` is notified with each queue that got jobs. Events
  without a subscription are marked relayed and counted
  (`PgJobs::unrouted`). `FOR UPDATE SKIP LOCKED` lets several hosts relay
  at once. A record the jobs table would refuse is quarantined with code
  `unroutable`, and a flagged one loses its payload. A data error of the
  batch's rows (SQLSTATE classes 22 and 23) re-runs the batch one record at
  a time and quarantines the record that fails alone with its code
  (`constraint_violation`, `invalid_record_data`), keeping its payload so a
  requeue can relay it. Each quarantine logs `outbox.quarantined`. Any
  other error, such as a revoked privilege or a failing trigger, aborts the
  batch without quarantining anything, logs `outbox.relay_failed` once per
  failure streak and is retried at the interval. A lost `LISTEN` session
  logs `db.listener_lost`, falls back to the interval and reconnects; an
  idle session is probed with `SELECT 1` every 30 s, so a half-open one is
  noticed too.
- **Requeue.** `PgJobs::requeue_outbox(id, principal)` returns a
  quarantined record to pending with an `admin_actions` row; a flagged one
  quarantined as `unroutable` fails `409 secret_purged` (R4a), others
  `not_quarantined` or `not_found`.
  The HTTP route arrives with the jobs admin API (AP-B2).

Error codes it registers: `invalid_topic`, `invalid_queue`,
`invalid_run_at`, `invalid_event_envelope`, `invalid_record_data`, `job_payload_limit`,
`kind_already_registered`, `kind_not_registered`, `too_many_kinds` and
`jobs_started` (422), `not_quarantined` (409), `jobs_not_ready` (503).

Process tests hold the relay with `MEREZHA_JOBS_TEST_RELAY=off` (or
`interval`, no `LISTEN`). Only builds with the `test-hooks` feature read
it. `merezha-cli`'s dev-dependency enables it, and Cargo unifies that into
every build that compiles the dev-dependencies: the `merezha` binary built
by `cargo test` or `--all-targets` reads the variable. Builds without them,
such as `cargo build --release -p merezha-cli` or `cargo build
--workspace`, ignore it.

## Dependencies

Checked with `cargo info` on 2026-10-23 (K) and 2026-10-24 (K2). All support
Rust 1.98.0.

| Crate | Version | MSRV | License | Features | Used by |
| --- | --- | --- | --- | --- | --- |
| `tokio-postgres` | 0.7.18 | 1.85 | MIT OR Apache-2.0 | `runtime` only | postgres |
| `deadpool-postgres` | 0.14.2 | 1.85 | MIT OR Apache-2.0 | `rt_tokio_1` only | postgres |
| `tokio-postgres-rustls` | 0.14.0 | unset | MIT | `aws-lc-rs` | postgres |
| `rustls` | 0.23.46 | 1.71 | Apache-2.0 OR ISC OR MIT | `aws_lc_rs`, `std`, `tls12` | postgres |
| `rustls-platform-verifier` | 0.7.1 | 1.85 | MIT OR Apache-2.0 | default | postgres |
| `sha2` | 0.11.1 | 1.88 | MIT OR Apache-2.0 | none | postgres (R14; the version `postgres-protocol` already locks) |
| `reqwest` | 0.13.5 | 1.85.0 | MIT OR Apache-2.0 | `rustls` only | platform |
| `tracing` | 0.1.44 | 1.65.0 | MIT | `std`, `attributes` | postgres, platform, cli |
| `tracing-subscriber` | 0.3.23 | 1.65.0 | MIT | `std`, `fmt`, `json`, `registry` | platform |
| `tower-http` | 0.7.1 | 1.65 | MIT | `cors` only | platform |
| `http-body-util` | 0.1.5 | 1.61 | MIT | default | platform |
| `getrandom` | 0.4.3 | 1.85 | MIT OR Apache-2.0 | none | platform |
| `jiff` | 0.2.38 | 1.70 | Unlicense OR MIT | `std`, `serde`, `tz-system`, `tzdb-bundle-always` (the R14 workspace pin; pulls `jiff-tzdb` 0.1.9, MSRV 1.70, Unlicense OR MIT) | platform: timestamps in ports (R14) |
| `uuid` | 1.29.0 | 1.89.0 | Apache-2.0 OR MIT | none | platform: notification and webhook handles |
| `bytes` | 1.12.1 | 1.57 | MIT | `std` | platform: blob bodies |
| `futures-core` | 0.3.34 | 1.36 | MIT OR Apache-2.0 | `std` | platform: blob streams |

The aws-lc-rs provider is shared by the database TLS connector and reqwest,
so one crypto provider is compiled. Wasmtime still locks `sha2` 0.10 for
itself.

## Limits and follow-ups

- Modules start even when migrations are pending in verify mode; readiness
  stays false. A module that needs its tables at start should check them.
- `Timeouts` (migrate 300 s, module start 10 s, drain 30 s, module shutdown
  10 s) are code defaults, not configuration yet.
- There is no request deadline at the edge yet; handlers bound their own
  work.
- `Host::start` installs no `Authenticator` yet, so the edge never
  authenticates until identity's block registers one.
- The outbox and jobs `trace_parent`/`trace_state` columns exist but stay
  empty until the telemetry block fills them from the current span
  (TE-T5).
- Jobs are created but not leased or run: dispatch, retry, drain,
  retention and the jobs admin API are AP-B2, cron is AP-B3.
- The application layer still uses memory or SQLite storage. PostgreSQL
  storage is stage 1.
