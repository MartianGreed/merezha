# Platform kernel

Status: stages K and K2 of the [backend framework program](framework.md). K2
aligned the kernel contracts with the block designs, as listed in
[cross-block decisions](../design/cross-block-decisions.md). Two crates form the
kernel every block builds on:

- `merezha-postgres` (`crates/postgres`): the PostgreSQL pool, deadline-scoped
  transactions, the module migration runner and the `TestDatabase` harness.
- `merezha-platform` (`crates/platform`): `ProjectConfig` for `merezha.toml`,
  the `Module` trait, `AppContext`, the cross-module ports, the HTTP edge,
  health endpoints and the host lifecycle.

Logging and OpenTelemetry export come from `merezha-telemetry`
(`crates/telemetry`, [telemetry design](../design/telemetry.md), stage TE-T1):
the name catalog and `event!` macro every crate uses, the `[telemetry]`
model the kernel parses, and the OTLP pipeline only the `merezha` binary
links.

`merezha-jobs` (`crates/jobs`, AP-B1 and AP-B2) is the jobs runtime the host
installs beside them: the PostgreSQL outbox, the jobs port, the relay, the
workers that lease and run jobs, retention and the jobs admin API (see
[Jobs and the outbox](#jobs-and-the-outbox)).

`merezha-blob` (`crates/blob`, CT-C6) holds the `BlobStore` drivers, local
filesystem and S3-compatible through `object_store`, so the kernel never
depends on `object_store`. The content block installs one (CT §6). The
local root must be mode 0700 (no group or other bits, checked at startup and
by readiness); it is the access boundary, and blob files and subdirectories
below it follow the process umask.

`merezha run --config merezha.toml` (`crates/cli/src/run.rs`) puts them
together with the existing runtime and application layer, and registers the
identity block ([Identity block](#identity-block)), the content block
([Content](#content)) and the notifications block
([Notifications block](#notifications-block)), each off until its
`[modules.<name>]` section is present. `merezha serve` is unchanged.

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
2. Build the telemetry pipeline from `[telemetry]` and install the log
   subscriber (`telemetry_initialized`, `invalid_telemetry_ca`,
   `telemetry_exporter_failed`). Exporters connect lazily, so a collector
   that is down never fails or delays startup.
3. Bind `http.listen` (`listener_failed`) and log `listener.ready` with the
   bound address: the early listener (TE §5). Until startup completes it
   answers `/health/live` with 200, `/health/startup` and `/health/ready`
   with 503, and every other route with
   `503 {"error":{"code":"starting","classification":"transient"}}` and
   `Retry-After: 1`, so a startup probe can cover long migrations. From
   here on, startup runs inside the `startup` span.
4. Load the runtime manifest (`invalid_manifest`).
5. Read and validate the optional application configuration, then open a
   memory or SQLite application as `serve` does. A PostgreSQL application
   (`[storage] backend = "postgres"`) is only validated here, including the
   refusal of total-capacity limits (`invalid_limits`); it opens in step 8
   as the host module `application`. Opening asks every service the runtime
   manifest declares with `application_protocol = 2` for `describe`
   ([application protocol](application.md#protocol-version-2)); a guest that
   does not speak version 2 fails `service_protocol_mismatch` (in step 8 for
   a PostgreSQL application, as the module's code).
6. Register every enabled module's error codes (`error_code_conflict`, see
   [Errors](#errors)), then open the pool and prove it with `SELECT 1`
   (`database_unavailable`).
7. Apply pending migrations when `database.migrate = true`, otherwise verify
   them. A changed checksum, unknown applied version or out-of-order version
   stops startup in both modes. Pending migrations in verify mode do not stop
   startup; readiness stays false until they are applied.
8. Start modules in registration order, each within 10 s or the bound it
   declares through `Module::start_timeout` (`module_start_failed`, logged
   as `module.start_failed` with the module's own `code`, or `timeout`;
   modules already started are shut down). The
   PostgreSQL application module starts first and reports its own code
   instead: `storage_schema` while its migrations are pending,
   `storage_configuration` when the database belongs to another
   application identity. The jobs runtime starts last, once every module
   declared its job handlers and subscriptions, and stops first.
9. Evaluate readiness once, switch the listener to the full router and log
   `startup.completed` with `duration_ms` since the process started;
   `/health/startup` answers 200 from then on.

A failed step logs `startup.failed`, closes the early listener
(`listener.stopped`) and exits 2. SIGINT or SIGTERM during startup
interrupts it before the next step, or during a migration (rolled back with
its session) or a module start (that module is abandoned); the modules
already started shut down within the module shutdown budget, the early
listener closes and the process exits 0.

SIGINT or SIGTERM after startup runs the drain sequence: `shutdown.started`
and `listener.draining` are logged and readiness answers 503 at once while
liveness stays 200; the listener keeps serving for `http.drain_delay_ms`
(default 5 s) so load balancers stop routing, then stops accepting, and
in-flight requests drain for up to 30 s (`listener.drain_timeout` past that).
Modules shut down in reverse order within 10 s (the jobs runtime first), the
pool closes, `listener.stopped` and `shutdown.completed` are logged, traces,
logs and metrics are flushed within `telemetry.shutdown_timeout_ms` and the
process exits 0. A serving failure exits 1.

### Routes

| Path | Owner | Mounted when |
| --- | --- | --- |
| `GET /health/live` | kernel | always, from the early listener on |
| `GET /health/startup` | kernel | always, from the early listener on |
| `GET /health/ready` | kernel | always, from the early listener on |
| `GET /v1/admin/status` | kernel (TE §5) | always; see [Status](#status) |
| `/v1/<module>/...` | each enabled module (`/v1/identity/...` for identity) | its `[modules.<module>]` section exists |
| `/v1/application/...` | application layer (the `application` module on PostgreSQL) | `[application]` is set |
| `/v1/jobs/admin/...` | jobs runtime (AP §D.1) | always; each route is a permission check |
| `/health`, `/v1/services/...`, `/v1/pipelines/...`, `/v1/reload` | raw runtime | the listener is private |

Without the identity block there is no authentication. A listener is private
when `http.public` is false, `http.listen` is a loopback address and
`http.trusted_proxies` is empty; a trusted proxy means requests arrive from
elsewhere even on loopback. A private listener keeps the explicit allow-all
policy and the raw runtime endpoints, as `serve` does. Any other listener gets
the deny-all policy, and the raw runtime endpoints are not mounted. Every
application command and query then fails `403 unauthorized`, except routes
declaring `access = "public"`, and `401 unauthenticated` on routes needing a
principal ([route permissions](#application-route-permissions)). Catch-up and
rebuild are checked through the policy as the permissions
`application.projections.catch_up` and `application.projections.rebuild` at
`ResourceScope::Application`, for the principal the edge authenticated: none
until the identity block is enabled, so they fail `401 unauthenticated`, and
`403 unauthorized` for a principal without the grant. The PostgreSQL
application's admin routes ([application storage](#application-storage)) need
`application.admin.read` to read and `application.admin.write` to abandon a
rebuild or delete a revision, checked the same way. A deployment exposes
nothing unauthenticated by accident.

The principal the edge authenticated reaches application guests through
`command_as` and `query_as`, as the limited projection
`Principal::guest(scope)` builds (subject, kind, tenant and the role keys at
the route's scope; never the session, the grants or a secret). Only services
declaring `application_protocol = 2` receive it. Without a principal the
request is anonymous, and a `metadata.principal` in a request body fails
`422 principal_not_trusted`; an `actor` other than the principal's subject
fails `403 actor_mismatch`.

With the identity block the edge authenticates every request through its
`Authenticator` (sessions and `mzk_` API keys, `HostOptions::authenticator`),
the host refuses the allow-all policy beside it
(`allow_all_with_authenticator`), and `merezha run` installs identity's RBAC
`Authorizer` (`HostParts::authorizer`) on every listener, loopback included,
and its `Directory` (`HostOptions::directory`). The application and jobs
admin routes and `/v1/admin/status` then answer to real grants:
`401 unauthenticated` without a principal, `403 unauthorized` without the
permission at `Application` (`application.admin.read`, `jobs.admin.read`,
`system.status.read` and the rest of identity §5's catalog), and a
`platform_admin` session passes every check once its
`authorization.break_glass` audit row is written; when that write fails the
use is refused, `503 audit_unavailable`. Application commands and queries
answer to their route declarations through the same `Authorizer`, before any
guest call. The raw runtime endpoints, which have no
authentication of their own, are never mounted beside the authenticator,
even on a private listener.

#### Application route permissions

Each `[[commands]]` and `[[queries]]` route of a version 2 `application.toml`
declares `permission = "..."` or `access = "public" | "authenticated"`, with
`scope = "application" | "tenant"` and, on tenant commands, `resource`
([application](application.md#route-permissions-and-tenant-streams-version-2),
AP §C.4). The HTTP adapter runs every command and query as the edge's
principal under the host policy (`merezha_cli::application_http::EdgeCaller`)
and the application checks the route before validating the payload, reading
a receipt or calling any guest:

| Declaration | No principal | Principal |
| --- | --- | --- |
| `access = "public"` | passes | passes |
| `access = "authenticated"` | `401 unauthenticated` | passes |
| `permission` | `401 unauthenticated` | `Authorizer::check(principal, permission, scope)`: `Application`, `Tenant(principal.tenant)`, or `Resource { tenant, kind: resource, id: stream }`; `403 unauthorized` without the grant, `platform_admin` passes (break-glass audited first) |
| neither (version 1) | served only under the loopback allow-all policy; `403 unauthorized` otherwise | the same |

`access = "authenticated"` checks no permission, so it ignores an API key's
permission list: any principal, session or key, may call it. A tenant route
without an active organization fails `403 tenant_required`.
Tenant commands write `{tenant}:{aggregate}/{stream}` of the principal's
active organization and key their receipts by it; tenant queries read its
partition. A principal's receipts are scoped apart from anonymous callers
naming the same `actor` (PostgreSQL stores the principal's scope as
`@{subject}` in `merezha_app.receipts.actor`). On the platform host the
application's payload `Authorizer` hook is `AllowAll`: route declarations are
the policy.

At startup `merezha run` checks the whole document and stops with the first
problem, listing every one in `startup.failed` (`issues: [{key, code}]`):
with the identity block, `route_access_undeclared` for a route declaring
neither; `unknown_permission` for a route permission outside the
application's `[[permissions]]` and the blocks' catalog; and the document's
own `route_access_conflict`, `invalid_permission`, `block_prefix`,
`duplicate_permission`, `resource_requires_tenant`,
`tenant_query_unpartitioned`, `partitioned_query_unscoped`,
`tenant_aggregate`, `partitions_require_postgres`, `cross_tenant_query` and
`cross_tenant_scoped`. `cross_tenant_query` fails closed: with any
tenant-scoped route, an application-scoped query on an unpartitioned
projection open to anonymous callers (`public`, undeclared) or to every
principal (`authenticated`, or a permission in `builtin_grants.user`)
returns every tenant's data, so it must say `cross_tenant = true`. The rule
cannot see permissions granted at runtime through application roles: a
permission held by most users through application roles exposes every
tenant; use `scope = "tenant"`. `block_resource_kind` refuses a route
`resource` naming a block's resource kind (content's `content_type`). The application's
`[[permissions]]` join identity's catalog
(`IdentityModule::add_application_permissions`), so roles, keys and
`builtin_grants` can grant them, and route `resource` kinds become kinds
tenant role assignments may target (`add_tenant_resource_kinds`); routes check
them inside the caller's organization only, so an application-level
assignment on one fails `422 resource_kind_tenant_only`. With the identity block a
`builtin_grants.user` grant that a route checks at `scope = "tenant"` logs
`identity.user_grant_tenant_route` (warn, `permission`, `route`): `user`
grants hold in every organization, so tenant permissions belong in `member`.

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
yet: the jobs runtime (AP-B1 onward) and the PostgreSQL application core's
background work (AP-A3) give them their behaviour. Configuring them now
fails early on typos instead of being ignored.

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
| `drain_delay_ms` | duration | `5000` | no | 0 to 10 min; after SIGTERM, readiness answers 503 for this long before the listener stops accepting |

### `[http.cors]`

CORS is off while `allowed_origins` is empty.

| Key | Type | Default | Secret | Notes |
| --- | --- | --- | --- | --- |
| `allowed_origins` | list of strings | `[]` | no | exact lowercase `http(s)://host[:port]`, or `["*"]` alone; at most 64 |
| `allow_credentials` | boolean | `false` | no | refused with `*` |
| `max_age_seconds` | integer | `600` | no | 0 to 86400 |

Allowed methods are GET, POST, PUT, PATCH and DELETE. Allowed request
headers are `content-type`, `authorization`, `x-request-id`, `traceparent`,
`tracestate`, `merezha-organization` (identity's active organization),
`x-csrf-token` (identity's CSRF header, so a cross-origin
front end can send it) and `last-event-id` (so a cross-origin fetch-based
event-stream client resumes the inbox stream). `x-request-id` is exposed to
scripts.

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

The PostgreSQL application core's tables (AP §D.2). `[application.projections]`,
`[application.reactors]`, `[application.background]` and `[application.append]`
apply to a PostgreSQL application ([application storage](#application-storage),
[reactors](#reactors)); memory and SQLite applications ignore them (and refuse
`[[reactors]]`):

| Table | Key | Type | Default | Notes |
| --- | --- | --- | --- | --- |
| `[application.projections]` | `catch_up` | `auto` or `manual` | `auto` | `auto`: background catch-up on every local commit, `merezha_app_events` notification and `interval_ms`. `manual`: only the admin endpoints run catch-up; new revisions still warm up and requested rebuilds still run in the background |
| | `interval_ms` | duration | `5000` | 100 ms to 10 min; the wake that covers lost notifications and external restores |
| | `run_timeout_ms` | duration | `30000` | 1 s to 5 min, per background run; its statements end 5 s later at the latest (`run_timeout`) |
| | `warm_readiness` | boolean | `true` | `/health/ready` is 503 until this instance's configured projection revisions are warm |
| `[application.reactors]` | `interval_ms` | duration | `5000` | 100 ms to 10 min; the reactors' wake that covers lost notifications |
| | `retry_base_ms` | duration | `1000` | 100 ms to 10 min; at most `retry_max_ms` (`retry_base_exceeds_max`); the first retry of a failed reactor batch waits half of it to all of it (equal jitter), doubling per consecutive failure |
| | `retry_max_ms` | duration | `300000` | 1 s to 1 day; the ceiling of that backoff, also for a halted reactor's retries |
| `[application.background]` | `max_concurrent` | integer | `2` | 1 to 16, shared by projection, rebuild and reactor runs; at most `database.max_detached - 1`. It also caps the explicit catch-ups holding their own lock session. The runtime manifest's `max_concurrent_invocations` must be at least `limits.max_concurrent` plus this (`invalid_concurrency_budget` at startup), and `/v1/reload` refuses a manifest below it (`invalid_manifest`, the old generation keeps serving) |
| `[application.append]` | `lock_timeout_max_ms` | duration | `5000` | 10 ms to 30 s; the append waits for the head lock at most `min(remaining deadline - 50 ms, lock_timeout_max_ms)`, then fails `503 storage_busy` |

### `[jobs]`

Always present: jobs run whenever the database is configured, and there is
no `enabled` key (R12). AP-B1 gives `[jobs.relay]`, `[jobs.subscriptions]` and `max_attempts`
(`[jobs.defaults]` and per queue) their behaviour: the relay and
`enqueue` stamp each job with its queue's `max_attempts`. AP-B2 gives the
workers theirs: `workers`, `poll_interval_ms`, `reaper_interval_ms`,
`drain_timeout_ms`, every `[jobs.defaults]` and `[jobs.queues.<name>]`
key, `[jobs.retention]` and `[jobs.readiness] max_lag_ms` (see "Jobs and
the outbox"). AP-B3 gives `[jobs.scheduler]` and `[[jobs.schedules]]`
theirs (cron, below); `[jobs.subscriptions]` also paces the schedule
declarations.

| Table | Key | Type | Default | Notes |
| --- | --- | --- | --- | --- |
| `[jobs]` | `workers` | boolean | `true` | `false`: enqueue, relay and outbox only |
| | `poll_interval_ms` | duration | `1000` | 100 ms to 1 min |
| | `reaper_interval_ms` | duration | `5000` | 100 ms to 10 min |
| | `drain_timeout_ms` | duration | `20000` | 0 to 10 min |
| `[jobs.relay]` | `interval_ms` | duration | `1000` | 100 ms to 10 min |
| | `batch` | integer | `100` | 1 to 1000 |
| `[jobs.scheduler]` | `interval_ms` | duration | `10000` | 1 s to 1 h; every instance ticks this often (and once at start) |
| `[jobs.defaults]` | `max_attempts` | integer | `10` | 1 to 100 |
| | `backoff_base_ms` | duration | `1000` | 10 ms to 1 day; at most `backoff_max_ms` (`backoff_base_exceeds_max`) |
| | `backoff_max_ms` | duration | `3600000` | 10 ms to 1 day |
| | `lease_ms` | duration | `30000` | 1 s to 10 min |
| | `timeout_ms` | duration | `30000` | 1 s to 15 min |
| `[jobs.queues.<name>]` | `concurrency` | integer | `2` | per instance, 1 to 256; at most 256 across queues (`concurrency_total_exceeded`) |
| | `global_concurrency` | integer | `0` | 0 is unlimited |
| | `max_attempts`, `backoff_base_ms`, `backoff_max_ms`, `lease_ms`, `timeout_ms` | as `[jobs.defaults]` | inherited | queue settings live only here (R1) |
| `[jobs.retention]` | `succeeded`, `dead`, `outbox`, `audit` | duration string | `7d`, `30d`, `7d`, `365d` | 1 h to 3650 d |
| `[jobs.subscriptions]` | `heartbeat` | duration string | `5m` | 100 ms to 1 d; every instance re-declares its subscriptions and schedules this often |
| | `retire_after` | duration string | `1h` | 300 ms to 30 d, at least three heartbeats (`retire_after_too_short`); a subscription or schedule nobody declared for this long is retired |
| `[jobs.readiness]` | `max_lag_ms` | duration | unset | 1 s to 1 day; unset, backlog is no readiness input (R10) |

Queue names are dot-separated `[a-z][a-z0-9_]*` segments, at most 64 bytes
(`invalid_queue_name`), at most 64 queues. `[[jobs.schedules]]` entries take
`name` (required, unique, `[a-z][a-z0-9_.-]{0,63}`, else `invalid_name`),
`cron` (required, exactly five fields: minute, hour, day of month, month,
day of week), `timezone` (an IANA name, default `UTC`), `queue` and `kind`
(required) and `payload` (a table, default `{}`, no dates, at most 64 KiB as
JSON), at most 256 entries. Validation parses each expression with `croner`
and resolves each zone from the bundled tzdb, so a bad entry fails startup
with every other problem: `invalid_cron` (six fields with seconds, `@daily`
nicknames, out-of-range values, or an expression that never fires such as
`0 0 30 2 *`), `invalid_timezone` (not shaped like a zone name) and
`unknown_timezone` (not in the tzdb).

```toml
[[jobs.schedules]]
name = "tasks.reminders"
cron = "0 9 * * mon-fri"      # 09:00 on weekdays
timezone = "Europe/Paris"
queue = "default"
kind = "tasks.send_reminders"
payload = { only = ["late"] }
```

### `[telemetry]`

`log_level` and `format` drive the stderr log stream; the other keys drive
the OTLP pipeline (TE §3). Every value is checked at startup and all
problems are reported together. `traces.trust_inbound` decides which
callers' `traceparent` the edge honors (see [The HTTP edge](#the-http-edge)).
`metrics.sample_interval_ms` paces the database-backed gauges (the
projection lag sampler and the jobs and outbox gauges) and
`propagation.outbound_hosts` takes effect with the outbound client (TE-T6);
it validates now.

Standard `OTEL_*` variables are never read: `merezha` removes them from its
environment before any thread starts and logs their names once as
`telemetry.otel_env_ignored`.

OTLP export is opt-in: unless `endpoint`, a per-signal `endpoint` or
`signals` is set in the file, the environment or a flag, `signals` is `[]`.
`ca_file` is read at startup (at most 1 MiB of PEM, `invalid_telemetry_ca`) when any signal is exported. With
`signals = []` no exporter or SDK thread exists; stderr logs, health and
status remain.

| Key | Type | Default | Secret | Notes |
| --- | --- | --- | --- | --- |
| `log_level` | `error`, `warn`, `info`, `debug`, `trace` | `info` | no | filters the log output only |
| `format` | `json` or `text` | `json` | no | written to stderr |
| `log_buffer_lines` | integer | `8192` | no | 64 to 1048576 |
| `endpoint` | URL | `http://127.0.0.1:4318` (used only when `signals` is set explicitly) | no | `http` or `https`, no userinfo, query or fragment. `http/protobuf` appends `/v1/traces`, `/v1/metrics`, `/v1/logs`; `grpc` uses it as the channel URI |
| `protocol` | `http/protobuf` or `grpc` | `http/protobuf` | no | |
| `compression` | `gzip` or `none` | `gzip` | no | |
| `timeout_ms` | duration | `10000` | no | 1 s to 60 s |
| `headers` | table of secrets | `{}` | yes | lowercase header names, not `content-type` or `te`; at most 32. From the environment, `_` in the name becomes `-` |
| `ca_file` | path | none | no | |
| `allow_plaintext_headers` | boolean | `false` | no | without it, headers with a non-loopback `http://` endpoint fail `plaintext_headers_refused` |
| `shutdown_timeout_ms` | duration | `5000` | no | 100 ms to 60 s |
| `signals` | list of `traces`, `metrics`, `logs` | all three when an endpoint is configured, else `[]` | no | OTLP export is opt-in: a plain `merezha run` starts no exporter |

| Table | Keys |
| --- | --- |
| `[telemetry.resource]` | `service_name` (default `app.id`), `service_version` (default `app.version`), `service_namespace`, `environment`: 1 to 128 printable bytes, `""` for unset; `attributes`: at most 16 strings whose keys start with `cloud.`, `container.`, `deployment.`, `host.`, `k8s.` or `os.` |
| `[telemetry.traces]` | `sampler` (`parent_based_ratio`, `always_off`), `ratio` (0.0 to 1.0, default 1.0, root decisions only), `trust_inbound` (`trusted_proxies`, `always`, `never`), `endpoint` (used verbatim), `max_queue` (2048 spans queued plus in flight; more are dropped and counted), `max_batch` (512, at most `max_queue`), `schedule_delay_ms` (5000) |
| `[telemetry.metrics]` | `interval_ms` (30000, at least 1 s), `temporality` (`cumulative`, `delta`), `sample_interval_ms` (15000: how often the database-backed gauges are refreshed while metrics are exported), `endpoint` |
| `[telemetry.logs]` | `level` (`info`), `endpoint`, `max_queue`, `max_batch`, `schedule_delay_ms` (1000) |
| `[telemetry.privacy]` | `record_tenant_id` (`true`), `record_subject_id` (`false`), `record_client_address` (`false`; no signal carries `client.address` yet, so `true` has no effect) |
| `[telemetry.propagation]` | `outbound_hosts`: at most 64 exact lowercase host names |

### `[modules.<name>]`

Each table is opaque to the kernel and validated by the module of that name
through `Module::configure`. Module issues are reported with kernel issues. A
section without a registered module fails `unknown_module`. A registered
module without a section stays disabled. `ModuleSection::secret` resolves
secrets with the kernel rules, `ModuleSection::secret_at(&["media", "s3",
"secret_access_key"])` the same for a key in nested tables (a `{ env }` or
`{ file }` reference in the file: `MEREZHA_*` overrides reach top-level
module keys only), and `ModuleSection::base_dir` is the directory relative
paths resolve against (the canonical directory of `merezha.toml`).

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
`plaintext_headers_refused`, `detached_sessions_exhausted`,
`nested_override_unsupported` (a `MEREZHA_*` variable or `--set` naming a
key inside a module's nested table: overrides reach top-level module keys
only).

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
  every response.
- opens the server span `{METHOD} {http.route}` (`{METHOD}` when no route
  matched; methods outside the HTTP registry are `_OTHER`), kind SERVER,
  with `http.request.method`, `http.route` (the router's template, never the
  path), `url.scheme`, `http.response.status_code`, `error.type` (the
  status, 5xx only) and `merezha.request_id`. Only 5xx responses set the
  span status to error. `/health/*` requests get metrics but no span.
- applies the trust policy `telemetry.traces.trust_inbound` (TE §2.1) to a
  valid W3C `traceparent` (version `00`) and `tracestate` (at most 512
  bytes): `trusted_proxies` (default) trusts peers in
  `http.trusted_proxies` and loopback, `always` every peer, `never` none. A
  trusted caller's context parents the server span and its sampled flag is
  honored. An untrusted caller's context starts a new root under the ratio
  sampler and becomes a span link with
  `merezha.link.reason = untrusted_parent`, so public clients cannot force
  sampling. Malformed headers are ignored. Only trusted callers get a
  `traceparent` response header. Handlers read `TraceContext`, the server
  span's ids.
- records `http.server.request.duration` (method, route, status,
  `error.type`, scheme), `http.server.active_requests` (method, scheme),
  `http.server.request.body.size` (declared `Content-Length`; method,
  route) and `merezha.http.server.rejected` for what the edge itself
  refused (`merezha.reason`: `starting` from the early listener, `body_limit`
  for a declared or read body over `http.body_limit`; a module's
  `overloaded` or `timeout` reply is not an edge rejection), through the
  meter of scope `merezha.edge` (`EDGE_SCOPE`) from the meters the binary passes in `HostOptions::meters`. `client.address` is not
  recorded on server spans yet, so `telemetry.privacy.record_client_address
  = true` has no effect.
- refuses bodies over `http.body_limit`: a larger `Content-Length` fails at
  once and an undeclared body fails while it is read. The same limit replaces
  the 2 MiB default of axum's `Bytes` and `Json` extractors. A route a
  module lists in `Module::body_limits` (exact method; the path matched segment by segment, a `{name}` segment matching any one segment) is read up
  to its own limit instead, its declared length left to the route;
  `edge::is_body_limit(&error)` tells the route that a body read stopped
  at that limit, so it answers its own code (`asset_too_large`).
- applies CORS and the optional `Authenticator`.
- logs one `http.request.completed` event with method, matched route, status, duration,
  request id and trace id (the line's `trace_id`): at error for 500, at
  info for other 5xx and for requests of 1 s or more, at debug otherwise, so
  the default `info` stream shows failures and slow requests only. It never
  logs paths, query strings, headers or bodies.

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
| 503 | `overloaded`, `starting` (the early listener) and every transient code the kernel emits (`database_unavailable`, `database_timeout`, `commit_outcome_unknown`, `internal`, `storage_*`, `audit_unavailable` for a break-glass use whose audit row could not be written, ...), with `Retry-After` |
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

| Endpoint | 200 | 503 |
| --- | --- | --- |
| `GET /health/live` | `{"status":"live"}` while the process serves, draining included | never set deliberately |
| `GET /health/startup` | `{"status":"started"}` once startup completed (step 9 above) | `{"status":"starting"}` before |
| `GET /health/ready` | `{"status":"ready"}`: started, not draining, and the cached checks pass | `{"status":"not_ready"}` |

Readiness checks run in a background task every 2 s, each bounded by 1 s:
the pool answers `SELECT 1`, migrations are current, and every module's
`ready()`. Probes read the cached result and never touch the database.
Module checks run concurrently, each on a blocking thread of its own: one
that panics reports `internal`, one that blocks or runs past 1 s reports
`timeout` (and no second check of that module starts while it is still
stuck), and neither delays the other checks. One evaluation takes at most
3 s (database, migration re-verification, then the concurrent module
checks). An evaluation older than the refresh interval plus 3 s plus a 1 s
margin, 6 s by default, counts as not ready, so readiness can be at most
that stale and a refresh that stops turns it 503 instead of keeping the
last answer. If
migrations were pending at startup, readiness verifies them again until
another host has applied them. A module may hold readiness for a bounded
warm-up (R10): the jobs runtime until its first subscription sync, and,
with `jobs.readiness.max_lag_ms`, while the job backlog is older; content
while its index builds run. Projection lag past warm-up and the job backlog
without that key never count, and neither does the OTLP collector or any
outbound provider. The response never names the failing check; the log gets
`health.not_ready` with the first failing `check`, all failing `checks` and
the first `code` on each transition (`health.ready` on recovery), and
`/v1/admin/status` shows every check.

## Status

`GET /v1/admin/status` (`crates/platform/src/status.rs`, TE §5) is the
protected diagnostics document. An authenticator beside the allow-all
policy (`Authorizer::allows_all`) stops startup with
`allow_all_with_authenticator`, so identity never runs with permission
checks off. With the identity block (an authenticator
in `HostOptions::authenticator`) it requires `system.status.read` at
`ResourceScope::Application`; a tenant-scoped grant is not enough,
`platform_admin` passes, and failures are `401 unauthenticated` and
`403 unauthorized`. Without identity it is served only on a private listener
(see [Routes](#routes)) to a loopback TCP peer, and is `404 not_found`
otherwise. One request runs at a time (a concurrent one gets
`429 rate_limited` with `Retry-After: 1`), each section is read within 2 s
(a failed one shows `{"code": ...}`), and responses carry
`Cache-Control: no-store`.

Every field is a code, a count, a configured name or a timestamp:

- `service`: `name`, `version` (as in `service.name`/`service.version`),
  `host_version`, `instance_id`, `started_at`, `uptime_s`.
- `startup`: `state` (`starting`, `started`), `completed_at`,
  `duration_ms`.
- `readiness`: `ready`, `draining`, `stale` (the last evaluation is past
  the staleness bound), `checks` (`name`, `ok`, `code`,
  `duration_ms`, `checked_at`) from the last evaluation.
- `database`: `pool` (`max`, `size`, `idle`, `waiting`, from the pool
  without I/O) and `migrations` per module (`applied`, `pending`).
- `jobs`: `JobsHandle::queue_status()` as returned, the data behind
  `GET /v1/jobs/admin/queues` and `outbox`; the endpoint queries no jobs
  table itself (R10).
- `application`: AP's `Application::status()` as returned, the body of
  `GET /v1/application/status` (head, projections, reactors, instance,
  listener); present when `[application]` is set.
- `catalog` (`generation`, `services`, `pipelines`, `loaded_at`) and
  `telemetry` (`Telemetry::status()`: exporter counts per signal, the
  endpoint's scheme, host and port only, never header values).
- `application`, `catalog` and `telemetry` are `StatusSection`s that
  `merezha run` passes in `HostOptions::status`; the endpoint embeds what
  they return and queries no table of theirs.
- `modules`: each module's `name`, `ready` and `code` from the last
  evaluation.

## Logging

Events are emitted with `merezha_telemetry::event!(LEVEL, "area.noun_verb",
field = …)`, whose name and fields must appear in the catalog
(`crates/telemetry/src/names.rs`, TE §4); `testing::Harness::assert_catalogued`
fails a suite that emits anything else. The `merezha` binary installs one
`tracing` registry with three layers:

1. stderr, at `log_level` for Merezha targets and never more verbose than
   `info` for other crates. `format = "json"` writes one flat line per event
   with the field names `merezha_cli::emit` used:
   `timestamp_unix_ms`, `level` (lowercase), `event`, `service` (the
   service name; `merezha` in raw mode and before the configuration is
   read), `version` (the host version), `instance` (`app.instance_id`),
   `trace_id` and `span_id` inside a recording span (or the event's own
   `trace_id`, as on `http.request.completed`), the scope ids
   `request_id`, `correlation_id`, `causation_id` and `message_id`, then the
   event's catalog fields. Strings are cut to 512 bytes. A field named like
   an envelope key is prefixed with the event's area (`migration_version`).
   Events from other crates keep only `target`, `level` and
   `event = "dependency.log"`. The exporter's own crates (`opentelemetry*`,
   `tonic*`) never reach stderr: with the collector down they would repeat
   for every batch, and `telemetry.export_failed` reports instead. The HTTP
   transport crates the server shares with the exporter (`hyper`, `h2`,
   `tower`, `reqwest`) reach stderr at `warn` and above, except from the
   exporter's own threads. Writes go through a lossy non-blocking writer of
   `log_buffer_lines` lines, so a slow pipe drops lines instead of blocking.
   `format = "text"` uses the stock compact formatter.
2. the span bridge, for Merezha spans at `info` and above, whatever
   `log_level` says;
3. the OTLP log bridge, for catalog events at `telemetry.logs.level`, with
   attributes mapped through the catalog (`queue` becomes `job.queue`) and
   the allowlist. Stderr-only events (`telemetry.export_failed`,
   `identity.dev_link`) and other crates' events are never exported.

`startup.failed` is an error, including the `invalid_config` and
`invalid_arguments` lines written before the subscriber exists.

Kernel events: `startup.failed`, `startup.completed`, `runtime.loaded`, `error.registration_refused`,
`db.connected`, `db.unavailable`, `db.pool_timeout`, `migration.checked`, `migration.pending`,
`migration.applied`, `migration.checksum_mismatch`, `migration.refused`,
`migration.invalid_index_dropped`, `module.started`, `module.start_failed`,
`module.stopped`, `module.shutdown_timeout`, `listener.ready`,
`listener.draining`, `listener.drain_timeout`, `listener.stopped`,
`shutdown.started`, `shutdown.completed`,
`module.source_refused`, `http.request.completed`, `health.ready`,
`health.not_ready`, `notification.captured`, `telemetry.otel_env_ignored`
and `telemetry.export_failed` (at most once per signal per minute, with the
code and the cumulative number of lost items), from the content block
`content.model_loaded`, `content.model_activated`, `content.model_tightened`,
`content.model_breaking`, `content.index_created`, `content.index_dropped`
and `content.index_failed`, and from the jobs runtime
`outbox.relayed`, `outbox.quarantined`, `outbox.relay_failed`,
`job.admin_action`, `job.subscription_sync_failed`, `job.leased`, `job.completed`,
`job.retry_scheduled`, `job.dead_lettered`, `job.lease_lost`,
`job.drain_abandoned`, `job.reissue_claim_lost`, `job.worker_failed` (a lease, settle, heartbeat,
reaper, retention, lag, release, scheduler or gauge `sampler` statement
failing, once per failure streak), `job.queue_unconfigured`,
`db.listener_lost`, `db.listener_restored`. The jobs runtime emits all of
them through `event!` (TE-T5).

Exported spans pass the attribute allowlist (TE §1.5): unknown keys are
dropped and counted in `merezha.telemetry.attributes.dropped`, strings cut to
256 bytes, at most 64 attributes, 32 events and 8 links per span. Exports
are accounted exactly: `exported + failed + dropped + pending == emitted`
per signal, reported by `merezha.telemetry.exported`,
`merezha.telemetry.dropped` and the `telemetry` section of
`/v1/admin/status` (`Telemetry::status`).

The raw runtime (`merezha serve`) writes the same JSON lines to stderr with
`service = merezha`, logs at `debug` so each `invocation.completed` appears
as before, and runs no OTLP pipeline.

### Runtime and application telemetry

Guest calls, dispatch and projections are instrumented (TE-T3; TE §1.2,
§1.3, §2.2, §2.4):

- **Spans.** `guest {service} {operation}` around every guest call, as a
  child of the dispatch span (or of `pipeline {id}` for raw pipelines,
  see [runtime](runtime.md)); `command {tag}` and `query {tag}` around each
  dispatch, a child of the HTTP request span, with
  `merezha.correlation_id`, `merezha.message_id`, `merezha.causation_id`,
  the stream, expected version, position and event count;
  `projection {name}` around each catch-up stretch that processed events,
  always a root, linked to at most 8 distinct traces of the events it
  processed (`merezha.link.reason = source_event`). Tags, names and
  services the configuration does not declare record `_unknown`.
- **Outcome.** `merezha.outcome` is `accepted` (`ok` for queries),
  `replayed`, `rejected` (permanent: validation, guest rejection, unknown
  handler or projection, input limit), `denied` (`unauthorized`),
  `conflict` or `failed` (transient, internal, timeout, trap, fuel or memory
  limit, malformed guest reply, storage failure). Only `failed` sets the
  span status to error. A code the guest replied itself is kept only as the
  span attribute `merezha.guest.error_code`; `error.type`, logs
  (`command.completed`, `projection.failed`, `projection.rebuild_failed`)
  and metrics say `guest_error`, for dispatch and projection runs alike.
  Guest output that is not a protocol response is `invalid_response`
  (`failed`); `validation_failed`, the domain's own refusal, is
  `rejected`.
- **Instruments.** `merezha.guest.*` and `merezha.catalog.*` from the
  runtime; `merezha.command.duration`, `merezha.query.duration`,
  `merezha.events.appended`, `merezha.command.receipts.replayed`,
  `merezha.events.head`, `merezha.projection.lag.events`,
  `merezha.projection.lag.age`, `merezha.projection.catch_up.duration`
  (trigger `background` for scheduler runs, `admin` for explicit catch-ups,
  `rebuild`), `merezha.projection.events.processed` and, on PostgreSQL,
  `merezha.projection.warm`, `merezha.projection.rebuild.remaining`,
  `merezha.projection.lock.held` (the age of this host's longest-held run
  lock) and `merezha.projection.lock.contended` (lock attempts that found
  another holder). Modules build their instruments from
  `AppContext::telemetry().meter(module)`.
- **Lag.** Gauge callbacks read an in-memory cache only: local commits move
  the head, catch-ups move the checkpoint and record the first unprocessed
  event's age, and while metrics are exported a sampler re-reads the head,
  every checkpoint, warmth and rebuild progress every
  `telemetry.metrics.sample_interval_ms`, so other hosts' writes and runs
  count. Lag age is 0 when the checkpoint equals the head. Lag feeds alerts
  and status, never readiness.
- **Logs.** `command.completed` and `query.completed` at debug (warn for
  `failed`) with `tag`, `outcome`, `code` and `duration_ms`;
  `projection.caught_up`, `projection.failed`, `projection.warmed`,
  `projection.rebuild_completed`, `projection.rebuild_failed`,
  `db.pool_undersized`, `db.listener_lost` and `db.listener_restored` go
  through the catalog macro.
- **Stored context.** On PostgreSQL every appended event stores the command
  span's W3C context in `merezha_app.events.trace_parent`/`trace_state`,
  sampled or not (the flags travel in the `traceparent`); it never reaches a
  guest or a `StoredEvent`. Without traces exported the columns stay NULL.
- **Correlation.** The HTTP adapter fills a missing `metadata`,
  `message_id` (128 random bits, hex) or `correlation_id` (the current trace
  id, 32 lowercase hex; 128 random bits when no trace is recorded) on
  `/v1/application/commands` and `/queries`; caller-supplied ids are kept.
- **Blocking threads.** Blocking work goes through
  `merezha_telemetry::spawn_blocking_in_span`, which carries the dispatcher
  and the current span; `clippy.toml` bans `tokio::task::spawn_blocking` and
  `Handle::spawn_blocking` everywhere but `merezha-telemetry`.

## Module contract

`Module` (`crates/platform/src/module.rs`) is what every block implements:

| Method | Contract |
| --- | --- |
| `name()` | `[a-z][a-z0-9_]*`, at most 64 bytes, unique, not `admin` (the host's `/v1/admin/status`), `application`, `api`, `services`, `pipelines`, `reload`, `health`, `jobs` (the jobs runtime), `outbox` or `cron` (job key prefixes the relay and the scheduler mint), or `config` (the source of configured schedules). Names the section, route prefix and migration ledger key; never changes. |
| `configure(&ModuleSection)` | validate and keep `[modules.<name>]`; return every issue at once. Default: only an empty section. |
| `migrations()` | ordered forward-only `Migration::new(version, name, sql)` or `Migration::non_transactional(..)` for its own schema |
| `statements()` | the registered `Statement` lists it runs (`&[sql::STATEMENTS]`); default none. Not read at startup: `crates/cli/tests/statement_registry.rs` checks every module's lists with `check_registry` beside the kernel's, and fails for a module with migrations that lists none or a statement constant of its crate that no list holds |
| `errors()` | `ErrorCode { code, status }` it returns, registered at startup; default none |
| `extra_sources()` | kernel sources it writes as besides its name; only `api` for `notifications` is granted |
| `events()` | `ModuleEvents { publishes, consumes }`, logged at start |
| `start(AppContext)` | start background work within the start timeout |
| `start_timeout()` | the bound on its `start`; `None` (default) keeps the host's 10 s. Content declares its activation bound (300 s plus 10 s) because it builds indexes before it is ready |
| `routes(&AppContext)` | an axum `Router` nested under `/v1/<name>` |
| `ready()` | readiness contribution, answered within 1 s |
| `blob_store()` | the `BlobStore` it provides to every context, opened from its settings; `Ok(None)` (default) provides none. Asked once after `configure`, before any module starts; a failure stops startup naming the module, a second provider stops it with `blob_store_conflict`. Content provides the store `[modules.content.media]` configures |
| `notifier()` | the `NotifierFactory` it provides to every context in place of the capture fallback; `None` (default) provides none. Asked once after `configure`, before any module starts, so a module that starts earlier already binds it; a second provider stops startup with `notifier_conflict`. The factory answers `unavailable` until its module started. The notifications block provides it |
| `credential_free_routes()` | `CredentialFreeRoute { method, path }` for routes that take no credential (a signed link, a provider callback): for a request the router matched to that route template (axum's matched path, so a static route of the same shape stays authenticated) with every `{name}` segment non-empty, the edge never runs the authenticator (no session lookup, Origin or CSRF check, no refusal of a stale cookie) and inserts `Authenticated(None)`, so such a route cannot see a principal and must never act on one; default none. A path must start with `/` and hold no `..`, `%`, `//`, query or fragment; the host prefixes `/v1/<name>` and refuses any other path at startup with `invalid_credential_free_route`. Notifications declares `GET` and `POST /unsubscribe`, `POST /providers/brevo/email` and `POST /providers/brevo/sms/{delivery_id}/{mac}` |
| `body_limits()` | `RouteBodyLimit { method, path, limit }` for routes that bound their own body (the upload route): the edge reads such a body up to `limit` instead of `http.body_limit` and leaves a declared length to the route; default none. Notifications declares 1 MiB for `POST /providers/brevo/email` |
| `draining()` | the listener is about to stop accepting and drain in-flight requests: end long-lived responses (event streams) and refuse new ones, so the drain does not wait on them until its timeout. Called once, after `http.drain_delay_ms`, before the drain and `shutdown`; must not block; default nothing. Notifications ends every inbox stream there |
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
- `migrate()`: `database.migrate`. Modules whose start does schema work of
  their own beyond migrations (content model activation) do it only when
  it is `true`, and otherwise verify and report not ready while it is
  pending.
- `clock()`: `Clock` with wall-clock `now()` and monotonic `monotonic()`;
  `SystemClock` by default, replaceable in tests.
- `random()`: `Random`, the operating system CSPRNG through `getrandom`;
  `context::random_token` makes 256-bit hex secrets.
- `http()`: `OutboundHttp`, a reqwest client with rustls and the platform
  trust store, a 5 s connect timeout, no redirects and no retries. `send`
  takes a deadline and a response size limit and fails `upstream_timeout`,
  `upstream_unavailable` or `upstream_body_limit`. `exchange` is the same
  call with an `OutboundFailure { failure, maybe_sent }`: `maybe_sent` is
  `false` only when no connection was made (connect, DNS or TLS failure, or
  a deadline already passed), so a provider client can tell a safe resend
  from an ambiguous one. `OutboundHttp::with_connect_timeout(d)` builds the
  same client with another connect timeout (`brevo.connect_timeout`).
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
- `telemetry()`: the host's `Meters`; `telemetry().meter(module)` gives the
  meter a module builds its instruments from once, a no-op meter unless
  metrics are exported (`enabled()`). The host passes `HostOptions.meters`
  (no-op meters by default).

`AppContext::builder` starts from the disabled-block fallbacks; blocks replace
them through the builder (`outbox`, `jobs`, `notifications`, `directory`,
`authenticator`, `blobs`, `telemetry`). `Host::start` installs the blob
store a module provides (`Module::blob_store`) and the notifier factory a
module provides (`Module::notifier`) in every context.

## Ports

Defined in `crates/platform/src/ports/`. Ports that write inside the
caller's transaction take `&mut Tx<'_>` and never commit it. No port method
waits on a second pooled connection while a caller's transaction is open
(R4): callers read the `Directory` before they open their transaction and
pass what they learned in (`NotificationRequest::members`).

| Port | Contract | Fallback without its block |
| --- | --- | --- |
| `Authenticator` | `RequestHead` (request parts) to `Option<Arc<Principal>>`; `Ok(None)` without credentials, permanent `unauthenticated` for bad ones, transient when its store is down; loads every grant into the snapshot. `rejection_headers(request, failure)` (default none) adds headers to the edge's error response when it refused a request, such as identity's `Set-Cookie` clearing a dead session cookie | none: no principal |
| `Principal` | `subject` (`usr_..`, `key_..`, or `sys_<name>` for system principals, `Principal::system`), `kind` (`User`, `ApiKey`, `System`), `user`, `service_account`, `tenant`, `grants: Arc<GrantSnapshot>`, `session: Option<SessionFacts>` | |
| `GrantSnapshot` | break-glass, application grants, application-level resource grants, tenant grants, resource grants in the tenant and an API key limit; `evaluate` and `Principal::check` decide synchronously in identity's order (ID §5) and return `Allowed::{Grant, BreakGlass}` or `AccessDenied { reason }` | |
| `ResourceScope` | `Application`, `Tenant(id)`, `Resource { tenant: Option<id>, kind, id }`; a resource without a tenant is application-level (R5). A tenant grant never satisfies an application check | |
| `Authorizer` | extends the application `Authorizer` with `check(Option<&Principal>, permission, scope) -> Result<Allowed, AccessDenied>`; synchronous and I/O-free. Without a principal it denies `Unauthenticated` (401), otherwise `unauthorized` (403). Identity's `RbacAuthorizer` evaluates the principal's snapshot | `LoopbackAllowAll` on a private listener (constructible only for loopback addresses), `DenyAll` otherwise (401 anonymous, 403 with a principal) |
| `Outbox` | `append(tx, source, &[OutboxRecord], OnDuplicate) -> Appended` (AP §B.3); records carry a `DedupeKey` (at most 160 bytes, R4a; `DedupeKey::api(subject, key)` namespaces HTTP keys), an `OutboxTarget` (`Event { topic }` or `Job { queue, kind }`), a JSON payload of at most 64 KiB, `contains_secret`, `run_at`, `priority` and a `MessageContext`; at most 256 records per call. `OutboxRecord::event(source, topic, NewEvent)` (or `SourceOutbox::event`) builds an event record whose payload is the `IntegrationEvent` envelope (R11). `JobFailure` carries the code, classification and `retry_after` job handlers report | `UnavailableOutbox`: fails `outbox_unavailable`, so the caller's transaction rolls back. `merezha_jobs::PgOutbox` is the PostgreSQL implementation |
| `Jobs` | through `JobsHandle`: `enqueue(NewJob) -> Enqueued` in its own transaction, idempotent on the queue and `{source}:{key}`; `cancel(id)`; `register(kind, queue, Arc<dyn JobHandler>, KindOptions)` and `subscribe(topic, kind)` before the jobs runtime starts; `queue_status() -> QueueStatus` (per queue `ready`, `delayed`, `leased`, `dead`, `oldest_ready_age_s` and slots, plus the outbox backlog and up to 100 quarantined records), the read behind `GET /v1/jobs/admin/queues` and `outbox` and TE's status and gauges (R10). `JobHandler`, `JobContext`, `KindOptions` and `Reissue` are AP §B.3's; `NewJob` hides its payload from `Debug` | `UnavailableJobs`: every call fails `jobs_unavailable` |
| `Notifier` | `validate(&request)` (pure), `request(tx, NotificationRequest)` and `emit_webhook(tx, WebhookEvent)` (NT §3.1); a webhook's `source_event_id` is an `EventId` of up to 225 bytes, room for `{source}:{dedupe_key}`. Requests carry topic, 1 to 100 recipients (`User`, `Tenant`, `Application { roles }`, `Address`), channels, template, `data`, `secret_data`, locale, dedupe key, tenant, `members`, `not_before`, `expires_at`, collapse key and correlation. With a tenant every user recipient must be in `members` (`recipient_not_in_tenant`) for every source but `identity` (`MEMBERSHIP_EXEMPT_SOURCES`; the rule fails closed). `Debug` never shows data, secret data or addresses. `NotifierFactory::bind(source)` hands out bound notifiers | `CaptureNotifications`: validates structurally, logs a redacted `notification.captured` warning, keeps the last 64 redacted records (never `data`, `secret_data` or addresses) and delivers nothing. With `[modules.notifications]` the block's `BlockNotifiers` replaces it (`Module::notifier`) |
| `Directory` | `users(ids)` (at most 100) to verified `Contact`s, `tenant_members(tenant, roles?, PageRequest)` and `application_members(roles?, PageRequest)` (pages of at most 500, keyset by user id), `is_member(user, tenant)` (R7). Installed through `HostOptions::directory`; identity's implements it. `resolves()` is `true` except for the fallback, so notifications can tell "no such user" from "no directory" | `EmptyDirectory`: resolves nothing; nobody is a member; `resolves()` is `false` |
| `BlobStore` | `put(key, BlobBody, attrs) -> BlobStat` streaming, `get(key, range?) -> BlobRead` streaming, `head`, idempotent `delete`, `signed_get_url(key, ttl) -> Option<String>` (CT §6); `BlobKey` is 1 to 1024 bytes of `[A-Za-z0-9._-]` segments; `BlobError` maps to classified failures | `UnavailableBlobStore`: fails permanent `blob_store_unavailable`. `merezha_blob::ObjectBlobStore` is the local and S3 implementation |

## Identity block

`merezha-identity` (`crates/identity`, [design](../design/identity.md)) is
the identity block, a `Module` named `identity` with schema
`merezha_identity` (ledger key `identity`, migrations 1 `identity_core`,
2 `organizations_rbac_api_keys` and 3 `oauth_passkeys`). Slice ID PR 1 ships accounts with
email and password, email verification, password sign-in, sessions (cookie
and bearer), the Origin and CSRF checks, reauthentication, magic links,
password reset and change, email change, account deletion, PostgreSQL rate
limits, audit events, the cleanup job and the session `Authenticator`.
Slice ID PR 2 adds organizations, memberships, invitations, custom,
built-in and declared roles at tenant and application scope, service
accounts, API keys in the `Authenticator`, the RBAC `Authorizer`, the
`Directory` and `merezha identity platform-admin` ([Organizations, RBAC and
keys](#organizations-rbac-and-keys)). Slice ID PR 3 adds OAuth 2.0 and OIDC
sign-in (Google, GitHub, generic OIDC) and WebAuthn passkeys ([OAuth, OIDC
and passkeys](#oauth-oidc-and-passkeys)).

```toml
[modules.identity]
app_origin = "https://app.example.com"
secret_key = { env = "MEREZHA_IDENTITY_SECRET_KEY" }   # at least 32 bytes

[modules.identity.password]                 # enables password sign-in

[modules.identity.magic_links]              # enables magic links
```

| Key | Default | Range | Meaning |
| --- | --- | --- | --- |
| `app_origin` | required | `https://host[:port]`, or `http://localhost`, `127.0.0.1`, `[::1]` on a private loopback listener | The front end: links, `return_to`, the Origin check |
| `api_base_url` | `app_origin` | as `app_origin` | Externally visible base of this host (OAuth callbacks, PR 3) |
| `allowed_origins` | `[app_origin]` | 1 to 32 origins | Accepted `Origin` values |
| `secret_key`, `secret_key_previous` | required, none | secrets of at least 32 bytes (`secret_too_short`) | HKDF derives the CSRF, rate-limit and audit-subject keys; CSRF tokens of the previous key stay valid during a rotation |
| `registration` | `open` | `open`, `invite_only`, `closed` | `invite_only` refuses sign-up until invitations (PR 2) |
| `require_verified_email` | `true` | | Unverified accounts cannot sign in |
| `unverified_account_ttl` | `7d` | 1 h to 365 d | Cleanup deletes never-used unverified accounts after it, only while verification is required |
| `dev_print_links` | `false` | private loopback listener only (`refused_when_public`) | Log every emailed link as `identity.dev_link` on stderr |
| `notify_new_sign_in` | `false` | | `identity.new_sign_in` on each password sign-in |
| `links.verify_email`, `links.magic_link`, `links.password_reset`, `links.email_change` | `/auth/verify-email`, `/auth/magic-link`, `/auth/reset-password`, `/auth/confirm-email` | relative paths or absolute URLs on `app_origin` (`invalid_url`) | Pages the token is appended to as `#token=...` |
| `password.min_length`, `password.max_length` | `12`, `256` | 8 to 256 | Unicode scalar values after NFKC; the blocklist and context words apply, no composition rules |
| `password.blocklist_file` | none | a `{ file }` or `{ env }` secret, at most 64 KiB | One extra refused password per line |
| `password.argon2_memory_kib`, `argon2_iterations`, `argon2_parallelism` | `65536`, `2`, `1` | at least 19456 and 2 (`out_of_range`) | Argon2id cost; tune with `merezha identity password-benchmark` |
| `password.hash_concurrency`, `password.hash_queue_timeout` | `0` (available parallelism), `2s` | 0 to 1024, 10 ms to 30 s | Concurrent hashes; a longer wait answers `503 overloaded` |
| `sessions.idle_timeout`, `absolute_timeout`, `reauthentication_window`, `touch_interval` | `7d`, `30d`, `10m`, `60s` | idle at most absolute (`idle_exceeds_absolute`) | Session lifetime; a request writes `last_seen_at` at most once per touch interval |
| `sessions.max_per_user`, `sessions.bearer` | `50`, `true` | 1 to 1000 | The least recently seen session is evicted past the cap; `bearer` allows `"transport": "bearer"` |
| `tokens.email_verification_ttl`, `magic_link_ttl`, `password_reset_ttl`, `email_change_ttl` | `24h`, `15m`, `30m`, `1h` | | One-time token lifetimes |
| `magic_links.sign_up` | `true` | | Unknown addresses get a sign-up link while registration is open |
| `rate_limits.<policy>.limit`, `.period` | identity §7 | 1 to 1000000, 1 s to 30 d | Policies `sign_in_ip`, `sign_in_account_ip`, `sign_in_account`, `registration_ip`, `email_address_ip`, `email_address`, `email_ip`, `token_ip`, `reauth_session`, `invitation_organization` (100 per day, per organization, on arrival), `api_key_invalid_ip` (60 per minute, on failure), `passkey_ip` (60 per 10 min, on arrival, the passkey options routes), `oauth_ip` (30 per 10 min, on arrival, `oauth/{provider}/start`), `oauth_callback_ip` (30 per 10 min, on failure of a callback naming a live flow, checked first), `oauth_callback_unknown_ip` (300 per 10 min, on failure of a callback naming no live flow) |
| `links.invitation`, `tokens.invitation_ttl` | `/auth/invitation`, `7d` | 1 h to 30 d | The invitation page and lifetime |
| `organizations.user_can_create`, `max_per_user`, `max_members` | `true`, `50`, `10000` | 1 to 10000, 1 to 1000000 | `false` gives an application without organizations (`POST /organizations` answers 403); organizations one user may create (`organization_limit`); members per organization |
| `rbac.cache_entries`, `max_custom_roles`, `max_resource_assignments` | `10000`, `100`, `500` | 2 to 1000000, 1 to 1000, 1 to 10000 | Grant snapshots cached per instance; custom roles per organization (`role_limit`); resource grants per holder and scope kept in a snapshot (more are ignored, `identity.grants_truncated`) |
| `rbac.builtin_grants.user`, `.admin`, `.member` | `[]` | catalog permissions or trailing wildcards, for `user` never inside a block namespace (`unknown_permission`) | Added to the implicit `user` role (every active user, every organization), `admin` and `member` |
| `rbac.application_roles.<key>.name`, `.description`, `.permissions` | none | key `[a-z][a-z0-9_]{0,62}`, not `owner`, `admin`, `member`, `platform_admin`, `user` (`reserved_role_key`); at most 64 roles; permissions from the catalog, wildcards only outside block namespaces | Declared application roles, synced into `roles` at start |
| `service_accounts.max_per_scope` | `100` | 1 to 10000 | Per organization and for the application (`service_account_limit`) |
| `api_keys.max_per_scope`, `max_ttl`, `touch_interval` | `100`, `365d`, `5m` | 1 to 10000, 1 h to 3650 d, 1 s to 1 d | Unrevoked keys per scope (`api_key_limit`); the furthest `expires_at`; `last_used_at` writes |
| `audit.retention` | `400d` | 1 d to 3650 d | Audit rows older than this are deleted by the cleanup job |
| `cleanup.interval`, `cleanup.batch_size` | `10m`, `1000` | minutes dividing an hour, hours dividing a day, or `1d`; 1 to 10000 | The `identity.cleanup` cron schedule and its delete batches |

At least one sign-in method is required: the `password` or `magic_links`
table, or an OAuth provider (`no_sign_in_method`). Passkeys alone are none,
since they are registered from a signed-in session (owner answer ID7).

Registration sets a binding cookie,
`__Host-merezha_verify` (`merezha_verify` with the development cookie,
`HttpOnly`, `SameSite=Lax`, for `tokens.email_verification_ttl`) whose
digest is kept on the user row until verification; resends set none. A
verification confirmed without the registration's cookie claims an
unverified account: its password, credentials and sessions go, and the new
session answers `"password_unset": true`. Email change tokens are bound to
the requesting session; any other confirmation answers
`401 sign_in_required` and changes nothing; a rotation of that session
carries the token to the new session id. Password reset, verification
resend and magic-link requests only queue an `identity.link_request` job
(queue `identity`, flagged `contains_secret`, so its payload is nulled at
relay and at the terminal state); the job looks the account up and requests the mail, so
the request path is the same for every address. Every statement identity
runs is a registered `Statement` (`merezha_identity::sql::STATEMENTS`).

At start the block refuses to run without a notification delivery path
(`notifications_required`, logged as `module.start_failed`): verification
and reset mail would be dropped by the capture fallback. With
`[modules.notifications]` present the block's notifier delivers (NT-N2:
with the default capture driver, mail lands in `merezha_notify.captures`
and `GET /v1/notifications/dev/captures`; a plain-http development
`app_origin` needs `templates.allow_http_links = true`, since the `url`
filter accepts `http:` only for loopback with it). Without the block, only
`dev_print_links = true` on a private loopback listener starts it, and
every link then appears on stderr as `identity.dev_link` (stderr only,
never exported). It also registers the
`identity.cleanup` and `identity.link_request` kinds on queue `identity` and
the cleanup schedule (UTC), which
deletes expired sessions and tokens, drained rate-limit buckets, audit rows
past retention and, while `require_verified_email = true`, unverified
accounts past their TTL that never signed in.

Routes, all under `/v1/identity/`, JSON bodies of at most 16 KiB
(`415 unsupported_media_type`, `413 body_limit`), every answer
`Cache-Control: no-store`: `POST registrations`, `POST email-verifications`,
`POST email-verifications/confirm`, `POST sessions`, `GET`/`DELETE session`,
`POST session/reauthenticate`, `GET sessions`, `DELETE sessions/{id}`,
`POST sessions/revoke-all`, `POST magic-links`, `POST magic-links/consume`,
`POST password-resets`, `POST password-resets/complete`, `GET`/`PATCH`/`DELETE me`,
`PUT me/password`, `POST me/email-changes`, `POST me/email-changes/confirm`,
`GET me/security-events` (identity §4 has the bodies and errors).

Sessions travel in the cookie `__Host-merezha_session` (`Secure`, `HttpOnly`,
`SameSite=Lax`, `Max-Age` to the absolute expiry), or `merezha_session`
without `Secure` when `app_origin` is a loopback origin on a private
listener, or as `Authorization: Bearer mzs_...`. A cookie and an
`Authorization` header together answer `400 ambiguous_credentials`; a bad,
expired or revoked credential `401 unauthenticated` (a dead cookie is
cleared in the same answer); a database failure `503 unavailable`. Unsafe
cookie requests on every route need an allowed `Origin` (or none with
`Sec-Fetch-Site: same-origin`, else `403 origin_rejected`) and the
`X-CSRF-Token` the session answers return (`403 csrf_failed`). Anonymous
unsafe identity requests need the Origin check only, so non-browser clients
send `Origin: <app_origin>`; bearer requests need neither.

Error codes it registers: `ambiguous_credentials` (400),
`invalid_credentials`, `invalid_token`, `reauthentication_required`,
`sign_in_required` (401),
`origin_rejected`, `csrf_failed`, `email_unverified`, `account_disabled`,
`registration_closed` (403), `email_taken`, `sole_owner` (409),
`password_too_short`, `password_too_long`, `password_blocklisted`,
`password_contextual`, `notifications_required`, `blocklist_unavailable`
(422).

### Organizations, RBAC and keys

Routes (identity §4 has the bodies and errors), all JSON with the same
limits; `{org}` is an `org_` id and every check under it is at
`Tenant(org)`, every check under `application/` at `Application`:

- `POST`/`GET organizations`, `PUT session/organization`, `GET permissions`
  (the catalog), `GET me/permissions`, `POST invitations/inspect`,
  `POST invitations/accept`;
- `GET`/`PATCH`/`DELETE organizations/{org}`, `POST .../ownership-transfers`,
  `GET .../me/permissions`, `GET .../members`, `PATCH`/`DELETE
  .../members/{membership}`, `POST`/`GET .../invitations`, `DELETE
  .../invitations/{id}`, `GET`/`POST .../roles`, `PATCH`/`DELETE
  .../roles/{role}`, `GET`/`POST .../role-assignments`, `DELETE
  .../role-assignments/{id}`, `GET`/`POST .../service-accounts`,
  `PATCH`/`DELETE .../service-accounts/{id}`, `GET`/`POST .../api-keys`,
  `DELETE .../api-keys/{id}`, `GET .../audit-events`;
- `GET application/users`, `PATCH application/users/{user}`, `GET
  application/roles`, `GET`/`POST application/role-assignments`, `DELETE
  application/role-assignments/{id}`, and the service account, API key and
  audit routes of a tenant under `application/`.

Listings take `?cursor&limit` (1 to 100, default 50) and answer
`next_cursor`. An id of another type or another organization answers
`404 not_found`, never `403`; a tenant route called by someone with no
standing in the organization (no membership, no application grant) is
`404` too. Every grant path (custom roles, assignments, member roles,
invitations, API keys, user status) requires the caller to hold every
permission involved at the target scope (`403 permission_escalation`); only
owners grant or remove `owner`, and an organization always keeps one
(`409 sole_owner`, also for `DELETE /me`). `platform_admin` and the implicit
`user` are never assigned over HTTP (`403 builtin_role`).

The active organization comes from `PUT session/organization` or the
`Merezha-Organization: org_...` header (it wins; CORS allows it): one the
caller belongs to, or any with `identity.organization.enter` at
`Application` (audited `organization.entered`), else `404 not_found`. A
principal carries a grant snapshot built once per request and cached per
instance by `(credential, organization)` with the user's, service
account's and organization's `authz_version`: any grant change is visible
on the next request on every instance.

API keys are `Authorization: Bearer mzk_<16 base32>_<43 base64url>`, shown
once when created; only the SHA-256 digest of the secret is stored. A key
is pinned to one organization or the application, lists its permissions,
and its power is that list intersected with its owner's live grants at its
scope: a tenant key never acts elsewhere, an application key never in a
tenant, and a `platform_admin`'s key never breaks glass. A key minted by
someone other than its owner (a service account's) is also capped, on
every request, at what its minter holds now at the key's scope, and has no
power once the minter is deleted, disabled or no longer a member; deleting
or disabling an account revokes every key it minted. A revoked (audited
`api_key.used_after_revocation`) or expired key, an inactive owner, and a
user's tenant key without its owner's live membership answer
`401 unauthenticated`; removing a member revokes its keys for the
organization, and so are the service account keys it minted there
(listings show `created_by`); its pending invitations are deleted, and a
role change that drops the authority behind them does the same. Accepting
an invitation re-checks its inviter's live grants
(`409 invitation_stale`). Failed key presentations spend `api_key_invalid_ip`
(`429 rate_limited` with `Retry-After`). Revoked and expired keys are
deleted by the cleanup job after 30 days, expired invitations at once.

`merezha run` installs identity's RBAC `Authorizer` and `Directory` with
the block, and builds its catalog from identity's and the host's
permissions, `merezha_content::CONTENT_PERMISSIONS` and
`notifications.templates.preview`; the resource kind `content_type` may be
assigned. At start the block syncs the declared application roles; a
declared role removed while still assigned stops startup
(`declared_role_in_use`, logged `identity.declared_role_in_use` with the
role key and count), and stored custom role grants the catalog no longer
has are counted in `identity.unknown_permissions` and grant nothing.

`merezha identity platform-admin grant|revoke EMAIL --config PATH
[--set SECTION.KEY=VALUE]...` is the only way to grant the break-glass
`platform_admin` role. It needs the identity and jobs migrations applied,
a verified active account to grant, and prints
`{"status":"ok","action":"granted"|"revoked"|"unchanged","user_id"}` with
exit code 0; an unknown account (`not_found`), an unverified one
(`email_unverified`), invalid configuration or pending migrations
(`migrations_pending`) print `{"status":"invalid","error":{"code"}}` with
exit code 2, a database failure `{"status":"failed",...}` with exit code 1.
It is audited `platform_admin.granted` / `platform_admin.revoked` and
publishes `identity.user_application_roles_changed`.

Integration events (R11, in the change's transaction, envelope `tenant` the
organization): `identity.organization_created`, `_updated`, `_deleted`,
`identity.membership_created`, `_roles_changed`, `_removed`,
`identity.user_application_roles_changed`, `identity.user_disabled`,
`identity.user_enabled`, besides PR 1's.

Error codes PR 2 registers: `permission_escalation`, `email_mismatch`,
`builtin_role` (403), `slug_taken`, `slug_reserved`, `role_key_taken`,
`role_in_use`, `already_member`, `invitation_stale` (409), `unknown_permission`,
`unknown_resource_kind`, `resource_kind_tenant_only`, `organization_limit`, `role_limit`,
`assignment_limit`, `service_account_limit`, `api_key_limit`,
`declared_role_in_use` (422).

The permission matrix of the built-in roles is
[identity-permissions.md](../design/identity-permissions.md), generated by a
test.

`merezha identity password-benchmark [--config PATH] [--set SECTION.KEY=VALUE]...
[--memory-kib N] [--iterations N] [--parallelism N] [--samples N]` hashes
`--samples` times (default 5, at most 100) with the project's
`[modules.identity.password]` cost, the flags overriding it, and prints
`{"status":"ok","algorithm":"argon2id","argon2_memory_kib",
"argon2_iterations","argon2_parallelism","samples","min_ms","median_ms",
"max_ms"}` with exit code 0; invalid arguments, configuration or cost below
the floors print `{"status":"invalid","error":{"code"}}` with exit code 2.
Aim for about 60 to 120 ms per hash on production cores.

### OAuth, OIDC and passkeys

```toml
[modules.identity.oauth]
link_by_verified_email = false              # owner answer ID1

[modules.identity.oauth.providers.google]   # discovery at accounts.google.com
client_id = "123.apps.googleusercontent.com"
client_secret = { env = "GOOGLE_CLIENT_SECRET" }

[modules.identity.oauth.providers.github]
client_id = "Iv1.abc"
client_secret = { env = "GITHUB_CLIENT_SECRET" }

[modules.identity.oauth.providers.acme_sso]
kind = "oidc"
issuer = "https://sso.acme.example"
client_id = "merezha"
client_secret = { file = "/run/secrets/acme_sso" }

[modules.identity.passkeys]                 # needs the `passkeys` build feature
```

| Key | Default | Range | Meaning |
| --- | --- | --- | --- |
| `links.sign_in_redirect`, `links.oauth_error` | `/`, `/auth/error` | as the other links | Where an OAuth callback lands without `return_to`, and where a failed one lands with `#error=<code>` |
| `oauth.flow_ttl`, `oauth.http_timeout` | `10m`, `10s` | 1 min to 1 h, 1 s to 30 s | Lifetime of a started flow and its binding cookie; deadline of each call to a provider |
| `oauth.link_by_verified_email` | `false` | | A trusted verified address of an existing verified account links instead of answering `account_exists` |
| `oauth.providers.<name>` | none | at most 16; `name` `[a-z][a-z0-9_]{0,31}` (`invalid_provider_name`); `google` and `github` imply their kind, any other name needs `kind = "oidc"` | One provider; its redirect URI is `{api_base_url}/v1/identity/oauth/<name>/callback` |
| `.issuer` | Google's for `google`, none for `github` (`unknown_key`) | `https://`, or `http://` on a loopback host on a private loopback listener (`insecure_origin`); required for `oidc` | The OIDC issuer: discovery, `iss` checks |
| `.client_id`, `.client_secret` | required | 1 to 256 bytes; a `{ env }` or `{ file }` secret | The client registration |
| `.display_name`, `.scopes` | `Google`, `GitHub` or the name; `openid email profile`, GitHub `read:user user:email` | 1 to 64 characters; 1 to 16 scope tokens, OIDC ones with `openid` (`missing_openid_scope`) | Shown by `GET oauth/providers`; requested scopes |
| `.trust_email`, `.allow_sign_up` | `true` for Google and GitHub, `false` for `oidc`; `true` | | The provider's verified address counts as verified (section 6); new accounts may be created through it |
| `.allowed_hosted_domains` | `[]` (any) | Google only, lowercase domains | Google's `hd` claim must be one of them |
| `passkeys.rp_id`, `passkeys.rp_name` | the host of `app_origin`, `app.name` | `app_origin`'s host or a registrable suffix of it (`rp_id_mismatch`) | The WebAuthn relying party |
| `passkeys.origins` | `[app_origin]` | each one of `allowed_origins` (`origin_not_allowed`) under the RP id | Accepted WebAuthn origins |
| `passkeys.ceremony_ttl`, `passkeys.discoverable`, `passkeys.max_per_user` | `5m`, `true`, `20` | 30 s to 30 min, 1 to 100 | Ceremony lifetime; usernameless (conditional UI) sign-in; passkeys per user (`passkey_limit`) |

A build without the `passkeys` feature of `merezha-identity` refuses the
`passkeys` table (`passkeys_unavailable`); the prebuilt `merezha` host has
it. The feature pulls `webauthn-rs`, MPL-2.0, and OpenSSL, built vendored
(a C compiler and Perl at build time, no system OpenSSL at run time).

Routes (identity §4 has the bodies and errors): `GET oauth/providers`,
`POST oauth/{provider}/start` (`{intent: "sign_in" | "link", return_to?}`,
a link needs a recent session), `GET oauth/{provider}/callback`,
`GET me/oauth-identities`, `DELETE me/oauth-identities/{id}` (recent),
`POST passkeys/registrations/options` and `/verify` (recent),
`POST passkeys/authentications/options` and `/verify`,
`POST session/reauthenticate/passkey/options` and `/verify`,
`GET me/passkeys`, `PATCH me/passkeys/{id}` (`{label}`),
`DELETE me/passkeys/{id}` (recent). The three `verify` routes take JSON
bodies of up to 64 KiB, the others 16 KiB. Without a provider the OAuth
routes answer `404 not_found`; without the `passkeys` table, the passkey
routes do too.

A start sets the flow binding cookie `__Host-merezha_oauth`
(`merezha_oauth` with the development cookie, `HttpOnly`,
`SameSite=Lax`, for `oauth.flow_ttl`; a well-formed one the browser
already holds is kept, so two tabs can sign in at once) and answers the
provider's authorization URL with `state`, a PKCE S256 challenge and, for
OIDC, a `nonce`. The callback consumes the flow by `state` first (a replay
finds nothing), requires the binding cookie, the provider of its path and
an `iss` equal to the issuer when one is sent, exchanges the code, checks
the ID token (signature against the provider's key set, refetched once on
an unknown `kid` and at most once a minute; `RS256`, `ES256`, `EdDSA` only;
`iss`, `aud`, `azp`, `exp`, `iat` within five minutes, `nonce`), drops the
provider's tokens, and applies the linking rules of identity §6. A
provider whose discovery document advertises RFC 9207 must send `iss`.
Identities store the issuer they were linked under, and one whose issuer
differs from the provider's current one never signs in (`oauth_failed`).
Failed callbacks naming a live flow are charged to `oauth_callback_ip`,
checked first (`#error=rate_limited`); one naming no live flow is counted,
not audited, and charged to `oauth_callback_unknown_ip`, so junk never locks
out the same IP's legitimate callbacks. A link callback re-checks that the
starting session is still within the reauthentication window
(`#error=reauthentication_required`).
The binding cookie is cleared only by its own browser's callback once no
other flow of it is pending, so two tabs both complete. It always
answers `303`: to `return_to` (resolved on `app_origin`) or
`links.sign_in_redirect` with the session cookie on a sign-in, or to
`links.oauth_error#error=<code>`, codes `invalid_state`, `oauth_denied`,
`oauth_failed`, `provider_unavailable`, `rate_limited`,
`reauthentication_required`, `account_exists`,
`identity_already_linked`, `registration_closed`, `email_unverified`,
`account_disabled`; `Referrer-Policy: no-referrer` keeps the code out of the
next page's `Referer`. An account created through a provider not trusted
for its address stays unverified: while verification is required the
callback lands on `email_unverified`, sets the registration binding cookie
and mails the verification link, which, clicked in that browser, keeps the
identity (a registration binding the browser already holds is reused).
Turning a provider's `trust_email` on later verifies nothing it created
before. A link onto an existing account and a passkey registration send
`identity.sign_in_method_added` (security, email and inbox); both re-check
their session under the user lock before storing anything. A sign-in through a trusted provider onto an unverified account
runs the ownership claim first (its password, passkeys, OAuth identities,
sessions and keys go). Discovery documents and key sets are cached for an
hour per instance, a failure for 30 s; an unreachable or broken provider
answers `503 provider_unavailable` and never affects readiness.

Passkey options for an email return its credential ids, or one or two
decoy ids (16, 32 or 64 bytes) derived from `secret_key` for an unknown address or a user
without passkeys, in the same shape; without an email, a discoverable
ceremony with `mediation: "conditional"`. Ceremonies are stored server-side,
consumed before verification, bound to their user and session, and live
`passkeys.ceremony_ttl`; the cleanup job deletes expired flows and
ceremonies. A counter that went backwards on a credential with a nonzero
counter refuses the sign-in (`invalid_credentials`) and audits
`passkey.counter_regression`. Removing an identity or a passkey that is the
last way in (a password with password sign-in on, a passkey, an identity of
a configured provider, or a verified address with magic links on) answers
`409 last_credential`.

Error codes PR 3 registers: `invalid_ceremony` (401), `last_credential`,
`identity_already_linked`, `passkey_exists` (409), `passkey_limit`,
`passkey_rejected`, `passkeys_config` (422), `provider_unavailable` (503).

`deny.toml` (`cargo deny check`) allows MPL-2.0 for `webauthn-rs` and
records scoped ignores for RUSTSEC-2023-0071 (`rsa` 0.9) and
RUSTSEC-2026-0387 (`crypto-bigint` 0.5), both pulled by `openidconnect`
4.0.1 and unreachable from identity, which only verifies signatures with
public keys.

## Notifications block

`merezha run` registers the notifications block (`crates/notifications`,
[design](../design/notifications.md)). It stays off until
`[modules.notifications]` is present. Slice NT-N1 ships its configuration,
templates, previews and capture drivers. Slice NT-N2 adds the `Notifier`
other blocks request through, expansion, the email and SMS delivery worker
over the capture drivers, the inbox REST routes, preferences, suppressions,
unsubscribe, `send`, `status` and `redeliver`. Slice NT-N3 adds Brevo
email: the email driver, the email status callback and `merezha
notifications brevo register-webhook` ([Brevo email](#brevo-email)). Slice
NT-N4 adds Brevo SMS: the SMS driver and its signed status callback
([Brevo SMS](#brevo-sms)). Slice NT-N5 adds the inbox stream ([Inbox
stream](#inbox-stream)), NT-N6 Web Push ([Web Push](#web-push)) and
NT-N7 outgoing webhooks ([Outgoing webhooks](#outgoing-webhooks)).

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
locales, the channel drivers and `warnings` (`[{key, code}]`, such as
`security_mail_disabled` when `email.driver = "disabled"` drops identity's
mail to `skipped`), and exit code 0; or `status` `invalid` with
`issues: [{key, code, origin}]` and exit code 2. `merezha run` refuses to start on the same issues
(`invalid_config`). Issues name settings and template files, never values.

### What starts with the block

- Migration 1 (`core`) creates the schema `merezha_notify` with the core
  tables of NT §2.4: `notifications`, `deliveries`, `delivery_attempts`,
  `provider_events`, `preferences`, `user_settings`, `suppressions` and
  `captures`. Migration 2 (`inbox`, NT-N2) adds `inbox_counters` and
  `inbox_items`, stores user and tenant ids as text and adds the deliveries'
  recipient `name`. Migration 3 (`provider_events`, NT-N3) adds pending
  provider events. Migration 4 (`push`, NT-N6) adds
  `push_subscriptions`. Migration 5 (`webhooks`, NT-N7) adds
  `webhook_endpoints`, `webhook_events`, `webhook_deliveries`,
  `webhook_attempts` and `webhook_audit`. The checksums are pinned by a test; later slices
  add migrations.
- At start the block registers `notify.expand` (queue `notify.expand`),
  the delivery kinds of its capture drivers and the maintenance kinds and
  schedules with the jobs runtime.
- The block claims the `api` source (`Module::extra_sources`) for its HTTP
  send route.
- Routes under `/v1/notifications/`:

  | Method and path | Access | Answer |
  | --- | --- | --- |
  | `GET templates` | `notifications.templates.preview` at `Application` | `[{name, description, locales, channels}]` |
  | `POST templates/{name}/preview` | same | `{locale?, data?, channels?}` renders every channel (`Cache-Control: no-store`): `{email: {subject, html, text}, sms: {text, encoding, segments}, push: {title, body, url}, inbox: {title, body, link, payload}}`. Without `data`, the template's preview data. `data.secret` is the preview's secret data. 422 `render_failed` with `details: [{path: "/channels/<channel>", code}]`, `reserved_data_key`, `data_too_large`, `channel_not_allowed`, `invalid_locale`; 400 `invalid_input`; 404 for an unknown template |
  | `GET templates/{name}/preview.html?locale=` | same | the HTML email with `Content-Security-Policy: default-src 'none'; style-src 'unsafe-inline'; img-src https: data:; sandbox` |
  | `GET dev/captures?channel=email\|sms&limit=1..100` | a loopback peer (not through a trusted proxy) with a loopback `Host` (`localhost`, `127.0.0.1` or `[::1]`, any port, against DNS rebinding) while a capture driver is active | the newest captured messages, `[{delivery_id, channel, target, rendered, captured_at}]`; 404 otherwise |

  The three template routes answer 404 when `templates.preview = false`.
  Without the identity block the kernel policy decides: allowed on a private
  loopback listener, 401 elsewhere.
- The capture drivers (`email.driver = "capture"`, the default, and
  `sms.driver = "capture"`) keep each rendered message in
  `merezha_notify.captures`, the newest `retention.captures` rows, and accept
  it with provider `capture`. A unique index on the delivery and its
  idempotency key makes a second capture of the same generation answer
  duplicate, as Brevo does. Captures hold rendered
  secret data, which is why `http.public = true` refuses a capture driver.

### Requests, expansion and delivery

The block provides the `Notifier` of every context (`Module::notifier`),
bound to the caller's source; without the block the kernel's capture
fallback stays. `validate` checks the topic, template, channels (a subset
of the topic's), 1 to `limits.max_recipients` recipients, reserved data keys
and `limits.max_data_bytes` (data plus secret data), then dry-renders every
requested channel in the default locale with strict undefined. `request`
adds the membership rule (with a tenant, every user recipient in
`members`, else `recipient_not_in_tenant`, for every source but `identity`)
and
appends one job record (`notify.expand`, outbox source `notifications`, key
`request:{notification_id}`, flagged `contains_secret` when the request has
`secret_data`, `run_at = not_before`) in the caller's transaction. It makes
no Directory call and takes no other connection, so it completes on a pool
whose only connection the caller holds. The notification id is
`UUIDv5(source, dedupe_key)`, so the same key twice names one notification
(`duplicate: true` while the outbox row is retained). `emit_webhook`
appends a `notify.webhook_event` record for a declared event type
(`[modules.notifications.webhooks]` present, else `webhooks_disabled`),
which the webhook fan-out handles ([Outgoing webhooks](#outgoing-webhooks)).
The whole record, envelope and metadata, is at most
`webhooks.max_payload_bytes` and the outbox's 64 KiB payload limit
(`data_too_large`).
Metrics label only declared topics; any other `notify.topic` is
`unknown`.

The `notify.expand` job resolves recipients through the `Directory` before
it opens its transaction: `users` in batches of 100, tenant and application
members a page at a time up to `limits.max_targets + 1`. It reads
preferences, suppressions and time zones, then writes in one transaction
the notification, one delivery per channel and target (the same mailbox
from a user and an explicit address is one delivery), the inbox items
numbered from the users' `inbox_counters` (locked in ascending user order),
`pg_notify('merezha_notify_inbox', user)` and one job record per email or
SMS delivery (`notify.deliver.<channel>`, key `deliver:{id}:g{generation}`,
flagged when the request had secret data), at most 256 per append. A
second run of the same job writes nothing. Deliveries that are not sent
get `skipped` with a reason:

| Reason | When |
| --- | --- |
| `unknown_user` | the Directory did not return the user |
| `no_directory` | no identity block (`EmptyDirectory`): user, tenant and application recipients; addresses still work |
| `channel_disabled` | the channel's driver is `disabled` or its section absent; with `email.driver = "disabled"` built-in security mail is recorded this way and `merezha notifications check` warns `security_mail_disabled` |
| `preference` | the user's `(topic, channel)` or `('*', channel)` preference is off, or the channel is outside a transactional topic's `default_channels` without an opt-in; security topics ignore preferences |
| `not_opted_in` | a marketing topic without the user's explicit `enabled = true` for the topic and channel |
| `suppressed` | a suppression covers the address: `invalid` for every topic, the other reasons for non-security topics (topic-scoped ones for their topic only) |
| `no_subscription` | a push target whose user has no live subscription made with the current VAPID key in the notification's tenant scope; at send, a subscription that is gone, moved to another user or expired since |

More targets than `limits.max_targets` (or `max_marketing_targets` for a
marketing topic) reject the notification (`status = rejected`,
`status_reason = too_many_targets`) with no deliveries. A request past
`expires_at` expands its deliveries as `expired`. Inbox items are rendered
during expansion and stored with `expires_at = now + inbox.retention`;
inbox deliveries are created `delivered`.

The delivery worker (`notify.deliver.email`, `notify.deliver.sms`, and
`notify.deliver.push` with a push section) registers email and SMS for a
`capture` or `brevo` driver. Per attempt it locks the delivery (another generation or a
settled delivery is done; past `expires_at` it becomes `expired`;
suppressions and preferences are checked again), commits attempt `n + 1`
as `started`, renders from the stored data and the job's secret data, sends
before `min(10 s, job deadline - 2 s)` (`brevo.timeout` in place of 10 s
for Brevo) and records the outcome: `accepted`
(capture), `retrying` with the job retried, `failed` with a reason
(`render_failed`, `data_purged`, `secret_purged`, `invalid_address`,
`retries_exhausted` on the last attempt), or `unknown` for an ambiguous SMS
(never resent, NT1). A last attempt still `started` is ambiguous: email
resends with the same idempotency key (`UUIDv5(delivery_id,
"g{generation}")`, which the capture driver answers as duplicate), SMS
becomes `unknown`. Non-security email carries an unsubscribe link when
`public_base_url` is set.

Secret data is rendered into email and SMS, never into the inbox, never
stored in a `merezha_notify` table other than the development `captures`,
never logged; it lives only in the flagged expansion and delivery job
payloads, which the relay and the jobs runtime null (R3). Identity's user
and tenant ids are stored as text (migration 2).

Maintenance, each a cron schedule on queue `notify.maintenance` (UTC):

| Task (kind `notify.maintenance.<task>`) | Cron | Work |
| --- | --- | --- |
| `settle_notifications` | every minute | sets `settled_at` on notifications whose deliveries all settled, 1,000 per batch |
| `purge_data` | every 5 minutes | nulls `data` (and the deliveries' recipient names) of notifications settled more than `retention.data_after_settled` ago; counts `merezha.notification.data_purged` |
| `inbox_expire` | hourly | deletes items past `expires_at` or archived more than `inbox.archived_retention` ago, 100 users per transaction in ascending order, raising each user's `purged_through` |
| `webhook_auto_disable` | every 5 minutes | with `[modules.notifications.webhooks]`: disables the endpoints failing for `webhooks.auto_disable_after` with at least 10 failed attempts since, and alerts ([Outgoing webhooks](#outgoing-webhooks)) |
| `retention` | daily (03:29) | the webhook rows so far: nulls event bodies older than `retention.webhook_deliveries`, deletes events older than `retention.deliveries` with no open delivery (their deliveries and attempts with them), deletes audit rows older than 400 days, drops previous secrets whose rotation window closed, and with `secret_key_previous` set seals again under `secret_key` the secrets still under another key (by their key id), then records `merezha.webhook.secrets.previous_key` and `merezha.webhook.secrets.unreadable`; 500 rows per batch. Each step runs whatever the earlier ones did: a failing step is logged `notification.maintenance_step_failed` (`task`, `reason` = the step, `code`) and becomes the run's outcome after the others ran |
| `push_expire` | hourly (minute 43) | with `[modules.notifications.push]`: deletes subscriptions past `expires_at`, made with another VAPID key and unused for 7 days, or with 10 consecutive failures and no success in 30 days, 1,000 per batch; counts `merezha.notification.push_subscriptions_removed` (`expired`, `key_changed`, `failures`) |

### Brevo email

With `email.driver = "brevo"` the worker sends through Brevo's
`POST {brevo.base_url}/v3/smtp/email` on the kernel's outbound client
(`brevo.connect_timeout`, deadline `brevo.timeout`, no automatic retry, no
`traceparent`), with `api-key`, `accept` and `content-type:
application/json`. Each request has one recipient (names cut to 70
characters), the topic as tag, `headers.idempotencyKey` (`UUIDv5(delivery_id,
"g{generation}")`, the same on every attempt of a generation),
`headers.X-Mailin-custom = mzd:<delivery_id>`, the RFC 8058
`List-Unsubscribe` and `List-Unsubscribe-Post` headers for non-security
topics with an unsubscribe link, and `X-Sib-Sandbox: drop` with
`brevo.sandbox = true`. The 201's `messageId` becomes the delivery's
`provider_message_id`.

| Brevo answer | Attempt outcome | Delivery |
| --- | --- | --- |
| 2xx | `accepted` | `accepted` |
| 400 `duplicate_parameter` | `duplicate` | `accepted` without a message id (SMS: `unknown` / `ambiguous`) |
| 400 `duplicate_request` | `transient` | `retrying`, at least 60 s later |
| 401, 402, 403, or code `unauthorized`, `not_enough_credits`, `permission_denied`, `account_under_validation` | `transient` (`provider_account`) | `retrying`, at least 5 min later (at most the `notify.email` `backoff_max_ms`, which must be at least 5 min, else startup fails `backoff_max_too_short`). The answer consumes no attempt: the next try is a new job of the same generation (outbox key `deliver:{id}:g{n}:a{attempt}`), until the notification's `expires_at`, or 24 h after the run's first refusal, then `failed` / `provider_account_unresolved`. One error log `notification.provider_account_error` per run of refusals; `merezha.notification.provider_account_errors` counts each |
| 408, 425, 429 | `rate_limited` | `retrying` after `max(Retry-After, x-sib-ratelimit-reset)` (at most 1 h, and at most the queue's `backoff_max_ms`); warn log `notification.provider_rate_limited` |
| other 4xx | `permanent` | `failed` / `provider_rejected_<code>` for `invalid_parameter`, `missing_parameter`, `out_of_range`, `document_not_found`, `not_acceptable`, else `provider_rejected` |
| 5xx | `transient` (`provider_unavailable`) | `retrying`, after `Retry-After` when sent |
| no connection (connect, DNS, TLS) | `transient` (`upstream_unavailable`, `upstream_timeout`) | `retrying` |
| timeout or connection lost once connected | `ambiguous` | `retrying`; the resend reuses the key |

Only Brevo's error `code` is read; its `message` is never stored or logged.
On the last attempt a retryable answer becomes `failed` /
`retries_exhausted` and the job is dead-lettered. A crash after the call
leaves a `started` attempt: the rerun marks it `ambiguous` and resends with
the same key, which Brevo answers `duplicate_parameter`. A resend must
start within 25 minutes of the generation's first ambiguous attempt, the
30-minute key window less a margin: a retry or rerun that would start
later sets the delivery `unknown` / `idempotency_window` and never resends
(a provider event can still settle it; `redeliver` starts a new generation
and key). The block assumes
that answer is a 400, that only a processed request takes the key and that
the key holds 30 minutes, as `testkit::FakeBrevo` models; this needs a check
against a real Brevo account (NT §12).

**Status callback.** `POST /v1/notifications/providers/brevo/email` is a
credential-free route: the edge never runs the session authenticator on it,
so Brevo's bearer is never read as a session. The handler checks the client
address the edge resolved through `http.trusted_proxies` against
`brevo.webhook_allowed_cidrs` (when not empty), then compares
`Authorization: Bearer` with `brevo.webhook_token` in constant time;
either failure answers 401 `unauthenticated`, stores nothing and logs
`notification.provider_event_rejected` (code `source_not_allowed`,
`missing_token` or `bad_token`); with CIDRs set, a request whose client
address the edge could not resolve is refused too. Without a Brevo email driver the route is
404. The body (an event or an array of at most 2,000, at most 1 MiB, else
413 `body_limit`) is applied in one transaction:

- events are normalized (`request`, `delivered`, `deferred`, `soft_bounce`,
  `hard_bounce`, `invalid_email`, `blocked`, `spam`, `unsubscribed`,
  `error`); opens and clicks are dropped unstored (NT3);
- each is correlated by `X-Mailin-custom`, then by message id (resolved
  first, then locked in ascending delivery order), and stored in
  `provider_events` once per `sha256(brevo|email|message id|event|ts_event)`,
  the delivery and the address standing in for a missing message id;
- `delivered` moves `accepted` or `unknown` to `delivered`; `hard_bounce`
  and `invalid_email` move `accepted`, `unknown` or `delivered` to `bounced`
  and add a `hard_bounce` or `invalid` suppression of the address; `spam`
  moves `accepted` or `delivered` to `complained` and adds a
  `spam_complaint` suppression; `blocked` and `error` move `accepted` or
  `unknown` to `failed`; `unsubscribed` adds an `unsubscribed` suppression
  for the delivery's topic; the rest is recorded only. Suppressions use the
  delivery's own address. An event about a delivery still `queued`,
  `sending` or `retrying` is stored `pending` (migration 3) and the worker
  applies its status change in the transaction that records the outcome;
- an uncorrelated event is stored with no delivery and no effect at all (no
  status, no suppression: an event can name any address) and still
  answered 200 `{}`, so Brevo stops retrying.

`provider_events.reason` holds Brevo's reason text (at most 200
characters), personal data such as a bounced address, until the row's
retention; it is never logged.

Hard bounce and spam suppressions cover every non-security topic, `invalid`
every topic, so the next non-security email to a bounced address is
`skipped` / `suppressed`. Logs and metrics carry the normalized event name
and whether it was correlated, never an address, subject, reason text or
message id.

**Webhook registration.**

```sh
merezha notifications brevo register-webhook --config merezha.toml [--url URL [--allow-host]]
```

loads and validates the project like `check`, then calls Brevo's `POST
/v3/webhooks` with `type: "transactional"`, the events `delivered`,
`hardBounce`, `softBounce`, `blocked`, `spam`, `invalid`, `deferred`,
`unsubscribed` and `auth: {type: "bearer", token: <brevo.webhook_token>}`,
to `public_base_url` + `/v1/notifications/providers/brevo/email`, or to
`URL` (https, or http to loopback), which must have that path and
`public_base_url`'s host and port; `--allow-host` accepts another host with
the `host_override` warning (in `warnings` and on stderr). It prints
`{"status":"registered","id":N,"url":"…","warnings":[…]}` (exit 0) or
`{"status":"failed","code":"…"}` (exit 1: `brevo_not_configured`,
`missing_public_base_url`, `invalid_url`, `callback_path_mismatch`,
`callback_host_mismatch`, `provider_account`,
`provider_rejected[_<code>]`, `provider_unavailable` or a transport code);
an invalid project prints the `check` report (exit 2). The key and the
token are never printed. Switch Brevo's open and click tracking off in the
console: the block stores no engagement data.

### Brevo SMS

With `sms.driver = "brevo"` the worker sends through Brevo's `POST
{brevo.base_url}/v3/transactionalSMS/send` on the same client and
classifier as email (`brevo.timeout`, no automatic retry, no
`traceparent`). Each request has one number, written without its `+`
(`"recipient": "33612345678"`), `sender = sms.sender`, the rendered
`content`, `type: "marketing"` for a marketing topic (else
`transactional`), `unicodeEnabled: true` when the content (with the
prefix) is outside GSM-7, `organisationPrefix` when
`sms.organisation_prefix` is set, the tags `[<topic>, "mzd:<delivery_id>"]`
and `webUrl`, the delivery's signed status URL (below). The 201's numeric
`messageId` is stored as text. `brevo.sandbox` covers email only: Brevo has
no SMS sandbox, so a staging project with Brevo SMS sends real messages.

The content is rendered from `sms.txt` on every attempt and segmented as
GSM-7 (160 septets, 153 per concatenated part, an extension character
counting two) or UCS-2 (70 units, 67 per part), with `organisation_prefix`
and a space counted in. A request whose content exceeds `sms.max_segments`
in the default locale is refused before the caller commits (422
`render_failed`); a delivery whose content exceeds it at send time (another
locale, a template change) fails `render_failed` and sends nothing. Each
accepted SMS adds its parts to `merezha.notification.sms_segments`
(`notify.encoding` `gsm7` or `ucs2`).

Brevo answers are classified as for email, except that no idempotency key
is sent: an ambiguous attempt (a timeout or a dropped connection once
connected, a crash that left a `started` attempt, or a
`duplicate_parameter` answer, which cannot tell whether the SMS went out)
sets the delivery `unknown` / `ambiguous` and is never resent (NT1). A later
status report can still settle it, and `redeliver` starts a new
generation. Account refusals wait for an operator as email does, without
spending attempts.

**Status callback.** `POST
/v1/notifications/providers/brevo/sms/{delivery_id}/{mac}` is a
credential-free route. Its path is the `webUrl` the block sent,
`public_base_url` + `/v1/notifications/providers/brevo/sms/<delivery
id>/<mac>`, with `mac = base64url(HMAC-SHA256(k_sms, delivery
id))[..22]`, `k_sms = HKDF-SHA256(secret_key, "merezha-notify/sms-status/v1")`
and the id in its hyphenated lowercase form. The handler checks
`brevo.webhook_allowed_cidrs` (when not empty) against the edge-resolved
client address, then recomputes the MAC under `secret_key` and
`secret_key_previous` and compares in constant time; either failure (or a
non-canonical id) answers 401 `unauthenticated`, stores nothing and logs
`notification.provider_event_rejected` (code `source_not_allowed` or
`bad_mac`). Without the Brevo SMS driver the route is 404. The body (a
report or an array of at most 100, within `http.body_limit`, at most 64
KiB) runs through the email callback's transaction, correlated by the
path's delivery only (never by message id) and deduplicated on
`sha256(brevo|sms|message id|msg_status|ts_event)`:

| `msg_status` | Effect |
| --- | --- |
| `delivered` | `accepted` or `unknown` to `delivered` |
| `hard_bounce` | `accepted` or `unknown` to `bounced` / `hard_bounce`, plus a `hard_bounce` suppression of the number |
| `rejected` | `accepted` or `unknown` to `failed` / `provider_rejected` |
| `blacklisted`, `unsubscribed` (a STOP reply) | an `unsubscribed` suppression of the number for every non-security SMS topic |
| `sent`, `accepted`, `soft_bounce` | recorded only |

A redelivery keeps the delivery id, and so the status URL: a report whose
message id is not the one of the delivery's current generation is about an
earlier send. It is stored and its suppression applies, but it moves no
status, so a late `rejected` about the first SMS cannot fail (and invite a
third send of) the second. The email callback applies the same rule to
`X-Mailin-custom` correlation.

A `webUrl` never expires and lets whoever holds it report on that
delivery (and suppress its number's non-security SMS topics). Set
`brevo.webhook_allowed_cidrs` to Brevo's ranges (`1.179.112.0/20`,
`172.246.240.0/20`) and keep `/v1/notifications/providers/brevo/sms/`
paths out of proxy and load-balancer access logs.

Replies are dropped unstored (their text is the recipient's message), and
so are unknown statuses. A report about a delivery still `queued`,
`sending` or `retrying` is stored `pending` and applied by the worker once
the outcome is recorded. Brevo's `description` is kept as the event's
reason (at most 200 characters, never logged). Numbers are never logged,
put in spans or used as metric labels; the status route shows them masked
(`+33•••••••78`).

### Routes of NT-N2

Under `/v1/notifications/`. Self routes need a principal of kind `user`
(401 without one, 403 for API keys) and act on its user; application routes
check their permission at `Application` scope.

| Method and path | Access | Answer |
| --- | --- | --- |
| `POST send` | `notifications.send` | body: `topic`, `recipients` (`{user: "usr_.." \| {id}}`, `{tenant: {id, roles?}}`, `{application: {roles}}`, `{address: {email, name?, locale?}}`, `{address: {phone, locale?}}`), `channels?`, `template?`, `data?`, `secret_data?`, `locale?`, `dedupe_key` (stored as `api:{subject}:{key}`, at most 160 bytes in all, R4a), `tenant?`, `not_before?` (at most 30 days ahead, else 422 `validation_failed` at `/not_before`), `expires_at?` (after `not_before`, else at `/expires_at`), `collapse_key?`. With a tenant, user recipients are checked with `Directory::is_member` before the transaction. 202 `{notification_id, duplicate}`; 422 `invalid_dedupe_key`, `unknown_topic`, `unknown_template`, `channel_not_allowed`, `too_many_recipients`, `invalid_recipient`, `recipient_not_in_tenant`, `reserved_data_key`, `data_too_large`, `render_failed`; 503 `unavailable` (Directory) |
| `GET status/{notification_id}` | `notifications.status.read` for the caller's own sends (source `api`, key `api:{subject}:…`); `notifications.admin` for every notification; 404 otherwise | `{id, topic, status, status_reason, created_at, settled_at, deliveries: [{id, channel, status, status_reason, attempt_count, target, accepted_at, settled_at}]}`, targets masked (`a***@example.com`, `+33•••••••78`, `push`, `inbox`), at most 1,000 deliveries; 404 |
| `POST deliveries/{id}/redeliver` | `notifications.admin` | 202 `{generation}`: a new generation and job; 409 `not_redeliverable` (not `failed`, `unknown` or `expired`, or an inbox delivery), 409 `secret_purged` (the request had secret data), 410 `data_purged`; 404 |
| `GET inbox?cursor=&limit=&state=unread\|all&topic=&tenant=` | self | `{items: [InboxItem], next_cursor}`, newest first, 1 to 100 (default 30), archived items hidden; 400 `invalid_cursor` |
| `GET inbox/unread-count?tenant=` | self | `{unread}` |
| `POST inbox/{id}/read`, `…/unread`, `…/archive` | self | the `InboxItem`; a change takes the next number of the user's counter as `change_seq`; a no-op takes none. 404 for another user's item |
| `POST inbox/read-all` | self | `{up_to_seq?, tenant?}`: `{updated}`, at most 10,000 items |
| `GET preferences` | self | `{topics: [{topic, category, description, channels: [{channel, enabled, configurable}]}], settings: {timezone}}`; security and other non-configurable topics are hidden |
| `PUT preferences` | self | `{preferences: [{topic \| "*", channel, enabled}], settings?: {timezone}}`, at most 200 entries, upserted; 422 `topic_not_configurable` (with `details`), `invalid_timezone` |
| `POST unsubscribe` | token | the RFC 8058 form `List-Unsubscribe=One-Click` with `?token=`, or JSON `{token}`: a user token writes `(user, topic, email, false)`, an address token a topic-scoped `unsubscribed` suppression; 200 `{}`; 400 `token_invalid` |
| `GET unsubscribe?token=` | token | changes nothing: 303 to `links.unsubscribe_page?token=…` when set, else a page with one POST button; 400 `token_invalid` |
| `GET suppressions?channel=&address=&cursor=` | `notifications.suppressions.manage` | `{items: [{id, channel, address, topic, reason, created_at}], next_cursor}`, 100 per page |
| `POST suppressions`, `DELETE suppressions/{id}` | same | `{channel: email\|sms, address, topic?, reason: "manual"}`: 201 with the suppression (an existing one for the same key is returned); 204; 404 |

`InboxItem` is `{id, topic, title, body, link, payload, tenant_id, seq,
change_seq, created_at, read_at, archived_at}`. Every inbox route sees the
items without a tenant plus those of the principal's active tenant; a
principal without an active tenant sees only the tenantless ones. A
`tenant` parameter naming another tenant than the active one answers 403
`unauthorized`, and another tenant's item answers 404. Unsubscribe tokens are
`v1.<b64url(claims)>.<b64url(HMAC-SHA256(k_unsub, "v1." + b64url(claims)))>`
with `k_unsub = HKDF-SHA256(secret_key, "merezha-notify/unsubscribe/v1")`;
tokens signed with `secret_key_previous` still verify and never expire.
A token must name exactly one of a user and an address, the `email`
channel and a configured, user-configurable topic. Both unsubscribe routes
are credential-free (`Module::credential_free_routes`): a browser holding a
session cookie without its CSRF header, or a stale cookie, unsubscribes
like a mail client.

### Inbox stream

`GET /v1/notifications/inbox/stream` (NT-N5, NT §5.4) streams a user's inbox
changes as Server-Sent Events.

- **Access.** A principal of kind `user` (401 `unauthenticated` without
  one, 403 `unauthorized` for any other kind, API keys included: a key
  never streams), through the session cookie
  (`EventSource` with `withCredentials`) or `Authorization: Bearer`. The
  route takes no query string (400 `invalid_input`), so a credential never
  travels in a URL. Scope is the inbox routes': tenantless items plus those
  of the active tenant.
- **Response.** `Content-Type: text/event-stream`, `Cache-Control:
  no-store`, `X-Accel-Buffering: no`. The stream opens with `retry: 5000`.
  The edge applies no request deadline and no compression, so nothing
  cuts or buffers it.
- **Events.** `item` with `id: <change_seq>` and the `InboxItem` in its
  current state; `unread` (`{unread}`, no id) after each batch of items; a
  `: hb` comment after `sse.heartbeat` without other events.
- **Resume.** `Last-Event-ID` holding a decimal `change_seq` replays every
  item in scope whose current `change_seq` is above it, once, in order.
  Without a usable id the stream sends `hello` with `id: <last_seq>` (0
  without a counter row) and `{unread}`. An id above `last_seq`, at or below
  `purged_through` once anything was purged, or with more than
  `sse.replay_max` rows in scope changed since, gets `reset` with `{}` and
  `id: <last_seq>`; the client refetches and the stream goes on live from
  there.
- **Limits.** `sse.max_streams_per_user` streams per user on one instance;
  one more gets 429 `stream_limit` and no stream is evicted.
  `sse.max_streams` per instance, then 503 `overloaded` with
  `Retry-After: 5`. A stream lives `sse.max_lifetime` ±10 % and the client
  reconnects. Every `sse.reauth_interval` the stream's own credentials run
  through the `Authenticator` again; a revoked or expired session, another
  user or active tenant (a switched organization, a removed membership),
  or a failure to tell ends it. Each check counts as activity of the
  session, as any request does: an open stream keeps the session's sliding
  idle timeout (identity `sessions.idle_timeout`) from running out, and
  only its absolute timeout or a revocation ends it.
- **Backpressure.** At most 64 events wait for one client. A client that
  does not take an event within one `sse.heartbeat` interval is
  disconnected and resumes with `Last-Event-ID`; writers never wait on a
  stream.
- **Fan-out.** Each instance holds one `LISTEN merezha_notify_inbox`
  session outside the pool (`Database::listen`); a notification wakes only
  the local streams of the user it names, and each stream also reconciles
  every `sse.reconcile_interval`. A lost session is logged once as
  `notification.sse_listen_lost` (warn, `code`) and reopened after a
  backoff that starts at 1 s and doubles to 30 s, each delay drawn from
  the upper half of its step and never below 1 s. Every attempt after the
  first waits its delay, and the backoff starts again from 1 s only after
  a session stayed up 30 s, so a session that connects and drops again
  keeps backing off. The session is probed every 30 s with
  `Listener::check`. On its return it logs
  `notification.sse_listen_restored`, counts
  `merezha.inbox.listen.reconnects` and wakes every local stream. The same
  wake follows a burst of 64 notifications, which may have overflowed the
  session's queue. Such a wake reaches each user with local streams once,
  spread evenly over 1 s, and runs at most once per second: requests made
  during one coalesce into a single next one, so the streams of an instance
  never all query at once. Readiness never depends on it.
- **Drain.** `Module::draining` ends every stream before the listener
  drains, and later requests get 503 `overloaded`; clients reconnect to
  another instance with `Last-Event-ID`.
- **Telemetry.** `merezha.inbox.sse.connections`, `merezha.inbox.sse.events`
  (`notify.event`: `item`, `reset`, `hello`, `unread`),
  `merezha.inbox.sse.rejected` (`notify.reason`: `per_user`, `capacity`,
  `auth`, the last also for a stream ended by re-authentication) and a short
  root span `notify sse` (`notify.resumed`) for the opening and for each
  batch a wake triggers.

### Web Push

`[modules.notifications.push]` turns push on (NT §5.3). Generate a key pair
and keep the private half as a secret:

```sh
cargo run --locked -p merezha-cli -- notifications vapid generate
```

It prints one JSON object, `{status: "generated", private_key,
private_key_pem, public_key, key_id}`, and exits 0; it reads no
configuration. `private_key` (base64url of the scalar) or `private_key_pem`
goes into `vapid_private_key = { env = "…" }` or `{ file = "…" }`;
`public_key` is the browser's `applicationServerKey`; `key_id` is the first
16 hex of SHA-256 of the uncompressed public key. The key is never logged.

```toml
[modules.notifications.push]
vapid_private_key = { file = "/run/secrets/vapid.pem" }
vapid_subject = "mailto:ops@acme.com"
# ttl = "1d"
# allowed_hosts = ["fcm.googleapis.com", "updates.push.services.mozilla.com",
#                  "push.services.mozilla.com", "web.push.apple.com", "notify.windows.com"]
# max_subscriptions_per_user = 10
```

Routes under `/v1/notifications/`:

| Method and path | Access | Answer |
| --- | --- | --- |
| `GET push/vapid-public-key` | public (credential-free) | `{public_key, key_id}`; 404 `push_disabled` without the section |
| `POST push/subscriptions` | self | `PushSubscription.toJSON()` (`{endpoint, expirationTime, keys: {p256dh, auth}}`) plus `label?` (at most 64 characters): 201 `{id, key_id, created_at}`. 422 `push_host_not_allowed` (`details: [{path: "/endpoint", code: "host_not_allowed"}]`), `subscription_invalid` (`/endpoint` `invalid_url`, `/keys/p256dh` or `/keys/auth` `invalid_key`, `/label` or `/expirationTime` `invalid_value`); 404 `push_disabled` |
| `GET push/subscriptions` | self | `[{id, label, endpoint_origin, created_at, last_success_at}]`, newest first; the endpoint and keys are never returned |
| `DELETE push/subscriptions/{id}` | self | 204; 404 for a missing subscription, another user's, or another tenant's |

- **Allowlist (strict).** An endpoint must be `https` on port 443, with no
  userinfo or fragment, at most 2,048 bytes, and a host equal to or a
  dot-suffix of an `allowed_hosts` entry. The host must be a valid DNS name
  (labels of 1 to 63 bytes, at most 253 bytes in all, no empty label);
  otherwise the answer is `subscription_invalid`. It is checked at subscribe and
  again before every send, so removing an entry stops pushes to its
  subscriptions (`failed` / `push_host_not_allowed`). With
  `egress.allow_private_networks = true` (refused with `http.public`),
  `http` and other ports pass too, for local push services; the host rule
  stays.
- **Ownership and tenants.** `p256dh` must be a valid uncompressed P-256
  point and `auth` 16 bytes. A subscription records the user and the
  session's active tenant, like the inbox: a notification with a tenant
  reaches only the subscriptions made in that tenant, one without a tenant
  every subscription of its user; listing and deleting see the tenantless
  subscriptions and the active tenant's. A known endpoint is upserted: it
  moves to the subscribing user (a browser profile that changed account),
  and when it changes user or tenant it gets a new id, so a push queued for
  the previous owner is not sent. Past `max_subscriptions_per_user` the
  least recently used subscriptions are deleted, under a per-user lock;
  the subscription the request just made is never the one deleted.
- **One browser, one tenant (for client authors).** A browser profile has
  one endpoint per application server key, and the endpoint is unique, so a
  subscription serves one user in one tenant at a time. Subscribing again
  after switching tenants moves the subscription (new id) to the new
  tenant: the browser stops receiving the previous tenant's notifications.
  A subscription made without an active tenant receives only the
  notifications without a tenant. In v1 one browser cannot receive several
  tenants' pushes at once; a client should resubscribe when the user
  switches tenants and say so in its interface.
- **Key rotation.** Deploy the new `vapid_private_key`; clients compare the
  `key_id` of `GET push/vapid-public-key` with the one they subscribed
  with and resubscribe. Each instance targets only the subscriptions of
  its own key, and `push_expire` deletes another key's subscriptions only
  once they have gone unused for 7 days (`last_used_at`). During a rolling
  deploy the instances on the old and on the new key each keep using their
  own subscriptions, so neither deletes the other's; after the rollout the
  old key's subscriptions stop being used and go a week later. Until the
  rollout completes, a push job picked up by an instance on the other key
  is `skipped` / `no_subscription`: that push is not sent, though the
  subscription stays. The inbox keeps the durable copy of the notification.
  Issue #66 tracks sending these pushes instead.
- **Delivery.** Expansion creates one push delivery per live subscription
  made with the current VAPID key (`push_subscription_id`, target key
  `pushsub:<id>`). The `notify.deliver.push` worker (queue `notify.push`)
  runs the steps of the email worker; it encrypts the payload with RFC
  8291 `aes128gcm` and signs an RFC 8292 VAPID JWT (`aud` the endpoint
  origin, `exp` 12 h, `sub` the subject, cached per origin for an hour).
  Headers: `TTL` (`push.ttl` in seconds), `Urgency` from the topic
  priority, and `Topic` (the first 32 base64url characters of SHA-256 of
  the collapse key) when a collapse key is set. The payload is
  `{id, notification_id, topic, title, body, url, tag}`, never more than
  3,000 bytes, with `body` cut at a character boundary to fit. The render
  refuses (`render_failed`) a push whose other fields would not fit with
  the worst-case collapse key (64 bytes JSON-escaped to 384); should a
  payload still not fit with an empty body, `tag` is dropped, and if even
  that does not fit the delivery fails `push_payload_too_large`.
- **Outcomes.** 2xx: `accepted`, with `Location` (at most 256 characters) as
  the message id. 404 or 410: the subscription is deleted and the delivery
  `failed` / `subscription_gone`. 400 (`push_rejected`), 413
  (`push_payload_too_large`) and 3xx (`push_redirected`, redirects are never
  followed) are permanent. 401 or 403: transient `push_auth`, logged as
  `notification.provider_account_error` and counted, permanent from the
  third attempt. 429 waits for `Retry-After`, at most `push.ttl` (and 24 h);
  5xx and network errors retry.
  A crash between the call and its record resends: the payload's `id` lets
  the client deduplicate. Each attempt refreshes the subscription's
  `last_used_at`, and success resets `consecutive_failures`.
- **Egress guard (NT §5.6).** Pushes go through a dedicated client: no
  redirects, no proxy, a 5 s connect timeout, at most 2 idle connections per
  host, and a DNS resolver that drops blocked addresses (the private,
  loopback, link-local, documentation, multicast and reserved ranges of NT
  §5.6, IPv4-mapped, IPv4-translated (`::ffff:0:0:0/96`),
  IPv4-compatible, NAT64 `64:ff9b::/96` and 6to4 addresses by their
  embedded IPv4, the whole local-use NAT64 prefix
  `64:ff9b:1::/48`, and `egress.deny_cidrs`, matched against both the
  address and the IPv4 it embeds). The client dials only the
  addresses the resolver returned, so a name cannot rebind between check
  and connect; IP literals are checked before the request. A refused
  destination fails the delivery `egress_blocked` (permanent), logs
  `notification.egress_blocked` and counts
  `merezha.notification.egress_blocked{notify.target=push}`. With
  `MEREZHA_NOTIFY_NO_EGRESS=1` only loopback addresses are dialled.
- Endpoints, `p256dh`, `auth`, payloads and the VAPID key never appear in
  logs, spans, metrics or `Debug` output.

### Outgoing webhooks

`[modules.notifications.webhooks]` turns signed outgoing webhooks on (NT
§5.5). The `notify.webhook` queue must keep the 24-hour retry step and
three request timeouts per lease (startup refuses otherwise):

```toml
[modules.notifications.webhooks]
# max_endpoints_per_tenant = 20
# max_payload_bytes = 262144
# timeout = "10s"
# connect_timeout = "5s"
# concurrency_per_endpoint = 4
# auto_disable_after = "5d"
# allow_http = false               # development only; refused with http.public
alert_roles = ["owner", "admin"]   # tenant roles told about a disabled endpoint
application_alert_roles = []       # empty: no alert for application-level endpoints
event_types = { "tasks.task_completed" = { description = "A task was completed" } }

[jobs.queues."notify.webhook"]
concurrency = 16
max_attempts = 10
backoff_base_ms = 1000
backoff_max_ms = 86400000
```

Routes under `/v1/notifications/`. Every `tenants/{tenant_id}/webhooks…`
route checks its permission at `Tenant(tenant_id)` scope; the same routes
under `webhooks…` are the application-level registry (NT6), checked at
`Application` scope. An endpoint of another registry answers 404, as a
missing one does; a missing grant is 403 `unauthorized`. Without the section
every route answers 422 `webhooks_disabled`.

| Method and path | Permission | Answer |
| --- | --- | --- |
| `GET webhook-secrets/status` | `notifications.admin` at `Application` | `{current_key_id, previous_key_configured, previous_key, unreadable, previous_key_removable}`, counted from the database (see Secrets below) |
| `GET webhook-event-types` | any authenticated principal | `[{type, description}]`: the application's `event_types`, the content module's `content.entry_published`, `content.entry_unpublished`, `content.entry_deleted`, and `webhook.test` |
| `GET …webhooks` | `notifications.webhooks.read` | `[Endpoint]`, oldest first, never the secret |
| `POST …webhooks` | `notifications.webhooks.manage` | `{url, description?, event_types?}`: 201 `Endpoint + {secret}`. 422 `webhook_url_rejected` (`details: [{path: "/url", code}]`, `code` one of `scheme`, `userinfo`, `fragment`, `blocked_address`, `unresolvable`, `too_long`, `invalid_url`), `unknown_event_type` (`/event_types/<i>`), `validation_failed` (`/description`, `/event_types`); 409 `webhook_endpoint_limit` past `max_endpoints_per_tenant` |
| `GET` / `PATCH` / `DELETE …webhooks/{id}` | read / manage / manage | PATCH `{url?, description?, event_types?, revision}` (`description: null` clears it): 200 `Endpoint`; 412 `revision_conflict` on a stale revision. DELETE 204, the endpoint's deliveries and attempts with it |
| `POST …webhooks/{id}/rotate-secret` | manage | `{keep_previous_hours?}` (0 to 72, default 24; 0 keeps no previous secret): 200 `{secret, previous_expires_at}` |
| `POST …webhooks/{id}/enable`, `…/disable` | manage | 200 `Endpoint`. Disable (`manual`) cancels the queued deliveries; enable clears `failing_since` |
| `POST …webhooks/{id}/test` | manage | 202 `{delivery_id}`: a `webhook.test` event (`data: {endpoint_id}`) for this endpoint alone, whatever its filter; 409 `endpoint_disabled`; 429 `rate_limited` (with `Retry-After: 60`) past 5 tests per endpoint or 30 per registry within a minute, counted in the database for every instance |
| `GET …webhooks/{id}/deliveries?status=&cursor=` | read | `{items: [{id, event_type, status, attempt_count, last_http_status, created_at, attempts: [{attempt, outcome, http_status, duration_ms, started_at}]}], next_cursor}`, newest first, 50 per page; each item shows its latest 20 attempts (`attempt_count` is the total); 400 `invalid_cursor` |
| `POST …webhooks/{id}/deliveries/{delivery_id}/redeliver` | manage | a settled delivery (`succeeded`, `failed`, `cancelled`), or an open one untouched for an hour (its job is dead; a job still alive finds the newer generation and stops), as a new generation: 202 `{generation}`; 409 `not_redeliverable` (open and recently active), `endpoint_disabled`; 410 `event_body_expired` |

`Endpoint` is `{id, url, description, event_types, status, disabled_reason,
failing_since, last_success_at, revision, created_at}`. A filter is an exact
type of the catalog or `prefix.*`; an empty filter matches every type.

- **URLs.** Registration (and PATCH) accepts `https`, or `http` with
  `allow_http`; no userinfo or fragment; at most 2,048 characters; a host
  that is not a blocked IP literal (`127.0.0.1`, `[::ffff:127.0.0.1]` and
  `0x7f000001` are all the same blocked address) and that resolves within
  5 s to at least one address the egress guard allows. That check only
  gives early feedback: the one that counts is the guarded resolver at
  send time. Requests go through the egress guard Web Push uses (see
  [Web Push](#web-push), with `webhooks.connect_timeout`): the client dials
  only the addresses the resolver allowed, IP literals are checked before
  the request, and `MEREZHA_NOTIFY_NO_EGRESS=1` dials loopback only.
- **Secrets.** `whsec_` plus base64 of 32 random bytes, shown only in the
  create and rotate answers. At rest the secret is AES-256-GCM ciphertext
  under `k_wh = HKDF-SHA256(secret_key, "merezha-notify/webhook-secret/v1")`
  with a random nonce and the endpoint id as associated data; opening tries
  `secret_key`, then `secret_key_previous`. To rotate `secret_key`, deploy
  the new key with the old one as `secret_key_previous` and keep it until a
  daily `retention` run has resealed the secrets. A secret neither key
  opens fails its deliveries `secret_unreadable`. Each sealed secret
  records the id of its key (`key_id`, `previous_key_id`: 8 bytes of
  SHA-256 over a label and the key), so `retention` reseals only the rows
  still under another key, 500 per page and 20 pages per run, each run
  picking up what the last one left (trigger the
  `notify.maintenance.retention` schedule through the jobs admin API to
  run it sooner). **Before removing `secret_key_previous`, check
  `GET /v1/notifications/webhook-secrets/status`** (`notifications.admin`
  at `Application`): it counts from the database on every call, whatever
  instance answers, `{current_key_id, previous_key_configured,
  previous_key, unreadable, previous_key_removable}`, and the previous key
  may go only when `previous_key` is 0 (`previous_key_removable`).
  `previous_key` counts every endpoint with a secret still under another
  key, including one found unreadable under another previous key, since
  the keys now configured were not all tried on it. `unreadable` counts
  only endpoints tried with exactly the configured pair: neither key opens
  them, so they do not block removing the previous key, which loses
  nothing of theirs. The gauges
  `merezha.webhook.secrets.previous_key` and
  `merezha.webhook.secrets.unreadable` show the same counts: every
  instance counts at start, every 5 minutes (a task stopped at shutdown)
  and after a `retention` run, and reports nothing before its first
  count. A secret neither key opens is marked
  with the pair of keys it was tried with and its ciphertext, skipped by
  later runs and left out of `previous_key`; `unreadable` counts these.
  Its deliveries fail `secret_unreadable`. To recover, configure the key
  it was sealed under as `secret_key_previous` again, or repair the row's
  ciphertext (either change makes the mark stale, so it is retried), or
  rotate the endpoint's secret (`rotate-secret` with
  `keep_previous_hours = 0`; the receiver takes the new secret), or
  delete the endpoint. A row damaged while its `key_id` is already the
  current key's is never selected or counted: it shows up only as
  deliveries failing `secret_unreadable`, and rotating its secret repairs
  it. Decrypted secrets are
  wiped from memory when dropped. Secrets never appear in logs, spans, metrics, errors,
  `Debug` output or other answers.
- **Fan-out.** `notify.webhook_event` (queue `notify.expand`) stores the
  event once (`UUIDv5(source, source_event_id)`) with its envelope frozen,
  `{"type":…,"timestamp":"<RFC 3339>","data":…}`, and, in the same
  transaction, one delivery (`UUIDv5(event id, endpoint id)`) per matching
  active endpoint of the event's registry with its job record
  (`notify.webhook.deliver`, key `webhook:{delivery_id}:g{generation}`). A
  tenant event reaches only that tenant's endpoints, a tenantless event only
  the application-level ones. A second run of the same job writes nothing.
- **Requests.** `POST <url>` with `content-type: application/json`,
  `user-agent: Merezha-Webhooks/1`, `webhook-id: msg_<delivery id hex>`
  (stable across retries and redeliveries: receivers deduplicate on it),
  `webhook-timestamp` (this attempt's Unix seconds) and `webhook-signature:
  v1,<base64(HMAC-SHA256(secret, "{id}.{timestamp}.{body}"))>` over the
  frozen body (Standard Webhooks). During a rotation window a second `v1,`
  signature made with the previous secret follows, separated by a space.
  Receivers should accept any listed signature and a 5-minute timestamp
  tolerance. Each instance sends at most `concurrency_per_endpoint`
  requests per endpoint at once. A job that finds no free slot never waits:
  it hands the delivery to a new job of the same generation 1 s later,
  doubling per hand-off up to 30 s (±50 % jitter), records no attempt and
  ends, so neither the schedule nor the job's attempts are spent and one
  slow receiver cannot hold the queue's workers. The hand-off's key is
  `webhook:{id}:g{generation}:b{n}`, so one replayed after a lost
  completion starts no second chain. A chain is bounded per step of the
  schedule: after 20 hand-offs, or 5 minutes after its first, a still-busy
  endpoint costs the delivery a `failed` / `endpoint_busy` attempt on the
  schedule below (it does not set `failing_since` and does not count
  toward auto-disable), so the delivery backs off and eventually fails
  like any other; the next step starts a fresh budget. Hand-offs are due on
  PostgreSQL's clock, which schedules jobs. The request deadline is
  `min(timeout, job deadline - 2 s)`.
- **Outcomes.** Any 2xx succeeds and clears `failing_since`. The first
  1 KiB of the response body is kept as the attempt's `response_snippet`
  (lossy UTF-8, control characters removed), then the connection is
  dropped. A 410 fails the delivery `gone` and disables the endpoint
  (`disabled_reason = gone`), cancelling its queued deliveries. Every other
  answer, a 3xx included (redirects are never followed), a timeout, a
  network error or a destination the egress guard refuses (attempt
  `blocked`, `merezha.notification.egress_blocked{notify.target=webhook}`
  and `notification.egress_blocked`), is a failure: it sets
  `failing_since` if unset and retries at +5 s, +5 min, +30 min, +2 h,
  +5 h, +10 h, +14 h, +20 h and +24 h (±10 %, never past 24 h). After the
  tenth failed attempt of a generation the delivery is `failed`
  (`retries_exhausted`). A job's own attempts (`max_attempts`) are spent
  by the schedule and by retryable errors such as a database failure; when
  the last one would retry, the delivery is settled `failed` with that
  error's code instead (an attempt still `started` becomes `ambiguous`), so
  no delivery stays open behind a dead job. Should even that write fail,
  redeliver accepts the open delivery once it has been untouched for an
  hour. A crash between the request and its record resends with the same
  `webhook-id`, and a run that lost its lease records nothing once another
  run took the delivery over.
- **Auto-disable.** `webhook_auto_disable` disables an endpoint whose
  `failing_since` is older than `auto_disable_after` and that failed at
  least 10 attempts since (`disabled_reason = failing`), cancels its queued
  deliveries, and in the same transaction requests
  `notifications.webhook_disabled` (email and inbox, data `{endpoint_id}`,
  dedupe key `whdisable:{endpoint_id}:{unix seconds}`) for the members of
  the endpoint's tenant holding an `alert_roles` role, or the holders of an
  `application_alert_roles` role for an application-level endpoint. An
  empty list sends no alert. Every disable logs
  `notification.webhook_endpoint_disabled` and counts
  `merezha.webhook.endpoints_disabled{notify.reason}`. Cancelled deliveries
  are sent again only through redeliver.
- **Audit.** Create, update, rotate, enable, disable (manual, `gone`,
  `failing`) and delete write a row to `merezha_notify.webhook_audit`
  (endpoint, tenant, action, reason, actor subject, revision, time), never
  the secret or the URL. The block's own disables name `sys_notifications`.
- **Telemetry.** `merezha.webhook.attempts{notify.class}`,
  `merezha.webhook.deliveries{notify.outcome}`,
  `merezha.webhook.endpoints_disabled{notify.reason}` and the gauges
  `merezha.webhook.secrets.previous_key` and
  `merezha.webhook.secrets.unreadable`. The job span carries
  `notify.webhook_delivery_id`, `notify.endpoint_id` and
  `http.response.status_code`; URLs never reach telemetry.
- **Accepted risk: an internal DNS oracle.** A principal allowed to manage
  endpoints learns a little about the deployment's resolver: registration
  answers `blocked_address` for a name that resolves only to blocked
  addresses and `unresolvable` for one that does not resolve, and the
  deliveries view shows an attempt `blocked` (with its duration) when a
  name later resolves only to blocked addresses. Nothing is ever dialled,
  and response bodies of refused destinations do not exist, so this tells
  whether an internal name exists, not what it serves. A deployment that
  must hide its internal names gives the block a resolver without them.

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
of NT §4. The output limits cap the output stream: a render fails
`output_too_large` when what it writes passes the channel's limit, and
`templates.fuel` bounds the instructions it runs. Intermediate values, such
as a string built by an expression, a `{% set %}…{% endset %}` block or a
macro body, are built in memory before anything is written and are bounded
only by fuel and the template's author. Templates are operator-authored
project code; untrusted templates are not supported. In the inbox, `secret` is a guard
holding no data: any use of it, through partials included, refuses the
render with `inbox_references_secret`. Subjects, push and inbox titles and
SMS also refuse U+2028, U+2029 and the bidirectional overrides and isolates
(U+202A to U+202E, U+2066 to U+2069). The filters are `url` (`https:`, or `http:` to loopback with
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
| `sms` | `sender` required unless disabled: at most 11 alphanumerics or 15 digits; `organisation_prefix` at most 32 characters, counted in the segments; `max_segments` 1 to 10 |
| `brevo` | required with a `brevo` driver; `api_key` required then, `webhook_token` (at least 32 characters, `secret_too_short`) with the email driver; `timeout` 1 s to 60 s, `connect_timeout` 100 ms to 30 s |
| `push` | `vapid_private_key` a secret holding a PKCS#8 PEM or base64url of a 32-byte P-256 scalar, parsed at startup (`invalid_vapid_key`), `vapid_subject` `mailto:` or `https:`; `ttl` 0 to 28 d; `allowed_hosts` at most 64 lowercase host names (default: the five services of NT §7.2); `max_subscriptions_per_user` 1 to 100 |
| `egress` | `allow_private_networks` (development and tests; also lets push endpoints use `http` and any port), `deny_cidrs` at most 64 networks, always refused |
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
(`lease_too_short`), a `notify.webhook` `backoff_max_ms` under 24 h or,
with the Brevo email driver, a `notify.email` `backoff_max_ms` under 5 min
(`backoff_max_too_short`), and with `MEREZHA_NOTIFY_NO_EGRESS=1` a
non-loopback `brevo.base_url` (`egress_not_loopback`).

`Module::configure` sees only the module's section, so the binary hands
the block the facts these checks need when it registers it
(`HostFacts::load`: the configuration directory, `http.public`, `app.name`,
`[jobs]` and `MEREZHA_NOTIFY_NO_EGRESS`). They are read from the file,
environment and flag layers with the kernel's precedence, `[jobs]` validated
by the kernel on its own, so these checks run even when other kernel
settings are invalid and every problem is reported in one pass. Each issue
carries the layer that set its key, or `default`.

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
  `commit_outcome_unknown` drop it rather than reuse it. Each detached
  session sets `client_connection_check_interval = 500ms`
  (`CONNECTION_CHECK_INTERVAL`), so the server ends a session whose client
  went away within about half a second even mid-statement: an aborted
  migration run, content activation or background runner stuck in a
  `CREATE INDEX CONCURRENTLY` or a lock wait releases its advisory lock
  then instead of at its statement timeout.
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
  holds one per host, and the notifications block one for its inbox
  stream.

### Statements and telemetry

Every statement runs under a registered name ([telemetry design](../design/telemetry.md)
§1.3, TE-T4). A `Statement` constant carries a `name` (`notify.capture.insert`),
a `summary` (`INSERT merezha_notify.captures`) and its `sql`;
`Statement::new` checks the labels at compile time: names are two or more
dotted `[a-z][a-z0-9_]*` segments, never `adhoc`; summaries are one to three
upper-case words (the operation) and at most one lower-case target (the
collection), or a function written `name()`. Statements run through the pool
handle, on any session (`PooledClient`, `Tx`, `DetachedClient` or the
driver's own client and transaction), with the driver method's contract:

```rust
const INSERT_NOTE: Statement = Statement::new(
    "notes.insert",
    "INSERT notes.notes",
    "INSERT INTO notes.notes (id, body) VALUES ($1, $2)",
);
db.execute(tx, &INSERT_NOTE, &[&id, &body]).await.map_err(|e| classify(&e))?;
```

`query`, `query_one`, `query_opt`, `execute` and `batch` run `statement.sql`;
`batch_text` runs text built at run time (DDL naming a derived index, a
computed `SET`) under a registered name, and `query_text`,
`query_one_text`, `query_opt_text` and `execute_text` a statement whose
text varies per call (a list's filters, sort keys and cursor condition)
with its values as parameters, with the driver's contract of the method
they are named after; `statement.sql` documents the shape and the text is
never recorded. The kernel's own statements are
`merezha_postgres::KERNEL_STATEMENTS` (`kernel.*`); a block lists its own
and checks them against the kernel's with `check_registry` in a test: the
application store's are `merezha_application::postgres::STATEMENTS`
(`app.*`; the append is `app.events.append`, summary
`INSERT merezha_app.events`, run prepared through `query_one_prepared`), the
capture driver's `merezha_notifications::drivers::STATEMENTS`, identity's
`merezha_identity::sql::STATEMENTS`, the jobs runtime's
`merezha_jobs::STATEMENTS` (`jobs.*`), content's `merezha_content::STATEMENTS`
(`content.*`, `merezha content diff`'s included) and its assets'
`merezha_content::media::STATEMENTS`. Each module returns its lists from
`Module::statements()`, and `crates/cli/tests/statement_registry.rs` checks
the lists of every module the binary hosts together, as it runs them: the
blocks `merezha_cli::modules` registers, the jobs runtime and the
application module, so a new block is checked once the binary registers
it. The same test parses each crate's non-test sources and fails for a
statement constant missing from its module's lists (the kernel's from
`KERNEL_STATEMENTS`), so a list a module leaves out is caught too, and for
a crate declaring statements that is no module. SQL sent through the
driver directly is not observed at all, so one test parses every crate's
`src/` with `syn` (`crates/postgres/tests/statement_scan.rs`, rules in its
`scan` module) and reports each call of a driver method (`query`,
`query_one`, `query_opt`, `query_scalar`, `execute`, `execute_typed`,
`batch_execute`, `query_typed`, `simple_query`, `prepare`, `prepare_typed`,
`prepare_cached`, `bind`, `copy_in`, `copy_out` and the rest of
`scan::DRIVER_METHODS`, which a test keeps equal to the locked
`tokio-postgres` and `deadpool-postgres` methods that take a query,
statement or portal): a method call, turbofish included, with the driver's
argument count (so `Application::query(query)` is not one) whose second
argument is not `&STATEMENT`, a constant the sources declare as a
`Statement` and the same file does not declare as another constant or
static (a parameter constant such as `&NONE` is not one); a path call or
method reference on a driver type or trait (`Transaction::execute(tx, ..)`,
`GenericClient::query`, `<T as GenericClient>::query`, a `use .. as` or
`type` alias of one in the same file, a generic parameter bounded by one);
and the same inside macro arguments and `macro_rules!` bodies, where a path
through a metavariable (`$t::execute(..)`, `<$t>::execute(..)`) and a
method named by one (`$client.$m(..)`, `<$t>::$m(..)`) are reported too.
Comments and string contents are ignored. Without types, an alias declared
in another file or a driver value reached through an unbounded generic
stays invisible. Its allowlist names each file still allowed such calls,
with the exact count and the reason, and only shrinks: a new call fails,
and so does an entry whose file has fewer calls than listed. The append
prepares through `Database::prepare_cached(&client, &STATEMENT)`.

- **`db.client.operation.duration`** (seconds) times every registered
  statement with `db.system.name = postgresql`, `db.operation.name` and
  `db.collection.name` from the summary, `merezha.module`, and on failure
  `db.response.status_code` (the SQLSTATE) and `error.type` (the `classify`
  code, or `cancelled` when the caller dropped the statement first).
- **Spans.** Inside a caller's span the span bridge records (a
  `merezha*` span at `info` or above, with a valid OpenTelemetry context), a statement is a CLIENT span named
  after its summary with the same attributes plus `db.query.summary` and
  `merezha.db.statement`; `Database::transaction` and
  `DetachedClient::transaction` open `transaction {module}` with
  `merezha.db.pool.wait` (seconds), the statements (`BEGIN`, the
  `SET LOCAL` bound, the work, `COMMIT`) as children. Without a caller span
  (readiness probes, background polls) nothing opens a span; the metrics
  still count. Spans never carry SQL text, parameters or server messages. A
  span's status is `Error` for transient failures and `database_error`;
  conflicts and constraint violations leave it unset.
- **Module.** `Database::for_module(name)` labels a handle's statements,
  transactions and listeners with `merezha.module = name`; each module's
  `AppContext::database()` is scoped to the module. The host's own handle
  is `kernel`.
- **Pool.** `db.client.connection.count` (`idle`, `used`),
  `db.client.connection.max` and `db.client.connection.pending_requests`
  read the pool's status, without I/O, at each collection;
  `db.client.connection.wait_time` records each successful acquisition;
  `db.client.connection.timeouts` counts acquisitions that gave up
  (`database_busy`), which also log `db.pool_timeout` at most every 10 s.
  The pool name is `merezha`. Detached sessions are the pool
  `merezha.detached` (`count{used}`, `max` = `max_detached`,
  `pending_requests`, `wait_time` and `timeouts` of slot waits), and open
  listener sessions the pool `merezha.listen` (`count{used}` only: they are
  not bounded).
- **Listeners.** A listener session that ended (`recv` returned `None`) or
  failed `check` is lost; the next successful `listen` on the same channels
  counts one `merezha.db.listener.reconnects`. The `db.listener_lost` and
  `db.listener_restored` events stay with the blocks that reconnect.
- **Migrations.** Inside a caller's span (the host's `startup`), an apply
  run opens `migrate {module}` for each module whose migrations it executes,
  with that module's statements as children; a failed migration sets its
  status `Error`. Each successful run sets `merezha.db.migrations.pending`
  per declared module (0 after an apply), so readiness's verify runs keep it
  current.

The instruments come from the meter the pool was built with:
`Database::connect_with_meter(&config, &meter)`; `Database::connect` uses
no-op instruments. `merezha_telemetry::Meters` is the one meter source
of the workspace: `Meters::new(provider)` (enabled), `Meters::noop()` or
`Meters::default()` (records nothing), `enabled()`, and `meter(scope)`.
`Telemetry::meters()` returns the SDK provider's when metrics are
exported and no-op meters otherwise. `HostOptions::meters` builds the edge
instruments from `meter("merezha.edge")` and the kernel pool from
`meter("merezha-postgres")`; `merezha run` passes `Telemetry::meters()`,
and `Host::start` keeps no-op meters.

### Application storage

An application whose `application.toml` says

```toml
[storage]
backend = "postgres"
application = "tasks-v1"   # identity recorded in merezha_app.manifest
```

stores its events, receipts and projections in the schema `merezha_app` on the
project pool ([design](../design/application-platform.md) §A). There is no URL
and no `initialize` key; the migration runner creates the schema under the
ledger key `application`. The first start on the migrated schema records the
identity, and a host naming another identity fails `storage_configuration`.
Several `merezha run` hosts may serve one database at once; there is no owner
lock. `[limits]` total-capacity keys (`max_events`, `max_streams`,
`max_store_bytes`, `max_idempotency`) fail `invalid_limits`, because capacity
is a disk concern here; `max_stream_events` bounds each history.

Every command commits through one autocommit call of `merezha_app.append`,
which locks the single `merezha_app.head` row. Positions have no gaps and
commit in position order across hosts. A stale expected version fails `409
concurrency_conflict`; a lock wait beyond the deadline or
`[application.append] lock_timeout_max_ms` fails `503 storage_busy`; a
connection lost during the append fails `503 commit_outcome_unknown` and
nothing needs a restart: retry with the same idempotency key, which replays the
stored receipt when the first attempt committed. Receipts are rows committed
with their events, so a keyed command replays on any host.

#### Projections in the background

Each host runs a scheduler with one live lane and one rebuild lane per
configured projection revision, at most `[application.background]
max_concurrent` runs at once ([design](../design/application-platform.md)
§A.7). With `catch_up = "auto"` a lane wakes on a commit on this host, on a
`merezha_app_events` notification (the application's own `LISTEN` session,
reconnected with backoff from 100 ms to 10 s, logging `db.listener_lost` and
`db.listener_restored`; a reconnect wakes every live lane) and every
`interval_ms`. A run holds the session advisory lock `pg_try_advisory_lock(1297760592,
hashtext('p:{name}:{revision}'))` (`b:` for a rebuild) on a connection
detached from the pool and named `merezha p:{name}:{revision}` in
`pg_stat_activity`, so one host runs a projection at a time; pooled sessions
never take one. Closing the session releases the lock, whether the run ends,
fails, is cancelled at shutdown or its guest panics. A stuck holder shows in
`pg_locks` with `classid = 1297760592`; end it with `pg_terminate_backend`.
Each batch publishes state and checkpoint with a row-revision
compare-and-swap, so a run whose lock session died cannot overwrite newer
work. A failed run marks the row `failing` with its `failure_code` (log
`projection.failed`), keeps the served pair and retries after 1 s, doubling
to 5 min.

`POST /v1/application/projections/{name}/catch-up` answers `{state,
checkpoint}` as before: it runs to the head it read under the lock, or, when
another host's run holds it, waits for that run to reach the head within the
request deadline (`504 timeout` after it; progress stays). A query may carry
`"min_position": P` (a command receipt's `position`): the host waits, within
the deadline, until the projection reached it, else `503 projection_behind`.
A projection revision new to the database starts at checkpoint 0 and warms up
in the background with `live = false` up to the head at its creation, then
`live = true`; until then its queries fail `503 projection_warming` and, with
`warm_readiness`, the host is not ready (log `projection.warmed`). Old
revisions keep serving the hosts that still run them.

`POST .../rebuild` (body `{}` or `{"resume": true}`) answers `202
{"rebuild":{"name","revision","target"}}` and rebuilds in the background, on
whichever host takes its lock, from the configured initial state up to
`target`, the head at the request. Queries keep serving the old state; the
finished candidate replaces it atomically as the next generation (log
`projection.rebuild_completed`), and live catch-up continues from there. A
second request fails `409 rebuild_in_progress`; a failed batch marks the
rebuild `failed` (log `projection.rebuild_failed`) without touching the
served pair, `{"resume": true}` resumes it from its persisted candidate and
`DELETE .../rebuild` abandons it (`204`). Embedding callers of
`Application::rebuild` get the same rebuild run to its swap within the call.

A `partition = "tenant"` projection (migration 2, `partitions`, of the
`application` ledger) keeps one state per tenant in
`merezha_app.projection_partitions (name, revision, generation, tenant,
state)`; its `projections` row holds the checkpoint, the CAS row revision and
the served generation, and no state. A batch groups its events by their
stream's tenant (`merezha_app.events.tenant`), loads the touched partitions,
calls the guest once per tenant with `project { partition }` and commits CAS
first (`app.projections.commit_partitioned`, then `app.partitions.write`), so
a batch that lost its CAS writes no partition. Events of application streams
only advance the checkpoint. A tenant query reads the row and the tenant's
partition of the served generation in one statement (`app.partitions.read`);
a tenant without one reads the configured initial state. A rebuild writes the
next generation beside the served one, swaps it in with the row, then deletes
older generations in chunks of 1000 (`app.partitions.sweep_old`); before its
first batch it clears partitions an abandoned rebuild left
(`app.partitions.sweep_stale`). Each rebuild request is a new candidate
(`projection_rebuilds.candidate`): a run stops when the request it started on
was abandoned and requested again, so it never builds on the old request's
partitions. Changing `partition` needs a new `revision`
(`storage_configuration`). `POST .../catch-up` on a partitioned projection
answers `{checkpoint, head}`.

| Route under `/v1/application/` | Permission | Answer |
| --- | --- | --- |
| `GET status` | `application.admin.read` | `{"application","head","projections":[{"name","checkpoint","lag_events","lag_seconds","status","failure_code"}],"reactors":[{"name","status","lag_events"}],"instance":{"id","background_runs"},"listener"}`; `listener` is `connected`, `reconnecting` or `off` |
| `GET projections` | `application.admin.read` | `{"head","projections":[{"name","revision","partitioned","checkpoint","lag","warm","status","failure_code","rebuild"?:{"checkpoint","target","status"},"updated_at"}]}` |
| `GET projections/{name}` | `application.admin.read` | one entry plus `"revisions":[{"revision","checkpoint","warm"}]` |
| `POST projections/{name}/catch-up` | `application.projections.catch_up` | `{state, checkpoint}` |
| `POST projections/{name}/rebuild` | `application.projections.rebuild` | `202` as above |
| `DELETE projections/{name}/rebuild` | `application.admin.write` | `204` |
| `DELETE projections/{name}/revisions/{revision}` | `application.admin.write` | `204`, also for a projection no longer configured; `409 revision_active` for the configured revision |

Connections: each run holds one detached session beside the pool, as do at
most `max_concurrent` explicit catch-ups, plus the application's `LISTEN`
session; with the jobs relay's and the job dispatcher's, a host holds three
`LISTEN` sessions. A lane whose lock attempt found another host holding it
waits 1 s (the holder's linger) before its next attempt, so commits on a host
that does not hold the lock open few lock sessions. At
startup the host logs `db.pool_undersized` (warn, `pool_size`,
`recommended`) when `database.pool_size` is below `limits.max_concurrent +
background.max_concurrent + 2 + number of job queues + 2`.

#### Reactors

`[[reactors]]` in `application.toml` ([application: reactors](application.md#reactors-version-2-postgresql),
[design](../design/application-platform.md) §C.5) run on the same scheduler,
one lane per configured reactor, sharing `[application.background]
max_concurrent`. A lane wakes on a commit on this host, on a
`merezha_app_events` notification, every `[application.reactors]
interval_ms` and at its retry time; a run holds
`pg_try_advisory_lock(1297760592, hashtext('r:{name}'))` on a detached session
named `merezha r:{name}`, so one host runs a reactor at a time. Migration
0003 adds `merezha_app.reactors` (checkpoint, row revision, `active`, `paused`
or `halted`, failure bookkeeping, `retry_at`), `reactor_skips` and
`reactor_rejections`. A new reactor's row starts at the head (or 0 with
`start = "beginning"`); a reactor removed from configuration keeps its row.

Each batch's checkpoint transaction runs on the run's session and writes,
in order: every accepted effect through the module's notifier
(`Notifier::request`, `Notifier::emit_webhook`) and outbox (one
`application`/`application.command` job record), bound to the source
`application` and keyed `reaction:{reactor}:{event_id}:{index}`; the
rejected effects; and the new checkpoint, compared with the row revision.
Directory membership checks run before it opens. `application` recipients
are refused on tenant events (`invalid_recipient`): application roles span
every tenant. Without the notifications
block the capture fallback logs `notification.captured` and delivers
nothing. When any reactor allows `command` effects, the module registers
the `application.command` handler on queue `application`
(`[jobs.queues.application]`, defaults with a startup warning when absent):
it runs the command as `sys_reactor_{name}` on the event's tenant with the
reactor's `permissions` only, idempotency key `rx:{reactor}:{position}:{index}`;
conflicts and transient failures retry, permanent ones (`unauthorized`,
`command_not_allowed`, `reactor_not_found` for a reactor no longer
configured) dead-letter. Routes that declare neither `permission` nor
`access` admit reactors only under the loopback allow-all policy.

Logs: `reactor.halted` (error, once per halt), `reactor.skipped` (info),
`reactor.effect_rejected` (warn, with the effect type as `reason` and the
code), `reactor.isolating` (warn: a batch of several events failed
transiently three times in a row and is retried one event at a time from
`position`). Metrics: `merezha.reactor.batch.duration` (`accepted`, `failed`,
`halted`), `merezha.reactor.effects` (`accepted`, `rejected` per effect
type), `merezha.reactor.skipped`, `merezha.reactor.halted`,
`merezha.reactor.lag.events`, `merezha.reactor.lag.age`,
`merezha.reactor.lock.held` and `merezha.reactor.lock.contended`, with
`merezha.reactor.name`. Batches with events to react to open a root `react
{reactor}` span linked to their source events' traces.

| Route under `/v1/application/` | Permission | Answer |
| --- | --- | --- |
| `GET reactors` | `application.admin.read` | `{"head","reactors":[{"name","checkpoint","lag","status","halted_position","failure_code","failures","retry_at","rejected"}]}` for the configured reactors; `lag` is `head - checkpoint`; `rejected` is counted up to 10,000 (the status document and the lag sampler never count) |
| `GET reactors/{name}/rejections?limit=&after=` | `application.admin.read` | `{"rejections":[{"position","index","effect","code","rejected_at"}],"next"?}`, oldest first, `limit` 1 to 200 (default 50), `after` the previous page's `next` (`{position}.{index}`); `400 invalid_input` for another parameter |
| `POST reactors/{name}/pause` and `/resume` | `application.admin.write` | `{"name","status"}`; a paused reactor runs no batch, and a batch that was running when it paused does not commit; resume makes it due now |
| `POST reactors/{name}/retry` | `application.admin.write` | `{"name","status"}`; the next attempt runs now |
| `POST reactors/{name}/skip` | `application.admin.write` | body `{"position":P}` with `P = checkpoint + 1` (else `409 invalid_skip`) or `{"to":"head"}`; a `reactor_skips` row with the principal (`admin` or `skip_to_head`) covering `first_position` to `position`, the reactor active again (a paused one stays paused); `{"name","checkpoint"}` |

These routes work on any reactor row, configured or not, so an operator can
skip a removed reactor to the head before re-adding it; a name without a row
is `404 reactor_not_found`.

`merezha application verify --config merezha.toml` checks, read-only and in
chunks of 10,000 positions inside one repeatable-read snapshot, that positions
are contiguous, the head equals the highest position, stream rows and versions
match their events, every receipt names its events and every checkpoint is at
most the head. It prints one JSON report on stdout,
`{"ok","head","events","streams","receipts","projections","violation_count","violations":[{"check","subject"}]}`
(at most 100 violations listed), and exits 0 when everything holds, 1 on a
violation and 2 when it could not check, printing
`{"ok":false,"error":{"code","classification"}}` (for example `storage_schema`
before the migrations ran). It reads the whole history, so it is not part of
startup; run it after a restore. It stops after 10 minutes.

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

Tests share one server and run in parallel, so none depends on how fast a
statement or a thread is. They force their interleavings with test hooks,
advisory locks, `pg_stat_activity` polls or a sentinel notification (which
arrives after any earlier one), date rows back instead of waiting for a
window to pass, and bound elapsed time only by limits derived from the
configured timeouts. Waits poll and return as soon as their condition holds.
The few tests that measure latency or race a client deadline against the
server's sit in a `timing` module of their test file. A plain
`cargo test --locked --workspace` runs everything; CI runs the suite with
`-- --skip timing::`, then the timing tests alone with
`-- --test-threads=1 timing::`, and both steps gate.

The content suites fit a server with the default `max_connections = 100`
at twelve test threads. A pool keeps every connection it opened until it
drops, so their fixtures keep pools small: an HTTP fixture host has
`pool_size = 4` and waits up to 30 s for a pooled connection, its test
queries through a pool of 3, and the module tests' hosts have
`pool_size = 6`. The rebuild tests, which start up to five hosts each,
hold one of three slots for the whole test.

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
skip. Both jobs split the tests as described under "Test harness": the
parallel run, then the `timing` tests one at a time. GitHub's macOS runners
cannot start service containers. The macOS job runs the same checks, and its
PostgreSQL tests print the skip message.

## Jobs and the outbox

`merezha-jobs` implements AP §B for AP-B1, AP-B2 and AP-B3. Its migration 0001 (ledger key
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
  (`invalid_dedupe_key`). The runtime's `Outbox` port (the one
  `JobsRuntime::ports` hands to contexts) runs the insert as the registered
  `jobs.outbox.append`, with its DB span and duration series; a bare
  `PgOutbox` value, which holds no `Database`, runs the same SQL unobserved.
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
  `not_quarantined` or `not_found`. Its HTTP route is
  `POST /v1/jobs/admin/outbox/{id}/requeue`.
- **Workers** (AP-B2; off with `jobs.workers = false`, an API-only role
  that still enqueues and relays). They start once the first subscription
  sync succeeded. Each queue a registered kind names or `[jobs.queues]`
  configures gets one dispatcher with `concurrency` slots; a registered
  queue missing from `[jobs.queues]` runs on the defaults with 2 slots and
  logs `job.queue_unconfigured`. A dispatcher leases only while it has
  free slots, only kinds this instance registered, `priority DESC, run_at,
  id`, with `FOR UPDATE SKIP LOCKED`; it wakes on `LISTEN merezha_jobs`
  (the payload names the queue), on a freed slot and every
  `jobs.poll_interval_ms`. `global_concurrency > 0` caps running jobs of
  the queue across instances under a transaction advisory lock. Each lease
  gets a fresh `lease_token`, an attempts row and `lease_owner =
  app.instance_id`, and every later write is fenced on the token, so a
  handler whose lease was lost can never settle the job.
- **Retry.** A handler runs in its own task with a `JobContext` (attempt,
  idempotency key, deadline, `cancel`) and the queue's `timeout_ms`.
  Success is `succeeded`; `permanent` is `dead` at once; `transient` and
  `conflict` retry until `max_attempts`, then `dead`; a panic is `dead`
  with `handler_panic`; a payload `serde_json` cannot read back is `dead`
  with `invalid_job_payload`; a timeout fires `cancel`, aborts the handler
  and is a transient `timeout`. The delay is equal jitter over
  `min(backoff_max, backoff_base * 2^(attempt - 1))` from the OS CSPRNG,
  at least the handler's `retry_after` (capped at `backoff_max`). Logs:
  `job.leased`, `job.completed` (debug), `job.retry_scheduled` (warn),
  `job.dead_lettered` (error), never with payloads.
- **Heartbeat and reaper.** Every third of the shortest `lease_ms`, one
  statement extends every lease the instance holds; a token missing from
  the result lost its lease, so its `cancel` fires and its result is
  discarded (`job.lease_lost`). Every `jobs.reaper_interval_ms`, any
  instance reclaims expired leases (100 per statement, `SKIP LOCKED`): the
  attempt counts, the job goes back to `available` after an unjittered
  backoff or `dead` at `max_attempts`, code `lease_expired`.
- **Drain.** `shutdown` stops the dispatchers, reaper and retention, keeps
  heartbeating, waits up to `jobs.drain_timeout_ms`, fires `cancel`, gives
  2 s, then releases what is left in one fenced statement: `available`
  now with the attempt refunded, the attempts row `released`, logged as
  `job.drain_abandoned` with the ids. A handler that fails after the
  cutoff is released, not retried; one that succeeds is settled. A lease
  that completes after the drain began is held before its dispatcher
  exits, never run, and released the same way. `merezha run` sets the
  module shutdown budget to the 10 s default plus `drain_timeout_ms` plus
  3 s, so the modules after the jobs runtime keep their 10 s.
- **Give back.** A handler that cannot do the work on this instance returns
  `JobFailure::release()` (reserved code `job_released`): the worker
  releases that lease at once with the drain's fenced statement
  (`available` now, attempt refunded, attempts row `released`) and logs
  `job.released`. `JobHandler::accepts_leases()` (default `true`) is read
  before every lease; the worker leases only kinds whose handler accepts.
- **Cron** (AP-B3, AP §B.7). Schedules come from `[[jobs.schedules]]`
  (source `config`) and from modules through `JobsHandle::schedule(
  ScheduleDeclaration { name, cron, timezone, queue, kind, payload })`,
  called from `Module::start` for a kind the module registered
  (`kind_not_registered`), with a name no other schedule of the instance
  uses (`schedule_already_declared`), at most 256 per instance
  (`too_many_schedules`), validated like config (`invalid_schedule_name`,
  `invalid_cron`, `invalid_timezone`, `unknown_timezone`) and refused
  after start (`jobs_started`). Module name `config` is reserved for the
  configured schedules' source. Schedules follow the subscriptions'
  rolling-deploy rule: at start and every `jobs.subscriptions.heartbeat`,
  each instance upserts the schedules it declares (`last_declared_at`,
  reactivating a retired one) and retires those nobody declared for
  `jobs.subscriptions.retire_after` (`retired_at` set, the row kept, never
  fired), all in one transaction under
  `pg_advisory_xact_lock(MZJB, hashtext('schedules'))`. A schedule only a
  newer version declares therefore survives older instances restarting.
  When two versions declare one name with different definitions
  (expression, zone, queue, kind or payload, digested in `definition`),
  the other definition replaces the stored one only if its `app.version`
  is strictly greater (dotted numbers; otherwise no precedence), if
  nobody re-declared the stored definition for `2 x heartbeat`, or if
  the row is retired; otherwise the stored one stands and the instance
  logs `job.schedule_conflict` once per streak. A replacement keeps a due
  run (it fires once, with the new definition) and otherwise recomputes
  `next_run_at` from now; a reactivation recomputes it from now.
  `merezha run` passes `app.version`. Every instance ticks every `jobs.scheduler.interval_ms` and
  once at start, `workers = false` included: under
  `pg_try_advisory_xact_lock(MZJB, hashtext('scheduler'))` (taken: skip
  this tick), it locks up to 32 due rows at a time with
  `FOR UPDATE SKIP LOCKED` and, per row, inserts one job keyed
  `cron:{name}:{epoch seconds of the occurrence}` (`ON CONFLICT DO
  NOTHING`, `schedule_name`, `scheduled_for`, the queue's
  `max_attempts`, correlation id = the key) and moves `next_run_at` to the
  first occurrence strictly after `max(occurrence, now)`: a downtime
  spanning several occurrences runs one job, then the normal cadence.
  Times come from the database clock. A due row this version cannot
  parse stays due for an instance that can, is kept out of the tick's
  later batches and is logged once per streak. Daylight saving time follows
  croner (OCPS): in a spring-forward gap a fixed-time schedule (`30 2 * *
  *`) runs at the first minute after the gap and an interval schedule
  (`*/30 * * * *`) skips the missing occurrences; in a fall-back overlap
  a fixed-time schedule runs once, in the first pass, and an interval
  schedule runs in both. `PgJobs::schedule_now` runs one tick. A failing
  tick logs `job.worker_failed` with task `scheduler`, a failing schedule
  declaration `job.schedule_sync_failed`, and each fired occurrence
  `job.schedule_fired` (debug). Readiness waits for the first schedule
  sync too.
- **Secrets (R3).** A flagged job's payload is nulled by the same
  statement that makes it terminal: success, dead letter (handler or
  reaper), cancel. The admin API never returns a flagged payload.
- **Retention.** At start and hourly, any instance purges in batches of
  1000 under `pg_try_advisory_xact_lock(MZJB, hashtext('retention'))`:
  succeeded and cancelled jobs after `jobs.retention.succeeded` (never a
  job with an open reissue claim), dead jobs
  after `jobs.retention.dead` (with an `admin_actions` row
  `retention_purge`), relayed outbox rows after `jobs.retention.outbox`,
  quarantined ones after `jobs.retention.dead`, admin actions after
  `jobs.retention.audit`. `PgJobs::purge_expired` runs one pass.
- **Readiness.** Ready once the subscriptions synced and the workers
  started. With `jobs.readiness.max_lag_ms`, the reaper loop also measures
  the age of the oldest due `available` job (one row through `jobs_due`)
  and readiness fails while it is
  older; unset, backlog is no readiness input (R10).
- **Admin API** under `/v1/jobs/admin/` (AP §D.1), each route checked at
  `ResourceScope::Application` through the host's policy before anything
  else: `jobs.admin.read` for reads, `jobs.admin.write` for writes, and
  `jobs.admin.payload` as well for `?payload=true`. Without the identity
  block, a loopback listener allows them and a public one answers 401.
  `GET queues` and `GET outbox` each serve their half of
  `JobsHandle::queue_status()`, every count through a partial index;
  `GET jobs?queue=&state=&kind=&limit=&after=` lists by id (limit 1 to
  200, default 50, `next` cursor), without payloads; `GET jobs/{id}` adds
  `attempt_history`; `POST jobs/{id}/replay`, `POST dead/replay` (at most
  1000), `POST dead/purge`, `POST jobs/{id}/cancel` and `POST
  outbox/{id}/requeue` write an `admin_actions` row with the principal and
  log `job.admin_action`. Replaying a flagged dead job calls its kind's
  `reissue` hook (`"state":"reissued"`) or fails `409 secret_purged`.
  Reissues go one job at a time: a claim transaction closes the dead row
  as `cancelled` with `replays + 1` and `reissue_claimed_at`, and writes
  the audit row; the hook runs within 10 s; success clears the claim, and
  a failure or timeout returns the job to `dead` (the hook's code for a
  single replay, `refused` in a bulk count). The reaper returns a claim
  older than 60 s (a crash between claim and hook) to `dead`. Both settles
  are fenced on the claim's own `reissue_claimed_at`: a hook that returns
  after its claim was reverted (and maybe claimed again) writes nothing,
  logs `job.reissue_claim_lost` and fails `409 reissue_claim_lost`. Replays run
  in a task detached from the request, so a disconnect or the HTTP drain
  cannot cut one; `POST dead/replay` answers `200` with
  `"complete":true`, or `202` with the counts so far and
  `"complete":false` when the run outlasts 30 s. `GET
  schedules?limit=&after=` lists schedules by name (limit 1 to 200,
  default 50, `next` cursor) with `name`, `source`, `cron`, `timezone`,
  `queue`, `kind`, `enabled`, `retired_at`, `next_run_at`, `last_run_at`
  and `last_job_id`, never the payload. `POST schedules/{name}/pause`
  sets `enabled = false` (a paused schedule never fires), `/resume`
  re-enables a paused one with `next_run_at` recomputed from now (the
  occurrences it missed never run), and `/trigger` enqueues one job now
  keyed `cron:{name}:manual:{key}`, with `key` from an optional
  `Idempotency-Key` header (1 to 120 bytes of `[A-Za-z0-9._:-]`, else
  `422`; a retry returns the same job with `"inserted":false`) or the
  time in milliseconds, without moving the cadence
  (`{"name","job_id","idempotency_key","inserted"}`; `409
  schedule_retired` for a retired schedule); pause and resume answer the
  schedule's listing entry, an unknown name or action is `404
  not_found` (after the permission check), and each writes an `admin_actions` row (`schedule_pause`,
  `schedule_resume`, `schedule_trigger`). Bad bodies and query
  strings fail `invalid_input` (400) or `validation_failed` (422) with
  `details` that never echo input.

Error codes it registers: `invalid_topic`, `invalid_queue`,
`invalid_run_at`, `invalid_event_envelope`, `invalid_record_data`, `job_payload_limit`,
`kind_already_registered`, `kind_not_registered`, `too_many_kinds` and
`jobs_started`, `invalid_schedule_name`, `schedule_already_declared` and
`too_many_schedules` (422), `not_quarantined`, `not_dead`, `not_cancellable`,
`reissue_claim_lost` and `schedule_retired` (409), `jobs_not_ready` (503); the kernel registers
`invalid_cron`, `invalid_timezone` and `unknown_timezone` (422), which
`merezha_platform::cron` returns. Migration 0002 adds the `outbox_quarantined`
index the admin listing, the outbox count and retention read, the
`jobs_due` index the readiness lag reads, and the `reissue_claimed_at`
claim column (only on `cancelled` rows) with its index. Migration 0003
adds `schedules.last_declared_at`, `retired_at`, `declared_version` and
`definition_declared_at` and the `schedules_due` index the tick reads.
Migration 0004 (TE-T5) adds `jobs.replay_trace_parent` and
`replay_trace_state`, the trace a replayed job died in.

**Telemetry** (TE §1.2, §1.3, §2.3; TE-T5). Spans and stored trace context:

- `PgOutbox::append` opens one `send {queue}` (job record) or
  `send {topic}` (event record) PRODUCER span per inserted record, a child
  of the caller's span, and stores its context in the row's `trace_parent`
  and `trace_state`, sampled or not, so the sampling decision travels. The
  contexts are written by a second statement in the same transaction, run
  only when a recording layer gives them; duplicates get no span.
  `JobsHandle::enqueue` does the same for a new job, not for an existing
  one. A `tracestate` over 512 bytes or 32 members is dropped (the parent
  is kept) rather than failing the write. The relay copies a record's
  context into every job it fans out to.
- Each non-empty relay batch runs in a root `relay outbox` span; an empty
  pass opens none (the batch is locked by a first statement, so an idle
  wake-up costs one statement as before).
- Each attempt runs in `process {queue}`, a CONSUMER child of the stored
  context, so retries continue the producer's trace as sibling spans (R10).
  It carries `job.id`, `job.queue`, `job.kind`, `job.attempt`,
  `job.max_attempts`, `messaging.message.id` and, at the end, `job.outcome`
  (`succeeded`, `retry`, `dead_lettered`, `lease_lost`, `released`),
  `job.code` and `job.classification`. Its status is `Error` for transient
  failures, timeouts, panics and lost leases only; `job.dead_lettered` is
  logged outside the span, since an error event inside it would mark it
  `Error` for a domain rejection. The handler runs inside
  it, so what it issues joins the trace. A job without a stored context
  (cron, rows written before TE-T5) starts a root.
- A dead-letter replay (`PgJobs::replay`, `replay_dead`, the admin routes)
  runs in `admin job.replay`, a child of the caller's span. It moves the
  job's context to `replay_trace_parent`/`replay_trace_state` and stores its
  own: the next attempts are its children and link to the trace the job
  died in (`merezha.link.reason = replay`). A reissued (flagged) job's hook
  runs in a replay span that links to that trace too; a bulk replay opens
  one child `admin job.replay` per reissue.

Instruments, built from `names.rs` on the meter `PgJobs::with_telemetry`
receives (`merezha run` passes its `Meters`; the scope is
`merezha_jobs::SCOPE`, `merezha.jobs`, and with metrics not exported, when
`Meters::enabled()` is false, the instruments stay no-ops):

- `merezha.job.enqueued` counts every new job: enqueues, relay fan-out,
  fired occurrences and manual triggers. `merezha.job.schedule.fired`
  counts fired occurrences (not manual triggers, not duplicates).
- `merezha.job.duration` has one point per attempt and outcome
  (classification on failures), `merezha.job.queue_latency` one per lease
  (lease start minus `run_at`), `merezha.job.attempts` one per terminal
  outcome (`succeeded`, `dead_lettered`). `merezha.job.dead_lettered`
  counts each dead job once, on the handler path after its fenced settle
  and on the reaper path. `merezha.job.lease_expired`, `replayed`
  (replays and reissues) and `cancelled` count per queue.
- `merezha.outbox.appended` counts inserted records per module and record
  kind at append time (a caller rolling back later still counts);
  `merezha.outbox.relayed` (every record marked relayed, unrouted events
  included), `merezha.outbox.unrouted` and `merezha.outbox.quarantined`
  (with the quarantine code as `error.type`) count committed relay work.
- The gauges `merezha.job.count` (per queue and `ready`, `delayed`,
  `leased`, `dead`), `merezha.job.lag`, `merezha.outbox.pending` and
  `merezha.outbox.lag` report a sample taken every
  `telemetry.metrics.sample_interval_ms` by every instance (the same
  database-level numbers everywhere): the queue status statements of
  `GET queues` and `GET outbox`, in one transaction with a 1 s statement
  timeout. A failing sample keeps the last one and logs
  `job.worker_failed` (`task = sampler`) once per streak; after 3
  consecutive failures (`merezha_jobs::STALE_SAMPLES`) the gauges report
  nothing until a sample succeeds, so dashboards show a gap rather than
  stale values. The sampler runs only when metrics are exported.

Process tests hold the relay with `MEREZHA_JOBS_TEST_RELAY=off` (or
`interval`, no `LISTEN`), and `MEREZHA_JOBS_TEST_HANDLER=<ms>` registers a
`test.sleep` kind on queue `default` whose handler sleeps that long,
ignoring `cancel`. Only builds with the `test-hooks` feature read
them; the feature also adds `PgJobs::without_heartbeat`,
`PgJobs::with_reissue_timing` (shorter hook deadline, claim timeout and
`202` wait), `PgJobs::with_lease_deadline` (a lease transaction that may
wait longer than 5 s on the queue lock) and `PgJobs::workers_stopping`
(whether the drain began), and
`merezha-jobs`' own tests enable it through a self dev-dependency. `merezha-cli`'s dev-dependency enables it, and Cargo unifies that into
every build that compiles the dev-dependencies: the `merezha` binary built
by `cargo test` or `--all-targets` reads the variable. Builds without them,
such as `cargo build --release -p merezha-cli` or `cargo build
--workspace`, ignore it.

## Content

`merezha-content` (`crates/content`, CT-C1 to CT-C5, CT-C7 and CT-C9) is the content block,
a `Module` named `content` with schema `merezha_content` (ledger key
`content`). C2 ships its schema, model activation and readiness; C3 the
management routes for drafts and revisions; C4 publishing, the
`published` rows, deletion, the trash and its purge job; C5 the delivery
API, preview, population and the model endpoints; C7 assets and media
([Content assets](#content-assets)); C9 organization types
([Content organizations](#content-organizations)); issue #46 asset
metadata in delivery and `delivery.public_base_url`. The design is
[content.md](../design/content.md).

```toml
[modules.content]          # the section's presence enables the block
model = "content.toml"     # the default
limits = { max_request_bytes = 1048576, max_entry_bytes = 262144,
           default_page_size = 25, max_page_size = 100, max_filters = 10,
           max_sort_keys = 2, max_batch = 100, query_timeout = "5s",
           max_populate_depth = 2, max_populated_entries = 500,
           max_response_bytes = 16777216 }   # the defaults
revisions = { retain = 100 }                           # the default
trash = { retention = "30d", purge_cron = "23 3 * * *" }   # the defaults
delivery = { cache_control_public = "public, max-age=60, stale-while-revalidate=300",
             cache_control_private = "private, no-cache" }      # the defaults
# delivery.public_base_url = "https://cms.example.com"  # unset by default: paths
```

| Key | Default | Range | Meaning |
| --- | --- | --- | --- |
| `limits.max_request_bytes` | 1048576 | 1024 to 8388608 | Largest management request body (`body_limit`, 413). The edge's `http.body_limit` applies first. |
| `limits.max_entry_bytes` | 262144 | 1024 to 1048576 | Largest encoded fields of one variant, before and after normalization (`body_limit`). |
| `limits.default_page_size` | 25 | 1 to `max_page_size` | `limit` of a list that names none. |
| `limits.max_page_size` | 100 | 1 to 500 | Largest `limit`. |
| `limits.max_filters` | 10 | 1 to 32 | Filter clauses per list. |
| `limits.max_sort_keys` | 2 | 1 to 4 | `sort` keys per list. |
| `limits.max_batch` | 100 | 1 to 500 | Items of one `POST /batch/publish` or `/batch/unpublish` (`validation_failed`, detail `/items` `max_items`). |
| `limits.query_timeout` | `"5s"` | 100 ms to 30 s | Deadline of each request's transaction, which sets `statement_timeout` (capped by `database.statement_timeout_ms`); an expired one answers `timeout` (504). |
| `revisions.retain` | 100 | 2 to 10000 | Revisions kept per variant; the published one is kept besides them. |
| `trash.retention` | `"30d"` | 1 day to 3650 days | How long a trashed entry stays before the purge job hard-deletes it. |
| `trash.purge_cron` | `"23 3 * * *"` | AP's cron parser, five fields | When `content.trash_purge` runs, in UTC (`invalid_cron`). |
| `limits.max_populate_depth` | 2 | 0 to 3 | Segments of one `populate` path (`invalid_query`, detail `/populate` `too_deep`); 0 turns population off. |
| `limits.max_populated_entries` | 500 | 1 to 2000 | Populated references in one response; past it the request fails `populate_limit` (400) rather than truncating. |
| `limits.max_response_bytes` | 16777216 | 65536 to 268435456 | Largest delivery body, and largest populated management read; past it the request fails `response_too_large` (400): ask for a smaller `limit`, fewer `fields` or less `populate`. |
| `delivery.cache_control_public` | `"public, max-age=60, stale-while-revalidate=300"` | visible ASCII, 1 to 256 bytes, no edge spaces | `Cache-Control` of a published read of an `access = "public"` type without a principal (`invalid_value`). |
| `delivery.cache_control_private` | `"private, no-cache"` | as above | `Cache-Control` of every other published read. |
| `delivery.public_base_url` | unset | an absolute `https` URL with a host (an IP address, or a DNS name of letter, digit and inner-hyphen labels: punycode for international names) and an optional path; visible ASCII, never trimmed, no whitespace or backslash; port 1 to 65535; no userinfo, query or fragment; at most 2048 bytes (`invalid_url`); `http` only to `localhost`, `127.0.0.0/8` or `::1` on a private listener (`insecure_origin`) | The public origin (and prefix) of the host. Expanded asset `url`s in delivery, the management asset JSON `url` and the `Location` of created entries and uploaded assets are absolute under it, normalized without a trailing slash; unset, they are paths. Its value enters published ETags. |

The tables take only these keys; `media` is C7's ([Content
assets](#content-assets)). Problems are reported with the
configuration, every one at once (`out_of_range`, `expected_integer`,
`expected_duration`, `expected_string`, `invalid_cron`, `invalid_value`,
`invalid_url`, `insecure_origin`, `expected_table`, `unknown_key`), keyed `modules.content.limits.<key>`,
`modules.content.revisions.retain`, `modules.content.trash.<key>` or
`modules.content.delivery.<key>`.

`model` follows the runtime manifest's path rules: relative to
`merezha.toml`, no `..` or absolute paths, and it must resolve (symlinks
included) to a regular file inside that directory (`invalid_path`; a
missing or unreadable file is `content_model_unreadable`). The file is
read and validated with the configuration, so `merezha run` reports every
model problem in its `invalid_config` issues, each keyed
`modules.content.model:<toml path>` with code `content_model_invalid`;
`merezha content diff` prints the same problems with their messages. Any
other key is `unknown_key` until the slice that gives it behaviour.

At start, after migrations:

- With `database.migrate = true`, one host at a time activates the model
  under the session advisory lock `0x6d7a636f6e74656e` (`mzconten`) on a
  connection detached from the pool, polled like the migration lock. It
  compares the model with the active row of `merezha_content.models`,
  builds the derived indexes (`content_ix_<16 hex>`) the model needs with
  `CREATE INDEX CONCURRENTLY IF NOT EXISTS`, outside any transaction,
  dropping an invalid one of the same name first, then in one transaction
  supersedes the active row, inserts the model as active, syncs `locales`,
  bumps every scope's generation and appends `content.model_activated` to
  the outbox. It then drops the derived indexes the model no longer needs.
  A transient build failure (lock or statement timeout, lost connection) is
  retried twice, after 1 s and 2 s; a third failure or a unique violation
  stops startup (`content.index_failed`, `index_build_failed`) with the
  previous model still active. The whole run is bounded by 300 s
  (`content_activation_timeout`). A current model gets the index
  reconciliation only, which repairs a crashed run.
- With `database.migrate = false` the host changes nothing. Readiness stays
  false with `content_model_pending` while the model is newer than the
  active one (or the schema is missing), `index_build_pending` while a
  derived index is missing or invalid, until another host completes it.
- In both modes, refused changes stop startup with `content_model_breaking`
  (each logged as `content.model_breaking` with its path and change code),
  and a changed fingerprint under the same version with
  `content_model_version_unchanged`. A host whose model is older than the
  active one starts but reports `content_model_outdated`, as does a host
  whose model another host superseded; such a host drains.

Every content write transaction starts with
`ContentState::check_writable`, a `FOR SHARE` lock on the loaded model's
active row. Without that row the active version decides: a newer one fails
the write `content_model_superseded` and turns the host outdated; an older
one or none (the loaded model is still pending, as on a new host before a
migrating one activated it) fails `content_model_pending`, and the host
stays as it is. Both are 503 with `Retry-After: 1`.

When an activation adds a locale or changes a fallback, or finds an older
rebuild still pending, its transaction marks any pending rebuild
`superseded`, records one for its version in `merezha_content.rebuilds`
(migration 3) and appends the job `content.published_rebuild` (queue
`content`, key `rebuild:<version>`). Readiness does not wait for it: any
current host's jobs runtime runs it in the background, re-materializing
the `published` rows of every live entry with a published variant, 500
entries per transaction in id order, each with the model version check,
the rebuild row locked `FOR UPDATE` (two runs never repeat a batch), the
entries locked `FOR SHARE` (saves are not blocked), a generation bump for
the scopes it touched and the resume cursor (`content.rebuild_batch`,
then `content.rebuild_completed`). The job's payload names its version;
a superseded or finished rebuild's job succeeds without work. Each run
does at least one batch; one that nears its job deadline or is cancelled
appends a continuation job (`rebuild:<version>:after:<job id>`, a
duplicate from a retried run ignored). A host that dies mid-batch leaves
the cursor at the last commit and the retried job resumes there. A
module start that did not just enqueue it enqueues a pending rebuild of
its model again (`rebuild:<version>:resume:<cursor>`, a key the runtime
already holds ignored), so a dead-lettered rebuild is retried by the next
deploy. A run alongside a live one waits on the cursor row and never
repeats a batch; content never reads the jobs runtime's tables. Until it is done, delivery may serve the
previous materialization for the affected locales, as in a rolling
deploy. `merezha content diff` prints `content_rebuild_pending version <n>
after <entry id or start>` while one is pending. A first activation has
nothing to rebuild. Fallback rows emit no events.

After the activation commits, the index drop pass (each drop bounded by
30 s) and the tightening count (30 s, on a pooled session after the lock is
released) are best effort: a failure logs `content.index_failed`
(`operation = "drop"`) or `content.model_tightened` with its `code`, and the
next start retries the drops.

Operations notes:

- Modules start before the listener binds, so a migrating host with a
  pending model is not listening (and answers no health probe) while it
  builds indexes: up to 310 s (the 300 s activation bound plus 10 s).
  Startup probes should allow for it.
- The activation transaction's `FOR UPDATE` on the active row waits for
  every content write holding `FOR SHARE`, bounded by
  `database.statement_timeout`. A steady stream of overlapping writes can
  starve it; activation then fails (`database_timeout`) and the next start
  retries. Deploy model changes at a quieter moment if writes never pause.

`merezha content diff --config merezha.toml [--set SECTION.KEY=VALUE]...`
prints, without changing anything, the file's model, the active one, the
status (`fresh`, `current`, `pending`, `content_model_outdated`,
`content_model_version_unchanged` or `content_model_breaking`), one
`<class> <path> <change>` line per classified change and the derived
indexes it would build or drop. It exits 0 when the model is current or
would activate, 1 when it would not, and 2 when the configuration, the
model or the database cannot be read (`content.diff_failed` at level
`error` on stderr with the code, and the issues for `invalid_config`; an
invalid model's problems are printed on stdout with their messages, one
`content_model_invalid: <toml path> <message>` line each).

Error codes it registers: `content_model_invalid`,
`content_model_version_unchanged`, `content_model_breaking`,
`content_not_configured`, `index_build_failed`, `validation_failed` (422),
`invalid_json` (400), `content_type_not_found` and
`organization_not_found` (404, issue #56), and
`content_model_superseded`, `content_model_outdated`,
`content_model_pending`, `content_model_conflict`,
`content_activation_timeout`, `index_build_pending` (503).

Once a host is outdated its content job kinds stop leasing: content
registers its handlers through `ContentState::guard_jobs`, whose
`JobHandler::accepts_leases` turns false, and the jobs worker leases only
kinds whose handler accepts (a kind that refuses stays `available` for the
other instances). A job leased in the instant before, or one whose
handler meets `content_model_superseded` mid-run, is given back with
`JobFailure::release()`: available again at once with its attempt
refunded, for a current host to run.

Content's job kinds (C4): `content.published_rebuild` (above, enqueued by
activation) and `content.trash_purge`, both on the `content` queue
(`[jobs.queues.content]` tunes it), declared at start as the cron
schedule `content.trash_purge` through `JobsHandle::schedule` with
`trash.purge_cron` in `UTC`. Each run hard-deletes entries trashed longer
than `trash.retention`, 100 per transaction (`FOR UPDATE SKIP LOCKED`,
cascading to variants, revisions, `published` rows and refs), appends one
`content.entry_purged` per entry, and continues until none is left, the
job is cancelled or its deadline is one `limits.query_timeout` away (the
next run continues). The admin API's schedule routes can pause, resume or
trigger it.

### Content management routes

All under `/v1/content/manage`, answered `private, no-store`:

| Route | Permission |
| --- | --- |
| `GET /entries/{type}` (list: `locale`, `status`, `filter[<field>][<op>]`, `sort`, `limit`, `cursor`, `fields`) | `content.entries.read` |
| `POST /entries/{type}` (`{id?, locale?, fields}`, 201 with `ETag` and `Location`) | `content.entries.write` |
| `GET /entries/{type}/{id}`, `GET /entries/{type}/{id}/referrers` (`stage`, `limit`, `cursor`) | `content.entries.read` |
| `GET /entries/{type}/{id}/{locale}` (`fields`, and `populate` since C5), `.../revisions` (`limit`, `cursor`), `.../revisions/{n}` | `content.entries.read` |
| `PUT` and `PATCH /entries/{type}/{id}/{locale}`, `POST .../revisions/{n}/restore` | `content.entries.write` |
| the same under `/singletons/{type}/{locale}` (no entry id; the first `PUT` with `If-None-Match: *` creates the entry) | as above |
| `POST /entries/{type}/{id}/{locale}/publish` (`{revision?}`, `If-Match`), `.../unpublish` (`If-Match`), and the same under `/singletons/{type}/{locale}` | `content.entries.publish` |
| `POST /batch/publish` (`{items: [{type, id, locale, revision}]}`), `POST /batch/unpublish` (`{items: [{type, id, locale}]}`) | `content.entries.publish` on every item |
| `DELETE /entries/{type}/{id}/{locale}` (`If-Match`, a non-default variant, 204) | `content.entries.write` |
| `DELETE /entries/{type}/{id}` (`if_unreferenced`, `purge`, 204), `POST /entries/{type}/{id}/undelete` (200 entry summary) | `content.entries.delete`, plus `content.entries.purge` with `purge=true` |
| `GET /trash` (`type`, `limit`, `cursor`) | `content.entries.read`: the given type, or every type the caller reads |
| `GET /model` (C5: the full model, `{version, fingerprint, locales, components, types}`) | `content.model.read`, in the principal's tenant when it has one, else at `Application` |

Each route checks its permission through the host's `Authorizer` before
anything else, at `ResourceScope::Resource { tenant, kind: "content_type",
id: <type> }` (an `Application` grant satisfies it), or at `Application`
for a type the model does not have, so an unauthorized caller cannot tell
types apart. Without the identity block a private listener allows
everything and a public one denies everything: `401 unauthenticated`
without a principal, `403 unauthorized` with one.

`PUT`, `PATCH` and restore need `If-Match: "r<n>"` (missing:
`precondition_required`, 428); any other value, or a revision that is no
longer the draft's, is `revision_conflict` (412) and writes nothing.
`If-None-Match: *` creates a variant (`variant_exists`, 412, when it
exists). `PATCH` takes `application/merge-patch+json` only
(`unsupported_media_type`, 415). An id of another type or scope, or one
that is not a UUID, is `entry_not_found` (404) on every route.

Publishing (C4) runs in one transaction: the model version check, the
entry locked `FOR UPDATE` and the variant under the route's scope
predicates, the draft revision compared with `If-Match`, publish-time
validation of the published revision (`required`, every rule, the
references, and `require_published`: a target with no `published` row in
the referrer's locale, `reference_unpublished`), then the variant's
`published` refs, the entry's `published` rows for every configured locale
(the first published variant of the locale's fallback chain, with the
default variant's published shared fields), a bump of the scope's
generation and `content.entry_published`. `{revision: n}` publishes an
older revision without touching the draft. A batch is all or none; a
failing item names itself in `details` (`/items/<n>...`), and its
`require_published` checks run after every item is materialized. Unpublish
clears the pointer, refs and rows (locales fall back further) and appends
`content.entry_unpublished` (reason `unpublish`); an unpublished variant is
left as it is. A unique value already published by another entry fails the
publish with a `unique` detail on its field.

Deleting an entry sets `deleted_at`, marks its variants `deleted` with
their published pointers cleared (freeing their unique values), removes
its `published` rows and refs, bumps the generation and appends one
`content.entry_unpublished` (reason `delete`) per published locale and one
`content.entry_deleted`. `if_unreferenced=true` fails `entry_referenced`
(409) while a ref in any scope points at it. Undelete returns the drafts
only, records their refs again and appends `content.entry_undeleted`; a
unique value or singleton slot taken meanwhile fails it (`unique` detail,
`singleton_exists`). `purge=true` hard-deletes at once, trashed or not,
and appends `content.entry_purged`. The variant delete removes a
non-default variant with its revisions and refs (`default_variant_required`,
409, for the default one); a published one falls back and appends
`content.entry_unpublished` with reason `delete`.

Codes added by C4: `entry_referenced`, `default_variant_required` (409).

Concurrency: a save, publish or restore locks the entries its references
resolve to `FOR KEY SHARE` until it commits, and `require_published`
share-locks the target too. A delete, purge, publish or unpublish of a
target (`FOR UPDATE`) therefore waits for such a save and then counts its
refs (`if_unreferenced`) or removes them with the entry (`purge`); a save
meeting a delete in flight waits and then reports `reference_not_found`;
a referrer's publish meeting its target's unpublish in flight waits and
then reports `reference_unpublished`. Two publishes that each require the
other can deadlock; PostgreSQL aborts one, answered `database_conflict`
(409, conflict, retryable). A purge also deletes the refs pointing at the
entry, in every scope. A batch's transaction gets one
`limits.query_timeout` per 50 items started (a full 500-item batch, ten);
each statement stays bounded by `database.statement_timeout_ms`.

Codes added by C3: `entry_not_found`, `revision_not_found` (404),
`locale_unknown`, `invalid_query`, `invalid_filter`, `invalid_sort`,
`invalid_cursor` (400), `variant_exists` (412), `precondition_required`
(428), `entry_exists`, `singleton_exists` (409). Every save appends
`content.entry_draft_saved` to the outbox in its transaction.

A cursor carries its sort values in the exact form the list selects
(timestamps `YYYY-MM-DDTHH:MM:SS.ffffffZ`, PostgreSQL's float8 and
numeric text); anything else, and any value PostgreSQL then refuses
(SQLSTATE class 22), is `invalid_cursor`. Migration 2 of
`merezha_content` is a declared non-transactional step building
`variants_by_update` (`draft_updated_at DESC, entry_id DESC`), the index
of the default `-sys.updated_at` list and its reverse.

`populate` on a management variant read (C5) expands the drafts of the
referenced entries, through the variant's locale chain, for the types the
caller holds `content.entries.read` on (each in its resource scope); any
other reference on a populated path is `{"id", "type", "missing": true}`.

### Content delivery routes

C5 serves published entries under `/v1/content` and organization types
under `/v1/content/o/{organization}` (organization types load beside
identity, [Content organizations](#content-organizations); an application
type under an organization's path, and the reverse, is
`content_type_not_found`):

| Route | Query |
| --- | --- |
| `GET /model` | none: the model restricted to the types the caller reads, reference targets (`to`, `entry_link_to`) among them, and only the components they embed |
| `GET /{type}` (collection) | `locale`, `fallback`, `filter[<field>][<op>]`, `sort`, `limit`, `cursor`, `fields`, `populate`, `stage` |
| `GET /{type}` (singleton), `GET /{type}/{id}`, `GET /{type}/by-slug/{slug}` | `locale`, `fallback`, `fields`, `populate`, `stage` |

Access is each type's `access`, checked before anything is read:
`public` serves anyone; `authenticated` any principal (an organization
type only one whose tenant is the path's organization, else 403
`unauthorized`); a permission is checked through the host's `Authorizer`
at `Resource { tenant, kind: "content_type", id: <type> }`, so an
`Application` grant or a grant on that type serves it and a grant on
another type does not. Without a principal a non-public type answers 401
`unauthenticated`. Delivery never consults the policy for `public`
types, so they are served on a public listener without identity too.
Unpublished and absent entries are both 404 `entry_not_found`.

`stage=draft` (preview) also needs `content.preview` at `Application`
scope (`Tenant(org)` for organization types) besides the type's
`access`; it reads the live drafts through the locale chain (each
entry's first variant in the chain, `DISTINCT ON`), with the same
filters, population and slugs, and answers `private, no-store` without
an ETag. In preview `sys.published_at` is the variant's own, null (sorted
last) for a draft never published.

- `locale` is any configured locale (default: the default locale); a
  non-localized type serves its default variant in every locale.
  `fallback=false` serves only the locale's own variant of a localized
  type. `sys.source_locale` names the variant served.
- Delivery filters take `sys.id` and `sys.published_at`; the default sort
  is `-sys.published_at`, whose tie-breaker `entry_id` runs against it so
  either direction is one scan of `published_list`. Cursors are bound to
  the type, scope, stage, locale, fallback, filters and sort.
- `by-slug` reads the slug in the locale's own variant first, then the
  fallback rows in chain order, then the entry id.
- `populate=author,body.author` expands entry references along dotted
  paths of field names, at most `limits.max_populate_depth` segments each,
  one batched query per level in the request's stage and locale chain. A
  segment expands every entry reference inside the field's value
  (references, components, component lists and `blocks`, and a rich text
  field's `entry_link` annotations). Only targets of types the caller
  reads (by their own `access`, and `content.preview` for drafts), in the
  request's scope or the application scope, are expanded, as `entry`
  (`sys` with id, type, locale, source locale and revision, and every
  field). Every reference in a delivery response whose target is not
  visible is `{"id", "type", "missing": true}`. A path's first segment
  must be among `fields` when `fields` is given (`not_selected`).
- Every asset reference of every entry shown, populated ones and preview
  included (asset fields, `asset` rich text embeds, inside components and
  `blocks` too; of a root only the fields `fields` selects), gains
  `asset`: `{id, filename, content_type, size, width, height, url}`, read
  in one statement (`content.delivery.assets`) per 1000 `(entry, locale,
  asset)` triples. It is `"asset": null` when the asset route would not
  serve the asset to this reader: trashed or purged (a forced delete), in
  a scope the entry cannot reference, or not in the refs of the variant
  and stage the value comes from (the variant served, or the default
  variant for a shared field; a draft-only asset in a published response
  is `null`). `url` is `/v1/content/assets/{id}/{filename}` for
  application entries and `/v1/content/o/{organization}/assets/{id}/{filename}`
  for an organization's, prefixed with `delivery.public_base_url` when
  set; preview returns the same delivery URLs, so a draft-only asset's URL
  answers 404 until it is published. A response holds at most 20000
  asset references, counted before any lookup (`asset_limit`, 400: ask
  for a smaller `limit` or fewer `fields`), so it runs at most 20 asset
  statements.
  Management reads show asset references as stored.

Published responses carry `ETag: W/"<8 hex of the model fingerprint>-<the
scope's generation>-<16 hex of SHA-256 of the route, the sorted query and
the caller's access class>"` (an organization response that populates
on an organization's path also carries the application generation
whenever the type can reference entries or holds asset fields, since
population, the `missing` checks and application assets read application
rows; a set `delivery.public_base_url` is part of the hash). Asset
patches, deletes and undeletes bump their scope's generation, so the
expanded metadata never outlives its tag. The access class is whether a
principal asked plus the set of types the caller reads there. A matching
`If-None-Match` answers 304 after reading only
`merezha_content.generations`; otherwise the generations are read first
and the entries after, in the same transaction. `Cache-Control` is
`delivery.cache_control_public` only when there is no principal, the
type is `public`, and every type reachable through its references that
the caller reads is `public` too (a policy may let anonymous callers read
a permission type); `delivery.cache_control_private` otherwise; every delivery
response, errors included, has `Vary: Authorization, Cookie`, and errors
are `private, no-store`.

Codes added by C5: `populate_limit`, `response_too_large` (400); by
issue #46: `asset_limit` (400). Every
content statement is registered: `merezha_content::STATEMENTS` (`content.*`:
activation and indexes, entries, revisions and references, publishing and
the trash, delivery, preview and population, and `merezha content diff`'s
`content.diff.read_only`) and `merezha_content::media::STATEMENTS` for
assets.

### Content assets

With `[modules.content.media]` (C7) the block stores uploaded files in a
`BlobStore` (`merezha-blob`'s local or S3 driver), provides that store to
every context, and accepts `asset` fields in the model (without the
section they fail `content_model_invalid`).

```toml
[modules.content.media]          # the section's presence enables media
driver = "local"                 # or "s3"; the default
local = { root = "/srv/merezha/media" }   # required for local; created 0700
serve = "proxy"                  # or "redirect" (S3 only); the default
signed_url_ttl = "5m"            # 60 s to 3600 s
max_upload_bytes = 52428800      # 1024 to 5368709120
max_concurrent_uploads = 4       # 1 to 64, per host
allowed_types = ["image/png", "image/jpeg", "image/gif", "image/webp", "image/avif",
                 "image/svg+xml", "application/pdf", "video/mp4", "video/webm",
                 "audio/mpeg", "audio/ogg", "text/plain", "text/csv"]
inline_types = ["image/png", "image/jpeg", "image/gif", "image/webp", "image/avif"]
spool_dir = "/var/tmp/merezha-content"   # default <OS temp dir>/merezha-content
asset_cache_control = "public, max-age=3600"
orphan_grace = "1d"              # 1 hour to 30 days
cleanup_cron = "47 3 * * *"      # UTC

[modules.content.media.s3]       # with driver = "s3"
bucket = "media"
region = "eu-west-1"
endpoint = "https://s3.example.com"        # optional; AWS by default
path_style = true
allow_http = false
access_key_id = { env = "S3_ACCESS_KEY_ID" }        # or { file = "..." }
secret_access_key = { env = "S3_SECRET_ACCESS_KEY" }
timeout = "30s"                  # 1 s to 300 s
key_prefix = ""
```

Every problem is reported at once, keyed `modules.content.media.<key>`:
`unknown_key`, `invalid_choice` (`driver`, `serve`), `missing_required`,
`invalid_path` (relative `local.root` or `spool_dir`), `out_of_range`,
`expected_duration`, `invalid_cron`, `invalid_media_type` (not a bare
lowercase MIME type), `duplicate_name`, `media_type_unsupported` (an
allowed type the sniffer cannot confirm: a binary signature `infer` knows,
or `text/plain`, `text/csv`, `application/json`, `image/svg+xml`),
`inline_type_refused` (HTML, SVG, any XML, PDF), `inline_type_not_allowed`
(not in `allowed_types`), `invalid_header_value`,
`unused_driver_settings` (the other driver's table), `media_config_invalid`
(`serve = "redirect"` without S3), and the S3 driver's own
(`invalid_name`, `invalid_endpoint`, `plaintext_endpoint_refused`,
`invalid_key_prefix`). The credentials are nested secrets: `{ env }` or
`{ file }` references only (`secret_literal`, `secret_unavailable`), never
in `Debug` or logs.

Startup opens the store (a local root is created 0700 and refused when it
is a symbolic link, owned by another user or open to group or others,
`blob_root_insecure`; a bucket is not contacted), makes `spool_dir`
private the same way, and removes `merezha-upload-*`
files older than an hour from it (`content.spool_cleared`). Readiness adds
the store's probe: a `head` of `content/readiness/probe`, plus a one-key
list on S3 (which needs `s3:ListBucket`), or the local root's mode.

Routes, under `/v1/content` (management answers `private, no-store`).
The management routes below work in the application scope, checked at
`Application`; each is mounted again under `/o/{organization}` for that
organization's assets, checked at `Tenant(org)` for a principal whose
active organization it is ([Content organizations](#content-organizations)):

| Route | Permission | Success |
| --- | --- | --- |
| `POST /manage/assets?filename=..&title=..` (raw body, `Content-Type`) | `content.assets.write` | 201 asset, `Location` (absolute under `delivery.public_base_url`, as is the asset's `url`) |
| `GET /manage/assets` (`content_type` exact or `image/*`, `filename_prefix`, `limit`, `cursor`) | `content.assets.read` | 200 `{items, page}`, newest first, live assets |
| `GET /manage/assets/{id}` | `content.assets.read` | 200 asset with `usage: {draft, published, elsewhere}` (refs of its own scope by stage, and of every other scope together: what `asset_in_use` counts), trashed ones too |
| `PATCH /manage/assets/{id}` (`application/merge-patch+json` over `{title, focal_point}`) | `content.assets.write` | 200 asset |
| `GET /manage/assets/{id}/file` (`Range`) | `content.assets.read` | 200 or 206, always `attachment`, trashed ones too |
| `DELETE /manage/assets/{id}` (`force`, `purge`) | `content.assets.delete` | 204, or 409 `asset_in_use` |
| `POST /manage/assets/{id}/undelete` | `content.assets.delete` | 200 asset |
| `GET /assets/{id}/{filename}`, `GET /o/{organization}/assets/{id}/{filename}` (`Range`, `If-None-Match`) | the referencing types' `access` on that path | 200, 206, 304, or 302 in redirect mode |

An upload is checked in this order: the permission, a free upload slot
(`overloaded`, 503, `Retry-After: 1`), the query (`filename` required;
`title` at most 500 characters), a declared `Content-Length` over
`max_upload_bytes` (`asset_too_large`, 413, before any byte is read) and
the declared type, parameters stripped and `image/jpg` read as
`image/jpeg`, against `allowed_types` (`media_type_rejected`, 415). The
route reads up to `max_upload_bytes` whatever `http.body_limit` says. The
body streams into a spool file while it is counted (past the limit,
`asset_too_large`), hashed (SHA-256) and checked as UTF-8 for text types;
a wait of 30 s for the next bytes, or a body not done within an hour,
fails `upload_incomplete` (400). Then the first 8 KiB must carry a binary
signature naming the declared type; without one, the declared type must be
a text type, the bytes valid UTF-8, and an SVG must have an `<svg` root
(`media_type_rejected`). HTML looks like text here: it is stored as the
declared plain text and served as an attachment. PNG, JPEG, GIF, WebP and
AVIF dimensions are read from the header; a side of 0 or over 50,000, or a
header that does not parse, is `media_type_rejected`. The stored filename
keeps `[A-Za-z0-9._-]` (others become `-`), drops leading punctuation,
takes the type's extension and is at most 200 bytes; it never names a key.

Blobs are content-addressed per scope at
`content/app/sha256/<2>/<2>/<sha256>`, and an organization's at
`content/org-<32 hex of the id's SHA-256>/sha256/...`, so no
organization id appears in a bucket listing and identical bytes in two
organizations are two objects. The blob claim is its own transaction:
a new `pending` row leased for an hour, or the existing row (`stored` or
`orphaned` skips the put, `pending` extends the lease and puts too, a row
being deleted is waited for up to 2 s, then `blob_unavailable`). The put
streams the spool file and must end within the lease. One transaction then
flips the locked blob row to `stored` (only from `stored`, `orphaned`, or
`pending` after this upload's own put), inserts the asset and appends
`content.asset_uploaded` (`scope, id, content_type, size, sha256, actor`).
A row that moved meanwhile (cleanup took it) sends the upload back to the
claim, three times at most. Two identical uploads at once both succeed
(the claim's insert takes any conflict). A refused or failed upload
removes its spool file, and the blob row and object it created; when that
object delete fails too, the row goes back to `pending` with its lease
over, and the next upload of the bytes joins it and puts again. A crashed
upload leaves a `pending` row that cleanup removes, with its object, once
the lease expires; an upload meeting a deletion whose lease expired takes
the row over. Every asset and blob write starts with the model version check.

Delivery serves an asset only while it is live and a published variant of
a type the caller may read under its `access` references it (on the
application path, the application's refs; on an organization's path,
that organization's refs to its own assets and to application assets),
and only under its stored filename; anything else is `asset_not_found` (404), so a draft-only upload
stays unreachable with its id. Proxy mode streams with the stored type
(`; charset=utf-8` for plain text and CSV), `Content-Disposition: inline`
for `inline_types` and `attachment; filename="<filename>"` otherwise,
`X-Content-Type-Options: nosniff`, `Content-Security-Policy: default-src
'none'; sandbox`, `ETag: "<sha256>"` (a matching `If-None-Match` is 304),
`Accept-Ranges: bytes`, `Vary: Authorization, Cookie`, and
`asset_cache_control` when a `public` type references it (`private,
no-cache` otherwise). One `bytes=` range (`a-b`, `a-`, `-n`) is 206 with
`Content-Range`; one starting at or past the end is 416
`range_not_satisfiable` with `Content-Range: bytes */<size>`; several
ranges, other units, malformed ranges and an `If-Range` naming other bytes
get the full body. Redirect mode (S3) answers 302 to a presigned URL valid
for `signed_url_ttl`, with `Cache-Control: private, max-age=<ttl - 30 s>`.
The bucket then serves the bytes without `nosniff` or the CSP, so give it
(or its CDN) a domain of its own, never the API's or the site's;
the object keeps the type and disposition set at its first put (`inline`
or a filename-less `attachment`, since one object serves every asset of
those bytes). On S3, `media.s3.timeout` bounds each request, and each wait
for bytes of an object read rather than the whole read, so a proxied
download of a large file is not cut while it moves.

Deleting an asset fails `asset_in_use` (409) while a ref in any scope
points at it, unless `force=true`, which leaves the refs (saves then
report `asset_not_found`). It is soft by default (`deleted_at`; the
asset leaves delivery and lists); `purge=true` hard-deletes at once,
trashed or not. A live asset's delete appends `content.asset_deleted`
(`forced`); patches append `content.asset_updated`; deletes, undeletes and
patches bump the scope's generation. The job `content.blob_cleanup`
(queue `content`, the cron schedule of the same name with `cleanup_cron`
in UTC, registered through `ContentState::guard_jobs`) first hard-deletes
assets trashed longer than `trash.retention`, 100 per transaction; a purge
marks its blob `orphaned` once no asset row (trashed ones included) uses
it. It then claims up to 1000 blobs, 100 per transaction (`FOR UPDATE SKIP
LOCKED`): orphaned ones past `orphan_grace`, pending ones past their lease
and deletions a crash abandoned (a ten-minute delete lease), marks them
`deleting` under a delete lease, commits, then for each blob re-takes the
row while the lease is still its own (renewing it), deletes the object
within half the lease and the row under the renewed lease; a failed delete
restores the previous state. A run whose lease expired and was taken over
touches nothing, so it never deletes an object uploaded again meanwhile;
one that loses its lease while the object delete runs leaves the row to
the new holder and counts the blob lost, not deleted
(`content.blob_cleanup` logs the count of blobs deleted under the run's
own lease). The fence lives in the database: a host stalled for longer
than the whole ten-minute lease between renewing it and sending the
object delete can still delete an object uploaded again with the same
bytes (content design section 6, issue #49). The delete is bounded by
half the lease and each S3 request by `media.s3.timeout`; keep that
timeout short and keep bucket versioning or soft delete on to recover
such an object.

Codes added by C7: `asset_not_found` (404), `asset_in_use` (409),
`asset_too_large` (413), `media_type_rejected` (415), `upload_incomplete`
(400), `spool_unavailable` (503), `media_config_invalid` (422), and
`merezha_blob::ERRORS` (`range_not_satisfiable` 416, `blob_unavailable` and
`blob_body_unreadable` 503, `blob_backend_rejected` 502, ...). The kernel
adds `blob_store_conflict` (422, startup).

Operations notes: give S3 buckets a lifecycle rule that aborts incomplete
multipart uploads (a cancelled put leaves one), and keep `spool_dir` on a
disk with room for `max_concurrent_uploads` times `max_upload_bytes`.

### Content organizations

A type declaring `scope = "organization"` (C9) keeps one set of entries
per organization, stored with scope `org:<organization id>`. Such types
load only when `[modules.identity]` is present, whose organizations are
the tenants; without it the model fails `content_model_invalid` at
`modules.content.model:types.<name>.scope`. Content never reads identity
tables: an organization without entries, or an unknown one, simply has
none.

- Management takes the organization from `Principal.tenant`: a session's
  active organization (`PUT /v1/identity/session/organization` or the
  `Merezha-Organization` header) or an organization API key's own. Checks
  run at `Resource { tenant: <org>, kind: "content_type", id: <type> }`.
  A principal without a tenant is checked at `Application` first, then
  answered `403 tenant_required`; an organization key never acts on
  application types (403). Another organization's ids are
  `entry_not_found` and `asset_not_found` on every route, singletons
  resolve the caller's own organization, and unique values, slugs,
  singletons, generations and blobs are per organization.
- Delivery under `/v1/content/o/{organization}`: an `authenticated` type
  serves principals whose tenant is that organization (anonymous 401,
  another organization's 403), a `public` one anyone, a permission type
  is checked at `Resource { tenant: <org>, .. }`. Responses hold that
  organization's rows and populated application entries only; any other
  target is `missing`. Their ETags carry the organization's generation,
  plus the application's when the type can reference entries.
- References: an organization entry may reference its own organization's
  entries and assets and the application's; an application entry only the
  application's. Any other target is `reference_not_found` (or
  `asset_not_found`), exactly like an absent id, so saves never reveal
  another tenant's ids; `reference_scope` is no longer reported. A client
  `id` taken in any scope is still `entry_exists` (409), since ids are one
  key: importers that set their own ids should use random UUIDs.
- Assets: the path chooses the scope, never the active organization.
  `/v1/content/manage/assets...` manages the application's (checked at
  `Application`; an organization key is refused, 403), and
  `/v1/content/o/{organization}/manage/assets...` that organization's
  (checked at `Tenant(org)`, and only for a principal whose active
  organization it is: 403 otherwise, application grants included). An
  organization asset's `url` is
  `/v1/content/o/{organization}/assets/{id}/{filename}` and its
  `Location` `/v1/content/o/{organization}/manage/assets/{id}`. Both
  upload routes read up to `media.max_upload_bytes`.
- Events of organization content carry the organization as their
  envelope `tenant` and `scope: "org:<id>"` in their payload.

#### Deleted organizations

When identity deletes an organization (issue #56), content stops serving
it and purges it. Content subscribes the job kind
`content.organization_deleted` (queue `content`) to
`identity.organization_deleted` whenever `[modules.identity]` is present.
The subscription's transaction records the scope in
`merezha_content.scopes_deleted` (migration 4) and appends the job
`content.organization_purge`. From that commit on:

- Every route of the organization answers `404 organization_not_found`:
  management of its entries, singletons, trash and batches (for any
  principal whose tenant it still is), everything under
  `/v1/content/o/{organization}` (delivery and its 304s, the model,
  asset management and asset delivery, the application assets its entries
  used included). The check runs after the route's permission check, so a
  principal of another organization still gets its 403.
- No write of the organization commits: every organization write takes
  a shared transaction lock on the scope and checks the record first, and
  the deletion takes that lock exclusively, so a write in flight commits
  before the deletion does (and is then purged) or is refused. An upload
  in flight gives up and removes its blob row and object.
- `content.organization_purge` hard-deletes, 100 rows per transaction:
  entries (with variants, revisions, `published` rows and outgoing refs),
  refs of any scope that point at them, assets and the refs to them, then
  hands the blobs to `content.blob_cleanup` (`orphaned` and due at once,
  whatever `media.orphan_grace`), removes the scope's generation and marks
  the record `purged`. On a host with media it appends a
  `content.blob_cleanup` run, which deletes the objects under C7's
  delete leases. Every step is idempotent: a host killed mid-purge leaves
  the job to the next lease on any host, and a run nearing its deadline
  appends its continuation. Another organization's rows and objects are
  never touched, including blobs of the same bytes. Application assets
  only the organization's entries used become deletable without `force`.
- The record stays after the purge (organization ids are never reused),
  so the organization never answers again. The purge appends no content
  events; consumers learn of it from `identity.organization_deleted`.
  Logs: `content.organization_deleted` (tenant, source, forced) when the
  scope is marked, `content.organization_purged` (tenant, entries,
  assets, blobs, forced) when the purge finishes it, from the job and
  from the operator command alike.

What the mark does not reach at once:

- A management or delivery request that passed its check just before
  the mark commits still answers from the rows it read. A write in that
  window cannot commit after the mark (it waits on the lock, then is
  refused or purged).
- With `media.serve = "redirect"`, a presigned URL handed out before the
  mark stays valid for up to `media.signed_url_ttl`, until the purge's
  blob cleanup deletes the object.
- Shared caches and CDNs keep a public response for its `max-age` plus
  `stale-while-revalidate` (`delivery.cache_control_public`,
  `media.asset_cache_control`): purge them for the organization's path
  if that matters.
- Content's past events (`content.*` outbox records and their jobs, which
  carry ids, never field values) stay in `merezha_jobs` until
  `[jobs.retention]` removes them; the kernel's application event store
  is not touched.

A purge job that will not run again by itself (a permanent failure, or
its last attempt) logs `content.organization_purge_failed` at error with
the tenant and the code, besides the jobs runtime's `job.dead_lettered`:
alert on it. Until the job is replayed through the jobs admin API, or the
operator command below finishes it, the organization's data stays at rest
(still refused on every route). The operator command also covers
organizations deleted before content subscribed, or a missed event:

`merezha content purge-organization ORGANIZATION --config merezha.toml
[--force] [--set SECTION.KEY=VALUE]...` marks the organization's scope
deleted (source `operator`) and runs the purge to the end in the
foreground, then prints `organization <id> marked deleted` (or `already
marked deleted`), `purged entries <n> assets <n> blobs <n>` and `status
purged` (`(already)` when nothing was left). It needs migration 4
(`content_migration_pending` otherwise) and refuses, with exit code 1 and
a `refused:` line, an organization identity still has, or one identity
cannot confirm deleted (an id identity never issues, or no identity
schema). `--force` purges it anyway after a `warning: --force:` line:
its content is gone for good and content refuses the organization from
then on, while identity keeps it. Exit code 2 when the arguments, the
configuration or the database fail (`content.purge_failed` on stderr
with the code). With `[modules.content.media]` configured, it appends a
`content.blob_cleanup` run that the next running host takes, which
deletes the released objects (`blobs released to content.blob_cleanup`).
Safe beside a running purge job; rerun it after an interruption. Besides
stdout it logs `content.organization_deleted` and
`content.organization_purged` on stderr with `source: "operator"` and
`forced: true` when `--force` overrode identity.

The way back from a forced purge: the purged content is gone, but the
organization can be served again (empty) by deleting its row from
`merezha_content.scopes_deleted`, only once its `state` is `purged`.
Deleting a `purging` row would let writes resume while a purge job is
still deleting the scope's rows.

## Dependencies

Checked with `cargo info` on 2026-10-23 (K), 2026-10-24 (K2) and 2026-10-26
(`syn`, `proc-macro2`). All support Rust 1.98.0.

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
| `infer` | 0.22.0 | 1.74 | MIT | none (no `std`, so no `cfb` OLE probing) | content: upload signatures |
| `imagesize` | 0.15.0 | unset (compiled and tested on 1.98.0) | MIT | `png`, `jpeg`, `gif`, `webp`, `heif` only | content: raster dimensions |
| `syn` | 3.0.9 | 1.71 | MIT OR Apache-2.0 | `full`, `parsing`, `visit` only; dev only | postgres: the statement scan parses the workspace and the locked driver sources; cli: the registry test reads each crate's statement constants |
| `proc-macro2` | 1.0.107 | 1.71 | MIT OR Apache-2.0 | `span-locations` only; dev only | postgres: line numbers of the statement scan's findings |
| `croner` | =4.0.2 | unset (edition 2021) | MIT | `jiff` only, no default `chrono` (R14); pulls `derive_builder` 0.20.2 (MIT OR Apache-2.0) and `strum` 0.27.2 (MIT, MSRV 1.66.1) with their proc-macro crates | platform: `cron`, used by config validation and the scheduler |

The aws-lc-rs provider is shared by the database TLS connector and reqwest,
so one crypto provider is compiled. Wasmtime still locks `sha2` 0.10 for
itself.

## Limits and follow-ups

- Modules start even when migrations are pending in verify mode; readiness
  stays false. A module that needs its tables at start should check them.
- `Timeouts` (migrate 300 s, module start 10 s, drain 30 s, module shutdown
  10 s) are code defaults, not configuration yet; `merezha run` adds
  `jobs.drain_timeout_ms` plus 3 s to the module shutdown budget.
- There is no request deadline at the edge yet; handlers bound their own
  work.
- The events, outbox and jobs `trace_parent`/`trace_state` columns are
  filled (TE-T3, TE-T5).
- Cron jobs keep the occurrence key as their correlation id instead of a
  host-minted trace id (TE §2.3, with TE-T3's host-minted ids).
- PostgreSQL application storage (AP-A2) has no background projection
  runners, warm-up readiness or application admin API yet (AP-A3); catch-up
  and rebuild run in the request.
- Reactors (AP-C3) have no process test with a Wasm reactor component yet:
  the `task-notifier` example is contract-tested natively and builds as a
  component, and the end-to-end example application (`merezha new`, stage 3)
  wires it with an `ArchiveTask` route.
- `merezha_app.reactor_rejections` and `reactor_skips` have no retention yet;
  `GET reactors` counts rejections up to 10,000. A reactor batch asks the
  Directory one `(user, tenant)` pair after another, and the capture notifier
  logs `notification.captured` before the checkpoint transaction commits.
