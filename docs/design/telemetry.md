# Telemetry block design

Status: proposed 2026-10-23, aligned with the cross-block decisions on
2026-10-24. Conforms to [framework architecture](../architecture/framework.md)
as amended by [cross-block decisions](cross-block-decisions.md) (R15); where
this design and that record differ, the record wins. Stage 1 work, which
depends on kernel stages K and K2.

This design owns `names.rs`, the single catalog of metric, span, attribute
and log event names for every block (R9). The identity, notifications,
content and application-platform designs reference it instead of defining
names; §1.3, §1.5 and §4 hold their rows.

The kernel already provides `tracing`-based JSON logs with stable event
names, W3C `traceparent` handling at the edge, `/health/live` and
`/health/ready`. This block adds OTLP export of traces, metrics and logs; the
naming, cardinality and redaction rules every block follows; propagation
through the application core, outbox and jobs; refined health plus a
protected diagnostics endpoint; a local backend, dashboards and alerts.

Guests get no telemetry API; the host observes them from outside.

## 0. Shape and ownership

```text
merezha-telemetry  (new crate)
  default features (tracing + opentelemetry API only; every library may use them)
    names.rs       every block's span, metric, attribute and event names (R9), typed
                   attribute constructors (§1.5) and the event! macro (§4)
    blocking.rs    spawn_blocking_in_span: carries Dispatch + Span into blocking threads
    config.rs      [telemetry] section model and secret headers; the kernel's loader
                   parses and validates every key into it
  feature "propagation" (adds tracing-opentelemetry; no exporter, no network code)
    propagation.rs traceparent parse/format, trust policy, links, stored context
  feature "http" (adds axum's types only; the platform edge)
    names.rs       attr::route(&MatchedPath)
  feature "pipeline" (the merezha binary only)
    pipeline.rs    providers, guarded processors, exporters, shutdown
    subscriber.rs  tracing registry: JSON writer + span bridge + OTLP log bridge
    json.rs        flat JSON event formatter (stable field names)
    allowlist.rs   attribute allowlist and value caps (export-time wall)
    status.rs      exporter counters for /v1/admin/status
  feature "testing" (implies "pipeline") in-memory harness and assertion helpers
```

Dependency rule: libraries depend only on the APIs. `merezha-runtime`,
`merezha-application`, `merezha-postgres` and the block crates use `tracing`
for spans and events, the `opentelemetry` API crate (`metrics` feature) for
instruments, and `merezha-telemetry` with default features for names. Each
takes an `opentelemetry::metrics::Meter` in its builder (default
`NoopMeterProvider`) and builds its `Metrics` struct of instruments once.
Crates that set remote parents or links (platform edge, jobs, application
projections and reactors) enable `propagation`. Only the `merezha` binary
enables `pipeline`, so `opentelemetry-otlp`, the log bridge and every exporter
link into the binary alone. `merezha-runtime` uses default features only and
stays free of the SDK and of any network code.

No OpenTelemetry globals: providers live in a `merezha_telemetry::Telemetry`
handle owned by the host, and `AppContext` exposes
`ctx.telemetry().meter(module)`. The tracing subscriber is the one global,
installed by the binary; tests use scoped dispatchers (§8).

## 1. Signals and naming

### 1.1 General rules

- Standard boundaries use semantic conventions schema 1.42.0, the one
  `opentelemetry-semantic-conventions` 0.33.1 ships. Stable names come from
  that crate; experimental ones (`db.client.connection.*`, `messaging.*`,
  `http.server.active_requests`, `url.template`) are spelled out in `names.rs`
  and pinned against the 1.42.0 registry by a test.
- Merezha names use the `merezha.` namespace, lower case, dotted. Units are
  UCUM (`s`, `By`, `1`, `{thing}`); metric durations are always f64 seconds,
  in every block (R9). Log fields keep the kernel's `duration_ms`.
- Block names follow one pattern (R9):

  | Block | Metrics | Attributes | Spans | Log events |
  | --- | --- | --- | --- | --- |
  | identity | `merezha.identity.*` | `identity.*` | `identity {operation}` | `identity.*` |
  | notifications | `merezha.notification.*`, `merezha.inbox.*`, `merezha.webhook.*` | `notify.*` | `notify {step}` | `notification.*` |
  | content | `merezha.content.*` | `content.*` | `content {operation}` | `content.*` |
  | jobs and outbox | `merezha.job.*`, `merezha.outbox.*` | `job.*` | `send`, `process`, `relay` | `job.*`, `outbox.*` |
  | host core (edge, runtime, application, reactors, DB, telemetry) | `merezha.*` | `merezha.*` | per §1.2 | per §4 |

  Semantic-convention keys (`http.*`, `db.*`, `messaging.*`, `server.address`,
  `url.scheme`, `error.type`, `user.id`) keep their standard names in every
  block. The allowlist (§1.5) keeps a block key only when it carries its
  module prefix and appears in the table.
- Log event names are `<area>.<noun>_<verb>` with a singular area and one dot:
  `job.dead_lettered`, `notification.failed`, `content.model_activated`.
  Notifications keep `notification.*` events and metrics with `notify.*`
  attributes, the names notifications.md uses.
- Boundary and DB spans are `INFO`. The OpenTelemetry layer has its own
  filter (INFO and above), independent of the log level.
- Span status is `Error` only when the host failed its own contract: internal
  or transient failure, guest trap, resource limit, dependency failure. Domain
  rejections, validation failures, conflicts, 4xx and `guest_error` replies
  leave it `Unset` and set `merezha.outcome`.
- Every boundary duration histogram carries an outcome attribute, so one
  instrument answers both rate and error ratio. There are no separate
  `*_errors_total` instruments; the TS `Metrics.boundary` calls/errors split
  maps to `outcome`.

### 1.2 Span catalog

| Boundary | Span name | Kind | Parent |
| --- | --- | --- | --- |
| HTTP server | `{METHOD} {http.route}`, or `{METHOD}` when unmatched | SERVER | inbound context per trust policy (§2.1) |
| Startup | `startup`, children `migrate {module}`, `catalog.load`, `module.start {name}` | INTERNAL | root |
| Command dispatch | `command {tag}` | INTERNAL | HTTP span or job span |
| Query dispatch | `query {tag}` | INTERNAL | HTTP span |
| Projection catch-up | `projection {name}` | INTERNAL | the command span (post-commit catch-up), otherwise root (interval, background, admin), with links |
| Reactor batch | `react {reactor}` | INTERNAL | root, with links to the source events' traces (reactors never run inside a request) |
| Guest call | `guest {service} {operation}` (`validate`, `decide`, `project`, `query`, `react`, `describe`, `raw`) | INTERNAL | dispatch span |
| Raw pipeline | `pipeline {id}`, with one `guest {service} raw` child per step | INTERNAL | HTTP span |
| DB transaction | `transaction {module}` | INTERNAL | caller |
| DB statement | `{db.query.summary}`, for example `INSERT merezha_app.events` | CLIENT | transaction or caller |
| Outbox append, enqueue | `send {queue}` (job records and `JobsHandle::enqueue`), `send {topic}` (event records) | PRODUCER | caller (inside its transaction) |
| Outbox relay | `relay outbox`, one per non-empty batch | INTERNAL | root |
| Job attempt | `process {queue}` | CONSUMER | stored producer context (§2.3) |
| Outbound HTTP | `{METHOD} {url.template}` | CLIENT | caller |
| Identity operation | `identity {operation}` (`login.password`, `login.magic_link`, `login.oauth`, `login.passkey`, `oauth.callback`, `session.resolve`, `api_key.resolve`, `password.reset`, `email.verify`) | INTERNAL | HTTP span |
| Notifications | `notify expand`, `notify {channel}` (`email`, `sms`, `push`, `inbox`), `notify webhook`; the request itself is the caller's `send notify.expand` span, and provider and webhook calls are outbound HTTP spans | INTERNAL | `process notify.expand`, `process notify.{channel}`, `process notify.webhook` |
| Inbox stream wake | `notify sse`, one short span per wake batch, not per stream lifetime | INTERNAL | root |
| Content operation | `content {operation}` (`entry.save`, `entry.publish`, `materialize`, `delivery.query`, `populate`, `asset.upload`, `blob.put`, `blob.get`, `schedule.run`) | INTERNAL | HTTP span or job span |
| Admin operation | `admin {operation}` (`projection.rebuild`, `reactor.retry`, `reactor.skip`, `job.replay`, `catalog.reload`) | INTERNAL | HTTP span |

No spans for: `/health/*` requests (metrics only), empty job-lease polls,
empty relay batches, projection and reactor ticks with no new events, and pool
acquisitions (their wait is the `merezha.db.pool.wait` attribute on the
transaction span plus a histogram). DB statement and transaction spans never
start a trace (T4): outside a caller's span (readiness probes, listener
probes, background polls) a statement only counts in the metrics.

### 1.3 Metric catalog

`B:` is a histogram with explicit boundaries. `C` is a counter, `UDC` an
up/down counter, `G` an observable gauge. Every attribute listed is the full
set allowed on that instrument.

**HTTP edge**

| Name | Type, unit | Attributes |
| --- | --- | --- |
| `http.server.request.duration` | B: semconv default `[0.005,0.01,0.025,0.05,0.075,0.1,0.25,0.5,0.75,1,2.5,5,7.5,10]`, `s` | `http.request.method`, `http.route`, `http.response.status_code`, `error.type`, `url.scheme` |
| `http.server.active_requests` | UDC `{request}` | `http.request.method`, `url.scheme` |
| `http.server.request.body.size` | B: `[256,1024,4096,16384,65536,262144,1048576]`, `By` | `http.request.method`, `http.route` |
| `merezha.http.server.rejected` | C `{request}` | `merezha.reason`: `overloaded`, `body_limit`, `timeout`, `starting`, `draining`, `cors` |

**Guest invocation** (runtime)

| Name | Type, unit | Attributes |
| --- | --- | --- |
| `merezha.guest.invocation.duration` | B: HTTP buckets, `s` | `merezha.guest.service`, `merezha.guest.operation`, `merezha.guest.outcome` |
| `merezha.guest.fuel.consumed` | B: `[1e4,1e5,1e6,1e7,1e8,1e9,1e10]`, `{fuel}` | `merezha.guest.service`, `merezha.guest.operation` |
| `merezha.guest.fuel.utilization` | B: `[0.01,0.05,0.1,0.25,0.5,0.75,0.9,0.99,1]`, `1` | same |
| `merezha.guest.memory.peak` | B: `[65536,262144,1048576,4194304,16777216,67108864,268435456]`, `By` | `merezha.guest.service` |
| `merezha.guest.invocations.active` | UDC `{invocation}` | none |
| `merezha.catalog.generation` | G `{generation}` | none |
| `merezha.catalog.reloads` | C `{reload}` | `merezha.outcome`: `ok`, `invalid_manifest` |

`merezha.guest.outcome` is `ok` plus the runtime `Error::code()` set:
`guest_error`, `invalid_input`, `input_limit`, `output_limit`,
`fuel_exhausted`, `memory_limit`, `timeout`, `trap`, `overloaded`, `internal`,
`not_found`. Two runtime additions feed these signals: `StoreState` gains
`peak_memory`, set to `desired` by `memory_growing` when growth is allowed,
and `Invocation::run` reports `fuel_limit - fuel_remaining` per step and in
total (pipelines keep sharing one budget). The runtime has no telemetry
dependency beyond `tracing` and its `Meter`.

**Application dispatch, projections and reactors** (application platform)

| Name | Type, unit | Attributes |
| --- | --- | --- |
| `merezha.command.duration` | B: HTTP buckets, `s` | `merezha.command.tag`, `merezha.outcome` |
| `merezha.query.duration` | B: HTTP buckets, `s` | `merezha.query.tag`, `merezha.outcome` |
| `merezha.events.appended` | C `{event}` | `merezha.command.tag` |
| `merezha.command.receipts.replayed` | C `{command}` | `merezha.command.tag` |
| `merezha.events.head` | G `{position}` | none |
| `merezha.projection.lag.events` | G `{event}` | `merezha.projection.name` |
| `merezha.projection.lag.age` | G `s` | `merezha.projection.name` |
| `merezha.projection.catch_up.duration` | B: HTTP buckets, `s` | `merezha.projection.name`, `merezha.projection.trigger` (`commit`, `interval`, `background`, `admin`, `rebuild`), `merezha.outcome` |
| `merezha.projection.events.processed` | C `{event}` | `merezha.projection.name` |
| `merezha.projection.lock.contended` | C `{attempt}` | `merezha.projection.name` |
| `merezha.projection.lock.held` | G `s` (age of the run lock this instance holds; absent when none) | `merezha.projection.name` |
| `merezha.projection.warm` | G `1` (0 or 1, this instance's configured revision) | `merezha.projection.name` |
| `merezha.projection.rebuild.remaining` | G `{event}` (target minus checkpoint; absent without a rebuild) | `merezha.projection.name` |
| `merezha.reactor.lag.events` / `merezha.reactor.lag.age` | G `{event}` / G `s` | `merezha.reactor.name` |
| `merezha.reactor.lock.contended` / `merezha.reactor.lock.held` | C `{attempt}` / G `s` | `merezha.reactor.name` |
| `merezha.reactor.batch.duration` | B: HTTP buckets, `s` | `merezha.reactor.name`, `merezha.outcome` (`accepted`, `failed`, `halted`) |
| `merezha.reactor.effects` | C `{effect}` | `merezha.reactor.name`, `merezha.reactor.effect` (`notify`, `webhook`, `command`), `merezha.outcome` (`accepted`, `rejected`) |
| `merezha.reactor.halted` | G `1` (0 or 1) | `merezha.reactor.name` |
| `merezha.reactor.skipped` | C `{event}` | `merezha.reactor.name`, `merezha.reason` (`admin`, `on_error_skip`, `skip_to_head`) |

Dispatch outcome (`merezha.outcome`) is computed from `Failure.classification`
and the host code:

| Outcome | When | Counts against the SLO |
| --- | --- | --- |
| `accepted` | command committed, or query answered (`ok` for queries) | no |
| `replayed` | idempotent receipt returned | no |
| `rejected` | `Permanent`: validation, guest domain rejection, handler or projection not found, input limit | no |
| `denied` | `unauthorized` | no |
| `conflict` | `Conflict`: expected-version mismatch, CAS loss | no (tracked) |
| `failed` | `Transient`, internal, timeout, guest trap, fuel or memory limit, storage failure | yes |

Lag gauges are not computed in callbacks. The catch-up loop updates them in an
in-memory cache: `head` is the latest appended position seen, and `oldest
unprocessed accepted_unix_ms` comes from the first unprocessed event. A
5-second sampler refreshes `head` from the store so that writes from other
instances count. The observable callbacks read only the cache. Lag age is 0
when the checkpoint equals the head. Reactor lag uses the same cache. Lag
gauges feed alerts and status, never readiness (R10). An effect that fails
validation (`recipient_not_in_tenant`) counts `rejected` and the reactor
advances; a malformed reply halts the reactor whatever its `on_error` (R4).
The append is the registered statement `app.events.append`, timed by
`db.client.operation.duration`; `storage_busy` counts as `failed` on
`merezha.command.duration`. A PostgreSQL feed gap logs `projection.failed`
with `code = event_feed_gap`.

**PostgreSQL** (merezha-postgres)

| Name | Type, unit | Attributes |
| --- | --- | --- |
| `db.client.operation.duration` | B: `[0.001,0.002,0.005,0.01,0.02,0.05,0.1,0.2,0.5,1,2,5]`, `s` | `db.system.name`=`postgresql`, `db.operation.name`, `db.collection.name`, `db.response.status_code` (SQLSTATE), `error.type`, `merezha.module` |
| `db.client.connection.count` | G `{connection}` | `db.client.connection.pool.name`, `db.client.connection.state` (`idle`, `used`) |
| `db.client.connection.max` | G `{connection}` | pool name |
| `db.client.connection.pending_requests` | G `{request}` | pool name |
| `db.client.connection.wait_time` | B: DB buckets, `s` | pool name |
| `db.client.connection.timeouts` | C `{timeout}` | pool name |
| `merezha.db.migrations.pending` | G `{migration}` | `merezha.module` |
| `merezha.db.listener.reconnects` | C `{reconnect}` (the shared `Listener`) | none |

Statements are registered statically: `merezha-postgres` exposes
`Statement { name, summary, sql }`. `name` (`app.events.append`) becomes
`merezha.db.statement`; `summary` (`INSERT merezha_app.events`) becomes the
span name and `db.query.summary`; operation and collection derive from it.
`Statement::new` checks both at compile time, and `adhoc` is a reserved
name. Statements run through `Database::query`, `execute` and their
siblings, which take any session (pooled client, `Tx`, detached client);
text built at run time runs through `batch_text` (no rows) or `query_text`
(parameters and rows) under a registered name.
SQL sent through the driver directly is invisible to telemetry rather than
reported as `adhoc` (T4: `Tx` stays the driver's transaction type so blocks
keep compiling), so one workspace scan (`crates/postgres/tests/statement_scan.rs`)
fails the build instead: it walks every crate's `src/` and flags each driver
method (`query`, `query_one`, `query_opt`, `execute`, `batch_execute`,
`query_typed`, `prepare`, `prepare_typed`, `prepare_cached`, `simple_query`,
`copy_in`, `copy_out` and the rest of `statement::DRIVER_METHODS`, matched
with the driver's argument count) whose second argument is not a registered
`&STATEMENT`. Its per-crate allowlist only shrinks: each entry names a file,
its exact call count and why (the registry's own dispatch, the test
harness's administrative connection, rusqlite in the SQLite store, a
same-named non-SQL method, test fixtures, the unmetered `PgOutbox`).
`error.type` is the `classify` code (`unique_violation`), or `cancelled` when
the caller dropped a statement before the server answered.
Pool gauges read `deadpool` `Pool::status()`, which does no I/O.

**Jobs and outbox** (application platform, part B)

| Name | Type, unit | Attributes |
| --- | --- | --- |
| `merezha.job.enqueued` | C `{job}` | `job.queue`, `job.kind` |
| `merezha.job.duration` | B: HTTP buckets, `s` | `job.queue`, `job.kind`, `job.outcome` (`succeeded`, `retry`, `dead_lettered`, `lease_lost`, `released`), `job.classification` (`permanent`, `transient`, `conflict`; absent on success) |
| `merezha.job.queue_latency` | B: `[0.01,0.05,0.1,0.5,1,5,10,30,60,300,900]`, `s` (lease start minus `run_at`) | `job.queue` |
| `merezha.job.attempts` | B: `[1,2,3,4,5,6,8,10,15,20]`, `{attempt}`, recorded at the terminal outcome | `job.queue`, `job.kind`, `job.outcome` |
| `merezha.job.dead_lettered` | C `{job}` | `job.queue`, `job.kind` |
| `merezha.job.replayed` / `merezha.job.cancelled` / `merezha.job.lease_expired` (reclaimed by the reaper) | C `{job}` | `job.queue` |
| `merezha.job.count` | G `{job}` | `job.queue`, `job.state` (`ready`, `delayed`, `leased`, `dead`) |
| `merezha.job.lag` | G `s` (age of the oldest due `available` job; the value `jobs.readiness.max_lag_ms` compares) | `job.queue` |
| `merezha.job.schedule.fired` | C `{run}` | `job.schedule` (configured names, at most 256) |
| `merezha.outbox.appended` | C `{record}` | `merezha.module` (the record's `source`), `job.record_kind` (`event`, `job`) |
| `merezha.outbox.relayed` / `merezha.outbox.unrouted` (event records the relay found no subscription for) | C `{record}` | `merezha.module` |
| `merezha.outbox.quarantined` | C `{record}` (records the relay set aside; see AP B.4) | `merezha.module`, `error.type` (the quarantine code) |
| `merezha.outbox.pending` / `merezha.outbox.lag` | G `{record}` / G `s` (age of the oldest unrelayed record) | none |

AP's per-outcome counters (`leased`, `succeeded`, `retried`, `lease_lost`,
`released`) are the count series of `merezha.job.duration` (§1.1). `JobFailure`
codes appear only as the span and log attribute `job.code`. `job.state` maps
AP's rows: `ready` and `delayed` are `available` with `run_at` past or future,
`leased` is `running`, `dead` is `dead`; terminal rows are not counted. AP's
admin queue listing uses the same state names.

Job and outbox gauges belong to this block (T5), not AP-B2. They come from
`JobsHandle::queue_status()`, the read behind `GET /v1/jobs/admin/queues` and
`outbox`, run once per instance every `telemetry.metrics.sample_interval_ms`
(default 15 s) with a 1 s statement timeout and cached. Every instance reports
the same database-level numbers; dashboards use
`max without (service_instance_id)`. The sample runs the same two
statements as those routes in one transaction, skips their quarantined
listing, runs only while metrics are exported, and keeps the last values
when a read fails (`job.worker_failed` with `task = sampler`, once per
streak); after 3 consecutive failed reads the gauges report nothing until a
read succeeds, so a stuck sampler shows as a gap, not as stale numbers.

Counting rules (TE-T5): `merezha.job.enqueued` counts every new job (enqueue,
relay fan-out, fired occurrences, manual triggers); `merezha.job.schedule.fired`
counts fired occurrences only. `merezha.outbox.appended` counts at append, in
the caller's transaction, so a rollback afterwards still counts; the relay
counters count committed batches, and `merezha.outbox.relayed` includes
unrouted event records. `merezha.job.dead_lettered` counts the handler path
after its fenced settle and the reaper path, so each dead job counts once.

**Outbound HTTP** (AppContext client)

| Name | Type, unit | Attributes |
| --- | --- | --- |
| `http.client.request.duration` | B: HTTP buckets, `s` | `http.request.method`, `server.address` (omitted for `webhook`), `http.response.status_code`, `error.type` (`timeout`, `connect`, `tls`, `body_limit`, or the status code), `merezha.provider` (`brevo`, `webpush`, `webhook`, `oidc`, `s3`), `merezha.provider.operation` |

Callers pass a static `Endpoint { provider, operation, url_template }`.
`url.full` and query strings are never recorded.

**Identity**

| Name | Type, unit | Attributes |
| --- | --- | --- |
| `merezha.identity.login.attempts` | C `{attempt}` | `identity.method` (`password`, `magic_link`, `oauth`, `passkey`), `identity.outcome` (`success`, `invalid_credentials`, `unverified`, `locked`, `rate_limited`, `expired`, `provider_error`), `identity.provider` (configured OIDC names, else `local`) |
| `merezha.identity.authentications` | C `{request}` | `identity.credential` (`session`, `api_key`, `none`), `identity.outcome` (`ok`, `invalid`, `expired`, `revoked`) |
| `merezha.identity.sessions.created` / `.revoked` | C `{session}` | `identity.method` / `identity.reason` (`logout`, `expired`, `admin`, `password_change`) |
| `merezha.identity.sessions.active` | G `{session}` (set by the `identity.cleanup` job) | none |
| `merezha.identity.tokens.issued` | C `{token}` | `identity.purpose` (`email_verification`, `magic_link`, `password_reset`, `email_change`, `invitation`) |
| `merezha.identity.rate_limited` | C `{request}` | `identity.policy` (identity's policy names, such as `sign_in.ip`) |
| `merezha.identity.password_hash.duration` / `merezha.identity.password_hash.queue_wait` | B: `[0.01,0.025,0.05,0.1,0.25,0.5,1,2]`, `s` | none |
| `merezha.identity.authz_cache` | C `{lookup}` | `identity.result` (`hit`, `miss`) |
| `merezha.identity.audit.write_failures` | C `{write}` | none |

**Notifications** (the names notifications.md §9 uses)

`topic` is the label for notification volume (R9): topics are a declared,
bounded set. `template` is never a label. `notify.channel` is `email`, `sms`,
`push` or `inbox` on notification instruments, and `webhook` only on webhook
spans.

| Name | Type, unit | Attributes |
| --- | --- | --- |
| `merezha.notification.requests` | C `{request}` | `notify.source` (module names), `notify.topic`, `notify.outcome` (`accepted`, `duplicate`, `invalid`) |
| `merezha.notification.deliveries` | C `{delivery}` | `notify.channel`, `notify.topic`, `notify.status` (`accepted` plus the terminal statuses `delivered`, `bounced`, `complained`, `failed`, `skipped`, `expired`, `unknown`, `cancelled`), `notify.reason` (closed reason enum) |
| `merezha.notification.attempts` | C `{attempt}` | `notify.channel`, `notify.provider` (`brevo`, `webpush`, `inbox`, `capture`), `notify.outcome` (`accepted`, `duplicate`, `transient`, `rate_limited`, `permanent`, `ambiguous`) |
| `merezha.notification.delivery.duration` | B: HTTP buckets, `s` (one provider attempt) | `notify.channel`, `notify.provider`, `notify.outcome` |
| `merezha.notification.latency` | B: job queue buckets, `s` (request until `accepted`) | `notify.channel` |
| `merezha.notification.provider_events` | C `{event}` | `notify.provider`, `notify.channel`, `notify.event` (normalized provider event), `notify.correlated` (`true`, `false`) |
| `merezha.notification.provider_account_errors` | C `{error}` | `notify.provider`, `notify.channel` |
| `merezha.notification.render_failures` | C `{render}` | `notify.channel`, `notify.topic` |
| `merezha.notification.sms_segments` | C `{segment}` | `notify.encoding` (`gsm7`, `ucs2`) |
| `merezha.notification.push_subscriptions_removed` | C `{subscription}` | `notify.reason` (`gone`, `expired`, `failures`, `user`, `key_changed`) |
| `merezha.notification.egress_blocked` / `merezha.notification.data_purged` | C `{request}` / C `{row}` | `notify.target` (`webhook`, `push`) / none |
| `merezha.inbox.sse.connections` | UDC `{connection}` | none |
| `merezha.inbox.sse.events` | C `{event}` | `notify.event` (`item`, `reset`, `hello`, `unread`) |
| `merezha.inbox.sse.rejected` | C `{connection}` | `notify.reason` (`per_user`, `capacity`, `auth`) |
| `merezha.inbox.listen.reconnects` | C `{reconnect}` | none |
| `merezha.webhook.deliveries` | C `{delivery}` | `notify.outcome` (`succeeded`, `failed`, `cancelled`) |
| `merezha.webhook.attempts` | C `{attempt}` | `notify.class` (`2xx`, `3xx`, `4xx`, `410`, `5xx`, `timeout`, `network`, `blocked`) |
| `merezha.webhook.endpoints_disabled` | C `{endpoint}` | `notify.reason` (`manual`, `gone`, `failing`) |

**Content** (content's §9 rows, in this catalog's spelling)

| Name | Type, unit | Attributes |
| --- | --- | --- |
| `merezha.content.delivery.not_modified` / `merezha.content.revision_conflicts` / `merezha.content.migration.rows` | C `{response}` / `{conflict}` / `{row}` | `content.type` |
| `merezha.content.query.rows` | B: `[1,10,25,50,100,250,500]`, `{row}` | `content.type`, `content.stage` |
| `merezha.content.populate.entries` | B: `[1,10,50,100,250,500,1000,2000]`, `{entry}` | `content.type` |
| `merezha.content.entries.saved` / `merezha.content.entries.published` / `merezha.content.entries.unpublished` | C `{entry}` | `content.type` |
| `merezha.content.validation.failures` | C `{failure}` | `content.type`, `content.code` (content's registered detail codes) |
| `merezha.content.schedule.lag` | B: job queue buckets, `s` (run time minus `run_at`) | `content.action` (`publish`, `unpublish`) |
| `merezha.content.upload.bytes` | B: `[1024,65536,1048576,10485760,52428800,536870912]`, `By` | `content.driver` (`local`, `s3`) |
| `merezha.content.schedule.failures` / `merezha.content.upload.rejected` | C `{action}` / C `{upload}` | `content.code` |
| `merezha.content.blob.duration` | B: HTTP buckets, `s` | `content.driver`, `content.operation` (`put`, `get`, `head`, `delete`, `sign`), `content.outcome` (`ok`, `not_found`, `failed`) |
| `merezha.content.blobs.cleaned` | C `{blob}` | `content.driver` |

Delivery latency is `http.server.request.duration` per content route; there
is no content HTTP instrument. Blob errors are the `failed` series of
`merezha.content.blob.duration` (§1.1), replacing content's
`merezha.content.blob.errors`. `content.type` takes the model's type names,
`content.stage` is `published` or `draft`; `content.locale` and `content.job`
(content job kinds) are span and log attributes only.

**Telemetry self-observation**

| Name | Type, unit | Attributes |
| --- | --- | --- |
| `merezha.telemetry.exported` | C `{item}` | `merezha.telemetry.signal` (`traces`, `metrics`, `logs`), `merezha.outcome` (`ok`, `failed`) |
| `merezha.telemetry.dropped` | C `{item}` | signal (adds `stderr` for the JSON writer), `merezha.reason` (`queue_full`, `export_failed`, `shutdown`) |
| `merezha.telemetry.attributes.dropped` | C `{attribute}` | signal |

### 1.4 Cardinality rules (strict)

1. A metric attribute value comes from a set closed at startup: code enums;
   names declared in configuration or manifests (routes, command and query
   tags, projections, reactors, queues, job kinds, schedules, notification
   topics, channels, providers, content types, rate-limit policies, guest
   services, modules); error codes registered at startup (R8); HTTP status
   codes; SQLSTATE codes. Template names are not metric attributes.
2. Never metric attributes: any identifier (request, trace, span, message,
   correlation, causation, stream, event, tenant, user, session, job, API
   key), raw paths, query strings, hosts derived from user data (webhook
   URLs), guest-produced codes, emails, IPs, user agents, free text.
3. Guest-produced failure codes become `guest_error` (or the host
   classification) on metrics. The exact code (1 to 128 ASCII
   `[A-Za-z0-9._\-:/]`, validated by `merezha-domain`) appears only in the
   span attribute `merezha.guest.error_code`.
4. Unknown tags or names (`handler_not_found`, `projection_not_found`) record
   `_unknown`. Methods outside the HTTP registry record `_OTHER`. Unmatched
   routes omit `http.route`.
5. Each instrument has a documented series budget in `names.rs`. For example,
   for `merezha.command.duration` the budget is `tags × 6 outcomes × buckets`,
   and for `merezha.notification.deliveries` it is
   `topics × 4 channels × 9 statuses × reasons`.
   The SDK cardinality limit stays at its default of 2000 attribute sets per
   instrument. Overflow lands in the `otel.metric.overflow=true` series, and a
   starter alert fires on it (§7). No unstable view features are enabled.
6. Spans may carry high-cardinality allowlisted ids (§1.5). Metrics never do.

### 1.5 Attribute allowlist and redaction

Allowlist, not denylist. The `allowlist.rs` table is the only source of truth.
A key not in the table is dropped at export.

| Key | Span | Metric | Log | Source / rule |
| --- | --- | --- | --- | --- |
| semconv HTTP keys above | yes | yes | no | edge; `http.route` from axum `MatchedPath` only |
| every metric attribute named in §1.3, in every block (`merezha.outcome`, `merezha.reason`, tags, names, `merezha.module`, `merezha.provider*`, `job.*`, `identity.*`, `notify.*`, `content.*`) | yes | on its instruments | yes | closed sets per §1.4 |
| `merezha.link.reason` | links only | no | no | fixed enum |
| `merezha.request_id`, `merezha.correlation_id`, `merezha.causation_id`, `merezha.message_id` | yes | never | yes | validated `Identity` |
| `merezha.stream.id`, `merezha.event.position`, `merezha.events.count`, `merezha.command.expected_version` | yes | never | yes | application |
| `merezha.principal.kind` (`user`, `api_key`, `system`, `anonymous`) | yes | no | yes | edge |
| `merezha.tenant.id` | yes when `record_tenant_id` (default true) | never | same | opaque id; identity's organization id is this key |
| `user.id` (opaque subject) | yes when `record_subject_id` (default false) | never | same | pseudonymous personal data, so off by default (TE1); identity's user id is this key |
| `client.address` | yes when `record_client_address` (default false); not recorded by the T2 edge yet, so the switch has no effect | never | same | personal data |
| `merezha.guest.*`, `merezha.catalog.generation` | yes | listed instruments only | yes | runtime |
| `merezha.projection.*`, `merezha.reactor.*` | yes | listed | yes | application |
| `db.*`, `merezha.db.*` (no `db.query.text`, no parameters) | yes | listed | no | statement registry |
| `messaging.system`=`merezha`, `messaging.operation.type`, `messaging.destination.name`, `messaging.message.id` (job id) | yes | no | yes | jobs |
| `job.id`, `job.attempt`, `job.max_attempts`, `job.code`; `identity.error_code`, `identity.permission` | yes | never | yes | jobs; identity (registered codes, catalog permissions) |
| `notify.notification_id`, `notify.delivery_id`, `notify.webhook_delivery_id`, `notify.endpoint_id` (never its URL), `notify.attempt`, `notify.generation`, `notify.target_count`, `notify.locale`, `notify.resumed`; `content.entry_id`, `content.asset_id`, `content.revision`, `content.locale`, `content.job` | yes | never | yes | notifications; content |
| `error.type` | yes | yes | yes | host code or SQLSTATE, never `Display` text |

Identity's user and organization ids are the `user.id` and `merezha.tenant.id`
keys; there is no `identity.session_id`, because session ids and K's
`SessionFacts` stay out of telemetry (R9, R13). System principals (`sys_<name>`,
R13; reactors are `sys_reactor_<name>`) record `merezha.principal.kind =
system`; reactor spans carry `merezha.reactor.name` instead of the subject.

Never recorded on any signal: request or response headers (`Authorization`,
`Cookie`, `Set-Cookie`, CSRF tokens, API keys); bodies, guest input or output,
command payloads, query results; passwords, tokens, magic-link URLs, session
ids, recipient addresses, phone numbers, push endpoints, template data,
`NotificationRequest.secret_data` (R2), content field values, filenames,
slugs and filter values; SQL text with values; PostgreSQL error messages (they quote row values);
`Display` of foreign errors; environment values; secrets of any form.

Three walls enforce this:

1. **Typed recording.** Attributes are set only through constructors in
   `names.rs` (`attr::route(&MatchedPath)`, `attr::outcome(Outcome)`). No
   public API takes an arbitrary key/value pair. Review rejects `%err` and
   `?value` fields in `tracing` macros. The span bridge runs with
   `with_error_fields_to_exceptions(false)`,
   `with_error_records_to_exceptions(false)`, `with_location(false)`,
   `with_threads(false)`.
2. **Export allowlist.** `AllowlistSpanProcessor` wraps the batch processor:
   `on_end` owns the `SpanData`, drops keys missing from the table (counted in
   `merezha.telemetry.attributes.dropped`) and truncates strings to 256 bytes,
   suffix `…`. `SpanLimits`: 64 attributes, 32 events, 8 links. Log records
   cannot be edited in an SDK 0.33 processor (`SdkLogRecord` exposes no
   removal), so logs are guarded before the bridge: the OTLP log layer accepts
   only `merezha*` targets, whose events come from the catalog macro with
   fixed field sets (checked by the catalog test, §8), and the JSON formatter
   applies the table and a 512-byte cap itself.
3. **Third-party events.** In the JSON stream, events from crates outside
   `merezha*` keep only `target`, `level` and the fixed name
   `dependency.log`; the message is dropped. The exporter's own crates
   (`opentelemetry*`, `tonic*`) are refused from stderr altogether (T1
   review: with the collector down they repeated per batch, half at error);
   the transport crates the server shares with the exporter (`hyper`, `h2`,
   `tower`, `reqwest`) are kept at `warn` and above, except on the
   exporter's threads (the SDK's processors and reader, the blocking
   OTLP/HTTP client, the gRPC runtime); `telemetry.export_failed` reports export
   failures, at most once per signal per minute. No third-party event enters
   the OTLP log bridge, so export errors cannot loop back into the exporter.

## 2. Context propagation

### 2.1 HTTP edge

The kernel owns the edge middleware; this block supplies its body, which runs
before routing.

1. Parse `traceparent` (version `00`, 55 bytes) and `tracestate` (at most 512
   bytes, 32 members) with `TraceContextPropagator`. Malformed headers are
   ignored.
2. Decide trust with `telemetry.traces.trust_inbound`: `trusted_proxies`
   (default) honors the inbound context only when the TCP peer is in
   `http.trusted_proxies` or loopback; `always` honors every context; `never`
   ignores them.
3. Trusted: the server span is its child and the sampled flag is honored.
4. Untrusted or absent: new root, ratio sampler. An untrusted context present
   becomes a span link (`merezha.link.reason=untrusted_parent`), so public
   clients cannot force 100% sampling but their trace stays joinable.
5. Responses carry `traceparent` only to trusted callers; everyone gets
   `x-request-id`.

Outbound HTTP never injects `traceparent` by default: Brevo, OIDC providers
and customer webhooks are third parties. `telemetry.propagation.outbound_hosts`
lists hosts that do receive it (the operator's own services).

### 2.2 Application metadata

`Metadata.correlation_id` is a business workflow id; the trace id is a
transport id. They are linked, not merged.

- **Caller-supplied** correlation ids are kept as-is and recorded as
  `merezha.correlation_id` on the dispatch span and every log event in scope.
- **Host-minted** correlation ids (HTTP adapter when the caller omits one,
  jobs, reactions, cron) are the current trace id as 32 lowercase hex, a valid
  `Identity`. For host-originated workflows the correlation id pastes straight
  into trace search. `message_id` is 128 random bits in hex from the
  AppContext CSPRNG.
- **Commands from reactions or jobs** inherit `correlation_id` from the causing
  event; `causation_id` is the causing `event_id` (or the job's `message_id`).
- **Guests** see `Metadata` exactly as today (protocol 2 adds only the
  principal). No trace field crosses into a guest: replies must stay
  deterministic under replay, and trace ids differ per attempt.
- **Events.** `merezha_app.events` has host-only nullable columns
  `trace_parent text NULL CHECK (octet_length(trace_parent) = 55)` and
  `trace_state text NULL CHECK (octet_length(trace_state) <= 512)`. AP's A2 migration
  creates them and owns the DDL; this block owns the names, the format and
  the propagation helpers (R10). `append()` fills them from the current span,
  the command span. They are not part of `StoredEvent` and never reach guests.
  Projection and reactor spans link (at most 8 distinct) to the source traces
  in their batch (`merezha.link.reason=source_event`). A post-commit catch-up
  inside the request is a child of the command span instead.

### 2.3 Outbox and jobs

- `merezha_jobs.outbox` and `merezha_jobs.jobs` carry `trace_parent text
  null` (55 bytes) and `trace_state text null` (at most 512). AP's B1
  migration creates them and owns the DDL, with these column names (R10).
- `outbox.append(tx, source, records, on_duplicate)` (R1) opens one
  `send {queue}` or `send {topic}` PRODUCER span per record inside the
  caller's transaction and stores its context, sampled or not: the flags
  travel in `traceparent`, so the sampling decision stays consistent
  downstream. `JobsHandle::enqueue` stores the current span's context the same
  way. The relay copies a record's stored context into every job it fans out.
- Each attempt opens `process {queue}`, a CONSUMER child of the stored
  context, with `job.id`, `job.attempt`, `job.max_attempts`,
  `messaging.message.id` (job id), `job.kind`, `job.outcome`. Retries continue
  the original trace, even hours later, as sibling spans (R10).
- Dead-letter replay (an operator action) starts a new trace with
  `admin job.replay` (a child of the admin request's span); its attempt
  spans link to the stored context (`merezha.link.reason=replay`) instead
  of being its children. The replay moves the job's stored context to
  `replay_trace_parent`/`replay_trace_state` (`merezha_jobs` migration
  0004, TE-T5, same formats) and stores the `admin job.replay` span's
  context, so the attempts after a replay are children of the replay span
  and every one of them links back to the trace the job died in. A
  reissued flagged job (no payload, R3) runs its hook in a replay span with
  the same link: the single replay's span, or one child `admin job.replay`
  per reissue in a bulk replay.
- Only inserted records and new jobs get a `send` span; duplicates write
  nothing and get none. A `tracestate` the columns would refuse is dropped,
  keeping the parent.
- Cron runs have no producer: each is a root with the ratio sampler and a
  host-minted correlation id. Commands issued by jobs follow §2.2 and are
  children of the job span.

### 2.4 Guests and blocking threads

Guest and dispatch work runs in `spawn_blocking`, where two thread-local
states would be lost: the `tracing` dispatcher (matters in tests) and the
OpenTelemetry context (activated on span enter).
`merezha_telemetry::blocking::spawn_blocking_in_span(f)` captures
`Dispatch::current()` and `Span::current()` and re-enters both in the closure.
Every `tokio::task::spawn_blocking` in the CLI and application paths moves to
it, and a clippy `disallowed-methods` entry bans the raw call outside
`merezha-telemetry`.

The host span `guest {service} {operation}` wraps `execution::call`,
instantiation included, and ends with fuel consumed and limit, peak memory and
limit, input and output bytes, `merezha.guest.outcome` and
`merezha.catalog.generation`. Guests have no imports, so nothing else needs
propagating.

## 3. Configuration: `[telemetry]`

This block extends the kernel's `[telemetry]` section, which already holds
`log_level` and `format` for the stderr log stream (R12, P3); there is no
`[telemetry.stderr]` table. Precedence and secrets follow `ProjectConfig`:
every key is overridable as `MEREZHA_TELEMETRY__<KEY>` (nested:
`MEREZHA_TELEMETRY__<TABLE>__<KEY>`), and unknown keys fail startup. Keys
ending `_ms` take K's duration type: integer milliseconds or `"500ms"`,
`"30s"`, `"15m"`. Standard `OTEL_*` variables are not read: Merezha sets every
SDK option programmatically, which overrides the environment, and logs
`telemetry.otel_env_ignored` with the variable names when any is present.
There is no `enabled` key (R12): `signals = []` turns OTLP export off.
OTLP export is opt-in (owner decision during T1 review, recorded in R12):
unless `endpoint`, a per-signal `endpoint` or `signals` is set in the file,
the environment or a flag, `signals` defaults to `[]`, so a plain
`merezha run` starts no exporter; configuring an endpoint enables all three.

```toml
[telemetry]
log_level = "info"                  # kernel key: stderr filter
format = "json"                     # kernel key: "json" | "text", written to stderr
log_buffer_lines = 8192             # bounded, lossy stderr writer
endpoint = "http://127.0.0.1:4318"  # base URL; scheme http|https; no userinfo, query or fragment
protocol = "http/protobuf"          # "http/protobuf" (default) | "grpc"
compression = "gzip"                # "gzip" | "none"
timeout_ms = 10000                  # per export request, 1000..=60000
headers = { authorization = { env = "OTLP_AUTHORIZATION" } }  # every value is a secret
ca_file = "/etc/ssl/private-ca.pem" # optional extra trust root for https endpoints
allow_plaintext_headers = false     # see validation
shutdown_timeout_ms = 5000          # total flush budget at shutdown, shared by signals
signals = ["traces", "metrics", "logs"]  # default when an endpoint is set, else []; []: no OTLP pipeline

[telemetry.resource]
service_name = "<app.id>"           # default app.id; "merezha" in raw runtime mode
service_version = "<app.version>"   # default app.version, else "unknown"
service_namespace = ""              # optional
environment = ""                    # deployment.environment.name; optional
# service.instance.id is always app.instance_id (R12; default hostname plus pid)
attributes = { "cloud.region" = "eu-west-3" }  # static strings, at most 16, keys from an allowlist prefix set

[telemetry.traces]
sampler = "parent_based_ratio"      # "parent_based_ratio" | "always_off"
ratio = 1.0                         # 0.0..=1.0, root decisions only
trust_inbound = "trusted_proxies"   # "trusted_proxies" | "always" | "never"
endpoint = ""                       # optional full URL override for this signal
max_queue = 2048                    # spans held for export (queued plus in flight)
max_batch = 512                     # <= max_queue
schedule_delay_ms = 5000

[telemetry.metrics]
interval_ms = 30000                 # >= 1000
temporality = "cumulative"          # "cumulative" | "delta"
sample_interval_ms = 15000          # database-backed gauges (jobs, outbox, head)
endpoint = ""

[telemetry.logs]
level = "info"                      # minimum level for the OTLP bridge
endpoint = ""
max_queue = 2048
max_batch = 512
schedule_delay_ms = 1000

[telemetry.privacy]
record_tenant_id = true
record_subject_id = false
record_client_address = false

[telemetry.propagation]
outbound_hosts = []                 # exact host names that receive traceparent
```

Endpoint resolution: `http/protobuf` appends `/v1/traces`, `/v1/metrics` and
`/v1/logs` to `endpoint`, as OTLP specifies for a base endpoint; a per-signal
`endpoint` is used verbatim. `grpc` uses `endpoint` as the channel URI
(`http://collector:4317`). An `https` scheme selects TLS: rustls with
`aws-lc-rs`, native roots, plus `ca_file` when set.

`http/protobuf` is the default because the OTLP specification recommends it,
it passes ordinary HTTP proxies and load balancers, every managed backend
accepts it, and it runs on the SDK's thread-based processors with the blocking
reqwest client, so the exporter never touches the request runtime. gRPC is
compiled in and selectable without rebuilding.

Validation (all errors reported together, per `ProjectConfig`): `ratio` in
`[0,1]`; `max_batch <= max_queue <= 65536`; `interval_ms >= 1000`;
`timeout_ms` and `shutdown_timeout_ms` in range; header names lowercase RFC
9110 tokens, `content-type` and `te` rejected; `endpoint` parses and has no
userinfo; `headers` with an `http://` endpoint whose host is not loopback is
refused unless `allow_plaintext_headers = true`, so secrets never travel in
clear by accident; `signals` names only `traces`, `metrics` and `logs`.
`Debug` and `Display` print header values as `<secret>`.

Behavior when the collector is down or slow:

- **Requests never wait on telemetry.** Span end and log emit enqueue with
  `try_send` and drop when full; metrics aggregate in memory; every exporter
  runs on an SDK-owned thread. A probe against the pinned crates confirmed
  it: against a blackhole collector (accepts, never replies), 300 000 spans
  took under 80 ms, the worst single span 84 µs (HTTP) and 21 µs (gRPC), RSS
  grew under 2 MiB, and shutdown returned at its 1 s timeout.
- **Bounded queues, exact drop counters.** The SDK counts drops privately and
  logs only the first, so `GuardedSpanProcessor` and `GuardedLogProcessor`
  track `pending` (queued plus in flight). A counting exporter decrements it
  per finished batch and counts the batch exported or failed. `on_end` and
  `emit` refuse items once `pending >= max_queue` and increment
  `merezha.telemetry.dropped{reason=queue_full}`. The inner SDK queue is sized
  `max_queue + max_batch`, so it never drops silently. Memory stays near
  `max_queue × item size`: under 8 MiB per signal at the defaults with
  64-attribute spans.
- **Retries.** The OTLP exporter retries 3 times (100 ms to 1.6 s backoff,
  honoring `Retry-After` and gRPC `RetryInfo`), then the batch counts as
  `export_failed`; the queue keeps filling and dropping meanwhile.
- **Diagnostics.** At most one `telemetry.export_failed` warning per signal
  per 60 s, to stderr only, with `error.type` and the cumulative drop count.
- **Startup and readiness** never depend on the collector; exporters connect
  lazily (§5).
- **Shutdown** flushes traces, logs, then metrics within `shutdown_timeout_ms`
  in total; anything left counts as `dropped{reason=shutdown}`.

Sampling: the default ratio 1.0 suits one application per host at modest
traffic; the operations guide recommends 0.05–0.2 above about 50 requests per
second, with collector tail sampling for errors. Metrics are never sampled.

## 4. Logs

One `tracing` registry carries three layers, each with its own
`tracing_subscriber::filter::Targets` filter. There is no regex-based
`EnvFilter`.

1. **JSON writer to stderr**, not stdout (owner answer P3, a framework
   amendment under R15): this keeps the documented `listener.ready` contract
   and the existing process tests, and container runtimes collect both
   streams. Writes go through `tracing-appender` `non_blocking` in lossy mode
   with `log_buffer_lines`, so a slow log pipe cannot block a request;
   `ErrorCounter::dropped_lines()` feeds
   `dropped{signal=stderr}`. A custom `FormatEvent` emits flat fields
   compatible with today's `merezha_cli::emit` (the stock JSON formatter nests
   fields and omits the event name):

   ```json
   {"timestamp_unix_ms":1761220000000,"level":"info","event":"listener.ready",
    "service":"tasks","version":"0.1.0","instance":"6f1c…","trace_id":"4bf9…",
    "span_id":"00f0…","request_id":"…","correlation_id":"…","address":"127.0.0.1:8080"}
   ```

   `service` is `service.name` (`"merezha"` in raw mode, as today); `version`
   is the host version; `trace_id` and `span_id` appear inside a recording
   span; the allowlisted scope fields `request_id`, `correlation_id`,
   `causation_id` and `message_id` are flattened in. `format = "text"` uses
   the stock compact formatter for local terminals.
2. **Span bridge**: `tracing-opentelemetry` with context activation on (its
   default), so log records inherit the active trace context.
3. **OTLP log bridge**: `opentelemetry-appender-tracing`. `EventName` is the
   tracing event `name`; severity maps ERROR=17, WARN=13, INFO=9, DEBUG=5,
   TRACE=1; attributes are the allowlisted fields; the body is the constant
   message, equal to the event name, so Loki lines are never empty.

Events are emitted only through `merezha_telemetry::event!(LEVEL,
"area.noun_verb", field = …)`, which expands to
`tracing::event!(name: "…", LEVEL, …, "…")`. The `names.rs` catalog lists each
event with its level and field set; a test fails on any emitted event missing
from it (§8). JSON lines keep short field names (`queue`, `code`); in OTLP log
records the catalog maps each field to its attribute key (`queue` becomes
`job.queue`, `topic` becomes `notify.topic`).

Stable event names (existing ones kept):

| Event | Level | Fields |
| --- | --- | --- |
| `startup.failed` | error (today it is `info`, which is a defect this block fixes) | `code` |
| `startup.completed` | info | `duration_ms`, `generation` |
| `listener.ready` / `listener.draining` / `listener.stopped` | info | `address` (+ `generation` in raw `serve` mode) / none / `code` |
| `registry.reloaded` / `registry.reload_failed` | info / warn | `generation`, `code` |
| `invocation.completed` (raw runtime) | debug if `ok`, warn for limit codes, error for `internal` | `request_id`, `generation`, `duration_ms`, `input_bytes`, `output_bytes`, `code` |
| `http.request.completed` | error for 500; info for other 5xx and for `duration_ms >= 1000`; debug otherwise | `method`, `route`, `status`, `duration_ms` |
| `command.completed` / `query.completed` | debug; warn for `failed` | `tag`, `outcome`, `code`, `duration_ms` |
| `projection.caught_up` / `projection.failed` | debug / warn | `projection`, `from`, `to`, `code` |
| `migration.applied` / `migration.pending` / `migration.checksum_mismatch` | info / warn / error | `module`, `version` |
| `db.pool_timeout` / `db.unavailable` | warn / error | `code` |
| `db.pool_undersized` (startup, a PostgreSQL application whose pool is below AP §A.8's guidance) | warn | `pool_size`, `recommended` |
| `db.listener_lost` / `db.listener_restored` | warn / info | `code` |
| `outbound.failed` | warn | `provider`, `operation`, `code` |
| `telemetry.export_failed` / `telemetry.otel_env_ignored` | warn / info | `signal`, `code`, `dropped` / `names` |
| `health.not_ready` | warn, once per state change | `check`, `code` |
| `shutdown.started` / `shutdown.completed` | info | `drain_ms` |
| `runtime.loaded` (kernel host) | info | `raw_runtime`, `application` |
| `error.registration_refused` (kernel, startup) | error | `module`, `code`, `reason` |
| `db.connected` | info | `pool_size` |
| `migration.checked` / `migration.refused` | info / error | `mode`, `applied`, `pending` / `code`, `module`, `version` |
| `migration.invalid_index_dropped` (non-transactional step retry, R15) | warn | `module`, `version` |
| `module.started` / `module.stopped` / `module.start_failed` / `module.shutdown_timeout` | info / info / error / warn | `module` (+ `publishes`, `consumes` on start) |
| `module.source_refused` (kernel, startup) | error | `module`, `sources` |
| `listener.drain_timeout` | warn | `drain_ms` |
| `health.ready` | info, once per state change | none |
| `notification.captured` (capture fallback without the notifications block) | warn | `source`, `kind`, `topic`, `recipients` (kinds only), `delivered` |

Block events (AP's former `jobs.*` events are `job.*`):

| Event | Level | Fields |
| --- | --- | --- |
| `projection.warmed` / `projection.rebuild_completed` / `projection.rebuild_failed` | info / info / warn | `projection`, `revision`, `checkpoint`, `code` |
| `reactor.halted` (malformed reply, or permanent failure with `on_error = "halt"`) / `reactor.skipped` / `reactor.effect_rejected` | error / info / warn | `reactor`, `position`, `index`, `reason`, `code` |
| `job.leased` / `job.completed` | debug | `job_id`, `queue`, `kind`, `attempt` |
| `job.released` (a handler gave the job back, `JobFailure::release`, CT-C3) | info | `job_id`, `queue`, `kind`, `attempt` |
| `job.retry_scheduled` / `job.dead_lettered` / `job.lease_lost` | warn / error / warn | `job_id`, `queue`, `kind`, `attempt`, `code`, `classification` |
| `job.replayed` / `job.cancelled` / `job.admin_action` / `job.drain_abandoned` | info / info / info / warn | `job_id` or `count`, `queue`, `action` / `count`, `job_ids` (at most 64) |
| `outbox.relayed` | debug | `relayed`, `unrouted` |
| `outbox.quarantined` | warn | `outbox_id`, `source`, `code` |
| `outbox.relay_failed` (a systemic relay failure, once per failure streak; nothing is quarantined) | error | `code`, `classification` |
| `job.subscription_sync_failed` (the start-up subscription sync, once per failure streak) | warn | `code`, `classification` |
| `job.worker_failed` (a worker statement failing: `lease`, `settle`, `heartbeat`, `reaper`, `retention`, `lag`, `release`, the cron tick `scheduler`, or the gauge `sampler`; once per failure streak, except a due schedule this version cannot parse, logged per tick with `schedule`) | warn | `task`, `code`, `classification`, `schedule` |
| `job.schedule_sync_failed` (declaring the instance's schedules, at start and on each heartbeat; once per failure streak) | warn | `code`, `classification` |
| `job.schedule_fired` (one occurrence enqueued its job) | debug | `schedule`, `job_id`, `scheduled_for` |
| `job.schedule_conflict` (this instance declares another definition for a schedule whose stored definition has precedence: a newer `app.version`, or declarers still alive; once per streak) | warn | `schedule`, `version` |
| `job.queue_unconfigured` (a registered kind's queue missing from `[jobs.queues]`, at worker start) | warn | `queue` |
| `job.reissue_claim_lost` (a reissue hook returned after the reaper reverted its claim; its settle wrote nothing) | warn | `job_id`, `hook` (`succeeded`, `failed`) |
| `identity.login_failed` | debug (the metric and the audit log carry the signal) | `method`, `outcome` |
| `identity.grants_truncated` (grants past `max_resource_assignments`) / `identity.audit_write_failed` | warn / error | `count` / `code` |
| `identity.user_grant_tenant_route` (startup: `builtin_grants.user` grants a permission a route checks at `scope = "tenant"`, AP-C2) | warn | `permission`, `route` |
| `identity.dev_link` | info, stderr only, never bridged to OTLP | `purpose`, `link`; emitted only with `dev_print_links = true` on loopback, the one catalog event that carries a secret |
| `notification.requested` / `notification.duplicate` / `notification.invalid` / `notification.rejected` (expansion) | debug / debug / warn / warn | `topic`, `source`, `notification_id`, `code` |
| `notification.accepted` / `notification.skipped` / `notification.retry_scheduled` / `notification.ambiguous` / `notification.expired` | debug / debug / warn / warn / warn | `channel`, `topic`, `delivery_id`, `attempt`, `reason`, `code` |
| `notification.failed` | warn (transient) / error (permanent after retries) | `channel`, `topic`, `delivery_id`, `code` |
| `notification.provider_rate_limited` / `notification.provider_account_error` | warn / error | `provider`, `channel`, `code` |
| `notification.provider_event_applied` / `notification.provider_event_uncorrelated` / `notification.provider_event_rejected` | debug / info / warn | `provider`, `channel`, `event`, `code` |
| `notification.webhook_endpoint_disabled` / `notification.egress_blocked` / `notification.push_subscription_removed` | warn / warn / info | `endpoint_id`, `target`, `reason` |
| `notification.sse_listen_lost` / `notification.sse_listen_restored` / `notification.template_reload_failed` | warn / info / warn | `code` |
| `content.model_loaded` / `content.model_activated` / `content.model_tightened` / `content.model_breaking` | info / info / warn / error | `version`, `fingerprint`, `types`, `locales` / `previous_version`, `change_classes` / `content_type`, `counts`, `truncated` / `path`, `change`, `code` |
| `content.migration_batch` / `content.index_created` / `content.index_dropped` | info | `type`, `rows` / `index`, `content_type`, `field`, `table`, `purpose` / `index`, `reason` |
| `content.index_failed` (a derived index build that stops activation, CT-C2) | error | `index`, `content_type`, `field`, `code`, `cause`, `attempts` |
| `content.rebuild_batch` / `content.rebuild_completed` (the background `published` rebuild, CT-C4) | info | `version`, `entries`, `done` / `version`, `entries` |
| `content.schedule_failed` | warn | `entry_id`, `action`, `code` |
| `content.blob_cleanup` / `content.spool_cleared` | info | `driver`, `count` |
| `content.organization_deleted` / `content.organization_purged` (issue #56, deleted organizations; also logged by `merezha content purge-organization`) | info | `tenant`, `source` (`event`, `operator`), `forced` / `tenant`, `entries`, `assets`, `blobs`, `forced` |
| `content.organization_purge_failed` (a purge job that will not run again by itself: permanent failure or last attempt; alert on it) | error | `tenant`, `code`, `attempt` |
| `content.purge_failed` (`merezha content purge-organization` cannot run) | error | `code`, `usage` |

Severity policy:

| Level | Meaning |
| --- | --- |
| error | The host broke its own contract, or an operator must act: internal 500, DB unavailable, migration mismatch, dead letter, permanent delivery failure, startup failure. |
| warn | Degraded but self-healing, or a tuning signal: transient failure with retry scheduled, pool timeout, guest fuel, memory or time limit, export failure, readiness lost. |
| info | Lifecycle and operator actions. |
| debug | Per-request and per-message success, client errors. |
| trace | Never enabled in release builds (`release_max_level_debug`). |

Client-caused outcomes (4xx, rejections, denials, failed logins) are never
warn or error. Audit events stay in identity's audit table, apart from
diagnostic logs, as the framework requires.

## 5. Health

| Endpoint | 200 when | 503 when | Body |
| --- | --- | --- | --- |
| `/health/live` | the async runtime answers the request | never set deliberately; a wedged runtime times out, which is the signal | `{"status":"live"}` (K's body, kept by T2) |
| `/health/startup` | startup is complete: config valid, migrations applied or verified, catalog compiled, every module `start()` returned | before that point | `{"status":"started"}` / `{"status":"starting"}` |
| `/health/ready` | started, not draining, the cached checks pass | otherwise | `{"status":"ready"}` / `{"status":"not_ready"}` |
| `/health` (raw runtime contract) | unchanged | unchanged | `{"status":"ok","generation":N}` |

`/health/startup`, the early listener, the admin status route and the stderr
log stream are accepted framework amendments (P1, R15).

Refinements:

- **Early listener** (project mode, `merezha run`; raw `serve` keeps its
  current order). The listener binds right after configuration validation
  and emits `listener.ready`. Until startup completes it serves only
  `/health/*`; other routes answer
  `503 {"error":{"code":"starting","classification":"transient"}}` with
  `Retry-After: 1`. T2 registers `starting` (503) in K's error registry (R8).
  A startup probe can then cover long migrations instead of the orchestrator
  killing the process. `startup.completed` marks the switch.
- **Readiness checks** run in a 2-second background task and probes read the
  cached result, so probes never hit the database: pool acquire plus
  `SELECT 1` within 1 s, migrations current, an active catalog generation,
  every module's `ready()`. Module checks run concurrently, each isolated on
  a blocking thread and bounded by 1 s (a panic is `internal`, a stuck check
  `timeout`). An evaluation older than interval + 3 × 1 s + 1 s (6 s by
  default) counts as not ready: that is the worst-case staleness of a probe
  answer, and a refresh that stops can never leave readiness at 200. A module may hold readiness for a bounded
  warm-up (R10): AP while projection revisions warm
  (`application.projections.warm_readiness`), content while its concurrent
  index builds run (R15).
- **Never in readiness:** the OTLP collector; Brevo, Web Push, webhook
  targets and OIDC providers (an outbound dependency degrades a feature, not
  the instance). Projection lag and job backlog are alerts, not routing.
  The one opt-in is `jobs.readiness.max_lag_ms` (AP key, unset by default):
  when set, readiness fails while `merezha.job.lag` exceeds it (R10).
  Projection lag past warm-up never enters readiness.
- **Draining.** SIGTERM flips readiness to `not_ready` at once; the host waits
  `http.drain_delay_ms` (a kernel `[http]` key T2 adds, default 5000) for
  load balancers, stops accepting, drains in-flight requests under the
  existing bounds, stops the job runner, then flushes telemetry. Liveness
  stays 200 throughout.
- **No public detail.** The failing check name and code go to the
  `health.not_ready` log and to the status endpoint only.

### `/v1/admin/status`

With identity enabled, it requires a principal holding `system.status.read`,
an `Application` scope permission (R5) this block registers; `platform_admin`
also passes. Failures are `401 unauthenticated` and `403 unauthorized` (R8).
Without identity, it is served only when the actual TCP peer is loopback and
`http.public = false` (the `AllowAll` trust model); otherwise it is 404. One
request runs at a time (a concurrent one gets `429 rate_limited` with
`Retry-After: 1`), queries use a 2 s statement timeout, and responses carry
`Cache-Control: no-store`. Every field is a code, a count, a configured name
or a timestamp (`jiff`, R14). The `application` and `jobs` sections embed, as
returned, AP's `Application::status()` and `JobsHandle::queue_status()`
documents (the bodies of `GET /v1/application/status` and
`/v1/jobs/admin/queues` and `outbox`); this endpoint never queries AP's tables
itself (R10), and omits a section whose subsystem is not configured.

```json
{"service": {"name": "tasks", "version": "1.4.0", "host_version": "0.1.0", "instance_id": "…", "started_at": "…", "uptime_s": 3600},
 "startup": {"state": "started", "completed_at": "…", "duration_ms": 812},
 "readiness": {"ready": true, "draining": false, "checks": [{"name": "database", "ok": true, "code": "ok", "duration_ms": 3, "checked_at": "…"}]},
 "database": {"pool": {"max": 16, "size": 6, "idle": 4, "waiting": 0}, "migrations": [{"module": "app", "applied": 7, "pending": 0}]},
 "catalog": {"generation": 3, "services": 4, "loaded_at": "…"},
 "application": {"application": "tasks", "head": 18234, "listener": "connected", "instance": {"id": "…", "background_runs": 1},
                 "projections": [{"name": "tasks.board", "checkpoint": 18230, "lag_events": 4, "lag_seconds": 0.4, "status": "active", "failure_code": null}],
                 "reactors": [{"name": "task-notifier", "status": "active", "lag_events": 0}]},
 "jobs": {"queues": [{"name": "notify.email", "ready": 0, "delayed": 2, "leased": 1, "dead": 0, "oldest_ready_age_s": 0, "concurrency": 8, "global_concurrency": 10}],
          "outbox": {"pending": 0, "oldest_age_s": 0, "unrouted": 0}},
 "telemetry": {"enabled": true, "protocol": "http/protobuf", "endpoint": "https://otlp.example.com:443", "sampling_ratio": 0.1,
               "signals": {"traces": {"exported": 120344, "failed": 0, "dropped": 0, "last_success_at": "…", "last_code": "ok"}}},
 "modules": [{"name": "identity", "ready": true, "code": "ok"}]}
```

`telemetry.endpoint` shows the scheme, host and port only; header values never
appear. `modules` lists the modules whose section is present (R12).

## 6. Local development and production backends

The local backend is a Contremaitre-managed `grafana/otel-lgtm:0.38.0`
container, pinned; the tag was checked on Docker Hub on 2026-10-23 and ships
amd64 and arm64. One container runs an OpenTelemetry Collector, Tempo,
Prometheus, Loki and Grafana.

The project scaffold (`merezha new`) writes `ops/telemetry/lgtm.Dockerfile`:

```dockerfile
FROM grafana/otel-lgtm:0.38.0
# Dashboard provisioning path documented by the image README ("Add custom dashboards").
COPY dashboards/ /otel-lgtm/grafana/conf/provisioning/dashboards/merezha/
COPY dashboards.yaml /otel-lgtm/grafana/conf/provisioning/dashboards/merezha.yaml
```

The local image ships dashboards only: the image documents no Prometheus
`rule_files` location, so CI checks the rule files with `promtool check rules`
(T7) and production loads them into its own Prometheus or Mimir ruler.

The `.contremaitre.yaml` additions:

```yaml
services:
  db:
    kind: postgres
  otel:
    build: ops/telemetry
    dockerfile: ops/telemetry/lgtm.Dockerfile
    port: 3000          # Grafana UI, local HTTPS route
    http: true
  app:
    # existing build, port and ready (the ready command probes /health/ready)
    depends_on: [db, otel]
    environment:
      MEREZHA_DATABASE__URL: '{{db.url}}'
      MEREZHA_TELEMETRY__ENDPOINT: 'http://{{otel.host}}:4318'
      MEREZHA_TELEMETRY__METRICS__INTERVAL_MS: '5000'
      MEREZHA_TELEMETRY__RESOURCE__ENVIRONMENT: local
```

The `otel` service declares no `ready` command, which would need an HTTP
client the image does not document; Contremaitre's default port check is
enough, because the app never waits on the collector (§5). Contremaitre's
managed PostgreSQL is 17; the floor stays 14 (R14).

The otel-lgtm image provisions Tempo, Loki and Prometheus datasources, and
trace-to-log correlation uses `trace_id`. Grafana opens on the `otel` URL with
the image's dev credentials (`admin`/`admin`); it is a local preview only.
Without Contremaitre, `docker run -p 3000:3000 -p 4317:4317 -p 4318:4318
grafana/otel-lgtm:0.38.0` works with the host's default endpoint unchanged.

Production points at any OTLP backend (Grafana Cloud, Honeycomb, Datadog
Agent, Elastic, New Relic, a self-run collector) with three keys: `endpoint`,
`protocol` (`grpc` if the backend wants 4317), and
`headers = { authorization = { env = "OTLP_AUTHORIZATION" } }` or the vendor's
key header. A collector that is still starting is harmless: early exports
fail and are counted.

Recommended topology: the host exports to a collector agent on the same node
or in the same pod, over loopback HTTP with no headers. The collector holds
the vendor credentials and does tail sampling, so the host never holds vendor
secrets. Set `temporality = "delta"` for backends that require it, such as
Datadog.

## 7. Dashboards and alerts

Decision: ship importable artifacts, versioned with the host. Grafana
dashboard JSON (schema version 39, `${DS_PROMETHEUS}`, `${DS_TEMPO}` and
`${DS_LOKI}` datasource inputs, so they import into any Grafana) and
Prometheus-format rule files (recording rules plus alerts), which Prometheus,
Mimir and Grafana alerting import all load. They live in
`deploy/telemetry/{dashboards,rules}/`, are embedded in the binary, and
`merezha telemetry export --out DIR` writes them with `dashboards.yaml`. The
scaffold copies them into `ops/telemetry/`. Vendor-native formats (Datadog
monitors) are out of scope; the rule-file PromQL is the reference definition.

Queries assume the Prometheus 3 / Mimir default OTLP translation
(`UnderscoreEscapingWithSuffixes`): `http.server.request.duration` becomes
`http_server_request_duration_seconds_bucket`, counters gain `_total`,
attribute dots become underscores (`job.queue` becomes `job_queue`),
`service.name` becomes `job` and `service.instance.id` becomes `instance`.
Every query filters on `job=~"$service"`.

Five dashboards: **Overview** (request rate, availability, latency
percentiles per route, command outcome split, instances, error logs from Loki,
Tempo search link); **Application** (command and query latency per tag,
projection lag and catch-up, guest fuel utilization heatmap, outcomes, peak
memory, reactor lag, halts and rejected effects); **Data and jobs** (pool
usage, waiters, timeouts, DB p95 per statement, append latency, jobs per
state, job lag, outbox lag, dead letters, attempts); **Delivery, identity and
content** (notification statuses per channel and topic, provider latency,
webhook status classes, sign-in and authentication outcomes, rate-limit hits
per policy, content delivery latency per route, blob errors); **Telemetry
health** (exported, failed, dropped per signal; dropped attributes; overflow
series).

SLOs and alerts. Multi-window burn rates follow the SRE workbook, over a
30-day window:

| SLO / alert | Definition (recording rule) | Fire |
| --- | --- | --- |
| HTTP availability 99.9% | good = `http_server_request_duration_seconds_count{http_response_status_code!~"5.."}`; total excludes `http_route=~"/health.*"` | page: burn > 14.4 over 1h and 5m, or > 6 over 6h and 30m; ticket: > 1 over 3d |
| HTTP latency 99% under 500 ms | good = bucket `le="0.5"` over count, same exclusions | same burn windows |
| Command failure rate | `failed / all` of `merezha_command_duration_seconds_count`; rejections and conflicts excluded | page > 1% over 10m; the rejection rate is a dashboard panel, alerting only at > 50% over 30m (ticket, likely a client bug) |
| Projection lag | `max by (merezha_projection_name) (merezha_projection_lag_age_seconds)` | ticket > 60 s for 10m; page > 300 s for 10m |
| Reactor halted | `max by (merezha_reactor_name) (merezha_reactor_halted) > 0` | page for 5m |
| Outbox lag | `max(merezha_outbox_lag_seconds)` | page > 120 s for 10m |
| Job lag | `max by (job_queue) (merezha_job_lag_seconds)` | ticket > 300 s for 15m |
| Dead letters | `increase(merezha_job_dead_lettered_total[15m]) > 0` | ticket |
| Delivery failures | `notify_status=~"failed\|bounced\|expired"` over all terminal statuses of `merezha_notification_deliveries_total`, per channel | ticket > 5% over 30m; page > 25% over 15m |
| DB pool saturation | `db_client_connection_pending_requests > 0` for 5m, or timeouts increase | ticket |
| Guest limits | `fuel_exhausted`, `memory_limit` or `timeout` outcomes > 1% of invocations over 15m | ticket |
| Instance absent | `absent_over_time(merezha_telemetry_exported_total[10m])` per expected job (the self-metric exists whenever export is on) | page |
| Telemetry loss | `increase(merezha_telemetry_dropped_total[15m]) > 0` or an `otel_metric_overflow="true"` series exists | ticket |

## 8. Test plan

**Harness.** `merezha_telemetry::testing::Harness` (feature `testing`) builds
an `SdkTracerProvider` with `SimpleSpanProcessor(InMemorySpanExporter)` behind
the production allowlist processor, an `SdkMeterProvider` with
`PeriodicReader(InMemoryMetricExporter)` flushed on demand, an
`SdkLoggerProvider` with `InMemoryLogExporter`, and the JSON formatter writing
into a `Vec<u8>`. It installs a scoped dispatcher
(`tracing::dispatcher::set_default`), so tests run in parallel without globals
and `spawn_blocking_in_span` carries it to blocking threads. Helpers:
`spans()`, `span("command tasks.create")`,
`metric("merezha.command.duration").points()`, `logs()`, `stderr_lines()`,
`assert_no_canary()`. Storage tests use `MEREZHA_TEST_DATABASE_URL`.

**Signals and attributes.**

- HTTP server span name, kind and semconv attributes; `http.route` is the
  template, never the raw path; unmatched routes omit it; `PROPFIND` records
  `_OTHER`; `/health/*` gives metrics but no spans.
- Guests (existing WAT fixtures): each `merezha_runtime::Error` maps to its
  `merezha.guest.outcome` and span status; fuel consumed equals limit minus
  remaining; peak memory is at least what the fixture grew; pipeline steps are
  child spans sharing one budget.
- Commands: each of accepted, replayed, rejected, denied, conflict, failed
  yields its outcome; only `failed` sets status `Error`.
- Projection lag gauges read N events and age above 0 after appends without
  catch-up, and 0 after it.
- DB spans carry summary and SQLSTATE on a unique violation; pool gauges match
  `Pool::status()`. Job attempt spans carry attempt numbers; the dead-letter
  counter increments exactly once.
- Event catalog: every event the suite emits must be in the catalog at its
  catalog level. Every block's suite runs the same check, so a name missing
  from `names.rs` fails that block's CI.
- Reactors: a malformed reply sets `merezha.reactor.halted` to 1 and logs
  `reactor.halted` once; a recipient outside the tenant counts `rejected` and
  the checkpoint advances.

**Cardinality.** 10 000 commands with random unknown tags yield at most
`catalog + 1` series for `merezha.command.duration`; a guest returning 10 000
distinct error codes yields one `guest_error` series; webhooks to 1 000 hosts
yield no `server.address` value.

**Redaction (canary).** A unique canary is planted in every input surface:
`Authorization`, `Cookie` and `X-Api-Key` headers, query string, body, command
payload, guest output and error payload, password, email, magic-link token,
session id, `secret_data` and template data of a notification request,
content field values and filenames, a DB unique violation quoting the value,
an outbound provider error body. All
exported spans, metrics and logs, stderr lines and the `/v1/admin/status` body
are scanned; any hit fails. The scan runs with `dev_print_links = false`, the
only mode that ships. The allowlist test checks an injected unknown
attribute is dropped and counted and over-cap strings are truncated. Config
`Debug` output never contains header secrets.

**Collector down (bounded memory, never blocks).** Through the real OTLP
exporters on a multi-thread runtime, both protocols, against a blackhole
listener (accepts, never replies) and a closed port: 1 000 000 spans and
200 000 log events from 8 tasks while serving 10 000 HTTP requests. Assert:
the slowest span end or emit call is under 1 ms; HTTP p99 is within 2× a
baseline run with `signals = []`;
`exported + failed + dropped + pending == emitted` per signal; RSS growth is
under 32 MiB (`getrusage` max RSS, Linux and macOS); shutdown returns within
`shutdown_timeout_ms + 500 ms`. Recovery: a stub OTLP/HTTP receiver (axum plus
`opentelemetry-proto` decoding) starts on the port and exports resume within
`2 × schedule_delay`.

**Propagation.**

- Edge: a trusted peer's traceparent makes the span a child; an untrusted one
  gives a new root with a link and its sampled flag is ignored; a malformed
  header is ignored. A host-minted correlation id equals the trace id hex; a
  caller-supplied one is preserved.
- Across the outbox, on PostgreSQL:
  1. Request A carries a traceparent and issues a command.
  2. A reaction writes an outbox row whose `trace_parent` carries A's trace id.
  3. `process {queue}` is a CONSUMER child of the `send` span, in trace A.
  4. A forced failure gives a second attempt, still in trace A (R10).
  5. Exhaustion dead-letters the job; replay starts a new trace linking to A.
  6. `notify email` and the outbound `POST /v3/smtp/email` are children of the
     attempt span.
  7. No `traceparent` header reaches the fake Brevo server.
- Projection batch spans link to their source command traces. With ratio 0 at
  the origin, the downstream job span is not recorded but the stored flags
  round-trip.

**Health.** `/health/startup` and `/health/ready` return 503 and app routes
return `starting` while a slow migration runs; readiness goes 503 when the
pool is unreachable and recovers; it stays 200 with the collector down;
SIGTERM flips readiness before the listener closes while liveness stays 200.
Readiness stays 503 while a projection revision warms. A 10-minute job
backlog and a projection 10 000 events behind leave it 200; setting
`jobs.readiness.max_lag_ms = 1000` makes the backlog turn it 503.
`/v1/admin/status` returns 403 `unauthorized` without `system.status.read` at
application scope (a tenant-scoped grant is not enough), 404 on a public
listener without identity, 429 on a concurrent request, and passes the canary
scan. Its `application` and `jobs` sections equal AP's admin responses, and
the statement registry shows it issued none of its own queries against
`merezha_app` or `merezha_jobs`.

**Config.** Every key and default; every invalid combination fails, reported
together; plaintext headers to a non-loopback host are refused;
`MEREZHA_TELEMETRY__TRACES__RATIO` overrides the file; `OTEL_*` variables are
ignored and listed by name in `telemetry.otel_env_ignored`.

**CI.** All of the above runs in the existing CI with the PostgreSQL 16
service container; nothing here needs more than the PostgreSQL 14 floor
(R14). A nightly job sends to `otel/opentelemetry-collector-contrib` with
the file exporter over both protocols and checks what arrives.

## 9. Crate choices

The versions below were resolved on 2026-10-23 with `cargo info` and a scratch
crate. They form a single `opentelemetry` 0.33.1 tree, no duplicate
OpenTelemetry versions, and one rustls crypto provider (`aws-lc-rs`, no
`ring`). The tree builds and runs on rustc 1.98.0 alongside
`tokio = "1.53.3"` (resolves to 1.54.1) and `axum = "0.8.9"`.

| Crate | Version | Features (`default-features = false` unless noted) | Used by | MSRV |
| --- | --- | --- | --- | --- |
| `opentelemetry` | `0.33.1` | `trace`, `metrics`, `logs`; libraries use `metrics` only | all | 1.75 |
| `opentelemetry_sdk` | `0.33.1` | `trace`, `metrics`, `logs`, `internal-logs`; dev: `testing` | telemetry | 1.75 |
| `opentelemetry-otlp` | `0.33.1` | `trace`, `metrics`, `logs`, `internal-logs`, `http-proto`, `reqwest-blocking-client`, `reqwest-rustls`, `gzip-http`, `grpc-tonic`, `tls-aws-lc`, `tls-roots`, `gzip-tonic` | telemetry | 1.75 |
| `tracing-opentelemetry` | `0.34.0` | none (no `metrics`, no `tracing-log`) | telemetry (`propagation`, `pipeline`) | 1.75 |
| `opentelemetry-semantic-conventions` | `0.33.1` (schema 1.42.0) | default (stable only); dev: `semconv_experimental` for the pin test | telemetry, postgres | 1.75 |
| `tracing` | `0.1.44` | `std`, `attributes`, `release_max_level_debug` (binary only) | all | 1.65 |
| `tracing-subscriber` | `0.3.23` | the workspace's `registry`, `fmt`, `std`, `json` (no `ansi`: text logs are written without colour) | telemetry | 1.65 |
| `tracing-appender` | `0.2.5` | default | telemetry | 1.63 |
| `reqwest` | `0.13.5` | the workspace's `rustls`, plus `blocking` | telemetry (`pipeline`): the OTLP/HTTP client, built with `ca_file` and without redirects | 1.64 |
| `libc` | `0.2.191` | default; dev only | telemetry tests (`getrusage`, thread counts) | 1.65 |

Transitively: `tonic` 0.14.6 has the highest MSRV in the tree (1.88);
`reqwest` resolves to 0.13.5 and `prost` to 0.14.4. Platform's outbound
client should use the same reqwest 0.13 line with rustls and `aws-lc-rs` to
share one copy. The host installs `rustls::crypto::aws_lc_rs::default_provider()`
at startup so no path can panic for want of a process-level provider.
Timestamps use the workspace time crate, `jiff` (R14); this block adds no
other.

Constraints confirmed in the 0.33.1 sources:

- `BatchSpanProcessor`, `BatchLogProcessor` and `PeriodicReader` run on
  SDK-owned threads, so OTLP/HTTP must use `reqwest-blocking-client` (the
  async clients are documented as incompatible with them). The exporter
  builds its blocking client on a helper thread, so building it inside
  `#[tokio::main]` is safe. The gRPC exporter must be built inside the Tokio
  runtime, which `main` provides.
- `with_max_export_timeout` exists only on the experimental async processor;
  the export timeout is the exporter's `with_timeout(timeout_ms)`.
- SDK drop counters are private, hence the guarded processors (§3).

T1 dropped `opentelemetry-appender-tracing`: it records `tracing` field
names verbatim, and the catalog maps each short field to its attribute key
(`queue` to `job.queue`), so the log bridge is a small layer in
`subscriber.rs` that builds `SdkLogRecord`s itself. The stub receiver of the
tests answers OTLP/HTTP without decoding, so `opentelemetry-proto` is not a
dev dependency either.

The six OpenTelemetry crates move in lockstep: one PR bumps them all and
reruns the `names.rs` semconv pin test.

## 10. Delivery slices

One PR each, per the framework's rules (real PostgreSQL where storage is
touched, docs exercised, independent review, green CI). All depend on K and
K2.

| # | Slice | Acceptance criteria |
| --- | --- | --- |
| T1 | `merezha-telemetry` core: feature split, `names.rs` with every block's rows, config section and validation (extending K's `[telemetry]`), resource from `app.instance_id`, three providers, guarded and allowlist processors, counting exporters, JSON formatter (replaces `merezha_cli::emit`, keeping its field names), event macro and catalog, shutdown flush, test harness | All §8 config tests pass. The collector-down test passes for both protocols within its bounds. Exact drop accounting holds. Existing CLI process tests still find `listener.ready` on stderr with the same fields. `startup.failed` logs at error. The canary test passes for the startup path. With `signals = []`, no OTel threads start. A library built with default features links no SDK exporter. |
| T2 | Edge and health: HTTP server spans and metrics, traceparent trust policy and links, `x-request-id`, early listener with `starting` gating and its registered code, `/health/startup`, cached readiness with module warm-up, drain sequence, `/v1/admin/status` (database, telemetry and modules sections) | The §8 HTTP, propagation-at-edge and health tests pass. Readiness is independent of the collector. Status is gated by `system.status.read` at application scope and passes the canary scan. |
| T3 | Runtime and application: `spawn_blocking_in_span` everywhere plus the clippy ban, guest spans and metrics (peak memory and fuel accounting in `merezha-runtime`), command, query and projection spans and metrics, outcome mapping, lag gauges and sampler, host-minted correlation ids, filling and reading AP's `events.trace_parent`/`trace_state`, projection links, the status `application` section from `Application::status()` (AP-A3) | The guest outcome matrix, the command outcome matrix, the lag test, the correlation tests and the cardinality tests pass. `merezha-runtime` gains no dependency beyond `tracing`, the `opentelemetry` API and `merezha-telemetry` default features. |
| T4 | PostgreSQL: `Statement` registry, DB spans and duration metric, pool gauges (the pool, detached sessions, listener sessions) and timeouts, listener reconnects, migration spans and pending gauge | The DB tests pass. No span contains SQL text or values (canary via unique violation). Every statement in the kernel and the application store is registered. |
| T4a | Registry adoption: every statement of `merezha-jobs` (`merezha_jobs::STATEMENTS`, `jobs.*`), `merezha-content` (`merezha_content::STATEMENTS` and `merezha_content::media::STATEMENTS`, `content.*`) and `merezha content diff` (`content.diff.read_only`, in content's list) runs through the registry, text built at run time through `query_text`, `query_one_text`, `query_opt_text`, `execute_text` or `batch_text`; the per-crate source scans become the one workspace scan with its shrinking allowlist | A refactor: the jobs and content suites pass unchanged, and the server's statement log over each suite holds the same distinct SQL texts before and after. Each crate's list passes `check_registry` beside the kernel's, and every block's lists pass it together (`crates/cli/tests/statement_registry.rs`). The workspace scan finds no unregistered call outside its allowlist, and none in `crates/jobs`, `crates/content` or `crates/cli` except the unmetered `PgOutbox` (two calls). |
| T5 | Jobs and outbox: filling and reading AP-B1's `trace_parent`/`trace_state` columns, `send`, `relay` and `process` spans, every `merezha.job.*` and `merezha.outbox.*` instrument including the gauges and their sampler over `JobsHandle::queue_status()`, replay links, the status `jobs` section; its legacy `event = "…"` calls moved to `event!` | The full outbox propagation test (§8, steps 1–7) passes on PostgreSQL, including the unsampled case. The dead-letter counter is exact. AP-B2 defines no instrument of its own. |
| T6 | Block instrumentation, landing in those blocks' PRs as a checklist owned by this design: `Endpoint` descriptors on the AppContext client and `outbound_hosts` injection; identity, notifications and content rows of §1.3, §1.5 and §4; reactor rows in AP-C3 | Each block PR includes its rows and passes the catalog test and the canary test for its inputs (password, token, session id, recipient, `secret_data`, body, field values). The fake provider receives no `traceparent`. |
| T7 | Local stack and artifacts: `deploy/telemetry/` dashboards and rules, `merezha telemetry export`, the scaffold's `ops/telemetry/`, the Contremaitre manifest for the example application, the operations guide section (production backends, sampling, collector topology) | `promtool check rules` and a dashboard JSON schema lint pass in CI. In the Contremaitre environment, one command produces a trace visible in Tempo with linked logs in Loki and a populated overview dashboard, checked by a verification profile that queries the Tempo and Prometheus HTTP APIs inside the `otel` service. |

Order, per the cross-block delivery order: stage 1 starts T1 alongside AP-A1
and AP-B1 (which carries the R10 columns), then T2 to T5 alongside AP-A2,
AP-B2, AP-B3 and AP-A3 (T3 needs AP-A2 and AP-A3, T4 `merezha-postgres`, T5
AP-B2). T6 rows land inside each block PR as it ships: ID PR 1 in stage 1;
ID PR 2 and PR 3, NT-N2 to NT-N8 and CT-C2 to CT-C8 in stage 2. Stage 3
brings T6's reactor rows in AP-C3, and T7, whose dashboards fill in as
signals land.

**T3 delivery notes.** As built in TE-T3:

- The lag sampler runs every `telemetry.metrics.sample_interval_ms` (the
  §3 key for database-backed gauges, default 15 s) rather than every 5 s,
  and only while metrics are exported. Between samples, local commits and
  catch-ups keep the cache current.
- `merezha.projection.trigger` takes `background` for every scheduler run
  (commit, notification and interval wakes coalesce), `admin` for explicit
  catch-ups and `rebuild`; `commit` and `interval` stay reserved, since no
  catch-up runs inside a request. Explicit catch-ups are roots too.
- A `projection {name}` span covers one stretch of batches up to one target;
  a background run that lingers and is woken again opens another span.
- `merezha.projection.lock.held` reports the older of a projection's live
  and rebuild locks; `merezha.projection.lock.contended` sums both kinds.
  Both read AP-A3's in-memory sources.
- The runtime does not decode domain replies, so the guest's own failure
  codes are recognised in the application: a code out of a guest call that
  the call path does not produce itself is the guest's reply, recorded as
  `merezha.guest.error_code` on the dispatch or projection span, with
  `error.type` and the log `code` (`command.completed`, `query.completed`,
  `projection.failed`, `projection.rebuild_failed`) set to `guest_error`.
  Guest output that does not decode as a protocol response is
  `invalid_response` and counts as `failed`.
- `AppContext::telemetry()` returns `merezha_telemetry::Meters` (default
  features), which the host takes from `HostOptions.meters`.
- `testing::Harness` registers one extra inert dispatcher per process: with a
  single registered dispatcher, `tracing-core` would cache a callsite first
  hit on another test's thread as never.

## 11. Decisions

The owner answered this design's open questions on 2026-10-24.
[Cross-block decisions](cross-block-decisions.md) is the record; this table
only points to it.

| Topic | Decision | Record |
| --- | --- | --- |
| Name catalog | `names.rs` is the only catalog for every block; durations in seconds; module-prefixed attributes; `topic` labels notifications | R9 |
| Log stream | stderr, through K's `[telemetry] log_level` and `format` | P3, R12 |
| Framework deviations | `/health/startup`, the early listener, `/v1/admin/status` and stderr logs are accepted amendments | P1, R15 |
| Personal data | `user.id` off by default, tenant id on, session ids never | TE1, R9 |
| Trace columns | `trace_parent` and `trace_state` on events, outbox and jobs, in AP's A2 and B1 migrations | R10 |
| Job retries | continue the original trace; only operator replay starts a new trace, with a link | R10 |
| Readiness and status | warm-up may hold readiness; job backlog counts only through `jobs.readiness.max_lag_ms`, projection lag never; status reads AP's data under `system.status.read` at `Application` scope | R10, R5 |
| Correlation and message ids | the HTTP adapter fills missing ones | AP4 |
| Instance id and app keys | `app.instance_id` is the one instance id; `app.id` and the optional `app.version` live under `[app]` | R12, internal |
| Defaults | `OTEL_*` variables ignored and logged; parent-based sampling at 1.0; Grafana JSON plus Prometheus rules; collector mTLS and exemplars deferred; one provider set per application | internal |
