# Operating Merezha telemetry

This guide is for the people who deploy and run a Merezha application and
watch it. The reference for every `[telemetry]` key is
[platform.md](../architecture/platform.md#telemetry); the design, with the
name catalog and the reasons behind it, is
[telemetry.md](../design/telemetry.md). This page says where to send the
signals in production, how to sample, how to lay out collectors, and how to
use the dashboards and alerts the host ships.

## What the host sends

- **Logs on stderr, always.** One JSON line per event (`format = "json"`),
  with stable event names from the catalog. Container runtimes collect
  stderr; nothing needs configuring.
- **OTLP, when asked.** Traces, metrics and logs go to an OTLP endpoint
  once `[telemetry] endpoint` (or `signals`) is set. A plain `merezha run`
  exports nothing.
- **Health and status.** `/health/live`, `/health/startup` and
  `/health/ready` for the orchestrator; `GET /v1/admin/status` for people,
  with the exporter counts per signal (see
  [Status](../architecture/platform.md#status)).

Every signal carries the resource `service.name` (default `app.id`),
`service.version` (default `app.version`), `service.instance.id`
(`app.instance_id`) and, when set, `deployment.environment.name`
(`[telemetry.resource] environment`).

The host never waits on telemetry: queues are bounded, a full queue drops
and counts (`merezha.telemetry.dropped`), and a collector that is down or
slow never fails readiness.

## Production backends

Three keys point the host at any OTLP backend: `endpoint`, `protocol`, and
`headers` for the credentials. Header values are secrets: write them as
`{ env = "NAME" }` or `{ file = "PATH" }`, never inline.

```toml
[telemetry]
endpoint = "https://otlp.example.com"   # base URL; /v1/traces, /v1/metrics, /v1/logs are appended
protocol = "http/protobuf"              # "grpc" when the backend wants port 4317
headers = { authorization = { env = "OTLP_AUTHORIZATION" } }

[telemetry.resource]
environment = "production"
```

| Backend | `endpoint` | Credentials header | Notes |
| --- | --- | --- | --- |
| A collector agent on the node or in the pod (recommended) | `http://127.0.0.1:4318` | none | the agent holds the vendor credentials; see [Collector topology](#collector-topology) |
| Grafana Cloud | the stack's OTLP gateway URL (`https://otlp-gateway-<zone>.grafana.net/otlp`) | `authorization`: `Basic <base64 of instance id:token>` | queries and rules work unchanged (Mimir's default translation) |
| Honeycomb | `https://api.honeycomb.io` | `x-honeycomb-team`: the API key | |
| New Relic | `https://otlp.nr-data.net` | `api-key`: the license key | |
| Elastic | the APM server or managed OTLP URL | `authorization`: `ApiKey <key>` | |
| Datadog | the Datadog Agent's OTLP receiver, `http://127.0.0.1:4318` | none (the agent holds the key) | set `[telemetry.metrics] temporality = "delta"` |

Rules that protect the credentials:

- `https` endpoints use the system roots plus `ca_file` when set.
- Headers over plain `http` to a host that is not loopback are refused at
  startup (`plaintext_headers_refused`) unless `allow_plaintext_headers =
  true`.
- `GET /v1/admin/status` shows the endpoint's scheme, host and port only;
  no log, span or status field holds a header value.

Standard `OTEL_*` environment variables are ignored (their names are logged
once as `telemetry.otel_env_ignored`); use the `MEREZHA_TELEMETRY__*`
overrides, such as `MEREZHA_TELEMETRY__ENDPOINT`.

A backend that is unreachable at startup is harmless: exports fail, are
retried three times, then counted as failed (`merezha.telemetry.exported`
with `merezha.outcome = failed`) and reported on stderr as
`telemetry.export_failed`, at most once per signal per minute.

## Collector topology

The recommended layout puts an OpenTelemetry Collector next to each host:
a sidecar in the pod, or an agent on the node. The host exports to it over
loopback HTTP with no headers; the collector holds the vendor credentials,
retries, batches, and decides which traces to keep. The host process then
never holds a vendor secret, and changing vendor is a collector change.

```text
merezha --OTLP/HTTP, loopback--> collector agent --OTLP, TLS + credentials--> backend
```

A minimal agent (`otel/opentelemetry-collector-contrib`):

```yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 127.0.0.1:4318
processors:
  memory_limiter:
    check_interval: 1s
    limit_percentage: 80
    spike_limit_percentage: 20
  tail_sampling:
    decision_wait: 10s
    policies:
      - { name: errors, type: status_code, status_code: { status_codes: [ERROR] } }
      - { name: slow, type: latency, latency: { threshold_ms: 500 } }
      - { name: rest, type: probabilistic, probabilistic: { sampling_percentage: 10 } }
  batch: {}
exporters:
  otlphttp:
    endpoint: https://otlp.example.com
    headers:
      authorization: ${env:OTLP_AUTHORIZATION}
service:
  pipelines:
    traces: { receivers: [otlp], processors: [memory_limiter, tail_sampling, batch], exporters: [otlphttp] }
    metrics: { receivers: [otlp], processors: [memory_limiter, batch], exporters: [otlphttp] }
    logs: { receivers: [otlp], processors: [memory_limiter, batch], exporters: [otlphttp] }
```

Tail sampling needs every span of a trace in the same collector. A Merezha
trace can cross instances: a command on one instance, the job it enqueued
on another (job attempts continue the producer's trace). With several
instances, put a gateway tier behind the agents and route traces to it with
the `loadbalancing` exporter keyed on the trace id, and tail-sample there.
With one instance, the agent above is enough.

Metrics and logs are never sampled.

## Sampling

`[telemetry.traces]` samples at the root of each trace; downstream spans
(job attempts, reactions) follow the root's decision, which travels in the
stored `traceparent`.

| Traffic | Host `ratio` | Collector |
| --- | --- | --- |
| Up to about 50 requests per second on one instance | `1.0` (the default) | no sampling needed |
| Higher, with a tail-sampling collector | `1.0` to the local agent | tail sampling keeps errors and slow traces plus a share of the rest |
| Higher, without a tail-sampling collector | `0.05` to `0.2` | none |

Head sampling at a low ratio drops error traces with the rest; the error is
still in the metrics (`merezha.outcome`, `http.response.status_code`) and in
the logs. Choose tail sampling when every failing request must keep its
trace.

`trust_inbound` decides whose sampling decision the host honours. The
default, `trusted_proxies`, follows an inbound `traceparent` only from
`http.trusted_proxies` and loopback; a public client's context becomes a
span link, so a client cannot force 100% sampling.

## Dashboards and alerts

The host embeds Grafana dashboards and Prometheus rule files built for its
own metric catalog. Write them out with the host version you run:

```sh
merezha telemetry export --out ops/telemetry
```

It writes, and on a second run replaces, these files and leaves any other
file in the directory alone. The directory may be a symbolic link, but
`dashboards` and `rules` inside it may not: the export refuses to write
through them (`not_a_directory`).

| File | What it is |
| --- | --- |
| `dashboards/merezha-overview.json` | request rate, availability, latency per route, command outcomes, activity (reactors, notifications, inbox stream, webhooks, dead letters), warning and error logs from Loki, error traces from Tempo |
| `dashboards/merezha-application.json` | command and query latency and outcomes per tag, projection lag and catch-up, guest fuel, memory and outcomes, reactor lag, halts and effects |
| `dashboards/merezha-data-jobs.json` | pool usage, waiters and timeouts, statement latency, event append latency, jobs per state, job lag, dead letters, attempts, outbox lag and flow |
| `dashboards/merezha-delivery.json` | notification statuses per channel and topic, provider latency, webhooks, the inbox stream, outbound providers, sign-in and authentication, rate limits, content delivery and blobs |
| `dashboards/merezha-telemetry.json` | exported, failed and dropped items per signal, dropped attributes, series past the cardinality limit |
| `dashboards.yaml` | Grafana file provisioning for the `dashboards/` directory, at the path the local `grafana/otel-lgtm` image uses |
| `rules/merezha-recording.yaml`, `rules/merezha-alerts.yaml` | Prometheus recording rules and alerts |

The output is one JSON report on stdout: `{"status":"exported", "files":
[...]}` with exit 0, `{"status":"failed","error":{"code":"export_failed",
"file":...,"reason":...},"files":[...]}` with exit 1 (`files` lists what
it had already replaced), or `invalid_arguments` with exit 2.
Run it again after upgrading the host, so the queries match the new
catalog.

### Metric names

The queries assume the OTLP translation of Prometheus 3 and Mimir by default
(`UnderscoreEscapingWithSuffixes`), which Grafana Cloud also uses: dots
become underscores, durations get `_seconds`, counters `_total`, gauges of
unit `1` `_ratio`, histograms `_bucket`, `_count` and `_sum`;
`service.name` becomes the `job` label and `service.instance.id` the
`instance` label. `merezha.command.duration` is therefore
`merezha_command_duration_seconds_bucket{job="tasks", merezha_outcome="failed"}`.
A backend configured with another translation, or one that is not
Prometheus-compatible, needs the queries rewritten; the PromQL in the rule
files is the reference definition of each alert.

### Importing the dashboards

- **Grafana UI**: Dashboards, New, Import, and upload a JSON file. Each
  dashboard has `Prometheus`, `Loki` and `Tempo` datasource variables
  (`DS_PROMETHEUS`, `DS_LOKI`, `DS_TEMPO`) that default to the first
  datasource of each type; pick others at the top of the dashboard.
- **File provisioning**: copy `dashboards/` to a directory Grafana reads,
  and `dashboards.yaml` into Grafana's `provisioning/dashboards/` with
  `path` changed to that directory. Provisioned dashboards are read-only in
  the UI; edit the files.
- The `Service` variable lists every `job` that exports
  `merezha_telemetry_exported_total`; every query filters on it.

### Loading the rules

Load both rule files together; the alerts read the recording rules.

- **Prometheus**: add them to `rule_files` in `prometheus.yml`.
- **Mimir or Grafana Cloud**: `mimirtool rules load rules/merezha-recording.yaml rules/merezha-alerts.yaml`.
- **Grafana alerting**: import them as data-source-managed rules of the
  Prometheus or Mimir datasource.

Check them before loading, and after any edit, with promtool:

```sh
promtool check rules rules/*.yaml
```

The repository runs `scripts/check-telemetry-artifacts.sh` in CI: the same
check, the rule unit tests in `deploy/telemetry/tests/`, and every dashboard
query.

### The alerts

`severity: page` wants a person now; `severity: ticket` wants a look within
a working day. Route them by that label. The HTTP SLOs exclude `/health/*`
requests and use multi-window burn rates over 30 days (SRE workbook):
paging when both windows of a pair burn faster than the factor, ticketing
when three days of errors exceed the budget.

| Alert | Severity | Fires when | Look at |
| --- | --- | --- | --- |
| `MerezhaHttpAvailabilityBudgetBurn` | page | 5xx ratio above 14.4 × 0.1% over 1 h and 5 min, or 6 × over 6 h and 30 min | the 5xx panel per route, `http.request.completed` errors, the database and guest panels |
| `MerezhaHttpAvailabilityBudgetBurnSlow` | ticket | 5xx ratio above 0.1% over 3 days and 6 h | same |
| `MerezhaHttpLatencyBudgetBurn` / `...Slow` | page / ticket | requests over 500 ms above 14.4 × 1% over 1 h and 5 min, or 6 × over 6 h and 30 min / above 1% over 3 days and 6 h | latency per route, statement latency, pool waiters, guest latency |
| `MerezhaCommandFailures` | page | more than 1% of commands `failed` over 10 min | `command.completed` warnings and their `code`, guests, database |
| `MerezhaCommandRejections` | ticket | more than 50% of commands `rejected` over 30 min | the client sending them |
| `MerezhaProjectionLagging` / `MerezhaProjectionStalled` | ticket / page | a projection's oldest unprocessed event is older than 60 s / 300 s for 10 min | `projection.failed`, `GET /v1/application/projections` |
| `MerezhaReactorHalted` | page | a reactor halted for 5 min | `reactor.halted`; fix the guest, or skip the event through the reactors admin API, then resume |
| `MerezhaOutboxLagging` | page | the oldest unrelayed outbox record is older than 120 s for 10 min | `outbox.relay_failed`, the database |
| `MerezhaJobsLagging` | ticket | the oldest due job of a queue waited more than 300 s for 15 min | queue concurrency (`[jobs.queues]`), job duration |
| `MerezhaJobsDeadLettered` | ticket | a job was dead-lettered in the last 15 min | `job.dead_lettered`; replay from the jobs admin API once fixed |
| `MerezhaNotificationDeliveryFailures` / `...High` | ticket / page | failed, bounced or expired above 5% over 30 min / 25% over 15 min, per channel | the [notifications guide](notifications.md), `notification.provider_account_error` |
| `MerezhaDatabasePoolSaturated` | ticket | requests waited for a connection for 5 min | `database.pool_size`, long transactions, `db.pool_undersized` at startup |
| `MerezhaDatabasePoolTimeouts` | ticket | a connection wait timed out | same, `db.pool_timeout` |
| `MerezhaGuestLimits` | ticket | more than 1% of a service's invocations hit fuel, memory or time limits over 15 min | the runtime manifest's `[limits]`, expensive inputs |
| `MerezhaInstanceAbsent` | page | a service that exported within the last 24 hours has exported nothing for 10 min | the instances, the collector |
| `MerezhaTelemetryDropped` | ticket | telemetry items were dropped in the last 15 min | `telemetry.export_failed` on stderr, the collector, `max_queue` |
| `MerezhaMetricOverflow` | ticket | a series with `otel_metric_overflow="true"` exists | which instrument overflowed: `{otel_metric_overflow="true"}` |

`MerezhaJobsDeadLettered`, `MerezhaDatabasePoolTimeouts` and
`MerezhaTelemetryDropped` also fire on the first value of a new series
(counters reach Prometheus at their first value, with no 0 before it). A
series counts as new when it had no sample in the hour before the alert's
window, so a collector outage under an hour raises nothing by itself. The
trade-off: an instance that restarts within the hour under the same
`app.instance_id` continues its old series, and a first event whose sample
lands outside the `increase` window after the restart can be missed. The
default instance id (hostname plus pid) changes on every restart, which
avoids this.

`MerezhaInstanceAbsent` only knows services it has seen in the last 24
hours: it fires 10 minutes after the last export and keeps firing until the
service exports again or 24 hours have passed, then resolves although the
service may still be down. A service that has not exported within those 24
hours is not known at all. For a service that must always run, add a rule
of your own, which has neither limit:

```yaml
- alert: TasksAbsent
  expr: absent_over_time(merezha_telemetry_exported_total{job="tasks"}[10m])
  labels: { severity: page }
```

## Local stack

`grafana/otel-lgtm` runs a collector, Tempo, Prometheus, Loki and Grafana
in one container. With the exported dashboards baked in:

```sh
merezha telemetry export --out ops/telemetry
cat > ops/telemetry/lgtm.Dockerfile <<'EOF'
FROM grafana/otel-lgtm:0.38.0
COPY dashboards/ /otel-lgtm/grafana/conf/provisioning/dashboards/merezha/
COPY dashboards.yaml /otel-lgtm/grafana/conf/provisioning/dashboards/merezha.yaml
EOF
docker build -t merezha-lgtm -f ops/telemetry/lgtm.Dockerfile ops/telemetry
docker run --rm -p 3000:3000 -p 4318:4318 merezha-lgtm
MEREZHA_TELEMETRY__ENDPOINT=http://127.0.0.1:4318 \
  MEREZHA_TELEMETRY__METRICS__INTERVAL_MS=5000 \
  merezha run --config merezha.toml
```

Grafana is at `http://127.0.0.1:3000` (`admin`/`admin`, a local preview
only), with the dashboards in the `Merezha` folder. The image's Prometheus
loads no rule files, so the alerts are not evaluated locally; check them
with promtool instead. Projects generated by `merezha new` are to get this
directory and a Contremaitre `otel` service written for them
([scaffold design](../design/scaffold.md) §3.10, slice SC-S4).

## Troubleshooting

- **Nothing arrives.** `GET /v1/admin/status` shows `telemetry.enabled`,
  the endpoint and, per signal, `exported`, `failed`, `dropped` and
  `last_code` (`ok`, `timeout`, `shutdown`, `export_failed`). stderr shows
  `telemetry.export_failed` with the signal, the code and the items lost so
  far, at most once a minute per signal.
- **Dropped items** (`merezha.telemetry.dropped`): `queue_full` means the
  collector is slower than the host produces; raise `max_queue` or fix the
  collector. `shutdown` means the flush budget (`shutdown_timeout_ms`) ran
  out. `stderr` counts log lines the lossy stderr writer dropped.
- **Dropped attributes** (`merezha.telemetry.attributes.dropped`) above 0
  are a host defect: an attribute outside the allowlist reached the
  exporter. Report it.
- **Overflow series**: an instrument passed 2000 attribute sets, so one of
  its attributes is not from a closed set. Report it with the instrument
  name.
- **Logs without traces**: log records carry `trace_id` only inside a
  sampled, recording span. Startup and background work outside a request
  have their own traces (`startup`, `relay outbox`, `process {queue}`).
