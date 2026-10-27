# Merezha

Merezha takes its name from the Ukrainian word **мережа**, meaning "network".
It runs portable WebAssembly component services and composes their successful
outputs through ordered pipelines. Application behavior lives in the components.

Build with Rust 1.98.0 or later, using the committed lockfile:

```sh
cargo build --locked --workspace
cargo test --locked --workspace
```

Services export [merezha:service@0.1.0](wit/service.wit). They take and return
opaque UTF-8 strings, conventionally JSON, and import no host capabilities.
Each call has a fresh store with memory, fuel, byte, timeout and concurrency
limits. Existing Node applications require a separate compatibility backend.

Put a portable component next to a version 1 TOML manifest using the
[complete schema and example](docs/architecture/contract.md), then start:

```sh
cargo run --locked -p merezha-cli -- serve --manifest /path/to/runtime.toml --listen 127.0.0.1:0
```

Read the actual ephemeral address from the JSON `listener.ready` event on stderr.
Use raw payload bytes when invoking a service or pipeline:

```sh
curl --fail-with-body --data-binary '{"name":"Ada"}' http://127.0.0.1:PORT/v1/services/normalize/invoke
curl --fail-with-body --data-binary '{"name":"Ada"}' http://127.0.0.1:PORT/v1/pipelines/document/invoke
curl --fail-with-body http://127.0.0.1:PORT/v1/services
curl --fail-with-body -X POST http://127.0.0.1:PORT/v1/reload
```

Reload rereads the startup manifest and atomically adds, removes or replaces the
catalog after complete validation. A failed reload leaves the old catalog
working. Already admitted pipelines finish with their original catalog and
budgets. Replace artifact files and manifests atomically before requesting reload.

The listener is restricted to loopback for trusted local use. Local processes
can invoke services and reload the catalog. Guest limits do not cap native
compiler memory or provide an OS sandbox. `serve` uses no PostgreSQL; see
[Platform host](#platform-host) for `run`. See
[runtime decisions and limitations](docs/architecture/runtime.md) and
[research coverage](docs/architecture/research-coverage.md).

Required host checks:

```sh
cargo fmt --all --check
cargo check --locked --workspace
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
cargo test --locked --workspace --doc
cargo doc --locked --workspace --no-deps
cargo build --locked --workspace
```

Tests assemble real components from WAT and exercise invocation, pipelines,
reload, isolation, budgets, guest faults and an actual ephemeral HTTP listener.
The separate services/examples contribution supplies higher-level guest recipes.

For stateful applications, the opt-in [application SDK and host](docs/architecture/application.md)
add guest deciders, command/query routing, bounded event history,
idempotency and checkpointed read models. Start with `serve --manifest PATH
--application PATH`; the original raw endpoints remain available. Memory mode
loses state on restart. Opt-in SQLite mode retains events, keyed receipts and
read models in one local database owned by one host. To try it,
`scripts/task-board-local.sh` serves the example task board with SQLite storage
on `127.0.0.1:8787`; the [task board guide](docs/services/README.md#local-task-board-with-durable-storage)
shows the `task-board` client commands. See the
[Structure parity map](docs/architecture/structure-parity.md)
for the bounded subset and deferred capabilities.

## Platform host

`merezha run --config merezha.toml` is the PostgreSQL-backed host for
framework projects. One file declares the app, HTTP edge, database, runtime
manifest, optional application and modules. It opens a bounded pool,
applies or verifies module migrations, serves `/health/live` and
`/health/ready`, and mounts the runtime and application routes behind an edge
that handles request ids, trace context, trusted proxies, CORS, body limits
and one error shape. It stops gracefully on SIGINT or SIGTERM.

```sh
cargo run --locked -p merezha-cli -- run --config merezha.toml
```

`[modules.notifications]` turns on the notifications block: templates with
previews, the `Notifier` other blocks request through, the in-app inbox,
preferences and unsubscribe, and email and SMS through the capture drivers
(listed at `GET /v1/notifications/dev/captures` on loopback); Brevo, push,
the SSE stream and webhooks arrive in later slices.
`merezha notifications check --config merezha.toml` validates its settings
and templates without starting the host; the
[operations guide](docs/operations/notifications.md) covers production
setup, retention and monitoring.

PostgreSQL tests run when `MEREZHA_TEST_DATABASE_URL` points to a server
where the role can create databases; otherwise they print a skip message.
`.contremaitre.yaml` provides a local PostgreSQL 17. The
[platform guide](docs/architecture/platform.md) has the full configuration
reference, the module and port contracts, and both local database setups.

With `[telemetry] endpoint` set, the host exports traces, metrics and logs
over OTLP. `merezha telemetry export --out ops/telemetry` writes the Grafana
dashboards and Prometheus alert rules that match the host's metrics; the
[telemetry operations guide](docs/operations/telemetry.md) covers production
backends, collector topology, sampling and the alerts.

## License

[MIT](LICENSE)
