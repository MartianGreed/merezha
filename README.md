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
compiler memory or provide an OS sandbox. PostgreSQL and developer tools remain
external. See [runtime decisions and limitations](docs/architecture/runtime.md)
and [research coverage](docs/architecture/research-coverage.md).

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
add guest deciders, command/query routing, finite in-memory event history,
idempotency and checkpointed read models. Start with `serve --manifest PATH
--application PATH`; the original raw endpoints remain available. This adapter
loses all application state on restart. See the [Structure parity map](docs/architecture/structure-parity.md)
for the bounded subset and deferred capabilities.

## License

[MIT](LICENSE)
