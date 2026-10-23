# Structure application-core parity

This bounded stage ports application-core semantics to Rust hosts and portable
no-import Wasm components. It does not run TypeScript/Effect unchanged or claim
full framework parity. The source inventory is pinned to Structure commit
[`cb86394d8471c25eadaf00cc83887814dc48bb09`](https://github.com/Ligerian-labs/structure/tree/cb86394d8471c25eadaf00cc83887814dc48bb09). All 27 `packages/*/package.json` export
maps, public `src/index.ts` files and README introductions were inspected.
Core implementation and test references below distinguish intended source
semantics from the guarantees supplied here.

Source paths in this table are relative to the pinned Structure repository.
Each package row refers to its `packages/<name>/README.md` and `src/index.ts`.
`jobs`, `mailer` and `storage` are included even though the source AGENTS package
map omits them. "Included" means present in this application-core contribution;
the separate guest example and scenario remain a services contribution.

| Package | Included subset | Deferred capability and prerequisite |
| --- | --- | --- |
| `@structure-ai/ai` | None | Provider generation/streaming/retries require explicit host network ports, secrets and provider tests. |
| `@structure-ai/auth` | Actor metadata only, not authentication | Passwords, links, passkeys, sessions, OAuth/API keys and tenants require trusted authentication and security lifecycle. |
| `@structure-ai/auth-pg` | None | Durable auth stores require authentication contracts, host PostgreSQL, migrations and recovery tests. |
| `@structure-ai/auth-sqlite` | None | Durable auth stores require authentication contracts and a host SQLite adapter. |
| `@structure-ai/authorization` | Host `Authorizer` on validated actions before replay and queries | RBAC matrix, scoped roles, conditions and principal resolution require policy/auth adapters. CLI explicitly selects local `AllowAll`. |
| `@structure-ai/bdd` | Rust behavior/component tests, no Gherkin API | Typed steps, scenario worlds and eventual-consistency runner require a separate testing package. |
| `@structure-ai/cli` | `serve --application PATH` wires local application dispatch | Generic typed command DSL, direct command/query CLI subcommands and full exit-code parity are deferred. |
| `@structure-ai/client` | Published versioned JSON shapes | Typed HttpApi client, bearer propagation, deadlines/retries require a stable client-language SDK. |
| `@structure-ai/config` | Strict bounded application TOML and startup validation | Settings DSL, env/file/override precedence and generated settings reference require a host configuration layer. |
| `@structure-ai/cqrs` | Unique command/query tags, guest validation, policy, metadata, dispatch deadlines, bounded memory or SQLite keyed receipts | Effect layers, metrics/tracing integration and distributed transactions. |
| `@structure-ai/domain` | Guest-safe identity, classified errors, `Decider`, deterministic replay and event envelope | Branded per-entity types, general value-object schema tooling and state-stored repository adapters. Identity validation is intentionally stricter. |
| `@structure-ai/dotenv` | None | Cascade/expansion/lossless editing require explicit host file/environment tools; no guest imports added. |
| `@structure-ai/eventsourcing` | Replaceable event port, finite memory adapter, atomic expected-version append, ordered metadata history, replay, batch checkpoints and atomic rebuild | Snapshots, upcasters, imports, partitions, outbox/inbox and durable effects need separate contracts and adapters. |
| `@structure-ai/eventsourcing-nisshi` | None | Broker log and SQL sidecar require operations, Kafka compatibility and crash/recovery proof. |
| `@structure-ai/eventsourcing-pg` | None | Durable events/checkpoints/outbox/inbox/idempotency require SQL migrations, transaction design and restart tests. |
| `@structure-ai/eventsourcing-sqlite` | Opt-in single-owner host database, atomic event/receipt append, verified reopen and crash recovery | Append-with-outbox, retention, simultaneous hosts and full source API parity remain deferred. |
| `@structure-ai/fixtures` | Host tests create business data through commands | Reusable fixture catalog, run receipts, readiness hooks and cleanup DSL remain separate. Guest scenario belongs to the services lane. |
| `@structure-ai/http` | Opt-in loopback JSON command/query/projection endpoints with classified failures | Typed HttpApi/OpenAPI, static mounts, distributed rate limiting and external authentication adapters. |
| `@structure-ai/jobs` | None | Delayed/recurring jobs, leases, retries and drain require a durable host queue and worker lifecycle. |
| `@structure-ai/mailer` | None | SMTP/provider/template/capture ports require explicit outbound effects and delivery policy. |
| `@structure-ai/mcp` | None | MCP tools/resources, serving and OAuth scope helpers require a separate protocol/auth bridge. JSON dispatch is not MCP parity. |
| `@structure-ai/migrations` | Explicit SQLite schema v1 bootstrap, ownership lock and exact schema/configuration verification before readiness | General migration catalog, upgrades, checksums, backfills and status DSL remain deferred. |
| `@structure-ai/observability` | Accepted message/correlation/causation/actor metadata; existing structured raw-runtime logs | Full correlation context, metrics, redaction layers, service identity and OTLP exporters. |
| `@structure-ai/playwright` | No browser API; independent component/HTTP tests | Frontend/browser target and isolated authenticated test control plane are prerequisites. |
| `@structure-ai/runtime` | Application registry/config startup; existing bounded runtime, shutdown and atomic raw catalog reload | Full framework readiness/finalizer/bootstrap APIs and application config reload. |
| `@structure-ai/storage` | None | Blob keys, streaming local/S3 drivers, serving policy, readiness and metrics require explicit host capabilities. |
| `@structure-ai/viewmodel` | Finite memory or persisted JSON state/checkpoint, ordered catch-up, query snapshots, row-revision CAS and atomic rebuild | Schema row/SQL DDL/query DSL and general migrations remain deferred. |

## Source contract decisions

`domain/src/Aggregate.ts` and `domain/test/domain.test.ts` define and exercise
initial/decide/evolve, invariant rejection and deterministic rehydration.
`EntityId.ts` validates nonempty branded strings; the Rust `Identity` adds a
finite portable character contract and does not generate random IDs in guests.
`errors.ts` supplies the permanent/transient/conflict taxonomy.

`cqrs/src/handler.ts` rejects duplicate tags across message kinds. `bus.ts` and
`test/cqrs.test.ts` establish validation before authorization, actor/tag/key
idempotency, canonical schema-encoded payloads, in-flight/mismatch outcomes and
release on failure. The Rust host also binds stream and expected-version
preconditions, never evicts successful records, and rejects capacity. It stores
the canonical payload directly, with bounded entries, rather than adding a hash
dependency. Rust/JSON compatibility is explicit in [the protocol](application.md),
including safe integers, decimal strings and typed normalization. Source bigint
positions and schema-encoded Date/undefined/bigint values are not transferred
implicitly to this JSON protocol.

`eventsourcing/src/EventStore.ts`, `memory.ts`, `AggregateStore.ts` and
`test/{eventstore,aggregate}.test.ts` define optimistic atomic append, initial
version zero, numbering from one, globally visible ordering and replay. The
Rust store preserves these subsets with count/byte ceilings. It returns a
conflict for a stale command rather than automatically rerunning business
decisions. Source snapshots, automatic conflict retries, event codec upcasters
and import/origin/partition metadata remain deferred.

`eventsourcing/src/Projection.ts`, `test/projection.test.ts` and
`viewmodel/src/hydration.ts` define ordered batches, checkpoints and rebuild's
`live=false`. Source handler effects are at-least-once and must be idempotent;
state and checkpoint are not inherently one transaction. The Rust JSON model
stages and swaps state/checkpoint atomically, preserves the last successful
batch on catch-up failure, and preserves the whole prior model on rebuild
failure. This stronger bounded contract is covered by explicit failure tests.
Source query read-only behavior is a convention; the Rust host gives query
guests no event-store capability and accepts only a query-shaped reply.

The SQLite stage now persists event/idempotency/read-model transactions in one
owned local database, with explicit schema bootstrap and verified reopen. Its
tests cover process interruption, independent-connection stale writes and
closed-store restore. This is a bounded subset, not full source package parity.
The next dependency order is reviewed schema upgrades and
snapshots/upcasters/outbox/inbox, then
external auth/policy, operational telemetry and external effects. Jobs, mail,
storage, AI, clients and MCP each need their own host capability and failure
contracts. None follows automatically from the application-core subset.
