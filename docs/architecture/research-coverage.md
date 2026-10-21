# Research coverage

All fifteen supplied research documents were reviewed as read-only inputs. This
table identifies their exact snapshot and records only sanitized architectural
conclusions. It copies no private application logs, credentials or transcripts.
The source documents are not committed in this change. Hashes allow an operator
with the original snapshot to verify the input identity. Historical experiments
and their measurements were not rerun by this host lane.

| Input | SHA-256 | Decision or limitation carried forward |
| --- | --- | --- |
| `docs/decisions/0001-runtime-research-scope.md` | `5b8e900386ed0d6219c2a7f65f43829033d5bc5f2b2105ada2de7872c5127d3e` | Original research sought unchanged application execution and environment isolation. Issue #3 explicitly selects a smaller component-native foundation; Node compatibility is a separate backend. |
| `docs/research/baseline-native-and-container.md` | `f9a50c7ccabeadbf205680fd0b5c00296dc834b83788da58fefe7b7b77069dbe` | Startup must be judged by answered requests rather than listening sockets. Historical native/container timings do not establish component host performance. |
| `docs/research/engine-full-ports-and-aot.md` | `80798de4e8be290f6fcc6efcdce033f9fc026c91f728953a84f39d9c09b9e681` | Engine presence and import names do not prove behavior. Avoid silent shims; portable Wasm is distinct from native AOT and hybrid host-JS execution. |
| `docs/research/engine-quickjs-family.md` | `d52d1480df490ebc24ed634837cee350fe5360f5fb33fdedaff2dd8ee5a4516b` | Small engines lack broad Node APIs and can fail silently. The host links no Node shims and tests positive component output rather than trusting clean exits. |
| `docs/research/engine-starlingmonkey.md` | `220b03f57c9072fab5e1a5cc9c5df2a135a02df95a454784593608d0a321d4cf` | Cheap request-scoped components are promising; Node sockets, host imports, version skew and reuse have limitations. Fresh isolated stores and a narrow pinned WIT avoid claiming Node coverage. |
| `docs/research/engine-wasmer-wasix.md` | `55936d312db0845b048066fa5dde04466a6abfc8c7fceeac6a7f9c7d403a388b` | WASIX belongs to Wasmer and has different port, resource and engine semantics. This host does not pretend these artifacts are Wasmtime components. |
| `docs/research/experiment-bigatelier-api.md` | `d732e24ec3dea3f55e04bb4ef115b2e3c741bb357e8c0ad922694950a4119af9` | Reported application compatibility depended on packaging, environment, signals and engine shape. Unfilled performance placeholders remain unknown. No API/Node compatibility claim is made here. |
| `docs/research/experiment-bigatelier-ssr.md` | `72b78c353841f4908925527d6be2f8fa724a6c974d83312ecf201e25dc389723` | SSR correctness, locale, workload, load and interpreter cost require separate proof. Historical SSR results are not evidence for this component host. |
| `docs/research/experiment-database.md` | `259b77fe0ea27a7796ec9cabe6fd90bdf2fd7a709c7531c1eb34c399d376946e` | Consistent database forks need database-aware protocols and host filesystem support. PostgreSQL lifecycle and persistence stay external. |
| `docs/research/host-prototype.md` | `169d40b1c4286e0644eb0aa52a4dce92936df03957130fbc4d283f63f5f9de4a` | The WASIX embedder demonstrated useful isolation hooks but disclosed native memory and lifecycle gaps. Keep those lessons and limitations explicit without importing its architecture. |
| `docs/research/host-runtimes.md` | `c8a4225b5d6f7a76740d6c5c414cdb79e55bb8805851b39bec71c0fba46a0bde` | Wasmtime offers component support, store memory limits, fuel and epochs. Use locked 49.0.3 embedding APIs; host-owned loopback HTTP avoids guest socket remapping. |
| `docs/research/inventory-api-lifecycle.md` | `eb8eda92472965241dcf0a315bc8ce28c941a9aac89f5207d574be42fd2dc4ec` | Background timers, readiness and graceful shutdown are application contracts. Fresh request components do not promise to run persistent Node jobs. |
| `docs/research/inventory-api-modules.md` | `51cc2651c10906727048fab28da41807db25c9bd3e52488eb3f7dd3c1e0abb70` | A module inventory includes loaders, native fallbacks, crypto and export conditions. Supporting a small string ABI is not evidence of support for that application dependency graph. |
| `docs/research/inventory-network-crypto.md` | `7f767ae4945ece86bd843a33d2489b3786f467e534e3f36ce678e1ca4f5a208b` | Raw sockets, STARTTLS, sync crypto and fetch differ materially. No ambient imports are linked; network and crypto services are outside this ABI. |
| `docs/research/inventory-ssr.md` | `a36406f8f4814ab06aecf1e2950c68aeaf7d1da1b7ecf865bc883a37eddc9c15` | SSR uses dynamic imports, streams, filesystem, HTTP and significant transient allocations. Those requirements motivate a clear compatibility boundary and bounded payload/concurrency policies. |

The broader research scope is preserved as research history. The executable
contract for this delivery is issue #3 and `wit/service.wit`, not an implicit
promise to reproduce every earlier application or environment experiment.

Primary library behavior was checked in the installed sources matching
Cargo.lock. Wasmtime 49.0.3 requires Rust 1.96.0; this workspace declares and
tests Rust 1.98.0. The host enables component model, runtime and Cranelift,
without WASI or threads. No dependency was upgraded to match a floating example.
