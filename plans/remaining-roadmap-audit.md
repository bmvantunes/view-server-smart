# Remaining Roadmap Audit

This audit maps the still-relevant `plans/*.md` requirements to current implementation
status. It is intentionally evidence-based: code and validation gates are the source of
truth, not plan text that predates later implementation work.

## Status Legend

- `Implemented`: covered by current code, tests, gates, and documentation.
- `Production-ready next`: should be implemented before calling the roadmap closed.
- `Deferred intentionally`: known future/optional work, not required for the current
  production milestone.
- `Remove or rewrite`: plan text is stale or no longer describes desired behavior.

## gRPC Plan

`plans/grpc.md` is implemented for the accepted gRPC scope.

| Area                                            | Status                 | Evidence                                                                                                               |
| ----------------------------------------------- | ---------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Source contracts and type gates                 | Implemented            | `packages/config/src/grpc-contract.ts`, `packages/config/src/index.test.ts`, `packages/runtime/src/index.test-d.ts`    |
| Runtime ownership validation                    | Implemented            | Runtime validation tests reject Kafka/gRPC and multi-feed ownership conflicts.                                         |
| Materialized gRPC runtime                       | Implemented            | `packages/runtime/src/grpc-ingress.ts`, materialized runtime tests, ConnectRPC tests.                                  |
| Leased gRPC runtime                             | Implemented            | `packages/runtime/src/grpc-lease-manager.ts`, leased runtime tests, ConnectRPC tests.                                  |
| gRPC health/lifecycle                           | Implemented            | `packages/runtime/src/grpc-health.ts`, runtime health tests, `vp run -w grpc:gate`.                                    |
| gRPC benchmark gates                            | Implemented            | `benchmarks/baselines/grpc-materialized.json`, `grpc-leased.json`, `grpc-leased-retained.json`, `vp run -w grpc:gate`. |
| Public config migration to source constructors  | Deferred intentionally | Listed under `Deferred Decisions`; current source markers are accepted.                                                |
| Session-scoped leased feeds and auth forwarding | Deferred intentionally | Runtime auth validates edge requests; gRPC feeds still use system-scoped shared feed identity.                         |
| Generic non-gRPC stream-source API              | Deferred intentionally | Plan explicitly keeps ConnectRPC-specific public API.                                                                  |
| Multi-source topics                             | Deferred intentionally | Requires a separate ordering/dedupe/restart contract.                                                                  |
| Custom live-event transport                     | Deferred intentionally | Browser transport remains Effect RPC WebSocket + NDJSON.                                                               |
| WAL/checkpointing for gRPC materialized feeds   | Deferred intentionally | Same recovery policy as the in-memory runtime.                                                                         |

## Column Live View Engine Plan

`plans/v2-column-live-view-engine-plan.md` is the umbrella product roadmap. The
current production slice is largely implemented, but the whole file is not complete
because it includes explicit future scope.

### Implemented Current Slice

| Area                                                          | Status      | Evidence                                                                                                                                                                                                                                                                       |
| ------------------------------------------------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Own in-memory live engine, no external analytical DB hot path | Implemented | `packages/column-live-view-engine`, `packages/runtime-core`; no chDB/Perspective runtime dependency.                                                                                                                                                                           |
| Explicit package seams                                        | Implemented | `packages/config`, `column-live-view-engine`, `runtime-core`, `client`, `protocol`, `in-memory`, `server`, `runtime`, `react`; `vp run -w check:package-exports`; `vp run -w check:internal-seams`.                                                                            |
| Typed public config/query API                                 | Implemented | Config type tests cover topic schemas, query select/where/order/group/aggregates, Kafka, and gRPC.                                                                                                                                                                             |
| Runtime URL in provider, not config                           | Implemented | React provider accepts runtime URL/client at provider boundary; browser bundles do not import runtime config.                                                                                                                                                                  |
| In-memory browser/test provider                               | Implemented | `@view-server/react/testing` and `@view-server/in-memory`; browser tests and in-memory benchmarks use real runtime-core/engine.                                                                                                                                                |
| Effect RPC WebSocket production transport                     | Implemented | `packages/server`, `packages/client/remote.ts`, protocol package, WebSocket/runtime tests.                                                                                                                                                                                     |
| Health hook, `/health`, and `/metrics` endpoints              | Implemented | `useViewServerHealth`, runtime/server health tests, health codecs, metrics route/runtime tests, root/runtime README docs.                                                                                                                                                      |
| Kafka runtime ingress                                         | Implemented | `@platformatic/kafka`, JSON/protobuf/custom codecs, source mapping, Docker Apache Kafka e2e, restart/startFrom policy.                                                                                                                                                         |
| gRPC runtime ingress                                          | Implemented | Covered by `plans/grpc.md` implementation.                                                                                                                                                                                                                                     |
| Runtime auth/session validation seam                          | Implemented | Optional `auth.validateRequest` on server/runtime validates WebSocket upgrades, `/health`, and `/metrics`; default remains anonymous.                                                                                                                                          |
| Snapshot/delta convergence                                    | Implemented | Engine/runtime/client tests cover raw, grouped, retained deltas, cleanup, and convergence.                                                                                                                                                                                     |
| Grouped queries and aggregates                                | Implemented | Grouped query tests, grouped aggregate/write benchmarks and gates.                                                                                                                                                                                                             |
| Backpressure at subscription/transport boundary               | Implemented | `BackpressureExceeded` typed status, queue-capacity tests, remote/client/protocol tests.                                                                                                                                                                                       |
| Benchmark baseline automation                                 | Implemented | Smoke, raw read/write, active sharing, grouped, WebSocket, Kafka, and gRPC baseline scripts.                                                                                                                                                                                   |
| Pre-gRPC readiness gate                                       | Implemented | `vp run -w pre-grpc:gate`.                                                                                                                                                                                                                                                     |
| TCP publish API/runtime ingress                               | Implemented | `packages/runtime/src/tcp-publish-ingress.ts`, runtime TCP tests for publish/patch/delete/publishMany, schema decode errors, bounded line/queue backpressure, source-owned topic rejection, startup failure, and shutdown cleanup.                                             |
| Runtime-core span/observability assertions                    | Implemented | Runtime-core tracing test captures client publish -> engine publish -> topic-store mutation/fanout -> live-subscription spans with real span-id parent links and topic/query attributes.                                                                                       |
| Minimal example app                                           | Implemented | `apps/example` defines typed config, production provider URL boundary, in-memory testing provider path, browser e2e test, type tests, and workspace build/check.                                                                                                               |
| Current public API examples in the roadmap                    | Implemented | `plans/v2-column-live-view-engine-plan.md` and `plans/grpc.md` now use `@view-server/config`, `createViewServerReact(viewServer)`, `createInMemoryViewServerReact(viewServerReact)`, `runViewServerRuntime(viewServer, options)`, and explicit `select` in raw query examples. |
| Production guide set                                          | Implemented | `docs/README.md`, `docs/public-api.md`, `docs/runtime-config.md`, `docs/kafka-mapping.md`, `docs/in-memory-browser-testing.md`, `docs/health-and-metrics.md`, `docs/query-semantics.md`, `docs/benchmarks-and-capacity.md`, and `docs/deployment.md`.                          |

### Production-Ready Next Items

No production-ready plan items remain after the production guide set. The next
step is validation of the documented scope with the readiness and benchmark
gates.

### Intentionally Deferred

These are in the plan, but should remain future work unless explicitly promoted.

| Item                                                     | Status                 | Reason                                                                                                                |
| -------------------------------------------------------- | ---------------------- | --------------------------------------------------------------------------------------------------------------------- |
| WAL/checkpoints                                          | Deferred intentionally | Current recovery contract is Kafka replay/startFrom based; WAL is allowed later but not required for first milestone. |
| Multi-consumer Kafka rebalance/revoke/checkpoint handoff | Deferred intentionally | Runtime documents single-consumer-per-group assumption.                                                               |
| Custom live-event WebSocket protocol                     | Deferred intentionally | Effect RPC WebSocket + NDJSON remains current production transport.                                                   |
| Rust/native/SIMD engine                                  | Deferred intentionally | TypeScript engine remains primary; native acceleration only if future benchmarks justify it.                          |
| User-defined indexes                                     | Deferred intentionally | Product principle remains automatic optimization from schemas and controlled query DSL.                               |
| Session-scoped gRPC leased feeds                         | Deferred intentionally | Edge auth exists, but leased feeds still need a separate session partitioning/forwarding contract.                    |

### Stale Or Needs Rewrite

| Plan text                                                                                                                       | Status            | Action                                                                                                                                                                                                |
| ------------------------------------------------------------------------------------------------------------------------------- | ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Earlier public API/runtime examples used obsolete package names, lazy Effect calls, and optional `totalRows` live-event fields. | Remove or rewrite | Reconciled with implemented `@view-server/*` packages, `Effect.runPromise` / `NodeRuntime.runMain` examples, required raw-query `select`, required `totalRows`, and current health hook return shape. |

## Recommended Implementation Order

1. Run `vp run -w ready`, `vp run -w pre-grpc:gate`, and `vp run -w grpc:gate` to prove the documented current scope still passes.
2. Do not reopen completed gRPC materialized/leased scope unless new tests reveal a real correctness gap.

After that, next work should come from a new explicit product decision or from promoting one intentionally deferred item.
