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

| Area                                            | Status                 | Evidence                                                                                                              |
| ----------------------------------------------- | ---------------------- | --------------------------------------------------------------------------------------------------------------------- |
| Source contracts and type gates                 | Implemented            | `packages/config/src/grpc-contract.ts`, `packages/config/src/index.test.ts`, `packages/runtime/src/index.test-d.ts`   |
| Runtime ownership validation                    | Implemented            | Runtime validation tests reject Kafka/gRPC and multi-feed ownership conflicts.                                        |
| Materialized gRPC runtime                       | Implemented            | `packages/runtime/src/grpc-ingress.ts`, materialized runtime tests, ConnectRPC tests.                                 |
| Leased gRPC runtime                             | Implemented            | `packages/runtime/src/grpc-lease-manager.ts`, leased runtime tests, ConnectRPC tests.                                 |
| gRPC health/lifecycle                           | Implemented            | `packages/runtime/src/grpc-health.ts`, runtime health tests, `pnpm run grpc:gate`.                                    |
| gRPC benchmark gates                            | Implemented            | `benchmarks/baselines/grpc-materialized.json`, `grpc-leased.json`, `grpc-leased-retained.json`, `pnpm run grpc:gate`. |
| Public config migration to source constructors  | Deferred intentionally | Listed under `Deferred Decisions`; current source markers are accepted.                                               |
| Session-scoped leased feeds and auth forwarding | Deferred intentionally | Current slice uses system-scoped shared feeds.                                                                        |
| Generic non-gRPC stream-source API              | Deferred intentionally | Plan explicitly keeps ConnectRPC-specific public API.                                                                 |
| Multi-source topics                             | Deferred intentionally | Requires a separate ordering/dedupe/restart contract.                                                                 |
| Custom live-event transport                     | Deferred intentionally | Browser transport remains Effect RPC WebSocket + NDJSON.                                                              |
| WAL/checkpointing for gRPC materialized feeds   | Deferred intentionally | Same recovery policy as the in-memory runtime.                                                                        |

## Column Live View Engine Plan

`plans/v2-column-live-view-engine-plan.md` is the umbrella product roadmap. The
current production slice is largely implemented, but the whole file is not complete
because it includes explicit future scope.

### Implemented Current Slice

| Area                                                          | Status      | Evidence                                                                                                                                                                                                                           |
| ------------------------------------------------------------- | ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Own in-memory live engine, no external analytical DB hot path | Implemented | `packages/column-live-view-engine`, `packages/runtime-core`; no chDB/Perspective runtime dependency.                                                                                                                               |
| Explicit package seams                                        | Implemented | `packages/config`, `column-live-view-engine`, `runtime-core`, `client`, `protocol`, `in-memory`, `server`, `runtime`, `react`; `pnpm run check:package-exports`; `pnpm run check:internal-seams`.                                  |
| Typed public config/query API                                 | Implemented | Config type tests cover topic schemas, query select/where/order/group/aggregates, Kafka, and gRPC.                                                                                                                                 |
| Runtime URL in provider, not config                           | Implemented | React provider accepts runtime URL/client at provider boundary; browser bundles do not import runtime config.                                                                                                                      |
| In-memory browser/test provider                               | Implemented | `@view-server/react/testing` and `@view-server/in-memory`; browser tests and in-memory benchmarks use real runtime-core/engine.                                                                                                    |
| Effect RPC WebSocket production transport                     | Implemented | `packages/server`, `packages/client/remote.ts`, protocol package, WebSocket/runtime tests.                                                                                                                                         |
| Health hook, `/health`, and `/metrics` endpoints              | Implemented | `useViewServerHealth`, runtime/server health tests, health codecs, metrics route/runtime tests, root/runtime README docs.                                                                                                          |
| Kafka runtime ingress                                         | Implemented | `@platformatic/kafka`, JSON/protobuf/custom codecs, source mapping, Docker Apache Kafka e2e, restart/startFrom policy.                                                                                                             |
| gRPC runtime ingress                                          | Implemented | Covered by `plans/grpc.md` implementation.                                                                                                                                                                                         |
| Snapshot/delta convergence                                    | Implemented | Engine/runtime/client tests cover raw, grouped, retained deltas, cleanup, and convergence.                                                                                                                                         |
| Grouped queries and aggregates                                | Implemented | Grouped query tests, grouped aggregate/write benchmarks and gates.                                                                                                                                                                 |
| Backpressure at subscription/transport boundary               | Implemented | `BackpressureExceeded` typed status, queue-capacity tests, remote/client/protocol tests.                                                                                                                                           |
| Benchmark baseline automation                                 | Implemented | Smoke, raw read/write, active sharing, grouped, WebSocket, Kafka, and gRPC baseline scripts.                                                                                                                                       |
| Pre-gRPC readiness gate                                       | Implemented | `pnpm run pre-grpc:gate`.                                                                                                                                                                                                          |
| TCP publish API/runtime ingress                               | Implemented | `packages/runtime/src/tcp-publish-ingress.ts`, runtime TCP tests for publish/patch/delete/publishMany, schema decode errors, bounded line/queue backpressure, source-owned topic rejection, startup failure, and shutdown cleanup. |

### Production-Ready Next Items

These are concrete gaps where the plan describes behavior that is still missing or
currently only documented as a future seam.

| Item                                 | Status                | Why it remains                                                                                   | Suggested first PR                                                                                                    |
| ------------------------------------ | --------------------- | ------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------- |
| Runtime auth/session validation seam | Production-ready next | gRPC has system/shared session context; server/runtime do not expose `auth.validateRequest` yet. | Add optional server/runtime auth validation at WebSocket and health/admin boundaries, initially anonymous by default. |
| Span/observability assertions        | Production-ready next | Code uses named `Effect.fn`, but tests do not prove key ingest -> engine -> fanout spans exist.  | Add one focused tracing test that captures span names for publish -> engine mutation -> subscription fanout.          |
| Example app                          | Production-ready next | Plan lists `apps/examples`; no `apps` files currently exist.                                     | Add a minimal example using real provider URL injection and in-memory provider test/demo path.                        |

### Intentionally Deferred

These are in the plan, but should remain future work unless explicitly promoted.

| Item                                                     | Status                 | Reason                                                                                                                |
| -------------------------------------------------------- | ---------------------- | --------------------------------------------------------------------------------------------------------------------- |
| WAL/checkpoints                                          | Deferred intentionally | Current recovery contract is Kafka replay/startFrom based; WAL is allowed later but not required for first milestone. |
| Multi-consumer Kafka rebalance/revoke/checkpoint handoff | Deferred intentionally | Runtime documents single-consumer-per-group assumption.                                                               |
| Custom live-event WebSocket protocol                     | Deferred intentionally | Effect RPC WebSocket + NDJSON remains current production transport.                                                   |
| Rust/native/SIMD engine                                  | Deferred intentionally | TypeScript engine remains primary; native acceleration only if future benchmarks justify it.                          |
| User-defined indexes                                     | Deferred intentionally | Product principle remains automatic optimization from schemas and controlled query DSL.                               |
| Session-scoped gRPC leased feeds                         | Deferred intentionally | Needs real authenticated sessions first.                                                                              |

### Stale Or Needs Rewrite

| Plan text                                             | Status            | Action                                                                 |
| ----------------------------------------------------- | ----------------- | ---------------------------------------------------------------------- |
| Some early examples show raw queries without `select` | Remove or rewrite | Public query API now requires explicit `select` or grouped aggregates. |

## Recommended Implementation Order

1. Runtime auth/session validation seam.
2. Span/observability assertions.
3. Minimal example app.

Do not reopen completed gRPC materialized/leased scope unless new tests reveal a real
correctness gap.
