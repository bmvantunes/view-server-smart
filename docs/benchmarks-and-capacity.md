# Benchmarks And Capacity

Benchmarks use Vitest benchmark mode through `vp test bench`. Do not add
ad-hoc benchmark runners for engine/runtime performance work.

## Gates

Root scripts provide benchmark profiles and regression comparison:

```sh
vp run -w bench:baseline:smoke
vp run -w bench:baseline:raw-read-write
vp run -w bench:baseline:active-query-sharing
vp run -w bench:baseline:grouped-admission
vp run -w bench:baseline:grouped-order-neutral
vp run -w bench:baseline:websocket-firehose
vp run -w bench:baseline:kafka-ingest
vp run -w bench:baseline:kafka-sustained-firehose
vp run -w bench:baseline:grpc-materialized
vp run -w bench:baseline:grpc-leased
vp run -w bench:baseline:grpc-leased-retained
```

Use `vp run -w pre-grpc:gate` before gRPC-focused work and `vp run -w grpc:gate`
for the gRPC profiles.

For a release-candidate capacity pass, run:

```sh
vp run -w release-candidate:capacity
```

This runs example browser/type checks, example builds, `pre-grpc:gate`,
`grpc:gate`, and the broad no-compare `bench:baseline:release` profile
serially. Do not run competing benchmark suites in parallel when recording
release-candidate numbers.

The release profile runs 10M-row engine cases and sets
`NODE_OPTIONS=--max-old-space-size=12288` so the benchmark process is not
limited by Node's default old-space cap.

## What To Measure

Read optimizations must measure write cost. For example, adding a column vector
or index can improve filtered reads while slowing publish/patch/delete. The
benchmark suite tracks both read latency and write tax for relevant profiles.

Core capacity profiles cover:

- raw snapshots
- filtered snapshots
- sorted top-k windows
- grouped aggregation
- live delta generation
- active query sharing
- WebSocket fanout
- Kafka ingest
- gRPC materialized and leased feeds

## Artifacts

Benchmark artifacts are written under package-local `.artifacts/` directories.
Stable baseline comparisons are managed by `scripts/run-benchmark-baseline.mjs`.
Noisy maximum latency should stay report-only unless repeated runs prove the
threshold is stable enough to gate CI.

## Release Candidate Notes

Record the machine/container shape beside any release-candidate benchmark
results:

- CPU model and allocated cores
- memory limit
- Node version
- Kafka broker location and topic partition counts
- row counts per View Server topic
- active browser/client count
- active subscription count
- Kafka input rate
- gRPC leased route count
- WebSocket fanout shape

The baseline gates catch regressions against committed smoke profiles. They are
not a substitute for one production-like capacity run before a real deployment.
