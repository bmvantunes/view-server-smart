# Benchmark Baselines

`benchmarks/baselines/smoke.json` is the committed smoke performance baseline.

Run the gate:

```bash
pnpm run bench:baseline:smoke
```

Refresh the smoke baseline only when a performance change is intentionally accepted:

```bash
pnpm run bench:baseline:smoke:update
```

The smoke gate runs the existing Vitest benchmark files serially and compares fresh `.artifacts`
summary/output JSON against the committed baseline. Engine smoke cases use small row counts with
multiple samples; browser smoke stays deliberately tiny to keep CI practical. The gate fails on
cleanup leaks, backpressure, queued-event growth, RSS growth, mean latency regressions, or p99
latency regressions beyond the code-owned thresholds mirrored in the baseline manifest. Latency
thresholds intentionally use the wider of the ratio and absolute windows because CI smoke runs are
small and noisy; structural metadata, counters, sample counts, and RSS remain strict.

Do not run benchmark profiles in parallel when comparing results.

Kafka runtime profiles are separate from the default smoke gate because they start the Apache Kafka
container and exercise real `@platformatic/kafka` producers/consumers:

```bash
pnpm run bench:baseline:kafka-ingest
pnpm run bench:baseline:kafka-sustained-firehose
```

`kafka-ingest` measures single JSON/protobuf source batches plus a mixed burst. `kafka-sustained-firehose`
uses the same Vitest benchmark file in `sustained-firehose` mode and sends repeated mixed producer
batches before waiting for final View Server convergence. Both profiles require exact Kafka lane
completeness in their summary artifacts: produced rows, engine rows, and committed offsets must agree.
They also record per-case write-path throughput from benchmark operation timers. The baseline gate
compares exact produced-row/sample metadata and guards `aggregateRowsPerSecond`, which is total rows
divided by total measured time across samples; per-sample mean/min rows-per-second stay in the
artifact for diagnosis but are intentionally not the regression gate because tiny Kafka sample sets
can contain one unusually fast or slow sample.
