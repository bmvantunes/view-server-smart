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
