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
latency regressions beyond the thresholds stored in the baseline manifest.

Do not run benchmark profiles in parallel when comparing results.
