import { describe, expect, it } from "@effect/vitest";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  buildBenchmarkBaseline,
  comparableBenchmarksFromVitestOutput,
  compareBenchmarkBaseline,
  readBenchmarkBaseline,
  readBenchmarkObservation,
  validateBenchmarkBaseline,
  writeBenchmarkBaseline,
} from "./benchmark-baseline.mjs";

const vitestOutput = {
  files: [
    {
      groups: [
        {
          fullName: "src/example.bench.ts > example benchmark group",
          benchmarks: [
            {
              max: 3,
              mean: 2,
              min: 1,
              name: "case a",
              p99: 3,
              sampleCount: 7,
            },
          ],
        },
      ],
    },
  ],
};

const summary = {
  artifactKind: "engine-benchmark-summary",
  backpressureCount: 0,
  benchmarkCases: ["case a"],
  benchmarkName: "example benchmark",
  benchmarkScope: "engine-raw-snapshot",
  browser: undefined,
  cleanupLeakCount: 0,
  groupedWriteAdmission: {
    configuredMode: "incremental",
    expectedAdmission: "incremental",
  },
  latency: {
    outputJsonPath: "actual.json",
    source: "vitest-output-json",
  },
  memory: {
    totalDelta: {
      rssBytes: 1024,
    },
  },
  mutationCount: 100,
  queuedEventCount: 0,
  rowCount: 100,
  subscriberCount: 1,
  topics: ["orders"],
};

const observation = {
  artifactKind: "engine-benchmark-summary",
  backpressureCount: 0,
  benchmarks: [
    {
      groupName: "src/example.bench.ts > example benchmark group",
      maxMs: 3,
      meanMs: 2,
      minMs: 1,
      name: "case a",
      p99Ms: 3,
      sampleCount: 7,
    },
  ],
  benchmarkCases: ["case a"],
  benchmarkName: "example benchmark",
  benchmarkScope: "engine-raw-snapshot",
  browser: undefined,
  cleanupLeakCount: 0,
  groupedWriteAdmission: {
    configuredMode: "incremental",
    expectedAdmission: "incremental",
  },
  latencySource: "vitest-output-json",
  memoryRssTotalDeltaBytes: 1024,
  minimumSampleCount: 5,
  mutationCount: 100,
  outputJsonPath: "actual.json",
  queuedEventCount: 0,
  rowCount: 100,
  seedBatchSize: undefined,
  subscriberCount: 1,
  summaryPath: "actual.summary.json",
  taskLabel: "task a",
  topics: ["orders"],
};

const taskPaths = (summaryPath: string, outputJsonPath: string) => ({
  expectedArtifactKind: "engine-benchmark-summary",
  expectedBenchmarkScope: "engine-raw-snapshot",
  expectedRowCount: 100,
  label: "task a",
  minimumSampleCount: 5,
  outputJsonPath,
  packageOutputJsonPath: "actual.json",
  summaryPath,
});

const browserTaskPaths = (summaryPath: string, outputJsonPath: string) => ({
  expectedArtifactKind: "react-browser-benchmark-summary",
  expectedBenchmarkScope: "react-in-memory-live-query",
  expectedRowCount: 100,
  label: "task a",
  minimumSampleCount: 5,
  outputJsonPath,
  packageOutputJsonPath: "actual.json",
  summaryPath,
});

describe("benchmark baseline comparison", () => {
  it("extracts comparable benchmark metrics from Vitest output", () => {
    expect(comparableBenchmarksFromVitestOutput(vitestOutput)).toStrictEqual([
      {
        maxMs: 3,
        meanMs: 2,
        minMs: 1,
        groupName: "src/example.bench.ts > example benchmark group",
        name: "case a",
        p99Ms: 3,
        sampleCount: 7,
      },
    ]);
  });

  it("reads benchmark observations from summary and Vitest artifacts", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-observation-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(summaryPath, `${JSON.stringify(summary)}\n`);
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(
      readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath)),
    ).toStrictEqual({
      ...observation,
      outputJsonPath,
      summaryPath,
    });
  });

  it("reads browser benchmark observations without process memory data", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-observation-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "react-browser-benchmark-summary",
        benchmarkScope: "react-in-memory-live-query",
        browser: {
          browser: "chromium",
          provider: "playwright",
        },
        groupedWriteAdmission: undefined,
        memory: {},
        seedBatchSize: 10,
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(
      readBenchmarkObservation(browserTaskPaths(summaryPath, outputJsonPath)),
    ).toStrictEqual({
      ...observation,
      artifactKind: "react-browser-benchmark-summary",
      benchmarkScope: "react-in-memory-live-query",
      browser: {
        browser: "chromium",
        provider: "playwright",
      },
      groupedWriteAdmission: undefined,
      memoryRssTotalDeltaBytes: undefined,
      outputJsonPath,
      seedBatchSize: 10,
      summaryPath,
    });
  });

  it("rejects engine benchmark observations with missing RSS memory data", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-observation-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({ ...summary, memory: { totalDelta: {} } })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() =>
      readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${summaryPath}.memory.totalDelta.rssBytes is required for engine-benchmark-summary.`,
    );
  });

  it("rejects benchmark observations with unknown artifact kinds", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-artifact-kind-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({ ...summary, artifactKind: "engine-benchmak-summary" })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() =>
      readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${summaryPath}.artifactKind must be engine-benchmark-summary or react-browser-benchmark-summary.`,
    );
  });

  it("rejects benchmark observations with too few samples", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-sample-count-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(summaryPath, `${JSON.stringify(summary)}\n`);
    writeFileSync(
      outputJsonPath,
      `${JSON.stringify({
        files: [
          {
            groups: [
              {
                fullName: "src/example.bench.ts > example benchmark group",
                benchmarks: [
                  {
                    ...vitestOutput.files[0].groups[0].benchmarks[0],
                    sampleCount: 1,
                  },
                ],
              },
            ],
          },
        ],
      })}\n`,
    );

    expect(() =>
      readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath)),
    ).toThrow(
      "task a / src/example.bench.ts > example benchmark group / case a: sampleCount must be at least 5 but was 1.",
    );
  });

  it("rejects benchmark observations that drift from expected task metadata", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-task-metadata-"));
    const artifactKindSummaryPath = join(directory, "artifact-kind.summary.json");
    const benchmarkScopeSummaryPath = join(directory, "benchmark-scope.summary.json");
    const rowCountSummaryPath = join(directory, "row-count.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);
    writeFileSync(
      artifactKindSummaryPath,
      `${JSON.stringify({ ...summary, artifactKind: "react-browser-benchmark-summary" })}\n`,
    );
    writeFileSync(
      benchmarkScopeSummaryPath,
      `${JSON.stringify({ ...summary, benchmarkScope: "other-scope" })}\n`,
    );
    writeFileSync(rowCountSummaryPath, `${JSON.stringify({ ...summary, rowCount: 101 })}\n`);

    expect(() =>
      readBenchmarkObservation(taskPaths(artifactKindSummaryPath, outputJsonPath)),
    ).toThrow(
      "task a: artifactKind changed from engine-benchmark-summary to react-browser-benchmark-summary.",
    );
    expect(() =>
      readBenchmarkObservation(taskPaths(benchmarkScopeSummaryPath, outputJsonPath)),
    ).toThrow("task a: benchmarkScope changed from engine-raw-snapshot to other-scope.");
    expect(() =>
      readBenchmarkObservation(taskPaths(rowCountSummaryPath, outputJsonPath)),
    ).toThrow("task a: rowCount changed from 100 to 101.");
  });

  it("rejects summaries that point at a different Vitest output artifact", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-output-path-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        latency: {
          outputJsonPath: "other.json",
          source: "vitest-output-json",
        },
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() =>
      readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${summaryPath}.latency.outputJsonPath changed from actual.json to other.json.`,
    );
  });

  it("roundtrips committed baseline manifests", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-baseline-"));
    const baselinePath = join(directory, "baseline.json");
    const baseline = buildBenchmarkBaseline("smoke", [observation]);

    writeBenchmarkBaseline(baselinePath, baseline);

    expect(readBenchmarkBaseline(baselinePath)).toStrictEqual(baseline);
  });

  it("validates baseline manifests with the default diagnostic path", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation]);

    expect(validateBenchmarkBaseline(baseline)).toStrictEqual(baseline);
  });

  it("rejects baseline writes with nonzero invariant counters", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-baseline-counter-"));
    const baselinePath = join(directory, "baseline.json");
    const baseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        cleanupLeakCount: 1,
      },
    ]);

    expect(() => writeBenchmarkBaseline(baselinePath, baseline)).toThrow(
      `Benchmark baseline ${baselinePath} is not writable:\ntask a: cleanupLeakCount must stay 0 but was 1.`,
    );
  });

  it("accepts benchmark results inside configured thresholds", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation]);
    const actual = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        benchmarks: [
          {
            ...observation.benchmarks[0],
            meanMs: 7,
            p99Ms: 13,
          },
        ],
        memoryRssTotalDeltaBytes: 2048,
      },
    ]);

    expect(compareBenchmarkBaseline(baseline, actual)).toStrictEqual({
      ok: true,
      regressions: [],
    });
  });

  it("reports missing tasks, counter regressions, memory regressions, and latency regressions", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation]);
    const regressed = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        backpressureCount: 1,
        benchmarks: [
          {
            groupName: "src/example.bench.ts > example benchmark group",
            maxMs: 300,
            meanMs: 40,
            minMs: 1,
            name: "case a",
            p99Ms: 100,
            sampleCount: 7,
          },
        ],
        cleanupLeakCount: 1,
        memoryRssTotalDeltaBytes: 200 * 1024 * 1024,
        queuedEventCount: 1,
      },
    ]);
    const withMissingTask = {
      ...regressed,
      tasks: [
        ...regressed.tasks,
        {
          ...observation,
          taskLabel: "extra actual task",
        },
      ],
    };

    expect(
      compareBenchmarkBaseline(
        {
          ...baseline,
          tasks: [
            ...baseline.tasks,
            {
              ...observation,
              taskLabel: "missing task",
            },
          ],
        },
        withMissingTask,
      ),
    ).toStrictEqual({
      ok: false,
      regressions: [
        "extra actual task: unexpected benchmark task in actual run.",
        "task a: cleanupLeakCount must stay 0 but was 1.",
        "task a: backpressureCount must stay 0 but was 1.",
        "task a: queuedEventCount must stay 0 but was 1.",
        "task a: total RSS delta regressed from 1024 bytes to 209715200 bytes; allowed <= 3072 bytes.",
        "task a / src/example.bench.ts > example benchmark group / case a: mean regressed from 2.000ms to 40.000ms; allowed <= 7.000ms.",
        "task a / src/example.bench.ts > example benchmark group / case a: p99 regressed from 3.000ms to 100.000ms; allowed <= 13.000ms.",
        "missing task: missing benchmark task in actual run.",
      ],
    });
  });

  it("reports missing benchmark cases", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation]);
    const changedCases = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        benchmarks: [
          {
            ...observation.benchmarks[0],
            name: "case b",
          },
        ],
      },
    ]);

    expect(compareBenchmarkBaseline(baseline, changedCases)).toStrictEqual({
      ok: false,
      regressions: [
        "task a: unexpected benchmark case src/example.bench.ts > example benchmark group / case b.",
        "task a: missing benchmark case src/example.bench.ts > example benchmark group / case a.",
      ],
    });
  });

  it("reports benchmark sample counts below the task minimum", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation]);
    const changedSampleCount = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        benchmarks: [
          {
            ...observation.benchmarks[0],
            sampleCount: 1,
          },
        ],
      },
    ]);

    expect(compareBenchmarkBaseline(baseline, changedSampleCount)).toStrictEqual({
      ok: false,
      regressions: [
        "task a / src/example.bench.ts > example benchmark group / case a: sampleCount must be at least 5 but was 1.",
      ],
    });
  });

  it("reports benchmark metadata drift", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation]);
    const changedMetadata = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        artifactKind: "react-browser-benchmark-summary",
        benchmarkCases: ["case b"],
        benchmarkName: "other benchmark",
        benchmarkScope: "react-in-memory-live-query",
        browser: {
          browser: "firefox",
          provider: "playwright",
        },
        groupedWriteAdmission: undefined,
        latencySource: "other-source",
        memoryRssTotalDeltaBytes: undefined,
        minimumSampleCount: 1,
        mutationCount: 1,
        outputJsonPath: "different.json",
        rowCount: 10,
        seedBatchSize: 10,
        subscriberCount: 2,
        summaryPath: "different.summary.json",
        topics: ["trades"],
      },
    ]);

    expect(compareBenchmarkBaseline(baseline, changedMetadata)).toStrictEqual({
      ok: false,
      regressions: [
        "task a: artifactKind changed from engine-benchmark-summary to react-browser-benchmark-summary.",
        "task a: benchmarkScope changed from engine-raw-snapshot to react-in-memory-live-query.",
        "task a: benchmarkName changed from example benchmark to other benchmark.",
        'task a: benchmarkCases changed from ["case a"] to ["case b"].',
        "task a: rowCount changed from 100 to 10.",
        "task a: mutationCount dropped from 100 to 1; allowed >= 90.",
        "task a: subscriberCount changed from 1 to 2.",
        'task a: topics changed from ["orders"] to ["trades"].',
        "task a: latencySource changed from vitest-output-json to other-source.",
        'task a: browser changed from undefined to {"browser":"firefox","provider":"playwright"}.',
        "task a: seedBatchSize changed from undefined to 10.",
        'task a: groupedWriteAdmission changed from {"configuredMode":"incremental","expectedAdmission":"incremental"} to undefined.',
        "task a: minimumSampleCount changed from 5 to 1.",
        "task a: outputJsonPath changed from actual.json to different.json.",
        "task a: summaryPath changed from actual.summary.json to different.summary.json.",
        "task a: memoryRssTotalDeltaBytes presence changed between baseline and actual run.",
      ],
    });
  });

  it("accepts browser benchmark manifests without process memory when the baseline also omits memory", () => {
    const withoutMemoryObservation = {
      ...observation,
      artifactKind: "react-browser-benchmark-summary",
      benchmarkScope: "react-in-memory-live-query",
      memoryRssTotalDeltaBytes: undefined,
    };
    const baseline = buildBenchmarkBaseline("smoke", [withoutMemoryObservation]);
    const withoutMemory = buildBenchmarkBaseline("smoke", [
      {
        ...withoutMemoryObservation,
        memoryRssTotalDeltaBytes: undefined,
      },
    ]);

    expect(compareBenchmarkBaseline(baseline, withoutMemory)).toStrictEqual({
      ok: true,
      regressions: [],
    });
  });

  it("rejects missing actual RSS data for engine baselines", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation]);
    const withoutMemory = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        memoryRssTotalDeltaBytes: undefined,
      },
    ]);

    expect(() => compareBenchmarkBaseline(baseline, withoutMemory)).toThrow(
      "Benchmark artifact field actual.tasks[0].memoryRssTotalDeltaBytes is required for engine-benchmark-summary.",
    );
  });

  it("rejects malformed Vitest output", () => {
    expect(() => comparableBenchmarksFromVitestOutput({ files: {} })).toThrow(
      "Benchmark artifact field vitestOutput.files must be an array.",
    );
  });

  it("rejects malformed benchmark summaries", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-malformed-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(summaryPath, `${JSON.stringify({ ...summary, rowCount: "100" })}\n`);
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() =>
      readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath)),
    ).toThrow(`Benchmark artifact field ${summaryPath}.rowCount must be a finite number.`);
  });

  it("rejects malformed benchmark mutation counters", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-malformed-mutations-"));
    const negativeSummaryPath = join(directory, "negative.summary.json");
    const fractionalSummaryPath = join(directory, "fractional.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(negativeSummaryPath, `${JSON.stringify({ ...summary, mutationCount: -1 })}\n`);
    writeFileSync(fractionalSummaryPath, `${JSON.stringify({ ...summary, mutationCount: 1.5 })}\n`);
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() =>
      readBenchmarkObservation(taskPaths(negativeSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${negativeSummaryPath}.mutationCount must be a non-negative integer.`,
    );
    expect(() =>
      readBenchmarkObservation(taskPaths(fractionalSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${fractionalSummaryPath}.mutationCount must be a non-negative integer.`,
    );
  });

  it("rejects malformed benchmark memory summaries", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-malformed-memory-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        memory: {
          totalDelta: {
            rssBytes: "1024",
          },
        },
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() =>
      readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${summaryPath}.memory.totalDelta.rssBytes must be a finite number.`,
    );
  });

  it("rejects malformed benchmark names", () => {
    expect(() =>
      comparableBenchmarksFromVitestOutput({
        files: [
          {
            groups: [
              {
                fullName: "src/example.bench.ts > example benchmark group",
                benchmarks: [
                  {
                    ...vitestOutput.files[0].groups[0].benchmarks[0],
                    name: "",
                  },
                ],
              },
            ],
          },
        ],
      }),
    ).toThrow("Benchmark artifact field benchmark.name must be a non-empty string.");
  });

  it("rejects malformed baseline manifests", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-malformed-baseline-"));
    const baselinePath = join(directory, "baseline.json");
    writeFileSync(baselinePath, "[]\n");

    expect(() => readBenchmarkBaseline(baselinePath)).toThrow(
      `Benchmark artifact field ${baselinePath} must be an object.`,
    );
  });

  it("rejects baseline manifests with unknown artifact kinds", () => {
    const baseline = {
      ...buildBenchmarkBaseline("smoke", [observation]),
      artifactKind: "benchmark-baseline",
    };

    expect(() => validateBenchmarkBaseline(baseline)).toThrow(
      "Benchmark artifact field baseline.artifactKind must be view-server-benchmark-baseline.",
    );
  });

  it("rejects non-positive benchmark sample requirements", () => {
    const baseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        minimumSampleCount: 0,
      },
    ]);

    expect(() => validateBenchmarkBaseline(baseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].minimumSampleCount must be a positive integer.",
    );
  });

  it("rejects editable baseline threshold drift", () => {
    const baseline = {
      ...buildBenchmarkBaseline("smoke", [observation]),
      thresholds: {
        latencyMean: {
          maxAbsoluteDeltaMs: 5000,
          maxRatio: 8000,
        },
        latencyP99: {
          maxAbsoluteDeltaMs: 10,
          maxRatio: 8,
        },
        memoryRssTotalDelta: {
          maxAbsoluteDeltaBytes: 134217728,
          maxRatio: 3,
        },
      },
    };

    expect(() => validateBenchmarkBaseline(baseline)).toThrow(
      "Benchmark artifact field baseline.thresholds must match code-owned default thresholds.",
    );
  });

  it("rejects empty baseline task manifests", () => {
    const baseline = buildBenchmarkBaseline("smoke", []);

    expect(() => compareBenchmarkBaseline(baseline, baseline)).toThrow(
      "Benchmark artifact field baseline.tasks must be a non-empty array.",
    );
  });

  it("rejects duplicate task labels in baseline manifests", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation, observation]);

    expect(() => compareBenchmarkBaseline(baseline, baseline)).toThrow(
      "Benchmark artifact field baseline.tasks contains duplicate taskLabel: task a.",
    );
  });

  it("rejects duplicate benchmark cases in baseline manifests", () => {
    const baseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        benchmarks: [observation.benchmarks[0], observation.benchmarks[0]],
      },
    ]);

    expect(() => compareBenchmarkBaseline(baseline, baseline)).toThrow(
      "Benchmark artifact field baseline.tasks[task a].benchmarks contains duplicate benchmark case: src/example.bench.ts > example benchmark group / case a.",
    );
  });
});
