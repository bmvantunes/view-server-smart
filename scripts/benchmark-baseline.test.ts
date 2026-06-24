import { describe, expect, it } from "@effect/vitest";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  benchmarkThresholdsForProfile,
  buildBenchmarkBaseline,
  comparableBenchmarksFromVitestOutput,
  compareBenchmarkBaseline,
  defaultBenchmarkThresholds,
  groupedOrderNeutralBenchmarkThresholds,
  kafkaIngestBenchmarkThresholds,
  kafkaSustainedFirehoseBenchmarkThresholds,
  readBenchmarkBaseline,
  readBenchmarkObservation,
  validateBenchmarkBaseline,
  websocketFirehoseBenchmarkThresholds,
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
  groupedKeyWidthParameters: undefined,
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

const runtimeHealth = {
  engine: {
    topics: {
      orders: {
        rowCount: 100,
      },
    },
  },
  kafka: {
    topics: {
      sourceOrders: {
        regions: {
          local: {
            committedOffset: "100",
          },
        },
        viewServerTopic: "orders",
      },
    },
  },
};

const runtimeKafkaIngestLanes = [
  {
    internalTopic: "orders",
    lane: "orders",
    producedRows: 100,
    region: "local",
    sourceTopic: "sourceOrders",
    sourceTopicAlias: "unique-topic-per-run:orders",
  },
];

const comparableRuntimeKafkaIngestLanes = [
  {
    internalTopic: "orders",
    lane: "orders",
    producedRows: 100,
    region: "local",
    sourceTopicAlias: "unique-topic-per-run:orders",
  },
];

const runtimeThroughputMutationCount = 700;

const runtimeThroughputHealth = {
  engine: {
    topics: {
      orders: {
        rowCount: runtimeThroughputMutationCount,
      },
    },
  },
  kafka: {
    topics: {
      sourceOrders: {
        regions: {
          local: {
            committedOffset: String(runtimeThroughputMutationCount),
          },
        },
        viewServerTopic: "orders",
      },
    },
  },
};

const runtimeThroughputKafkaIngestLanes = [
  {
    internalTopic: "orders",
    lane: "orders",
    producedRows: runtimeThroughputMutationCount,
    region: "local",
    sourceTopic: "sourceOrders",
    sourceTopicAlias: "unique-topic-per-run:orders",
  },
];

const comparableRuntimeThroughputKafkaIngestLanes = [
  {
    internalTopic: "orders",
    lane: "orders",
    producedRows: runtimeThroughputMutationCount,
    region: "local",
    sourceTopicAlias: "unique-topic-per-run:orders",
  },
];

const runtimeThroughput = {
  source: "benchmark-operation-timers",
  cases: [
    {
      aggregateRowsPerSecond: 1000,
      maxCommitObservedMs: 80,
      maxReadSnapshotMs: 6,
      maxTotalMs: 100,
      meanCommitObservedMs: 75,
      meanConvergenceMs: 75,
      meanProducerSendMs: 25,
      meanReadSnapshotMs: 5,
      meanRowsPerSecond: 1000,
      meanTotalMs: 100,
      minRowsPerSecond: 900,
      name: "case a",
      producedRowsPerSample: 100,
      readSnapshotRowsPerSample: 25,
      sampleCount: 7,
      totalProducedRows: 700,
    },
  ],
};

const comparableRuntimeThroughputCases = runtimeThroughput.cases;

const rawRuntimeMetrics = {
  eventLoopDelay: {
    maxMs: 4,
    meanMs: 2,
    p99Ms: 3,
  },
  healthPolling: {
    count: 11,
    maxMs: 2,
    totalMs: 7,
  },
  kafkaLag: {
    maxConsumerLagMessages: "9007199254740993",
    sampledRegionCount: 1,
    totalConsumerLagMessages: "9007199254740993",
  },
};

const runtimeMetrics = rawRuntimeMetrics;

const drainedRuntimeMetrics = {
  ...runtimeMetrics,
  kafkaLag: {
    maxConsumerLagMessages: "0",
    sampledRegionCount: 1,
    totalConsumerLagMessages: "0",
  },
};

const comparableNonKafkaRuntimeThroughputCases = [
  {
    aggregateRowsPerSecond: 1000,
    maxTotalMs: 100,
    meanConvergenceMs: 75,
    meanProducerSendMs: 25,
    meanRowsPerSecond: 1000,
    meanTotalMs: 100,
    minRowsPerSecond: 900,
    name: "case a",
    producedRowsPerSample: 100,
    sampleCount: 7,
    totalProducedRows: 700,
  },
];

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
  groupedKeyWidthParameters: undefined,
  groupedWriteAdmission: {
    configuredMode: "incremental",
    expectedAdmission: "incremental",
  },
  kafkaIngestLanes: undefined,
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
  throughputCases: undefined,
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

const runtimeTaskPaths = (summaryPath: string, outputJsonPath: string) => ({
  expectedArtifactKind: "runtime-benchmark-summary",
  expectedBenchmarkScope: "runtime-kafka-ingest",
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

  it("uses profile-specific baseline thresholds", () => {
    expect({
      groupedOrderNeutral: benchmarkThresholdsForProfile("grouped-order-neutral"),
      kafkaIngest: benchmarkThresholdsForProfile("kafka-ingest"),
      kafkaSustainedFirehose: benchmarkThresholdsForProfile("kafka-sustained-firehose"),
      smoke: benchmarkThresholdsForProfile("smoke"),
      websocketFirehose: benchmarkThresholdsForProfile("websocket-firehose"),
      kafkaIngestBaseline: buildBenchmarkBaseline("kafka-ingest", [observation]).thresholds,
      kafkaSustainedFirehoseBaseline: buildBenchmarkBaseline("kafka-sustained-firehose", [
        observation,
      ]).thresholds,
      smokeBaseline: buildBenchmarkBaseline("smoke", [observation]).thresholds,
      websocketFirehoseBaseline: buildBenchmarkBaseline("websocket-firehose", [observation])
        .thresholds,
      orderNeutralBaseline: buildBenchmarkBaseline("grouped-order-neutral", [observation])
        .thresholds,
    }).toStrictEqual({
      groupedOrderNeutral: groupedOrderNeutralBenchmarkThresholds,
      kafkaIngest: kafkaIngestBenchmarkThresholds,
      kafkaSustainedFirehose: kafkaSustainedFirehoseBenchmarkThresholds,
      smoke: defaultBenchmarkThresholds,
      websocketFirehose: websocketFirehoseBenchmarkThresholds,
      kafkaIngestBaseline: kafkaIngestBenchmarkThresholds,
      kafkaSustainedFirehoseBaseline: kafkaSustainedFirehoseBenchmarkThresholds,
      smokeBaseline: defaultBenchmarkThresholds,
      websocketFirehoseBaseline: websocketFirehoseBenchmarkThresholds,
      orderNeutralBaseline: groupedOrderNeutralBenchmarkThresholds,
    });
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

  it("reads active-query sharing structural counters from summary artifacts", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-observation-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        activeViewCountBeforeCleanup: 1,
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath))).toStrictEqual({
      ...observation,
      activeViewCountBeforeCleanup: 1,
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

  it("reads non-Kafka throughput observations without Kafka mutation reconciliation", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-observation-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        throughput: runtimeThroughput,
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath))).toStrictEqual({
      ...observation,
      outputJsonPath,
      summaryPath,
      throughputCases: comparableNonKafkaRuntimeThroughputCases,
    });
  });

  it("reads runtime benchmark observations with process memory data", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-observation-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeThroughputHealth,
        kafka: {
          ingestLanes: runtimeThroughputKafkaIngestLanes,
        },
        mutationCount: runtimeThroughputMutationCount,
        runtimeMetrics: rawRuntimeMetrics,
        throughput: runtimeThroughput,
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(
      readBenchmarkObservation(runtimeTaskPaths(summaryPath, outputJsonPath)),
    ).toStrictEqual({
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-ingest",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: comparableRuntimeThroughputKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
      outputJsonPath,
      runtimeMetrics,
      summaryPath,
      throughputCases: comparableRuntimeThroughputCases,
    });
  });

  it("reads non-Kafka runtime benchmark observations without Kafka health", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-observation-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-websocket-firehose",
        groupedWriteAdmission: undefined,
        health: {
          engine: {
            topics: {
              orders: {
                rowCount: 100,
              },
            },
          },
          transport: {
            activeClients: 0,
            activeStreams: 0,
          },
        },
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(
      readBenchmarkObservation({
        ...runtimeTaskPaths(summaryPath, outputJsonPath),
        expectedBenchmarkScope: "runtime-websocket-firehose",
      }),
    ).toStrictEqual({
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-websocket-firehose",
      groupedWriteAdmission: undefined,
      outputJsonPath,
      summaryPath,
    });
  });

  it("accepts duplicate throughput benchmark names across groups with matching sample counts", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-throughput-groups-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    const duplicateGroupVitestOutput = {
      files: [
        {
          groups: [
            {
              fullName: "src/example-a.bench.ts > first group",
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
            {
              fullName: "src/example-b.bench.ts > second group",
              benchmarks: [
                {
                  max: 4,
                  mean: 3,
                  min: 2,
                  name: "case a",
                  p99: 4,
                  sampleCount: 7,
                },
              ],
            },
          ],
        },
      ],
    };
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        throughput: runtimeThroughput,
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(duplicateGroupVitestOutput)}\n`);

    expect(readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath))).toStrictEqual({
      ...observation,
      benchmarks: [
        {
          groupName: "src/example-a.bench.ts > first group",
          maxMs: 3,
          meanMs: 2,
          minMs: 1,
          name: "case a",
          p99Ms: 3,
          sampleCount: 7,
        },
        {
          groupName: "src/example-b.bench.ts > second group",
          maxMs: 4,
          meanMs: 3,
          minMs: 2,
          name: "case a",
          p99Ms: 4,
          sampleCount: 7,
        },
      ],
      outputJsonPath,
      summaryPath,
      throughputCases: comparableNonKafkaRuntimeThroughputCases,
    });
  });

  it("rejects ambiguous throughput benchmark sample counts across groups", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-throughput-groups-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    const ambiguousGroupVitestOutput = {
      files: [
        {
          groups: [
            {
              fullName: "src/example-a.bench.ts > first group",
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
            {
              fullName: "src/example-b.bench.ts > second group",
              benchmarks: [
                {
                  max: 4,
                  mean: 3,
                  min: 2,
                  name: "case a",
                  p99: 4,
                  sampleCount: 8,
                },
              ],
            },
          ],
        },
      ],
    };
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        throughput: runtimeThroughput,
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(ambiguousGroupVitestOutput)}\n`);

    expect(() => readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath))).toThrow(
      `Benchmark artifact field ${summaryPath}.throughput.cases.benchmarks contains ambiguous benchmark sampleCount values for case a.`,
    );
  });

  it("rejects runtime benchmark observations with malformed throughput", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-throughput-"));
    const invalidSourceSummaryPath = join(directory, "invalid-source.summary.json");
    const invalidRowsSummaryPath = join(directory, "invalid-rows.summary.json");
    const invalidTotalRowsSummaryPath = join(directory, "invalid-total-rows.summary.json");
    const invalidPositiveRateSummaryPath = join(directory, "invalid-positive-rate.summary.json");
    const invalidAggregateSummaryPath = join(directory, "invalid-aggregate.summary.json");
    const invalidMinRowsSummaryPath = join(directory, "invalid-min-rows.summary.json");
    const invalidMaxTotalSummaryPath = join(directory, "invalid-max-total.summary.json");
    const invalidProducerTimerSummaryPath = join(directory, "invalid-producer-timer.summary.json");
    const invalidConvergenceTimerSummaryPath = join(
      directory,
      "invalid-convergence-timer.summary.json",
    );
    const invalidReadTimerMaximumSummaryPath = join(
      directory,
      "invalid-read-timer-maximum.summary.json",
    );
    const invalidReadTimerTotalSummaryPath = join(
      directory,
      "invalid-read-timer-total.summary.json",
    );
    const mismatchedNameSummaryPath = join(directory, "mismatched-name.summary.json");
    const mismatchedSampleCountSummaryPath = join(
      directory,
      "mismatched-sample-count.summary.json",
    );
    const extraThroughputCaseSummaryPath = join(directory, "extra-throughput-case.summary.json");
    const mismatchedMutationTotalSummaryPath = join(
      directory,
      "mismatched-mutation-total.summary.json",
    );
    const missingThroughputSummaryPath = join(directory, "missing-throughput.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      missingThroughputSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
      })}\n`,
    );
    writeFileSync(
      invalidSourceSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          source: "other-timer",
        },
      })}\n`,
    );
    writeFileSync(
      invalidRowsSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              producedRowsPerSample: 0,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      invalidTotalRowsSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              totalProducedRows: 699,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      invalidAggregateSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              aggregateRowsPerSecond: 999,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      invalidPositiveRateSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              aggregateRowsPerSecond: 0,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      invalidMinRowsSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              meanRowsPerSecond: 900,
              minRowsPerSecond: 901,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      invalidMaxTotalSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              maxTotalMs: 99,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      invalidProducerTimerSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              meanProducerSendMs: 101,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      invalidConvergenceTimerSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              meanConvergenceMs: 101,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      invalidReadTimerMaximumSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              maxReadSnapshotMs: 4,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      invalidReadTimerTotalSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              maxReadSnapshotMs: 102,
              meanReadSnapshotMs: 101,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      mismatchedNameSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              name: "case b",
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      mismatchedSampleCountSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              sampleCount: 6,
              totalProducedRows: 600,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      extraThroughputCaseSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            runtimeThroughput.cases[0],
            {
              ...runtimeThroughput.cases[0],
              name: "case b",
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      mismatchedMutationTotalSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: runtimeThroughput,
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(missingThroughputSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${missingThroughputSummaryPath}.throughput is required for runtime-kafka-ingest.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(invalidSourceSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${invalidSourceSummaryPath}.throughput.source must be benchmark-operation-timers.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(invalidRowsSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${invalidRowsSummaryPath}.throughput.cases[0].producedRowsPerSample must be a positive integer.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(invalidTotalRowsSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${invalidTotalRowsSummaryPath}.throughput.cases[0].totalProducedRows must equal producedRowsPerSample * sampleCount (700).`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(invalidAggregateSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${invalidAggregateSummaryPath}.throughput.cases[0].aggregateRowsPerSecond must match producedRowsPerSample * 1000 / meanTotalMs.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(invalidPositiveRateSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${invalidPositiveRateSummaryPath}.throughput.cases[0].aggregateRowsPerSecond must be a positive finite number.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(invalidMinRowsSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${invalidMinRowsSummaryPath}.throughput.cases[0].minRowsPerSecond must be less than or equal to meanRowsPerSecond.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(invalidMaxTotalSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${invalidMaxTotalSummaryPath}.throughput.cases[0].meanTotalMs must be less than or equal to maxTotalMs.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(invalidProducerTimerSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${invalidProducerTimerSummaryPath}.throughput.cases[0].meanProducerSendMs must be less than or equal to meanTotalMs.`,
    );
    expect(() =>
      readBenchmarkObservation(
        runtimeTaskPaths(invalidConvergenceTimerSummaryPath, outputJsonPath),
      ),
    ).toThrow(
      `Benchmark artifact field ${invalidConvergenceTimerSummaryPath}.throughput.cases[0].meanConvergenceMs must be less than or equal to meanTotalMs.`,
    );
    expect(() =>
      readBenchmarkObservation(
        runtimeTaskPaths(invalidReadTimerMaximumSummaryPath, outputJsonPath),
      ),
    ).toThrow(
      `Benchmark artifact field ${invalidReadTimerMaximumSummaryPath}.throughput.cases[0].meanReadSnapshotMs must be less than or equal to maxReadSnapshotMs.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(invalidReadTimerTotalSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${invalidReadTimerTotalSummaryPath}.throughput.cases[0].meanReadSnapshotMs must be less than or equal to meanTotalMs.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(mismatchedNameSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${mismatchedNameSummaryPath}.throughput.cases is missing throughput case case a.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(mismatchedSampleCountSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${mismatchedSampleCountSummaryPath}.throughput.cases.case a.sampleCount must equal benchmark sampleCount 7 but was 6.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(extraThroughputCaseSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${extraThroughputCaseSummaryPath}.throughput.cases contains throughput case without matching benchmark: case b.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(mismatchedMutationTotalSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${mismatchedMutationTotalSummaryPath}.throughput.cases totalProducedRows must equal mutationCount 100 but was 700.`,
    );
  });

  it("rejects incomplete runtime benchmark health", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-runtime-health-"));
    const duplicateLaneSummaryPath = join(directory, "duplicate-lane.summary.json");
    const malformedOffsetSummaryPath = join(directory, "malformed-offset.summary.json");
    const mismatchedProducedRowsSummaryPath = join(directory, "mismatched-produced-rows.summary.json");
    const extraRowsSummaryPath = join(directory, "extra-rows.summary.json");
    const extraOffsetsSummaryPath = join(directory, "extra-offsets.summary.json");
    const missingViewServerTopicSummaryPath = join(
      directory,
      "missing-view-server-topic.summary.json",
    );
    const staleRowsSummaryPath = join(directory, "stale-rows.summary.json");
    const staleSecondLaneRowsSummaryPath = join(directory, "stale-second-lane-rows.summary.json");
    const staleOffsetsSummaryPath = join(directory, "stale-offsets.summary.json");
    const wrongViewServerTopicSummaryPath = join(directory, "wrong-view-server-topic.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      duplicateLaneSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          engine: {
            topics: {
              orders: {
                rowCount: 100,
              },
              trades: {
                rowCount: 100,
              },
            },
          },
          kafka: {
            topics: {
              sourceOrders: {
                regions: {
                  local: {
                    committedOffset: "100",
                  },
                },
                viewServerTopic: "orders",
              },
              sourceTrades: {
                regions: {
                  local: {
                    committedOffset: "100",
                  },
                },
                viewServerTopic: "trades",
              },
            },
          },
        },
        kafka: {
          ingestLanes: [
            {
              internalTopic: "orders",
              lane: "orders",
              producedRows: 100,
              region: "local",
              sourceTopic: "sourceOrders",
              sourceTopicAlias: "unique-topic-per-run:orders",
            },
            {
              internalTopic: "trades",
              lane: "orders",
              producedRows: 100,
              region: "local",
              sourceTopic: "sourceTrades",
              sourceTopicAlias: "unique-topic-per-run:trades",
            },
          ],
        },
        mutationCount: 200,
        topics: ["orders", "trades"],
      })}\n`,
    );
    writeFileSync(
      malformedOffsetSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          ...runtimeHealth,
          kafka: {
            topics: {
              sourceOrders: {
                regions: {
                  local: {
                    committedOffset: "not-an-offset",
                  },
                },
                viewServerTopic: "orders",
              },
            },
          },
        },
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
      })}\n`,
    );
    writeFileSync(
      missingViewServerTopicSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          ...runtimeHealth,
          kafka: {
            topics: {
              sourceOrders: {
                regions: {
                  local: {
                    committedOffset: "100",
                  },
                },
              },
            },
          },
        },
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
      })}\n`,
    );
    writeFileSync(
      mismatchedProducedRowsSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          engine: {
            topics: {
              orders: {
                rowCount: 99,
              },
            },
          },
          kafka: {
            topics: {
              sourceOrders: {
                regions: {
                  local: {
                    committedOffset: "99",
                  },
                },
                viewServerTopic: "orders",
              },
            },
          },
        },
        kafka: {
          ingestLanes: [
            {
              ...runtimeKafkaIngestLanes[0],
              producedRows: 99,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      extraRowsSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          ...runtimeHealth,
          engine: {
            topics: {
              orders: {
                rowCount: 101,
              },
            },
          },
        },
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
      })}\n`,
    );
    writeFileSync(
      extraOffsetsSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          ...runtimeHealth,
          kafka: {
            topics: {
              sourceOrders: {
                regions: {
                  local: {
                    committedOffset: "101",
                  },
                },
                viewServerTopic: "orders",
              },
            },
          },
        },
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
      })}\n`,
    );
    writeFileSync(
      staleRowsSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          ...runtimeHealth,
          engine: {
            topics: {
              orders: {
                rowCount: 99,
              },
            },
          },
        },
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
      })}\n`,
    );
    writeFileSync(
      staleSecondLaneRowsSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          engine: {
            topics: {
              orders: {
                rowCount: 100,
              },
              trades: {
                rowCount: 0,
              },
            },
          },
          kafka: {
            topics: {
              sourceOrders: {
                regions: {
                  local: {
                    committedOffset: "100",
                  },
                },
                viewServerTopic: "orders",
              },
              sourceTrades: {
                regions: {
                  local: {
                    committedOffset: "100",
                  },
                },
                viewServerTopic: "trades",
              },
            },
          },
        },
        kafka: {
          ingestLanes: [
            {
              internalTopic: "orders",
              lane: "orders",
              producedRows: 100,
              region: "local",
              sourceTopic: "sourceOrders",
              sourceTopicAlias: "unique-topic-per-run:orders",
            },
            {
              internalTopic: "trades",
              lane: "trades",
              producedRows: 100,
              region: "local",
              sourceTopic: "sourceTrades",
              sourceTopicAlias: "unique-topic-per-run:trades",
            },
          ],
        },
        mutationCount: 200,
        topics: ["orders", "trades"],
      })}\n`,
    );
    writeFileSync(
      staleOffsetsSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          ...runtimeHealth,
          kafka: {
            topics: {
              sourceOrders: {
                regions: {
                  local: {
                    committedOffset: "99",
                  },
                },
                viewServerTopic: "orders",
              },
            },
          },
        },
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
      })}\n`,
    );
    writeFileSync(
      wrongViewServerTopicSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          ...runtimeHealth,
          kafka: {
            topics: {
              sourceOrders: {
                regions: {
                  local: {
                    committedOffset: "100",
                  },
                },
                viewServerTopic: "trades",
              },
            },
          },
        },
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(duplicateLaneSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${duplicateLaneSummaryPath}.kafka.ingestLanes contains duplicate lane orders in lanes orders and orders.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(malformedOffsetSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${malformedOffsetSummaryPath}.health.kafka.topics.sourceOrders.regions.local.committedOffset must be a non-negative integer string.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(missingViewServerTopicSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${missingViewServerTopicSummaryPath}.health.kafka.topics.sourceOrders.viewServerTopic must be a non-empty string.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(mismatchedProducedRowsSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${mismatchedProducedRowsSummaryPath}.kafka.ingestLanes producedRows total must equal mutationCount 100 but was 99.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(staleRowsSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${staleRowsSummaryPath}.health.engine.topics.orders.rowCount must equal producedRows 100 for Kafka ingest lane orders but was 99.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(extraRowsSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${extraRowsSummaryPath}.health.engine.topics.orders.rowCount must equal producedRows 100 for Kafka ingest lane orders but was 101.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(staleSecondLaneRowsSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${staleSecondLaneRowsSummaryPath}.health.engine.topics.trades.rowCount must equal producedRows 100 for Kafka ingest lane trades but was 0.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(staleOffsetsSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${staleOffsetsSummaryPath}.health.kafka.topics.sourceOrders.regions.local.committedOffset must equal producedRows 100 for Kafka ingest lane orders but was 99.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(wrongViewServerTopicSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${wrongViewServerTopicSummaryPath}.health.kafka.topics.sourceOrders.viewServerTopic must equal internalTopic orders for Kafka ingest lane orders but was trades.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(extraOffsetsSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${extraOffsetsSummaryPath}.health.kafka.topics.sourceOrders.regions.local.committedOffset must equal producedRows 100 for Kafka ingest lane orders but was 101.`,
    );
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
      `Benchmark artifact field ${summaryPath}.artifactKind must be engine-benchmark-summary, react-browser-benchmark-summary, or runtime-benchmark-summary.`,
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
            meanMs: 15,
            p99Ms: 23,
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

  it("reports active-query sharing structural regressions", () => {
    const baseline = buildBenchmarkBaseline("active-query-sharing", [
      {
        ...observation,
        activeViewCountBeforeCleanup: 1,
      },
    ]);
    const regressed = buildBenchmarkBaseline("active-query-sharing", [
      {
        ...observation,
        activeViewCountBeforeCleanup: 50,
      },
    ]);

    expect(compareBenchmarkBaseline(baseline, regressed)).toStrictEqual({
      ok: false,
      regressions: [
        "task a: activeViewCountBeforeCleanup changed from 1 to 50.",
      ],
    });
  });

  it("does not force optional active-query structural counters onto older baselines", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation]);
    const actual = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        activeViewCountBeforeCleanup: 1,
      },
    ]);

    expect(compareBenchmarkBaseline(baseline, actual)).toStrictEqual({
      ok: true,
      regressions: [],
    });
  });

  it("rejects large relative latency regressions for grouped order-neutral baselines", () => {
    const subMillisecondObservation = {
      ...observation,
      benchmarks: [
        {
          ...observation.benchmarks[0],
          meanMs: 0.2,
          p99Ms: 0.3,
        },
      ],
    };
    const baseline = buildBenchmarkBaseline("grouped-order-neutral", [
      subMillisecondObservation,
    ]);
    const actual = buildBenchmarkBaseline("grouped-order-neutral", [
      {
        ...subMillisecondObservation,
        benchmarks: [
          {
            ...subMillisecondObservation.benchmarks[0],
            meanMs: 1.3,
            p99Ms: 2,
          },
        ],
      },
    ]);

    expect(compareBenchmarkBaseline(baseline, actual)).toStrictEqual({
      ok: false,
      regressions: [
        "task a / src/example.bench.ts > example benchmark group / case a: mean regressed from 0.200ms to 1.300ms; allowed <= 1.200ms.",
        "task a / src/example.bench.ts > example benchmark group / case a: p99 regressed from 0.300ms to 2.000ms; allowed <= 1.800ms.",
      ],
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
        "task a / src/example.bench.ts > example benchmark group / case a: mean regressed from 2.000ms to 40.000ms; allowed <= 16.000ms.",
        "task a / src/example.bench.ts > example benchmark group / case a: p99 regressed from 3.000ms to 100.000ms; allowed <= 24.000ms.",
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

  it("does not require Kafka read snapshot gates for non-Kafka throughput profiles", () => {
    const nonKafkaThroughputObservation = {
      ...observation,
      throughputCases: comparableNonKafkaRuntimeThroughputCases,
    };
    const baseline = buildBenchmarkBaseline("smoke", [nonKafkaThroughputObservation]);
    const actual = buildBenchmarkBaseline("smoke", [nonKafkaThroughputObservation]);

    expect(compareBenchmarkBaseline(baseline, actual)).toStrictEqual({
      ok: true,
      regressions: [],
    });
  });

  it("requires exact Kafka ingest mutation counts", () => {
    const kafkaObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-ingest",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: comparableRuntimeThroughputKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
      throughputCases: comparableRuntimeThroughputCases,
    };
    const baseline = buildBenchmarkBaseline("kafka-ingest", [kafkaObservation]);
    const changedMutationCount = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaObservation,
        mutationCount: 1400,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            aggregateRowsPerSecond: 2000,
            meanRowsPerSecond: 2000,
            minRowsPerSecond: 1800,
            producedRowsPerSample: 200,
            totalProducedRows: 1400,
          },
        ],
      },
    ]);

    expect(compareBenchmarkBaseline(baseline, changedMutationCount)).toStrictEqual({
      ok: false,
      regressions: [
        "task a: mutationCount changed from 700 to 1400.",
        "task a: case a throughput producedRowsPerSample changed from 100 to 200.",
        "task a: case a throughput totalProducedRows changed from 700 to 1400.",
      ],
    });
  });

  it("requires exact WebSocket firehose mutation counts", () => {
    const websocketObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-websocket-firehose",
      groupedWriteAdmission: undefined,
      mutationCount: 5,
    };
    const baseline = buildBenchmarkBaseline("websocket-firehose", [websocketObservation]);
    const increasedMutationCount = buildBenchmarkBaseline("websocket-firehose", [
      {
        ...websocketObservation,
        mutationCount: 10,
      },
    ]);

    expect(compareBenchmarkBaseline(baseline, increasedMutationCount)).toStrictEqual({
      ok: false,
      regressions: ["task a: mutationCount changed from 5 to 10."],
    });
  });

  it("reports Kafka ingest throughput regressions", () => {
    const kafkaObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-ingest",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: comparableRuntimeThroughputKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
      throughputCases: comparableRuntimeThroughputCases,
    };
    const baseline = buildBenchmarkBaseline("kafka-ingest", [kafkaObservation]);
    const regressedThroughput = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            aggregateRowsPerSecond: 400,
            maxTotalMs: 250,
            meanTotalMs: 250,
          },
        ],
      },
    ]);

    expect(compareBenchmarkBaseline(baseline, regressedThroughput)).toStrictEqual({
      ok: false,
      regressions: [
        "task a / case a: aggregateRowsPerSecond throughput regressed from 1000.000 rows/sec to 400.000 rows/sec; allowed >= 750.000 rows/sec.",
      ],
    });
  });

  it("reports Kafka commit-observed latency regressions", () => {
    const kafkaObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-ingest",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: comparableRuntimeThroughputKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
      throughputCases: comparableRuntimeThroughputCases,
    };
    const baseline = buildBenchmarkBaseline("kafka-ingest", [kafkaObservation]);
    const regressedCommitObserved = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            aggregateRowsPerSecond: 100_000 / 2_100,
            maxCommitObservedMs: 2_600,
            maxTotalMs: 2_600,
            meanCommitObservedMs: 2_076,
            meanRowsPerSecond: 100_000 / 2_100,
            meanTotalMs: 2_100,
            minRowsPerSecond: 40,
          },
        ],
      },
    ]);

    expect(compareBenchmarkBaseline(baseline, regressedCommitObserved)).toStrictEqual({
      ok: false,
      regressions: [
        "task a / case a: aggregateRowsPerSecond throughput regressed from 1000.000 rows/sec to 47.619 rows/sec; allowed >= 750.000 rows/sec.",
        "task a / case a: meanCommitObservedMs regressed from 75.000ms to 2076.000ms; allowed <= 2075.000ms.",
        "task a / case a: maxCommitObservedMs regressed from 80.000ms to 2600.000ms; allowed <= 2580.000ms.",
      ],
    });
  });

  it("reports Kafka read snapshot workload and latency regressions", () => {
    const kafkaObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-ingest",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: comparableRuntimeThroughputKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
      throughputCases: comparableRuntimeThroughputCases,
    };
    const baseline = buildBenchmarkBaseline("kafka-ingest", [kafkaObservation]);
    const regressedReadSnapshot = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            maxReadSnapshotMs: 61,
            meanReadSnapshotMs: 41,
            readSnapshotRowsPerSample: 10,
          },
        ],
      },
    ]);

    expect(compareBenchmarkBaseline(baseline, regressedReadSnapshot)).toStrictEqual({
      ok: false,
      regressions: [
        "task a: case a throughput readSnapshotRowsPerSample changed from 25 to 10.",
        "task a / case a: meanReadSnapshotMs regressed from 5.000ms to 41.000ms; allowed <= 40.000ms.",
        "task a / case a: maxReadSnapshotMs regressed from 6.000ms to 61.000ms; allowed <= 60.000ms.",
      ],
    });
  });

  it("uses the Kafka profile throughput threshold", () => {
    const kafkaObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-ingest",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: comparableRuntimeThroughputKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
      throughputCases: comparableRuntimeThroughputCases,
    };
    const baseline = buildBenchmarkBaseline("kafka-ingest", [kafkaObservation]);
    const thresholdThroughput = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            aggregateRowsPerSecond: 750,
            maxTotalMs: 133.33333333333334,
            meanRowsPerSecond: 750,
            meanTotalMs: 133.33333333333334,
            minRowsPerSecond: 700,
          },
        ],
      },
    ]);
    const belowThresholdThroughput = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            aggregateRowsPerSecond: 749,
            maxTotalMs: 133.5113484646195,
            meanRowsPerSecond: 749,
            meanTotalMs: 133.5113484646195,
            minRowsPerSecond: 700,
          },
        ],
      },
    ]);

    expect(compareBenchmarkBaseline(baseline, thresholdThroughput)).toStrictEqual({
      ok: true,
      regressions: [],
    });
    expect(compareBenchmarkBaseline(baseline, belowThresholdThroughput)).toStrictEqual({
      ok: false,
      regressions: [
        "task a / case a: aggregateRowsPerSecond throughput regressed from 1000.000 rows/sec to 749.000 rows/sec; allowed >= 750.000 rows/sec.",
      ],
    });
  });

  it("requires throughput cases in committed Kafka baselines", () => {
    const kafkaObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-ingest",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: comparableRuntimeThroughputKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
    };
    const baseline = buildBenchmarkBaseline("kafka-ingest", [kafkaObservation]);
    const completeBaseline = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaObservation,
        throughputCases: comparableRuntimeThroughputCases,
      },
    ]);
    const emptyThroughputBaseline = {
      ...completeBaseline,
      tasks: [
        {
          ...completeBaseline.tasks[0],
          throughputCases: [],
        },
      ],
    };
    const renamedThroughputBaseline = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            name: "case b",
          },
        ],
      },
    ]);
    const mismatchedSampleCountBaseline = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            sampleCount: 6,
            totalProducedRows: 600,
          },
        ],
      },
    ]);

    expect(() => validateBenchmarkBaseline(baseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].throughputCases is required for runtime-kafka-ingest.",
    );
    expect(() => validateBenchmarkBaseline(emptyThroughputBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].throughputCases must be a non-empty array.",
    );
    expect(() => validateBenchmarkBaseline(renamedThroughputBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].throughputCases is missing throughput case case a.",
    );
    expect(() => validateBenchmarkBaseline(mismatchedSampleCountBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].throughputCases.case a.sampleCount must equal benchmark sampleCount 7 but was 6.",
    );
  });

  it("requires throughput cases in committed Kafka sustained firehose baselines", () => {
    const kafkaObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-sustained-firehose",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: comparableRuntimeThroughputKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
    };
    const baseline = buildBenchmarkBaseline("kafka-sustained-firehose", [kafkaObservation]);

    expect(() => validateBenchmarkBaseline(baseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].throughputCases is required for runtime-kafka-sustained-firehose.",
    );
  });

  it("reports missing runtime metrics without gating noisy runtime metric values", () => {
    const withRuntimeMetrics = {
      ...observation,
      runtimeMetrics,
    };
    const baseline = buildBenchmarkBaseline("smoke", [withRuntimeMetrics]);
    const changedRuntimeMetrics = buildBenchmarkBaseline("smoke", [
      {
        ...withRuntimeMetrics,
        runtimeMetrics: {
          ...runtimeMetrics,
          eventLoopDelay: {
            maxMs: 999,
            meanMs: 777,
            p99Ms: 888,
          },
        },
      },
    ]);
    const missingRuntimeMetrics = buildBenchmarkBaseline("smoke", [observation]);

    expect(compareBenchmarkBaseline(baseline, changedRuntimeMetrics)).toStrictEqual({
      ok: true,
      regressions: [],
    });
    expect(compareBenchmarkBaseline(baseline, missingRuntimeMetrics)).toStrictEqual({
      ok: false,
      regressions: ["task a: runtimeMetrics presence changed."],
    });
  });

  it("preserves Kafka lag precision in runtime metrics", () => {
    const baseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        runtimeMetrics,
      },
    ]);

    expect(validateBenchmarkBaseline(baseline).tasks[0].runtimeMetrics).toStrictEqual(
      runtimeMetrics,
    );
  });

  it("normalizes safe numeric Kafka lag runtime metrics", () => {
    const safeNumericLagBaseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        runtimeMetrics: {
          ...runtimeMetrics,
          kafkaLag: {
            maxConsumerLagMessages: 5,
            sampledRegionCount: 1,
            totalConsumerLagMessages: 5,
          },
        },
      },
    ]);

    expect(validateBenchmarkBaseline(safeNumericLagBaseline).tasks[0].runtimeMetrics).toStrictEqual({
      ...runtimeMetrics,
      kafkaLag: {
        maxConsumerLagMessages: "5",
        sampledRegionCount: 1,
        totalConsumerLagMessages: "5",
      },
    });
  });

  it("rejects impossible runtime metric durations", () => {
    const negativeEventLoopDelayBaseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        runtimeMetrics: {
          ...runtimeMetrics,
          eventLoopDelay: {
            ...runtimeMetrics.eventLoopDelay,
            maxMs: -1,
          },
        },
      },
    ]);
    const inconsistentEventLoopMeanBaseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        runtimeMetrics: {
          ...runtimeMetrics,
          eventLoopDelay: {
            maxMs: 4,
            meanMs: 5,
            p99Ms: 3,
          },
        },
      },
    ]);
    const inconsistentEventLoopP99Baseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        runtimeMetrics: {
          ...runtimeMetrics,
          eventLoopDelay: {
            maxMs: 4,
            meanMs: 2,
            p99Ms: 5,
          },
        },
      },
    ]);
    const inconsistentHealthPollingBaseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        runtimeMetrics: {
          ...runtimeMetrics,
          healthPolling: {
            ...runtimeMetrics.healthPolling,
            maxMs: 8,
            totalMs: 7,
          },
        },
      },
    ]);
    const kafkaThroughputObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-ingest",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: comparableRuntimeThroughputKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
      throughputCases: comparableRuntimeThroughputCases,
    };
    const inconsistentCommitMeanBaseline = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaThroughputObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            maxCommitObservedMs: 10,
            meanCommitObservedMs: 11,
          },
        ],
      },
    ]);
    const impossibleCommitMaxBaseline = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaThroughputObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            maxCommitObservedMs: 101,
          },
        ],
      },
    ]);
    const impossibleCommitMeanBaseline = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaThroughputObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            aggregateRowsPerSecond: 100_000 / 79,
            maxCommitObservedMs: 90,
            meanCommitObservedMs: 80,
            meanRowsPerSecond: 100_000 / 79,
            meanTotalMs: 79,
          },
        ],
      },
    ]);

    expect(() => validateBenchmarkBaseline(negativeEventLoopDelayBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeMetrics.eventLoopDelay.maxMs must be a non-negative finite number.",
    );
    expect(() => validateBenchmarkBaseline(inconsistentEventLoopMeanBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeMetrics.eventLoopDelay.meanMs must be less than or equal to baseline.tasks[0].runtimeMetrics.eventLoopDelay.maxMs.",
    );
    expect(() => validateBenchmarkBaseline(inconsistentEventLoopP99Baseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeMetrics.eventLoopDelay.p99Ms must be less than or equal to baseline.tasks[0].runtimeMetrics.eventLoopDelay.maxMs.",
    );
    expect(() => validateBenchmarkBaseline(inconsistentHealthPollingBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeMetrics.healthPolling.totalMs must be greater than or equal to baseline.tasks[0].runtimeMetrics.healthPolling.maxMs.",
    );
    expect(() => validateBenchmarkBaseline(inconsistentCommitMeanBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].throughputCases[0].meanCommitObservedMs must be less than or equal to maxCommitObservedMs.",
    );
    expect(() => validateBenchmarkBaseline(impossibleCommitMaxBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].throughputCases[0].maxCommitObservedMs must be less than or equal to maxTotalMs.",
    );
    expect(() => validateBenchmarkBaseline(impossibleCommitMeanBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].throughputCases[0].meanCommitObservedMs must be less than or equal to meanTotalMs.",
    );
  });

  it("rejects unsafe numeric Kafka lag runtime metrics", () => {
    const unsafeNumericLagBaseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        runtimeMetrics: {
          ...runtimeMetrics,
          kafkaLag: {
            maxConsumerLagMessages: 9_007_199_254_740_992,
            sampledRegionCount: 1,
            totalConsumerLagMessages: "0",
          },
        },
      },
    ]);

    expect(() => validateBenchmarkBaseline(unsafeNumericLagBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeMetrics.kafkaLag.maxConsumerLagMessages must be a safe non-negative integer.",
    );
  });

  it("rejects unsafe Kafka committed offset strings", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-runtime-health-"));
    const summaryPath = join(directory, "unsafe-committed-offset.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          ...runtimeHealth,
          kafka: {
            topics: {
              sourceOrders: {
                regions: {
                  local: {
                    committedOffset: "9007199254740993",
                  },
                },
                viewServerTopic: "orders",
              },
            },
          },
        },
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: runtimeThroughput,
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() => readBenchmarkObservation(runtimeTaskPaths(summaryPath, outputJsonPath))).toThrow(
      `Benchmark artifact field ${summaryPath}.health.kafka.topics.sourceOrders.regions.local.committedOffset must be a safe integer string.`,
    );
  });

  it("requires drained final Kafka lag for sustained firehose baselines", () => {
    const twoLaneKafkaIngestLanes = [
      ...comparableRuntimeThroughputKafkaIngestLanes,
      {
        internalTopic: "trades",
        lane: "trades",
        producedRows: runtimeThroughputMutationCount,
        region: "local",
        sourceTopicAlias: "unique-topic-per-run:trades",
      },
    ];
    const sustainedFirehoseObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-sustained-firehose",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: twoLaneKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
      runtimeMetrics: {
        ...drainedRuntimeMetrics,
        kafkaLag: {
          ...drainedRuntimeMetrics.kafkaLag,
          sampledRegionCount: twoLaneKafkaIngestLanes.length,
        },
      },
      throughputCases: comparableRuntimeThroughputCases,
    };
    const baseline = buildBenchmarkBaseline("kafka-sustained-firehose", [
      sustainedFirehoseObservation,
    ]);
    const nonZeroLagBaseline = buildBenchmarkBaseline("kafka-sustained-firehose", [
      {
        ...sustainedFirehoseObservation,
        runtimeMetrics,
      },
    ]);
    const partialLagSampleBaseline = buildBenchmarkBaseline("kafka-sustained-firehose", [
      {
        ...sustainedFirehoseObservation,
        runtimeMetrics: drainedRuntimeMetrics,
      },
    ]);
    const missingMaxLagBaseline = buildBenchmarkBaseline("kafka-sustained-firehose", [
      {
        ...sustainedFirehoseObservation,
        runtimeMetrics: {
          ...sustainedFirehoseObservation.runtimeMetrics,
          kafkaLag: {
            ...sustainedFirehoseObservation.runtimeMetrics.kafkaLag,
            maxConsumerLagMessages: null,
          },
        },
      },
    ]);
    const missingRuntimeMetricsBaseline = buildBenchmarkBaseline("kafka-sustained-firehose", [
      {
        ...sustainedFirehoseObservation,
        runtimeMetrics: undefined,
      },
    ]);
    const missingKafkaIngestLanesBaseline = buildBenchmarkBaseline("kafka-sustained-firehose", [
      {
        ...sustainedFirehoseObservation,
        kafkaIngestLanes: undefined,
      },
    ]);
    const missingKafkaIngestLanesWithArtifactKindDriftBaseline = buildBenchmarkBaseline(
      "kafka-sustained-firehose",
      [
        {
          ...sustainedFirehoseObservation,
          artifactKind: "engine-benchmark-summary",
          kafkaIngestLanes: undefined,
        },
      ],
    );

    expect(compareBenchmarkBaseline(baseline, baseline)).toStrictEqual({
      ok: true,
      regressions: [],
    });
    expect(compareBenchmarkBaseline(baseline, nonZeroLagBaseline)).toStrictEqual({
      ok: false,
      regressions: [
        "task a: runtimeMetrics.kafkaLag sampled 1 regions but expected 2.",
        "task a: final Kafka lag must be 0 but was 9007199254740993.",
        "task a: max final Kafka lag must be 0 but was 9007199254740993.",
      ],
    });
    expect(compareBenchmarkBaseline(baseline, partialLagSampleBaseline)).toStrictEqual({
      ok: false,
      regressions: ["task a: runtimeMetrics.kafkaLag sampled 1 regions but expected 2."],
    });
    expect(compareBenchmarkBaseline(baseline, missingMaxLagBaseline)).toStrictEqual({
      ok: false,
      regressions: ["task a: max final Kafka lag must be 0 but was null."],
    });
    expect(compareBenchmarkBaseline(baseline, missingRuntimeMetricsBaseline)).toStrictEqual({
      ok: false,
      regressions: [
        "task a: runtimeMetrics presence changed.",
        "task a: runtimeMetrics.kafkaLag is required.",
      ],
    });
    expect(() => validateBenchmarkBaseline(missingKafkaIngestLanesBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].kafkaIngestLanes is required for runtime-kafka-sustained-firehose.",
    );
    expect(() =>
      validateBenchmarkBaseline(missingKafkaIngestLanesWithArtifactKindDriftBaseline),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].kafkaIngestLanes is required for runtime-kafka-sustained-firehose.",
    );
  });

  it("reports throughput metadata drift", () => {
    const withThroughput = {
      ...observation,
      throughputCases: comparableRuntimeThroughputCases,
    };
    const baseline = buildBenchmarkBaseline("smoke", [withThroughput]);
    const missingThroughput = buildBenchmarkBaseline("smoke", [observation]);
    const renamedThroughput = buildBenchmarkBaseline("smoke", [
      {
        ...withThroughput,
        benchmarks: [
          {
            ...observation.benchmarks[0],
            name: "case b",
          },
        ],
        benchmarkCases: ["case b"],
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            name: "case b",
          },
        ],
      },
    ]);

    expect(compareBenchmarkBaseline(baseline, missingThroughput)).toStrictEqual({
      ok: false,
      regressions: ["task a: throughputCases presence changed."],
    });
    expect(compareBenchmarkBaseline(baseline, renamedThroughput)).toStrictEqual({
      ok: false,
      regressions: [
        'task a: benchmarkCases changed from ["case a"] to ["case b"].',
        "task a: unexpected throughput case case b.",
        "task a: missing throughput case case a.",
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
        groupedKeyWidthParameters: {
          constantGroupCount: 257,
          keyWidths: [1, 2, 4, 8],
          orderedKeyCount: 8,
          semanticProbe: {
            groupByEightOrderedTotalRows: 4,
            groupByEightTotalRows: 5,
            groupByFourTotalRows: 3,
            groupByOneTotalRows: 1,
            groupByTwoTotalRows: 2,
            orderedFirstGroupKey8: "probe-8-z",
            orderedFirstRowCount: "10",
            orderedSecondGroupKey8: "probe-8-y",
            orderedSecondRowCount: "9",
            orderedWindowRows: 4,
          },
          windowLimit: 250,
        },
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
        'task a: groupedKeyWidthParameters changed from undefined to {"constantGroupCount":257,"keyWidths":[1,2,4,8],"orderedKeyCount":8,"semanticProbe":{"groupByEightOrderedTotalRows":4,"groupByEightTotalRows":5,"groupByFourTotalRows":3,"groupByOneTotalRows":1,"groupByTwoTotalRows":2,"orderedFirstGroupKey8":"probe-8-z","orderedFirstRowCount":"10","orderedSecondGroupKey8":"probe-8-y","orderedSecondRowCount":"9","orderedWindowRows":4},"windowLimit":250}.',
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
        ...defaultBenchmarkThresholds,
        latencyMean: {
          maxAbsoluteDeltaMs: 5000,
          maxRatio: 8000,
        },
      },
    };

    expect(() => validateBenchmarkBaseline(baseline)).toThrow(
      "Benchmark artifact field baseline.thresholds must match code-owned profile thresholds.",
    );
  });

  it("rejects stale extra baseline threshold keys", () => {
    const baseline = {
      ...buildBenchmarkBaseline("smoke", [observation]),
      thresholds: {
        ...defaultBenchmarkThresholds,
        commitObservedMean: {
          maxAbsoluteDeltaMs: 10,
          maxRatio: 2,
        },
      },
    };

    expect(() => validateBenchmarkBaseline(baseline)).toThrow(
      "Benchmark artifact field baseline.thresholds must contain exactly these keys: latencyMean, latencyP99, memoryRssTotalDelta, throughputAggregateRowsPerSecond.",
    );
  });

  it("rejects stale extra nested baseline threshold keys", () => {
    const baseline = {
      ...buildBenchmarkBaseline("smoke", [observation]),
      thresholds: {
        ...defaultBenchmarkThresholds,
        latencyMean: {
          ...defaultBenchmarkThresholds.latencyMean,
          staleKey: 123,
        },
      },
    };

    expect(() => validateBenchmarkBaseline(baseline)).toThrow(
      "Benchmark artifact field baseline.thresholds.latencyMean must contain exactly these keys: maxAbsoluteDeltaMs, maxRatio.",
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
