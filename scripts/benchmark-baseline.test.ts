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
      kafkaIngestBaseline: buildBenchmarkBaseline("kafka-ingest", [observation]).thresholds,
      kafkaSustainedFirehoseBaseline: buildBenchmarkBaseline("kafka-sustained-firehose", [
        observation,
      ]).thresholds,
      smokeBaseline: buildBenchmarkBaseline("smoke", [observation]).thresholds,
      orderNeutralBaseline: buildBenchmarkBaseline("grouped-order-neutral", [observation])
        .thresholds,
    }).toStrictEqual({
      groupedOrderNeutral: groupedOrderNeutralBenchmarkThresholds,
      kafkaIngest: kafkaIngestBenchmarkThresholds,
      kafkaSustainedFirehose: kafkaSustainedFirehoseBenchmarkThresholds,
      smoke: defaultBenchmarkThresholds,
      kafkaIngestBaseline: kafkaIngestBenchmarkThresholds,
      kafkaSustainedFirehoseBaseline: kafkaSustainedFirehoseBenchmarkThresholds,
      smokeBaseline: defaultBenchmarkThresholds,
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
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
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
      kafkaIngestLanes: comparableRuntimeKafkaIngestLanes,
      outputJsonPath,
      summaryPath,
    });
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

  it("requires exact Kafka ingest mutation counts", () => {
    const kafkaObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-ingest",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: comparableRuntimeKafkaIngestLanes,
    };
    const baseline = buildBenchmarkBaseline("kafka-ingest", [kafkaObservation]);
    const changedMutationCount = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaObservation,
        mutationCount: 99,
      },
    ]);

    expect(compareBenchmarkBaseline(baseline, changedMutationCount)).toStrictEqual({
      ok: false,
      regressions: ["task a: mutationCount changed from 100 to 99."],
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
      "Benchmark artifact field baseline.thresholds must match code-owned profile thresholds.",
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
