import { afterEach, describe, expect, it, vi } from "@effect/vitest";
import { readFileSync } from "node:fs";
import {
  activeFallbackGroupedViewCountFromEngineHealth,
  activeIncrementalGroupedViewCountFromEngineHealth,
  activeViewCountFromEngineHealth,
  backpressureCountFromEngineHealth,
  benchmarkOutputJsonPath,
  benchmarkSummaryPath,
  cleanupLeakCountFromEngineHealth,
  failOnBenchmarkCleanupLeaks,
  groupedFullEvaluationCountFromEngineHealth,
  groupedPatchedEvaluationCountFromEngineHealth,
  isBenchmarkEngineHealth,
  memoryDelta,
  memorySnapshot,
  queuedEventCountFromEngineHealth,
  writeBenchmarkArtifact,
  type BenchmarkMemorySnapshot,
} from "./benchmark-artifact";

const memory = (value: number): BenchmarkMemorySnapshot => ({
  arrayBuffersBytes: value,
  externalBytes: value + 1,
  heapTotalBytes: value + 2,
  heapUsedBytes: value + 3,
  rssBytes: value + 4,
});

describe("benchmark artifact helpers", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("builds benchmark artifact paths from env or fallback", () => {
    vi.stubEnv("VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON", undefined);
    expect(benchmarkOutputJsonPath("raw-snapshot-100rows.json")).toBe(
      ".artifacts/raw-snapshot-100rows.json",
    );
    vi.stubEnv("VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON", " ");
    expect(benchmarkOutputJsonPath("raw-snapshot-100rows.json")).toBe(
      ".artifacts/raw-snapshot-100rows.json",
    );

    vi.stubEnv("VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON", " .artifacts/custom.json ");
    expect(benchmarkOutputJsonPath("raw-snapshot-100rows.json")).toBe(".artifacts/custom.json");
  });

  it("builds summary paths for json and non-json outputs", () => {
    expect(benchmarkSummaryPath(".artifacts/raw.json")).toBe(".artifacts/raw.summary.json");
    expect(benchmarkSummaryPath(".artifacts/raw")).toBe(".artifacts/raw.summary.json");
  });

  it("computes memory and health counters", () => {
    const currentMemory = memorySnapshot();
    expect(typeof currentMemory.rssBytes).toBe("number");

    expect(memoryDelta(memory(1), memory(3))).toStrictEqual({
      arrayBuffersBytes: 2,
      externalBytes: 2,
      heapTotalBytes: 2,
      heapUsedBytes: 2,
      rssBytes: 2,
    });

    const health = {
      activeSubscriptions: 2,
      backpressureEvents: 5,
      maxQueueDepth: 999,
      queuedEvents: 3,
      topics: {
        orders: {
          activeFallbackGroupedViews: 0,
          activeIncrementalGroupedViews: 0,
          activeViews: 7,
          groupedFullEvaluationCount: 0,
          groupedPatchedEvaluationCount: 0,
        },
      },
    };
    expect(isBenchmarkEngineHealth(health)).toBe(true);
    expect(cleanupLeakCountFromEngineHealth(health)).toBe(12);
    expect(backpressureCountFromEngineHealth(health)).toBe(5);
    expect(queuedEventCountFromEngineHealth(health)).toBe(3);
    expect(activeViewCountFromEngineHealth(health)).toBe(7);
    expect(activeFallbackGroupedViewCountFromEngineHealth(health)).toBe(0);
    expect(activeIncrementalGroupedViewCountFromEngineHealth(health)).toBe(0);
    expect(groupedFullEvaluationCountFromEngineHealth(health)).toBe(0);
    expect(groupedPatchedEvaluationCountFromEngineHealth(health)).toBe(0);

    const healthWithGroupedDiagnostics = {
      activeSubscriptions: 0,
      backpressureEvents: 0,
      maxQueueDepth: 0,
      queuedEvents: 0,
      topics: {
        orders: {
          activeFallbackGroupedViews: 0,
          activeIncrementalGroupedViews: 1,
          activeViews: 1,
          groupedFullEvaluationCount: 2,
          groupedPatchedEvaluationCount: 3,
        },
        trades: {
          activeFallbackGroupedViews: 1,
          activeIncrementalGroupedViews: 0,
          activeViews: 1,
          groupedFullEvaluationCount: 5,
          groupedPatchedEvaluationCount: 7,
        },
      },
    };
    expect(isBenchmarkEngineHealth(healthWithGroupedDiagnostics)).toBe(true);
    expect(groupedFullEvaluationCountFromEngineHealth(healthWithGroupedDiagnostics)).toBe(7);
    expect(groupedPatchedEvaluationCountFromEngineHealth(healthWithGroupedDiagnostics)).toBe(10);

    const minimalTopicHealth = {
      activeSubscriptions: 0,
      backpressureEvents: 0,
      maxQueueDepth: 0,
      queuedEvents: 0,
      topics: {
        orders: {
          activeViews: 1,
        },
      },
    };
    expect(isBenchmarkEngineHealth(minimalTopicHealth)).toBe(true);
    expect(activeFallbackGroupedViewCountFromEngineHealth(minimalTopicHealth)).toBe(0);
    expect(activeIncrementalGroupedViewCountFromEngineHealth(minimalTopicHealth)).toBe(0);
    expect(groupedFullEvaluationCountFromEngineHealth(minimalTopicHealth)).toBe(0);
    expect(groupedPatchedEvaluationCountFromEngineHealth(minimalTopicHealth)).toBe(0);

    const healthWithoutTopics = {
      activeSubscriptions: 2,
      backpressureEvents: 5,
      maxQueueDepth: 999,
      queuedEvents: 3,
    };
    expect(isBenchmarkEngineHealth(healthWithoutTopics)).toBe(true);
    expect(activeViewCountFromEngineHealth(healthWithoutTopics)).toBe(0);
    expect(activeFallbackGroupedViewCountFromEngineHealth(healthWithoutTopics)).toBe(0);
    expect(activeIncrementalGroupedViewCountFromEngineHealth(healthWithoutTopics)).toBe(0);
    expect(groupedFullEvaluationCountFromEngineHealth(healthWithoutTopics)).toBe(0);
    expect(groupedPatchedEvaluationCountFromEngineHealth(healthWithoutTopics)).toBe(0);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: 3,
        topics: undefined,
      }),
    ).toBe(true);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: 3,
        topics: null,
      }),
    ).toBe(false);

    expect(isBenchmarkEngineHealth(null)).toBe(false);
    expect(isBenchmarkEngineHealth({ activeSubscriptions: 1 })).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: "2",
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: 3,
      }),
    ).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: 3,
        topics: {
          orders: {
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: 0,
            activeViews: "7",
          },
        },
      }),
    ).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: 3,
        topics: {
          orders: null,
        },
      }),
    ).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: 3,
        topics: {
          orders: {
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: 0,
          },
        },
      }),
    ).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: 3,
        topics: {
          orders: {
            activeFallbackGroupedViews: "0",
            activeIncrementalGroupedViews: 0,
            activeViews: 7,
            groupedFullEvaluationCount: 0,
            groupedPatchedEvaluationCount: 0,
          },
        },
      }),
    ).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: 3,
        topics: {
          orders: {
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: Number.NaN,
            activeViews: 7,
            groupedFullEvaluationCount: 0,
            groupedPatchedEvaluationCount: 0,
          },
        },
      }),
    ).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: Number.NaN,
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: 3,
      }),
    ).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: Number.POSITIVE_INFINITY,
        maxQueueDepth: 999,
        queuedEvents: 3,
      }),
    ).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: 5,
        maxQueueDepth: Number.NEGATIVE_INFINITY,
        queuedEvents: 3,
      }),
    ).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: Number.NaN,
      }),
    ).toBe(false);
    expect(cleanupLeakCountFromEngineHealth({})).toBe(0);
    expect(backpressureCountFromEngineHealth({})).toBe(0);
    expect(queuedEventCountFromEngineHealth({})).toBe(0);
    expect(failOnBenchmarkCleanupLeaks(0)).toBeUndefined();
    expect(() => failOnBenchmarkCleanupLeaks(2)).toThrow(
      "Benchmark cleanup leaked 2 active resource(s).",
    );
  });

  it("writes benchmark summary artifacts", () => {
    const outputJsonPath = ".artifacts/benchmark-artifact-test.json";
    writeBenchmarkArtifact({
      artifactKind: "engine-benchmark-summary",
      backpressureCount: 0,
      benchmarkCases: ["case-a"],
      benchmarkName: "benchmark artifact test",
      benchmarkScope: "engine-raw-snapshot",
      cleanupLeakCount: 0,
      groupedWriteAdmission: {
        activeFallbackGroupedViewsAfterSetup: 0,
        activeFallbackGroupedViewsBeforeCleanup: 0,
        activeIncrementalGroupedViewsAfterSetup: 2,
        activeIncrementalGroupedViewsBeforeCleanup: 2,
        activeViewsAfterSetup: 2,
        activeViewsBeforeCleanup: 2,
        configuredMode: "incremental",
        expectedAdmission: "incremental",
        groupedFullEvaluationCountAfterSetup: 0,
        groupedFullEvaluationCountBeforeCleanup: 2,
        groupedPatchedEvaluationCountAfterSetup: 0,
        groupedPatchedEvaluationCountBeforeCleanup: 3,
        incrementalAdmissionLimits: {
          maxGroups: 10,
          maxMembers: 20,
          maxMembersPerGroup: 30,
          maxRetainedValueEntries: 40,
        },
        priceThreshold: 900,
        seedMutationCount: 100,
        timedMutationCount: 10,
        writeBatchSize: 32,
      },
      health: {
        status: "ready",
      },
      latency: {
        outputJsonPath,
        source: "vitest-output-json",
      },
      memoryAfterBenchmark: memory(5),
      memoryAfterSetup: memory(3),
      memoryBefore: memory(1),
      mutationCount: 10,
      notes: ["test artifact"],
      outputJsonPath,
      preCleanupHealth: {
        status: "ready",
      },
      queuedEventCount: 0,
      rowCount: 100,
      subscriberCount: 1,
      topics: ["orders"],
    });

    expect(readFileSync(".artifacts/benchmark-artifact-test.summary.json", "utf8")).toBe(
      `${JSON.stringify(
        {
          artifactKind: "engine-benchmark-summary",
          backpressureCount: 0,
          benchmarkCases: ["case-a"],
          benchmarkName: "benchmark artifact test",
          benchmarkScope: "engine-raw-snapshot",
          cleanupLeakCount: 0,
          groupedWriteAdmission: {
            activeFallbackGroupedViewsAfterSetup: 0,
            activeFallbackGroupedViewsBeforeCleanup: 0,
            activeIncrementalGroupedViewsAfterSetup: 2,
            activeIncrementalGroupedViewsBeforeCleanup: 2,
            activeViewsAfterSetup: 2,
            activeViewsBeforeCleanup: 2,
            configuredMode: "incremental",
            expectedAdmission: "incremental",
            groupedFullEvaluationCountAfterSetup: 0,
            groupedFullEvaluationCountBeforeCleanup: 2,
            groupedPatchedEvaluationCountAfterSetup: 0,
            groupedPatchedEvaluationCountBeforeCleanup: 3,
            incrementalAdmissionLimits: {
              maxGroups: 10,
              maxMembers: 20,
              maxMembersPerGroup: 30,
              maxRetainedValueEntries: 40,
            },
            priceThreshold: 900,
            seedMutationCount: 100,
            timedMutationCount: 10,
            writeBatchSize: 32,
          },
          health: {
            status: "ready",
          },
          latency: {
            outputJsonPath,
            source: "vitest-output-json",
          },
          memory: {
            afterBenchmark: memory(5),
            afterSetup: memory(3),
            before: memory(1),
            benchmarkDelta: memoryDelta(memory(3), memory(5)),
            setupDelta: memoryDelta(memory(1), memory(3)),
            totalDelta: memoryDelta(memory(1), memory(5)),
          },
          mutationCount: 10,
          notes: ["test artifact"],
          outputJsonPath,
          preCleanupHealth: {
            status: "ready",
          },
          queuedEventCount: 0,
          rowCount: 100,
          subscriberCount: 1,
          topics: ["orders"],
        },
        undefined,
        2,
      )}\n`,
    );
  });
});
