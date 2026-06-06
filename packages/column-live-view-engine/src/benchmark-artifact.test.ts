import { afterEach, describe, expect, it, vi } from "@effect/vitest";
import { readFileSync } from "node:fs";
import {
  activeViewCountFromEngineHealth,
  backpressureCountFromEngineHealth,
  benchmarkOutputJsonPath,
  benchmarkSummaryPath,
  cleanupLeakCountFromEngineHealth,
  failOnBenchmarkCleanupLeaks,
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
          activeViews: 7,
        },
      },
    };
    expect(isBenchmarkEngineHealth(health)).toBe(true);
    expect(cleanupLeakCountFromEngineHealth(health)).toBe(12);
    expect(backpressureCountFromEngineHealth(health)).toBe(5);
    expect(queuedEventCountFromEngineHealth(health)).toBe(3);
    expect(activeViewCountFromEngineHealth(health)).toBe(7);

    const healthWithoutTopics = {
      activeSubscriptions: 2,
      backpressureEvents: 5,
      maxQueueDepth: 999,
      queuedEvents: 3,
    };
    expect(isBenchmarkEngineHealth(healthWithoutTopics)).toBe(true);
    expect(activeViewCountFromEngineHealth(healthWithoutTopics)).toBe(0);
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
            activeViews: "7",
          },
        },
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
