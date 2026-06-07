import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";

declare const process: {
  readonly cwd: () => string;
  readonly env: Record<string, string | undefined>;
  readonly memoryUsage: () => {
    readonly arrayBuffers: number;
    readonly external: number;
    readonly heapTotal: number;
    readonly heapUsed: number;
    readonly rss: number;
  };
};

export type BenchmarkMemorySnapshot = {
  readonly arrayBuffersBytes: number;
  readonly externalBytes: number;
  readonly heapTotalBytes: number;
  readonly heapUsedBytes: number;
  readonly rssBytes: number;
};

export type BenchmarkMemoryDelta = {
  readonly arrayBuffersBytes: number;
  readonly externalBytes: number;
  readonly heapTotalBytes: number;
  readonly heapUsedBytes: number;
  readonly rssBytes: number;
};

export type BenchmarkEngineHealth = {
  readonly activeSubscriptions: number;
  readonly backpressureEvents: number;
  readonly maxQueueDepth: number;
  readonly queuedEvents: number;
  readonly topics?: Readonly<Record<string, BenchmarkTopicHealth>>;
};

export type BenchmarkTopicHealth = {
  readonly activeViews: number;
};

export type BenchmarkArtifactInput = {
  readonly artifactKind: "engine-benchmark-summary";
  readonly benchmarkName: string;
  readonly benchmarkScope:
    | "engine-raw-snapshot"
    | "engine-raw-predicate-index"
    | "engine-raw-live-fanout"
    | "engine-raw-active-retained-delta"
    | "engine-raw-write"
    | "engine-grouped-aggregate"
    | "engine-grouped-write";
  readonly rowCount: number;
  readonly mutationCount: number;
  readonly subscriberCount: number;
  readonly topics: ReadonlyArray<string>;
  readonly benchmarkCases: ReadonlyArray<string>;
  readonly outputJsonPath: string;
  readonly memoryBefore: BenchmarkMemorySnapshot;
  readonly memoryAfterSetup: BenchmarkMemorySnapshot;
  readonly memoryAfterBenchmark: BenchmarkMemorySnapshot;
  readonly latency: {
    readonly source: "vitest-output-json";
    readonly outputJsonPath: string;
  };
  readonly backpressureCount: number;
  readonly cleanupLeakCount: number;
  readonly queuedEventCount: number;
  readonly health: unknown;
  readonly notes: ReadonlyArray<string>;
};

export const memorySnapshot = (): BenchmarkMemorySnapshot => {
  const memory = process.memoryUsage();
  return {
    arrayBuffersBytes: memory.arrayBuffers,
    externalBytes: memory.external,
    heapTotalBytes: memory.heapTotal,
    heapUsedBytes: memory.heapUsed,
    rssBytes: memory.rss,
  };
};

export const memoryDelta = (
  before: BenchmarkMemorySnapshot,
  after: BenchmarkMemorySnapshot,
): BenchmarkMemoryDelta => ({
  arrayBuffersBytes: after.arrayBuffersBytes - before.arrayBuffersBytes,
  externalBytes: after.externalBytes - before.externalBytes,
  heapTotalBytes: after.heapTotalBytes - before.heapTotalBytes,
  heapUsedBytes: after.heapUsedBytes - before.heapUsedBytes,
  rssBytes: after.rssBytes - before.rssBytes,
});

export const benchmarkOutputJsonPath = (fallbackName: string): string => {
  const configured = process.env["VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON"];
  if (configured !== undefined && configured.trim() !== "") {
    return configured.trim();
  }
  return join(".artifacts", fallbackName);
};

export const benchmarkSummaryPath = (outputJsonPath: string): string => {
  if (outputJsonPath.endsWith(".json")) {
    return `${outputJsonPath.slice(0, -".json".length)}.summary.json`;
  }
  return `${outputJsonPath}.summary.json`;
};

export const cleanupLeakCountFromEngineHealth = (health: unknown): number => {
  if (!isBenchmarkEngineHealth(health)) {
    return 0;
  }
  return health.activeSubscriptions + health.queuedEvents + activeViewCountFromEngineHealth(health);
};

export const backpressureCountFromEngineHealth = (health: unknown): number => {
  if (!isBenchmarkEngineHealth(health)) {
    return 0;
  }
  return health.backpressureEvents;
};

export const queuedEventCountFromEngineHealth = (health: unknown): number => {
  if (!isBenchmarkEngineHealth(health)) {
    return 0;
  }
  return health.queuedEvents;
};

export const failOnBenchmarkCleanupLeaks = (cleanupLeakCount: number): void => {
  if (cleanupLeakCount > 0) {
    throw new Error(`Benchmark cleanup leaked ${cleanupLeakCount} active resource(s).`);
  }
};

export const isBenchmarkEngineHealth = (value: unknown): value is BenchmarkEngineHealth => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  if (
    !("activeSubscriptions" in value) ||
    !("backpressureEvents" in value) ||
    !("maxQueueDepth" in value) ||
    !("queuedEvents" in value)
  ) {
    return false;
  }
  const hasEngineCounters =
    typeof value.activeSubscriptions === "number" &&
    typeof value.backpressureEvents === "number" &&
    typeof value.maxQueueDepth === "number" &&
    typeof value.queuedEvents === "number";

  if (!hasEngineCounters) {
    return false;
  }

  if (!("topics" in value)) {
    return true;
  }

  const topics = value.topics;
  if (topics === undefined) {
    return true;
  }
  if (typeof topics !== "object" || topics === null) {
    return false;
  }

  for (const topic of Object.values(topics)) {
    if (
      typeof topic !== "object" ||
      topic === null ||
      !("activeViews" in topic) ||
      typeof topic.activeViews !== "number"
    ) {
      return false;
    }
  }

  return true;
};

export const activeViewCountFromEngineHealth = (health: BenchmarkEngineHealth): number => {
  if (health.topics === undefined) {
    return 0;
  }
  let activeViewCount = 0;
  for (const topic of Object.values(health.topics)) {
    activeViewCount += topic.activeViews;
  }
  return activeViewCount;
};

export const writeBenchmarkArtifact = (input: BenchmarkArtifactInput): void => {
  const summaryPath = benchmarkSummaryPath(input.outputJsonPath);
  mkdirSync(dirname(summaryPath), { recursive: true });
  writeFileSync(
    summaryPath,
    `${JSON.stringify(
      {
        artifactKind: input.artifactKind,
        backpressureCount: input.backpressureCount,
        benchmarkCases: input.benchmarkCases,
        benchmarkName: input.benchmarkName,
        benchmarkScope: input.benchmarkScope,
        cleanupLeakCount: input.cleanupLeakCount,
        health: input.health,
        latency: input.latency,
        memory: {
          afterBenchmark: input.memoryAfterBenchmark,
          afterSetup: input.memoryAfterSetup,
          before: input.memoryBefore,
          benchmarkDelta: memoryDelta(input.memoryAfterSetup, input.memoryAfterBenchmark),
          setupDelta: memoryDelta(input.memoryBefore, input.memoryAfterSetup),
          totalDelta: memoryDelta(input.memoryBefore, input.memoryAfterBenchmark),
        },
        mutationCount: input.mutationCount,
        notes: input.notes,
        outputJsonPath: input.outputJsonPath,
        queuedEventCount: input.queuedEventCount,
        rowCount: input.rowCount,
        subscriberCount: input.subscriberCount,
        topics: input.topics,
      },
      undefined,
      2,
    )}\n`,
  );
};
