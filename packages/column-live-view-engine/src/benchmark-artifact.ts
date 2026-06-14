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
  readonly activeFallbackGroupedViews?: number;
  readonly activeIncrementalGroupedViews?: number;
  readonly activeViews: number;
  readonly groupedFullEvaluationCount?: number;
  readonly groupedPatchedEvaluationCount?: number;
};

export type BenchmarkGroupedWriteAdmission = {
  readonly activeFallbackGroupedViewsAfterSetup: number;
  readonly activeFallbackGroupedViewsBeforeCleanup: number;
  readonly activeIncrementalGroupedViewsAfterSetup: number;
  readonly activeIncrementalGroupedViewsBeforeCleanup: number;
  readonly activeViewsAfterSetup: number;
  readonly activeViewsBeforeCleanup: number;
  readonly configuredMode: "fallback" | "incremental";
  readonly expectedAdmission: "fallback" | "incremental";
  readonly groupedFullEvaluationCountAfterSetup?: number;
  readonly groupedFullEvaluationCountBeforeCleanup?: number;
  readonly groupedPatchedEvaluationCountAfterSetup?: number;
  readonly groupedPatchedEvaluationCountBeforeCleanup?: number;
  readonly incrementalAdmissionLimits: {
    readonly maxGroups: number;
    readonly maxMembers: number;
    readonly maxMembersPerGroup: number;
    readonly maxRetainedValueEntries: number;
  };
  readonly priceThreshold: number | null;
  readonly readerProfile?: "aggregate-ordered" | "dual" | "order-neutral";
  readonly seedMutationCount: number;
  readonly timedMutationCount: number;
  readonly writeBatchSize: number;
};

export type BenchmarkGroupedKeyWidthParameters = {
  readonly constantGroupCount: number;
  readonly keyWidths: ReadonlyArray<number>;
  readonly orderedKeyCount: number;
  readonly semanticProbe: {
    readonly groupByEightTotalRows: number;
    readonly groupByEightOrderedTotalRows: number;
    readonly groupByFourTotalRows: number;
    readonly groupByOneTotalRows: number;
    readonly groupByTwoTotalRows: number;
    readonly orderedFirstGroupKey8: string;
    readonly orderedFirstRowCount: string;
    readonly orderedSecondGroupKey8: string;
    readonly orderedSecondRowCount: string;
    readonly orderedWindowRows: number;
  };
  readonly windowLimit: number;
};

export type BenchmarkArtifactInput = {
  readonly artifactKind: "engine-benchmark-summary";
  readonly benchmarkName: string;
  readonly benchmarkScope:
    | "engine-raw-snapshot"
    | "engine-raw-predicate-index"
    | "engine-raw-live-fanout"
    | "engine-query-delta-operations"
    | "engine-raw-active-retained-delta"
    | "engine-raw-write"
    | "engine-grouped-aggregate"
    | "engine-grouped-key-width"
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
  readonly groupedKeyWidthParameters?: BenchmarkGroupedKeyWidthParameters;
  readonly groupedWriteAdmission?: BenchmarkGroupedWriteAdmission;
  readonly queuedEventCount: number;
  readonly health: unknown;
  readonly notes: ReadonlyArray<string>;
  readonly preCleanupHealth?: unknown;
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
    isFiniteNumber(value.activeSubscriptions) &&
    isFiniteNumber(value.backpressureEvents) &&
    isFiniteNumber(value.maxQueueDepth) &&
    isFiniteNumber(value.queuedEvents);

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
    if (!isBenchmarkTopicHealth(topic)) {
      return false;
    }
  }

  return true;
};

const isFiniteNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value);

const isOptionalFiniteNumber = (value: unknown): value is number | undefined =>
  value === undefined || isFiniteNumber(value);

const isBenchmarkTopicHealth = (value: unknown): value is BenchmarkTopicHealth => {
  if (typeof value !== "object" || value === null || !("activeViews" in value)) {
    return false;
  }
  const activeFallbackGroupedViews =
    "activeFallbackGroupedViews" in value ? value.activeFallbackGroupedViews : undefined;
  const activeIncrementalGroupedViews =
    "activeIncrementalGroupedViews" in value ? value.activeIncrementalGroupedViews : undefined;
  const groupedFullEvaluationCount =
    "groupedFullEvaluationCount" in value ? value.groupedFullEvaluationCount : undefined;
  const groupedPatchedEvaluationCount =
    "groupedPatchedEvaluationCount" in value ? value.groupedPatchedEvaluationCount : undefined;
  return (
    isFiniteNumber(value.activeViews) &&
    isOptionalFiniteNumber(activeFallbackGroupedViews) &&
    isOptionalFiniteNumber(activeIncrementalGroupedViews) &&
    isOptionalFiniteNumber(groupedFullEvaluationCount) &&
    isOptionalFiniteNumber(groupedPatchedEvaluationCount)
  );
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

export const activeFallbackGroupedViewCountFromEngineHealth = (
  health: BenchmarkEngineHealth,
): number => {
  if (health.topics === undefined) {
    return 0;
  }
  let activeViewCount = 0;
  for (const topic of Object.values(health.topics)) {
    activeViewCount += topic.activeFallbackGroupedViews ?? 0;
  }
  return activeViewCount;
};

export const activeIncrementalGroupedViewCountFromEngineHealth = (
  health: BenchmarkEngineHealth,
): number => {
  if (health.topics === undefined) {
    return 0;
  }
  let activeViewCount = 0;
  for (const topic of Object.values(health.topics)) {
    activeViewCount += topic.activeIncrementalGroupedViews ?? 0;
  }
  return activeViewCount;
};

export const groupedFullEvaluationCountFromEngineHealth = (
  health: BenchmarkEngineHealth,
): number => {
  if (health.topics === undefined) {
    return 0;
  }
  let groupedFullEvaluationCount = 0;
  for (const topic of Object.values(health.topics)) {
    groupedFullEvaluationCount += topic.groupedFullEvaluationCount ?? 0;
  }
  return groupedFullEvaluationCount;
};

export const groupedPatchedEvaluationCountFromEngineHealth = (
  health: BenchmarkEngineHealth,
): number => {
  if (health.topics === undefined) {
    return 0;
  }
  let groupedPatchedEvaluationCount = 0;
  for (const topic of Object.values(health.topics)) {
    groupedPatchedEvaluationCount += topic.groupedPatchedEvaluationCount ?? 0;
  }
  return groupedPatchedEvaluationCount;
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
        groupedKeyWidthParameters: input.groupedKeyWidthParameters,
        groupedWriteAdmission: input.groupedWriteAdmission,
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
        preCleanupHealth: input.preCleanupHealth,
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
