// Benchmarks intentionally import Vitest directly: @effect/vitest does not expose `bench`.
import { afterAll, beforeAll, bench, describe, expect } from "vitest";
import { defineViewServerConfig } from "@view-server/config";
import { Effect, Schema } from "effect";
import { createColumnLiveViewEngine, type ColumnLiveViewEngine } from "./index";
import {
  backpressureCountFromEngineHealth,
  benchmarkOutputJsonPath,
  cleanupLeakCountFromEngineHealth,
  failOnBenchmarkCleanupLeaks,
  isBenchmarkEngineHealth,
  memorySnapshot,
  queuedEventCountFromEngineHealth,
  writeBenchmarkArtifact,
  type BenchmarkMemorySnapshot,
} from "./benchmark-artifact";

declare const process: {
  readonly env: Record<string, string | undefined>;
};

const Order = Schema.Struct({
  id: Schema.String,
  groupKey1: Schema.String,
  groupKey2: Schema.String,
  groupKey3: Schema.String,
  groupKey4: Schema.String,
  groupKey5: Schema.String,
  groupKey6: Schema.String,
  groupKey7: Schema.String,
  groupKey8: Schema.String,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

type Topics = typeof viewServer.topics;
type Engine = ColumnLiveViewEngine<Topics>;
type OrderRow = typeof Order.Type;
type GroupedOneRow = {
  readonly groupKey1: string;
  readonly rowCount: bigint;
};
type GroupedTwoRow = {
  readonly groupKey1: string;
  readonly groupKey2: string;
  readonly rowCount: bigint;
};
type GroupedFourRow = {
  readonly groupKey1: string;
  readonly groupKey2: string;
  readonly groupKey3: string;
  readonly groupKey4: string;
  readonly rowCount: bigint;
};
type GroupedEightOrderedRow = {
  readonly groupKey1: string;
  readonly groupKey2: string;
  readonly groupKey3: string;
  readonly groupKey4: string;
  readonly groupKey5: string;
  readonly groupKey6: string;
  readonly groupKey7: string;
  readonly groupKey8: string;
  readonly rowCount: bigint;
};
type GroupedKeyWidthValidation = {
  readonly groupByEightFirstRow: GroupedEightOrderedRow | undefined;
  readonly groupByEightOrderedFirstRow: GroupedEightOrderedRow | undefined;
  readonly groupByEightOrderedSecondRow: GroupedEightOrderedRow | undefined;
  readonly groupByEightOrderedTotalRows: number;
  readonly groupByEightOrderedWindowRows: number;
  readonly groupByEightTotalRows: number;
  readonly groupByEightWindowRows: number;
  readonly groupByFourFirstRow: GroupedFourRow | undefined;
  readonly groupByFourTotalRows: number;
  readonly groupByFourWindowRows: number;
  readonly groupByOneFirstRow: GroupedOneRow | undefined;
  readonly groupByOneTotalRows: number;
  readonly groupByOneWindowRows: number;
  readonly groupByTwoFirstRow: GroupedTwoRow | undefined;
  readonly groupByTwoTotalRows: number;
  readonly groupByTwoWindowRows: number;
};
type BenchmarkProfile = {
  engine: Engine | undefined;
  memoryAfterSetup: BenchmarkMemorySnapshot | undefined;
  validation: GroupedKeyWidthValidation | undefined;
};

const defaultBatchSize = 10_000;
const defaultBenchmarkTimeMs = 250;
const defaultIterations = 5;
const defaultRowCount = 100_000;
const defaultWarmupIterations = 0;
const defaultWarmupTimeMs = 0;
const minimumRowCount = 128;

const positiveIntegerFromEnv = (name: string, fallback: number): number => {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const trimmed = raw.trim();
  if (!/^[1-9]\d*$/u.test(trimmed)) {
    throw new Error(`${name} must be a positive integer.`);
  }
  const parsed = Number.parseInt(trimmed, 10);
  if (Number.isSafeInteger(parsed) && parsed > 0) {
    return parsed;
  }
  throw new Error(`${name} must be a positive integer.`);
};

const nonNegativeIntegerFromEnv = (name: string, fallback: number): number => {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const trimmed = raw.trim();
  if (!/^(0|[1-9]\d*)$/u.test(trimmed)) {
    throw new Error(`${name} must be a non-negative integer.`);
  }
  const parsed = Number.parseInt(trimmed, 10);
  if (Number.isSafeInteger(parsed) && parsed >= 0) {
    return parsed;
  }
  throw new Error(`${name} must be a non-negative integer.`);
};

const rowCountFromEnv = (): number => {
  const raw = process.env["VIEW_SERVER_ENGINE_BENCH_ROWS"];
  if (raw !== undefined && raw.includes(",")) {
    throw new Error("VIEW_SERVER_ENGINE_BENCH_ROWS accepts one row count per benchmark run.");
  }
  const rowCount = positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_ROWS", defaultRowCount);
  if (rowCount >= minimumRowCount) {
    return rowCount;
  }
  throw new Error(`VIEW_SERVER_ENGINE_BENCH_ROWS must be at least ${minimumRowCount}.`);
};

const benchmarkRowCount = rowCountFromEnv();
const batchSize = positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE", defaultBatchSize);
const constantGroupCount = Math.min(benchmarkRowCount, 257);
const benchmarkWindowLimit = 250;
const outputJsonPath = benchmarkOutputJsonPath(`grouped-key-width-${benchmarkRowCount}rows.json`);
const memoryBefore = memorySnapshot();
const benchOptions = {
  iterations: positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_ITERATIONS", defaultIterations),
  time: nonNegativeIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_TIME_MS", defaultBenchmarkTimeMs),
  warmupIterations: nonNegativeIntegerFromEnv(
    "VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS",
    defaultWarmupIterations,
  ),
  warmupTime: nonNegativeIntegerFromEnv(
    "VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS",
    defaultWarmupTimeMs,
  ),
};
const profile: BenchmarkProfile = {
  engine: undefined,
  memoryAfterSetup: undefined,
  validation: undefined,
};

const groupToken = (valueIndex: number): string => String(valueIndex).padStart(4, "0");
const groupValueRowCount = (valueIndex: number): bigint =>
  BigInt(Math.floor((benchmarkRowCount - 1 - valueIndex) / constantGroupCount) + 1);
const groupFieldValues = (valueIndex: number) => ({
  groupKey1: `key-1-${groupToken(valueIndex)}`,
  groupKey2: `key-2-${groupToken(Math.floor(valueIndex / 128))}`,
  groupKey3: `key-3-${groupToken(Math.floor((valueIndex % 128) / 64))}`,
  groupKey4: `key-4-${groupToken(Math.floor((valueIndex % 64) / 32))}`,
  groupKey5: `key-5-${groupToken(Math.floor((valueIndex % 32) / 16))}`,
  groupKey6: `key-6-${groupToken(Math.floor((valueIndex % 16) / 8))}`,
  groupKey7: `key-7-${groupToken(Math.floor((valueIndex % 8) / 4))}`,
  groupKey8: `key-8-${groupToken(valueIndex % 4)}`,
});

const expectedGroupedEightOrderedRow = (valueIndex: number): GroupedEightOrderedRow => {
  const fields = groupFieldValues(valueIndex);
  return {
    ...fields,
    rowCount: groupValueRowCount(valueIndex),
  };
};

const expectedGroupedOneRow = (valueIndex: number): GroupedOneRow => {
  const fields = groupFieldValues(valueIndex);
  return {
    groupKey1: fields.groupKey1,
    rowCount: groupValueRowCount(valueIndex),
  };
};

const expectedGroupedTwoRow = (valueIndex: number): GroupedTwoRow => {
  const fields = groupFieldValues(valueIndex);
  return {
    groupKey1: fields.groupKey1,
    groupKey2: fields.groupKey2,
    rowCount: groupValueRowCount(valueIndex),
  };
};

const expectedGroupedFourRow = (valueIndex: number): GroupedFourRow => {
  const fields = groupFieldValues(valueIndex);
  return {
    groupKey1: fields.groupKey1,
    groupKey2: fields.groupKey2,
    groupKey3: fields.groupKey3,
    groupKey4: fields.groupKey4,
    rowCount: groupValueRowCount(valueIndex),
  };
};

const seedOrder = (index: number): OrderRow => {
  const valueIndex = index % constantGroupCount;
  const fields = groupFieldValues(valueIndex);
  return {
    id: `order-${index}`,
    ...fields,
  };
};

const seedEngine = Effect.fn("ColumnLiveViewEngine.bench.groupedKeyWidth.seed")(function* (
  engine: Engine,
  rowCount: number,
) {
  let next = 0;
  while (next < rowCount) {
    const count = Math.min(batchSize, rowCount - next);
    const rows = Array.from({ length: count }, (_value, offset) => seedOrder(next + offset));
    yield* engine.publishMany("orders", rows);
    next += count;
  }
});

const profileEngine = (benchmarkProfile: BenchmarkProfile): Engine => {
  if (benchmarkProfile.engine === undefined) {
    throw new Error("Grouped key width benchmark is not initialized.");
  }
  return benchmarkProfile.engine;
};

const countAggregates = {
  rowCount: { aggFunc: "count" },
} as const;

const groupByOneQuery = () =>
  ({
    aggregates: countAggregates,
    groupBy: ["groupKey1"],
    limit: benchmarkWindowLimit,
  }) as const;

const groupByTwoQuery = () =>
  ({
    aggregates: countAggregates,
    groupBy: ["groupKey1", "groupKey2"],
    limit: benchmarkWindowLimit,
  }) as const;

const groupByFourQuery = () =>
  ({
    aggregates: countAggregates,
    groupBy: ["groupKey1", "groupKey2", "groupKey3", "groupKey4"],
    limit: benchmarkWindowLimit,
  }) as const;

const groupByEightQuery = () =>
  ({
    aggregates: countAggregates,
    groupBy: [
      "groupKey1",
      "groupKey2",
      "groupKey3",
      "groupKey4",
      "groupKey5",
      "groupKey6",
      "groupKey7",
      "groupKey8",
    ],
    limit: benchmarkWindowLimit,
  }) as const;

const groupByEightOrderedQuery = () =>
  ({
    aggregates: countAggregates,
    groupBy: [
      "groupKey1",
      "groupKey2",
      "groupKey3",
      "groupKey4",
      "groupKey5",
      "groupKey6",
      "groupKey7",
      "groupKey8",
    ],
    orderBy: [
      { field: "groupKey2", direction: "desc" },
      { field: "groupKey3", direction: "desc" },
      { field: "groupKey4", direction: "desc" },
      { field: "groupKey5", direction: "desc" },
      { field: "groupKey6", direction: "desc" },
      { field: "groupKey7", direction: "desc" },
      { field: "groupKey8", direction: "desc" },
    ],
    limit: benchmarkWindowLimit,
  }) as const;

const groupedKeyWidthParameters = (validation: GroupedKeyWidthValidation | undefined) => ({
  constantGroupCount,
  keyWidths: [1, 2, 4, 8],
  orderedKeyCount: 8,
  semanticProbe: {
    groupByEightOrderedTotalRows: validation?.groupByEightOrderedTotalRows ?? 0,
    groupByEightTotalRows: validation?.groupByEightTotalRows ?? 0,
    groupByFourTotalRows: validation?.groupByFourTotalRows ?? 0,
    groupByOneTotalRows: validation?.groupByOneTotalRows ?? 0,
    groupByTwoTotalRows: validation?.groupByTwoTotalRows ?? 0,
    orderedFirstGroupKey8: validation?.groupByEightOrderedFirstRow?.groupKey8 ?? "",
    orderedFirstRowCount: validation?.groupByEightOrderedFirstRow?.rowCount.toString() ?? "0",
    orderedSecondGroupKey8: validation?.groupByEightOrderedSecondRow?.groupKey8 ?? "",
    orderedSecondRowCount: validation?.groupByEightOrderedSecondRow?.rowCount.toString() ?? "0",
    orderedWindowRows: validation?.groupByEightOrderedWindowRows ?? 0,
  },
  windowLimit: benchmarkWindowLimit,
});

beforeAll(async () => {
  const engine = Effect.runSync(createColumnLiveViewEngine({ topics: viewServer.topics }));
  profile.engine = engine;
  await Effect.runPromise(seedEngine(engine, benchmarkRowCount));

  const groupByOneSnapshot = await Effect.runPromise(engine.snapshot("orders", groupByOneQuery()));
  const groupByTwoSnapshot = await Effect.runPromise(engine.snapshot("orders", groupByTwoQuery()));
  const groupByFourSnapshot = await Effect.runPromise(
    engine.snapshot("orders", groupByFourQuery()),
  );
  const groupByEightSnapshot = await Effect.runPromise(
    engine.snapshot("orders", groupByEightQuery()),
  );
  const groupByEightOrderedSnapshot = await Effect.runPromise(
    engine.snapshot("orders", groupByEightOrderedQuery()),
  );

  profile.memoryAfterSetup = memorySnapshot();
  profile.validation = {
    groupByEightFirstRow: groupByEightSnapshot.rows[0],
    groupByEightOrderedFirstRow: groupByEightOrderedSnapshot.rows[0],
    groupByEightOrderedSecondRow: groupByEightOrderedSnapshot.rows[1],
    groupByEightOrderedTotalRows: groupByEightOrderedSnapshot.totalRows,
    groupByEightOrderedWindowRows: groupByEightOrderedSnapshot.rows.length,
    groupByEightTotalRows: groupByEightSnapshot.totalRows,
    groupByEightWindowRows: groupByEightSnapshot.rows.length,
    groupByFourFirstRow: groupByFourSnapshot.rows[0],
    groupByFourTotalRows: groupByFourSnapshot.totalRows,
    groupByFourWindowRows: groupByFourSnapshot.rows.length,
    groupByOneFirstRow: groupByOneSnapshot.rows[0],
    groupByOneTotalRows: groupByOneSnapshot.totalRows,
    groupByOneWindowRows: groupByOneSnapshot.rows.length,
    groupByTwoFirstRow: groupByTwoSnapshot.rows[0],
    groupByTwoTotalRows: groupByTwoSnapshot.totalRows,
    groupByTwoWindowRows: groupByTwoSnapshot.rows.length,
  };
}, 0);

afterAll(async () => {
  const memoryAfterSetup = profile.memoryAfterSetup ?? memoryBefore;
  const validation = profile.validation;
  let health: unknown = {
    status: "not-started",
  };
  if (profile.engine !== undefined) {
    const engine = profile.engine;
    health = await Effect.runPromise(engine.health().pipe(Effect.ensuring(engine.close())));
    profile.engine = undefined;
  }
  profile.memoryAfterSetup = undefined;
  profile.validation = undefined;

  const memoryAfterBenchmark = memorySnapshot();
  const cleanupLeakCount = cleanupLeakCountFromEngineHealth(health);
  expect(isBenchmarkEngineHealth(health)).toBe(true);
  expect(validation).toStrictEqual({
    groupByEightFirstRow: expectedGroupedEightOrderedRow(0),
    groupByEightOrderedFirstRow: expectedGroupedEightOrderedRow(constantGroupCount - 1),
    groupByEightOrderedSecondRow: expectedGroupedEightOrderedRow(constantGroupCount - 2),
    groupByEightOrderedTotalRows: constantGroupCount,
    groupByEightOrderedWindowRows: Math.min(benchmarkWindowLimit, constantGroupCount),
    groupByEightTotalRows: constantGroupCount,
    groupByEightWindowRows: Math.min(benchmarkWindowLimit, constantGroupCount),
    groupByFourFirstRow: expectedGroupedFourRow(0),
    groupByFourTotalRows: constantGroupCount,
    groupByFourWindowRows: Math.min(benchmarkWindowLimit, constantGroupCount),
    groupByOneFirstRow: expectedGroupedOneRow(0),
    groupByOneTotalRows: constantGroupCount,
    groupByOneWindowRows: Math.min(benchmarkWindowLimit, constantGroupCount),
    groupByTwoFirstRow: expectedGroupedTwoRow(0),
    groupByTwoTotalRows: constantGroupCount,
    groupByTwoWindowRows: Math.min(benchmarkWindowLimit, constantGroupCount),
  });
  writeBenchmarkArtifact({
    artifactKind: "engine-benchmark-summary",
    backpressureCount: backpressureCountFromEngineHealth(health),
    benchmarkCases: [
      "groupBy one key",
      "groupBy two keys",
      "groupBy four keys",
      "groupBy eight keys",
      "groupBy eight ordered keys",
    ],
    benchmarkName: "grouped key width engine benchmark",
    benchmarkScope: "engine-grouped-key-width",
    cleanupLeakCount,
    groupedKeyWidthParameters: groupedKeyWidthParameters(validation),
    health,
    latency: {
      outputJsonPath,
      source: "vitest-output-json",
    },
    memoryAfterBenchmark,
    memoryAfterSetup,
    memoryBefore,
    mutationCount: benchmarkRowCount,
    notes: [
      "Latency percentiles are emitted by Vitest in outputJsonPath.",
      "The timed cases keep aggregate work intentionally small and grouped cardinality constant so grouped key width costs are visible.",
      "A separate semantic probe validates that wider grouped keys split groups and that later grouped fields affect ordering.",
      "The eight-key ordered timed case adds grouped field orderBy work on top of key materialization.",
    ],
    outputJsonPath,
    queuedEventCount: queuedEventCountFromEngineHealth(health),
    rowCount: benchmarkRowCount,
    subscriberCount: 0,
    topics: ["orders"],
  });
  failOnBenchmarkCleanupLeaks(cleanupLeakCount);
}, 0);

describe(`grouped key width engine benchmark: ${benchmarkRowCount} rows`, () => {
  bench(
    "groupBy one key",
    async () => {
      await Effect.runPromise(profileEngine(profile).snapshot("orders", groupByOneQuery()));
    },
    benchOptions,
  );

  bench(
    "groupBy two keys",
    async () => {
      await Effect.runPromise(profileEngine(profile).snapshot("orders", groupByTwoQuery()));
    },
    benchOptions,
  );

  bench(
    "groupBy four keys",
    async () => {
      await Effect.runPromise(profileEngine(profile).snapshot("orders", groupByFourQuery()));
    },
    benchOptions,
  );

  bench(
    "groupBy eight keys",
    async () => {
      await Effect.runPromise(profileEngine(profile).snapshot("orders", groupByEightQuery()));
    },
    benchOptions,
  );

  bench(
    "groupBy eight ordered keys",
    async () => {
      await Effect.runPromise(
        profileEngine(profile).snapshot("orders", groupByEightOrderedQuery()),
      );
    },
    benchOptions,
  );
});
