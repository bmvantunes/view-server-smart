// Benchmarks intentionally import Vitest directly: @effect/vitest does not expose `bench`.
import { afterAll, beforeAll, bench, describe, expect } from "vitest";
import { defineViewServerConfig } from "@view-server/config";
import { Cause, Effect, Exit, Schema, Scope, Stream } from "effect";
import {
  createColumnLiveViewEngine,
  type ColumnLiveViewEngine,
  type ColumnLiveViewEngineEvent,
  type ColumnLiveViewSubscription,
} from "./index";
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
  price: Schema.Finite,
  quantity: Schema.BigInt,
  region: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  updatedAt: Schema.Number,
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
type OrderStatus = OrderRow["status"];
type GroupedWriteMode = "fallback" | "incremental";
type GroupedReaderName = "primary" | "secondary";
type GroupedEvent = ColumnLiveViewEngineEvent<object>;
type GroupedSubscription = ColumnLiveViewSubscription<object>;
type GroupedEventReader = (count: number) => Effect.Effect<ReadonlyArray<GroupedEvent>, Cause.Done>;
type DeltaDrainRecord = {
  readonly caseName: string;
  readonly fromVersion: number;
  readonly operationCount: number;
  readonly operationTypes: ReadonlyArray<string>;
  readonly readerName: GroupedReaderName;
  readonly toVersion: number;
  readonly totalRows: number;
};
type BenchmarkProfile = {
  readonly rowCount: number;
  deltaRecords: Array<DeltaDrainRecord>;
  deltaVersionCount: number;
  deleteKeys: ReadonlyArray<string>;
  engine: Engine | undefined;
  extremeReplaceIndexes: ReadonlyArray<number>;
  groupMoveKeys: ReadonlyArray<string>;
  memoryAfterSetup: BenchmarkMemorySnapshot | undefined;
  nextAppendIndex: number;
  nextDeleteKeyIndex: number;
  nextExtremeReplaceIndex: number;
  nextGroupMoveKeyIndex: number;
  nextSameGroupPatchKeyIndex: number;
  regionStatusReader: GroupedEventReader | undefined;
  regionStatusScope: Scope.Closeable | undefined;
  regionStatusSubscription: GroupedSubscription | undefined;
  rowMutationCount: number;
  sameGroupPatchKeys: ReadonlyArray<string>;
  statusReader: GroupedEventReader | undefined;
  statusScope: Scope.Closeable | undefined;
  statusSubscription: GroupedSubscription | undefined;
};

const defaultBenchmarkTimeMs = 0;
const defaultIterations = 5;
const defaultMutationBatchSize = 1;
const defaultRowCount = 100_000;
const defaultSeedBatchSize = 10_000;
const defaultWarmupIterations = 0;
const defaultWarmupTimeMs = 0;
const incrementalGroupedMemberTarget = 6_000;
const minimumRowCount = 128;
const priceDomainMax = 1_000_000;

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
  if (raw === undefined || raw.trim() === "") {
    return defaultRowCount;
  }
  if (raw.includes(",")) {
    throw new Error("VIEW_SERVER_ENGINE_BENCH_ROWS accepts one row count per benchmark run.");
  }
  const rowCount = positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_ROWS", defaultRowCount);
  if (rowCount >= minimumRowCount) {
    return rowCount;
  }
  throw new Error(`VIEW_SERVER_ENGINE_BENCH_ROWS must be at least ${minimumRowCount}.`);
};

const groupedWriteModeFromEnv = (): GroupedWriteMode => {
  const raw = process.env["VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_MODE"];
  if (raw === undefined || raw.trim() === "") {
    return "incremental";
  }
  const trimmed = raw.trim();
  if (trimmed === "fallback" || trimmed === "incremental") {
    return trimmed;
  }
  throw new Error("VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_MODE must be fallback or incremental.");
};

const benchmarkRowCount = rowCountFromEnv();
const groupedWriteMode = groupedWriteModeFromEnv();
const seedBatchSize = positiveIntegerFromEnv(
  "VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE",
  defaultSeedBatchSize,
);
const mutationBatchSize = positiveIntegerFromEnv(
  "VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE",
  defaultMutationBatchSize,
);
const maximumSafeMutationBatchSize = Math.max(1, Math.floor(benchmarkRowCount / 20));
if (seedBatchSize > benchmarkRowCount) {
  throw new Error("VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE must be less than or equal to row count.");
}
if (mutationBatchSize > maximumSafeMutationBatchSize) {
  throw new Error(
    `VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE must be <= ${maximumSafeMutationBatchSize} for this row count.`,
  );
}

const outputJsonPath = benchmarkOutputJsonPath(
  `grouped-write-${groupedWriteMode}-${benchmarkRowCount}rows-${mutationBatchSize}mutations.json`,
);
const expectedInitialVersion = Math.ceil(benchmarkRowCount / seedBatchSize);
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
if (benchOptions.time > 0) {
  throw new Error("Grouped write benchmark mutates shared engine state; time must stay disabled.");
}
if (benchOptions.warmupIterations > 0 || benchOptions.warmupTime > 0) {
  throw new Error(
    "Grouped write benchmark mutates shared engine state; warmup must stay disabled.",
  );
}

const profile: BenchmarkProfile = {
  rowCount: benchmarkRowCount,
  deltaRecords: [],
  deltaVersionCount: 0,
  deleteKeys: [],
  engine: undefined,
  extremeReplaceIndexes: [],
  groupMoveKeys: [],
  memoryAfterSetup: undefined,
  nextAppendIndex: benchmarkRowCount,
  nextDeleteKeyIndex: 0,
  nextExtremeReplaceIndex: 0,
  nextGroupMoveKeyIndex: 0,
  nextSameGroupPatchKeyIndex: 0,
  regionStatusReader: undefined,
  regionStatusScope: undefined,
  regionStatusSubscription: undefined,
  rowMutationCount: benchmarkRowCount,
  sameGroupPatchKeys: [],
  statusReader: undefined,
  statusScope: undefined,
  statusSubscription: undefined,
};

const orderStatus = (index: number): OrderStatus => {
  if (index % 5 === 0) {
    return "cancelled";
  }
  if (index % 3 === 0) {
    return "closed";
  }
  return "open";
};

const region = (index: number): string => {
  if (index % 7 === 0) {
    return "apac";
  }
  if (index % 5 === 0) {
    return "amer";
  }
  return "emea";
};

const seedOrder = (index: number): OrderRow => ({
  id: `order-${index}`,
  price: index % 1_000_000,
  quantity: BigInt((index % 997) + 1),
  region: region(index),
  status: orderStatus(index),
  updatedAt: index,
});

const generatedOrder = (prefix: string, index: number, generation: number): OrderRow => ({
  id: `${prefix}-${index}`,
  price: (index + generation) % 1_000_000,
  quantity: BigInt(((index + generation) % 997) + 1),
  region: region(index + generation),
  status: orderStatus(index + generation),
  updatedAt: 1_000_000_000 + generation + index,
});

const rowCountPriceCycles = (rowCount: number): number => Math.ceil(rowCount / priceDomainMax);

const effectivePriceDomain = (rowCount: number): number => Math.min(rowCount, priceDomainMax);

const incrementalPriceThreshold = (rowCount: number): number => {
  const matchingDistinctPrices = Math.max(
    1,
    Math.min(
      effectivePriceDomain(rowCount),
      Math.floor(incrementalGroupedMemberTarget / rowCountPriceCycles(rowCount)),
    ),
  );
  return effectivePriceDomain(rowCount) - matchingDistinctPrices;
};

const rowMatchesGroupedWriteMode = (index: number): boolean =>
  groupedWriteMode === "fallback" ||
  seedOrder(index).price >= incrementalPriceThreshold(benchmarkRowCount);

const appendedRows = (startIndex: number, generation: number): ReadonlyArray<OrderRow> =>
  Array.from({ length: mutationBatchSize }, (_value, offset) =>
    groupedWriteMode === "incremental"
      ? {
          ...generatedOrder("grouped-append", startIndex + offset, generation),
          price: incrementalPriceThreshold(benchmarkRowCount) + offset,
          region: "synthetic-append",
          status: "open",
        }
      : generatedOrder("grouped-append", startIndex + offset, generation),
  );

const matchingSeedKeysForStatus = (
  searchStartIndex: number,
  requiredCount: number,
  status: OrderStatus,
): ReadonlyArray<string> => {
  const keys: Array<string> = [];
  let index = searchStartIndex;
  while (index < benchmarkRowCount && keys.length < requiredCount) {
    if (orderStatus(index) === status && rowMatchesGroupedWriteMode(index)) {
      keys.push(`order-${index}`);
    }
    index += 1;
  }
  if (keys.length !== requiredCount) {
    throw new Error(
      `Expected ${requiredCount} seeded ${status} key(s) for grouped write benchmark, found ${keys.length}.`,
    );
  }
  return keys;
};

const matchingSeedIndexesForGroup = (
  requiredCount: number,
  status: OrderStatus,
  requiredRegion: string,
): ReadonlyArray<number> => {
  const rows: Array<OrderRow> = [];
  let index = 0;
  while (index < benchmarkRowCount) {
    const row = seedOrder(index);
    if (
      row.status === status &&
      row.region === requiredRegion &&
      rowMatchesGroupedWriteMode(index)
    ) {
      rows.push(row);
    }
    index += 1;
  }
  rows.sort((left, right) => left.price - right.price || left.updatedAt - right.updatedAt);
  if (rows.length < requiredCount) {
    throw new Error(
      `Expected ${requiredCount} seeded ${requiredRegion}/${status} row(s) for grouped write benchmark, found ${rows.length}.`,
    );
  }
  return rows
    .slice(0, requiredCount)
    .map((row) => Number.parseInt(row.id.slice("order-".length), 10));
};

const extremeReplacementRows = (
  indexes: ReadonlyArray<number>,
  generation: number,
): ReadonlyArray<OrderRow> =>
  indexes.map((index, offset) => ({
    ...seedOrder(index),
    price: 5_000_000 + generation + offset,
    quantity: BigInt(20_000 + offset),
    updatedAt: 5_000_000_000 + generation + offset,
  }));

const primaryGroupedAggregateQuery = () => {
  const base = {
    groupBy: ["region", "status"],
    aggregates: {
      averagePrice: { aggFunc: "avg", field: "price" },
      distinctOrders: { aggFunc: "countDistinct", field: "id" },
      maxPrice: { aggFunc: "max", field: "price" },
      minPrice: { aggFunc: "min", field: "price" },
      rowCount: { aggFunc: "count" },
      totalPrice: { aggFunc: "sum", field: "price" },
      totalQuantity: { aggFunc: "sum", field: "quantity" },
    },
    orderBy: [
      { field: "region", direction: "asc" },
      { field: "status", direction: "asc" },
    ],
    limit: 20,
  } as const;
  if (groupedWriteMode === "incremental") {
    return {
      ...base,
      where: {
        price: { gte: incrementalPriceThreshold(benchmarkRowCount) },
      },
    } as const;
  }
  return base;
};

const secondaryGroupedAggregateQuery = () => {
  const base = {
    groupBy: ["status", "region"],
    aggregates: {
      averagePrice: { aggFunc: "avg", field: "price" },
      distinctOrders: { aggFunc: "countDistinct", field: "id" },
      maxUpdatedAt: { aggFunc: "max", field: "updatedAt" },
      rowCount: { aggFunc: "count" },
      totalPrice: { aggFunc: "sum", field: "price" },
    },
    orderBy: [
      { aggregate: "totalPrice", direction: "desc" },
      { field: "status", direction: "asc" },
      { field: "region", direction: "asc" },
    ],
    limit: 20,
  } as const;
  if (groupedWriteMode === "incremental") {
    return {
      ...base,
      where: {
        price: { gte: incrementalPriceThreshold(benchmarkRowCount) },
      },
    } as const;
  }
  return base;
};

const seedEngine = Effect.fn("ColumnLiveViewEngine.bench.groupedWrite.seed")(function* (
  engine: Engine,
  rowCount: number,
) {
  let next = 0;
  while (next < rowCount) {
    const count = Math.min(seedBatchSize, rowCount - next);
    const rows = Array.from({ length: count }, (_value, offset) => seedOrder(next + offset));
    yield* engine.publishMany("orders", rows);
    next += count;
  }
});

const makeEventReader = (
  subscription: GroupedSubscription,
  scope: Scope.Closeable,
): Effect.Effect<GroupedEventReader> =>
  Stream.toPull(subscription.events).pipe(
    Effect.map(
      (pull): GroupedEventReader =>
        (count) =>
          Effect.gen(function* () {
            const events: Array<GroupedEvent> = [];
            while (events.length < count) {
              const chunk = yield* pull;
              events.push(...chunk);
            }
            if (events.length !== count) {
              throw new Error(`Expected ${count} grouped event(s), pulled ${events.length}.`);
            }
            return events;
          }),
    ),
    Effect.provideService(Scope.Scope, scope),
  );

const initialSnapshotVersion = (events: ReadonlyArray<GroupedEvent>, label: string): number => {
  const event = events[0];
  if (events.length !== 1 || event === undefined || event.type !== "snapshot") {
    throw new Error(`Expected exactly one ${label} initial snapshot event.`);
  }
  return event.version;
};

const deltaRecord = (
  events: ReadonlyArray<GroupedEvent>,
  readerName: GroupedReaderName,
  caseName: string,
): DeltaDrainRecord => {
  const event = events[0];
  if (events.length !== 1 || event === undefined || event.type !== "delta") {
    throw new Error(`Expected exactly one ${readerName} delta event for ${caseName}.`);
  }
  return {
    caseName,
    fromVersion: event.fromVersion,
    operationCount: event.operations.length,
    operationTypes: event.operations.map((operation) => operation.type),
    readerName,
    toVersion: event.toVersion,
    totalRows: event.totalRows,
  };
};

const profileEngine = (benchmarkProfile: BenchmarkProfile): Engine => {
  if (benchmarkProfile.engine === undefined) {
    throw new Error(
      `Grouped write benchmark ${benchmarkProfile.rowCount} rows is not initialized.`,
    );
  }
  return benchmarkProfile.engine;
};

const profileStatusReader = (benchmarkProfile: BenchmarkProfile): GroupedEventReader => {
  if (benchmarkProfile.statusReader === undefined) {
    throw new Error(
      `Grouped write benchmark ${benchmarkProfile.rowCount} rows has no status reader.`,
    );
  }
  return benchmarkProfile.statusReader;
};

const profileRegionStatusReader = (benchmarkProfile: BenchmarkProfile): GroupedEventReader => {
  if (benchmarkProfile.regionStatusReader === undefined) {
    throw new Error(
      `Grouped write benchmark ${benchmarkProfile.rowCount} rows has no region/status reader.`,
    );
  }
  return benchmarkProfile.regionStatusReader;
};

const drainDeltas = async (benchmarkProfile: BenchmarkProfile, caseName: string): Promise<void> => {
  const statusEvents = await Effect.runPromise(profileStatusReader(benchmarkProfile)(1));
  const regionStatusEvents = await Effect.runPromise(
    profileRegionStatusReader(benchmarkProfile)(1),
  );
  benchmarkProfile.deltaRecords.push(deltaRecord(statusEvents, "primary", caseName));
  benchmarkProfile.deltaRecords.push(deltaRecord(regionStatusEvents, "secondary", caseName));
};

beforeAll(async () => {
  const engine = Effect.runSync(createColumnLiveViewEngine({ topics: viewServer.topics }));
  await Effect.runPromise(seedEngine(engine, profile.rowCount));
  const requiredMutationKeys = benchOptions.iterations * mutationBatchSize;

  const statusSubscription = await Effect.runPromise(
    engine.subscribe("orders", primaryGroupedAggregateQuery()),
  );
  const regionStatusSubscription = await Effect.runPromise(
    engine.subscribe("orders", secondaryGroupedAggregateQuery()),
  );
  const statusScope = Effect.runSync(Scope.make("parallel"));
  const regionStatusScope = Effect.runSync(Scope.make("parallel"));
  const statusReader = await Effect.runPromise(makeEventReader(statusSubscription, statusScope));
  const regionStatusReader = await Effect.runPromise(
    makeEventReader(regionStatusSubscription, regionStatusScope),
  );
  const statusInitialVersion = initialSnapshotVersion(
    await Effect.runPromise(statusReader(1)),
    "primary grouped",
  );
  const regionStatusInitialVersion = initialSnapshotVersion(
    await Effect.runPromise(regionStatusReader(1)),
    "secondary grouped",
  );

  expect(statusInitialVersion).toBe(expectedInitialVersion);
  expect(regionStatusInitialVersion).toBe(expectedInitialVersion);

  profile.deleteKeys = matchingSeedKeysForStatus(
    Math.floor(benchmarkRowCount * 0.66),
    requiredMutationKeys,
    "open",
  );
  profile.engine = engine;
  profile.extremeReplaceIndexes = matchingSeedIndexesForGroup(requiredMutationKeys, "open", "emea");
  profile.groupMoveKeys = matchingSeedKeysForStatus(
    Math.floor(benchmarkRowCount * 0.33),
    requiredMutationKeys,
    "open",
  );
  profile.memoryAfterSetup = memorySnapshot();
  profile.regionStatusReader = regionStatusReader;
  profile.regionStatusScope = regionStatusScope;
  profile.regionStatusSubscription = regionStatusSubscription;
  profile.sameGroupPatchKeys = matchingSeedKeysForStatus(0, requiredMutationKeys, "open");
  profile.statusReader = statusReader;
  profile.statusScope = statusScope;
  profile.statusSubscription = statusSubscription;
}, 0);

afterAll(async () => {
  const expectedMutationDeltaEvents = profile.deltaVersionCount * 2;
  const expectedFromVersions = Array.from(
    { length: profile.deltaVersionCount },
    (_value, index) => expectedInitialVersion + index,
  );
  const statusFromVersions = profile.deltaRecords
    .filter((record) => record.readerName === "primary")
    .map((record) => record.fromVersion);
  const regionStatusFromVersions = profile.deltaRecords
    .filter((record) => record.readerName === "secondary")
    .map((record) => record.fromVersion);
  const memoryAfterSetup = profile.memoryAfterSetup ?? memoryBefore;
  expect(profile.deltaRecords.length).toBe(expectedMutationDeltaEvents);
  expect(statusFromVersions).toStrictEqual(expectedFromVersions);
  expect(regionStatusFromVersions).toStrictEqual(expectedFromVersions);
  for (const record of profile.deltaRecords) {
    expect(record.toVersion).toBe(record.fromVersion + 1);
    expect(record.operationCount > 0).toBe(true);
    expect(record.operationTypes.length).toBe(record.operationCount);
    expect(record.totalRows > 0).toBe(true);
  }
  if (profile.engine !== undefined) {
    const healthBeforeCleanup = await Effect.runPromise(profile.engine.health());
    expect(isBenchmarkEngineHealth(healthBeforeCleanup)).toBe(true);
    const benchmarkHealthBeforeCleanup = isBenchmarkEngineHealth(healthBeforeCleanup)
      ? healthBeforeCleanup
      : undefined;
    expect(benchmarkHealthBeforeCleanup?.activeSubscriptions).toBe(2);
    expect(benchmarkHealthBeforeCleanup?.backpressureEvents).toBe(0);
    expect(benchmarkHealthBeforeCleanup?.queuedEvents).toBe(0);
  }
  if (profile.statusSubscription !== undefined) {
    await Effect.runPromise(profile.statusSubscription.close());
    profile.statusSubscription = undefined;
  }
  if (profile.regionStatusSubscription !== undefined) {
    await Effect.runPromise(profile.regionStatusSubscription.close());
    profile.regionStatusSubscription = undefined;
  }
  if (profile.statusScope !== undefined) {
    await Effect.runPromise(Scope.close(profile.statusScope, Exit.void));
    profile.statusScope = undefined;
  }
  if (profile.regionStatusScope !== undefined) {
    await Effect.runPromise(Scope.close(profile.regionStatusScope, Exit.void));
    profile.regionStatusScope = undefined;
  }
  let health: unknown = {
    status: "not-started",
  };
  if (profile.engine !== undefined) {
    health = await Effect.runPromise(profile.engine.health());
    expect(isBenchmarkEngineHealth(health)).toBe(true);
    await Effect.runPromise(profile.engine.close());
    profile.engine = undefined;
  }
  profile.memoryAfterSetup = undefined;
  profile.regionStatusReader = undefined;
  profile.deleteKeys = [];
  profile.extremeReplaceIndexes = [];
  profile.groupMoveKeys = [];
  profile.sameGroupPatchKeys = [];
  profile.statusReader = undefined;
  const memoryAfterBenchmark = memorySnapshot();
  const cleanupLeakCount = cleanupLeakCountFromEngineHealth(health);
  writeBenchmarkArtifact({
    artifactKind: "engine-benchmark-summary",
    backpressureCount: backpressureCountFromEngineHealth(health),
    benchmarkCases: [
      "grouped publishMany append batch",
      "grouped publishMany replace extrema batch",
      "grouped patch aggregate values",
      "grouped patch group moves",
      "grouped delete existing rows",
    ],
    benchmarkName: "grouped write engine benchmark",
    benchmarkScope: "engine-grouped-write",
    cleanupLeakCount,
    health,
    latency: {
      outputJsonPath,
      source: "vitest-output-json",
    },
    memoryAfterBenchmark,
    memoryAfterSetup,
    memoryBefore,
    mutationCount: profile.rowMutationCount,
    notes: [
      "Latency percentiles are emitted by Vitest in outputJsonPath.",
      "Timed bodies include the grouped write operation and draining one delta from each active grouped subscription.",
      groupedWriteMode === "incremental"
        ? "Incremental mode uses selective grouped subscriptions sized under the current grouped incremental admission limits."
        : "Fallback mode intentionally uses broad grouped subscriptions and measures full grouped fallback rebuild pressure.",
      "The benchmark keeps two grouped live subscriptions open: primary region/status aggregates and secondary status/region aggregates.",
      `Seed row count: ${profile.rowCount}. Configured write batch size: ${mutationBatchSize}.`,
      `Grouped write mode: ${groupedWriteMode}.`,
      groupedWriteMode === "incremental"
        ? `Incremental mode price threshold: ${incrementalPriceThreshold(benchmarkRowCount)}.`
        : "Fallback mode has no selective price threshold.",
      `Grouped write engine versions during timed samples: ${profile.deltaVersionCount}.`,
    ],
    outputJsonPath,
    queuedEventCount: queuedEventCountFromEngineHealth(health),
    rowCount: profile.rowCount,
    subscriberCount: 2,
    topics: ["orders"],
  });
  failOnBenchmarkCleanupLeaks(cleanupLeakCount);
}, 0);

describe(`grouped write engine benchmark: ${profile.rowCount} rows`, () => {
  bench(
    "grouped publishMany append batch",
    async () => {
      const engine = profileEngine(profile);
      const startIndex = profile.nextAppendIndex;
      const rows = appendedRows(startIndex, startIndex);
      profile.nextAppendIndex += rows.length;
      profile.deltaVersionCount += 1;
      profile.rowMutationCount += rows.length;
      await Effect.runPromise(engine.publishMany("orders", rows));
      await drainDeltas(profile, "grouped publishMany append batch");
    },
    benchOptions,
  );

  bench(
    "grouped publishMany replace extrema batch",
    async () => {
      const engine = profileEngine(profile);
      const start = profile.nextExtremeReplaceIndex;
      const indexes = profile.extremeReplaceIndexes.slice(start, start + mutationBatchSize);
      if (indexes.length !== mutationBatchSize) {
        throw new Error("Grouped write benchmark exhausted extrema replacement indexes.");
      }
      profile.nextExtremeReplaceIndex += indexes.length;
      profile.deltaVersionCount += 1;
      profile.rowMutationCount += indexes.length;
      await Effect.runPromise(
        engine.publishMany("orders", extremeReplacementRows(indexes, profile.rowMutationCount)),
      );
      await drainDeltas(profile, "grouped publishMany replace extrema batch");
    },
    benchOptions,
  );

  bench(
    "grouped patch aggregate values",
    async () => {
      const engine = profileEngine(profile);
      for (let offset = 0; offset < mutationBatchSize; offset += 1) {
        const key = profile.sameGroupPatchKeys[profile.nextSameGroupPatchKeyIndex];
        if (key === undefined) {
          throw new Error("Grouped write benchmark exhausted same-group patch keys.");
        }
        profile.nextSameGroupPatchKeyIndex += 1;
        profile.deltaVersionCount += 1;
        profile.rowMutationCount += 1;
        await Effect.runPromise(
          engine.patch("orders", key, {
            price: 2_000_000 + profile.rowMutationCount + offset,
            quantity: BigInt(10_000 + offset),
            updatedAt: 2_000_000_000 + profile.rowMutationCount + offset,
          }),
        );
        await drainDeltas(profile, "grouped patch aggregate values");
      }
    },
    benchOptions,
  );

  bench(
    "grouped patch group moves",
    async () => {
      const engine = profileEngine(profile);
      for (let offset = 0; offset < mutationBatchSize; offset += 1) {
        const key = profile.groupMoveKeys[profile.nextGroupMoveKeyIndex];
        if (key === undefined) {
          throw new Error("Grouped write benchmark exhausted group-move keys.");
        }
        profile.nextGroupMoveKeyIndex += 1;
        profile.deltaVersionCount += 1;
        profile.rowMutationCount += 1;
        await Effect.runPromise(
          engine.patch("orders", key, {
            status: "closed",
            updatedAt: 3_000_000_000 + profile.rowMutationCount + offset,
          }),
        );
        await drainDeltas(profile, "grouped patch group moves");
      }
    },
    benchOptions,
  );

  bench(
    "grouped delete existing rows",
    async () => {
      const engine = profileEngine(profile);
      for (let offset = 0; offset < mutationBatchSize; offset += 1) {
        const key = profile.deleteKeys[profile.nextDeleteKeyIndex];
        if (key === undefined) {
          throw new Error("Grouped write benchmark exhausted delete keys.");
        }
        profile.nextDeleteKeyIndex += 1;
        profile.deltaVersionCount += 1;
        profile.rowMutationCount += 1;
        await Effect.runPromise(engine.delete("orders", key));
        await drainDeltas(profile, "grouped delete existing rows");
      }
    },
    benchOptions,
  );
});
