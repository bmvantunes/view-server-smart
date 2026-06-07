// Benchmarks intentionally import Vitest directly: @effect/vitest does not expose `bench`.
import { afterAll, beforeAll, bench, describe, expect } from "vitest";
import { defineViewServerConfig } from "@view-server/config";
import { Cause, Effect, Exit, Schema, Scope, Stream } from "effect";
import {
  createColumnLiveViewEngine,
  type ColumnLiveViewEngine,
  type ColumnLiveViewEngineError,
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
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Finite,
  region: Schema.String,
  score: Schema.Finite,
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
type SelectedOrderRow = Pick<OrderRow, "id" | "score" | "status" | "updatedAt">;
type RetainedDeltaCaseName =
  | "count-only"
  | "exhausted-lookahead"
  | "noop"
  | "predicate-enter"
  | "visible-delete";
type OrderSubscription = ColumnLiveViewSubscription<SelectedOrderRow>;
type OrderEvent = ColumnLiveViewEngineEvent<SelectedOrderRow>;
type OrderDeltaEvent = Extract<OrderEvent, { readonly type: "delta" }>;
type OrderDeltaOperations = OrderDeltaEvent["operations"];
type OrderEventReader = (count: number) => Effect.Effect<ReadonlyArray<OrderEvent>, Cause.Done>;
type CountOnlyValidation = {
  readonly caseName: "count-only";
  readonly events: ReadonlyArray<OrderEvent>;
  readonly fromVersion: number;
  readonly toVersion: number;
  readonly totalRows: number;
};
type ExhaustedLookaheadValidation = {
  readonly caseName: "exhausted-lookahead";
  readonly events: ReadonlyArray<OrderEvent>;
  readonly firstInsertedIndex: number;
  readonly firstRemovedIndex: number;
  readonly fromVersion: number;
  readonly secondInsertedIndex: number;
  readonly secondRemovedIndex: number;
  readonly toVersion: number;
  readonly totalRowsAfterFirstDelete: number;
  readonly totalRowsAfterSecondDelete: number;
};
type PredicateEnterValidation = {
  readonly caseName: "predicate-enter";
  readonly enteredRow: OrderRow;
  readonly events: ReadonlyArray<OrderEvent>;
  readonly fromVersion: number;
  readonly removedKey: string;
  readonly toVersion: number;
  readonly totalRows: number;
};
type VisibleDeleteValidation = {
  readonly caseName: "visible-delete";
  readonly events: ReadonlyArray<OrderEvent>;
  readonly fromVersion: number;
  readonly insertedIndex: number;
  readonly removedIndex: number;
  readonly toVersion: number;
  readonly totalRows: number;
};
type RecordedValidation =
  | CountOnlyValidation
  | ExhaustedLookaheadValidation
  | PredicateEnterValidation
  | VisibleDeleteValidation;
type RetainedDeltaCaseDefinition = {
  readonly benchmarkLabel: string;
  readonly subscribe: (
    engine: Engine,
  ) => Effect.Effect<OrderSubscription, ColumnLiveViewEngineError>;
  readonly run: (
    profile: BenchmarkProfile,
  ) => Effect.Effect<void, Cause.Done | ColumnLiveViewEngineError>;
};

type BenchmarkProfile = {
  readonly retainedCaseName: RetainedDeltaCaseName;
  readonly rowCount: number;
  engine: Engine | undefined;
  eventReader: OrderEventReader | undefined;
  lastDeliveredVersion: number;
  measuredMutationCount: number;
  memoryAfterSetup: BenchmarkMemorySnapshot | undefined;
  nextCountIndex: number;
  nextExhaustedDeleteIndex: number;
  nextNoopIndex: number;
  nextPredicateEnterIndex: number;
  nextVisibleDeleteIndex: number;
  scope: Scope.Closeable | undefined;
  subscription: OrderSubscription | undefined;
  validations: Array<RecordedValidation>;
};

const defaultBenchmarkTimeMs = 0;
const defaultBatchSize = 10_000;
const defaultIterations = 5;
const defaultRetainedCaseName: RetainedDeltaCaseName = "visible-delete";
const defaultRowCount = 100_000;
const defaultWarmupIterations = 0;
const defaultWarmupTimeMs = 0;
const minimumRowCount = 101;

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

const retainedCaseNameFromEnv = (): RetainedDeltaCaseName => {
  const raw = process.env["VIEW_SERVER_ENGINE_BENCH_RETAINED_CASE"];
  if (raw === undefined || raw.trim() === "") {
    return defaultRetainedCaseName;
  }
  const trimmed = raw.trim();
  if (
    trimmed === "count-only" ||
    trimmed === "exhausted-lookahead" ||
    trimmed === "noop" ||
    trimmed === "predicate-enter" ||
    trimmed === "visible-delete"
  ) {
    return trimmed;
  }
  throw new Error(
    "VIEW_SERVER_ENGINE_BENCH_RETAINED_CASE must be count-only, exhausted-lookahead, noop, predicate-enter, or visible-delete.",
  );
};

const benchmarkRowCount = rowCountFromEnv();
const retainedCaseName = retainedCaseNameFromEnv();
const batchSize = positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE", defaultBatchSize);
const outputJsonPath = benchmarkOutputJsonPath(
  `raw-active-retained-delta-${retainedCaseName}-${benchmarkRowCount}rows.json`,
);
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
if (benchOptions.warmupIterations > 0 || benchOptions.warmupTime > 0) {
  throw new Error(
    "Retained delta benchmark mutates shared engine state; warmup must stay disabled.",
  );
}
if (benchOptions.time !== 0) {
  throw new Error(
    "Retained delta benchmark is stateful; VIEW_SERVER_ENGINE_BENCH_TIME_MS must stay 0 so Vitest runs the configured iteration count exactly.",
  );
}
if (retainedCaseName === "visible-delete" && benchmarkRowCount < 50 + benchOptions.iterations) {
  throw new Error(
    "VIEW_SERVER_ENGINE_BENCH_ROWS must be at least limit + iterations for visible-delete.",
  );
}
if (
  retainedCaseName === "exhausted-lookahead" &&
  benchmarkRowCount < 50 + benchOptions.iterations * 2
) {
  throw new Error(
    "VIEW_SERVER_ENGINE_BENCH_ROWS must be at least limit + two deletions per iteration for exhausted-lookahead.",
  );
}

const seedVersion = Math.ceil(benchmarkRowCount / batchSize);
const profile: BenchmarkProfile = {
  retainedCaseName,
  rowCount: benchmarkRowCount,
  engine: undefined,
  eventReader: undefined,
  lastDeliveredVersion: seedVersion,
  measuredMutationCount: 0,
  memoryAfterSetup: undefined,
  nextCountIndex: benchmarkRowCount,
  nextExhaustedDeleteIndex: benchmarkRowCount - 1,
  nextNoopIndex: 0,
  nextPredicateEnterIndex: 0,
  nextVisibleDeleteIndex: benchmarkRowCount - 1,
  scope: undefined,
  subscription: undefined,
  validations: [],
};

const topKQuery = {
  select: ["id", "score", "status", "updatedAt"],
  where: {
    status: { eq: "open" },
  },
  orderBy: [{ field: "score", direction: "desc" }],
  limit: 50,
} as const;

const countOnlyQuery = {
  select: ["id", "score", "status", "updatedAt"],
  where: {
    status: { eq: "open" },
  },
  orderBy: [{ field: "score", direction: "desc" }],
  limit: 0,
} as const;

const subscribeTopK = (
  engine: Engine,
): Effect.Effect<OrderSubscription, ColumnLiveViewEngineError> =>
  engine.subscribe("orders", topKQuery);

const subscribeCountOnly = (
  engine: Engine,
): Effect.Effect<OrderSubscription, ColumnLiveViewEngineError> =>
  engine.subscribe("orders", countOnlyQuery);

const seedOrder = (index: number): OrderRow => ({
  id: `order-${index}`,
  customerId: `customer-${index % 100_000}`,
  status: "open",
  price: index % 1_000_000,
  region: "emea",
  score: index,
  updatedAt: index,
});

const closedNoopOrder = (index: number): OrderRow => ({
  id: `noop-${index}`,
  customerId: `customer-noop-${index}`,
  status: "closed",
  price: index,
  region: "emea",
  score: -index - 1,
  updatedAt: 2_000_000_000 + index,
});

const closedPredicateEnterOrder = (index: number): OrderRow => ({
  id: `enter-${index}`,
  customerId: `customer-enter-${index}`,
  status: "closed",
  price: index,
  region: "emea",
  score: 3_000_000_000 + index,
  updatedAt: 3_000_000_000 + index,
});

const countOrder = (index: number): OrderRow => ({
  id: `count-${index}`,
  customerId: `customer-count-${index}`,
  status: "open",
  price: index,
  region: "emea",
  score: -index - 1,
  updatedAt: 4_000_000_000 + index,
});

const seedEngine = Effect.fn("ColumnLiveViewEngine.bench.rawActiveRetainedDelta.seed")(function* (
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
    throw new Error(
      `Retained delta benchmark ${benchmarkProfile.rowCount} rows is not initialized.`,
    );
  }
  return benchmarkProfile.engine;
};

const profileEventReader = (benchmarkProfile: BenchmarkProfile): OrderEventReader => {
  if (benchmarkProfile.eventReader === undefined) {
    throw new Error(
      `Retained delta benchmark ${benchmarkProfile.rowCount} rows has no event reader.`,
    );
  }
  return benchmarkProfile.eventReader;
};

const makeEventReader = (
  subscription: OrderSubscription,
  scope: Scope.Closeable,
): Effect.Effect<OrderEventReader> =>
  Stream.toPull(subscription.events).pipe(
    Effect.map(
      (pull): OrderEventReader =>
        (count) =>
          Effect.gen(function* () {
            const events: Array<OrderEvent> = [];
            while (events.length < count) {
              const chunk = yield* pull;
              events.push(...chunk);
            }
            if (events.length !== count) {
              throw new Error(`Expected ${count} event(s), pulled ${events.length}.`);
            }
            return events;
          }),
    ),
    Effect.provideService(Scope.Scope, scope),
  );

const expectSingleDelta = (
  events: ReadonlyArray<OrderEvent>,
  expected: {
    readonly fromVersion: number;
    readonly toVersion: number;
    readonly operations: OrderDeltaOperations;
    readonly totalRows: number;
  },
): void => {
  expect(events).toStrictEqual([
    {
      type: "delta",
      topic: "orders",
      queryId: "query-0",
      fromVersion: expected.fromVersion,
      toVersion: expected.toVersion,
      operations: expected.operations,
      totalRows: expected.totalRows,
    },
  ]);
};

const validateCountOnly = (validation: CountOnlyValidation): void => {
  expectSingleDelta(validation.events, {
    fromVersion: validation.fromVersion,
    toVersion: validation.toVersion,
    operations: [],
    totalRows: validation.totalRows,
  });
};

const validateExhaustedLookahead = (validation: ExhaustedLookaheadValidation): void => {
  expect(validation.events).toStrictEqual([
    {
      type: "delta",
      topic: "orders",
      queryId: "query-0",
      fromVersion: validation.fromVersion,
      toVersion: validation.fromVersion + 1,
      operations: [
        {
          type: "remove",
          key: `order-${validation.firstRemovedIndex}`,
        },
        {
          type: "insert",
          key: `order-${validation.firstInsertedIndex}`,
          row: {
            id: `order-${validation.firstInsertedIndex}`,
            score: validation.firstInsertedIndex,
            status: "open",
            updatedAt: validation.firstInsertedIndex,
          },
          index: 49,
        },
      ],
      totalRows: validation.totalRowsAfterFirstDelete,
    },
    {
      type: "delta",
      topic: "orders",
      queryId: "query-0",
      fromVersion: validation.fromVersion + 1,
      toVersion: validation.toVersion,
      operations: [
        {
          type: "remove",
          key: `order-${validation.secondRemovedIndex}`,
        },
        {
          type: "insert",
          key: `order-${validation.secondInsertedIndex}`,
          row: {
            id: `order-${validation.secondInsertedIndex}`,
            score: validation.secondInsertedIndex,
            status: "open",
            updatedAt: validation.secondInsertedIndex,
          },
          index: 49,
        },
      ],
      totalRows: validation.totalRowsAfterSecondDelete,
    },
  ]);
};

const validatePredicateEnter = (validation: PredicateEnterValidation): void => {
  expectSingleDelta(validation.events, {
    fromVersion: validation.fromVersion,
    toVersion: validation.toVersion,
    operations: [
      {
        type: "remove",
        key: validation.removedKey,
      },
      {
        type: "insert",
        key: validation.enteredRow.id,
        row: {
          id: validation.enteredRow.id,
          score: validation.enteredRow.score,
          status: "open",
          updatedAt: validation.enteredRow.updatedAt,
        },
        index: 0,
      },
    ],
    totalRows: validation.totalRows,
  });
};

const validateVisibleDelete = (validation: VisibleDeleteValidation): void => {
  expectSingleDelta(validation.events, {
    fromVersion: validation.fromVersion,
    toVersion: validation.toVersion,
    operations: [
      {
        type: "remove",
        key: `order-${validation.removedIndex}`,
      },
      {
        type: "insert",
        key: `order-${validation.insertedIndex}`,
        row: {
          id: `order-${validation.insertedIndex}`,
          score: validation.insertedIndex,
          status: "open",
          updatedAt: validation.insertedIndex,
        },
        index: 49,
      },
    ],
    totalRows: validation.totalRows,
  });
};

const validateRecordedEvents = (validation: RecordedValidation): void => {
  switch (validation.caseName) {
    case "count-only": {
      validateCountOnly(validation);
      return;
    }
    case "exhausted-lookahead": {
      validateExhaustedLookahead(validation);
      return;
    }
    case "predicate-enter": {
      validatePredicateEnter(validation);
      return;
    }
    case "visible-delete": {
      validateVisibleDelete(validation);
      return;
    }
  }
};

const retainedCases: Record<RetainedDeltaCaseName, RetainedDeltaCaseDefinition> = {
  "count-only": {
    benchmarkLabel: "count-only retained insert delta",
    subscribe: subscribeCountOnly,
    run: Effect.fn("ColumnLiveViewEngine.bench.rawActiveRetainedDelta.countOnly")(
      function* (benchmarkProfile) {
        const engine = profileEngine(benchmarkProfile);
        const readEvent = profileEventReader(benchmarkProfile);
        const row = countOrder(benchmarkProfile.nextCountIndex);
        benchmarkProfile.nextCountIndex += 1;
        const fromVersion = benchmarkProfile.lastDeliveredVersion;
        const toVersion = fromVersion + 1;
        yield* engine.publish("orders", row);
        benchmarkProfile.measuredMutationCount += 1;
        const events = yield* readEvent(1);
        benchmarkProfile.validations.push({
          caseName: "count-only",
          events,
          fromVersion,
          toVersion,
          totalRows: benchmarkProfile.nextCountIndex,
        });
        benchmarkProfile.lastDeliveredVersion = toVersion;
      },
    ),
  },
  "exhausted-lookahead": {
    benchmarkLabel: "retained visible delete pair lookahead plus fallback",
    subscribe: subscribeTopK,
    run: Effect.fn("ColumnLiveViewEngine.bench.rawActiveRetainedDelta.exhaustedLookahead")(
      function* (benchmarkProfile) {
        const engine = profileEngine(benchmarkProfile);
        const readEvent = profileEventReader(benchmarkProfile);
        const firstRemovedIndex = benchmarkProfile.nextExhaustedDeleteIndex;
        const secondRemovedIndex = firstRemovedIndex - 1;
        const firstInsertedIndex = firstRemovedIndex - 50;
        const secondInsertedIndex = firstRemovedIndex - 51;
        benchmarkProfile.nextExhaustedDeleteIndex -= 2;
        const fromVersion = benchmarkProfile.lastDeliveredVersion;
        const toVersion = fromVersion + 2;
        yield* engine.delete("orders", `order-${firstRemovedIndex}`);
        yield* engine.delete("orders", `order-${secondRemovedIndex}`);
        benchmarkProfile.measuredMutationCount += 2;
        const events = yield* readEvent(2);
        benchmarkProfile.validations.push({
          caseName: "exhausted-lookahead",
          events,
          firstInsertedIndex,
          firstRemovedIndex,
          fromVersion,
          secondInsertedIndex,
          secondRemovedIndex,
          toVersion,
          totalRowsAfterFirstDelete: benchmarkProfile.nextExhaustedDeleteIndex + 2,
          totalRowsAfterSecondDelete: benchmarkProfile.nextExhaustedDeleteIndex + 1,
        });
        benchmarkProfile.lastDeliveredVersion = toVersion;
      },
    ),
  },
  noop: {
    benchmarkLabel: "retained nonmatching update delete no-op",
    subscribe: subscribeTopK,
    run: Effect.fn("ColumnLiveViewEngine.bench.rawActiveRetainedDelta.noop")(
      function* (benchmarkProfile) {
        const engine = profileEngine(benchmarkProfile);
        const row = closedNoopOrder(benchmarkProfile.nextNoopIndex);
        benchmarkProfile.nextNoopIndex += 1;
        yield* engine.publish("orders", row);
        yield* engine.patch("orders", row.id, {
          price: row.price + 1,
        });
        yield* engine.delete("orders", row.id);
        benchmarkProfile.measuredMutationCount += 3;
        benchmarkProfile.lastDeliveredVersion += 3;
      },
    ),
  },
  "predicate-enter": {
    benchmarkLabel: "retained predicate-enter update delta",
    subscribe: subscribeTopK,
    run: Effect.fn("ColumnLiveViewEngine.bench.rawActiveRetainedDelta.predicateEnter")(
      function* (benchmarkProfile) {
        const engine = profileEngine(benchmarkProfile);
        const readEvent = profileEventReader(benchmarkProfile);
        const predicateEnterIndex = benchmarkProfile.nextPredicateEnterIndex;
        const row = closedPredicateEnterOrder(predicateEnterIndex);
        benchmarkProfile.nextPredicateEnterIndex += 1;
        const removedKey =
          predicateEnterIndex < 50
            ? `order-${benchmarkProfile.rowCount - 50 + predicateEnterIndex}`
            : `enter-${predicateEnterIndex - 50}`;
        const fromVersion = benchmarkProfile.lastDeliveredVersion;
        const toVersion = fromVersion + 2;
        yield* engine.publish("orders", row);
        yield* engine.patch("orders", row.id, {
          status: "open",
        });
        benchmarkProfile.measuredMutationCount += 2;
        const events = yield* readEvent(1);
        benchmarkProfile.validations.push({
          caseName: "predicate-enter",
          enteredRow: row,
          events,
          fromVersion,
          removedKey,
          toVersion,
          totalRows: benchmarkProfile.rowCount + benchmarkProfile.nextPredicateEnterIndex,
        });
        benchmarkProfile.lastDeliveredVersion = toVersion;
      },
    ),
  },
  "visible-delete": {
    benchmarkLabel: "retained visible delete refill/fallback sequence",
    subscribe: subscribeTopK,
    run: Effect.fn("ColumnLiveViewEngine.bench.rawActiveRetainedDelta.visibleDelete")(
      function* (benchmarkProfile) {
        const engine = profileEngine(benchmarkProfile);
        const readEvent = profileEventReader(benchmarkProfile);
        const removedIndex = benchmarkProfile.nextVisibleDeleteIndex;
        const insertedIndex = removedIndex - 50;
        benchmarkProfile.nextVisibleDeleteIndex -= 1;
        const fromVersion = benchmarkProfile.lastDeliveredVersion;
        const toVersion = fromVersion + 1;
        yield* engine.delete("orders", `order-${removedIndex}`);
        benchmarkProfile.measuredMutationCount += 1;
        const events = yield* readEvent(1);
        benchmarkProfile.validations.push({
          caseName: "visible-delete",
          events,
          fromVersion,
          insertedIndex,
          removedIndex,
          toVersion,
          totalRows: benchmarkProfile.nextVisibleDeleteIndex + 1,
        });
        benchmarkProfile.lastDeliveredVersion = toVersion;
      },
    ),
  },
};

const retainedCase = retainedCases[profile.retainedCaseName];

beforeAll(async () => {
  const engine = Effect.runSync(createColumnLiveViewEngine({ topics: viewServer.topics }));
  await Effect.runPromise(seedEngine(engine, profile.rowCount));
  const subscription = await Effect.runPromise(retainedCase.subscribe(engine));
  const scope = Effect.runSync(Scope.make("parallel"));
  const eventReader = await Effect.runPromise(makeEventReader(subscription, scope));
  await Effect.runPromise(eventReader(1));
  profile.engine = engine;
  profile.eventReader = eventReader;
  profile.memoryAfterSetup = memorySnapshot();
  profile.scope = scope;
  profile.subscription = subscription;
}, 0);

afterAll(async () => {
  const memoryAfterSetup = profile.memoryAfterSetup ?? memoryBefore;
  let healthBeforeCleanup: unknown = {
    status: "not-started",
  };
  if (profile.engine !== undefined) {
    healthBeforeCleanup = await Effect.runPromise(profile.engine.health());
  }
  if (!isBenchmarkEngineHealth(healthBeforeCleanup)) {
    throw new Error("Retained delta benchmark expected engine health before cleanup.");
  }
  expect(healthBeforeCleanup.activeSubscriptions).toBe(1);
  expect(healthBeforeCleanup.backpressureEvents).toBe(0);
  const queuedEventCountBeforeCleanup = queuedEventCountFromEngineHealth(healthBeforeCleanup);
  expect(queuedEventCountBeforeCleanup).toBe(0);
  for (const validation of profile.validations) {
    validateRecordedEvents(validation);
  }
  profile.validations = [];
  if (profile.subscription !== undefined) {
    await Effect.runPromise(profile.subscription.close());
    profile.subscription = undefined;
  }
  if (profile.scope !== undefined) {
    await Effect.runPromise(Scope.close(profile.scope, Exit.void));
    profile.scope = undefined;
  }
  let health: unknown = {
    status: "not-started",
  };
  if (profile.engine !== undefined) {
    health = await Effect.runPromise(profile.engine.health());
    await Effect.runPromise(profile.engine.close());
    profile.engine = undefined;
  }
  profile.eventReader = undefined;
  profile.memoryAfterSetup = undefined;
  const memoryAfterBenchmark = memorySnapshot();
  const cleanupLeakCount = cleanupLeakCountFromEngineHealth(health);
  writeBenchmarkArtifact({
    artifactKind: "engine-benchmark-summary",
    backpressureCount: backpressureCountFromEngineHealth(health),
    benchmarkCases: [retainedCase.benchmarkLabel],
    benchmarkName: `raw active retained delta benchmark (${profile.retainedCaseName})`,
    benchmarkScope: "engine-raw-active-retained-delta",
    cleanupLeakCount,
    health,
    latency: {
      outputJsonPath,
      source: "vitest-output-json",
    },
    memoryAfterBenchmark,
    memoryAfterSetup,
    memoryBefore,
    mutationCount: profile.rowCount + profile.measuredMutationCount,
    notes: [
      "Latency percentiles are emitted by Vitest in outputJsonPath.",
      "mutationCount includes setup seed rows plus retained delta benchmark mutations.",
      "One retained delta case runs per process to avoid cross-case subscription queues.",
      "The noop case measures nonmatching retained update/delete fanout and asserts no queued events.",
      "Visible-delete and exhausted-lookahead cases intentionally mutate the same active top-k subscription across iterations.",
    ],
    outputJsonPath,
    queuedEventCount: queuedEventCountBeforeCleanup,
    rowCount: profile.rowCount,
    subscriberCount: 1,
    topics: ["orders"],
  });
  failOnBenchmarkCleanupLeaks(cleanupLeakCount);
}, 0);

describe(`raw active retained delta benchmark: ${profile.retainedCaseName}, ${profile.rowCount} rows`, () => {
  bench(
    retainedCase.benchmarkLabel,
    async () => {
      await Effect.runPromise(retainedCase.run(profile));
    },
    benchOptions,
  );
});
