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
type SelectedOrderRow = Pick<OrderRow, "id" | "customerId" | "price" | "status" | "updatedAt">;
type OrderSubscription = ColumnLiveViewSubscription<SelectedOrderRow>;
type OrderEvent = ColumnLiveViewEngineEvent<SelectedOrderRow>;
type OrderEventReader = () => Effect.Effect<ReadonlyArray<OrderEvent>, Cause.Done>;
type FanoutCaseName = "same-window" | "ten-window";
type CaseSpecificDeltaExpectation = (
  eventChunks: ReadonlyArray<ReadonlyArray<OrderEvent>>,
  rowId: string,
  readerCount: number,
) => void;

type FanoutCaseProfile = {
  engine: Engine | undefined;
  nextDeltaVersion: number;
  readers: ReadonlyArray<OrderEventReader>;
  scope: Scope.Closeable | undefined;
  subscriptions: ReadonlyArray<OrderSubscription>;
  nextDeltaIndex: number;
};

type BenchmarkProfile = {
  readonly rowCount: number;
  readonly subscriberCount: number;
  readonly fanoutCaseName: FanoutCaseName;
  fanoutCase: FanoutCaseProfile;
  memoryAfterSetup: BenchmarkMemorySnapshot | undefined;
};

const defaultRowCount = 100_000;
const defaultBatchSize = 10_000;
const defaultSubscriberCount = 50;
const defaultIterations = 5;
const defaultBenchmarkTimeMs = 250;
const defaultWarmupIterations = 0;
const defaultWarmupTimeMs = 0;
const defaultFanoutCaseName: FanoutCaseName = "same-window";

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
  return positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_ROWS", defaultRowCount);
};

const fanoutCaseNameFromEnv = (): FanoutCaseName => {
  const raw = process.env["VIEW_SERVER_ENGINE_BENCH_FANOUT_CASE"];
  if (raw === undefined || raw.trim() === "") {
    return defaultFanoutCaseName;
  }
  const trimmed = raw.trim();
  if (trimmed === "same-window" || trimmed === "ten-window") {
    return trimmed;
  }
  throw new Error("VIEW_SERVER_ENGINE_BENCH_FANOUT_CASE must be same-window or ten-window.");
};

const benchmarkRowCount = rowCountFromEnv();
const fanoutCaseName = fanoutCaseNameFromEnv();
const batchSize = positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE", defaultBatchSize);
const subscriberCount = positiveIntegerFromEnv(
  "VIEW_SERVER_ENGINE_BENCH_SUBSCRIBERS",
  defaultSubscriberCount,
);
const outputJsonPath = benchmarkOutputJsonPath(
  `raw-live-fanout-${fanoutCaseName}-${benchmarkRowCount}rows-${subscriberCount}subs.json`,
);
const memoryBefore = memorySnapshot();
const benchOptions = {
  iterations: positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_ITERATIONS", defaultIterations),
  time: positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_TIME_MS", defaultBenchmarkTimeMs),
  warmupIterations: nonNegativeIntegerFromEnv(
    "VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS",
    defaultWarmupIterations,
  ),
  warmupTime: nonNegativeIntegerFromEnv(
    "VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS",
    defaultWarmupTimeMs,
  ),
};

const emptyFanoutCaseProfile = (): FanoutCaseProfile => ({
  engine: undefined,
  nextDeltaVersion: Math.ceil(benchmarkRowCount / batchSize),
  readers: [],
  scope: undefined,
  subscriptions: [],
  nextDeltaIndex: benchmarkRowCount,
});

const profile: BenchmarkProfile = {
  rowCount: benchmarkRowCount,
  subscriberCount,
  fanoutCaseName,
  fanoutCase: emptyFanoutCaseProfile(),
  memoryAfterSetup: undefined,
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
  customerId: `customer-${index % 100_000}`,
  status: orderStatus(index),
  price: index % 1_000_000,
  region: region(index),
  updatedAt: index,
});

const deltaOrder = (index: number): OrderRow => ({
  id: `delta-${index}`,
  customerId: `customer-delta-${index % 100_000}`,
  status: "open",
  price: 1_000_000 + (index % 10_000),
  region: "emea",
  updatedAt: 1_000_000_000 + index,
});

const fanoutCaseWindowOffset = (caseName: FanoutCaseName): ((index: number) => number) => {
  if (caseName === "same-window") {
    return () => 0;
  }
  return (index) => index % 10;
};

const seedEngine = Effect.fn("ColumnLiveViewEngine.bench.rawLiveFanout.seed")(function* (
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

const fanoutCaseEngine = (
  benchmarkProfile: BenchmarkProfile,
  fanoutCase: FanoutCaseProfile,
): Engine => {
  if (fanoutCase.engine === undefined) {
    throw new Error(`Benchmark profile ${benchmarkProfile.rowCount} rows is not initialized.`);
  }
  return fanoutCase.engine;
};

const makeEventReader = (
  subscription: OrderSubscription,
  scope: Scope.Closeable,
): Effect.Effect<OrderEventReader> =>
  Stream.toPull(subscription.events).pipe(
    Effect.map(
      (pull): OrderEventReader =>
        () =>
          Effect.map(pull, (chunk) => [...chunk]),
    ),
    Effect.provideService(Scope.Scope, scope),
  );

const makeSubscriptions = Effect.fn("ColumnLiveViewEngine.bench.rawLiveFanout.subscribe")(
  function* (engine: Engine, count: number, windowOffset: (index: number) => number) {
    const subscriptions = yield* Effect.forEach(
      Array.from({ length: count }, (_value, index) => index),
      (index) =>
        engine.subscribe("orders", {
          select: ["id", "customerId", "price", "status", "updatedAt"],
          where: {
            status: { eq: "open" },
          },
          orderBy: [{ field: "updatedAt", direction: "desc" }],
          offset: windowOffset(index),
          limit: 50,
        }),
      { concurrency: "unbounded" },
    );
    const scope = yield* Scope.make("parallel");
    const readers = yield* Effect.forEach(subscriptions, (subscription) =>
      makeEventReader(subscription, scope),
    );
    yield* Effect.forEach(readers, (reader) => reader(), { concurrency: "unbounded" });
    return {
      readers,
      scope,
      subscriptions,
    };
  },
);

const makeFanoutCase = Effect.fn("ColumnLiveViewEngine.bench.rawLiveFanout.case.make")(function* (
  benchmarkProfile: BenchmarkProfile,
  fanoutCase: FanoutCaseProfile,
  windowOffset: (index: number) => number,
) {
  const engine = yield* createColumnLiveViewEngine({ topics: viewServer.topics });
  fanoutCase.engine = engine;
  yield* seedEngine(engine, benchmarkProfile.rowCount);
  const subscriptions = yield* makeSubscriptions(
    engine,
    benchmarkProfile.subscriberCount,
    windowOffset,
  );
  fanoutCase.engine = engine;
  fanoutCase.readers = subscriptions.readers;
  fanoutCase.scope = subscriptions.scope;
  fanoutCase.subscriptions = subscriptions.subscriptions;
});

const closeSubscriptions = Effect.fn("ColumnLiveViewEngine.bench.rawLiveFanout.closeSubscriptions")(
  function* (subscriptions: ReadonlyArray<OrderSubscription>, scope: Scope.Closeable | undefined) {
    yield* Effect.forEach(subscriptions, (subscription) => subscription.close(), {
      concurrency: "unbounded",
    });
    if (scope !== undefined) {
      yield* Scope.close(scope, Exit.void);
    }
  },
);

const publishAndRead = Effect.fn("ColumnLiveViewEngine.bench.rawLiveFanout.publishAndRead")(
  function* (benchmarkProfile: BenchmarkProfile, fanoutCase: FanoutCaseProfile) {
    const engine = fanoutCaseEngine(benchmarkProfile, fanoutCase);
    const row = deltaOrder(fanoutCase.nextDeltaIndex);
    fanoutCase.nextDeltaIndex += 1;
    fanoutCase.nextDeltaVersion += 1;
    const expectedDeltaVersion = fanoutCase.nextDeltaVersion;
    yield* engine.publish("orders", row);
    const eventChunks = yield* Effect.forEach(fanoutCase.readers, (reader) => reader(), {
      concurrency: "unbounded",
    });
    expect(eventChunks).toHaveLength(fanoutCase.readers.length);
    expect(
      eventChunks.map((events) =>
        events.some((event) => eventIsDeltaVersion(event, expectedDeltaVersion)),
      ),
    ).toStrictEqual(Array.from({ length: fanoutCase.readers.length }, () => true));
    expectCaseSpecificDelta[benchmarkProfile.fanoutCaseName](
      eventChunks,
      row.id,
      fanoutCase.readers.length,
    );
  },
);

const eventIsDeltaVersion = (event: OrderEvent, expectedDeltaVersion: number): boolean =>
  event.type === "delta" && event.toVersion === expectedDeltaVersion;

const eventIncludesDeltaRow = (event: OrderEvent, rowId: string): boolean => {
  if (event.type !== "delta") {
    return false;
  }
  return event.operations.some((operation) => {
    if (operation.type === "insert" || operation.type === "update") {
      return operation.key === rowId && operation.row.id === rowId;
    }
    return false;
  });
};

const expectSameWindowDeltaRows: CaseSpecificDeltaExpectation = (
  eventChunks,
  rowId,
  readerCount,
) => {
  expect(
    eventChunks.map((events) => events.some((event) => eventIncludesDeltaRow(event, rowId))),
  ).toStrictEqual(Array.from({ length: readerCount }, () => true));
};

const expectTenWindowDeltaRows: CaseSpecificDeltaExpectation = (
  eventChunks,
  _rowId,
  readerCount,
) => {
  expect(eventChunks).toHaveLength(readerCount);
};

const expectCaseSpecificDelta: Record<FanoutCaseName, CaseSpecificDeltaExpectation> = {
  "same-window": expectSameWindowDeltaRows,
  "ten-window": expectTenWindowDeltaRows,
};

beforeAll(async () => {
  await Effect.runPromise(
    makeFanoutCase(profile, profile.fanoutCase, fanoutCaseWindowOffset(profile.fanoutCaseName)),
  );
  profile.memoryAfterSetup = memorySnapshot();
}, 0);

afterAll(async () => {
  const memoryAfterSetup = profile.memoryAfterSetup ?? memoryBefore;
  const mutationCount = profile.fanoutCase.nextDeltaIndex;
  await Effect.runPromise(
    closeSubscriptions(profile.fanoutCase.subscriptions, profile.fanoutCase.scope),
  );
  const health =
    profile.fanoutCase.engine === undefined
      ? {
          status: "not-started",
        }
      : await Effect.runPromise(profile.fanoutCase.engine.health());
  const cleanupLeakCount = cleanupLeakCountFromEngineHealth(health);
  if (profile.fanoutCase.engine !== undefined) {
    await Effect.runPromise(profile.fanoutCase.engine.close());
  }
  profile.fanoutCase = emptyFanoutCaseProfile();
  profile.memoryAfterSetup = undefined;
  const memoryAfterBenchmark = memorySnapshot();
  writeBenchmarkArtifact({
    artifactKind: "engine-benchmark-summary",
    backpressureCount: backpressureCountFromEngineHealth(health),
    benchmarkCases: [`${profile.fanoutCaseName} subscribers publish + delta fanout`],
    benchmarkName: "raw live fanout benchmark",
    benchmarkScope: "engine-raw-live-fanout",
    cleanupLeakCount,
    health,
    latency: {
      outputJsonPath,
      source: "vitest-output-json",
    },
    memoryAfterBenchmark,
    memoryAfterSetup,
    memoryBefore,
    mutationCount,
    notes: [
      "Latency percentiles are emitted by Vitest in outputJsonPath.",
      "mutationCount includes setup seed rows plus live fanout publish benchmark iterations.",
    ],
    outputJsonPath,
    queuedEventCount: queuedEventCountFromEngineHealth(health),
    rowCount: profile.rowCount,
    subscriberCount: profile.subscriberCount,
    topics: ["orders"],
  });
  failOnBenchmarkCleanupLeaks(cleanupLeakCount);
}, 0);

describe(`raw live fanout benchmark: ${profile.rowCount} rows, ${profile.subscriberCount} subscribers, ${profile.fanoutCaseName}`, () => {
  bench(
    `${profile.fanoutCaseName} subscribers publish + delta fanout`,
    async () => {
      await Effect.runPromise(publishAndRead(profile, profile.fanoutCase));
    },
    benchOptions,
  );
});
