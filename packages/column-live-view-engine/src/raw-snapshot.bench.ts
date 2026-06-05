// Benchmarks intentionally import Vitest directly: @effect/vitest does not expose `bench`.
import { afterAll, beforeAll, bench, describe } from "vitest";
import { defineViewServerConfig } from "@view-server/config";
import { Cause, Effect, Exit, Schema, Scope, Stream } from "effect";
import {
  createColumnLiveViewEngine,
  type ColumnLiveViewEngine,
  type ColumnLiveViewEngineEvent,
  type ColumnLiveViewSubscription,
} from "./index";

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
type SelectedOrderRow = Pick<OrderRow, "id" | "price" | "status" | "updatedAt">;
type OrderSubscription = ColumnLiveViewSubscription<SelectedOrderRow>;
type OrderEventReader = (
  count: number,
) => Effect.Effect<ReadonlyArray<ColumnLiveViewEngineEvent<SelectedOrderRow>>, Cause.Done>;

type BenchmarkProfile = {
  readonly rowCount: number;
  engine: Engine | undefined;
  eventReader: OrderEventReader | undefined;
  scope: Scope.Closeable | undefined;
  subscription: OrderSubscription | undefined;
  nextDeltaIndex: number;
};

const defaultRowCount = 100_000;
const defaultBatchSize = 10_000;
const defaultIterations = 5;
const defaultBenchmarkTimeMs = 250;
const defaultWarmupIterations = 0;
const defaultWarmupTimeMs = 0;

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
  const trimmed = raw.trim();
  if (!/^[1-9]\d*$/u.test(trimmed)) {
    throw new Error("VIEW_SERVER_ENGINE_BENCH_ROWS must be a positive integer.");
  }
  const parsed = Number.parseInt(trimmed, 10);
  if (Number.isSafeInteger(parsed) && parsed > 0) {
    return parsed;
  }
  throw new Error("VIEW_SERVER_ENGINE_BENCH_ROWS must be a positive integer.");
};

const benchmarkRowCount = rowCountFromEnv();
const batchSize = positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE", defaultBatchSize);
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

const profile: BenchmarkProfile = {
  rowCount: benchmarkRowCount,
  engine: undefined,
  eventReader: undefined,
  scope: undefined,
  subscription: undefined,
  nextDeltaIndex: benchmarkRowCount,
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

const seedEngine = Effect.fn("ColumnLiveViewEngine.bench.rawSnapshot.seed")(function* (
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

const profileEngine = (profile: BenchmarkProfile): Engine => {
  if (profile.engine === undefined) {
    throw new Error(`Benchmark profile ${profile.rowCount} rows is not initialized.`);
  }
  return profile.engine;
};

const profileEventReader = (profile: BenchmarkProfile): OrderEventReader => {
  if (profile.eventReader === undefined) {
    throw new Error(`Benchmark profile ${profile.rowCount} rows has no event reader.`);
  }
  return profile.eventReader;
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
            const events: Array<ColumnLiveViewEngineEvent<SelectedOrderRow>> = [];
            while (events.length < count) {
              const chunk = yield* pull;
              events.push(...chunk);
            }
            return events.slice(0, count);
          }),
    ),
    Effect.provideService(Scope.Scope, scope),
  );

beforeAll(async () => {
  const engine = Effect.runSync(createColumnLiveViewEngine({ topics: viewServer.topics }));
  await Effect.runPromise(seedEngine(engine, profile.rowCount));
  const subscription = await Effect.runPromise(
    engine.subscribe("orders", {
      select: ["id", "price", "status", "updatedAt"],
      where: {
        status: { eq: "open" },
      },
      orderBy: [{ field: "updatedAt", direction: "desc" }],
      limit: 50,
    }),
  );
  const scope = Effect.runSync(Scope.make("parallel"));
  const eventReader = await Effect.runPromise(makeEventReader(subscription, scope));
  await Effect.runPromise(eventReader(1));
  profile.engine = engine;
  profile.eventReader = eventReader;
  profile.scope = scope;
  profile.subscription = subscription;
}, 0);

afterAll(async () => {
  const scope = profile.scope;
  if (scope !== undefined) {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
  const subscription = profile.subscription;
  if (subscription !== undefined) {
    await Effect.runPromise(subscription.close());
  }
  const engine = profile.engine;
  if (engine !== undefined) {
    await Effect.runPromise(engine.close());
  }
}, 0);

describe(`raw snapshot and delta engine benchmark: ${profile.rowCount} rows`, () => {
  bench(
    "equality filter + top-k sort",
    async () => {
      await Effect.runPromise(
        profileEngine(profile).snapshot("orders", {
          select: ["id", "price", "status", "updatedAt"],
          where: {
            status: { eq: "open" },
          },
          orderBy: [{ field: "updatedAt", direction: "desc" }],
          limit: 50,
        }),
      );
    },
    benchOptions,
  );

  bench(
    "selective equality filter + fallback top-k sort",
    async () => {
      await Effect.runPromise(
        profileEngine(profile).snapshot("orders", {
          select: ["id", "price", "status", "updatedAt"],
          where: {
            price: { eq: Math.floor(profile.rowCount / 2) % 1_000_000 },
          },
          orderBy: [
            { field: "updatedAt", direction: "desc" },
            { field: "id", direction: "asc" },
          ],
          limit: 50,
        }),
      );
    },
    benchOptions,
  );

  bench(
    "ordered equality filter + indexed seek",
    async () => {
      await Effect.runPromise(
        profileEngine(profile).snapshot("orders", {
          select: ["id", "price", "status", "updatedAt"],
          where: {
            price: { eq: Math.floor(profile.rowCount / 2) % 1_000_000 },
          },
          orderBy: [{ field: "price", direction: "asc" }],
          limit: 50,
        }),
      );
    },
    benchOptions,
  );

  bench(
    "ordered in filter + indexed seek",
    async () => {
      await Effect.runPromise(
        profileEngine(profile).snapshot("orders", {
          select: ["id", "price", "status", "updatedAt"],
          where: {
            price: {
              in: [100, Math.floor(profile.rowCount / 2) % 1_000_000, profile.rowCount - 1],
            },
          },
          orderBy: [{ field: "price", direction: "asc" }],
          limit: 50,
        }),
      );
    },
    benchOptions,
  );

  bench(
    "range filter + top-k sort",
    async () => {
      await Effect.runPromise(
        profileEngine(profile).snapshot("orders", {
          select: ["id", "price", "region", "updatedAt"],
          where: {
            price: { gte: 500_000 },
          },
          orderBy: [{ field: "price", direction: "asc" }],
          limit: 100,
        }),
      );
    },
    benchOptions,
  );

  bench(
    "compound filter + top-k sort",
    async () => {
      await Effect.runPromise(
        profileEngine(profile).snapshot("orders", {
          select: ["id", "price", "region", "status", "updatedAt"],
          where: {
            status: { eq: "open" },
            region: { eq: "emea" },
            price: { gte: 250_000 },
          },
          orderBy: [{ field: "updatedAt", direction: "desc" }],
          limit: 50,
        }),
      );
    },
    benchOptions,
  );

  bench(
    "filtered totalRows via zero-row window",
    async () => {
      await Effect.runPromise(
        profileEngine(profile).snapshot("orders", {
          select: ["id"],
          where: {
            status: { eq: "open" },
            price: { gte: 100_000 },
          },
          limit: 0,
        }),
      );
    },
    benchOptions,
  );

  bench(
    "live subscription delta after publish",
    async () => {
      const readEvent = profileEventReader(profile);
      const engine = profileEngine(profile);
      const row = deltaOrder(profile.nextDeltaIndex);
      profile.nextDeltaIndex += 1;
      await Effect.runPromise(engine.publish("orders", row));
      await Effect.runPromise(readEvent(1));
    },
    benchOptions,
  );
});
