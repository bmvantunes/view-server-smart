// Benchmarks intentionally import Vitest directly: @effect/vitest does not expose `bench`.
import { afterAll, beforeAll, bench, describe } from "vitest";
import { defineViewServerConfig } from "@view-server/config";
import { Effect, Schema } from "effect";
import { fromStringUnsafe } from "effect/BigDecimal";
import { createColumnLiveViewEngine, type ColumnLiveViewEngine } from "./index";
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
  quantity: Schema.BigInt,
  decimalPrice: Schema.BigDecimal,
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
type WriteMode = "base" | "indexed";

type BenchmarkProfile = {
  currentRowCount: number;
  engine: Engine | undefined;
  memoryAfterSetup: BenchmarkMemorySnapshot | undefined;
  nextAppendIndex: number;
  nextDeleteIndex: number;
  nextPatchGeneration: number;
  nextReadAfterDeleteGeneration: number;
  nextReadAfterReplaceGeneration: number;
  nextReplaceGeneration: number;
  rowCount: number;
};

type BenchmarkCase = {
  readonly name: string;
  readonly run: () => Promise<void>;
};

const defaultBatchSize = 1_000;
const defaultBenchmarkTimeMs = 0;
const defaultIterations = 5;
const defaultRowCount = 100_000;
const defaultWarmupIterations = 0;
const defaultWarmupTimeMs = 0;
const defaultWriteMode: WriteMode = "indexed";

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

const writeModeFromEnv = (): WriteMode => {
  const raw = process.env["VIEW_SERVER_ENGINE_BENCH_WRITE_MODE"];
  if (raw === undefined || raw.trim() === "") {
    return defaultWriteMode;
  }
  const trimmed = raw.trim();
  if (trimmed === "base" || trimmed === "indexed") {
    return trimmed;
  }
  throw new Error("VIEW_SERVER_ENGINE_BENCH_WRITE_MODE must be base or indexed.");
};

const benchmarkRowCount = rowCountFromEnv();
const writeMode = writeModeFromEnv();
const batchSize = positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE", defaultBatchSize);
if (batchSize > benchmarkRowCount) {
  throw new Error("VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE must be less than or equal to row count.");
}
const outputJsonPath = benchmarkOutputJsonPath(
  `raw-write-${writeMode}-${benchmarkRowCount}rows.json`,
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
if (benchOptions.time > 0 || benchOptions.warmupIterations > 0 || benchOptions.warmupTime > 0) {
  throw new Error(
    "Raw write benchmark mutates shared engine state; time and warmup must stay disabled.",
  );
}

const profile: BenchmarkProfile = {
  currentRowCount: benchmarkRowCount,
  engine: undefined,
  memoryAfterSetup: undefined,
  nextAppendIndex: benchmarkRowCount,
  nextDeleteIndex: 0,
  nextPatchGeneration: 0,
  nextReadAfterDeleteGeneration: 0,
  nextReadAfterReplaceGeneration: 0,
  nextReplaceGeneration: 0,
  rowCount: benchmarkRowCount,
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

const benchmarkMutationCount = (): number =>
  profile.rowCount +
  (profile.nextAppendIndex - benchmarkRowCount) +
  profile.nextReplaceGeneration * batchSize +
  profile.nextPatchGeneration * batchSize +
  profile.nextDeleteIndex * 2 +
  (writeMode === "indexed"
    ? profile.nextReadAfterReplaceGeneration + profile.nextReadAfterDeleteGeneration * 3
    : 0);

const benchmarkCases = [
  "publish append single row",
  "patch existing rows one by one",
  ...(writeMode === "indexed"
    ? ["replace single row then indexed snapshot", "delete non-last row then indexed snapshot"]
    : []),
  "publishMany append batch",
  "publishMany replace existing batch",
  "append then delete batch",
];

const seedOrder = (index: number): OrderRow => ({
  id: `order-${index}`,
  customerId: `customer-${index % 100_000}`,
  status: orderStatus(index),
  price: index % 1_000_000,
  quantity: BigInt(index % 1_000_000),
  decimalPrice: fromStringUnsafe(String(index % 1_000_000)),
  region: region(index),
  updatedAt: index,
});

const generatedOrder = (prefix: string, index: number, generation: number): OrderRow => ({
  id: `${prefix}-${index}`,
  customerId: `customer-${index % 100_000}`,
  status: orderStatus(index + generation),
  price: (index + generation) % 1_000_000,
  quantity: BigInt((index + generation) % 1_000_000),
  decimalPrice: fromStringUnsafe(String((index + generation) % 1_000_000)),
  region: region(index + generation),
  updatedAt: 1_000_000_000 + generation + index,
});

const writeBatch = (
  prefix: string,
  startIndex: number,
  generation: number,
): ReadonlyArray<OrderRow> =>
  Array.from({ length: batchSize }, (_value, offset) =>
    generatedOrder(prefix, startIndex + offset, generation),
  );

const seedEngine = Effect.fn("ColumnLiveViewEngine.bench.rawWrite.seed")(function* (
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

const warmReadPathState = Effect.fn("ColumnLiveViewEngine.bench.rawWrite.warmReadPathState")(
  function* (engine: Engine) {
    const select = [
      "id",
      "customerId",
      "price",
      "quantity",
      "decimalPrice",
      "status",
      "updatedAt",
    ] as const;
    yield* engine.snapshot("orders", {
      select,
      where: {
        customerId: { eq: "customer-1" },
      },
      limit: 50,
    });
    yield* engine.snapshot("orders", {
      select,
      where: {
        status: { eq: "open" },
      },
      limit: 50,
    });
    yield* engine.snapshot("orders", {
      select,
      where: {
        price: { gte: 0 },
        status: { eq: "open" },
      },
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 50,
    });
    yield* engine.snapshot("orders", {
      select,
      where: {
        quantity: { gte: 0n },
        status: { eq: "open" },
      },
      orderBy: [{ field: "quantity", direction: "asc" }],
      limit: 50,
    });
    yield* engine.snapshot("orders", {
      select,
      where: {
        decimalPrice: { gte: fromStringUnsafe("0") },
        status: { eq: "open" },
      },
      orderBy: [{ field: "decimalPrice", direction: "asc" }],
      limit: 50,
    });
  },
);

const prepareWriteMode = Effect.fn("ColumnLiveViewEngine.bench.rawWrite.prepareWriteMode")(
  function* (engine: Engine, mode: WriteMode) {
    if (mode === "indexed") {
      yield* warmReadPathState(engine);
    }
  },
);

const profileEngine = (profile: BenchmarkProfile): Engine => {
  if (profile.engine === undefined) {
    throw new Error(`Raw write benchmark profile ${profile.rowCount} rows is not initialized.`);
  }
  return profile.engine;
};

beforeAll(async () => {
  const engine = Effect.runSync(createColumnLiveViewEngine({ topics: viewServer.topics }));
  await Effect.runPromise(seedEngine(engine, profile.rowCount));
  await Effect.runPromise(prepareWriteMode(engine, writeMode));
  profile.engine = engine;
  profile.memoryAfterSetup = memorySnapshot();
}, 0);

afterAll(async () => {
  const memoryAfterSetup = profile.memoryAfterSetup ?? memoryBefore;
  let health: unknown = {
    status: "not-started",
  };
  if (profile.engine !== undefined) {
    health = await Effect.runPromise(profile.engine.health());
    await Effect.runPromise(profile.engine.close());
    profile.engine = undefined;
  }
  profile.memoryAfterSetup = undefined;
  const memoryAfterBenchmark = memorySnapshot();
  const cleanupLeakCount = cleanupLeakCountFromEngineHealth(health);
  writeBenchmarkArtifact({
    artifactKind: "engine-benchmark-summary",
    backpressureCount: backpressureCountFromEngineHealth(health),
    benchmarkCases,
    benchmarkName: `raw write engine benchmark (${writeMode})`,
    benchmarkScope: "engine-raw-write",
    cleanupLeakCount,
    health,
    latency: {
      outputJsonPath,
      source: "vitest-output-json",
    },
    memoryAfterBenchmark,
    memoryAfterSetup,
    memoryBefore,
    mutationCount: benchmarkMutationCount(),
    notes: [
      "Latency percentiles are emitted by Vitest in outputJsonPath.",
      writeMode === "indexed"
        ? "Indexed write mode pre-warms scalar predicate buckets and three ordered raw window indexes before timed writes. Single-row appends, replacements, patches, and deletes maintain affected ordered indexes incrementally; multi-row batch writes still measure scalar maintenance plus ordered-index invalidation/clear behavior."
        : "Base write mode measures decoded engine writes without pre-warmed read-path indexes.",
      `Seed row count: ${profile.rowCount}. Final tracked row count: ${profile.currentRowCount}.`,
    ],
    outputJsonPath,
    queuedEventCount: queuedEventCountFromEngineHealth(health),
    rowCount: profile.rowCount,
    subscriberCount: 0,
    topics: ["orders"],
  });
  failOnBenchmarkCleanupLeaks(cleanupLeakCount);
}, 0);

describe(`raw write engine benchmark: ${profile.rowCount} rows`, () => {
  const benchmarkDefinitions: ReadonlyArray<BenchmarkCase> = [
    {
      name: "publish append single row",
      run: async () => {
        const engine = profileEngine(profile);
        const index = profile.nextAppendIndex;
        profile.nextAppendIndex += 1;
        profile.currentRowCount += 1;
        await Effect.runPromise(
          engine.publish("orders", generatedOrder("single-append", index, index)),
        );
      },
    },
    {
      name: "patch existing rows one by one",
      run: async () => {
        const engine = profileEngine(profile);
        const generation = profile.nextPatchGeneration;
        profile.nextPatchGeneration += 1;
        for (let offset = 0; offset < batchSize; offset += 1) {
          await Effect.runPromise(
            engine.patch("orders", `order-${offset}`, {
              price: generation + offset,
              quantity: BigInt(generation + offset),
              decimalPrice: fromStringUnsafe(String(generation + offset)),
              updatedAt: 2_000_000_000 + generation + offset,
            }),
          );
        }
      },
    },
    ...(writeMode === "indexed"
      ? [
          {
            name: "replace single row then indexed snapshot",
            run: async () => {
              const engine = profileEngine(profile);
              const generation = profile.nextReadAfterReplaceGeneration;
              profile.nextReadAfterReplaceGeneration += 1;
              await Effect.runPromise(
                engine.publish("orders", generatedOrder("order", 0, generation)),
              );
              await Effect.runPromise(
                engine.snapshot("orders", {
                  select: ["id", "price", "updatedAt"],
                  where: {
                    price: { gte: 0 },
                  },
                  orderBy: [{ field: "price", direction: "asc" }],
                  limit: 50,
                }),
              );
            },
          },
          {
            name: "delete non-last row then indexed snapshot",
            run: async () => {
              const engine = profileEngine(profile);
              const generation = profile.nextReadAfterDeleteGeneration;
              profile.nextReadAfterDeleteGeneration += 1;
              const deletedRow = generatedOrder("delete-read", generation * 2, generation);
              const movedRow = generatedOrder("delete-read", generation * 2 + 1, generation);
              await Effect.runPromise(engine.publish("orders", deletedRow));
              await Effect.runPromise(engine.publish("orders", movedRow));
              await Effect.runPromise(engine.delete("orders", deletedRow.id));
              profile.currentRowCount += 1;
              await Effect.runPromise(
                engine.snapshot("orders", {
                  select: ["id", "price", "updatedAt"],
                  where: {
                    price: { gte: 0 },
                  },
                  orderBy: [{ field: "price", direction: "asc" }],
                  limit: 50,
                }),
              );
            },
          },
        ]
      : []),
    {
      name: "publishMany append batch",
      run: async () => {
        const engine = profileEngine(profile);
        const rows = writeBatch("append", profile.nextAppendIndex, profile.nextAppendIndex);
        profile.nextAppendIndex += batchSize;
        profile.currentRowCount += batchSize;
        await Effect.runPromise(engine.publishMany("orders", rows));
      },
    },
    {
      name: "publishMany replace existing batch",
      run: async () => {
        const engine = profileEngine(profile);
        const rows = writeBatch("order", 0, profile.nextReplaceGeneration);
        profile.nextReplaceGeneration += 1;
        await Effect.runPromise(engine.publishMany("orders", rows));
      },
    },
    {
      name: "append then delete batch",
      run: async () => {
        const engine = profileEngine(profile);
        const startIndex = profile.nextDeleteIndex;
        const rows = writeBatch("delete", startIndex, startIndex);
        profile.nextDeleteIndex += batchSize;
        await Effect.runPromise(engine.publishMany("orders", rows));
        for (const row of rows) {
          await Effect.runPromise(engine.delete("orders", row.id));
        }
      },
    },
  ];

  for (const benchmarkCase of benchmarkDefinitions) {
    bench(benchmarkCase.name, benchmarkCase.run, benchOptions);
  }
});
