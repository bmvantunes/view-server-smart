// Benchmarks intentionally import Vitest directly: @effect/vitest does not expose `bench`.
import { afterAll, beforeAll, bench, describe } from "vitest";
import { create, toBinary } from "@bufbuild/protobuf";
import type { Message } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import { defineViewServerConfig, grpc, type ViewServerHealth } from "@view-server/config";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { Clock, Config, Effect, Queue, Schedule, Schema, Stream } from "effect";
import {
  makeViewServerRuntimeCore,
  type ViewServerRuntimeCoreInstance,
} from "@view-server/runtime-core";
import { makeViewServerGrpcHealthLedger } from "./grpc-health";
import { makeViewServerGrpcIngress, type ViewServerGrpcIngress } from "./grpc-ingress";

declare const process: {
  readonly env: Record<string, string | undefined>;
  readonly memoryUsage: () => {
    readonly arrayBuffers: number;
    readonly external: number;
    readonly heapTotal: number;
    readonly heapUsed: number;
    readonly rss: number;
  };
};

type BenchmarkMemorySnapshot = {
  readonly arrayBuffersBytes: number;
  readonly externalBytes: number;
  readonly heapTotalBytes: number;
  readonly heapUsedBytes: number;
  readonly rssBytes: number;
};

type GrpcOrderValueMessage = Message<"viewserver.runtime.bench.OrderValue"> & {
  readonly customerId: string;
  readonly status: "open" | "closed" | "cancelled";
  readonly price: number;
  readonly updatedAt: number;
};

type GrpcOrderKeyMessage = Message<"viewserver.runtime.bench.OrderKey"> & {
  readonly orderId: string;
};

type BenchmarkProfile = {
  readonly health: ReturnType<typeof makeViewServerGrpcHealthLedger<Topics>>;
  readonly ingress: ViewServerGrpcIngress;
  readonly memoryAfterSetup: BenchmarkMemorySnapshot;
  readonly queue: Queue.Queue<GrpcOrderValueMessage>;
  readonly runtimeCore: ViewServerRuntimeCoreInstance<Topics>;
};

type BenchmarkCase = {
  readonly name: string;
  readonly run: () => Promise<void>;
};

type GrpcMaterializedSample = {
  readonly healthOverlayMs: number;
  readonly name: string;
  readonly rows: number;
  readonly rowsPerSecond: number;
  readonly snapshotMs: number;
  readonly streamConvergenceMs: number;
  readonly totalRows: number;
};

class GrpcMaterializedBenchmarkConvergenceError extends Schema.TaggedErrorClass<GrpcMaterializedBenchmarkConvergenceError>()(
  "GrpcMaterializedBenchmarkConvergenceError",
  {
    message: Schema.String,
  },
) {}

const GrpcOrder = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: GrpcOrder,
      key: "id",
      source: grpc.materialized(),
    },
  },
});

type Topics = typeof viewServer.topics;
type OrderRow = typeof GrpcOrder.Type;
type OrderStatus = OrderRow["status"];

const defaultBatchSize = 256;
const defaultBenchmarkTimeMs = 0;
const defaultIterations = 5;
const defaultSeedRows = 1_000;
const defaultWarmupIterations = 0;
const defaultWarmupTimeMs = 0;
const convergenceTimeout = "10 seconds";
const memoryBefore = memorySnapshot();

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

const batchSize = positiveIntegerFromEnv(
  "VIEW_SERVER_RUNTIME_BENCH_GRPC_BATCH_SIZE",
  defaultBatchSize,
);
const seedRows = positiveIntegerFromEnv(
  "VIEW_SERVER_RUNTIME_BENCH_GRPC_SEED_ROWS",
  defaultSeedRows,
);
const outputJsonPath = benchmarkOutputJsonPath(
  `grpc-materialized-${seedRows}seed-${batchSize}batch.json`,
);
const benchOptions = {
  iterations: positiveIntegerFromEnv("VIEW_SERVER_RUNTIME_BENCH_ITERATIONS", defaultIterations),
  time: nonNegativeIntegerFromEnv("VIEW_SERVER_RUNTIME_BENCH_TIME_MS", defaultBenchmarkTimeMs),
  warmupIterations: nonNegativeIntegerFromEnv(
    "VIEW_SERVER_RUNTIME_BENCH_WARMUP_ITERATIONS",
    defaultWarmupIterations,
  ),
  warmupTime: nonNegativeIntegerFromEnv(
    "VIEW_SERVER_RUNTIME_BENCH_WARMUP_TIME_MS",
    defaultWarmupTimeMs,
  ),
};
if (benchOptions.time > 0 || benchOptions.warmupIterations > 0 || benchOptions.warmupTime > 0) {
  throw new Error(
    "gRPC materialized benchmark mutates shared runtime state; time and warmup must stay disabled.",
  );
}

let profile: BenchmarkProfile | undefined;
const samples: Array<GrpcMaterializedSample> = [];
let nextRowIndex = 0;
let offeredRowCount = 0;

const base64FromBytes = (bytes: Uint8Array) =>
  globalThis.btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(""));

const runtimeGrpcProtoFile = fileDesc(
  base64FromBytes(
    toBinary(
      FileDescriptorProtoSchema,
      create(FileDescriptorProtoSchema, {
        name: "viewserver/runtime-bench.proto",
        package: "viewserver.runtime.bench",
        syntax: "proto3",
        messageType: [
          {
            name: "OrderValue",
            field: [
              { name: "customer_id", number: 1, type: FieldDescriptorProto_Type.STRING },
              { name: "status", number: 2, type: FieldDescriptorProto_Type.STRING },
              { name: "price", number: 3, type: FieldDescriptorProto_Type.DOUBLE },
              { name: "updated_at", number: 4, type: FieldDescriptorProto_Type.DOUBLE },
            ],
          },
          {
            name: "OrderKey",
            field: [{ name: "order_id", number: 1, type: FieldDescriptorProto_Type.STRING }],
          },
        ],
        service: [
          {
            name: "OrdersService",
            method: [
              {
                name: "StreamOrders",
                inputType: ".viewserver.runtime.bench.OrderKey",
                outputType: ".viewserver.runtime.bench.OrderValue",
                serverStreaming: true,
              },
            ],
          },
        ],
      }),
    ),
  ),
);

const grpcOrderValueSchema = messageDesc<GrpcOrderValueMessage>(runtimeGrpcProtoFile, 0);
const grpcOrderKeySchema = messageDesc<GrpcOrderKeyMessage>(runtimeGrpcProtoFile, 1);
const grpcOrdersService = serviceDesc<{
  readonly streamOrders: {
    readonly input: typeof grpcOrderKeySchema;
    readonly output: typeof grpcOrderValueSchema;
    readonly methodKind: "server_streaming";
  };
}>(runtimeGrpcProtoFile, 0);

const grpcClients = {
  orders: grpc.connectClient({
    service: grpcOrdersService,
    baseUrl: Config.succeed("https://orders.example.test"),
  }),
};

const grpcFeed = viewServer.grpcFeed<typeof grpcClients>();
const grpcHealthClientBaseUrls = {
  orders: "https://orders.example.test",
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

const grpcOrderValue = (index: number): GrpcOrderValueMessage => ({
  $typeName: "viewserver.runtime.bench.OrderValue",
  customerId: `order-${index}`,
  status: orderStatus(index),
  price: index % 10_000,
  updatedAt: index,
});

const grpcMaterializedFeed = (stream: Stream.Stream<GrpcOrderValueMessage, never, never>) =>
  grpcFeed.materializedFeed({
    topic: "orders",
    client: "orders",
    method: "streamOrders",
    request: () => ({ orderId: "all" }),
    acquire: () => stream,
    map: ({ value }) => ({
      id: value.customerId,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region: "usa",
      updatedAt: value.updatedAt,
    }),
  });

const createRuntimeCoreForBenchmark = Effect.fn("ViewServerRuntime.grpc.bench.core.make")(
  function* () {
    return yield* makeViewServerRuntimeCore(viewServer, {});
  },
);

function memorySnapshot(): BenchmarkMemorySnapshot {
  const memory = process.memoryUsage();
  return {
    arrayBuffersBytes: memory.arrayBuffers,
    externalBytes: memory.external,
    heapTotalBytes: memory.heapTotal,
    heapUsedBytes: memory.heapUsed,
    rssBytes: memory.rss,
  };
}

function memoryDelta(
  before: BenchmarkMemorySnapshot,
  after: BenchmarkMemorySnapshot,
): BenchmarkMemorySnapshot {
  return {
    arrayBuffersBytes: after.arrayBuffersBytes - before.arrayBuffersBytes,
    externalBytes: after.externalBytes - before.externalBytes,
    heapTotalBytes: after.heapTotalBytes - before.heapTotalBytes,
    heapUsedBytes: after.heapUsedBytes - before.heapUsedBytes,
    rssBytes: after.rssBytes - before.rssBytes,
  };
}

function benchmarkOutputJsonPath(fallbackName: string): string {
  const configured = process.env["VIEW_SERVER_RUNTIME_BENCH_OUTPUT_JSON"];
  if (configured !== undefined && configured.trim() !== "") {
    return configured.trim();
  }
  return join(".artifacts", fallbackName);
}

function benchmarkSummaryPath(path: string): string {
  if (path.endsWith(".json")) {
    return `${path.slice(0, -".json".length)}.summary.json`;
  }
  return `${path}.summary.json`;
}

function writeJsonFile(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(value, undefined, 2)}\n`);
}

const nextRows = (count: number): ReadonlyArray<GrpcOrderValueMessage> => {
  const start = nextRowIndex;
  nextRowIndex += count;
  return Array.from({ length: count }, (_value, offset) => grpcOrderValue(start + offset));
};

const topicHealthValues = (health: ViewServerHealth<Topics>) => [health.engine.topics.orders];

const cleanupLeakCountFromHealth = (health: ViewServerHealth<Topics>): number => {
  let leakCount = 0;
  for (const topicHealth of topicHealthValues(health)) {
    leakCount +=
      topicHealth.activeSubscriptions + topicHealth.activeViews + topicHealth.queuedEvents;
  }
  return leakCount;
};

const queuedEventCountFromHealth = (health: ViewServerHealth<Topics>): number => {
  let queuedEventCount = 0;
  for (const topicHealth of topicHealthValues(health)) {
    queuedEventCount += topicHealth.queuedEvents;
  }
  return queuedEventCount;
};

const backpressureCountFromHealth = (health: ViewServerHealth<Topics>): number => {
  let backpressureCount = 0;
  for (const topicHealth of topicHealthValues(health)) {
    backpressureCount += topicHealth.backpressureEvents;
  }
  return backpressureCount;
};

const offerRows = Effect.fn("ViewServerRuntime.grpc.bench.rows.offer")(function* (
  queue: Queue.Queue<GrpcOrderValueMessage>,
  rows: ReadonlyArray<GrpcOrderValueMessage>,
) {
  yield* Effect.forEach(rows, (row) => Queue.offer(queue, row), {
    discard: true,
  });
  yield* Effect.sync(() => {
    offeredRowCount += rows.length;
  });
});

const waitForTotalRows = Effect.fn("ViewServerRuntime.grpc.bench.totalRows.wait")(function* (
  runtimeCore: ViewServerRuntimeCoreInstance<Topics>,
  expectedTotalRows: number,
) {
  return yield* runtimeCore.client
    .snapshot("orders", {
      select: ["id", "price"],
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 50,
    })
    .pipe(
      Effect.repeat({
        schedule: Schedule.addDelay(Schedule.recurs(200), () => Effect.succeed("1 millis")),
        until: (snapshot) => snapshot.totalRows === expectedTotalRows,
      }),
      Effect.timeout(convergenceTimeout),
      Effect.flatMap((snapshot) =>
        snapshot === undefined
          ? Effect.fail(
              new GrpcMaterializedBenchmarkConvergenceError({
                message: `gRPC materialized benchmark did not converge to ${expectedTotalRows} rows within ${convergenceTimeout}.`,
              }),
            )
          : Effect.succeed(snapshot),
      ),
    );
});

const readHealthOverlay = Effect.fn("ViewServerRuntime.grpc.bench.healthOverlay.read")(function* (
  runtimeCore: ViewServerRuntimeCoreInstance<Topics>,
  health: ReturnType<typeof makeViewServerGrpcHealthLedger<Topics>>,
) {
  const nowMillis = yield* Clock.currentTimeMillis;
  return health.healthOverlay(yield* runtimeCore.client.health(), nowMillis);
});

const runGrpcBatch = Effect.fn("ViewServerRuntime.grpc.bench.batch.run")(function* (
  name: string,
  currentProfile: BenchmarkProfile,
  rows: ReadonlyArray<GrpcOrderValueMessage>,
) {
  const before = performance.now();
  yield* offerRows(currentProfile.queue, rows);
  const snapshot = yield* waitForTotalRows(currentProfile.runtimeCore, nextRowIndex);
  const afterConvergence = performance.now();
  const healthBefore = performance.now();
  const health = yield* readHealthOverlay(currentProfile.runtimeCore, currentProfile.health);
  const healthAfter = performance.now();
  const readBefore = performance.now();
  yield* currentProfile.runtimeCore.client.snapshot("orders", {
    select: ["id", "price", "status"],
    where: {
      status: { eq: "open" },
      price: { gte: 10 },
    },
    orderBy: [{ field: "updatedAt", direction: "desc" }],
    limit: 100,
  });
  const readAfter = performance.now();
  const streamConvergenceMs = afterConvergence - before;
  samples.push({
    healthOverlayMs: healthAfter - healthBefore,
    name,
    rows: rows.length,
    rowsPerSecond: (rows.length / streamConvergenceMs) * 1_000,
    snapshotMs: readAfter - readBefore,
    streamConvergenceMs,
    totalRows: snapshot.totalRows,
  });
  if (health.status === "degraded") {
    throw new Error("gRPC materialized benchmark health became degraded.");
  }
});

const runHealthOverlayCase = Effect.fn("ViewServerRuntime.grpc.bench.healthOverlay.run")(function* (
  name: string,
  currentProfile: BenchmarkProfile,
) {
  const before = performance.now();
  const health = yield* readHealthOverlay(currentProfile.runtimeCore, currentProfile.health);
  const after = performance.now();
  samples.push({
    healthOverlayMs: after - before,
    name,
    rows: 0,
    rowsPerSecond: 0,
    snapshotMs: 0,
    streamConvergenceMs: 0,
    totalRows: health.engine.topics.orders.rowCount,
  });
  if (health.status === "degraded") {
    throw new Error("gRPC materialized benchmark health became degraded.");
  }
});

const currentBenchmarkProfile = (): BenchmarkProfile => {
  const currentProfile = profile;
  if (currentProfile === undefined) {
    throw new Error("gRPC materialized benchmark setup did not create a profile.");
  }
  return currentProfile;
};

const benchmarkCases: ReadonlyArray<BenchmarkCase> = [
  {
    name: "gRPC materialized stream batch",
    run: () =>
      Effect.runPromise(
        runGrpcBatch(
          "gRPC materialized stream batch",
          currentBenchmarkProfile(),
          nextRows(batchSize),
        ),
      ),
  },
  {
    name: "gRPC materialized burst",
    run: () =>
      Effect.runPromise(
        runGrpcBatch("gRPC materialized burst", currentBenchmarkProfile(), nextRows(batchSize * 4)),
      ),
  },
  {
    name: "gRPC materialized health overlay",
    run: () =>
      Effect.runPromise(
        runHealthOverlayCase("gRPC materialized health overlay", currentBenchmarkProfile()),
      ),
  },
];

const summarizeSamples = (
  name: string,
): {
  readonly maxHealthOverlayMs: number;
  readonly maxSnapshotMs: number;
  readonly maxStreamConvergenceMs: number;
  readonly meanHealthOverlayMs: number;
  readonly meanRowsPerSecond: number;
  readonly meanSnapshotMs: number;
  readonly meanStreamConvergenceMs: number;
  readonly name: string;
  readonly sampleCount: number;
  readonly totalRows: number;
} => {
  const matching = samples.filter((sample) => sample.name === name);
  const totals = matching.reduce(
    (accumulator, sample) => ({
      healthOverlayMs: accumulator.healthOverlayMs + sample.healthOverlayMs,
      rowsPerSecond: accumulator.rowsPerSecond + sample.rowsPerSecond,
      snapshotMs: accumulator.snapshotMs + sample.snapshotMs,
      streamConvergenceMs: accumulator.streamConvergenceMs + sample.streamConvergenceMs,
    }),
    {
      healthOverlayMs: 0,
      rowsPerSecond: 0,
      snapshotMs: 0,
      streamConvergenceMs: 0,
    },
  );
  const sampleCount = matching.length;
  return {
    maxHealthOverlayMs: Math.max(...matching.map((sample) => sample.healthOverlayMs)),
    maxSnapshotMs: Math.max(...matching.map((sample) => sample.snapshotMs)),
    maxStreamConvergenceMs: Math.max(...matching.map((sample) => sample.streamConvergenceMs)),
    meanHealthOverlayMs: totals.healthOverlayMs / sampleCount,
    meanRowsPerSecond: totals.rowsPerSecond / sampleCount,
    meanSnapshotMs: totals.snapshotMs / sampleCount,
    meanStreamConvergenceMs: totals.streamConvergenceMs / sampleCount,
    name,
    sampleCount,
    totalRows: matching[matching.length - 1]?.totalRows ?? 0,
  };
};

beforeAll(async () => {
  profile = await Effect.runPromise(
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<GrpcOrderValueMessage>();
      const runtimeCore = yield* createRuntimeCoreForBenchmark();
      const health = makeViewServerGrpcHealthLedger<Topics>({
        clients: grpcHealthClientBaseUrls,
        feeds: {
          ordersFeed: {
            client: "orders",
            lifecycle: "materialized",
            topic: "orders",
          },
        },
      });
      const ingress = yield* makeViewServerGrpcIngress(
        viewServer,
        runtimeCore.client,
        Effect.void,
        {
          clientBaseUrls: grpcHealthClientBaseUrls,
          clients: grpcClients,
          feeds: {
            ordersFeed: grpcMaterializedFeed(Stream.fromQueue(queue)),
          },
        },
        health,
      );
      yield* runGrpcBatch(
        "setup seed",
        {
          health,
          ingress,
          memoryAfterSetup: memorySnapshot(),
          queue,
          runtimeCore,
        },
        nextRows(seedRows),
      );
      samples.length = 0;
      return {
        health,
        ingress,
        memoryAfterSetup: memorySnapshot(),
        queue,
        runtimeCore,
      };
    }),
  );
});

afterAll(async () => {
  const currentProfile = profile;
  let health: ViewServerHealth<Topics> | undefined;
  if (currentProfile !== undefined) {
    await Effect.runPromise(currentProfile.ingress.close);
    health = await Effect.runPromise(
      readHealthOverlay(currentProfile.runtimeCore, currentProfile.health),
    );
    await Effect.runPromise(currentProfile.runtimeCore.close);
  }
  const memoryAfterBenchmark = memorySnapshot();
  const cleanupLeakCount = health === undefined ? 0 : cleanupLeakCountFromHealth(health);
  const backpressureCount = health === undefined ? 0 : backpressureCountFromHealth(health);
  const queuedEventCount = health === undefined ? 0 : queuedEventCountFromHealth(health);
  writeJsonFile(benchmarkSummaryPath(outputJsonPath), {
    artifactKind: "runtime-benchmark-summary",
    backpressureCount,
    batchSize,
    benchmarkCases: benchmarkCases.map((benchmarkCase) => benchmarkCase.name),
    benchmarkName: "gRPC materialized runtime benchmark",
    benchmarkScope: "runtime-grpc-materialized",
    cases: benchmarkCases.map((benchmarkCase) => summarizeSamples(benchmarkCase.name)),
    cleanupLeakCount,
    health,
    latency: {
      outputJsonPath,
      source: "vitest-output-json",
    },
    memory: {
      afterBenchmark: memoryAfterBenchmark,
      afterSetup: currentProfile?.memoryAfterSetup,
      before: memoryBefore,
      setupDelta:
        currentProfile?.memoryAfterSetup === undefined
          ? undefined
          : memoryDelta(memoryBefore, currentProfile.memoryAfterSetup),
      totalDelta: memoryDelta(memoryBefore, memoryAfterBenchmark),
    },
    grpcParameters: {
      batchSize,
      seedRows,
    },
    mutationCount: offeredRowCount,
    notes: [
      "Latency percentiles are emitted by Vitest in outputJsonPath.",
      "Benchmark uses the production materialized gRPC ingress with an in-memory Stream source.",
      "Each timed sample writes through the gRPC ingress queue, waits for runtime-core convergence, then performs a filtered/sorted snapshot.",
    ],
    queuedEventCount,
    rowCount: seedRows,
    seedRows,
    subscriberCount: 0,
    topics: ["orders"],
  });
  if (cleanupLeakCount > 0) {
    throw new Error(
      `gRPC materialized benchmark cleanup leaked ${cleanupLeakCount} active resource(s).`,
    );
  }
});

describe("runtime gRPC materialized benchmark", () => {
  for (const benchmarkCase of benchmarkCases) {
    bench(benchmarkCase.name, benchmarkCase.run, benchOptions);
  }
});
