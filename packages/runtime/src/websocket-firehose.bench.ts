// Benchmarks intentionally import Vitest directly: @effect/vitest does not expose `bench`.
import { afterAll, beforeAll, bench, describe, expect } from "vitest";
import { makeViewServerClient, type ViewServerRemoteClient } from "@view-server/client/remote";
import type { ViewServerLiveEvent, ViewServerLiveSubscription } from "@view-server/client";
import { defineViewServerConfig, type ViewServerHealth } from "@view-server/config";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { Cause, Effect, Exit, Schema, Scope, Stream } from "effect";
import { makeViewServerRuntime, type ViewServerRuntime } from "./index";

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

type WebSocketCaseName = "same-window" | "ten-window";

type BenchmarkProfile = {
  client: ViewServerRemoteClient<Topics> | undefined;
  memoryAfterSetup: BenchmarkMemorySnapshot | undefined;
  nextDeltaIndex: number;
  nextDeltaVersion: number;
  readers: ReadonlyArray<OrderEventReader>;
  runtime: ViewServerRuntime<Topics> | undefined;
  scope: Scope.Closeable | undefined;
  subscriptions: ReadonlyArray<OrderSubscription>;
  timedMutationCount: number;
};

type OrderRow = typeof Order.Type;
type OrderStatus = OrderRow["status"];
type OrderSubscription = ViewServerLiveSubscription<OrderRow>;
type OrderEvent = ViewServerLiveEvent<OrderRow, string, string>;
type OrderEventReader = () => Effect.Effect<ReadonlyArray<OrderEvent>, Cause.Done>;

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

const defaultCaseName: WebSocketCaseName = "same-window";
const defaultIterations = 5;
const defaultRowCount = 1_000;
const defaultSubscriberCount = 10;
const defaultWarmupIterations = 0;
const defaultWarmupTimeMs = 0;
const memoryBefore = memorySnapshot();
const maxReaderChunks = 20;
const readerTimeout = "1 second";
const setupTimeout = "10 seconds";

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
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer.`);
  }
  return parsed;
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
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer.`);
  }
  return parsed;
};

const caseNameFromEnv = (): WebSocketCaseName => {
  const raw = process.env["VIEW_SERVER_RUNTIME_BENCH_WEBSOCKET_CASE"];
  if (raw === undefined || raw.trim() === "") {
    return defaultCaseName;
  }
  const trimmed = raw.trim();
  if (trimmed === "same-window" || trimmed === "ten-window") {
    return trimmed;
  }
  throw new Error("VIEW_SERVER_RUNTIME_BENCH_WEBSOCKET_CASE must be same-window or ten-window.");
};

const caseName = caseNameFromEnv();
const rowCount = positiveIntegerFromEnv(
  "VIEW_SERVER_RUNTIME_BENCH_WEBSOCKET_ROWS",
  defaultRowCount,
);
const subscriberCount = positiveIntegerFromEnv(
  "VIEW_SERVER_RUNTIME_BENCH_WEBSOCKET_SUBSCRIBERS",
  defaultSubscriberCount,
);
const outputJsonPath = benchmarkOutputJsonPath(
  `websocket-firehose-${caseName}-${rowCount}rows-${subscriberCount}subs.json`,
);
const benchOptions = {
  iterations: positiveIntegerFromEnv("VIEW_SERVER_RUNTIME_BENCH_ITERATIONS", defaultIterations),
  time: nonNegativeIntegerFromEnv("VIEW_SERVER_RUNTIME_BENCH_TIME_MS", 1),
  warmupIterations: nonNegativeIntegerFromEnv(
    "VIEW_SERVER_RUNTIME_BENCH_WARMUP_ITERATIONS",
    defaultWarmupIterations,
  ),
  warmupTime: nonNegativeIntegerFromEnv(
    "VIEW_SERVER_RUNTIME_BENCH_WARMUP_TIME_MS",
    defaultWarmupTimeMs,
  ),
};

const profile: BenchmarkProfile = {
  client: undefined,
  memoryAfterSetup: undefined,
  nextDeltaIndex: rowCount,
  nextDeltaVersion: 1,
  readers: [],
  runtime: undefined,
  scope: undefined,
  subscriptions: [],
  timedMutationCount: 0,
};

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
  customerId: `customer-${index % 10_000}`,
  status: orderStatus(index),
  price: index % 1_000_000,
  region: region(index),
  updatedAt: index,
});

const deltaOrder = (index: number): OrderRow => ({
  id: `delta-${index}`,
  customerId: `customer-delta-${index % 10_000}`,
  status: "open",
  price: 1_000_000 + (index % 10_000),
  region: "emea",
  updatedAt: 1_000_000_000 + index,
});

const windowOffset = (index: number): number => {
  if (caseName === "same-window") {
    return 0;
  }
  return index % 10;
};

const seedRows = Array.from({ length: rowCount }, (_value, index) => seedOrder(index));
const matchingSeedRows = seedRows
  .filter((row) => row.status === "open" && row.price >= 0)
  .toSorted((left, right) => right.updatedAt - left.updatedAt || left.id.localeCompare(right.id));

const expectedSnapshotRows = (subscriberIndex: number): ReadonlyArray<OrderRow> =>
  matchingSeedRows.slice(windowOffset(subscriberIndex), windowOffset(subscriberIndex) + 50);

const expectedSnapshotKeys = (subscriberIndex: number): ReadonlyArray<string> =>
  expectedSnapshotRows(subscriberIndex).map((row) => row.id);

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

const eventIsDeltaVersion = (event: OrderEvent, expectedDeltaVersion: number): boolean =>
  event.type === "delta" && event.toVersion === expectedDeltaVersion;

const deltaRowsForKey = (event: OrderEvent, rowId: string): ReadonlyArray<OrderRow> => {
  if (event.type !== "delta") {
    return [];
  }
  return event.operations.flatMap((operation) => {
    if (operation.type === "insert" || operation.type === "update") {
      if (operation.key === rowId) {
        return [operation.row];
      }
    }
    return [];
  });
};

const expectInsertedRowVisibility = (
  eventChunks: ReadonlyArray<ReadonlyArray<OrderEvent>>,
  row: OrderRow,
) => {
  expect(
    eventChunks.map((events, index) => ({
      insertedRows: events.flatMap((event) => deltaRowsForKey(event, row.id)),
      subscriber: index,
    })),
  ).toStrictEqual(
    Array.from({ length: subscriberCount }, (_value, index) => ({
      insertedRows: caseName === "same-window" || index % 10 === 0 ? [row] : [],
      subscriber: index,
    })),
  );
};

const eventIsSnapshot = (event: OrderEvent): boolean => event.type === "snapshot";

const readBoundedChunk = (reader: OrderEventReader) => reader().pipe(Effect.timeout(readerTimeout));

const readUntilSnapshot = Effect.fn("ViewServerRuntime.websocketFirehose.bench.readSnapshot")(
  function* (reader: OrderEventReader, subscriberIndex: number) {
    let attempts = 0;
    let events: ReadonlyArray<OrderEvent> = [];
    while (!events.some(eventIsSnapshot) && attempts < maxReaderChunks) {
      attempts += 1;
      events = [...events, ...(yield* readBoundedChunk(reader))];
    }
    if (!events.some(eventIsSnapshot)) {
      throw new Error(
        `WebSocket firehose benchmark reader did not receive a snapshot within ${maxReaderChunks} chunk(s).`,
      );
    }
    expect(events.map((event) => event.type)).toStrictEqual(["snapshot"]);
    expect(
      events.map((event) => {
        if (event.type === "snapshot") {
          return {
            keys: event.keys,
            rows: event.rows,
            topic: event.topic,
            totalRows: event.totalRows,
            version: event.version,
          };
        }
        return event;
      }),
    ).toStrictEqual([
      {
        keys: expectedSnapshotKeys(subscriberIndex),
        rows: expectedSnapshotRows(subscriberIndex),
        topic: "orders",
        totalRows: matchingSeedRows.length,
        version: 1,
      },
    ]);
  },
);

const readUntilDeltaVersion = Effect.fn("ViewServerRuntime.websocketFirehose.bench.readDelta")(
  function* (reader: OrderEventReader, expectedDeltaVersion: number) {
    let attempts = 0;
    let events: ReadonlyArray<OrderEvent> = [];
    while (
      !events.some((event) => eventIsDeltaVersion(event, expectedDeltaVersion)) &&
      attempts < maxReaderChunks
    ) {
      attempts += 1;
      events = [...events, ...(yield* readBoundedChunk(reader))];
    }
    if (!events.some((event) => eventIsDeltaVersion(event, expectedDeltaVersion))) {
      throw new Error(
        `WebSocket firehose benchmark reader did not receive delta version ${expectedDeltaVersion} within ${maxReaderChunks} chunk(s).`,
      );
    }
    return events;
  },
);

const expectTimedDeltaEvents = (
  eventChunks: ReadonlyArray<ReadonlyArray<OrderEvent>>,
  expectedFromVersion: number,
  expectedDeltaVersion: number,
  expectedTotalRows: number,
) => {
  expect(eventChunks.map((events) => events.map((event) => event.type))).toStrictEqual(
    Array.from({ length: profile.readers.length }, () => ["delta"]),
  );
  expect(
    eventChunks.map((events) =>
      events.map((event) => {
        if (event.type === "delta") {
          return {
            fromVersion: event.fromVersion,
            hasOperations: event.operations.length > 0,
            toVersion: event.toVersion,
            topic: event.topic,
            totalRows: event.totalRows,
          };
        }
        return {
          fromVersion: -1,
          hasOperations: false,
          toVersion: -1,
          topic: event.topic,
          totalRows: -1,
        };
      }),
    ),
  ).toStrictEqual(
    Array.from({ length: profile.readers.length }, () => [
      {
        fromVersion: expectedFromVersion,
        hasOperations: true,
        toVersion: expectedDeltaVersion,
        topic: "orders",
        totalRows: expectedTotalRows,
      },
    ]),
  );
};

const setupBenchmark = Effect.fn("ViewServerRuntime.websocketFirehose.bench.setup")(function* () {
  const runtime = yield* makeViewServerRuntime(viewServer, {
    host: "127.0.0.1",
    subscriptionQueueCapacity: 1024,
    websocketPort: 0,
  });
  profile.runtime = runtime;
  yield* runtime.client.publishMany("orders", seedRows);
  const client = yield* makeViewServerClient(viewServer, {
    subscriptionBufferSize: 1024,
    url: runtime.url,
  });
  profile.client = client;
  const subscriptions = yield* Effect.forEach(
    Array.from({ length: subscriberCount }, (_value, index) => index),
    (index) =>
      client.subscribe("orders", {
        select: ["id", "customerId", "status", "price", "region", "updatedAt"],
        where: {
          price: { gte: 0 },
          status: { eq: "open" },
        },
        orderBy: [{ field: "updatedAt", direction: "desc" }],
        offset: windowOffset(index),
        limit: 50,
      }),
    { concurrency: "unbounded" },
  );
  profile.subscriptions = subscriptions;
  const scope = yield* Scope.make("parallel");
  profile.scope = scope;
  const readers = yield* Effect.forEach(subscriptions, (subscription) =>
    makeEventReader(subscription, scope),
  );
  profile.readers = readers;
  yield* Effect.forEach(readers, (reader, index) => readUntilSnapshot(reader, index), {
    concurrency: "unbounded",
  });
  profile.memoryAfterSetup = memorySnapshot();
});

const benchmarkRuntime = Effect.fn("ViewServerRuntime.websocketFirehose.bench.publishAndRead")(
  function* () {
    const runtime = profile.runtime;
    if (runtime === undefined) {
      throw new Error("WebSocket firehose benchmark runtime is not initialized.");
    }
    const row = deltaOrder(profile.nextDeltaIndex);
    profile.nextDeltaIndex += 1;
    profile.nextDeltaVersion += 1;
    const expectedDeltaVersion = profile.nextDeltaVersion;
    const expectedFromVersion = expectedDeltaVersion - 1;
    const expectedTotalRows = matchingSeedRows.length + profile.timedMutationCount + 1;
    yield* runtime.client.publish("orders", row);
    const eventChunks = yield* Effect.forEach(
      profile.readers,
      (reader) => readUntilDeltaVersion(reader, expectedDeltaVersion),
      {
        concurrency: "unbounded",
      },
    );
    expect(eventChunks).toHaveLength(profile.readers.length);
    expectTimedDeltaEvents(
      eventChunks,
      expectedFromVersion,
      expectedDeltaVersion,
      expectedTotalRows,
    );
    expectInsertedRowVisibility(eventChunks, row);
    profile.timedMutationCount += 1;
  },
);

const cleanupLeakCountFromHealth = (health: ViewServerHealth<Topics>): number => {
  let topicResources = 0;
  for (const topicHealth of Object.values(health.engine.topics)) {
    topicResources +=
      topicHealth.activeSubscriptions + topicHealth.activeViews + topicHealth.queuedEvents;
  }
  return topicResources + health.transport.activeStreams + health.transport.activeClients;
};

const queuedEventCountFromHealth = (health: ViewServerHealth<Topics>): number => {
  let queuedEventCount = 0;
  for (const topicHealth of Object.values(health.engine.topics)) {
    queuedEventCount += topicHealth.queuedEvents;
  }
  return queuedEventCount + health.transport.queuedMessages;
};

const backpressureCountFromHealth = (health: ViewServerHealth<Topics>): number => {
  let backpressureCount = health.transport.backpressureEvents;
  for (const topicHealth of Object.values(health.engine.topics)) {
    backpressureCount += topicHealth.backpressureEvents;
  }
  return backpressureCount;
};

const waitForCleanupHealth: (
  runtime: ViewServerRuntime<Topics>,
) => Effect.Effect<ViewServerHealth<Topics>, unknown> = Effect.fn(
  "ViewServerRuntime.websocketFirehose.bench.cleanupHealth",
)(function* (runtime: ViewServerRuntime<Topics>) {
  let attempts = 0;
  let health = yield* runtime.health();
  while (cleanupLeakCountFromHealth(health) > 0 && attempts < 50) {
    attempts += 1;
    yield* Effect.sleep("5 millis");
    health = yield* runtime.health();
  }
  return health;
});

beforeAll(async () => {
  await Effect.runPromise(setupBenchmark().pipe(Effect.timeout(setupTimeout)));
}, 15_000);

afterAll(async () => {
  const closeSubscriptionsExit = await Effect.runPromiseExit(
    Effect.forEach(profile.subscriptions, (subscription) => subscription.close(), {
      concurrency: "unbounded",
    }),
  );
  const closeScopeExit =
    profile.scope === undefined
      ? Exit.void
      : await Effect.runPromiseExit(Scope.close(profile.scope, Exit.void));
  const closeClientExit =
    profile.client === undefined ? Exit.void : await Effect.runPromiseExit(profile.client.close);
  let finalHealth: ViewServerHealth<Topics> | undefined;
  let finalHealthFailure: string | undefined;
  if (profile.runtime === undefined) {
    finalHealthFailure = "WebSocket firehose benchmark runtime was not initialized.";
  } else {
    const finalHealthExit = await Effect.runPromiseExit(waitForCleanupHealth(profile.runtime));
    if (Exit.isSuccess(finalHealthExit)) {
      finalHealth = finalHealthExit.value;
    } else {
      finalHealthFailure = Cause.pretty(finalHealthExit.cause);
    }
  }
  const closeRuntimeExit =
    profile.runtime === undefined ? Exit.void : await Effect.runPromiseExit(profile.runtime.close);
  const memoryAfterSetup = profile.memoryAfterSetup ?? memoryBefore;
  const memoryAfterBenchmark = memorySnapshot();
  const cleanupLeakCount = finalHealth === undefined ? 0 : cleanupLeakCountFromHealth(finalHealth);
  const backpressureCount =
    finalHealth === undefined ? 0 : backpressureCountFromHealth(finalHealth);
  const queuedEventCount = finalHealth === undefined ? 0 : queuedEventCountFromHealth(finalHealth);
  writeJsonFile(benchmarkSummaryPath(outputJsonPath), {
    artifactKind: "runtime-benchmark-summary",
    backpressureCount,
    benchmarkCases: [`${caseName} remote websocket publish + fanout`],
    benchmarkName: "WebSocket firehose runtime benchmark",
    benchmarkScope: "runtime-websocket-firehose",
    cleanupLeakCount,
    health: finalHealth ?? null,
    latency: {
      outputJsonPath,
      source: "vitest-output-json",
    },
    memory: {
      afterBenchmark: memoryAfterBenchmark,
      afterSetup: memoryAfterSetup,
      before: memoryBefore,
      setupDelta: memoryDelta(memoryBefore, memoryAfterSetup),
      totalDelta: memoryDelta(memoryBefore, memoryAfterBenchmark),
    },
    mutationCount: profile.timedMutationCount,
    notes: [
      "Latency percentiles are emitted by Vitest in outputJsonPath.",
      "Benchmark uses the production Effect RPC WebSocket transport with NDJSON serialization.",
      "Runtime mutation client publishes rows; remote client subscriptions consume deltas through WebSocket.",
      "rowCount is the seeded table size; mutationCount is the number of measured publish+fanout iterations.",
    ],
    queuedEventCount,
    rowCount,
    seedRowCount: rowCount,
    subscriberCount,
    topics: ["orders"],
  });
  if (Exit.isFailure(closeSubscriptionsExit)) {
    throw new Error(
      `WebSocket firehose benchmark subscription cleanup failed: ${Cause.pretty(closeSubscriptionsExit.cause)}`,
    );
  }
  if (Exit.isFailure(closeScopeExit)) {
    throw new Error(
      `WebSocket firehose benchmark scope cleanup failed: ${Cause.pretty(closeScopeExit.cause)}`,
    );
  }
  if (Exit.isFailure(closeClientExit)) {
    throw new Error(
      `WebSocket firehose benchmark client cleanup failed: ${Cause.pretty(closeClientExit.cause)}`,
    );
  }
  if (finalHealthFailure !== undefined) {
    throw new Error(`WebSocket firehose benchmark final health read failed: ${finalHealthFailure}`);
  }
  if (Exit.isFailure(closeRuntimeExit)) {
    throw new Error(
      `WebSocket firehose benchmark runtime cleanup failed: ${Cause.pretty(closeRuntimeExit.cause)}`,
    );
  }
  if (cleanupLeakCount > 0) {
    throw new Error(
      `WebSocket firehose benchmark cleanup leaked ${cleanupLeakCount} active resource(s).`,
    );
  }
}, 0);

describe(`WebSocket firehose runtime benchmark: ${caseName}, ${rowCount} rows, ${subscriberCount} subscribers`, () => {
  bench(
    `${caseName} remote websocket publish + fanout`,
    async () => {
      await Effect.runPromise(benchmarkRuntime());
    },
    benchOptions,
  );
});
