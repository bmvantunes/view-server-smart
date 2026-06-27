// Benchmarks intentionally import Vitest directly: @effect/vitest does not expose `bench`.
import { afterAll, beforeAll, bench, describe, expect } from "vitest";
import { create, toBinary } from "@bufbuild/protobuf";
import type { Message } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { defineViewServerConfig, grpc, type ViewServerHealth } from "@view-server/config";
import type { ViewServerLiveSubscription } from "@view-server/client";
import { ignoreLoggedTypedFailuresPreserveNonTypedFailures } from "@view-server/effect-utils";
import {
  makeViewServerRuntimeCoreInternal,
  type ViewServerRuntimeCoreInternalInstance,
} from "@view-server/runtime-core/internal";
import { Clock, Config, Effect, Exit, Fiber, Queue, Schema, Scope, Stream } from "effect";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { makeViewServerGrpcHealthLedger } from "./grpc-health";
import {
  makeViewServerGrpcLeaseManager,
  type ViewServerGrpcLeaseManager,
} from "./grpc-lease-manager";

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

type GrpcOrderValueMessage = Message<"viewserver.runtime.bench.OrderValue"> & {
  readonly customerId: string;
  readonly status: "open" | "closed" | "cancelled";
  readonly price: number;
  readonly region: string;
  readonly updatedAt: number;
};

type GrpcOrderKeyMessage = Message<"viewserver.runtime.bench.OrderKey"> & {
  readonly region: string;
};

type BenchmarkMemorySnapshot = {
  readonly arrayBuffersBytes: number;
  readonly externalBytes: number;
  readonly heapTotalBytes: number;
  readonly heapUsedBytes: number;
  readonly rssBytes: number;
};

type ProjectedOrderRow = {
  readonly id: string;
  readonly price: number;
  readonly region: string;
  readonly status: GrpcOrderValueMessage["status"];
};

type BenchmarkProfile = {
  readonly health: ReturnType<typeof makeViewServerGrpcHealthLedger<Topics>>;
  readonly manager: ViewServerGrpcLeaseManager<Topics>;
  readonly memoryAfterSetup: BenchmarkMemorySnapshot;
  readonly queues: ReadonlyMap<string, Queue.Queue<GrpcOrderValueMessage>>;
  readonly runtimeCore: ViewServerRuntimeCoreInternalInstance<Topics>;
  readonly scope: Scope.Closeable;
};

type BenchmarkCase = {
  readonly name: string;
  readonly run: () => Promise<void>;
};

type WatchedSubscription = {
  readonly fiber: Fiber.Fiber<void, never>;
  readonly subscription: ViewServerLiveSubscription<ProjectedOrderRow>;
  readonly totalRowsQueue: Queue.Queue<number>;
};

type GrpcLeasedSample = {
  readonly activeLeasedFeeds: number;
  readonly cleanupMs: number;
  readonly cleanupActiveLeasedFeeds: number;
  readonly deltaFanoutMs: number;
  readonly healthOverlayMs: number;
  readonly name: string;
  readonly rows: number;
  readonly rowsPerSecond: number;
  readonly snapshotMs: number;
  readonly subscriptionMs: number;
};

class GrpcLeasedBenchmarkConvergenceError extends Schema.TaggedErrorClass<GrpcLeasedBenchmarkConvergenceError>()(
  "GrpcLeasedBenchmarkConvergenceError",
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
      source: grpc.leased({
        routeBy: ["region"],
      }),
    },
  },
});

type Topics = typeof viewServer.topics;
type OrderRow = typeof GrpcOrder.Type;
type OrderStatus = OrderRow["status"];

const defaultIterations = 5;
const defaultRetainedRows = 50_000;
const defaultRowsPerFeed = 50;
const defaultRouteCount = 25;
const convergenceTimeout = "10 seconds";
const memoryBefore = memorySnapshot();
const ignoreGrpcLeasedBenchmarkSubscriptionCloseFailure =
  ignoreLoggedTypedFailuresPreserveNonTypedFailures(
    "gRPC leased benchmark subscription close failed.",
  );

const positiveIntegerFromEnv = (name: string, fallback: number): number => {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const normalized = raw.trim();
  if (!/^[1-9]\d*$/u.test(normalized)) {
    throw new Error(`${name} must be a positive integer.`);
  }
  const parsed = Number.parseInt(normalized, 10);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`${name} must be a safe positive integer.`);
  }
  return parsed;
};

const rowsPerFeed = positiveIntegerFromEnv(
  "VIEW_SERVER_RUNTIME_BENCH_GRPC_LEASED_ROWS_PER_FEED",
  defaultRowsPerFeed,
);
const routeCount = positiveIntegerFromEnv(
  "VIEW_SERVER_RUNTIME_BENCH_GRPC_LEASED_ROUTE_COUNT",
  defaultRouteCount,
);
const retainedRows = positiveIntegerFromEnv(
  "VIEW_SERVER_RUNTIME_BENCH_GRPC_LEASED_RETAINED_ROWS",
  defaultRetainedRows,
);
const outputJsonPath = benchmarkOutputJsonPath(
  `grpc-leased-${rowsPerFeed}rows-${routeCount}routes-${retainedRows}retained.json`,
);
const benchOptions = {
  iterations: positiveIntegerFromEnv("VIEW_SERVER_RUNTIME_BENCH_ITERATIONS", defaultIterations),
  time: 0,
  warmupIterations: 0,
  warmupTime: 0,
};

let profile: BenchmarkProfile | undefined;
let healthRefreshProfile: BenchmarkProfile | undefined;
let benchmarkSetupMemory: BenchmarkMemorySnapshot | undefined;
const healthRefreshSubscriptions: Array<WatchedSubscription> = [];
let nextRowIndex = 0;
let offeredRowCount = 0;
const samples: Array<GrpcLeasedSample> = [];

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
              { name: "region", number: 4, type: FieldDescriptorProto_Type.STRING },
              { name: "updated_at", number: 5, type: FieldDescriptorProto_Type.DOUBLE },
            ],
          },
          {
            name: "OrderKey",
            field: [{ name: "region", number: 1, type: FieldDescriptorProto_Type.STRING }],
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

const orderStatus = (index: number): OrderStatus =>
  index % 5 === 0 ? "cancelled" : index % 3 === 0 ? "closed" : "open";

const grpcOrderValue = (region: string, index: number): GrpcOrderValueMessage => ({
  $typeName: "viewserver.runtime.bench.OrderValue",
  customerId: `${region}-order-${index}`,
  status: orderStatus(index),
  price: index % 10_000,
  region,
  updatedAt: index,
});

const nextRows = (region: string, count: number): ReadonlyArray<GrpcOrderValueMessage> => {
  const start = nextRowIndex;
  nextRowIndex += count;
  return Array.from({ length: count }, (_value, offset) => grpcOrderValue(region, start + offset));
};

const nextOpenRows = (region: string, count: number): ReadonlyArray<GrpcOrderValueMessage> => {
  const start = nextRowIndex;
  nextRowIndex += count;
  return Array.from({ length: count }, (_value, offset) => ({
    ...grpcOrderValue(region, start + offset),
    price: 10 + offset,
    status: "open",
  }));
};

const maxObservedSubscriberCount = (): number => Math.max(50, routeCount);

const topicHealthValues = (health: ViewServerHealth<Topics>) => [health.engine.topics.orders];

const cleanupLeakCountFromHealth = (health: ViewServerHealth<Topics>): number => {
  let leakCount = 0;
  for (const topicHealth of topicHealthValues(health)) {
    leakCount +=
      topicHealth.activeSubscriptions +
      topicHealth.activeViews +
      topicHealth.queuedEvents +
      topicHealth.rowCount;
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

const queueForRoute = (
  queues: ReadonlyMap<string, Queue.Queue<GrpcOrderValueMessage>>,
  region: string,
): Queue.Queue<GrpcOrderValueMessage> => queues.get(region) ?? missingBenchmarkQueue(region);

const missingBenchmarkQueue = (region: string): never => {
  throw new Error(`gRPC leased benchmark route ${region} was not configured.`);
};

const grpcLeasedFeed = (queues: ReadonlyMap<string, Queue.Queue<GrpcOrderValueMessage>>) =>
  grpcFeed.leasedFeed({
    topic: "orders",
    client: "orders",
    method: "streamOrders",
    routeBy: ["region"],
    request: ({ region }) => ({ region }),
    acquire: ({ route }) => Stream.fromQueue(queueForRoute(queues, String(route.region))),
    map: ({ value }) => ({
      id: `${value.region}:${value.customerId}`,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region: value.region,
      updatedAt: value.updatedAt,
    }),
  });

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
  return configured === undefined || configured.trim() === ""
    ? join(".artifacts", fallbackName)
    : configured.trim();
}

function benchmarkSummaryPath(path: string): string {
  return path.endsWith(".json")
    ? `${path.slice(0, -".json".length)}.summary.json`
    : `${path}.summary.json`;
}

function writeJsonFile(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(value, undefined, 2)}\n`);
}

const currentBenchmarkProfile = (): BenchmarkProfile => {
  const currentProfile = profile;
  return currentProfile ?? missingBenchmarkProfile();
};

const currentHealthRefreshProfile = (): BenchmarkProfile => {
  const currentProfile = healthRefreshProfile;
  return currentProfile ?? missingBenchmarkProfile();
};

const missingBenchmarkProfile = (): never => {
  throw new Error("gRPC leased benchmark setup did not create a profile.");
};

const makeBenchmarkProfile = Effect.fn("ViewServerRuntime.grpc.leased.bench.profile.make")(
  function* () {
    const scope = yield* Scope.make("parallel");
    const queueEntries = yield* Effect.forEach(
      [
        "seed",
        "first",
        "reuse",
        "many-subscribers",
        "retained-filter",
        "delta-fanout",
        "cleanup-latency",
        ...Array.from({ length: routeCount }, (_value, index) => `route-${index}`),
      ],
      (region) =>
        Queue.unbounded<GrpcOrderValueMessage>().pipe(
          Effect.map((queue) => [region, queue] as const),
        ),
    );
    const queues = new Map(queueEntries);
    const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
    const health = makeViewServerGrpcHealthLedger<Topics>({
      clients: grpcHealthClientBaseUrls,
      feeds: {},
    });
    const manager = yield* makeViewServerGrpcLeaseManager(
      viewServer,
      runtimeCore.internalClient,
      runtimeCore.liveClient,
      runtimeCore.internalLiveClient,
      Effect.void,
      {
        clientBaseUrls: grpcHealthClientBaseUrls,
        clients: grpcClients,
        feeds: {
          ordersLease: grpcLeasedFeed(queues),
        },
        materializedReconnect: {
          delay: "1 second",
          maxReconnects: 60,
        },
      },
      health,
    );
    return {
      health,
      manager,
      memoryAfterSetup: memorySnapshot(),
      queues,
      runtimeCore,
      scope,
    };
  },
);

const offerRows = Effect.fn("ViewServerRuntime.grpc.leased.bench.rows.offer")(function* (
  queue: Queue.Queue<GrpcOrderValueMessage>,
  rows: ReadonlyArray<GrpcOrderValueMessage>,
) {
  yield* Effect.forEach(rows, (row) => Queue.offer(queue, row), { discard: true });
  yield* Effect.sync(() => {
    offeredRowCount += rows.length;
  });
});

const readHealthOverlay = Effect.fn("ViewServerRuntime.grpc.leased.bench.healthOverlay.read")(
  function* (
    runtimeCore: ViewServerRuntimeCoreInternalInstance<Topics>,
    health: BenchmarkProfile["health"],
  ) {
    const nowMillis = yield* Clock.currentTimeMillis;
    return health.healthOverlay(yield* runtimeCore.client.health(), nowMillis);
  },
);

const watchSubscriptionRows = Effect.fn("ViewServerRuntime.grpc.leased.bench.subscription.watch")(
  function* (subscription: ViewServerLiveSubscription<ProjectedOrderRow>, scope?: Scope.Closeable) {
    const totalRowsQueue = yield* Queue.unbounded<number>();
    const collectRows = subscription.events.pipe(
      Stream.runForEach((event) =>
        event.type === "snapshot" || event.type === "delta"
          ? Queue.offer(totalRowsQueue, event.totalRows).pipe(Effect.asVoid)
          : Effect.void,
      ),
    );
    const fiber =
      scope === undefined
        ? yield* collectRows.pipe(Effect.forkChild)
        : yield* collectRows.pipe(Effect.forkIn(scope, { startImmediately: true }));
    return {
      fiber,
      subscription,
      totalRowsQueue,
    };
  },
);

const waitForWatchedRows = Effect.fn("ViewServerRuntime.grpc.leased.bench.watchedRows.wait")(
  function* (watched: WatchedSubscription, expectedRows: number) {
    return yield* Queue.take(watched.totalRowsQueue).pipe(
      Effect.repeat({
        until: (totalRows) => totalRows === expectedRows,
      }),
      Effect.timeout(convergenceTimeout),
      Effect.flatMap((totalRows) =>
        totalRows === undefined
          ? Effect.fail(
              new GrpcLeasedBenchmarkConvergenceError({
                message: `gRPC leased benchmark subscription did not converge to ${expectedRows} rows within ${convergenceTimeout}.`,
              }),
            )
          : Effect.succeed(totalRows),
      ),
    );
  },
);

const closeWatchedSubscriptions = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.subscriptions.close",
)(function* (watchedSubscriptions: ReadonlyArray<WatchedSubscription>) {
  yield* Effect.forEach(
    watchedSubscriptions,
    (watched) =>
      watched.subscription
        .close()
        .pipe(
          ignoreGrpcLeasedBenchmarkSubscriptionCloseFailure,
          Effect.andThen(Fiber.interrupt(watched.fiber)),
          Effect.asVoid,
        ),
    { discard: true },
  );
});

const closeTrackedSubscriptions = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.subscriptions.closeTracked",
)(function* (watchedSubscriptions: Array<WatchedSubscription>) {
  yield* closeWatchedSubscriptions(watchedSubscriptions);
  watchedSubscriptions.length = 0;
});

const closeProfileResources = Effect.fn("ViewServerRuntime.grpc.leased.bench.profile.close")(
  function* (currentProfile: BenchmarkProfile) {
    const closeRuntimeCore = currentProfile.runtimeCore.close;
    const closeManagerAndRuntimeCore = currentProfile.manager.close.pipe(
      Effect.ensuring(closeRuntimeCore),
    );
    yield* Scope.close(currentProfile.scope, Exit.void).pipe(
      Effect.ensuring(closeManagerAndRuntimeCore),
    );
  },
);

const closeOptionalProfileResources = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.profile.closeOptional",
)(function* (currentProfile: BenchmarkProfile | undefined) {
  if (currentProfile === undefined) {
    return;
  }
  yield* closeProfileResources(currentProfile);
});

const activeLeasedFeedCount = (health: ViewServerHealth<Topics>): number =>
  Object.values(health.grpc?.feeds.orders?.leased ?? {}).length;

const prepareHealthRefreshProfile = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.healthRefreshOverhead.prepare",
)(function* (currentProfile: BenchmarkProfile) {
  const regions = Array.from({ length: routeCount }, (_value, index) => `route-${index}`);
  const subscriptions = yield* Effect.forEach(regions, (region) =>
    currentProfile.manager.liveClient.subscribe("orders", {
      select: ["id", "price", "status", "region"],
      where: {
        region: { eq: region },
      },
      orderBy: [{ field: "updatedAt", direction: "desc" }],
      limit: 10,
    }),
  );
  const watchedSubscriptions = yield* Effect.forEach(subscriptions, (subscription) =>
    watchSubscriptionRows(subscription, currentProfile.scope).pipe(
      Effect.tap((watched) =>
        Effect.sync(() => {
          healthRefreshSubscriptions.push(watched);
        }),
      ),
    ),
  );
  yield* Effect.forEach(
    regions,
    (region) => offerRows(queueForRoute(currentProfile.queues, region), nextRows(region, 1)),
    { discard: true },
  );
  yield* Effect.forEach(
    watchedSubscriptions,
    (subscription) => waitForWatchedRows(subscription, 1),
    { discard: true },
  );
  const health = yield* readHealthOverlay(currentProfile.runtimeCore, currentProfile.health);
  expect(activeLeasedFeedCount(health)).toBe(routeCount);
});

const runLeasedSubscriptionCase = Effect.fn("ViewServerRuntime.grpc.leased.bench.subscription.run")(
  function* (
    name: string,
    currentProfile: BenchmarkProfile,
    region: string,
    subscriberCount: number,
    preOpenSubscriber = false,
  ) {
    const trackedSubscriptions: Array<WatchedSubscription> = [];
    const trackSubscription = (watched: WatchedSubscription) =>
      Effect.sync(() => {
        trackedSubscriptions.push(watched);
        return watched;
      });
    return yield* Effect.gen(function* () {
      const preOpenedSubscriptions = preOpenSubscriber
        ? [
            yield* currentProfile.manager.liveClient.subscribe("orders", {
              select: ["id", "price", "status", "region"],
              where: {
                region: { eq: region },
              },
              orderBy: [{ field: "updatedAt", direction: "desc" }],
              limit: 100,
            }),
          ]
        : [];
      const watchedPreOpenedSubscriptions = yield* Effect.forEach(
        preOpenedSubscriptions,
        (subscription) =>
          watchSubscriptionRows(subscription).pipe(Effect.flatMap(trackSubscription)),
      );
      const beforeSubscribe = performance.now();
      const subscriptions = yield* Effect.forEach(
        Array.from({ length: subscriberCount }, () => region),
        (routeRegion) =>
          currentProfile.manager.liveClient.subscribe("orders", {
            select: ["id", "price", "status", "region"],
            where: {
              region: { eq: routeRegion },
            },
            orderBy: [{ field: "updatedAt", direction: "desc" }],
            limit: 100,
          }),
      );
      const afterSubscribe = performance.now();
      const watchedSubscriptions = yield* Effect.forEach(subscriptions, (subscription) =>
        watchSubscriptionRows(subscription).pipe(Effect.flatMap(trackSubscription)),
      );
      const rows = nextRows(region, rowsPerFeed);
      const beforeRows = performance.now();
      yield* offerRows(queueForRoute(currentProfile.queues, region), rows);
      yield* Effect.forEach(watchedSubscriptions, (subscription) =>
        waitForWatchedRows(subscription, rows.length),
      );
      yield* Effect.forEach(watchedPreOpenedSubscriptions, (subscription) =>
        waitForWatchedRows(subscription, rows.length),
      );
      const afterRows = performance.now();
      const healthBefore = performance.now();
      const health = yield* readHealthOverlay(currentProfile.runtimeCore, currentProfile.health);
      const healthAfter = performance.now();
      const cleanupBefore = performance.now();
      yield* closeTrackedSubscriptions(trackedSubscriptions);
      const cleanupAfter = performance.now();
      const cleanupHealth = yield* readHealthOverlay(
        currentProfile.runtimeCore,
        currentProfile.health,
      );
      expect(activeLeasedFeedCount(cleanupHealth)).toBe(0);
      samples.push({
        activeLeasedFeeds: activeLeasedFeedCount(health),
        cleanupMs: cleanupAfter - cleanupBefore,
        cleanupActiveLeasedFeeds: activeLeasedFeedCount(cleanupHealth),
        deltaFanoutMs: 0,
        healthOverlayMs: healthAfter - healthBefore,
        name,
        rows: rows.length,
        rowsPerSecond: (rows.length / (afterRows - beforeRows)) * 1_000,
        snapshotMs: afterRows - beforeRows,
        subscriptionMs: afterSubscribe - beforeSubscribe,
      });
    }).pipe(Effect.ensuring(closeTrackedSubscriptions(trackedSubscriptions)));
  },
);

const runLocalFilterCase = Effect.fn("ViewServerRuntime.grpc.leased.bench.localFilter.run")(
  function* (name: string, currentProfile: BenchmarkProfile) {
    const trackedSubscriptions: Array<WatchedSubscription> = [];
    return yield* Effect.gen(function* () {
      const beforeSubscribe = performance.now();
      const subscription = yield* currentProfile.manager.liveClient.subscribe("orders", {
        select: ["id", "price", "status", "region"],
        where: {
          region: { eq: "seed" },
          status: { eq: "open" },
          price: { gte: 10 },
        },
        orderBy: [{ field: "updatedAt", direction: "desc" }],
        limit: rowsPerFeed,
      });
      const afterSubscribe = performance.now();
      const watchedSubscription = yield* watchSubscriptionRows(subscription).pipe(
        Effect.tap((watched) =>
          Effect.sync(() => {
            trackedSubscriptions.push(watched);
          }),
        ),
      );
      const rows = nextOpenRows("seed", rowsPerFeed);
      const beforeSnapshot = performance.now();
      yield* offerRows(queueForRoute(currentProfile.queues, "seed"), rows);
      yield* waitForWatchedRows(watchedSubscription, rows.length);
      const afterSnapshot = performance.now();
      const healthBefore = performance.now();
      const health = yield* readHealthOverlay(currentProfile.runtimeCore, currentProfile.health);
      const healthAfter = performance.now();
      const cleanupBefore = performance.now();
      yield* closeTrackedSubscriptions(trackedSubscriptions);
      const cleanupAfter = performance.now();
      const cleanupHealth = yield* readHealthOverlay(
        currentProfile.runtimeCore,
        currentProfile.health,
      );
      expect(activeLeasedFeedCount(cleanupHealth)).toBe(0);
      samples.push({
        activeLeasedFeeds: activeLeasedFeedCount(health),
        cleanupMs: cleanupAfter - cleanupBefore,
        cleanupActiveLeasedFeeds: activeLeasedFeedCount(cleanupHealth),
        deltaFanoutMs: 0,
        healthOverlayMs: healthAfter - healthBefore,
        name,
        rows: rows.length,
        rowsPerSecond: (rows.length / (afterSnapshot - beforeSnapshot)) * 1_000,
        snapshotMs: afterSnapshot - beforeSnapshot,
        subscriptionMs: afterSubscribe - beforeSubscribe,
      });
    }).pipe(Effect.ensuring(closeTrackedSubscriptions(trackedSubscriptions)));
  },
);

const runManyRouteCase = Effect.fn("ViewServerRuntime.grpc.leased.bench.manyRoutes.run")(function* (
  name: string,
  currentProfile: BenchmarkProfile,
) {
  const trackedSubscriptions: Array<WatchedSubscription> = [];
  return yield* Effect.gen(function* () {
    const regions = Array.from({ length: routeCount }, (_value, index) => `route-${index}`);
    const beforeSubscribe = performance.now();
    const subscriptions = yield* Effect.forEach(regions, (region) =>
      currentProfile.manager.liveClient.subscribe("orders", {
        select: ["id", "price", "status", "region"],
        where: {
          region: { eq: region },
        },
        orderBy: [{ field: "updatedAt", direction: "desc" }],
        limit: 10,
      }),
    );
    const afterSubscribe = performance.now();
    const watchedSubscriptions = yield* Effect.forEach(subscriptions, (subscription) =>
      watchSubscriptionRows(subscription).pipe(
        Effect.tap((watched) =>
          Effect.sync(() => {
            trackedSubscriptions.push(watched);
          }),
        ),
      ),
    );
    const beforeRows = performance.now();
    yield* Effect.forEach(
      regions,
      (region) => offerRows(queueForRoute(currentProfile.queues, region), nextRows(region, 1)),
      { discard: true },
    );
    yield* Effect.forEach(
      watchedSubscriptions,
      (subscription) => waitForWatchedRows(subscription, 1),
      {
        discard: true,
      },
    );
    const afterRows = performance.now();
    const healthBefore = performance.now();
    const health = yield* readHealthOverlay(currentProfile.runtimeCore, currentProfile.health);
    const healthAfter = performance.now();
    const cleanupBefore = performance.now();
    yield* closeTrackedSubscriptions(trackedSubscriptions);
    const cleanupAfter = performance.now();
    const cleanupHealth = yield* readHealthOverlay(
      currentProfile.runtimeCore,
      currentProfile.health,
    );
    expect(activeLeasedFeedCount(cleanupHealth)).toBe(0);
    samples.push({
      activeLeasedFeeds: activeLeasedFeedCount(health),
      cleanupMs: cleanupAfter - cleanupBefore,
      cleanupActiveLeasedFeeds: activeLeasedFeedCount(cleanupHealth),
      deltaFanoutMs: 0,
      healthOverlayMs: healthAfter - healthBefore,
      name,
      rows: regions.length,
      rowsPerSecond: (regions.length / (afterRows - beforeRows)) * 1_000,
      snapshotMs: afterRows - beforeRows,
      subscriptionMs: afterSubscribe - beforeSubscribe,
    });
  }).pipe(Effect.ensuring(closeTrackedSubscriptions(trackedSubscriptions)));
});

const runRetainedLocalFilterCase = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.retainedLocalFilter.run",
)(function* (name: string, currentProfile: BenchmarkProfile) {
  const trackedSubscriptions: Array<WatchedSubscription> = [];
  return yield* Effect.gen(function* () {
    const holderSubscription = yield* currentProfile.manager.liveClient.subscribe("orders", {
      select: ["id", "price", "status", "region"],
      where: {
        region: { eq: "retained-filter" },
      },
      orderBy: [{ field: "updatedAt", direction: "desc" }],
      limit: 100,
    });
    const holder = yield* watchSubscriptionRows(holderSubscription).pipe(
      Effect.tap((watched) =>
        Effect.sync(() => {
          trackedSubscriptions.push(watched);
        }),
      ),
    );
    const rows = nextOpenRows("retained-filter", retainedRows);
    yield* offerRows(queueForRoute(currentProfile.queues, "retained-filter"), rows);
    yield* waitForWatchedRows(holder, rows.length);
    const beforeSubscribe = performance.now();
    const subscription = yield* currentProfile.manager.liveClient.subscribe("orders", {
      select: ["id", "price", "status", "region"],
      where: {
        region: { eq: "retained-filter" },
        status: { eq: "open" },
        price: { gte: 10 },
      },
      orderBy: [{ field: "updatedAt", direction: "desc" }],
      limit: 100,
    });
    const afterSubscribe = performance.now();
    const watchedSubscription = yield* watchSubscriptionRows(subscription).pipe(
      Effect.tap((watched) =>
        Effect.sync(() => {
          trackedSubscriptions.push(watched);
        }),
      ),
    );
    const beforeSnapshot = performance.now();
    yield* waitForWatchedRows(watchedSubscription, rows.length);
    const afterSnapshot = performance.now();
    const healthBefore = performance.now();
    const health = yield* readHealthOverlay(currentProfile.runtimeCore, currentProfile.health);
    const healthAfter = performance.now();
    const cleanupBefore = performance.now();
    yield* closeTrackedSubscriptions(trackedSubscriptions);
    const cleanupAfter = performance.now();
    const cleanupHealth = yield* readHealthOverlay(
      currentProfile.runtimeCore,
      currentProfile.health,
    );
    expect(activeLeasedFeedCount(cleanupHealth)).toBe(0);
    samples.push({
      activeLeasedFeeds: activeLeasedFeedCount(health),
      cleanupMs: cleanupAfter - cleanupBefore,
      cleanupActiveLeasedFeeds: activeLeasedFeedCount(cleanupHealth),
      deltaFanoutMs: 0,
      healthOverlayMs: healthAfter - healthBefore,
      name,
      rows: rows.length,
      rowsPerSecond: (rows.length / (afterSnapshot - beforeSnapshot)) * 1_000,
      snapshotMs: afterSnapshot - beforeSnapshot,
      subscriptionMs: afterSubscribe - beforeSubscribe,
    });
  }).pipe(Effect.ensuring(closeTrackedSubscriptions(trackedSubscriptions)));
});

const runDeltaFanoutCase = Effect.fn("ViewServerRuntime.grpc.leased.bench.deltaFanout.run")(
  function* (name: string, currentProfile: BenchmarkProfile) {
    const trackedSubscriptions: Array<WatchedSubscription> = [];
    return yield* Effect.gen(function* () {
      const beforeSubscribe = performance.now();
      const subscriptions = yield* Effect.forEach(
        Array.from({ length: 25 }, () => "delta-fanout"),
        (region) =>
          currentProfile.manager.liveClient.subscribe("orders", {
            select: ["id", "price", "status", "region"],
            where: {
              region: { eq: region },
            },
            orderBy: [{ field: "updatedAt", direction: "desc" }],
            limit: 100,
          }),
      );
      const afterSubscribe = performance.now();
      const watchedSubscriptions = yield* Effect.forEach(subscriptions, (subscription) =>
        watchSubscriptionRows(subscription).pipe(
          Effect.tap((watched) =>
            Effect.sync(() => {
              trackedSubscriptions.push(watched);
            }),
          ),
        ),
      );
      yield* offerRows(
        queueForRoute(currentProfile.queues, "delta-fanout"),
        nextRows("delta-fanout", 1),
      );
      yield* Effect.forEach(watchedSubscriptions, (subscription) =>
        waitForWatchedRows(subscription, 1),
      );
      const rows = nextRows("delta-fanout", rowsPerFeed);
      const beforeDelta = performance.now();
      yield* offerRows(queueForRoute(currentProfile.queues, "delta-fanout"), rows);
      yield* Effect.forEach(watchedSubscriptions, (subscription) =>
        waitForWatchedRows(subscription, rows.length + 1),
      );
      const afterDelta = performance.now();
      const healthBefore = performance.now();
      const health = yield* readHealthOverlay(currentProfile.runtimeCore, currentProfile.health);
      const healthAfter = performance.now();
      const cleanupBefore = performance.now();
      yield* closeTrackedSubscriptions(trackedSubscriptions);
      const cleanupAfter = performance.now();
      const cleanupHealth = yield* readHealthOverlay(
        currentProfile.runtimeCore,
        currentProfile.health,
      );
      expect(activeLeasedFeedCount(cleanupHealth)).toBe(0);
      samples.push({
        activeLeasedFeeds: activeLeasedFeedCount(health),
        cleanupMs: cleanupAfter - cleanupBefore,
        cleanupActiveLeasedFeeds: activeLeasedFeedCount(cleanupHealth),
        deltaFanoutMs: afterDelta - beforeDelta,
        healthOverlayMs: healthAfter - healthBefore,
        name,
        rows: rows.length,
        rowsPerSecond: (rows.length / (afterDelta - beforeDelta)) * 1_000,
        snapshotMs: afterDelta - beforeDelta,
        subscriptionMs: afterSubscribe - beforeSubscribe,
      });
    }).pipe(Effect.ensuring(closeTrackedSubscriptions(trackedSubscriptions)));
  },
);

const runPartitionedWriteConvergenceCase = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.partitionedWriteConvergence.run",
)(function* (name: string, currentProfile: BenchmarkProfile) {
  const trackedSubscriptions: Array<WatchedSubscription> = [];
  return yield* Effect.gen(function* () {
    const regions = Array.from({ length: routeCount }, (_value, index) => `route-${index}`);
    const beforeSubscribe = performance.now();
    const subscriptions = yield* Effect.forEach(regions, (region) =>
      currentProfile.manager.liveClient.subscribe("orders", {
        select: ["id", "price", "status", "region"],
        where: {
          region: { eq: region },
        },
        orderBy: [{ field: "updatedAt", direction: "desc" }],
        limit: rowsPerFeed,
      }),
    );
    const afterSubscribe = performance.now();
    const watchedSubscriptions = yield* Effect.forEach(subscriptions, (subscription) =>
      watchSubscriptionRows(subscription).pipe(
        Effect.tap((watched) =>
          Effect.sync(() => {
            trackedSubscriptions.push(watched);
          }),
        ),
      ),
    );
    const beforeRows = performance.now();
    yield* Effect.forEach(
      regions,
      (region) =>
        offerRows(queueForRoute(currentProfile.queues, region), nextRows(region, rowsPerFeed)),
      { discard: true },
    );
    yield* Effect.forEach(
      watchedSubscriptions,
      (subscription) => waitForWatchedRows(subscription, rowsPerFeed),
      { discard: true },
    );
    const afterRows = performance.now();
    const healthBefore = performance.now();
    const health = yield* readHealthOverlay(currentProfile.runtimeCore, currentProfile.health);
    const healthAfter = performance.now();
    expect(activeLeasedFeedCount(health)).toBe(routeCount);
    const cleanupBefore = performance.now();
    yield* closeTrackedSubscriptions(trackedSubscriptions);
    const cleanupAfter = performance.now();
    const cleanupHealth = yield* readHealthOverlay(
      currentProfile.runtimeCore,
      currentProfile.health,
    );
    expect(activeLeasedFeedCount(cleanupHealth)).toBe(0);
    samples.push({
      activeLeasedFeeds: activeLeasedFeedCount(health),
      cleanupMs: cleanupAfter - cleanupBefore,
      cleanupActiveLeasedFeeds: activeLeasedFeedCount(cleanupHealth),
      deltaFanoutMs: 0,
      healthOverlayMs: healthAfter - healthBefore,
      name,
      rows: rowsPerFeed * regions.length,
      rowsPerSecond: ((rowsPerFeed * regions.length) / (afterRows - beforeRows)) * 1_000,
      snapshotMs: afterRows - beforeRows,
      subscriptionMs: afterSubscribe - beforeSubscribe,
    });
  }).pipe(Effect.ensuring(closeTrackedSubscriptions(trackedSubscriptions)));
});

const runHealthRefreshOverheadCase = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.healthRefreshOverhead.run",
)(function* (name: string, currentProfile: BenchmarkProfile) {
  const beforeHealth = performance.now();
  const healthResults = yield* Effect.forEach(
    Array.from({ length: routeCount }, () => currentProfile),
    (profileForRead) => readHealthOverlay(profileForRead.runtimeCore, profileForRead.health),
  );
  const afterHealth = performance.now();
  expect(healthResults.map(activeLeasedFeedCount)).toStrictEqual(
    Array.from({ length: routeCount }, () => routeCount),
  );
  samples.push({
    activeLeasedFeeds: routeCount,
    cleanupMs: 0,
    cleanupActiveLeasedFeeds: 0,
    deltaFanoutMs: 0,
    healthOverlayMs: afterHealth - beforeHealth,
    name,
    rows: 0,
    rowsPerSecond: 0,
    snapshotMs: 0,
    subscriptionMs: 0,
  });
});

const runCleanupLatencyCase = Effect.fn("ViewServerRuntime.grpc.leased.bench.cleanupLatency.run")(
  function* (name: string, currentProfile: BenchmarkProfile) {
    const trackedSubscriptions: Array<WatchedSubscription> = [];
    return yield* Effect.gen(function* () {
      const beforeSubscribe = performance.now();
      const subscription = yield* currentProfile.manager.liveClient.subscribe("orders", {
        select: ["id", "price", "status", "region"],
        where: {
          region: { eq: "cleanup-latency" },
        },
        orderBy: [{ field: "updatedAt", direction: "desc" }],
        limit: 100,
      });
      const afterSubscribe = performance.now();
      const watchedSubscription = yield* watchSubscriptionRows(subscription).pipe(
        Effect.tap((watched) =>
          Effect.sync(() => {
            trackedSubscriptions.push(watched);
          }),
        ),
      );
      const rows = nextRows("cleanup-latency", rowsPerFeed);
      const beforeSnapshot = performance.now();
      yield* offerRows(queueForRoute(currentProfile.queues, "cleanup-latency"), rows);
      yield* waitForWatchedRows(watchedSubscription, rows.length);
      const afterSnapshot = performance.now();
      const healthBefore = performance.now();
      const health = yield* readHealthOverlay(currentProfile.runtimeCore, currentProfile.health);
      const healthAfter = performance.now();
      const cleanupBefore = performance.now();
      yield* closeTrackedSubscriptions(trackedSubscriptions);
      const cleanupAfter = performance.now();
      const cleanupHealth = yield* readHealthOverlay(
        currentProfile.runtimeCore,
        currentProfile.health,
      );
      expect(activeLeasedFeedCount(cleanupHealth)).toBe(0);
      samples.push({
        activeLeasedFeeds: activeLeasedFeedCount(health),
        cleanupMs: cleanupAfter - cleanupBefore,
        cleanupActiveLeasedFeeds: activeLeasedFeedCount(cleanupHealth),
        deltaFanoutMs: 0,
        healthOverlayMs: healthAfter - healthBefore,
        name,
        rows: rows.length,
        rowsPerSecond: (rows.length / (afterSnapshot - beforeSnapshot)) * 1_000,
        snapshotMs: afterSnapshot - beforeSnapshot,
        subscriptionMs: afterSubscribe - beforeSubscribe,
      });
    }).pipe(Effect.ensuring(closeTrackedSubscriptions(trackedSubscriptions)));
  },
);

const benchmarkCases: ReadonlyArray<BenchmarkCase> = [
  {
    name: "gRPC leased first subscriber",
    run: () =>
      Effect.runPromise(
        runLeasedSubscriptionCase(
          "gRPC leased first subscriber",
          currentBenchmarkProfile(),
          "first",
          1,
        ),
      ),
  },
  {
    name: "gRPC leased same-route reuse",
    run: () =>
      Effect.runPromise(
        runLeasedSubscriptionCase(
          "gRPC leased same-route reuse",
          currentBenchmarkProfile(),
          "reuse",
          9,
          true,
        ),
      ),
  },
  {
    name: "gRPC leased one route many subscribers",
    run: () =>
      Effect.runPromise(
        runLeasedSubscriptionCase(
          "gRPC leased one route many subscribers",
          currentBenchmarkProfile(),
          "many-subscribers",
          50,
        ),
      ),
  },
  {
    name: "gRPC leased local-filter live snapshot",
    run: () =>
      Effect.runPromise(
        runLocalFilterCase("gRPC leased local-filter live snapshot", currentBenchmarkProfile()),
      ),
  },
  {
    name: "gRPC leased retained local-filter snapshot",
    run: () =>
      Effect.runPromise(
        runRetainedLocalFilterCase(
          "gRPC leased retained local-filter snapshot",
          currentBenchmarkProfile(),
        ),
      ),
  },
  {
    name: "gRPC leased delta fanout",
    run: () =>
      Effect.runPromise(runDeltaFanoutCase("gRPC leased delta fanout", currentBenchmarkProfile())),
  },
  {
    name: "gRPC leased partitioned write convergence",
    run: () =>
      Effect.runPromise(
        runPartitionedWriteConvergenceCase(
          "gRPC leased partitioned write convergence",
          currentBenchmarkProfile(),
        ),
      ),
  },
  {
    name: "gRPC leased health refresh overhead",
    run: () =>
      Effect.runPromise(
        runHealthRefreshOverheadCase(
          "gRPC leased health refresh overhead",
          currentHealthRefreshProfile(),
        ),
      ),
  },
  {
    name: "gRPC leased last-subscriber cleanup",
    run: () =>
      Effect.runPromise(
        runCleanupLatencyCase("gRPC leased last-subscriber cleanup", currentBenchmarkProfile()),
      ),
  },
  {
    name: "gRPC leased many routes",
    run: () =>
      Effect.runPromise(runManyRouteCase("gRPC leased many routes", currentBenchmarkProfile())),
  },
];

const summarizeSamples = (
  name: string,
): {
  readonly maxActiveLeasedFeeds: number;
  readonly maxCleanupMs: number;
  readonly maxCleanupActiveLeasedFeeds: number;
  readonly maxDeltaFanoutMs: number;
  readonly maxHealthOverlayMs: number;
  readonly maxSnapshotMs: number;
  readonly maxSubscriptionMs: number;
  readonly meanCleanupMs: number;
  readonly meanDeltaFanoutMs: number;
  readonly meanHealthOverlayMs: number;
  readonly meanRowsPerSecond: number;
  readonly meanSnapshotMs: number;
  readonly meanSubscriptionMs: number;
  readonly name: string;
  readonly sampleCount: number;
} => {
  const matching = samples.filter((sample) => sample.name === name);
  if (matching.length === 0) {
    throw new Error(`gRPC leased benchmark case ${name} produced no samples.`);
  }
  const totals = matching.reduce(
    (accumulator, sample) => ({
      cleanupMs: accumulator.cleanupMs + sample.cleanupMs,
      deltaFanoutMs: accumulator.deltaFanoutMs + sample.deltaFanoutMs,
      healthOverlayMs: accumulator.healthOverlayMs + sample.healthOverlayMs,
      rowsPerSecond: accumulator.rowsPerSecond + sample.rowsPerSecond,
      snapshotMs: accumulator.snapshotMs + sample.snapshotMs,
      subscriptionMs: accumulator.subscriptionMs + sample.subscriptionMs,
    }),
    {
      cleanupMs: 0,
      deltaFanoutMs: 0,
      healthOverlayMs: 0,
      rowsPerSecond: 0,
      snapshotMs: 0,
      subscriptionMs: 0,
    },
  );
  const sampleCount = matching.length;
  return {
    maxActiveLeasedFeeds: Math.max(...matching.map((sample) => sample.activeLeasedFeeds)),
    maxCleanupMs: Math.max(...matching.map((sample) => sample.cleanupMs)),
    maxCleanupActiveLeasedFeeds: Math.max(
      ...matching.map((sample) => sample.cleanupActiveLeasedFeeds),
    ),
    maxDeltaFanoutMs: Math.max(...matching.map((sample) => sample.deltaFanoutMs)),
    maxHealthOverlayMs: Math.max(...matching.map((sample) => sample.healthOverlayMs)),
    maxSnapshotMs: Math.max(...matching.map((sample) => sample.snapshotMs)),
    maxSubscriptionMs: Math.max(...matching.map((sample) => sample.subscriptionMs)),
    meanCleanupMs: totals.cleanupMs / sampleCount,
    meanDeltaFanoutMs: totals.deltaFanoutMs / sampleCount,
    meanHealthOverlayMs: totals.healthOverlayMs / sampleCount,
    meanRowsPerSecond: totals.rowsPerSecond / sampleCount,
    meanSnapshotMs: totals.snapshotMs / sampleCount,
    meanSubscriptionMs: totals.subscriptionMs / sampleCount,
    name,
    sampleCount,
  };
};

beforeAll(async () => {
  let createdBenchmarkProfile: BenchmarkProfile | undefined;
  let createdHealthProfile: BenchmarkProfile | undefined;
  profile = await Effect.runPromise(
    Effect.gen(function* () {
      const benchmarkProfile = yield* makeBenchmarkProfile();
      yield* Effect.sync(() => {
        createdBenchmarkProfile = benchmarkProfile;
      });
      const healthProfile = yield* makeBenchmarkProfile();
      yield* Effect.sync(() => {
        createdHealthProfile = healthProfile;
      });
      yield* prepareHealthRefreshProfile(healthProfile);
      samples.length = 0;
      offeredRowCount = 0;
      healthRefreshProfile = healthProfile;
      benchmarkSetupMemory = memorySnapshot();
      return benchmarkProfile;
    }).pipe(
      Effect.tapCause(() =>
        closeTrackedSubscriptions(healthRefreshSubscriptions).pipe(
          Effect.ensuring(
            closeOptionalProfileResources(createdHealthProfile).pipe(
              Effect.ensuring(closeOptionalProfileResources(createdBenchmarkProfile)),
            ),
          ),
        ),
      ),
    ),
  );
});

afterAll(async () => {
  const currentProfile = currentBenchmarkProfile();
  const currentHealthProfile = currentHealthRefreshProfile();
  const { health, healthRefreshCleanupHealth } = await Effect.runPromise(
    Effect.gen(function* () {
      yield* closeTrackedSubscriptions(healthRefreshSubscriptions);
      const healthRefreshCleanupHealth: ViewServerHealth<Topics> = yield* readHealthOverlay(
        currentHealthProfile.runtimeCore,
        currentHealthProfile.health,
      );
      expect(activeLeasedFeedCount(healthRefreshCleanupHealth)).toBe(0);
      expect(cleanupLeakCountFromHealth(healthRefreshCleanupHealth)).toBe(0);
      yield* currentProfile.manager.close;
      const health: ViewServerHealth<Topics> = yield* readHealthOverlay(
        currentProfile.runtimeCore,
        currentProfile.health,
      );
      expect(activeLeasedFeedCount(health)).toBe(0);
      return {
        health,
        healthRefreshCleanupHealth,
      };
    }).pipe(
      Effect.ensuring(
        closeProfileResources(currentHealthProfile).pipe(
          Effect.ensuring(closeProfileResources(currentProfile)),
        ),
      ),
    ),
  );
  const memoryAfterBenchmark = memorySnapshot();
  const cleanupLeakCount =
    cleanupLeakCountFromHealth(health) + cleanupLeakCountFromHealth(healthRefreshCleanupHealth);
  const backpressureCount =
    backpressureCountFromHealth(health) + backpressureCountFromHealth(healthRefreshCleanupHealth);
  const queuedEventCount =
    queuedEventCountFromHealth(health) + queuedEventCountFromHealth(healthRefreshCleanupHealth);
  const setupMemory = benchmarkSetupMemory;
  writeJsonFile(benchmarkSummaryPath(outputJsonPath), {
    artifactKind: "runtime-benchmark-summary",
    backpressureCount,
    benchmarkCases: benchmarkCases.map((benchmarkCase) => benchmarkCase.name),
    benchmarkName: "gRPC leased runtime benchmark",
    benchmarkScope: "runtime-grpc-leased",
    cases: benchmarkCases.map((benchmarkCase) => summarizeSamples(benchmarkCase.name)),
    cleanupLeakCount,
    health,
    healthRefreshHealth: healthRefreshCleanupHealth,
    latency: {
      outputJsonPath,
      source: "vitest-output-json",
    },
    memory: {
      afterBenchmark: memoryAfterBenchmark,
      afterSetup: setupMemory,
      before: memoryBefore,
      setupDelta: setupMemory === undefined ? undefined : memoryDelta(memoryBefore, setupMemory),
      totalDelta: memoryDelta(memoryBefore, memoryAfterBenchmark),
    },
    grpcParameters: {
      retainedRows,
      routeCount,
      rowsPerFeed,
    },
    mutationCount: offeredRowCount,
    notes: [
      "Latency percentiles are emitted by Vitest in outputJsonPath.",
      "Benchmark uses the production leased gRPC manager with in-memory Stream sources.",
      "Rows are written into leased feed queues, then observed through the runtime-core live subscription path.",
      "Retained local-filter case keeps one lease holder active so the measured subscriber reads an already-retained feed.",
      "Partitioned write-convergence case opens routeCount leased feeds, writes rowsPerFeed rows into each route, and waits for every subscriber to converge.",
      "Health refresh overhead case uses a separately prepared runtime with routeCount active leased feeds; the Vitest-timed body only reads routeCount health overlays and validates every read. That profile is cleaned up once in afterAll.",
    ],
    queuedEventCount,
    routeCount,
    rowCount: rowsPerFeed,
    rowsPerFeed,
    retainedRows,
    subscriberCount: maxObservedSubscriberCount(),
    topics: ["orders"],
  });
  if (cleanupLeakCount > 0) {
    throw new Error(`gRPC leased benchmark cleanup leaked ${cleanupLeakCount} active resource(s).`);
  }
});

describe("runtime gRPC leased benchmark", () => {
  for (const benchmarkCase of benchmarkCases) {
    bench(benchmarkCase.name, benchmarkCase.run, benchOptions);
  }
});
