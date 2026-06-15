import { NodeSocket } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import type { ViewServerLiveEvent, ViewServerRuntimeLiveClient } from "@view-server/client";
import { makeViewServerClient } from "@view-server/client/remote";
import {
  defineViewServerConfig,
  VIEW_SERVER_HEALTH_TOPIC,
  type ViewServerHealth,
  type ViewServerRuntimeError,
  type ViewServerTransportError,
} from "@view-server/config";
import {
  ViewServerHealthSchema,
  ViewServerRpcErrorSchema,
  ViewServerRpcs,
} from "@view-server/protocol";
import { createViewServerRuntimeCore } from "@view-server/runtime-core";
import {
  Context,
  Cause,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Logger,
  ManagedRuntime,
  References,
  Schema,
  SchemaGetter,
  Scope,
  Stream,
} from "effect";
import { fromStringUnsafe } from "effect/BigDecimal";
import { AtomRef } from "effect/unstable/reactivity";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError";
import * as Socket from "effect/unstable/socket/Socket";
import * as Net from "node:net";
import { makeViewServerWebSocketServer } from "./index";
import { makeViewServerRpcHandlers } from "./rpc-handlers";
import { closeTrackedSockets, makeTrackedSocket } from "./websocket-tracking";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

const Trade = Schema.Struct({
  id: Schema.String,
  quantity: Schema.BigInt,
});

const Quote = Schema.Struct({
  id: Schema.String,
  price: Schema.BigDecimal,
});

const HealthJson = Schema.Struct({
  status: Schema.String,
  engine: Schema.Struct({
    topics: Schema.Struct({
      orders: Schema.Struct({
        rowCount: Schema.Number,
      }),
    }),
  }),
});

const TcpAddress = Schema.Struct({
  port: Schema.Number,
});

class ServerTestJsonParseError extends Schema.TaggedErrorClass<ServerTestJsonParseError>()(
  "ServerTestJsonParseError",
  {
    cause: Schema.Unknown,
  },
) {}

class ServerTestMalformedUpgradeError extends Schema.TaggedErrorClass<ServerTestMalformedUpgradeError>()(
  "ServerTestMalformedUpgradeError",
  {
    cause: Schema.Unknown,
  },
) {}

class ServerTestTcpError extends Schema.TaggedErrorClass<ServerTestTcpError>()(
  "ServerTestTcpError",
  {
    cause: Schema.Unknown,
  },
) {}

class ServerTestWebSocketOpenError extends Schema.TaggedErrorClass<ServerTestWebSocketOpenError>()(
  "ServerTestWebSocketOpenError",
  {
    cause: Schema.Unknown,
  },
) {}

const BadJsonField = Schema.String.pipe(
  Schema.encodeTo(Schema.Any, {
    decode: SchemaGetter.transform((value) => (typeof value === "string" ? value : "decoded")),
    encode: SchemaGetter.transform(() => Symbol("not-json")),
  }),
);

const BadJsonRow = Schema.Struct({
  id: BadJsonField,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
    trades: {
      schema: Trade,
      key: "id",
    },
    quotes: {
      schema: Quote,
      key: "id",
    },
  },
});

const edgeViewServer = defineViewServerConfig({
  topics: {
    badjson: {
      schema: BadJsonRow,
      key: "id",
    },
  },
});

const createServerTestRuntime = createViewServerRuntimeCore;

const kafkaStartFromHealth = {
  consumerGroupId: "view-server-test",
  fallbackMode: "latest",
  mode: "latest",
} as const;

type OrderRow = typeof Order.Type;
type TradeRow = typeof Trade.Type;
type QuoteRow = typeof Quote.Type;

const order = (id: string, price: number): OrderRow => ({
  id,
  price,
});

const trade = (id: string, quantity: bigint): TradeRow => ({
  id,
  quantity,
});

const quote = (id: string, price: string): QuoteRow => ({
  id,
  price: fromStringUnsafe(price),
});

const serverHealthWithOrdersRowCount = (
  health: ViewServerHealth<typeof viewServer.topics>,
  rowCount: number,
): ViewServerHealth<typeof viewServer.topics> => ({
  ...health,
  engine: {
    ...health.engine,
    topics: {
      ...health.engine.topics,
      orders: {
        ...health.engine.topics.orders,
        rowCount,
      },
    },
  },
});

class RawViewServerRpcClient extends Context.Service<
  RawViewServerRpcClient,
  RpcClient.FromGroup<typeof ViewServerRpcs, RpcClientError>
>()("RawViewServerRpcClient") {}

const makeRawRpcClient = Effect.fn("ViewServerServer.test.rawRpcClient.make")(function* (
  url: string,
) {
  const layer: Layer.Layer<RawViewServerRpcClient, never, never> = Layer.effect(
    RawViewServerRpcClient,
  )(RpcClient.make(ViewServerRpcs)).pipe(
    Layer.provide(RpcClient.layerProtocolSocket()),
    Layer.provide([NodeSocket.layerWebSocket(url), RpcSerialization.layerNdjson]),
  );
  const runtime = ManagedRuntime.make(layer);
  const context = yield* runtime.contextEffect;
  return {
    close: runtime.disposeEffect,
    rpc: Context.get(context, RawViewServerRpcClient),
  };
});

const fetchJson = Effect.fn("ViewServerServer.test.fetchJson")(function* (url: string) {
  const response = yield* Effect.promise(() => fetch(url));
  const text = yield* Effect.promise(() => response.text());
  const value = yield* Effect.try({
    try: (): unknown => JSON.parse(text),
    catch: (cause) => new ServerTestJsonParseError({ cause }),
  });
  return { response, value };
});

const reserveTcpPort = Effect.fn("ViewServerServer.test.tcp.reservePort")(function* () {
  const server = yield* Effect.acquireRelease(
    Effect.callback<Net.Server, ServerTestTcpError>((resume) => {
      const blocker = Net.createServer();
      const cleanup = () => {
        blocker.removeAllListeners();
      };
      blocker.once("error", (cause) => {
        cleanup();
        resume(Effect.fail(new ServerTestTcpError({ cause })));
      });
      blocker.listen(0, "127.0.0.1", () => {
        cleanup();
        resume(Effect.succeed(blocker));
      });
    }),
    (server) =>
      Effect.callback<void>((resume) => {
        server.close(() => {
          resume(Effect.void);
        });
      }),
  );
  const address = yield* Schema.decodeUnknownEffect(TcpAddress)(server.address());
  return address.port;
});

const sendMalformedWebSocketUpgrade = Effect.fn("ViewServerServer.test.websocket.malformedUpgrade")(
  function* (url: string) {
    const target = new URL(url.replace("ws://", "http://"));
    yield* Effect.callback<void, ServerTestMalformedUpgradeError>((resume, signal) => {
      const socket = Net.createConnection({
        host: target.hostname,
        port: Number(target.port),
      });
      const cleanup = () => {
        socket.removeAllListeners();
        socket.destroy();
      };
      const succeed = () => {
        cleanup();
        resume(Effect.void);
      };
      const fail = (cause: unknown) => {
        cleanup();
        resume(Effect.fail(new ServerTestMalformedUpgradeError({ cause })));
      };

      signal.addEventListener("abort", cleanup, { once: true });
      socket.once("connect", () => {
        socket.write(
          [
            `GET ${target.pathname} HTTP/1.1`,
            `Host: ${target.host}`,
            "Connection: Upgrade",
            "Upgrade: websocket",
            "",
            "",
          ].join("\r\n"),
          () => {
            socket.destroy();
          },
        );
      });
      socket.once("close", succeed);
      socket.once("error", fail);
      return Effect.sync(cleanup);
    });
  },
);

const openRawWebSocket = Effect.fn("ViewServerServer.test.websocket.raw.open")(function* (
  url: string,
) {
  return yield* Effect.callback<globalThis.WebSocket, ServerTestWebSocketOpenError>(
    (resume, signal) => {
      const socket = new WebSocket(url);
      const cleanup = () => {
        socket.removeEventListener("open", opened);
        socket.removeEventListener("error", failed);
        signal.removeEventListener("abort", aborted);
      };
      function opened() {
        cleanup();
        resume(Effect.succeed(socket));
      }
      function failed(cause: Event) {
        cleanup();
        socket.close();
        resume(Effect.fail(new ServerTestWebSocketOpenError({ cause })));
      }
      function aborted() {
        cleanup();
        socket.close();
      }

      signal.addEventListener("abort", aborted, { once: true });
      socket.addEventListener("open", opened, { once: true });
      socket.addEventListener("error", failed, { once: true });
      return Effect.sync(aborted);
    },
  );
});

describe("@view-server/server", () => {
  it.live("serves an in-memory runtime through Effect RPC WebSocket", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      let openedClients = 0;
      let closedClients = 0;
      let openedStreams = 0;
      let closedStreams = 0;
      const clientClosedSignal = yield* Deferred.make<void>();
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
        transport: {
          clientOpened: Effect.sync(() => {
            openedClients += 1;
          }),
          clientClosed: Effect.gen(function* () {
            closedClients += 1;
            yield* Deferred.succeed(clientClosedSignal, void 0);
          }),
          streamOpened: Effect.sync(() => {
            openedStreams += 1;
          }),
          streamClosed: Effect.sync(() => {
            closedStreams += 1;
          }),
        },
      });
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      const subscription = yield* client.subscribe("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const eventsFiber = yield* subscription.events.pipe(
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.sleep("10 millis");

      yield* inMemory.client.publish("orders", order("b", 20));
      yield* inMemory.client.publishMany("orders", [order("a", 10)]);

      const events = yield* Fiber.join(eventsFiber);
      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(events[1]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [{ type: "insert", key: "b", row: { id: "b", price: 20 }, index: 0 }],
        totalRows: 1,
      });
      expect(events[2]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [{ type: "insert", key: "a", row: { id: "a", price: 10 }, index: 0 }],
        totalRows: 2,
      });

      const healthSummarySubscription = yield* client.subscribeHealthSummary();
      const healthSummaryEvents = yield* healthSummarySubscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
      );
      const healthSummarySnapshots = Array.from(healthSummaryEvents).filter(
        (event) => event.type === "snapshot",
      );
      expect(healthSummarySnapshots[0]?.rows[0]?.runtimeStatus).toBe("ready");
      expect(healthSummarySnapshots[0]?.rows[0]?.connectionStatus).toBe("connected");
      yield* healthSummarySubscription.close();

      const healthSubscription = yield* client.subscribeHealth();
      const healthEvents = yield* healthSubscription.events.pipe(Stream.take(1), Stream.runCollect);
      const healthSnapshots = Array.from(healthEvents).filter((event) => event.type === "snapshot");
      expect(healthSnapshots[0]?.rows[0]?.rowCount).toBe(2);
      yield* healthSubscription.close();

      yield* inMemory.client.reset();
      expect((yield* inMemory.client.health()).engine.topics.orders.rowCount).toBe(0);

      yield* Effect.sleep("10 millis");
      const afterClose = yield* inMemory.client.health();
      expect(afterClose.engine.topics.orders.activeSubscriptions).toBe(0);

      yield* client.close;
      yield* Deferred.await(clientClosedSignal);
      expect(openedClients).toBe(1);
      expect(closedClients).toBe(1);
      expect(openedStreams).toBe(3);
      expect(closedStreams).toBe(3);
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("does not count plain HTTP GET requests as websocket clients", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      let openedClients = 0;
      let closedClients = 0;
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
        transport: {
          clientOpened: Effect.sync(() => {
            openedClients += 1;
          }),
          clientClosed: Effect.sync(() => {
            closedClients += 1;
          }),
        },
      });

      const response = yield* Effect.promise(() => fetch(server.url.replace("ws://", "http://")));
      yield* Effect.promise(() => response.text());

      expect(response.ok).toBe(false);
      expect(openedClients).toBe(0);
      expect(closedClients).toBe(0);
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("does not count malformed websocket upgrades as clients", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      let openedClients = 0;
      let closedClients = 0;
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
        transport: {
          clientOpened: Effect.sync(() => {
            openedClients += 1;
          }),
          clientClosed: Effect.sync(() => {
            closedClients += 1;
          }),
        },
      });

      yield* sendMalformedWebSocketUpgrade(server.url);

      expect(openedClients).toBe(0);
      expect(closedClients).toBe(0);
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("fails when the websocket server port is unavailable", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const inMemory = createServerTestRuntime(viewServer);
        const reservedPort = yield* reserveTcpPort();

        const startupExit = yield* makeViewServerWebSocketServer(
          viewServer,
          {
            liveClient: inMemory.liveClient,
            runtime: inMemory.client,
          },
          { host: "127.0.0.1", port: reservedPort },
        ).pipe(Effect.exit);

        expect(Exit.isFailure(startupExit)).toBe(true);
        yield* inMemory.close;
      }),
    ),
  );

  it.live("closes tracked websocket clients when interrupted during the open hook", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      let openedClients = 0;
      let closedClients = 0;
      const clientOpenedSignal = yield* Deferred.make<void>();
      const clientClosedSignal = yield* Deferred.make<void>();
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
        transport: {
          clientOpened: Effect.gen(function* () {
            openedClients += 1;
            yield* Deferred.succeed(clientOpenedSignal, void 0);
            return yield* Effect.never;
          }),
          clientClosed: Effect.gen(function* () {
            closedClients += 1;
            yield* Deferred.succeed(clientClosedSignal, void 0);
          }),
        },
      });

      const socket = yield* openRawWebSocket(server.url);
      yield* Deferred.await(clientOpenedSignal);
      socket.close();
      const closeFiber = yield* server.close.pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(clientClosedSignal);
      yield* Fiber.join(closeFiber);

      expect(openedClients).toBe(1);
      expect(closedClients).toBe(1);
      yield* inMemory.close;
    }),
  );

  it.live("tracks active websocket close frames and logs shutdown close failures", () => {
    const logs: Array<{
      readonly cause: Cause.Cause<unknown>;
      readonly logLevel: unknown;
      readonly message: unknown;
    }> = [];
    const logger = Logger.make<unknown, void>((options) => {
      logs.push({
        cause: options.cause,
        logLevel: options.logLevel,
        message: options.message,
      });
    });

    return Effect.gen(function* () {
      const activeSocketClosers = new Set<Effect.Effect<void, unknown>>();
      const socketOpened = yield* Deferred.make<void>();
      const socketClosed = yield* Deferred.make<void>();
      const writtenChunks: Array<Uint8Array | string | Socket.CloseEvent> = [];
      const writeFailure = new Socket.SocketError({
        reason: new Socket.SocketWriteError({ cause: "write failed" }),
      });
      const successfulSocket = Socket.make({
        writer: Effect.succeed((chunk) =>
          Effect.sync(() => {
            writtenChunks.push(chunk);
          }),
        ),
        runRaw: (_handler, options) =>
          Effect.gen(function* () {
            const onOpen = options?.onOpen ?? Effect.void;
            yield* onOpen;
            yield* Deferred.succeed(socketOpened, undefined);
            return yield* Effect.never;
          }),
      });
      const trackedSocket = makeTrackedSocket(
        successfulSocket,
        Effect.void,
        Deferred.succeed(socketClosed, undefined),
        activeSocketClosers,
      );

      const socketFiber = yield* trackedSocket
        .runRaw(() => Effect.void)
        .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(socketOpened);
      expect(activeSocketClosers.size).toBe(1);

      yield* closeTrackedSockets(activeSocketClosers);
      expect(writtenChunks).toStrictEqual([
        new Socket.CloseEvent(1001, "View Server shutting down"),
      ]);

      const failingCloser = Effect.fail(writeFailure);
      activeSocketClosers.add(failingCloser);
      yield* closeTrackedSockets(activeSocketClosers);
      activeSocketClosers.delete(failingCloser);
      expect(logs).toHaveLength(1);
      expect(logs[0]?.message).toStrictEqual(["WebSocket shutdown failed."]);
      expect(logs[0]?.logLevel).toBe("Warn");
      expect(Cause.hasFails(logs[0]?.cause ?? Cause.empty)).toBe(true);

      yield* Fiber.interrupt(socketFiber);
      yield* Deferred.await(socketClosed);
      expect(activeSocketClosers.size).toBe(0);
    }).pipe(
      Effect.provide(Logger.layer([logger])),
      Effect.provideService(References.MinimumLogLevel, "Trace"),
    );
  });

  it.live("does not let a stuck websocket close frame block other tracked sockets", () =>
    Effect.gen(function* () {
      const activeSocketClosers = new Set<Effect.Effect<void, unknown>>();
      const fastCloseRan = yield* Deferred.make<void>();
      activeSocketClosers.add(Effect.never);
      activeSocketClosers.add(Deferred.succeed(fastCloseRan, undefined));

      yield* closeTrackedSockets(activeSocketClosers).pipe(Effect.timeout("1 second"));
      yield* Deferred.await(fastCloseRan).pipe(Effect.timeout("1 second"));
    }),
  );

  it.live("serves GET /health beside the websocket RPC endpoint", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });

      yield* inMemory.client.publish("orders", order("a", 10));

      const readyHealth = yield* fetchJson(server.healthUrl);
      const readyBody = yield* Schema.decodeUnknownEffect(HealthJson)(readyHealth.value);
      expect(readyHealth.response.status).toBe(200);
      expect(readyBody.status).toBe("ready");
      expect(readyBody.engine.topics.orders.rowCount).toBe(1);

      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("closes transport stream counters when subscription acquisition fails", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      let openedStreams = 0;
      let closedStreams = 0;
      const subscribeError: ViewServerRuntimeError = {
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        message: "subscription unavailable",
      };
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: {
          ...inMemory.liveClient,
          subscribeRuntime: () => Effect.fail(subscribeError),
        },
        runtime: inMemory.client,
        transport: {
          streamOpened: Effect.sync(() => {
            openedStreams += 1;
          }),
          streamClosed: Effect.sync(() => {
            closedStreams += 1;
          }),
        },
      });
      const client = yield* makeViewServerClient(viewServer, { url: server.url });

      const subscription = yield* client.subscribe("orders", {
        select: ["id"],
      });
      const failedEvents = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(Array.from(failedEvents)).toStrictEqual([
        {
          type: "status",
          topic: "orders",
          queryId: "remote",
          status: "error",
          code: "RuntimeUnavailable",
          message: "subscription unavailable",
        },
      ]);
      expect(openedStreams).toBe(1);
      expect(closedStreams).toBe(1);
      yield* subscription.close();
      yield* client.close;
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("returns 500 when runtime health fails", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const healthError: ViewServerRuntimeError = {
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        message: "health unavailable",
      };
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          health: () => Effect.fail(healthError),
        },
      });

      const health = yield* fetchJson(server.healthUrl);

      expect(health.response.status).toBe(500);
      expect(health.value).toStrictEqual(healthError);

      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("returns 500 when runtime health is semantically invalid", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const baseHealth = yield* inMemory.client.health();
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          ...inMemory.client,
          health: () =>
            Effect.succeed({
              ...baseHealth,
              kafka: {
                startFrom: kafkaStartFromHealth,
                regions: {},
                topics: {
                  source_orders: {
                    status: "ready",
                    sourceTopic: "source_orders",
                    viewServerTopic: "missing",
                    regions: {},
                  },
                },
              },
            }),
        },
      });

      const health = yield* fetchJson(server.healthUrl);

      expect(health.response.status).toBe(500);
      expect(health.value).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidRow",
        message: "Health payload references unknown topic: missing",
        topic: "missing",
      });

      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("returns 503 for degraded health and serializes bigint fields", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const baseHealth = yield* inMemory.client.health();
      const degradedHealth: ViewServerHealth<typeof viewServer.topics> = {
        ...baseHealth,
        status: "degraded",
        kafka: {
          startFrom: kafkaStartFromHealth,
          regions: {},
          topics: {
            source_orders: {
              status: "degraded",
              sourceTopic: "source_orders",
              viewServerTopic: "orders",
              regions: {
                usa: {
                  connected: true,
                  assignedPartitions: 1,
                  messagesPerSecond: 0,
                  bytesPerSecond: 0,
                  decodedMessagesPerSecond: 0,
                  decodeFailuresPerSecond: 0,
                  mappingFailuresPerSecond: 0,
                  processingFailuresPerSecond: 0,
                  lastMessageAt: null,
                  lastCommitAt: null,
                  consumerLagMessages: 42n,
                  lagSampledAt: null,
                  committedOffset: null,
                  lastError: null,
                },
              },
            },
          },
        },
      };
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          health: () => Effect.succeed(degradedHealth),
        },
      });

      const expectedHealth =
        yield* Schema.encodeUnknownEffect(ViewServerHealthSchema)(degradedHealth);
      const health = yield* fetchJson(server.healthUrl);

      expect(health.response.status).toBe(503);
      expect(health.value).toStrictEqual(expectedHealth);

      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("serves fresh runtime health for Kubernetes readiness", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const baseHealth = yield* inMemory.client.health();
      const degradedHealth: ViewServerHealth<typeof viewServer.topics> = {
        ...baseHealth,
        status: "degraded",
        kafka: {
          startFrom: kafkaStartFromHealth,
          regions: {},
          topics: {
            source_orders: {
              status: "ready",
              sourceTopic: "source_orders",
              viewServerTopic: "orders",
              regions: {
                usa: {
                  connected: true,
                  assignedPartitions: 1,
                  messagesPerSecond: 0,
                  bytesPerSecond: 0,
                  decodedMessagesPerSecond: 0,
                  decodeFailuresPerSecond: 0,
                  mappingFailuresPerSecond: 0,
                  processingFailuresPerSecond: 0,
                  lastMessageAt: null,
                  lastCommitAt: null,
                  consumerLagMessages: 42n,
                  lagSampledAt: null,
                  committedOffset: null,
                  lastError: null,
                },
              },
            },
          },
        },
      };
      const cachedHealth = AtomRef.make<ViewServerHealth<typeof viewServer.topics>>(degradedHealth);
      const liveClient: ViewServerRuntimeLiveClient<typeof viewServer.topics> = {
        ...inMemory.liveClient,
        health: cachedHealth,
      };
      let runtimeHealthCalls = 0;
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient,
        runtime: {
          health: () =>
            Effect.sync(() => {
              runtimeHealthCalls += 1;
              return baseHealth;
            }),
        },
      });

      const firstHealth = yield* fetchJson(server.healthUrl);
      const firstBody = yield* Schema.decodeUnknownEffect(HealthJson)(firstHealth.value);
      expect(firstHealth.response.status).toBe(200);
      expect(firstBody.status).toBe("ready");
      expect(runtimeHealthCalls).toBe(1);

      yield* Effect.sync(() => {
        cachedHealth.set(baseHealth);
      });
      const secondHealth = yield* fetchJson(server.healthUrl);
      const secondBody = yield* Schema.decodeUnknownEffect(HealthJson)(secondHealth.value);
      expect(secondHealth.response.status).toBe(200);
      expect(secondBody.status).toBe("ready");
      expect(runtimeHealthCalls).toBe(2);

      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("preserves typed server errors for raw RPC clients", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });
      const raw = yield* makeRawRpcClient(server.url);

      const unknownSubscribeTopic = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "missing",
          query: { select: ["id"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(unknownSubscribeTopic.code).toBe("InvalidTopic");

      const malformedHealthQuery = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: VIEW_SERVER_HEALTH_TOPIC,
          query: { select: ["rowCount"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(malformedHealthQuery.code).toBe("InvalidQuery");
      expect(malformedHealthQuery.message).toBe("Health query select must be exactly: id");

      const unknownSelect = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["missing"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(unknownSelect.code).toBe("InvalidQuery");

      const unknownWhere = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            where: { missing: { eq: "x" } },
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(unknownWhere.code).toBe("InvalidQuery");

      const unknownOrderBy = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            orderBy: [{ field: "missing", direction: "asc" }],
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(unknownOrderBy.code).toBe("InvalidQuery");

      const emptySelect = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: [] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(emptySelect.code).toBe("InvalidQuery");
      expect(emptySelect.message).toBe("Query select must include at least one field");

      const invalidOffset = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"], offset: -1 },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(invalidOffset.code).toBe("InvalidQuery");
      expect(invalidOffset.message).toBe("Query offset must be a non-negative integer");

      const invalidLimit = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"], limit: -1 },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(invalidLimit.code).toBe("InvalidQuery");
      expect(invalidLimit.message).toBe("Query limit must be a non-negative integer");

      const extraTopLevelQueryKey = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"], whre: { id: "a" } },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(extraTopLevelQueryKey.code).toBe("InvalidQuery");

      const extraOrderByKey = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            orderBy: [{ field: "id", direction: "asc", aggregate: "total" }],
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(extraOrderByKey.code).toBe("InvalidQuery");

      const invalidFilter = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            where: { price: { gt: "bad" } },
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(invalidFilter.code).toBe("InvalidQuery");

      const invalidStartsWith = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            where: { price: { startsWith: "1" } },
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(invalidStartsWith.code).toBe("InvalidQuery");

      const invalidNumericStartsWith = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            where: { price: { startsWith: 1 } },
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(invalidNumericStartsWith.code).toBe("InvalidQuery");
      expect(invalidNumericStartsWith.message).toBe("Filter price does not support startsWith");

      const unsupportedFilter = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            where: {
              id: {
                startsWith: "a",
                raw: "value",
              },
            },
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(unsupportedFilter.code).toBe("InvalidQuery");

      const richFilterEvents = yield* raw.rpc["ViewServer.Subscribe"]({
        topic: "orders",
        query: {
          select: ["id", "price"],
          where: {
            id: {
              in: ["a", "b"],
              startsWith: "a",
            },
            price: 10,
          },
          offset: 0,
        },
      }).pipe(Stream.take(1), Stream.runCollect);
      expect(richFilterEvents[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });

      yield* raw.close;
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("rejects malformed live-client rows during server event encoding", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const makeServerForEvent = Effect.fn("ViewServerServer.test.malformedEventServer.make")(
        function* (event: ViewServerLiveEvent<object>) {
          const liveClient = {
            ...inMemory.liveClient,
            subscribeRuntime: () =>
              Effect.succeed({
                events: Stream.make(event),
                close: () => Effect.void,
              }),
          };
          const server = yield* makeViewServerWebSocketServer(viewServer, {
            liveClient,
            runtime: inMemory.client,
          });
          const raw = yield* makeRawRpcClient(server.url);
          return { raw, server };
        },
      );
      const makeServerForRow = (row: object) =>
        makeServerForEvent({
          type: "snapshot",
          topic: "orders",
          queryId: "malformed",
          version: 0,
          keys: ["bad"],
          rows: [row],
          totalRows: 1,
        });

      const invalidFieldType = yield* makeServerForRow({ id: 1 });
      const invalidFieldTypeError = yield* Effect.flip(
        invalidFieldType.raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(invalidFieldTypeError.code).toBe("InvalidRow");
      yield* invalidFieldType.raw.close;
      yield* invalidFieldType.server.close;

      const unknownField = yield* makeServerForRow({ id: "ok", missing: "bad" });
      const unknownFieldError = yield* Effect.flip(
        unknownField.raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(unknownFieldError.code).toBe("InvalidRow");
      expect(unknownFieldError.message).toBe("Unexpected row field for topic orders: missing");
      yield* unknownField.raw.close;
      yield* unknownField.server.close;

      const missingField = yield* makeServerForRow({ price: 10 });
      const missingFieldError = yield* Effect.flip(
        missingField.raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(missingFieldError.code).toBe("InvalidRow");
      expect(missingFieldError.message).toBe("Missing row field for topic orders: id");
      yield* missingField.raw.close;
      yield* missingField.server.close;

      const wrongTopic = yield* makeServerForEvent({
        type: "status",
        topic: "trades",
        queryId: "wrong-topic",
        status: "ready",
        code: "Ready",
      });
      const wrongTopicError = yield* Effect.flip(
        wrongTopic.raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(wrongTopicError.code).toBe("InvalidRow");
      expect(wrongTopicError.message).toBe("Received event for trades while subscribed to orders");
      yield* wrongTopic.raw.close;
      yield* wrongTopic.server.close;

      yield* inMemory.close;
    }),
  );

  it.live("logs typed RPC handler stream finalization close failures", () => {
    const logs: Array<{
      readonly cause: Cause.Cause<unknown>;
      readonly logLevel: unknown;
      readonly message: unknown;
    }> = [];
    const logger = Logger.make<unknown, void>((options) => {
      logs.push({
        cause: options.cause,
        logLevel: options.logLevel,
        message: options.message,
      });
    });
    let closeAttempts = 0;
    const closeFailure: ViewServerTransportError = {
      _tag: "ViewServerTransportError",
      code: "SubscriptionClosed",
      message: "close failed",
      topic: "orders",
      queryId: "close-failure",
    };
    return Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const closeStarted = yield* Deferred.make<void>();
      const liveClient = {
        ...inMemory.liveClient,
        subscribeRuntime: () =>
          Effect.succeed({
            events: Stream.make({
              type: "snapshot",
              topic: "orders",
              queryId: "close-failure",
              version: 0,
              keys: ["order-1"],
              rows: [{ id: "order-1" }],
              totalRows: 1,
            } satisfies ViewServerLiveEvent<{ readonly id: string }>),
            close: () =>
              Effect.gen(function* () {
                closeAttempts += 1;
                yield* Deferred.succeed(closeStarted, undefined);
                return yield* Effect.fail(closeFailure);
              }),
          }),
      };
      const handlerScope = yield* Scope.make("parallel");
      const handlers = makeViewServerRpcHandlers(
        viewServer,
        {
          liveClient,
          runtime: inMemory.client,
        },
        handlerScope,
      );

      const stream = handlers["ViewServer.Subscribe"]({
        topic: "orders",
        query: { select: ["id"] },
      });
      const events = yield* stream.pipe(Stream.take(1), Stream.runCollect);

      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "close-failure",
        version: 0,
        keys: ["order-1"],
        rows: [{ id: "order-1" }],
        totalRows: 1,
      });

      yield* Deferred.await(closeStarted);
      expect(closeAttempts).toBe(1);
      expect(logs).toHaveLength(1);
      expect(logs[0]?.message).toStrictEqual(["RPC subscription close failed."]);
      expect(logs[0]?.logLevel).toBe("Warn");
      expect(Cause.hasFails(logs[0]?.cause ?? Cause.empty)).toBe(true);
      expect(Cause.hasDies(logs[0]?.cause ?? Cause.empty)).toBe(false);
      expect(Cause.hasInterrupts(logs[0]?.cause ?? Cause.empty)).toBe(false);
      yield* Scope.close(handlerScope, Exit.void);
      yield* inMemory.close;
    }).pipe(
      Effect.provide(Logger.layer([logger])),
      Effect.provideService(References.MinimumLogLevel, "Trace"),
    );
  });

  it.live(
    "preserves RPC handler stream finalization close defects mixed with typed failures",
    () => {
      const logs: Array<{
        readonly cause: Cause.Cause<unknown>;
        readonly logLevel: unknown;
        readonly message: unknown;
      }> = [];
      const logger = Logger.make<unknown, void>((options) => {
        logs.push({
          cause: options.cause,
          logLevel: options.logLevel,
          message: options.message,
        });
      });
      let closeAttempts = 0;
      const closeFailure: ViewServerTransportError = {
        _tag: "ViewServerTransportError",
        code: "SubscriptionClosed",
        message: "close failed",
        topic: "orders",
        queryId: "close-failure",
      };
      return Effect.gen(function* () {
        const inMemory = createServerTestRuntime(viewServer);
        const closeStarted = yield* Deferred.make<void>();
        const liveClient = {
          ...inMemory.liveClient,
          subscribeRuntime: () =>
            Effect.succeed({
              events: Stream.make({
                type: "snapshot",
                topic: "orders",
                queryId: "close-failure",
                version: 0,
                keys: ["order-1"],
                rows: [{ id: "order-1" }],
                totalRows: 1,
              } satisfies ViewServerLiveEvent<{ readonly id: string }>),
              close: () =>
                Effect.gen(function* () {
                  closeAttempts += 1;
                  yield* Deferred.succeed(closeStarted, undefined);
                  return yield* Effect.failCause(
                    Cause.fromReasons([
                      Cause.makeFailReason(closeFailure),
                      Cause.makeDieReason("close defect"),
                    ]),
                  );
                }),
            }),
        };
        const handlerScope = yield* Scope.make("parallel");
        const handlers = makeViewServerRpcHandlers(
          viewServer,
          {
            liveClient,
            runtime: inMemory.client,
          },
          handlerScope,
        );

        const stream = handlers["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"] },
        });
        const cause = yield* stream.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.sandbox,
          Effect.flip,
        );

        yield* Deferred.await(closeStarted);
        expect(Cause.hasDies(cause)).toBe(true);
        expect(Cause.hasFails(cause)).toBe(false);
        expect(closeAttempts).toBe(1);
        expect(logs).toHaveLength(1);
        expect(logs[0]?.message).toStrictEqual(["RPC subscription close failed."]);
        expect(logs[0]?.logLevel).toBe("Warn");
        expect(Cause.hasFails(logs[0]?.cause ?? Cause.empty)).toBe(true);
        expect(Cause.hasDies(logs[0]?.cause ?? Cause.empty)).toBe(false);
        expect(Cause.hasInterrupts(logs[0]?.cause ?? Cause.empty)).toBe(false);
        yield* Scope.close(handlerScope, Exit.void);
        yield* inMemory.close;
      }).pipe(
        Effect.provide(Logger.layer([logger])),
        Effect.provideService(References.MinimumLogLevel, "Trace"),
      );
    },
  );

  it.live("does not fail RPC stream finalization when typed subscription close fails", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const closeStarted = yield* Deferred.make<void>();
      let closeAttempts = 0;
      const closeFailure: ViewServerTransportError = {
        _tag: "ViewServerTransportError",
        code: "SubscriptionClosed",
        message: "close failed",
        topic: "orders",
        queryId: "close-failure",
      };
      const liveClient = {
        ...inMemory.liveClient,
        subscribeRuntime: () =>
          Effect.succeed({
            events: Stream.make({
              type: "snapshot",
              topic: "orders",
              queryId: "close-failure",
              version: 0,
              keys: ["order-1"],
              rows: [{ id: "order-1" }],
              totalRows: 1,
            } satisfies ViewServerLiveEvent<{ readonly id: string }>),
            close: () =>
              Effect.gen(function* () {
                closeAttempts += 1;
                yield* Deferred.succeed(closeStarted, undefined);
                return yield* Effect.fail(closeFailure);
              }),
          }),
      };
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient,
        runtime: inMemory.client,
      });
      const raw = yield* makeRawRpcClient(server.url);

      const events = yield* raw.rpc["ViewServer.Subscribe"]({
        topic: "orders",
        query: { select: ["id"] },
      }).pipe(Stream.take(1), Stream.runCollect);

      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "close-failure",
        version: 0,
        keys: ["order-1"],
        rows: [{ id: "order-1" }],
        totalRows: 1,
      });

      yield* Deferred.await(closeStarted);
      expect(closeAttempts).toBe(1);
      yield* raw.close;
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("rejects non-json schema encodings during server event encoding", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(edgeViewServer);
      const event: ViewServerLiveEvent<object> = {
        type: "snapshot",
        topic: "badjson",
        queryId: "badjson",
        version: 0,
        keys: ["bad"],
        rows: [{ id: "bad" }],
        totalRows: 1,
      };
      const liveClient = {
        ...inMemory.liveClient,
        subscribeRuntime: () =>
          Effect.succeed({
            events: Stream.make(event),
            close: () => Effect.void,
          }),
      };
      const server = yield* makeViewServerWebSocketServer(edgeViewServer, {
        liveClient,
        runtime: inMemory.client,
      });
      const raw = yield* makeRawRpcClient(server.url);

      const error = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "badjson",
          query: { select: ["id"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(error.code).toBe("InvalidRow");
      expect(error.message).toMatch(/Field id is not JSON-safe/);

      yield* raw.close;
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("round-trips BigInt rows and filters through the RPC NDJSON transport", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      const subscription = yield* client.subscribe("trades", {
        where: {
          quantity: { gte: 10n },
        },
        select: ["id", "quantity"],
        orderBy: [{ field: "quantity", direction: "asc" }],
        limit: 10,
      });
      const eventsFiber = yield* subscription.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.sleep("10 millis");

      yield* inMemory.client.publish("trades", trade("a", 5n));
      yield* inMemory.client.publish("trades", trade("b", 10n));

      const events = yield* Fiber.join(eventsFiber);
      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "trades",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(events[1]).toStrictEqual({
        type: "delta",
        topic: "trades",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 2,
        operations: [{ type: "insert", key: "b", row: { id: "b", quantity: 10n }, index: 0 }],
        totalRows: 1,
      });

      yield* Effect.sleep("10 millis");
      yield* client.close;
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("round-trips BigDecimal rows and filters through the RPC NDJSON transport", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      const subscription = yield* client.subscribe("quotes", {
        where: {
          price: { gte: fromStringUnsafe("10.50") },
        },
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const eventsFiber = yield* subscription.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.sleep("10 millis");

      yield* inMemory.client.publish("quotes", quote("a", "9.99"));
      yield* inMemory.client.publish("quotes", quote("b", "10.50"));

      const events = yield* Fiber.join(eventsFiber);
      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "quotes",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(events[1]).toStrictEqual({
        type: "delta",
        topic: "quotes",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 2,
        operations: [
          {
            type: "insert",
            key: "b",
            row: { id: "b", price: fromStringUnsafe("10.5") },
            index: 0,
          },
        ],
        totalRows: 1,
      });

      yield* Effect.sleep("10 millis");
      yield* client.close;
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("encodes snapshot rows, move/remove deltas, and close statuses", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });
      const client = yield* makeViewServerClient(viewServer, { url: server.url });

      yield* inMemory.client.publishMany("orders", [order("a", 10), order("b", 20)]);
      const subscription = yield* client.subscribe("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const eventsFiber = yield* subscription.events.pipe(
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.sleep("10 millis");

      yield* inMemory.client.patch("orders", "a", { price: 30 });
      yield* inMemory.client.delete("orders", "b");

      const events = yield* Fiber.join(eventsFiber);
      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["a", "b"],
        rows: [
          { id: "a", price: 10 },
          { id: "b", price: 20 },
        ],
        totalRows: 2,
      });
      expect(events[1]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          { type: "move", key: "a", fromIndex: 0, toIndex: 1 },
          { type: "update", key: "a", row: { id: "a", price: 30 }, index: 1 },
        ],
        totalRows: 2,
      });
      expect(events[2]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 2,
        toVersion: 3,
        operations: [{ type: "remove", key: "b" }],
        totalRows: 1,
      });

      yield* client.close;
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("encodes subscription closed status when the runtime closes", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      const subscription = yield* client.subscribe("orders", {
        select: ["id"],
        limit: 10,
      });
      const eventsFiber = yield* subscription.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.sleep("10 millis");

      yield* inMemory.close;

      const events = yield* Fiber.join(eventsFiber);
      expect(events[1]).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "closed",
        code: "SubscriptionClosed",
        message: "Subscription closed because the engine closed.",
      });

      yield* client.close;
      yield* server.close;
    }),
  );

  it.live("serves health from the runtime instead of stale live-client state", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const baseHealth = yield* inMemory.client.health();
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          ...inMemory.client,
          health: () =>
            Effect.succeed({
              ...baseHealth,
              engine: {
                topics: {
                  ...baseHealth.engine.topics,
                  orders: {
                    ...baseHealth.engine.topics.orders,
                    rowCount: 123,
                  },
                },
              },
            }),
        },
      });
      const client = yield* makeViewServerClient(viewServer, { url: server.url });

      expect(client.health.value.engine.topics.orders.rowCount).toBe(123);

      yield* client.close;
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("rejects semantically invalid runtime health over unary RPC", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const baseHealth = yield* inMemory.client.health();
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          ...inMemory.client,
          health: () =>
            Effect.succeed({
              ...baseHealth,
              kafka: {
                startFrom: kafkaStartFromHealth,
                regions: {},
                topics: {
                  source_orders: {
                    status: "ready",
                    sourceTopic: "source_orders",
                    viewServerTopic: "missing",
                    regions: {},
                  },
                },
              },
            }),
        },
      });
      const raw = yield* makeRawRpcClient(server.url);

      const invalidHealth = yield* Effect.flip(raw.rpc["ViewServer.Health"]()).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)),
      );
      expect(invalidHealth.code).toBe("InvalidRow");
      expect(invalidHealth.message).toBe("Health payload references unknown topic: missing");

      yield* raw.close;
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("closes in-flight remote RPC health reads when the websocket server closes", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const readStarted = yield* Deferred.make<void>();
      const readInterrupted = yield* Deferred.make<void>();
      let readCount = 0;
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          health: () =>
            Effect.sync(() => {
              readCount += 1;
            }).pipe(
              Effect.andThen(Deferred.succeed(readStarted, undefined)),
              Effect.andThen(Effect.never),
              Effect.ensuring(Deferred.succeed(readInterrupted, undefined)),
            ),
        },
      });
      const raw = yield* makeRawRpcClient(server.url);

      const first = yield* raw.rpc["ViewServer.Health"]().pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(readStarted);
      const second = yield* raw.rpc["ViewServer.Health"]().pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Effect.yieldNow;
      yield* server.close.pipe(Effect.timeout("1 second"));
      yield* Deferred.await(readInterrupted).pipe(Effect.timeout("1 second"));
      const [firstExit, secondExit] = yield* Effect.all([Fiber.join(first), Fiber.join(second)], {
        concurrency: 2,
      }).pipe(Effect.timeout("1 second"));

      expect(readCount).toBe(1);
      expect(Exit.isFailure(firstExit)).toBe(true);
      expect(Exit.isFailure(secondExit)).toBe(true);
      yield* raw.close;
      yield* inMemory.close;
    }),
  );

  it.live("coalesces concurrent RPC health reads while an active read is running", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const baseHealth = yield* inMemory.client.health();
      const firstHealth = serverHealthWithOrdersRowCount(baseHealth, 1);
      const secondHealth = serverHealthWithOrdersRowCount(baseHealth, 2);
      const readStarted = yield* Deferred.make<void>();
      const releaseRead = yield* Deferred.make<void>();
      let readCount = 0;
      const healthReads = new Map<
        number,
        Effect.Effect<ViewServerHealth<typeof viewServer.topics>>
      >([
        [
          0,
          Effect.gen(function* () {
            yield* Deferred.succeed(readStarted, undefined);
            yield* Deferred.await(releaseRead);
            return firstHealth;
          }),
        ],
        [1, Effect.succeed(secondHealth)],
      ]);
      const handlerScope = yield* Scope.make("parallel");
      const handlers = makeViewServerRpcHandlers(
        viewServer,
        {
          liveClient: inMemory.liveClient,
          runtime: {
            health: () =>
              Effect.suspend(() => {
                const nextRead =
                  healthReads.get(readCount) ??
                  Effect.die(new Error(`Unexpected health read: ${readCount}`));
                readCount += 1;
                return nextRead;
              }),
          },
        },
        handlerScope,
      );

      const first = yield* handlers["ViewServer.Health"]().pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(readStarted);
      const second = yield* handlers["ViewServer.Health"]().pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.succeed(releaseRead, undefined);

      const [firstResult, secondResult] = yield* Effect.all(
        [Fiber.join(first), Fiber.join(second)],
        { concurrency: 2 },
      ).pipe(Effect.timeout("1 second"));
      const thirdResult = yield* handlers["ViewServer.Health"]();

      expect(readCount).toBe(2);
      expect(firstResult).toStrictEqual(firstHealth);
      expect(secondResult).toStrictEqual(firstHealth);
      expect(thirdResult).toStrictEqual(secondHealth);
      yield* Scope.close(handlerScope, Exit.void);
      yield* inMemory.close;
    }),
  );

  it.live("clears failed RPC health reads so later callers retry", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const baseHealth = yield* inMemory.client.health();
      const recoveredHealth = serverHealthWithOrdersRowCount(baseHealth, 3);
      const healthError: ViewServerRuntimeError = {
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        message: "health unavailable",
      };
      let readCount = 0;
      const healthReads = new Map<
        number,
        Effect.Effect<ViewServerHealth<typeof viewServer.topics>, ViewServerRuntimeError>
      >([
        [0, Effect.fail(healthError)],
        [1, Effect.succeed(recoveredHealth)],
      ]);
      const handlerScope = yield* Scope.make("parallel");
      const handlers = makeViewServerRpcHandlers(
        viewServer,
        {
          liveClient: inMemory.liveClient,
          runtime: {
            health: () =>
              Effect.suspend(() => {
                const nextRead =
                  healthReads.get(readCount) ??
                  Effect.die(new Error(`Unexpected health read: ${readCount}`));
                readCount += 1;
                return nextRead;
              }),
          },
        },
        handlerScope,
      );

      const failedHealth = yield* Effect.flip(handlers["ViewServer.Health"]());
      const retriedHealth = yield* handlers["ViewServer.Health"]();

      expect(readCount).toBe(2);
      expect(failedHealth).toStrictEqual(healthError);
      expect(retriedHealth).toStrictEqual(recoveredHealth);
      yield* Scope.close(handlerScope, Exit.void);
      yield* inMemory.close;
    }),
  );

  it.live("keeps shared RPC health reads alive when the leader caller is interrupted", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const baseHealth = yield* inMemory.client.health();
      const firstHealth = serverHealthWithOrdersRowCount(baseHealth, 3);
      const recoveredHealth = serverHealthWithOrdersRowCount(baseHealth, 4);
      const readStarted = yield* Deferred.make<void>();
      const releaseRead = yield* Deferred.make<void>();
      let readCount = 0;
      const healthReads = new Map<
        number,
        Effect.Effect<ViewServerHealth<typeof viewServer.topics>, ViewServerRuntimeError>
      >([
        [
          0,
          Effect.gen(function* () {
            readCount += 1;
            yield* Deferred.succeed(readStarted, undefined);
            yield* Deferred.await(releaseRead);
            return firstHealth;
          }),
        ],
        [
          1,
          Effect.sync(() => {
            readCount += 1;
            return recoveredHealth;
          }),
        ],
      ]);
      const handlerScope = yield* Scope.make("parallel");
      const handlers = makeViewServerRpcHandlers(
        viewServer,
        {
          liveClient: inMemory.liveClient,
          runtime: {
            health: () =>
              Effect.suspend(
                () =>
                  healthReads.get(readCount) ??
                  Effect.die(new Error(`Unexpected health read: ${readCount}`)),
              ),
          },
        },
        handlerScope,
      );

      const leader = yield* handlers["ViewServer.Health"]().pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(readStarted);
      const follower = yield* handlers["ViewServer.Health"]().pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Fiber.interrupt(leader).pipe(
        Effect.timeout("1 second"),
        Effect.onError(() => Deferred.succeed(releaseRead, undefined).pipe(Effect.asVoid)),
      );
      const leaderExit = yield* Fiber.await(leader).pipe(Effect.timeout("1 second"));
      yield* Deferred.succeed(releaseRead, undefined);
      const followerHealth = yield* Fiber.join(follower).pipe(Effect.timeout("1 second"));
      const retriedHealth = yield* handlers["ViewServer.Health"]();

      expect(followerHealth).toStrictEqual(firstHealth);
      expect(Exit.hasInterrupts(leaderExit)).toBe(true);
      expect(readCount).toBe(2);
      expect(retriedHealth).toStrictEqual(recoveredHealth);
      yield* Scope.close(handlerScope, Exit.void);
      yield* inMemory.close;
    }),
  );

  it.live("interrupts active RPC health reads when the handler scope closes", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const readStarted = yield* Deferred.make<void>();
      const readInterrupted = yield* Deferred.make<void>();
      const handlerScope = yield* Scope.make("parallel");
      const handlers = makeViewServerRpcHandlers(
        viewServer,
        {
          liveClient: inMemory.liveClient,
          runtime: {
            health: () =>
              Deferred.succeed(readStarted, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(Deferred.succeed(readInterrupted, undefined)),
              ),
          },
        },
        handlerScope,
      );

      const healthFiber = yield* handlers["ViewServer.Health"]().pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(readStarted);
      yield* Scope.close(handlerScope, Exit.void).pipe(Effect.timeout("1 second"));
      yield* Deferred.await(readInterrupted).pipe(Effect.timeout("1 second"));
      const healthExit = yield* Fiber.await(healthFiber).pipe(Effect.timeout("1 second"));

      expect(Exit.hasInterrupts(healthExit)).toBe(true);
      yield* inMemory.close;
    }),
  );

  it.live(
    "does not wait forever when closing handler scope with a non-cancellable active RPC health worker",
    () =>
      Effect.gen(function* () {
        const inMemory = createServerTestRuntime(viewServer);
        const baseHealth = yield* inMemory.client.health();
        const readStarted = yield* Deferred.make<void>();
        const releaseRead = yield* Deferred.make<void>();
        const workerReadFinished = yield* Deferred.make<void>();
        const handlerScope = yield* Scope.make("parallel");
        const handlers = makeViewServerRpcHandlers(
          viewServer,
          {
            liveClient: inMemory.liveClient,
            runtime: {
              health: () =>
                Effect.uninterruptible(
                  Deferred.succeed(readStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseRead)),
                    Effect.as(baseHealth),
                    Effect.ensuring(Deferred.succeed(workerReadFinished, undefined)),
                  ),
                ),
            },
          },
          handlerScope,
        );

        const healthFiber = yield* handlers["ViewServer.Health"]().pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Deferred.await(readStarted);
        yield* Scope.close(handlerScope, Exit.void).pipe(Effect.timeout("1 second"));
        const healthExit = yield* Fiber.await(healthFiber).pipe(Effect.timeout("1 second"));
        yield* Deferred.succeed(releaseRead, undefined);
        yield* Deferred.await(workerReadFinished).pipe(Effect.timeout("1 second"));
        yield* Effect.yieldNow;

        expect(Exit.hasInterrupts(healthExit)).toBe(true);
        yield* inMemory.close;
      }),
  );

  it.live("interrupts RPC health reads started after the handler scope closes", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const handlerScope = yield* Scope.make("parallel");
      const handlers = makeViewServerRpcHandlers(
        viewServer,
        {
          liveClient: inMemory.liveClient,
          runtime: inMemory.client,
        },
        handlerScope,
      );

      yield* Scope.close(handlerScope, Exit.void);
      const healthExit = yield* handlers["ViewServer.Health"]().pipe(Effect.exit);

      expect(Exit.hasInterrupts(healthExit)).toBe(true);
      yield* inMemory.close;
    }),
  );

  it.live("serves custom paths and maps hostile remote inputs", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const server = yield* makeViewServerWebSocketServer(
        viewServer,
        {
          liveClient: inMemory.liveClient,
          runtime: inMemory.client,
        },
        { host: "127.0.0.1", path: "/custom-rpc" },
      );
      const client = yield* makeViewServerClient(viewServer, { url: server.url });

      const invalidSubscribeTopic = yield* Effect.flip(
        // @ts-expect-error hostile callers can still send unknown topics over the wire.
        client.subscribe("missing", {
          select: ["id"],
        }),
      );
      expect(invalidSubscribeTopic.code).toBe("InvalidTopic");

      const invalidQuery = yield* Effect.flip(
        client.subscribe("orders", {
          // @ts-expect-error hostile callers can still send malformed queries over the wire.
          select: [1],
        }),
      );
      expect(invalidQuery.code).toBe("InvalidQuery");
      expect(invalidQuery.message).toBe('Expected string, got 1\n  at ["select"][0]');

      const unknownSelect = yield* Effect.flip(
        client.subscribe("orders", {
          // @ts-expect-error hostile callers can still send unknown projected fields.
          select: ["missing"],
        }),
      );
      expect(unknownSelect.code).toBe("InvalidQuery");
      expect(unknownSelect.message).toBe("Query references an unknown field for topic: orders");

      const unknownWhere = yield* Effect.flip(
        client.subscribe("orders", {
          // @ts-expect-error invalid query collapse keeps selected fields from being accepted.
          select: ["id"],
          where: {
            // @ts-expect-error hostile callers can still send unknown filter fields.
            missing: { eq: "x" },
          },
        }),
      );
      expect(unknownWhere.code).toBe("InvalidQuery");
      expect(unknownWhere.message).toBe("Query references an unknown field for topic: orders");

      const unknownOrderBy = yield* Effect.flip(
        client.subscribe("orders", {
          // @ts-expect-error invalid query collapse keeps selected fields from being accepted.
          select: ["id"],
          orderBy: [
            {
              // @ts-expect-error hostile callers can still send unknown sort fields.
              field: "missing",
              direction: "asc",
            },
          ],
        }),
      );
      expect(unknownOrderBy.code).toBe("InvalidQuery");
      expect(unknownOrderBy.message).toBe("Query references an unknown field for topic: orders");

      yield* client.close;
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("cleans up engine subscribers when the remote websocket disconnects", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      const subscription = yield* client.subscribe("orders", {
        select: ["id", "price"],
        limit: 10,
      });
      const eventsFiber = yield* subscription.events.pipe(Stream.runDrain, Effect.forkChild);

      yield* Effect.sleep("10 millis");
      const beforeDisconnect = yield* inMemory.client.health();
      expect(beforeDisconnect.engine.topics.orders.activeSubscriptions).toBe(1);

      yield* client.close;
      yield* Effect.sleep("10 millis");

      const afterDisconnect = yield* inMemory.client.health();
      expect(afterDisconnect.engine.topics.orders.activeSubscriptions).toBe(0);

      yield* Fiber.interrupt(eventsFiber);
      yield* server.close;
      yield* inMemory.close;
    }),
  );
});
