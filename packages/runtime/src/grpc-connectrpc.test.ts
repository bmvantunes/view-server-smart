import { create, toBinary } from "@bufbuild/protobuf";
import type { Message } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { createClient } from "@connectrpc/connect";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { applyEvent, initialClientState, liveQueryResult } from "@effect-view-server/client";
import type { ViewServerLiveSubscription } from "@effect-view-server/client";
import {
  defineViewServerConfig,
  grpc,
  type LiveQueryResult,
  type ViewServerRuntimeClient,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Option, Schema, Stream } from "effect";
import * as Http2 from "node:http2";
import { makeViewServerRuntime } from "./index";

type ConnectOrderRequestMessage = Message<"viewserver.runtime.connect.OrderRequest"> & {
  readonly region: string;
};

type ConnectOrderEventMessage = Message<"viewserver.runtime.connect.OrderEvent"> & {
  readonly orderId: string;
  readonly customerId: string;
  readonly status: "open" | "closed";
  readonly price: number;
  readonly region: string;
  readonly updatedAt: number;
};

type ConnectRuntimeOrderRow = {
  readonly id: string;
  readonly customerId: string;
  readonly status: "open" | "closed";
  readonly price: number;
  readonly region: string;
  readonly updatedAt: number;
};

type ConnectRuntimeLiveRow = Pick<ConnectRuntimeOrderRow, "id" | "price" | "region">;

class ConnectRpcIntegrationError extends Schema.TaggedErrorClass<ConnectRpcIntegrationError>()(
  "ConnectRpcIntegrationError",
  {
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Unknown),
  },
) {}

const base64FromBytes = (bytes: Uint8Array): string =>
  globalThis.btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(""));

const connectRuntimeProtoFile = fileDesc(
  base64FromBytes(
    toBinary(
      FileDescriptorProtoSchema,
      create(FileDescriptorProtoSchema, {
        name: "viewserver/runtime/connect.proto",
        package: "viewserver.runtime.connect",
        syntax: "proto3",
        messageType: [
          {
            name: "OrderRequest",
            field: [{ name: "region", number: 1, type: FieldDescriptorProto_Type.STRING }],
          },
          {
            name: "OrderEvent",
            field: [
              { name: "order_id", number: 1, type: FieldDescriptorProto_Type.STRING },
              { name: "customer_id", number: 2, type: FieldDescriptorProto_Type.STRING },
              { name: "status", number: 3, type: FieldDescriptorProto_Type.STRING },
              { name: "price", number: 4, type: FieldDescriptorProto_Type.DOUBLE },
              { name: "region", number: 5, type: FieldDescriptorProto_Type.STRING },
              { name: "updated_at", number: 6, type: FieldDescriptorProto_Type.DOUBLE },
            ],
          },
        ],
        service: [
          {
            name: "OrdersService",
            method: [
              {
                name: "StreamOrders",
                inputType: ".viewserver.runtime.connect.OrderRequest",
                outputType: ".viewserver.runtime.connect.OrderEvent",
                serverStreaming: true,
              },
            ],
          },
        ],
      }),
    ),
  ),
);

const ConnectOrderRequestSchema = messageDesc<ConnectOrderRequestMessage>(
  connectRuntimeProtoFile,
  0,
);
const ConnectOrderEventSchema = messageDesc<ConnectOrderEventMessage>(connectRuntimeProtoFile, 1);
const ConnectOrdersService = serviceDesc<{
  readonly streamOrders: {
    readonly input: typeof ConnectOrderRequestSchema;
    readonly output: typeof ConnectOrderEventSchema;
    readonly methodKind: "server_streaming";
  };
}>(connectRuntimeProtoFile, 0);

const ConnectOrder = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const materializedViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: ConnectOrder,
      key: "id",
      source: grpc.materialized(),
    },
  },
});

const leasedViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: ConnectOrder,
      key: "id",
      source: grpc.leased({
        routeBy: ["region"],
      }),
    },
  },
});

const orderEvent = (input: ConnectRuntimeOrderRow): ConnectOrderEventMessage =>
  create(ConnectOrderEventSchema, {
    orderId: input.id,
    customerId: input.customerId,
    status: input.status,
    price: input.price,
    region: input.region,
    updatedAt: input.updatedAt,
  });

const connectRowsForRegion = (region: string): ReadonlyArray<ConnectOrderEventMessage> =>
  region === "all"
    ? [
        orderEvent({
          id: "order-all-1",
          customerId: "customer-1",
          status: "open",
          price: 10,
          region: "usa",
          updatedAt: 1,
        }),
        orderEvent({
          id: "order-all-2",
          customerId: "customer-2",
          status: "closed",
          price: 20,
          region: "london",
          updatedAt: 2,
        }),
      ]
    : [
        orderEvent({
          id: `order-${region}-1`,
          customerId: `${region}-customer-1`,
          status: "open",
          price: 10,
          region,
          updatedAt: 1,
        }),
        orderEvent({
          id: `order-${region}-2`,
          customerId: `${region}-customer-2`,
          status: "open",
          price: 20,
          region,
          updatedAt: 2,
        }),
      ];

const waitForAbort = (signal: AbortSignal): Promise<void> =>
  signal.aborted
    ? Promise.resolve()
    : new Promise((resolve) => {
        signal.addEventListener("abort", () => resolve(), { once: true });
      });

const makeConnectRpcServer = Effect.fn("ViewServerRuntime.grpc.connectRpc.server.make")(
  function* () {
    const requestsByRegion = new Map<string, number>();
    const sessions = new Set<Http2.ServerHttp2Session>();
    const server = yield* Effect.acquireRelease(
      Effect.callback<Http2.Http2Server, ConnectRpcIntegrationError>((resume) => {
        const http2Server = Http2.createServer(
          connectNodeAdapter({
            routes: (router) =>
              router.service(ConnectOrdersService, {
                streamOrders: async function* (request, context) {
                  requestsByRegion.set(
                    request.region,
                    (requestsByRegion.get(request.region) ?? 0) + 1,
                  );
                  for (const row of connectRowsForRegion(request.region)) {
                    yield row;
                  }
                  await waitForAbort(context.signal);
                },
              }),
          }),
        );
        http2Server.on("session", (session) => {
          sessions.add(session);
          session.once("close", () => {
            sessions.delete(session);
          });
        });
        const onError = (cause: unknown) => {
          http2Server.off("error", onError);
          resume(
            Effect.fail(
              new ConnectRpcIntegrationError({
                message: "ConnectRPC test server failed to start.",
                cause,
              }),
            ),
          );
        };
        http2Server.once("error", onError);
        http2Server.listen(0, "127.0.0.1", () => {
          http2Server.off("error", onError);
          resume(Effect.succeed(http2Server));
        });
      }),
      (server) =>
        Effect.callback<void>((resume) => {
          for (const session of sessions) {
            session.close();
            session.destroy();
          }
          server.close(() => resume(Effect.void));
        }),
    );
    const address = server.address();
    if (typeof address !== "object" || address === null || typeof address.port !== "number") {
      return yield* new ConnectRpcIntegrationError({
        message: "ConnectRPC test server did not expose a TCP port.",
        cause: address,
      });
    }
    return {
      baseUrl: `http://127.0.0.1:${address.port}`,
      requestsByRegion,
    };
  },
);

const mapOrderEvent = (value: ConnectOrderEventMessage): ConnectRuntimeOrderRow => ({
  id: value.orderId,
  customerId: value.customerId,
  status: value.status,
  price: value.price,
  region: value.region,
  updatedAt: value.updatedAt,
});

const readDirectConnectRows = Effect.fn("ViewServerRuntime.grpc.connectRpc.direct.read")(function* (
  baseUrl: string,
  region: string,
) {
  const client = createClient(
    ConnectOrdersService,
    createGrpcTransport({
      baseUrl,
    }),
  );
  const rows = yield* Effect.tryPromise({
    try: async () => {
      const collected: Array<ConnectRuntimeOrderRow> = [];
      for await (const row of client.streamOrders({ region })) {
        collected.push(mapOrderEvent(row));
        if (collected.length === 2) {
          break;
        }
      }
      return collected;
    },
    catch: (cause) =>
      new ConnectRpcIntegrationError({
        message: "Direct ConnectRPC client did not read expected rows.",
        cause,
      }),
  }).pipe(Effect.timeout("2 seconds"));
  return yield* rows === undefined
    ? Effect.fail(
        new ConnectRpcIntegrationError({
          message: "Direct ConnectRPC client timed out.",
        }),
      )
    : Effect.succeed(rows);
});

const waitForMaterializedSnapshot = Effect.fn(
  "ViewServerRuntime.grpc.connectRpc.materialized.snapshot.wait",
)(function* (
  client: Pick<ViewServerRuntimeClient<typeof materializedViewServer.topics>, "snapshot">,
  expectedTotalRows: number,
) {
  const poll = (
    remainingAttempts: number,
  ): Effect.Effect<
    LiveQueryResult<Pick<ConnectRuntimeOrderRow, "id" | "price" | "region">>,
    ConnectRpcIntegrationError | ViewServerRuntimeError
  > =>
    client
      .snapshot("orders", {
        select: ["id", "price", "region"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      })
      .pipe(
        Effect.flatMap((snapshot) => {
          if (snapshot.totalRows === expectedTotalRows) {
            return Effect.succeed(snapshot);
          }
          if (remainingAttempts === 0) {
            return Effect.fail(
              new ConnectRpcIntegrationError({
                message: `Materialized ConnectRPC snapshot did not reach ${expectedTotalRows} rows.`,
              }),
            );
          }
          return Effect.sleep("5 millis").pipe(Effect.andThen(poll(remainingAttempts - 1)));
        }),
      );
  return yield* poll(100);
});

const waitForLiveQueryResult = Effect.fn("ViewServerRuntime.grpc.connectRpc.liveResult.wait")(
  function* (
    subscription: ViewServerLiveSubscription<ConnectRuntimeLiveRow>,
    expectedTotalRows: number,
  ) {
    let state = initialClientState<ConnectRuntimeLiveRow>();
    const result = yield* subscription.events.pipe(
      Stream.map((event) => {
        state = applyEvent(state, event);
        return liveQueryResult(state);
      }),
      Stream.filter((next) => next.totalRows === expectedTotalRows && next.status === "ready"),
      Stream.runHead,
      Effect.timeout("2 seconds"),
    );
    if (result === undefined) {
      return yield* new ConnectRpcIntegrationError({
        message: `Leased ConnectRPC subscription did not reach ${expectedTotalRows} rows.`,
      });
    }
    return yield* Option.match(result, {
      onNone: () =>
        Effect.fail(
          new ConnectRpcIntegrationError({
            message: `Leased ConnectRPC subscription ended before ${expectedTotalRows} rows.`,
          }),
        ),
      onSome: Effect.succeed,
    });
  },
);

const closeWithin = Effect.fn("ViewServerRuntime.grpc.connectRpc.closeWithin")(function* (
  close: Effect.Effect<void>,
  message: string,
) {
  const closeResult = yield* close.pipe(Effect.as("closed"), Effect.timeout("2 seconds"));
  if (closeResult === undefined) {
    return yield* new ConnectRpcIntegrationError({ message });
  }
});

describe("@effect-view-server/runtime ConnectRPC gRPC integration", () => {
  it.live("reads rows through a direct ConnectRPC gRPC client", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const connectServer = yield* makeConnectRpcServer();
        const rows = yield* readDirectConnectRows(connectServer.baseUrl, "all");
        expect(rows).toStrictEqual([
          {
            id: "order-all-1",
            customerId: "customer-1",
            status: "open",
            price: 10,
            region: "usa",
            updatedAt: 1,
          },
          {
            id: "order-all-2",
            customerId: "customer-2",
            status: "closed",
            price: 20,
            region: "london",
            updatedAt: 2,
          },
        ]);
      }),
    ),
  );

  it.live("ingests materialized feeds through the configured ConnectRPC gRPC client path", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const connectServer = yield* makeConnectRpcServer();
        const grpcClients = {
          orders: grpc.connectClient({
            service: ConnectOrdersService,
            baseUrl: connectServer.baseUrl,
          }),
        };
        const grpcFeed = materializedViewServer.grpcFeed<typeof grpcClients>();
        const upstreamAbort = new AbortController();
        let releaseCount = 0;
        const runtime = yield* Effect.acquireRelease(
          makeViewServerRuntime(materializedViewServer, {
            websocketPort: 0,
            grpc: {
              clients: grpcClients,
              feeds: {
                ordersFeed: grpcFeed.materializedFeed({
                  topic: "orders",
                  client: "orders",
                  method: "streamOrders",
                  request: () => ({ region: "all" }),
                  acquire: ({ client, request }) =>
                    Stream.fromAsyncIterable(
                      client.streamOrders(request, {
                        signal: upstreamAbort.signal,
                      }),
                      (cause) => cause,
                    ),
                  release: () =>
                    Effect.sync(() => {
                      releaseCount += 1;
                      upstreamAbort.abort();
                    }),
                  map: ({ value }) => mapOrderEvent(value),
                }),
              },
            },
          }),
          (runtime) => runtime.close,
        );

        const snapshot = yield* waitForMaterializedSnapshot(runtime.client, 2);
        expect({
          rows: snapshot.rows,
          totalRows: snapshot.totalRows,
          status: snapshot.status,
          statusCode: snapshot.statusCode,
        }).toStrictEqual({
          rows: [
            { id: "order-all-1", price: 10, region: "usa" },
            { id: "order-all-2", price: 20, region: "london" },
          ],
          totalRows: 2,
          status: "ready",
          statusCode: "Ready",
        });
        expect(snapshot.version).toBeGreaterThan(0);
        expect(connectServer.requestsByRegion.get("all")).toBe(1);
        yield* closeWithin(runtime.close, "ConnectRPC materialized runtime did not close.");
        expect(releaseCount).toBe(1);
        expect(upstreamAbort.signal.aborted).toBe(true);
      }),
    ),
  );

  it.live("shares leased feeds through the configured ConnectRPC gRPC client path", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const connectServer = yield* makeConnectRpcServer();
        const grpcClients = {
          orders: grpc.connectClient({
            service: ConnectOrdersService,
            baseUrl: connectServer.baseUrl,
          }),
        };
        const grpcFeed = leasedViewServer.grpcFeed<typeof grpcClients>();
        const upstreamAbort = new AbortController();
        let releaseCount = 0;
        const runtime = yield* Effect.acquireRelease(
          makeViewServerRuntime(leasedViewServer, {
            websocketPort: 0,
            grpc: {
              clients: grpcClients,
              feeds: {
                ordersByRegion: grpcFeed.leasedFeed({
                  topic: "orders",
                  client: "orders",
                  method: "streamOrders",
                  routeBy: ["region"],
                  request: ({ region }) => ({ region }),
                  acquire: ({ client, request }) =>
                    Stream.fromAsyncIterable(
                      client.streamOrders(request, {
                        signal: upstreamAbort.signal,
                      }),
                      (cause) => cause,
                    ),
                  release: () =>
                    Effect.sync(() => {
                      releaseCount += 1;
                      upstreamAbort.abort();
                    }),
                  map: ({ value }) => mapOrderEvent(value),
                }),
              },
            },
          }),
          (runtime) => runtime.close,
        );
        const firstSubscription = yield* Effect.acquireRelease(
          runtime.liveClient.subscribe("orders", {
            select: ["id", "price", "region"],
            where: {
              region: { eq: "usa" },
            },
            orderBy: [{ field: "price", direction: "asc" }],
            limit: 10,
          }),
          (subscription) => subscription.close().pipe(Effect.orDie),
        );
        const secondSubscription = yield* Effect.acquireRelease(
          runtime.liveClient.subscribe("orders", {
            select: ["id", "price", "region"],
            where: {
              region: { eq: "usa" },
              price: { gte: 20 },
            },
            orderBy: [{ field: "price", direction: "asc" }],
            limit: 10,
          }),
          (subscription) => subscription.close().pipe(Effect.orDie),
        );

        const [firstResult, secondResult] = yield* Effect.all(
          [
            waitForLiveQueryResult(firstSubscription, 2),
            waitForLiveQueryResult(secondSubscription, 1),
          ],
          {
            concurrency: "unbounded",
          },
        );
        expect({
          rows: firstResult.rows,
          totalRows: firstResult.totalRows,
          status: firstResult.status,
          statusCode: firstResult.statusCode,
          message: firstResult.message,
        }).toStrictEqual({
          rows: [
            { id: "order-usa-1", price: 10, region: "usa" },
            { id: "order-usa-2", price: 20, region: "usa" },
          ],
          totalRows: 2,
          status: "ready",
          statusCode: "Ready",
          message: undefined,
        });
        expect(firstResult.version).toBeGreaterThan(0);
        expect({
          rows: secondResult.rows,
          totalRows: secondResult.totalRows,
          status: secondResult.status,
          statusCode: secondResult.statusCode,
          message: secondResult.message,
        }).toStrictEqual({
          rows: [{ id: "order-usa-2", price: 20, region: "usa" }],
          totalRows: 1,
          status: "ready",
          statusCode: "Ready",
          message: undefined,
        });
        expect(secondResult.version).toBeGreaterThan(0);
        expect(connectServer.requestsByRegion.get("usa")).toBe(1);
        yield* closeWithin(
          secondSubscription.close().pipe(Effect.orDie),
          "ConnectRPC second leased subscription did not close.",
        );
        yield* closeWithin(
          firstSubscription.close().pipe(Effect.orDie),
          "ConnectRPC first leased subscription did not close.",
        );
        expect(releaseCount).toBe(1);
        expect(upstreamAbort.signal.aborted).toBe(true);
      }),
    ),
  );
});
