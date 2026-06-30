import { describe, expect, it } from "@effect/vitest";
import { create, toBinary } from "@bufbuild/protobuf";
import type { Message } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import type { ColumnLiveViewEngineHealth } from "@effect-view-server/column-live-view-engine";
import { makeViewServerClient } from "@effect-view-server/client/remote";
import { ViewServerAuthError } from "@effect-view-server/server";
import {
  defineViewServerConfig,
  grpc,
  kafka,
  type TransportHealth,
  type ViewServerHealth,
  type ViewServerRuntimeError,
  type ViewServerRuntimeClient,
} from "@effect-view-server/config";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import type { ViewServerRuntimeCoreInternalLiveClient } from "@effect-view-server/runtime-core/internal";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import {
  Cause,
  Clock,
  Config,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Option,
  Queue,
  Schedule,
  Schema,
  Stream,
} from "effect";
import * as BigDecimal from "effect/BigDecimal";
import type { ViewServerRuntimeDependencies } from "./internal";
import {
  makeDefaultRuntimeDependencies,
  makeViewServerRuntimeWithDependencies,
  runViewServerRuntimeWithDependencies,
} from "./internal";
import { makeViewServerGrpcHealthLedger } from "./grpc-health";
import { makeViewServerGrpcIngress, ViewServerGrpcIngressError } from "./grpc-ingress";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";
import { makeViewServerRuntime, runViewServerRuntime } from "./index";
import { ViewServerKafkaIngressError } from "./kafka-ingress";
import {
  installTcpPublishAcceptedSocket,
  installTcpServerSteadyStateErrorHandler,
  makeViewServerTcpPublishIngress,
  rejectTcpSocketWhenClosed,
  tcpPublishUrl,
  ViewServerTcpPublishIngressError,
  writeTcpJsonLine,
} from "./tcp-publish-ingress";
import {
  resolveViewServerRuntimeOptions,
  validateGrpcSourceFeeds,
  validateSourceOwnership,
  type ResolvedViewServerGrpcRuntimeOptions,
  type ResolvedViewServerKafkaRuntimeOptions,
} from "./runtime-options";
import { makeViewServerRuntimeTransportHealth } from "./transport-health";
import * as Net from "node:net";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
});

const HealthJson = Schema.Struct({
  status: Schema.Literals(["ready", "degraded", "starting", "stopping"]),
  engine: Schema.Struct({
    topics: Schema.Struct({
      orders: Schema.Struct({
        rowCount: Schema.Number,
      }),
    }),
  }),
});

class RuntimeHealthJsonParseError extends Schema.TaggedErrorClass<RuntimeHealthJsonParseError>()(
  "RuntimeHealthJsonParseError",
  {
    cause: Schema.Unknown,
  },
) {}

class RuntimeJsonParseError extends Schema.TaggedErrorClass<RuntimeJsonParseError>()(
  "RuntimeJsonParseError",
  {
    cause: Schema.Unknown,
  },
) {}

class RuntimeTestFailure extends Schema.TaggedErrorClass<RuntimeTestFailure>()(
  "RuntimeTestFailure",
  {
    message: Schema.String,
  },
) {}

class RuntimeTcpTestFailure extends Schema.TaggedErrorClass<RuntimeTcpTestFailure>()(
  "RuntimeTcpTestFailure",
  {
    cause: Schema.Unknown,
    message: Schema.String,
  },
) {}

const TcpPublishResponse = Schema.Union([
  Schema.Struct({
    ok: Schema.Literal(true),
  }),
  Schema.Struct({
    ok: Schema.Literal(false),
    error: Schema.Struct({
      _tag: Schema.String,
      code: Schema.optional(Schema.String),
      message: Schema.String,
      phase: Schema.optional(Schema.String),
      status: Schema.optional(Schema.Number),
      topic: Schema.optional(Schema.String),
    }),
  }),
]);

type TcpPublishResponse = typeof TcpPublishResponse.Type;

const TestTcpAddress = Schema.Struct({
  address: Schema.String,
  family: Schema.String,
  port: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

const NestedTcpOrder = Schema.Struct({
  id: Schema.String,
  meta: Schema.Struct({
    desk: Schema.String,
  }),
});

const nestedTcpViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: NestedTcpOrder,
      key: "id",
    },
  },
});

const TransformTcpOrder = Schema.Struct({
  id: Schema.String,
  quantity: Schema.BigIntFromString,
});

const transformTcpViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: TransformTcpOrder,
      key: "id",
    },
  },
});

const JsonCodecTcpNested = Schema.Struct({
  encodedQuantity: Schema.BigIntFromString,
  runtimeAmount: Schema.BigDecimal,
  runtimeQuantity: Schema.optionalKey(Schema.BigInt),
});

const JsonCodecTcpOrder = Schema.Struct({
  allocations: Schema.Record(Schema.String, JsonCodecTcpNested).check(Schema.isMinProperties(1)),
  fills: Schema.Array(JsonCodecTcpNested).check(Schema.isMinLength(1)),
  id: Schema.String,
  amount: Schema.BigDecimal,
  checkedOptionalMeta: Schema.optionalKey(JsonCodecTcpNested).check(Schema.isMaxProperties(0)),
  checkedSuspendedEmptyMeta: Schema.optionalKey(
    Schema.suspend(() => Schema.Struct({}).check(Schema.isMaxProperties(0))),
  ),
  checkedSuspendedMeta: Schema.optionalKey(
    Schema.suspend(() => JsonCodecTcpNested.check(Schema.isMaxProperties(0))),
  ),
  meta: JsonCodecTcpNested,
  nullableMeta: Schema.NullOr(JsonCodecTcpNested),
  optionalMeta: Schema.optionalKey(JsonCodecTcpNested),
  optionalValueMeta: Schema.optional(JsonCodecTcpNested),
  quantity: Schema.BigInt,
  suspendedMeta: Schema.suspend(() => JsonCodecTcpNested),
  tuple: Schema.Tuple([JsonCodecTcpNested]),
  tupleRest: Schema.TupleWithRest(Schema.Tuple([JsonCodecTcpNested]), [JsonCodecTcpNested]),
  tupleRestTrailing: Schema.TupleWithRest(Schema.Tuple([JsonCodecTcpNested]), [
    JsonCodecTcpNested,
    JsonCodecTcpNested,
  ]),
  unionMeta: Schema.Union([JsonCodecTcpNested, Schema.Undefined]),
});

const jsonCodecTcpViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: JsonCodecTcpOrder,
      key: "id",
    },
  },
});

type JsonCodecTcpRecursiveNode = {
  readonly id: bigint;
  readonly amount: BigDecimal.BigDecimal;
  readonly runtimeQuantity: bigint;
  readonly child: JsonCodecTcpRecursiveNode | null;
};

const JsonCodecTcpRecursiveNode: Schema.Codec<JsonCodecTcpRecursiveNode, unknown, never, never> =
  Schema.suspend(
    (): Schema.Codec<JsonCodecTcpRecursiveNode, unknown, never, never> =>
      Schema.Struct({
        id: Schema.BigIntFromString,
        amount: Schema.BigDecimal,
        runtimeQuantity: Schema.BigInt,
        child: Schema.NullOr(JsonCodecTcpRecursiveNode),
      }),
  );
const JsonCodecTcpRecursiveOrder = Schema.Struct({
  id: Schema.String,
  node: JsonCodecTcpRecursiveNode,
});

const jsonCodecTcpRecursiveViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: JsonCodecTcpRecursiveOrder,
      key: "id",
    },
  },
});

type OrderRow = typeof Order.Type;

type GrpcOrderValueMessage = Message<"viewserver.runtime.OrderValue"> & {
  readonly customerId: string;
  readonly status: "open" | "closed" | "cancelled";
  readonly price: number;
  readonly updatedAt: number;
};

type GrpcOrderKeyMessage = Message<"viewserver.runtime.OrderKey"> & {
  readonly orderId: string;
};

const base64FromBytes = (bytes: Uint8Array) =>
  globalThis.btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(""));

const runtimeGrpcProtoFile = fileDesc(
  base64FromBytes(
    toBinary(
      FileDescriptorProtoSchema,
      create(FileDescriptorProtoSchema, {
        name: "viewserver/runtime.proto",
        package: "viewserver.runtime",
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
                inputType: ".viewserver.runtime.OrderKey",
                outputType: ".viewserver.runtime.OrderValue",
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

const GrpcOrder = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const grpcViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: GrpcOrder,
      key: "id",
      source: grpc.materialized(),
    },
  },
});

type GrpcTopics = typeof grpcViewServer.topics;

const grpcAndKafkaViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: GrpcOrder,
      key: "id",
      source: grpc.materialized(),
    },
    audit: {
      schema: Order,
      key: "id",
    },
  },
});

const leasedGrpcViewServer = defineViewServerConfig({
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

const PublicKeyGrpcOrder = Schema.Struct({
  id: Schema.String.pipe(Schema.check(Schema.isPattern(/^public-/))),
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const publicKeyLeasedGrpcViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: PublicKeyGrpcOrder,
      key: "id",
      source: grpc.leased({
        routeBy: ["region"],
      }),
    },
  },
});

const keyLeasedGrpcViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: GrpcOrder,
      key: "id",
      source: grpc.leased({
        routeBy: ["id"],
      }),
    },
  },
});

const RouteEncodingOrder = Schema.Struct({
  id: Schema.String,
  text: Schema.String,
  amount: Schema.BigDecimal,
  count: Schema.BigInt,
  disabled: Schema.Boolean,
  score: Schema.Number,
  flag: Schema.Boolean,
  none: Schema.Null,
  plainScore: Schema.Number,
  tags: Schema.Array(Schema.String),
  meta: Schema.Struct({
    desk: Schema.String,
  }),
  weird: Schema.Unknown,
});

const routeEncodingLeasedGrpcViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: RouteEncodingOrder,
      key: "id",
      source: grpc.leased({
        routeBy: [
          "amount",
          "count",
          "disabled",
          "flag",
          "meta",
          "none",
          "plainScore",
          "score",
          "tags",
          "text",
          "weird",
        ],
      }),
    },
  },
});

const groupedKeyEncodingLeasedGrpcViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: RouteEncodingOrder,
      key: "id",
      source: grpc.leased({
        routeBy: ["text"],
      }),
    },
  },
});

const grpcClients = {
  orders: grpc.connectClient({
    service: grpcOrdersService,
    baseUrl: Config.succeed("https://orders.example.test"),
  }),
};

const grpcClientsWithOrphan = {
  ...grpcClients,
  orphan: grpc.connectClient({
    service: grpcOrdersService,
    baseUrl: Config.succeed("https://orphan.example.test"),
  }),
};

const grpcFeed = grpcViewServer.grpcFeed<typeof grpcClients>();
const grpcFeedWithOrphan = grpcViewServer.grpcFeed<typeof grpcClientsWithOrphan>();
const mixedGrpcFeed = grpcAndKafkaViewServer.grpcFeed<typeof grpcClients>();
const leasedGrpcFeed = leasedGrpcViewServer.grpcFeed<typeof grpcClients>();
const publicKeyLeasedGrpcFeed = publicKeyLeasedGrpcViewServer.grpcFeed<typeof grpcClients>();
const keyLeasedGrpcFeed = keyLeasedGrpcViewServer.grpcFeed<typeof grpcClients>();
const routeEncodingLeasedGrpcFeed =
  routeEncodingLeasedGrpcViewServer.grpcFeed<typeof grpcClients>();
const groupedKeyEncodingLeasedGrpcFeed =
  groupedKeyEncodingLeasedGrpcViewServer.grpcFeed<typeof grpcClients>();

const grpcOrderValue = (
  customerId: string,
  price: number,
  status: GrpcOrderValueMessage["status"] = "open",
): GrpcOrderValueMessage => ({
  $typeName: "viewserver.runtime.OrderValue",
  customerId,
  status,
  price,
  updatedAt: price,
});

const grpcMaterializedFeed = (stream: Stream.Stream<GrpcOrderValueMessage, unknown, never>) =>
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

const grpcMaterializedFeedWithRelease = (
  stream: Stream.Stream<GrpcOrderValueMessage, unknown, never>,
  release: Effect.Effect<void>,
) =>
  grpcFeed.materializedFeed({
    topic: "orders",
    client: "orders",
    method: "streamOrders",
    request: () => ({ orderId: "all" }),
    acquire: () => stream,
    release: () => release,
    map: ({ value }) => ({
      id: value.customerId,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region: "usa",
      updatedAt: value.updatedAt,
    }),
  });

const grpcMaterializedFeedWithRequestFailure = () =>
  grpcFeed.materializedFeed({
    topic: "orders",
    client: "orders",
    method: "streamOrders",
    request: () => {
      throw new Error("request exploded");
    },
    acquire: () => Stream.never,
    map: ({ value }) => ({
      id: value.customerId,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region: "usa",
      updatedAt: value.updatedAt,
    }),
  });

const grpcMaterializedFeedWithAcquireFailure = () =>
  grpcFeed.materializedFeed({
    topic: "orders",
    client: "orders",
    method: "streamOrders",
    request: () => ({ orderId: "all" }),
    acquire: () => {
      throw new Error("acquire exploded");
    },
    map: ({ value }) => ({
      id: value.customerId,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region: "usa",
      updatedAt: value.updatedAt,
    }),
  });

const grpcMaterializedFeedWithMappingFailure = (
  stream: Stream.Stream<GrpcOrderValueMessage, unknown, never>,
) =>
  grpcFeed.materializedFeed({
    topic: "orders",
    client: "orders",
    method: "streamOrders",
    request: () => ({ orderId: "all" }),
    acquire: () => stream,
    map: () => {
      throw new Error("mapping exploded");
    },
  });

const grpcMaterializedFeedWithOrphanClient = () =>
  grpcFeedWithOrphan.materializedFeed({
    topic: "orders",
    client: "orphan",
    method: "streamOrders",
    request: () => ({ orderId: "all" }),
    acquire: () => Stream.never,
    map: ({ value }) => ({
      id: value.customerId,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region: "usa",
      updatedAt: value.updatedAt,
    }),
  });

const grpcLeasedFeed = (input: {
  readonly streamForRegion: (
    region: string,
  ) => Stream.Stream<GrpcOrderValueMessage, unknown, never>;
  readonly acquired?: (region: string) => void;
  readonly release?: Effect.Effect<void>;
}) =>
  leasedGrpcFeed.leasedFeed({
    topic: "orders",
    client: "orders",
    method: "streamOrders",
    routeBy: ["region"],
    request: ({ region }) => ({ orderId: region }),
    acquire: ({ route }) => {
      input.acquired?.(route.region);
      return input.streamForRegion(route.region);
    },
    release: () => input.release ?? Effect.void,
    map: ({ value, route }) => ({
      id: `${route.region}:${value.customerId}`,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region: route.region,
      updatedAt: value.updatedAt,
    }),
  });

const grpcPublicKeyLeasedFeed = (input: {
  readonly streamForRegion: (
    region: string,
  ) => Stream.Stream<GrpcOrderValueMessage, unknown, never>;
}) =>
  publicKeyLeasedGrpcFeed.leasedFeed({
    topic: "orders",
    client: "orders",
    method: "streamOrders",
    routeBy: ["region"],
    request: ({ region }) => ({ orderId: region }),
    acquire: ({ route }) => input.streamForRegion(route.region),
    map: ({ value, route }) => ({
      id: `public-${route.region}-${value.customerId}`,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region: route.region,
      updatedAt: value.updatedAt,
    }),
  });

const grpcKeyLeasedFeed = (input: {
  readonly streamForId: (id: string) => Stream.Stream<GrpcOrderValueMessage, unknown, never>;
}) =>
  keyLeasedGrpcFeed.leasedFeed({
    topic: "orders",
    client: "orders",
    method: "streamOrders",
    routeBy: ["id"],
    request: ({ id }) => ({ orderId: id }),
    acquire: ({ route }) => input.streamForId(route.id),
    map: ({ value, route }) => ({
      id: route.id,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region: "key-route",
      updatedAt: value.updatedAt,
    }),
  });

const routeEncodingValues = {
  amount: BigDecimal.fromStringUnsafe("123.45"),
  count: 9007199254740993n,
  disabled: false,
  flag: true,
  meta: {
    desk: "equities",
  },
  none: null,
  plainScore: 42,
  score: -0,
  tags: ["fast", "shared"],
  text: "route",
  weird: {
    alpha: "first",
    stable: "route",
  },
};

const grpcRouteEncodingLeasedFeed = () =>
  routeEncodingLeasedGrpcFeed.leasedFeed({
    topic: "orders",
    client: "orders",
    method: "streamOrders",
    routeBy: [
      "amount",
      "count",
      "disabled",
      "flag",
      "meta",
      "none",
      "plainScore",
      "score",
      "tags",
      "text",
      "weird",
    ],
    request: (route) => ({ orderId: String(route.text) }),
    acquire: () => Stream.never,
    map: () => ({
      id: "route-encoding",
      ...routeEncodingValues,
    }),
  });

const longRunningGrpcStream = (
  values: ReadonlyArray<GrpcOrderValueMessage>,
): Stream.Stream<GrpcOrderValueMessage, never, never> =>
  Stream.make(...values).pipe(Stream.concat(Stream.never));

const order = (id: string, price: number): OrderRow => ({
  id,
  price,
});

const bearerAuth = {
  validateRequest: (request: { readonly headers: Readonly<Record<string, string>> }) =>
    request.headers["authorization"] === "Bearer view-server-test"
      ? Effect.succeed({
          forwardedHeaders: {
            authorization: request.headers["authorization"],
          },
          id: "session-1",
          systemHeaders: {},
        })
      : Effect.fail(
          new ViewServerAuthError({
            message: "Missing or invalid authorization header.",
            status: 401,
          }),
        ),
};

const nullRecord = <Value>(
  entries: ReadonlyArray<readonly [string, Value]>,
): Record<string, Value> => {
  const record: Record<string, Value> = Object.create(null);
  for (const [key, value] of entries) {
    record[key] = value;
  }
  return record;
};

const fetchHealth = Effect.fn("ViewServerRuntime.test.health.fetch")(function* (url: string) {
  const response = yield* Effect.promise(() => fetch(url));
  const text = yield* Effect.promise(() => response.text());
  const value = yield* Effect.try({
    try: (): unknown => JSON.parse(text),
    catch: (cause) => new RuntimeHealthJsonParseError({ cause }),
  });
  const health = yield* Schema.decodeUnknownEffect(HealthJson)(value);
  return { response, health };
});

const fetchText = Effect.fn("ViewServerRuntime.test.text.fetch")(function* (url: string) {
  const response = yield* Effect.promise(() => fetch(url));
  const text = yield* Effect.promise(() => response.text());
  return { response, text };
});

const fetchJson = Effect.fn("ViewServerRuntime.test.json.fetch")(function* (url: string) {
  const response = yield* Effect.promise(() => fetch(url));
  const text = yield* Effect.promise(() => response.text());
  const value = yield* Effect.try({
    try: (): unknown => JSON.parse(text),
    catch: (cause) => new RuntimeJsonParseError({ cause }),
  });
  return { response, value };
});

const tcpUrlConnectHost = (hostname: string): string =>
  hostname.startsWith("[") && hostname.endsWith("]") ? hostname.slice(1, -1) : hostname;

const connectTcpPublishSocket = Effect.fn("ViewServerRuntime.test.tcp.connect")(function* (
  url: string,
) {
  const parsedUrl = new URL(url);
  const port = Number(parsedUrl.port);
  if (!Number.isSafeInteger(port)) {
    return yield* new RuntimeTcpTestFailure({
      message: "TCP publish URL did not include a valid port.",
      cause: url,
    });
  }
  return yield* Effect.callback<Net.Socket, RuntimeTcpTestFailure>((resume) => {
    const socket = Net.createConnection({
      host: tcpUrlConnectHost(parsedUrl.hostname),
      port,
    });
    socket.setEncoding("utf8");
    socket.once("connect", () => {
      resume(Effect.succeed(socket));
    });
    socket.once("error", (cause) => {
      resume(
        Effect.fail(
          new RuntimeTcpTestFailure({
            message: "TCP publish socket failed to connect.",
            cause,
          }),
        ),
      );
    });
  });
});

const readTcpPublishResponse = Effect.fn("ViewServerRuntime.test.tcp.response.read")(function* (
  socket: Net.Socket,
) {
  const line = yield* Effect.callback<string, RuntimeTcpTestFailure>((resume) => {
    let buffer = "";
    socket.on("data", (chunk: string) => {
      buffer += chunk;
      const newlineIndex = buffer.indexOf("\n");
      if (newlineIndex >= 0) {
        resume(Effect.succeed(buffer.slice(0, newlineIndex)));
      }
    });
    socket.once("error", (cause) => {
      resume(
        Effect.fail(
          new RuntimeTcpTestFailure({
            message: "TCP publish socket failed while reading response.",
            cause,
          }),
        ),
      );
    });
  });
  const value = yield* Effect.try({
    try: (): unknown => JSON.parse(line),
    catch: (cause) =>
      new RuntimeTcpTestFailure({
        message: "TCP publish response was not valid JSON.",
        cause,
      }),
  });
  return yield* Schema.decodeUnknownEffect(TcpPublishResponse)(value).pipe(
    Effect.mapError(
      (cause) =>
        new RuntimeTcpTestFailure({
          message: "TCP publish response did not match the test schema.",
          cause,
        }),
    ),
  );
});

const readTcpPublishResponses = Effect.fn("ViewServerRuntime.test.tcp.responses.read")(function* (
  socket: Net.Socket,
  count: number,
) {
  const lines = yield* Effect.callback<ReadonlyArray<string>, RuntimeTcpTestFailure>((resume) => {
    let buffer = "";
    const responses: Array<string> = [];
    socket.on("data", (chunk: string) => {
      buffer += chunk;
      let newlineIndex = buffer.indexOf("\n");
      while (newlineIndex >= 0) {
        responses.push(buffer.slice(0, newlineIndex));
        buffer = buffer.slice(newlineIndex + 1);
        newlineIndex = buffer.indexOf("\n");
      }
      if (responses.length === count) {
        resume(Effect.succeed(responses));
      }
    });
    socket.once("error", (cause) => {
      resume(
        Effect.fail(
          new RuntimeTcpTestFailure({
            message: "TCP publish socket failed while reading responses.",
            cause,
          }),
        ),
      );
    });
  });
  return yield* Effect.forEach(lines, (line) =>
    Effect.try({
      try: (): unknown => JSON.parse(line),
      catch: (cause) =>
        new RuntimeTcpTestFailure({
          message: "TCP publish response was not valid JSON.",
          cause,
        }),
    }).pipe(
      Effect.flatMap((value) => Schema.decodeUnknownEffect(TcpPublishResponse)(value)),
      Effect.mapError(
        (cause) =>
          new RuntimeTcpTestFailure({
            message: "TCP publish response did not match the test schema.",
            cause,
          }),
      ),
    ),
  );
});

const sendTcpPublishLine = Effect.fn("ViewServerRuntime.test.tcp.line.send")(function* (
  url: string,
  line: string,
) {
  const socket = yield* Effect.acquireRelease(connectTcpPublishSocket(url), (socket) =>
    Effect.sync(() => socket.destroy()),
  );
  socket.write(`${line}\n`);
  return yield* readTcpPublishResponse(socket).pipe(Effect.timeout("1 second"));
});

const sendTcpPublishCommand = Effect.fn("ViewServerRuntime.test.tcp.command.send")(function* (
  url: string,
  command: object,
) {
  return yield* sendTcpPublishLine(url, JSON.stringify(command));
});

const closeTestTcpServer = (server: Net.Server): Effect.Effect<void> =>
  Effect.promise(
    () =>
      new Promise<void>((resolve) => {
        server.close(() => resolve());
      }),
  );

const reserveTcpPort = Effect.fn("ViewServerRuntime.test.tcp.port.reserve")(function* () {
  const server = yield* Effect.callback<Net.Server, RuntimeTcpTestFailure>((resume) => {
    const nextServer = Net.createServer();
    nextServer.once("error", (cause) => {
      resume(
        Effect.fail(
          new RuntimeTcpTestFailure({
            message: "Test TCP server failed to reserve a port.",
            cause,
          }),
        ),
      );
    });
    nextServer.listen({ host: "127.0.0.1", port: 0 }, () => {
      resume(Effect.succeed(nextServer));
    });
  });
  const address = yield* Schema.decodeUnknownEffect(TestTcpAddress)(server.address()).pipe(
    Effect.mapError(
      (cause) =>
        new RuntimeTcpTestFailure({
          message: "Test TCP server produced an invalid listen address.",
          cause,
        }),
    ),
  );
  return { server, port: address.port };
});

const waitForTransportHealth = Effect.fn("ViewServerRuntime.test.transportHealth.wait")(function* (
  health: () => Effect.Effect<{ readonly transport: TransportHealth }, unknown>,
  expected: {
    readonly activeClients: number;
    readonly activeStreams: number;
  },
) {
  return yield* health().pipe(
    Effect.map((value) => value.transport),
    Effect.repeat({
      schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
      until: (transport) =>
        transport.activeClients === expected.activeClients &&
        transport.activeStreams === expected.activeStreams,
    }),
  );
});

const waitForGrpcSnapshotRows = Effect.fn("ViewServerRuntime.test.grpc.snapshotRows.wait")(
  function* (client: ViewServerRuntimeClient<GrpcTopics>, expectedTotalRows: number) {
    return yield* client
      .snapshot("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      })
      .pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (snapshot) => snapshot.totalRows === expectedTotalRows,
        }),
      );
  },
);

const readGrpcHealthOverlay = Effect.fn("ViewServerRuntime.test.grpc.healthOverlay.read")(
  function* (
    client: ViewServerRuntimeClient<GrpcTopics>,
    health: ReturnType<typeof makeViewServerGrpcHealthLedger<GrpcTopics>>,
    nowMillis: number,
  ) {
    return health.healthOverlay(yield* client.health(), nowMillis);
  },
);

const readGrpcHealthOverlayNow = Effect.fn("ViewServerRuntime.test.grpc.healthOverlay.readNow")(
  function* (
    client: ViewServerRuntimeClient<GrpcTopics>,
    health: ReturnType<typeof makeViewServerGrpcHealthLedger<GrpcTopics>>,
  ) {
    const nowMillis = yield* Clock.currentTimeMillis;
    return health.healthOverlay(yield* client.health(), nowMillis);
  },
);

const makeGrpcHealth = (
  grpcOptions: ResolvedViewServerGrpcRuntimeOptions<GrpcTopics, typeof grpcClients>,
) =>
  makeViewServerGrpcHealthLedger<GrpcTopics>({
    clients: grpcOptions.clientBaseUrls,
    feeds: {
      ordersFeed: {
        client: "orders",
        lifecycle: "materialized",
        topic: "orders",
      },
    },
  });

const grpcHealthFeed = (health: ViewServerHealth<GrpcTopics>) =>
  health.grpc?.feeds["orders"]?.materialized["ordersFeed"];

const grpcHealthClient = (health: ViewServerHealth<GrpcTopics>) => health.grpc?.clients["orders"];

const fastGrpcMaterializedReconnect = {
  delay: "10 millis",
  maxReconnects: 3,
} satisfies ResolvedViewServerGrpcRuntimeOptions<GrpcTopics>["materializedReconnect"];

const resolveGrpcRuntimeOptions = Effect.fn("ViewServerRuntime.test.grpc.options.resolve")(
  function* (feed: ReturnType<typeof grpcMaterializedFeed>) {
    const options = yield* resolveViewServerRuntimeOptions<
      GrpcTopics,
      Record<string, string>,
      typeof grpcClients
    >({
      grpc: {
        clients: grpcClients,
        feeds: {
          ordersFeed: feed,
        },
        materializedReconnect: fastGrpcMaterializedReconnect,
      },
    });
    return yield* Effect.fromNullishOr(options.grpcOptions);
  },
);

const resolveLeasedGrpcRuntimeOptions = Effect.fn(
  "ViewServerRuntime.test.grpc.leased.options.resolve",
)(function* (feed: ReturnType<typeof grpcLeasedFeed>) {
  const options = yield* resolveViewServerRuntimeOptions<
    typeof leasedGrpcViewServer.topics,
    Record<string, string>,
    typeof grpcClients
  >({
    grpc: {
      clients: grpcClients,
      feeds: {
        ordersLease: feed,
      },
      materializedReconnect: fastGrpcMaterializedReconnect,
    },
  });
  return yield* Effect.fromNullishOr(options.grpcOptions);
});

const makeLeasedGrpcHealth = (
  grpcOptions: ResolvedViewServerGrpcRuntimeOptions<
    typeof leasedGrpcViewServer.topics,
    typeof grpcClients
  >,
) =>
  makeViewServerGrpcHealthLedger<typeof leasedGrpcViewServer.topics>({
    clients: grpcOptions.clientBaseUrls,
    feeds: {},
  });

type LeasedOrdersQuery = {
  readonly select: readonly ["id", "customerId", "price", "region"];
  readonly where: {
    readonly region: {
      readonly eq: string;
    };
  };
  readonly orderBy: readonly [
    {
      readonly field: "price";
      readonly direction: "asc";
    },
  ];
  readonly limit: 10;
};

const leasedOrdersQuery = (region: string): LeasedOrdersQuery => ({
  select: ["id", "customerId", "price", "region"],
  where: {
    region: { eq: region },
  },
  orderBy: [{ field: "price", direction: "asc" }],
  limit: 10,
});

const waitForLeasedGrpcSnapshotRows = Effect.fn(
  "ViewServerRuntime.test.grpc.leased.snapshotRows.wait",
)(function* (
  client: ViewServerRuntimeClient<typeof leasedGrpcViewServer.topics>,
  region: string,
  expectedTotalRows: number,
) {
  return yield* client.snapshot("orders", leasedOrdersQuery(region)).pipe(
    Effect.repeat({
      schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
      until: (snapshot) => snapshot.totalRows === expectedTotalRows,
    }),
  );
});

describe("@effect-view-server/runtime", () => {
  it.live("starts a websocket runtime with health endpoint and runtime-core mutation client", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        rpcPath: "/runtime-rpc",
        healthPath: "/runtime-health",
        metricsPath: "/runtime-metrics",
      });
      const remoteClient = yield* makeViewServerClient(viewServer, { url: runtime.url });
      const subscription = yield* remoteClient.subscribe("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const eventsFiber = yield* subscription.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      const connectedTransport = yield* waitForTransportHealth(runtime.client.health, {
        activeClients: 1,
        activeStreams: 1,
      });
      expect(runtime.liveClient.health.value.transport.activeStreams).toBe(1);
      expect(connectedTransport).toStrictEqual({
        activeClients: 1,
        activeStreams: 1,
        activeSubscriptions: 1,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        queuedMessages: 0,
        queuedBytes: 0,
        droppedClients: 0,
        backpressureEvents: 0,
        reconnects: 0,
        lastError: null,
      });

      yield* runtime.client.publish("orders", order("a", 10));

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
        operations: [{ type: "insert", key: "a", row: { id: "a", price: 10 }, index: 0 }],
        totalRows: 1,
      });

      const health = yield* fetchHealth(runtime.healthUrl);
      const metrics = yield* fetchText(runtime.metricsUrl);
      expect(runtime.url.endsWith("/runtime-rpc")).toBe(true);
      expect(runtime.healthUrl.endsWith("/runtime-health")).toBe(true);
      expect(runtime.metricsUrl.endsWith("/runtime-metrics")).toBe(true);
      expect(health.response.status).toBe(200);
      expect(health.health.engine.topics.orders.rowCount).toBe(1);
      expect(metrics.response.status).toBe(200);
      expect(metrics.text).toContain(
        'view_server_engine_topic_rows{topic="orders",state="total"} 1',
      );

      yield* subscription.close().pipe(Effect.timeout("1 second"));
      yield* remoteClient.close;
      const disconnectedTransport = yield* waitForTransportHealth(runtime.client.health, {
        activeClients: 0,
        activeStreams: 0,
      });
      expect(disconnectedTransport).toStrictEqual({
        activeClients: 0,
        activeStreams: 0,
        activeSubscriptions: 0,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        queuedMessages: 0,
        queuedBytes: 0,
        droppedClients: 0,
        backpressureEvents: 0,
        reconnects: 0,
        lastError: null,
      });
      yield* runtime.close;
    }),
  );

  it.live("accepts TCP publish commands through the runtime mutation path", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpPublishUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);
      const subscription = yield* runtime.liveClient.subscribe("orders", {
        select: ["id", "price"],
        where: {
          price: { gte: 10 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const eventsFiber = yield* subscription.events.pipe(
        Stream.take(5),
        Stream.runCollect,
        Effect.forkChild,
      );

      const responses = [
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publish",
          topic: "orders",
          row: order("a", 10),
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "patch",
          topic: "orders",
          key: "a",
          patch: { price: 5 },
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publishMany",
          topic: "orders",
          rows: [order("b", 20), order("c", 30)],
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "delete",
          topic: "orders",
          key: "b",
        }),
      ];

      expect(responses).toStrictEqual([{ ok: true }, { ok: true }, { ok: true }, { ok: true }]);

      const events = yield* Fiber.join(eventsFiber);
      expect(events).toStrictEqual([
        {
          type: "snapshot",
          topic: "orders",
          queryId: "query-0",
          version: 0,
          keys: [],
          rows: [],
          totalRows: 0,
        },
        {
          type: "delta",
          topic: "orders",
          queryId: "query-0",
          fromVersion: 0,
          toVersion: 1,
          operations: [{ type: "insert", key: "a", row: { id: "a", price: 10 }, index: 0 }],
          totalRows: 1,
        },
        {
          type: "delta",
          topic: "orders",
          queryId: "query-0",
          fromVersion: 1,
          toVersion: 2,
          operations: [{ type: "remove", key: "a" }],
          totalRows: 0,
        },
        {
          type: "delta",
          topic: "orders",
          queryId: "query-0",
          fromVersion: 2,
          toVersion: 3,
          operations: [
            { type: "insert", key: "b", row: { id: "b", price: 20 }, index: 0 },
            { type: "insert", key: "c", row: { id: "c", price: 30 }, index: 1 },
          ],
          totalRows: 2,
        },
        {
          type: "delta",
          topic: "orders",
          queryId: "query-0",
          fromVersion: 3,
          toVersion: 4,
          operations: [{ type: "remove", key: "b" }],
          totalRows: 1,
        },
      ]);
      yield* subscription.close();
      yield* runtime.close;
    }),
  );

  it.live("passes TCP JSON rows to runtime core without double-decoding transform schemas", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(transformTcpViewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const responses = [
        yield* sendTcpPublishCommand(tcpUrl, {
          op: "publish",
          topic: "orders",
          row: { id: "a", quantity: "9007199254740993" },
        }),
        yield* sendTcpPublishCommand(tcpUrl, {
          op: "patch",
          topic: "orders",
          key: "a",
          patch: { quantity: "9007199254740995" },
        }),
        yield* sendTcpPublishCommand(tcpUrl, {
          op: "publishMany",
          topic: "orders",
          rows: [{ id: "b", quantity: "9007199254740997" }],
        }),
      ];
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "quantity"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(responses).toStrictEqual([{ ok: true }, { ok: true }, { ok: true }]);
      expect(snapshot).toStrictEqual({
        rows: [
          { id: "a", quantity: 9007199254740995n },
          { id: "b", quantity: 9007199254740997n },
        ],
        totalRows: 2,
        version: 3,
        status: "ready",
        statusCode: "Ready",
      });
      yield* runtime.close;
    }),
  );

  it.live("decodes TCP rows and patches through topic JSON codecs before publishing", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(jsonCodecTcpViewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const responses = [
        yield* sendTcpPublishCommand(tcpUrl, {
          op: "publish",
          topic: "orders",
          row: {
            allocations: {
              ["__proto__"]: {
                encodedQuantity: "2002",
                runtimeAmount: "22.25",
                runtimeQuantity: "21002",
              },
              primary: {
                encodedQuantity: "2001",
                runtimeAmount: "21.25",
                runtimeQuantity: "21001",
              },
            },
            fills: [
              {
                encodedQuantity: "3001",
                runtimeAmount: "31.25",
                runtimeQuantity: "31001",
              },
            ],
            id: "a",
            amount: "123.45",
            meta: {
              encodedQuantity: "1001",
              runtimeAmount: "11.25",
              runtimeQuantity: "11001",
            },
            nullableMeta: {
              encodedQuantity: "7001",
              runtimeAmount: "71.25",
            },
            optionalMeta: {
              encodedQuantity: "8001",
              runtimeAmount: "81.25",
            },
            optionalValueMeta: {
              encodedQuantity: "9001",
              runtimeAmount: "91.25",
            },
            checkedSuspendedEmptyMeta: {},
            quantity: "9007199254740993",
            suspendedMeta: {
              encodedQuantity: "11001",
              runtimeAmount: "111.25",
              runtimeQuantity: "111001",
            },
            tuple: [
              {
                encodedQuantity: "4001",
                runtimeAmount: "41.25",
                runtimeQuantity: "41001",
              },
            ],
            tupleRest: [
              {
                encodedQuantity: "5001",
                runtimeAmount: "51.25",
                runtimeQuantity: "51001",
              },
              {
                encodedQuantity: "5002",
                runtimeAmount: "52.25",
                runtimeQuantity: "51002",
              },
            ],
            tupleRestTrailing: [
              {
                encodedQuantity: "6001",
                runtimeAmount: "61.25",
                runtimeQuantity: "61001",
              },
              {
                encodedQuantity: "6002",
                runtimeAmount: "62.25",
                runtimeQuantity: "61002",
              },
              {
                encodedQuantity: "6003",
                runtimeAmount: "63.25",
                runtimeQuantity: "61003",
              },
            ],
            unionMeta: {
              encodedQuantity: "10001",
              runtimeAmount: "101.25",
              runtimeQuantity: "101001",
            },
          },
        }),
        yield* sendTcpPublishCommand(tcpUrl, {
          op: "patch",
          topic: "orders",
          key: "a",
          patch: {
            allocations: {
              ["__proto__"]: {
                encodedQuantity: "2004",
                runtimeAmount: "24.25",
                runtimeQuantity: "21004",
              },
              primary: {
                encodedQuantity: "2003",
                runtimeAmount: "23.25",
                runtimeQuantity: "21003",
              },
            },
            amount: "678.90",
            fills: [
              {
                encodedQuantity: "3003",
                runtimeAmount: "33.25",
                runtimeQuantity: "31003",
              },
            ],
            meta: {
              encodedQuantity: "1003",
              runtimeAmount: "33.75",
              runtimeQuantity: "11003",
            },
            nullableMeta: {
              encodedQuantity: "7003",
              runtimeAmount: "73.25",
            },
            optionalMeta: {
              encodedQuantity: "8003",
              runtimeAmount: "83.25",
            },
            optionalValueMeta: {
              encodedQuantity: "9003",
              runtimeAmount: "93.25",
            },
            quantity: "9007199254740995",
            suspendedMeta: {
              encodedQuantity: "11003",
              runtimeAmount: "113.25",
              runtimeQuantity: "111003",
            },
            tuple: [
              {
                encodedQuantity: "4003",
                runtimeAmount: "43.25",
                runtimeQuantity: "41003",
              },
            ],
            tupleRest: [
              {
                encodedQuantity: "5003",
                runtimeAmount: "53.25",
                runtimeQuantity: "51003",
              },
              {
                encodedQuantity: "5004",
                runtimeAmount: "54.25",
                runtimeQuantity: "51004",
              },
            ],
            tupleRestTrailing: [
              {
                encodedQuantity: "6003",
                runtimeAmount: "63.25",
                runtimeQuantity: "61003",
              },
              {
                encodedQuantity: "6004",
                runtimeAmount: "64.25",
                runtimeQuantity: "61004",
              },
              {
                encodedQuantity: "6005",
                runtimeAmount: "65.25",
                runtimeQuantity: "61005",
              },
            ],
            unionMeta: {
              encodedQuantity: "10003",
              runtimeAmount: "103.25",
              runtimeQuantity: "101003",
            },
          },
        }),
        yield* sendTcpPublishCommand(tcpUrl, {
          op: "publishMany",
          topic: "orders",
          rows: [
            {
              allocations: {
                primary: {
                  encodedQuantity: "2005",
                  runtimeAmount: "25.25",
                },
              },
              fills: [
                {
                  encodedQuantity: "3005",
                  runtimeAmount: "35.25",
                },
              ],
              id: "b",
              amount: "42.25",
              meta: {
                encodedQuantity: "1005",
                runtimeAmount: "55.50",
                runtimeQuantity: "11005",
              },
              nullableMeta: null,
              optionalMeta: {
                encodedQuantity: "8005",
                runtimeAmount: "85.25",
              },
              optionalValueMeta: {
                encodedQuantity: "9005",
                runtimeAmount: "95.25",
              },
              quantity: "9007199254740997",
              suspendedMeta: {
                encodedQuantity: "11005",
                runtimeAmount: "115.25",
              },
              tuple: [
                {
                  encodedQuantity: "4005",
                  runtimeAmount: "45.25",
                },
              ],
              tupleRest: [
                {
                  encodedQuantity: "5005",
                  runtimeAmount: "55.25",
                },
                {
                  encodedQuantity: "5006",
                  runtimeAmount: "56.25",
                },
              ],
              tupleRestTrailing: [
                {
                  encodedQuantity: "6005",
                  runtimeAmount: "65.25",
                },
                {
                  encodedQuantity: "6006",
                  runtimeAmount: "66.25",
                },
                {
                  encodedQuantity: "6007",
                  runtimeAmount: "67.25",
                },
              ],
              unionMeta: {
                encodedQuantity: "10005",
                runtimeAmount: "105.25",
              },
            },
          ],
        }),
      ];
      const invalidNestedResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          meta: {
            encodedQuantity: "1007",
          },
        },
      });
      const invalidNullNestedResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          meta: null,
        },
      });
      const invalidArrayResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          fills: [],
        },
      });
      const invalidRecordResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          allocations: {},
        },
      });
      const invalidTupleResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          tuple: [],
        },
      });
      const invalidTupleExtraResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          tuple: [
            {
              encodedQuantity: "4007",
              runtimeAmount: "47.25",
              runtimeQuantity: "41007",
            },
            {
              encodedQuantity: "4008",
              runtimeAmount: "48.25",
              runtimeQuantity: "41008",
            },
          ],
        },
      });
      const invalidTupleRestResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          tupleRest: [],
        },
      });
      const invalidOptionalWrapperResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          optionalMeta: {
            encodedQuantity: "8007",
          },
        },
      });
      const invalidNullableWrapperResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          nullableMeta: {
            encodedQuantity: "7007",
          },
        },
      });
      const invalidUnionWrapperResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          unionMeta: {
            encodedQuantity: "10007",
          },
        },
      });
      const invalidCheckedWrapperResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          checkedOptionalMeta: {
            encodedQuantity: "11007",
            runtimeAmount: "111.25",
          },
        },
      });
      const invalidSuspendedWrapperResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          suspendedMeta: {
            encodedQuantity: "11009",
          },
        },
      });
      const invalidCheckedSuspendedWrapperResponse = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: {
          checkedSuspendedMeta: {
            encodedQuantity: "12009",
            runtimeAmount: "129.25",
            runtimeQuantity: "121009",
          },
        },
      });
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: [
          "allocations",
          "amount",
          "fills",
          "id",
          "meta",
          "nullableMeta",
          "optionalMeta",
          "optionalValueMeta",
          "quantity",
          "suspendedMeta",
          "tuple",
          "tupleRest",
          "tupleRestTrailing",
          "unionMeta",
        ],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(responses).toStrictEqual([{ ok: true }, { ok: true }, { ok: true }]);
      expect(invalidNestedResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidNullNestedResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidArrayResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidRecordResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidTupleResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidTupleExtraResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidTupleRestResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidOptionalWrapperResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidNullableWrapperResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidUnionWrapperResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidCheckedWrapperResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidSuspendedWrapperResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(invalidCheckedSuspendedWrapperResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(snapshot).toStrictEqual({
        rows: [
          {
            allocations: {
              ["__proto__"]: {
                encodedQuantity: 2004n,
                runtimeAmount: BigDecimal.fromStringUnsafe("24.25"),
                runtimeQuantity: 21004n,
              },
              primary: {
                encodedQuantity: 2003n,
                runtimeAmount: BigDecimal.fromStringUnsafe("23.25"),
                runtimeQuantity: 21003n,
              },
            },
            id: "a",
            amount: BigDecimal.fromStringUnsafe("678.90"),
            fills: [
              {
                encodedQuantity: 3003n,
                runtimeAmount: BigDecimal.fromStringUnsafe("33.25"),
                runtimeQuantity: 31003n,
              },
            ],
            meta: {
              encodedQuantity: 1003n,
              runtimeAmount: BigDecimal.fromStringUnsafe("33.75"),
              runtimeQuantity: 11003n,
            },
            nullableMeta: {
              encodedQuantity: 7003n,
              runtimeAmount: BigDecimal.fromStringUnsafe("73.25"),
            },
            optionalMeta: {
              encodedQuantity: 8003n,
              runtimeAmount: BigDecimal.fromStringUnsafe("83.25"),
            },
            optionalValueMeta: {
              encodedQuantity: 9003n,
              runtimeAmount: BigDecimal.fromStringUnsafe("93.25"),
            },
            quantity: 9007199254740995n,
            suspendedMeta: {
              encodedQuantity: 11003n,
              runtimeAmount: BigDecimal.fromStringUnsafe("113.25"),
              runtimeQuantity: 111003n,
            },
            tuple: [
              {
                encodedQuantity: 4003n,
                runtimeAmount: BigDecimal.fromStringUnsafe("43.25"),
                runtimeQuantity: 41003n,
              },
            ],
            tupleRest: [
              {
                encodedQuantity: 5003n,
                runtimeAmount: BigDecimal.fromStringUnsafe("53.25"),
                runtimeQuantity: 51003n,
              },
              {
                encodedQuantity: 5004n,
                runtimeAmount: BigDecimal.fromStringUnsafe("54.25"),
                runtimeQuantity: 51004n,
              },
            ],
            tupleRestTrailing: [
              {
                encodedQuantity: 6003n,
                runtimeAmount: BigDecimal.fromStringUnsafe("63.25"),
                runtimeQuantity: 61003n,
              },
              {
                encodedQuantity: 6004n,
                runtimeAmount: BigDecimal.fromStringUnsafe("64.25"),
                runtimeQuantity: 61004n,
              },
              {
                encodedQuantity: 6005n,
                runtimeAmount: BigDecimal.fromStringUnsafe("65.25"),
                runtimeQuantity: 61005n,
              },
            ],
            unionMeta: {
              encodedQuantity: 10003n,
              runtimeAmount: BigDecimal.fromStringUnsafe("103.25"),
              runtimeQuantity: 101003n,
            },
          },
          {
            allocations: {
              primary: {
                encodedQuantity: 2005n,
                runtimeAmount: BigDecimal.fromStringUnsafe("25.25"),
              },
            },
            id: "b",
            amount: BigDecimal.fromStringUnsafe("42.25"),
            fills: [
              {
                encodedQuantity: 3005n,
                runtimeAmount: BigDecimal.fromStringUnsafe("35.25"),
              },
            ],
            meta: {
              encodedQuantity: 1005n,
              runtimeAmount: BigDecimal.fromStringUnsafe("55.50"),
              runtimeQuantity: 11005n,
            },
            nullableMeta: null,
            optionalMeta: {
              encodedQuantity: 8005n,
              runtimeAmount: BigDecimal.fromStringUnsafe("85.25"),
            },
            optionalValueMeta: {
              encodedQuantity: 9005n,
              runtimeAmount: BigDecimal.fromStringUnsafe("95.25"),
            },
            quantity: 9007199254740997n,
            suspendedMeta: {
              encodedQuantity: 11005n,
              runtimeAmount: BigDecimal.fromStringUnsafe("115.25"),
            },
            tuple: [
              {
                encodedQuantity: 4005n,
                runtimeAmount: BigDecimal.fromStringUnsafe("45.25"),
              },
            ],
            tupleRest: [
              {
                encodedQuantity: 5005n,
                runtimeAmount: BigDecimal.fromStringUnsafe("55.25"),
              },
              {
                encodedQuantity: 5006n,
                runtimeAmount: BigDecimal.fromStringUnsafe("56.25"),
              },
            ],
            tupleRestTrailing: [
              {
                encodedQuantity: 6005n,
                runtimeAmount: BigDecimal.fromStringUnsafe("65.25"),
              },
              {
                encodedQuantity: 6006n,
                runtimeAmount: BigDecimal.fromStringUnsafe("66.25"),
              },
              {
                encodedQuantity: 6007n,
                runtimeAmount: BigDecimal.fromStringUnsafe("67.25"),
              },
            ],
            unionMeta: {
              encodedQuantity: 10005n,
              runtimeAmount: BigDecimal.fromStringUnsafe("105.25"),
            },
          },
        ],
        totalRows: 2,
        version: 3,
        status: "ready",
        statusCode: "Ready",
      });
      yield* runtime.close;
    }),
  );

  it.live("decodes recursive suspended TCP rows through topic JSON codecs", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(jsonCodecTcpRecursiveViewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const response = yield* sendTcpPublishCommand(tcpUrl, {
        op: "publish",
        topic: "orders",
        row: {
          id: "recursive",
          node: {
            id: "1",
            amount: "10.25",
            runtimeQuantity: "9007199254740993",
            child: {
              id: "2",
              amount: "20.25",
              runtimeQuantity: "9007199254740995",
              child: {
                id: "3",
                amount: "30.25",
                runtimeQuantity: "9007199254740997",
                child: null,
              },
            },
          },
        },
      });
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "node"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(response).toStrictEqual({ ok: true });
      expect(snapshot).toStrictEqual({
        rows: [
          {
            id: "recursive",
            node: {
              id: 1n,
              amount: BigDecimal.fromStringUnsafe("10.25"),
              runtimeQuantity: 9007199254740993n,
              child: {
                id: 2n,
                amount: BigDecimal.fromStringUnsafe("20.25"),
                runtimeQuantity: 9007199254740995n,
                child: {
                  id: 3n,
                  amount: BigDecimal.fromStringUnsafe("30.25"),
                  runtimeQuantity: 9007199254740997n,
                  child: null,
                },
              },
            },
          },
        ],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      yield* runtime.close;
    }),
  );

  it.live("returns a usable bracketed TCP publish URL for IPv6 hosts", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        tcpPublishHost: "::1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);
      const response = yield* sendTcpPublishCommand(tcpUrl, {
        op: "publish",
        topic: "orders",
        row: order("ipv6", 42),
      });
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      });

      expect(tcpUrl.startsWith("tcp://[")).toBe(true);
      expect(tcpUrl.includes("]:")).toBe(true);
      expect(response).toStrictEqual({ ok: true });
      expect(snapshot).toStrictEqual({
        rows: [{ id: "ipv6", price: 42 }],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      yield* runtime.close;
    }),
  );

  it.live("rejects invalid TCP publish batches without partial mutation", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpPublishUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const response = yield* sendTcpPublishCommand(tcpPublishUrl, {
        op: "publishMany",
        topic: "orders",
        rows: [order("valid", 10), { id: "invalid", price: "not-a-number" }],
      });
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(response).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish row did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(snapshot).toStrictEqual({
        rows: [],
        totalRows: 0,
        version: 0,
        status: "ready",
        statusCode: "Ready",
      });
      yield* runtime.close;
    }),
  );

  it.live("rejects TCP publish rows that do not match the target topic schema", () =>
    Effect.gen(function* () {
      const schemaSafetyViewServer = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            key: "id",
          },
          trades: {
            schema: Trade,
            key: "id",
          },
        },
      });
      const runtime = yield* makeViewServerRuntime(schemaSafetyViewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpPublishUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const tradeShapedOrderResponse = yield* sendTcpPublishCommand(tcpPublishUrl, {
        op: "publish",
        topic: "orders",
        row: {
          id: "trade-1",
          symbol: "AAPL",
        },
      });
      const extraFieldOrderResponse = yield* sendTcpPublishCommand(tcpPublishUrl, {
        op: "publish",
        topic: "orders",
        row: {
          id: "order-1",
          price: 10,
          symbol: "AAPL",
        },
      });
      const missingRequiredOrderResponse = yield* sendTcpPublishCommand(tcpPublishUrl, {
        op: "publish",
        topic: "orders",
        row: {
          id: "order-2",
        },
      });
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(tradeShapedOrderResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish row did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(extraFieldOrderResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish row did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(missingRequiredOrderResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish row did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      expect(snapshot).toStrictEqual({
        rows: [],
        totalRows: 0,
        version: 0,
        status: "ready",
        statusCode: "Ready",
      });
      yield* runtime.close;
    }),
  );

  it.live("preserves runtime error codes in TCP publish responses", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const response = yield* sendTcpPublishCommand(tcpUrl, {
        op: "patch",
        topic: "orders",
        key: "missing",
        patch: { price: 11 },
      });

      expect(response).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidRow",
          message: "Cannot patch missing key: missing",
          topic: "orders",
        },
      });
      yield* runtime.close;
    }),
  );

  it.live("returns typed TCP publish errors for malformed commands", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpPublishUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const responses = [
        yield* sendTcpPublishLine(tcpPublishUrl, "{"),
        yield* sendTcpPublishLine(tcpPublishUrl, JSON.stringify("not-object")),
        yield* sendTcpPublishLine(tcpPublishUrl, "   \n{}"),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publish",
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publish",
          topic: "orders",
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publishMany",
          topic: "orders",
          rows: {},
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "patch",
          topic: "orders",
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "patch",
          topic: "orders",
          key: "a",
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "delete",
          topic: "orders",
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "noop",
          topic: "orders",
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publish",
          topic: "orders",
          row: order("a", 10),
          rows: [order("b", 20)],
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publish",
          topic: "orders",
          row: order("a", 10),
          unknown: true,
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publish",
          topic: "unknown",
          row: order("a", 10),
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publish",
          topic: "orders",
          row: { ...order("a", 10), unknown: true },
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "publish",
          topic: "orders",
          row: { id: "missing-price" },
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "patch",
          topic: "orders",
          key: "a",
          patch: { unknown: true },
        }),
        yield* sendTcpPublishLine(
          tcpPublishUrl,
          `{"op":"patch","topic":"orders","key":"a","patch":{"constructor":10}}\n`,
        ),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "patch",
          topic: "orders",
          key: "a",
          patch: { price: "expensive" },
        }),
        yield* sendTcpPublishCommand(tcpPublishUrl, {
          op: "delete",
          topic: "unknown",
          key: "a",
        }),
      ];

      expect(responses).toStrictEqual([
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must be valid JSON.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command must match the publish command schema.",
            phase: "decode",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish cannot find View Server topic unknown.",
            phase: "decode",
            topic: "unknown",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish row did not match View Server topic orders.",
            phase: "decode",
            topic: "orders",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish row did not match View Server topic orders.",
            phase: "decode",
            topic: "orders",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish patch did not match View Server topic orders.",
            phase: "decode",
            topic: "orders",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish patch did not match View Server topic orders.",
            phase: "decode",
            topic: "orders",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish patch did not match View Server topic orders.",
            phase: "decode",
            topic: "orders",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish cannot find View Server topic unknown.",
            phase: "decode",
            topic: "unknown",
          },
        },
      ]);
      yield* runtime.close;
    }),
  );

  it.live("rejects non-strict TCP publish patch field values at the decode boundary", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(nestedTcpViewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpPublishUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const response = yield* sendTcpPublishCommand(tcpPublishUrl, {
        op: "patch",
        topic: "orders",
        key: "a",
        patch: { meta: { desk: "LDN", unknown: true } },
      });

      expect(response).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish patch did not match View Server topic orders.",
          phase: "decode",
          topic: "orders",
        },
      });
      yield* runtime.close;
    }),
  );

  it.live("rejects invalid TCP publish server options before listening", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        websocketPort: 0,
      });
      const invalidOptions = [
        { port: -1 },
        { port: 65536 },
        { port: 1.5 },
        { port: Number.NaN },
        { port: Number.POSITIVE_INFINITY },
        { maxLineBytes: 0, port: 0 },
        { maxConnections: 0, port: 0 },
        { maxQueuedCommands: 0, port: 0 },
        { maxGlobalQueuedCommands: 0, port: 0 },
      ];
      const errors = yield* Effect.forEach(invalidOptions, (options) =>
        makeViewServerTcpPublishIngress(viewServer, runtime.client, options).pipe(Effect.flip),
      );

      expect(
        errors.map((error) => ({
          message: error.message,
          phase: error.phase,
        })),
      ).toStrictEqual([
        {
          message: "TCP publish port must be a safe integer between 0 and 65535.",
          phase: "configuration",
        },
        {
          message: "TCP publish port must be a safe integer between 0 and 65535.",
          phase: "configuration",
        },
        {
          message: "TCP publish port must be a safe integer between 0 and 65535.",
          phase: "configuration",
        },
        {
          message: "TCP publish port must be a safe integer between 0 and 65535.",
          phase: "configuration",
        },
        {
          message: "TCP publish port must be a safe integer between 0 and 65535.",
          phase: "configuration",
        },
        {
          message: "TCP publish maxLineBytes must be a positive safe integer.",
          phase: "configuration",
        },
        {
          message: "TCP publish maxConnections must be a positive safe integer.",
          phase: "configuration",
        },
        {
          message: "TCP publish maxQueuedCommands must be a positive safe integer.",
          phase: "configuration",
        },
        {
          message: "TCP publish maxGlobalQueuedCommands must be a positive safe integer.",
          phase: "configuration",
        },
      ]);
      yield* runtime.close;
    }),
  );

  it.live("rejects TCP publish commands for source-owned topics", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        websocketPort: 0,
      });
      const ingress = yield* makeViewServerTcpPublishIngress(viewServer, runtime.client, {
        port: 0,
        rejectedTopics: new Set(["orders"]),
      });

      const response = yield* sendTcpPublishCommand(ingress.url, {
        op: "publish",
        topic: "orders",
        row: order("a", 10),
      });

      expect(response).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish cannot mutate source-owned View Server topic orders.",
          phase: "runtime",
          topic: "orders",
        },
      });
      yield* ingress.close;
      yield* runtime.close;
    }),
  );

  it.live("requires auth for TCP publish mutations when runtime auth is configured", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        auth: bearerAuth,
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpPublishUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const rejected = yield* sendTcpPublishCommand(tcpPublishUrl, {
        op: "publish",
        topic: "orders",
        row: order("rejected", 10),
      });
      const accepted = yield* sendTcpPublishCommand(tcpPublishUrl, {
        headers: {
          authorization: "Bearer view-server-test",
        },
        op: "publish",
        topic: "orders",
        row: order("accepted", 20),
      });
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(rejected).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerAuthError",
          message: "Missing or invalid authorization header.",
          status: 401,
        },
      });
      expect(accepted).toStrictEqual({ ok: true });
      expect(snapshot).toStrictEqual({
        rows: [{ id: "accepted", price: 20 }],
        status: "ready",
        statusCode: "Ready",
        totalRows: 1,
        version: 1,
      });
      yield* runtime.close;
    }),
  );

  it.live("passes TCP publish peer address into auth validation", () =>
    Effect.gen(function* () {
      const remoteAddress = yield* Deferred.make<string>();
      const runtime = yield* makeViewServerRuntime(viewServer, {
        auth: {
          validateRequest: (request) =>
            Option.match(request.remoteAddress, {
              onNone: () =>
                Effect.fail(
                  new ViewServerAuthError({
                    message: "TCP auth did not receive a peer address.",
                    status: 403,
                  }),
                ),
              onSome: (address) =>
                Deferred.succeed(remoteAddress, address).pipe(
                  Effect.as({
                    forwardedHeaders: {},
                    id: "tcp-session",
                    systemHeaders: {},
                  }),
                ),
            }),
        },
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpPublishUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);

      const accepted = yield* sendTcpPublishCommand(tcpPublishUrl, {
        op: "publish",
        topic: "orders",
        row: order("accepted", 20),
      });
      const observedRemoteAddress = yield* Deferred.await(remoteAddress);

      expect(accepted).toStrictEqual({ ok: true });
      expect(observedRemoteAddress).toBe("127.0.0.1");
      yield* runtime.close;
    }),
  );

  it.live("passes source-owned topics into TCP publish ingress rejection policy", () =>
    Effect.gen(function* () {
      type MixedTopics = typeof grpcAndKafkaViewServer.topics;
      type RuntimeDependencies = ViewServerRuntimeDependencies<MixedTopics>;
      const regions = {
        local: "localhost:9092",
      };
      const localKafkaTopic = grpcAndKafkaViewServer.kafkaTopic<typeof regions>();
      const feed = mixedGrpcFeed.materializedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => Stream.never,
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      let rejectedTopics: ReadonlyArray<string> = [];
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<MixedTopics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.void,
          }),
        makeKafkaIngress: () => Effect.succeed({ close: Effect.void }),
        makeGrpcIngress: () => Effect.succeed({ close: Effect.void }),
        makeTcpPublishIngress: (_config, _client, options) => {
          rejectedTopics = Array.from(options.rejectedTopics ?? []);
          return Effect.succeed({
            url: "tcp://127.0.0.1:1235",
            close: Effect.void,
          });
        },
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        grpcAndKafkaViewServer,
        {
          host: "127.0.0.1",
          tcpPublishPort: 1235,
          kafka: {
            consumerGroupId: "view-server-tcp-source-owned",
            regions,
            topics: {
              "audit-source": localKafkaTopic({
                regions: ["local"],
                value: kafka.json(Order),
                key: kafka.stringKey(),
                viewServerTopic: "audit",
                getSafeRowKey: ({ key }) => key,
                mapping: ({ key, value }) => ({
                  id: key,
                  price: value.price,
                }),
              }),
            },
          },
          grpc: {
            clients: grpcClients,
            feeds: {
              ordersFeed: feed,
            },
          },
        },
      );

      expect({
        rejectedTopics,
        tcpPublishUrl: runtime.tcpPublishUrl,
      }).toStrictEqual({
        rejectedTopics: ["audit", "orders"],
        tcpPublishUrl: "tcp://127.0.0.1:1235",
      });
      yield* runtime.close;
    }),
  );

  it.live("bounds TCP publish line size and command queue", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        websocketPort: 0,
      });
      const rejectedAcceptedSocket = new Net.Socket();
      const partialAcceptedSocket = new Net.Socket();
      const partialAcceptedSocketDestroyed: Array<"socket"> = [];
      partialAcceptedSocket.destroy = () => {
        partialAcceptedSocketDestroyed.push("socket");
        return partialAcceptedSocket;
      };
      installTcpPublishAcceptedSocket(
        rejectedAcceptedSocket,
        {
          activeChains: new Set(),
          activeFibers: new Set(),
          closed: true,
          preCommandDeadlineMs: undefined,
          queuedCommands: 0,
          socketStates: new Map(),
          sockets: new Set(),
        },
        viewServer,
        runtime.client,
        { port: 0 },
      );
      const partialAcceptedSocketState = {
        activeChains: new Set<Promise<void>>(),
        activeFibers: new Set<Fiber.Fiber<void, never>>(),
        closed: false,
        preCommandDeadlineMs: 1,
        queuedCommands: 0,
        socketStates: new Map(),
        sockets: new Set<Net.Socket>(),
      };
      installTcpPublishAcceptedSocket(
        partialAcceptedSocket,
        partialAcceptedSocketState,
        viewServer,
        runtime.client,
        { port: 0 },
      );
      partialAcceptedSocket.emit("data", "  ");
      yield* Effect.sleep("20 millis");
      const unauthorizedSocket = new Net.Socket();
      const unauthorizedSocketResponses: Array<string> = [];
      const unauthorizedSocketState = {
        activeChains: new Set<Promise<void>>(),
        activeFibers: new Set<Fiber.Fiber<void, never>>(),
        closed: false,
        preCommandDeadlineMs: 1_000,
        queuedCommands: 0,
        socketStates: new Map(),
        sockets: new Set<Net.Socket>(),
      };
      Object.defineProperty(unauthorizedSocket, "write", {
        value: (chunk: string, callback: () => void) => {
          unauthorizedSocketResponses.push(chunk);
          callback();
          return true;
        },
      });
      installTcpPublishAcceptedSocket(
        unauthorizedSocket,
        unauthorizedSocketState,
        viewServer,
        runtime.client,
        {
          auth: bearerAuth,
          port: 0,
        },
      );
      const initialUnauthorizedDeadline =
        unauthorizedSocketState.socketStates.get(unauthorizedSocket)?.preCommandDeadline;
      unauthorizedSocket.emit(
        "data",
        `${JSON.stringify({
          op: "publish",
          topic: "orders",
          row: order("unauthorized", 10),
        })}\n`,
      );
      yield* Effect.promise(() => Promise.allSettled(unauthorizedSocketState.activeChains));
      const slowPublishStarted = yield* Deferred.make<void>();
      const slowPublishInterrupted = yield* Deferred.make<void>();
      const deadlineClearedPublishStarted = yield* Deferred.make<void>();
      const deadlineClearedPublishInterrupted = yield* Deferred.make<void>();
      const globalPublishStarted = yield* Deferred.make<void>();
      const globalPublishInterrupted = yield* Deferred.make<void>();
      const disconnectedPublishStarted = yield* Deferred.make<void>();
      const disconnectedPublishInterrupted = yield* Deferred.make<void>();
      const closedQueuePublishStarted = yield* Deferred.make<void>();
      const closedQueuePublishInterrupted = yield* Deferred.make<void>();
      const slowPublishClient = Object.create(runtime.client);
      Object.defineProperty(slowPublishClient, "publish", {
        value: () =>
          Deferred.succeed(slowPublishStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(slowPublishInterrupted, undefined)),
          ),
      });
      const deadlineClearedPublishClient = Object.create(runtime.client);
      Object.defineProperty(deadlineClearedPublishClient, "publish", {
        value: () =>
          Deferred.succeed(deadlineClearedPublishStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(deadlineClearedPublishInterrupted, undefined)),
          ),
      });
      const globalPublishClient = Object.create(runtime.client);
      Object.defineProperty(globalPublishClient, "publish", {
        value: () =>
          Deferred.succeed(globalPublishStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(globalPublishInterrupted, undefined)),
          ),
      });
      const disconnectedPublishClient = Object.create(runtime.client);
      Object.defineProperty(disconnectedPublishClient, "publish", {
        value: () =>
          Deferred.succeed(disconnectedPublishStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(disconnectedPublishInterrupted, undefined)),
          ),
      });
      const closedQueuePublishClient = Object.create(runtime.client);
      Object.defineProperty(closedQueuePublishClient, "publish", {
        value: () =>
          Deferred.succeed(closedQueuePublishStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(closedQueuePublishInterrupted, undefined)),
          ),
      });
      const compactCommandLine = `${JSON.stringify({
        op: "publish",
        topic: "orders",
        row: order("a", 10),
      })}\n`;
      const deadlineClearedSocket = new Net.Socket();
      const deadlineClearedSocketDestroyed: Array<"socket"> = [];
      const deadlineClearedSocketState = {
        activeChains: new Set<Promise<void>>(),
        activeFibers: new Set<Fiber.Fiber<void, never>>(),
        closed: false,
        preCommandDeadlineMs: 1,
        queuedCommands: 0,
        socketStates: new Map(),
        sockets: new Set<Net.Socket>(),
      };
      deadlineClearedSocket.destroy = () => {
        deadlineClearedSocketDestroyed.push("socket");
        deadlineClearedSocket.emit("close");
        return deadlineClearedSocket;
      };
      installTcpPublishAcceptedSocket(
        deadlineClearedSocket,
        deadlineClearedSocketState,
        viewServer,
        deadlineClearedPublishClient,
        { port: 0 },
      );
      deadlineClearedSocket.emit("data", compactCommandLine);
      yield* Deferred.await(deadlineClearedPublishStarted).pipe(Effect.timeout("1 second"));
      yield* Effect.sleep("20 millis");
      const rearmedSocket = new Net.Socket();
      const rearmedSocketDestroyed: Array<"socket"> = [];
      const rearmedSocketResponses: Array<string> = [];
      const rearmedSocketState = {
        activeChains: new Set<Promise<void>>(),
        activeFibers: new Set<Fiber.Fiber<void, never>>(),
        closed: false,
        preCommandDeadlineMs: 1,
        queuedCommands: 0,
        socketStates: new Map(),
        sockets: new Set<Net.Socket>(),
      };
      rearmedSocket.destroy = () => {
        rearmedSocketDestroyed.push("socket");
        rearmedSocket.emit("close");
        return rearmedSocket;
      };
      Object.defineProperty(rearmedSocket, "write", {
        value: (chunk: string, callback: () => void) => {
          rearmedSocketResponses.push(chunk);
          callback();
          return true;
        },
      });
      installTcpPublishAcceptedSocket(
        rearmedSocket,
        rearmedSocketState,
        viewServer,
        runtime.client,
        { port: 0 },
      );
      rearmedSocket.emit(
        "data",
        `${JSON.stringify({
          op: "publish",
          topic: "orders",
          row: order("rearmed", 10),
        })}\n`,
      );
      yield* Effect.sleep("20 millis");
      const oversizedIngress = yield* makeViewServerTcpPublishIngress(viewServer, runtime.client, {
        maxLineBytes: 8,
        port: 0,
      });
      const oversizedCompleteLineIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        runtime.client,
        {
          maxLineBytes: 8,
          port: 0,
        },
      );
      const coalescedIngress = yield* makeViewServerTcpPublishIngress(viewServer, runtime.client, {
        maxLineBytes: Buffer.byteLength(compactCommandLine, "utf8"),
        port: 0,
      });
      const queuedIngress = yield* makeViewServerTcpPublishIngress(viewServer, slowPublishClient, {
        maxQueuedCommands: 2,
        port: 0,
      });
      const globalQueuedIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        globalPublishClient,
        {
          maxGlobalQueuedCommands: 1,
          maxQueuedCommands: 2,
          port: 0,
        },
      );
      const disconnectedIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        disconnectedPublishClient,
        { port: 0 },
      );
      const connectionCappedIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        runtime.client,
        {
          maxConnections: 1,
          port: 0,
        },
      );
      const closedQueueIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        closedQueuePublishClient,
        {
          maxQueuedCommands: 1,
          port: 0,
        },
      );
      const oversizedSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(oversizedIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      oversizedSocket.write("this-line-is-too-large");
      const oversizedResponse = yield* readTcpPublishResponse(oversizedSocket).pipe(
        Effect.timeout("1 second"),
      );

      const oversizedCompleteLineSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(oversizedCompleteLineIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      oversizedCompleteLineSocket.write("this-line-is-too-large\n");
      const oversizedCompleteLineResponse = yield* readTcpPublishResponse(
        oversizedCompleteLineSocket,
      ).pipe(Effect.timeout("1 second"));

      const coalescedSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(coalescedIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      coalescedSocket.write(`${compactCommandLine}${compactCommandLine}`);
      const coalescedResponses = yield* readTcpPublishResponses(coalescedSocket, 2).pipe(
        Effect.timeout("1 second"),
      );

      const queuedSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(queuedIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      const commandLine = `${JSON.stringify({
        op: "publish",
        topic: "orders",
        row: order("a", 10),
      })}\n`;
      queuedSocket.write(commandLine);
      yield* Deferred.await(slowPublishStarted);
      queuedSocket.write(commandLine);
      queuedSocket.write(commandLine);
      const queuedResponse = yield* readTcpPublishResponse(queuedSocket).pipe(
        Effect.timeout("1 second"),
      );

      const globalQueuedFirstSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(globalQueuedIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      const globalQueuedSecondSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(globalQueuedIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      globalQueuedFirstSocket.write(commandLine);
      yield* Deferred.await(globalPublishStarted);
      globalQueuedSecondSocket.write(commandLine);
      const globalQueuedResponse = yield* readTcpPublishResponse(globalQueuedSecondSocket).pipe(
        Effect.timeout("1 second"),
      );

      const disconnectedSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(disconnectedIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      disconnectedSocket.write(commandLine);
      yield* Deferred.await(disconnectedPublishStarted);
      disconnectedSocket.destroy();
      yield* Deferred.await(disconnectedPublishInterrupted).pipe(Effect.timeout("1 second"));

      const closedQueueSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(closedQueueIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      closedQueueSocket.write(`${commandLine}${commandLine}${commandLine}`);
      const closedQueueResponse = yield* readTcpPublishResponse(closedQueueSocket).pipe(
        Effect.timeout("1 second"),
      );

      const heldConnectionCappedSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(connectionCappedIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      const rejectedConnectionCappedSocket = yield* Effect.acquireRelease(
        connectTcpPublishSocket(connectionCappedIngress.url),
        (socket) => Effect.sync(() => socket.destroy()),
      );
      const connectionCappedResponse = yield* readTcpPublishResponse(
        rejectedConnectionCappedSocket,
      ).pipe(Effect.timeout("1 second"));
      yield* Effect.sleep("10 millis");

      expect(oversizedResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish command exceeded 8 bytes without a newline.",
          phase: "backpressure",
        },
      });
      expect(oversizedCompleteLineResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish command exceeded 8 bytes.",
          phase: "backpressure",
        },
      });
      expect(coalescedResponses).toStrictEqual([{ ok: true }, { ok: true }]);
      expect(queuedResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish command queue exceeded 2 commands.",
          phase: "backpressure",
        },
      });
      expect(globalQueuedResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish global command queue exceeded 1 commands.",
          phase: "backpressure",
        },
      });
      expect(closedQueueResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish command queue exceeded 1 commands.",
          phase: "backpressure",
        },
      });
      expect(connectionCappedResponse).toStrictEqual({
        ok: false,
        error: {
          _tag: "ViewServerTcpPublishIngressError",
          message: "TCP publish connection count exceeded 1 sockets.",
          phase: "backpressure",
        },
      });
      expect(oversizedSocket.destroyed).toBe(true);
      expect(oversizedCompleteLineSocket.destroyed).toBe(true);
      expect(rejectedConnectionCappedSocket.destroyed).toBe(true);
      expect(rejectedAcceptedSocket.destroyed).toBe(true);
      expect(partialAcceptedSocketDestroyed).toStrictEqual(["socket"]);
      expect(unauthorizedSocketResponses).toStrictEqual([
        '{"ok":false,"error":{"_tag":"ViewServerAuthError","message":"Missing or invalid authorization header.","status":401}}\n',
      ]);
      expect(unauthorizedSocketState.socketStates.get(unauthorizedSocket)?.preCommandDeadline).toBe(
        initialUnauthorizedDeadline,
      );
      expect(deadlineClearedSocketDestroyed).toStrictEqual([]);
      expect(rearmedSocketResponses).toStrictEqual(['{"ok":true}\n']);
      expect(rearmedSocketDestroyed).toStrictEqual(["socket"]);

      deadlineClearedSocket.destroy();
      unauthorizedSocket.destroy();
      yield* Deferred.await(deadlineClearedPublishInterrupted).pipe(Effect.timeout("1 second"));
      heldConnectionCappedSocket.destroy();
      yield* connectionCappedIngress.close;
      yield* closedQueueIngress.close.pipe(Effect.timeout("1 second"));
      yield* disconnectedIngress.close;
      yield* globalQueuedIngress.close.pipe(Effect.timeout("1 second"));
      yield* Deferred.await(globalPublishInterrupted);
      yield* queuedIngress.close.pipe(Effect.timeout("1 second"));
      yield* queuedIngress.close.pipe(Effect.timeout("1 second"));
      yield* Deferred.await(slowPublishInterrupted);
      yield* coalescedIngress.close;
      yield* oversizedCompleteLineIngress.close;
      yield* oversizedIngress.close;
      yield* oversizedIngress.close;
      yield* runtime.close;
    }),
  );

  it.live("keeps TCP response backpressure non-fatal through deterministic internals", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        websocketPort: 0,
      });
      const writtenChunks: Array<string> = [];
      const destroyed: Array<"socket"> = [];
      const listeners: Partial<Record<"close" | "error", () => void>> = {};
      let writeCallback: () => void = () => undefined;
      const closedServerState = {
        activeChains: new Set<Promise<void>>(),
        activeFibers: new Set<Fiber.Fiber<void, never>>(),
        closed: false,
        preCommandDeadlineMs: undefined,
        queuedCommands: 0,
        socketStates: new Map(),
        sockets: new Set<Net.Socket>(),
      };
      const ignoredAfterCloseSocket = new Net.Socket();
      const state = {
        activeFibers: new Set<Fiber.Fiber<void, never>>(),
        buffer: "",
        chain: Promise.resolve(),
        closed: false,
        preCommandDeadline: undefined,
        queuedCommands: 0,
      };

      const pendingWrite = writeTcpJsonLine(
        {
          destroyed: false,
          off: (event) => {
            delete listeners[event];
            return new Net.Socket();
          },
          once: (event, listener) => {
            listeners[event] = () => listener();
            return new Net.Socket();
          },
          write: (chunk, callback) => {
            writtenChunks.push(chunk);
            writeCallback = callback;
            return false;
          },
        },
        state,
        { ok: true },
      );
      yield* Effect.promise(() => Promise.resolve());
      expect({
        listenerNames: Object.keys(listeners),
        stateClosed: state.closed,
        writtenChunks,
      }).toStrictEqual({
        listenerNames: ["close", "error"],
        stateClosed: false,
        writtenChunks: ['{"ok":true}\n'],
      });
      writeCallback();
      writeCallback();
      yield* Effect.promise(() => pendingWrite);
      yield* Effect.promise(() =>
        writeTcpJsonLine(
          {
            destroyed: true,
            off: () => new Net.Socket(),
            once: () => new Net.Socket(),
            write: (chunk, callback) => {
              writtenChunks.push(chunk);
              callback();
              return true;
            },
          },
          state,
          { ok: false },
        ),
      );
      const acceptedWhileClosed = rejectTcpSocketWhenClosed(true, {
        destroy: () => {
          destroyed.push("socket");
        },
      });
      const acceptedWhileOpen = rejectTcpSocketWhenClosed(false, {
        destroy: () => {
          destroyed.push("socket");
        },
      });
      installTcpPublishAcceptedSocket(
        ignoredAfterCloseSocket,
        closedServerState,
        viewServer,
        runtime.client,
        { port: 0 },
      );
      closedServerState.closed = true;
      ignoredAfterCloseSocket.emit("data", "ignored\n");
      ignoredAfterCloseSocket.destroy();

      expect({
        acceptedWhileClosed,
        acceptedWhileOpen,
        destroyed,
        ignoredAfterCloseQueuedCommands: closedServerState.queuedCommands,
        listenerNames: Object.keys(listeners),
        stateClosed: state.closed,
        writtenChunks,
      }).toStrictEqual({
        acceptedWhileClosed: true,
        acceptedWhileOpen: false,
        destroyed: ["socket"],
        ignoredAfterCloseQueuedCommands: 0,
        listenerNames: [],
        stateClosed: false,
        writtenChunks: ['{"ok":true}\n'],
      });
      yield* runtime.close;
    }),
  );

  it.live("returns typed TCP publish errors for malformed runtime clients", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        websocketPort: 0,
      });
      const missingPublishClient = Object.create(runtime.client);
      Object.defineProperty(missingPublishClient, "publish", {
        value: undefined,
      });
      const nonEffectPublishClient = Object.create(runtime.client);
      Object.defineProperty(nonEffectPublishClient, "publish", {
        value: () => "not-an-effect",
      });
      const defectPublishClient = Object.create(runtime.client);
      Object.defineProperty(defectPublishClient, "publish", {
        value: () => Effect.die("tcp publish defect"),
      });
      const failingPublishClient = Object.create(runtime.client);
      Object.defineProperty(failingPublishClient, "publish", {
        value: () =>
          Effect.fail(
            new RuntimeTcpTestFailure({
              cause: "typed runtime failure",
              message: "typed runtime publish failure",
            }),
          ),
      });
      const stringFailingPublishClient = Object.create(runtime.client);
      Object.defineProperty(stringFailingPublishClient, "publish", {
        value: () => Effect.fail("string runtime failure"),
      });
      const unavailablePublishClient = Object.create(runtime.client);
      Object.defineProperty(unavailablePublishClient, "publish", {
        value: () =>
          Effect.fail({
            _tag: "ViewServerRuntimeError",
            code: "RuntimeUnavailable",
            message: "runtime unavailable for tcp test",
          } satisfies ViewServerRuntimeError),
      });
      const missingPublishIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        missingPublishClient,
        { port: 0 },
      );
      const nonEffectPublishIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        nonEffectPublishClient,
        { port: 0 },
      );
      const defectPublishIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        defectPublishClient,
        { port: 0 },
      );
      const failingPublishIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        failingPublishClient,
        { port: 0 },
      );
      const stringFailingPublishIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        stringFailingPublishClient,
        { port: 0 },
      );
      const unavailablePublishIngress = yield* makeViewServerTcpPublishIngress(
        viewServer,
        unavailablePublishClient,
        { port: 0 },
      );

      const responses = [
        yield* sendTcpPublishCommand(missingPublishIngress.url, {
          op: "publish",
          topic: "orders",
          row: order("a", 10),
        }),
        yield* sendTcpPublishCommand(nonEffectPublishIngress.url, {
          op: "publish",
          topic: "orders",
          row: order("a", 10),
        }),
        yield* sendTcpPublishCommand(defectPublishIngress.url, {
          op: "publish",
          topic: "orders",
          row: order("a", 10),
        }),
        yield* sendTcpPublishCommand(failingPublishIngress.url, {
          op: "publish",
          topic: "orders",
          row: order("a", 10),
        }),
        yield* sendTcpPublishCommand(stringFailingPublishIngress.url, {
          op: "publish",
          topic: "orders",
          row: order("a", 10),
        }),
        yield* sendTcpPublishCommand(unavailablePublishIngress.url, {
          op: "publish",
          topic: "orders",
          row: order("a", 10),
        }),
      ];

      expect(responses).toStrictEqual([
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "Runtime publish did not return an Effect for TCP publish topic orders.",
            phase: "runtime",
            topic: "orders",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "Runtime publish did not return an Effect for TCP publish topic orders.",
            phase: "runtime",
            topic: "orders",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish command failed with an untyped cause.",
            phase: "runtime",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish runtime publish failed for topic orders.",
            phase: "runtime",
            topic: "orders",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerTcpPublishIngressError",
            message: "TCP publish runtime publish failed for topic orders.",
            phase: "runtime",
            topic: "orders",
          },
        },
        {
          ok: false,
          error: {
            _tag: "ViewServerRuntimeError",
            code: "RuntimeUnavailable",
            message: "runtime unavailable for tcp test",
          },
        },
      ]);

      yield* unavailablePublishIngress.close;
      yield* stringFailingPublishIngress.close;
      yield* failingPublishIngress.close;
      yield* defectPublishIngress.close;
      yield* nonEffectPublishIngress.close;
      yield* missingPublishIngress.close;
      yield* runtime.close;
    }),
  );

  it.live("closes TCP publish servers on steady-state server errors", () =>
    Effect.gen(function* () {
      const closed = yield* Deferred.make<void>();
      const server = new Net.Server();
      const listenerCountBefore = server.listenerCount("error");
      installTcpServerSteadyStateErrorHandler(
        server,
        Deferred.succeed(closed, undefined).pipe(Effect.asVoid),
      );
      const listenerCountAfter = server.listenerCount("error");

      server.emit("error", new Error("tcp test steady-state failure"));
      yield* Deferred.await(closed).pipe(Effect.timeout("1 second"));

      expect({
        listenerCountAfter,
        listenerCountBefore,
      }).toStrictEqual({
        listenerCountAfter: 1,
        listenerCountBefore: 0,
      });
      yield* Effect.sync(() => server.removeAllListeners());
    }),
  );

  it.live("fails startup when the TCP publish port is already bound", () =>
    Effect.gen(function* () {
      const reserved = yield* Effect.acquireRelease(reserveTcpPort(), ({ server }) =>
        closeTestTcpServer(server),
      );
      const exit = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        tcpPublishPort: reserved.port,
        websocketPort: 0,
      }).pipe(Effect.exit);

      expect(
        Exit.isFailure(exit)
          ? Option.match(Cause.findErrorOption(exit.cause), {
              onNone: () => null,
              onSome: (error) => ({
                message: error instanceof Error ? error.message : undefined,
                phase: error instanceof ViewServerTcpPublishIngressError ? error.phase : undefined,
                tag: error instanceof ViewServerTcpPublishIngressError ? error._tag : undefined,
              }),
            })
          : null,
      ).toStrictEqual({
        message: "TCP publish server failed to listen.",
        phase: "listen",
        tag: "ViewServerTcpPublishIngressError",
      });
    }),
  );

  it.live("closes the TCP publish endpoint with runtime shutdown", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const tcpPublishUrl = yield* Effect.fromNullishOr(runtime.tcpPublishUrl);
      const response = yield* sendTcpPublishCommand(tcpPublishUrl, {
        op: "publish",
        topic: "orders",
        row: order("a", 10),
      });

      expect(response).toStrictEqual({ ok: true });

      const heldSocket = yield* connectTcpPublishSocket(tcpPublishUrl);
      yield* runtime.close;
      heldSocket.destroy();
      const connectExit = yield* connectTcpPublishSocket(tcpPublishUrl).pipe(Effect.exit);
      expect(Exit.isFailure(connectExit)).toBe(true);
    }),
  );

  it.live("supports default paths and queue capacity options", () =>
    Effect.gen(function* () {
      const defaultRuntime = yield* makeViewServerRuntime(viewServer);
      expect(defaultRuntime.url.endsWith("/rpc")).toBe(true);
      expect(defaultRuntime.healthUrl.endsWith("/health")).toBe(true);
      expect(defaultRuntime.metricsUrl.endsWith("/metrics")).toBe(true);
      expect("subscribeRuntime" in defaultRuntime.liveClient).toBe(false);
      yield* defaultRuntime.close;

      const configuredRuntime = yield* makeViewServerRuntime(viewServer, {
        websocketPort: 0,
        tcpPublishPort: 0,
        subscriptionQueueCapacity: 1,
      });
      expect(configuredRuntime.url.endsWith("/rpc")).toBe(true);
      expect(configuredRuntime.healthUrl.endsWith("/health")).toBe(true);
      expect(configuredRuntime.metricsUrl.endsWith("/metrics")).toBe(true);
      const configuredTcpPublishUrl = yield* Effect.fromNullishOr(configuredRuntime.tcpPublishUrl);
      expect(configuredTcpPublishUrl.startsWith("tcp://")).toBe(true);
      expect([
        tcpPublishUrl({ address: "127.0.0.1", port: 1234 }),
        tcpPublishUrl({ address: "::1", port: 1234 }),
        tcpPublishUrl({ address: "::", port: 1234 }),
      ]).toStrictEqual(["tcp://127.0.0.1:1234", "tcp://[::1]:1234", "tcp://[::]:1234"]);
      yield* configuredRuntime.close;
    }),
  );

  it.effect("tracks runtime transport stream health", () =>
    Effect.gen(function* () {
      const transport = makeViewServerRuntimeTransportHealth<typeof viewServer.topics>();
      const engineHealth = {
        status: "ready",
        version: 1,
        topics: {
          orders: {
            status: "ready",
            rowCount: 10,
            liveRowCount: 10,
            deletedRowCount: 0,
            version: 3,
            lastMutationAt: 1,
            mutationsPerSecond: 2,
            rowsPerSecond: 2,
            pendingMutationBatches: 0,
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: 1,
            activeViews: 1,
            groupedFullEvaluationCount: 0,
            groupedPatchedEvaluationCount: 0,
            activeSubscriptions: 4,
            queuedEvents: 5,
            maxQueueDepth: 6,
            backpressureEvents: 7,
            memoryBytes: 8,
            tombstoneCount: 0,
            compactionPending: false,
          },
        },
        activeSubscriptions: 4,
        queuedEvents: 5,
        maxQueueDepth: 6,
        backpressureEvents: 7,
      } satisfies ColumnLiveViewEngineHealth<typeof viewServer.topics>;

      expect(transport.transportHealth(engineHealth).activeStreams).toBe(0);
      expect(transport.transportHealth(engineHealth).activeClients).toBe(0);
      yield* transport.clientOpened;
      yield* transport.streamOpened;
      yield* transport.streamOpened;
      expect(transport.transportHealth(engineHealth)).toStrictEqual({
        activeClients: 1,
        activeStreams: 2,
        activeSubscriptions: 4,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        queuedMessages: 5,
        queuedBytes: 0,
        droppedClients: 0,
        backpressureEvents: 7,
        reconnects: 0,
        lastError: null,
      });
      yield* transport.streamClosed;
      yield* transport.streamClosed;
      yield* transport.streamClosed;
      expect(transport.transportHealth(engineHealth).activeStreams).toBe(0);
      expect(transport.transportHealth(engineHealth).activeClients).toBe(1);
      yield* transport.clientClosed;
      yield* transport.clientClosed;
      expect(transport.transportHealth(engineHealth).activeClients).toBe(0);
    }),
  );

  it.live("forwards runtime options to the runtime core and websocket server", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<typeof viewServer.topics>;
      let runtimeCoreOptions: Parameters<RuntimeDependencies["makeRuntimeCore"]>[1] | undefined;
      let serverInput: Parameters<RuntimeDependencies["makeServer"]>[1] | undefined;
      let serverOptions: Parameters<RuntimeDependencies["makeServer"]>[2] | undefined;
      let tcpPublishOptions:
        | Parameters<RuntimeDependencies["makeTcpPublishIngress"]>[2]
        | undefined;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeRuntimeCore: (config, options) => {
          runtimeCoreOptions = options;
          return makeViewServerRuntimeCoreInternal(config, options);
        },
        makeServer: (_config, input, options) => {
          serverInput = input;
          serverOptions = options;
          return Effect.succeed({
            url: "ws://127.0.0.1:0/custom-rpc",
            healthUrl: "http://127.0.0.1:0/custom-health",
            metricsUrl: "http://127.0.0.1:0/custom-metrics",
            close: Effect.void,
          });
        },
        makeTcpPublishIngress: (_config, _client, options) => {
          tcpPublishOptions = options;
          return Effect.succeed({
            url: `tcp://${options.host ?? "127.0.0.1"}:${options.port}`,
            close: Effect.void,
          });
        },
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
        auth: bearerAuth,
        groupedIncrementalAdmissionLimits: {
          maxGroups: 1,
        },
        host: "0.0.0.0",
        websocketPort: 1234,
        tcpPublishHost: "127.0.0.1",
        tcpPublishMaxConnections: 9,
        tcpPublishPort: 1235,
        rpcPath: "/custom-rpc",
        healthPath: "/custom-health",
        metricsPath: "/custom-metrics",
        subscriptionQueueCapacity: 7,
      });

      expect({
        runtimeCoreOptions: {
          subscriptionQueueCapacity: runtimeCoreOptions?.subscriptionQueueCapacity,
          groupedIncrementalAdmissionLimits: runtimeCoreOptions?.groupedIncrementalAdmissionLimits,
          transportHealthType: typeof runtimeCoreOptions?.transportHealth,
        },
        serverTransportHooks: {
          clientOpenedType: typeof serverInput?.transport?.clientOpened,
          clientClosedType: typeof serverInput?.transport?.clientClosed,
          streamOpenedType: typeof serverInput?.transport?.streamOpened,
          streamClosedType: typeof serverInput?.transport?.streamClosed,
        },
        serverAuthType: typeof serverInput?.auth?.validateRequest,
        serverOptions,
        tcpPublishAuthType: typeof tcpPublishOptions?.auth?.validateRequest,
        tcpPublishOptions,
        tcpPublishUrl: runtime.tcpPublishUrl,
      }).toStrictEqual({
        runtimeCoreOptions: {
          subscriptionQueueCapacity: 7,
          groupedIncrementalAdmissionLimits: {
            maxGroups: 1,
          },
          transportHealthType: "function",
        },
        serverTransportHooks: {
          clientOpenedType: "object",
          clientClosedType: "object",
          streamOpenedType: "object",
          streamClosedType: "object",
        },
        serverAuthType: "function",
        serverOptions: {
          host: "0.0.0.0",
          port: 1234,
          path: "/custom-rpc",
          healthPath: "/custom-health",
          metricsPath: "/custom-metrics",
        },
        tcpPublishAuthType: "function",
        tcpPublishOptions: {
          auth: bearerAuth,
          host: "127.0.0.1",
          maxConnections: 9,
          port: 1235,
          rejectedTopics: new Set(),
        },
        tcpPublishUrl: "tcp://127.0.0.1:1235",
      });
      yield* runtime.close;
    }),
  );

  it.live("forwards runtime auth validation to operational HTTP endpoints", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        auth: bearerAuth,
      });

      const health = yield* fetchJson(runtime.healthUrl);
      const metrics = yield* fetchJson(runtime.metricsUrl);

      expect(health.response.status).toBe(401);
      expect(health.value).toStrictEqual({
        _tag: "ViewServerAuthError",
        message: "Missing or invalid authorization header.",
      });
      expect(metrics.response.status).toBe(401);
      expect(metrics.value).toStrictEqual({
        _tag: "ViewServerAuthError",
        message: "Missing or invalid authorization header.",
      });

      yield* runtime.close;
    }),
  );

  it.live("resolves Kafka runtime options and starts configured ingress", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<typeof viewServer.topics>;
      const regions = {
        local: Config.succeed("localhost:9092"),
      };
      let kafkaOptionsSummary:
        | {
            readonly consume: ResolvedViewServerKafkaRuntimeOptions<
              typeof viewServer.topics
            >["consume"];
            readonly consumerGroupId: string;
            readonly regions: Readonly<Record<string, string>>;
            readonly startFrom: ResolvedViewServerKafkaRuntimeOptions<
              typeof viewServer.topics
            >["startFrom"];
            readonly topics: Readonly<
              Record<
                string,
                { readonly regions: ReadonlyArray<string>; readonly viewServerTopic: string }
              >
            >;
          }
        | undefined;
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.void,
          }),
        makeKafkaIngress: (_config, _client, _requestHealthRefresh, options) => {
          kafkaOptionsSummary = {
            consume: options.consume,
            consumerGroupId: options.consumerGroupId,
            regions: options.regions,
            startFrom: options.startFrom,
            topics: Object.fromEntries(
              Object.entries(options.topics).map(([sourceTopic, topic]) => [
                sourceTopic,
                {
                  regions: topic.regions,
                  viewServerTopic: topic.viewServerTopic,
                },
              ]),
            ),
          };
          return Effect.succeed({
            close: Effect.void,
          });
        },
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
        kafka: {
          consumerGroupId: "view-server-test-runtime",
          regions,
          topics: {
            "orders-source": localKafkaTopic({
              regions: ["local"],
              value: kafka.json(Order),
              key: kafka.stringKey(),
              viewServerTopic: "orders",
              getSafeRowKey: ({ key }) => key,
              mapping: ({ key, value }) => ({
                id: key,
                price: value.price,
              }),
            }),
          },
        },
      });

      expect({
        consume: kafkaOptionsSummary?.consume,
        consumerGroupId: kafkaOptionsSummary?.consumerGroupId,
        regions: kafkaOptionsSummary?.regions,
        startFrom: kafkaOptionsSummary?.startFrom,
        topics: kafkaOptionsSummary?.topics,
      }).toStrictEqual({
        consume: {
          consumerGroupId: "view-server-test-runtime",
          fallbackMode: "earliest",
          mode: "committed",
        },
        consumerGroupId: "view-server-test-runtime",
        regions: nullRecord([["local", "localhost:9092"]]),
        startFrom: {
          committedConsumerGroup: "view-server-test-runtime",
        },
        topics: {
          "orders-source": {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });

      yield* runtime.close;
    }),
  );

  it.live("resolves gRPC runtime options and starts configured materialized ingress", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<GrpcTopics>;
      const feed = grpcMaterializedFeed(Stream.never);
      let grpcOptionsSummary:
        | {
            readonly clientBaseUrls: Readonly<Record<string, string>>;
            readonly clientNames: ReadonlyArray<string>;
            readonly feeds: Readonly<
              Record<
                string,
                {
                  readonly client: string;
                  readonly lifecycle: string;
                  readonly method: string;
                  readonly topic: string;
                }
              >
            >;
            readonly materializedReconnect: ResolvedViewServerGrpcRuntimeOptions<GrpcTopics>["materializedReconnect"];
          }
        | undefined;
      let grpcHealthLedgerCreated = false;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<GrpcTopics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.void,
          }),
        makeGrpcHealthLedger: (config, options) => {
          grpcHealthLedgerCreated =
            config === grpcViewServer &&
            options.clientBaseUrls["orders"] === "https://orders.example.test";
          return makeDefaultRuntimeDependencies<GrpcTopics>().makeGrpcHealthLedger(config, options);
        },
        makeGrpcIngress: (_config, _client, _requestHealthRefresh, options) => {
          grpcOptionsSummary = {
            clientBaseUrls: options.clientBaseUrls,
            clientNames: Object.keys(options.clients),
            feeds: Object.fromEntries(
              Object.entries(options.feeds).map(([feedName, resolvedFeed]) => [
                feedName,
                {
                  client: resolvedFeed.client,
                  lifecycle: resolvedFeed.lifecycle,
                  method: resolvedFeed.method,
                  topic: resolvedFeed.topic,
                },
              ]),
            ),
            materializedReconnect: options.materializedReconnect,
          };
          return Effect.succeed({
            close: Effect.void,
          });
        },
      };

      const grpcRuntimeOptions = {
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersFeed: feed,
          },
          materializedReconnect: {
            delay: "100 millis",
            maxReconnects: 5,
          },
        },
      };
      const runtime = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        grpcViewServer,
        grpcRuntimeOptions,
      );

      expect(grpcHealthLedgerCreated).toBe(true);
      expect({
        clientBaseUrls: grpcOptionsSummary?.clientBaseUrls,
        clientNames: grpcOptionsSummary?.clientNames,
        feeds: grpcOptionsSummary?.feeds,
        materializedReconnect: grpcOptionsSummary?.materializedReconnect,
      }).toStrictEqual({
        clientBaseUrls: nullRecord([["orders", "https://orders.example.test"]]),
        clientNames: ["orders"],
        feeds: {
          ordersFeed: {
            client: "orders",
            lifecycle: "materialized",
            method: "streamOrders",
            topic: "orders",
          },
        },
        materializedReconnect: {
          delay: "100 millis",
          maxReconnects: 5,
        },
      });

      yield* runtime.close;
    }),
  );

  it.live("rejects multiple gRPC feeds targeting the same View Server topic", () =>
    Effect.gen(function* () {
      const firstFeed = grpcMaterializedFeed(Stream.never);
      const secondFeed = grpcFeedWithOrphan.materializedFeed({
        topic: "orders",
        client: "orphan",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => Stream.never,
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });

      const error = yield* makeViewServerRuntime(grpcViewServer, {
        grpc: {
          clients: grpcClientsWithOrphan,
          feeds: {
            ordersFeed: firstFeed,
            secondOrdersFeed: secondFeed,
          },
        },
      }).pipe(Effect.flip);

      expect(error).toBeInstanceOf(ViewServerGrpcIngressError);
      expect(error.message).toBe(
        "gRPC feed secondOrdersFeed conflicts with ordersFeed; View Server topic orders already has a gRPC feed owner.",
      );
    }),
  );

  it.live("accepts leased gRPC feeds for runtime lease management", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedFeed({
        streamForRegion: () => Stream.never,
      });

      const options = yield* resolveViewServerRuntimeOptions<
        typeof leasedGrpcViewServer.topics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersLease: feed,
          },
        },
      });

      expect(options.grpcOptions?.feeds["ordersLease"]).toBe(feed);
    }),
  );

  it.live("rejects invalid materialized gRPC reconnect maxReconnects", () =>
    Effect.gen(function* () {
      const error = yield* makeViewServerRuntime(grpcViewServer, {
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersFeed: grpcMaterializedFeed(Stream.never),
          },
          materializedReconnect: {
            delay: "10 millis",
            maxReconnects: Infinity,
          },
        },
      }).pipe(Effect.flip);

      expect(error).toBeInstanceOf(ViewServerGrpcIngressError);
      expect(error).toStrictEqual(
        new ViewServerGrpcIngressError({
          message:
            "gRPC materialized reconnect maxReconnects must be a finite non-negative integer.",
          cause: Infinity,
          phase: "configuration",
        }),
      );
    }),
  );

  it.live("rejects invalid materialized gRPC reconnect delay", () =>
    Effect.gen(function* () {
      const error = yield* makeViewServerRuntime(grpcViewServer, {
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersFeed: grpcMaterializedFeed(Stream.never),
          },
          materializedReconnect: {
            delay: "Infinity",
            maxReconnects: 3,
          },
        },
      }).pipe(Effect.flip);

      expect(error).toBeInstanceOf(ViewServerGrpcIngressError);
      expect(error).toStrictEqual(
        new ViewServerGrpcIngressError({
          message: "gRPC materialized reconnect delay must be finite and positive.",
          cause: "Infinity",
          phase: "configuration",
        }),
      );
    }),
  );

  it.live("rejects zero materialized gRPC reconnect delay", () =>
    Effect.gen(function* () {
      const error = yield* makeViewServerRuntime(grpcViewServer, {
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersFeed: grpcMaterializedFeed(Stream.never),
          },
          materializedReconnect: {
            delay: 0,
            maxReconnects: 3,
          },
        },
      }).pipe(Effect.flip);

      expect(error).toBeInstanceOf(ViewServerGrpcIngressError);
      expect(error).toStrictEqual(
        new ViewServerGrpcIngressError({
          message: "gRPC materialized reconnect delay must be finite and positive.",
          cause: 0,
          phase: "configuration",
        }),
      );
    }),
  );

  it.live("keeps refined public leased row keys out of internal storage keys", () =>
    Effect.gen(function* () {
      const feed = grpcPublicKeyLeasedFeed({
        streamForRegion: (region) =>
          longRunningGrpcStream([grpcOrderValue(`${region}-order-1`, 10)]),
      });
      const options = yield* resolveViewServerRuntimeOptions<
        typeof publicKeyLeasedGrpcViewServer.topics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersLease: feed,
          },
        },
      });
      const grpcOptions = yield* Effect.fromNullishOr(options.grpcOptions);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(
        publicKeyLeasedGrpcViewServer,
        {},
      );
      const health = makeViewServerGrpcHealthLedger<typeof publicKeyLeasedGrpcViewServer.topics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        publicKeyLeasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const firstSubscription = yield* manager.liveClient.subscribe("orders", {
        select: ["id", "customerId", "price", "region"],
        where: {
          region: { eq: "usa" },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      const subscription = yield* manager.liveClient.subscribe("orders", {
        select: ["id", "customerId", "price", "region"],
        where: {
          region: { eq: "usa" },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(Array.from(events)).toStrictEqual([
        {
          type: "snapshot",
          topic: "orders",
          queryId: "query-1",
          version: 1,
          keys: ["public-usa-usa-order-1"],
          rows: [
            {
              id: "public-usa-usa-order-1",
              customerId: "usa-order-1",
              price: 10,
              region: "usa",
            },
          ],
          totalRows: 1,
        },
      ]);
      yield* subscription.close();
      yield* firstSubscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("releases an interrupted same-route leased subscriber acquisition", () =>
    Effect.gen(function* () {
      let released = 0;
      const feed = grpcLeasedFeed({
        release: Effect.sync(() => {
          released += 1;
        }),
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const secondSubscribeStarted = yield* Deferred.make<void>();
      const internalSubscription = {
        events: Stream.never,
        close: () => Effect.void,
      };
      const subscribeResults =
        yield* Queue.unbounded<
          Effect.Effect<typeof internalSubscription, ViewServerRuntimeError>
        >();
      yield* Queue.offer(subscribeResults, Effect.succeed(internalSubscription));
      yield* Queue.offer(
        subscribeResults,
        Effect.gen(function* () {
          yield* Deferred.succeed(secondSubscribeStarted, undefined);
          return yield* Effect.never;
        }),
      );
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        subscribeInternal: () => Queue.take(subscribeResults).pipe(Effect.flatten),
        subscribeRuntimeInternal: runtimeCore.internalLiveClient.subscribeRuntimeInternal,
      };
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        fakeInternalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const firstSubscription = yield* manager.liveClient.subscribe(
        "orders",
        leasedOrdersQuery("usa"),
      );
      const secondFiber = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.forkChild);
      yield* Deferred.await(secondSubscribeStarted);
      yield* Fiber.interrupt(secondFiber);
      const activeHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect({
        released,
        subscriberCount:
          activeHealth.grpc?.feeds["orders"]?.leased[
            "orders/ordersLease/leased/region=string%3A3%3Ausa"
          ]?.subscriberCount,
      }).toStrictEqual({
        released: 0,
        subscriberCount: 1,
      });
      yield* firstSubscription.close();
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 1_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {}).length === 0,
        }),
      );

      expect({
        released,
        leasedFeeds: cleanedHealth.grpc?.feeds["orders"]?.leased ?? {},
      }).toStrictEqual({
        released: 1,
        leasedFeeds: {},
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects public one-shot snapshots for leased gRPC topics", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<typeof leasedGrpcViewServer.topics>;
      const feed = grpcLeasedFeed({
        streamForRegion: () => Stream.never,
      });
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof leasedGrpcViewServer.topics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.void,
          }),
      };
      const runtime = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        leasedGrpcViewServer,
        {
          grpc: {
            clients: grpcClients,
            feeds: {
              ordersLease: feed,
            },
          },
        },
      );

      const snapshot: (
        topic: string,
        query: unknown,
      ) => Effect.Effect<unknown, ViewServerRuntimeError> = Object.getOwnPropertyDescriptor(
        runtime.client,
        "snapshot",
      )?.value;
      expect(typeof snapshot).toBe("function");
      const error = yield* snapshot("orders", leasedOrdersQuery("usa")).pipe(Effect.flip);

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "UnsupportedQuery",
        topic: "orders",
        message:
          "Leased gRPC topics do not support direct runtime mutations or one-shot snapshots; use a live subscription so the runtime can own lease lifecycle.",
      });
      yield* runtime.close;
    }),
  );

  it.live("rejects direct runtime mutations for leased gRPC topics", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<typeof leasedGrpcViewServer.topics>;
      const feed = grpcLeasedFeed({
        streamForRegion: () => Stream.never,
      });
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof leasedGrpcViewServer.topics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.void,
          }),
      };
      const runtime = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        leasedGrpcViewServer,
        {
          grpc: {
            clients: grpcClients,
            feeds: {
              ordersLease: feed,
            },
          },
        },
      );
      const publish: (topic: string, row: unknown) => Effect.Effect<void, ViewServerRuntimeError> =
        Object.getOwnPropertyDescriptor(runtime.client, "publish")?.value;
      const publishMany: (
        topic: string,
        rows: ReadonlyArray<unknown>,
      ) => Effect.Effect<void, ViewServerRuntimeError> = Object.getOwnPropertyDescriptor(
        runtime.client,
        "publishMany",
      )?.value;
      const patch: (
        topic: string,
        key: string,
        patchValue: unknown,
      ) => Effect.Effect<void, ViewServerRuntimeError> = Object.getOwnPropertyDescriptor(
        runtime.client,
        "patch",
      )?.value;
      const deleteRow: (topic: string, key: string) => Effect.Effect<void, ViewServerRuntimeError> =
        Object.getOwnPropertyDescriptor(runtime.client, "delete")?.value;
      expect(typeof publish).toBe("function");
      expect(typeof publishMany).toBe("function");
      expect(typeof patch).toBe("function");
      expect(typeof deleteRow).toBe("function");
      const row = {
        id: "order-1",
        customerId: "customer-1",
        status: "open",
        price: 10,
        region: "usa",
        updatedAt: 10,
      };

      const publishError = yield* publish("orders", row).pipe(Effect.flip);
      const publishManyError = yield* publishMany("orders", [row]).pipe(Effect.flip);
      const patchError = yield* patch("orders", "order-1", { price: 11 }).pipe(Effect.flip);
      const deleteError = yield* deleteRow("orders", "order-1").pipe(Effect.flip);

      expect([publishError, publishManyError, patchError, deleteError]).toStrictEqual([
        {
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          topic: "orders",
          message:
            "Leased gRPC topics do not support direct runtime mutations or one-shot snapshots; use a live subscription so the runtime can own lease lifecycle.",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          topic: "orders",
          message:
            "Leased gRPC topics do not support direct runtime mutations or one-shot snapshots; use a live subscription so the runtime can own lease lifecycle.",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          topic: "orders",
          message:
            "Leased gRPC topics do not support direct runtime mutations or one-shot snapshots; use a live subscription so the runtime can own lease lifecycle.",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          topic: "orders",
          message:
            "Leased gRPC topics do not support direct runtime mutations or one-shot snapshots; use a live subscription so the runtime can own lease lifecycle.",
        },
      ]);
      yield* runtime.close;
    }),
  );

  it.live("delegates direct runtime reset when no leased gRPC topics exist", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeed(Stream.never);
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      yield* manager.client.publish("orders", {
        id: "order-1",
        customerId: "customer-1",
        status: "open",
        price: 10,
        region: "usa",
        updatedAt: 10,
      });
      yield* manager.client.reset();
      const snapshot = yield* manager.client.snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      });

      expect(snapshot).toStrictEqual({
        version: 0,
        rows: [],
        totalRows: 0,
        status: "ready",
        statusCode: "Ready",
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects direct manager reset when leased gRPC topics exist", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedFeed({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const error = yield* manager.client.reset().pipe(Effect.flip);

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "UnsupportedQuery",
        message:
          "Leased gRPC topics do not support direct runtime reset; close the runtime or leased subscriptions so the lease manager owns cleanup.",
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects runtime startup when a declared leased gRPC source has no feed", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<typeof leasedGrpcViewServer.topics>;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof leasedGrpcViewServer.topics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.void,
          }),
      };

      const error = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        leasedGrpcViewServer,
      ).pipe(Effect.flip);

      expect(error).toBeInstanceOf(ViewServerGrpcIngressError);
      expect(error.message).toBe(
        "View Server topic orders declares gRPC leased source but no matching gRPC feed was configured.",
      );
    }),
  );

  it.live("rejects gRPC feeds that target unknown or non-gRPC View Server topics", () =>
    Effect.gen(function* () {
      const unknownTopicFeed = mixedGrpcFeed.materializedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => Stream.never,
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const nonGrpcTopicFeed = mixedGrpcFeed.materializedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => Stream.never,
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      Object.defineProperty(unknownTopicFeed, "topic", { value: "unknown" });
      Object.defineProperty(nonGrpcTopicFeed, "topic", { value: "audit" });

      const unknownTopicOptions = yield* resolveViewServerRuntimeOptions<
        typeof grpcAndKafkaViewServer.topics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersFeed: grpcMaterializedFeed(Stream.never),
            unknownTopicFeed,
          },
        },
      });
      const nonGrpcTopicOptions = yield* resolveViewServerRuntimeOptions<
        typeof grpcAndKafkaViewServer.topics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersFeed: grpcMaterializedFeed(Stream.never),
            nonGrpcTopicFeed,
          },
        },
      });
      const unknownTopicError = yield* validateGrpcSourceFeeds(
        grpcAndKafkaViewServer,
        yield* Effect.fromNullishOr(unknownTopicOptions.grpcOptions),
      ).pipe(Effect.flip);
      const nonGrpcTopicError = yield* validateGrpcSourceFeeds(
        grpcAndKafkaViewServer,
        yield* Effect.fromNullishOr(nonGrpcTopicOptions.grpcOptions),
      ).pipe(Effect.flip);

      expect({
        unknownTopicMessage: unknownTopicError.message,
        unknownTopicFeedName: Reflect.get(unknownTopicError, "feedName"),
        nonGrpcTopicMessage: nonGrpcTopicError.message,
        nonGrpcTopicFeedName: Reflect.get(nonGrpcTopicError, "feedName"),
      }).toStrictEqual({
        unknownTopicMessage:
          "gRPC feed unknownTopicFeed references unknown View Server topic unknown.",
        unknownTopicFeedName: "unknownTopicFeed",
        nonGrpcTopicMessage:
          "gRPC feed nonGrpcTopicFeed targets View Server topic audit, but that topic does not declare a gRPC source.",
        nonGrpcTopicFeedName: "nonGrpcTopicFeed",
      });
    }),
  );

  it.live("rejects gRPC feed lifecycle mismatches from the feed validation boundary", () =>
    Effect.gen(function* () {
      const materializedFeed = grpcMaterializedFeed(Stream.never);
      const leasedFeed = grpcLeasedFeed({
        streamForRegion: () => Stream.never,
      });
      Object.defineProperty(materializedFeed, "lifecycle", { value: "leased" });
      Object.defineProperty(leasedFeed, "lifecycle", { value: "materialized" });

      const materializedTopicLeasedFeedOptions = yield* resolveViewServerRuntimeOptions<
        typeof grpcViewServer.topics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            materializedFeed,
          },
        },
      });
      const leasedTopicMaterializedFeedOptions = yield* resolveViewServerRuntimeOptions<
        typeof leasedGrpcViewServer.topics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            leasedFeed,
          },
        },
      });
      const materializedTopicLeasedFeedError = yield* validateGrpcSourceFeeds(
        grpcViewServer,
        yield* Effect.fromNullishOr(materializedTopicLeasedFeedOptions.grpcOptions),
      ).pipe(Effect.flip);
      const leasedTopicMaterializedFeedError = yield* validateGrpcSourceFeeds(
        leasedGrpcViewServer,
        yield* Effect.fromNullishOr(leasedTopicMaterializedFeedOptions.grpcOptions),
      ).pipe(Effect.flip);

      expect({
        materializedTopicMessage: materializedTopicLeasedFeedError.message,
        materializedTopicFeedName: Reflect.get(materializedTopicLeasedFeedError, "feedName"),
        leasedTopicMessage: leasedTopicMaterializedFeedError.message,
        leasedTopicFeedName: Reflect.get(leasedTopicMaterializedFeedError, "feedName"),
      }).toStrictEqual({
        materializedTopicMessage:
          "gRPC feed materializedFeed lifecycle leased does not match View Server topic orders source lifecycle materialized.",
        materializedTopicFeedName: "materializedFeed",
        leasedTopicMessage:
          "gRPC feed leasedFeed lifecycle materialized does not match View Server topic orders source lifecycle leased.",
        leasedTopicFeedName: "leasedFeed",
      });
    }),
  );

  it.live("rejects leased gRPC feed routeBy mismatches from the feed validation boundary", () =>
    Effect.gen(function* () {
      const localViewServer = defineViewServerConfig({
        topics: {
          orders: {
            schema: GrpcOrder,
            key: "id",
            source: grpc.leased({
              routeBy: ["region", "status"],
            }),
          },
        },
      });
      const localGrpcFeed = localViewServer.grpcFeed<typeof grpcClients>();
      const feed = localGrpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["region", "status"],
        request: ({ region, status }) => ({ orderId: `${region}:${status}` }),
        acquire: () => Stream.never,
        map: ({ value, route }) => ({
          id: `${route.region}:${value.customerId}`,
          customerId: value.customerId,
          status: route.status,
          price: value.price,
          region: route.region,
          updatedAt: value.updatedAt,
        }),
      });
      Object.defineProperty(feed, "routeBy", {
        value: ["status", "region"],
      });
      const invalidFeedRouteBy = localGrpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["region", "status"],
        request: ({ region, status }) => ({ orderId: `${region}:${status}` }),
        acquire: () => Stream.never,
        map: ({ value, route }) => ({
          id: `${route.region}:${value.customerId}`,
          customerId: value.customerId,
          status: route.status,
          price: value.price,
          region: route.region,
          updatedAt: value.updatedAt,
        }),
      });
      Object.defineProperty(invalidFeedRouteBy, "routeBy", {
        value: ["region", 1],
      });
      const invalidSourceRouteByViewServer = defineViewServerConfig({
        topics: {
          orders: {
            schema: GrpcOrder,
            key: "id",
            source: grpc.leased({
              routeBy: ["region", "status"],
            }),
          },
        },
      });
      Object.defineProperty(invalidSourceRouteByViewServer.topics.orders.source, "routeBy", {
        value: ["region", 1],
      });
      const resolvedOptions = yield* resolveViewServerRuntimeOptions<
        typeof localViewServer.topics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersLease: feed,
          },
        },
      });
      const resolvedGrpcOptions = yield* Effect.fromNullishOr(resolvedOptions.grpcOptions);
      const error = yield* validateGrpcSourceFeeds(localViewServer, resolvedGrpcOptions).pipe(
        Effect.flip,
      );
      const invalidFeedRouteByOptions = {
        ...resolvedGrpcOptions,
        feeds: {
          ordersLease: invalidFeedRouteBy,
        },
      };
      const invalidFeedRouteByError = yield* validateGrpcSourceFeeds(
        localViewServer,
        invalidFeedRouteByOptions,
      ).pipe(Effect.flip);
      const invalidSourceRouteByError = yield* validateGrpcSourceFeeds(
        invalidSourceRouteByViewServer,
        resolvedGrpcOptions,
      ).pipe(Effect.flip);

      expect({
        message: error.message,
        feedName: Reflect.get(error, "feedName"),
        invalidFeedRouteByMessage: invalidFeedRouteByError.message,
        invalidSourceRouteByMessage: invalidSourceRouteByError.message,
      }).toStrictEqual({
        message:
          "gRPC leased feed ordersLease routeBy status, region does not match View Server topic orders source routeBy region, status.",
        feedName: "ordersLease",
        invalidFeedRouteByMessage:
          "gRPC leased feed ordersLease routeBy  does not match View Server topic orders source routeBy region, status.",
        invalidSourceRouteByMessage:
          "gRPC leased feed ordersLease routeBy status, region does not match View Server topic orders source routeBy .",
      });
    }),
  );

  it.live("rejects gRPC feeds when topic source metadata is malformed", () =>
    Effect.gen(function* () {
      const nullTopicConfig = defineViewServerConfig({
        topics: {
          orders: {
            schema: GrpcOrder,
            key: "id",
            source: grpc.materialized(),
          },
        },
      });
      Object.defineProperty(nullTopicConfig.topics, "orders", { value: null });
      const nonGrpcKindConfig = defineViewServerConfig({
        topics: {
          orders: {
            schema: GrpcOrder,
            key: "id",
            source: grpc.materialized(),
          },
        },
      });
      Object.defineProperty(nonGrpcKindConfig.topics.orders.source, "kind", {
        value: "not-grpc",
      });
      const invalidLifecycleConfig = defineViewServerConfig({
        topics: {
          orders: {
            schema: GrpcOrder,
            key: "id",
            source: grpc.materialized(),
          },
        },
      });
      Object.defineProperty(invalidLifecycleConfig.topics.orders.source, "lifecycle", {
        value: "invalid-lifecycle",
      });
      const feed = grpcMaterializedFeed(Stream.never);
      const grpcOptions: ResolvedViewServerGrpcRuntimeOptions<
        typeof grpcViewServer.topics,
        typeof grpcClients
      > = {
        clients: grpcClients,
        clientBaseUrls: nullRecord([["orders", "https://orders.example.test"]]),
        feeds: {
          ordersFeed: feed,
        },
        materializedReconnect: fastGrpcMaterializedReconnect,
      };
      const nullTopicError = yield* validateGrpcSourceFeeds(nullTopicConfig, grpcOptions).pipe(
        Effect.flip,
      );
      const nonGrpcKindError = yield* validateGrpcSourceFeeds(nonGrpcKindConfig, grpcOptions).pipe(
        Effect.flip,
      );
      const invalidLifecycleError = yield* validateGrpcSourceFeeds(
        invalidLifecycleConfig,
        grpcOptions,
      ).pipe(Effect.flip);

      expect({
        nullTopicMessage: nullTopicError.message,
        nullTopicFeedName: Reflect.get(nullTopicError, "feedName"),
        nonGrpcKindMessage: nonGrpcKindError.message,
        nonGrpcKindFeedName: Reflect.get(nonGrpcKindError, "feedName"),
        invalidLifecycleMessage: invalidLifecycleError.message,
        invalidLifecycleFeedName: Reflect.get(invalidLifecycleError, "feedName"),
      }).toStrictEqual({
        nullTopicMessage:
          "gRPC feed ordersFeed targets View Server topic orders, but that topic does not declare a gRPC source.",
        nullTopicFeedName: "ordersFeed",
        nonGrpcKindMessage:
          "gRPC feed ordersFeed targets View Server topic orders, but that topic does not declare a gRPC source.",
        nonGrpcKindFeedName: "ordersFeed",
        invalidLifecycleMessage:
          "gRPC feed ordersFeed targets View Server topic orders, but that topic does not declare a gRPC source.",
        invalidLifecycleFeedName: "ordersFeed",
      });
    }),
  );

  it.live(
    "rejects gRPC feed lifecycle mismatch when validation sees inconsistent resolved options",
    () =>
      Effect.gen(function* () {
        const feed = grpcMaterializedFeed(Stream.never);
        Object.defineProperty(feed, "lifecycle", { value: "leased" });
        const grpcOptions: ResolvedViewServerGrpcRuntimeOptions<
          typeof grpcViewServer.topics,
          typeof grpcClients
        > = {
          clients: grpcClients,
          clientBaseUrls: nullRecord([["orders", "https://orders.example.test"]]),
          feeds: {
            ordersFeed: feed,
          },
          materializedReconnect: fastGrpcMaterializedReconnect,
        };

        const error = yield* validateGrpcSourceFeeds(grpcViewServer, grpcOptions).pipe(Effect.flip);

        expect({
          message: error.message,
          feedName: Reflect.get(error, "feedName"),
        }).toStrictEqual({
          message:
            "gRPC feed ordersFeed lifecycle leased does not match View Server topic orders source lifecycle materialized.",
          feedName: "ordersFeed",
        });
      }),
  );

  it.effect("rejects resolved Kafka and gRPC ownership of the same View Server topic", () =>
    Effect.gen(function* () {
      const error = yield* validateSourceOwnership(
        {
          topics: {
            "orders-source": {
              viewServerTopic: "orders",
            },
          },
        },
        {
          feeds: {
            ordersFeed: {
              topic: "orders",
            },
          },
        },
      ).pipe(Effect.flip);

      expect(error).toBeInstanceOf(ViewServerGrpcIngressError);
      expect(error.message).toBe(
        "View Server topic orders cannot be owned by both Kafka source orders-source and gRPC feed ordersFeed.",
      );
    }),
  );

  it.live("closes started resources when gRPC ingress startup fails", () =>
    Effect.gen(function* () {
      type MixedTopics = typeof grpcAndKafkaViewServer.topics;
      type RuntimeDependencies = ViewServerRuntimeDependencies<MixedTopics>;
      const feed = mixedGrpcFeed.materializedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => Stream.never,
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const regions = {
        local: "localhost:9092",
      };
      const localKafkaTopic = grpcAndKafkaViewServer.kafkaTopic<typeof regions>();
      let serverClosed = 0;
      let kafkaClosed = 0;
      let runtimeCoreClosed = 0;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<MixedTopics>(),
        makeRuntimeCore: (config, options) =>
          makeDefaultRuntimeDependencies<MixedTopics>()
            .makeRuntimeCore(config, options)
            .pipe(
              Effect.map((runtimeCore) => ({
                ...runtimeCore,
                close: runtimeCore.close.pipe(
                  Effect.andThen(
                    Effect.sync(() => {
                      runtimeCoreClosed += 1;
                    }),
                  ),
                ),
              })),
            ),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.sync(() => {
              serverClosed += 1;
            }),
          }),
        makeKafkaIngress: () =>
          Effect.succeed({
            close: Effect.sync(() => {
              kafkaClosed += 1;
            }),
          }),
        makeGrpcIngress: () =>
          Effect.fail(
            new ViewServerGrpcIngressError({
              message: "gRPC failed during startup",
              cause: "startup",
            }),
          ),
      };

      const exit = yield* Effect.exit(
        makeViewServerRuntimeWithDependencies(dependencies, grpcAndKafkaViewServer, {
          kafka: {
            consumerGroupId: "view-server-grpc-startup-failure",
            regions,
            topics: {
              "orders-source": localKafkaTopic({
                regions: ["local"],
                value: kafka.json(Order),
                key: kafka.stringKey(),
                viewServerTopic: "audit",
                getSafeRowKey: ({ key }) => key,
                mapping: ({ key, value }) => ({
                  id: key,
                  price: value.price,
                }),
              }),
            },
          },
          grpc: {
            clients: grpcClients,
            feeds: {
              ordersFeed: feed,
            },
          },
        }),
      );

      expect({
        failed: Exit.isFailure(exit),
        kafkaClosed,
        runtimeCoreClosed,
        serverClosed,
      }).toStrictEqual({
        failed: true,
        kafkaClosed: 1,
        runtimeCoreClosed: 1,
        serverClosed: 1,
      });
    }),
  );

  it.live("closes server and runtime core when gRPC ingress startup fails without Kafka", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<GrpcTopics>;
      const feed = grpcMaterializedFeed(Stream.never);
      let serverClosed = 0;
      let runtimeCoreClosed = 0;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<GrpcTopics>(),
        makeRuntimeCore: (config, options) =>
          makeDefaultRuntimeDependencies<GrpcTopics>()
            .makeRuntimeCore(config, options)
            .pipe(
              Effect.map((runtimeCore) => ({
                ...runtimeCore,
                close: runtimeCore.close.pipe(
                  Effect.andThen(
                    Effect.sync(() => {
                      runtimeCoreClosed += 1;
                    }),
                  ),
                ),
              })),
            ),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.sync(() => {
              serverClosed += 1;
            }),
          }),
        makeGrpcIngress: () =>
          Effect.fail(
            new ViewServerGrpcIngressError({
              message: "gRPC failed before Kafka existed",
              cause: "startup",
            }),
          ),
      };

      const exit = yield* Effect.exit(
        makeViewServerRuntimeWithDependencies(dependencies, grpcViewServer, {
          grpc: {
            clients: grpcClients,
            feeds: {
              ordersFeed: feed,
            },
          },
        }),
      );

      expect({
        failed: Exit.isFailure(exit),
        runtimeCoreClosed,
        serverClosed,
      }).toStrictEqual({
        failed: true,
        runtimeCoreClosed: 1,
        serverClosed: 1,
      });
    }),
  );

  it.live("tracks gRPC materialized feed health and same-window rate increments", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeViewServerGrpcHealthLedger<GrpcTopics>({
        clients: {
          orders: "https://orders.example.test",
        },
        feeds: {
          ordersFeed: {
            client: "orders",
            lifecycle: "materialized",
            topic: "orders",
          },
        },
      });

      const startingHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);
      yield* health.clientConnected("missing", 1_000);
      yield* health.clientDegraded("missing", "ignored");
      yield* health.feedReady("missing");
      yield* health.feedStopping("missing");
      yield* health.feedDegraded("missing", "ignored");
      yield* health.rowsPublished("missing", {
        messages: 1,
        rows: 1,
        nowMillis: 2_000,
      });
      yield* health.mappingFailed("missing", {
        message: "ignored",
        nowMillis: 2_000,
      });
      yield* health.publishFailed("missing", {
        message: "ignored",
        nowMillis: 2_000,
      });
      yield* health.clientConnected("orders", 1_000);
      yield* health.feedReady("ordersFeed");
      yield* health.rowsPublished("ordersFeed", {
        messages: 1,
        rows: 2,
        nowMillis: 2_000,
      });
      yield* health.rowsPublished("ordersFeed", {
        messages: 3,
        rows: 4,
        nowMillis: 2_000,
      });
      const readyHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect({
        startingStatus: startingHealth.status,
        startingActiveFeeds: startingHealth.grpc?.clients["orders"]?.activeFeeds,
        readyStatus: readyHealth.status,
        readyActiveFeeds: readyHealth.grpc?.clients["orders"]?.activeFeeds,
        materialized: readyHealth.grpc?.feeds["orders"]?.materialized["ordersFeed"],
      }).toStrictEqual({
        startingStatus: "starting",
        startingActiveFeeds: 0,
        readyStatus: "ready",
        readyActiveFeeds: 1,
        materialized: {
          status: "ready",
          lifecycle: "materialized",
          feedName: "ordersFeed",
          feedKey: "orders/ordersFeed/materialized",
          topic: "orders",
          subscriberCount: 0,
          rowCount: 0,
          messagesPerSecond: 4,
          rowsPerSecond: 6,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          reconnects: 0,
          lastMessageAt: 2_000,
          lastError: null,
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.live(
    "tracks active leased gRPC feed health in the ledger without pre-registering leases",
    () =>
      Effect.gen(function* () {
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
        const health = makeViewServerGrpcHealthLedger<typeof leasedGrpcViewServer.topics>({
          clients: {
            orders: "https://orders.example.test",
          },
          feeds: {},
        });
        const ordersLeaseKey = "orders/ordersLease/leased";

        const startingHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);
        yield* health.clientConnected("orders", 1_000);
        yield* health.leasedFeedStarting({
          feedName: "ordersLease",
          feedKey: ordersLeaseKey,
          topic: "orders",
          clientName: "orders",
        });
        yield* health.feedReady(ordersLeaseKey);
        const readyHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
        yield* health.subscriberAdded("missingLease");
        yield* health.subscriberRemoved("missingLease");
        yield* health.subscriberRemoved(ordersLeaseKey);
        yield* health.leasedFeedRemoved("missingLease");
        yield* health.leasedFeedStarting({
          feedName: "orphanLease",
          feedKey: "orders/orphanLease/leased/region=string%3A3%3Ausa",
          topic: "orders",
          clientName: "orphan",
        });
        yield* health.leasedFeedRemoved("orders/orphanLease/leased/region=string%3A3%3Ausa");
        yield* health.leasedFeedStarting({
          feedName: "degradedLease",
          feedKey: "orders/degradedLease/leased/region=string%3A3%3Ausa",
          topic: "orders",
          clientName: "orders",
        });
        yield* health.feedDegraded(
          "orders/degradedLease/leased/region=string%3A3%3Ausa",
          "leased route failed",
        );
        yield* health.leasedFeedStarting({
          feedName: "degradedLeaseTwo",
          feedKey: "orders/degradedLeaseTwo/leased/region=string%3A6%3Alondon",
          topic: "orders",
          clientName: "orders",
        });
        yield* health.feedDegraded(
          "orders/degradedLeaseTwo/leased/region=string%3A6%3Alondon",
          "second leased route failed",
        );
        yield* health.clientDegraded("orders", "leased route failed");
        const degradedMixedHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_500);
        yield* health.leasedFeedRemoved("orders/degradedLease/leased/region=string%3A3%3Ausa");
        const stillDegradedHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_750);
        yield* health.leasedFeedRemoved(
          "orders/degradedLeaseTwo/leased/region=string%3A6%3Alondon",
        );
        const afterDefensiveRemovalHealth = health.healthOverlay(
          yield* runtimeCore.client.health(),
          3_000,
        );

        expect({
          startingActiveFeeds: startingHealth.grpc?.clients["orders"]?.activeFeeds,
          startingFeedKeys: Object.keys(startingHealth.grpc?.feeds["orders"]?.leased ?? {}),
          readyActiveFeeds: readyHealth.grpc?.clients["orders"]?.activeFeeds,
          readyFeed: readyHealth.grpc?.feeds["orders"]?.leased[ordersLeaseKey],
          degradedMixedClientStatus: degradedMixedHealth.grpc?.clients["orders"]?.status,
          degradedMixedRuntimeStatus: degradedMixedHealth.status,
          stillDegradedClientStatus: stillDegradedHealth.grpc?.clients["orders"]?.status,
          stillDegradedClientError: stillDegradedHealth.grpc?.clients["orders"]?.lastError,
          subscriberCountAfterNoops:
            afterDefensiveRemovalHealth.grpc?.feeds["orders"]?.leased[ordersLeaseKey]
              ?.subscriberCount,
          clientStatusAfterRemovingDegradedLease:
            afterDefensiveRemovalHealth.grpc?.clients["orders"]?.status,
          clientErrorAfterRemovingDegradedLease:
            afterDefensiveRemovalHealth.grpc?.clients["orders"]?.lastError,
          defensiveRemovalFeedKeys: Object.keys(
            afterDefensiveRemovalHealth.grpc?.feeds["orders"]?.leased ?? {},
          ),
        }).toStrictEqual({
          startingActiveFeeds: 0,
          startingFeedKeys: [],
          readyActiveFeeds: 1,
          readyFeed: {
            status: "ready",
            lifecycle: "leased",
            feedName: "ordersLease",
            feedKey: "orders/ordersLease/leased",
            topic: "orders",
            subscriberCount: 0,
            rowCount: 0,
            messagesPerSecond: 0,
            rowsPerSecond: 0,
            decodeFailuresPerSecond: 0,
            mappingFailuresPerSecond: 0,
            publishFailuresPerSecond: 0,
            reconnects: 0,
            lastMessageAt: null,
            lastError: null,
          },
          degradedMixedClientStatus: "degraded",
          degradedMixedRuntimeStatus: "degraded",
          stillDegradedClientStatus: "degraded",
          stillDegradedClientError: "second leased route failed",
          subscriberCountAfterNoops: 0,
          clientStatusAfterRemovingDegradedLease: "connected",
          clientErrorAfterRemovingDegradedLease: null,
          defensiveRemovalFeedKeys: [ordersLeaseKey],
        });

        yield* runtimeCore.close;
      }),
  );

  it.live(
    "keeps gRPC client starting when removing a leased feed leaves starting materialized work",
    () =>
      Effect.gen(function* () {
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
        const health = makeViewServerGrpcHealthLedger<typeof leasedGrpcViewServer.topics>({
          clients: {
            orders: "https://orders.example.test",
          },
          feeds: {
            ordersFeed: {
              client: "orders",
              lifecycle: "materialized",
              topic: "orders",
            },
            ordersLease: {
              client: "orders",
              lifecycle: "leased",
              topic: "orders",
            },
          },
        });

        yield* health.clientConnected("orders", 1_000);
        yield* health.leasedFeedRemoved("orders/ordersLease/leased");
        const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

        expect({
          client: currentHealth.grpc?.clients["orders"],
          materializedStatus:
            currentHealth.grpc?.feeds["orders"]?.materialized["ordersFeed"]?.status,
          leasedFeedKeys: Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {}),
          runtimeStatus: currentHealth.status,
        }).toStrictEqual({
          client: {
            status: "starting",
            baseUrl: "https://orders.example.test",
            activeFeeds: 0,
            lastConnectedAt: 1_000,
            lastError: null,
          },
          materializedStatus: "starting",
          leasedFeedKeys: [],
          runtimeStatus: "starting",
        });

        yield* runtimeCore.close;
      }),
  );

  it.live("opens a leased gRPC feed on first subscriber and removes rows after last close", () =>
    Effect.gen(function* () {
      let acquired = 0;
      let released = 0;
      const feed = grpcLeasedFeed({
        acquired: () => {
          acquired += 1;
        },
        release: Effect.sync(() => {
          released += 1;
        }),
        streamForRegion: (region) =>
          longRunningGrpcStream([
            grpcOrderValue(`${region}-order-1`, 10),
            grpcOrderValue(`${region}-order-2`, 20),
          ]),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const idleHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const readySnapshot = yield* waitForLeasedGrpcSnapshotRows(
        runtimeCore.internalClient,
        "usa",
        2,
      );
      const readyHealthNow = yield* Clock.currentTimeMillis;
      const readyHealth = health.healthOverlay(yield* runtimeCore.client.health(), readyHealthNow);

      yield* subscription.close().pipe(Effect.timeout("1 second"));
      yield* Effect.yieldNow;
      const emptySnapshot = yield* waitForLeasedGrpcSnapshotRows(
        runtimeCore.internalClient,
        "usa",
        0,
      );
      const stoppedHealth = health.healthOverlay(yield* runtimeCore.client.health(), 3_000);

      expect(acquired).toBe(1);
      expect(released).toBe(1);
      expect({
        status: idleHealth.status,
        client: idleHealth.grpc?.clients["orders"],
        leasedFeeds: Object.keys(idleHealth.grpc?.feeds["orders"]?.leased ?? {}),
      }).toStrictEqual({
        status: "ready",
        client: {
          status: "connected",
          baseUrl: "https://orders.example.test",
          activeFeeds: 0,
          lastConnectedAt: null,
          lastError: null,
        },
        leasedFeeds: [],
      });
      expect(readySnapshot.rows).toStrictEqual([
        {
          id: "usa:usa-order-1",
          customerId: "usa-order-1",
          price: 10,
          region: "usa",
        },
        {
          id: "usa:usa-order-2",
          customerId: "usa-order-2",
          price: 20,
          region: "usa",
        },
      ]);
      expect(Object.keys(readyHealth.grpc?.feeds["orders"]?.leased ?? {})).toStrictEqual([
        "orders/ordersLease/leased/region=string%3A3%3Ausa",
      ]);
      expect(
        readyHealth.grpc?.feeds["orders"]?.leased[
          "orders/ordersLease/leased/region=string%3A3%3Ausa"
        ],
      ).toStrictEqual({
        status: "ready",
        lifecycle: "leased",
        feedName: "ordersLease",
        feedKey: "orders/ordersLease/leased/region=string%3A3%3Ausa",
        topic: "orders",
        subscriberCount: 1,
        rowCount: 2,
        messagesPerSecond: 2,
        rowsPerSecond: 2,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        reconnects: 0,
        lastMessageAt:
          readyHealth.grpc?.feeds["orders"]?.leased[
            "orders/ordersLease/leased/region=string%3A3%3Ausa"
          ]?.lastMessageAt,
        lastError: null,
      });
      expect(emptySnapshot).toStrictEqual({
        rows: [],
        totalRows: 0,
        version: emptySnapshot.version,
        status: "ready",
        statusCode: "Ready",
      });
      expect(Object.keys(stoppedHealth.grpc?.feeds["orders"]?.leased ?? {})).toStrictEqual([]);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("reuses a leased gRPC feed for same-route subscribers", () =>
    Effect.gen(function* () {
      let acquired = 0;
      let released = 0;
      const feed = grpcLeasedFeed({
        acquired: () => {
          acquired += 1;
        },
        release: Effect.sync(() => {
          released += 1;
        }),
        streamForRegion: (region) =>
          longRunningGrpcStream([grpcOrderValue(`${region}-order-1`, 10)]),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const first = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const second = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      const sharedHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      yield* first.close();
      const afterFirstClose = health.healthOverlay(yield* runtimeCore.client.health(), 3_000);
      yield* second.close();
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 0);

      expect(acquired).toBe(1);
      expect(released).toBe(1);
      expect(
        sharedHealth.grpc?.feeds["orders"]?.leased[
          "orders/ordersLease/leased/region=string%3A3%3Ausa"
        ]?.subscriberCount,
      ).toBe(2);
      expect(
        afterFirstClose.grpc?.feeds["orders"]?.leased[
          "orders/ordersLease/leased/region=string%3A3%3Ausa"
        ]?.subscriberCount,
      ).toBe(1);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("externalizes leased gRPC row keys on public live events", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedFeed({
        streamForRegion: (region) =>
          longRunningGrpcStream([grpcOrderValue(`${region}-order-1`, 10)]),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const events = yield* subscription.events.pipe(Stream.take(2), Stream.runCollect);

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
        operations: [
          {
            type: "insert",
            key: "usa:usa-order-1",
            row: {
              id: "usa:usa-order-1",
              customerId: "usa-order-1",
              price: 10,
              region: "usa",
            },
            index: 0,
          },
        ],
        totalRows: 1,
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("preserves public row-key tie-break ordering for leased gRPC feeds", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedFeed({
        streamForRegion: (region) =>
          longRunningGrpcStream([
            grpcOrderValue(`${region}-a `, 10),
            grpcOrderValue(`${region}-a!`, 10),
          ]),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribe("orders", {
        select: ["id", "price"],
        where: {
          region: { eq: "usa" },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const events = yield* subscription.events.pipe(Stream.take(2), Stream.runCollect);

      expect(events[1]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "insert",
            key: "usa:usa-a ",
            row: {
              id: "usa:usa-a ",
              price: 10,
            },
            index: 0,
          },
          {
            type: "insert",
            key: "usa:usa-a!",
            row: {
              id: "usa:usa-a!",
              price: 10,
            },
            index: 1,
          },
        ],
        totalRows: 2,
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("shares leased gRPC feeds while applying grouped queries locally", () =>
    Effect.gen(function* () {
      let acquired = 0;
      const feed = grpcLeasedFeed({
        acquired: () => {
          acquired += 1;
        },
        streamForRegion: (region) =>
          longRunningGrpcStream([
            grpcOrderValue(`${region}-order-1`, 10),
            grpcOrderValue(`${region}-order-2`, 20),
          ]),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const first = yield* manager.liveClient.subscribe("orders", {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          region: { eq: "usa" },
        },
        orderBy: [{ field: "status", direction: "asc" }],
        limit: 10,
      });
      const firstEventQueue = yield* Queue.unbounded<unknown>();
      const firstEventsFiber = yield* first.events.pipe(
        Stream.runForEach((event) => Queue.offer(firstEventQueue, event)),
        Effect.forkChild,
      );
      const firstSnapshot = yield* Queue.take(firstEventQueue);
      const firstDelta = yield* Queue.take(firstEventQueue);
      const openStatusGroupKey = '["array",[["array",[["string","status"],["string","open"]]]]]';
      const second = yield* manager.liveClient.subscribe("orders", {
        groupBy: ["status"],
        aggregates: {
          totalPrice: { aggFunc: "sum", field: "price" },
        },
        where: {
          region: { eq: "usa" },
        },
        orderBy: [{ aggregate: "totalPrice", direction: "desc" }],
        limit: 10,
      });
      const secondEvents = yield* second.events.pipe(Stream.take(1), Stream.runCollect);

      expect(acquired).toBe(1);
      expect(firstSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(firstDelta).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "insert",
            key: openStatusGroupKey,
            row: {
              status: "open",
              rowCount: 2n,
            },
            index: 0,
          },
        ],
        totalRows: 1,
      });
      expect(secondEvents[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-1",
        version: 1,
        keys: [openStatusGroupKey],
        rows: [
          {
            status: "open",
            totalPrice: BigDecimal.fromStringUnsafe("30"),
          },
        ],
        totalRows: 1,
      });
      yield* first.close();
      yield* second.close();
      yield* Fiber.interrupt(firstEventsFiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("externalizes grouped leased gRPC keys that include the topic key field", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedFeed({
        streamForRegion: (region) =>
          longRunningGrpcStream([
            grpcOrderValue(`${region}-order-1`, 10),
            grpcOrderValue(`${region}-order-2`, 20),
          ]),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const first = yield* manager.liveClient.subscribe("orders", {
        groupBy: ["id"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          region: { eq: "usa" },
        },
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const firstEventQueue = yield* Queue.unbounded<unknown>();
      const firstEventsFiber = yield* first.events.pipe(
        Stream.runForEach((event) => Queue.offer(firstEventQueue, event)),
        Effect.forkChild,
      );
      const firstSnapshot = yield* Queue.take(firstEventQueue);
      const firstDelta = yield* Queue.take(firstEventQueue);
      const second = yield* manager.liveClient.subscribe("orders", {
        groupBy: ["id"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          region: { eq: "usa" },
        },
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const secondEvents = yield* second.events.pipe(Stream.take(1), Stream.runCollect);
      const firstPublicGroupKey =
        '["array",[["array",[["string","id"],["string","usa:usa-order-1"]]]]]';
      const secondPublicGroupKey =
        '["array",[["array",[["string","id"],["string","usa:usa-order-2"]]]]]';

      expect(firstSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(firstDelta).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "insert",
            key: firstPublicGroupKey,
            row: {
              id: "usa:usa-order-1",
              rowCount: 1n,
            },
            index: 0,
          },
          {
            type: "insert",
            key: secondPublicGroupKey,
            row: {
              id: "usa:usa-order-2",
              rowCount: 1n,
            },
            index: 1,
          },
        ],
        totalRows: 2,
      });
      expect(secondEvents[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-1",
        version: 1,
        keys: [firstPublicGroupKey, secondPublicGroupKey],
        rows: [
          {
            id: "usa:usa-order-1",
            rowCount: 1n,
          },
          {
            id: "usa:usa-order-2",
            rowCount: 1n,
          },
        ],
        totalRows: 2,
      });

      yield* first.close();
      yield* second.close();
      yield* Fiber.interrupt(firstEventsFiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("supports leased gRPC feeds routed by the topic key", () =>
    Effect.gen(function* () {
      const feed = grpcKeyLeasedFeed({
        streamForId: (id) => longRunningGrpcStream([grpcOrderValue(`${id}-customer`, 10)]),
      });
      const grpcOptions = yield* resolveViewServerRuntimeOptions<
        typeof keyLeasedGrpcViewServer.topics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersById: feed,
          },
        },
      }).pipe(Effect.map((options) => options.grpcOptions));
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(keyLeasedGrpcViewServer, {});
      const health = makeViewServerGrpcHealthLedger<typeof keyLeasedGrpcViewServer.topics>({
        clients: {
          orders: "https://orders.example.test",
        },
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        keyLeasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        yield* Effect.fromNullishOr(grpcOptions),
        health,
      );

      const subscription = yield* manager.liveClient.subscribe("orders", {
        select: ["id", "customerId", "price"],
        where: {
          id: { eq: "order-1" },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const events = yield* subscription.events.pipe(Stream.take(2), Stream.runCollect);

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
        operations: [
          {
            type: "insert",
            key: "order-1",
            row: {
              id: "order-1",
              customerId: "order-1-customer",
              price: 10,
            },
            index: 0,
          },
        ],
        totalRows: 1,
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("creates stable leased feed keys for non-string route values", () =>
    Effect.gen(function* () {
      const feed = grpcRouteEncodingLeasedFeed();
      const resolvedOptions = yield* resolveViewServerRuntimeOptions<
        typeof routeEncodingLeasedGrpcViewServer.topics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersLease: feed,
          },
        },
      });
      const grpcOptions = yield* Effect.fromNullishOr(resolvedOptions.grpcOptions);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(
        routeEncodingLeasedGrpcViewServer,
        {},
      );
      const health = makeViewServerGrpcHealthLedger<
        typeof routeEncodingLeasedGrpcViewServer.topics
      >({
        clients: {
          orders: "https://orders.example.test",
        },
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        routeEncodingLeasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribeRuntime("orders", {
        select: ["id"],
        where: {
          amount: { eq: routeEncodingValues.amount },
          count: { eq: routeEncodingValues.count },
          disabled: { eq: routeEncodingValues.disabled },
          flag: { eq: routeEncodingValues.flag },
          meta: { eq: routeEncodingValues.meta },
          none: { eq: routeEncodingValues.none },
          plainScore: { eq: routeEncodingValues.plainScore },
          score: { eq: routeEncodingValues.score },
          tags: { eq: routeEncodingValues.tags },
          text: { eq: routeEncodingValues.text },
          weird: { eq: routeEncodingValues.weird },
        },
        limit: 10,
      });
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect(Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {})).toStrictEqual([
        "orders/ordersLease/leased/amount=bigDecimal%3A6%3A123.45&count=bigint%3A16%3A9007199254740993&disabled=boolean%3A5%3Afalse&flag=boolean%3A4%3Atrue&meta=object%3A28%3A6%3A%22desk%2217%3Astring%3A8%3Aequities&none=null%3A4%3Anull&plainScore=number%3A2%3A42&score=number%3A2%3A-0&tags=array%3A34%3A13%3Astring%3A4%3Afast15%3Astring%3A6%3Ashared&text=string%3A5%3Aroute&weird=object%3A53%3A7%3A%22alpha%2214%3Astring%3A5%3Afirst8%3A%22stable%2214%3Astring%3A5%3Aroute",
      ]);
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("externalizes grouped leased gRPC route values with public grouped keys", () =>
    Effect.gen(function* () {
      const feed = groupedKeyEncodingLeasedGrpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["text"],
        request: ({ text }) => ({ orderId: text }),
        acquire: () =>
          Stream.make(
            grpcOrderValue("route-encoding-1", 10),
            grpcOrderValue("route-encoding-2", 20),
            grpcOrderValue("route-encoding-3", 30),
          ),
        map: ({ value }) => ({
          id: value.customerId,
          ...routeEncodingValues,
          meta: {
            desk: value.price === 30 ? "credit" : value.price === 20 ? "rates" : "equities",
          },
          tags:
            value.price === 30
              ? ["unsupported"]
              : value.price === 20
                ? ["slow", "shared"]
                : routeEncodingValues.tags,
          weird: undefined,
        }),
      });
      const resolvedOptions = yield* resolveViewServerRuntimeOptions<
        typeof groupedKeyEncodingLeasedGrpcViewServer.topics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersLease: feed,
          },
        },
      });
      const grpcOptions = yield* Effect.fromNullishOr(resolvedOptions.grpcOptions);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(
        groupedKeyEncodingLeasedGrpcViewServer,
        {},
      );
      const health = makeViewServerGrpcHealthLedger<
        typeof groupedKeyEncodingLeasedGrpcViewServer.topics
      >({
        clients: {
          orders: "https://orders.example.test",
        },
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        groupedKeyEncodingLeasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribeRuntime("orders", {
        groupBy: [
          "amount",
          "count",
          "disabled",
          "flag",
          "none",
          "plainScore",
          "score",
          "text",
          "weird",
          "meta",
          "tags",
        ],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          text: { eq: routeEncodingValues.text },
        },
        limit: 10,
      });
      const deltaEvents = yield* subscription.events.pipe(
        Stream.filter((event) => event.type === "delta"),
        Stream.take(4),
        Stream.runCollect,
      );
      const publicGroupedKeyOne =
        '["array",[["array",[["string","amount"],["bigDecimal","123.45"]]],["array",[["string","count"],["bigint","9007199254740993"]]],["array",[["string","disabled"],["boolean",false]]],["array",[["string","flag"],["boolean",true]]],["array",[["string","none"],["null"]]],["array",[["string","plainScore"],["number","42"]]],["array",[["string","score"],["number","-0"]]],["array",[["string","text"],["string","route"]]],["array",[["string","weird"],["undefined"]]],["array",[["string","meta"],["canonical","object:28:6:\\"desk\\"17:string:8:equities"]]],["array",[["string","tags"],["canonical","array:34:13:string:4:fast15:string:6:shared"]]]]]';
      const publicGroupedKeyTwo =
        '["array",[["array",[["string","amount"],["bigDecimal","123.45"]]],["array",[["string","count"],["bigint","9007199254740993"]]],["array",[["string","disabled"],["boolean",false]]],["array",[["string","flag"],["boolean",true]]],["array",[["string","none"],["null"]]],["array",[["string","plainScore"],["number","42"]]],["array",[["string","score"],["number","-0"]]],["array",[["string","text"],["string","route"]]],["array",[["string","weird"],["undefined"]]],["array",[["string","meta"],["canonical","object:25:6:\\"desk\\"14:string:5:rates"]]],["array",[["string","tags"],["canonical","array:34:13:string:4:slow15:string:6:shared"]]]]]';
      const publicGroupedKeyThree =
        '["array",[["array",[["string","amount"],["bigDecimal","123.45"]]],["array",[["string","count"],["bigint","9007199254740993"]]],["array",[["string","disabled"],["boolean",false]]],["array",[["string","flag"],["boolean",true]]],["array",[["string","none"],["null"]]],["array",[["string","plainScore"],["number","42"]]],["array",[["string","score"],["number","-0"]]],["array",[["string","text"],["string","route"]]],["array",[["string","weird"],["undefined"]]],["array",[["string","meta"],["canonical","object:26:6:\\"desk\\"15:string:6:credit"]]],["array",[["string","tags"],["canonical","array:24:21:string:11:unsupported"]]]]]';

      expect(deltaEvents[0]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "insert",
            key: publicGroupedKeyThree,
            row: {
              amount: routeEncodingValues.amount,
              count: routeEncodingValues.count,
              disabled: routeEncodingValues.disabled,
              flag: routeEncodingValues.flag,
              none: routeEncodingValues.none,
              plainScore: routeEncodingValues.plainScore,
              score: routeEncodingValues.score,
              text: routeEncodingValues.text,
              weird: undefined,
              meta: {
                desk: "credit",
              },
              tags: ["unsupported"],
              rowCount: 1n,
            },
            index: 0,
          },
          {
            type: "insert",
            key: publicGroupedKeyOne,
            row: {
              amount: routeEncodingValues.amount,
              count: routeEncodingValues.count,
              disabled: routeEncodingValues.disabled,
              flag: routeEncodingValues.flag,
              none: routeEncodingValues.none,
              plainScore: routeEncodingValues.plainScore,
              score: routeEncodingValues.score,
              text: routeEncodingValues.text,
              weird: undefined,
              meta: routeEncodingValues.meta,
              tags: routeEncodingValues.tags,
              rowCount: 1n,
            },
            index: 1,
          },
          {
            type: "insert",
            key: publicGroupedKeyTwo,
            row: {
              amount: routeEncodingValues.amount,
              count: routeEncodingValues.count,
              disabled: routeEncodingValues.disabled,
              flag: routeEncodingValues.flag,
              none: routeEncodingValues.none,
              plainScore: routeEncodingValues.plainScore,
              score: routeEncodingValues.score,
              text: routeEncodingValues.text,
              weird: undefined,
              meta: {
                desk: "rates",
              },
              tags: ["slow", "shared"],
              rowCount: 1n,
            },
            index: 2,
          },
        ],
        totalRows: 3,
      });
      expect(deltaEvents[1]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "remove",
            key: publicGroupedKeyOne,
          },
        ],
        totalRows: 2,
      });
      expect(deltaEvents[2]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 2,
        toVersion: 3,
        operations: [
          {
            type: "remove",
            key: publicGroupedKeyTwo,
          },
        ],
        totalRows: 1,
      });
      expect(deltaEvents[3]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 3,
        toVersion: 4,
        operations: [
          {
            type: "remove",
            key: publicGroupedKeyThree,
          },
        ],
        totalRows: 0,
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("fails grouped leased gRPC event streams for non-canonical public key values", () =>
    Effect.gen(function* () {
      const feed = groupedKeyEncodingLeasedGrpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["text"],
        request: ({ text }) => ({ orderId: text }),
        acquire: () => Stream.make(grpcOrderValue("route-encoding-1", 10)),
        map: ({ value }) => ({
          id: value.customerId,
          ...routeEncodingValues,
          weird: new Uint8Array([1]),
        }),
      });
      const resolvedOptions = yield* resolveViewServerRuntimeOptions<
        typeof groupedKeyEncodingLeasedGrpcViewServer.topics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersLease: feed,
          },
        },
      });
      const grpcOptions = yield* Effect.fromNullishOr(resolvedOptions.grpcOptions);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(
        groupedKeyEncodingLeasedGrpcViewServer,
        {},
      );
      const health = makeViewServerGrpcHealthLedger<
        typeof groupedKeyEncodingLeasedGrpcViewServer.topics
      >({
        clients: {
          orders: "https://orders.example.test",
        },
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        groupedKeyEncodingLeasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const subscription = yield* manager.liveClient.subscribeRuntime("orders", {
        groupBy: ["weird"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          text: { eq: routeEncodingValues.text },
        },
        limit: 10,
      });
      const events = yield* subscription.events.pipe(Stream.runCollect);

      expect(events).toStrictEqual([
        {
          type: "snapshot",
          topic: "orders",
          queryId: "query-0",
          version: 0,
          keys: [],
          rows: [],
          totalRows: 0,
        },
        {
          type: "status",
          topic: "orders",
          queryId: "query-0",
          status: "error",
          code: "RuntimeUnavailable",
          message: "Leased gRPC grouped key value cannot be encoded as a stable public key",
        },
      ]);
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("fails grouped leased gRPC snapshots for non-canonical public key values", () =>
    Effect.gen(function* () {
      const feed = groupedKeyEncodingLeasedGrpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["text"],
        request: ({ text }) => ({ orderId: text }),
        acquire: () => longRunningGrpcStream([grpcOrderValue("route-encoding-1", 10)]),
        map: ({ value }) => ({
          id: value.customerId,
          ...routeEncodingValues,
          weird: new Uint8Array([1]),
        }),
      });
      const resolvedOptions = yield* resolveViewServerRuntimeOptions<
        typeof groupedKeyEncodingLeasedGrpcViewServer.topics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersLease: feed,
          },
        },
      });
      const grpcOptions = yield* Effect.fromNullishOr(resolvedOptions.grpcOptions);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(
        groupedKeyEncodingLeasedGrpcViewServer,
        {},
      );
      const health = makeViewServerGrpcHealthLedger<
        typeof groupedKeyEncodingLeasedGrpcViewServer.topics
      >({
        clients: {
          orders: "https://orders.example.test",
        },
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        groupedKeyEncodingLeasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const rawSubscription = yield* manager.liveClient.subscribeRuntime("orders", {
        select: ["id", "weird"],
        where: {
          text: { eq: routeEncodingValues.text },
        },
        limit: 10,
      });
      const rawEventQueue = yield* Queue.unbounded<unknown>();
      const rawEventsFiber = yield* rawSubscription.events.pipe(
        Stream.runForEach((event) => Queue.offer(rawEventQueue, event)),
        Effect.forkChild,
      );
      const rawSnapshot = yield* Queue.take(rawEventQueue);
      const rawDelta = yield* Queue.take(rawEventQueue);
      const groupedSubscription = yield* manager.liveClient.subscribeRuntime("orders", {
        groupBy: ["weird"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          text: { eq: routeEncodingValues.text },
        },
        limit: 10,
      });
      const groupedEvents = yield* groupedSubscription.events.pipe(Stream.runCollect);

      expect([rawSnapshot, rawDelta]).toStrictEqual([
        {
          type: "snapshot",
          topic: "orders",
          queryId: "query-0",
          version: 0,
          keys: [],
          rows: [],
          totalRows: 0,
        },
        {
          type: "delta",
          topic: "orders",
          queryId: "query-0",
          fromVersion: 0,
          toVersion: 1,
          operations: [
            {
              type: "insert",
              key: "route-encoding-1",
              row: {
                id: "route-encoding-1",
                weird: new Uint8Array([1]),
              },
              index: 0,
            },
          ],
          totalRows: 1,
        },
      ]);
      expect(groupedEvents).toStrictEqual([
        {
          type: "status",
          topic: "orders",
          queryId: "query-1",
          status: "error",
          code: "RuntimeUnavailable",
          message: "Leased gRPC grouped key value cannot be encoded as a stable public key",
        },
      ]);
      yield* rawSubscription.close();
      yield* groupedSubscription.close();
      yield* Fiber.interrupt(rawEventsFiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects non-canonical leased gRPC route values instead of sharing a fallback key", () =>
    Effect.gen(function* () {
      const feed = grpcRouteEncodingLeasedFeed();
      const resolvedOptions = yield* resolveViewServerRuntimeOptions<
        typeof routeEncodingLeasedGrpcViewServer.topics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersLease: feed,
          },
        },
      });
      const grpcOptions = yield* Effect.fromNullishOr(resolvedOptions.grpcOptions);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(
        routeEncodingLeasedGrpcViewServer,
        {},
      );
      const health = makeViewServerGrpcHealthLedger<
        typeof routeEncodingLeasedGrpcViewServer.topics
      >({
        clients: {
          orders: "https://orders.example.test",
        },
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        routeEncodingLeasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const symbolError = yield* manager.liveClient
        .subscribeRuntime("orders", {
          select: ["id"],
          where: {
            amount: { eq: routeEncodingValues.amount },
            count: { eq: routeEncodingValues.count },
            disabled: { eq: routeEncodingValues.disabled },
            flag: { eq: routeEncodingValues.flag },
            meta: { eq: routeEncodingValues.meta },
            none: { eq: routeEncodingValues.none },
            plainScore: { eq: routeEncodingValues.plainScore },
            score: { eq: routeEncodingValues.score },
            tags: { eq: routeEncodingValues.tags },
            text: { eq: routeEncodingValues.text },
            weird: { eq: Symbol("leased-route") },
          },
          limit: 1,
        })
        .pipe(Effect.flip);
      const objectError = yield* manager.liveClient
        .subscribeRuntime("orders", {
          select: ["id"],
          where: {
            amount: { eq: routeEncodingValues.amount },
            count: { eq: routeEncodingValues.count },
            disabled: { eq: routeEncodingValues.disabled },
            flag: { eq: routeEncodingValues.flag },
            meta: { eq: routeEncodingValues.meta },
            none: { eq: routeEncodingValues.none },
            plainScore: { eq: routeEncodingValues.plainScore },
            score: { eq: routeEncodingValues.score },
            tags: { eq: routeEncodingValues.tags },
            text: { eq: routeEncodingValues.text },
            weird: { eq: new Map([["stable", "route"]]) },
          },
          limit: 1,
        })
        .pipe(Effect.flip);

      expect({
        symbolError,
        objectError,
        leasedFeeds: Object.keys(
          health.healthOverlay(yield* runtimeCore.client.health(), 1_000).grpc?.feeds["orders"]
            ?.leased ?? {},
        ),
      }).toStrictEqual({
        symbolError: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message:
            "Leased topic orders route field weird value cannot be used as a stable leased gRPC route key.",
        },
        objectError: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message:
            "Leased topic orders route field weird value cannot be used as a stable leased gRPC route key.",
        },
        leasedFeeds: [],
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("opens independent leased gRPC feeds for different routes", () =>
    Effect.gen(function* () {
      const acquiredRegions: Array<string> = [];
      const feed = grpcLeasedFeed({
        acquired: (region) => {
          acquiredRegions.push(region);
        },
        streamForRegion: (region) =>
          longRunningGrpcStream([grpcOrderValue(`${region}-order-1`, region.length)]),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const usa = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const eu = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("eu"));
      const usaSnapshot = yield* waitForLeasedGrpcSnapshotRows(
        runtimeCore.internalClient,
        "usa",
        1,
      );
      const euSnapshot = yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "eu", 1);
      const routeHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect(acquiredRegions).toStrictEqual(["usa", "eu"]);
      expect(usaSnapshot.rows).toStrictEqual([
        {
          id: "usa:usa-order-1",
          customerId: "usa-order-1",
          price: 3,
          region: "usa",
        },
      ]);
      expect(euSnapshot.rows).toStrictEqual([
        {
          id: "eu:eu-order-1",
          customerId: "eu-order-1",
          price: 2,
          region: "eu",
        },
      ]);
      expect(Object.keys(routeHealth.grpc?.feeds["orders"]?.leased ?? {})).toStrictEqual([
        "orders/ordersLease/leased/region=string%3A3%3Ausa",
        "orders/ordersLease/leased/region=string%3A2%3Aeu",
      ]);
      yield* usa.close();
      yield* eu.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("externalizes leased gRPC snapshot and delta live events", () =>
    Effect.gen(function* () {
      const values = yield* Queue.unbounded<GrpcOrderValueMessage>();
      const feed = grpcLeasedFeed({
        streamForRegion: () => Stream.fromQueue(values),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const starter = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* Queue.offer(values, grpcOrderValue("order-1", 10));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const events = yield* Queue.unbounded<unknown>();
      const fiber = yield* subscription.events.pipe(
        Stream.runForEach((event) => Queue.offer(events, event)),
        Effect.forkChild,
      );
      const snapshot = yield* Queue.take(events);
      yield* Queue.offer(values, grpcOrderValue("order-2", 20));
      const delta = yield* Queue.take(events).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (event) =>
            typeof event === "object" && event !== null && Reflect.get(event, "type") === "delta",
        }),
      );

      expect(snapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-1",
        version: 1,
        keys: ["usa:order-1"],
        rows: [
          {
            id: "usa:order-1",
            customerId: "order-1",
            price: 10,
            region: "usa",
          },
        ],
        totalRows: 1,
      });
      expect(delta).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-1",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "insert",
            key: "usa:order-2",
            row: {
              id: "usa:order-2",
              customerId: "order-2",
              price: 20,
              region: "usa",
            },
            index: 1,
          },
        ],
        totalRows: 2,
      });
      yield* subscription.close();
      yield* starter.close();
      yield* Fiber.interrupt(fiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rewrites leased gRPC public row-key filters before local query execution", () =>
    Effect.gen(function* () {
      const values = yield* Queue.unbounded<GrpcOrderValueMessage>();
      const feed = grpcLeasedFeed({
        streamForRegion: () => Stream.fromQueue(values),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const starter = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* Queue.offer(values, grpcOrderValue("order-1", 10));
      yield* Queue.offer(values, grpcOrderValue("order-2", 20));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 2);
      const arrayFilterSubscription = yield* manager.liveClient.subscribe("orders", {
        select: ["id", "price"],
        where: {
          region: { eq: "usa" },
          id: { in: ["usa:order-2"] },
        },
        limit: 10,
      });
      const scalarFilterSubscription = yield* manager.liveClient.subscribe("orders", {
        select: ["id", "price"],
        where: {
          region: { eq: "usa" },
          id: "usa:order-1",
        },
        limit: 10,
      });
      const arrayFilterEvents = yield* arrayFilterSubscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
      );
      const scalarFilterEvents = yield* scalarFilterSubscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
      );

      expect({
        arrayFilterEvents: Array.from(arrayFilterEvents),
        scalarFilterEvents: Array.from(scalarFilterEvents),
      }).toStrictEqual({
        arrayFilterEvents: [
          {
            type: "snapshot",
            topic: "orders",
            queryId: "query-1",
            version: 1,
            keys: ["usa:order-2"],
            rows: [
              {
                id: "usa:order-2",
                price: 20,
              },
            ],
            totalRows: 1,
          },
        ],
        scalarFilterEvents: [
          {
            type: "snapshot",
            topic: "orders",
            queryId: "query-2",
            version: 1,
            keys: ["usa:order-1"],
            rows: [
              {
                id: "usa:order-1",
                price: 10,
              },
            ],
            totalRows: 1,
          },
        ],
      });
      yield* scalarFilterSubscription.close();
      yield* arrayFilterSubscription.close();
      yield* starter.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("externalizes leased gRPC remove deltas when rows leave a result window", () =>
    Effect.gen(function* () {
      const values = yield* Queue.unbounded<GrpcOrderValueMessage>();
      const feed = grpcLeasedFeed({
        streamForRegion: () => Stream.fromQueue(values),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const starter = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* Queue.offer(values, grpcOrderValue("order-1", 10));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      const subscription = yield* manager.liveClient.subscribe("orders", {
        select: ["id", "price"],
        where: {
          region: { eq: "usa" },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 1,
      });
      const events = yield* Queue.unbounded<unknown>();
      const fiber = yield* subscription.events.pipe(
        Stream.runForEach((event) => Queue.offer(events, event)),
        Effect.forkChild,
      );
      const snapshot = yield* Queue.take(events);
      yield* Queue.offer(values, grpcOrderValue("order-0", 5));
      const delta = yield* Queue.take(events).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (event) =>
            typeof event === "object" && event !== null && Reflect.get(event, "type") === "delta",
        }),
      );

      expect(snapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-1",
        version: 1,
        keys: ["usa:order-1"],
        rows: [
          {
            id: "usa:order-1",
            price: 10,
          },
        ],
        totalRows: 1,
      });
      expect(delta).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-1",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "remove",
            key: "usa:order-1",
          },
          {
            type: "insert",
            key: "usa:order-0",
            row: {
              id: "usa:order-0",
              price: 5,
            },
            index: 0,
          },
        ],
        totalRows: 2,
      });
      yield* subscription.close();
      yield* starter.close();
      yield* Fiber.interrupt(fiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("passes leased gRPC runtime status events through the manager", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedFeed({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const statusEvent = {
        type: "status",
        topic: "orders",
        queryId: "internal-status",
        status: "ready",
        code: "Ready",
        message: "internal status",
      } as const;
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        subscribeInternal: () =>
          Effect.succeed({
            events: Stream.make(statusEvent),
            close: () => Effect.void,
          }),
        subscribeRuntimeInternal: () =>
          Effect.succeed({
            events: Stream.make(statusEvent),
            close: () => Effect.void,
          }),
      };
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        fakeInternalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(Array.from(events)).toStrictEqual([statusEvent]);
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("delivers leased terminal status when the runtime stream has no initial event", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedFeed({
        streamForRegion: () => Stream.empty,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        subscribeInternal: () =>
          Effect.succeed({
            events: Stream.empty,
            close: () => Effect.void,
          }),
        subscribeRuntimeInternal: () =>
          Effect.succeed({
            events: Stream.empty,
            close: () => Effect.void,
          }),
      };
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        fakeInternalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const terminalStatus = yield* subscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.timeout("1 second"),
      );

      expect(Array.from(terminalStatus)).toStrictEqual([
        {
          type: "status",
          topic: "orders",
          queryId: "orders/leased-status",
          status: "error",
          code: "RuntimeUnavailable",
          message: "gRPC leased upstream completed unexpectedly.",
        },
      ]);
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("passes through malformed internal leased rows without rewriting non-string keys", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedFeed({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const malformedSnapshot = {
        type: "snapshot",
        topic: "orders",
        queryId: "internal-malformed",
        version: 1,
        keys: ["internal-key"],
        rows: [
          {
            id: 123,
            price: 10,
          },
        ],
        totalRows: 1,
      } as const;
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        subscribeInternal: runtimeCore.internalLiveClient.subscribeInternal,
        subscribeRuntimeInternal: () =>
          Effect.succeed({
            events: Stream.make(malformedSnapshot),
            close: () => Effect.void,
          }),
      };
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        fakeInternalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribeRuntime("orders", {
        select: ["id", "price"],
        where: {
          region: { eq: "usa" },
          id: { eq: "usa:order-1" },
        },
        limit: 10,
      });
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(Array.from(events)).toStrictEqual([
        {
          type: "snapshot",
          topic: "orders",
          queryId: "internal-malformed",
          version: 1,
          keys: ["internal-key"],
          rows: [
            {
              id: 123,
              price: 10,
            },
          ],
          totalRows: 1,
        },
      ]);
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("keeps non-string leased row-key predicates unchanged in internal runtime queries", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedFeed({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        subscribeInternal: (_topic, query) =>
          Effect.succeed({
            events: Stream.make({
              type: "status",
              topic: "orders",
              queryId: "internal-query",
              status: "ready",
              code: "Ready",
              message: JSON.stringify(query),
            }),
            close: () => Effect.void,
          }),
        subscribeRuntimeInternal: (_topic, query) =>
          Effect.succeed({
            events: Stream.make({
              type: "status",
              topic: "orders",
              queryId: "internal-query",
              status: "ready",
              code: "Ready",
              message: JSON.stringify(query),
            }),
            close: () => Effect.void,
          }),
      };
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        fakeInternalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      type LeasedOrdersRuntimeKeyQuery = {
        readonly select: readonly ["id", "price"];
        readonly where: {
          readonly region: {
            readonly eq: string;
          };
          readonly id: {
            readonly eq: string;
          };
        };
        readonly limit: 10;
      };
      const query = {
        select: ["id", "price"],
        where: {
          region: { eq: "usa" },
          id: { eq: "usa:order-1" },
        },
        limit: 10,
      } satisfies LeasedOrdersRuntimeKeyQuery;
      const subscribeEffect = manager.liveClient.subscribeRuntime("orders", query);
      Object.defineProperty(query.where.id, "eq", { value: 123 });

      const subscription = yield* subscribeEffect;
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(Array.from(events)).toStrictEqual([
        {
          type: "status",
          topic: "orders",
          queryId: "internal-query",
          status: "ready",
          code: "Ready",
          message:
            '{"select":["id","price"],"where":{"region":{"eq":"usa"},"id":{"eq":123}},"limit":10}',
        },
      ]);
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("releases leased gRPC leases when internal subscription creation fails", () =>
    Effect.gen(function* () {
      let released = 0;
      const feed = grpcLeasedFeed({
        release: Effect.sync(() => {
          released += 1;
        }),
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const subscriptionFailure: ViewServerRuntimeError = {
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "internal subscription failed",
      };
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        subscribeInternal: () => Effect.fail(subscriptionFailure),
        subscribeRuntimeInternal: () => Effect.fail(subscriptionFailure),
      };
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        fakeInternalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscribeError = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.flip);
      const subscribeRuntimeError = yield* manager.liveClient
        .subscribeRuntime("orders", leasedOrdersQuery("eu"))
        .pipe(Effect.flip);
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect({
        subscribeError,
        subscribeRuntimeError,
        released,
        leasedFeedKeys: Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {}),
      }).toStrictEqual({
        subscribeError: subscriptionFailure,
        subscribeRuntimeError: subscriptionFailure,
        released: 2,
        leasedFeedKeys: [],
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("keeps leased gRPC leases alive when an additional internal subscription fails", () =>
    Effect.gen(function* () {
      let released = 0;
      const feed = grpcLeasedFeed({
        release: Effect.sync(() => {
          released += 1;
        }),
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const subscriptionFailure: ViewServerRuntimeError = {
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "second internal subscription failed",
      };
      const internalSubscription = {
        events: Stream.never,
        close: () => Effect.void,
      };
      const subscribeResults =
        yield* Queue.unbounded<
          Effect.Effect<typeof internalSubscription, ViewServerRuntimeError>
        >();
      yield* Queue.offer(subscribeResults, Effect.succeed(internalSubscription));
      yield* Queue.offer(subscribeResults, Effect.fail(subscriptionFailure));
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        subscribeInternal: () => Queue.take(subscribeResults).pipe(Effect.flatten),
        subscribeRuntimeInternal: runtimeCore.internalLiveClient.subscribeRuntimeInternal,
      };
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        fakeInternalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const firstSubscription = yield* manager.liveClient.subscribe(
        "orders",
        leasedOrdersQuery("usa"),
      );
      const secondSubscribeError = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.flip);
      const activeHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect({
        secondSubscribeError,
        released,
        subscriberCount:
          activeHealth.grpc?.feeds["orders"]?.leased[
            "orders/ordersLease/leased/region=string%3A3%3Ausa"
          ]?.subscriberCount,
      }).toStrictEqual({
        secondSubscribeError: subscriptionFailure,
        released: 0,
        subscriberCount: 1,
      });
      yield* firstSubscription.close().pipe(Effect.timeout("1 second"));
      const closedHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      expect({
        released,
        leasedFeedKeys: Object.keys(closedHealth.grpc?.feeds["orders"]?.leased ?? {}),
      }).toStrictEqual({
        released: 1,
        leasedFeedKeys: [],
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("isolates identical public leased row keys across different routes", () =>
    Effect.gen(function* () {
      const feed = leasedGrpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["region"],
        request: ({ region }) => ({ orderId: region }),
        acquire: () => longRunningGrpcStream([grpcOrderValue("shared-order", 10)]),
        map: ({ value, route }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: route.region,
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const usa = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const eu = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("eu"));
      const usaSnapshot = yield* waitForLeasedGrpcSnapshotRows(
        runtimeCore.internalClient,
        "usa",
        1,
      );
      const euSnapshot = yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "eu", 1);
      yield* usa.close();
      const euAfterUsaClose = yield* waitForLeasedGrpcSnapshotRows(
        runtimeCore.internalClient,
        "eu",
        1,
      );

      expect(usaSnapshot.rows).toStrictEqual([
        {
          id: "shared-order",
          customerId: "shared-order",
          price: 10,
          region: "usa",
        },
      ]);
      expect(euSnapshot.rows).toStrictEqual([
        {
          id: "shared-order",
          customerId: "shared-order",
          price: 10,
          region: "eu",
        },
      ]);
      expect(euAfterUsaClose.rows).toStrictEqual([
        {
          id: "shared-order",
          customerId: "shared-order",
          price: 10,
          region: "eu",
        },
      ]);
      yield* eu.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("isolates leased feeds by internal feed partition before local route predicates", () =>
    Effect.gen(function* () {
      const usaQueue = yield* Queue.unbounded<GrpcOrderValueMessage>();
      const euQueue = yield* Queue.unbounded<GrpcOrderValueMessage>();
      const feed = leasedGrpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["region"],
        request: ({ region }) => ({ orderId: region }),
        acquire: ({ route }) =>
          route.region === "usa" ? Stream.fromQueue(usaQueue) : Stream.fromQueue(euQueue),
        map: ({ value, route }) => ({
          id: `${route.region}:${value.customerId}`,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const eu = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("eu"));
      const euEvents = yield* Queue.unbounded<unknown>();
      const euEventsFiber = yield* eu.events.pipe(
        Stream.runForEach((event) => Queue.offer(euEvents, event)),
        Effect.forkChild,
      );
      const euInitialSnapshot = yield* Queue.take(euEvents);
      yield* Queue.offer(euQueue, grpcOrderValue("shared-order", 10));
      const euRouteMismatchedEvent = yield* Queue.take(euEvents);
      const routeMismatchSnapshot = yield* runtimeCore.internalClient.snapshot("orders", {
        select: ["id", "region"],
        where: {
          region: { eq: "usa" },
        },
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const usa = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const usaEventsFiber = yield* usa.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Queue.offer(usaQueue, grpcOrderValue("shared-order", 10));
      const usaEvents = yield* Fiber.join(usaEventsFiber);
      const usaSnapshot = usaEvents[0];
      const usaDelta = usaEvents[1];

      expect(usaSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-1",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(usaDelta).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-1",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "insert",
            key: "usa:shared-order",
            row: {
              id: "usa:shared-order",
              customerId: "shared-order",
              price: 10,
              region: "usa",
            },
            index: 0,
          },
        ],
        totalRows: 1,
      });
      expect(euInitialSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(euRouteMismatchedEvent).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "error",
        code: "RuntimeUnavailable",
        message: "gRPC leased upstream failed.",
      });
      expect(routeMismatchSnapshot).toStrictEqual({
        rows: [],
        totalRows: 0,
        version: 0,
        status: "ready",
        statusCode: "Ready",
      });
      yield* usa.close();
      yield* eu.close();
      yield* Fiber.interrupt(euEventsFiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("shares one leased gRPC feed while applying different local filters", () =>
    Effect.gen(function* () {
      let acquired = 0;
      const feed = grpcLeasedFeed({
        acquired: () => {
          acquired += 1;
        },
        streamForRegion: (region) =>
          longRunningGrpcStream([
            grpcOrderValue(`${region}-cheap`, 10),
            grpcOrderValue(`${region}-expensive`, 90),
          ]),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const cheap = yield* manager.liveClient.subscribe("orders", {
        select: ["id", "price", "region"],
        where: {
          region: { eq: "usa" },
          price: { lte: 20 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const expensive = yield* manager.liveClient.subscribe("orders", {
        select: ["id", "price", "region"],
        where: {
          region: { eq: "usa" },
          price: { gte: 50 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const cheapSnapshot = yield* runtimeCore.internalClient
        .snapshot("orders", {
          select: ["id", "price", "region"],
          where: {
            region: { eq: "usa" },
            price: { lte: 20 },
          },
          orderBy: [{ field: "price", direction: "asc" }],
          limit: 10,
        })
        .pipe(
          Effect.repeat({
            schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
            until: (snapshot) => snapshot.totalRows === 1,
          }),
        );
      const expensiveSnapshot = yield* runtimeCore.internalClient
        .snapshot("orders", {
          select: ["id", "price", "region"],
          where: {
            region: { eq: "usa" },
            price: { gte: 50 },
          },
          orderBy: [{ field: "price", direction: "asc" }],
          limit: 10,
        })
        .pipe(
          Effect.repeat({
            schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
            until: (snapshot) => snapshot.totalRows === 1,
          }),
        );

      expect(acquired).toBe(1);
      expect(cheapSnapshot.rows).toStrictEqual([
        {
          id: "usa:usa-cheap",
          price: 10,
          region: "usa",
        },
      ]);
      expect(expensiveSnapshot.rows).toStrictEqual([
        {
          id: "usa:usa-expensive",
          price: 90,
          region: "usa",
        },
      ]);
      yield* cheap.close();
      yield* expensive.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects leased gRPC subscriptions without exact route equality", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedFeed({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const error = yield* Effect.flip(
        manager.liveClient.subscribeRuntime("orders", {
          select: ["id"],
          where: {
            region: { startsWith: "u" },
          },
          limit: 10,
        }),
      );

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "orders",
        message: "Leased topic orders route field region must use an exact eq filter.",
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("accepts decoded transform schema values for leased gRPC routes", () =>
    Effect.gen(function* () {
      const BigIntRouteOrder = Schema.Struct({
        id: Schema.String,
        accountId: Schema.BigIntFromString,
        customerId: Schema.String,
        price: Schema.Number,
      });
      const localViewServer = defineViewServerConfig({
        topics: {
          orders: {
            schema: BigIntRouteOrder,
            key: "id",
            source: grpc.leased({
              routeBy: ["accountId"],
            }),
          },
        },
      });
      const localGrpcFeed = localViewServer.grpcFeed<typeof grpcClients>();
      let acquiredRoute: bigint | null = null;
      const feed = localGrpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["accountId"],
        request: ({ accountId }) => ({ orderId: accountId.toString() }),
        acquire: ({ route }) => {
          acquiredRoute = route.accountId;
          return Stream.never;
        },
        map: ({ value, route }) => ({
          id: `${route.accountId}:${value.customerId}`,
          accountId: route.accountId,
          customerId: value.customerId,
          price: value.price,
        }),
      });
      const grpcOptions = yield* resolveViewServerRuntimeOptions<
        typeof localViewServer.topics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersLease: feed,
          },
        },
      }).pipe(Effect.flatMap((options) => Effect.fromNullishOr(options.grpcOptions)));
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(localViewServer, {});
      const health = makeViewServerGrpcHealthLedger<typeof localViewServer.topics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        localViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribeRuntime("orders", {
        select: ["id", "accountId", "customerId"],
        where: {
          accountId: { eq: 7n },
        },
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const snapshot = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect({
        acquiredRoute,
        snapshot,
      }).toStrictEqual({
        acquiredRoute: 7n,
        snapshot: [
          {
            type: "snapshot",
            topic: "orders",
            queryId: "query-0",
            version: 0,
            keys: [],
            rows: [],
            totalRows: 0,
          },
        ],
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects decoded leased gRPC route values that fail topic schema validation", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedFeed({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const delayedRuntimeQuery = leasedOrdersQuery("usa");
      const subscribeRuntimeEffect = manager.liveClient.subscribeRuntime(
        "orders",
        delayedRuntimeQuery,
      );
      Object.defineProperty(delayedRuntimeQuery.where.region, "eq", {
        value: 123,
      });

      const error = yield* Effect.flip(subscribeRuntimeEffect);

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "orders",
        message: "Leased topic orders route field region value does not match the topic schema.",
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects leased gRPC route extraction when query shape or route schema is invalid", () =>
    Effect.gen(function* () {
      const localViewServer = defineViewServerConfig({
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
      const localGrpcFeed = localViewServer.grpcFeed<typeof grpcClients>();
      const missingRouteFieldFeed = localGrpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["region"],
        request: ({ region }) => ({ orderId: region }),
        acquire: () => Stream.never,
        map: ({ value, route }) => ({
          id: `${route.region}:${value.customerId}`,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: route.region,
          updatedAt: value.updatedAt,
        }),
      });
      Object.defineProperty(localViewServer.topics.orders.source, "routeBy", {
        value: ["missing"],
      });
      Object.defineProperty(missingRouteFieldFeed, "routeBy", {
        value: ["missing"],
      });
      const grpcOptions = yield* resolveViewServerRuntimeOptions<
        typeof localViewServer.topics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersLease: missingRouteFieldFeed,
          },
        },
      }).pipe(Effect.flatMap((options) => Effect.fromNullishOr(options.grpcOptions)));
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(localViewServer, {});
      const health = makeViewServerGrpcHealthLedger<typeof localViewServer.topics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        localViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const missingWhereError = yield* manager.liveClient
        .subscribeRuntime("orders", {
          select: ["id"],
          limit: 10,
        })
        .pipe(Effect.flip);
      const missingFieldQuery = leasedOrdersQuery("usa");
      Object.defineProperty(missingFieldQuery.where, "missing", {
        value: { eq: "usa" },
      });
      const missingFieldError = yield* manager.liveClient
        .subscribeRuntime("orders", missingFieldQuery)
        .pipe(Effect.flip);

      expect({
        missingWhereError,
        missingFieldError,
      }).toStrictEqual({
        missingWhereError: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message: "Leased topic orders requires exact equality filters for route fields: missing.",
        },
        missingFieldError: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message: "Leased topic orders route field missing is not in the topic schema.",
        },
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects leased gRPC route extraction when topic source metadata is corrupted", () =>
    Effect.gen(function* () {
      const localViewServer = defineViewServerConfig({
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
      const localGrpcFeed = localViewServer.grpcFeed<typeof grpcClients>();
      const feed = localGrpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["region"],
        request: ({ region }) => ({ orderId: region }),
        acquire: () => Stream.never,
        map: ({ value, route }) => ({
          id: `${route.region}:${value.customerId}`,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: route.region,
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveViewServerRuntimeOptions<
        typeof localViewServer.topics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersLease: feed,
          },
        },
      }).pipe(Effect.flatMap((options) => Effect.fromNullishOr(options.grpcOptions)));
      Object.defineProperty(localViewServer.topics.orders, "source", {
        value: grpc.materialized(),
      });
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(localViewServer, {});
      const health = makeViewServerGrpcHealthLedger<typeof localViewServer.topics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        localViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const missingWhereError = yield* manager.liveClient
        .subscribeRuntime("orders", {
          select: ["id"],
          limit: 10,
        })
        .pipe(Effect.flip);
      const nonExactRouteError = yield* manager.liveClient
        .subscribeRuntime("orders", {
          select: ["id"],
          where: {
            region: { startsWith: "u" },
          },
          limit: 10,
        })
        .pipe(Effect.flip);

      expect({
        missingWhereError,
        nonExactRouteError,
      }).toStrictEqual({
        missingWhereError: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message: "Leased topic orders requires exact equality filters for route fields: region.",
        },
        nonExactRouteError: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message: "Leased topic orders route field region must use an exact eq filter.",
        },
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live(
    "fails leased gRPC subscription when feed route metadata changes during acquisition",
    () =>
      Effect.gen(function* () {
        const feed = grpcLeasedFeed({
          streamForRegion: () => Stream.empty,
        });
        const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
        let routeByReads = 0;
        Object.defineProperty(feed, "routeBy", {
          get: () => {
            routeByReads += 1;
            return routeByReads === 1 ? ["region"] : ["region", "status"];
          },
        });
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
        const health = makeLeasedGrpcHealth(grpcOptions);
        const manager = yield* makeViewServerGrpcLeaseManager(
          leasedGrpcViewServer,
          runtimeCore.internalClient,
          runtimeCore.liveClient,
          runtimeCore.internalLiveClient,
          Effect.void,
          grpcOptions,
          health,
        );

        const exit = yield* Effect.exit(
          manager.liveClient.subscribe("orders", leasedOrdersQuery("usa")),
        );

        expect({
          error: Exit.isFailure(exit)
            ? exit.cause.reasons.find(Cause.isFailReason)?.error
            : undefined,
          routeByReads,
        }).toStrictEqual({
          error: {
            _tag: "ViewServerRuntimeError",
            code: "RuntimeUnavailable",
            topic: "orders",
            message: "Leased gRPC route is missing configured field status",
          },
          routeByReads: 2,
        });
        yield* manager.close;
        yield* runtimeCore.close;
      }),
  );

  it.live("fails leased gRPC subscription when acquire fails before returning a stream", () =>
    Effect.gen(function* () {
      let released = 0;
      const feed = grpcLeasedFeed({
        streamForRegion: () => {
          throw new Error("leased acquire exploded");
        },
        release: Effect.sync(() => {
          released += 1;
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const error = yield* Effect.flip(
        manager.liveClient.subscribe("orders", leasedOrdersQuery("usa")),
      );
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "gRPC leased feed acquire failed for ordersLease",
      });
      expect(Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {})).toStrictEqual([]);
      expect(released).toBe(1);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("fails leased gRPC subscription when client creation throws", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedFeed({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
        () => {
          throw new Error("client exploded");
        },
      );

      const error = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.flip);

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "gRPC leased client creation failed for ordersLease",
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("fails leased gRPC subscription when request creation throws", () =>
    Effect.gen(function* () {
      const feed = leasedGrpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["region"],
        request: () => {
          throw new Error("request exploded");
        },
        acquire: () => Stream.never,
        map: ({ value, route }) => ({
          id: `${route.region}:${value.customerId}`,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: route.region,
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const error = yield* Effect.flip(
        manager.liveClient.subscribe("orders", leasedOrdersQuery("usa")),
      );

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "gRPC leased feed request creation failed for ordersLease",
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live(
    "releases leased gRPC scope when topic key lookup fails during subscription wrapping",
    () =>
      Effect.gen(function* () {
        let released = 0;
        const localViewServer = defineViewServerConfig({
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
        const localGrpcFeed = localViewServer.grpcFeed<typeof grpcClients>();
        const feed = localGrpcFeed.leasedFeed({
          topic: "orders",
          client: "orders",
          method: "streamOrders",
          routeBy: ["region"],
          request: ({ region }) => {
            Object.defineProperty(localViewServer.topics, "orders", {
              value: undefined,
            });
            return { orderId: region };
          },
          acquire: () => Stream.never,
          release: () =>
            Effect.sync(() => {
              released += 1;
            }),
          map: ({ value, route }) => ({
            id: `${route.region}:${value.customerId}`,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: route.region,
            updatedAt: value.updatedAt,
          }),
        });
        const resolvedOptions = yield* resolveViewServerRuntimeOptions<
          typeof localViewServer.topics,
          Record<string, string>,
          typeof grpcClients
        >({
          grpc: {
            clients: grpcClients,
            feeds: {
              ordersLease: feed,
            },
          },
        });
        const grpcOptions = yield* Effect.fromNullishOr(resolvedOptions.grpcOptions);
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(localViewServer, {});
        const health = makeViewServerGrpcHealthLedger<typeof localViewServer.topics>({
          clients: grpcOptions.clientBaseUrls,
          feeds: {},
        });
        const manager = yield* makeViewServerGrpcLeaseManager(
          localViewServer,
          runtimeCore.internalClient,
          runtimeCore.liveClient,
          runtimeCore.internalLiveClient,
          Effect.void,
          grpcOptions,
          health,
        );

        const error = yield* manager.liveClient
          .subscribe("orders", leasedOrdersQuery("usa"))
          .pipe(Effect.flip);
        const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

        expect({
          error,
          released,
          leasedFeedKeys: Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {}),
        }).toStrictEqual({
          error: {
            _tag: "ViewServerRuntimeError",
            code: "RuntimeUnavailable",
            topic: "orders",
            message: "gRPC leased feed ordersLease references unknown topic orders",
          },
          released: 1,
          leasedFeedKeys: [],
        });
        yield* manager.close;
        yield* runtimeCore.close;
      }),
  );

  it.live(
    "releases leased gRPC scope when topic key lookup fails during runtime subscription wrapping",
    () =>
      Effect.gen(function* () {
        let released = 0;
        const localViewServer = defineViewServerConfig({
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
        const localGrpcFeed = localViewServer.grpcFeed<typeof grpcClients>();
        const feed = localGrpcFeed.leasedFeed({
          topic: "orders",
          client: "orders",
          method: "streamOrders",
          routeBy: ["region"],
          request: ({ region }) => {
            Object.defineProperty(localViewServer.topics, "orders", {
              value: undefined,
            });
            return { orderId: region };
          },
          acquire: () => Stream.never,
          release: () =>
            Effect.sync(() => {
              released += 1;
            }),
          map: ({ value, route }) => ({
            id: `${route.region}:${value.customerId}`,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: route.region,
            updatedAt: value.updatedAt,
          }),
        });
        const resolvedOptions = yield* resolveViewServerRuntimeOptions<
          typeof localViewServer.topics,
          Record<string, string>,
          typeof grpcClients
        >({
          grpc: {
            clients: grpcClients,
            feeds: {
              ordersLease: feed,
            },
          },
        });
        const grpcOptions = yield* Effect.fromNullishOr(resolvedOptions.grpcOptions);
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(localViewServer, {});
        const health = makeViewServerGrpcHealthLedger<typeof localViewServer.topics>({
          clients: grpcOptions.clientBaseUrls,
          feeds: {},
        });
        const manager = yield* makeViewServerGrpcLeaseManager(
          localViewServer,
          runtimeCore.internalClient,
          runtimeCore.liveClient,
          runtimeCore.internalLiveClient,
          Effect.void,
          grpcOptions,
          health,
        );

        const error = yield* manager.liveClient
          .subscribeRuntime("orders", leasedOrdersQuery("usa"))
          .pipe(Effect.flip);
        const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

        expect({
          error,
          released,
          leasedFeedKeys: Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {}),
        }).toStrictEqual({
          error: {
            _tag: "ViewServerRuntimeError",
            code: "RuntimeUnavailable",
            topic: "orders",
            message: "gRPC leased feed ordersLease references unknown topic orders",
          },
          released: 1,
          leasedFeedKeys: [],
        });
        yield* manager.close;
        yield* runtimeCore.close;
      }),
  );

  it.live("fails leased gRPC subscription when acquire does not return a Stream", () =>
    Effect.gen(function* () {
      const feed = leasedGrpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["region"],
        request: ({ region }) => ({ orderId: region }),
        // @ts-expect-error defensive runtime-boundary test intentionally returns a non-stream.
        acquire: () => "not-a-stream",
        map: ({ value, route }) => ({
          id: `${route.region}:${value.customerId}`,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: route.region,
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const error = yield* Effect.flip(
        manager.liveClient.subscribe("orders", leasedOrdersQuery("usa")),
      );

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "gRPC leased feed acquire did not return a Stream for ordersLease",
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live(
    "marks leased gRPC feed degraded when runtime publishMany violates the client contract",
    () =>
      Effect.gen(function* () {
        const feed = grpcLeasedFeed({
          streamForRegion: (region) => Stream.make(grpcOrderValue(`${region}-order-1`, 10)),
        });
        const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
        Object.defineProperty(runtimeCore.internalClient, "publishManyWithStorageKeys", {
          value: () => "not-an-effect",
        });
        const health = makeLeasedGrpcHealth(grpcOptions);
        const manager = yield* makeViewServerGrpcLeaseManager(
          leasedGrpcViewServer,
          runtimeCore.internalClient,
          runtimeCore.liveClient,
          runtimeCore.internalLiveClient,
          Effect.void,
          grpcOptions,
          health,
        );

        const subscription = yield* manager.liveClient.subscribe(
          "orders",
          leasedOrdersQuery("usa"),
        );
        const degradedHealth = yield* Effect.gen(function* () {
          return health.healthOverlay(yield* runtimeCore.client.health(), 1_000);
        }).pipe(
          Effect.repeat({
            schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
            until: (currentHealth) =>
              currentHealth.grpc?.feeds["orders"]?.leased[
                "orders/ordersLease/leased/region=string%3A3%3Ausa"
              ]?.status === "degraded",
          }),
        );

        expect(
          degradedHealth.grpc?.feeds["orders"]?.leased[
            "orders/ordersLease/leased/region=string%3A3%3Ausa"
          ]?.lastError,
        ).toContain(
          "Runtime publishManyWithStorageKeys did not return an Effect for leased gRPC feed ordersLease",
        );
        yield* subscription.close();
        yield* manager.close;
        yield* runtimeCore.close;
      }),
  );

  it.live("marks leased gRPC feed degraded when runtime publishMany fails", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedFeed({
        streamForRegion: (region) => Stream.make(grpcOrderValue(`${region}-order-1`, 10)),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      Object.defineProperty(runtimeCore.internalClient, "publishManyWithStorageKeys", {
        value: () =>
          Effect.fail(
            new RuntimeTestFailure({
              message: "runtime publishManyWithStorageKeys failed",
            }),
          ),
      });
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const degradedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 1_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            currentHealth.grpc?.feeds["orders"]?.leased[
              "orders/ordersLease/leased/region=string%3A3%3Ausa"
            ]?.status === "degraded",
        }),
      );

      expect(
        degradedHealth.grpc?.feeds["orders"]?.leased[
          "orders/ordersLease/leased/region=string%3A3%3Ausa"
        ]?.lastError,
      ).toContain("gRPC leased feed publish failed for ordersLease");
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("keeps leased gRPC close total when runtime delete violates the client contract", () =>
    Effect.gen(function* () {
      let released = 0;
      const feed = grpcLeasedFeed({
        release: Effect.sync(() => {
          released += 1;
        }),
        streamForRegion: (region) =>
          longRunningGrpcStream([grpcOrderValue(`${region}-order-1`, 10)]),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      Object.defineProperty(runtimeCore.internalClient, "delete", {
        value: () => "not-an-effect",
      });

      yield* subscription.close();
      const idleHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect({
        released,
        leasedFeed:
          idleHealth.grpc?.feeds["orders"]?.leased[
            "orders/ordersLease/leased/region=string%3A3%3Ausa"
          ],
      }).toStrictEqual({
        released: 1,
        leasedFeed: {
          status: "degraded",
          lifecycle: "leased",
          feedName: "ordersLease",
          feedKey: "orders/ordersLease/leased/region=string%3A3%3Ausa",
          topic: "orders",
          subscriberCount: 0,
          rowCount: 1,
          messagesPerSecond: 0,
          rowsPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          reconnects: 0,
          lastMessageAt:
            idleHealth.grpc?.feeds["orders"]?.leased[
              "orders/ordersLease/leased/region=string%3A3%3Ausa"
            ]?.lastMessageAt,
          lastError: "gRPC leased feed row cleanup failed for ordersLease",
        },
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("keeps leased gRPC close total when runtime delete fails", () =>
    Effect.gen(function* () {
      let released = 0;
      const feed = grpcLeasedFeed({
        release: Effect.sync(() => {
          released += 1;
        }),
        streamForRegion: (region) =>
          longRunningGrpcStream([grpcOrderValue(`${region}-order-1`, 10)]),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      Object.defineProperty(runtimeCore.internalClient, "delete", {
        value: () =>
          Effect.fail(
            new RuntimeTestFailure({
              message: "runtime delete failed",
            }),
          ),
      });

      yield* subscription.close();
      const idleHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect({
        released,
        leasedFeed:
          idleHealth.grpc?.feeds["orders"]?.leased[
            "orders/ordersLease/leased/region=string%3A3%3Ausa"
          ],
      }).toStrictEqual({
        released: 1,
        leasedFeed: {
          status: "degraded",
          lifecycle: "leased",
          feedName: "ordersLease",
          feedKey: "orders/ordersLease/leased/region=string%3A3%3Ausa",
          topic: "orders",
          subscriberCount: 0,
          rowCount: 1,
          messagesPerSecond: 0,
          rowsPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          reconnects: 0,
          lastMessageAt:
            idleHealth.grpc?.feeds["orders"]?.leased[
              "orders/ordersLease/leased/region=string%3A3%3Ausa"
            ]?.lastMessageAt,
          lastError: "gRPC leased feed row cleanup failed for ordersLease",
        },
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("fails leased gRPC subscription when the client configuration is missing", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedFeed({
        streamForRegion: () => Stream.never,
      });
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeViewServerGrpcHealthLedger<typeof leasedGrpcViewServer.topics>({
        clients: {},
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        {
          // @ts-expect-error defensive runtime-boundary test intentionally omits the configured client.
          clients: {},
          clientBaseUrls: {},
          feeds: {
            ordersLease: feed,
          },
        },
        health,
      );

      const error = yield* Effect.flip(
        manager.liveClient.subscribe("orders", leasedOrdersQuery("usa")),
      );

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "gRPC leased feed ordersLease references missing client: orders",
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("fails leased gRPC subscription when the client URL is unresolved", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedFeed({
        streamForRegion: () => Stream.never,
      });
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeViewServerGrpcHealthLedger<typeof leasedGrpcViewServer.topics>({
        clients: {},
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        {
          clients: grpcClients,
          clientBaseUrls: {},
          feeds: {
            ordersLease: feed,
          },
          materializedReconnect: fastGrpcMaterializedReconnect,
        },
        health,
      );

      const error = yield* Effect.flip(
        manager.liveClient.subscribe("orders", leasedOrdersQuery("usa")),
      );

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "gRPC leased feed ordersLease references unresolved client URL: orders",
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("marks leased gRPC feed degraded when mapped rows have a non-string row key", () =>
    Effect.gen(function* () {
      const localViewServer = defineViewServerConfig({
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
      const localGrpcFeed = localViewServer.grpcFeed<typeof grpcClients>();
      const firstValue = yield* Deferred.make<GrpcOrderValueMessage>();
      const feed = localGrpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["region"],
        request: ({ region }) => ({ orderId: region }),
        acquire: () =>
          Stream.fromEffect(Deferred.await(firstValue)).pipe(Stream.concat(Stream.never)),
        map: ({ value, route }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: route.region,
          updatedAt: value.updatedAt,
        }),
      });
      const resolvedOptions = yield* resolveViewServerRuntimeOptions<
        typeof localViewServer.topics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersLease: feed,
          },
        },
      });
      const grpcOptions = yield* Effect.fromNullishOr(resolvedOptions.grpcOptions);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(localViewServer, {});
      const health = makeViewServerGrpcHealthLedger<typeof localViewServer.topics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        localViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribeRuntime("orders", {
        select: ["id"],
        where: {
          region: { eq: "usa" },
        },
        limit: 10,
      });
      Object.defineProperty(localViewServer.topics.orders, "key", {
        value: "price",
      });
      yield* Deferred.succeed(firstValue, grpcOrderValue("numeric-key", 10));
      const degradedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            currentHealth.grpc?.feeds["orders"]?.leased[
              "orders/ordersLease/leased/region=string%3A3%3Ausa"
            ]?.status === "degraded",
        }),
      );

      expect(
        degradedHealth.grpc?.feeds["orders"]?.leased[
          "orders/ordersLease/leased/region=string%3A3%3Ausa"
        ]?.lastError,
      ).toContain("gRPC leased feed row key price for orders is not a string");
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("closes active leased gRPC feeds when the manager closes", () =>
    Effect.gen(function* () {
      let released = 0;
      const streamInterrupted = yield* Deferred.make<void>();
      const feed = grpcLeasedFeed({
        release: Effect.sync(() => {
          released += 1;
        }),
        streamForRegion: (region) =>
          longRunningGrpcStream([grpcOrderValue(`${region}-order-1`, 10)]).pipe(
            Stream.ensuring(Deferred.succeed(streamInterrupted, undefined)),
          ),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const eventQueue = yield* Queue.unbounded<unknown>();
      const eventsFiber = yield* subscription.events.pipe(
        Stream.runForEach((event) => Queue.offer(eventQueue, event)),
        Effect.forkChild,
      );
      const snapshotEvent = yield* Queue.take(eventQueue);
      const insertEvent = yield* Queue.take(eventQueue);
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      yield* manager.close;
      const shutdownStatusEvent = yield* Queue.poll(eventQueue);
      yield* Deferred.await(streamInterrupted);
      const closedSubscribeError = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.flip);
      const emptySnapshot = yield* waitForLeasedGrpcSnapshotRows(
        runtimeCore.internalClient,
        "usa",
        0,
      );
      const stoppedHealth = health.healthOverlay(yield* runtimeCore.client.health(), 3_000);

      expect({
        closedSubscribeError,
        snapshotEvent,
        insertEvent,
        shutdownStatusEvent,
        released,
        rows: emptySnapshot.rows,
        totalRows: emptySnapshot.totalRows,
        leasedFeeds: Object.keys(stoppedHealth.grpc?.feeds["orders"]?.leased ?? {}),
      }).toStrictEqual({
        closedSubscribeError: {
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          topic: "orders",
          message: "gRPC leased feed manager is closed.",
        },
        snapshotEvent: {
          type: "snapshot",
          topic: "orders",
          queryId: "query-0",
          version: 0,
          keys: [],
          rows: [],
          totalRows: 0,
        },
        insertEvent: {
          type: "delta",
          topic: "orders",
          queryId: "query-0",
          fromVersion: 0,
          toVersion: 1,
          operations: [
            {
              type: "insert",
              key: "usa:usa-order-1",
              row: {
                id: "usa:usa-order-1",
                customerId: "usa-order-1",
                price: 10,
                region: "usa",
              },
              index: 0,
            },
          ],
          totalRows: 1,
        },
        shutdownStatusEvent: Option.none(),
        released: 1,
        rows: [],
        totalRows: 0,
        leasedFeeds: [],
      });
      yield* Fiber.interrupt(eventsFiber);
      yield* subscription.close();
      yield* runtimeCore.close;
    }),
  );

  it.live("closes leased gRPC feeds when release callback throws", () =>
    Effect.gen(function* () {
      const feed = leasedGrpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["region"],
        request: ({ region }) => ({ orderId: region }),
        acquire: ({ route }) =>
          longRunningGrpcStream([grpcOrderValue(`${route.region}-order-1`, 10)]),
        release: () => {
          throw new Error("release exploded");
        },
        map: ({ value, route }) => ({
          id: `${route.region}:${value.customerId}`,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: route.region,
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      yield* subscription.close();
      const emptySnapshot = yield* waitForLeasedGrpcSnapshotRows(
        runtimeCore.internalClient,
        "usa",
        0,
      );
      const idleHealth = health.healthOverlay(yield* runtimeCore.client.health(), 3_000);

      expect({
        totalRows: emptySnapshot.totalRows,
        leasedFeeds: Object.keys(idleHealth.grpc?.feeds["orders"]?.leased ?? {}),
      }).toStrictEqual({
        totalRows: 0,
        leasedFeeds: [],
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("closes leased gRPC feeds when release callback returns a non-Effect", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedFeed({
        streamForRegion: (region) =>
          longRunningGrpcStream([grpcOrderValue(`${region}-order-1`, 10)]),
      });
      Object.defineProperty(feed, "release", {
        value: () => "not-an-effect",
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      yield* subscription.close();
      const emptySnapshot = yield* waitForLeasedGrpcSnapshotRows(
        runtimeCore.internalClient,
        "usa",
        0,
      );
      const idleHealth = health.healthOverlay(yield* runtimeCore.client.health(), 3_000);

      expect({
        totalRows: emptySnapshot.totalRows,
        leasedFeeds: Object.keys(idleHealth.grpc?.feeds["orders"]?.leased ?? {}),
      }).toStrictEqual({
        totalRows: 0,
        leasedFeeds: [],
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("marks leased gRPC feed degraded when mapping throws", () =>
    Effect.gen(function* () {
      const feed = leasedGrpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["region"],
        request: ({ region }) => ({ orderId: region }),
        acquire: () => Stream.make(grpcOrderValue("bad-map", 10)).pipe(Stream.concat(Stream.never)),
        map: () => {
          throw new Error("mapping exploded");
        },
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const degradedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 3_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            currentHealth.grpc?.feeds["orders"]?.leased[
              "orders/ordersLease/leased/region=string%3A3%3Ausa"
            ]?.status === "degraded",
        }),
      );

      expect(degradedHealth.status).toBe("degraded");
      expect(
        degradedHealth.grpc?.feeds["orders"]?.leased[
          "orders/ordersLease/leased/region=string%3A3%3Ausa"
        ]?.lastError,
      ).toContain("gRPC leased feed mapping failed for ordersLease");
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("marks leased gRPC feed degraded when mapping returns an invalid row", () =>
    Effect.gen(function* () {
      const feed = leasedGrpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["region"],
        request: ({ region }) => ({ orderId: region }),
        acquire: () =>
          Stream.make(grpcOrderValue("invalid-row", 10)).pipe(Stream.concat(Stream.never)),
        map: ({ value, route }) => {
          const row = {
            id: `${route.region}:${value.customerId}`,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: route.region,
            updatedAt: value.updatedAt,
          };
          Object.defineProperty(row, "status", { value: "not-a-status" });
          return row;
        },
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const degradedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 3_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            currentHealth.grpc?.feeds["orders"]?.leased[
              "orders/ordersLease/leased/region=string%3A3%3Ausa"
            ]?.status === "degraded",
        }),
      );

      expect(degradedHealth.status).toBe("degraded");
      expect(
        degradedHealth.grpc?.feeds["orders"]?.leased[
          "orders/ordersLease/leased/region=string%3A3%3Ausa"
        ]?.lastError,
      ).toContain("gRPC leased feed mapping produced an invalid row for ordersLease");
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("marks leased gRPC feed degraded when upstream stream fails", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedFeed({
        streamForRegion: () => Stream.fail("upstream down"),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const degradedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 3_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            currentHealth.grpc?.feeds["orders"]?.leased[
              "orders/ordersLease/leased/region=string%3A3%3Ausa"
            ]?.status === "degraded",
        }),
      );

      expect({
        runtimeStatus: degradedHealth.status,
        clientStatus: degradedHealth.grpc?.clients["orders"]?.status,
        feedStatus:
          degradedHealth.grpc?.feeds["orders"]?.leased[
            "orders/ordersLease/leased/region=string%3A3%3Ausa"
          ]?.status,
      }).toStrictEqual({
        runtimeStatus: "degraded",
        clientStatus: "degraded",
        feedStatus: "degraded",
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("externalizes leased gRPC cleanup remove deltas after upstream completion", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedFeed({
        streamForRegion: (region) => Stream.make(grpcOrderValue(`${region}-order-1`, 10)),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const eventQueue = yield* Queue.unbounded<unknown>();
      const eventsFiber = yield* subscription.events.pipe(
        Stream.runForEach((event) => Queue.offer(eventQueue, event)),
        Effect.forkChild,
      );
      const snapshot = yield* Queue.take(eventQueue);
      const insertDelta = yield* Queue.take(eventQueue);
      const removeDelta = yield* Queue.take(eventQueue);
      const terminalStatus = yield* Queue.take(eventQueue);

      expect(snapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(insertDelta).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "insert",
            key: "usa:usa-order-1",
            row: {
              id: "usa:usa-order-1",
              customerId: "usa-order-1",
              price: 10,
              region: "usa",
            },
            index: 0,
          },
        ],
        totalRows: 1,
      });
      expect(removeDelta).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "remove",
            key: "usa:usa-order-1",
          },
        ],
        totalRows: 0,
      });
      expect(terminalStatus).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "error",
        code: "RuntimeUnavailable",
        message: "gRPC leased upstream completed unexpectedly.",
      });
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 3_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {}).length === 0,
        }),
      );
      expect(Object.keys(cleanedHealth.grpc?.feeds["orders"]?.leased ?? {})).toStrictEqual([]);

      yield* subscription.close();
      yield* Fiber.interrupt(eventsFiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("keeps leased gRPC subscribers informed when upstream cleanup delete fails", () =>
    Effect.gen(function* () {
      const completeUpstream = yield* Deferred.make<void>();
      const feed = grpcLeasedFeed({
        streamForRegion: (region) =>
          Stream.make(grpcOrderValue(`${region}-order-1`, 10)).pipe(
            Stream.concat(Stream.fromEffect(Deferred.await(completeUpstream)).pipe(Stream.drain)),
          ),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const eventQueue = yield* Queue.unbounded<unknown>();
      const eventsFiber = yield* subscription.events.pipe(
        Stream.runForEach((event) => Queue.offer(eventQueue, event)),
        Effect.forkChild,
      );
      const snapshot = yield* Queue.take(eventQueue);
      const insertDelta = yield* Queue.take(eventQueue);
      Object.defineProperty(runtimeCore.internalClient, "delete", {
        value: () =>
          Effect.fail(
            new RuntimeTestFailure({
              message: "runtime delete failed during upstream cleanup",
            }),
          ),
      });
      yield* Deferred.succeed(completeUpstream, undefined);
      const terminalStatus = yield* Queue.take(eventQueue);

      expect(snapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(insertDelta).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "insert",
            key: "usa:usa-order-1",
            row: {
              id: "usa:usa-order-1",
              customerId: "usa-order-1",
              price: 10,
              region: "usa",
            },
            index: 0,
          },
        ],
        totalRows: 1,
      });
      expect(terminalStatus).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "error",
        code: "RuntimeUnavailable",
        message: "gRPC leased upstream completed unexpectedly.",
      });
      const degradedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 3_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            currentHealth.grpc?.feeds["orders"]?.leased[
              "orders/ordersLease/leased/region=string%3A3%3Ausa"
            ]?.rowCount === 1,
        }),
      );
      expect(
        degradedHealth.grpc?.feeds["orders"]?.leased[
          "orders/ordersLease/leased/region=string%3A3%3Ausa"
        ]?.lastError,
      ).toContain("gRPC leased feed row cleanup failed for ordersLease");

      yield* subscription.close();
      yield* Fiber.interrupt(eventsFiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("marks leased gRPC feed degraded when upstream self-interrupts", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedFeed({
        streamForRegion: () => Stream.fromEffect(Effect.interrupt),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const degradedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 3_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            currentHealth.grpc?.clients["orders"]?.status === "degraded" &&
            currentHealth.grpc?.feeds["orders"]?.leased[
              "orders/ordersLease/leased/region=string%3A3%3Ausa"
            ]?.status === "degraded",
        }),
      );

      expect({
        clientStatus: degradedHealth.grpc?.clients["orders"]?.status,
        feedStatus:
          degradedHealth.grpc?.feeds["orders"]?.leased[
            "orders/ordersLease/leased/region=string%3A3%3Ausa"
          ]?.status,
      }).toStrictEqual({
        clientStatus: "degraded",
        feedStatus: "degraded",
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects leased gRPC topics when no leased feed is configured", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeViewServerGrpcHealthLedger<typeof leasedGrpcViewServer.topics>({
        clients: {
          orders: "https://orders.example.test",
        },
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        {
          clients: grpcClients,
          clientBaseUrls: nullRecord([["orders", "https://orders.example.test"]]),
          feeds: {},
          materializedReconnect: fastGrpcMaterializedReconnect,
        },
        health,
      );

      const error = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.flip);

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "Leased gRPC topic orders has no configured leased feed.",
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("delegates non-leased topics through the gRPC lease manager", () =>
    Effect.gen(function* () {
      const localViewServer = defineViewServerConfig({
        topics: {
          orders: {
            schema: GrpcOrder,
            key: "id",
            source: grpc.leased({
              routeBy: ["region"],
            }),
          },
          audit: {
            schema: Order,
            key: "id",
          },
        },
      });
      const localGrpcFeed = localViewServer.grpcFeed<typeof grpcClients>();
      const mixedLeaseFeed = localGrpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["region"],
        request: ({ region }) => ({ orderId: region }),
        acquire: () => Stream.never,
        map: ({ value, route }) => ({
          id: `${route.region}:${value.customerId}`,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: route.region,
          updatedAt: value.updatedAt,
        }),
      });
      const resolvedOptions = yield* resolveViewServerRuntimeOptions<
        typeof localViewServer.topics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersLease: mixedLeaseFeed,
          },
        },
      });
      const grpcOptions = yield* Effect.fromNullishOr(resolvedOptions.grpcOptions);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(localViewServer, {});
      const health = makeViewServerGrpcHealthLedger<typeof localViewServer.topics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        localViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      yield* manager.client.publish("audit", order("audit-1", 42));
      yield* manager.client.publishMany("audit", [order("audit-2", 5)]);
      yield* manager.client.patch("audit", "audit-2", { price: 7 });
      yield* manager.client.delete("audit", "audit-1");
      const snapshot = yield* manager.client.snapshot("audit", {
        select: ["id", "price"],
        limit: 10,
      });
      const subscription = yield* manager.liveClient.subscribe("audit", {
        select: ["id", "price"],
        limit: 10,
      });
      const runtimeSubscription = yield* manager.liveClient.subscribeRuntime("audit", {
        select: ["id", "price"],
        limit: 10,
      });
      const event = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);
      const runtimeEvent = yield* runtimeSubscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
      );

      expect({
        snapshot,
        event: Array.from(event),
        runtimeEvent: Array.from(runtimeEvent),
      }).toStrictEqual({
        snapshot: {
          version: 4,
          rows: [{ id: "audit-2", price: 7 }],
          totalRows: 1,
          status: "ready",
          statusCode: "Ready",
        },
        event: [
          {
            type: "snapshot",
            topic: "audit",
            queryId: "query-0",
            version: 4,
            keys: ["audit-2"],
            rows: [{ id: "audit-2", price: 7 }],
            totalRows: 1,
          },
        ],
        runtimeEvent: [
          {
            type: "snapshot",
            topic: "audit",
            queryId: "query-1",
            version: 4,
            keys: ["audit-2"],
            rows: [{ id: "audit-2", price: 7 }],
            totalRows: 1,
          },
        ],
      });
      yield* runtimeSubscription.close();
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("cleans completed leased feeds and allows later subscribers to reacquire", () =>
    Effect.gen(function* () {
      let released = 0;
      const feed = grpcLeasedFeed({
        streamForRegion: (region) => Stream.make(grpcOrderValue(`${region}-order-1`, 10)),
        release: Effect.sync(() => {
          released += 1;
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const first = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const degradedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            currentHealth.grpc?.feeds["orders"]?.leased[
              "orders/ordersLease/leased/region=string%3A3%3Ausa"
            ]?.status === "degraded",
        }),
      );
      const rejectedDuringActiveTerminal = yield* Effect.flip(
        manager.liveClient.subscribe("orders", leasedOrdersQuery("usa")),
      );
      const eventQueue = yield* Queue.unbounded<unknown>();
      const eventsFiber = yield* first.events.pipe(
        Stream.runForEach((event) => Queue.offer(eventQueue, event)),
        Effect.forkChild,
      );
      const terminalStatus = yield* Queue.take(eventQueue).pipe(
        Effect.repeat({
          until: (event) => Reflect.get(Object(event), "type") === "status",
        }),
        Effect.timeout("1 second"),
      );

      expect(terminalStatus).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "error",
        code: "RuntimeUnavailable",
        message: "gRPC leased upstream completed unexpectedly.",
      });
      expect({
        released,
        runtimeStatus: degradedHealth.status,
        clientStatus: degradedHealth.grpc?.clients["orders"]?.status,
        feedStatus:
          degradedHealth.grpc?.feeds["orders"]?.leased[
            "orders/ordersLease/leased/region=string%3A3%3Ausa"
          ]?.status,
        rejectedDuringActiveTerminal,
      }).toStrictEqual({
        released: 1,
        runtimeStatus: "degraded",
        clientStatus: "degraded",
        feedStatus: "degraded",
        rejectedDuringActiveTerminal: {
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          topic: "orders",
          message:
            "gRPC leased upstream is not accepting new subscribers after completion or failure.",
        },
      });
      yield* first.close();
      yield* Fiber.interrupt(eventsFiber);
      const idleHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 3_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {}).length === 0,
        }),
      );
      expect({
        released,
        runtimeStatus: idleHealth.status,
        clientStatus: idleHealth.grpc?.clients["orders"]?.status,
        leasedFeeds: Object.keys(idleHealth.grpc?.feeds["orders"]?.leased ?? {}),
      }).toStrictEqual({
        released: 1,
        runtimeStatus: "ready",
        clientStatus: "connected",
        leasedFeeds: [],
      });

      const second = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const secondEvents = yield* second.events.pipe(Stream.take(4), Stream.runCollect);

      expect(
        Array.from(secondEvents).map((event) => Reflect.get(Object(event), "type")),
      ).toStrictEqual(["snapshot", "delta", "delta", "status"]);
      yield* second.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live(
    "delivers terminal status when a leased gRPC stream completes before publishing rows",
    () =>
      Effect.gen(function* () {
        const feed = grpcLeasedFeed({
          streamForRegion: () => Stream.empty,
        });
        const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
        const health = makeLeasedGrpcHealth(grpcOptions);
        const manager = yield* makeViewServerGrpcLeaseManager(
          leasedGrpcViewServer,
          runtimeCore.internalClient,
          runtimeCore.liveClient,
          runtimeCore.internalLiveClient,
          Effect.void,
          grpcOptions,
          health,
        );

        const subscription = yield* manager.liveClient.subscribe(
          "orders",
          leasedOrdersQuery("usa"),
        );
        const eventQueue = yield* Queue.unbounded<unknown>();
        const eventsFiber = yield* subscription.events.pipe(
          Stream.runForEach((event) => Queue.offer(eventQueue, event)),
          Effect.forkChild,
        );
        const snapshotEvent = yield* Queue.take(eventQueue);
        const terminalStatus = yield* Queue.take(eventQueue).pipe(Effect.timeout("1 second"));
        const cleanedHealth = yield* Effect.gen(function* () {
          return health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
        }).pipe(
          Effect.repeat({
            schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
            until: (currentHealth) =>
              Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {}).length === 0,
          }),
        );

        expect({
          snapshotEvent,
          terminalStatus,
          runtimeStatus: cleanedHealth.status,
          leasedFeeds: Object.keys(cleanedHealth.grpc?.feeds["orders"]?.leased ?? {}),
        }).toStrictEqual({
          snapshotEvent: {
            type: "snapshot",
            topic: "orders",
            queryId: "query-0",
            version: 0,
            keys: [],
            rows: [],
            totalRows: 0,
          },
          terminalStatus: {
            type: "status",
            topic: "orders",
            queryId: "query-0",
            status: "error",
            code: "RuntimeUnavailable",
            message: "gRPC leased upstream completed unexpectedly.",
          },
          runtimeStatus: "ready",
          leasedFeeds: [],
        });
        yield* subscription.close();
        yield* Fiber.interrupt(eventsFiber);
        yield* manager.close;
        yield* runtimeCore.close;
      }),
  );

  it.live("marks materialized gRPC feed degraded when stream completion exhausts reconnects", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeed(Stream.make(grpcOrderValue("order-1", 10)));
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => {
            const feedHealth = grpcHealthFeed(currentHealth);
            return (
              feedHealth?.status === "degraded" &&
              feedHealth.reconnects === 3 &&
              feedHealth.lastError === "gRPC feed ordersFeed completed unexpectedly."
            );
          },
        }),
      );

      expect(grpcHealthFeed(degradedHealth)?.lastError).toBe(
        "gRPC feed ordersFeed completed unexpectedly.",
      );
      expect(grpcHealthFeed(degradedHealth)?.reconnects).toBe(3);
      expect(grpcHealthClient(degradedHealth)?.activeFeeds).toBe(0);
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live(
    "marks materialized gRPC feed degraded when delayed stream completion exhausts reconnects",
    () =>
      Effect.gen(function* () {
        let acquireCount = 0;
        const streams = [
          Stream.fromEffect(Effect.sleep("20 millis")).pipe(Stream.drain),
          Stream.fromEffect(Effect.sleep("20 millis")).pipe(Stream.drain),
        ];
        const feed = grpcFeed.materializedFeed({
          topic: "orders",
          client: "orders",
          method: "streamOrders",
          request: () => ({ orderId: "all" }),
          acquire: () => {
            const stream = streams[acquireCount] ?? Stream.never;
            acquireCount += 1;
            return stream;
          },
          map: ({ value }) => ({
            id: value.customerId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: "usa",
            updatedAt: value.updatedAt,
          }),
        });
        const options = yield* resolveViewServerRuntimeOptions<
          GrpcTopics,
          Record<string, string>,
          typeof grpcClients
        >({
          grpc: {
            clients: grpcClients,
            feeds: {
              ordersFeed: feed,
            },
            materializedReconnect: {
              delay: "10 millis",
              maxReconnects: 1,
            },
          },
        });
        const grpcOptions = yield* Effect.fromNullishOr(options.grpcOptions);
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
        const health = makeGrpcHealth(grpcOptions);
        const ingress = yield* makeViewServerGrpcIngress(
          grpcViewServer,
          runtimeCore.client,
          Effect.void,
          grpcOptions,
          health,
        );

        const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
          Effect.repeat({
            schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
            until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
          }),
        );

        expect(grpcHealthFeed(degradedHealth)?.lastError).toBe(
          "gRPC feed ordersFeed completed unexpectedly.",
        );
        expect(grpcHealthFeed(degradedHealth)?.reconnects).toBe(1);
        expect(acquireCount).toBe(2);
        yield* ingress.close;
        yield* runtimeCore.close;
      }),
  );

  it.live("uses one materialized gRPC reconnect budget across completion and failure", () =>
    Effect.gen(function* () {
      let acquireCount = 0;
      const streams = [Stream.empty, Stream.fail("upstream down"), Stream.never];
      const feed = grpcFeed.materializedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => {
          const stream = streams[acquireCount] ?? Stream.never;
          acquireCount += 1;
          return stream;
        },
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const options = yield* resolveViewServerRuntimeOptions<
        GrpcTopics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersFeed: feed,
          },
          materializedReconnect: {
            delay: "10 millis",
            maxReconnects: 1,
          },
        },
      });
      const grpcOptions = yield* Effect.fromNullishOr(options.grpcOptions);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect({
        acquireCount,
        lastError: grpcHealthFeed(degradedHealth)?.lastError,
        reconnects: grpcHealthFeed(degradedHealth)?.reconnects,
      }).toStrictEqual({
        acquireCount: 2,
        lastError:
          "gRPC feed ordersFeed failed: gRPC feed stream failed for ordersFeed: upstream down",
        reconnects: 1,
      });
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("marks materialized gRPC feed stopping when the stream is interrupted", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeed(Stream.failCause(Cause.interrupt()));
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const stoppingHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "stopping",
        }),
      );

      expect(grpcHealthFeed(stoppingHealth)?.lastError).toBe(null);
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("keeps materialized gRPC stream interruption terminal when release fails", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const feed = grpcFeed.materializedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => Stream.failCause(Cause.interrupt()),
        release: () =>
          Effect.gen(function* () {
            releaseCount += 1;
            return yield* Effect.fail("release down");
          }),
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const stoppingHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "stopping",
        }),
      );

      expect({
        lastError: grpcHealthFeed(stoppingHealth)?.lastError,
        reconnects: grpcHealthFeed(stoppingHealth)?.reconnects,
        releaseCount,
      }).toStrictEqual({
        lastError: null,
        reconnects: 0,
        releaseCount: 1,
      });
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live(
    "does not reconnect materialized gRPC feed when failure cause includes interruption",
    () =>
      Effect.gen(function* () {
        const feed = grpcMaterializedFeed(
          Stream.failCause(
            Cause.fromReasons([
              Cause.makeFailReason("upstream down during shutdown"),
              Cause.makeInterruptReason(),
            ]),
          ),
        );
        const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
        const health = makeGrpcHealth(grpcOptions);
        const ingress = yield* makeViewServerGrpcIngress(
          grpcViewServer,
          runtimeCore.client,
          Effect.void,
          grpcOptions,
          health,
        );

        const stoppingHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
          Effect.repeat({
            schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
            until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "stopping",
          }),
        );

        expect(grpcHealthFeed(stoppingHealth)).toStrictEqual({
          status: "stopping",
          lifecycle: "materialized",
          feedName: "ordersFeed",
          feedKey: "orders/ordersFeed/materialized",
          topic: "orders",
          subscriberCount: 0,
          rowCount: 0,
          messagesPerSecond: 0,
          rowsPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          reconnects: 0,
          lastMessageAt: null,
          lastError: null,
        });
        yield* ingress.close;
        yield* runtimeCore.close;
      }),
  );

  it.live("publishes materialized gRPC stream rows into runtime core and health", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeed(
        longRunningGrpcStream([grpcOrderValue("order-1", 10), grpcOrderValue("order-2", 5)]),
      );
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeViewServerGrpcHealthLedger<GrpcTopics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {
          ordersFeed: {
            client: "orders",
            lifecycle: "materialized",
            topic: "orders",
          },
        },
      });
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const snapshot = yield* waitForGrpcSnapshotRows(runtimeCore.client, 2);
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      const clientHealth = currentHealth.grpc?.clients["orders"];
      const feedHealth = currentHealth.grpc?.feeds["orders"]?.materialized["ordersFeed"];

      expect(snapshot).toStrictEqual({
        rows: [
          { id: "order-2", price: 5 },
          { id: "order-1", price: 10 },
        ],
        totalRows: 2,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      expect(clientHealth).toStrictEqual({
        status: "connected",
        baseUrl: "https://orders.example.test",
        activeFeeds: 1,
        lastConnectedAt: clientHealth?.lastConnectedAt,
        lastError: null,
      });
      expect(feedHealth).toStrictEqual({
        status: "ready",
        lifecycle: "materialized",
        feedName: "ordersFeed",
        feedKey: "orders/ordersFeed/materialized",
        topic: "orders",
        subscriberCount: 0,
        rowCount: 2,
        messagesPerSecond: 0,
        rowsPerSecond: 0,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        reconnects: 0,
        lastMessageAt: feedHealth?.lastMessageAt,
        lastError: null,
      });
      expect(typeof clientHealth?.lastConnectedAt).toBe("number");
      expect(typeof feedHealth?.lastMessageAt).toBe("number");

      yield* ingress.close;
      const stoppedHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      expect(stoppedHealth.grpc?.feeds["orders"]?.materialized["ordersFeed"]?.status).toBe(
        "stopping",
      );
      yield* runtimeCore.close;
    }),
  );

  it.live(
    "reports materialized gRPC row count from engine health instead of cumulative publishes",
    () =>
      Effect.gen(function* () {
        const feed = grpcMaterializedFeed(
          longRunningGrpcStream([
            grpcOrderValue("order-1", 10),
            grpcOrderValue("order-1", 20),
            grpcOrderValue("order-1", 30),
          ]),
        );
        const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
        const health = makeGrpcHealth(grpcOptions);
        const ingress = yield* makeViewServerGrpcIngress(
          grpcViewServer,
          runtimeCore.client,
          Effect.void,
          grpcOptions,
          health,
        );

        const snapshot = yield* waitForGrpcSnapshotRows(runtimeCore.client, 1);
        const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

        expect(snapshot).toStrictEqual({
          rows: [{ id: "order-1", price: 30 }],
          totalRows: 1,
          version: 1,
          status: "ready",
          statusCode: "Ready",
        });
        expect(currentHealth.grpc?.feeds["orders"]?.materialized["ordersFeed"]?.rowCount).toBe(1);

        yield* ingress.close;
        yield* runtimeCore.close;
      }),
  );

  it.live("marks materialized gRPC feed degraded when the stream defects", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeed(Stream.fromEffect(Effect.die("defect down")));
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect(grpcHealthFeed(degradedHealth)?.lastError).toContain("gRPC feed ordersFeed failed:");
      expect(grpcHealthFeed(degradedHealth)?.lastError).toContain("defect down");
      expect(grpcHealthFeed(degradedHealth)?.reconnects).toBe(0);
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("does not reset materialized gRPC reconnect budget during slow release", () =>
    Effect.gen(function* () {
      let acquireCount = 0;
      let releaseCount = 0;
      const feed = grpcFeed.materializedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => {
          acquireCount += 1;
          return Stream.fail("upstream down");
        },
        release: () =>
          Effect.gen(function* () {
            releaseCount += 1;
            yield* Effect.sleep("25 millis");
          }),
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const options = yield* resolveViewServerRuntimeOptions<
        GrpcTopics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersFeed: feed,
          },
          materializedReconnect: {
            delay: "10 millis",
            maxReconnects: 1,
          },
        },
      });
      const grpcOptions = yield* Effect.fromNullishOr(options.grpcOptions);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect({
        acquireCount,
        releaseCount,
        reconnects: grpcHealthFeed(degradedHealth)?.reconnects,
      }).toStrictEqual({
        acquireCount: 2,
        releaseCount: 2,
        reconnects: 1,
      });
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("resets materialized gRPC reconnect failure streak after publishing a batch", () =>
    Effect.gen(function* () {
      let acquireCount = 0;
      const failAfterProgress = yield* Deferred.make<void>();
      const streams = [
        Stream.fail("first transient failure"),
        Stream.make(grpcOrderValue("progress-row", 11)).pipe(
          Stream.concat(
            Stream.fromEffect(Deferred.await(failAfterProgress)).pipe(
              Stream.drain,
              Stream.concat(Stream.fail("second transient failure after progress")),
            ),
          ),
        ),
        longRunningGrpcStream([grpcOrderValue("final-row", 12)]),
      ];
      const feed = grpcFeed.materializedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => {
          const stream = streams[acquireCount] ?? Stream.never;
          acquireCount += 1;
          return stream;
        },
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const options = yield* resolveViewServerRuntimeOptions<
        GrpcTopics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersFeed: feed,
          },
          materializedReconnect: {
            delay: "10 millis",
            maxReconnects: 1,
          },
        },
      });
      const grpcOptions = yield* Effect.fromNullishOr(options.grpcOptions);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const progressSnapshot = yield* waitForGrpcSnapshotRows(runtimeCore.client, 1);
      yield* Deferred.succeed(failAfterProgress, undefined);
      const finalSnapshot = yield* waitForGrpcSnapshotRows(runtimeCore.client, 2);
      const finalHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.reconnects === 2,
        }),
      );

      expect(progressSnapshot).toStrictEqual({
        rows: [{ id: "progress-row", price: 11 }],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      expect(finalSnapshot).toStrictEqual({
        rows: [
          { id: "progress-row", price: 11 },
          { id: "final-row", price: 12 },
        ],
        totalRows: 2,
        version: 2,
        status: "ready",
        statusCode: "Ready",
      });
      expect(grpcHealthFeed(finalHealth)?.reconnects).toBe(2);
      expect(acquireCount).toBe(3);

      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live(
    "resets materialized gRPC reconnect failure streak after staying open for one delay",
    () =>
      Effect.gen(function* () {
        let acquireCount = 0;
        const streams = [
          Stream.fail("first transient failure"),
          Stream.fromEffect(Effect.sleep("20 millis")).pipe(
            Stream.drain,
            Stream.concat(Stream.fail("second transient failure after stable open")),
          ),
          longRunningGrpcStream([grpcOrderValue("stable-reset-row", 13)]),
        ];
        const feed = grpcFeed.materializedFeed({
          topic: "orders",
          client: "orders",
          method: "streamOrders",
          request: () => ({ orderId: "all" }),
          acquire: () => {
            const stream = streams[acquireCount] ?? Stream.never;
            acquireCount += 1;
            return stream;
          },
          map: ({ value }) => ({
            id: value.customerId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: "usa",
            updatedAt: value.updatedAt,
          }),
        });
        const options = yield* resolveViewServerRuntimeOptions<
          GrpcTopics,
          Record<string, string>,
          typeof grpcClients
        >({
          grpc: {
            clients: grpcClients,
            feeds: {
              ordersFeed: feed,
            },
            materializedReconnect: {
              delay: "10 millis",
              maxReconnects: 1,
            },
          },
        });
        const grpcOptions = yield* Effect.fromNullishOr(options.grpcOptions);
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
        const health = makeGrpcHealth(grpcOptions);
        const ingress = yield* makeViewServerGrpcIngress(
          grpcViewServer,
          runtimeCore.client,
          Effect.void,
          grpcOptions,
          health,
        );

        const snapshot = yield* waitForGrpcSnapshotRows(runtimeCore.client, 1);
        const finalHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
          Effect.repeat({
            schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
            until: (currentHealth) => grpcHealthFeed(currentHealth)?.reconnects === 2,
          }),
        );

        expect(snapshot).toStrictEqual({
          rows: [{ id: "stable-reset-row", price: 13 }],
          totalRows: 1,
          version: 1,
          status: "ready",
          statusCode: "Ready",
        });
        expect(grpcHealthFeed(finalHealth)?.reconnects).toBe(2);
        expect(acquireCount).toBe(3);

        yield* ingress.close;
        yield* runtimeCore.close;
      }),
  );

  it.live("reconnects materialized gRPC feed after a transient upstream failure", () =>
    Effect.gen(function* () {
      let acquireCount = 0;
      let releaseCount = 0;
      const streams = [
        Stream.fail("upstream down"),
        longRunningGrpcStream([grpcOrderValue("order-after-reconnect", 10)]),
      ];
      const feed = grpcFeed.materializedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => {
          const stream = streams[acquireCount] ?? Stream.never;
          acquireCount += 1;
          return stream;
        },
        release: () =>
          Effect.sync(() => {
            releaseCount += 1;
          }),
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const readyHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => {
            const currentFeed = grpcHealthFeed(currentHealth);
            return (
              currentFeed?.status === "ready" &&
              currentFeed.reconnects === 1 &&
              currentFeed.rowCount === 1
            );
          },
        }),
      );
      const snapshot = yield* waitForGrpcSnapshotRows(runtimeCore.client, 1);
      const feedHealth = grpcHealthFeed(readyHealth);

      expect(snapshot).toStrictEqual({
        rows: [{ id: "order-after-reconnect", price: 10 }],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      expect(feedHealth).toStrictEqual({
        status: "ready",
        lifecycle: "materialized",
        feedName: "ordersFeed",
        feedKey: "orders/ordersFeed/materialized",
        topic: "orders",
        subscriberCount: 0,
        rowCount: 1,
        messagesPerSecond: 1,
        rowsPerSecond: 1,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        reconnects: 1,
        lastMessageAt: feedHealth?.lastMessageAt,
        lastError: null,
      });
      expect(acquireCount).toBe(2);
      expect(releaseCount).toBe(1);

      yield* ingress.close;
      expect(releaseCount).toBe(2);
      yield* runtimeCore.close;
    }),
  );

  it.live("marks materialized gRPC feed degraded when release fails after stream failure", () =>
    Effect.gen(function* () {
      let acquireCount = 0;
      let releaseCount = 0;
      const feed = grpcFeed.materializedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => {
          acquireCount += 1;
          return Stream.fail("upstream down");
        },
        release: () =>
          Effect.gen(function* () {
            releaseCount += 1;
            return yield* Effect.fail("release down");
          }),
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const options = yield* resolveViewServerRuntimeOptions<
        GrpcTopics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersFeed: feed,
          },
          materializedReconnect: {
            delay: "10 millis",
            maxReconnects: 3,
          },
        },
      });
      const grpcOptions = yield* Effect.fromNullishOr(options.grpcOptions);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect(grpcHealthFeed(degradedHealth)?.lastError).toBe(
        "gRPC feed ordersFeed failed: gRPC feed release failed for ordersFeed: release down",
      );
      expect(grpcHealthFeed(degradedHealth)?.reconnects).toBe(0);
      expect(acquireCount).toBe(1);
      expect(releaseCount).toBe(1);
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("marks materialized gRPC feed degraded when release defects after completion", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const feed = grpcFeed.materializedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => Stream.empty,
        release: () =>
          Effect.gen(function* () {
            releaseCount += 1;
            return yield* Effect.die("release defect");
          }),
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect(grpcHealthFeed(degradedHealth)?.lastError).toContain("gRPC feed ordersFeed failed:");
      expect(grpcHealthFeed(degradedHealth)?.lastError).toContain("release defect");
      expect(grpcHealthFeed(degradedHealth)?.reconnects).toBe(0);
      expect(releaseCount).toBe(1);
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("marks materialized gRPC feed stopping when release is interrupted", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const feed = grpcFeed.materializedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => Stream.fail("upstream down"),
        release: () =>
          Effect.gen(function* () {
            releaseCount += 1;
            return yield* Effect.interrupt;
          }),
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const stoppingHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "stopping",
        }),
      );

      expect(grpcHealthFeed(stoppingHealth)?.lastError).toBe(null);
      expect(grpcHealthFeed(stoppingHealth)?.reconnects).toBe(0);
      expect(releaseCount).toBe(1);
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("marks materialized gRPC feed health degraded when the stream fails", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeed(Stream.fail("upstream down"));
      const options = yield* resolveViewServerRuntimeOptions<
        GrpcTopics,
        Record<string, string>,
        typeof grpcClients
      >({
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersFeed: feed,
          },
          materializedReconnect: {
            delay: "10 millis",
            maxReconnects: 0,
          },
        },
      });
      const grpcOptions = yield* Effect.fromNullishOr(options.grpcOptions);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeViewServerGrpcHealthLedger<GrpcTopics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {
          ordersFeed: {
            client: "orders",
            lifecycle: "materialized",
            topic: "orders",
          },
        },
      });
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlay(runtimeCore.client, health, 2_000).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            currentHealth.grpc?.feeds["orders"]?.materialized["ordersFeed"]?.status === "degraded",
        }),
      );

      expect(degradedHealth.status).toBe("degraded");
      expect(degradedHealth.grpc?.clients["orders"]?.status).toBe("degraded");
      expect(degradedHealth.grpc?.feeds["orders"]?.materialized["ordersFeed"]?.status).toBe(
        "degraded",
      );
      expect(degradedHealth.grpc?.feeds["orders"]?.materialized["ordersFeed"]?.lastError).toBe(
        "gRPC feed ordersFeed failed: gRPC feed stream failed for ordersFeed: upstream down",
      );
      expect(degradedHealth.grpc?.feeds["orders"]?.materialized["ordersFeed"]?.reconnects).toBe(0);

      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("releases materialized gRPC feed resources when ingress closes", () =>
    Effect.gen(function* () {
      const released = yield* Deferred.make<void>();
      const feed = grpcMaterializedFeedWithRelease(
        Stream.never,
        Deferred.succeed(released, undefined),
      );
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      yield* ingress.close;
      yield* Deferred.await(released);
      expect(
        grpcHealthFeed(health.healthOverlay(yield* runtimeCore.client.health(), 2_000)),
      ).toStrictEqual({
        status: "stopping",
        lifecycle: "materialized",
        feedName: "ordersFeed",
        feedKey: "orders/ordersFeed/materialized",
        topic: "orders",
        subscriberCount: 0,
        rowCount: 0,
        messagesPerSecond: 0,
        rowsPerSecond: 0,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        reconnects: 0,
        lastMessageAt: null,
        lastError: null,
      });
      yield* runtimeCore.close;
    }),
  );

  it.live(
    "does not reconnect completed materialized gRPC feed when close starts during release",
    () =>
      Effect.gen(function* () {
        let releaseCount = 0;
        const releaseStarted = yield* Deferred.make<void>();
        const releaseContinue = yield* Deferred.make<void>();
        const feed = grpcFeed.materializedFeed({
          topic: "orders",
          client: "orders",
          method: "streamOrders",
          request: () => ({ orderId: "all" }),
          acquire: () => Stream.empty,
          release: () =>
            Effect.gen(function* () {
              releaseCount += 1;
              yield* Deferred.succeed(releaseStarted, undefined);
              yield* Deferred.await(releaseContinue);
            }),
          map: ({ value }) => ({
            id: value.customerId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: "usa",
            updatedAt: value.updatedAt,
          }),
        });
        const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
        const health = makeGrpcHealth(grpcOptions);
        const ingress = yield* makeViewServerGrpcIngress(
          grpcViewServer,
          runtimeCore.client,
          Effect.void,
          grpcOptions,
          health,
        );

        yield* Deferred.await(releaseStarted);
        const closeFiber = yield* ingress.close.pipe(Effect.forkChild);
        yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
          Effect.repeat({
            schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
            until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "stopping",
          }),
        );
        yield* Deferred.succeed(releaseContinue, undefined);
        yield* Fiber.join(closeFiber);

        expect(releaseCount).toBe(1);
        expect(
          grpcHealthFeed(health.healthOverlay(yield* runtimeCore.client.health(), 2_000)),
        ).toStrictEqual({
          status: "stopping",
          lifecycle: "materialized",
          feedName: "ordersFeed",
          feedKey: "orders/ordersFeed/materialized",
          topic: "orders",
          subscriberCount: 0,
          rowCount: 0,
          messagesPerSecond: 0,
          rowsPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          reconnects: 0,
          lastMessageAt: null,
          lastError: null,
        });
        yield* runtimeCore.close;
      }),
  );

  it.live("ignores materialized gRPC release failure when close starts during release", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const releaseStarted = yield* Deferred.make<void>();
      const releaseContinue = yield* Deferred.make<void>();
      const feed = grpcFeed.materializedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => Stream.empty,
        release: () =>
          Effect.gen(function* () {
            releaseCount += 1;
            yield* Deferred.succeed(releaseStarted, undefined);
            yield* Deferred.await(releaseContinue);
            return yield* Effect.fail("release down after close");
          }),
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      yield* Deferred.await(releaseStarted);
      const closeFiber = yield* ingress.close.pipe(Effect.forkChild);
      yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "stopping",
        }),
      );
      yield* Deferred.succeed(releaseContinue, undefined);
      yield* Fiber.join(closeFiber);

      expect({
        releaseCount,
        feed: grpcHealthFeed(health.healthOverlay(yield* runtimeCore.client.health(), 2_000)),
      }).toStrictEqual({
        releaseCount: 1,
        feed: {
          status: "stopping",
          lifecycle: "materialized",
          feedName: "ordersFeed",
          feedKey: "orders/ordersFeed/materialized",
          topic: "orders",
          subscriberCount: 0,
          rowCount: 0,
          messagesPerSecond: 0,
          rowsPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          reconnects: 0,
          lastMessageAt: null,
          lastError: null,
        },
      });
      yield* runtimeCore.close;
    }),
  );

  it.live("does not reconnect failed materialized gRPC feed when close starts during release", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const releaseStarted = yield* Deferred.make<void>();
      const releaseContinue = yield* Deferred.make<void>();
      const feed = grpcFeed.materializedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => Stream.fail("upstream down"),
        release: () =>
          Effect.gen(function* () {
            releaseCount += 1;
            yield* Deferred.succeed(releaseStarted, undefined);
            yield* Deferred.await(releaseContinue);
          }),
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      yield* Deferred.await(releaseStarted);
      const closeFiber = yield* ingress.close.pipe(Effect.forkChild);
      yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "stopping",
        }),
      );
      yield* Deferred.succeed(releaseContinue, undefined);
      yield* Fiber.join(closeFiber);

      expect(releaseCount).toBe(1);
      expect(
        grpcHealthFeed(health.healthOverlay(yield* runtimeCore.client.health(), 2_000)),
      ).toStrictEqual({
        status: "stopping",
        lifecycle: "materialized",
        feedName: "ordersFeed",
        feedKey: "orders/ordersFeed/materialized",
        topic: "orders",
        subscriberCount: 0,
        rowCount: 0,
        messagesPerSecond: 0,
        rowsPerSecond: 0,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        reconnects: 0,
        lastMessageAt: null,
        lastError: null,
      });
      yield* runtimeCore.close;
    }),
  );

  it.live("ignores materialized gRPC release construction failures during ingress close", () =>
    Effect.gen(function* () {
      const feed = grpcFeed.materializedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all-orders" }),
        acquire: () => Stream.never,
        release: () => {
          throw new Error("release exploded");
        },
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      yield* ingress.close;
      expect(
        grpcHealthFeed(health.healthOverlay(yield* runtimeCore.client.health(), 2_000))?.status,
      ).toBe("stopping");
      yield* runtimeCore.close;
    }),
  );

  it.live("refreshes materialized gRPC health after an idle feed becomes ready", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeed(Stream.never);
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const readyHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      expect(grpcHealthFeed(readyHealth)?.status).toBe("ready");
      expect(grpcHealthClient(readyHealth)?.activeFeeds).toBe(1);
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("ignores reconnect health updates for unknown gRPC feeds", () =>
    Effect.gen(function* () {
      const grpcOptions = yield* resolveGrpcRuntimeOptions(grpcMaterializedFeed(Stream.never));
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);

      yield* health.feedReconnecting("missingFeed", "ignored reconnect");
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect(grpcHealthFeed(currentHealth)).toStrictEqual({
        status: "starting",
        lifecycle: "materialized",
        feedName: "ordersFeed",
        feedKey: "orders/ordersFeed/materialized",
        topic: "orders",
        subscriberCount: 0,
        rowCount: 0,
        messagesPerSecond: 0,
        rowsPerSecond: 0,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        reconnects: 0,
        lastMessageAt: null,
        lastError: null,
      });
      yield* runtimeCore.close;
    }),
  );

  it.live("fails materialized gRPC ingress startup when request creation fails", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeedWithRequestFailure();
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const error = yield* Effect.flip(
        makeViewServerGrpcIngress(
          grpcViewServer,
          runtimeCore.client,
          Effect.void,
          grpcOptions,
          health,
        ),
      );

      expect(error._tag).toBe("ViewServerGrpcIngressError");
      expect(error.message).toBe("gRPC feed request creation failed for ordersFeed");
      expect(error.feedName).toBe("ordersFeed");
      expect(error.topic).toBe("orders");
      yield* runtimeCore.close;
    }),
  );

  it.live("fails materialized gRPC ingress startup when client creation throws", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeed(Stream.never);
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const error = yield* Effect.flip(
        makeViewServerGrpcIngress(
          grpcViewServer,
          runtimeCore.client,
          Effect.void,
          grpcOptions,
          health,
          () => {
            throw new Error("client factory exploded");
          },
        ),
      );

      expect(error._tag).toBe("ViewServerGrpcIngressError");
      expect(error.message).toBe("gRPC client creation failed for ordersFeed");
      expect(error.feedName).toBe("ordersFeed");
      expect(error.topic).toBe("orders");
      yield* runtimeCore.close;
    }),
  );

  it.live("ignores leased feeds in the materialized gRPC ingress", () =>
    Effect.gen(function* () {
      let acquired = 0;
      const feed = grpcLeasedFeed({
        acquired: () => {
          acquired += 1;
        },
        streamForRegion: () => Stream.never,
      });
      const grpcOptions: ResolvedViewServerGrpcRuntimeOptions<
        typeof leasedGrpcViewServer.topics,
        typeof grpcClients
      > = {
        clients: grpcClients,
        clientBaseUrls: nullRecord([["orders", "https://orders.example.test"]]),
        feeds: {
          ordersLease: feed,
        },
        materializedReconnect: fastGrpcMaterializedReconnect,
      };
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeViewServerGrpcHealthLedger<typeof leasedGrpcViewServer.topics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {},
      });

      const ingress = yield* makeViewServerGrpcIngress(
        leasedGrpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect(acquired).toBe(0);
      expect(Object.keys(currentHealth.grpc?.feeds ?? {})).toStrictEqual([]);
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("closes already-started gRPC feed resources when another feed fails startup", () =>
    Effect.gen(function* () {
      const released = yield* Deferred.make<void>();
      const runningFeed = grpcMaterializedFeedWithRelease(
        Stream.never,
        Deferred.succeed(released, undefined),
      );
      const failingFeed = grpcMaterializedFeedWithRequestFailure();
      const grpcOptions: ResolvedViewServerGrpcRuntimeOptions<GrpcTopics, typeof grpcClients> = {
        clients: grpcClients,
        clientBaseUrls: nullRecord([["orders", "https://orders.example.test"]]),
        feeds: {
          runningFeed,
          failingFeed,
        },
        materializedReconnect: fastGrpcMaterializedReconnect,
      };
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeViewServerGrpcHealthLedger<GrpcTopics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {
          runningFeed: {
            client: "orders",
            lifecycle: "materialized",
            topic: "orders",
          },
          failingFeed: {
            client: "orders",
            lifecycle: "materialized",
            topic: "orders",
          },
        },
      });

      const error = yield* Effect.flip(
        makeViewServerGrpcIngress(
          grpcViewServer,
          runtimeCore.client,
          Effect.void,
          grpcOptions,
          health,
        ),
      );

      yield* Deferred.await(released);
      expect(error.message).toBe("gRPC feed request creation failed for failingFeed");
      expect(error.feedName).toBe("failingFeed");
      expect(error.topic).toBe("orders");
      yield* runtimeCore.close;
    }),
  );

  it.live("fails materialized gRPC ingress startup when feed client is missing", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeedWithOrphanClient();
      const grpcOptions: ResolvedViewServerGrpcRuntimeOptions<
        GrpcTopics,
        typeof grpcClientsWithOrphan
      > = {
        // @ts-expect-error defensive runtime-boundary test intentionally omits the orphan client.
        clients: {},
        clientBaseUrls: {},
        feeds: {
          ordersFeed: feed,
        },
        materializedReconnect: fastGrpcMaterializedReconnect,
      };
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeViewServerGrpcHealthLedger<GrpcTopics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {
          ordersFeed: {
            client: "orders",
            lifecycle: "materialized",
            topic: "orders",
          },
        },
      });
      const error = yield* Effect.flip(
        makeViewServerGrpcIngress(
          grpcViewServer,
          runtimeCore.client,
          Effect.void,
          grpcOptions,
          health,
        ),
      );

      expect(error.message).toBe("gRPC feed ordersFeed references missing client: orphan");
      expect(error.feedName).toBe("ordersFeed");
      expect(error.topic).toBe("orders");
      yield* runtimeCore.close;
    }),
  );

  it.live("fails materialized gRPC ingress startup when feed client URL is unresolved", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeed(Stream.never);
      const grpcOptions: ResolvedViewServerGrpcRuntimeOptions<GrpcTopics, typeof grpcClients> = {
        clients: grpcClients,
        clientBaseUrls: {},
        feeds: {
          ordersFeed: feed,
        },
        materializedReconnect: fastGrpcMaterializedReconnect,
      };
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeViewServerGrpcHealthLedger<GrpcTopics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {
          ordersFeed: {
            client: "orders",
            lifecycle: "materialized",
            topic: "orders",
          },
        },
      });
      const error = yield* Effect.flip(
        makeViewServerGrpcIngress(
          grpcViewServer,
          runtimeCore.client,
          Effect.void,
          grpcOptions,
          health,
        ),
      );

      expect(error.message).toBe("gRPC feed ordersFeed references unresolved client URL: orders");
      expect(error.feedName).toBe("ordersFeed");
      expect(error.topic).toBe("orders");
      yield* runtimeCore.close;
    }),
  );

  it.live("marks materialized gRPC feed degraded when acquire throws", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeedWithAcquireFailure();
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect(degradedHealth.status).toBe("degraded");
      expect(grpcHealthClient(degradedHealth)?.status).toBe("degraded");
      expect(grpcHealthFeed(degradedHealth)?.status).toBe("degraded");
      expect(grpcHealthFeed(degradedHealth)?.lastError).toBe(
        "gRPC feed ordersFeed failed: gRPC feed acquire failed for ordersFeed: acquire exploded",
      );
      expect(grpcHealthFeed(degradedHealth)?.reconnects).toBe(3);
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("records materialized gRPC mapping failures in health", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeedWithMappingFailure(
        longRunningGrpcStream([grpcOrderValue("order-1", 10)]),
      );
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect(degradedHealth.status).toBe("degraded");
      expect(grpcHealthFeed(degradedHealth)?.mappingFailuresPerSecond).toBe(1);
      expect(grpcHealthFeed(degradedHealth)?.publishFailuresPerSecond).toBe(0);
      expect(grpcHealthFeed(degradedHealth)?.reconnects).toBe(0);
      expect(grpcHealthFeed(degradedHealth)?.lastError).toBe(
        "gRPC feed ordersFeed failed: gRPC feed mapping failed for ordersFeed: mapping exploded",
      );
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("records materialized gRPC invalid mapped rows as mapping failures", () =>
    Effect.gen(function* () {
      const feed = grpcFeed.materializedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => longRunningGrpcStream([grpcOrderValue("invalid-materialized-row", 10)]),
        map: ({ value }) => {
          const row = {
            id: value.customerId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: "usa",
            updatedAt: value.updatedAt,
          };
          Object.defineProperty(row, "status", { value: "not-a-status" });
          return row;
        },
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect(degradedHealth.status).toBe("degraded");
      expect(grpcHealthFeed(degradedHealth)?.mappingFailuresPerSecond).toBe(1);
      expect(grpcHealthFeed(degradedHealth)?.publishFailuresPerSecond).toBe(0);
      expect(grpcHealthFeed(degradedHealth)?.reconnects).toBe(0);
      expect(grpcHealthFeed(degradedHealth)?.lastError).toContain(
        "gRPC feed mapping produced an invalid row for ordersFeed",
      );
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("records materialized gRPC publish failures in health", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeed(longRunningGrpcStream([grpcOrderValue("order-1", 10)]));
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcViewServer, {});
      const publishFailure: ViewServerRuntimeError = {
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        message: "publish unavailable",
        topic: "orders",
      };
      const failingRuntimeClient: ViewServerRuntimeClient<GrpcTopics> = {
        ...runtimeCore.client,
        publishMany: () => Effect.fail(publishFailure),
      };
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        failingRuntimeClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect(degradedHealth.status).toBe("degraded");
      expect(grpcHealthFeed(degradedHealth)?.mappingFailuresPerSecond).toBe(0);
      expect(grpcHealthFeed(degradedHealth)?.publishFailuresPerSecond).toBe(1);
      expect(grpcHealthFeed(degradedHealth)?.reconnects).toBe(0);
      expect(grpcHealthFeed(degradedHealth)?.lastError).toBe(
        "gRPC feed ordersFeed failed: gRPC feed publish failed for ordersFeed: publish unavailable",
      );
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.effect("resolves explicit Kafka start policies", () =>
    Effect.gen(function* () {
      const regions = {
        local: "localhost:9092",
      };
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const topics = {
        "orders-source": localKafkaTopic({
          regions: ["local"],
          value: kafka.json(Order),
          key: kafka.stringKey(),
          viewServerTopic: "orders",
          getSafeRowKey: ({ key }) => key,
          mapping: ({ key, value }) => ({
            id: key,
            price: value.price,
          }),
        }),
      };

      const earliest = yield* resolveViewServerRuntimeOptions({
        kafka: {
          consumerGroupId: "view-server-earliest",
          regions,
          startFrom: "earliest",
          topics,
        },
      });
      const latest = yield* resolveViewServerRuntimeOptions({
        kafka: {
          consumerGroupId: "view-server-latest",
          regions,
          startFrom: "latest",
          topics,
        },
      });
      const committed = yield* resolveViewServerRuntimeOptions({
        kafka: {
          consumerGroupId: "view-server-default",
          regions,
          startFrom: {
            committedConsumerGroup: "view-server-existing-group",
            fallback: "fail",
          },
          topics,
        },
      });
      const committedWithDefaultFallback = yield* resolveViewServerRuntimeOptions({
        kafka: {
          consumerGroupId: "view-server-default-fallback",
          regions,
          startFrom: {
            committedConsumerGroup: "view-server-existing-default-fallback-group",
          },
          topics,
        },
      });

      expect({
        committed: committed.kafkaOptions?.consume,
        committedWithDefaultFallback: committedWithDefaultFallback.kafkaOptions?.consume,
        earliest: earliest.kafkaOptions?.consume,
        latest: latest.kafkaOptions?.consume,
      }).toStrictEqual({
        committed: {
          consumerGroupId: "view-server-existing-group",
          fallbackMode: "fail",
          mode: "committed",
        },
        committedWithDefaultFallback: {
          consumerGroupId: "view-server-existing-default-fallback-group",
          fallbackMode: "earliest",
          mode: "committed",
        },
        earliest: {
          consumerGroupId: "view-server-earliest",
          fallbackMode: "earliest",
          mode: "earliest",
        },
        latest: {
          consumerGroupId: "view-server-latest",
          fallbackMode: "latest",
          mode: "latest",
        },
      });
    }),
  );

  it.live("preserves dangerous Kafka runtime option keys", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<typeof viewServer.topics>;
      const regions = nullRecord([["__proto__", Config.succeed("localhost:9092")]]);
      let kafkaOptionsSummary:
        | {
            readonly consumerGroupId: string;
            readonly regions: Readonly<Record<string, string>>;
            readonly topics: Readonly<
              Record<
                string,
                { readonly regions: ReadonlyArray<string>; readonly viewServerTopic: string }
              >
            >;
          }
        | undefined;
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const dangerousTopic = localKafkaTopic({
        regions: ["__proto__"],
        value: kafka.json(Order),
        key: kafka.stringKey(),
        viewServerTopic: "orders",
        getSafeRowKey: ({ key }) => key,
        mapping: ({ key, value }) => ({
          id: key,
          price: value.price,
        }),
      });
      const topics = nullRecord([["__proto__", dangerousTopic]]);
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.void,
          }),
        makeKafkaIngress: (_config, _client, _requestHealthRefresh, options) => {
          kafkaOptionsSummary = {
            consumerGroupId: options.consumerGroupId,
            regions: options.regions,
            topics: Object.fromEntries(
              Object.entries(options.topics).map(([sourceTopic, topic]) => [
                sourceTopic,
                {
                  regions: topic.regions,
                  viewServerTopic: topic.viewServerTopic,
                },
              ]),
            ),
          };
          return Effect.succeed({
            close: Effect.void,
          });
        },
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
        kafka: {
          consumerGroupId: "view-server-dangerous-key-test-runtime",
          regions,
          topics,
        },
      });

      expect(Object.hasOwn(kafkaOptionsSummary?.regions ?? {}, "__proto__")).toBe(true);
      expect(Object.hasOwn(kafkaOptionsSummary?.topics ?? {}, "__proto__")).toBe(true);
      expect({
        consumerGroupId: kafkaOptionsSummary?.consumerGroupId,
        region: kafkaOptionsSummary?.regions["__proto__"],
        topicRegions: kafkaOptionsSummary?.topics["__proto__"]?.regions,
        viewServerTopic: kafkaOptionsSummary?.topics["__proto__"]?.viewServerTopic,
      }).toStrictEqual({
        consumerGroupId: "view-server-dangerous-key-test-runtime",
        region: "localhost:9092",
        topicRegions: ["__proto__"],
        viewServerTopic: "orders",
      });

      yield* runtime.close;
    }),
  );

  it.live("returns unavailable health when Kafka ingress is degraded", () =>
    Effect.gen(function* () {
      const regions = {
        local: "localhost:9092",
      };
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeKafkaIngress: (_config, _client, _requestHealthRefresh, _options, health) =>
          health.regionDisconnected("local", "lost").pipe(
            Effect.as({
              close: Effect.void,
            }),
          ),
      };

      yield* Effect.acquireUseRelease(
        makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
          kafka: {
            consumerGroupId: "view-server-test-degraded",
            regions,
            topics: {
              "orders-source": localKafkaTopic({
                regions: ["local"],
                value: kafka.json(Order),
                key: kafka.stringKey(),
                viewServerTopic: "orders",
                getSafeRowKey: ({ key }) => key,
                mapping: ({ key, value }) => ({
                  id: key,
                  price: value.price,
                }),
              }),
            },
          },
        }),
        (runtime) =>
          Effect.gen(function* () {
            const health = yield* fetchHealth(runtime.healthUrl);

            expect(health.response.status).toBe(503);
            expect(health.health.status).toBe("degraded");
            expect(health.health.engine.topics.orders.rowCount).toBe(0);
          }),
        (runtime) => runtime.close,
      );
    }),
  );

  it.live("public live client close closes the websocket server and runtime core", () =>
    Effect.gen(function* () {
      let serverCloseCount = 0;
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeRuntimeCore: makeViewServerRuntimeCoreInternal,
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.sync(() => {
              serverCloseCount += 1;
            }),
          }),
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer);
      yield* runtime.liveClient.close;
      const health = yield* runtime.client.health();

      expect(serverCloseCount).toBe(1);
      expect(health.status).toBe("stopping");
    }),
  );

  it.live("run helper keeps the runtime alive until the main fiber is interrupted", () =>
    Effect.gen(function* () {
      let serverCloseCount = 0;
      const serverStarted = yield* Deferred.make<void>();
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeRuntimeCore: makeViewServerRuntimeCoreInternal,
        makeServer: () =>
          Deferred.succeed(serverStarted, void 0).pipe(
            Effect.as({
              url: "ws://127.0.0.1:0/rpc",
              healthUrl: "http://127.0.0.1:0/health",
              metricsUrl: "http://127.0.0.1:0/metrics",
              close: Effect.sync(() => {
                serverCloseCount += 1;
              }),
            }),
          ),
      };

      const fiber = yield* runViewServerRuntimeWithDependencies(dependencies, viewServer).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(serverStarted);
      yield* Effect.sleep("10 millis");
      expect(serverCloseCount).toBe(0);

      yield* Fiber.interrupt(fiber);
      expect(serverCloseCount).toBe(1);
    }),
  );

  it.live("run helper closes Kafka ingress when the main fiber is interrupted", () =>
    Effect.gen(function* () {
      let kafkaCloseCount = 0;
      let runtimeCoreClosed = false;
      let serverCloseCount = 0;
      const kafkaStarted = yield* Deferred.make<void>();
      const serverStarted = yield* Deferred.make<void>();
      const regions = {
        local: "localhost:9092",
      };
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeRuntimeCore: (config, options) =>
          makeViewServerRuntimeCoreInternal(config, options).pipe(
            Effect.map((runtimeCore) => ({
              ...runtimeCore,
              close: runtimeCore.close.pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    runtimeCoreClosed = true;
                  }),
                ),
              ),
            })),
          ),
        makeServer: () =>
          Deferred.succeed(serverStarted, undefined).pipe(
            Effect.as({
              url: "ws://127.0.0.1:0/rpc",
              healthUrl: "http://127.0.0.1:0/health",
              metricsUrl: "http://127.0.0.1:0/metrics",
              close: Effect.sync(() => {
                serverCloseCount += 1;
              }),
            }),
          ),
        makeKafkaIngress: () =>
          Deferred.succeed(kafkaStarted, undefined).pipe(
            Effect.as({
              close: Effect.sync(() => {
                kafkaCloseCount += 1;
              }),
            }),
          ),
      };

      const fiber = yield* runViewServerRuntimeWithDependencies(dependencies, viewServer, {
        kafka: {
          consumerGroupId: "view-server-test-runtime-interrupt",
          regions,
          topics: {
            "orders-source": localKafkaTopic({
              regions: ["local"],
              value: kafka.json(Order),
              key: kafka.stringKey(),
              viewServerTopic: "orders",
              getSafeRowKey: ({ key }) => key,
              mapping: ({ key, value }) => ({
                id: key,
                price: value.price,
              }),
            }),
          },
        },
      }).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(serverStarted);
      yield* Deferred.await(kafkaStarted);
      yield* Effect.sleep("10 millis");

      expect({
        kafkaCloseCount,
        runtimeCoreClosed,
        serverCloseCount,
      }).toStrictEqual({
        kafkaCloseCount: 0,
        runtimeCoreClosed: false,
        serverCloseCount: 0,
      });

      yield* Fiber.interrupt(fiber);

      expect({
        kafkaCloseCount,
        runtimeCoreClosed,
        serverCloseCount,
      }).toStrictEqual({
        kafkaCloseCount: 1,
        runtimeCoreClosed: true,
        serverCloseCount: 1,
      });
    }),
  );

  it.live("public run helper starts a launchable websocket runtime", () =>
    Effect.gen(function* () {
      const fiber = yield* runViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      }).pipe(Effect.forkChild({ startImmediately: true }));

      yield* Effect.sleep("20 millis");
      yield* Fiber.interrupt(fiber);
    }),
  );

  it.live("public run helper supports default runtime options", () =>
    Effect.gen(function* () {
      const fiber = yield* runViewServerRuntime(viewServer).pipe(
        Effect.forkChild({ startImmediately: true }),
      );

      yield* Effect.sleep("20 millis");
      yield* Fiber.interrupt(fiber);
    }),
  );

  it.live("releases the runtime core when server startup fails before returning a runtime", () =>
    Effect.gen(function* () {
      let closed = false;
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeRuntimeCore: (config, options) =>
          makeViewServerRuntimeCoreInternal(config, options).pipe(
            Effect.map((runtimeCore) => ({
              ...runtimeCore,
              close: runtimeCore.close.pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    closed = true;
                  }),
                ),
              ),
            })),
          ),
        makeServer: () => Effect.die(new Error("server startup failed")),
      };

      const startupExit = yield* Effect.exit(
        makeViewServerRuntimeWithDependencies(dependencies, viewServer),
      );

      expect(Exit.isFailure(startupExit)).toBe(true);
      expect(closed).toBe(true);
    }),
  );

  it.live("releases server and runtime core when Kafka ingress startup fails", () =>
    Effect.gen(function* () {
      let runtimeCoreClosed = false;
      let serverClosed = false;
      const regions = {
        local: "localhost:9092",
      };
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeRuntimeCore: (config, options) =>
          makeViewServerRuntimeCoreInternal(config, options).pipe(
            Effect.map((runtimeCore) => ({
              ...runtimeCore,
              close: runtimeCore.close.pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    runtimeCoreClosed = true;
                  }),
                ),
              ),
            })),
          ),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.sync(() => {
              serverClosed = true;
            }),
          }),
        makeKafkaIngress: () =>
          Effect.fail(
            new ViewServerKafkaIngressError({
              message: "Kafka ingress startup failed",
              cause: "startup failed",
            }),
          ),
      };

      const startupExit = yield* Effect.exit(
        makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
          kafka: {
            consumerGroupId: "view-server-test-startup-failure",
            regions,
            topics: {
              "orders-source": localKafkaTopic({
                regions: ["local"],
                value: kafka.json(Order),
                key: kafka.stringKey(),
                viewServerTopic: "orders",
                getSafeRowKey: ({ key }) => key,
                mapping: ({ key, value }) => ({
                  id: key,
                  price: value.price,
                }),
              }),
            },
          },
        }),
      );

      expect(Exit.isFailure(startupExit)).toBe(true);
      expect(serverClosed).toBe(true);
      expect(runtimeCoreClosed).toBe(true);
    }),
  );
});
