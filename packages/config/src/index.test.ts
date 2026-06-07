import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import { create, toBinary } from "@bufbuild/protobuf";
import { fileDesc, messageDesc } from "@bufbuild/protobuf/codegenv2";
import type { GenMessage } from "@bufbuild/protobuf/codegenv2";
import type { Message } from "@bufbuild/protobuf";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import type * as BigDecimal from "effect/BigDecimal";
import { Config, Effect, Exit, Schema } from "effect";
import {
  decodeKafkaCodec,
  decodeKafkaTopicMessage,
  defineViewServerConfig,
  kafka,
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
  viewServerReservedTopicNames,
  viewServerSchemaFieldMetadata,
  viewServerTopicNameIsReserved,
  viewServerHealthSummaryFromHealth,
  viewServerHealthSummaryRowFromHealth,
  viewServerHealthTopicRowsFromHealth,
  type KafkaCodec,
  type KafkaMappingInput,
  type KafkaMessageMetadata,
  type KafkaCodecError,
  type KafkaCodecType,
  type KafkaDecodeError,
  type KafkaTopicDefinition,
  type ExactGroupedQuery,
  type ExactRawQuery,
  type GroupedQuery,
  type LiveQueryResult,
  type LiveQueryRow,
  type LiveSubscription,
  type LiveTransportAdapter,
  type RawQuery,
  type SnapshotEvent,
  type StatusEvent,
  type TopicRuntimeHealth,
  type TopicRow,
  type ValidateLiveQuery,
  type ViewServerBackpressureError,
  type ViewServerHealth,
  type ViewServerHealthDetails,
  type ViewServerHealthSummary,
  type ViewServerHealthSummaryRow,
  type ViewServerHealthTopicRow,
  type ViewServerRuntimeClient,
  type ViewServerRuntimeError,
  type ViewServerTransportError,
} from "./index";
import { runtimeConfig, runtimeEnvironmentConfig } from "./runtime";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  quantity: Schema.Number,
  price: Schema.Number,
  region: Schema.String,
});

const Position = Schema.Struct({
  id: Schema.String,
  accountId: Schema.String,
  symbol: Schema.String,
  active: Schema.Boolean,
  quantity: Schema.BigInt,
  optionalQuantity: Schema.Union([Schema.BigInt, Schema.Undefined]),
  price: Schema.BigDecimal,
  notional: Schema.Number,
  optionalNotional: Schema.Union([Schema.Number, Schema.Undefined]),
});

const OrderWithExtraSourceField = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
  ze: Schema.Boolean,
});

declare const decimal: (value: string) => BigDecimal.BigDecimal;

type OrdersValueMessage = Message<"viewserver.test.OrderValue"> & {
  readonly customerId: string;
  readonly status: "open" | "closed" | "cancelled";
  readonly price: number;
  readonly updatedAt: number;
};

type OrdersKeyMessage = Message<"viewserver.test.OrderKey"> & {
  readonly orderId: string;
};

type TradesValueMessage = Message<"viewserver.test.TradeValue"> & {
  readonly symbol: string;
  readonly quantity: number;
  readonly price: number;
};

type CustomKafkaCodecError = {
  readonly _tag: "CustomKafkaCodecError";
  readonly message: string;
};

const base64FromBytes = (bytes: Uint8Array) =>
  globalThis.btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(""));

const textEncoder = new TextEncoder();

const kafkaTestMetadata = (region: "usa" | "london"): KafkaMessageMetadata<typeof region> => ({
  sourceTopic: "orders-source",
  sourceRegion: region,
  partition: 0,
  offset: "1",
  timestamp: null,
  headers: {},
});

const testProtoFile = fileDesc(
  base64FromBytes(
    toBinary(
      FileDescriptorProtoSchema,
      create(FileDescriptorProtoSchema, {
        name: "viewserver/test.proto",
        package: "viewserver.test",
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
          {
            name: "TradeValue",
            field: [
              { name: "symbol", number: 1, type: FieldDescriptorProto_Type.STRING },
              { name: "quantity", number: 2, type: FieldDescriptorProto_Type.DOUBLE },
              { name: "price", number: 3, type: FieldDescriptorProto_Type.DOUBLE },
            ],
          },
        ],
      }),
    ),
  ),
);

const ordersValueSchema = messageDesc<OrdersValueMessage>(testProtoFile, 0);
const ordersKeySchema = messageDesc<OrdersKeyMessage>(testProtoFile, 1);
const tradesValueSchema = messageDesc<TradesValueMessage>(testProtoFile, 2);

declare const generatedOrdersValueSchema: GenMessage<
  Message<"viewserver.test.OrderValue"> & {
    readonly customerId: string;
    readonly status: "open" | "closed" | "cancelled";
    readonly price: number;
    readonly updatedAt: number;
  }
>;
declare const generatedOrdersKeySchema: GenMessage<
  Message<"viewserver.test.OrderKey"> & {
    readonly orderId: string;
  }
>;

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
    positions: {
      schema: Position,
      key: "id",
    },
  },
});

const runtimeTopicHealth = (
  status: TopicRuntimeHealth["status"],
  rowCount: number,
): TopicRuntimeHealth => ({
  status,
  rowCount,
  liveRowCount: rowCount,
  deletedRowCount: 0,
  version: rowCount,
  lastMutationAt: null,
  mutationsPerSecond: rowCount,
  rowsPerSecond: rowCount,
  pendingMutationBatches: 0,
  activeFallbackGroupedViews: 0,
  activeIncrementalGroupedViews: 0,
  activeViews: 0,
  activeSubscriptions: 0,
  queuedEvents: 0,
  maxQueueDepth: 0,
  backpressureEvents: 0,
  memoryBytes: 0,
  tombstoneCount: 0,
  compactionPending: false,
});

type LiveQueryCall<Topics extends object> = {
  <
    Topic extends Extract<keyof Topics, string>,
    const Query extends GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: Query &
      ExactGroupedQuery<TopicRow<Topics, Topic>, NoInfer<Query>> &
      ValidateLiveQuery<NoInfer<Query>>,
  ): LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>;
  <
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: Query &
      ExactRawQuery<TopicRow<Topics, Topic>, NoInfer<Query>> &
      ValidateLiveQuery<NoInfer<Query>>,
  ): LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>;
};

const kafkaRegions = {
  usa: runtimeConfig.kafkaBootstrapServers("VIEW_SERVER_KAFKA_USA_BOOTSTRAP_SERVERS"),
  london: runtimeConfig.kafkaBootstrapServers("VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS"),
};

const kafkaTopic = viewServer.kafkaTopic<typeof kafkaRegions>();
const ordersValueKafkaCodec = kafka.protobuf(ordersValueSchema);
const ordersKeyKafkaCodec = kafka.protobuf(ordersKeySchema);
const tradesValueKafkaCodec = kafka.protobuf(tradesValueSchema);

describe("defineViewServerConfig", () => {
  it("derives schema field metadata for query validation", () => {
    expect(viewServerSchemaFieldMetadata(Schema.Number)).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigDecimal",
    });
    expect(viewServerSchemaFieldMetadata(Schema.BigInt)).toStrictEqual({
      isNumeric: true,
      isPureBigInt: true,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigint",
    });
    expect(viewServerSchemaFieldMetadata(Schema.BigDecimal)).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigDecimal",
    });
    expect(
      viewServerSchemaFieldMetadata(Schema.Union([Schema.BigInt, Schema.BigInt])),
    ).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigint",
    });
    expect(
      viewServerSchemaFieldMetadata(Schema.Union([Schema.BigInt, Schema.Number])),
    ).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigDecimal",
    });
    expect(viewServerSchemaFieldMetadata(Schema.Literal(1))).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigDecimal",
    });
    expect(viewServerSchemaFieldMetadata(Schema.Literals([1, 2]))).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigDecimal",
    });
    expect(viewServerSchemaFieldMetadata(Schema.Literal(1n))).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigint",
    });
    expect(
      viewServerSchemaFieldMetadata(Schema.Union([Schema.Number, Schema.Undefined])),
    ).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(
      viewServerSchemaFieldMetadata(Schema.Union([Schema.BigInt, Schema.Undefined])),
    ).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata(Schema.Undefined)).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata(Schema.Union([Schema.Undefined]))).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata(Schema.Union([]))).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata(undefined)).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata("not-a-schema")).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata({ ast: "not-an-effect-ast" })).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata(Schema.Literals(["open", "closed"]))).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: true,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(
      viewServerSchemaFieldMetadata(
        Schema.Union([
          Schema.Struct({ id: Schema.String }),
          Schema.Struct({ id: Schema.String, name: Schema.String }),
        ]),
      ),
    ).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: true,
      isStructuredObject: true,
    });
    expect(viewServerSchemaFieldMetadata(Schema.Struct({ id: Schema.String }))).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: true,
      isStructuredObject: true,
    });
    expect(viewServerSchemaFieldMetadata(Schema.Array(Schema.String))).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: true,
      isStructuredObject: false,
    });
  });

  it("defines topics and pure runtime option contracts without starting a runtime", () => {
    const runtimeOptions = viewServer.defineRuntimeOptions({
      websocketPort: runtimeEnvironmentConfig.websocketPort,
      tcpPublishPort: runtimeConfig.port("VIEW_SERVER_TCP_PUBLISH_PORT"),
      kafka: {
        regions: kafkaRegions,
        topics: {
          orders: kafkaTopic({
            regions: ["usa", "london"],
            value: kafka.protobuf(ordersValueSchema),
            key: kafka.protobuf(ordersKeySchema),
            viewServerTopic: "orders",
            mapping: ({ key, value, region }) => {
              expectTypeOf(key).toEqualTypeOf<OrdersKeyMessage>();
              expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
              expectTypeOf(region).toEqualTypeOf<"usa" | "london">();
              return {
                id: key.orderId,
                customerId: value.customerId,
                status: value.status,
                price: value.price,
                region,
                updatedAt: value.updatedAt,
              };
            },
          }),
          trades: kafkaTopic({
            regions: ["usa"],
            value: kafka.protobuf(tradesValueSchema),
            viewServerTopic: "trades",
            mapping: ({ key, value, region }) => {
              expectTypeOf(key).toEqualTypeOf<string>();
              expectTypeOf(value).toEqualTypeOf<TradesValueMessage>();
              expectTypeOf(region).toEqualTypeOf<"usa">();
              return {
                id: key,
                symbol: value.symbol,
                quantity: value.quantity,
                price: value.price,
                region,
              };
            },
          }),
        },
      },
    });

    expect(runtimeOptions.kafka.regions["usa"]).toBe(kafkaRegions.usa);
    expect(viewServer.topics.orders.key).toBe("id");
    expect(runtimeOptions.websocketPort).toBe(runtimeEnvironmentConfig.websocketPort);
    expect(Config.isConfig(runtimeEnvironmentConfig.tcpPublishPort)).toBe(true);
    expect(Config.isConfig(runtimeConfig.port("VIEW_SERVER_TCP_PUBLISH_PORT"))).toBe(true);
  });

  it.effect("defines typed Kafka source codecs", () =>
    Effect.gen(function* () {
      const bytesCodec = kafka.bytes();
      const stringCodec = kafka.string();
      const stringKeyCodec = kafka.stringKey();
      const jsonCodec = kafka.json(Order);
      const protobufCodec = kafka.protobuf(ordersValueSchema);
      const customCodec = kafka.codec({
        name: "custom-order-value",
        decode: ({ bytes }): Effect.Effect<{ readonly byteLength: number }, never> =>
          Effect.succeed({
            byteLength: bytes.byteLength,
          }),
      });
      const customErrorCodec = kafka.codec({
        name: "custom-order-value-with-error",
        decode: (): Effect.Effect<{ readonly id: string }, CustomKafkaCodecError> =>
          Effect.fail({
            _tag: "CustomKafkaCodecError",
            message: "decode failed",
          }),
      });

      expect(bytesCodec.format).toBe("bytes");
      expect(stringCodec.format).toBe("string");
      expect(stringKeyCodec.format).toBe("string");
      expect(jsonCodec.schema).toBe(Order);
      expect(protobufCodec.descriptor).toBe(ordersValueSchema);
      expect(customCodec.name).toBe("custom-order-value");
      expect(
        yield* decodeKafkaCodec(bytesCodec, {
          bytes: new Uint8Array([1, 2, 3]),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual(new Uint8Array([1, 2, 3]));
      expect(
        yield* decodeKafkaCodec(stringCodec, {
          bytes: textEncoder.encode("order-value"),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toBe("order-value");
      expect(
        yield* decodeKafkaCodec(stringKeyCodec, {
          bytes: textEncoder.encode("order-key"),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toBe("order-key");
      expect(
        yield* decodeKafkaCodec(jsonCodec, {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "order-1",
              customerId: "customer-1",
              status: "open",
              price: 42,
              region: "usa",
              updatedAt: 1,
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual({
        id: "order-1",
        customerId: "customer-1",
        status: "open",
        price: 42,
        region: "usa",
        updatedAt: 1,
      });
      expect(
        yield* decodeKafkaCodec(protobufCodec, {
          bytes: toBinary(
            ordersValueSchema,
            create(ordersValueSchema, {
              customerId: "customer-1",
              status: "open",
              price: 42,
              updatedAt: 1,
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual(
        create(ordersValueSchema, {
          customerId: "customer-1",
          status: "open",
          price: 42,
          updatedAt: 1,
        }),
      );
      expect(
        yield* customCodec.decode({
          bytes: new Uint8Array([1, 2, 3]),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual({
        byteLength: 3,
      });
      const jsonParseFailure = yield* Effect.exit(
        decodeKafkaCodec(jsonCodec, {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const jsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(jsonCodec, {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "order-1",
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const protobufFailure = yield* Effect.exit(
        decodeKafkaCodec(protobufCodec, {
          bytes: new Uint8Array([255]),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const customFailure = yield* Effect.exit(
        decodeKafkaCodec(customErrorCodec, {
          bytes: new Uint8Array([1]),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      expect(Exit.isFailure(jsonParseFailure)).toBe(true);
      expect(Exit.isFailure(jsonSchemaFailure)).toBe(true);
      expect(Exit.isFailure(protobufFailure)).toBe(true);
      expect(Exit.isFailure(customFailure)).toBe(true);

      expectTypeOf<KafkaCodecType<typeof bytesCodec>>().toEqualTypeOf<Uint8Array>();
      expectTypeOf<KafkaCodecType<typeof stringCodec>>().toEqualTypeOf<string>();
      expectTypeOf<KafkaCodecType<typeof stringKeyCodec>>().toEqualTypeOf<string>();
      expectTypeOf<KafkaCodecType<typeof jsonCodec>>().toEqualTypeOf<typeof Order.Type>();
      expectTypeOf<KafkaCodecType<typeof protobufCodec>>().toEqualTypeOf<OrdersValueMessage>();
      expectTypeOf<KafkaCodecType<typeof customCodec>>().toEqualTypeOf<{
        readonly byteLength: number;
      }>();
      expectTypeOf<KafkaCodecError<typeof jsonCodec>>().toEqualTypeOf<KafkaDecodeError>();
      expectTypeOf<KafkaCodecError<typeof protobufCodec>>().toEqualTypeOf<KafkaDecodeError>();
      expectTypeOf<KafkaCodecError<typeof customCodec>>().toEqualTypeOf<never>();
      expectTypeOf<
        KafkaCodecError<typeof customErrorCodec>
      >().toEqualTypeOf<CustomKafkaCodecError>();
    }),
  );

  it("does not expose executable React or runtime placeholders from config", () => {
    expect(Object.keys(viewServer)).toStrictEqual(["topics", "defineRuntimeOptions", "kafkaTopic"]);
  });
});

describe("public type surface", () => {
  it("exposes health and transport contracts", () => {
    const snapshot: SnapshotEvent<{ readonly id: string }> = {
      type: "snapshot",
      topic: "orders",
      queryId: "query-1",
      version: 1,
      keys: ["order-1"],
      rows: [{ id: "order-1" }],
      totalRows: 1,
    };

    const metadata: KafkaMessageMetadata<"usa"> = {
      sourceTopic: "orders",
      sourceRegion: "usa",
      partition: 0,
      offset: "1",
      timestamp: null,
      headers: {},
    };

    const topicHealth: TopicRuntimeHealth = {
      status: "ready",
      rowCount: 1,
      liveRowCount: 1,
      deletedRowCount: 0,
      version: 1,
      lastMutationAt: null,
      mutationsPerSecond: 0,
      rowsPerSecond: 0,
      pendingMutationBatches: 0,
      activeFallbackGroupedViews: 0,
      activeIncrementalGroupedViews: 0,
      activeViews: 0,
      activeSubscriptions: 0,
      queuedEvents: 0,
      maxQueueDepth: 0,
      backpressureEvents: 0,
      memoryBytes: 0,
      tombstoneCount: 0,
      compactionPending: false,
    };

    const health: ViewServerHealth<typeof viewServer.topics> = {
      status: "ready",
      version: 1,
      uptimeMs: 100,
      engine: {
        topics: {
          orders: topicHealth,
          trades: topicHealth,
          positions: topicHealth,
        },
      },
      transport: {
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
      },
    };

    const backpressure: StatusEvent = {
      type: "status",
      topic: "orders",
      queryId: "query-1",
      status: "error",
      code: "BackpressureExceeded",
      message: "client queue exceeded configured limits",
    };

    expect(snapshot.rows[0]).toStrictEqual({
      id: "order-1",
    });
    expect(metadata.sourceRegion).toBe("usa");
    expect(health.engine.topics["orders"].rowCount).toBe(1);
    expect(backpressure).toStrictEqual({
      type: "status",
      topic: "orders",
      queryId: "query-1",
      status: "error",
      code: "BackpressureExceeded",
      message: "client queue exceeded configured limits",
    });
    expectTypeOf<LiveTransportAdapter>().toHaveProperty("subscribe");
    expectTypeOf<Effect.Success<ReturnType<LiveTransportAdapter["subscribe"]>>>().toEqualTypeOf<
      LiveSubscription<unknown>
    >();
    expectTypeOf<
      Effect.Error<ReturnType<LiveTransportAdapter["subscribe"]>>
    >().toEqualTypeOf<ViewServerTransportError>();
  });

  it("derives pushed health summary and detailed rows from runtime health", () => {
    const health: ViewServerHealth<typeof viewServer.topics> = {
      status: "degraded",
      version: 7,
      uptimeMs: 100,
      engine: {
        topics: {
          orders: runtimeTopicHealth("ready", 10),
          trades: runtimeTopicHealth("degraded", 20),
          positions: runtimeTopicHealth("starting", 30),
        },
      },
      kafka: {
        regions: {
          usa: {
            status: "connected",
            brokers: "localhost:9092",
            lastConnectedAt: null,
            lastError: null,
          },
        },
        topics: {
          sourceOrders: {
            status: "ready",
            sourceTopic: "orders-source",
            viewServerTopic: "orders",
            regions: {
              usa: {
                connected: true,
                assignedPartitions: 1,
                messagesPerSecond: 10,
                bytesPerSecond: 100,
                decodedMessagesPerSecond: 10,
                decodeFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: null,
                lastCommitAt: null,
                consumerLagMessages: 5n,
                consumerLagMs: null,
                lagSampledAt: null,
                highWatermarkOffset: "10",
                committedOffset: "5",
                lastError: null,
              },
              london: {
                connected: true,
                assignedPartitions: 1,
                messagesPerSecond: 0,
                bytesPerSecond: 0,
                decodedMessagesPerSecond: 0,
                decodeFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: null,
                lastCommitAt: null,
                consumerLagMessages: null,
                consumerLagMs: null,
                lagSampledAt: null,
                highWatermarkOffset: null,
                committedOffset: null,
                lastError: null,
              },
            },
          },
          sourceTrades: {
            status: "stalled",
            sourceTopic: "trades-source",
            viewServerTopic: "trades",
            regions: {
              usa: {
                connected: false,
                assignedPartitions: 1,
                messagesPerSecond: 0,
                bytesPerSecond: 0,
                decodedMessagesPerSecond: 0,
                decodeFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: null,
                lastCommitAt: null,
                consumerLagMessages: 11n,
                consumerLagMs: null,
                lagSampledAt: null,
                highWatermarkOffset: "20",
                committedOffset: "9",
                lastError: "stalled",
              },
            },
          },
        },
      },
      transport: {
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
      },
    };

    const summary = viewServerHealthSummaryFromHealth(health, 123n);
    const summaryRow = viewServerHealthSummaryRowFromHealth(health, 123n);
    const rows = viewServerHealthTopicRowsFromHealth(health, 123n);
    const healthWithoutKafka: ViewServerHealth<typeof viewServer.topics> = {
      status: health.status,
      version: health.version,
      uptimeMs: health.uptimeMs,
      engine: health.engine,
      transport: health.transport,
    };
    const stoppingRows = viewServerHealthTopicRowsFromHealth(
      {
        ...health,
        status: "stopping",
      },
      456n,
    );

    expect(summary).toStrictEqual({
      status: "degraded",
      runtimeStatus: "degraded",
      connectionStatus: "connected",
      unhealthyTopics: ["trades", "positions"],
      updatedAtNanos: 123n,
      maxKafkaLag: 11n,
    });
    expect(summaryRow).toStrictEqual({
      id: "summary",
      status: "degraded",
      runtimeStatus: "degraded",
      connectionStatus: "connected",
      unhealthyTopics: ["trades", "positions"],
      updatedAtNanos: 123n,
      maxKafkaLag: 11n,
    });
    expect(rows.map((row) => [row.id, row.kafkaLag, row.status])).toStrictEqual([
      ["orders", 5n, "ready"],
      ["trades", 11n, "degraded"],
      ["positions", 0n, "starting"],
    ]);
    expect(viewServerHealthSummaryFromHealth(healthWithoutKafka, 123n).maxKafkaLag).toBe(0n);
    expect(stoppingRows.map((row) => row.status)).toStrictEqual([
      "stopping",
      "stopping",
      "stopping",
    ]);
    expect({
      summary: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
      detailed: VIEW_SERVER_HEALTH_TOPIC,
      detailedIsReserved: viewServerTopicNameIsReserved(VIEW_SERVER_HEALTH_TOPIC),
      ordersIsReserved: viewServerTopicNameIsReserved("orders"),
      all: viewServerReservedTopicNames,
    }).toStrictEqual({
      summary: "__view_server_health_summary",
      detailed: "__view_server_health",
      detailedIsReserved: true,
      ordersIsReserved: false,
      all: [VIEW_SERVER_HEALTH_SUMMARY_TOPIC, VIEW_SERVER_HEALTH_TOPIC],
    });
    expectTypeOf(summary).toEqualTypeOf<ViewServerHealthSummary<typeof viewServer.topics>>();
    expectTypeOf(summaryRow).toEqualTypeOf<ViewServerHealthSummaryRow<typeof viewServer.topics>>();
    expectTypeOf(rows[0]).toEqualTypeOf<
      ViewServerHealthTopicRow<"orders" | "trades" | "positions"> | undefined
    >();
    expectTypeOf<ViewServerHealthDetails<"orders">["status"]>().toEqualTypeOf<
      "ready" | "degraded" | "starting" | "stopping" | "connecting" | "disconnected"
    >();
  });

  it("derives query result rows from select and grouped aggregates", () => {
    const assertQueryTypes = (useLiveQuery: LiveQueryCall<typeof viewServer.topics>) => {
      const selectedRawResult = useLiveQuery("orders", {
        select: ["id", "customerId", "status", "price", "region", "updatedAt"],
        where: {
          status: { eq: "open" },
        },
      });

      expectTypeOf(selectedRawResult).toEqualTypeOf<{
        readonly rows: ReadonlyArray<{
          readonly id: string;
          readonly customerId: string;
          readonly status: "open" | "closed" | "cancelled";
          readonly price: number;
          readonly region: string;
          readonly updatedAt: number;
        }>;
        readonly totalRows: number;
        readonly version: number;
        readonly status: "loading" | "ready" | "stale" | "closed" | "error";
        readonly statusCode?:
          | "Ready"
          | "SnapshotStale"
          | "SubscriptionClosed"
          | "TransportError"
          | "BackpressureExceeded"
          | "InvalidTopic"
          | "InvalidRow"
          | "InvalidQuery"
          | "UnsupportedQuery"
          | "RuntimeUnavailable"
          | "RuntimeResetFailed"
          | undefined;
        readonly message?: string | undefined;
      }>();

      const selectedResult = useLiveQuery("orders", {
        select: ["customerId", "status", "updatedAt"],
        where: {
          customerId: { startsWith: "customer-" },
          status: "open",
          updatedAt: { gte: 1, lte: 10 },
        },
      });

      expectTypeOf(selectedResult).toEqualTypeOf<{
        readonly rows: ReadonlyArray<{
          readonly customerId: string;
          readonly status: "open" | "closed" | "cancelled";
          readonly updatedAt: number;
        }>;
        readonly totalRows: number;
        readonly version: number;
        readonly status: "loading" | "ready" | "stale" | "closed" | "error";
        readonly statusCode?:
          | "Ready"
          | "SnapshotStale"
          | "SubscriptionClosed"
          | "TransportError"
          | "BackpressureExceeded"
          | "InvalidTopic"
          | "InvalidRow"
          | "InvalidQuery"
          | "UnsupportedQuery"
          | "RuntimeUnavailable"
          | "RuntimeResetFailed"
          | undefined;
        readonly message?: string | undefined;
      }>();

      const rawRows = useLiveQuery("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 50,
      }).rows;

      const groupedRows = useLiveQuery("orders", {
        groupBy: ["status"],
        aggregates: {
          count: { aggFunc: "count" },
          totalPrice: { aggFunc: "sum", field: "price" },
          averageUpdatedAt: { aggFunc: "avg", field: "updatedAt" },
          firstStatus: { aggFunc: "min", field: "status" },
        },
        where: {
          region: "london",
        },
        orderBy: [
          { aggregate: "totalPrice", direction: "desc" },
          { field: "status", direction: "asc" },
        ],
      }).rows;

      expectTypeOf(rawRows).toEqualTypeOf<
        ReadonlyArray<{ readonly id: string; readonly price: number }>
      >();
      type GroupedRow = (typeof groupedRows)[number];
      expectTypeOf<GroupedRow>().toEqualTypeOf<{
        readonly status: "open" | "closed" | "cancelled";
        readonly count: bigint;
        readonly totalPrice: BigDecimal.BigDecimal;
        readonly averageUpdatedAt: BigDecimal.BigDecimal;
        readonly firstStatus: "open" | "closed" | "cancelled";
      }>();

      const singleAggregateResult = useLiveQuery("orders", {
        groupBy: ["region"],
        aggregates: { uniqueCustomers: { aggFunc: "countDistinct", field: "customerId" } },
      });

      expectTypeOf(singleAggregateResult).toEqualTypeOf<{
        readonly rows: ReadonlyArray<{
          readonly region: string;
          readonly uniqueCustomers: bigint;
        }>;
        readonly totalRows: number;
        readonly version: number;
        readonly status: "loading" | "ready" | "stale" | "closed" | "error";
        readonly statusCode?:
          | "Ready"
          | "SnapshotStale"
          | "SubscriptionClosed"
          | "TransportError"
          | "BackpressureExceeded"
          | "InvalidTopic"
          | "InvalidRow"
          | "InvalidQuery"
          | "UnsupportedQuery"
          | "RuntimeUnavailable"
          | "RuntimeResetFailed"
          | undefined;
        readonly message?: string | undefined;
      }>();

      const positionRows = useLiveQuery("positions", {
        select: ["id", "price", "quantity"],
        where: {
          accountId: { startsWith: "acct-" },
          active: true,
          quantity: { gte: 1n, lte: 100n },
          price: { gt: decimal("10.00") },
          notional: { lt: 1_000_000 },
        },
        orderBy: [
          { field: "price", direction: "desc" },
          { field: "quantity", direction: "asc" },
        ],
      }).rows;

      expectTypeOf(positionRows).toEqualTypeOf<
        ReadonlyArray<{
          readonly id: string;
          readonly price: BigDecimal.BigDecimal;
          readonly quantity: bigint;
        }>
      >();

      const groupedPositionRows = useLiveQuery("positions", {
        groupBy: ["accountId", "active"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          symbolCount: { aggFunc: "countDistinct", field: "symbol" },
          totalQuantity: { aggFunc: "sum", field: "quantity" },
          totalPrice: { aggFunc: "sum", field: "price" },
          totalNotional: { aggFunc: "sum", field: "notional" },
          averagePrice: { aggFunc: "avg", field: "price" },
          firstAccountId: { aggFunc: "min", field: "accountId" },
          maxQuantity: { aggFunc: "max", field: "quantity" },
        },
        orderBy: [
          { aggregate: "totalQuantity", direction: "desc" },
          { field: "accountId", direction: "asc" },
        ],
      }).rows;

      expectTypeOf<(typeof groupedPositionRows)[number]>().toEqualTypeOf<{
        readonly accountId: string;
        readonly active: boolean;
        readonly rowCount: bigint;
        readonly symbolCount: bigint;
        readonly totalQuantity: bigint;
        readonly totalPrice: BigDecimal.BigDecimal;
        readonly totalNotional: BigDecimal.BigDecimal;
        readonly averagePrice: BigDecimal.BigDecimal;
        readonly firstAccountId: string;
        readonly maxQuantity: bigint;
      }>();

      const optionalNumericSumQuery = {
        groupBy: ["accountId"],
        aggregates: {
          totalOptionalQuantity: { aggFunc: "sum", field: "optionalQuantity" },
        },
      } satisfies {
        readonly groupBy: readonly ["accountId"];
        readonly aggregates: {
          readonly totalOptionalQuantity: {
            readonly aggFunc: "sum";
            readonly field: "optionalQuantity";
          };
        };
      };
      // @ts-expect-error optional numeric fields cannot be summed without an explicit non-null mapping.
      useLiveQuery("positions", optionalNumericSumQuery);

      const optionalNumberSumQuery = {
        groupBy: ["accountId"],
        aggregates: {
          totalOptionalNotional: { aggFunc: "sum", field: "optionalNotional" },
        },
      } satisfies {
        readonly groupBy: readonly ["accountId"];
        readonly aggregates: {
          readonly totalOptionalNotional: {
            readonly aggFunc: "sum";
            readonly field: "optionalNotional";
          };
        };
      };
      // @ts-expect-error optional numeric fields cannot be summed without an explicit non-null mapping.
      useLiveQuery("positions", optionalNumberSumQuery);

      const dynamicAggregateAlias: string = "dynamicTotal";
      const dynamicAggregateQuery = {
        groupBy: ["status"],
        aggregates: {
          [dynamicAggregateAlias]: { aggFunc: "sum", field: "price" },
        },
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly [key: string]: {
            readonly aggFunc: "sum";
            readonly field: "price";
          };
        };
      };
      // @ts-expect-error aggregate aliases must be literal object keys.
      const _invalidDynamicAggregateAlias: ExactGroupedQuery<
        typeof Order.Type,
        typeof dynamicAggregateQuery
      > &
        ValidateLiveQuery<typeof dynamicAggregateQuery> = dynamicAggregateQuery;

      void _invalidDynamicAggregateAlias;
    };

    expect(assertQueryTypes).toBeTypeOf("function");
  });

  it.effect("infers and decodes Kafka mapping callback parameters through the topic helper", () =>
    Effect.gen(function* () {
      const topic = kafkaTopic({
        regions: ["usa", "london"],
        value: kafka.protobuf(ordersValueSchema),
        viewServerTopic: "orders",
        mapping: ({ key, value, region }) => {
          expectTypeOf(key).toEqualTypeOf<string>();
          expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
          expectTypeOf(region).toEqualTypeOf<"usa" | "london">();
          return {
            id: key,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region,
            updatedAt: value.updatedAt,
          };
        },
      });

      expect(topic.viewServerTopic).toBe("orders");
      expect(
        yield* decodeKafkaTopicMessage(topic, {
          keyBytes: textEncoder.encode("order-1"),
          valueBytes: toBinary(
            ordersValueSchema,
            create(ordersValueSchema, {
              customerId: "customer-1",
              status: "open",
              price: 42,
              updatedAt: 1,
            }),
          ),
          region: "london",
          metadata: kafkaTestMetadata("london"),
        }),
      ).toStrictEqual({
        viewServerTopic: "orders",
        row: {
          id: "order-1",
          customerId: "customer-1",
          status: "open",
          price: 42,
          region: "london",
          updatedAt: 1,
        },
      });

      const keyedTopic = kafkaTopic({
        regions: ["usa"],
        value: kafka.protobuf(ordersValueSchema),
        key: kafka.protobuf(ordersKeySchema),
        viewServerTopic: "orders",
        mapping: ({ key, value, region }) => {
          expectTypeOf(key).toEqualTypeOf<OrdersKeyMessage>();
          expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
          expectTypeOf(region).toEqualTypeOf<"usa">();
          return {
            id: key.orderId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region,
            updatedAt: value.updatedAt,
          };
        },
      });

      expect(keyedTopic.key.descriptor).toBe(ordersKeySchema);
      expect(
        yield* decodeKafkaTopicMessage(keyedTopic, {
          keyBytes: toBinary(
            ordersKeySchema,
            create(ordersKeySchema, {
              orderId: "order-keyed-1",
            }),
          ),
          valueBytes: toBinary(
            ordersValueSchema,
            create(ordersValueSchema, {
              customerId: "customer-keyed-1",
              status: "closed",
              price: 84,
              updatedAt: 2,
            }),
          ),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual({
        viewServerTopic: "orders",
        row: {
          id: "order-keyed-1",
          customerId: "customer-keyed-1",
          status: "closed",
          price: 84,
          region: "usa",
          updatedAt: 2,
        },
      });

      const throwingTopic = kafkaTopic({
        regions: ["usa"],
        value: kafka.protobuf(ordersValueSchema),
        viewServerTopic: "orders",
        mapping: () => {
          throw new Error("mapper failed");
        },
      });
      const mappingFailure = yield* Effect.exit(
        decodeKafkaTopicMessage(throwingTopic, {
          keyBytes: textEncoder.encode("order-throws"),
          valueBytes: toBinary(
            ordersValueSchema,
            create(ordersValueSchema, {
              customerId: "customer-throws",
              status: "open",
              price: 1,
              updatedAt: 1,
            }),
          ),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      expect(Exit.isFailure(mappingFailure)).toBe(true);
    }),
  );

  it("supports json and custom Kafka source codecs without weakening mapping exactness", () => {
    const jsonTopic = kafkaTopic({
      regions: ["usa"],
      value: kafka.json(Order),
      viewServerTopic: "orders",
      mapping: ({ key, value, region }) => {
        expectTypeOf(key).toEqualTypeOf<string>();
        expectTypeOf(value).toEqualTypeOf<typeof Order.Type>();
        expectTypeOf(region).toEqualTypeOf<"usa">();
        return value;
      },
    });

    const customTopic = kafkaTopic({
      regions: ["london"],
      value: kafka.codec({
        name: "trade-json-lines",
        decode: (): Effect.Effect<
          {
            readonly tradeId: string;
            readonly symbol: string;
            readonly quantity: number;
            readonly price: number;
          },
          never
        > =>
          Effect.succeed({
            tradeId: "trade-1",
            symbol: "AAPL",
            quantity: 10,
            price: 42,
          }),
      }),
      viewServerTopic: "trades",
      mapping: ({ key, value, region }) => {
        expectTypeOf(key).toEqualTypeOf<string>();
        expectTypeOf(value).toEqualTypeOf<{
          readonly tradeId: string;
          readonly symbol: string;
          readonly quantity: number;
          readonly price: number;
        }>();
        expectTypeOf(region).toEqualTypeOf<"london">();
        return {
          id: value.tradeId,
          symbol: value.symbol,
          quantity: value.quantity,
          price: value.price,
          region,
        };
      },
    });

    expect(jsonTopic.value.format).toBe("json");
    expect(customTopic.value.format).toBe("custom");
  });

  it("keeps real Protobuf-ES v2 generated schema inference typechecked", () => {
    expect(assertGeneratedSchemaContracts).toBeTypeOf("function");
  });
});

const assertGeneratedSchemaContracts = () => {
  const keyedTopic = kafkaTopic({
    regions: ["usa", "london"],
    value: kafka.protobuf(generatedOrdersValueSchema),
    key: kafka.protobuf(generatedOrdersKeySchema),
    viewServerTopic: "orders",
    mapping: ({ key, value, region }) => {
      expectTypeOf(key).toEqualTypeOf<
        Message<"viewserver.test.OrderKey"> & { readonly orderId: string }
      >();
      expectTypeOf(value).toEqualTypeOf<
        Message<"viewserver.test.OrderValue"> & {
          readonly customerId: string;
          readonly status: "open" | "closed" | "cancelled";
          readonly price: number;
          readonly updatedAt: number;
        }
      >();
      expectTypeOf(region).toEqualTypeOf<"usa" | "london">();
      return {
        id: key.orderId,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region,
        updatedAt: value.updatedAt,
      };
    },
  });

  expectTypeOf<KafkaCodecType<typeof keyedTopic.key>>().toEqualTypeOf<
    Message<"viewserver.test.OrderKey"> & {
      readonly orderId: string;
    }
  >();

  kafkaTopic({
    regions: ["usa", "london"],
    value: kafka.protobuf(generatedOrdersValueSchema),
    viewServerTopic: "orders",
    mapping: ({ key, value, region }) => {
      expectTypeOf(key).toEqualTypeOf<string>();
      expectTypeOf(value).toEqualTypeOf<
        Message<"viewserver.test.OrderValue"> & {
          readonly customerId: string;
          readonly status: "open" | "closed" | "cancelled";
          readonly price: number;
          readonly updatedAt: number;
        }
      >();
      expectTypeOf(region).toEqualTypeOf<"usa" | "london">();
      return {
        id: key,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region,
        updatedAt: value.updatedAt,
      };
    },
  });
};

const assertCompileTimeContracts = () => {
  const localKafkaRegions = {
    usa: "broker-a:9092",
  };
  const localKafkaTopic = viewServer.kafkaTopic<typeof localKafkaRegions>();
  const londonKafkaRegions = {
    london: "broker-b:9092",
  };
  const londonKafkaTopic = viewServer.kafkaTopic<typeof londonKafkaRegions>()({
    regions: ["london"],
    value: kafka.protobuf(ordersValueSchema),
    viewServerTopic: "orders",
    mapping: ({ key, value, region }) => ({
      id: key,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region,
      updatedAt: value.updatedAt,
    }),
  });
  const validLocalOrdersTopic = localKafkaTopic({
    regions: ["usa"],
    value: kafka.protobuf(ordersValueSchema),
    viewServerTopic: "orders",
    mapping: ({ key, value, region }) => ({
      id: key,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region,
      updatedAt: value.updatedAt,
    }),
  });
  const validKeyedLocalOrdersTopic = localKafkaTopic({
    regions: ["usa"],
    value: kafka.protobuf(ordersValueSchema),
    key: kafka.protobuf(ordersKeySchema),
    viewServerTopic: "orders",
    mapping: ({ key, value, region }) => ({
      id: key.orderId,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region,
      updatedAt: value.updatedAt,
    }),
  });
  const spreadValueMismatchTopic = {
    ...validLocalOrdersTopic,
    value: kafka.string(),
  };
  const spreadKeyMismatchTopic = {
    ...validKeyedLocalOrdersTopic,
    key: kafka.stringKey(),
  };
  const spreadMappingMismatchTopic = {
    ...validLocalOrdersTopic,
    mapping: (): typeof Trade.Type => ({
      id: "trade-1",
      symbol: "AAPL",
      quantity: 1,
      price: 42,
      region: "usa",
    }),
  };
  const spreadTargetMismatchTopic = {
    ...validLocalOrdersTopic,
    viewServerTopic: "trades",
  };
  type UnsafeJsonParseResult = ReturnType<typeof JSON.parse>;
  const unsafeValueCodec: KafkaCodec<UnsafeJsonParseResult> = kafka.bytes();
  const unsafeErrorCodec: KafkaCodec<string, UnsafeJsonParseResult> = kafka.string();
  const unknownErrorCodec: KafkaCodec<string, unknown> = kafka.string();

  // @ts-expect-error protobuf descriptors cannot be inferred from any
  kafka.protobuf(JSON.parse("{}"));

  // @ts-expect-error json schemas cannot be inferred from any
  kafka.json(JSON.parse("{}"));

  // @ts-expect-error custom Kafka codec values cannot infer any
  kafka.codec({
    name: "unsafe-effect-json-parse",
    decode: () => Effect.succeed(JSON.parse("{}")),
  });

  // @ts-expect-error custom Kafka codec errors cannot infer any
  kafka.codec({
    name: "unsafe-effect-json-parse-error",
    decode: () => Effect.fail(JSON.parse("{}")),
  });

  localKafkaTopic({
    regions: ["usa"],
    // @ts-expect-error Kafka value codecs cannot be inferred from any
    value: JSON.parse("{}"),
    viewServerTopic: "orders",
    mapping: (): typeof Order.Type => ({
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    }),
  });

  localKafkaTopic({
    regions: ["usa"],
    value: kafka.protobuf(ordersValueSchema),
    // @ts-expect-error Kafka key codecs cannot be inferred from any
    key: JSON.parse("{}"),
    viewServerTopic: "orders",
    mapping: (): typeof Order.Type => ({
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    }),
  });

  localKafkaTopic({
    regions: ["usa"],
    // @ts-expect-error Kafka value codecs cannot be widened to KafkaCodec<any>
    value: unsafeValueCodec,
    viewServerTopic: "orders",
    mapping: (): typeof Order.Type => ({
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    }),
  });

  localKafkaTopic({
    regions: ["usa"],
    // @ts-expect-error Kafka codec error channels cannot be widened to any
    value: unsafeErrorCodec,
    viewServerTopic: "orders",
    mapping: (): typeof Order.Type => ({
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    }),
  });

  localKafkaTopic({
    regions: ["usa"],
    // @ts-expect-error Kafka codec error channels cannot be widened to unknown
    value: unknownErrorCodec,
    viewServerTopic: "orders",
    mapping: (): typeof Order.Type => ({
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    }),
  });

  expectTypeOf<
    KafkaMappingInput<
      typeof viewServer.topics,
      "orders",
      "usa" | "london",
      typeof ordersValueKafkaCodec,
      typeof ordersKeyKafkaCodec
    >["key"]
  >().toEqualTypeOf<OrdersKeyMessage>();
  expectTypeOf<
    KafkaMappingInput<
      typeof viewServer.topics,
      "orders",
      "usa" | "london",
      typeof ordersValueKafkaCodec,
      typeof ordersKeyKafkaCodec
    >["value"]
  >().toEqualTypeOf<OrdersValueMessage>();
  expectTypeOf<
    KafkaMappingInput<
      typeof viewServer.topics,
      "orders",
      "usa" | "london",
      typeof ordersValueKafkaCodec,
      typeof ordersKeyKafkaCodec
    >["region"]
  >().toEqualTypeOf<"usa" | "london">();
  expectTypeOf<
    KafkaMappingInput<
      typeof viewServer.topics,
      "orders",
      "usa" | "london",
      typeof ordersValueKafkaCodec,
      typeof ordersKeyKafkaCodec
    >["schema"]
  >().toEqualTypeOf<typeof Order>();
  expectTypeOf<
    KafkaMappingInput<
      typeof viewServer.topics,
      "orders",
      "usa" | "london",
      typeof ordersValueKafkaCodec,
      typeof ordersKeyKafkaCodec
    >["metadata"]["sourceRegion"]
  >().toEqualTypeOf<"usa" | "london">();
  expectTypeOf<
    KafkaMappingInput<
      typeof viewServer.topics,
      "trades",
      "usa",
      typeof tradesValueKafkaCodec,
      undefined
    >["key"]
  >().toEqualTypeOf<string>();
  expectTypeOf<
    KafkaMappingInput<
      typeof viewServer.topics,
      "trades",
      "usa",
      typeof tradesValueKafkaCodec,
      undefined
    >["value"]
  >().toEqualTypeOf<TradesValueMessage>();
  expectTypeOf<
    KafkaMappingInput<
      typeof viewServer.topics,
      "trades",
      "usa",
      typeof tradesValueKafkaCodec,
      undefined
    >["region"]
  >().toEqualTypeOf<"usa">();
  expectTypeOf<
    KafkaMappingInput<
      typeof viewServer.topics,
      "trades",
      "usa",
      typeof tradesValueKafkaCodec,
      undefined
    >["schema"]
  >().toEqualTypeOf<typeof Trade>();

  const assertRuntimeContracts = (runtime: ViewServerRuntimeClient<typeof viewServer.topics>) => {
    const publishEffect = runtime.publish("orders", {
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    });
    const snapshotEffect = runtime.snapshot("orders", {
      select: ["id"],
      where: {
        status: "open",
      },
    });
    const patchEffect = runtime.patch("orders", "order-1", {
      price: 43,
      status: "closed",
    });

    expectTypeOf<Effect.Error<typeof publishEffect>>().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<Effect.Error<typeof snapshotEffect>>().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<Effect.Error<typeof patchEffect>>().toEqualTypeOf<ViewServerRuntimeError>();

    const invalidPublishWrongField = runtime.publish("orders", {
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      // @ts-expect-error publish rows must match the topic schema
      updatedAt: "not-a-number",
    });

    const invalidPublishMissingField = runtime.publish("trades", {
      id: "trade-1",
      symbol: "AAPL",
      quantity: 1,
      price: 42,
      // @ts-expect-error publish rows must include all required topic fields
      updatedAt: 1,
    });

    const invalidPublishTopic = runtime.publish(
      // @ts-expect-error runtime publish topics are constrained to configured topics
      "customers",
      {
        id: "customer-1",
      },
    );

    const invalidPatchField = runtime.patch("orders", "order-1", {
      // @ts-expect-error patch fields must belong to the selected topic row
      missing: true,
    });

    const invalidPatchValue = runtime.patch("orders", "order-1", {
      // @ts-expect-error patch field values must match the selected topic row
      price: "not-a-number",
    });

    const invalidSnapshotTopic = runtime.snapshot(
      // @ts-expect-error snapshot topics are constrained to configured topics
      "customers",
      {},
    );

    const invalidSnapshotFilter = runtime.snapshot("orders", {
      // @ts-expect-error invalid query collapse keeps selected fields from being accepted
      select: ["id"],
      where: {
        // @ts-expect-error snapshot filters must use values from the selected topic row
        price: "not-a-number",
      },
    });
    expectTypeOf<
      Effect.Error<typeof invalidPublishWrongField>
    >().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<
      Effect.Error<typeof invalidPublishMissingField>
    >().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<
      Effect.Error<typeof invalidPublishTopic>
    >().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<Effect.Error<typeof invalidPatchField>>().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<Effect.Error<typeof invalidPatchValue>>().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<
      Effect.Error<typeof invalidSnapshotTopic>
    >().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<
      Effect.Error<typeof invalidSnapshotFilter>
    >().toEqualTypeOf<ViewServerRuntimeError>();
  };

  expectTypeOf(assertRuntimeContracts).toBeFunction();
  expectTypeOf<ViewServerBackpressureError>().toMatchTypeOf<ViewServerRuntimeError>();

  defineViewServerConfig({
    topics: {
      invalid: {
        schema: Order,
        // @ts-expect-error topic keys must be string fields from the Effect Schema row type
        key: "missing",
      },
    },
  });

  defineViewServerConfig({
    topics: {
      loose: {
        // @ts-expect-error topic schemas must expose concrete fields for query typing and wire validation
        schema: Schema.Record(Schema.String, Schema.String),
        // @ts-expect-error non-field schemas cannot provide a valid string row key
        key: "id",
      },
    },
  });

  defineViewServerConfig({
    topics: {
      // @ts-expect-error system health topic names are reserved
      __view_server_health: {
        schema: Order,
        key: "id",
      },
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    tcpPublishPort: 8081,
    // @ts-expect-error runtime options reject unknown top-level fields
    extraRuntimeField: true,
    kafka: {
      regions: {
        usa: "broker-a:9092",
      },
      topics: {},
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    tcpPublishPort: 8081,
    // @ts-expect-error runtime options must include Kafka topic definitions
    kafka: {
      regions: {
        usa: "broker-a:9092",
      },
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    tcpPublishPort: 8081,
    kafka: {
      regions: {
        usa: "broker-a:9092",
      },
      topics: {},
      // @ts-expect-error runtime kafka options reject unknown fields
      extraKafkaField: true,
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    tcpPublishPort: 8081,
    kafka: {
      regions: localKafkaRegions,
      topics: {
        // @ts-expect-error Kafka source topics must be created with viewServer.kafkaTopic
        orders: {
          regions: ["usa"],
          value: kafka.protobuf(ordersValueSchema),
          viewServerTopic: "orders",
          mapping: () => ({
            id: "order-1",
            customerId: "customer-1",
            status: "open",
            price: 42,
            region: "usa",
            updatedAt: 1,
          }),
        },
      },
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    tcpPublishPort: 8081,
    kafka: {
      regions: localKafkaRegions,
      topics: {
        // @ts-expect-error spread-mutated Kafka topic values must still match mapping input types
        orders: spreadValueMismatchTopic,
      },
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    tcpPublishPort: 8081,
    kafka: {
      regions: localKafkaRegions,
      topics: {
        // @ts-expect-error spread-mutated Kafka topic keys must still match mapping input types
        orders: spreadKeyMismatchTopic,
      },
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    tcpPublishPort: 8081,
    kafka: {
      regions: localKafkaRegions,
      topics: {
        // @ts-expect-error spread-mutated Kafka mappings must still return the target topic row
        orders: spreadMappingMismatchTopic,
      },
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    tcpPublishPort: 8081,
    kafka: {
      regions: localKafkaRegions,
      topics: {
        // @ts-expect-error spread-mutated Kafka target topics must still match the mapping row
        orders: spreadTargetMismatchTopic,
      },
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    tcpPublishPort: 8081,
    kafka: {
      regions: localKafkaRegions,
      topics: {
        // @ts-expect-error Kafka topic helper regions must match runtime kafka.regions
        orders: londonKafkaTopic,
      },
    },
  });

  // @ts-expect-error Kafka topic regions are constrained to kafka.regions keys
  localKafkaTopic({ regions: ["USA"] });

  // @ts-expect-error Kafka topic regions must be non-empty
  localKafkaTopic({ regions: [] });

  // @ts-expect-error Kafka mappings must target a configured View Server topic
  localKafkaTopic({ viewServerTopic: "customers" });

  const invalidExtraKafkaTopicField: KafkaTopicDefinition<
    typeof viewServer.topics,
    typeof localKafkaRegions,
    "orders",
    typeof ordersValueKafkaCodec,
    undefined,
    readonly ["usa"]
  > = {
    regions: ["usa"],
    value: kafka.protobuf(ordersValueSchema),
    viewServerTopic: "orders",
    // @ts-expect-error Kafka topic definitions reject unknown topic contract fields
    extraTopicField: true,
    mapping: ({ key, value, region }) => ({
      id: key,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region,
      updatedAt: value.updatedAt,
    }),
  };

  expectTypeOf(invalidExtraKafkaTopicField.viewServerTopic).toEqualTypeOf<"orders">();

  localKafkaTopic({
    regions: ["usa"],
    value: kafka.protobuf(ordersValueSchema),
    // @ts-expect-error unsupported Kafka key codecs must fail instead of inferring unknown
    key: {},
    viewServerTopic: "orders",
    mapping: ({ key, value, region }) => ({
      id: key,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region,
      updatedAt: value.updatedAt,
    }),
  });

  localKafkaTopic({
    regions: ["usa"],
    value: kafka.json(OrderWithExtraSourceField),
    viewServerTopic: "orders",
    // @ts-expect-error returning source JSON value directly rejects fields outside the target row
    mapping: ({ value }) => value,
  });

  localKafkaTopic({
    regions: ["usa"],
    value: kafka.protobuf(ordersValueSchema),
    key: kafka.protobuf(ordersKeySchema),
    viewServerTopic: "orders",
    // @ts-expect-error unannotated mapping returns must match the target View Server topic row
    mapping: ({ key, value, region }) => ({
      id: key.orderId,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region,
    }),
  });

  localKafkaTopic({
    regions: ["usa"],
    value: kafka.protobuf(ordersValueSchema),
    key: kafka.protobuf(ordersKeySchema),
    viewServerTopic: "orders",
    // @ts-expect-error unannotated mapping returns reject extra fields outside the target row
    mapping: ({ key, value, region }) => ({
      id: key.orderId,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region,
      updatedAt: value.updatedAt,
      ze: true,
    }),
  });

  localKafkaTopic({
    regions: ["usa"],
    value: kafka.protobuf(ordersValueSchema),
    key: kafka.protobuf(ordersKeySchema),
    viewServerTopic: "orders",
    mapping: ({ key, value, schema, metadata }) => {
      expectTypeOf(key).toEqualTypeOf<OrdersKeyMessage>();
      expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
      expectTypeOf(schema).toEqualTypeOf<typeof Order>();
      expectTypeOf(metadata.sourceRegion).toEqualTypeOf<"usa">();
      expectTypeOf(metadata.headers).toEqualTypeOf<
        Readonly<Record<string, string | Uint8Array | ReadonlyArray<string | Uint8Array>>>
      >();
      return {
        id: key.orderId,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: metadata.sourceRegion,
        updatedAt: value.updatedAt,
      };
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    tcpPublishPort: 8081,
    kafka: {
      regions: {
        usa: "broker-a:9092",
      },
      topics: {
        orders: localKafkaTopic({
          regions: ["usa"],
          value: kafka.protobuf(ordersValueSchema),
          key: kafka.protobuf(ordersKeySchema),
          viewServerTopic: "orders",
          // @ts-expect-error mapping return must match the target View Server topic row type
          mapping: ({ key, value, region }) => ({
            id: key.orderId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region,
          }),
        }),
      },
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    tcpPublishPort: 8081,
    kafka: {
      regions: {
        usa: "broker-a:9092",
      },
      topics: {
        orders: localKafkaTopic({
          regions: ["usa"],
          value: kafka.protobuf(ordersValueSchema),
          key: kafka.protobuf(ordersKeySchema),
          viewServerTopic: "orders",
          // @ts-expect-error raw runtime topic mappings must return the target topic row
          mapping: ({ key, value, region }) => ({
            id: key.orderId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region,
          }),
        }),
      },
    },
  });

  const assertLiveQueryContracts = (useLiveQuery: LiveQueryCall<typeof viewServer.topics>) => {
    // @ts-expect-error raw queries must explicitly select projected fields.
    useLiveQuery("orders", {
      where: { status: "open" },
    });

    const unknownWhereFieldQuery = {
      select: ["id"],
      where: {
        missing: "open",
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly missing: "open" };
    };
    // @ts-expect-error raw queries reject fields not present on the selected topic.
    useLiveQuery("orders", unknownWhereFieldQuery);

    const wrongFilterValueQuery = {
      select: ["id"],
      where: {
        price: "not-a-number",
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly price: "not-a-number" };
    };
    // @ts-expect-error filter values must match the selected field type.
    useLiveQuery("orders", wrongFilterValueQuery);

    const stringRangeFilterQuery = {
      select: ["id"],
      where: {
        status: { gte: "open" },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly status: { readonly gte: "open" } };
    };
    // @ts-expect-error string filters do not accept range operators.
    useLiveQuery("orders", stringRangeFilterQuery);

    const invalidStatusInFilter = {
      select: ["id"],
      where: {
        status: { in: ["open", "pending"] },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: {
        readonly status: {
          readonly in: readonly ["open", "pending"];
        };
      };
    };
    // @ts-expect-error filter arrays must contain selected field values
    const _invalidStatusInFilter: RawQuery<typeof Order.Type> &
      ExactRawQuery<typeof Order.Type, typeof invalidStatusInFilter> = invalidStatusInFilter;

    const numericStartsWithFilterQuery = {
      select: ["id"],
      where: {
        price: { startsWith: "1" },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly price: { readonly startsWith: "1" } };
    };
    // @ts-expect-error number filters do not accept string-only operators.
    useLiveQuery("orders", numericStartsWithFilterQuery);

    const booleanRangeFilterQuery = {
      select: ["id"],
      where: {
        active: { gte: true },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly active: { readonly gte: true } };
    };
    // @ts-expect-error boolean filters do not accept range operators.
    useLiveQuery("positions", booleanRangeFilterQuery);

    const booleanStartsWithFilterQuery = {
      select: ["id"],
      where: {
        active: { startsWith: "t" },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly active: { readonly startsWith: "t" } };
    };
    // @ts-expect-error boolean filters do not accept string-only operators.
    useLiveQuery("positions", booleanStartsWithFilterQuery);

    const bigDecimalStringFilterQuery = {
      select: ["id"],
      where: {
        price: { gte: "10.00" },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly price: { readonly gte: "10.00" } };
    };
    // @ts-expect-error BigDecimal filters require BigDecimal values, not strings.
    useLiveQuery("positions", bigDecimalStringFilterQuery);

    const bigDecimalStartsWithFilterQuery = {
      select: ["id"],
      where: {
        price: { startsWith: "10" },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly price: { readonly startsWith: "10" } };
    };
    // @ts-expect-error BigDecimal filters do not accept string-only operators.
    useLiveQuery("positions", bigDecimalStartsWithFilterQuery);

    const bigintNumberFilterQuery = {
      select: ["id"],
      where: {
        quantity: { gte: 1 },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly quantity: { readonly gte: 1 } };
    };
    // @ts-expect-error bigint filters require bigint values, not numbers.
    useLiveQuery("positions", bigintNumberFilterQuery);

    const optionalNumericEqualityRows = useLiveQuery("positions", {
      select: ["id"],
      where: {
        optionalQuantity: { eq: 1n },
        optionalNotional: 100,
      },
    }).rows;
    expectTypeOf(optionalNumericEqualityRows).toEqualTypeOf<
      ReadonlyArray<{ readonly id: string }>
    >();

    const optionalBigintUndefinedFilterQuery = {
      select: ["id"],
      where: {
        optionalQuantity: undefined,
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly optionalQuantity: undefined };
    };
    // @ts-expect-error optional filters reject present undefined values.
    useLiveQuery("positions", optionalBigintUndefinedFilterQuery);

    const optionalBigintUnionFilterQuery = (optionalQuantity: bigint | undefined) =>
      ({
        select: ["id"],
        where: {
          optionalQuantity,
        },
      }) satisfies {
        readonly select: readonly ["id"];
        readonly where: { readonly optionalQuantity: bigint | undefined };
      };
    // @ts-expect-error optional filters reject unions that can contain undefined.
    useLiveQuery("positions", optionalBigintUnionFilterQuery(1n));

    const optionalBigintUndefinedEqualityFilterQuery = {
      select: ["id"],
      where: {
        optionalQuantity: { eq: undefined },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly optionalQuantity: { readonly eq: undefined } };
    };
    // @ts-expect-error optional equality filters reject present undefined values.
    useLiveQuery("positions", optionalBigintUndefinedEqualityFilterQuery);

    const optionalBigintUnionEqualityFilterQuery = (eq: bigint | undefined) =>
      ({
        select: ["id"],
        where: {
          optionalQuantity: { eq },
        },
      }) satisfies {
        readonly select: readonly ["id"];
        readonly where: { readonly optionalQuantity: { readonly eq: bigint | undefined } };
      };
    // @ts-expect-error optional equality filters reject unions that can contain undefined.
    useLiveQuery("positions", optionalBigintUnionEqualityFilterQuery(1n));

    const optionalBigintRangeFilterQuery = {
      select: ["id"],
      where: {
        optionalQuantity: { gte: 1n },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly optionalQuantity: { readonly gte: 1n } };
    };
    // @ts-expect-error optional numeric fields only support equality filters.
    useLiveQuery("positions", optionalBigintRangeFilterQuery);

    const optionalBigintEqualityWithRangeFilterQuery = {
      select: ["id"],
      where: {
        optionalQuantity: { eq: 1n, gte: 1n },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly optionalQuantity: { readonly eq: 1n; readonly gte: 1n } };
    };
    // @ts-expect-error optional numeric exact filters reject range operators even when equality is present.
    useLiveQuery("positions", optionalBigintEqualityWithRangeFilterQuery);

    const optionalNumberRangeFilterQuery = {
      select: ["id"],
      where: {
        optionalNotional: { lte: 100 },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly optionalNotional: { readonly lte: 100 } };
    };
    // @ts-expect-error optional numeric fields only support equality filters.
    useLiveQuery("positions", optionalNumberRangeFilterQuery);

    const optionalNumberEqualityWithRangeFilterQuery = {
      select: ["id"],
      where: {
        optionalNotional: { eq: 100, lte: 100 },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly optionalNotional: { readonly eq: 100; readonly lte: 100 } };
    };
    // @ts-expect-error optional numeric exact filters reject range operators even when equality is present.
    useLiveQuery("positions", optionalNumberEqualityWithRangeFilterQuery);

    const unknownOrderByFieldQuery = {
      select: ["id"],
      orderBy: [{ field: "missing", direction: "asc" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly orderBy: readonly [{ readonly field: "missing"; readonly direction: "asc" }];
    };
    // @ts-expect-error orderBy fields are constrained to the selected topic row.
    useLiveQuery("orders", unknownOrderByFieldQuery);

    const invalidOrderByDirectionQuery = {
      select: ["id"],
      orderBy: [{ field: "price", direction: "ascending" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly orderBy: readonly [{ readonly field: "price"; readonly direction: "ascending" }];
    };
    // @ts-expect-error sort direction is constrained to asc or desc.
    useLiveQuery("orders", invalidOrderByDirectionQuery);

    const rawAggregateOrderByQuery = {
      select: ["id"],
      orderBy: [{ aggregate: "totalPrice", direction: "desc" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly orderBy: readonly [{ readonly aggregate: "totalPrice"; readonly direction: "desc" }];
    };
    // @ts-expect-error raw orderBy cannot reference aggregate aliases.
    useLiveQuery("orders", rawAggregateOrderByQuery);

    const invalidSelectedFields = {
      select: ["id", "missing"],
    } satisfies {
      readonly select: readonly ["id", "missing"];
    };
    // @ts-expect-error projected fields are constrained to the selected topic row
    useLiveQuery("orders", invalidSelectedFields);

    const invalidGroupByField = {
      groupBy: ["missing"],
      aggregates: { count: { aggFunc: "count" } },
    } satisfies {
      readonly groupBy: readonly ["missing"];
      readonly aggregates: {
        readonly count: {
          readonly aggFunc: "count";
        };
      };
    };
    // @ts-expect-error grouped queries reject groupBy fields not present on the topic row
    const _invalidGroupByField: ExactGroupedQuery<typeof Order.Type, typeof invalidGroupByField> =
      invalidGroupByField;

    const invalidGroupedSelect = {
      groupBy: ["status"],
      select: ["id"],
      aggregates: { count: { aggFunc: "count" } },
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly select: readonly ["id"];
      readonly aggregates: {
        readonly count: {
          readonly aggFunc: "count";
        };
      };
    };
    // @ts-expect-error grouped queries cannot select raw fields.
    const _invalidGroupedSelect: ExactGroupedQuery<typeof Order.Type, typeof invalidGroupedSelect> =
      invalidGroupedSelect;

    const invalidAggregateAliasCollision = {
      groupBy: ["status"],
      aggregates: { status: { aggFunc: "count" } },
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly status: {
          readonly aggFunc: "count";
        };
      };
    };
    // @ts-expect-error aggregate aliases cannot collide with groupBy fields
    const _invalidAggregateAliasCollision: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidAggregateAliasCollision
    > &
      ValidateLiveQuery<typeof invalidAggregateAliasCollision> = invalidAggregateAliasCollision;

    const invalidEmptyAggregates = {
      groupBy: ["status"],
      aggregates: {},
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {};
    };
    // @ts-expect-error grouped queries require at least one aggregate alias.
    const _invalidEmptyAggregates: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidEmptyAggregates
    > &
      ValidateLiveQuery<typeof invalidEmptyAggregates> = invalidEmptyAggregates;

    const invalidDangerousAggregateAlias = {
      groupBy: ["status"],
      aggregates: { constructor: { aggFunc: "count" } },
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly constructor: {
          readonly aggFunc: "count";
        };
      };
    };
    // @ts-expect-error grouped aggregate aliases must not use dangerous object keys.
    const _invalidDangerousAggregateAlias: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidDangerousAggregateAlias
    > &
      ValidateLiveQuery<typeof invalidDangerousAggregateAlias> = invalidDangerousAggregateAlias;

    const invalidGroupedOrderByRawField = {
      groupBy: ["status"],
      aggregates: { count: { aggFunc: "count" } },
      orderBy: [{ field: "price", direction: "desc" }],
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly count: {
          readonly aggFunc: "count";
        };
      };
      readonly orderBy: readonly [
        {
          readonly field: "price";
          readonly direction: "desc";
        },
      ];
    };
    // @ts-expect-error grouped orderBy only accepts groupBy fields or aggregate aliases.
    const _invalidGroupedOrderByRawField: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidGroupedOrderByRawField
    > = invalidGroupedOrderByRawField;

    const invalidGroupedOrderByDirection = {
      groupBy: ["status"],
      aggregates: { count: { aggFunc: "count" } },
      orderBy: [{ aggregate: "count", direction: "descending" }],
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly count: {
          readonly aggFunc: "count";
        };
      };
      readonly orderBy: readonly [
        {
          readonly aggregate: "count";
          readonly direction: "descending";
        },
      ];
    };
    // @ts-expect-error grouped orderBy direction is constrained to asc or desc.
    const _invalidGroupedOrderByDirection: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidGroupedOrderByDirection
    > = invalidGroupedOrderByDirection;

    const invalidGroupedOrderByAggregate = {
      groupBy: ["status"],
      aggregates: { count: { aggFunc: "count" } },
      orderBy: [{ aggregate: "totalPrice", direction: "desc" }],
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly count: {
          readonly aggFunc: "count";
        };
      };
      readonly orderBy: readonly [
        {
          readonly aggregate: "totalPrice";
          readonly direction: "desc";
        },
      ];
    };
    // @ts-expect-error grouped orderBy aggregate aliases must exist in aggregates.
    const _invalidGroupedOrderByAggregate: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidGroupedOrderByAggregate
    > = invalidGroupedOrderByAggregate;

    const invalidGroupedOrderByFieldKey = {
      groupBy: ["status"],
      aggregates: { count: { aggFunc: "count" } },
      orderBy: [{ orderByField: "status", direction: "asc" }],
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly count: {
          readonly aggFunc: "count";
        };
      };
      readonly orderBy: readonly [
        {
          readonly orderByField: "status";
          readonly direction: "asc";
        },
      ];
    };
    // @ts-expect-error grouped orderBy group fields use field, not orderByField.
    const _invalidGroupedOrderByFieldKey: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidGroupedOrderByFieldKey
    > = invalidGroupedOrderByFieldKey;

    const invalidGroupedOrderByAggregateKey = {
      groupBy: ["status"],
      aggregates: { count: { aggFunc: "count" } },
      orderBy: [{ field: "count", direction: "desc" }],
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly count: {
          readonly aggFunc: "count";
        };
      };
      readonly orderBy: readonly [
        {
          readonly field: "count";
          readonly direction: "desc";
        },
      ];
    };
    // @ts-expect-error grouped orderBy aggregate aliases use aggregate, not field.
    const _invalidGroupedOrderByAggregateKey: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidGroupedOrderByAggregateKey
    > = invalidGroupedOrderByAggregateKey;

    const invalidGroupedOrderByBothFieldAndAggregate = {
      groupBy: ["status"],
      aggregates: { count: { aggFunc: "count" } },
      orderBy: [{ field: "status", aggregate: "count", direction: "desc" }],
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly count: {
          readonly aggFunc: "count";
        };
      };
      readonly orderBy: readonly [
        {
          readonly field: "status";
          readonly aggregate: "count";
          readonly direction: "desc";
        },
      ];
    };
    // @ts-expect-error grouped orderBy entries must choose field or aggregate, not both.
    const _invalidGroupedOrderByBothFieldAndAggregate: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidGroupedOrderByBothFieldAndAggregate
    > = invalidGroupedOrderByBothFieldAndAggregate;

    const rawOrderByFieldAndAggregateQuery = {
      select: ["id"],
      orderBy: [{ field: "price", aggregate: "totalPrice", direction: "desc" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly orderBy: readonly [
        {
          readonly field: "price";
          readonly aggregate: "totalPrice";
          readonly direction: "desc";
        },
      ];
    };
    // @ts-expect-error raw orderBy entries cannot also include aggregate.
    useLiveQuery("orders", rawOrderByFieldAndAggregateQuery);

    const invalidOrderSumField = {
      groupBy: ["status"],
      aggregates: {
        badTotal: { aggFunc: "sum", field: "status" },
      },
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly badTotal: {
          readonly aggFunc: "sum";
          readonly field: "status";
        };
      };
    };
    // @ts-expect-error sum and avg aggregate fields must be numeric
    const _invalidOrderSumField: ExactGroupedQuery<typeof Order.Type, typeof invalidOrderSumField> =
      invalidOrderSumField;

    const invalidAggregateExtraKey = {
      groupBy: ["status"],
      aggregates: {
        totalPrice: { aggFunc: "sum", field: "price", typo: true },
      },
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly totalPrice: {
          readonly aggFunc: "sum";
          readonly field: "price";
          readonly typo: true;
        };
      };
    };
    // @ts-expect-error aggregate definitions reject extra keys through variables.
    const _invalidAggregateExtraKey: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidAggregateExtraKey
    > = invalidAggregateExtraKey;

    const invalidCountAggregateField = {
      groupBy: ["status"],
      aggregates: {
        rowCount: { aggFunc: "count", field: "price" },
      },
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly rowCount: {
          readonly aggFunc: "count";
          readonly field: "price";
        };
      };
    };
    // @ts-expect-error count aggregate definitions must not include a field.
    const _invalidCountAggregateField: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidCountAggregateField
    > = invalidCountAggregateField;

    const invalidPositionSumField = {
      groupBy: ["accountId"],
      aggregates: {
        badSymbolTotal: { aggFunc: "sum", field: "symbol" },
      },
    } satisfies {
      readonly groupBy: readonly ["accountId"];
      readonly aggregates: {
        readonly badSymbolTotal: {
          readonly aggFunc: "sum";
          readonly field: "symbol";
        };
      };
    };
    // @ts-expect-error sum aggregate fields must be numeric, bigint, or BigDecimal
    const _invalidPositionSumField: ExactGroupedQuery<
      typeof Position.Type,
      typeof invalidPositionSumField
    > = invalidPositionSumField;

    const invalidOrderAverageField = {
      groupBy: ["status"],
      aggregates: {
        badAverage: { aggFunc: "avg", field: "status" },
      },
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly badAverage: {
          readonly aggFunc: "avg";
          readonly field: "status";
        };
      };
    };
    // @ts-expect-error avg aggregate fields must be numeric
    const _invalidOrderAverageField: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidOrderAverageField
    > = invalidOrderAverageField;

    const invalidPositionAverageField = {
      groupBy: ["accountId"],
      aggregates: {
        badSymbolAverage: { aggFunc: "avg", field: "symbol" },
      },
    } satisfies {
      readonly groupBy: readonly ["accountId"];
      readonly aggregates: {
        readonly badSymbolAverage: {
          readonly aggFunc: "avg";
          readonly field: "symbol";
        };
      };
    };
    // @ts-expect-error avg aggregate fields must be numeric, bigint, or BigDecimal
    const _invalidPositionAverageField: ExactGroupedQuery<
      typeof Position.Type,
      typeof invalidPositionAverageField
    > = invalidPositionAverageField;
  };

  expectTypeOf(assertLiveQueryContracts).toBeFunction();

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    tcpPublishPort: 8081,
    kafka: {
      regions: {
        usa: "broker-a:9092",
      },
      topics: {
        orders: localKafkaTopic({
          regions: ["usa"],
          value: kafka.protobuf(ordersValueSchema),
          key: kafka.protobuf(ordersKeySchema),
          viewServerTopic: "orders",
          mapping: ({ key, value, region }) => {
            expectTypeOf(key).toEqualTypeOf<OrdersKeyMessage>();
            expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
            expectTypeOf(region).toEqualTypeOf<"usa">();
            return {
              id: key.orderId,
              customerId: value.customerId,
              status: value.status,
              price: value.price,
              region,
              updatedAt: value.updatedAt,
            };
          },
        }),
        trades: localKafkaTopic({
          regions: ["usa"],
          value: kafka.protobuf(tradesValueSchema),
          viewServerTopic: "trades",
          mapping: ({ key, value, region }) => {
            expectTypeOf(key).toEqualTypeOf<string>();
            expectTypeOf(value).toEqualTypeOf<TradesValueMessage>();
            expectTypeOf(region).toEqualTypeOf<"usa">();
            return {
              id: key,
              symbol: value.symbol,
              quantity: value.quantity,
              price: value.price,
              region,
            };
          },
        }),
      },
    },
  });

  // @ts-expect-error unsupported Kafka value codecs must fail instead of inferring unknown
  localKafkaTopic({ value: {} });

  // @ts-expect-error $typeName-only objects are message instances, not generated schemas/codecs
  localKafkaTopic({ value: { $typeName: "viewserver.test.OrderValue" } });

  // @ts-expect-error arbitrary decoder shapes are not accepted as Kafka codecs
  localKafkaTopic({ value: { fromBinary: (_bytes: Uint8Array) => ({}) } });

  // @ts-expect-error row Effect schemas are not Kafka codecs unless wrapped with kafka.json
  localKafkaTopic({ value: Order });
};

describe("compile-time contract assertions", () => {
  it("keeps negative type tests typechecked without executing placeholders", () => {
    expect(assertCompileTimeContracts).toBeTypeOf("function");
  });
});

describe("reserved system topic validation", () => {
  it("rejects reserved health topic names at runtime", () => {
    const reservedTopicName: string = VIEW_SERVER_HEALTH_SUMMARY_TOPIC;
    expect(() =>
      defineViewServerConfig({
        topics: {
          [reservedTopicName]: {
            schema: Order,
            key: "id",
          },
        },
      }),
    ).toThrow("View Server topic name is reserved for system health streams");
  });

  it("rejects reserved row field names at runtime", () => {
    const reservedFieldName = "__proto__";
    const BadRow = Schema.Struct({
      id: Schema.String,
      [reservedFieldName]: Schema.String,
    });

    expect(() =>
      defineViewServerConfig({
        topics: {
          badRows: {
            schema: BadRow,
            key: "id",
          },
        },
      }),
    ).toThrow("uses a reserved row field name: __proto__");
  });
});
