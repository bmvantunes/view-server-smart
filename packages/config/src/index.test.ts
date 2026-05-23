import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import type { GenMessage } from "@bufbuild/protobuf/codegenv2";
import type { Message } from "@bufbuild/protobuf";
import type { Effect } from "effect";
import { Config, Schema } from "effect";
import {
  defineProto,
  defineViewServerConfig,
  type KafkaMappingInput,
  type KafkaMessageMetadata,
  type KafkaTopicDefinition,
  type LiveSubscription,
  type LiveTransportAdapter,
  type ProtobufEsGeneratedMessageDescriptor,
  type ReactHookContracts,
  type SnapshotEvent,
  type StatusEvent,
  type ViewServerBackpressureError,
  type ViewServerHealth,
  type ViewServerInMemoryProviderOptions,
  type ViewServerInMemoryRuntime,
  type ViewServerRuntimeError,
  type ViewServerTransportError,
} from "./index.ts";
import { runtimeConfig, runtimeEnvironmentConfig } from "./runtime.ts";

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

type OrderValue = {
  readonly customerId: string;
  readonly status: "open" | "closed" | "cancelled";
  readonly price: number;
  readonly updatedAt: number;
};

type OrderKey = {
  readonly orderId: string;
};

type TradeValue = {
  readonly symbol: string;
  readonly quantity: number;
  readonly price: number;
};

type GeneratedOrderValue = Message<"viewserver.test.OrderValue"> & OrderValue;
type GeneratedOrderKey = Message<"viewserver.test.OrderKey"> & OrderKey;

const ordersValueProto = defineProto<OrderValue>();
const ordersKeyProto = defineProto<OrderKey>();
const tradesValueProto = defineProto<TradeValue>();

const ordersValueSchema: ProtobufEsGeneratedMessageDescriptor<OrderValue> = {
  typeName: "viewserver.test.OrderValue",
  fields: {},
  _viewServerProtoType: (value) => value,
};

const ordersKeySchema: ProtobufEsGeneratedMessageDescriptor<OrderKey> = {
  typeName: "viewserver.test.OrderKey",
  fields: {},
  _viewServerProtoType: (value) => value,
};

declare const generatedOrdersValueSchema: GenMessage<GeneratedOrderValue>;
declare const generatedOrdersKeySchema: GenMessage<GeneratedOrderKey>;

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
  },
});

const kafkaRegions = {
  usa: runtimeConfig.kafkaBootstrapServers("VIEW_SERVER_KAFKA_USA_BOOTSTRAP_SERVERS"),
  london: runtimeConfig.kafkaBootstrapServers("VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS"),
};

const kafkaTopic = viewServer.kafkaTopic<typeof kafkaRegions>();

type OrdersWithKeyMapping = KafkaMappingInput<
  typeof viewServer.topics,
  "orders",
  "usa" | "london",
  typeof ordersValueProto,
  typeof ordersKeyProto
>;

type TradesStringKeyMapping = KafkaMappingInput<
  typeof viewServer.topics,
  "trades",
  "usa",
  typeof tradesValueProto,
  undefined
>;

describe("defineViewServerConfig", () => {
  it("defines topics and pure runtime option contracts without starting a runtime", () => {
    const runtimeOptions = viewServer.defineRuntimeOptions({
      websocketPort: runtimeEnvironmentConfig.websocketPort,
      tcpPublishPort: runtimeConfig.port("VIEW_SERVER_TCP_PUBLISH_PORT"),
      kafka: {
        regions: kafkaRegions,
        topics: {
          orders: kafkaTopic({
            regions: ["usa", "london"],
            protoValue: ordersValueProto,
            protoKey: ordersKeyProto,
            viewServerTopic: "orders",
            mapping: ({ key, value, region }: OrdersWithKeyMapping) => ({
              id: key.orderId,
              customerId: value.customerId,
              status: value.status,
              price: value.price,
              region,
              updatedAt: value.updatedAt,
            }),
          }),
          trades: kafkaTopic({
            regions: ["usa"],
            protoValue: tradesValueProto,
            viewServerTopic: "trades",
            mapping: ({ key, value, region }: TradesStringKeyMapping) => ({
              id: key,
              symbol: value.symbol,
              quantity: value.quantity,
              price: value.price,
              region,
            }),
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

  it("does not expose executable React or runtime placeholders from config", () => {
    expect(Object.keys(viewServer)).toEqual(["topics", "defineRuntimeOptions", "kafkaTopic"]);
  });
});

describe("public type surface", () => {
  it("exposes health and transport contracts", () => {
    const snapshot: SnapshotEvent<{ readonly id: string }> = {
      type: "snapshot",
      topic: "orders",
      queryId: "query-1",
      version: 1,
      rows: [{ id: "order-1" }],
    };

    const metadata: KafkaMessageMetadata<"usa"> = {
      sourceTopic: "orders",
      sourceRegion: "usa",
      partition: 0,
      offset: "1",
      timestamp: null,
      headers: {},
    };

    const health: ViewServerHealth = {
      status: "ready",
      version: 1,
      uptimeMs: 100,
      engine: {
        topics: {
          orders: {
            status: "ready",
            topic: "orders",
            rowCount: 1,
            liveRowCount: 1,
            deletedRowCount: 0,
            version: 1,
            lastMutationAt: null,
            mutationsPerSecond: 0,
            rowsPerSecond: 0,
            pendingMutationBatches: 0,
            activeViews: 0,
            activeSubscriptions: 0,
            queuedEvents: 0,
            maxQueueDepth: 0,
            memoryBytes: 0,
            tombstoneCount: 0,
            compactionPending: false,
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

    const backpressure: StatusEvent = {
      type: "status",
      topic: "orders",
      queryId: "query-1",
      status: "error",
      code: "BackpressureExceeded",
      message: "client queue exceeded configured limits",
    };

    expect(snapshot.rows[0]?.id).toBe("order-1");
    expect(metadata.sourceRegion).toBe("usa");
    expect(health.engine.topics["orders"]?.rowCount).toBe(1);
    expect(backpressure.code).toBe("BackpressureExceeded");
    expectTypeOf<LiveTransportAdapter>().toHaveProperty("subscribe");
    expectTypeOf<Effect.Success<ReturnType<LiveTransportAdapter["subscribe"]>>>().toEqualTypeOf<
      LiveSubscription<unknown>
    >();
    expectTypeOf<
      Effect.Error<ReturnType<LiveTransportAdapter["subscribe"]>>
    >().toEqualTypeOf<ViewServerTransportError>();
  });

  it("derives query result rows from fields and grouped aggregates", () => {
    const assertQueryTypes = (react: ReactHookContracts<typeof viewServer.topics>) => {
      const rawRows = react.useLiveQuery("orders", {
        fields: ["id", "price"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 50,
      }).rows;

      const groupedRows = react.useLiveQuery("orders", {
        groupBy: ["status"],
        aggregates: [
          { type: "count", as: "count" },
          { type: "sum", field: "price", as: "totalPrice" },
          { type: "avg", field: "updatedAt", as: "averageUpdatedAt" },
          { type: "min", field: "status", as: "firstStatus" },
        ],
        where: {
          region: "london",
        },
      }).rows;

      expectTypeOf(rawRows).toEqualTypeOf<
        ReadonlyArray<{ readonly id: string; readonly price: number }>
      >();
      type GroupedRow = (typeof groupedRows)[number];
      expectTypeOf<GroupedRow>().toMatchTypeOf<{
        readonly status: "open" | "closed" | "cancelled";
        readonly count: number;
        readonly totalPrice: number;
        readonly averageUpdatedAt: number;
        readonly firstStatus: "open" | "closed" | "cancelled";
      }>();
    };

    expect(assertQueryTypes).toBeTypeOf("function");
  });

  it("infers unannotated Kafka mapping callback parameters through the topic helper", () => {
    const topic = kafkaTopic({
      regions: ["usa", "london"],
      protoValue: ordersValueSchema,
      viewServerTopic: "orders",
      mapping: ({ key, value, region }) => {
        expectTypeOf(key).toEqualTypeOf<string>();
        expectTypeOf(value).toEqualTypeOf<OrderValue>();
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

    const keyedTopic = kafkaTopic({
      regions: ["usa"],
      protoValue: ordersValueSchema,
      protoKey: ordersKeySchema,
      viewServerTopic: "orders",
      mapping: ({ key, value, region }) => {
        expectTypeOf(key).toEqualTypeOf<OrderKey>();
        expectTypeOf(value).toEqualTypeOf<OrderValue>();
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

    expect(keyedTopic.protoKey).toBe(ordersKeySchema);
  });

  it("keeps real Protobuf-ES v2 generated schema inference typechecked", () => {
    expect(assertGeneratedSchemaContracts).toBeTypeOf("function");
  });
});

const assertGeneratedSchemaContracts = () => {
  const keyedTopic = kafkaTopic({
    regions: ["usa", "london"],
    protoValue: generatedOrdersValueSchema,
    protoKey: generatedOrdersKeySchema,
    viewServerTopic: "orders",
    mapping: ({ key, value, region }) => {
      expectTypeOf(key).toEqualTypeOf<GeneratedOrderKey>();
      expectTypeOf(value).toEqualTypeOf<GeneratedOrderValue>();
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

  expectTypeOf(keyedTopic.protoKey).toEqualTypeOf<GenMessage<GeneratedOrderKey>>();

  kafkaTopic({
    regions: ["usa", "london"],
    protoValue: generatedOrdersValueSchema,
    viewServerTopic: "orders",
    mapping: ({ key, value, region }) => {
      expectTypeOf(key).toEqualTypeOf<string>();
      expectTypeOf(value).toEqualTypeOf<GeneratedOrderValue>();
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

  expectTypeOf<OrdersWithKeyMapping["key"]>().toEqualTypeOf<OrderKey>();
  expectTypeOf<OrdersWithKeyMapping["value"]>().toEqualTypeOf<OrderValue>();
  expectTypeOf<OrdersWithKeyMapping["region"]>().toEqualTypeOf<"usa" | "london">();
  expectTypeOf<OrdersWithKeyMapping["schema"]>().toEqualTypeOf<typeof Order>();
  expectTypeOf<OrdersWithKeyMapping["metadata"]["sourceRegion"]>().toEqualTypeOf<
    "usa" | "london"
  >();
  expectTypeOf<TradesStringKeyMapping["key"]>().toEqualTypeOf<string>();
  expectTypeOf<TradesStringKeyMapping["value"]>().toEqualTypeOf<TradeValue>();
  expectTypeOf<TradesStringKeyMapping["region"]>().toEqualTypeOf<"usa">();
  expectTypeOf<TradesStringKeyMapping["schema"]>().toEqualTypeOf<typeof Trade>();

  const assertRuntimeContracts = (runtime: ViewServerInMemoryRuntime<typeof viewServer.topics>) => {
    const publishEffect = runtime.publish("orders", {
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    });
    const snapshotEffect = runtime.snapshot("orders", {
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

  const assertProviderContracts = (
    options: ViewServerInMemoryProviderOptions<typeof viewServer.topics>,
  ) => {
    type ProviderOrderSeedRow = NonNullable<NonNullable<typeof options.seed>["orders"]>[number];
    expectTypeOf<ProviderOrderSeedRow>().toEqualTypeOf<{
      readonly id: string;
      readonly customerId: string;
      readonly status: "open" | "closed" | "cancelled";
      readonly price: number;
      readonly region: string;
      readonly updatedAt: number;
    }>();
  };

  expectTypeOf(assertProviderContracts).toBeFunction();

  const validProviderOptions: ViewServerInMemoryProviderOptions<typeof viewServer.topics> = {
    seed: {
      orders: [
        {
          id: "order-1",
          customerId: "customer-1",
          status: "open",
          price: 42,
          region: "usa",
          updatedAt: 1,
        },
      ],
    },
  };

  expectTypeOf(validProviderOptions.runtime).toEqualTypeOf<
    ViewServerInMemoryRuntime<typeof viewServer.topics> | undefined
  >();

  const invalidProviderOptions: ViewServerInMemoryProviderOptions<typeof viewServer.topics> = {
    seed: {
      orders: [
        {
          id: "order-1",
          customerId: "customer-1",
          status: "open",
          price: 42,
          region: "usa",
          // @ts-expect-error seed rows must match the topic schema
          updatedAt: "not-a-number",
        },
      ],
    },
  };

  expectTypeOf(invalidProviderOptions).toMatchTypeOf<
    ViewServerInMemoryProviderOptions<typeof viewServer.topics>
  >();

  defineViewServerConfig({
    topics: {
      invalid: {
        schema: Order,
        // @ts-expect-error topic keys must be string fields from the Effect Schema row type
        key: "missing",
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

  localKafkaTopic({
    // @ts-expect-error Kafka topic regions are constrained to kafka.regions keys
    regions: ["USA"],
    protoValue: ordersValueProto,
    viewServerTopic: "orders",
    mapping: ({ key, value }) => ({
      id: key,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region: "usa",
      updatedAt: value.updatedAt,
    }),
  });

  localKafkaTopic({
    // @ts-expect-error Kafka topic regions must be non-empty
    regions: [],
    protoValue: ordersValueProto,
    viewServerTopic: "orders",
    mapping: ({ key, value }) => ({
      id: key,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region: "usa",
      updatedAt: value.updatedAt,
    }),
  });

  localKafkaTopic({
    regions: ["usa"],
    protoValue: ordersValueProto,
    // @ts-expect-error Kafka mappings must target a configured View Server topic
    viewServerTopic: "customers",
    mapping: ({ key, value, region }) => ({
      id: key,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region,
      updatedAt: value.updatedAt,
    }),
  });

  const invalidExtraKafkaTopicField: KafkaTopicDefinition<
    typeof viewServer.topics,
    typeof localKafkaRegions,
    "orders",
    typeof ordersValueProto,
    undefined,
    readonly ["usa"]
  > = {
    regions: ["usa"],
    protoValue: ordersValueProto,
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
    protoValue: ordersValueProto,
    // @ts-expect-error unsupported proto key descriptors must fail instead of inferring unknown
    protoKey: {},
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
    protoValue: ordersValueProto,
    protoKey: ordersKeyProto,
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
    protoValue: ordersValueProto,
    protoKey: ordersKeyProto,
    viewServerTopic: "orders",
    mapping: ({ key, value, schema, metadata }) => {
      expectTypeOf(key).toEqualTypeOf<OrderKey>();
      expectTypeOf(value).toEqualTypeOf<OrderValue>();
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
          protoValue: ordersValueProto,
          protoKey: ordersKeyProto,
          viewServerTopic: "orders",
          // @ts-expect-error mapping return must match the target View Server topic row type
          mapping: ({ key, value, region }: OrdersWithKeyMapping) => ({
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
          protoValue: ordersValueProto,
          protoKey: ordersKeyProto,
          viewServerTopic: "orders",
          // @ts-expect-error raw runtime topic mappings must return the target topic row
          mapping: ({ key, value, region }: OrdersWithKeyMapping) => ({
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

  const assertReactContracts = (react: ReactHookContracts<typeof viewServer.topics>) => {
    react.useLiveQuery("orders", {
      where: {
        // @ts-expect-error raw queries reject fields not present on the selected topic
        missing: "open",
      },
    });

    react.useLiveQuery("orders", {
      where: {
        // @ts-expect-error filter values must match the selected field type
        price: "not-a-number",
      },
    });

    react.useLiveQuery("orders", {
      where: {
        status: {
          // @ts-expect-error filter arrays must contain selected field values
          in: ["open", "pending"],
        },
      },
    });

    react.useLiveQuery("orders", {
      orderBy: [
        {
          // @ts-expect-error orderBy fields are constrained to the selected topic row
          field: "missing",
          direction: "asc",
        },
      ],
    });

    react.useLiveQuery("orders", {
      orderBy: [
        {
          field: "price",
          // @ts-expect-error sort direction is constrained to asc or desc
          direction: "ascending",
        },
      ],
    });

    react.useLiveQuery("orders", {
      // @ts-expect-error projected fields are constrained to the selected topic row
      fields: ["id", "missing"],
    });

    react.useLiveQuery("orders", {
      // @ts-expect-error grouped queries reject groupBy fields not present on the topic row
      groupBy: ["missing"],
      aggregates: [{ type: "count", as: "count" }],
    });

    react.useLiveQuery("orders", {
      groupBy: ["status"],
      aggregates: [
        {
          type: "sum",
          // @ts-expect-error sum and avg aggregate fields must be numeric
          field: "status",
          as: "badTotal",
        },
      ],
    });

    react.useLiveQuery("orders", {
      groupBy: ["status"],
      aggregates: [
        {
          type: "avg",
          // @ts-expect-error avg aggregate fields must be numeric
          field: "status",
          as: "badAverage",
        },
      ],
    });

    expectTypeOf(react.useViewServerTestRuntime()).toHaveProperty("publish");
  };

  expectTypeOf(assertReactContracts).toBeFunction();

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
          protoValue: ordersValueProto,
          protoKey: ordersKeyProto,
          viewServerTopic: "orders",
          mapping: ({ key, value, region }: OrdersWithKeyMapping) => {
            expectTypeOf(key).toEqualTypeOf<OrderKey>();
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
          protoValue: tradesValueProto,
          viewServerTopic: "trades",
          mapping: ({ key, value, region }: TradesStringKeyMapping) => {
            expectTypeOf(key).toEqualTypeOf<string>();
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

  localKafkaTopic({
    regions: ["usa"],
    // @ts-expect-error unsupported proto descriptors must fail instead of inferring unknown
    protoValue: {},
    viewServerTopic: "orders",
    mapping: ({ key }) => ({
      id: key,
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    }),
  });

  localKafkaTopic({
    regions: ["usa"],
    // @ts-expect-error $typeName-only objects are message instances, not generated schemas
    protoValue: { $typeName: "viewserver.test.OrderValue" },
    viewServerTopic: "orders",
    mapping: ({ key }) => ({
      id: key,
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    }),
  });

  localKafkaTopic({
    regions: ["usa"],
    // @ts-expect-error arbitrary decoder shapes are not accepted as generated proto schemas
    protoValue: {
      fromBinary: (_bytes: Uint8Array) => ({
        customerId: "customer-1",
        status: "open",
        price: 42,
        updatedAt: 1,
      }),
    },
    viewServerTopic: "orders",
    mapping: ({ key }) => ({
      id: key,
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    }),
  });

  localKafkaTopic({
    regions: ["usa"],
    // @ts-expect-error row Effect schemas are not Kafka proto descriptors
    protoValue: Order,
    viewServerTopic: "orders",
    mapping: ({ key }) => ({
      id: key,
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    }),
  });
};

describe("compile-time contract assertions", () => {
  it("keeps negative type tests typechecked without executing placeholders", () => {
    expect(assertCompileTimeContracts).toBeTypeOf("function");
  });
});
