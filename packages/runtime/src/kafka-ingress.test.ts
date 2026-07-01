import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import { create, toBinary } from "@bufbuild/protobuf";
import {
  Admin,
  Producer,
  stringSerializers,
  type Message as KafkaMessage,
} from "@platformatic/kafka";
import { defineViewServerConfig, kafka } from "@effect-view-server/config";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Buffer } from "node:buffer";
import { Crypto, Effect, Option, Schedule, Schema } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import { makeViewServerRuntime } from "./index";
import { makeViewServerKafkaHealthLedger } from "./kafka-health";
import { processKafkaMessage, ViewServerKafkaIngressError } from "./kafka-ingress";
import { resolveViewServerRuntimeOptions } from "./runtime-options";
import {
  type OrderKey,
  OrderKeySchema,
  type OrderValue,
  OrderValueSchema,
} from "./test-fixtures/runtime_orders_pb";

const kafkaBootstrapServers =
  process.env["VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS"] ?? "localhost:9092";
const londonKafkaBootstrapServers =
  process.env["VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS"] ?? "localhost:9094";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  price: Schema.Number,
});

const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  quantity: Schema.Number,
});

const IncomingOrder = Schema.Struct({
  customerId: Schema.String,
  price: Schema.Number,
});

const IncomingTrade = Schema.Struct({
  symbol: Schema.String,
  quantity: Schema.Number,
});

const PrecisePosition = Schema.Struct({
  id: Schema.String,
  accountId: Schema.String,
  quantity: Schema.BigInt,
  price: Schema.BigDecimal,
});

const IncomingPrecisePosition = Schema.Struct({
  accountId: Schema.String,
  quantity: Schema.BigInt,
  price: Schema.BigDecimal,
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
  },
});

const preciseViewServer = defineViewServerConfig({
  topics: {
    positions: {
      schema: PrecisePosition,
      key: "id",
    },
  },
});

type ProducerMessage = {
  readonly topic: string;
  readonly key: string;
  readonly value: string;
};

type BinaryProducerMessage = {
  readonly topic: string;
  readonly key: Buffer;
  readonly value: Buffer;
};

const nullRecord = <Value>(entries: Record<string, Value>): Record<string, Value> => {
  const record: Record<string, Value> = Object.create(null);
  return Object.assign(record, entries);
};

const kafkaProcessorMessage = (input: {
  readonly key: string;
  readonly offset?: bigint;
  readonly topic: string;
  readonly value: string;
}): KafkaMessage<Buffer, Buffer, Buffer, Buffer> => ({
  commit: () => undefined,
  headers: new Map(),
  key: Buffer.from(input.key),
  metadata: {},
  offset: input.offset ?? 0n,
  partition: 0,
  timestamp: 0n,
  toJSON: () => ({
    headers: [],
    key: Buffer.from(input.key),
    metadata: {},
    offset: String(input.offset ?? 0n),
    partition: 0,
    timestamp: "0",
    topic: input.topic,
    value: Buffer.from(input.value),
  }),
  topic: input.topic,
  value: Buffer.from(input.value),
});

const uniqueTopicName = Effect.fn("ViewServerRuntime.kafka.test.topicName")(function* (
  prefix: string,
) {
  const crypto = yield* Crypto.Crypto;
  const uuid = yield* crypto.randomUUIDv7;
  return `view-server-${prefix}-${uuid.replaceAll("-", "")}`;
});

const uniqueGroupId = Effect.fn("ViewServerRuntime.kafka.test.groupId")(function* () {
  const crypto = yield* Crypto.Crypto;
  const uuid = yield* crypto.randomUUIDv7;
  return `view-server-test-${uuid.replaceAll("-", "")}`;
});

const sendKafkaMessages = Effect.fn("ViewServerRuntime.kafka.test.produce")(function* (
  bootstrapServers: string,
  clientId: string,
  messages: ReadonlyArray<ProducerMessage>,
) {
  const producer = new Producer<string, string, string, string>({
    bootstrapBrokers: [bootstrapServers],
    clientId,
    serializers: stringSerializers,
  });

  return yield* Effect.acquireUseRelease(
    Effect.succeed(producer),
    (currentProducer) =>
      Effect.promise(() =>
        currentProducer.send({
          messages: [...messages],
        }),
      ),
    (currentProducer) => Effect.promise(() => currentProducer.close()).pipe(Effect.ignore),
  );
});

const sendBinaryKafkaMessages = Effect.fn("ViewServerRuntime.kafka.test.produceBinary")(function* (
  bootstrapServers: string,
  clientId: string,
  messages: ReadonlyArray<BinaryProducerMessage>,
) {
  const producer = new Producer<Buffer, Buffer, Buffer, Buffer>({
    bootstrapBrokers: [bootstrapServers],
    clientId,
  });

  return yield* Effect.acquireUseRelease(
    Effect.succeed(producer),
    (currentProducer) =>
      Effect.promise(() =>
        currentProducer.send({
          messages: [...messages],
        }),
      ),
    (currentProducer) => Effect.promise(() => currentProducer.close()),
  );
});

const createKafkaTopics = Effect.fn("ViewServerRuntime.kafka.test.createTopics")(function* (
  bootstrapServers: string,
  topics: ReadonlyArray<string>,
) {
  const admin = new Admin({
    bootstrapBrokers: [bootstrapServers],
    clientId: "view-server-kafka-ingress-test-admin",
  });

  return yield* Effect.acquireUseRelease(
    Effect.succeed(admin),
    (currentAdmin) =>
      Effect.promise(() =>
        currentAdmin.createTopics({
          partitions: 1,
          replicas: 1,
          topics: [...topics],
        }),
      ),
    (currentAdmin) => Effect.promise(() => currentAdmin.close()).pipe(Effect.ignore),
  );
});

const healthPollSchedule = Schedule.addDelay(Schedule.recurs(100), () =>
  Effect.succeed("25 millis"),
);
const kafkaRestartPollSchedule = Schedule.addDelay(Schedule.recurs(400), () =>
  Effect.succeed("25 millis"),
);

describe("@effect-view-server/runtime Kafka ingress", () => {
  it.effect("rejects topic-owned Kafka source messages for missing View Server topics", () =>
    Effect.gen(function* () {
      const regions = {
        local: kafkaBootstrapServers,
      };
      const corruptedViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
          },
          ghost: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "ghost-source",
              regions: ["local"],
              value: kafka.json(IncomingOrder),
              key: kafka.stringKey(),
              map: ({ value, rowKey }) => ({
                id: rowKey,
                customerId: value.customerId,
                price: value.price,
              }),
            }),
          },
        },
      });
      const resolved = yield* resolveViewServerRuntimeOptions(corruptedViewServer, {
        kafka: {
          consumerGroupId: "view-server-missing-topic-guard",
        },
      });
      const kafkaOptions = Option.getOrThrow(Option.fromNullishOr(resolved.kafkaOptions));
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(corruptedViewServer, {});
      const health = makeViewServerKafkaHealthLedger<typeof corruptedViewServer.topics>({
        regions: kafkaOptions.regions,
        startFrom: kafkaOptions.consume,
        topics: {
          "ghost-source": {
            regions: ["local"],
            viewServerTopic: "ghost",
          },
        },
      });

      Reflect.deleteProperty(corruptedViewServer.topics, "ghost");
      const error = yield* Effect.flip(
        processKafkaMessage(
          corruptedViewServer,
          runtimeCore.internalClient,
          runtimeCore.requestHealthRefresh,
          kafkaOptions,
          health,
          "local",
          kafkaProcessorMessage({
            key: "ghost-1",
            topic: "ghost-source",
            value: JSON.stringify({
              customerId: "customer-1",
              price: 10,
            }),
          }),
        ),
      );
      yield* runtimeCore.close;

      expect(error).toStrictEqual(
        new ViewServerKafkaIngressError({
          message: "Kafka source references unknown View Server topic: ghost",
          cause: "missing-view-server-topic",
          region: "local",
          sourceTopic: "ghost-source",
        }),
      );
    }),
  );

  it.effect("decodes topic-owned Kafka source messages into runtime rows", () =>
    Effect.gen(function* () {
      const regions = {
        local: kafkaBootstrapServers,
      };
      const topicOwnedViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "orders-source",
              regions: ["local"],
              value: kafka.json(IncomingOrder),
              key: kafka.stringKey(),
              map: ({ value, rowKey }) => ({
                id: rowKey,
                customerId: value.customerId,
                price: value.price,
              }),
            }),
          },
        },
      });
      const resolved = yield* resolveViewServerRuntimeOptions(topicOwnedViewServer, {
        kafka: {
          consumerGroupId: "view-server-topic-owned-decode",
        },
      });
      const kafkaOptions = Option.getOrThrow(Option.fromNullishOr(resolved.kafkaOptions));
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(topicOwnedViewServer, {});
      const health = makeViewServerKafkaHealthLedger<typeof topicOwnedViewServer.topics>({
        regions: kafkaOptions.regions,
        startFrom: kafkaOptions.consume,
        topics: {
          "orders-source": {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });

      yield* processKafkaMessage(
        topicOwnedViewServer,
        runtimeCore.internalClient,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        health,
        "local",
        kafkaProcessorMessage({
          key: "order-1",
          topic: "orders-source",
          value: JSON.stringify({
            customerId: "customer-1",
            price: 10,
          }),
        }),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        limit: 10,
      });
      yield* runtimeCore.close;

      expect(snapshot).toStrictEqual({
        version: 1,
        rows: [
          {
            id: "order-1",
            customerId: "customer-1",
            price: 10,
          },
        ],
        totalRows: 1,
        status: "ready",
        statusCode: "Ready",
      });
    }),
  );

  it.effect("uses Kafka source row keys as source-owned storage keys", () =>
    Effect.gen(function* () {
      const regions = {
        local: kafkaBootstrapServers,
      };
      const topicOwnedViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "orders-source",
              regions: ["local"],
              value: kafka.json(IncomingOrder),
              key: kafka.stringKey(),
              map: ({ value, rowKey }) => ({
                id: `${rowKey}-${value.customerId}`,
                customerId: value.customerId,
                price: value.price,
              }),
            }),
          },
        },
      });
      const resolved = yield* resolveViewServerRuntimeOptions(topicOwnedViewServer, {
        kafka: {
          consumerGroupId: "view-server-topic-owned-storage-key",
        },
      });
      const kafkaOptions = Option.getOrThrow(Option.fromNullishOr(resolved.kafkaOptions));
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(topicOwnedViewServer, {});
      const health = makeViewServerKafkaHealthLedger<typeof topicOwnedViewServer.topics>({
        regions: kafkaOptions.regions,
        startFrom: kafkaOptions.consume,
        topics: {
          "orders-source": {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });

      yield* processKafkaMessage(
        topicOwnedViewServer,
        runtimeCore.internalClient,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        health,
        "local",
        kafkaProcessorMessage({
          key: "order-1",
          topic: "orders-source",
          value: JSON.stringify({
            customerId: "customer-1",
            price: 10,
          }),
        }),
      );
      yield* processKafkaMessage(
        topicOwnedViewServer,
        runtimeCore.internalClient,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        health,
        "local",
        kafkaProcessorMessage({
          key: "order-1",
          topic: "orders-source",
          value: JSON.stringify({
            customerId: "customer-2",
            price: 20,
          }),
        }),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        limit: 10,
      });
      yield* runtimeCore.close;

      expect(snapshot).toStrictEqual({
        version: 2,
        rows: [
          {
            id: "order-1-customer-2",
            customerId: "customer-2",
            price: 20,
          },
        ],
        totalRows: 1,
        status: "ready",
        statusCode: "Ready",
      });
    }),
  );

  it.live(
    "ingests isolated Kafka topics into independent View Server topics and reports health",
    () =>
      Effect.gen(function* () {
        const ordersSourceTopic = yield* uniqueTopicName("orders");
        const tradesSourceTopic = yield* uniqueTopicName("trades");
        const consumerGroupId = yield* uniqueGroupId();
        yield* createKafkaTopics(kafkaBootstrapServers, [ordersSourceTopic, tradesSourceTopic]);

        const regions = {
          local: kafkaBootstrapServers,
        };
        const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();

        yield* Effect.acquireUseRelease(
          makeViewServerRuntime(viewServer, {
            host: "127.0.0.1",
            websocketPort: 0,
            kafka: {
              consumerGroupId,
              regions,
              topics: {
                [ordersSourceTopic]: localKafkaTopic({
                  regions: ["local"],
                  value: kafka.json(IncomingOrder),
                  key: kafka.stringKey(),
                  viewServerTopic: "orders",
                  mapping: ({ key, value }) => ({
                    id: key,
                    customerId: value.customerId,
                    price: value.price,
                  }),
                }),
                [tradesSourceTopic]: localKafkaTopic({
                  regions: ["local"],
                  value: kafka.json(IncomingTrade),
                  key: kafka.stringKey(),
                  viewServerTopic: "trades",
                  mapping: ({ key, value }) => ({
                    id: key,
                    symbol: value.symbol,
                    quantity: value.quantity,
                  }),
                }),
              },
            },
          }),
          (runtime) =>
            Effect.gen(function* () {
              yield* sendKafkaMessages(kafkaBootstrapServers, "view-server-kafka-ingress-test", [
                {
                  topic: ordersSourceTopic,
                  key: "order-1",
                  value: JSON.stringify({
                    customerId: "customer-1",
                    price: 10,
                  }),
                },
                {
                  topic: tradesSourceTopic,
                  key: "trade-1",
                  value: JSON.stringify({
                    symbol: "AAPL",
                    quantity: 100,
                  }),
                },
                {
                  topic: ordersSourceTopic,
                  key: "order-2",
                  value: JSON.stringify({
                    customerId: "customer-2",
                    price: 20,
                  }),
                },
              ]);

              const ordersSnapshot = yield* runtime.client
                .snapshot("orders", {
                  select: ["id", "customerId", "price"],
                  orderBy: [{ field: "id", direction: "asc" }],
                  limit: 10,
                })
                .pipe(
                  Effect.repeat({
                    schedule: healthPollSchedule,
                    until: (snapshot) => snapshot.totalRows === 2,
                  }),
                );
              const tradesSnapshot = yield* runtime.client
                .snapshot("trades", {
                  select: ["id", "symbol", "quantity"],
                  orderBy: [{ field: "id", direction: "asc" }],
                  limit: 10,
                })
                .pipe(
                  Effect.repeat({
                    schedule: healthPollSchedule,
                    until: (snapshot) => snapshot.totalRows === 1,
                  }),
                );
              const health = yield* runtime.client.health().pipe(
                Effect.repeat({
                  schedule: healthPollSchedule,
                  until: (currentHealth) =>
                    currentHealth.engine.topics.orders.rowCount === 2 &&
                    currentHealth.engine.topics.trades.rowCount === 1 &&
                    currentHealth.kafka?.topics[ordersSourceTopic]?.status === "ready" &&
                    currentHealth.kafka?.topics[tradesSourceTopic]?.status === "ready" &&
                    currentHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]
                      ?.assignedPartitions === 1 &&
                    currentHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]
                      ?.committedOffset === "2" &&
                    currentHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]
                      ?.consumerLagMessages === 0n &&
                    currentHealth.kafka?.topics[tradesSourceTopic]?.regions["local"]
                      ?.assignedPartitions === 1 &&
                    currentHealth.kafka?.topics[tradesSourceTopic]?.regions["local"]
                      ?.committedOffset === "1" &&
                    currentHealth.kafka?.topics[tradesSourceTopic]?.regions["local"]
                      ?.consumerLagMessages === 0n,
                }),
              );

              expect({
                status: health.status,
                ordersSnapshot,
                tradesSnapshot,
                engineRows: {
                  orders: health.engine.topics.orders.rowCount,
                  trades: health.engine.topics.trades.rowCount,
                },
                kafka: health.kafka,
              }).toStrictEqual({
                status: "ready",
                ordersSnapshot: {
                  status: "ready",
                  statusCode: "Ready",
                  rows: [
                    {
                      id: "order-1",
                      customerId: "customer-1",
                      price: 10,
                    },
                    {
                      id: "order-2",
                      customerId: "customer-2",
                      price: 20,
                    },
                  ],
                  totalRows: 2,
                  version: expect.any(Number),
                },
                tradesSnapshot: {
                  status: "ready",
                  statusCode: "Ready",
                  rows: [
                    {
                      id: "trade-1",
                      symbol: "AAPL",
                      quantity: 100,
                    },
                  ],
                  totalRows: 1,
                  version: expect.any(Number),
                },
                engineRows: {
                  orders: 2,
                  trades: 1,
                },
                kafka: {
                  startFrom: {
                    consumerGroupId,
                    fallbackMode: "earliest",
                    mode: "committed",
                  },
                  regions: nullRecord({
                    local: {
                      status: "connected",
                      brokers: kafkaBootstrapServers,
                      lastConnectedAt: expect.any(Number),
                      lastError: null,
                    },
                  }),
                  topics: nullRecord({
                    [ordersSourceTopic]: {
                      status: "ready",
                      sourceTopic: ordersSourceTopic,
                      viewServerTopic: "orders",
                      regions: nullRecord({
                        local: {
                          connected: true,
                          assignedPartitions: 1,
                          messagesPerSecond: expect.any(Number),
                          bytesPerSecond: expect.any(Number),
                          decodedMessagesPerSecond: expect.any(Number),
                          decodeFailuresPerSecond: 0,
                          mappingFailuresPerSecond: 0,
                          publishFailuresPerSecond: 0,
                          commitFailuresPerSecond: 0,
                          processingFailuresPerSecond: 0,
                          lastMessageAt: expect.any(Number),
                          lastCommitAt: expect.any(Number),
                          consumerLagMessages: 0n,
                          lagSampledAt: expect.any(Number),
                          committedOffset: "2",
                          lastError: null,
                        },
                      }),
                    },
                    [tradesSourceTopic]: {
                      status: "ready",
                      sourceTopic: tradesSourceTopic,
                      viewServerTopic: "trades",
                      regions: nullRecord({
                        local: {
                          connected: true,
                          assignedPartitions: 1,
                          messagesPerSecond: expect.any(Number),
                          bytesPerSecond: expect.any(Number),
                          decodedMessagesPerSecond: expect.any(Number),
                          decodeFailuresPerSecond: 0,
                          mappingFailuresPerSecond: 0,
                          publishFailuresPerSecond: 0,
                          commitFailuresPerSecond: 0,
                          processingFailuresPerSecond: 0,
                          lastMessageAt: expect.any(Number),
                          lastCommitAt: expect.any(Number),
                          consumerLagMessages: 0n,
                          lagSampledAt: expect.any(Number),
                          committedOffset: "1",
                          lastError: null,
                        },
                      }),
                    },
                  }),
                },
              });
            }),
          (runtime) => runtime.close.pipe(Effect.ignore),
        );
      }).pipe(Effect.provide(NodeCrypto.layer)),
  );

  it.live("ingests multiple Kafka topics across logical regions independently", () =>
    Effect.gen(function* () {
      const regionalOrdersSourceTopic = yield* uniqueTopicName("regional-orders");
      const usaTradesSourceTopic = yield* uniqueTopicName("usa-trades");
      const consumerGroupId = yield* uniqueGroupId();
      yield* createKafkaTopics(kafkaBootstrapServers, [
        regionalOrdersSourceTopic,
        usaTradesSourceTopic,
      ]);
      yield* createKafkaTopics(londonKafkaBootstrapServers, [regionalOrdersSourceTopic]);

      const regions = {
        usa: kafkaBootstrapServers,
        london: londonKafkaBootstrapServers,
      };
      const regionalKafkaTopic = viewServer.kafkaTopic<typeof regions>();

      yield* Effect.acquireUseRelease(
        makeViewServerRuntime(viewServer, {
          host: "127.0.0.1",
          websocketPort: 0,
          kafka: {
            consumerGroupId,
            regions,
            topics: {
              [regionalOrdersSourceTopic]: regionalKafkaTopic({
                regions: ["usa", "london"],
                value: kafka.json(IncomingOrder),
                key: kafka.stringKey(),
                viewServerTopic: "orders",
                mapping: ({ key, value, region }) => {
                  expectTypeOf(region).toEqualTypeOf<"usa" | "london">();
                  return {
                    id: `${region}:${key}`,
                    customerId: `${region}:${value.customerId}`,
                    price: value.price,
                  };
                },
              }),
              [usaTradesSourceTopic]: regionalKafkaTopic({
                regions: ["usa"],
                value: kafka.json(IncomingTrade),
                key: kafka.stringKey(),
                viewServerTopic: "trades",
                mapping: ({ key, value, region }) => {
                  expectTypeOf(region).toEqualTypeOf<"usa">();
                  return {
                    id: key,
                    symbol: value.symbol,
                    quantity: value.quantity,
                  };
                },
              }),
            },
          },
        }),
        (runtime) =>
          Effect.gen(function* () {
            yield* sendKafkaMessages(kafkaBootstrapServers, "view-server-kafka-usa-ingress-test", [
              {
                topic: regionalOrdersSourceTopic,
                key: "regional-order-1",
                value: JSON.stringify({
                  customerId: "usa-customer-1",
                  price: 10,
                }),
              },
              {
                topic: usaTradesSourceTopic,
                key: "usa-trade-1",
                value: JSON.stringify({
                  symbol: "AAPL",
                  quantity: 100,
                }),
              },
              {
                topic: regionalOrdersSourceTopic,
                key: "regional-order-2",
                value: JSON.stringify({
                  customerId: "usa-customer-2",
                  price: 30,
                }),
              },
            ]);
            yield* sendKafkaMessages(
              londonKafkaBootstrapServers,
              "view-server-kafka-london-ingress-test",
              [
                {
                  topic: regionalOrdersSourceTopic,
                  key: "regional-order-1",
                  value: JSON.stringify({
                    customerId: "london-customer-1",
                    price: 20,
                  }),
                },
              ],
            );

            const ordersSnapshot = yield* runtime.client
              .snapshot("orders", {
                select: ["id", "customerId", "price"],
                orderBy: [{ field: "id", direction: "asc" }],
                limit: 10,
              })
              .pipe(
                Effect.repeat({
                  schedule: kafkaRestartPollSchedule,
                  until: (snapshot) => snapshot.totalRows === 3,
                }),
              );
            const tradesSnapshot = yield* runtime.client
              .snapshot("trades", {
                select: ["id", "symbol", "quantity"],
                orderBy: [{ field: "id", direction: "asc" }],
                limit: 10,
              })
              .pipe(
                Effect.repeat({
                  schedule: kafkaRestartPollSchedule,
                  until: (snapshot) => snapshot.totalRows === 1,
                }),
              );
            const health = yield* runtime.client.health().pipe(
              Effect.repeat({
                schedule: kafkaRestartPollSchedule,
                until: (currentHealth) =>
                  currentHealth.engine.topics.orders.rowCount === 3 &&
                  currentHealth.engine.topics.trades.rowCount === 1 &&
                  currentHealth.kafka?.regions["usa"]?.status === "connected" &&
                  currentHealth.kafka?.regions["london"]?.status === "connected" &&
                  currentHealth.kafka?.topics[regionalOrdersSourceTopic]?.status === "ready" &&
                  currentHealth.kafka?.topics[regionalOrdersSourceTopic]?.regions["usa"]
                    ?.assignedPartitions === 1 &&
                  currentHealth.kafka?.topics[regionalOrdersSourceTopic]?.regions["usa"]
                    ?.committedOffset === "2" &&
                  currentHealth.kafka?.topics[regionalOrdersSourceTopic]?.regions["usa"]
                    ?.consumerLagMessages === 0n &&
                  currentHealth.kafka?.topics[regionalOrdersSourceTopic]?.regions["london"]
                    ?.assignedPartitions === 1 &&
                  currentHealth.kafka?.topics[regionalOrdersSourceTopic]?.regions["london"]
                    ?.committedOffset === "1" &&
                  currentHealth.kafka?.topics[regionalOrdersSourceTopic]?.regions["london"]
                    ?.consumerLagMessages === 0n &&
                  currentHealth.kafka?.topics[usaTradesSourceTopic]?.status === "ready" &&
                  currentHealth.kafka?.topics[usaTradesSourceTopic]?.regions["usa"]
                    ?.assignedPartitions === 1 &&
                  currentHealth.kafka?.topics[usaTradesSourceTopic]?.regions["usa"]
                    ?.committedOffset === "1" &&
                  currentHealth.kafka?.topics[usaTradesSourceTopic]?.regions["usa"]
                    ?.consumerLagMessages === 0n,
              }),
            );

            expect({
              status: health.status,
              ordersSnapshot,
              tradesSnapshot,
              engineRows: {
                orders: health.engine.topics.orders.rowCount,
                trades: health.engine.topics.trades.rowCount,
              },
              kafka: health.kafka,
            }).toStrictEqual({
              status: "ready",
              ordersSnapshot: {
                status: "ready",
                statusCode: "Ready",
                rows: [
                  {
                    id: "london:regional-order-1",
                    customerId: "london:london-customer-1",
                    price: 20,
                  },
                  {
                    id: "usa:regional-order-1",
                    customerId: "usa:usa-customer-1",
                    price: 10,
                  },
                  {
                    id: "usa:regional-order-2",
                    customerId: "usa:usa-customer-2",
                    price: 30,
                  },
                ],
                totalRows: 3,
                version: expect.any(Number),
              },
              tradesSnapshot: {
                status: "ready",
                statusCode: "Ready",
                rows: [
                  {
                    id: "usa-trade-1",
                    symbol: "AAPL",
                    quantity: 100,
                  },
                ],
                totalRows: 1,
                version: expect.any(Number),
              },
              engineRows: {
                orders: 3,
                trades: 1,
              },
              kafka: {
                startFrom: {
                  consumerGroupId,
                  fallbackMode: "earliest",
                  mode: "committed",
                },
                regions: nullRecord({
                  london: {
                    status: "connected",
                    brokers: londonKafkaBootstrapServers,
                    lastConnectedAt: expect.any(Number),
                    lastError: null,
                  },
                  usa: {
                    status: "connected",
                    brokers: kafkaBootstrapServers,
                    lastConnectedAt: expect.any(Number),
                    lastError: null,
                  },
                }),
                topics: nullRecord({
                  [regionalOrdersSourceTopic]: {
                    status: "ready",
                    sourceTopic: regionalOrdersSourceTopic,
                    viewServerTopic: "orders",
                    regions: nullRecord({
                      london: {
                        connected: true,
                        assignedPartitions: 1,
                        messagesPerSecond: expect.any(Number),
                        bytesPerSecond: expect.any(Number),
                        decodedMessagesPerSecond: expect.any(Number),
                        decodeFailuresPerSecond: 0,
                        mappingFailuresPerSecond: 0,
                        publishFailuresPerSecond: 0,
                        commitFailuresPerSecond: 0,
                        processingFailuresPerSecond: 0,
                        lastMessageAt: expect.any(Number),
                        lastCommitAt: expect.any(Number),
                        consumerLagMessages: 0n,
                        lagSampledAt: expect.any(Number),
                        committedOffset: "1",
                        lastError: null,
                      },
                      usa: {
                        connected: true,
                        assignedPartitions: 1,
                        messagesPerSecond: expect.any(Number),
                        bytesPerSecond: expect.any(Number),
                        decodedMessagesPerSecond: expect.any(Number),
                        decodeFailuresPerSecond: 0,
                        mappingFailuresPerSecond: 0,
                        publishFailuresPerSecond: 0,
                        commitFailuresPerSecond: 0,
                        processingFailuresPerSecond: 0,
                        lastMessageAt: expect.any(Number),
                        lastCommitAt: expect.any(Number),
                        consumerLagMessages: 0n,
                        lagSampledAt: expect.any(Number),
                        committedOffset: "2",
                        lastError: null,
                      },
                    }),
                  },
                  [usaTradesSourceTopic]: {
                    status: "ready",
                    sourceTopic: usaTradesSourceTopic,
                    viewServerTopic: "trades",
                    regions: nullRecord({
                      usa: {
                        connected: true,
                        assignedPartitions: 1,
                        messagesPerSecond: expect.any(Number),
                        bytesPerSecond: expect.any(Number),
                        decodedMessagesPerSecond: expect.any(Number),
                        decodeFailuresPerSecond: 0,
                        mappingFailuresPerSecond: 0,
                        publishFailuresPerSecond: 0,
                        commitFailuresPerSecond: 0,
                        processingFailuresPerSecond: 0,
                        lastMessageAt: expect.any(Number),
                        lastCommitAt: expect.any(Number),
                        consumerLagMessages: 0n,
                        lagSampledAt: expect.any(Number),
                        committedOffset: "1",
                        lastError: null,
                      },
                    }),
                  },
                }),
              },
            });
          }),
        (runtime) => runtime.close.pipe(Effect.ignore),
      );
    }).pipe(Effect.provide(NodeCrypto.layer)),
  );

  it.live("preserves high-precision Kafka JSON values through real Kafka ingestion", () =>
    Effect.gen(function* () {
      const positionsSourceTopic = yield* uniqueTopicName("json-precise-positions");
      const consumerGroupId = yield* uniqueGroupId();
      yield* createKafkaTopics(kafkaBootstrapServers, [positionsSourceTopic]);

      const regions = {
        local: kafkaBootstrapServers,
      };
      const localKafkaTopic = preciseViewServer.kafkaTopic<typeof regions>();

      yield* Effect.acquireUseRelease(
        makeViewServerRuntime(preciseViewServer, {
          host: "127.0.0.1",
          websocketPort: 0,
          kafka: {
            consumerGroupId,
            regions,
            topics: {
              [positionsSourceTopic]: localKafkaTopic({
                regions: ["local"],
                value: kafka.json(IncomingPrecisePosition),
                key: kafka.stringKey(),
                viewServerTopic: "positions",
                mapping: ({ key, value, region }) => {
                  expectTypeOf(key).toEqualTypeOf<string>();
                  expectTypeOf(value).toEqualTypeOf<typeof IncomingPrecisePosition.Type>();
                  expectTypeOf(region).toEqualTypeOf<"local">();
                  expect(typeof value.quantity).toBe("bigint");
                  expect(value.quantity).toBe(9007199254740993n);
                  expect(BigDecimal.isBigDecimal(value.price)).toBe(true);
                  expect(BigDecimal.format(value.price)).toBe("1234567890.123456789");
                  return {
                    id: key,
                    accountId: value.accountId,
                    quantity: value.quantity,
                    price: value.price,
                  };
                },
              }),
            },
          },
        }),
        (runtime) =>
          Effect.gen(function* () {
            yield* sendKafkaMessages(
              kafkaBootstrapServers,
              "view-server-kafka-json-precision-ingress-test",
              [
                {
                  topic: positionsSourceTopic,
                  key: "position-precise-1",
                  value: JSON.stringify({
                    accountId: "account-precise-1",
                    quantity: "9007199254740993",
                    price: "1234567890.123456789",
                  }),
                },
              ],
            );

            const positionsSnapshot = yield* runtime.client
              .snapshot("positions", {
                select: ["id", "accountId", "quantity", "price"],
                orderBy: [{ field: "id", direction: "asc" }],
                limit: 10,
              })
              .pipe(
                Effect.repeat({
                  schedule: healthPollSchedule,
                  until: (snapshot) => snapshot.totalRows === 1,
                }),
              );
            const health = yield* runtime.client.health().pipe(
              Effect.repeat({
                schedule: healthPollSchedule,
                until: (currentHealth) =>
                  currentHealth.engine.topics.positions.rowCount === 1 &&
                  currentHealth.kafka?.topics[positionsSourceTopic]?.status === "ready" &&
                  currentHealth.kafka?.topics[positionsSourceTopic]?.regions["local"]
                    ?.committedOffset === "1" &&
                  currentHealth.kafka?.topics[positionsSourceTopic]?.regions["local"]
                    ?.consumerLagMessages === 0n,
              }),
            );

            expect({
              status: health.status,
              positionsSnapshot: {
                ...positionsSnapshot,
                rows: positionsSnapshot.rows.map((row) => ({
                  ...row,
                  price: BigDecimal.format(row.price),
                })),
              },
              engineRows: {
                positions: health.engine.topics.positions.rowCount,
              },
              kafka: health.kafka,
            }).toStrictEqual({
              status: "ready",
              positionsSnapshot: {
                status: "ready",
                statusCode: "Ready",
                rows: [
                  {
                    id: "position-precise-1",
                    accountId: "account-precise-1",
                    quantity: 9007199254740993n,
                    price: "1234567890.123456789",
                  },
                ],
                totalRows: 1,
                version: expect.any(Number),
              },
              engineRows: {
                positions: 1,
              },
              kafka: {
                startFrom: {
                  consumerGroupId,
                  fallbackMode: "earliest",
                  mode: "committed",
                },
                regions: nullRecord({
                  local: {
                    status: "connected",
                    brokers: kafkaBootstrapServers,
                    lastConnectedAt: expect.any(Number),
                    lastError: null,
                  },
                }),
                topics: nullRecord({
                  [positionsSourceTopic]: {
                    status: "ready",
                    sourceTopic: positionsSourceTopic,
                    viewServerTopic: "positions",
                    regions: nullRecord({
                      local: {
                        connected: true,
                        assignedPartitions: 1,
                        messagesPerSecond: expect.any(Number),
                        bytesPerSecond: expect.any(Number),
                        decodedMessagesPerSecond: expect.any(Number),
                        decodeFailuresPerSecond: 0,
                        mappingFailuresPerSecond: 0,
                        publishFailuresPerSecond: 0,
                        commitFailuresPerSecond: 0,
                        processingFailuresPerSecond: 0,
                        lastMessageAt: expect.any(Number),
                        lastCommitAt: expect.any(Number),
                        consumerLagMessages: 0n,
                        lagSampledAt: expect.any(Number),
                        committedOffset: "1",
                        lastError: null,
                      },
                    }),
                  },
                }),
              },
            });
          }),
        (runtime) => runtime.close.pipe(Effect.ignore),
      );
    }).pipe(Effect.provide(NodeCrypto.layer)),
  );

  it.live("ingests protobuf Kafka key and value messages into a View Server topic", () =>
    Effect.gen(function* () {
      const ordersSourceTopic = yield* uniqueTopicName("protobuf-orders");
      const consumerGroupId = yield* uniqueGroupId();
      yield* createKafkaTopics(kafkaBootstrapServers, [ordersSourceTopic]);

      const regions = {
        local: kafkaBootstrapServers,
      };
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();

      yield* Effect.acquireUseRelease(
        makeViewServerRuntime(viewServer, {
          host: "127.0.0.1",
          websocketPort: 0,
          kafka: {
            consumerGroupId,
            regions,
            topics: {
              [ordersSourceTopic]: localKafkaTopic({
                regions: ["local"],
                value: kafka.protobuf(OrderValueSchema),
                key: kafka.protobuf(OrderKeySchema),
                viewServerTopic: "orders",
                mapping: ({ key, value, region }) => {
                  expectTypeOf(key).toEqualTypeOf<OrderKey>();
                  expectTypeOf(value).toEqualTypeOf<OrderValue>();
                  expectTypeOf(region).toEqualTypeOf<"local">();
                  return {
                    id: key.orderId,
                    customerId: value.customerId,
                    price: value.price,
                  };
                },
              }),
            },
          },
        }),
        (runtime) =>
          Effect.gen(function* () {
            yield* sendBinaryKafkaMessages(
              kafkaBootstrapServers,
              "view-server-kafka-protobuf-ingress-test",
              [
                {
                  topic: ordersSourceTopic,
                  key: Buffer.from(
                    toBinary(
                      OrderKeySchema,
                      create(OrderKeySchema, {
                        orderId: "protobuf-order-1",
                      }),
                    ),
                  ),
                  value: Buffer.from(
                    toBinary(
                      OrderValueSchema,
                      create(OrderValueSchema, {
                        customerId: "protobuf-customer-1",
                        price: 42,
                      }),
                    ),
                  ),
                },
              ],
            );

            const ordersSnapshot = yield* runtime.client
              .snapshot("orders", {
                select: ["id", "customerId", "price"],
                orderBy: [{ field: "id", direction: "asc" }],
                limit: 10,
              })
              .pipe(
                Effect.repeat({
                  schedule: healthPollSchedule,
                  until: (snapshot) => snapshot.totalRows === 1,
                }),
              );
            const health = yield* runtime.client.health().pipe(
              Effect.repeat({
                schedule: healthPollSchedule,
                until: (currentHealth) =>
                  currentHealth.engine.topics.orders.rowCount === 1 &&
                  currentHealth.kafka?.topics[ordersSourceTopic]?.status === "ready" &&
                  currentHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]
                    ?.committedOffset === "1" &&
                  currentHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]
                    ?.consumerLagMessages === 0n,
              }),
            );

            expect({
              status: health.status,
              ordersSnapshot,
              engineRows: {
                orders: health.engine.topics.orders.rowCount,
                trades: health.engine.topics.trades.rowCount,
              },
              kafka: health.kafka,
            }).toStrictEqual({
              status: "ready",
              ordersSnapshot: {
                status: "ready",
                statusCode: "Ready",
                rows: [
                  {
                    id: "protobuf-order-1",
                    customerId: "protobuf-customer-1",
                    price: 42,
                  },
                ],
                totalRows: 1,
                version: expect.any(Number),
              },
              engineRows: {
                orders: 1,
                trades: 0,
              },
              kafka: {
                startFrom: {
                  consumerGroupId,
                  fallbackMode: "earliest",
                  mode: "committed",
                },
                regions: nullRecord({
                  local: {
                    status: "connected",
                    brokers: kafkaBootstrapServers,
                    lastConnectedAt: expect.any(Number),
                    lastError: null,
                  },
                }),
                topics: nullRecord({
                  [ordersSourceTopic]: {
                    status: "ready",
                    sourceTopic: ordersSourceTopic,
                    viewServerTopic: "orders",
                    regions: nullRecord({
                      local: {
                        connected: true,
                        assignedPartitions: 1,
                        messagesPerSecond: expect.any(Number),
                        bytesPerSecond: expect.any(Number),
                        decodedMessagesPerSecond: expect.any(Number),
                        decodeFailuresPerSecond: 0,
                        mappingFailuresPerSecond: 0,
                        publishFailuresPerSecond: 0,
                        commitFailuresPerSecond: 0,
                        processingFailuresPerSecond: 0,
                        lastMessageAt: expect.any(Number),
                        lastCommitAt: expect.any(Number),
                        consumerLagMessages: 0n,
                        lagSampledAt: expect.any(Number),
                        committedOffset: "1",
                        lastError: null,
                      },
                    }),
                  },
                }),
              },
            });
          }),
        (runtime) => runtime.close.pipe(Effect.ignore),
      );
    }).pipe(Effect.provide(NodeCrypto.layer)),
  );

  it.live("replays Kafka from earliest after restart and upserts duplicate row ids", () =>
    Effect.gen(function* () {
      const ordersSourceTopic = yield* uniqueTopicName("restart-orders");
      const consumerGroupId = yield* uniqueGroupId();
      yield* createKafkaTopics(kafkaBootstrapServers, [ordersSourceTopic]);

      const regions = {
        local: kafkaBootstrapServers,
      };
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const runtimeOptions = (consumerGroupId: string) => ({
        host: "127.0.0.1",
        websocketPort: 0,
        kafka: {
          consumerGroupId,
          startFrom: "earliest" as const,
          regions,
          topics: {
            [ordersSourceTopic]: localKafkaTopic({
              regions: ["local"],
              value: kafka.json(IncomingOrder),
              key: kafka.stringKey(),
              viewServerTopic: "orders",
              mapping: ({ key, value }) => ({
                id: key,
                customerId: value.customerId,
                price: value.price,
              }),
            }),
          },
        },
      });

      yield* sendKafkaMessages(kafkaBootstrapServers, "view-server-kafka-restart-test", [
        {
          topic: ordersSourceTopic,
          key: "order-0",
          value: JSON.stringify({
            customerId: "customer-0-earliest-sentinel",
            price: 1,
          }),
        },
        {
          topic: ordersSourceTopic,
          key: "order-1",
          value: JSON.stringify({
            customerId: "customer-1-initial",
            price: 10,
          }),
        },
        {
          topic: ordersSourceTopic,
          key: "order-1",
          value: JSON.stringify({
            customerId: "customer-1-replayed-update",
            price: 99,
          }),
        },
        {
          topic: ordersSourceTopic,
          key: "order-2",
          value: JSON.stringify({
            customerId: "customer-2",
            price: 20,
          }),
        },
      ]);

      const firstResult = yield* Effect.acquireUseRelease(
        makeViewServerRuntime(viewServer, runtimeOptions(consumerGroupId)),
        (runtime) =>
          Effect.gen(function* () {
            const firstSnapshot = yield* runtime.client
              .snapshot("orders", {
                select: ["id", "customerId", "price"],
                orderBy: [{ field: "id", direction: "asc" }],
                limit: 10,
              })
              .pipe(
                Effect.repeat({
                  schedule: kafkaRestartPollSchedule,
                  until: (snapshot) =>
                    snapshot.totalRows === 3 &&
                    snapshot.rows[0]?.customerId === "customer-0-earliest-sentinel" &&
                    snapshot.rows[1]?.customerId === "customer-1-replayed-update",
                }),
              );
            const firstHealth = yield* runtime.client.health().pipe(
              Effect.repeat({
                schedule: kafkaRestartPollSchedule,
                until: (currentHealth) =>
                  currentHealth.engine.topics.orders.rowCount === 3 &&
                  currentHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]
                    ?.committedOffset === "4" &&
                  currentHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]
                    ?.consumerLagMessages === 0n,
              }),
            );
            return {
              firstSnapshot,
              firstHealth,
            };
          }),
        (runtime) => runtime.close.pipe(Effect.ignore),
      );

      const replayResult = yield* Effect.acquireUseRelease(
        makeViewServerRuntime(viewServer, runtimeOptions(consumerGroupId)),
        (runtime) =>
          Effect.gen(function* () {
            const replaySnapshot = yield* runtime.client
              .snapshot("orders", {
                select: ["id", "customerId", "price"],
                orderBy: [{ field: "id", direction: "asc" }],
                limit: 10,
              })
              .pipe(
                Effect.repeat({
                  schedule: kafkaRestartPollSchedule,
                  until: (snapshot) =>
                    snapshot.totalRows === 3 &&
                    snapshot.rows[0]?.customerId === "customer-0-earliest-sentinel" &&
                    snapshot.rows[1]?.customerId === "customer-1-replayed-update",
                }),
              );
            const replayHealth = yield* runtime.client.health().pipe(
              Effect.repeat({
                schedule: kafkaRestartPollSchedule,
                until: (currentHealth) =>
                  currentHealth.engine.topics.orders.rowCount === 3 &&
                  currentHealth.kafka?.topics[ordersSourceTopic]?.status === "ready" &&
                  currentHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]
                    ?.committedOffset === "4" &&
                  currentHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]
                    ?.consumerLagMessages === 0n,
              }),
            );
            return {
              replaySnapshot,
              replayHealth,
            };
          }),
        (runtime) => runtime.close.pipe(Effect.ignore),
      );

      expect({
        firstSnapshot: firstResult.firstSnapshot,
        firstHealth: {
          status: firstResult.firstHealth.status,
          engineRows: firstResult.firstHealth.engine.topics.orders.rowCount,
          committedOffset:
            firstResult.firstHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]
              ?.committedOffset,
        },
        replaySnapshot: replayResult.replaySnapshot,
        replayHealth: {
          status: replayResult.replayHealth.status,
          startFrom: replayResult.replayHealth.kafka?.startFrom,
          kafkaRegions: replayResult.replayHealth.kafka?.regions,
          engineRows: replayResult.replayHealth.engine.topics.orders.rowCount,
          kafkaTopic: replayResult.replayHealth.kafka?.topics[ordersSourceTopic],
        },
      }).toStrictEqual({
        firstSnapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "order-0",
              customerId: "customer-0-earliest-sentinel",
              price: 1,
            },
            {
              id: "order-1",
              customerId: "customer-1-replayed-update",
              price: 99,
            },
            {
              id: "order-2",
              customerId: "customer-2",
              price: 20,
            },
          ],
          totalRows: 3,
          version: expect.any(Number),
        },
        firstHealth: {
          status: "ready",
          engineRows: 3,
          committedOffset: "4",
        },
        replaySnapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "order-0",
              customerId: "customer-0-earliest-sentinel",
              price: 1,
            },
            {
              id: "order-1",
              customerId: "customer-1-replayed-update",
              price: 99,
            },
            {
              id: "order-2",
              customerId: "customer-2",
              price: 20,
            },
          ],
          totalRows: 3,
          version: expect.any(Number),
        },
        replayHealth: {
          status: "ready",
          startFrom: {
            consumerGroupId,
            fallbackMode: "earliest",
            mode: "earliest",
          },
          kafkaRegions: nullRecord({
            local: {
              status: "connected",
              brokers: kafkaBootstrapServers,
              lastConnectedAt: expect.any(Number),
              lastError: null,
            },
          }),
          engineRows: 3,
          kafkaTopic: {
            status: "ready",
            sourceTopic: ordersSourceTopic,
            viewServerTopic: "orders",
            regions: nullRecord({
              local: {
                connected: true,
                assignedPartitions: 1,
                messagesPerSecond: expect.any(Number),
                bytesPerSecond: expect.any(Number),
                decodedMessagesPerSecond: expect.any(Number),
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                commitFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: expect.any(Number),
                lastCommitAt: expect.any(Number),
                consumerLagMessages: 0n,
                lagSampledAt: expect.any(Number),
                committedOffset: "4",
                lastError: null,
              },
            }),
          },
        },
      });
    }).pipe(Effect.provide(NodeCrypto.layer)),
  );

  it.live("starts Kafka from latest without backfilling old rows", () =>
    Effect.gen(function* () {
      const ordersSourceTopic = yield* uniqueTopicName("latest-start-orders");
      const consumerGroupId = yield* uniqueGroupId();
      yield* createKafkaTopics(kafkaBootstrapServers, [ordersSourceTopic]);

      const regions = {
        local: kafkaBootstrapServers,
      };
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const runtimeOptions = {
        host: "127.0.0.1",
        websocketPort: 0,
        kafka: {
          consumerGroupId,
          regions,
          startFrom: "latest" as const,
          topics: {
            [ordersSourceTopic]: localKafkaTopic({
              regions: ["local"],
              value: kafka.json(IncomingOrder),
              key: kafka.stringKey(),
              viewServerTopic: "orders",
              mapping: ({ key, value }) => ({
                id: key,
                customerId: value.customerId,
                price: value.price,
              }),
            }),
          },
        },
      };

      yield* sendKafkaMessages(kafkaBootstrapServers, "view-server-kafka-latest-start-test", [
        {
          topic: ordersSourceTopic,
          key: "order-1",
          value: JSON.stringify({
            customerId: "customer-1-before-latest-start",
            price: 10,
          }),
        },
        {
          topic: ordersSourceTopic,
          key: "order-2",
          value: JSON.stringify({
            customerId: "customer-2-before-latest-start",
            price: 20,
          }),
        },
      ]);

      const result = yield* Effect.acquireUseRelease(
        makeViewServerRuntime(viewServer, runtimeOptions),
        (runtime) =>
          Effect.gen(function* () {
            const startHealth = yield* runtime.client.health().pipe(
              Effect.repeat({
                schedule: kafkaRestartPollSchedule,
                until: (currentHealth) =>
                  currentHealth.engine.topics.orders.rowCount === 0 &&
                  currentHealth.kafka?.startFrom.mode === "latest" &&
                  currentHealth.kafka.topics[ordersSourceTopic]?.status === "ready" &&
                  currentHealth.kafka.topics[ordersSourceTopic]?.regions["local"]
                    ?.assignedPartitions === 1 &&
                  currentHealth.kafka.topics[ordersSourceTopic]?.regions["local"]
                    ?.consumerLagMessages === 0n,
              }),
            );

            yield* sendKafkaMessages(kafkaBootstrapServers, "view-server-kafka-latest-start-test", [
              {
                topic: ordersSourceTopic,
                key: "order-3",
                value: JSON.stringify({
                  customerId: "customer-3-after-latest-start",
                  price: 30,
                }),
              },
            ]);

            const snapshot = yield* runtime.client
              .snapshot("orders", {
                select: ["id", "customerId", "price"],
                orderBy: [{ field: "id", direction: "asc" }],
                limit: 10,
              })
              .pipe(
                Effect.repeat({
                  schedule: kafkaRestartPollSchedule,
                  until: (currentSnapshot) =>
                    currentSnapshot.totalRows === 1 &&
                    currentSnapshot.rows[0]?.customerId === "customer-3-after-latest-start",
                }),
              );
            const finalHealth = yield* runtime.client.health().pipe(
              Effect.repeat({
                schedule: kafkaRestartPollSchedule,
                until: (currentHealth) =>
                  currentHealth.engine.topics.orders.rowCount === 1 &&
                  currentHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]
                    ?.committedOffset === "3" &&
                  currentHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]
                    ?.consumerLagMessages === 0n,
              }),
            );
            return {
              finalHealth,
              snapshot,
              startHealth,
            };
          }),
        (runtime) => runtime.close.pipe(Effect.ignore),
      );

      expect({
        startHealth: {
          status: result.startHealth.status,
          startFrom: result.startHealth.kafka?.startFrom,
          engineRows: result.startHealth.engine.topics.orders.rowCount,
          kafkaTopic: result.startHealth.kafka?.topics[ordersSourceTopic],
        },
        snapshot: result.snapshot,
        finalHealth: {
          status: result.finalHealth.status,
          startFrom: result.finalHealth.kafka?.startFrom,
          engineRows: result.finalHealth.engine.topics.orders.rowCount,
          committedOffset:
            result.finalHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]?.committedOffset,
        },
      }).toStrictEqual({
        startHealth: {
          status: "ready",
          startFrom: {
            consumerGroupId,
            fallbackMode: "latest",
            mode: "latest",
          },
          engineRows: 0,
          kafkaTopic: {
            status: "ready",
            sourceTopic: ordersSourceTopic,
            viewServerTopic: "orders",
            regions: nullRecord({
              local: {
                connected: true,
                assignedPartitions: 1,
                messagesPerSecond: 0,
                bytesPerSecond: 0,
                decodedMessagesPerSecond: 0,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                commitFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: null,
                lastCommitAt: null,
                consumerLagMessages: 0n,
                lagSampledAt: expect.any(Number),
                committedOffset: null,
                lastError: null,
              },
            }),
          },
        },
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "order-3",
              customerId: "customer-3-after-latest-start",
              price: 30,
            },
          ],
          totalRows: 1,
          version: expect.any(Number),
        },
        finalHealth: {
          status: "ready",
          startFrom: {
            consumerGroupId,
            fallbackMode: "latest",
            mode: "latest",
          },
          engineRows: 1,
          committedOffset: "3",
        },
      });
    }).pipe(Effect.provide(NodeCrypto.layer)),
  );

  it.live("resumes committed Kafka offsets after restart without rebuilding old rows", () =>
    Effect.gen(function* () {
      const ordersSourceTopic = yield* uniqueTopicName("committed-restart-orders");
      const committedConsumerGroupId = yield* uniqueGroupId();
      const configuredConsumerGroupId = yield* uniqueGroupId();
      yield* createKafkaTopics(kafkaBootstrapServers, [ordersSourceTopic]);

      const regions = {
        local: kafkaBootstrapServers,
      };
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const runtimeOptions = {
        host: "127.0.0.1",
        websocketPort: 0,
        kafka: {
          consumerGroupId: configuredConsumerGroupId,
          regions,
          startFrom: {
            committedConsumerGroup: committedConsumerGroupId,
          },
          topics: {
            [ordersSourceTopic]: localKafkaTopic({
              regions: ["local"],
              value: kafka.json(IncomingOrder),
              key: kafka.stringKey(),
              viewServerTopic: "orders",
              mapping: ({ key, value }) => ({
                id: key,
                customerId: value.customerId,
                price: value.price,
              }),
            }),
          },
        },
      };

      yield* sendKafkaMessages(kafkaBootstrapServers, "view-server-kafka-committed-restart-test", [
        {
          topic: ordersSourceTopic,
          key: "order-1",
          value: JSON.stringify({
            customerId: "customer-1-before-commit",
            price: 10,
          }),
        },
        {
          topic: ordersSourceTopic,
          key: "order-2",
          value: JSON.stringify({
            customerId: "customer-2-before-commit",
            price: 20,
          }),
        },
      ]);

      const firstResult = yield* Effect.acquireUseRelease(
        makeViewServerRuntime(viewServer, {
          ...runtimeOptions,
          kafka: {
            ...runtimeOptions.kafka,
            consumerGroupId: committedConsumerGroupId,
          },
        }),
        (runtime) =>
          Effect.gen(function* () {
            const firstSnapshot = yield* runtime.client
              .snapshot("orders", {
                select: ["id", "customerId", "price"],
                orderBy: [{ field: "id", direction: "asc" }],
                limit: 10,
              })
              .pipe(
                Effect.repeat({
                  schedule: kafkaRestartPollSchedule,
                  until: (snapshot) => snapshot.totalRows === 2,
                }),
              );
            const firstHealth = yield* runtime.client.health().pipe(
              Effect.repeat({
                schedule: kafkaRestartPollSchedule,
                until: (currentHealth) =>
                  currentHealth.engine.topics.orders.rowCount === 2 &&
                  currentHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]
                    ?.committedOffset === "2" &&
                  currentHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]
                    ?.consumerLagMessages === 0n,
              }),
            );
            return {
              firstSnapshot,
              firstHealth,
            };
          }),
        (runtime) => runtime.close.pipe(Effect.ignore),
      );

      yield* sendKafkaMessages(kafkaBootstrapServers, "view-server-kafka-committed-restart-test", [
        {
          topic: ordersSourceTopic,
          key: "order-3",
          value: JSON.stringify({
            customerId: "customer-3-after-commit",
            price: 30,
          }),
        },
      ]);

      const resumedResult = yield* Effect.acquireUseRelease(
        makeViewServerRuntime(viewServer, runtimeOptions),
        (runtime) =>
          Effect.gen(function* () {
            const resumedSnapshot = yield* runtime.client
              .snapshot("orders", {
                select: ["id", "customerId", "price"],
                orderBy: [{ field: "id", direction: "asc" }],
                limit: 10,
              })
              .pipe(
                Effect.repeat({
                  schedule: kafkaRestartPollSchedule,
                  until: (snapshot) =>
                    snapshot.totalRows === 1 &&
                    snapshot.rows[0]?.customerId === "customer-3-after-commit",
                }),
              );
            const resumedHealth = yield* runtime.client.health().pipe(
              Effect.repeat({
                schedule: kafkaRestartPollSchedule,
                until: (currentHealth) =>
                  currentHealth.engine.topics.orders.rowCount === 1 &&
                  currentHealth.kafka?.topics[ordersSourceTopic]?.status === "ready" &&
                  currentHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]
                    ?.committedOffset === "3" &&
                  currentHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]
                    ?.consumerLagMessages === 0n,
              }),
            );
            return {
              resumedSnapshot,
              resumedHealth,
            };
          }),
        (runtime) => runtime.close.pipe(Effect.ignore),
      );

      expect({
        configuredGroupIsDistinct: configuredConsumerGroupId !== committedConsumerGroupId,
        firstSnapshot: firstResult.firstSnapshot,
        firstHealth: {
          status: firstResult.firstHealth.status,
          startFrom: firstResult.firstHealth.kafka?.startFrom,
          engineRows: firstResult.firstHealth.engine.topics.orders.rowCount,
          committedOffset:
            firstResult.firstHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]
              ?.committedOffset,
        },
        resumedSnapshot: resumedResult.resumedSnapshot,
        resumedHealth: {
          status: resumedResult.resumedHealth.status,
          startFrom: resumedResult.resumedHealth.kafka?.startFrom,
          engineRows: resumedResult.resumedHealth.engine.topics.orders.rowCount,
          kafkaTopic: resumedResult.resumedHealth.kafka?.topics[ordersSourceTopic],
        },
      }).toStrictEqual({
        configuredGroupIsDistinct: true,
        firstSnapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "order-1",
              customerId: "customer-1-before-commit",
              price: 10,
            },
            {
              id: "order-2",
              customerId: "customer-2-before-commit",
              price: 20,
            },
          ],
          totalRows: 2,
          version: expect.any(Number),
        },
        firstHealth: {
          status: "ready",
          startFrom: {
            consumerGroupId: committedConsumerGroupId,
            fallbackMode: "earliest",
            mode: "committed",
          },
          engineRows: 2,
          committedOffset: "2",
        },
        resumedSnapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "order-3",
              customerId: "customer-3-after-commit",
              price: 30,
            },
          ],
          totalRows: 1,
          version: expect.any(Number),
        },
        resumedHealth: {
          status: "ready",
          startFrom: {
            consumerGroupId: committedConsumerGroupId,
            fallbackMode: "earliest",
            mode: "committed",
          },
          engineRows: 1,
          kafkaTopic: {
            status: "ready",
            sourceTopic: ordersSourceTopic,
            viewServerTopic: "orders",
            regions: nullRecord({
              local: {
                connected: true,
                assignedPartitions: 1,
                messagesPerSecond: expect.any(Number),
                bytesPerSecond: expect.any(Number),
                decodedMessagesPerSecond: expect.any(Number),
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                commitFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: expect.any(Number),
                lastCommitAt: expect.any(Number),
                consumerLagMessages: 0n,
                lagSampledAt: expect.any(Number),
                committedOffset: "3",
                lastError: null,
              },
            }),
          },
        },
      });
    }).pipe(Effect.provide(NodeCrypto.layer)),
  );
});
