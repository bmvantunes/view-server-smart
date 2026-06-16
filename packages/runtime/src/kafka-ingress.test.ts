import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import { create, toBinary } from "@bufbuild/protobuf";
import { Admin, Producer, stringSerializers } from "@platformatic/kafka";
import { defineViewServerConfig, kafka } from "@view-server/config";
import { Buffer } from "node:buffer";
import { Crypto, Effect, Schedule, Schema } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import { makeViewServerRuntime } from "./index";
import {
  type OrderKey,
  OrderKeySchema,
  type OrderValue,
  OrderValueSchema,
} from "./test-fixtures/runtime_orders_pb";

const kafkaBootstrapServers =
  process.env["VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS"] ?? "localhost:9092";

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

describe("@view-server/runtime Kafka ingress", () => {
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
});
