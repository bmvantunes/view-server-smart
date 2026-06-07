import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { describe, expect, it } from "@effect/vitest";
import { Admin, Producer, stringSerializers } from "@platformatic/kafka";
import { defineViewServerConfig, kafka } from "@view-server/config";
import { Crypto, Effect, Schedule, Schema } from "effect";
import { makeViewServerRuntime } from "./index";

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

type ProducerMessage = {
  readonly topic: string;
  readonly key: string;
  readonly value: string;
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
                {
                  topic: ordersSourceTopic,
                  key: "order-bad-json",
                  value: "{",
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
                    currentHealth.kafka?.topics[ordersSourceTopic]?.status === "degraded" &&
                    currentHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]?.lastError ===
                      "Failed to parse Kafka JSON payload" &&
                    currentHealth.kafka?.topics[tradesSourceTopic]?.status === "ready",
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
                status: "degraded",
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
                  version: 2,
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
                  version: 1,
                },
                engineRows: {
                  orders: 2,
                  trades: 1,
                },
                kafka: {
                  regions: {
                    local: {
                      status: "connected",
                      brokers: kafkaBootstrapServers,
                      lastConnectedAt: expect.any(Number),
                      lastError: null,
                    },
                  },
                  topics: {
                    [ordersSourceTopic]: {
                      status: "degraded",
                      sourceTopic: ordersSourceTopic,
                      viewServerTopic: "orders",
                      regions: {
                        local: {
                          connected: true,
                          assignedPartitions: 0,
                          messagesPerSecond: expect.any(Number),
                          bytesPerSecond: expect.any(Number),
                          decodedMessagesPerSecond: expect.any(Number),
                          decodeFailuresPerSecond: 1,
                          processingFailuresPerSecond: 0,
                          lastMessageAt: expect.any(Number),
                          lastCommitAt: expect.any(Number),
                          consumerLagMessages: null,
                          consumerLagMs: null,
                          lagSampledAt: null,
                          highWatermarkOffset: "1",
                          committedOffset: "1",
                          lastError: "Failed to parse Kafka JSON payload",
                        },
                      },
                    },
                    [tradesSourceTopic]: {
                      status: "ready",
                      sourceTopic: tradesSourceTopic,
                      viewServerTopic: "trades",
                      regions: {
                        local: {
                          connected: true,
                          assignedPartitions: 0,
                          messagesPerSecond: 1,
                          bytesPerSecond: 39,
                          decodedMessagesPerSecond: 1,
                          decodeFailuresPerSecond: 0,
                          processingFailuresPerSecond: 0,
                          lastMessageAt: expect.any(Number),
                          lastCommitAt: expect.any(Number),
                          consumerLagMessages: null,
                          consumerLagMs: null,
                          lagSampledAt: null,
                          highWatermarkOffset: "0",
                          committedOffset: "0",
                          lastError: null,
                        },
                      },
                    },
                  },
                },
              });
            }),
          (runtime) => runtime.close.pipe(Effect.ignore),
        );
      }).pipe(Effect.provide(NodeCrypto.layer)),
  );
});
