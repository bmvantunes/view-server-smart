import { defineViewServerConfig, kafka } from "effect-view-server/config";
import { createViewServerReact } from "effect-view-server/react";
import { Schema } from "effect";

export const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

export const KafkaOrder = Schema.Struct({
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  updatedAt: Schema.Number,
});

export const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

export const viewServerReact = createViewServerReact(viewServer);
export const { ViewServerProvider, useLiveQuery, useViewServerHealth, useViewServerHealthSummary } =
  viewServerReact;

export const kafkaRegions = {
  local: "127.0.0.1:9092",
};

const kafkaTopic = viewServer.kafkaTopic<typeof kafkaRegions>();

export const kafkaTopics = {
  "view-server-example-orders": kafkaTopic({
    regions: ["local"],
    value: kafka.json(KafkaOrder),
    key: kafka.stringKey(),
    viewServerTopic: "orders",
    getSafeRowKey: ({ key }) => key,
    mapping: ({ key, value, region }) => ({
      id: key,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region,
      updatedAt: value.updatedAt,
    }),
  }),
};
