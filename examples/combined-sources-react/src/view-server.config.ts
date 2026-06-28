import { defineViewServerConfig, grpc, kafka } from "@view-server/config";
import { createViewServerReact } from "@view-server/react";
import { Schema } from "effect";
import { combinedService } from "./grpc-descriptors";

export const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  strategyId: Schema.String,
  updatedAt: Schema.Number,
});

export const Strategy = Schema.Struct({
  id: Schema.String,
  strategyId: Schema.String,
  region: Schema.String,
  status: Schema.Literals(["active", "paused"]),
  notional: Schema.Number,
  updatedAt: Schema.Number,
});

export const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  side: Schema.Literals(["buy", "sell"]),
  quantity: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

export const KafkaTrade = Schema.Struct({
  symbol: Schema.String,
  side: Schema.Literals(["buy", "sell"]),
  quantity: Schema.Number,
  updatedAt: Schema.Number,
});

export const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
      source: grpc.leased({ routeBy: ["strategyId", "region"] }),
    },
    strategies: {
      schema: Strategy,
      key: "id",
      source: grpc.materialized(),
    },
    trades: {
      schema: Trade,
      key: "id",
    },
  },
});

export const viewServerReact = createViewServerReact(viewServer);
export const { ViewServerProvider, useLiveQuery, useViewServerHealth, useViewServerHealthSummary } =
  viewServerReact;

export const grpcClients = {
  combined: grpc.connectClient({
    service: combinedService,
    baseUrl: "http://127.0.0.1:4319",
  }),
};

export const kafkaRegions = {
  local: "127.0.0.1:9092",
};

const kafkaTopic = viewServer.kafkaTopic<typeof kafkaRegions>();

export const kafkaTopics = {
  "view-server-example-trades": kafkaTopic({
    regions: ["local"],
    value: kafka.json(KafkaTrade),
    key: kafka.stringKey(),
    viewServerTopic: "trades",
    mapping: ({ key, value, region }) => ({
      id: key,
      symbol: value.symbol,
      side: value.side,
      quantity: value.quantity,
      region,
      updatedAt: value.updatedAt,
    }),
  }),
};
