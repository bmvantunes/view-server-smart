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

export const kafkaRegions = {
  usa: "127.0.0.1:9092",
  london: "127.0.0.1:9094",
};

export const viewServer = defineViewServerConfig({
  kafka: kafkaRegions,
  topics: {
    orders: {
      schema: Order,
      key: "id",
      kafkaSource: kafka.source({
        topic: "view-server-example-orders-usa",
        regions: ["usa"],
        value: kafka.json(KafkaOrder),
        key: kafka.stringKey(),
        map: ({ value, region, rowKey }) => ({
          id: rowKey,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region,
          updatedAt: value.updatedAt,
        }),
      }),
    },
    trades: {
      schema: Trade,
      key: "id",
      kafkaSource: kafka.source({
        topic: "view-server-example-trades-london",
        regions: ["london"],
        value: kafka.json(KafkaTrade),
        key: kafka.stringKey(),
        map: ({ value, region, rowKey }) => ({
          id: rowKey,
          symbol: value.symbol,
          side: value.side,
          quantity: value.quantity,
          region,
          updatedAt: value.updatedAt,
        }),
      }),
    },
  },
});

export const viewServerReact = createViewServerReact(viewServer);
export const { ViewServerProvider, useLiveQuery, useViewServerHealth, useViewServerHealthSummary } =
  viewServerReact;
