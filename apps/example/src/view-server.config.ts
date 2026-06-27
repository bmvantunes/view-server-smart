import { defineViewServerConfig } from "@view-server/config";
import { createViewServerReact } from "@view-server/react";
import { Schema } from "effect";

export const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

export const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  quantity: Schema.BigInt,
  price: Schema.Number,
  region: Schema.String,
});

export const viewServer = defineViewServerConfig({
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

export const viewServerReact = createViewServerReact(viewServer);

export const { ViewServerProvider, useLiveQuery, useViewServerHealthSummary } = viewServerReact;
