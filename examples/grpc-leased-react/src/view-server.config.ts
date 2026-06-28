import { defineViewServerConfig, grpc } from "@view-server/config";
import { createViewServerReact } from "@view-server/react";
import { Schema } from "effect";
import { ordersService } from "./grpc-descriptors";

export const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  strategyId: Schema.String,
  updatedAt: Schema.Number,
});

export const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
      source: grpc.leased({ routeBy: ["strategyId", "region"] }),
    },
  },
});

export const viewServerReact = createViewServerReact(viewServer);
export const { ViewServerProvider, useLiveQuery, useViewServerHealthSummary } = viewServerReact;

export const grpcClients = {
  orders: grpc.connectClient({
    service: ordersService,
    baseUrl: "http://127.0.0.1:4317",
  }),
};
