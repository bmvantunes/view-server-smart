import { defineViewServerConfig, grpc } from "@view-server/config";
import { createViewServerReact } from "@view-server/react";
import { Schema } from "effect";
import { strategiesService } from "./grpc-descriptors";

export const Strategy = Schema.Struct({
  id: Schema.String,
  strategyId: Schema.String,
  region: Schema.String,
  status: Schema.Literals(["active", "paused"]),
  notional: Schema.Number,
  updatedAt: Schema.Number,
});

export const viewServer = defineViewServerConfig({
  topics: {
    strategies: {
      schema: Strategy,
      key: "id",
      source: grpc.materialized(),
    },
  },
});

export const viewServerReact = createViewServerReact(viewServer);
export const { ViewServerProvider, useLiveQuery, useViewServerHealthSummary } = viewServerReact;

export const grpcClients = {
  strategies: grpc.connectClient({
    service: strategiesService,
    baseUrl: "http://127.0.0.1:4318",
  }),
};
