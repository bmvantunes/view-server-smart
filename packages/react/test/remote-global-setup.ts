import { defineViewServerConfig } from "@view-server/config";
import { createViewServerRuntimeCore } from "@view-server/runtime-core";
import { makeViewServerWebSocketServer } from "@view-server/server";
import { Effect, Schema } from "effect";

type Project = {
  readonly provide: (key: "viewServerRemoteUrl", value: string) => void;
};

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
  quantity: Schema.BigInt,
  price: Schema.Number,
  region: Schema.String,
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

export const setup = async (project: Project) => {
  const runtimeCore = createViewServerRuntimeCore(viewServer);
  const server = await Effect.runPromise(
    makeViewServerWebSocketServer(viewServer, {
      liveClient: runtimeCore.liveClient,
      runtime: runtimeCore.client,
    }),
  );
  project.provide("viewServerRemoteUrl", server.url);

  return async () => {
    await Effect.runPromise(server.close.pipe(Effect.andThen(runtimeCore.close)));
  };
};
