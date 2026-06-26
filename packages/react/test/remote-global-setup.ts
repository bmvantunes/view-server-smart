import { env } from "node:process";

type Project = {
  readonly provide: (key: "viewServerRemoteUrl", value: string) => void;
};

const skipRemoteGlobalSetup = (): boolean =>
  env["VIEW_SERVER_REACT_SKIP_REMOTE_GLOBAL_SETUP"] === "1";

export const setup = async (project: Project) => {
  if (skipRemoteGlobalSetup()) {
    project.provide("viewServerRemoteUrl", "ws://127.0.0.1:0/rpc");
    return () => Promise.resolve();
  }

  const { defineViewServerConfig } = await import("@view-server/config");
  const { createInMemoryViewServerTesting } = await import("@view-server/in-memory/testing");
  const { makeViewServerWebSocketServer } = await import("@view-server/server");
  const { Effect, Schema } = await import("effect");

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

  const runtimeCore = createInMemoryViewServerTesting(viewServer);
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
