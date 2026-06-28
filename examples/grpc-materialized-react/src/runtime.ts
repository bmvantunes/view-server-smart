import { NodeRuntime } from "@effect/platform-node";
import { runViewServerRuntime } from "@view-server/runtime";
import { Stream } from "effect";
import { grpcClients, viewServer } from "./view-server.config";

const grpcFeed = viewServer.grpcFeed<typeof grpcClients>();

const strategiesFeed = grpcFeed.materializedFeed({
  topic: "strategies",
  client: "strategies",
  method: "streamStrategies",
  request: () => ({ universe: "global" }),
  acquire: () =>
    Stream.make(
      {
        $typeName: "viewserver.example.StrategyValue",
        strategyId: "strategy-alpha",
        region: "usa",
        status: "active",
        notional: 100,
        updatedAt: 1,
      },
      {
        $typeName: "viewserver.example.StrategyValue",
        strategyId: "strategy-beta",
        region: "london",
        status: "paused",
        notional: 75,
        updatedAt: 2,
      },
    ).pipe(Stream.concat(Stream.never)),
  map: ({ value }) => ({
    id: `${value.strategyId}:${value.region}`,
    strategyId: value.strategyId,
    region: value.region,
    status: value.status,
    notional: value.notional,
    updatedAt: value.updatedAt,
  }),
});

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    websocketPort: 8080,
    grpc: {
      clients: grpcClients,
      feeds: { strategiesFeed },
    },
  }),
);
