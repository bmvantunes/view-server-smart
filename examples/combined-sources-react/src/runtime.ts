import { NodeRuntime } from "@effect/platform-node";
import { runViewServerRuntime } from "@view-server/runtime";
import { Stream } from "effect";
import { grpcClients, kafkaRegions, kafkaTopics, viewServer } from "./view-server.config";

const grpcFeed = viewServer.grpcFeed<typeof grpcClients>();

const ordersByStrategyRegion = grpcFeed.leasedFeed({
  topic: "orders",
  client: "combined",
  method: "streamOrders",
  routeBy: ["strategyId", "region"],
  request: ({ strategyId, region }) => ({ strategyId, region }),
  acquire: ({ route }) =>
    Stream.make({
      $typeName: "viewserver.combined.OrderValue",
      customerId: `customer-${route.strategyId}`,
      status: "open",
      price: 15,
      updatedAt: 1,
    }).pipe(Stream.concat(Stream.never)),
  map: ({ value, route }) => ({
    id: `${route.strategyId}:${route.region}:${value.customerId}`,
    customerId: value.customerId,
    status: value.status,
    price: value.price,
    region: route.region,
    strategyId: route.strategyId,
    updatedAt: value.updatedAt,
  }),
});

const strategiesFeed = grpcFeed.materializedFeed({
  topic: "strategies",
  client: "combined",
  method: "streamStrategies",
  request: () => ({ universe: "global" }),
  acquire: () =>
    Stream.make({
      $typeName: "viewserver.combined.StrategyValue",
      strategyId: "strategy-alpha",
      region: "usa",
      status: "active",
      notional: 100,
      updatedAt: 1,
    }).pipe(Stream.concat(Stream.never)),
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
    kafka: {
      consumerGroupId: "view-server-example-combined-sources-react",
      startFrom: "latest",
      regions: kafkaRegions,
      topics: kafkaTopics,
    },
    grpc: {
      clients: grpcClients,
      feeds: { ordersByStrategyRegion, strategiesFeed },
    },
  }),
);
