import { NodeRuntime } from "@effect/platform-node";
import { runViewServerRuntime } from "@view-server/runtime";
import { Effect, Stream } from "effect";
import { grpcClients, viewServer } from "./view-server.config";

const grpcFeed = viewServer.grpcFeed<typeof grpcClients>();

const ordersByStrategyRegion = grpcFeed.leasedFeed({
  topic: "orders",
  client: "orders",
  method: "streamOrders",
  routeBy: ["strategyId", "region"],
  request: ({ strategyId, region }) => ({ strategyId, region }),
  acquire: ({ route }) =>
    Stream.make(
      {
        $typeName: "viewserver.example.OrderValue",
        customerId: `customer-${route.strategyId}`,
        status: "open",
        price: 10,
        updatedAt: 1,
      },
      {
        $typeName: "viewserver.example.OrderValue",
        customerId: `customer-${route.region}`,
        status: "open",
        price: 20,
        updatedAt: 2,
      },
    ).pipe(Stream.concat(Stream.never)),
  release: () => Effect.logInfo("Released leased gRPC orders feed."),
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

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    websocketPort: 8080,
    grpc: {
      clients: grpcClients,
      feeds: { ordersByStrategyRegion },
    },
  }),
);
