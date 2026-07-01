# Effect View Server

## Guides

- [Public API](./docs/public-api.md)
- [Runtime Config](./docs/runtime-config.md)
- [Kafka Mapping](./docs/kafka-mapping.md)
- [In-Memory Browser Testing](./docs/in-memory-browser-testing.md)
- [Health And Metrics](./docs/health-and-metrics.md)
- [Query Semantics](./docs/query-semantics.md)
- [Benchmarks And Capacity](./docs/benchmarks-and-capacity.md)
- [Deployment](./docs/deployment.md)
- [Operations](./docs/operations.md)
- [Examples](./examples/README.md)

## Install

Core/server-only consumers need only the main package:

```sh
npm install effect-view-server
```

React consumers should also install the React subpath peers:

```sh
npm install effect-view-server react react-dom @effect/atom-react
```

## Source-Owned Config

Topics declare their source of truth directly. Kafka regions and gRPC clients are
configured once, then each topic chooses `kafkaSource`, `grpcSource`, or no
source for TCP/manual publishing:

```ts
import { Config, Schema } from "effect";
import { defineViewServerConfig, grpc, kafka } from "effect-view-server/config";
import { ordersService, strategiesService } from "./generated/grpc";
import { OrdersKeySchema, OrdersValueSchema } from "./generated/orders";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  strategyId: Schema.String,
  updatedAt: Schema.Number,
});

const Strategy = Schema.Struct({
  id: Schema.String,
  strategyId: Schema.String,
  region: Schema.String,
  status: Schema.Literals(["active", "paused"]),
  notional: Schema.Number,
  updatedAt: Schema.Number,
});

export const viewServer = defineViewServerConfig({
  kafka: {
    usa: Config.string("KAFKA_USA_BOOTSTRAP"),
    london: Config.string("KAFKA_LONDON_BOOTSTRAP"),
  },
  grpc: {
    clients: {
      orders: grpc.connectClient({
        service: ordersService,
        baseUrl: "https://orders-grpc.example.com",
      }),
      strategies: grpc.connectClient({
        service: strategiesService,
        baseUrl: "https://strategies-grpc.example.com",
      }),
    },
  },
  topics: {
    orders: {
      schema: Order,
      key: "id",
      kafkaSource: kafka.source({
        topic: "sourceOrdersUsa",
        regions: ["usa"],
        value: kafka.protobuf(OrdersValueSchema),
        key: kafka.protobuf(OrdersKeySchema),
        map: ({ key, value, region, rowKey }) => ({
          id: rowKey,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region,
          strategyId: key.strategyId,
          updatedAt: value.updatedAt,
        }),
      }),
    },
    strategies: {
      schema: Strategy,
      key: "id",
      grpcSource: grpc.materialized(),
    },
    ordersByStrategy: {
      schema: Order,
      key: "id",
      grpcSource: grpc.leased({ routeBy: ["strategyId", "region"] }),
    },
  },
});
```

The region tuple `["usa"]`, gRPC client names, source topics, route fields, and
mapping inputs/outputs are all type checked. A topic can have only one owner:
Kafka, gRPC, or external/manual publishing.

## Remote React provider

Server code starts a runtime through Effect RPC WebSocket plus same-server
`GET /health` and `GET /metrics` endpoints:

Node entrypoints should use `@effect/platform-node`'s `NodeRuntime.runMain` so
`SIGINT` and `SIGTERM` interrupt the main fiber and run Effect finalizers.

```ts
import { NodeRuntime } from "@effect/platform-node";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { Stream } from "effect";
import { viewServer } from "./view-server-config";

const grpcFeed = viewServer.grpcFeed();

const ordersByStrategyFeed = grpcFeed.leasedFeed({
  topic: "ordersByStrategy",
  client: "orders",
  method: "streamOrders",
  routeBy: ["strategyId", "region"],
  request: ({ strategyId, region }) => ({ strategyId, region }),
  acquire: ({ client, request }) =>
    Stream.fromAsyncIterable(client.streamOrders(request), (cause) => cause),
  map: ({ value, route }) => ({
    id: `${route.strategyId}:${route.region}:${value.orderId}`,
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
  client: "strategies",
  method: "streamStrategies",
  request: () => ({ universe: "global" }),
  acquire: ({ client, request }) =>
    Stream.fromAsyncIterable(client.streamStrategies(request), (cause) => cause),
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
    host: "127.0.0.1",
    websocketPort: 8080,
    tcpPublishPort: 8081,
    kafka: {
      consumerGroupId: "orders-view-server",
      startFrom: "latest",
    },
    grpc: {
      feeds: { ordersByStrategyFeed, strategiesFeed },
    },
  }),
);
```

The same-server `GET /health` endpoint serves the cached runtime health snapshot
for deployment readiness checks. Internal `bigint` health fields, such as Kafka
lag, are encoded as decimal strings in the JSON response.

When `tcpPublishPort` is configured, the runtime also opens a non-browser TCP
NDJSON publish endpoint and exposes its `tcpPublishUrl`. That endpoint supports
`publish`, `publishMany`, `patch`, and `delete` commands and routes every
mutation through the same Runtime Core path as Kafka, gRPC, and in-memory tests.
TCP publish is for externally-published topics only; Kafka/gRPC-owned topics are
rejected so one View Server topic has one source of truth. TCP publish has its
own `tcpPublishHost` and defaults to `127.0.0.1`; it does not inherit the public
WebSocket/HTTP host. The TCP endpoint is bounded by connection, line-size, and
queued-command limits.

The same-server `GET /metrics` endpoint serves Prometheus text exposition derived
from the same cached health snapshot. It exposes scrape-safe runtime, transport,
engine, Kafka, and gRPC gauges/counters. It is not a full mirror of health:
high-cardinality values such as raw error messages and route-specific leased
feed keys remain in `GET /health`. Scrape failures that cannot decode health return `200` with
`view_server_metrics_error 1` so the scrape itself remains observable.

Browser React code keeps using the normal provider and hooks:

```tsx
import { createViewServerReact } from "effect-view-server/react";
import { viewServer } from "./view-server-config";

const react = createViewServerReact(viewServer);

export function App() {
  return (
    <react.ViewServerProvider url={window.__APP_CONFIG__.VIEW_SERVER_URL}>
      <Orders />
    </react.ViewServerProvider>
  );
}

function Orders() {
  const orders = react.useLiveQuery("orders", {
    select: ["id", "price"],
    orderBy: [{ field: "price", direction: "asc" }],
    limit: 20,
  });

  return <pre>{JSON.stringify(orders.rows, null, 2)}</pre>;
}
```
