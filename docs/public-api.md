# Public API

## Define Topics

Application code starts with `defineViewServerConfig`. Each topic has an Effect
Schema and a string row key field. The topic schema is the source of truth for
query typing, runtime validation, protocol encoding, and in-memory tests.

```ts
import { defineViewServerConfig, grpc, kafka } from "effect-view-server/config";
import { createViewServerReact } from "effect-view-server/react";
import { Config, Schema } from "effect";
import { ordersService } from "./generated/grpc";
import { OrdersValueSchema } from "./generated/orders";

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
    },
  },
});

export const viewServerReact = createViewServerReact(viewServer);
export const { ViewServerProvider, useLiveQuery, useViewServerHealthSummary } = viewServerReact;
```

Topics without `kafkaSource` or `grpcSource` are externally/manual published
topics, for example through TCP publish or an in-memory test client. A topic can
only have one source owner.

## React Provider

Production React code passes a runtime URL to the provider. The provider owns
the remote Effect RPC WebSocket client. Application components use hooks from
the same `createViewServerReact(viewServer)` binding object.

```tsx
export function AppRoot() {
  return (
    <ViewServerProvider url={window.__APP_CONFIG__.VIEW_SERVER_URL}>
      <App />
    </ViewServerProvider>
  );
}
```

## Live Queries

Raw queries must declare `select`. This prevents accidentally returning every
column from wide topics.

```tsx
function Orders() {
  const orders = useLiveQuery("orders", {
    select: ["id", "price", "status"],
    where: {
      status: { eq: "open" },
      customerId: { startsWith: "customer-" },
      price: { gte: 10 },
    },
    orderBy: [{ field: "price", direction: "desc" }],
    limit: 20,
  });

  return <pre>{JSON.stringify(orders.rows, null, 2)}</pre>;
}
```

Grouped queries use `groupBy` plus an aggregate object keyed by output alias.
Aggregate aliases become fields on the returned row type.

```tsx
const totals = useLiveQuery("orders", {
  groupBy: ["status"],
  aggregates: {
    rowCount: { aggFunc: "count" },
    totalPrice: { aggFunc: "sum", field: "price" },
    maxUpdatedAt: { aggFunc: "max", field: "updatedAt" },
  },
  orderBy: [{ aggregate: "totalPrice", direction: "desc" }],
  limit: 10,
});
```

The public API is designed so consumers do not need `as const` to keep type
safety for normal `select`, `where`, `orderBy`, `groupBy`, and aggregate
queries.
