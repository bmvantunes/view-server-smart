# @view-server/runtime

Production runtime composition for View Server.

This package wires Runtime Core, Effect RPC WebSocket transport, `GET /health`,
and optional Kafka ingestion. The in-memory engine remains the single mutation
path; Kafka and future ingress adapters publish into the same runtime core used
by tests.

## Entrypoint

Node entrypoints should use `NodeRuntime.runMain` so process signals interrupt the
main fiber and run Effect finalizers.

```ts
import { NodeRuntime } from "@effect/platform-node";
import { runViewServerRuntime } from "@view-server/runtime";
import { viewServer } from "./view-server-config";

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    host: "0.0.0.0",
    websocketPort: 8080,
  }),
);
```

`runViewServerRuntime` logs the WebSocket and health URLs when the runtime starts
and keeps the server alive until the main fiber is interrupted.

## Health

The runtime exposes a same-server `GET /health` endpoint for deployment
readiness checks. It returns `200` when the runtime status is `ready` and a
non-`200` status when the runtime is starting, degraded, or stopping.

The JSON response is the current runtime health snapshot. Internal `bigint`
fields, such as Kafka lag, are encoded as decimal strings.

React applications should use the pushed health hooks from `@view-server/react`;
`GET /health` is for infrastructure and smoke checks, not UI polling.

## Kafka Ingestion

Kafka is optional. When configured, runtime options must provide an explicit
consumer group and typed source topics:

```ts
import { Config } from "effect";
import { NodeRuntime } from "@effect/platform-node";
import { kafka } from "@view-server/config";
import { runViewServerRuntime } from "@view-server/runtime";
import { viewServer } from "./view-server-config";
import { OrdersKey, OrdersValue } from "./generated/orders";

const kafkaRegions = {
  usa: Config.string("KAFKA_USA_BOOTSTRAP"),
  london: Config.string("KAFKA_LONDON_BOOTSTRAP"),
};

const kafkaTopic = viewServer.kafkaTopic<typeof kafkaRegions>();

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "orders-view-server",
      regions: kafkaRegions,
      topics: {
        sourceOrders: kafkaTopic({
          regions: ["usa", "london"],
          value: kafka.protobuf(OrdersValue),
          key: kafka.protobuf(OrdersKey),
          viewServerTopic: "orders",
          mapping: ({ key, value, region }) => ({
            id: key.orderId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region,
            updatedAt: value.updatedAt,
          }),
        }),
      },
    },
  }),
);
```

Startup fails through Effect `Config` if required environment-backed Kafka
broker values are missing. Do not silently default brokers or production
secrets.

## Kafka Delivery Contract

During a live process, Kafka messages are decoded, mapped, grouped into small
microbatches, and published into Runtime Core with `publishMany`.

Offsets are committed only after the corresponding Runtime Core publish
succeeds. If a publish fails, the original Kafka messages remain uncommitted so
Kafka can replay them.

If a later message in a batch fails decode or mapping after earlier messages
were decoded, the decoded prefix is published and committed before the failing
message is reported. If the Kafka stream fails after yielding messages, the
yielded batch is flushed before Kafka health marks the region disconnected and
overall runtime health becomes degraded.

## Start Position And Restart Semantics

`startFrom` controls where Kafka consumers begin:

- `"earliest"` replays from the beginning for the configured consumer group.
- `"latest"` starts from the latest offsets for the configured consumer group.
- `{ committedConsumerGroup, fallback }` resumes committed offsets for an
  existing group and uses `fallback` when no committed offsets exist. `fallback`
  can be `"earliest"`, `"latest"`, or `"fail"`.

The default is `{ committedConsumerGroup: consumerGroupId, fallback: "earliest" }`.

Important: committed consumer-group resume is not durable View Server recovery
by itself. Runtime Core rows are in memory and this package does not yet provide
a WAL or checkpoint. If the process dies after committing Kafka offsets, a
restart from committed offsets can skip rows that existed only in memory.

Deployments that need rebuild-after-restart semantics must replay Kafka from an
authoritative position, such as `startFrom: "earliest"` or a fresh/reset
dedicated rebuild consumer group, until durable checkpoints are added.

## Current Consumer Group Assumption

The current runtime starts one consumer per configured region that has at least
one Source Topic. The actual group used is the normalized consumer group from
`startFrom`: `consumerGroupId` for `"earliest"` / `"latest"`, or
`committedConsumerGroup` for committed resume. Runtime health records that
normalized group, assignments, and lag for the current process, but full
rebalance/revoke handoff and checkpoint handoff are not implemented yet.

## Cleanup

Runtime finalizers stop Kafka consumers, close the WebSocket server, and close
Runtime Core resources. Use Effect runtime ownership (`NodeRuntime.runMain`,
scopes, and finalizers) rather than process-level ad-hoc cleanup.
