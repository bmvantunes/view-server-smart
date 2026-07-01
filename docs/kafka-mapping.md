# Kafka Mapping

Kafka source topics are configured from the typed View Server config. The
mapping function receives typed decoded Kafka key/value data and must return a
row that matches the target View Server topic schema.

```ts
import { Config } from "effect";
import { NodeRuntime } from "@effect/platform-node";
import { defineViewServerConfig, kafka } from "effect-view-server/config";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { KafkaTrade, Order, Trade } from "./schemas";
import { OrderValueSchema } from "./generated/orders";

const kafkaRegions = {
  usa: Config.string("KAFKA_USA_BOOTSTRAP"),
  london: Config.string("KAFKA_LONDON_BOOTSTRAP"),
};

export const viewServer = defineViewServerConfig({
  kafka: kafkaRegions,
  topics: {
    orders: {
      schema: Order,
      key: "id",
      kafkaSource: kafka.source({
        topic: "sourceOrdersUsa",
        regions: ["usa"],
        value: kafka.protobuf(OrderValueSchema),
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
      kafkaSource: kafka.source({
        topic: "sourceTradesLondon",
        regions: ["london"],
        value: kafka.json(KafkaTrade),
        key: kafka.stringKey(),
        map: ({ value, region, rowKey }) => ({
          id: rowKey,
          symbol: value.symbol,
          side: value.side,
          quantity: value.quantity,
          region,
          updatedAt: value.updatedAt,
        }),
      }),
    },
  },
});

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "orders-view-server",
    },
  }),
);
```

`kafka.protobuf(...)` expects the Buf generated `DescMessage` descriptor symbol,
not a TypeScript value type.

The region names in each `kafkaSource.regions` tuple are checked against
`config.kafka`. In the example above, `["usa"]` and `["london"]` are valid, but
`["paris"]` fails at compile time.

## Contract

- `regions` is type-checked against the configured Kafka region names.
- `kafkaSource` is owned by exactly one View Server topic, so the runtime cannot
  accidentally publish the same source into a different topic.
- `key` is typed from the configured key codec. If no key codec is configured,
  the key is a string.
- `value` is typed from the configured value codec.
- `map` output is validated against the target topic schema before publish.

The legacy `viewServer.kafkaTopic()` + `runtime.kafka.topics` API is still
available for admin-owned/manual source wiring, but new Kafka integrations
should prefer topic-owned `kafkaSource` definitions.

## Delivery

Kafka messages are decoded, mapped, microbatched, and published through Runtime
Core with `publishMany`. Offsets are committed only after the corresponding
Runtime Core publish succeeds.

If a message fails decode or mapping, health records a decode or mapping failure
for the source topic and region. If publishing fails, the corresponding messages
remain uncommitted so Kafka can replay them.

## Restart Semantics

Runtime Core rows live in memory. There is no durable WAL/checkpoint yet. For
rebuild-after-restart semantics, configure Kafka replay from an authoritative
position such as `startFrom: "earliest"` or a fresh rebuild consumer group.

Committed consumer-group resume is useful for live at-least-once processing, but
it is not durable View Server recovery by itself.

`startFrom` is currently a runtime-level consumer policy. A single View Server
runtime cannot read one Kafka source topic from `"earliest"` and another from
`"latest"` with the same consumer group. If you need mixed start positions today,
run separate runtime instances with separate consumer groups and configs, for
example a replay/rebuild runtime using `startFrom: "earliest"` and a live-tail
runtime using `startFrom: "latest"`.
