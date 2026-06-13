import { describe, expectTypeOf, it } from "@effect/vitest";
import { defineViewServerConfig, kafka, type ViewServerRuntimeError } from "@view-server/config";
import type { Config, Effect } from "effect";
import { Schema } from "effect";
import type { HttpServerError } from "effect/unstable/http";
import {
  makeViewServerRuntime,
  runViewServerRuntime,
  type ViewServerRuntime,
  type ViewServerKafkaIngressError,
  type ViewServerRuntimeOptions,
} from "./index";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

const runtimeEffect = makeViewServerRuntime(viewServer);
const runtimeWithGroupedAdmissionLimits = makeViewServerRuntime(viewServer, {
  groupedIncrementalAdmissionLimits: {
    maxGroups: 1,
  },
});
const runEffect = runViewServerRuntime(viewServer);
declare const runtime: Effect.Success<typeof runtimeEffect>;

const usaKafkaRegions = {
  usa: "localhost:9092",
};
const londonKafkaRegions = {
  london: "localhost:9093",
};
const usaKafkaTopic = viewServer.kafkaTopic<typeof usaKafkaRegions>();
const londonKafkaTopic = viewServer.kafkaTopic<typeof londonKafkaRegions>()({
  regions: ["london"],
  value: kafka.json(Order),
  key: kafka.stringKey(),
  viewServerTopic: "orders",
  mapping: ({ key, value }) => ({
    id: key,
    price: value.price,
  }),
});

describe("runtime type contracts", () => {
  it("preserves configured topic types through runtime clients", () => {
    expectTypeOf(runtime.url).toEqualTypeOf<ViewServerRuntime<typeof viewServer.topics>["url"]>();
    expectTypeOf(runtime.healthUrl).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["healthUrl"]
    >();
    expectTypeOf(runtime.health).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["health"]
    >();
    expectTypeOf(runtime.close).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["close"]
    >();
    expectTypeOf<Effect.Success<typeof runEffect>>().toEqualTypeOf<never>();
    expectTypeOf<Effect.Error<typeof runEffect>>().toEqualTypeOf<
      HttpServerError.ServeError | Config.ConfigError | ViewServerKafkaIngressError
    >();
    expectTypeOf<Effect.Success<typeof runtimeWithGroupedAdmissionLimits>>().toMatchTypeOf<
      ViewServerRuntime<typeof viewServer.topics>
    >();

    const publish = runtime.client.publish("orders", {
      id: "order-1",
      price: 10,
    });
    expectTypeOf<Parameters<typeof runtime.client.publish>>().toEqualTypeOf<
      Parameters<ViewServerRuntime<typeof viewServer.topics>["client"]["publish"]>
    >();
    const subscribe = runtime.liveClient.subscribe("orders", {
      select: ["id", "price"],
    });

    expectTypeOf<Effect.Error<typeof publish>>().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf(subscribe).not.toBeAny();

    const invalidPublish = runtime.client.publish("orders", {
      id: "order-1",
      price: 10,
      // @ts-expect-error runtime mutation client rejects fields outside the topic row.
      prcie: 10,
    });

    const invalidSubscribe = runtime.liveClient.subscribe("orders", {
      // @ts-expect-error runtime live client rejects fields outside the topic row.
      select: ["prcie"],
    });
    const invalidTopicPublish = runtime.client.publish(
      // @ts-expect-error runtime mutation client rejects unknown topics.
      "missing",
      {
        id: "order-1",
        price: 10,
      },
    );
    const invalidSnapshot = runtime.client.snapshot("orders", {
      // @ts-expect-error invalid query collapse keeps selected fields from being accepted.
      select: ["id"],
      where: {
        // @ts-expect-error runtime query client rejects unknown filter fields.
        prcie: { gte: 10 },
      },
    });
    const invalidOptions = makeViewServerRuntime(viewServer, {
      // @ts-expect-error runtime options reject string ports.
      websocketPort: "8080",
    });
    const invalidTcpPublishPortOptions = makeViewServerRuntime(viewServer, {
      // @ts-expect-error TCP publish ingress is not wired by the runtime package yet.
      tcpPublishPort: 8081,
    });
    const invalidPathOptions = makeViewServerRuntime(viewServer, {
      // @ts-expect-error runtime paths must be absolute HTTP paths.
      rpcPath: "runtime-rpc",
    });
    const invalidHealthPathOptions = makeViewServerRuntime(viewServer, {
      // @ts-expect-error runtime health paths must be absolute HTTP paths.
      healthPath: "runtime-health",
    });
    const invalidWildcardRpcPathOptions = makeViewServerRuntime(viewServer, {
      // @ts-expect-error runtime RPC path must be a concrete slash-prefixed client URL path.
      rpcPath: "*",
    });
    const invalidWildcardHealthPathOptions = makeViewServerRuntime(viewServer, {
      // @ts-expect-error runtime health path must be a concrete slash-prefixed client URL path.
      healthPath: "*",
    });
    const invalidGroupedAdmissionLimitKey = makeViewServerRuntime(viewServer, {
      groupedIncrementalAdmissionLimits: {
        // @ts-expect-error grouped admission limit keys are exact.
        maxGroupz: 1,
      },
    });
    const invalidGroupedAdmissionLimitValue = makeViewServerRuntime(viewServer, {
      groupedIncrementalAdmissionLimits: {
        // @ts-expect-error grouped admission limits must be numeric.
        maxGroups: "1",
      },
    });
    const runtimeWithKafka = makeViewServerRuntime(viewServer, {
      kafka: {
        consumerGroupId: "view-server-type-test",
        regions: usaKafkaRegions,
        startFrom: "latest",
        topics: {
          orders: usaKafkaTopic({
            regions: ["usa"],
            value: kafka.json(Order),
            key: kafka.stringKey(),
            viewServerTopic: "orders",
            mapping: ({ key, value }) => ({
              id: key,
              price: value.price,
            }),
          }),
        },
      },
    });
    const runtimeWithCommittedKafkaStart = makeViewServerRuntime(viewServer, {
      kafka: {
        consumerGroupId: "view-server-type-test",
        regions: usaKafkaRegions,
        startFrom: {
          committedConsumerGroup: "view-server-existing-group",
          fallback: "fail",
        },
        topics: {
          orders: usaKafkaTopic({
            regions: ["usa"],
            value: kafka.json(Order),
            key: kafka.stringKey(),
            viewServerTopic: "orders",
            mapping: ({ key, value }) => ({
              id: key,
              price: value.price,
            }),
          }),
        },
      },
    });
    const invalidKafkaStartFrom = makeViewServerRuntime(viewServer, {
      kafka: {
        consumerGroupId: "view-server-type-test",
        regions: usaKafkaRegions,
        // @ts-expect-error runtime Kafka startFrom only accepts earliest, latest, or committed group config.
        startFrom: "middle",
        topics: {
          orders: usaKafkaTopic({
            regions: ["usa"],
            value: kafka.json(Order),
            key: kafka.stringKey(),
            viewServerTopic: "orders",
            mapping: ({ key, value }) => ({
              id: key,
              price: value.price,
            }),
          }),
        },
      },
    });
    const invalidCommittedKafkaStartFallback = makeViewServerRuntime(viewServer, {
      kafka: {
        consumerGroupId: "view-server-type-test",
        regions: usaKafkaRegions,
        startFrom: {
          committedConsumerGroup: "view-server-existing-group",
          // @ts-expect-error committed Kafka start fallback must be earliest, latest, or fail.
          fallback: "middle",
        },
        topics: {
          orders: usaKafkaTopic({
            regions: ["usa"],
            value: kafka.json(Order),
            key: kafka.stringKey(),
            viewServerTopic: "orders",
            mapping: ({ key, value }) => ({
              id: key,
              price: value.price,
            }),
          }),
        },
      },
    });
    const invalidCommittedKafkaStartMissingGroup = makeViewServerRuntime(viewServer, {
      kafka: {
        consumerGroupId: "view-server-type-test",
        regions: usaKafkaRegions,
        // @ts-expect-error committed Kafka start config requires committedConsumerGroup.
        startFrom: {
          fallback: "earliest",
        },
        topics: {
          orders: usaKafkaTopic({
            regions: ["usa"],
            value: kafka.json(Order),
            key: kafka.stringKey(),
            viewServerTopic: "orders",
            mapping: ({ key, value }) => ({
              id: key,
              price: value.price,
            }),
          }),
        },
      },
    });
    const invalidCommittedKafkaStartKey = makeViewServerRuntime(viewServer, {
      kafka: {
        consumerGroupId: "view-server-type-test",
        regions: usaKafkaRegions,
        startFrom: {
          committedConsumerGroup: "view-server-existing-group",
          // @ts-expect-error committed Kafka start config rejects unknown keys.
          committedConsumerGroupId: "view-server-typo",
        },
        topics: {
          orders: usaKafkaTopic({
            regions: ["usa"],
            value: kafka.json(Order),
            key: kafka.stringKey(),
            viewServerTopic: "orders",
            mapping: ({ key, value }) => ({
              id: key,
              price: value.price,
            }),
          }),
        },
      },
    });
    const invalidKafkaOptionKey = makeViewServerRuntime(viewServer, {
      kafka: {
        consumerGroupId: "view-server-type-test",
        // @ts-expect-error runtime Kafka options reject misspelled consumer group keys.
        consumerGroupID: "view-server-typo",
        regions: usaKafkaRegions,
        topics: {
          orders: usaKafkaTopic({
            regions: ["usa"],
            value: kafka.json(Order),
            key: kafka.stringKey(),
            viewServerTopic: "orders",
            mapping: ({ key, value }) => ({
              id: key,
              price: value.price,
            }),
          }),
        },
      },
    });
    const invalidMissingKafkaConsumerGroup = makeViewServerRuntime(viewServer, {
      // @ts-expect-error runtime Kafka options require an explicit per-runtime consumer group id.
      kafka: {
        regions: usaKafkaRegions,
        topics: {
          orders: usaKafkaTopic({
            regions: ["usa"],
            value: kafka.json(Order),
            key: kafka.stringKey(),
            viewServerTopic: "orders",
            mapping: ({ key, value }) => ({
              id: key,
              price: value.price,
            }),
          }),
        },
      },
    });
    const invalidKafkaRegionRuntime = makeViewServerRuntime(viewServer, {
      kafka: {
        consumerGroupId: "view-server-type-test",
        regions: usaKafkaRegions,
        topics: {
          // @ts-expect-error direct runtime Kafka topics must match runtime kafka.regions keys.
          orders: londonKafkaTopic,
        },
      },
    });
    expectTypeOf(invalidPublish).not.toBeAny();
    expectTypeOf(invalidSubscribe).not.toBeAny();
    expectTypeOf(invalidTopicPublish).not.toBeAny();
    expectTypeOf(invalidSnapshot).not.toBeAny();
    expectTypeOf(invalidOptions).not.toBeAny();
    expectTypeOf(invalidTcpPublishPortOptions).not.toBeAny();
    expectTypeOf(invalidPathOptions).not.toBeAny();
    expectTypeOf(invalidHealthPathOptions).not.toBeAny();
    expectTypeOf(invalidWildcardRpcPathOptions).not.toBeAny();
    expectTypeOf(invalidWildcardHealthPathOptions).not.toBeAny();
    expectTypeOf(invalidGroupedAdmissionLimitKey).not.toBeAny();
    expectTypeOf(invalidGroupedAdmissionLimitValue).not.toBeAny();
    expectTypeOf<Effect.Success<typeof runtimeWithKafka>>().toMatchTypeOf<
      ViewServerRuntime<typeof viewServer.topics>
    >();
    expectTypeOf<Effect.Success<typeof runtimeWithCommittedKafkaStart>>().toMatchTypeOf<
      ViewServerRuntime<typeof viewServer.topics>
    >();
    expectTypeOf(invalidKafkaStartFrom).not.toBeAny();
    expectTypeOf(invalidCommittedKafkaStartFallback).not.toBeAny();
    expectTypeOf(invalidCommittedKafkaStartMissingGroup).not.toBeAny();
    expectTypeOf(invalidCommittedKafkaStartKey).not.toBeAny();
    expectTypeOf(invalidKafkaOptionKey).not.toBeAny();
    expectTypeOf(invalidMissingKafkaConsumerGroup).not.toBeAny();
    expectTypeOf(invalidKafkaRegionRuntime).not.toBeAny();
    expectTypeOf<ViewServerRuntimeOptions>().not.toHaveProperty("port");
    expectTypeOf<ViewServerRuntimeOptions>().not.toHaveProperty("path");
    expectTypeOf<ViewServerRuntimeOptions>().not.toHaveProperty("tcpPublishPort");
  });
});
