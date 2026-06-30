import { describe, expectTypeOf, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  grpc,
  kafka,
  type GrpcFeedDefinition,
  type GrpcRuntimeClients,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import type { ViewServerAuth } from "@effect-view-server/server";
import type { Config } from "effect";
import { Effect, Schema } from "effect";
import type { HttpServerError } from "effect/unstable/http";
import {
  makeViewServerRuntime,
  runViewServerRuntime,
  type ViewServerRuntime,
  type ViewServerGrpcRuntimeOptions,
  type ViewServerGrpcIngressError,
  type ViewServerKafkaIngressError,
  type ViewServerTcpPublishIngressError,
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

const leasedViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
      source: grpc.leased({
        routeBy: ["id"],
      }),
    },
  },
});

const materializedGrpcViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
      source: grpc.materialized(),
    },
  },
});

const runtimeEffect = makeViewServerRuntime(viewServer);
const leasedRuntimeEffect = makeViewServerRuntime(leasedViewServer);
const runtimeWithGroupedAdmissionLimits = makeViewServerRuntime(viewServer, {
  groupedIncrementalAdmissionLimits: {
    maxGroups: 1,
  },
});
const runtimeWithAuth = makeViewServerRuntime(viewServer, {
  auth: {
    validateRequest: () =>
      Effect.succeed({
        forwardedHeaders: {},
        id: null,
        systemHeaders: {},
      }),
  } satisfies ViewServerAuth,
});
const runEffect = runViewServerRuntime(viewServer);
declare const runtime: Effect.Success<typeof runtimeEffect>;
declare const leasedRuntime: Effect.Success<typeof leasedRuntimeEffect>;
declare const grpcRuntimeClients: GrpcRuntimeClients;
declare const grpcOrdersFeed: GrpcFeedDefinition<
  typeof materializedGrpcViewServer.topics,
  typeof grpcRuntimeClients
>;

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
  getSafeRowKey: ({ key }) => key,
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
    expectTypeOf(runtime.metricsUrl).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["metricsUrl"]
    >();
    expectTypeOf(runtime.tcpPublishUrl).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["tcpPublishUrl"]
    >();
    expectTypeOf(runtime.health).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["health"]
    >();
    expectTypeOf(runtime.close).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["close"]
    >();
    expectTypeOf<Effect.Success<typeof runEffect>>().toEqualTypeOf<never>();
    expectTypeOf<Effect.Error<typeof runEffect>>().toEqualTypeOf<
      | HttpServerError.ServeError
      | Config.ConfigError
      | ViewServerRuntimeError
      | ViewServerKafkaIngressError
      | ViewServerGrpcIngressError
      | ViewServerTcpPublishIngressError
    >();
    expectTypeOf<Effect.Success<typeof runtimeWithGroupedAdmissionLimits>>().toMatchTypeOf<
      ViewServerRuntime<typeof viewServer.topics>
    >();
    expectTypeOf<Effect.Success<typeof runtimeWithAuth>>().toMatchTypeOf<
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
    const leasedSubscribe = leasedRuntime.liveClient.subscribe("orders", {
      where: {
        id: { eq: "order-1" },
      },
      select: ["id"],
    });

    expectTypeOf<Effect.Error<typeof publish>>().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf(subscribe).not.toBeAny();
    expectTypeOf(leasedSubscribe).not.toBeAny();

    const missingRouteQuery = {
      select: ["id"],
    } satisfies {
      readonly select: readonly ["id"];
    };
    const shorthandRouteQuery = {
      where: {
        id: "order-1",
      },
      select: ["id"],
    } satisfies {
      readonly where: {
        readonly id: "order-1";
      };
      readonly select: readonly ["id"];
    };
    // @ts-expect-error leased gRPC snapshots are live-subscription-only.
    const invalidLeasedSnapshot = leasedRuntime.client.snapshot("orders", missingRouteQuery);
    // @ts-expect-error leased gRPC topics reject direct runtime publishes.
    const invalidLeasedPublish = leasedRuntime.client.publish("orders", {
      id: "order-1",
      price: 10,
    });
    // @ts-expect-error leased gRPC topics reject direct runtime batch publishes.
    const invalidLeasedPublishMany = leasedRuntime.client.publishMany("orders", [
      {
        id: "order-1",
        price: 10,
      },
    ]);
    // @ts-expect-error leased gRPC topics reject direct runtime patches.
    const invalidLeasedPatch = leasedRuntime.client.patch("orders", "order-1", {
      price: 10,
    });
    // @ts-expect-error leased gRPC topics reject direct runtime deletes.
    const invalidLeasedDelete = leasedRuntime.client.delete("orders", "order-1");
    // @ts-expect-error leased gRPC runtimes reject direct runtime reset.
    const _invalidLeasedReset = leasedRuntime.client.reset();
    const invalidLeasedSubscribe = leasedRuntime.liveClient.subscribe(
      "orders",
      // @ts-expect-error leased gRPC route filters must be exact eq predicates.
      shorthandRouteQuery,
    );
    expectTypeOf(invalidLeasedSnapshot).not.toBeAny();
    expectTypeOf(invalidLeasedPublish).not.toBeAny();
    expectTypeOf(invalidLeasedPublishMany).not.toBeAny();
    expectTypeOf(invalidLeasedPatch).not.toBeAny();
    expectTypeOf(invalidLeasedDelete).not.toBeAny();
    expectTypeOf(invalidLeasedSubscribe).not.toBeAny();
    expectTypeOf(runtime.client.reset).not.toBeAny();

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
    // @ts-expect-error runtime options reject string ports.
    const invalidOptions = makeViewServerRuntime(viewServer, {
      websocketPort: "8080",
    });
    const tcpPublishPortOptions = makeViewServerRuntime(viewServer, {
      tcpPublishMaxConnections: 16,
      tcpPublishPort: 8081,
    });
    expectTypeOf<Effect.Success<typeof tcpPublishPortOptions>>().toMatchTypeOf<
      ViewServerRuntime<typeof viewServer.topics>
    >();
    // @ts-expect-error runtime TCP publish port rejects string ports.
    const invalidTcpPublishPortOptions = makeViewServerRuntime(viewServer, {
      tcpPublishPort: "8081",
    });
    // @ts-expect-error runtime TCP publish connection cap rejects string values.
    const invalidTcpPublishMaxConnectionsOptions = makeViewServerRuntime(viewServer, {
      tcpPublishMaxConnections: "16",
      tcpPublishPort: 8081,
    });
    // @ts-expect-error runtime paths must be absolute HTTP paths.
    const invalidPathOptions = makeViewServerRuntime(viewServer, {
      rpcPath: "runtime-rpc",
    });
    const invalidAuthOptions = {
      auth: {
        validateRequest: () => "not an effect",
      },
    };
    // @ts-expect-error runtime auth validator must return an Effect.
    invalidAuthOptions satisfies ViewServerRuntimeOptions<typeof viewServer.topics>;
    // @ts-expect-error runtime health paths must be absolute HTTP paths.
    const invalidHealthPathOptions = makeViewServerRuntime(viewServer, {
      healthPath: "runtime-health",
    });
    // @ts-expect-error runtime metrics paths must be absolute HTTP paths.
    const invalidMetricsPathOptions = makeViewServerRuntime(viewServer, {
      metricsPath: "runtime-metrics",
    });
    // @ts-expect-error runtime RPC path must be a concrete slash-prefixed client URL path.
    const invalidWildcardRpcPathOptions = makeViewServerRuntime(viewServer, {
      rpcPath: "*",
    });
    // @ts-expect-error runtime health path must be a concrete slash-prefixed client URL path.
    const invalidWildcardHealthPathOptions = makeViewServerRuntime(viewServer, {
      healthPath: "*",
    });
    // @ts-expect-error runtime metrics path must be a concrete slash-prefixed client URL path.
    const invalidWildcardMetricsPathOptions = makeViewServerRuntime(viewServer, {
      metricsPath: "*",
    });
    const invalidGroupedAdmissionLimitKey = makeViewServerRuntime(viewServer, {
      groupedIncrementalAdmissionLimits: {
        // @ts-expect-error grouped admission limits reject unknown keys.
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
            getSafeRowKey: ({ key }) => key,
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
            getSafeRowKey: ({ key }) => key,
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
            getSafeRowKey: ({ key }) => key,
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
        // @ts-expect-error committed Kafka start fallback must be earliest, latest, or fail.
        startFrom: {
          committedConsumerGroup: "view-server-existing-group",
          fallback: "middle",
        },
        topics: {
          orders: usaKafkaTopic({
            regions: ["usa"],
            value: kafka.json(Order),
            key: kafka.stringKey(),
            viewServerTopic: "orders",
            getSafeRowKey: ({ key }) => key,
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
            getSafeRowKey: ({ key }) => key,
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
            getSafeRowKey: ({ key }) => key,
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
            getSafeRowKey: ({ key }) => key,
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
            getSafeRowKey: ({ key }) => key,
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
    const runtimeWithGrpc = makeViewServerRuntime(materializedGrpcViewServer, {
      grpc: {
        clients: grpcRuntimeClients,
        feeds: {
          ordersFeed: grpcOrdersFeed,
        },
        materializedReconnect: {
          delay: "100 millis",
          maxReconnects: 5,
        },
      },
    });
    const invalidGrpcReconnectKey = makeViewServerRuntime(materializedGrpcViewServer, {
      grpc: {
        clients: grpcRuntimeClients,
        feeds: {
          ordersFeed: grpcOrdersFeed,
        },
        materializedReconnect: {
          delay: "100 millis",
          maxReconnects: 5,
          // @ts-expect-error runtime gRPC reconnect options reject unknown fields.
          maxAttempts: 5,
        },
      },
    });
    const invalidGrpcReconnectMax = {
      delay: "100 millis",
      // @ts-expect-error runtime gRPC reconnect maxReconnects must be a number.
      maxReconnects: "5",
    } satisfies NonNullable<
      ViewServerGrpcRuntimeOptions<
        typeof materializedGrpcViewServer.topics
      >["materializedReconnect"]
    >;
    const invalidGrpcReconnectDelay = {
      // @ts-expect-error runtime gRPC reconnect delay must be a Duration.Input.
      delay: false,
      maxReconnects: 5,
    } satisfies NonNullable<
      ViewServerGrpcRuntimeOptions<
        typeof materializedGrpcViewServer.topics
      >["materializedReconnect"]
    >;
    const invalidGrpcOptionKey = makeViewServerRuntime(materializedGrpcViewServer, {
      grpc: {
        clients: grpcRuntimeClients,
        feeds: {
          ordersFeed: grpcOrdersFeed,
        },
        // @ts-expect-error runtime gRPC options reject unknown fields.
        feedz: {},
      },
    });
    expectTypeOf(invalidPublish).not.toBeAny();
    expectTypeOf(invalidSubscribe).not.toBeAny();
    expectTypeOf(invalidTopicPublish).not.toBeAny();
    expectTypeOf(invalidSnapshot).not.toBeAny();
    expectTypeOf(invalidOptions).not.toBeAny();
    expectTypeOf(invalidPathOptions).not.toBeAny();
    expectTypeOf(invalidHealthPathOptions).not.toBeAny();
    expectTypeOf(invalidMetricsPathOptions).not.toBeAny();
    expectTypeOf(invalidWildcardRpcPathOptions).not.toBeAny();
    expectTypeOf(invalidWildcardHealthPathOptions).not.toBeAny();
    expectTypeOf(invalidWildcardMetricsPathOptions).not.toBeAny();
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
    expectTypeOf<Effect.Success<typeof runtimeWithGrpc>>().toMatchTypeOf<
      ViewServerRuntime<typeof materializedGrpcViewServer.topics>
    >();
    expectTypeOf(invalidGrpcReconnectKey).not.toBeAny();
    expectTypeOf(invalidGrpcReconnectMax).not.toBeAny();
    expectTypeOf(invalidGrpcReconnectDelay).not.toBeAny();
    expectTypeOf(invalidGrpcOptionKey).not.toBeAny();
    expectTypeOf(invalidTcpPublishPortOptions).not.toBeAny();
    expectTypeOf(invalidTcpPublishMaxConnectionsOptions).not.toBeAny();
    expectTypeOf<ViewServerRuntimeOptions>().not.toHaveProperty("port");
    expectTypeOf<ViewServerRuntimeOptions>().not.toHaveProperty("path");
    expectTypeOf<ViewServerRuntimeOptions>().toHaveProperty("tcpPublishMaxConnections");
    expectTypeOf<ViewServerRuntimeOptions>().toHaveProperty("tcpPublishHost");
    expectTypeOf<ViewServerRuntimeOptions>().toHaveProperty("tcpPublishPort");
    expectTypeOf<ViewServerRuntimeOptions>().toHaveProperty("grpc");
  });
});
