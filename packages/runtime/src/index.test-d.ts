import { describe, expectTypeOf, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  grpc,
  kafka,
  type GrpcConnectClientDefinition,
  type GrpcFeedDefinition,
  type GrpcRuntimeClients,
  type RuntimeRegions,
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
  type ViewServerRuntimeOptionsInput,
  type ViewServerRuntimeOptions,
} from "./index";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
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

const multiMaterializedGrpcViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
      grpcSource: grpc.materialized(),
    },
    trades: {
      schema: Trade,
      key: "id",
      grpcSource: grpc.materialized(),
    },
  },
});

const usaKafkaRegions = {
  usa: "localhost:9092",
};
const londonKafkaRegions = {
  london: "localhost:9093",
};
const broadKafkaRegions: RuntimeRegions = usaKafkaRegions;

const kafkaOwnedViewServer = defineViewServerConfig({
  kafka: usaKafkaRegions,
  topics: {
    orders: {
      schema: Order,
      key: "id",
      kafkaSource: kafka.source({
        topic: "orders-source",
        regions: ["usa"],
        value: kafka.json(Order),
        key: kafka.stringKey(),
        map: ({ value, rowKey }) => ({
          id: rowKey,
          price: value.price,
        }),
      }),
    },
  },
});

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

const runtimeEffect = makeViewServerRuntime(viewServer);
const legacyKafkaRuntimeEffect = makeViewServerRuntime(viewServer, {
  kafka: {
    consumerGroupId: "view-server-legacy-kafka-owned-type-test",
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
const kafkaOwnedRuntimeEffect = makeViewServerRuntime(kafkaOwnedViewServer, {
  kafka: {
    consumerGroupId: "view-server-kafka-owned-type-test",
  },
});
const kafkaOwnedRuntimeWithExplicitRegionsEffect = makeViewServerRuntime(kafkaOwnedViewServer, {
  kafka: {
    consumerGroupId: "view-server-kafka-owned-explicit-regions-type-test",
    regions: usaKafkaRegions,
  },
});
const invalidKafkaOwnedRuntimeWithWrongRegions = makeViewServerRuntime(kafkaOwnedViewServer, {
  kafka: {
    consumerGroupId: "view-server-kafka-owned-wrong-regions-type-test",
    // @ts-expect-error runtime Kafka regions for a source-owned topic must include the source regions.
    regions: londonKafkaRegions,
  },
});
const _invalidKafkaOwnedRuntimeWithBroadRegions = makeViewServerRuntime(kafkaOwnedViewServer, {
  kafka: {
    consumerGroupId: "view-server-kafka-owned-broad-regions-type-test",
    // @ts-expect-error source-owned Kafka runtime regions must be exact enough to prove source coverage.
    regions: broadKafkaRegions,
  },
});
// @ts-expect-error Kafka-owned source configs require runtime Kafka options with a consumer group.
const invalidKafkaOwnedRuntimeWithoutOptions = makeViewServerRuntime(kafkaOwnedViewServer);
const invalidKafkaOwnedRuntimeWithExplicitTopics = makeViewServerRuntime(kafkaOwnedViewServer, {
  kafka: {
    // @ts-expect-error Kafka-owned source configs reject explicit runtime Kafka topics.
    consumerGroupId: "view-server-kafka-owned-explicit-topics",
    // @ts-expect-error Kafka-owned source configs reject explicit runtime Kafka topics.
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
declare const legacyKafkaRuntime: Effect.Success<typeof legacyKafkaRuntimeEffect>;
declare const kafkaOwnedRuntime: Effect.Success<typeof kafkaOwnedRuntimeEffect>;
declare const grpcRuntimeClients: GrpcRuntimeClients;
declare const exactGrpcRuntimeClients: {
  readonly ordersClient: GrpcConnectClientDefinition;
};
declare const grpcOrdersFeed: GrpcFeedDefinition<
  typeof materializedGrpcViewServer.topics,
  typeof grpcRuntimeClients
>;
declare const broadMaterializedGrpcFeed: GrpcFeedDefinition<
  typeof multiMaterializedGrpcViewServer.topics,
  typeof grpcRuntimeClients
>;
type MultiGrpcSourceVisible = typeof multiMaterializedGrpcViewServer.topics.orders extends {
  readonly grpcSource: object;
}
  ? true
  : false;
expectTypeOf<MultiGrpcSourceVisible>().toEqualTypeOf<true>();
expectTypeOf(broadMaterializedGrpcFeed.topic).toEqualTypeOf<"orders" | "trades">();
declare const leasedGrpcOrdersFeed: GrpcFeedDefinition<
  typeof leasedViewServer.topics,
  typeof grpcRuntimeClients
>;
const materializedGrpcViewServerWithConfigClients = defineViewServerConfig({
  grpc: {
    clients: exactGrpcRuntimeClients,
  },
  topics: {
    orders: {
      schema: Order,
      key: "id",
      source: grpc.materialized(),
    },
  },
});
declare const grpcOrdersFeedForConfigClients: GrpcFeedDefinition<
  typeof materializedGrpcViewServerWithConfigClients.topics,
  typeof exactGrpcRuntimeClients
>;
const materializedGrpcRuntimeEffect = makeViewServerRuntime(materializedGrpcViewServer, {
  grpc: {
    clients: grpcRuntimeClients,
    feeds: {
      ordersFeed: grpcOrdersFeed,
    },
  },
});
const leasedRuntimeEffect = makeViewServerRuntime(leasedViewServer, {
  grpc: {
    clients: grpcRuntimeClients,
    feeds: {
      ordersFeed: leasedGrpcOrdersFeed,
    },
  },
});
declare const leasedRuntime: Effect.Success<typeof leasedRuntimeEffect>;
const invalidMaterializedGrpcRuntimeWithoutClients = makeViewServerRuntime(
  materializedGrpcViewServer,
  {
    // @ts-expect-error runtime gRPC feeds require clients when config.grpc.clients is absent.
    grpc: {
      feeds: {
        ordersFeed: grpcOrdersFeed,
      },
    },
  },
);
const materializedGrpcRuntimeWithConfigClientsEffect = makeViewServerRuntime(
  materializedGrpcViewServerWithConfigClients,
  {
    grpc: {
      feeds: {
        ordersFeed: grpcOrdersFeedForConfigClients,
      },
    },
  },
);
// @ts-expect-error gRPC-owned source configs require runtime gRPC feed options.
const _invalidMaterializedGrpcRuntimeWithoutOptions = makeViewServerRuntime(
  materializedGrpcViewServer,
);
const _invalidMaterializedGrpcRuntimeWithoutGrpc = makeViewServerRuntime(
  materializedGrpcViewServer,
  // @ts-expect-error gRPC-owned source configs require runtime gRPC feeds.
  {
    websocketPort: 8080,
  },
);
const _invalidMaterializedGrpcRuntimeWithoutMatchingFeed = makeViewServerRuntime(
  materializedGrpcViewServer,
  {
    grpc: {
      clients: grpcRuntimeClients,
      // @ts-expect-error gRPC-owned source configs require a feed for each source-owned topic.
      feeds: {},
    },
  },
);
const _invalidMultiMaterializedGrpcRuntimeWithBroadFeed = makeViewServerRuntime(
  multiMaterializedGrpcViewServer,
  {
    grpc: {
      clients: grpcRuntimeClients,
      // @ts-expect-error source-owned gRPC feed coverage requires single-topic feed definitions.
      feeds: {
        ordersFeed: broadMaterializedGrpcFeed,
      },
    },
  },
);
declare const materializedGrpcRuntime: Effect.Success<typeof materializedGrpcRuntimeEffect>;
type BroadMultiMaterializedGrpcRuntimeOptions = {
  readonly grpc: {
    readonly clients: typeof grpcRuntimeClients;
    readonly feeds: {
      readonly ordersFeed: typeof broadMaterializedGrpcFeed;
    };
  };
};
const _invalidBroadFeedRuntimeOptions: ViewServerRuntimeOptionsInput<
  typeof multiMaterializedGrpcViewServer.topics,
  RuntimeRegions,
  typeof grpcRuntimeClients,
  BroadMultiMaterializedGrpcRuntimeOptions
> = {
  grpc: {
    clients: grpcRuntimeClients,
    // @ts-expect-error source-owned gRPC feed coverage requires exact single-topic feed definitions.
    feeds: {
      ordersFeed: broadMaterializedGrpcFeed,
    },
  },
};

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
    const kafkaOwnedSnapshot = kafkaOwnedRuntime.client.snapshot("orders", {
      select: ["id", "price"],
    });
    const materializedGrpcSnapshot = materializedGrpcRuntime.client.snapshot("orders", {
      select: ["id", "price"],
    });
    const legacyKafkaSnapshot = legacyKafkaRuntime.client.snapshot("orders", {
      select: ["id", "price"],
    });

    expectTypeOf<Effect.Error<typeof publish>>().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf(subscribe).not.toBeAny();
    expectTypeOf(leasedSubscribe).not.toBeAny();
    expectTypeOf(kafkaOwnedSnapshot).not.toBeAny();
    expectTypeOf(materializedGrpcSnapshot).not.toBeAny();
    expectTypeOf(legacyKafkaSnapshot).not.toBeAny();

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
    // @ts-expect-error Kafka-owned topics reject direct runtime publishes.
    const invalidKafkaOwnedPublish = kafkaOwnedRuntime.client.publish("orders", {
      id: "order-1",
      price: 10,
    });
    // @ts-expect-error Kafka-owned topics reject direct runtime patches.
    const invalidKafkaOwnedPatch = kafkaOwnedRuntime.client.patch("orders", "order-1", {
      price: 11,
    });
    // @ts-expect-error Kafka-owned topics reject direct runtime deletes.
    const invalidKafkaOwnedDelete = kafkaOwnedRuntime.client.delete("orders", "order-1");
    // @ts-expect-error source-owned runtimes reject direct runtime reset.
    const invalidKafkaOwnedReset = kafkaOwnedRuntime.client.reset();
    // @ts-expect-error legacy runtime Kafka topics reject direct runtime publishes.
    const invalidLegacyKafkaPublish = legacyKafkaRuntime.client.publish("orders", {
      id: "order-1",
      price: 10,
    });
    // @ts-expect-error legacy runtime Kafka topics reject direct runtime batch publishes.
    const invalidLegacyKafkaPublishMany = legacyKafkaRuntime.client.publishMany("orders", [
      {
        id: "order-1",
        price: 10,
      },
    ]);
    // @ts-expect-error legacy runtime Kafka topics reject direct runtime patches.
    const invalidLegacyKafkaPatch = legacyKafkaRuntime.client.patch("orders", "order-1", {
      price: 11,
    });
    // @ts-expect-error legacy runtime Kafka topics reject direct runtime deletes.
    const invalidLegacyKafkaDelete = legacyKafkaRuntime.client.delete("orders", "order-1");
    // @ts-expect-error legacy runtime Kafka topics reject direct runtime reset.
    const invalidLegacyKafkaReset = legacyKafkaRuntime.client.reset();
    // @ts-expect-error materialized gRPC-owned topics reject direct runtime publishes.
    const invalidMaterializedGrpcPublish = materializedGrpcRuntime.client.publish("orders", {
      id: "order-1",
      price: 10,
    });
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
    expectTypeOf(invalidKafkaOwnedPublish).not.toBeAny();
    expectTypeOf(invalidKafkaOwnedPatch).not.toBeAny();
    expectTypeOf(invalidKafkaOwnedDelete).not.toBeAny();
    expectTypeOf(invalidKafkaOwnedReset).not.toBeAny();
    expectTypeOf(invalidLegacyKafkaPublish).not.toBeAny();
    expectTypeOf(invalidLegacyKafkaPublishMany).not.toBeAny();
    expectTypeOf(invalidLegacyKafkaPatch).not.toBeAny();
    expectTypeOf(invalidLegacyKafkaDelete).not.toBeAny();
    expectTypeOf(invalidLegacyKafkaReset).not.toBeAny();
    expectTypeOf(invalidMaterializedGrpcPublish).not.toBeAny();
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
    const invalidOptions = makeViewServerRuntime(viewServer, {
      // @ts-expect-error runtime options reject string ports.
      websocketPort: "8080",
    });
    const tcpPublishPortOptions = makeViewServerRuntime(viewServer, {
      tcpPublishMaxConnections: 16,
      tcpPublishPort: 8081,
    });
    expectTypeOf<Effect.Success<typeof tcpPublishPortOptions>>().toMatchTypeOf<
      ViewServerRuntime<typeof viewServer.topics>
    >();
    const invalidTcpPublishPortOptions = makeViewServerRuntime(viewServer, {
      // @ts-expect-error runtime TCP publish port rejects string ports.
      tcpPublishPort: "8081",
    });
    const invalidTcpPublishMaxConnectionsOptions = makeViewServerRuntime(viewServer, {
      // @ts-expect-error runtime TCP publish connection cap rejects string values.
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
            mapping: ({ key, value }) => ({
              id: key,
              price: value.price,
            }),
          }),
        },
      },
    });
    const invalidLegacyKafkaTopicDiscriminant = makeViewServerRuntime(viewServer, {
      kafka: {
        consumerGroupId: "view-server-legacy-discriminant-type-test",
        regions: usaKafkaRegions,
        topics: {
          orders: {
            ...usaKafkaTopic({
              regions: ["usa"],
              value: kafka.json(Order),
              key: kafka.stringKey(),
              viewServerTopic: "orders",
              mapping: ({ key, value }) => ({
                id: key,
                price: value.price,
              }),
            }),
            // @ts-expect-error legacy runtime Kafka topics reject source-topic discriminants.
            topic: "orders-source",
          },
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
    const invalidKafkaRuntimeWithoutRegions = makeViewServerRuntime(viewServer, {
      // @ts-expect-error direct runtime Kafka topics require runtime kafka.regions when config.kafka is absent.
      kafka: {
        consumerGroupId: "view-server-type-test",
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
    expectTypeOf(invalidLegacyKafkaTopicDiscriminant).not.toBeAny();
    expectTypeOf<Effect.Success<typeof runtimeWithCommittedKafkaStart>>().toMatchTypeOf<
      ViewServerRuntime<typeof viewServer.topics>
    >();
    expectTypeOf<Effect.Success<typeof kafkaOwnedRuntimeWithExplicitRegionsEffect>>().toMatchTypeOf<
      ViewServerRuntime<typeof kafkaOwnedViewServer.topics>
    >();
    expectTypeOf(invalidKafkaOwnedRuntimeWithWrongRegions).not.toBeAny();
    expectTypeOf(invalidKafkaRuntimeWithoutRegions).not.toBeAny();
    expectTypeOf(invalidKafkaStartFrom).not.toBeAny();
    expectTypeOf(invalidCommittedKafkaStartFallback).not.toBeAny();
    expectTypeOf(invalidCommittedKafkaStartMissingGroup).not.toBeAny();
    expectTypeOf(invalidCommittedKafkaStartKey).not.toBeAny();
    expectTypeOf(invalidKafkaOptionKey).not.toBeAny();
    expectTypeOf(invalidMissingKafkaConsumerGroup).not.toBeAny();
    expectTypeOf(invalidKafkaOwnedRuntimeWithoutOptions).not.toBeAny();
    expectTypeOf(invalidKafkaOwnedRuntimeWithExplicitTopics).not.toBeAny();
    expectTypeOf(invalidMaterializedGrpcRuntimeWithoutClients).not.toBeAny();
    expectTypeOf(invalidKafkaRegionRuntime).not.toBeAny();
    expectTypeOf<Effect.Success<typeof runtimeWithGrpc>>().toMatchTypeOf<
      ViewServerRuntime<typeof materializedGrpcViewServer.topics>
    >();
    expectTypeOf<
      Effect.Success<typeof materializedGrpcRuntimeWithConfigClientsEffect>
    >().toMatchTypeOf<
      ViewServerRuntime<typeof materializedGrpcViewServerWithConfigClients.topics>
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
