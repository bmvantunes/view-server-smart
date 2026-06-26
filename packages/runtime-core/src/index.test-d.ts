import { describe, expectTypeOf, it } from "@effect/vitest";
import type { ViewServerLiveSubscription } from "@view-server/client";
import { defineViewServerConfig, grpc, type ViewServerRuntimeError } from "@view-server/config";
import type { Effect } from "effect";
import { Schema } from "effect";
import { createViewServerRuntimeCore, makeViewServerRuntimeCore } from "./index";

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

const runtimeCore = createViewServerRuntimeCore(viewServer);
const runtimeCoreEffect = makeViewServerRuntimeCore(viewServer, {});
const runtimeCoreWithGroupedAdmissionLimits = createViewServerRuntimeCore(viewServer, {
  groupedIncrementalAdmissionLimits: {
    maxGroups: 1,
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
const leasedRuntimeCore = createViewServerRuntimeCore(leasedViewServer);

describe("runtime-core type contracts", () => {
  it("preserves runtime and live client topic types", () => {
    const publish = runtimeCore.client.publish("orders", {
      id: "order-1",
      price: 42,
    });
    const subscription = runtimeCore.liveClient.subscribe("orders", {
      select: ["id"],
    });
    const invalidPatch = runtimeCore.client.patch("orders", "order-1", {
      price: 10,
      // @ts-expect-error patches cannot contain fields outside the topic schema.
      prcie: 10,
    });
    const runtimeCoreWithTransportHealth = createViewServerRuntimeCore(viewServer, {
      transportHealth: (health) => {
        expectTypeOf(health.topics.orders.rowCount).toEqualTypeOf<number>();
        return {
          activeClients: 1,
          activeStreams: health.activeSubscriptions,
          activeSubscriptions: health.activeSubscriptions,
          messagesPerSecond: 0,
          bytesPerSecond: 0,
          queuedMessages: health.queuedEvents,
          queuedBytes: 0,
          droppedClients: 0,
          backpressureEvents: health.backpressureEvents,
          reconnects: 0,
          lastError: null,
        };
      },
    });
    const invalidGroupedAdmissionLimitKey = createViewServerRuntimeCore(viewServer, {
      groupedIncrementalAdmissionLimits: {
        // @ts-expect-error grouped admission limit keys are exact.
        maxGroupz: 1,
      },
    });
    const invalidGroupedAdmissionLimitValue = createViewServerRuntimeCore(viewServer, {
      groupedIncrementalAdmissionLimits: {
        // @ts-expect-error grouped admission limits must be numeric.
        maxGroups: "1",
      },
    });
    // @ts-expect-error public runtime-core instances must not expose route-bypassing internals.
    const _internalLiveClient = runtimeCore.internalLiveClient;
    type _InternalLiveClientFromMake = Effect.Success<
      typeof runtimeCoreEffect
      // @ts-expect-error public runtime-core factory success must not expose route-bypassing internals.
    >["internalLiveClient"];

    expectTypeOf<Effect.Error<typeof publish>>().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<Effect.Success<typeof runtimeCoreEffect>>().toEqualTypeOf<typeof runtimeCore>();
    expectTypeOf<Effect.Success<typeof subscription>>().toEqualTypeOf<
      ViewServerLiveSubscription<{
        readonly id: string;
      }>
    >();
    expectTypeOf(invalidPatch).not.toBeAny();
    expectTypeOf(runtimeCoreWithTransportHealth.client).toEqualTypeOf<typeof runtimeCore.client>();
    expectTypeOf(runtimeCoreWithGroupedAdmissionLimits.client).toEqualTypeOf<
      typeof runtimeCore.client
    >();
    expectTypeOf(invalidGroupedAdmissionLimitKey).not.toBeAny();
    expectTypeOf(invalidGroupedAdmissionLimitValue).not.toBeAny();
  });

  it("rejects leased gRPC topics from public runtime-core clients", () => {
    const leasedQuery = {
      where: {
        id: { eq: "order-1" },
      },
      select: ["id"],
    } satisfies {
      readonly where: {
        readonly id: {
          readonly eq: "order-1";
        };
      };
      readonly select: readonly ["id"];
    };
    // @ts-expect-error public runtime-core clients reject direct leased gRPC snapshots.
    const invalidLeasedSnapshot = leasedRuntimeCore.client.snapshot("orders", leasedQuery);
    // @ts-expect-error public runtime-core clients reject direct leased gRPC publishes.
    const invalidLeasedPublish = leasedRuntimeCore.client.publish("orders", {
      id: "order-1",
      price: 42,
    });
    // @ts-expect-error public runtime-core clients reject direct leased gRPC batch publishes.
    const invalidLeasedPublishMany = leasedRuntimeCore.client.publishMany("orders", [
      {
        id: "order-1",
        price: 42,
      },
    ]);
    // @ts-expect-error public runtime-core clients reject direct leased gRPC patches.
    const invalidLeasedPatch = leasedRuntimeCore.client.patch("orders", "order-1", {
      price: 10,
    });
    // @ts-expect-error public runtime-core clients reject direct leased gRPC deletes.
    const invalidLeasedDelete = leasedRuntimeCore.client.delete("orders", "order-1");
    // @ts-expect-error public runtime-core clients reject direct leased gRPC reset.
    const invalidLeasedReset = leasedRuntimeCore.client.reset();
    // @ts-expect-error public runtime-core live clients reject direct leased gRPC subscriptions.
    const _invalidLeasedSubscribe = leasedRuntimeCore.liveClient.subscribe("orders", leasedQuery);

    expectTypeOf(invalidLeasedSnapshot).not.toBeAny();
    expectTypeOf(invalidLeasedPublish).not.toBeAny();
    expectTypeOf(invalidLeasedPublishMany).not.toBeAny();
    expectTypeOf(invalidLeasedPatch).not.toBeAny();
    expectTypeOf(invalidLeasedDelete).not.toBeAny();
    expectTypeOf(invalidLeasedReset).not.toBeAny();
  });
});
