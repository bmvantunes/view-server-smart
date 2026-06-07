import { describe, expectTypeOf, it } from "@effect/vitest";
import type { ViewServerLiveSubscription } from "@view-server/client";
import { defineViewServerConfig, type ViewServerRuntimeError } from "@view-server/config";
import type { Effect } from "effect";
import { Schema } from "effect";
import { createViewServerRuntimeCore } from "./index";

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
const runtimeCoreWithGroupedAdmissionLimits = createViewServerRuntimeCore(viewServer, {
  groupedIncrementalAdmissionLimits: {
    maxGroups: 1,
  },
});

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

    expectTypeOf<Effect.Error<typeof publish>>().toEqualTypeOf<ViewServerRuntimeError>();
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
});
