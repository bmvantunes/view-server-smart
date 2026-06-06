import { describe, expectTypeOf, it } from "@effect/vitest";
import type { ViewServerLiveSubscription } from "@view-server/client";
import { defineViewServerConfig, type ViewServerRuntimeError } from "@view-server/config";
import type { Effect } from "effect";
import { Schema } from "effect";
import { createInMemoryViewServer } from "./index";

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

const inMemory = createInMemoryViewServer(viewServer);
const invalidTransportHealthOption = createInMemoryViewServer(viewServer, {
  // @ts-expect-error in-memory does not expose Runtime Core transport adapter hooks.
  transportHealth: () => ({
    activeClients: 0,
    activeStreams: 0,
    activeSubscriptions: 0,
    messagesPerSecond: 0,
    bytesPerSecond: 0,
    queuedMessages: 0,
    queuedBytes: 0,
    droppedClients: 0,
    backpressureEvents: 0,
    reconnects: 0,
    lastError: null,
  }),
});

describe("in-memory type contracts", () => {
  it("preserves runtime and live client topic types", () => {
    const publish = inMemory.client.publish("orders", {
      id: "order-1",
      price: 42,
    });
    const subscription = inMemory.liveClient.subscribe("orders", {
      select: ["id"],
    });
    const invalidPatch = inMemory.client.patch("orders", "order-1", {
      price: 10,
      // @ts-expect-error patches cannot contain fields outside the topic schema.
      prcie: 10,
    });

    expectTypeOf<Effect.Error<typeof publish>>().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<Effect.Success<typeof subscription>>().toEqualTypeOf<
      ViewServerLiveSubscription<{
        readonly id: string;
      }>
    >();
    expectTypeOf(inMemory.liveClient).not.toHaveProperty("subscribeRuntime");
    expectTypeOf(invalidPatch).not.toBeAny();
    expectTypeOf(invalidTransportHealthOption).not.toBeAny();
  });
});
