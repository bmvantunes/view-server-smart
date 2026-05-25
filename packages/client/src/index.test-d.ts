import { describe, expectTypeOf, it } from "@effect/vitest";
import { defineViewServerConfig } from "@view-server/config";
import type {
  ViewServerHealth,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@view-server/config";
import type { Effect } from "effect";
import { Schema } from "effect";
import type { ViewServerLiveClient, ViewServerLiveSubscription } from "./index";

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

declare const client: ViewServerLiveClient<typeof viewServer.topics>;

describe("client type contracts", () => {
  it("preserves selected row types through live subscriptions", () => {
    const subscription = client.subscribe("orders", {
      select: ["id"],
    });

    expectTypeOf<Effect.Success<typeof subscription>>().toEqualTypeOf<
      ViewServerLiveSubscription<{
        readonly id: string;
      }>
    >();
    expectTypeOf<Effect.Error<typeof subscription>>().toEqualTypeOf<
      ViewServerRuntimeError | ViewServerTransportError
    >();
  });

  it("rejects nullish selected fields", () => {
    const undefinedSelectedField = client.subscribe("orders", {
      select: [
        // @ts-expect-error selected fields must be topic field names, not undefined.
        undefined,
      ],
    });

    const nullSelectedField = client.subscribe("orders", {
      select: [
        // @ts-expect-error selected fields must be topic field names, not null.
        null,
      ],
    });

    expectTypeOf(undefinedSelectedField).not.toBeAny();
    expectTypeOf(nullSelectedField).not.toBeAny();
  });

  it("exposes health as a read-only ref", () => {
    expectTypeOf(client.health.value).toEqualTypeOf<ViewServerHealth<typeof viewServer.topics>>();

    // @ts-expect-error public live client health must not expose mutation.
    client.health.set(client.health.value);
  });
});
