import { describe, expectTypeOf, it } from "@effect/vitest";
import { defineViewServerConfig } from "effect-view-server";
import type { LiveQueryResult } from "effect-view-server/config";
import { createViewServerReact } from "effect-view-server/react";
import { createInMemoryViewServerReact } from "effect-view-server/react/testing";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { createViewServerWebSocketServer } from "effect-view-server/server";
import { createInMemoryViewServer } from "effect-view-server/in-memory";
import { Schema } from "effect";
import type { ViewServerLiveClient } from "effect-view-server/client";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed"]),
  price: Schema.Number,
  region: Schema.String,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

const react = createViewServerReact(viewServer);

describe("public effect-view-server facade type contracts", () => {
  it("exposes public package subpaths", () => {
    expectTypeOf(defineViewServerConfig).not.toBeAny();
    expectTypeOf(createViewServerReact).not.toBeAny();
    expectTypeOf(createInMemoryViewServerReact).not.toBeAny();
    expectTypeOf(runViewServerRuntime).not.toBeAny();
    expectTypeOf(createViewServerWebSocketServer).not.toBeAny();
    expectTypeOf(createInMemoryViewServer).not.toBeAny();
    expectTypeOf<ViewServerLiveClient<typeof viewServer.topics>>().not.toBeAny();
  });

  it("preserves query result inference through public subpaths", () => {
    const result = react.useLiveQuery("orders", {
      select: ["id", "price"],
      where: {
        status: {
          eq: "open",
        },
      },
      orderBy: [{ field: "price", direction: "desc" }],
      limit: 10,
    });

    expectTypeOf(result).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly price: number;
      }>
    >();
  });

  it("rejects invalid query and config contracts through public subpaths", () => {
    // @ts-expect-error raw queries must explicitly select columns.
    react.useLiveQuery("orders", { where: { status: { eq: "open" } } });

    const unknownProjectedFieldQuery = {
      select: ["missing"],
    } satisfies {
      readonly select: readonly ["missing"];
    };
    // @ts-expect-error projected fields must be topic field names.
    react.useLiveQuery("orders", unknownProjectedFieldQuery);

    const stringRangeFilterQuery = {
      select: ["id"],
      where: {
        status: {
          gte: "open",
        },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: {
        readonly status: {
          readonly gte: "open";
        };
      };
    };
    // @ts-expect-error string fields do not accept range predicates.
    react.useLiveQuery("orders", stringRangeFilterQuery);

    defineViewServerConfig({
      topics: {
        invalidOrders: {
          schema: Order,
          // @ts-expect-error topic keys must reference fields on the topic schema.
          key: "missing",
        },
      },
    });
  });

  it("preserves public in-memory client and React testing provider types", () => {
    const runtime = createInMemoryViewServer(viewServer);
    const testReact = createInMemoryViewServerReact(react);

    expectTypeOf(runtime.client.publish).parameter(0).toEqualTypeOf<"orders">();
    expectTypeOf(runtime.client.publish).parameter(1).toEqualTypeOf<typeof Order.Type>();
    expectTypeOf(testReact.client.publish).parameter(0).toEqualTypeOf<"orders">();
    expectTypeOf(testReact.ViewServerInMemoryProvider).not.toBeAny();
  });
});
