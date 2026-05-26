import { describe, expectTypeOf, it } from "@effect/vitest";
import type { ViewServerLiveClient } from "@view-server/client";
import {
  defineViewServerConfig,
  type LiveQueryResult,
  type ViewServerRuntimeError,
} from "@view-server/config";
import { createViewServerReact as createViewServerReactFromPackage } from "@view-server/react";
import {
  createInMemoryViewServerReact as createInMemoryViewServerReactFromPackageTesting,
  type ViewServerInMemoryOptions as ViewServerInMemoryOptionsFromPackageTesting,
} from "@view-server/react/testing";
import type { Effect } from "effect";
import { Schema } from "effect";
import type { ReactNode } from "react";
import { createViewServerReact } from "./index";
import { ViewServerReactClientProvider } from "./internal";
import { createInMemoryViewServerReact, type ViewServerInMemoryOptions } from "./testing";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
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
const { ViewServerProvider, useLiveQuery, useViewServerHealth, useViewServerHealthSummary } = react;
const ViewServerClientProvider = react[ViewServerReactClientProvider];

const createInMemoryViewServer = (options?: ViewServerInMemoryOptions) =>
  createInMemoryViewServerReact(react, options);

declare const liveClient: ViewServerLiveClient<typeof viewServer.topics>;

declare const dynamicSingleField: "id" | "price";

describe("React type contracts", () => {
  it("preserves selected row result types", () => {
    const selected = useLiveQuery("orders", {
      select: ["id", "price"],
      orderBy: [{ field: "price", direction: "desc" }],
      limit: 5,
    });

    expectTypeOf(selected).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly price: number;
      }>
    >();
  });

  it("requires explicit selected row result types", () => {
    const selectedRows = useLiveQuery("orders", {
      select: ["id", "customerId", "status", "price", "region", "updatedAt"],
      where: {
        status: { eq: "open" },
        customerId: { startsWith: "customer-" },
        price: { gte: 10 },
      },
      orderBy: [{ field: "updatedAt", direction: "asc" }],
      limit: 10,
    });

    expectTypeOf(selectedRows.rows[0]).toEqualTypeOf<
      | {
          readonly id: string;
          readonly customerId: string;
          readonly status: "open" | "closed" | "cancelled";
          readonly price: number;
          readonly region: string;
          readonly updatedAt: number;
        }
      | undefined
    >();
    expectTypeOf(selectedRows.status).toEqualTypeOf<
      "loading" | "ready" | "stale" | "closed" | "error"
    >();
    expectTypeOf(selectedRows.statusCode).toEqualTypeOf<
      | "Ready"
      | "SnapshotStale"
      | "SubscriptionClosed"
      | "TransportError"
      | "BackpressureExceeded"
      | "InvalidTopic"
      | "InvalidRow"
      | "InvalidQuery"
      | "UnsupportedQuery"
      | "RuntimeUnavailable"
      | "RuntimeResetFailed"
      | undefined
    >();
  });

  it("rejects invalid raw query select", () => {
    useLiveQuery("orders", {
      // @ts-expect-error raw queries must explicitly select columns.
      where: {
        status: "open",
      },
    });

    useLiveQuery("orders", {
      // @ts-expect-error raw queries must select at least one column.
      select: [],
    });

    useLiveQuery("orders", {
      select: ["id"],
      where: {
        // @ts-expect-error unknown where fields are rejected.
        prcie: 10,
      },
    });

    useLiveQuery("orders", {
      select: ["id"],
      // @ts-expect-error unknown orderBy fields are rejected.
      orderBy: [
        {
          field: "prcie",
          direction: "asc",
        },
      ],
    });

    useLiveQuery("orders", {
      // @ts-expect-error unknown projected fields are rejected.
      select: ["id", "prcie"],
    });

    useLiveQuery("orders", {
      select: [
        // @ts-expect-error selected fields must be topic field names, not undefined.
        undefined,
      ],
    });

    useLiveQuery("orders", {
      select: [
        // @ts-expect-error selected fields must be topic field names, not null.
        null,
      ],
    });

    const dynamicSingleTupleSelectedFieldsQuery = {
      select: [dynamicSingleField],
    } satisfies {
      readonly select: readonly [typeof dynamicSingleField];
    };
    const dynamicSelected = useLiveQuery("orders", dynamicSingleTupleSelectedFieldsQuery);
    expectTypeOf(dynamicSelected.rows[0]).toEqualTypeOf<
      Partial<{ readonly id: string; readonly price: number }> | undefined
    >();
  });

  it("rejects invalid raw query operators", () => {
    useLiveQuery("orders", {
      select: ["id"],
      where: {
        // @ts-expect-error string fields do not support range filters.
        status: {
          gte: "open",
        },
      },
    });

    useLiveQuery("orders", {
      select: ["id"],
      where: {
        // @ts-expect-error numeric fields do not support string filters.
        price: {
          startsWith: "10",
        },
      },
    });
  });

  it("keeps health and in-memory client keyed by configured topics", () => {
    const health = useViewServerHealth();
    const healthSummary = useViewServerHealthSummary();
    const provider = ViewServerProvider({ url: "ws://127.0.0.1:8080/rpc", children: null });
    const clientProvider = ViewServerClientProvider({ client: liveClient, children: null });
    const inMemoryViewServer = createInMemoryViewServer({ subscriptionQueueCapacity: 1 });
    type Client = typeof inMemoryViewServer.client;
    const publish = inMemoryViewServer.client.publish("orders", {
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    });

    expectTypeOf(health.rows[0]?.rowCount).toEqualTypeOf<number | undefined>();
    expectTypeOf(health.rows[0]?.id).toEqualTypeOf<"orders" | undefined>();
    expectTypeOf(healthSummary.status).toEqualTypeOf<
      "ready" | "degraded" | "starting" | "stopping" | "connecting" | "connected" | "disconnected"
    >();
    expectTypeOf(provider).toEqualTypeOf<ReactNode>();
    expectTypeOf(clientProvider).toEqualTypeOf<ReactNode>();
    expectTypeOf<Parameters<Client["publish"]>>().toEqualTypeOf<
      [topic: "orders", row: typeof Order.Type]
    >();
    expectTypeOf<Effect.Error<typeof publish>>().toEqualTypeOf<ViewServerRuntimeError>();
  });

  it("rejects provider seed data", () => {
    const inMemoryViewServer = createInMemoryViewServer();
    void inMemoryViewServer.ViewServerInMemoryProvider({
      children: null,
      // @ts-expect-error setup data must go through runtime.publish or runtime.publishMany.
      seed: {},
    });
  });

  it("requires testing helpers to reuse React bindings", () => {
    // @ts-expect-error testing helpers need the app binding, not just the config.
    createInMemoryViewServerReact(viewServer);
  });

  it("rejects grouped queries for the in-memory runtime slice", () => {
    const { client } = createInMemoryViewServer();
    const groupedQuery = {
      groupBy: ["status"],
      aggregates: { count: { aggFunc: "count" } },
    };

    const invalidGroupedSnapshot =
      // @ts-expect-error grouped queries are not part of the raw in-memory runtime slice yet.
      client.snapshot("orders", groupedQuery);

    const invalidPatch = client.patch("orders", "order-1", {
      price: 10,
      // @ts-expect-error patches cannot contain fields outside the topic schema.
      prcie: 10,
    });

    expectTypeOf(invalidGroupedSnapshot).not.toBeAny();
    expectTypeOf(invalidPatch).not.toBeAny();
  });

  it("preserves consumer types through @view-server/react package imports", () => {
    const consumerReact = createViewServerReactFromPackage(viewServer);
    const selected = consumerReact.useLiveQuery("orders", {
      select: ["id", "price"],
    });
    const provider = consumerReact.ViewServerProvider({
      url: "ws://127.0.0.1:8080/rpc",
      children: null,
    });

    expectTypeOf(selected).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly price: number;
      }>
    >();
    expectTypeOf(provider).toEqualTypeOf<ReactNode>();

    void consumerReact.ViewServerProvider({
      // @ts-expect-error public production provider accepts a URL, not a caller-owned client.
      client: liveClient,
      children: null,
    });

    consumerReact.useLiveQuery("orders", {
      // @ts-expect-error consumer package imports still reject unknown selected fields.
      select: ["prcie"],
    });

    consumerReact.useLiveQuery("orders", {
      select: [
        // @ts-expect-error consumer package imports still reject undefined selected fields.
        undefined,
      ],
    });

    consumerReact.useLiveQuery("orders", {
      select: [
        // @ts-expect-error consumer package imports still reject null selected fields.
        null,
      ],
    });
  });

  it("preserves consumer testing types through @view-server/react/testing package imports", () => {
    const consumerReact = createViewServerReactFromPackage(viewServer);
    const options = {
      subscriptionQueueCapacity: 1,
    } satisfies ViewServerInMemoryOptionsFromPackageTesting;
    const inMemory = createInMemoryViewServerReactFromPackageTesting(consumerReact, options);
    const provider = inMemory.ViewServerInMemoryProvider({ children: null });
    const publish = inMemory.client.publish("orders", {
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    });

    expectTypeOf(provider).toEqualTypeOf<ReactNode>();
    expectTypeOf<Parameters<typeof inMemory.client.publish>>().toEqualTypeOf<
      [topic: "orders", row: typeof Order.Type]
    >();
    expectTypeOf<Effect.Error<typeof publish>>().toEqualTypeOf<ViewServerRuntimeError>();

    // @ts-expect-error testing helper consumers must pass React bindings, not config.
    createInMemoryViewServerReactFromPackageTesting(viewServer);

    const invalidPublish = inMemory.client.publish("orders", {
      id: "order-2",
      customerId: "customer-2",
      status: "open",
      price: 42,
      region: "usa",
      // @ts-expect-error consumer testing client keeps exact topic row requirements.
      updateddAt: 1,
    });
    expectTypeOf(invalidPublish).not.toBeAny();
  });
});
