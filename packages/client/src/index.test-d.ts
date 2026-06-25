import { describe, expectTypeOf, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  grpc,
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
} from "@view-server/config";
import type {
  ViewServerHealth,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@view-server/config";
import type { Effect } from "effect";
import type { Stream } from "effect";
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

const mixedSourceViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
      source: grpc.leased({
        routeBy: ["id"],
      }),
    },
    positions: {
      schema: Order,
      key: "id",
    },
  },
});

declare const client: ViewServerLiveClient<typeof viewServer.topics>;
declare const leasedClient: ViewServerLiveClient<typeof leasedViewServer.topics>;
declare const mixedSourceClient: ViewServerLiveClient<typeof mixedSourceViewServer.topics>;
declare const mixedSourceTopic: "orders" | "positions";

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
      // @ts-expect-error selected fields must be topic field names, not undefined.
      select: [undefined],
    });

    const nullSelectedField = client.subscribe("orders", {
      // @ts-expect-error selected fields must be topic field names, not null.
      select: [null],
    });

    expectTypeOf(undefinedSelectedField).not.toBeAny();
    expectTypeOf(nullSelectedField).not.toBeAny();
  });

  it("requires leased gRPC route predicates in live subscriptions", () => {
    const routedSubscription = leasedClient.subscribe("orders", {
      where: {
        id: { eq: "order-1" },
      },
      select: ["id", "price"],
    });
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
    // @ts-expect-error leased gRPC subscriptions require exact eq route filters.
    const missingRouteSubscription = leasedClient.subscribe("orders", missingRouteQuery);
    // @ts-expect-error leased gRPC route filters must not use shorthand equality.
    const shorthandRouteSubscription = leasedClient.subscribe("orders", shorthandRouteQuery);

    expectTypeOf<Effect.Success<typeof routedSubscription>>().toEqualTypeOf<
      ViewServerLiveSubscription<{
        readonly id: string;
        readonly price: number;
      }>
    >();
    expectTypeOf(missingRouteSubscription).not.toBeAny();
    expectTypeOf(shorthandRouteSubscription).not.toBeAny();
  });

  it("keeps leased gRPC route predicates when the topic is a union", () => {
    const routedUnionSubscription = mixedSourceClient.subscribe(mixedSourceTopic, {
      where: {
        id: { eq: "order-1" },
      },
      select: ["id"],
    });
    const missingRouteQuery = {
      select: ["id"],
    } satisfies {
      readonly select: readonly ["id"];
    };

    const missingRouteSubscription = mixedSourceClient.subscribe(
      mixedSourceTopic,
      // @ts-expect-error dynamic topic unions that include a leased topic still require route filters.
      missingRouteQuery,
    );

    expectTypeOf<Effect.Success<typeof routedUnionSubscription>>().toEqualTypeOf<
      ViewServerLiveSubscription<{
        readonly id: string;
      }>
    >();
    expectTypeOf<Effect.Error<typeof routedUnionSubscription>>().toEqualTypeOf<
      ViewServerRuntimeError | ViewServerTransportError
    >();
    expectTypeOf(missingRouteSubscription).not.toBeAny();
  });

  it("exposes health as a read-only ref", () => {
    expectTypeOf(client.health.value).toEqualTypeOf<ViewServerHealth<typeof viewServer.topics>>();

    // @ts-expect-error public live client health must not expose mutation.
    client.health.set(client.health.value);
  });

  it("preserves pushed health subscription row and error types", () => {
    const summary = client.subscribeHealthSummary();
    const details = client.subscribeHealth();

    expectTypeOf<Effect.Success<typeof summary>>().toEqualTypeOf<
      ViewServerLiveSubscription<
        ViewServerHealthSummaryRow<typeof viewServer.topics>,
        typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        "summary"
      >
    >();
    expectTypeOf<Effect.Error<typeof summary>>().toEqualTypeOf<
      ViewServerRuntimeError | ViewServerTransportError
    >();
    expectTypeOf<Effect.Success<typeof details>>().toEqualTypeOf<
      ViewServerLiveSubscription<
        ViewServerHealthTopicRow<"orders">,
        typeof VIEW_SERVER_HEALTH_TOPIC,
        "orders"
      >
    >();
    expectTypeOf<Effect.Error<typeof details>>().toEqualTypeOf<
      ViewServerRuntimeError | ViewServerTransportError
    >();

    type SummaryEvent = Stream.Success<Effect.Success<typeof summary>["events"]>;
    type SummarySnapshot = Extract<SummaryEvent, { readonly type: "snapshot" }>;
    type SummaryDeltaOperation = Extract<
      SummaryEvent,
      { readonly type: "delta" }
    >["operations"][number];
    expectTypeOf<SummarySnapshot["keys"]>().toEqualTypeOf<readonly ["summary"]>();
    expectTypeOf<SummarySnapshot["rows"][0]["id"]>().toEqualTypeOf<"summary">();
    expectTypeOf<SummarySnapshot["rows"][0]["maxKafkaLag"]>().toEqualTypeOf<bigint | null>();
    expectTypeOf<SummarySnapshot["totalRows"]>().toEqualTypeOf<1>();
    expectTypeOf<
      Extract<SummaryDeltaOperation, { readonly type: "insert" }>
    >().toEqualTypeOf<never>();
    expectTypeOf<
      Extract<SummaryDeltaOperation, { readonly type: "remove" }>
    >().toEqualTypeOf<never>();

    type DetailEvent = Stream.Success<Effect.Success<typeof details>["events"]>;
    type DetailSnapshot = Extract<DetailEvent, { readonly type: "snapshot" }>;
    type DetailDeltaOperation = Extract<
      DetailEvent,
      { readonly type: "delta" }
    >["operations"][number];
    expectTypeOf<DetailSnapshot["keys"][number]>().toEqualTypeOf<"orders">();
    expectTypeOf<DetailSnapshot["rows"][number]["id"]>().toEqualTypeOf<"orders">();
    expectTypeOf<DetailSnapshot["rows"][number]["kafkaLag"]>().toEqualTypeOf<bigint | null>();
    expectTypeOf<
      Extract<DetailDeltaOperation, { readonly type: "insert" }>
    >().toEqualTypeOf<never>();
    expectTypeOf<
      Extract<DetailDeltaOperation, { readonly type: "remove" }>
    >().toEqualTypeOf<never>();
  });
});
