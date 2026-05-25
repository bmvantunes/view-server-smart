import { describe, expectTypeOf, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  type DeltaEvent,
  type LiveQuery,
  type LiveQueryResult,
  type RawQuery,
  type SnapshotEvent,
} from "@view-server/config";
import type { Effect, Stream } from "effect";
import { Schema } from "effect";
import type {
  ColumnLiveViewEngine,
  ColumnLiveViewEngineConfig,
  ColumnLiveViewEngineEvent,
  ColumnLiveViewEngineHealth,
  ColumnLiveViewSubscription,
  ColumnLiveViewTopicHealth,
} from "./index";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Finite,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
    trades: {
      schema: Order,
      key: "id",
    },
  },
});

type Topics = typeof viewServer.topics;
type Engine = ColumnLiveViewEngine<Topics>;
type OrderRow = typeof Order.Type;
type EffectSuccess<Value> =
  Value extends Effect.Effect<infer Success, infer _Error, infer _Services> ? Success : never;
type SubscriptionRow<Value> = Value extends ColumnLiveViewSubscription<infer Row> ? Row : never;
type SnapshotRow<Value> = Value extends SnapshotEvent<infer Row> ? Row : never;
type DeltaRow<Value> = Value extends DeltaEvent<infer Row> ? Row : never;
type StreamEvent<Value> =
  Value extends ColumnLiveViewSubscription<infer _Row> ? Stream.Success<Value["events"]> : never;

declare const engine: Engine;
declare const dynamicSingleField: "id" | "price";
declare const optionalNarrowFieldsQuery: {
  readonly select?: readonly ["id"];
};

describe("ColumnLiveViewEngine type contract", () => {
  it("requires explicit selected-row snapshots and subscription events", () => {
    const idSnapshot = engine.snapshot("orders", { select: ["id"] });
    expectTypeOf<EffectSuccess<typeof idSnapshot>>().toEqualTypeOf<
      LiveQueryResult<{ readonly id: string }>
    >();

    const fullSubscription = engine.subscribe("orders", { select: ["id"] });
    expectTypeOf<EffectSuccess<typeof fullSubscription>>().toEqualTypeOf<
      ColumnLiveViewSubscription<{ readonly id: string }>
    >();
    expectTypeOf<SubscriptionRow<EffectSuccess<typeof fullSubscription>>>().toEqualTypeOf<{
      readonly id: string;
    }>();
    type FullEvent = ColumnLiveViewEngineEvent<
      SubscriptionRow<EffectSuccess<typeof fullSubscription>>
    >;
    expectTypeOf<SnapshotRow<Extract<FullEvent, { readonly type: "snapshot" }>>>().toEqualTypeOf<{
      readonly id: string;
    }>();
    expectTypeOf<DeltaRow<Extract<FullEvent, { readonly type: "delta" }>>>().toEqualTypeOf<{
      readonly id: string;
    }>();
  });

  it("types selected-row snapshots and subscription events", () => {
    const selectedSnapshot = engine.snapshot("orders", {
      select: ["id", "price"],
    });
    expectTypeOf<EffectSuccess<typeof selectedSnapshot>>().toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly price: number;
      }>
    >();
    expectTypeOf<EffectSuccess<typeof selectedSnapshot>["rows"][number]>().toEqualTypeOf<{
      readonly id: string;
      readonly price: number;
    }>();

    const subscription = engine.subscribe("orders", {
      select: ["customerId", "status"],
    });
    expectTypeOf<EffectSuccess<typeof subscription>>().toEqualTypeOf<
      ColumnLiveViewSubscription<{
        readonly customerId: string;
        readonly status: "open" | "closed" | "cancelled";
      }>
    >();
    type SelectedRow = SubscriptionRow<EffectSuccess<typeof subscription>>;
    expectTypeOf<SelectedRow>().toEqualTypeOf<{
      readonly customerId: string;
      readonly status: "open" | "closed" | "cancelled";
    }>();
    type SelectedEvent = StreamEvent<EffectSuccess<typeof subscription>>;
    expectTypeOf<SelectedEvent>().toEqualTypeOf<ColumnLiveViewEngineEvent<SelectedRow>>();
    type SelectedSnapshot = Extract<SelectedEvent, { readonly type: "snapshot" }>;
    type SelectedDeltaOperation = Extract<
      Extract<SelectedEvent, { readonly type: "delta" }>["operations"][number],
      { readonly type: "insert" | "update" }
    >;
    expectTypeOf<SelectedSnapshot["rows"][number]>().toEqualTypeOf<{
      readonly customerId: string;
      readonly status: "open" | "closed" | "cancelled";
    }>();
    expectTypeOf<SelectedDeltaOperation["row"]>().toEqualTypeOf<{
      readonly customerId: string;
      readonly status: "open" | "closed" | "cancelled";
    }>();

    const health = engine.health();
    expectTypeOf<EffectSuccess<typeof health>>().toEqualTypeOf<
      ColumnLiveViewEngineHealth<Topics>
    >();
    expectTypeOf<
      EffectSuccess<typeof health>["topics"]["orders"]
    >().toEqualTypeOf<ColumnLiveViewTopicHealth>();
    type HealthTopics = EffectSuccess<typeof health>["topics"];
    // @ts-expect-error health topics preserve configured topic keys.
    expectTypeOf<HealthTopics["missing"]>().toEqualTypeOf<ColumnLiveViewTopicHealth>();
    // @ts-expect-error the map key is the topic identity; values do not duplicate it.
    expectTypeOf<HealthTopics["orders"]["topic"]>().toEqualTypeOf<"orders">();
  });

  it("types valid mutation calls", () => {
    const validPatch = engine.patch("orders", "order-1", {
      price: 42,
      status: "closed",
    });
    expectTypeOf<EffectSuccess<typeof validPatch>>().toEqualTypeOf<void>();
  });

  it("rejects invalid raw query select and topics", () => {
    const _invalidSelectedField = engine.snapshot("orders", {
      // @ts-expect-error invalid selected field is rejected.
      select: ["missing"],
    });
    const _invalidSubscribeSelectedField = engine.subscribe("orders", {
      // @ts-expect-error invalid selected field is rejected for subscriptions.
      select: ["missing"],
    });

    const _invalidWhereField = engine.snapshot("orders", {
      select: ["id"],
      // @ts-expect-error invalid where field is rejected.
      where: { missing: "value" },
    });

    const _invalidOrderField = engine.snapshot("orders", {
      select: ["id"],
      // @ts-expect-error invalid order field is rejected.
      orderBy: [
        {
          field: "missing",
          direction: "asc",
        },
      ],
    });

    // @ts-expect-error invalid topic is rejected.
    const _invalidTopic = engine.snapshot("missing", { select: ["id"] });
    // @ts-expect-error invalid subscription topic is rejected.
    const _invalidSubscribeTopic = engine.subscribe("missing", { select: ["id"] });

    const extraRawQuery = {
      select: ["id"],
      typo: true,
    } as const;
    // @ts-expect-error extra raw query keys are rejected through variables.
    const _invalidExtraRawQueryKey = engine.snapshot("orders", extraRawQuery);

    const extraWhereField = {
      select: ["id"],
      where: {
        status: "open",
        missing: "x",
      },
    } as const;
    // @ts-expect-error extra where fields are rejected through variables.
    const _invalidExtraWhereField = engine.snapshot("orders", extraWhereField);

    const extraFilterOperator = {
      select: ["id"],
      where: {
        status: {
          eq: "open",
          typo: true,
        },
      },
    } as const;
    // @ts-expect-error extra filter operator keys are rejected through variables.
    const _invalidExtraFilterOperator = engine.snapshot("orders", extraFilterOperator);

    const extraOrderByEntry = {
      select: ["id"],
      orderBy: [
        {
          field: "id",
          direction: "asc",
          typo: true,
        },
      ],
    } as const;
    // @ts-expect-error extra orderBy entry keys are rejected through variables.
    const _invalidExtraOrderByEntry = engine.snapshot("orders", extraOrderByEntry);

    const dynamicSingleTupleSelectedFieldsQuery = {
      select: [dynamicSingleField],
    } as const;
    const _dynamicSingleTupleSelectedFieldsSnapshot = engine.snapshot(
      "orders",
      dynamicSingleTupleSelectedFieldsQuery,
    );

    const broadSelectedFields: ReadonlyArray<"id" | "price"> = ["id", "price"];
    const broadSelectedFieldsQuery = {
      select: broadSelectedFields,
    };
    const _invalidBroadSelectedFieldsSnapshot = engine.snapshot(
      "orders",
      // @ts-expect-error broad selected field arrays are rejected because result rows must be exact.
      broadSelectedFieldsQuery,
    );

    // @ts-expect-error raw queries must explicitly select projected fields.
    const broadRawQueryWithoutFields: RawQuery<OrderRow> = {
      where: { status: "open" },
    };
    const _invalidOptionalNarrowFieldsSnapshot = engine.snapshot(
      "orders",
      // @ts-expect-error optional selected fields are rejected because omitted and present cases project different row shapes.
      optionalNarrowFieldsQuery,
    );
    const _invalidOptionalNarrowFieldsSubscription = engine.subscribe(
      "orders",
      // @ts-expect-error optional selected fields are rejected because omitted and present cases project different row shapes.
      optionalNarrowFieldsQuery,
    );

    void _invalidSelectedField;
    void _invalidSubscribeSelectedField;
    void _invalidWhereField;
    void _invalidOrderField;
    void _invalidTopic;
    void _invalidSubscribeTopic;
    void _invalidExtraRawQueryKey;
    void _invalidExtraWhereField;
    void _invalidExtraFilterOperator;
    void _invalidExtraOrderByEntry;
    void _dynamicSingleTupleSelectedFieldsSnapshot;
    void broadRawQueryWithoutFields;
    void _invalidOptionalNarrowFieldsSnapshot;
    void _invalidOptionalNarrowFieldsSubscription;
  });

  it("rejects invalid patch shapes", () => {
    // @ts-expect-error patch row field types are enforced.
    const _invalidPatchType = engine.patch("orders", "order-1", { price: "42" });

    // @ts-expect-error patch row fields are enforced.
    const _invalidPatchField = engine.patch("orders", "order-1", { missing: true });

    const invalidPatchVariable = {
      price: 42,
      missing: true,
    };
    // @ts-expect-error patch row fields are enforced through variables.
    const _invalidPatchVariable = engine.patch("orders", "order-1", invalidPatchVariable);

    void _invalidPatchType;
    void _invalidPatchField;
    void _invalidPatchVariable;
  });

  it("rejects invalid engine topic keys", () => {
    const _invalidKeyConfig: ColumnLiveViewEngineConfig<{
      readonly orders: {
        readonly schema: typeof Order;
        readonly key: "missing";
      };
    }> = {
      topics: {
        orders: {
          schema: Order,
          // @ts-expect-error engine topic keys must be string select in the schema.
          key: "missing",
        },
      },
    };

    void _invalidKeyConfig;
  });

  it("rejects grouped queries in the raw-only engine slice", () => {
    const _groupedSnapshot = engine.snapshot("orders", {
      // @ts-expect-error grouped queries are not part of this raw-only slice.
      groupBy: ["status"],
      // @ts-expect-error grouped queries are not part of this raw-only slice.
      aggregates: { count: { aggFunc: "count" } },
    });

    const _groupedSubscription = engine.subscribe("orders", {
      // @ts-expect-error grouped subscriptions are not part of this raw-only slice.
      groupBy: ["status"],
      // @ts-expect-error grouped subscriptions are not part of this raw-only slice.
      aggregates: { count: { aggFunc: "count" } },
    });

    const groupedVariable: LiveQuery<OrderRow> = {
      groupBy: ["status"],
      aggregates: { count: { aggFunc: "count" } },
    };
    // @ts-expect-error widened grouped query variables are rejected.
    const _groupedVariableSnapshot = engine.snapshot("orders", groupedVariable);
    // @ts-expect-error widened grouped subscription variables are rejected.
    const _groupedVariableSubscription = engine.subscribe("orders", groupedVariable);

    void _groupedSnapshot;
    void _groupedSubscription;
    void _groupedVariableSnapshot;
    void _groupedVariableSubscription;
  });
});
