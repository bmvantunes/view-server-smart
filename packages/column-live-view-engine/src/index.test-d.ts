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
declare const tupleUnionFields: readonly ["id"] | readonly ["id", "price"];
declare const dynamicSingleField: "id" | "price";
declare const optionalNarrowFieldsQuery: {
  readonly fields?: readonly ["id"];
};

describe("ColumnLiveViewEngine type contract", () => {
  it("types full-row snapshots and subscription events", () => {
    const fullSnapshot = engine.snapshot("orders", {});
    expectTypeOf<EffectSuccess<typeof fullSnapshot>>().toEqualTypeOf<LiveQueryResult<OrderRow>>();

    const fullSubscription = engine.subscribe("orders", {});
    expectTypeOf<EffectSuccess<typeof fullSubscription>>().toEqualTypeOf<
      ColumnLiveViewSubscription<OrderRow>
    >();
    expectTypeOf<
      SubscriptionRow<EffectSuccess<typeof fullSubscription>>
    >().toEqualTypeOf<OrderRow>();
    type FullEvent = ColumnLiveViewEngineEvent<
      SubscriptionRow<EffectSuccess<typeof fullSubscription>>
    >;
    expectTypeOf<
      SnapshotRow<Extract<FullEvent, { readonly type: "snapshot" }>>
    >().toEqualTypeOf<OrderRow>();
    expectTypeOf<
      DeltaRow<Extract<FullEvent, { readonly type: "delta" }>>
    >().toEqualTypeOf<OrderRow>();
  });

  it("types selected-row snapshots and subscription events", () => {
    const selectedSnapshot = engine.snapshot("orders", {
      fields: ["id", "price"],
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
      fields: ["customerId", "status"],
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

  it("rejects invalid raw query fields and topics", () => {
    const _invalidSelectedField = engine.snapshot("orders", {
      // @ts-expect-error invalid selected field is rejected.
      fields: ["missing"],
    });
    const _invalidSubscribeSelectedField = engine.subscribe("orders", {
      // @ts-expect-error invalid selected field is rejected for subscriptions.
      fields: ["missing"],
    });

    const _invalidWhereField = engine.snapshot("orders", {
      // @ts-expect-error invalid where field is rejected.
      where: { missing: "value" },
    });

    const _invalidOrderField = engine.snapshot("orders", {
      orderBy: [
        {
          // @ts-expect-error invalid order field is rejected.
          field: "missing",
          direction: "asc",
        },
      ],
    });

    // @ts-expect-error invalid topic is rejected.
    const _invalidTopic = engine.snapshot("missing", {});
    // @ts-expect-error invalid subscription topic is rejected.
    const _invalidSubscribeTopic = engine.subscribe("missing", {});

    const extraRawQuery = {
      fields: ["id"],
      typo: true,
    } as const;
    // @ts-expect-error extra raw query keys are rejected through variables.
    const _invalidExtraRawQueryKey = engine.snapshot("orders", extraRawQuery);

    const extraWhereField = {
      where: {
        status: "open",
        missing: "x",
      },
    } as const;
    // @ts-expect-error extra where fields are rejected through variables.
    const _invalidExtraWhereField = engine.snapshot("orders", extraWhereField);

    const extraFilterOperator = {
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

    const dynamicSelectedFieldsQuery: LiveQuery<OrderRow> = {
      fields: ["id"],
    };
    const _invalidDynamicSelectedFieldsSnapshot = engine.snapshot(
      "orders",
      // @ts-expect-error dynamic selected field arrays are rejected because they cannot prove the projected row shape.
      dynamicSelectedFieldsQuery,
    );
    const _invalidDynamicSelectedFieldsSubscription = engine.subscribe(
      "orders",
      // @ts-expect-error dynamic selected field arrays are rejected because they cannot prove the projected row shape.
      dynamicSelectedFieldsQuery,
    );

    const tupleUnionSelectedFieldsQuery = {
      fields: tupleUnionFields,
    };
    const _invalidTupleUnionSelectedFieldsSnapshot = engine.snapshot(
      "orders",
      // @ts-expect-error tuple-union fields are rejected because each branch projects a different row shape.
      tupleUnionSelectedFieldsQuery,
    );
    const _invalidTupleUnionSelectedFieldsSubscription = engine.subscribe(
      "orders",
      // @ts-expect-error tuple-union fields are rejected because each branch projects a different row shape.
      tupleUnionSelectedFieldsQuery,
    );

    const dynamicSingleTupleSelectedFieldsQuery = {
      fields: [dynamicSingleField],
    } as const;
    const _invalidDynamicSingleTupleSelectedFieldsSnapshot = engine.snapshot(
      "orders",
      // @ts-expect-error dynamic tuple field entries are rejected because the projected row shape is not fixed.
      dynamicSingleTupleSelectedFieldsQuery,
    );
    const _invalidDynamicSingleTupleSelectedFieldsSubscription = engine.subscribe(
      "orders",
      // @ts-expect-error dynamic tuple field entries are rejected because the projected row shape is not fixed.
      dynamicSingleTupleSelectedFieldsQuery,
    );

    const broadRawQueryWithoutFields: RawQuery<OrderRow> = {
      where: { status: "open" },
    };
    const _invalidBroadRawQueryWithoutFieldsSnapshot = engine.snapshot(
      "orders",
      // @ts-expect-error broad RawQuery variables are rejected because optional fields could be dynamic.
      broadRawQueryWithoutFields,
    );
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
    void _invalidDynamicSelectedFieldsSnapshot;
    void _invalidDynamicSelectedFieldsSubscription;
    void _invalidTupleUnionSelectedFieldsSnapshot;
    void _invalidTupleUnionSelectedFieldsSubscription;
    void _invalidDynamicSingleTupleSelectedFieldsSnapshot;
    void _invalidDynamicSingleTupleSelectedFieldsSubscription;
    void _invalidBroadRawQueryWithoutFieldsSnapshot;
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
          // @ts-expect-error engine topic keys must be string fields in the schema.
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
      aggregates: [{ type: "count", as: "count" }],
    });

    const _groupedSubscription = engine.subscribe("orders", {
      // @ts-expect-error grouped subscriptions are not part of this raw-only slice.
      groupBy: ["status"],
      // @ts-expect-error grouped subscriptions are not part of this raw-only slice.
      aggregates: [{ type: "count", as: "count" }],
    });

    const groupedVariable: LiveQuery<OrderRow> = {
      groupBy: ["status"],
      aggregates: [{ type: "count", as: "count" }],
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
