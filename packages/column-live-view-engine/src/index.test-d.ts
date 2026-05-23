import { describe, expectTypeOf, it } from "@effect/vitest";
import { defineViewServerConfig, type LiveQuery, type LiveQueryResult } from "@view-server/config";
import type { Effect } from "effect";
import { Schema } from "effect";
import type {
  ColumnLiveViewEngine,
  ColumnLiveViewEngineConfig,
  ColumnLiveViewEngineHealth,
  ColumnLiveViewSubscription,
  ColumnLiveViewTopicHealth,
} from "./index.ts";

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
type OrderRow = Schema.Schema.Type<typeof Order>;
type EffectSuccess<Value> =
  Value extends Effect.Effect<infer Success, infer _Error, infer _Services> ? Success : never;

declare const engine: Engine;

describe("ColumnLiveViewEngine type contract", () => {
  it("types raw snapshots and subscriptions conservatively for this slice", () => {
    const fullSnapshot = engine.snapshot("orders", {});
    expectTypeOf<EffectSuccess<typeof fullSnapshot>>().toEqualTypeOf<LiveQueryResult<OrderRow>>();

    const selectedSnapshot = engine.snapshot("orders", {
      fields: ["id", "price"],
    });
    expectTypeOf<EffectSuccess<typeof selectedSnapshot>>().toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly price: number;
      }>
    >();

    const subscription = engine.subscribe("orders", {
      fields: ["customerId", "status"],
    });
    expectTypeOf<EffectSuccess<typeof subscription>>().toEqualTypeOf<
      ColumnLiveViewSubscription<{
        readonly customerId: string;
        readonly status: "open" | "closed" | "cancelled";
      }>
    >();

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

    void _invalidSelectedField;
    void _invalidWhereField;
    void _invalidOrderField;
    void _invalidTopic;
    void _invalidExtraRawQueryKey;
    void _invalidExtraWhereField;
    void _invalidExtraFilterOperator;
    void _invalidExtraOrderByEntry;
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
