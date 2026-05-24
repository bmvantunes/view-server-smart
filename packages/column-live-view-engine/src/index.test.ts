import { describe, expect, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  type DeltaEvent,
  type RawQuery,
  type SnapshotEvent,
} from "@view-server/config";
import { Cause, Effect, Schema, Scope, Stream } from "effect";
import { fromStringUnsafe } from "effect/BigDecimal";
import {
  createColumnLiveViewEngine,
  type ColumnLiveViewEngine,
  type ColumnLiveViewEngineConfig,
  type ColumnLiveViewEngineEvent,
  type ColumnLiveViewSubscription,
} from "./index.ts";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Finite,
  region: Schema.String,
  updatedAt: Schema.Number,
  note: Schema.optionalKey(Schema.String),
});

const Position = Schema.Struct({
  id: Schema.String,
  accountId: Schema.String,
  symbol: Schema.String,
  active: Schema.Boolean,
  quantity: Schema.BigInt,
  price: Schema.BigDecimal,
});

const Instrument = Schema.Struct({
  id: Schema.String,
  metadata: Schema.Struct({
    venue: Schema.String,
    risk: Schema.Struct({
      tier: Schema.Number,
      lot: Schema.BigInt,
    }),
  }),
  operatorLike: Schema.Struct({
    eq: Schema.String,
  }),
  tags: Schema.Array(Schema.String),
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
    positions: {
      schema: Position,
      key: "id",
    },
    instruments: {
      schema: Instrument,
      key: "id",
    },
  },
});

type Topics = typeof viewServer.topics;
type Engine = ColumnLiveViewEngine<Topics>;
type OrderRow = typeof Order.Type;
type InstrumentRow = typeof Instrument.Type;

const order = (
  id: string,
  status: OrderRow["status"],
  price: number,
  updatedAt: number,
  region = "emea",
): OrderRow => ({
  id,
  customerId: `customer-${id}`,
  status,
  price,
  region,
  updatedAt,
});

const makeEngine = (): Effect.Effect<Engine> =>
  createColumnLiveViewEngine({ topics: viewServer.topics });

const instrument = (
  id: string,
  venue: string,
  tier: number,
  tags: ReadonlyArray<string>,
): InstrumentRow => ({
  id,
  metadata: {
    venue,
    risk: {
      tier,
      lot: BigInt(tier),
    },
  },
  operatorLike: {
    eq: venue,
  },
  tags: [...tags],
});

const rowField = (row: object, field: string): unknown => {
  for (const [key, value] of Object.entries(row)) {
    if (key === field) {
      return value;
    }
  }
  return undefined;
};

const rowIds = (rows: ReadonlyArray<object>): ReadonlyArray<unknown> =>
  rows.map((row) => rowField(row, "id"));

const takeEvents = <Row>(
  subscription: ColumnLiveViewSubscription<Row>,
  count: number,
): Effect.Effect<ReadonlyArray<ColumnLiveViewEngineEvent<Row>>> =>
  subscription.events.pipe(Stream.take(count), Stream.runCollect);

const makeEventReader = <Row>(
  subscription: ColumnLiveViewSubscription<Row>,
): Effect.Effect<
  (count: number) => Effect.Effect<ReadonlyArray<ColumnLiveViewEngineEvent<Row>>, Cause.Done>,
  never,
  Scope.Scope
> =>
  Effect.gen(function* () {
    const pull = yield* Stream.toPull(subscription.events);
    return (count) =>
      Effect.gen(function* () {
        const events: Array<ColumnLiveViewEngineEvent<Row>> = [];
        while (events.length < count) {
          const chunk = yield* pull;
          events.push(...chunk);
        }
        return events.slice(0, count);
      });
  });

const collectEvents = <Row>(
  subscription: ColumnLiveViewSubscription<Row>,
): Effect.Effect<ReadonlyArray<ColumnLiveViewEngineEvent<Row>>> =>
  subscription.events.pipe(Stream.runCollect);

const firstEvent = <Row>(
  events: ReadonlyArray<ColumnLiveViewEngineEvent<Row>>,
): ColumnLiveViewEngineEvent<Row> => {
  expect(events).not.toEqual([]);
  return events[0]!;
};

const expectSnapshotEvent: <Row>(
  event: ColumnLiveViewEngineEvent<Row>,
) => asserts event is SnapshotEvent<Row> = (event) => {
  expect(event).toMatchObject({ type: "snapshot" });
};

const expectDeltaEvent: <Row>(
  event: ColumnLiveViewEngineEvent<Row>,
) => asserts event is DeltaEvent<Row> = (event) => {
  expect(event).toMatchObject({ type: "delta" });
};

const expectSnapshotRows = <Row>(
  event: ColumnLiveViewEngineEvent<Row>,
  rows: ReadonlyArray<Row>,
) => {
  expectSnapshotEvent(event);
  expect(event.rows).toEqual(rows);
};

const expectDefined = <Value>(value: Value | undefined): Value => {
  expect(value).not.toBeUndefined();
  return value!;
};

type ClientState<Row> = {
  readonly keys: ReadonlyArray<string>;
  readonly rows: ReadonlyArray<Row>;
};

const applyDelta = <Row>(state: ClientState<Row>, event: DeltaEvent<Row>): ClientState<Row> => {
  const keys = [...state.keys];
  const rows = [...state.rows];

  for (const operation of event.operations) {
    if (operation.type === "remove") {
      const index = keys.indexOf(operation.key);
      expect(index).toBeGreaterThanOrEqual(0);
      keys.splice(index, 1);
      rows.splice(index, 1);
    }
    if (operation.type === "insert") {
      keys.splice(operation.index, 0, operation.key);
      rows.splice(operation.index, 0, operation.row);
    }
    if (operation.type === "update") {
      const index = keys.indexOf(operation.key);
      expect(index).toBeGreaterThanOrEqual(0);
      rows[index] = operation.row;
    }
    if (operation.type === "move") {
      const index = keys.indexOf(operation.key);
      expect(index).toBeGreaterThanOrEqual(0);
      const row = expectDefined(rows[index]);
      keys.splice(index, 1);
      rows.splice(index, 1);
      keys.splice(operation.toIndex, 0, operation.key);
      rows.splice(operation.toIndex, 0, row);
    }
  }

  return { keys, rows };
};

const stateFromSnapshot = <Row>(event: ColumnLiveViewEngineEvent<Row>): ClientState<Row> => {
  expectSnapshotEvent(event);
  return {
    keys: event.keys,
    rows: event.rows,
  };
};

const expectDeltaConverges = <Row>(
  state: ClientState<Row>,
  event: ColumnLiveViewEngineEvent<Row>,
  freshRows: ReadonlyArray<Row>,
): ClientState<Row> => {
  expectDeltaEvent(event);
  const nextState = applyDelta(state, event);
  expect(nextState.rows).toEqual(freshRows);
  return nextState;
};

describe("ColumnLiveViewEngine raw snapshots", () => {
  it.effect("publishes rows and snapshots a filtered, sorted, windowed raw query", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      yield* engine.publishMany("orders", [
        order("1", "open", 10, 1, "emea"),
        order("2", "open", 40, 2, "amer"),
        order("3", "closed", 30, 3, "emea"),
        order("4", "open", 20, 4, "emea"),
        order("5", "open", 50, 5, "emea"),
        {
          ...order("6", "open", 15, 4, "emea"),
          customerId: "account-6",
        },
      ]);

      const snapshot = yield* engine.snapshot("orders", {
        where: {
          customerId: { startsWith: "customer-" },
          status: "open",
          price: { gte: 10, lt: 50 },
          updatedAt: { lte: 4 },
          region: { eq: "emea" },
        },
        orderBy: [{ field: "price", direction: "desc" }],
        offset: 1,
        limit: 1,
      });

      expect(snapshot).toEqual({
        rows: [order("1", "open", 10, 1, "emea")],
        totalRows: 2,
        version: 1,
      });

      const equalStringSort = yield* engine.snapshot("orders", {
        where: {
          status: "open",
        },
        orderBy: [{ field: "status", direction: "asc" }],
      });
      expect(rowIds(equalStringSort.rows)).toEqual(["1", "2", "4", "5", "6"]);

      const reverseInsertEngine = yield* makeEngine();
      yield* reverseInsertEngine.publishMany("orders", [
        order("b", "open", 10, 1),
        order("a", "open", 20, 2),
      ]);
      const equalStringSortReverseInsert = yield* reverseInsertEngine.snapshot("orders", {
        orderBy: [{ field: "status", direction: "asc" }],
      });
      expect(rowIds(equalStringSortReverseInsert.rows)).toEqual(["a", "b"]);
    }),
  );

  it.effect("returns only selected fields", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      yield* engine.publish("orders", order("1", "open", 10, 1));

      const snapshot = yield* engine.snapshot("orders", {
        fields: ["customerId", "status", "updatedAt"],
        where: {
          status: "open",
        },
      });

      expect(snapshot.rows).toEqual([
        {
          customerId: "customer-1",
          status: "open",
          updatedAt: 1,
        },
      ]);
    }),
  );

  it.effect("does not expose stored row objects through snapshots", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));

      const snapshot = yield* engine.snapshot("orders", {});
      expect(snapshot.rows).toHaveLength(1);
      Object.assign(snapshot.rows[0]!, { price: 999 });

      const fresh = yield* engine.snapshot("orders", {});
      expect(fresh.rows).toEqual([order("1", "open", 10, 1)]);
    }),
  );

  it.effect("deep-clones nested rows and supports object-valued equality filters", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const emptyStructuredQuery = yield* engine.snapshot("instruments", {
        where: {
          metadata: {
            venue: "xnys",
            risk: {
              tier: 1,
              lot: 1n,
            },
          },
        },
      });
      expect(emptyStructuredQuery.rows).toEqual([]);

      yield* engine.publishMany("instruments", [
        instrument("1", "xnys", 1, ["equity", "us"]),
        instrument("2", "xlon", 2, ["equity", "uk"]),
      ]);

      const metadataQuery = yield* engine.snapshot("instruments", {
        where: {
          metadata: {
            venue: "xnys",
            risk: {
              tier: 1,
              lot: 1n,
            },
          },
        },
      });
      expect(rowIds(metadataQuery.rows)).toEqual(["1"]);

      const arrayQuery = yield* engine.snapshot("instruments", {
        where: {
          tags: ["equity", "us"],
        },
      });
      expect(rowIds(arrayQuery.rows)).toEqual(["1"]);

      const operatorObjectQuery = yield* engine.snapshot("instruments", {
        where: {
          metadata: {
            eq: {
              venue: "xlon",
              risk: {
                tier: 2,
                lot: 2n,
              },
            },
          },
        },
      });
      expect(rowIds(operatorObjectQuery.rows)).toEqual(["2"]);

      const operatorLikeDirectObjectQuery = yield* engine.snapshot("instruments", {
        where: {
          operatorLike: {
            eq: "xnys",
          },
        },
      });
      expect(rowIds(operatorLikeDirectObjectQuery.rows)).toEqual(["1"]);

      const operatorLikeWrappedObjectQuery = yield* engine.snapshot("instruments", {
        where: {
          operatorLike: {
            eq: {
              eq: "xlon",
            },
          },
        },
      });
      expect(rowIds(operatorLikeWrappedObjectQuery.rows)).toEqual(["2"]);

      const operatorLikeObjectNeq = yield* engine.snapshot("instruments", {
        where: {
          operatorLike: {
            neq: {
              eq: "not-present",
            },
          },
        },
      });
      expect(rowIds(operatorLikeObjectNeq.rows)).toEqual(["1", "2"]);

      const operatorLikeObjectNeqEqual = yield* engine.snapshot("instruments", {
        where: {
          operatorLike: {
            neq: {
              eq: "xnys",
            },
          },
        },
      });
      expect(rowIds(operatorLikeObjectNeqEqual.rows)).toEqual(["2"]);

      const objectInQuery = yield* engine.snapshot("instruments", {
        where: {
          operatorLike: {
            in: [
              {
                eq: "xlon",
              },
            ],
          },
        },
      });
      expect(rowIds(objectInQuery.rows)).toEqual(["2"]);

      const invalidObjectInQuery = yield* engine.snapshot("instruments", {
        where: {
          operatorLike: {
            // @ts-expect-error runtime validation handles hostile untyped structured filters.
            in: [undefined],
          },
        },
      });
      expect(rowIds(invalidObjectInQuery.rows)).toEqual([]);

      const fullSnapshot = yield* engine.snapshot("instruments", {});
      expect(fullSnapshot.rows).toHaveLength(2);
      Object.assign(Object(fullSnapshot.rows[0]).metadata.risk, { tier: 999 });
      Object(fullSnapshot.rows[0]).tags.push("mutated");

      const projectedSnapshot = yield* engine.snapshot("instruments", {
        fields: ["metadata", "tags"],
        where: {
          id: "1",
        },
      });
      expect(projectedSnapshot.rows).toEqual([
        {
          metadata: {
            venue: "xnys",
            risk: {
              tier: 1,
              lot: 1n,
            },
          },
          tags: ["equity", "us"],
        },
      ]);
      Object.assign(Object(projectedSnapshot.rows[0]).metadata.risk, { tier: 777 });
      Object(projectedSnapshot.rows[0]).tags.push("projected-mutation");

      const fresh = yield* engine.snapshot("instruments", {
        where: {
          id: "1",
        },
      });
      expect(fresh.rows).toEqual([instrument("1", "xnys", 1, ["equity", "us"])]);
    }),
  );

  it.effect("does not retain nested publish or patch input references", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const original = instrument("1", "xnys", 1, ["equity", "us"]);
      yield* engine.publish("instruments", original);

      Object.assign(original.metadata.risk, { tier: 999 });
      Object(original.tags).push("mutated-after-publish");

      const afterPublishMutation = yield* engine.snapshot("instruments", {});
      expect(afterPublishMutation.rows).toEqual([instrument("1", "xnys", 1, ["equity", "us"])]);

      const patch = {
        metadata: {
          venue: "xlon",
          risk: {
            tier: 2,
            lot: 2n,
          },
        },
        operatorLike: {
          eq: "xlon",
        },
        tags: ["equity", "uk"],
      };
      yield* engine.patch("instruments", "1", patch);

      patch.metadata.risk.tier = 777;
      patch.operatorLike.eq = "mutated-after-patch";
      patch.tags.push("mutated-after-patch");

      const afterPatchMutation = yield* engine.snapshot("instruments", {});
      expect(afterPatchMutation.rows).toEqual([instrument("1", "xlon", 2, ["equity", "uk"])]);
    }),
  );

  it.effect("supports bigint and BigDecimal raw comparison semantics", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      yield* engine.publishMany("positions", [
        {
          id: "position-1",
          accountId: "account-1",
          symbol: "AAPL",
          active: true,
          quantity: 10n,
          price: fromStringUnsafe("3.00"),
        },
        {
          id: "position-2",
          accountId: "account-1",
          symbol: "MSFT",
          active: false,
          quantity: 20n,
          price: fromStringUnsafe("2.00"),
        },
        {
          id: "position-3",
          accountId: "account-1",
          symbol: "TSLA",
          active: true,
          quantity: 9n,
          price: fromStringUnsafe("1.00"),
        },
        {
          id: "position-4",
          accountId: "account-1",
          symbol: "NVDA",
          active: true,
          quantity: 10n,
          price: fromStringUnsafe("1.00"),
        },
      ]);

      const snapshot = yield* engine.snapshot("positions", {
        where: {
          quantity: { gt: 9n },
          price: { gte: fromStringUnsafe("2.00") },
        },
        orderBy: [
          { field: "active", direction: "asc" },
          { field: "price", direction: "asc" },
        ],
      });

      expect(rowIds(snapshot.rows)).toEqual(["position-2", "position-1"]);

      const fallbackOrdered = yield* engine.snapshot("positions", {
        orderBy: [{ field: "active", direction: "asc" }],
      });
      expect(rowIds(fallbackOrdered.rows)).toEqual([
        "position-2",
        "position-1",
        "position-3",
        "position-4",
      ]);

      const symbolOrdered = yield* engine.snapshot("positions", {
        orderBy: [{ field: "symbol", direction: "desc" }],
        where: {
          price: { eq: fromStringUnsafe("1.00") },
        },
      });
      expect(rowIds(symbolOrdered.rows)).toEqual(["position-3", "position-4"]);

      const quantityOrdered = yield* engine.snapshot("positions", {
        orderBy: [{ field: "quantity", direction: "asc" }],
      });
      expect(rowIds(quantityOrdered.rows)).toEqual([
        "position-3",
        "position-1",
        "position-4",
        "position-2",
      ]);

      const booleanNotEqual = yield* engine.snapshot("positions", {
        where: {
          active: { neq: false },
        },
        orderBy: [{ field: "symbol", direction: "asc" }],
      });
      expect(rowIds(booleanNotEqual.rows)).toEqual(["position-1", "position-4", "position-3"]);

      const decimalNotEqual = yield* engine.snapshot("positions", {
        where: {
          price: { neq: fromStringUnsafe("2.00") },
        },
        orderBy: [{ field: "symbol", direction: "asc" }],
      });
      expect(rowIds(decimalNotEqual.rows)).toEqual(["position-1", "position-4", "position-3"]);
    }),
  );

  it.effect("sorts object, array, and missing values deterministically", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("instruments", [
        instrument("3", "xnas", 3, ["bond"]),
        instrument("1", "xnys", 1, ["equity", "us"]),
        instrument("2", "xlon", 2, ["equity", "uk"]),
      ]);

      const objectOrdered = yield* engine.snapshot("instruments", {
        orderBy: [{ field: "metadata", direction: "asc" }],
      });
      expect(rowIds(objectOrdered.rows)).toEqual(["1", "2", "3"]);

      const arrayOrdered = yield* engine.snapshot("instruments", {
        orderBy: [{ field: "tags", direction: "desc" }],
      });
      expect(rowIds(arrayOrdered.rows)).toEqual(["1", "2", "3"]);

      yield* engine.publish("orders", order("1", "open", 10, 1));
      yield* engine.publish("orders", { ...order("2", "open", 20, 2), note: "visible" });
      yield* engine.publish("orders", order("3", "open", 30, 3));
      const missingOrdered = yield* engine.snapshot("orders", {
        orderBy: [{ field: "note", direction: "asc" }],
      });
      expect(rowIds(missingOrdered.rows)).toEqual(["1", "3", "2"]);
    }),
  );

  it.effect(
    "uses the configured row key as the final tiebreaker for equal ascending sort fields",
    () =>
      Effect.gen(function* () {
        const engine = yield* makeEngine();
        yield* engine.publishMany("orders", [
          order("c", "open", 10, 1),
          order("a", "open", 10, 1),
          order("b", "open", 10, 1),
        ]);

        const snapshot = yield* engine.snapshot("orders", {
          orderBy: [{ field: "price", direction: "asc" }],
        });

        expect(rowIds(snapshot.rows)).toEqual(["a", "b", "c"]);
      }),
  );

  it.effect("uses the configured row key as the default order without explicit sort fields", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("c", "open", 30, 3),
        order("a", "closed", 10, 1),
        order("b", "cancelled", 20, 2),
      ]);

      const snapshot = yield* engine.snapshot("orders", {});

      expect(rowIds(snapshot.rows)).toEqual(["a", "b", "c"]);
    }),
  );

  it.effect(
    "uses the configured row key as the final tiebreaker for equal descending sort fields",
    () =>
      Effect.gen(function* () {
        const engine = yield* makeEngine();
        yield* engine.publishMany("orders", [
          order("c", "open", 10, 1),
          order("a", "open", 10, 1),
          order("b", "open", 10, 1),
        ]);

        const snapshot = yield* engine.snapshot("orders", {
          orderBy: [{ field: "price", direction: "desc" }],
        });

        expect(rowIds(snapshot.rows)).toEqual(["a", "b", "c"]);
      }),
  );

  it.effect("uses the configured row key after all sort fields compare equal", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("c", "closed", 10, 1, "emea"),
        order("a", "closed", 10, 1, "emea"),
        order("b", "closed", 10, 1, "emea"),
      ]);

      const snapshot = yield* engine.snapshot("orders", {
        orderBy: [
          { field: "price", direction: "desc" },
          { field: "status", direction: "asc" },
          { field: "region", direction: "desc" },
          { field: "updatedAt", direction: "asc" },
        ],
      });

      expect(rowIds(snapshot.rows)).toEqual(["a", "b", "c"]);
    }),
  );

  it.effect("exercises raw filter exclusion branches through public snapshots", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        { ...order("1", "open", 10, 1, "emea"), customerId: "account-1" },
        order("2", "open", 9, 1, "emea"),
        order("3", "open", 10, 0, "emea"),
        order("4", "cancelled", 10, 1, "emea"),
        order("5", "open", 10, 1, "amer"),
        order("6", "open", 10, 5, "emea"),
        order("7", "open", 10, 1, "emea"),
      ]);

      const allRows = yield* engine.snapshot("orders", {});
      expect(allRows.totalRows).toBe(7);

      const snapshot = yield* engine.snapshot("orders", {
        where: {
          customerId: { startsWith: "customer-" },
          price: { gt: 9 },
          updatedAt: { gte: 1, lte: 4 },
          status: { in: ["open"] },
          region: { eq: "emea" },
        },
      });
      expect(rowIds(snapshot.rows)).toEqual(["7"]);

      const notOpen = yield* engine.snapshot("orders", {
        where: {
          status: { neq: "open" },
        },
      });
      expect(rowIds(notOpen.rows)).toEqual(["4"]);
    }),
  );
});

describe("ColumnLiveViewEngine subscriptions", () => {
  it.effect("emits the initial snapshot before live deltas", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));

      const subscription = yield* engine.subscribe("orders", {
        where: {
          status: "open",
        },
      });
      const events = yield* takeEvents(subscription, 1);

      expectSnapshotRows(firstEvent(events), [order("1", "open", 10, 1)]);
      yield* subscription.close();
    }),
  );

  it.effect("emits snapshot keys for projected subscriptions without selected id fields", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [order("b", "open", 10, 1), order("a", "open", 10, 1)]);

      const subscription = yield* engine.subscribe("orders", {
        fields: ["customerId", "status"],
      });
      const take = yield* makeEventReader(subscription);
      const initialEvents = yield* take(1);
      const snapshot = firstEvent(initialEvents);
      expectSnapshotEvent(snapshot);
      expect(snapshot.keys).toEqual(["a", "b"]);
      expect(snapshot.rows).toEqual([
        { customerId: "customer-a", status: "open" },
        { customerId: "customer-b", status: "open" },
      ]);

      let state = stateFromSnapshot(snapshot);
      yield* engine.delete("orders", "a");
      const deleteEvents = yield* take(1);
      state = expectDeltaConverges(state, firstEvent(deleteEvents), [
        { customerId: "customer-b", status: "open" },
      ]);
      expect(state.keys).toEqual(["b"]);
      yield* subscription.close();
    }),
  );

  it.effect("emits projected rows in subscription delta operations", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("a", "open", 10, 1));

      const subscription = yield* engine.subscribe("orders", {
        fields: ["customerId", "status"],
      });
      const take = yield* makeEventReader(subscription);
      yield* take(1);

      yield* engine.patch("orders", "a", { status: "closed", price: 99 });
      const firstDelta = firstEvent(yield* take(1));
      expectDeltaEvent(firstDelta);
      expect(firstDelta.operations).toEqual([
        {
          type: "update",
          key: "a",
          row: {
            customerId: "customer-a",
            status: "closed",
          },
          index: 0,
        },
      ]);
      yield* engine.publish("orders", order("b", "open", 20, 2));
      const secondDelta = firstEvent(yield* take(1));
      expectDeltaEvent(secondDelta);
      expect(secondDelta.operations).toEqual([
        {
          type: "insert",
          key: "b",
          row: {
            customerId: "customer-b",
            status: "open",
          },
          index: 1,
        },
      ]);
      yield* subscription.close();
    }),
  );

  it.effect("reports queued events for active subscribers", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const subscription = yield* engine.subscribe("orders", {});

      yield* engine.publish("orders", order("1", "open", 10, 1));

      const health = yield* engine.health();
      expect(health.activeSubscriptions).toBe(1);
      expect(health.queuedEvents).toBe(2);
      expect(health.topics["orders"].queuedEvents).toBe(2);

      yield* subscription.close();
    }),
  );

  it.effect("emits publish, patch, and delete deltas that converge to fresh snapshots", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [order("1", "open", 10, 1), order("2", "closed", 20, 2)]);

      const query = {
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "asc" }],
      } satisfies RawQuery<OrderRow>;
      const subscription = yield* engine.subscribe("orders", query);
      const take = yield* makeEventReader(subscription);
      const initialEvents = yield* take(1);
      let state = stateFromSnapshot(firstEvent(initialEvents));

      yield* engine.publish("orders", order("3", "open", 5, 3));
      const publishEvents = yield* take(1);
      const afterPublish = yield* engine.snapshot("orders", query);
      state = expectDeltaConverges(state, firstEvent(publishEvents), afterPublish.rows);

      yield* engine.patch("orders", "1", { price: 30 });
      const patchEvents = yield* take(1);
      const afterPatch = yield* engine.snapshot("orders", query);
      state = expectDeltaConverges(state, firstEvent(patchEvents), afterPatch.rows);

      yield* engine.delete("orders", "3");
      const deleteEvents = yield* take(1);
      const afterDelete = yield* engine.snapshot("orders", query);
      state = expectDeltaConverges(state, firstEvent(deleteEvents), afterDelete.rows);

      expect(state.rows).toEqual([order("1", "open", 30, 1)]);
      yield* subscription.close();
    }),
  );

  it.effect("serializes concurrent publishes before notifying subscribers", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const subscription = yield* engine.subscribe("orders", {});
      const take = yield* makeEventReader(subscription);
      yield* take(1);

      yield* Effect.all(
        ["c", "a", "b"].map((id, index) =>
          engine.publish("orders", order(id, "open", 10 + index, index)),
        ),
        { concurrency: "unbounded" },
      );

      yield* take(3);
      const fresh = yield* engine.snapshot("orders", {});
      expect(rowIds(fresh.rows)).toEqual(["a", "b", "c"]);
      expect(fresh.version).toBe(3);
      yield* subscription.close();
    }),
  );

  it.effect("idempotent subscription close removes active subscribers from health", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const subscription = yield* engine.subscribe("orders", {});

      const active = yield* engine.health();
      expect(active.topics["orders"].activeSubscriptions).toBe(1);
      expect(active.activeSubscriptions).toBe(1);

      yield* subscription.close();
      yield* subscription.close();

      const closed = yield* engine.health();
      expect(closed.topics["orders"].activeSubscriptions).toBe(0);
      expect(closed.activeSubscriptions).toBe(0);
    }),
  );

  it.effect("stream finalization releases active subscribers", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const subscription = yield* engine.subscribe("orders", {});

      const events = yield* takeEvents(subscription, 1);
      expect(events.map((event) => event.type)).toEqual(["snapshot"]);

      const health = yield* engine.health();
      expect(health.topics["orders"].activeSubscriptions).toBe(0);
      expect(health.activeSubscriptions).toBe(0);
    }),
  );

  it.effect("does not emit deltas for invisible updates or no-op visible patches", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [order("1", "open", 10, 1), order("2", "closed", 20, 2)]);
      const subscription = yield* engine.subscribe("orders", {
        where: {
          status: "open",
        },
      });
      const take = yield* makeEventReader(subscription);
      yield* take(1);

      yield* engine.patch("orders", "2", { price: 25 });
      yield* engine.patch("orders", "1", { price: 10 });
      yield* subscription.close();

      const remaining = yield* collectEvents(subscription);
      expect(remaining).toEqual([]);
    }),
  );

  it.effect("freezes subscription query semantics at subscribe time", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));

      const query: { where: { status: OrderRow["status"] } } = {
        where: {
          status: "open",
        },
      };
      const subscription = yield* engine.subscribe("orders", query);
      const take = yield* makeEventReader(subscription);
      const initialEvents = yield* take(1);
      let state = stateFromSnapshot(firstEvent(initialEvents));

      query.where.status = "closed";
      yield* engine.publish("orders", order("2", "open", 20, 2));

      const events = yield* take(1);
      const event = firstEvent(events);
      state = expectDeltaConverges(state, event, [
        order("1", "open", 10, 1),
        order("2", "open", 20, 2),
      ]);
      expect(state.keys).toEqual(["1", "2"]);
      yield* subscription.close();
    }),
  );

  it.effect("does not let consumer snapshot mutations corrupt subscription cursors", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("instruments", instrument("1", "xnys", 1, ["equity", "us"]));

      const subscription = yield* engine.subscribe("instruments", {
        orderBy: [{ field: "id", direction: "asc" }],
      });
      const take = yield* makeEventReader(subscription);
      const initialEvents = yield* take(1);
      const initial = firstEvent(initialEvents);
      expectSnapshotEvent(initial);
      Object.assign(Object(initial.rows[0]).metadata.risk, { tier: 999 });
      Object(initial.rows[0]).tags.push("mutated-client-row");

      yield* engine.publish("instruments", instrument("2", "xlon", 2, ["equity", "uk"]));
      const events = yield* take(1);
      const event = firstEvent(events);
      expectDeltaEvent(event);
      expect(event.operations).toEqual([
        {
          type: "insert",
          key: "2",
          row: instrument("2", "xlon", 2, ["equity", "uk"]),
          index: 1,
        },
      ]);
      yield* subscription.close();
    }),
  );

  it.effect("does not broaden results for invalid runtime range operands", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));

      const invalidGt = yield* engine.snapshot("orders", {
        where: {
          price: {
            // @ts-expect-error malformed runtime queries must not broaden results.
            gt: "9",
          },
        },
      });
      expect(invalidGt.rows).toEqual([]);

      const invalidGtNaN = yield* engine.snapshot("orders", {
        where: {
          price: {
            gt: Number.NaN,
          },
        },
      });
      expect(invalidGtNaN.rows).toEqual([]);

      const invalidGte = yield* engine.snapshot("orders", {
        where: {
          price: {
            // @ts-expect-error malformed runtime queries must not broaden results.
            gte: "9",
          },
        },
      });
      expect(invalidGte.rows).toEqual([]);

      const invalidLt = yield* engine.snapshot("orders", {
        where: {
          price: {
            // @ts-expect-error malformed runtime queries must not broaden results.
            lt: "11",
          },
        },
      });
      expect(invalidLt.rows).toEqual([]);

      const invalidLte = yield* engine.snapshot("orders", {
        where: {
          price: {
            // @ts-expect-error malformed runtime queries must not broaden results.
            lte: "11",
          },
        },
      });
      expect(invalidLte.rows).toEqual([]);

      const invalidIn = yield* engine.snapshot("orders", {
        where: {
          status: {
            // @ts-expect-error malformed runtime queries must not throw or broaden results.
            in: 1,
          },
        },
      });
      expect(invalidIn.rows).toEqual([]);

      const invalidStartsWith = yield* engine.snapshot("orders", {
        where: {
          customerId: {
            // @ts-expect-error malformed runtime queries must not throw or broaden results.
            startsWith: Symbol("customer"),
          },
        },
      });
      expect(invalidStartsWith.rows).toEqual([]);

      const invalidNeq = yield* engine.snapshot("orders", {
        where: {
          price: {
            // @ts-expect-error malformed runtime queries must not broaden results.
            neq: "10",
          },
        },
      });
      expect(invalidNeq.rows).toEqual([]);

      const invalidNeqNaN = yield* engine.snapshot("orders", {
        where: {
          price: {
            neq: Number.NaN,
          },
        },
      });
      expect(invalidNeqNaN.rows).toEqual([]);

      const undefinedEquals = yield* engine.snapshot("orders", {
        where: {
          // @ts-expect-error malformed runtime queries must not broaden results.
          status: {
            eq: undefined,
          },
        },
      });
      expect(undefinedEquals.rows).toEqual([]);

      const undefinedDirectRuntimeQuery: object = {
        where: Object.fromEntries([["status", undefined]]),
      };
      const undefinedDirectFilter = yield* engine.snapshot("orders", undefinedDirectRuntimeQuery);
      expect(undefinedDirectFilter.rows).toEqual([]);

      const undefinedInFilter = yield* engine.snapshot("orders", {
        where: {
          // @ts-expect-error malformed runtime queries must not broaden results.
          status: { in: [undefined] },
        },
      });
      expect(undefinedInFilter.rows).toEqual([]);

      const sparseValues = Array<string>();
      sparseValues[1] = "open";
      const sparseRuntimeQuery: object = {
        where: {
          status: { in: sparseValues },
        },
      };
      const sparseInFilter = yield* engine.snapshot("orders", sparseRuntimeQuery);
      expect(sparseInFilter.rows).toEqual([]);

      const emptyFilter = yield* Effect.flip(
        engine.snapshot("orders", {
          where: {
            status: {},
          },
        }),
      );
      expect(emptyFilter).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported filter operator"),
      });

      const unknownOperator = yield* Effect.flip(
        engine.snapshot("orders", {
          where: {
            status: {
              // @ts-expect-error malformed runtime queries must not broaden results.
              equals: "open",
            },
          },
        }),
      );
      expect(unknownOperator).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported filter operator"),
      });

      const typoFieldEmptyFilter = yield* Effect.flip(
        engine.snapshot("orders", {
          where: {
            // @ts-expect-error malformed runtime query fields must be rejected.
            statuz: {},
          },
        }),
      );
      expect(typoFieldEmptyFilter).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unknown field: statuz"),
      });
    }),
  );

  it.effect("emits move and update operations for sort movement without full-window churn", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [order("1", "open", 10, 1), order("2", "open", 20, 2)]);
      const subscription = yield* engine.subscribe("orders", {
        orderBy: [{ field: "price", direction: "asc" }],
      });
      const take = yield* makeEventReader(subscription);
      const initialEvents = yield* take(1);
      let state = stateFromSnapshot(firstEvent(initialEvents));

      yield* engine.patch("orders", "1", { price: 30 });
      const events = yield* take(1);
      const event = firstEvent(events);
      expect(event).toMatchObject({
        type: "delta",
        operations: [
          {
            type: "move",
            key: "2",
            fromIndex: 1,
            toIndex: 0,
          },
          {
            type: "update",
            key: "1",
            row: order("1", "open", 30, 1),
            index: 1,
          },
        ],
      });
      state = expectDeltaConverges(state, event, [
        order("2", "open", 20, 2),
        order("1", "open", 30, 1),
      ]);
      expect(state.keys).toEqual(["2", "1"]);
      yield* subscription.close();
    }),
  );

  it.effect("keeps subscription deltas indexed by configured row-key tiebreaks", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("c", "open", 10, 1),
        order("a", "open", 10, 1),
        order("b", "open", 10, 1),
      ]);
      const subscription = yield* engine.subscribe("orders", {
        orderBy: [{ field: "price", direction: "asc" }],
      });
      const take = yield* makeEventReader(subscription);
      const initialEvents = yield* take(1);
      let state = stateFromSnapshot(firstEvent(initialEvents));
      expect(state.keys).toEqual(["a", "b", "c"]);

      yield* engine.patch("orders", "b", { customerId: "customer-b-updated" });
      const events = yield* take(1);
      const event = firstEvent(events);
      expect(event).toMatchObject({
        type: "delta",
        operations: [
          {
            type: "update",
            key: "b",
            row: { ...order("b", "open", 10, 1), customerId: "customer-b-updated" },
            index: 1,
          },
        ],
      });
      state = expectDeltaConverges(state, event, [
        order("a", "open", 10, 1),
        { ...order("b", "open", 10, 1), customerId: "customer-b-updated" },
        order("c", "open", 10, 1),
      ]);
      expect(state.keys).toEqual(["a", "b", "c"]);
      yield* subscription.close();
    }),
  );

  it.effect("emits an update when an optional field appears on a visible row", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));
      const query = {
        where: {
          status: "open",
        },
      } satisfies RawQuery<OrderRow>;
      const subscription = yield* engine.subscribe("orders", query);
      const take = yield* makeEventReader(subscription);
      const initialEvents = yield* take(1);
      let state = stateFromSnapshot(firstEvent(initialEvents));

      yield* engine.patch("orders", "1", { note: "newly-visible" });
      const events = yield* take(1);
      const event = firstEvent(events);
      expect(event).toMatchObject({
        type: "delta",
        operations: [
          {
            type: "update",
            key: "1",
            row: {
              ...order("1", "open", 10, 1),
              note: "newly-visible",
            },
            index: 0,
          },
        ],
      });

      const fresh = yield* engine.snapshot("orders", query);
      state = expectDeltaConverges(state, event, fresh.rows);
      expect(state.rows).toEqual([
        {
          ...order("1", "open", 10, 1),
          note: "newly-visible",
        },
      ]);
      yield* subscription.close();
    }),
  );

  it.effect("removes a deleted visible row and inserts the next row entering the window", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("1", "open", 10, 1),
        order("2", "open", 20, 2),
        order("3", "open", 30, 3),
      ]);
      const query = {
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 2,
      } satisfies RawQuery<OrderRow>;
      const subscription = yield* engine.subscribe("orders", query);
      const take = yield* makeEventReader(subscription);
      const initialEvents = yield* take(1);
      let state = stateFromSnapshot(firstEvent(initialEvents));

      yield* engine.delete("orders", "1");
      const events = yield* take(1);
      const event = firstEvent(events);
      expect(event).toMatchObject({
        type: "delta",
        operations: [
          {
            type: "remove",
            key: "1",
          },
          {
            type: "insert",
            key: "3",
            row: order("3", "open", 30, 3),
            index: 1,
          },
        ],
      });
      state = expectDeltaConverges(state, event, [
        order("2", "open", 20, 2),
        order("3", "open", 30, 3),
      ]);
      expect(state.keys).toEqual(["2", "3"]);
      yield* subscription.close();
    }),
  );

  it.effect("closes a subscriber and records health counters when its bounded queue is full", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngine({
        topics: viewServer.topics,
        subscriptionQueueCapacity: 1,
      });
      const subscription = yield* engine.subscribe("orders", {});

      yield* engine.publish("orders", order("1", "open", 10, 1));

      const health = yield* engine.health();
      expect(health.activeSubscriptions).toBe(0);
      expect(health.backpressureEvents).toBe(1);
      expect(health.topics["orders"].activeSubscriptions).toBe(0);
      expect(health.topics["orders"].backpressureEvents).toBe(1);
      expect(health.topics["orders"].maxQueueDepth).toBe(1);

      const events = yield* collectEvents(subscription);
      expect(events.map((event) => event.type)).toEqual(["status"]);
      expect(events[0]).toMatchObject({
        type: "status",
        code: "BackpressureExceeded",
      });
    }),
  );

  it.effect("falls back to default subscription capacity for invalid config capacity", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngine({
        topics: viewServer.topics,
        subscriptionQueueCapacity: Number.NaN,
      });
      const subscription = yield* engine.subscribe("orders", {});
      yield* engine.publish("orders", order("1", "open", 10, 1));

      const health = yield* engine.health();
      expect(health.activeSubscriptions).toBe(1);
      expect(health.backpressureEvents).toBe(0);
      yield* subscription.close();
    }),
  );

  it.effect("reset closes subscriptions instead of emitting lower-version deltas", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));
      const subscription = yield* engine.subscribe("orders", {});
      const take = yield* makeEventReader(subscription);
      yield* take(1);

      yield* engine.patch("orders", "1", { price: 20 });
      yield* engine.reset();

      const events = yield* take(1);
      expect(events).toHaveLength(1);
      const event = firstEvent(events);
      expect(event).toMatchObject({
        type: "delta",
        fromVersion: 1,
        toVersion: 2,
      });

      const health = yield* engine.health();
      expect(health.version).toBe(0);
      expect(health.activeSubscriptions).toBe(0);
    }),
  );
});

describe("ColumnLiveViewEngine validation and health", () => {
  it.effect("fails invalid row publishes with a typed schema error", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      const error = yield* Effect.flip(engine.publish("orders", order("1", "open", Number.NaN, 1)));

      expect(error).toMatchObject({
        _tag: "InvalidRowError",
        topic: "orders",
        message: expect.stringContaining("a finite number"),
      });
    }),
  );

  it.effect("clones non-plain object row fields before exposing them", () =>
    Effect.gen(function* () {
      const WithPayload = Schema.Struct({
        id: Schema.String,
        payload: Schema.ObjectKeyword,
      });
      const engine = yield* createColumnLiveViewEngine({
        topics: {
          payloads: {
            schema: WithPayload,
            key: "id",
          },
        },
      });
      const payload = new Map([["venue", "xnys"]]);

      const emptyObjectKeywordFilter = yield* engine.snapshot("payloads", {
        where: {
          payload: { venue: "xlon" },
        },
      });
      expect(emptyObjectKeywordFilter.rows).toEqual([]);

      yield* engine.publish("payloads", { id: "1", payload });
      yield* engine.publish("payloads", { id: "2", payload: { venue: "xlon" } });

      const snapshot = yield* engine.snapshot("payloads", {});
      expect(snapshot.rows[0]?.payload).toEqual(payload);
      expect(snapshot.rows[0]?.payload).not.toBe(payload);

      const objectFilter = yield* engine.snapshot("payloads", {
        where: {
          payload: { venue: "xlon" },
        },
      });
      expect(objectFilter.rows).toEqual([{ id: "2", payload: { venue: "xlon" } }]);
    }),
  );

  it.effect("rejects non-cloneable object rows and query filters through typed errors", () =>
    Effect.gen(function* () {
      const WithPayload = Schema.Struct({
        id: Schema.String,
        payload: Schema.ObjectKeyword,
      });
      const engine = yield* createColumnLiveViewEngine({
        topics: {
          payloads: {
            schema: WithPayload,
            key: "id",
          },
        },
      });

      const rowError = yield* Effect.flip(
        engine.publish("payloads", { id: "1", payload: new WeakMap() }),
      );
      expect(rowError._tag).toBe("InvalidRowError");

      yield* engine.publish("payloads", { id: "2", payload: { venue: "xnys" } });
      const queryError = yield* Effect.flip(
        engine.snapshot("payloads", {
          where: {
            payload: new WeakMap(),
          },
        }),
      );
      expect(queryError._tag).toBe("InvalidQueryError");
    }),
  );

  it.effect("keeps a runtime guard for unsafely cast invalid key configs", () =>
    Effect.gen(function* () {
      const invalidKeyConfig = {
        topics: {
          orders: {
            schema: Order,
            key: "missing",
          },
        },
      };
      // @ts-expect-error invalid configs can still reach runtime through untyped callers.
      const engine = yield* createColumnLiveViewEngine(invalidKeyConfig);

      const error = yield* Effect.flip(engine.publish("orders", order("1", "open", 10, 1)));

      expect(error).toMatchObject({
        _tag: "InvalidRowError",
        message: expect.stringContaining("Key field missing"),
      });
    }),
  );

  it.effect(
    "supports unsafely cast decoders without struct field metadata for unfiltered health",
    () =>
      Effect.gen(function* () {
        const engine = yield* createColumnLiveViewEngine({
          topics: {
            loose: {
              schema: Schema.ObjectKeyword,
              // @ts-expect-error ObjectKeyword has no known string fields; this exercises runtime fallback only.
              key: "id",
            },
          },
        });

        const health = yield* engine.health();
        expect(health.topics["loose"].rowCount).toBe(0);
      }),
  );

  it.effect("fails missing-key patches and key-changing patches", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      const missing = yield* Effect.flip(engine.patch("orders", "missing", { price: 20 }));
      expect(missing).toMatchObject({
        _tag: "InvalidRowError",
        message: expect.stringContaining("Cannot patch missing key"),
      });

      yield* engine.publish("orders", order("1", "open", 10, 1));
      const changedKey = yield* Effect.flip(engine.patch("orders", "1", { id: "2" }));
      expect(changedKey).toMatchObject({
        _tag: "InvalidRowError",
        message: expect.stringContaining("must not change"),
      });
    }),
  );

  it.effect("updates health row counts and versions", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      const empty = yield* engine.health();
      expect(empty.version).toBe(0);
      expect(empty.topics["orders"].rowCount).toBe(0);
      expect(empty.topics["orders"].version).toBe(0);

      yield* engine.publishMany("orders", [order("1", "open", 10, 1), order("2", "closed", 20, 2)]);
      yield* engine.patch("orders", "1", { price: 30 });
      yield* engine.delete("orders", "2");

      const mutated = yield* engine.health();
      expect(mutated.version).toBe(3);
      expect(mutated.topics["orders"].rowCount).toBe(1);
      expect(mutated.topics["orders"].version).toBe(3);

      yield* engine.reset();

      const reset = yield* engine.health();
      expect(reset.version).toBe(0);
      expect(reset.topics["orders"].rowCount).toBe(0);
      expect(reset.topics["orders"].version).toBe(0);
    }),
  );

  it.effect("reads health state when the returned Effect is executed", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const delayedMutatedHealth = engine.health();

      yield* engine.publish("orders", order("1", "open", 10, 1));

      const mutated = yield* delayedMutatedHealth;
      expect(mutated.status).toBe("ready");
      expect(mutated.version).toBe(1);
      expect(mutated.topics["orders"].rowCount).toBe(1);
      expect(mutated.topics["orders"].version).toBe(1);

      const delayedClosedHealth = engine.health();
      yield* engine.close();

      const closed = yield* delayedClosedHealth;
      expect(closed.status).toBe("stopping");
      expect(closed.topics["orders"].status).toBe("degraded");
    }),
  );

  it.effect("falls back to the default queue capacity when configured capacity is invalid", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngine({
        topics: viewServer.topics,
        subscriptionQueueCapacity: 0,
      });
      const subscription = yield* engine.subscribe("orders", {});

      yield* subscription.close();

      const health = yield* engine.health();
      expect(health.activeSubscriptions).toBe(0);
    }),
  );

  it.effect("keeps a runtime guard for untyped grouped aggregate query callers", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const groupedRuntimeQuery: object = {
        groupBy: ["status"],
        aggregates: [{ type: "count", as: "count" }],
      };

      const error = yield* Effect.flip(engine.snapshot("orders", groupedRuntimeQuery));

      expect(error).toMatchObject({
        _tag: "UnsupportedQueryError",
        message: expect.stringContaining("Grouped aggregate queries are not implemented"),
      });

      const subscribeError = yield* Effect.flip(engine.subscribe("orders", groupedRuntimeQuery));
      expect(subscribeError._tag).toBe("UnsupportedQueryError");
    }),
  );

  it.effect("does not throw defects for nullish malformed raw queries", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      const nullError = yield* Effect.flip(
        // @ts-expect-error malformed untyped callers are still handled by the Effect error channel.
        engine.snapshot("orders", null),
      );
      expect(nullError).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("Raw query must be a plain object"),
      });

      const undefinedError = yield* Effect.flip(
        // @ts-expect-error malformed untyped callers are still handled by the Effect error channel.
        engine.subscribe("orders", null),
      );
      expect(undefinedError._tag).toBe("InvalidQueryError");

      const undefinedSnapshot = yield* engine.snapshot(
        "orders",
        // @ts-expect-error undefined is normalized to the empty raw query at runtime.
        undefined,
      );
      expect(undefinedSnapshot.rows).toEqual([]);
    }),
  );

  it.effect("fails malformed raw query shapes through the typed error channel", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));

      const invalidWhere = yield* Effect.flip(
        engine.snapshot("orders", {
          // @ts-expect-error malformed runtime query where must be rejected.
          where: "bad",
        }),
      );
      expect(invalidWhere).toMatchObject({
        _tag: "InvalidQueryError",
        topic: "orders",
        message: expect.stringContaining("where"),
      });

      const invalidWhereArray = yield* Effect.flip(
        engine.snapshot("orders", {
          // @ts-expect-error malformed runtime query where array must be rejected.
          where: [],
        }),
      );
      expect(invalidWhereArray._tag).toBe("InvalidQueryError");

      // @ts-expect-error runtime validation still rejects hostile untyped inputs.
      const invalidTopLevelArray = yield* Effect.flip(engine.snapshot("orders", []));
      expect(invalidTopLevelArray._tag).toBe("InvalidQueryError");

      // @ts-expect-error runtime validation still rejects hostile untyped inputs.
      const invalidTopLevelMap = yield* Effect.flip(engine.snapshot("orders", new Map()));
      expect(invalidTopLevelMap).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("plain object"),
      });

      const invalidWhereMapQuery: object = {
        where: new Map([["status", "open"]]),
      };
      const invalidWhereMap = yield* Effect.flip(engine.snapshot("orders", invalidWhereMapQuery));
      expect(invalidWhereMap).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("where"),
      });

      const unknownTopLevelRawQuery: object = {
        where: {
          status: "open",
        },
        whre: {
          status: "closed",
        },
      };
      const unknownTopLevelKey = yield* Effect.flip(
        engine.snapshot("orders", unknownTopLevelRawQuery),
      );
      expect(unknownTopLevelKey).toMatchObject({
        _tag: "InvalidQueryError",
        topic: "orders",
        message: expect.stringContaining("unsupported key: whre"),
      });

      const invalidOrderBy = yield* Effect.flip(
        engine.snapshot("orders", {
          // @ts-expect-error malformed runtime query orderBy must be rejected.
          orderBy: "bad",
        }),
      );
      expect(invalidOrderBy._tag).toBe("InvalidQueryError");

      const invalidFields = yield* Effect.flip(
        engine.snapshot("orders", {
          // @ts-expect-error malformed runtime query fields must be rejected.
          fields: "id",
        }),
      );
      expect(invalidFields._tag).toBe("InvalidQueryError");

      const invalidFieldEntry = yield* Effect.flip(
        engine.snapshot("orders", {
          // @ts-expect-error malformed runtime query field entries must be rejected.
          fields: [1],
        }),
      );
      expect(invalidFieldEntry._tag).toBe("InvalidQueryError");

      const invalidOffset = yield* Effect.flip(
        engine.snapshot("orders", {
          // @ts-expect-error malformed runtime query offset must be rejected.
          offset: "0",
        }),
      );
      expect(invalidOffset).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("offset"),
      });

      const invalidOffsetNaN = yield* Effect.flip(
        engine.snapshot("orders", {
          offset: Number.NaN,
        }),
      );
      expect(invalidOffsetNaN).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("offset"),
      });

      const invalidOffsetNegative = yield* Effect.flip(
        engine.snapshot("orders", {
          offset: -1,
        }),
      );
      expect(invalidOffsetNegative).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("offset"),
      });

      const invalidOffsetFraction = yield* Effect.flip(
        engine.snapshot("orders", {
          offset: 0.5,
        }),
      );
      expect(invalidOffsetFraction).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("offset"),
      });

      const invalidLimit = yield* Effect.flip(
        engine.snapshot("orders", {
          // @ts-expect-error malformed runtime query limit must be rejected.
          limit: "1",
        }),
      );
      expect(invalidLimit).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("limit"),
      });

      const invalidLimitInfinity = yield* Effect.flip(
        engine.snapshot("orders", {
          limit: Number.POSITIVE_INFINITY,
        }),
      );
      expect(invalidLimitInfinity).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("limit"),
      });

      const invalidOrderByEntry = yield* Effect.flip(
        engine.snapshot("orders", {
          orderBy: [
            // @ts-expect-error malformed runtime query orderBy entry must be rejected.
            "bad",
          ],
        }),
      );
      expect(invalidOrderByEntry._tag).toBe("InvalidQueryError");

      const invalidOrderByExtraKeyQuery: object = {
        orderBy: [
          {
            field: "price",
            direction: "asc",
            typo: true,
          },
        ],
      };
      const invalidOrderByExtraKey = yield* Effect.flip(
        engine.snapshot("orders", invalidOrderByExtraKeyQuery),
      );
      expect(invalidOrderByExtraKey).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported key: typo"),
      });

      const invalidOrderByField = yield* Effect.flip(
        engine.snapshot("orders", {
          orderBy: [
            // @ts-expect-error malformed runtime query orderBy field must be rejected.
            {
              direction: "asc",
            },
          ],
        }),
      );
      expect(invalidOrderByField._tag).toBe("InvalidQueryError");

      const unknownOrderByField = yield* Effect.flip(
        engine.snapshot("orders", {
          orderBy: [
            {
              // @ts-expect-error runtime query unknown orderBy fields must be rejected.
              field: "prcie",
              direction: "asc",
            },
          ],
        }),
      );
      expect(unknownOrderByField).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("orderBy"),
      });

      const unknownProjectionField = yield* Effect.flip(
        engine.snapshot("orders", {
          // @ts-expect-error runtime query unknown projected fields must be rejected.
          fields: ["prcie"],
        }),
      );
      expect(unknownProjectionField).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("fields"),
      });

      const unknownWhereField = yield* Effect.flip(
        engine.snapshot("orders", {
          where: {
            // @ts-expect-error runtime query unknown where fields must be rejected.
            prcie: 10,
          },
        }),
      );
      expect(unknownWhereField).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("where"),
      });

      const unknownFilterOperator = yield* Effect.flip(
        engine.snapshot("orders", {
          where: {
            // @ts-expect-error runtime query unknown filter operators must be rejected.
            status: { equals: "open" },
          },
        }),
      );
      expect(unknownFilterOperator).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported filter operator"),
      });

      const mixedKnownAndUnknownFilterOperator = yield* Effect.flip(
        engine.snapshot("orders", {
          where: {
            // @ts-expect-error runtime query unknown filter operators must be rejected.
            status: { eq: "open", typo: true },
          },
        }),
      );
      expect(mixedKnownAndUnknownFilterOperator).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported filter operator"),
      });

      const invalidOrderByDirection = yield* Effect.flip(
        engine.snapshot("orders", {
          orderBy: [
            {
              field: "price",
              // @ts-expect-error malformed runtime query orderBy direction must be rejected.
              direction: "sideways",
            },
          ],
        }),
      );
      expect(invalidOrderByDirection._tag).toBe("InvalidQueryError");
    }),
  );

  it.effect("fails unknown topics and closes the engine idempotently", () =>
    Effect.gen(function* () {
      const looseConfig: ColumnLiveViewEngineConfig<Topics> = { topics: viewServer.topics };
      const engine = yield* createColumnLiveViewEngine(looseConfig);
      const subscription = yield* engine.subscribe("orders", {});

      const missingTopicConfig: ColumnLiveViewEngineConfig<Record<string, Topics["orders"]>> = {
        topics: {
          orders: viewServer.topics.orders,
        },
      };
      const looseEngine = yield* createColumnLiveViewEngine(missingTopicConfig);
      const missing = yield* Effect.flip(looseEngine.snapshot("missing", {}));
      expect(missing._tag).toBe("InvalidTopicError");

      yield* engine.close();
      yield* engine.close();
      yield* subscription.close();

      const closedHealth = yield* engine.health();
      expect(closedHealth.status).toBe("stopping");

      const closedError = yield* Effect.flip(engine.publish("orders", order("1", "open", 10, 1)));
      expect(closedError._tag).toBe("EngineClosedError");
    }),
  );
});
