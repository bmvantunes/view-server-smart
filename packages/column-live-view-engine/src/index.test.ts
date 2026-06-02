import { describe, expect, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  type DeltaEvent,
  type GroupedQuery,
  type RawQuery,
  type SnapshotEvent,
  type StatusEvent,
} from "@view-server/config";
import { Cause, Deferred, Effect, Exit, Fiber, Option, Schema, Scope, Stream } from "effect";
import { format as formatBigDecimal, fromStringUnsafe, isBigDecimal } from "effect/BigDecimal";
import {
  createColumnLiveViewEngine,
  EngineClosedError,
  InvalidQueryError,
  InvalidRowError,
  InvalidTopicError,
  type ColumnLiveViewEngine,
  type ColumnLiveViewEngineConfig,
  type ColumnLiveViewEngineEvent,
  type ColumnLiveViewSubscription,
} from "./index";
import {
  acquireMaterializedQueryExecution,
  acquireRawQueryExecution,
  activeStoreRawQueryExecutionCount,
  releaseMaterializedQueryExecution,
} from "./active-query";
import {
  acquireLiveSubscriptionHandoff,
  closeInterruptedAcquiredSubscription,
  type LiveTopicSubscriber,
} from "./live-subscription";
import {
  prepareRawQuery,
  rawQueryCompilerMetadata,
  stableQueryValueString,
} from "./raw-query-compiler";
import { evaluateCompiledGroupedQuery, prepareGroupedQuery } from "./grouped-query-compiler";
import { cloneRecord, cloneRow, fieldValue, rowsEqual } from "./row-values";
import {
  closeTopicStoreSubscriptions,
  collectTopicStoreHealth,
  registerTopicStoreSubscription,
  resetTopicStore,
  TopicStore,
  topicStoreRawQueryMetadata,
  topicStoreReadModel,
} from "./topic-store";

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
  operatorRangeLike: Schema.Struct({
    gte: Schema.Number,
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
type PositionRow = typeof Position.Type;
type InstrumentRow = typeof Instrument.Type;

const orderSelect: readonly ["id", "customerId", "status", "price", "region", "updatedAt"] = [
  "id",
  "customerId",
  "status",
  "price",
  "region",
  "updatedAt",
];
const instrumentSelect: readonly ["id", "metadata", "operatorLike", "operatorRangeLike", "tags"] = [
  "id",
  "metadata",
  "operatorLike",
  "operatorRangeLike",
  "tags",
];

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

const position = (
  id: string,
  symbol: string,
  quantity: bigint,
  price: string,
  active = true,
): PositionRow => ({
  id,
  accountId: `account-${id}`,
  symbol,
  active,
  quantity,
  price: fromStringUnsafe(price),
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
  operatorRangeLike: {
    gte: tier,
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

const normalizeDecimalFields = <Row extends object>(
  rows: ReadonlyArray<Row>,
): ReadonlyArray<Record<string, unknown>> =>
  rows.map((row) =>
    Object.fromEntries(
      Object.entries(row).map(([key, value]) => [
        key,
        isBigDecimal(value) ? formatBigDecimal(value) : value,
      ]),
    ),
  );

const normalizeDecimalAndBigIntFields = <Row extends object>(
  rows: ReadonlyArray<Row>,
): ReadonlyArray<Record<string, unknown>> =>
  rows.map((row) =>
    Object.fromEntries(
      Object.entries(row).map(([key, value]) => [
        key,
        isBigDecimal(value)
          ? formatBigDecimal(value)
          : typeof value === "bigint"
            ? value.toString()
            : value,
      ]),
    ),
  );

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
  expect(events).not.toStrictEqual([]);
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

const expectStatusEvent: <Row>(
  event: ColumnLiveViewEngineEvent<Row>,
) => asserts event is StatusEvent = (event) => {
  expect(event).toMatchObject({ type: "status" });
};

const expectSnapshotRows = <Row>(
  event: ColumnLiveViewEngineEvent<Row>,
  rows: ReadonlyArray<Row>,
) => {
  expectSnapshotEvent(event);
  expect(event.rows).toStrictEqual(rows);
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
  expect(nextState.rows).toStrictEqual(freshRows);
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
        select: orderSelect,
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

      expect(snapshot).toStrictEqual({
        rows: [order("1", "open", 10, 1, "emea")],
        totalRows: 2,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });

      const equalStringSort = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "status", direction: "asc" }],
      });
      expect(rowIds(equalStringSort.rows)).toStrictEqual(["1", "2", "4", "5", "6"]);

      const reverseInsertEngine = yield* makeEngine();
      yield* reverseInsertEngine.publishMany("orders", [
        order("b", "open", 10, 1),
        order("a", "open", 20, 2),
      ]);
      const equalStringSortReverseInsert = yield* reverseInsertEngine.snapshot("orders", {
        select: ["id"],
        orderBy: [{ field: "status", direction: "asc" }],
      });
      expect(rowIds(equalStringSortReverseInsert.rows)).toStrictEqual(["a", "b"]);
    }),
  );

  it.effect("returns only selected fields", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      yield* engine.publish("orders", order("1", "open", 10, 1));

      const snapshot = yield* engine.snapshot("orders", {
        select: ["customerId", "status", "updatedAt"],
        where: {
          status: "open",
        },
      });

      expect(snapshot.rows).toStrictEqual([
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

      const snapshot = yield* engine.snapshot("orders", { select: orderSelect });
      expect(snapshot.rows).toHaveLength(1);
      Object.assign(snapshot.rows[0]!, { price: 999 });

      const fresh = yield* engine.snapshot("orders", { select: orderSelect });
      expect(fresh.rows).toStrictEqual([order("1", "open", 10, 1)]);
    }),
  );

  it.effect("deep-clones nested rows and supports object-valued equality filters", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const emptyStructuredQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
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
      expect(emptyStructuredQuery.rows).toStrictEqual([]);

      yield* engine.publishMany("instruments", [
        instrument("1", "xnys", 1, ["equity", "us"]),
        instrument("2", "xlon", 2, ["equity", "uk"]),
      ]);

      const metadataQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
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
      expect(rowIds(metadataQuery.rows)).toStrictEqual(["1"]);

      const arrayQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: {
          tags: ["equity", "us"],
        },
      });
      expect(rowIds(arrayQuery.rows)).toStrictEqual(["1"]);

      const operatorObjectQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
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
      expect(rowIds(operatorObjectQuery.rows)).toStrictEqual(["2"]);

      const operatorLikeDirectObjectQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: {
          operatorLike: {
            eq: "xnys",
          },
        },
      });
      expect(rowIds(operatorLikeDirectObjectQuery.rows)).toStrictEqual(["1"]);

      const operatorRangeLikeDirectObjectQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: {
          operatorRangeLike: {
            gte: 2,
          },
        },
      });
      expect(rowIds(operatorRangeLikeDirectObjectQuery.rows)).toStrictEqual(["2"]);

      const operatorLikeWrappedObjectQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: {
          operatorLike: {
            eq: {
              eq: "xlon",
            },
          },
        },
      });
      expect(rowIds(operatorLikeWrappedObjectQuery.rows)).toStrictEqual(["2"]);

      const operatorLikeObjectNeq = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: {
          operatorLike: {
            neq: {
              eq: "not-present",
            },
          },
        },
      });
      expect(rowIds(operatorLikeObjectNeq.rows)).toStrictEqual(["1", "2"]);

      const operatorLikeObjectNeqEqual = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: {
          operatorLike: {
            neq: {
              eq: "xnys",
            },
          },
        },
      });
      expect(rowIds(operatorLikeObjectNeqEqual.rows)).toStrictEqual(["2"]);

      const objectInQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
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
      expect(rowIds(objectInQuery.rows)).toStrictEqual(["2"]);

      const invalidObjectRuntimeQuery: object = {
        select: ["id"],
        where: {
          operatorLike: { in: [undefined] },
        },
      };
      // @ts-expect-error runtime validation handles hostile untyped structured filters.
      const invalidObjectInQuery = yield* engine.snapshot("instruments", invalidObjectRuntimeQuery);
      expect(rowIds(invalidObjectInQuery.rows)).toStrictEqual([]);

      const fullSnapshot = yield* engine.snapshot("instruments", {
        select: ["id", "metadata", "tags"],
      });
      expect(fullSnapshot.rows).toHaveLength(2);
      Object.assign(Object(fullSnapshot.rows[0]).metadata.risk, { tier: 999 });
      Object(fullSnapshot.rows[0]).tags.push("mutated");

      const projectedSnapshot = yield* engine.snapshot("instruments", {
        select: ["metadata", "tags"],
        where: {
          id: "1",
        },
      });
      expect(projectedSnapshot.rows).toStrictEqual([
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
        select: instrumentSelect,
        where: {
          id: "1",
        },
      });
      expect(fresh.rows).toStrictEqual([instrument("1", "xnys", 1, ["equity", "us"])]);
    }),
  );

  it.effect("does not retain nested publish or patch input references", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const original = instrument("1", "xnys", 1, ["equity", "us"]);
      yield* engine.publish("instruments", original);

      Object.assign(original.metadata.risk, { tier: 999 });
      Object(original.tags).push("mutated-after-publish");

      const afterPublishMutation = yield* engine.snapshot("instruments", {
        select: instrumentSelect,
      });
      expect(afterPublishMutation.rows).toStrictEqual([
        instrument("1", "xnys", 1, ["equity", "us"]),
      ]);

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
        operatorRangeLike: {
          gte: 2,
        },
        tags: ["equity", "uk"],
      };
      yield* engine.patch("instruments", "1", patch);

      patch.metadata.risk.tier = 777;
      patch.operatorLike.eq = "mutated-after-patch";
      patch.operatorRangeLike.gte = 777;
      patch.tags.push("mutated-after-patch");

      const afterPatchMutation = yield* engine.snapshot("instruments", {
        select: instrumentSelect,
      });
      expect(afterPatchMutation.rows).toStrictEqual([instrument("1", "xlon", 2, ["equity", "uk"])]);
    }),
  );

  it.effect("evaluates grouped snapshots with aggregate ordering and windows", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("1", "open", 10, 1, "emea"),
        order("2", "open", 20, 2, "amer"),
        order("3", "closed", 5, 3, "emea"),
        order("4", "closed", 20, 4, "amer"),
        order("5", "cancelled", 0, 5, "emea"),
      ]);

      const snapshot = yield* engine.snapshot("orders", {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          distinctRegions: { aggFunc: "countDistinct", field: "region" },
          totalPrice: { aggFunc: "sum", field: "price" },
          averagePrice: { aggFunc: "avg", field: "price" },
          minUpdatedAt: { aggFunc: "min", field: "updatedAt" },
          maxUpdatedAt: { aggFunc: "max", field: "updatedAt" },
        },
        orderBy: [
          { aggregate: "totalPrice", direction: "desc" },
          { field: "status", direction: "asc" },
        ],
        offset: 0,
        limit: 2,
      });

      expect(snapshot.totalRows).toBe(3);
      expect(normalizeDecimalFields(snapshot.rows)).toStrictEqual([
        {
          status: "open",
          rowCount: 2n,
          distinctRegions: 2n,
          totalPrice: "30",
          averagePrice: "15",
          minUpdatedAt: 1,
          maxUpdatedAt: 2,
        },
        {
          status: "closed",
          rowCount: 2n,
          distinctRegions: 2n,
          totalPrice: "25",
          averagePrice: "12.5",
          minUpdatedAt: 3,
          maxUpdatedAt: 4,
        },
      ]);

      const filteredSnapshot = yield* engine.snapshot("orders", {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          region: "emea",
        },
        orderBy: [{ field: "status", direction: "asc" }],
      });
      expect(filteredSnapshot.totalRows).toBe(3);
      expect(filteredSnapshot.rows).toStrictEqual([
        { status: "cancelled", rowCount: 1n },
        { status: "closed", rowCount: 1n },
        { status: "open", rowCount: 1n },
      ]);

      const noExplicitOrderSnapshot = yield* engine.snapshot("orders", {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
      });
      expect(noExplicitOrderSnapshot.rows).toStrictEqual([
        { status: "cancelled", rowCount: 1n },
        { status: "closed", rowCount: 2n },
        { status: "open", rowCount: 2n },
      ]);

      const delimiterEngine = yield* makeEngine();
      yield* delimiterEngine.publishMany("orders", [
        {
          ...order("1", "open", 10, 1, 'region:string:"emea|x'),
          customerId: 'customer|region:string:"emea',
        },
        {
          ...order("2", "open", 20, 2, "x"),
          customerId: 'customer|region:string:"emea',
        },
      ]);
      const delimiterSnapshot = yield* delimiterEngine.snapshot("orders", {
        groupBy: ["customerId", "region"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        orderBy: [
          { field: "customerId", direction: "asc" },
          { field: "region", direction: "asc" },
        ],
      });
      expect(delimiterSnapshot.totalRows).toBe(2);
      expect(delimiterSnapshot.rows).toStrictEqual([
        {
          customerId: 'customer|region:string:"emea',
          region: 'region:string:"emea|x',
          rowCount: 1n,
        },
        {
          customerId: 'customer|region:string:"emea',
          region: "x",
          rowCount: 1n,
        },
      ]);

      yield* engine.patch("orders", "1", { note: "same" });
      yield* engine.patch("orders", "2", { note: "same" });
      const equalMinMaxSnapshot = yield* engine.snapshot("orders", {
        groupBy: ["status"],
        aggregates: {
          minNote: { aggFunc: "min", field: "note" },
          maxNote: { aggFunc: "max", field: "note" },
        },
        where: {
          status: "open",
        },
      });
      expect(equalMinMaxSnapshot.rows).toStrictEqual([
        { status: "open", minNote: "same", maxNote: "same" },
      ]);

      const emptyNumericQuery: object = {
        groupBy: ["status"],
        aggregates: {
          noteTotal: { aggFunc: "sum", field: "note" },
          averageNote: { aggFunc: "avg", field: "note" },
        },
        orderBy: [{ field: "status", direction: "asc" }],
      };
      const nonNumericAggregateError = yield* Effect.flip(
        engine.snapshot(
          "orders",
          // @ts-expect-error hostile runtime callers can still aggregate non-numeric fields.
          emptyNumericQuery,
        ),
      );
      expect(nonNumericAggregateError).toBeInstanceOf(InvalidQueryError);
      expect(nonNumericAggregateError.message).toBe(
        "Grouped query aggregate noteTotal must reference a numeric field.",
      );
    }),
  );

  it.effect("normalizes BigDecimal values for grouped keys and distinct aggregates", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("positions", [
        position("1", "AAPL", 10n, "1.50"),
        position("2", "AAPL", 20n, "1.5"),
      ]);

      const groupedByPrice = yield* engine.snapshot("positions", {
        groupBy: ["price"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
      });
      expect(normalizeDecimalFields(groupedByPrice.rows)).toStrictEqual([
        {
          price: "1.5",
          rowCount: 2n,
        },
      ]);

      const distinctPrice = yield* engine.snapshot("positions", {
        groupBy: ["symbol"],
        aggregates: {
          distinctPrice: { aggFunc: "countDistinct", field: "price" },
        },
      });
      expect(distinctPrice.rows).toStrictEqual([
        {
          symbol: "AAPL",
          distinctPrice: 1n,
        },
      ]);
    }),
  );

  it.effect("evaluates grouped bigint aggregate states", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("positions", [
        position("1", "AAPL", 10n, "1.00"),
        position("2", "AAPL", 20n, "2.00"),
        position("3", "MSFT", 5n, "3.00"),
      ]);

      const bigintSnapshot = yield* engine.snapshot("positions", {
        groupBy: ["symbol"],
        aggregates: {
          totalQuantity: { aggFunc: "sum", field: "quantity" },
          averageQuantity: { aggFunc: "avg", field: "quantity" },
          minQuantity: { aggFunc: "min", field: "quantity" },
          maxQuantity: { aggFunc: "max", field: "quantity" },
        },
        orderBy: [{ aggregate: "totalQuantity", direction: "desc" }],
      });
      expect(normalizeDecimalAndBigIntFields(bigintSnapshot.rows)).toStrictEqual([
        {
          symbol: "AAPL",
          totalQuantity: "30",
          averageQuantity: "15",
          minQuantity: "10",
          maxQuantity: "20",
        },
        {
          symbol: "MSFT",
          totalQuantity: "5",
          averageQuantity: "5",
          minQuantity: "5",
          maxQuantity: "5",
        },
      ]);
    }),
  );

  it.effect("ignores malformed runtime values for bigint grouped sums", () =>
    Effect.gen(function* () {
      const PositionForCompiler = Schema.Struct({
        id: Schema.String,
        symbol: Schema.String,
        quantity: Schema.BigInt,
      });
      const rows = new Map<string, object>([
        ["bad", { id: "bad", symbol: "AAPL", quantity: "not-a-bigint" }],
        ["good", { id: "good", symbol: "AAPL", quantity: 3n }],
      ]);
      const compiled = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(PositionForCompiler),
        {
          groupBy: ["symbol"],
          aggregates: {
            totalQuantity: { aggFunc: "sum", field: "quantity" },
          },
        },
      );
      const evaluation = evaluateCompiledGroupedQuery(
        {
          rows: () => rows,
          version: () => 1,
        },
        compiled,
      );
      expect(normalizeDecimalAndBigIntFields(evaluation.rows)).toStrictEqual([
        {
          symbol: "AAPL",
          totalQuantity: "3",
        },
      ]);
    }),
  );

  it.effect("evaluates grouped mixed numeric aggregate states", () =>
    Effect.gen(function* () {
      const Mixed = Schema.Struct({
        id: Schema.String,
        group: Schema.String,
        amount: Schema.Union([Schema.Number, Schema.BigInt, Schema.BigDecimal]),
        optionalQuantity: Schema.Union([Schema.BigInt, Schema.Undefined]),
      });
      const mixedViewServer = defineViewServerConfig({
        topics: {
          mixed: {
            schema: Mixed,
            key: "id",
          },
        },
      });
      const mixedEngine = yield* createColumnLiveViewEngine({
        topics: mixedViewServer.topics,
      });
      yield* mixedEngine.publishMany("mixed", [
        { id: "1", group: "x", amount: 1n, optionalQuantity: 5n },
        { id: "2", group: "x", amount: 2, optionalQuantity: undefined },
        { id: "3", group: "x", amount: fromStringUnsafe("3.5"), optionalQuantity: 7n },
        { id: "4", group: "x", amount: Number.NaN, optionalQuantity: undefined },
        { id: "5", group: "y", amount: Number.NaN, optionalQuantity: undefined },
        { id: "6", group: "z", amount: 1n, optionalQuantity: 1n },
        { id: "7", group: "z", amount: 2n, optionalQuantity: 2n },
      ]);
      const mixedQuery = {
        groupBy: ["group"],
        aggregates: {
          totalAmount: { aggFunc: "sum", field: "amount" },
          averageAmount: { aggFunc: "avg", field: "amount" },
        },
      } satisfies GroupedQuery<typeof Mixed.Type>;
      const mixedSnapshot = yield* mixedEngine.snapshot("mixed", mixedQuery);
      expect(normalizeDecimalAndBigIntFields(mixedSnapshot.rows)).toStrictEqual([
        {
          group: "x",
          totalAmount: "6.5",
          averageAmount:
            "2.166666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666667e+0",
        },
        {
          group: "y",
          totalAmount: "0",
          averageAmount: "0",
        },
        {
          group: "z",
          totalAmount: "3",
          averageAmount: "1.5",
        },
      ]);
    }),
  );

  it.effect("keeps non-plain object grouped keys distinct by stable value", () =>
    Effect.gen(function* () {
      const Payload = Schema.Struct({
        id: Schema.String,
        payload: Schema.ObjectKeyword,
      });
      const payloadViewServer = defineViewServerConfig({
        topics: {
          payloads: {
            schema: Payload,
            key: "id",
          },
        },
      });
      const payloadEngine = yield* createColumnLiveViewEngine({
        topics: payloadViewServer.topics,
      });
      yield* payloadEngine.publishMany("payloads", [
        { id: "map-a-1", payload: new Map([["venue", "xnys"]]) },
        { id: "map-b", payload: new Map([["venue", "xlon"]]) },
        { id: "map-a-2", payload: new Map([["venue", "xnys"]]) },
      ]);

      const snapshot = yield* payloadEngine.snapshot("payloads", {
        groupBy: ["payload"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        orderBy: [{ aggregate: "rowCount", direction: "desc" }],
      });

      expect(snapshot.totalRows).toBe(2);
      expect(snapshot.rows).toStrictEqual([
        {
          payload: new Map([["venue", "xnys"]]),
          rowCount: 2n,
        },
        {
          payload: new Map([["venue", "xlon"]]),
          rowCount: 1n,
        },
      ]);
    }),
  );

  it.effect("shares materialized grouped subscriptions and emits grouped deltas", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [order("1", "open", 10, 1), order("2", "closed", 5, 2)]);
      const query = {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          totalPrice: { aggFunc: "sum", field: "price" },
        },
        orderBy: [{ aggregate: "rowCount", direction: "desc" }],
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly rowCount: { readonly aggFunc: "count" };
          readonly totalPrice: { readonly aggFunc: "sum"; readonly field: "price" };
        };
        readonly orderBy: readonly [{ readonly aggregate: "rowCount"; readonly direction: "desc" }];
      };

      const first = yield* engine.subscribe("orders", query);
      const second = yield* engine.subscribe("orders", query);
      const readFirst = yield* makeEventReader(first);
      const readSecond = yield* makeEventReader(second);
      const firstSnapshot = firstEvent(yield* readFirst(1));
      const secondSnapshot = firstEvent(yield* readSecond(1));
      expectSnapshotEvent(firstSnapshot);
      expectSnapshotEvent(secondSnapshot);
      expect(normalizeDecimalFields(firstSnapshot.rows)).toStrictEqual([
        { status: "closed", rowCount: 1n, totalPrice: "5" },
        { status: "open", rowCount: 1n, totalPrice: "10" },
      ]);

      let health = yield* engine.health();
      expect(health.topics.orders.activeViews).toBe(1);
      expect(health.topics.orders.activeSubscriptions).toBe(2);

      yield* engine.publish("orders", order("3", "open", 7, 3));
      const firstDelta = firstEvent(yield* readFirst(1));
      const secondDelta = firstEvent(yield* readSecond(1));
      expectDeltaEvent(firstDelta);
      expectDeltaEvent(secondDelta);
      expect(firstDelta.totalRows).toBe(2);
      expect(secondDelta.totalRows).toBe(2);

      yield* first.close();
      health = yield* engine.health();
      expect(health.topics.orders.activeViews).toBe(1);
      expect(health.topics.orders.activeSubscriptions).toBe(1);

      yield* second.close();
      health = yield* engine.health();
      expect(health.topics.orders.activeViews).toBe(0);
      expect(health.topics.orders.activeSubscriptions).toBe(0);
    }),
  );

  it.effect("rejects malformed grouped queries through the typed error channel", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const sparseGroupBy = Array<string>();
      sparseGroupBy[1] = "status";
      const nonPlainGroupedQuery: object = Object.assign(new Map(), {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
      });
      const invalidCases: ReadonlyArray<{
        readonly query: unknown;
        readonly message: string;
      }> = [
        { query: null, message: "plain object" },
        { query: nonPlainGroupedQuery, message: "plain object" },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            typo: true,
          },
          message: "unsupported key: typo",
        },
        {
          query: {
            groupBy: ["status"],
            select: ["id"],
            aggregates: { rowCount: { aggFunc: "count" } },
          },
          message: "must not include select",
        },
        {
          query: { groupBy: [], aggregates: { rowCount: { aggFunc: "count" } } },
          message: "groupBy",
        },
        {
          query: { groupBy: sparseGroupBy, aggregates: { rowCount: { aggFunc: "count" } } },
          message: "groupBy",
        },
        {
          query: { groupBy: [1], aggregates: { rowCount: { aggFunc: "count" } } },
          message: "groupBy",
        },
        {
          query: { groupBy: ["missing"], aggregates: { rowCount: { aggFunc: "count" } } },
          message: "unknown field: missing",
        },
        { query: { groupBy: ["status"], aggregates: [] }, message: "aggregates" },
        {
          query: { groupBy: ["status"], aggregates: { status: { aggFunc: "count" } } },
          message: "collides",
        },
        {
          query: { groupBy: ["status"], aggregates: { constructor: { aggFunc: "count" } } },
          message: "aggregate alias is not allowed",
        },
        {
          query: { groupBy: ["status"], aggregates: { rowCount: "count" } },
          message: "plain object",
        },
        {
          query: { groupBy: ["status"], aggregates: { rowCount: { aggFunc: "median" } } },
          message: "unsupported aggFunc",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count", field: "price" } },
          },
          message: "must not include a field",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { total: { aggFunc: "sum", field: "price", typo: true } },
          },
          message: "unsupported key: typo",
        },
        {
          query: { groupBy: ["status"], aggregates: { total: { aggFunc: "sum" } } },
          message: "field must be a string",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { total: { aggFunc: "sum", field: "missing" } },
          },
          message: "unknown field: missing",
        },
        {
          query: { groupBy: ["status"], aggregates: { rowCount: { aggFunc: "count" } }, where: [] },
          message: "where",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: "bad",
          },
          message: "orderBy",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: ["bad"],
          },
          message: "plain objects",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: [{ field: "status", direction: "asc", typo: true }],
          },
          message: "unsupported key: typo",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: [{ field: "status", direction: "sideways" }],
          },
          message: "direction",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: [{ direction: "asc" }],
          },
          message: "choose field or aggregate",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: [{ field: "status", aggregate: "rowCount", direction: "asc" }],
          },
          message: "choose field or aggregate",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: [{ field: "price", direction: "asc" }],
          },
          message: "field must be present in groupBy",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: [{ aggregate: "missing", direction: "asc" }],
          },
          message: "aggregate must reference an aggregate alias",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            offset: -1,
          },
          message: "offset",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            limit: "1",
          },
          message: "limit",
        },
      ];

      for (const invalidCase of invalidCases) {
        const error = yield* Effect.flip(
          // @ts-expect-error hostile untyped runtime grouped query is still handled by runtime guards.
          engine.snapshot("orders", invalidCase.query),
        );
        expect(error._tag).toBe("InvalidQueryError");
        expect(error.message).toContain(invalidCase.message);
      }

      const orderMetadata = rawQueryCompilerMetadata(Order);
      const inconsistentMetadata = {
        ...orderMetadata,
        fieldMetadata: new Map(),
      };
      const missingSumResultKind = yield* Effect.flip(
        prepareGroupedQuery<typeof Order.Type, object>("orders", inconsistentMetadata, {
          groupBy: ["status"],
          aggregates: { totalPrice: { aggFunc: "sum", field: "price" } },
        }),
      );
      expect(missingSumResultKind).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("must reference a numeric field"),
      });
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
        select: ["id"],
        where: {
          quantity: { gt: 9n },
          price: { gte: fromStringUnsafe("2.00") },
        },
        orderBy: [
          { field: "active", direction: "asc" },
          { field: "price", direction: "asc" },
        ],
      });

      expect(rowIds(snapshot.rows)).toStrictEqual(["position-2", "position-1"]);

      const fallbackOrdered = yield* engine.snapshot("positions", {
        select: ["id"],
        orderBy: [{ field: "active", direction: "asc" }],
      });
      expect(rowIds(fallbackOrdered.rows)).toStrictEqual([
        "position-2",
        "position-1",
        "position-3",
        "position-4",
      ]);

      const symbolOrdered = yield* engine.snapshot("positions", {
        select: ["id"],
        orderBy: [{ field: "symbol", direction: "desc" }],
        where: {
          price: { eq: fromStringUnsafe("1.00") },
        },
      });
      expect(rowIds(symbolOrdered.rows)).toStrictEqual(["position-3", "position-4"]);

      const quantityOrdered = yield* engine.snapshot("positions", {
        select: ["id"],
        orderBy: [{ field: "quantity", direction: "asc" }],
      });
      expect(rowIds(quantityOrdered.rows)).toStrictEqual([
        "position-3",
        "position-1",
        "position-4",
        "position-2",
      ]);

      const booleanNotEqual = yield* engine.snapshot("positions", {
        select: ["id"],
        where: {
          active: { neq: false },
        },
        orderBy: [{ field: "symbol", direction: "asc" }],
      });
      expect(rowIds(booleanNotEqual.rows)).toStrictEqual([
        "position-1",
        "position-4",
        "position-3",
      ]);

      const decimalNotEqual = yield* engine.snapshot("positions", {
        select: ["id"],
        where: {
          price: { neq: fromStringUnsafe("2.00") },
        },
        orderBy: [{ field: "symbol", direction: "asc" }],
      });
      expect(rowIds(decimalNotEqual.rows)).toStrictEqual([
        "position-1",
        "position-4",
        "position-3",
      ]);
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
        select: ["id"],
        orderBy: [{ field: "metadata", direction: "asc" }],
      });
      expect(rowIds(objectOrdered.rows)).toStrictEqual(["1", "2", "3"]);

      const arrayOrdered = yield* engine.snapshot("instruments", {
        select: ["id"],
        orderBy: [{ field: "tags", direction: "desc" }],
      });
      expect(rowIds(arrayOrdered.rows)).toStrictEqual(["1", "2", "3"]);

      yield* engine.publish("orders", order("1", "open", 10, 1));
      yield* engine.publish("orders", { ...order("2", "open", 20, 2), note: "visible" });
      yield* engine.publish("orders", order("3", "open", 30, 3));
      const missingOrdered = yield* engine.snapshot("orders", {
        select: ["id"],
        orderBy: [{ field: "note", direction: "asc" }],
      });
      expect(rowIds(missingOrdered.rows)).toStrictEqual(["1", "3", "2"]);
    }),
  );

  it.effect(
    "uses the configured row key as the final tiebreaker for equal ascending sort select",
    () =>
      Effect.gen(function* () {
        const engine = yield* makeEngine();
        yield* engine.publishMany("orders", [
          order("c", "open", 10, 1),
          order("a", "open", 10, 1),
          order("b", "open", 10, 1),
        ]);

        const snapshot = yield* engine.snapshot("orders", {
          select: ["id"],
          orderBy: [{ field: "price", direction: "asc" }],
        });

        expect(rowIds(snapshot.rows)).toStrictEqual(["a", "b", "c"]);
      }),
  );

  it.effect("uses the configured row key as the default order without explicit sort select", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("c", "open", 30, 3),
        order("a", "closed", 10, 1),
        order("b", "cancelled", 20, 2),
      ]);

      const snapshot = yield* engine.snapshot("orders", { select: ["id"] });

      expect(rowIds(snapshot.rows)).toStrictEqual(["a", "b", "c"]);
    }),
  );

  it.effect(
    "uses the configured row key as the final tiebreaker for equal descending sort select",
    () =>
      Effect.gen(function* () {
        const engine = yield* makeEngine();
        yield* engine.publishMany("orders", [
          order("c", "open", 10, 1),
          order("a", "open", 10, 1),
          order("b", "open", 10, 1),
        ]);

        const snapshot = yield* engine.snapshot("orders", {
          select: ["id"],
          orderBy: [{ field: "price", direction: "desc" }],
        });

        expect(rowIds(snapshot.rows)).toStrictEqual(["a", "b", "c"]);
      }),
  );

  it.effect("uses the configured row key after all sort select compare equal", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("c", "closed", 10, 1, "emea"),
        order("a", "closed", 10, 1, "emea"),
        order("b", "closed", 10, 1, "emea"),
      ]);

      const snapshot = yield* engine.snapshot("orders", {
        select: ["id"],
        orderBy: [
          { field: "price", direction: "desc" },
          { field: "status", direction: "asc" },
          { field: "region", direction: "desc" },
          { field: "updatedAt", direction: "asc" },
        ],
      });

      expect(rowIds(snapshot.rows)).toStrictEqual(["a", "b", "c"]);
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

      const allRows = yield* engine.snapshot("orders", { select: ["id"] });
      expect(allRows.totalRows).toBe(7);

      const snapshot = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          customerId: { startsWith: "customer-" },
          price: { gt: 9 },
          updatedAt: { gte: 1, lte: 4 },
          status: { in: ["open"] },
          region: { eq: "emea" },
        },
      });
      expect(rowIds(snapshot.rows)).toStrictEqual(["7"]);

      const notOpen = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          status: { neq: "open" },
        },
      });
      expect(rowIds(notOpen.rows)).toStrictEqual(["4"]);
    }),
  );
});

describe("ColumnLiveViewEngine subscriptions", () => {
  it.effect("emits the initial snapshot before live deltas", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));

      const subscription = yield* engine.subscribe("orders", {
        select: orderSelect,
        where: {
          status: "open",
        },
      });
      const events = yield* takeEvents(subscription, 1);

      expectSnapshotRows(firstEvent(events), [order("1", "open", 10, 1)]);
      yield* subscription.close();
    }),
  );

  it.effect("emits snapshot keys for projected subscriptions without selected id select", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [order("b", "open", 10, 1), order("a", "open", 10, 1)]);

      const subscription = yield* engine.subscribe("orders", {
        select: ["customerId", "status"],
      });
      const take = yield* makeEventReader(subscription);
      const initialEvents = yield* take(1);
      const snapshot = firstEvent(initialEvents);
      expectSnapshotEvent(snapshot);
      expect(snapshot.keys).toStrictEqual(["a", "b"]);
      expect(snapshot.rows).toStrictEqual([
        { customerId: "customer-a", status: "open" },
        { customerId: "customer-b", status: "open" },
      ]);

      let state = stateFromSnapshot(snapshot);
      yield* engine.delete("orders", "a");
      const deleteEvents = yield* take(1);
      state = expectDeltaConverges(state, firstEvent(deleteEvents), [
        { customerId: "customer-b", status: "open" },
      ]);
      expect(state.keys).toStrictEqual(["b"]);
      yield* subscription.close();
    }),
  );

  it.effect("emits projected rows in subscription delta operations", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("a", "open", 10, 1));

      const subscription = yield* engine.subscribe("orders", {
        select: ["customerId", "status"],
      });
      const take = yield* makeEventReader(subscription);
      yield* take(1);

      yield* engine.patch("orders", "a", { status: "closed", price: 99 });
      const firstDelta = firstEvent(yield* take(1));
      expectDeltaEvent(firstDelta);
      expect(firstDelta.operations).toStrictEqual([
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
      expect(secondDelta.operations).toStrictEqual([
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
      const subscription = yield* engine.subscribe("orders", { select: orderSelect });

      yield* engine.publish("orders", order("1", "open", 10, 1));

      const health = yield* engine.health();
      expect(health.activeSubscriptions).toBe(1);
      expect(health.queuedEvents).toBe(2);
      expect(health.topics["orders"].queuedEvents).toBe(2);

      yield* subscription.close();
    }),
  );

  it.effect("reports current queued events after subscribers consume snapshots", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const firstSubscription = yield* engine.subscribe("orders", { select: ["id"] });
      const secondSubscription = yield* engine.subscribe("orders", { select: ["id"] });
      const takeFirst = yield* makeEventReader(firstSubscription);
      const takeSecond = yield* makeEventReader(secondSubscription);

      yield* takeFirst(1);
      yield* takeSecond(1);

      const health = yield* engine.health();
      expect(health.activeSubscriptions).toBe(2);
      expect(health.queuedEvents).toBe(0);
      expect(health.topics["orders"].activeViews).toBe(1);
      expect(health.topics["orders"].activeSubscriptions).toBe(2);
      expect(health.topics["orders"].queuedEvents).toBe(0);

      yield* firstSubscription.close();
      yield* secondSubscription.close();

      const closed = yield* engine.health();
      expect(closed.topics["orders"].activeViews).toBe(0);
    }),
  );

  it.effect("keeps shared active-query projections separate across deltas", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [order("a", "open", 10, 1), order("b", "open", 20, 2)]);

      const baseQuery = {
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "asc" }],
      } satisfies Omit<RawQuery<OrderRow>, "select">;
      const idSubscription = yield* engine.subscribe("orders", {
        ...baseQuery,
        select: ["id"],
      });
      const priceSubscription = yield* engine.subscribe("orders", {
        ...baseQuery,
        select: ["id", "price"],
      });
      const takeId = yield* makeEventReader(idSubscription);
      const takePrice = yield* makeEventReader(priceSubscription);
      let idState = stateFromSnapshot(firstEvent(yield* takeId(1)));
      let priceState = stateFromSnapshot(firstEvent(yield* takePrice(1)));

      const shared = yield* engine.health();
      expect(shared.topics["orders"].activeViews).toBe(1);

      yield* engine.patch("orders", "a", { price: 30 });
      idState = expectDeltaConverges(idState, firstEvent(yield* takeId(1)), [
        { id: "b" },
        { id: "a" },
      ]);
      priceState = expectDeltaConverges(priceState, firstEvent(yield* takePrice(1)), [
        { id: "b", price: 20 },
        { id: "a", price: 30 },
      ]);

      yield* idSubscription.close();
      yield* priceSubscription.close();

      const closed = yield* engine.health();
      expect(closed.topics["orders"].activeViews).toBe(0);
    }),
  );

  it.effect("emits publish, patch, and delete deltas that converge to fresh snapshots", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [order("1", "open", 10, 1), order("2", "closed", 20, 2)]);

      const query = {
        select: orderSelect,
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

      expect(state.rows).toStrictEqual([order("1", "open", 30, 1)]);
      yield* subscription.close();
    }),
  );

  it.effect("serializes concurrent publishes before notifying subscribers", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const subscription = yield* engine.subscribe("orders", { select: orderSelect });
      const take = yield* makeEventReader(subscription);
      yield* take(1);

      yield* Effect.all(
        ["c", "a", "b"].map((id, index) =>
          engine.publish("orders", order(id, "open", 10 + index, index)),
        ),
        { concurrency: "unbounded" },
      );

      yield* take(3);
      const fresh = yield* engine.snapshot("orders", { select: orderSelect });
      expect(rowIds(fresh.rows)).toStrictEqual(["a", "b", "c"]);
      expect(fresh.version).toBe(3);
      yield* subscription.close();
    }),
  );

  it.effect("serializes mixed concurrent writes before notifying subscribers", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [order("a", "open", 10, 1), order("b", "open", 20, 2)]);
      const subscription = yield* engine.subscribe("orders", { select: ["id"] });
      const take = yield* makeEventReader(subscription);
      yield* take(1);

      yield* Effect.all(
        [
          engine.patch("orders", "a", { price: 30 }),
          engine.delete("orders", "b"),
          engine.publish("orders", order("c", "closed", 40, 3)),
        ],
        { concurrency: "unbounded" },
      );

      yield* take(1);
      const fresh = yield* engine.snapshot("orders", { select: orderSelect });
      expect(fresh.version).toBe(4);
      expect(rowIds(fresh.rows)).toStrictEqual(["a", "c"]);
      expect(fresh.rows).toStrictEqual([order("a", "open", 30, 1), order("c", "closed", 40, 3)]);
      yield* subscription.close();
    }),
  );

  it.effect("idempotent subscription close removes active subscribers from health", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const subscription = yield* engine.subscribe("orders", { select: ["id"] });

      const active = yield* engine.health();
      expect(active.topics["orders"].activeSubscriptions).toBe(1);
      expect(active.activeSubscriptions).toBe(1);

      yield* subscription.close();
      yield* subscription.close();

      const closed = yield* engine.health();
      expect(closed.topics["orders"].activeSubscriptions).toBe(0);
      expect(closed.topics["orders"].activeViews).toBe(0);
      expect(closed.activeSubscriptions).toBe(0);
    }),
  );

  it.effect("does not record backpressure when explicit close races with publish", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const subscriptions = yield* Effect.all(
        Array.from({ length: 32 }, () => engine.subscribe("orders", { select: ["id"] })),
        { concurrency: "unbounded" },
      );

      yield* Effect.all(
        [
          Effect.all(
            subscriptions.map((subscription) => subscription.close()),
            { concurrency: "unbounded" },
          ),
          Effect.all(
            Array.from({ length: 32 }, (_, index) =>
              engine.publish("orders", order(`race-${index}`, "open", index, index)),
            ),
            { concurrency: "unbounded" },
          ),
        ],
        { concurrency: "unbounded" },
      );

      const health = yield* engine.health();
      expect(health.activeSubscriptions).toBe(0);
      expect(health.backpressureEvents).toBe(0);
      expect(health.topics["orders"].activeSubscriptions).toBe(0);
      expect(health.topics["orders"].backpressureEvents).toBe(0);
    }),
  );

  it.effect("stream finalization releases active subscribers", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const subscription = yield* engine.subscribe("orders", { select: ["id"] });

      const events = yield* takeEvents(subscription, 1);
      expect(events.map((event) => event.type)).toStrictEqual(["snapshot"]);

      const health = yield* engine.health();
      expect(health.topics["orders"].activeSubscriptions).toBe(0);
      expect(health.topics["orders"].activeViews).toBe(0);
      expect(health.activeSubscriptions).toBe(0);
    }),
  );

  it.effect("emits closed status before ending subscriptions when engine closes", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("a", "open", 10, 1));
      const subscription = yield* engine.subscribe("orders", { select: ["id"] });
      const take = yield* makeEventReader(subscription);

      const snapshot = yield* take(1);
      expectSnapshotRows(firstEvent(snapshot), [{ id: "a" }]);

      yield* engine.close();

      const closedEvents = yield* take(1);
      const closed = firstEvent(closedEvents);
      expectStatusEvent(closed);
      expect(closed).toMatchObject({
        status: "closed",
        code: "SubscriptionClosed",
        message: "Subscription closed because the engine closed.",
      });

      const health = yield* engine.health();
      expect(health.topics["orders"].activeSubscriptions).toBe(0);
      expect(health.topics["orders"].activeViews).toBe(0);
      expect(health.activeSubscriptions).toBe(0);
    }),
  );

  it.effect("does not register subscriptions after a concurrent engine close", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const subscribeAll = Effect.all(
        Array.from({ length: 64 }, () =>
          engine.subscribe("orders", { select: ["id"] }).pipe(Effect.result),
        ),
        { concurrency: "unbounded" },
      );

      yield* Effect.all([subscribeAll, engine.close()], { concurrency: "unbounded" });

      const health = yield* engine.health();
      expect(health.status).toBe("stopping");
      expect(health.topics["orders"].activeSubscriptions).toBe(0);
      expect(health.topics["orders"].activeViews).toBe(0);
      expect(health.activeSubscriptions).toBe(0);
    }),
  );

  it.effect("only closes acquired subscriptions for interrupted exits", () =>
    Effect.gen(function* () {
      let closeCount = 0;
      const subscription = {
        events: Stream.empty,
        close: () =>
          Effect.sync(() => {
            closeCount += 1;
          }),
      };

      yield* closeInterruptedAcquiredSubscription(Exit.succeed(undefined), subscription);
      yield* closeInterruptedAcquiredSubscription(Exit.interrupt(1), undefined);
      expect(closeCount).toBe(0);

      yield* closeInterruptedAcquiredSubscription(Exit.interrupt(1), subscription);
      expect(closeCount).toBe(1);
    }),
  );

  it.effect("closes acquired subscriptions when handoff is interrupted", () =>
    Effect.gen(function* () {
      const acquired = yield* Deferred.make<void>();
      const keepHandoffOpen = yield* Deferred.make<void>();
      let closeCount = 0;
      const subscription = {
        events: Stream.empty,
        close: () =>
          Effect.sync(() => {
            closeCount += 1;
          }),
      };

      const handoffFiber = yield* Effect.forkChild(
        acquireLiveSubscriptionHandoff(
          (markAcquired) =>
            Effect.gen(function* () {
              yield* markAcquired(subscription);
              return subscription;
            }),
          {
            beforeReturn: Effect.gen(function* () {
              yield* Deferred.succeed(acquired, undefined);
              yield* Deferred.await(keepHandoffOpen);
            }),
          },
        ),
      );

      yield* Deferred.await(acquired);
      yield* Fiber.interrupt(handoffFiber);
      expect(closeCount).toBe(1);
    }),
  );

  it.effect("closes acquired subscriptions when acquisition exits interrupted", () =>
    Effect.gen(function* () {
      const acquired = yield* Deferred.make<void>();
      let closeCount = 0;
      const subscription = {
        events: Stream.empty,
        close: () =>
          Effect.sync(() => {
            closeCount += 1;
          }),
      };

      const handoffFiber = yield* Effect.forkChild(
        acquireLiveSubscriptionHandoff((markAcquired) =>
          Effect.uninterruptible(
            Effect.gen(function* () {
              yield* markAcquired(subscription);
              yield* Deferred.succeed(acquired, undefined);
              yield* Effect.sleep("10 millis");
              return subscription;
            }),
          ),
        ),
      );

      yield* Deferred.await(acquired);
      yield* Fiber.interrupt(handoffFiber);
      expect(closeCount).toBe(1);
    }),
  );

  it.effect("interrupted topic-store close still releases subscribers and active queries", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      const compiled = yield* prepareRawQuery("orders", topicStoreRawQueryMetadata(store), {
        select: ["id"],
      });
      yield* acquireRawQueryExecution(topicStoreReadModel(store), compiled);

      const closeStarted = yield* Deferred.make<void>();
      const subscriber: LiveTopicSubscriber = {
        topic: "orders",
        queryId: "query-close",
        notify: () => Effect.void,
        queuedEvents: Effect.succeed(0),
        end: Effect.void,
        closeWithStatus: () =>
          Effect.gen(function* () {
            yield* Deferred.succeed(closeStarted, undefined);
            yield* Effect.sleep("10 millis");
          }),
        maxQueueDepth: 0,
        backpressureEvents: 0,
        closed: false,
      };
      yield* registerTopicStoreSubscription(store, subscriber);
      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store))).toBe(1);

      const closeFiber = yield* Effect.forkChild(closeTopicStoreSubscriptions(store));
      yield* Deferred.await(closeStarted);
      yield* Fiber.interrupt(closeFiber);

      const health = yield* collectTopicStoreHealth(store, false);
      expect(health.activeSubscriptions).toBe(0);
      expect(health.activeViews).toBe(0);
    }),
  );

  it.effect("interrupted topic-store reset still releases subscribers and active queries", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      const compiled = yield* prepareRawQuery("orders", topicStoreRawQueryMetadata(store), {
        select: ["id"],
      });
      yield* acquireRawQueryExecution(topicStoreReadModel(store), compiled);

      const closeStarted = yield* Deferred.make<void>();
      const subscriber: LiveTopicSubscriber = {
        topic: "orders",
        queryId: "query-reset",
        notify: () => Effect.void,
        queuedEvents: Effect.succeed(0),
        end: Effect.void,
        closeWithStatus: () =>
          Effect.gen(function* () {
            yield* Deferred.succeed(closeStarted, undefined);
            yield* Effect.sleep("10 millis");
          }),
        maxQueueDepth: 0,
        backpressureEvents: 0,
        closed: false,
      };
      yield* registerTopicStoreSubscription(store, subscriber);
      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store))).toBe(1);

      const resetFiber = yield* Effect.forkChild(resetTopicStore(store));
      yield* Deferred.await(closeStarted);
      yield* Fiber.interrupt(resetFiber);

      const health = yield* collectTopicStoreHealth(store, false);
      expect(health.activeSubscriptions).toBe(0);
      expect(health.activeViews).toBe(0);
      expect(health.version).toBe(0);
    }),
  );

  it.effect("materialized active queries ignore unchanged evaluations and unknown releases", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      const readModel = topicStoreReadModel(store);
      let evaluationCount = 0;
      const evaluate = () => {
        evaluationCount += 1;
        return {
          rows: [],
          keys: [],
          window: [],
          totalRows: 0,
          version: readModel.version(),
        };
      };

      const execution = yield* acquireMaterializedQueryExecution(
        readModel,
        "empty-materialized",
        evaluate,
      );
      const cursor = execution.createCursor();
      const unchanged = yield* execution.next("query-unchanged", cursor);

      expect(Option.isNone(unchanged)).toBe(true);
      expect(evaluationCount).toBe(1);
      yield* acquireMaterializedQueryExecution(readModel, "second-materialized", evaluate);
      expect(yield* activeStoreRawQueryExecutionCount(readModel)).toBe(2);
      yield* releaseMaterializedQueryExecution(readModel, "empty-materialized");
      expect(yield* activeStoreRawQueryExecutionCount(readModel)).toBe(1);
      yield* releaseMaterializedQueryExecution(readModel, "missing-materialized");
      yield* releaseMaterializedQueryExecution(readModel, "second-materialized");
      expect(yield* activeStoreRawQueryExecutionCount(readModel)).toBe(0);
    }),
  );

  it.effect("does not emit deltas for invisible updates or no-op visible patches", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [order("1", "open", 10, 1), order("2", "closed", 20, 2)]);
      const subscription = yield* engine.subscribe("orders", {
        select: orderSelect,
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
      expect(remaining).toStrictEqual([]);
    }),
  );

  it.effect("freezes subscription query semantics at subscribe time", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));
      const openStatus: OrderRow["status"] = "open";

      const query: {
        select: typeof orderSelect;
        where: {
          status: OrderRow["status"];
        };
      } = {
        select: orderSelect,
        where: {
          status: openStatus,
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
      expect(state.keys).toStrictEqual(["1", "2"]);
      yield* subscription.close();
    }),
  );

  it.effect("does not let consumer snapshot mutations corrupt subscription cursors", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("instruments", instrument("1", "xnys", 1, ["equity", "us"]));

      const subscription = yield* engine.subscribe("instruments", {
        select: instrumentSelect,
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
      expect(event.operations).toStrictEqual([
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

      const invalidGtQuery: object = {
        select: ["id"],
        where: {
          price: { gt: "9" },
        },
      };
      // @ts-expect-error malformed runtime queries must not broaden results.
      const invalidGt = yield* engine.snapshot("orders", invalidGtQuery);
      expect(invalidGt.rows).toStrictEqual([]);

      const invalidGtNaN = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          price: {
            gt: Number.NaN,
          },
        },
      });
      expect(invalidGtNaN.rows).toStrictEqual([]);

      const invalidGteQuery: object = {
        select: ["id"],
        where: {
          price: { gte: "9" },
        },
      };
      // @ts-expect-error malformed runtime queries must not broaden results.
      const invalidGte = yield* engine.snapshot("orders", invalidGteQuery);
      expect(invalidGte.rows).toStrictEqual([]);

      const invalidLtQuery: object = {
        select: ["id"],
        where: {
          price: { lt: "11" },
        },
      };
      // @ts-expect-error malformed runtime queries must not broaden results.
      const invalidLt = yield* engine.snapshot("orders", invalidLtQuery);
      expect(invalidLt.rows).toStrictEqual([]);

      const invalidLteQuery: object = {
        select: ["id"],
        where: {
          price: { lte: "11" },
        },
      };
      // @ts-expect-error malformed runtime queries must not broaden results.
      const invalidLte = yield* engine.snapshot("orders", invalidLteQuery);
      expect(invalidLte.rows).toStrictEqual([]);

      const invalidInQuery: object = {
        select: ["id"],
        where: {
          status: {
            in: 1,
          },
        },
      };
      // @ts-expect-error malformed runtime queries must not throw or broaden results.
      const invalidIn = yield* engine.snapshot("orders", invalidInQuery);
      expect(invalidIn.rows).toStrictEqual([]);

      const cyclicFilter: Array<unknown> = [];
      cyclicFilter.push(cyclicFilter);
      const cyclicQueryValue: object = {
        select: ["id"],
        where: {
          status: cyclicFilter,
        },
      };
      const cyclicQueryValueError = yield* Effect.flip(
        // @ts-expect-error hostile runtime query cycles must be rejected at the boundary.
        engine.snapshot("orders", cyclicQueryValue),
      );
      expect(cyclicQueryValueError).toMatchObject({
        _tag: "InvalidQueryError",
        message: "Raw query where field status contains unsupported query value.",
      });

      type CyclicRecord = {
        self?: CyclicRecord;
      };
      const cyclicRecord: CyclicRecord = {};
      cyclicRecord.self = cyclicRecord;
      const cyclicRecordQueryValue: object = {
        select: ["id"],
        where: {
          status: cyclicRecord,
        },
      };
      const cyclicRecordQueryValueError = yield* Effect.flip(
        // @ts-expect-error hostile runtime query object cycles must be rejected at the boundary.
        engine.snapshot("orders", cyclicRecordQueryValue),
      );
      expect(cyclicRecordQueryValueError).toMatchObject({
        _tag: "InvalidQueryError",
        message: "Raw query where field status contains unsupported query value.",
      });

      const invalidStartsWithQuery: object = {
        select: ["id"],
        where: {
          customerId: {
            startsWith: Symbol("customer"),
          },
        },
      };
      const invalidStartsWith = yield* Effect.flip(
        // @ts-expect-error malformed runtime queries must not throw or broaden results.
        engine.snapshot("orders", invalidStartsWithQuery),
      );
      expect(invalidStartsWith).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported query value"),
      });

      const invalidFunctionFilterQuery: object = {
        select: ["id"],
        where: {
          customerId: {
            eq: () => "customer",
          },
        },
      };
      const invalidFunctionFilter = yield* Effect.flip(
        // @ts-expect-error malformed runtime queries must not throw or broaden results.
        engine.snapshot("orders", invalidFunctionFilterQuery),
      );
      expect(invalidFunctionFilter).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported query value"),
      });

      let getterReads = 0;
      const throwingWhere = {};
      Object.defineProperty(throwingWhere, "status", {
        enumerable: true,
        get() {
          getterReads += 1;
          if (getterReads === 1) {
            return { eq: "open" };
          }
          throw {
            _tag: "HostileGetterFailure",
            message: "clone failed",
          };
        },
      });
      const throwingWhereQuery: object = {
        select: ["id"],
        where: throwingWhere,
      };
      const throwingWhereResult = yield* Effect.flip(
        // @ts-expect-error hostile runtime query getters must be rejected at the boundary.
        engine.snapshot("orders", throwingWhereQuery),
      );
      expect(throwingWhereResult).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("where could not be cloned"),
      });

      const stringRangeQuery: object = {
        select: ["id"],
        where: {
          status: { gte: "open" },
        },
      };
      const stringRange = yield* Effect.flip(
        // @ts-expect-error runtime query validation rejects unsupported string range operators.
        engine.snapshot("orders", stringRangeQuery),
      );
      expect(stringRange).toMatchObject({
        _tag: "InvalidQueryError",
        message: "Raw query where field status does not support range operators.",
      });

      const numericStartsWithQuery: object = {
        select: ["id"],
        where: {
          price: { startsWith: "1" },
        },
      };
      const numericStartsWith = yield* Effect.flip(
        // @ts-expect-error runtime query validation rejects unsupported numeric startsWith operators.
        engine.snapshot("orders", numericStartsWithQuery),
      );
      expect(numericStartsWith).toMatchObject({
        _tag: "InvalidQueryError",
        message: "Raw query where field price does not support startsWith.",
      });

      const arrayStartsWithQuery: object = {
        select: ["id"],
        where: {
          tags: { startsWith: "equity" },
        },
      };
      const arrayStartsWith = yield* Effect.flip(
        // @ts-expect-error runtime query validation rejects unsupported array startsWith operators.
        engine.snapshot("instruments", arrayStartsWithQuery),
      );
      expect(arrayStartsWith).toMatchObject({
        _tag: "InvalidQueryError",
        message: "Raw query where field tags does not support startsWith.",
      });

      const invalidNeqQuery: object = {
        select: ["id"],
        where: {
          price: { neq: "10" },
        },
      };
      // @ts-expect-error malformed runtime queries must not broaden results.
      const invalidNeq = yield* engine.snapshot("orders", invalidNeqQuery);
      expect(invalidNeq.rows).toStrictEqual([]);

      const invalidNeqNaN = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          price: {
            neq: Number.NaN,
          },
        },
      });
      expect(invalidNeqNaN.rows).toStrictEqual([]);

      const undefinedEqualsQuery: object = {
        select: ["id"],
        where: {
          status: {
            eq: undefined,
          },
        },
      };
      // @ts-expect-error malformed runtime queries must not broaden results.
      const undefinedEquals = yield* engine.snapshot("orders", undefinedEqualsQuery);
      expect(undefinedEquals.rows).toStrictEqual([]);

      const undefinedDirectRuntimeQuery: object = {
        select: ["id"],
        where: Object.fromEntries([["status", undefined]]),
      };
      // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
      const undefinedDirectFilter = yield* engine.snapshot("orders", undefinedDirectRuntimeQuery);
      expect(undefinedDirectFilter.rows).toStrictEqual([]);

      const undefinedInFilterQuery: object = {
        select: ["id"],
        where: {
          status: { in: [undefined] },
        },
      };
      // @ts-expect-error malformed runtime queries must not broaden results.
      const undefinedInFilter = yield* engine.snapshot("orders", undefinedInFilterQuery);
      expect(undefinedInFilter.rows).toStrictEqual([]);

      const sparseValues = Array<string>();
      sparseValues[1] = "open";
      const sparseRuntimeQuery: object = {
        select: ["id"],
        where: {
          status: { in: sparseValues },
        },
      };
      const sparseInFilter = yield* Effect.flip(
        // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
        engine.snapshot("orders", sparseRuntimeQuery),
      );
      expect(sparseInFilter).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported query value"),
      });

      const emptyFilter = yield* Effect.flip(
        engine.snapshot("orders", {
          select: ["id"],
          where: {
            status: {},
          },
        }),
      );
      expect(emptyFilter).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported filter operator"),
      });

      const unknownOperatorQuery: object = {
        select: ["id"],
        where: {
          status: {
            equals: "open",
          },
        },
      };
      const unknownOperator = yield* Effect.flip(
        // @ts-expect-error malformed runtime queries must not broaden results.
        engine.snapshot("orders", unknownOperatorQuery),
      );
      expect(unknownOperator).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported filter operator"),
      });

      const typoFieldEmptyFilterQuery: object = {
        select: ["id"],
        where: {
          statuz: {},
        },
      };
      const typoFieldEmptyFilter = yield* Effect.flip(
        // @ts-expect-error malformed runtime query where field must be rejected.
        engine.snapshot("orders", typoFieldEmptyFilterQuery),
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
        select: orderSelect,
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
      expect(state.keys).toStrictEqual(["2", "1"]);
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
        select: orderSelect,
        orderBy: [{ field: "price", direction: "asc" }],
      });
      const take = yield* makeEventReader(subscription);
      const initialEvents = yield* take(1);
      let state = stateFromSnapshot(firstEvent(initialEvents));
      expect(state.keys).toStrictEqual(["a", "b", "c"]);

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
      expect(state.keys).toStrictEqual(["a", "b", "c"]);
      yield* subscription.close();
    }),
  );

  it.effect("emits an update when an optional field appears on a visible row", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));
      const query = {
        select: [...orderSelect, "note"],
        where: {
          status: "open",
        },
      } satisfies RawQuery<OrderRow> & {
        readonly select: readonly [
          "id",
          "customerId",
          "status",
          "price",
          "region",
          "updatedAt",
          "note",
        ];
        readonly where: {
          readonly status: "open";
        };
      };
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
      expect(state.rows).toStrictEqual([
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
        select: orderSelect,
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
      expect(state.keys).toStrictEqual(["2", "3"]);
      yield* subscription.close();
    }),
  );

  it.effect("closes a subscriber and records health counters when its bounded queue is full", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngine({
        topics: viewServer.topics,
        subscriptionQueueCapacity: 1,
      });
      const subscription = yield* engine.subscribe("orders", { select: orderSelect });

      yield* engine.publish("orders", order("1", "open", 10, 1));

      const health = yield* engine.health();
      expect(health.activeSubscriptions).toBe(0);
      expect(health.backpressureEvents).toBe(1);
      expect(health.topics["orders"].activeSubscriptions).toBe(0);
      expect(health.topics["orders"].activeViews).toBe(0);
      expect(health.topics["orders"].backpressureEvents).toBe(1);
      expect(health.topics["orders"].maxQueueDepth).toBe(1);

      const events = yield* collectEvents(subscription);
      expect(events.map((event) => event.type)).toStrictEqual(["status"]);
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
      const subscription = yield* engine.subscribe("orders", { select: orderSelect });
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
      const subscription = yield* engine.subscribe("orders", { select: ["id"] });
      const take = yield* makeEventReader(subscription);
      yield* take(1);

      yield* engine.patch("orders", "1", { price: 20 });
      yield* engine.publish("orders", order("2", "open", 5, 2));
      yield* engine.reset();

      const closedRead = yield* take(1);
      expect(closedRead).toMatchObject([
        {
          type: "status",
          status: "closed",
          code: "SubscriptionClosed",
        },
      ]);

      const health = yield* engine.health();
      expect(health.version).toBe(0);
      expect(health.activeSubscriptions).toBe(0);
      expect(health.topics["orders"].activeViews).toBe(0);
    }),
  );

  it.effect("rejects reset after the engine is closed", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.close();

      const error = yield* Effect.flip(engine.reset());

      expect(error).toBeInstanceOf(EngineClosedError);
      expect(error._tag).toBe("EngineClosedError");
    }),
  );
});

describe("ColumnLiveViewEngine validation and health", () => {
  it("encodes primitive and unsupported query values deterministically", () => {
    function namedFilter() {
      return "ignored";
    }
    const anonymousFilter = () => "ignored";

    expect(JSON.parse(stableQueryValueString(null))).toStrictEqual(["null"]);
    expect(JSON.parse(stableQueryValueString(1n))).toStrictEqual(["bigint", "1"]);
    expect(JSON.parse(stableQueryValueString("x"))).toStrictEqual(["string", "x"]);
    expect(JSON.parse(stableQueryValueString(-0))).toStrictEqual(["number", "-0"]);
    expect(JSON.parse(stableQueryValueString(false))).toStrictEqual(["boolean", false]);
    expect(JSON.parse(stableQueryValueString(Symbol("filter")))).toStrictEqual([
      "unsupported",
      "symbol:filter",
    ]);
    expect(JSON.parse(stableQueryValueString(Symbol()))).toStrictEqual(["unsupported", "symbol:"]);
    expect(JSON.parse(stableQueryValueString(namedFilter))).toStrictEqual([
      "unsupported",
      "function:namedFilter",
    ]);
    expect(JSON.parse(stableQueryValueString(anonymousFilter))).toStrictEqual([
      "unsupported",
      "function:anonymousFilter",
    ]);
    expect(JSON.parse(stableQueryValueString(new Map()))).toStrictEqual(["map", []]);
    expect(
      JSON.parse(
        stableQueryValueString(
          new Map<unknown, unknown>([
            [{ id: "same" }, "b"],
            [{ id: "same" }, "a"],
          ]),
        ),
      ),
    ).toStrictEqual([
      "map",
      [
        [
          ["object", [["id", ["string", "same"]]]],
          ["string", "a"],
        ],
        [
          ["object", [["id", ["string", "same"]]]],
          ["string", "b"],
        ],
      ],
    ]);
    expect(
      JSON.parse(
        stableQueryValueString(
          new Map<unknown, unknown>([
            [{ id: "b" }, "same"],
            [{ id: "a" }, "same"],
          ]),
        ),
      ),
    ).toStrictEqual([
      "map",
      [
        [
          ["object", [["id", ["string", "a"]]]],
          ["string", "same"],
        ],
        [
          ["object", [["id", ["string", "b"]]]],
          ["string", "same"],
        ],
      ],
    ]);
    expect(JSON.parse(stableQueryValueString(new Set(["b", "a"])))).toStrictEqual([
      "set",
      [
        ["string", "a"],
        ["string", "b"],
      ],
    ]);
    class CustomQueryValue {}
    expect(JSON.parse(stableQueryValueString(new CustomQueryValue()))).toStrictEqual([
      "nonPlainObject",
      "[object Object]",
    ]);
    expect(JSON.parse(stableQueryValueString(undefined))).toStrictEqual(["undefined"]);

    const cyclicArray: Array<unknown> = [];
    cyclicArray.push(cyclicArray);
    expect(JSON.parse(stableQueryValueString(cyclicArray))).toStrictEqual(["array", [["cycle"]]]);

    const cyclicMap = new Map<unknown, unknown>();
    cyclicMap.set("self", cyclicMap);
    expect(JSON.parse(stableQueryValueString(cyclicMap))).toStrictEqual([
      "map",
      [[["string", "self"], ["cycle"]]],
    ]);

    const cyclicSet = new Set<unknown>();
    cyclicSet.add(cyclicSet);
    expect(JSON.parse(stableQueryValueString(cyclicSet))).toStrictEqual(["set", [["cycle"]]]);

    type CyclicObject = {
      self?: CyclicObject;
    };
    const cyclicObject: CyclicObject = {};
    cyclicObject.self = cyclicObject;
    expect(JSON.parse(stableQueryValueString(cyclicObject))).toStrictEqual([
      "object",
      [["self", ["cycle"]]],
    ]);
  });

  it("uses injective stable keys for structured query values", () => {
    const left = { a: "b", c: "d" };
    const right = { 'a:string:"b",c': "d" };

    expect(stableQueryValueString(left)).not.toBe(stableQueryValueString(right));
  });

  it("treats rows with different selected column counts as different", () => {
    expect(rowsEqual({ id: "1" }, { id: "1", note: "new" })).toBe(false);
  });

  it("ignores inherited row properties", () => {
    const inheritedRecord = Object.create({ inherited: "hidden" });
    inheritedRecord["id"] = "1";
    inheritedRecord["status"] = "open";

    const inheritedRow = Object.create({ inherited: "hidden" });
    inheritedRow["id"] = "1";
    inheritedRow["status"] = "open";

    expect(cloneRecord(inheritedRecord)).toStrictEqual({ id: "1", status: "open" });
    expect(cloneRow(inheritedRow)).toStrictEqual({ id: "1", status: "open" });
    expect(fieldValue(inheritedRow, "inherited")).toBeUndefined();
    expect(rowsEqual(inheritedRow, { id: "1", status: "open" })).toBe(true);
  });

  it.effect("fails invalid row publishes with a typed schema error", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      const error = yield* Effect.flip(engine.publish("orders", order("1", "open", Number.NaN, 1)));

      expect(error).toMatchObject({
        _tag: "InvalidRowError",
        topic: "orders",
        message: expect.stringContaining("a finite number"),
      });
      expect(error).toBeInstanceOf(InvalidRowError);
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
        select: ["id"],
        where: {
          payload: { venue: "xlon" },
        },
      });
      expect(emptyObjectKeywordFilter.rows).toStrictEqual([]);

      yield* engine.publish("payloads", { id: "1", payload });
      yield* engine.publish("payloads", { id: "2", payload: { venue: "xlon" } });

      const snapshot = yield* engine.snapshot("payloads", { select: ["id", "payload"] });
      expect(snapshot.rows[0]?.payload).toStrictEqual(payload);
      expect(snapshot.rows[0]?.payload).not.toBe(payload);

      const objectFilter = yield* engine.snapshot("payloads", {
        select: ["id", "payload"],
        where: {
          payload: { venue: "xlon" },
        },
      });
      expect(objectFilter.rows).toStrictEqual([{ id: "2", payload: { venue: "xlon" } }]);
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
          select: ["id"],
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

  it.effect("keeps runtime guards for non-struct topic schemas", () =>
    Effect.gen(function* () {
      const nonStructSchemaConfig = {
        topics: {
          loose: {
            schema: Schema.ObjectKeyword,
            key: "id",
          },
        },
      };
      // @ts-expect-error invalid configs can still reach runtime through untyped callers.
      const engine = yield* createColumnLiveViewEngine(nonStructSchemaConfig);
      const query: object = { select: ["id"] };

      const error = yield* Effect.flip(
        // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
        engine.snapshot("loose", query),
      );

      expect(error).toMatchObject({
        _tag: "InvalidQueryError",
        topic: "loose",
        message: expect.stringContaining("select"),
      });
    }),
  );

  it.effect("keeps runtime guards for malformed schema field metadata", () =>
    Effect.gen(function* () {
      const malformedFieldSchemaConfig = {
        topics: {
          loose: {
            schema: {
              fields: {
                id: "not-a-schema",
                label: { ast: "not-a-schema-ast" },
              },
            },
            key: "id",
          },
        },
      };
      // @ts-expect-error invalid configs can still reach runtime through untyped callers.
      const engine = yield* createColumnLiveViewEngine(malformedFieldSchemaConfig);
      const query: object = { select: ["id"] };

      const snapshot = yield* engine.snapshot(
        "loose",
        // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
        query,
      );

      expect(snapshot).toMatchObject({
        rows: [],
        totalRows: 0,
      });

      const metadata = rawQueryCompilerMetadata({
        // @ts-expect-error hostile schema metadata can contain malformed field entries.
        fields: {
          id: "not-a-schema",
          price: Schema.Number,
        },
      });
      expect(metadata.fieldNames.has("id")).toBe(true);
      expect(metadata.numericFieldNames.has("id")).toBe(false);
      expect(metadata.numericFieldNames.has("price")).toBe(true);

      const invalidNumericAggregateQuery: object = {
        groupBy: ["label"],
        aggregates: {
          totalId: { aggFunc: "sum", field: "id" },
        },
      };
      const invalidNumericAggregate = yield* Effect.flip(
        engine.snapshot(
          "loose",
          // @ts-expect-error malformed schema metadata makes the query shape untyped.
          invalidNumericAggregateQuery,
        ),
      );
      expect(invalidNumericAggregate).toMatchObject({
        _tag: "InvalidQueryError",
        topic: "loose",
        message: "Grouped query aggregate totalId must reference a numeric field.",
      });
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
      const nonPlainPatch = yield* Effect.flip(
        // @ts-expect-error hostile runtime callers can still send non-object patches.
        engine.patch("orders", "1", null),
      );
      expect(nonPlainPatch).toMatchObject({
        _tag: "InvalidRowError",
        message: expect.stringContaining("Patch must be a plain object"),
      });

      const symbolPatch = yield* Effect.flip(
        // @ts-expect-error hostile runtime callers can still send symbol patch fields.
        engine.patch("orders", "1", {
          [Symbol("bad")]: 20,
        }),
      );
      expect(symbolPatch).toMatchObject({
        _tag: "InvalidRowError",
        message: expect.stringContaining("Patch contains unknown field: Symbol(bad)"),
      });

      const changedKey = yield* Effect.flip(engine.patch("orders", "1", { id: "2" }));
      expect(changedKey).toMatchObject({
        _tag: "InvalidRowError",
        message: expect.stringContaining("must not change"),
      });

      const beforeUnknownPatch = yield* engine.health();
      const unknownField = yield* Effect.flip(
        engine.patch("orders", "1", {
          // @ts-expect-error hostile runtime callers can still send unknown patch fields.
          prcie: 20,
        }),
      );
      const afterUnknownPatch = yield* engine.health();
      expect(unknownField).toMatchObject({
        _tag: "InvalidRowError",
        message: expect.stringContaining("Patch contains unknown field: prcie"),
      });
      expect(afterUnknownPatch.version).toBe(beforeUnknownPatch.version);
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
      yield* engine.delete("orders", "missing");

      const mutated = yield* engine.health();
      expect(mutated.version).toBe(4);
      expect(mutated.topics["orders"].rowCount).toBe(1);
      expect(mutated.topics["orders"].version).toBe(4);
      expect(mutated.topics["orders"].lastMutationAt).not.toBeNull();
      expect(mutated.topics["orders"].pendingMutationBatches).toBe(0);

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

  it.effect("creates stores only for own topic definitions", () =>
    Effect.gen(function* () {
      const topicsWithInheritedDefinition: Record<string, Topics["orders"]> = Object.create({
        inherited: viewServer.topics.orders,
      });
      topicsWithInheritedDefinition["orders"] = viewServer.topics.orders;

      const engine = yield* createColumnLiveViewEngine({
        topics: topicsWithInheritedDefinition,
      });
      const health = yield* engine.health();
      expect(Object.keys(health.topics)).toStrictEqual(["orders"]);

      const inherited = yield* Effect.flip(engine.snapshot("inherited", { select: ["id"] }));
      expect(inherited).toMatchObject({
        _tag: "InvalidTopicError",
        topic: "inherited",
      });
    }),
  );

  it.effect("falls back to the default queue capacity when configured capacity is invalid", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngine({
        topics: viewServer.topics,
        subscriptionQueueCapacity: 0,
      });
      const subscription = yield* engine.subscribe("orders", { select: ["id"] });

      yield* subscription.close();

      const health = yield* engine.health();
      expect(health.activeSubscriptions).toBe(0);
    }),
  );

  it.effect("subscribes through the runtime-validated entrypoint", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const subscription = yield* engine.subscribeRuntime("orders", { select: ["id"] });
      yield* subscription.close();

      const health = yield* engine.health();
      expect(health.activeSubscriptions).toBe(0);
    }),
  );

  it.effect("keeps runtime guards for untyped grouped aggregate query callers", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const groupedRuntimeQuery: object = {
        groupBy: ["missing"],
        aggregates: { count: { aggFunc: "count" } },
      };

      const error = yield* Effect.flip(
        // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
        engine.snapshot("orders", groupedRuntimeQuery),
      );

      expect(error).toMatchObject({
        _tag: "InvalidQueryError",
        topic: "orders",
      });
      expect(error.message).toContain("unknown field: missing");

      const subscribeError = yield* Effect.flip(
        // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
        engine.subscribe("orders", groupedRuntimeQuery),
      );
      expect(subscribeError._tag).toBe("InvalidQueryError");
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

      const undefinedSnapshot = yield* Effect.flip(
        // @ts-expect-error undefined is rejected because raw queries must select columns.
        engine.snapshot("orders", undefined),
      );
      expect(undefinedSnapshot._tag).toBe("InvalidQueryError");
    }),
  );

  it.effect("fails malformed raw query shapes through the typed error channel", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));

      const invalidWhereQuery: object = {
        select: ["id"],
        where: "bad",
      };
      const invalidWhere = yield* Effect.flip(
        // @ts-expect-error malformed runtime query where must be rejected.
        engine.snapshot("orders", invalidWhereQuery),
      );
      expect(invalidWhere).toMatchObject({
        _tag: "InvalidQueryError",
        topic: "orders",
        message: expect.stringContaining("where"),
      });

      const invalidWhereArrayQuery: object = {
        select: ["id"],
        where: [],
      };
      const invalidWhereArray = yield* Effect.flip(
        // @ts-expect-error malformed runtime query where array must be rejected.
        engine.snapshot("orders", invalidWhereArrayQuery),
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
        select: ["id"],
        where: new Map([["status", "open"]]),
      };
      // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
      const invalidWhereMap = yield* Effect.flip(engine.snapshot("orders", invalidWhereMapQuery));
      expect(invalidWhereMap).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("where"),
      });

      const unknownTopLevelRawQuery: object = {
        select: ["id"],
        where: {
          status: "open",
        },
        whre: {
          status: "closed",
        },
      };
      const unknownTopLevelKey = yield* Effect.flip(
        // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
        engine.snapshot("orders", unknownTopLevelRawQuery),
      );
      expect(unknownTopLevelKey).toMatchObject({
        _tag: "InvalidQueryError",
        topic: "orders",
        message: expect.stringContaining("unsupported key: whre"),
      });

      const invalidOrderByQuery: object = {
        select: ["id"],
        orderBy: "bad",
      };
      const invalidOrderBy = yield* Effect.flip(
        // @ts-expect-error malformed runtime query orderBy must be rejected.
        engine.snapshot("orders", invalidOrderByQuery),
      );
      expect(invalidOrderBy._tag).toBe("InvalidQueryError");

      const invalidFields = yield* Effect.flip(
        engine.snapshot("orders", {
          // @ts-expect-error malformed runtime query select must be rejected.
          select: "id",
        }),
      );
      expect(invalidFields._tag).toBe("InvalidQueryError");

      const invalidFieldEntryQuery: object = {
        select: [1],
      };
      const invalidFieldEntry = yield* Effect.flip(
        engine.snapshot(
          "orders",
          // @ts-expect-error malformed runtime query field entries must be rejected.
          invalidFieldEntryQuery,
        ),
      );
      expect(invalidFieldEntry._tag).toBe("InvalidQueryError");

      const emptySelectQuery: { readonly select: ReadonlyArray<unknown> } = {
        select: [],
      };
      const invalidEmptySelect = yield* Effect.flip(
        // @ts-expect-error hostile empty select is still handled by runtime guards.
        engine.snapshot("orders", emptySelectQuery),
      );
      expect(invalidEmptySelect._tag).toBe("InvalidQueryError");

      const invalidOffsetQuery: object = {
        select: ["id"],
        offset: "0",
      };
      const invalidOffset = yield* Effect.flip(
        // @ts-expect-error malformed runtime query offset must be rejected.
        engine.snapshot("orders", invalidOffsetQuery),
      );
      expect(invalidOffset).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("offset"),
      });

      const invalidOffsetNaN = yield* Effect.flip(
        engine.snapshot("orders", {
          select: ["id"],
          offset: Number.NaN,
        }),
      );
      expect(invalidOffsetNaN).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("offset"),
      });

      const invalidOffsetNegative = yield* Effect.flip(
        engine.snapshot("orders", {
          select: ["id"],
          offset: -1,
        }),
      );
      expect(invalidOffsetNegative).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("offset"),
      });

      const invalidOffsetFraction = yield* Effect.flip(
        engine.snapshot("orders", {
          select: ["id"],
          offset: 0.5,
        }),
      );
      expect(invalidOffsetFraction).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("offset"),
      });

      const invalidLimitQuery: object = {
        select: ["id"],
        limit: "1",
      };
      const invalidLimit = yield* Effect.flip(
        // @ts-expect-error malformed runtime query limit must be rejected.
        engine.snapshot("orders", invalidLimitQuery),
      );
      expect(invalidLimit).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("limit"),
      });

      const invalidLimitInfinity = yield* Effect.flip(
        engine.snapshot("orders", {
          select: ["id"],
          limit: Number.POSITIVE_INFINITY,
        }),
      );
      expect(invalidLimitInfinity).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("limit"),
      });

      const invalidOrderByEntryQuery: object = {
        select: ["id"],
        orderBy: ["bad"],
      };
      const invalidOrderByEntry = yield* Effect.flip(
        // @ts-expect-error malformed runtime query orderBy entry must be rejected.
        engine.snapshot("orders", invalidOrderByEntryQuery),
      );
      expect(invalidOrderByEntry._tag).toBe("InvalidQueryError");

      const invalidOrderByExtraKeyQuery: object = {
        select: ["id"],
        orderBy: [
          {
            field: "price",
            direction: "asc",
            typo: true,
          },
        ],
      };
      const invalidOrderByExtraKey = yield* Effect.flip(
        // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
        engine.snapshot("orders", invalidOrderByExtraKeyQuery),
      );
      expect(invalidOrderByExtraKey).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported key: typo"),
      });

      const invalidOrderByFieldQuery: object = {
        select: ["id"],
        orderBy: [
          {
            direction: "asc",
          },
        ],
      };
      const invalidOrderByField = yield* Effect.flip(
        // @ts-expect-error malformed runtime query orderBy field must be rejected.
        engine.snapshot("orders", invalidOrderByFieldQuery),
      );
      expect(invalidOrderByField._tag).toBe("InvalidQueryError");

      const unknownOrderByFieldQuery: object = {
        select: ["id"],
        orderBy: [
          {
            field: "prcie",
            direction: "asc",
          },
        ],
      };
      const unknownOrderByField = yield* Effect.flip(
        // @ts-expect-error runtime query unknown orderBy fields must be rejected.
        engine.snapshot("orders", unknownOrderByFieldQuery),
      );
      expect(unknownOrderByField).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("orderBy"),
      });

      const unknownProjectionFieldQuery: object = {
        select: ["prcie"],
      };
      const unknownProjectionField = yield* Effect.flip(
        engine.snapshot(
          "orders",
          // @ts-expect-error runtime query unknown projected fields must be rejected.
          unknownProjectionFieldQuery,
        ),
      );
      expect(unknownProjectionField).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("select"),
      });

      const unknownWhereFieldQuery: object = {
        select: ["id"],
        where: {
          prcie: 10,
        },
      };
      const unknownWhereField = yield* Effect.flip(
        // @ts-expect-error runtime query unknown where fields must be rejected.
        engine.snapshot("orders", unknownWhereFieldQuery),
      );
      expect(unknownWhereField).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("where"),
      });

      const unknownFilterOperatorQuery: object = {
        select: ["id"],
        where: {
          status: { equals: "open" },
        },
      };
      const unknownFilterOperator = yield* Effect.flip(
        // @ts-expect-error runtime query unknown filter operators must be rejected.
        engine.snapshot("orders", unknownFilterOperatorQuery),
      );
      expect(unknownFilterOperator).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported filter operator"),
      });

      const mixedKnownAndUnknownFilterOperator = yield* Effect.flip(
        engine.snapshot("orders", {
          select: ["id"],
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

      const invalidOrderByDirectionQuery: object = {
        select: ["id"],
        orderBy: [
          {
            field: "price",
            direction: "sideways",
          },
        ],
      };
      const invalidOrderByDirection = yield* Effect.flip(
        // @ts-expect-error malformed runtime query orderBy direction must be rejected.
        engine.snapshot("orders", invalidOrderByDirectionQuery),
      );
      expect(invalidOrderByDirection._tag).toBe("InvalidQueryError");
    }),
  );

  it.effect("fails unknown topics and closes the engine idempotently", () =>
    Effect.gen(function* () {
      const looseConfig: ColumnLiveViewEngineConfig<Topics> = { topics: viewServer.topics };
      const engine = yield* createColumnLiveViewEngine(looseConfig);
      const subscription = yield* engine.subscribe("orders", { select: ["id"] });

      const missingTopicConfig: ColumnLiveViewEngineConfig<Record<string, Topics["orders"]>> = {
        topics: {
          orders: viewServer.topics.orders,
        },
      };
      const looseEngine = yield* createColumnLiveViewEngine(missingTopicConfig);
      const missing = yield* Effect.flip(looseEngine.snapshot("missing", { select: ["id"] }));
      expect(missing._tag).toBe("InvalidTopicError");
      expect(missing).toBeInstanceOf(InvalidTopicError);

      yield* engine.close();
      yield* engine.close();
      yield* subscription.close();

      const closedHealth = yield* engine.health();
      expect(closedHealth.status).toBe("stopping");

      const closedError = yield* Effect.flip(engine.publish("orders", order("1", "open", 10, 1)));
      expect(closedError._tag).toBe("EngineClosedError");
      expect(closedError).toBeInstanceOf(EngineClosedError);
    }),
  );
});
