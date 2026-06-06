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
import { ColumnarTopicStore } from "./columnar-topic-store";
import {
  acquireMaterializedQueryExecution,
  acquireRawQueryExecution,
  activeStoreRawQueryExecutionCount,
  clearStoreRawQueryExecutions,
  evaluateRawQuery,
  releaseMaterializedQueryExecution,
} from "./active-query";
import type { LiveTopicSubscriber } from "./topic-subscriber";
import {
  acquireSubscriptionHandoff,
  closeInterruptedAcquiredSubscription,
} from "./subscription-handoff";
import {
  prepareRawQuery,
  rawQueryCompilerMetadata,
  stableQueryValueString,
} from "./raw-query-compiler";
import { evaluateCompiledGroupedQuery, prepareGroupedQuery } from "./grouped-query-compiler";
import { makeIncrementalGroupedQueryExecution } from "./grouped-incremental-execution";
import { cloneRecord, cloneRow, fieldValue, rowsEqual, scalarEqualityKey } from "./row-values";
import type { TopicRowChangeBatch } from "./row-scan";
import { scanTopicRawWindow } from "./topic-raw-window-scanner";
import {
  createScalarPredicateIndexes,
  selectedPredicateCandidateSlots,
} from "./topic-predicate-candidate-index";
import {
  acquireTopicStoreSubscription,
  closeBackpressuredTopicStoreSubscription,
  closeTopicStoreSubscriptions,
  collectTopicStoreHealth,
  deleteTopicStoreRow,
  publishTopicStoreRow,
  publishTopicStoreRows,
  registerTopicStoreSubscription,
  resetTopicStore,
  TopicStore,
} from "./topic-store";
import { topicStoreRawQueryMetadata, topicStoreReadModel } from "./topic-store-state";

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

const registerTestTopicStoreSubscriber = (
  store: TopicStore,
  subscriber: LiveTopicSubscriber,
): Effect.Effect<void> =>
  acquireTopicStoreSubscription(store, (permit, markAcquired) =>
    Effect.gen(function* () {
      const subscription = {
        close: () => Effect.void,
      };
      yield* registerTopicStoreSubscription(permit, subscriber);
      yield* markAcquired(subscription);
      return subscription;
    }),
  ).pipe(Effect.asVoid);

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

const numericRowField = (row: object, field: string): number => {
  const value = fieldValue(row, field);
  if (typeof value === "number") {
    return value;
  }
  throw new Error(`Expected numeric row field ${field}.`);
};

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

  it.effect("returns exact totalRows while windowing a sorted raw snapshot", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      yield* engine.publishMany("orders", [
        order("a", "open", 1, 1),
        order("b", "open", 8, 2),
        order("c", "open", 3, 3),
        order("d", "open", 10, 4),
        order("e", "open", 5, 5),
        order("f", "open", 7, 6),
        order("g", "open", 2, 7),
        order("h", "closed", 99, 8),
      ]);

      const windowed = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "desc" }],
        offset: 2,
        limit: 3,
      });

      expect(windowed.rows).toStrictEqual([
        { id: "f", price: 7 },
        { id: "e", price: 5 },
        { id: "c", price: 3 },
      ]);
      expect(windowed.totalRows).toBe(7);

      const countOnly = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 0,
      });

      expect(countOnly.rows).toStrictEqual([]);
      expect(countOnly.totalRows).toBe(7);

      yield* engine.publish("orders", order("bb", "open", 8, 9));

      const afterTieAppend = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 4,
      });

      expect(afterTieAppend.rows).toStrictEqual([
        { id: "d", price: 10 },
        { id: "b", price: 8 },
        { id: "bb", price: 8 },
        { id: "f", price: 7 },
      ]);
      expect(afterTieAppend.totalRows).toBe(8);

      yield* engine.publish("orders", order("f", "open", 11, 10));

      const afterReplace = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 4,
      });

      expect(afterReplace.rows).toStrictEqual([
        { id: "f", price: 11 },
        { id: "d", price: 10 },
        { id: "b", price: 8 },
        { id: "bb", price: 8 },
      ]);
      expect(afterReplace.totalRows).toBe(8);

      yield* engine.publishMany("orders", [
        order("f", "open", 12, 11),
        order("i", "open", 9, 12),
        order("j", "open", 0, 13),
      ]);

      const afterAppend = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 5,
      });

      expect(afterAppend.rows).toStrictEqual([
        { id: "f", price: 12 },
        { id: "d", price: 10 },
        { id: "i", price: 9 },
        { id: "b", price: 8 },
        { id: "bb", price: 8 },
      ]);
      expect(afterAppend.totalRows).toBe(10);

      yield* engine.delete("orders", "d");

      const afterDelete = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 4,
      });

      expect(afterDelete.rows).toStrictEqual([
        { id: "f", price: 12 },
        { id: "i", price: 9 },
        { id: "b", price: 8 },
        { id: "bb", price: 8 },
      ]);
      expect(afterDelete.totalRows).toBe(9);
    }),
  );

  it.effect("uses ordered range seeks for raw snapshots", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      yield* engine.publishMany("orders", [
        order("a", "open", 1, 1),
        order("b", "open", 2, 2),
        order("c", "open", 3, 3),
        order("d", "open", 4, 4),
        order("e", "open", 5, 5),
        order("f", "open", 6, 6),
        order("g", "open", 7, 7),
        order("h", "open", 8, 8),
        order("z", "closed", 99, 9),
      ]);

      const ascendingInclusive = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gte: 3 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        offset: 1,
        limit: 2,
      });

      expect(ascendingInclusive.rows).toStrictEqual([
        { id: "d", price: 4 },
        { id: "e", price: 5 },
      ]);
      expect(ascendingInclusive.totalRows).toBe(7);

      const scalarFilteredOrderedWindow = yield* engine.snapshot("orders", {
        select: ["id", "price", "status"],
        where: {
          status: "closed",
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 1,
      });

      expect(scalarFilteredOrderedWindow.rows).toStrictEqual([
        { id: "z", price: 99, status: "closed" },
      ]);
      expect(scalarFilteredOrderedWindow.totalRows).toBe(1);

      const scalarFilteredEmptyOrderedWindow = yield* engine.snapshot("orders", {
        select: ["id", "customerId", "price"],
        where: {
          customerId: "missing-customer",
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 1,
      });

      expect(scalarFilteredEmptyOrderedWindow.rows).toStrictEqual([]);
      expect(scalarFilteredEmptyOrderedWindow.totalRows).toBe(0);

      const numericEquality = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { eq: 5 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(numericEquality.rows).toStrictEqual([{ id: "e", price: 5 }]);
      expect(numericEquality.totalRows).toBe(1);

      const numericInAscending = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { in: [8, 2, 99, 2] },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(numericInAscending.rows).toStrictEqual([
        { id: "b", price: 2 },
        { id: "h", price: 8 },
        { id: "z", price: 99 },
      ]);
      expect(numericInAscending.totalRows).toBe(3);

      const numericInDescending = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { in: [8, 2, 99, 2] },
        },
        orderBy: [{ field: "price", direction: "desc" }],
        offset: 1,
        limit: 1,
      });

      expect(numericInDescending.rows).toStrictEqual([{ id: "h", price: 8 }]);
      expect(numericInDescending.totalRows).toBe(3);

      const emptyIn = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { in: [] },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(emptyIn.rows).toStrictEqual([]);
      expect(emptyIn.totalRows).toBe(0);

      const stringEquality = yield* engine.snapshot("orders", {
        select: ["id", "status"],
        where: {
          status: { eq: "closed" },
        },
        orderBy: [{ field: "status", direction: "asc" }],
        limit: 10,
      });

      expect(stringEquality.rows).toStrictEqual([{ id: "z", status: "closed" }]);
      expect(stringEquality.totalRows).toBe(1);

      const stringIn = yield* engine.snapshot("orders", {
        select: ["id", "customerId"],
        where: {
          customerId: { in: ["customer-h", "customer-b", "customer-z", "customer-b"] },
        },
        orderBy: [{ field: "customerId", direction: "asc" }],
        limit: 10,
      });

      expect(stringIn.rows).toStrictEqual([
        { id: "b", customerId: "customer-b" },
        { id: "h", customerId: "customer-h" },
        { id: "z", customerId: "customer-z" },
      ]);
      expect(stringIn.totalRows).toBe(3);

      const numericInWithRange = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { in: [2, 5, 8, 99, 2], gte: 5, lt: 99 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        offset: 1,
        limit: 1,
      });

      expect(numericInWithRange.rows).toStrictEqual([{ id: "h", price: 8 }]);
      expect(numericInWithRange.totalRows).toBe(2);

      const equalityInIntersection = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { eq: 5, in: [2, 5, 8] },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(equalityInIntersection.rows).toStrictEqual([{ id: "e", price: 5 }]);
      expect(equalityInIntersection.totalRows).toBe(1);

      const contradictoryEqualityInIntersection = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { eq: 5, in: [2, 8] },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(contradictoryEqualityInIntersection.rows).toStrictEqual([]);
      expect(contradictoryEqualityInIntersection.totalRows).toBe(0);

      const equalityOutsideRange = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { eq: 5, gt: 5 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(equalityOutsideRange.rows).toStrictEqual([]);
      expect(equalityOutsideRange.totalRows).toBe(0);

      const ascendingExclusive = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gt: 3, lt: 7 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(ascendingExclusive.rows).toStrictEqual([
        { id: "d", price: 4 },
        { id: "e", price: 5 },
        { id: "f", price: 6 },
      ]);
      expect(ascendingExclusive.totalRows).toBe(3);

      const strongerDifferentBounds = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gt: 3, gte: 4, lt: 8, lte: 6 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(strongerDifferentBounds.rows).toStrictEqual([
        { id: "d", price: 4 },
        { id: "e", price: 5 },
        { id: "f", price: 6 },
      ]);
      expect(strongerDifferentBounds.totalRows).toBe(3);

      const strongerEqualBounds = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gte: 3, gt: 3, lte: 6, lt: 6 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(strongerEqualBounds.rows).toStrictEqual([
        { id: "d", price: 4 },
        { id: "e", price: 5 },
      ]);
      expect(strongerEqualBounds.totalRows).toBe(2);

      const descendingInclusive = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { lte: 6 },
        },
        orderBy: [{ field: "price", direction: "desc" }],
        offset: 1,
        limit: 2,
      });

      expect(descendingInclusive.rows).toStrictEqual([
        { id: "e", price: 5 },
        { id: "d", price: 4 },
      ]);
      expect(descendingInclusive.totalRows).toBe(6);

      const descendingExclusive = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { lt: 6 },
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 2,
      });

      expect(descendingExclusive.rows).toStrictEqual([
        { id: "e", price: 5 },
        { id: "d", price: 4 },
      ]);
      expect(descendingExclusive.totalRows).toBe(5);

      const descendingLowerBound = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gt: 3 },
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 2,
      });

      expect(descendingLowerBound.rows).toStrictEqual([
        { id: "z", price: 99 },
        { id: "h", price: 8 },
      ]);
      expect(descendingLowerBound.totalRows).toBe(6);

      const descendingLowerInclusive = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gte: 6 },
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 4,
      });

      expect(descendingLowerInclusive.rows).toStrictEqual([
        { id: "z", price: 99 },
        { id: "h", price: 8 },
        { id: "g", price: 7 },
        { id: "f", price: 6 },
      ]);
      expect(descendingLowerInclusive.totalRows).toBe(4);

      const exactInclusiveRange = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gte: 4, lte: 4 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 2,
      });

      expect(exactInclusiveRange.rows).toStrictEqual([{ id: "d", price: 4 }]);
      expect(exactInclusiveRange.totalRows).toBe(1);

      const impossibleRange = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gt: 4, lte: 4 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 5,
      });

      expect(impossibleRange.rows).toStrictEqual([]);
      expect(impossibleRange.totalRows).toBe(0);

      const invertedRange = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gt: 7, lt: 4 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 5,
      });

      expect(invertedRange.rows).toStrictEqual([]);
      expect(invertedRange.totalRows).toBe(0);

      const nonOrderFieldRange = yield* engine.snapshot("orders", {
        select: ["id", "price", "updatedAt"],
        where: {
          price: { gte: 4 },
        },
        orderBy: [{ field: "updatedAt", direction: "desc" }],
        limit: 2,
      });

      expect(nonOrderFieldRange.rows).toStrictEqual([
        { id: "z", price: 99, updatedAt: 9 },
        { id: "h", price: 8, updatedAt: 8 },
      ]);
      expect(nonOrderFieldRange.totalRows).toBe(6);

      yield* engine.publish("orders", order("hh", "open", 8, 10));

      const duplicateEqualityValue = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { eq: 8 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(duplicateEqualityValue.rows).toStrictEqual([
        { id: "h", price: 8 },
        { id: "hh", price: 8 },
      ]);
      expect(duplicateEqualityValue.totalRows).toBe(2);

      yield* engine.publish("orders", order("i", "open", 9, 11));

      const afterAppend = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gte: 8 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 3,
      });

      expect(afterAppend.rows).toStrictEqual([
        { id: "h", price: 8 },
        { id: "hh", price: 8 },
        { id: "i", price: 9 },
      ]);
      expect(afterAppend.totalRows).toBe(4);
    }),
  );

  it.effect("keeps exact predicate candidate indexes current after row mutations", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("a", "open", 10, 1),
        order("b", "closed", 20, 2),
        order("c", "open", 30, 3),
      ]);

      const initial = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(initial.rows).toStrictEqual([{ id: "a" }, { id: "c" }]);
      expect(initial.totalRows).toBe(2);

      yield* engine.publish("orders", order("a", "closed", 10, 4));
      yield* engine.delete("orders", "b");

      const afterUpdateAndSlotSwapDelete = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(afterUpdateAndSlotSwapDelete.rows).toStrictEqual([{ id: "c" }]);
      expect(afterUpdateAndSlotSwapDelete.totalRows).toBe(1);

      yield* engine.publish("orders", order("d", "open", 40, 5));
      yield* engine.delete("orders", "c");

      const afterInsertAndSecondSlotSwapDelete = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(afterInsertAndSecondSlotSwapDelete.rows).toStrictEqual([{ id: "d" }]);
      expect(afterInsertAndSecondSlotSwapDelete.totalRows).toBe(1);

      yield* engine.reset();

      const afterReset = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(afterReset.rows).toStrictEqual([]);
      expect(afterReset.totalRows).toBe(0);
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

  it.effect("evaluates bounded grouped windows without changing ordered aggregate results", () =>
    Effect.gen(function* () {
      const rows = new Map<string, object>(
        Array.from({ length: 1_100 }, (_value, index) => [
          `row-${index}`,
          position(`row-${index}`, `symbol-${index}`, BigInt(index), "1"),
        ]),
      );
      const compiled = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          groupBy: ["symbol"],
          aggregates: {
            totalQuantity: { aggFunc: "sum", field: "quantity" },
          },
          orderBy: [{ aggregate: "totalQuantity", direction: "desc" }],
          offset: 2,
          limit: 3,
        },
      );
      const evaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            for (const [key, row] of rows) {
              visitor(key, row);
            }
          },
          version: () => 1,
        },
        compiled,
      );

      expect(normalizeDecimalAndBigIntFields(evaluation.rows)).toStrictEqual([
        {
          symbol: "symbol-1097",
          totalQuantity: "1097",
        },
        {
          symbol: "symbol-1096",
          totalQuantity: "1096",
        },
        {
          symbol: "symbol-1095",
          totalQuantity: "1095",
        },
      ]);
      expect(evaluation.keys).toStrictEqual([
        '["array",[["array",[["string","symbol"],["string","symbol-1097"]]]]]',
        '["array",[["array",[["string","symbol"],["string","symbol-1096"]]]]]',
        '["array",[["array",[["string","symbol"],["string","symbol-1095"]]]]]',
      ]);
      expect(evaluation.totalRows).toBe(1_100);
    }),
  );

  it.effect("keeps grouped total rows for zero-limit windows", () =>
    Effect.gen(function* () {
      const rows = new Map<string, object>([
        ["1", position("1", "AAPL", 10n, "1")],
        ["2", position("2", "MSFT", 20n, "1")],
      ]);
      const compiled = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          groupBy: ["symbol"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          where: {
            symbol: "AAPL",
          },
          orderBy: [{ field: "symbol", direction: "asc" }],
          offset: 10_000,
          limit: 0,
        },
      );
      const evaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            for (const [key, row] of rows) {
              visitor(key, row);
            }
          },
          version: () => 1,
        },
        compiled,
      );

      expect(evaluation.rows).toStrictEqual([]);
      expect(evaluation.keys).toStrictEqual([]);
      expect(evaluation.totalRows).toBe(1);
    }),
  );

  it.effect("compares bounded grouped windows by avg, max, and stable group key", () =>
    Effect.gen(function* () {
      const rows = new Map<string, object>([
        ["a-1", position("a-1", "AAPL", 0n, "1")],
        ["a-2", position("a-2", "AAPL", 20n, "1")],
        ["m-1", position("m-1", "MSFT", 5n, "1")],
        ["m-2", position("m-2", "MSFT", 15n, "1")],
        ["z-1", position("z-1", "ZZZZ", 1n, "1")],
      ]);
      const compiled = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          groupBy: ["symbol"],
          aggregates: {
            averageQuantity: { aggFunc: "avg", field: "quantity" },
            maxQuantity: { aggFunc: "max", field: "quantity" },
          },
          orderBy: [
            { aggregate: "averageQuantity", direction: "asc" },
            { aggregate: "maxQuantity", direction: "desc" },
          ],
          limit: 3,
        },
      );
      const evaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            for (const [key, row] of rows) {
              visitor(key, row);
            }
          },
          version: () => 1,
        },
        compiled,
      );

      expect(normalizeDecimalAndBigIntFields(evaluation.rows)).toStrictEqual([
        {
          averageQuantity: "1",
          maxQuantity: "1",
          symbol: "ZZZZ",
        },
        {
          averageQuantity: "10",
          maxQuantity: "20",
          symbol: "AAPL",
        },
        {
          averageQuantity: "10",
          maxQuantity: "15",
          symbol: "MSFT",
        },
      ]);

      const tiedCount = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          groupBy: ["symbol"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          orderBy: [{ aggregate: "rowCount", direction: "desc" }],
          limit: 2,
        },
      );
      const tiedEvaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            visitor("b", position("b", "BBBB", 1n, "1"));
            visitor("a", position("a", "AAAA", 1n, "1"));
          },
          version: () => 1,
        },
        tiedCount,
      );
      expect(tiedEvaluation.rows).toStrictEqual([
        {
          rowCount: 1n,
          symbol: "AAAA",
        },
        {
          rowCount: 1n,
          symbol: "BBBB",
        },
      ]);

      const tiedSingleCount = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          groupBy: ["symbol"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          orderBy: [{ aggregate: "rowCount", direction: "desc" }],
          limit: 1,
        },
      );
      const tiedSingleEvaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            visitor("b", position("b", "BBBB", 1n, "1"));
            visitor("a", position("a", "AAAA", 1n, "1"));
          },
          version: () => 1,
        },
        tiedSingleCount,
      );
      expect(tiedSingleEvaluation.rows).toStrictEqual([
        {
          rowCount: 1n,
          symbol: "AAAA",
        },
      ]);

      const distinctCount = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          groupBy: ["symbol"],
          aggregates: {
            distinctPrices: { aggFunc: "countDistinct", field: "price" },
          },
          orderBy: [{ aggregate: "distinctPrices", direction: "desc" }],
          limit: 1,
        },
      );
      const distinctEvaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            visitor("a-1", position("a-1", "AAPL", 1n, "1"));
            visitor("a-2", position("a-2", "AAPL", 1n, "2"));
            visitor("m-1", position("m-1", "MSFT", 1n, "1"));
          },
          version: () => 1,
        },
        distinctCount,
      );
      expect(distinctEvaluation.rows).toStrictEqual([
        {
          distinctPrices: 2n,
          symbol: "AAPL",
        },
      ]);

      const fieldOrder = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          groupBy: ["symbol"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          orderBy: [{ field: "symbol", direction: "desc" }],
          limit: 1,
        },
      );
      const fieldOrderEvaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            visitor("a", position("a", "AAAA", 1n, "1"));
            visitor("b", position("b", "BBBB", 1n, "1"));
          },
          version: () => 1,
        },
        fieldOrder,
      );
      expect(fieldOrderEvaluation.rows).toStrictEqual([
        {
          rowCount: 1n,
          symbol: "BBBB",
        },
      ]);

      const defaultOrder = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          groupBy: ["symbol"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          limit: 1,
        },
      );
      const defaultOrderEvaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            visitor("b", position("b", "BBBB", 1n, "1"));
            visitor("a", position("a", "AAAA", 1n, "1"));
          },
          version: () => 1,
        },
        defaultOrder,
      );
      expect(defaultOrderEvaluation.rows).toStrictEqual([
        {
          rowCount: 1n,
          symbol: "AAAA",
        },
      ]);

      const PositionForCompiler = Schema.Struct({
        id: Schema.String,
        quantity: Schema.BigInt,
        symbol: Schema.String,
      });
      const zeroAverage = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(PositionForCompiler),
        {
          groupBy: ["symbol"],
          aggregates: {
            averageQuantity: { aggFunc: "avg", field: "quantity" },
          },
          orderBy: [{ aggregate: "averageQuantity", direction: "asc" }],
          limit: 1,
        },
      );
      const zeroAverageEvaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            visitor("bad", { id: "bad", quantity: "not-a-bigint", symbol: "BAD" });
            visitor("good", { id: "good", quantity: 10n, symbol: "GOOD" });
          },
          version: () => 1,
        },
        zeroAverage,
      );
      expect(normalizeDecimalAndBigIntFields(zeroAverageEvaluation.rows)).toStrictEqual([
        {
          averageQuantity: "0",
          symbol: "BAD",
        },
      ]);
    }),
  );

  it.effect("uses full grouped ordering when requested grouped window is too large", () =>
    Effect.gen(function* () {
      const rows = new Map<string, object>(
        Array.from({ length: 1_100 }, (_value, index) => [
          `row-${index}`,
          position(`row-${index}`, `symbol-${index}`, BigInt(index), "1"),
        ]),
      );
      const compiled = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          groupBy: ["symbol"],
          aggregates: {
            totalQuantity: { aggFunc: "sum", field: "quantity" },
          },
          orderBy: [{ aggregate: "totalQuantity", direction: "desc" }],
          limit: 1_025,
        },
      );
      const evaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            for (const [key, row] of rows) {
              visitor(key, row);
            }
          },
          version: () => 1,
        },
        compiled,
      );

      expect(normalizeDecimalAndBigIntFields(evaluation.rows.slice(0, 2))).toStrictEqual([
        {
          symbol: "symbol-1099",
          totalQuantity: "1099",
        },
        {
          symbol: "symbol-1098",
          totalQuantity: "1098",
        },
      ]);
      expect(evaluation.rows.length).toBe(1_025);
      expect(evaluation.totalRows).toBe(1_100);
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
          changesSince: () => [],
          scanRows: (visitor) => {
            for (const [key, row] of rows) {
              visitor(key, row);
            }
          },
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

  it.effect("applies incremental grouped change batches without rescanning rows", () =>
    Effect.gen(function* () {
      let version = 0;
      let scanCount = 0;
      let batches: ReadonlyArray<TopicRowChangeBatch<object>> = [];
      const rows = new Map<string, object>([
        ["1", order("1", "open", 10, 1, "emea")],
        ["2", order("2", "open", 20, 2, "amer")],
      ]);
      const store = {
        changesSince: () => batches,
        scanRows: (visitor: (key: string, row: object) => void) => {
          scanCount += 1;
          for (const [key, row] of rows) {
            visitor(key, row);
          }
        },
        version: () => version,
      };
      const compiled = yield* prepareGroupedQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          where: {
            region: "emea",
          },
          orderBy: [{ field: "status", direction: "asc" }],
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});

      expect(execution.latest().rows).toStrictEqual([{ status: "open", rowCount: 1n }]);
      expect(execution.latest().rows).toStrictEqual([{ status: "open", rowCount: 1n }]);
      expect(scanCount).toBe(1);

      const ignored = order("3", "closed", 30, 3, "amer");
      const inserted = order("4", "closed", 40, 4, "emea");
      rows.set("3", ignored);
      rows.set("4", inserted);
      version = 1;
      batches = [
        {
          version,
          changes: [
            {
              key: "ignored-old",
              previous: order("ignored-old", "open", 5, 5, "amer"),
              next: undefined,
            },
            {
              key: "missing-old-group",
              previous: order("missing-old-group", "cancelled", 5, 5, "emea"),
              next: undefined,
            },
            {
              key: "missing-open-member",
              previous: order("missing-open-member", "open", 5, 5, "emea"),
              next: undefined,
            },
            {
              key: "1",
              previous: undefined,
              next: order("1", "open", 15, 6, "emea"),
            },
            { key: "3", previous: undefined, next: ignored },
            { key: "4", previous: undefined, next: inserted },
          ],
        },
      ];

      expect(execution.latest().rows).toStrictEqual([
        { status: "closed", rowCount: 1n },
        { status: "open", rowCount: 1n },
      ]);
      expect(scanCount).toBe(1);
    }),
  );

  it.effect("tracks incremental zero-limit grouped counts without aggregate windows", () =>
    Effect.gen(function* () {
      let version = 0;
      let scanCount = 0;
      let batches: ReadonlyArray<TopicRowChangeBatch<object>> = [];
      const retainedCustomer = {
        ...order("1-extra", "open", 15, 4, "emea"),
        customerId: "customer-1",
      };
      const rows = new Map<string, object>([
        ["1", order("1", "open", 10, 1, "emea")],
        ["1-extra", retainedCustomer],
        ["2", order("2", "open", 20, 2, "amer")],
      ]);
      const store = {
        changesSince: () => batches,
        scanRows: (visitor: (key: string, row: object) => void) => {
          scanCount += 1;
          for (const [key, row] of rows) {
            visitor(key, row);
          }
        },
        version: () => version,
      };
      const compiled = yield* prepareGroupedQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["customerId"],
          aggregates: {
            totalPrice: { aggFunc: "sum", field: "price" },
          },
          where: {
            region: "emea",
          },
          offset: 10_000,
          limit: 0,
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});

      expect(execution.latest().rows).toStrictEqual([]);
      expect(execution.latest().totalRows).toBe(1);
      expect(scanCount).toBe(1);

      rows.delete("1-extra");
      version = 1;
      batches = [
        {
          version,
          changes: [{ key: "1-extra", previous: retainedCustomer, next: undefined }],
        },
      ];

      expect(execution.latest().rows).toStrictEqual([]);
      expect(execution.latest().totalRows).toBe(1);
      expect(scanCount).toBe(1);

      const inserted = order("3", "closed", 30, 3, "emea");
      rows.set("3", inserted);
      version = 2;
      batches = [
        {
          version,
          changes: [{ key: "3", previous: undefined, next: inserted }],
        },
      ];

      expect(execution.latest().rows).toStrictEqual([]);
      expect(execution.latest().totalRows).toBe(2);
      expect(scanCount).toBe(1);

      rows.delete("3");
      version = 3;
      batches = [
        {
          version,
          changes: [{ key: "3", previous: inserted, next: undefined }],
        },
      ];

      expect(execution.latest().rows).toStrictEqual([]);
      expect(execution.latest().totalRows).toBe(1);
      expect(scanCount).toBe(1);

      rows.set("3", inserted);
      version = 4;
      batches = [
        {
          version,
          changes: [
            {
              key: "missing-zero-limit",
              previous: order("missing-zero-limit", "open", 1, 1, "emea"),
              next: undefined,
            },
            {
              key: "ignored-zero-limit",
              previous: undefined,
              next: order("ignored-zero-limit", "open", 1, 1, "amer"),
            },
            {
              key: "ignored-zero-limit-previous",
              previous: order("ignored-zero-limit-previous", "open", 1, 1, "amer"),
              next: undefined,
            },
          ],
        },
      ];

      expect(execution.latest().rows).toStrictEqual([]);
      expect(execution.latest().totalRows).toBe(1);
      expect(scanCount).toBe(1);

      version = 5;
      batches = [
        {
          version,
          changes: [{ key: "3", previous: undefined, next: inserted }],
        },
      ];

      expect(execution.latest().rows).toStrictEqual([]);
      expect(execution.latest().totalRows).toBe(2);
      expect(scanCount).toBe(1);

      const repeatedGroupChanges = Array.from({ length: 4_097 }, (_value, index) => {
        const repeated = {
          ...order(`repeat-${index}`, "open", index, index, "emea"),
          customerId: "customer-3",
        };
        rows.set(`repeat-${index}`, repeated);
        return {
          key: `repeat-${index}`,
          previous: undefined,
          next: repeated,
        };
      });
      version = 6;
      batches = [
        {
          version,
          changes: repeatedGroupChanges,
        },
      ];

      expect(execution.latest().rows).toStrictEqual([]);
      expect(execution.latest().totalRows).toBe(2);
      expect(execution.incremental).toBe(true);
      expect(scanCount).toBe(1);

      const overflowChanges = Array.from({ length: 8_193 }, (_value, index) => {
        const overflow = order(`overflow-${index}`, "open", index, index, "emea");
        rows.set(`overflow-${index}`, overflow);
        return {
          key: `overflow-${index}`,
          previous: undefined,
          next: overflow,
        };
      });
      version = 7;
      batches = [
        {
          version,
          changes: overflowChanges,
        },
      ];

      expect(execution.latest().rows).toStrictEqual([]);
      expect(execution.latest().totalRows).toBe(8_195);
      expect(execution.incremental).toBe(false);
      expect(scanCount).toBe(2);
    }),
  );

  it.effect("falls back when materialized grouped admission is ignored by the row scanner", () =>
    Effect.gen(function* () {
      const rows = new Map<string, object>(
        Array.from({ length: 4_098 }, (_value, index) => [
          `row-${index}`,
          order(`row-${index}`, "open", index, index),
        ]),
      );
      const store = {
        changesSince: () => [],
        scanRows: (visitor: (key: string, row: object) => false | void) => {
          for (const [key, row] of rows) {
            visitor(key, row);
          }
        },
        version: () => 0,
      };
      const compiled = yield* prepareGroupedQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});

      expect(execution.incremental).toBe(false);
      expect(execution.latest().rows).toStrictEqual([{ status: "open", rowCount: 4_098n }]);
    }),
  );

  it.effect("falls back when count-only grouped admission is ignored by the row scanner", () =>
    Effect.gen(function* () {
      const rows = new Map<string, object>([
        ["ignored", order("ignored", "open", 1, 1, "amer")],
        ...Array.from({ length: 8_194 }, (_value, index): [string, object] => {
          const key = `row-${index}`;
          return [key, order(key, "open", index, index, "emea")];
        }),
      ]);
      const store = {
        changesSince: () => [],
        scanRows: (visitor: (key: string, row: object) => false | void) => {
          for (const [key, row] of rows) {
            visitor(key, row);
          }
        },
        version: () => 0,
      };
      const compiled = yield* prepareGroupedQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["customerId"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          where: {
            region: "emea",
          },
          limit: 0,
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});

      expect(execution.incremental).toBe(false);
      expect(execution.latest().rows).toStrictEqual([]);
      expect(execution.latest().totalRows).toBe(8_194);
    }),
  );

  it.effect("uses fallback grouped execution when incremental admission is too broad", () =>
    Effect.gen(function* () {
      let version = 0;
      const scanCounts: Array<number> = [];
      const rows = new Map<string, object>(
        Array.from({ length: 65_537 }, (_value, index) => [
          `row-${index}`,
          order(`row-${index}`, "open", index, index),
        ]),
      );
      const store = {
        changesSince: () => [],
        scanRows: (visitor: (key: string, row: object) => false | void) => {
          let scanCount = 0;
          for (const [key, row] of rows) {
            scanCount += 1;
            if (visitor(key, row) === false) {
              break;
            }
          }
          scanCounts.push(scanCount);
        },
        version: () => version,
      };
      const compiled = yield* prepareGroupedQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});
      expect(execution.incremental).toBe(false);
      expect(execution.latest().rows).toStrictEqual([{ status: "open", rowCount: 65_537n }]);
      expect(scanCounts).toStrictEqual([4_097, 65_537]);

      rows.set("closed", order("closed", "closed", 1, 65_538));
      version = 1;
      expect(execution.latest().rows).toStrictEqual([
        { status: "closed", rowCount: 1n },
        { status: "open", rowCount: 65_537n },
      ]);
    }),
  );

  it.effect(
    "switches grouped execution to fallback when an incremental batch exceeds admission",
    () =>
      Effect.gen(function* () {
        let version = 0;
        let batches: ReadonlyArray<TopicRowChangeBatch<object>> = [];
        const rows = new Map<string, object>();
        const store = {
          changesSince: () => batches,
          scanRows: (visitor: (key: string, row: object) => void) => {
            for (const [key, row] of rows) {
              visitor(key, row);
            }
          },
          version: () => version,
        };
        const compiled = yield* prepareGroupedQuery<object, object>(
          "orders",
          rawQueryCompilerMetadata(Order),
          {
            groupBy: ["status"],
            aggregates: {
              rowCount: { aggFunc: "count" },
            },
          },
        );
        const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});
        expect(execution.incremental).toBe(true);

        const changes = Array.from({ length: 65_537 }, (_value, index) => {
          const row = order(`row-${index}`, "open", index, index);
          rows.set(row.id, row);
          return {
            key: row.id,
            previous: undefined,
            next: row,
          };
        });
        version = 1;
        batches = [{ changes, version }];

        expect(execution.latest().rows).toStrictEqual([{ status: "open", rowCount: 65_537n }]);
        expect(execution.incremental).toBe(false);
        rows.set("closed", order("closed", "closed", 1, 65_538));
        version = 2;
        batches = [];
        expect(execution.latest().rows).toStrictEqual([
          { status: "closed", rowCount: 1n },
          { status: "open", rowCount: 65_537n },
        ]);
      }),
  );

  it.effect(
    "switches grouped execution to fallback when incremental batches exceed total admission",
    () =>
      Effect.gen(function* () {
        let version = 0;
        let batches: ReadonlyArray<TopicRowChangeBatch<object>> = [];
        const rows = new Map<string, object>();
        const store = {
          changesSince: () => batches,
          scanRows: (visitor: (key: string, row: object) => false | void) => {
            for (const [key, row] of rows) {
              if (visitor(key, row) === false) {
                break;
              }
            }
          },
          version: () => version,
        };
        const compiled = yield* prepareGroupedQuery<object, object>(
          "positions",
          rawQueryCompilerMetadata(Position),
          {
            groupBy: ["symbol"],
            aggregates: {
              rowCount: { aggFunc: "count" },
            },
          },
        );
        const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});
        expect(execution.incremental).toBe(true);

        const changes = Array.from({ length: 65_537 }, (_value, index) => {
          const row = position(`row-${index}`, `symbol-${index % 8_192}`, 1n, "1");
          rows.set(row.id, row);
          return {
            key: row.id,
            previous: undefined,
            next: row,
          };
        });
        version = 1;
        batches = [{ changes, version }];

        expect(execution.latest().totalRows).toBe(8_192);
        expect(execution.incremental).toBe(false);
      }),
  );

  it.effect(
    "switches grouped execution to fallback when incremental batches exceed group admission",
    () =>
      Effect.gen(function* () {
        let version = 0;
        let batches: ReadonlyArray<TopicRowChangeBatch<object>> = [];
        const rows = new Map<string, object>();
        const store = {
          changesSince: () => batches,
          scanRows: (visitor: (key: string, row: object) => false | void) => {
            for (const [key, row] of rows) {
              if (visitor(key, row) === false) {
                break;
              }
            }
          },
          version: () => version,
        };
        const compiled = yield* prepareGroupedQuery<object, object>(
          "positions",
          rawQueryCompilerMetadata(Position),
          {
            groupBy: ["symbol"],
            aggregates: {
              rowCount: { aggFunc: "count" },
            },
          },
        );
        const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});
        expect(execution.incremental).toBe(true);

        const changes = Array.from({ length: 8_193 }, (_value, index) => {
          const row = position(`group-row-${index}`, `group-symbol-${index}`, 1n, "1");
          rows.set(row.id, row);
          return {
            key: row.id,
            previous: undefined,
            next: row,
          };
        });
        version = 1;
        batches = [{ changes, version }];

        expect(execution.latest().totalRows).toBe(8_193);
        expect(execution.incremental).toBe(false);
      }),
  );

  it.effect("falls back to a grouped rebuild when the row-change journal is unavailable", () =>
    Effect.gen(function* () {
      let version = 0;
      let scanCount = 0;
      const rows = new Map<string, object>([["1", order("1", "open", 10, 1)]]);
      const store = {
        changesSince: () => undefined,
        scanRows: (visitor: (key: string, row: object) => void) => {
          scanCount += 1;
          for (const [key, row] of rows) {
            visitor(key, row);
          }
        },
        version: () => version,
      };
      const compiled = yield* prepareGroupedQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          orderBy: [{ field: "status", direction: "asc" }],
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});

      expect(execution.latest().rows).toStrictEqual([{ status: "open", rowCount: 1n }]);
      rows.set("2", order("2", "closed", 20, 2));
      version = 1;

      expect(execution.latest().rows).toStrictEqual([
        { status: "closed", rowCount: 1n },
        { status: "open", rowCount: 1n },
      ]);
      expect(scanCount).toBe(2);
    }),
  );

  it.effect("uses fallback when a grouped rebuild after a missed journal exceeds admission", () =>
    Effect.gen(function* () {
      let version = 0;
      const rows = new Map<string, object>([["initial", order("initial", "open", 1, 1)]]);
      const store = {
        changesSince: () => undefined,
        scanRows: (visitor: (key: string, row: object) => void) => {
          for (const [key, row] of rows) {
            visitor(key, row);
          }
        },
        version: () => version,
      };
      const compiled = yield* prepareGroupedQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
        },
      );
      const execution = makeIncrementalGroupedQueryExecution(store, compiled, () => {});
      expect(execution.latest().rows).toStrictEqual([{ status: "open", rowCount: 1n }]);

      for (let index = 0; index < 65_537; index += 1) {
        const row = order(`wide-${index}`, "closed", index, index);
        rows.set(row.id, row);
      }
      version = 1;

      expect(execution.latest().rows).toStrictEqual([
        { status: "closed", rowCount: 65_537n },
        { status: "open", rowCount: 1n },
      ]);
      rows.set("cancelled", order("cancelled", "cancelled", 1, 65_538));
      version = 2;
      expect(execution.latest().rows).toStrictEqual([
        { status: "cancelled", rowCount: 1n },
        { status: "closed", rowCount: 65_537n },
        { status: "open", rowCount: 1n },
      ]);
    }),
  );

  it.effect("rebuilds a real grouped execution after its row-change journal window is missed", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      yield* publishTopicStoreRow(store, order("initial", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      const readModel = topicStoreReadModel(store);
      const compiled = yield* prepareGroupedQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          orderBy: [{ field: "status", direction: "asc" }],
        },
      );
      const execution = yield* acquireMaterializedQueryExecution(
        readModel,
        "missed-real-journal",
        () => makeIncrementalGroupedQueryExecution(readModel, compiled, () => {}),
      );
      const cursor = execution.createCursor();

      for (let index = 0; index < 1_025; index += 1) {
        yield* publishTopicStoreRow(
          store,
          order(`late-${index}`, "closed", index, index),
          (topic, message) => InvalidRowError.make({ topic, message }),
        );
      }

      const next = yield* execution.next("missed-real-journal-query", cursor);
      expect(Option.isSome(next)).toBe(true);
      expect(expectDefined(Option.getOrUndefined(next)).totalRows).toBe(2);

      yield* releaseMaterializedQueryExecution(readModel, "missed-real-journal");
    }),
  );

  it.effect("evaluates raw queries through the storage scan interface", () =>
    Effect.gen(function* () {
      const rows = [
        { key: "closed", row: order("closed", "closed", 1, 1) },
        { key: "open-z", row: order("open-z", "open", 20, 3) },
        { key: "open-a", row: order("open-a", "open", 20, 4) },
        { key: "open-low", row: order("open-low", "open", 10, 2) },
      ];
      const compiled = yield* prepareRawQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          select: ["id", "price"],
          where: {
            status: "open",
          },
          orderBy: [
            {
              field: "price",
              direction: "desc",
            },
          ],
        },
      );
      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [
                {
                  field: "status",
                  operator: "eq",
                  value: "open",
                },
              ],
              callbackRequired: false,
              callbackSkippable: true,
            });
            expect(plan.orderBy).toStrictEqual([
              {
                field: "price",
                direction: "desc",
              },
            ]);
            const filtered = rows.filter((entry) => plan.matches(entry.row));
            const ordered = filtered.toSorted(plan.compare);
            const window = ordered.slice(
              plan.offset,
              plan.limit === undefined ? undefined : plan.offset + plan.limit,
            );
            return {
              keys: window.map((entry) => entry.key),
              window,
              totalRows: filtered.length,
            };
          },
          version: () => 7,
        },
        compiled,
      );

      expect(evaluation.keys).toStrictEqual(["open-a", "open-z", "open-low"]);
      expect(evaluation.rows).toStrictEqual([
        {
          id: "open-a",
          price: 20,
        },
        {
          id: "open-z",
          price: 20,
        },
        {
          id: "open-low",
          price: 10,
        },
      ]);
      expect(evaluation.totalRows).toBe(3);
      expect(evaluation.version).toBe(7);
    }),
  );

  it.effect("passes scalar ordering semantics to custom storage scanners", () =>
    Effect.gen(function* () {
      const active = { key: "active", row: position("active", "AAPL", 1n, "1", true) };
      const activeTie = { key: "active-tie", row: position("active-tie", "AAPL", 1n, "1", true) };
      const inactive = { key: "inactive", row: position("inactive", "AAPL", 1n, "1", false) };
      const orderRows = [
        { key: "closed", row: order("closed", "closed", 1, 1) },
        { key: "open-high", row: order("open-high", "open", 20, 2) },
        { key: "open-low", row: order("open-low", "open", 10, 3) },
      ];
      const booleanCompiled = yield* prepareRawQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          select: ["id"],
          orderBy: [{ field: "active", direction: "asc" }],
        },
      );
      const orderCompiled = yield* prepareRawQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          select: ["id"],
          where: {
            status: "open",
          },
          orderBy: [{ field: "price", direction: "asc" }],
        },
      );

      const booleanEvaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.compare(active, inactive)).toBe(1);
            expect(plan.compare(inactive, active)).toBe(-1);
            expect(plan.compare(active, activeTie)).toBe(-1);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 1,
        },
        booleanCompiled,
      );
      const orderEvaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            const filtered = orderRows.filter((entry) => plan.matches(entry.row));
            const ordered = filtered.toSorted(plan.compare);
            return {
              keys: ordered.map((entry) => entry.key),
              window: ordered,
              totalRows: filtered.length,
            };
          },
          version: () => 2,
        },
        orderCompiled,
      );

      expect(booleanEvaluation.totalRows).toBe(0);
      expect(orderEvaluation.keys).toStrictEqual(["open-low", "open-high"]);
      expect(orderEvaluation.version).toBe(2);
    }),
  );

  it.effect("passes typed scalar predicate plans to the storage scan interface", () =>
    Effect.gen(function* () {
      const compiled = yield* prepareRawQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          select: ["id"],
          where: {
            status: {
              neq: "cancelled",
              in: ["open", "closed"],
            },
            price: {
              neq: 50,
              gt: 1,
              gte: 2,
              lt: 100,
              lte: 99,
            },
            customerId: {
              startsWith: "customer-",
            },
            region: "emea",
          },
          orderBy: [
            {
              field: "region",
              direction: "asc",
            },
            {
              field: "price",
              direction: "desc",
            },
          ],
        },
      );
      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [
                {
                  field: "status",
                  operator: "neq",
                  value: "cancelled",
                },
                {
                  field: "status",
                  operator: "in",
                  values: ["open", "closed"],
                  valueKeys: new Set(["string:4:open", "string:6:closed"]),
                },
                {
                  field: "price",
                  operator: "neq",
                  value: 50,
                },
                {
                  field: "price",
                  operator: "gt",
                  value: 1,
                },
                {
                  field: "price",
                  operator: "gte",
                  value: 2,
                },
                {
                  field: "price",
                  operator: "lt",
                  value: 100,
                },
                {
                  field: "price",
                  operator: "lte",
                  value: 99,
                },
                {
                  field: "customerId",
                  operator: "startsWith",
                  value: "customer-",
                },
                {
                  field: "region",
                  operator: "eq",
                  value: "emea",
                },
              ],
              callbackRequired: false,
              callbackSkippable: true,
            });
            expect(plan.orderBy).toStrictEqual([
              {
                field: "region",
                direction: "asc",
              },
              {
                field: "price",
                direction: "desc",
              },
            ]);
            expect(plan.matches(order("open-low", "open", 10, 2))).toBe(true);
            expect(plan.matches(order("cancelled", "cancelled", 10, 2))).toBe(false);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 11,
        },
        compiled,
      );

      expect(evaluation.keys).toStrictEqual([]);
      expect(evaluation.rows).toStrictEqual([]);
      expect(evaluation.totalRows).toBe(0);
      expect(evaluation.version).toBe(11);
    }),
  );

  it.effect("passes indexed scalar in predicate keys to the storage scan interface", () =>
    Effect.gen(function* () {
      const matchedPrice = fromStringUnsafe("1.0");
      const positionCompiled = yield* prepareRawQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          select: ["id"],
          where: {
            active: {
              in: [true],
            },
            quantity: {
              in: [20n],
            },
            price: {
              in: [matchedPrice],
            },
          },
        },
      );

      const positionEvaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [
                {
                  field: "active",
                  operator: "in",
                  values: [true],
                  valueKeys: new Set(["boolean:true"]),
                },
                {
                  field: "quantity",
                  operator: "in",
                  values: [20n],
                  valueKeys: new Set(["bigint:20"]),
                },
                {
                  field: "price",
                  operator: "in",
                  values: [matchedPrice],
                  valueKeys: new Set(["bigDecimal:1"]),
                },
              ],
              callbackRequired: false,
              callbackSkippable: true,
            });
            expect(plan.matches(position("matched", "AAPL", 20n, "1", true))).toBe(true);
            expect(plan.matches(position("wrong-active", "AAPL", 20n, "1", false))).toBe(false);
            expect(plan.matches(position("wrong-quantity", "AAPL", 21n, "1", true))).toBe(false);
            expect(plan.matches(position("wrong-price", "AAPL", 20n, "2", true))).toBe(false);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 3,
        },
        positionCompiled,
      );

      expect(positionEvaluation.keys).toStrictEqual([]);
      expect(positionEvaluation.rows).toStrictEqual([]);
      expect(positionEvaluation.totalRows).toBe(0);
      expect(positionEvaluation.version).toBe(3);

      const NullableMetric = Schema.Struct({
        id: Schema.String,
        note: Schema.NullOr(Schema.String),
      });
      const nullableCompiled = yield* prepareRawQuery<object, object>(
        "nullableMetrics",
        rawQueryCompilerMetadata(NullableMetric),
        {
          select: ["id"],
          where: {
            note: {
              in: [null, "x"],
            },
          },
        },
      );

      const nullableEvaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [
                {
                  field: "note",
                  operator: "in",
                  values: [null, "x"],
                  valueKeys: new Set(["null", "string:1:x"]),
                },
              ],
              callbackRequired: false,
              callbackSkippable: true,
            });
            expect(plan.matches({ id: "null", note: null })).toBe(true);
            expect(plan.matches({ id: "x", note: "x" })).toBe(true);
            expect(plan.matches({ id: "y", note: "y" })).toBe(false);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 4,
        },
        nullableCompiled,
      );

      expect(nullableEvaluation.keys).toStrictEqual([]);
      expect(nullableEvaluation.rows).toStrictEqual([]);
      expect(nullableEvaluation.totalRows).toBe(0);
      expect(nullableEvaluation.version).toBe(4);
    }),
  );

  it.effect("passes typed bigint and bigdecimal range plans to the storage scan interface", () =>
    Effect.gen(function* () {
      const excludedPrice = fromStringUnsafe("0");
      const maxPrice = fromStringUnsafe("100");
      const compiled = yield* prepareRawQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          select: ["id"],
          where: {
            quantity: {
              gte: 10n,
            },
            price: {
              neq: excludedPrice,
              lt: maxPrice,
            },
          },
        },
      );
      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [
                {
                  field: "quantity",
                  operator: "gte",
                  value: 10n,
                },
                {
                  field: "price",
                  operator: "neq",
                  value: excludedPrice,
                },
                {
                  field: "price",
                  operator: "lt",
                  value: maxPrice,
                },
              ],
              callbackRequired: false,
              callbackSkippable: true,
            });
            expect(plan.matches(position("aapl", "AAPL", 20n, "10"))).toBe(true);
            expect(plan.matches(position("goog", "GOOG", 1n, "10"))).toBe(false);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 12,
        },
        compiled,
      );

      expect(evaluation.keys).toStrictEqual([]);
      expect(evaluation.rows).toStrictEqual([]);
      expect(evaluation.totalRows).toBe(0);
      expect(evaluation.version).toBe(12);
    }),
  );

  it.effect("passes typed numeric literal range plans to the storage scan interface", () =>
    Effect.gen(function* () {
      const LiteralMetrics = Schema.Struct({
        id: Schema.String,
        score: Schema.Literal(1),
        bucket: Schema.Literal(1n),
      });
      const compiled = yield* prepareRawQuery<object, object>(
        "literalMetrics",
        rawQueryCompilerMetadata(LiteralMetrics),
        {
          select: ["id"],
          where: {
            score: {
              gte: 1,
            },
            bucket: {
              lte: 1n,
            },
          },
        },
      );
      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [
                {
                  field: "score",
                  operator: "gte",
                  value: 1,
                },
                {
                  field: "bucket",
                  operator: "lte",
                  value: 1n,
                },
              ],
              callbackRequired: false,
              callbackSkippable: true,
            });
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 18,
        },
        compiled,
      );

      expect(evaluation.keys).toStrictEqual([]);
      expect(evaluation.rows).toStrictEqual([]);
      expect(evaluation.totalRows).toBe(0);
      expect(evaluation.version).toBe(18);
    }),
  );

  it.effect("keeps malformed scalar operators callback-only in the storage scan plan", () =>
    Effect.gen(function* () {
      const compiled = yield* prepareRawQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          select: ["id"],
          where: {
            status: {
              eq: undefined,
              in: [undefined],
            },
            price: {
              gt: undefined,
              gte: "9",
              lt: Number.NaN,
              lte: fromStringUnsafe("50"),
            },
            customerId: {
              startsWith: 1,
            },
            note: undefined,
          },
        },
      );
      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [],
              callbackRequired: true,
              callbackSkippable: false,
            });
            expect(plan.matches(order("open", "open", 10, 1))).toBe(false);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 14,
        },
        compiled,
      );

      expect(evaluation.keys).toStrictEqual([]);
      expect(evaluation.rows).toStrictEqual([]);
      expect(evaluation.totalRows).toBe(0);
      expect(evaluation.version).toBe(14);

      const structuredScalarCompiled = yield* prepareRawQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          select: ["id"],
          where: {
            status: ["open"],
            customerId: {
              eq: ["customer-open"],
            },
            region: {
              in: [["emea"]],
            },
            price: Number.NaN,
          },
        },
      );
      const structuredScalarEvaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [],
              callbackRequired: true,
              callbackSkippable: false,
            });
            expect(plan.matches(order("open", "open", 10, 1))).toBe(false);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 17,
        },
        structuredScalarCompiled,
      );

      expect(structuredScalarEvaluation.keys).toStrictEqual([]);
      expect(structuredScalarEvaluation.rows).toStrictEqual([]);
      expect(structuredScalarEvaluation.totalRows).toBe(0);
      expect(structuredScalarEvaluation.version).toBe(17);

      const bigintCompiled = yield* prepareRawQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          select: ["id"],
          where: {
            quantity: {
              neq: 1,
            },
          },
        },
      );
      const bigintEvaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [],
              callbackRequired: true,
              callbackSkippable: false,
            });
            expect(plan.matches(position("bad", "BAD", 10n, "10"))).toBe(false);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 15,
        },
        bigintCompiled,
      );

      expect(bigintEvaluation.keys).toStrictEqual([]);
      expect(bigintEvaluation.rows).toStrictEqual([]);
      expect(bigintEvaluation.totalRows).toBe(0);
      expect(bigintEvaluation.version).toBe(15);

      const bigDecimalCompiled = yield* prepareRawQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          select: ["id"],
          where: {
            price: {
              lt: 100,
            },
          },
        },
      );
      const bigDecimalEvaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [],
              callbackRequired: true,
              callbackSkippable: false,
            });
            expect(plan.matches(position("bad-price", "BAD", 10n, "10"))).toBe(false);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 16,
        },
        bigDecimalCompiled,
      );

      expect(bigDecimalEvaluation.keys).toStrictEqual([]);
      expect(bigDecimalEvaluation.rows).toStrictEqual([]);
      expect(bigDecimalEvaluation.totalRows).toBe(0);
      expect(bigDecimalEvaluation.version).toBe(16);

      const booleanCompiled = yield* prepareRawQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          select: ["id"],
          where: {
            active: {
              neq: true,
            },
          },
        },
      );
      const booleanEvaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [],
              callbackRequired: true,
              callbackSkippable: false,
            });
            expect(plan.matches(position("active", "ACT", 10n, "10", true))).toBe(false);
            expect(plan.matches(position("inactive", "INA", 10n, "10", false))).toBe(true);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 19,
        },
        booleanCompiled,
      );

      expect(booleanEvaluation.keys).toStrictEqual([]);
      expect(booleanEvaluation.rows).toStrictEqual([]);
      expect(booleanEvaluation.totalRows).toBe(0);
      expect(booleanEvaluation.version).toBe(19);

      const MixedNumeric = Schema.Struct({
        id: Schema.String,
        amount: Schema.Union([Schema.Number, Schema.BigInt, Schema.BigDecimal]),
      });
      const mixedNumericCompiled = yield* prepareRawQuery<object, object>(
        "mixedNumeric",
        rawQueryCompilerMetadata(MixedNumeric),
        {
          select: ["id"],
          where: {
            amount: {
              gt: 1,
            },
          },
        },
      );
      const mixedNumericEvaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [],
              callbackRequired: true,
              callbackSkippable: false,
            });
            expect(plan.matches({ id: "number", amount: 2 })).toBe(true);
            expect(plan.matches({ id: "bigint", amount: 2n })).toBe(false);
            expect(plan.matches({ id: "decimal", amount: fromStringUnsafe("2") })).toBe(false);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 20,
        },
        mixedNumericCompiled,
      );

      expect(mixedNumericEvaluation.keys).toStrictEqual([]);
      expect(mixedNumericEvaluation.rows).toStrictEqual([]);
      expect(mixedNumericEvaluation.totalRows).toBe(0);
      expect(mixedNumericEvaluation.version).toBe(20);
    }),
  );

  it.effect("keeps structured object predicates callback-only in the storage scan plan", () =>
    Effect.gen(function* () {
      const compiled = yield* prepareRawQuery<object, object>(
        "instruments",
        rawQueryCompilerMetadata(Instrument),
        {
          select: ["id"],
          where: {
            operatorLike: {
              eq: "xnys",
            },
            operatorRangeLike: {
              gte: 2,
            },
            tags: ["equity"],
          },
        },
      );
      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [],
              callbackRequired: true,
              callbackSkippable: false,
            });
            expect(plan.matches(instrument("1", "xnys", 1, ["equity"]))).toBe(false);
            expect(plan.matches(instrument("2", "xlon", 2, ["equity"]))).toBe(false);
            const directMatch = instrument("3", "xnys", 2, ["equity"]);
            expect(plan.matches(directMatch)).toBe(true);
            return {
              keys: ["3"],
              window: [
                {
                  key: "3",
                  row: directMatch,
                },
              ],
              totalRows: 1,
            };
          },
          version: () => 13,
        },
        compiled,
      );

      expect(evaluation.keys).toStrictEqual(["3"]);
      expect(evaluation.rows).toStrictEqual([
        {
          id: "3",
        },
      ]);
      expect(evaluation.totalRows).toBe(1);
      expect(evaluation.version).toBe(13);
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

  it.effect("updates grouped subscriptions incrementally across moves and deletes", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("1", "open", 10, 1, "emea"),
        order("2", "open", 20, 2, "amer"),
        order("3", "closed", 5, 3, "emea"),
      ]);
      const query = {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          distinctRegions: { aggFunc: "countDistinct", field: "region" },
          totalPrice: { aggFunc: "sum", field: "price" },
          averagePrice: { aggFunc: "avg", field: "price" },
          minUpdatedAt: { aggFunc: "min", field: "updatedAt" },
          maxUpdatedAt: { aggFunc: "max", field: "updatedAt" },
        },
        orderBy: [{ field: "status", direction: "asc" }],
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly rowCount: { readonly aggFunc: "count" };
          readonly distinctRegions: {
            readonly aggFunc: "countDistinct";
            readonly field: "region";
          };
          readonly totalPrice: { readonly aggFunc: "sum"; readonly field: "price" };
          readonly averagePrice: { readonly aggFunc: "avg"; readonly field: "price" };
          readonly minUpdatedAt: { readonly aggFunc: "min"; readonly field: "updatedAt" };
          readonly maxUpdatedAt: { readonly aggFunc: "max"; readonly field: "updatedAt" };
        };
        readonly orderBy: readonly [{ readonly field: "status"; readonly direction: "asc" }];
      };

      const subscription = yield* engine.subscribe("orders", query);
      const read = yield* makeEventReader(subscription);
      const snapshot = firstEvent(yield* read(1));
      expectSnapshotEvent(snapshot);
      let state = stateFromSnapshot(snapshot);
      expect(normalizeDecimalFields(state.rows)).toStrictEqual([
        {
          status: "closed",
          rowCount: 1n,
          distinctRegions: 1n,
          totalPrice: "5",
          averagePrice: "5",
          minUpdatedAt: 3,
          maxUpdatedAt: 3,
        },
        {
          status: "open",
          rowCount: 2n,
          distinctRegions: 2n,
          totalPrice: "30",
          averagePrice: "15",
          minUpdatedAt: 1,
          maxUpdatedAt: 2,
        },
      ]);

      yield* engine.patch("orders", "2", {
        status: "closed",
        price: 30,
        region: "emea",
        updatedAt: 4,
      });
      const movedDelta = firstEvent(yield* read(1));
      expectDeltaEvent(movedDelta);
      state = applyDelta(state, movedDelta);
      expect(normalizeDecimalFields(state.rows)).toStrictEqual([
        {
          status: "closed",
          rowCount: 2n,
          distinctRegions: 1n,
          totalPrice: "35",
          averagePrice: "17.5",
          minUpdatedAt: 3,
          maxUpdatedAt: 4,
        },
        {
          status: "open",
          rowCount: 1n,
          distinctRegions: 1n,
          totalPrice: "10",
          averagePrice: "10",
          minUpdatedAt: 1,
          maxUpdatedAt: 1,
        },
      ]);

      yield* engine.delete("orders", "1");
      const deleteDelta = firstEvent(yield* read(1));
      expectDeltaEvent(deleteDelta);
      state = applyDelta(state, deleteDelta);
      expect(normalizeDecimalFields(state.rows)).toStrictEqual([
        {
          status: "closed",
          rowCount: 2n,
          distinctRegions: 1n,
          totalPrice: "35",
          averagePrice: "17.5",
          minUpdatedAt: 3,
          maxUpdatedAt: 4,
        },
      ]);

      yield* subscription.close();
    }),
  );

  it.effect("converges grouped subscriptions for duplicate-key publishMany batches", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("same", "open", 1, 1, "emea"));
      const query = {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          totalPrice: { aggFunc: "sum", field: "price" },
        },
        orderBy: [{ field: "status", direction: "asc" }],
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly rowCount: { readonly aggFunc: "count" };
          readonly totalPrice: { readonly aggFunc: "sum"; readonly field: "price" };
        };
        readonly orderBy: readonly [{ readonly field: "status"; readonly direction: "asc" }];
      };

      const subscription = yield* engine.subscribe("orders", query);
      const read = yield* makeEventReader(subscription);
      let state = stateFromSnapshot(firstEvent(yield* read(1)));

      yield* engine.publishMany("orders", [
        order("same", "closed", 2, 2, "emea"),
        order("same", "open", 3, 3, "emea"),
        order("same", "closed", 4, 4, "emea"),
        order("other", "open", 10, 10, "amer"),
      ]);

      const delta = firstEvent(yield* read(1));
      expectDeltaEvent(delta);
      state = applyDelta(state, delta);
      expect(normalizeDecimalFields(state.rows)).toStrictEqual([
        { status: "closed", rowCount: 1n, totalPrice: "4" },
        { status: "open", rowCount: 1n, totalPrice: "10" },
      ]);

      yield* subscription.close();
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

      const decimalEqualityOrdered = yield* engine.snapshot("positions", {
        select: ["id"],
        where: {
          price: { eq: fromStringUnsafe("1.00") },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      expect(rowIds(decimalEqualityOrdered.rows)).toStrictEqual(["position-3", "position-4"]);
      expect(decimalEqualityOrdered.totalRows).toBe(2);

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

      const bigintEqualityOrdered = yield* engine.snapshot("positions", {
        select: ["id"],
        where: {
          quantity: { eq: 10n },
        },
        orderBy: [{ field: "quantity", direction: "asc" }],
        limit: 10,
      });
      expect(rowIds(bigintEqualityOrdered.rows)).toStrictEqual(["position-1", "position-4"]);
      expect(bigintEqualityOrdered.totalRows).toBe(2);

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

  it.effect("keeps column slot values in sync across replace, delete, reuse, and patch", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("1", "closed", 10, 1),
        order("2", "open", 20, 2),
        order("3", "open", 30, 3),
      ]);

      yield* engine.publish("orders", order("1", "open", 40, 4));

      const afterReplace = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
          price: { gt: 35 },
        },
        orderBy: [{ field: "price", direction: "desc" }],
      });
      expect(afterReplace.rows).toStrictEqual([{ id: "1", price: 40 }]);
      expect(afterReplace.totalRows).toBe(1);

      yield* engine.delete("orders", "1");

      const afterDelete = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
          price: { gt: 35 },
        },
      });
      expect(afterDelete.rows).toStrictEqual([]);
      expect(afterDelete.totalRows).toBe(0);

      const movedRowAfterDelete = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
          price: { lt: 35 },
        },
        orderBy: [{ field: "price", direction: "desc" }],
      });
      expect(movedRowAfterDelete.rows).toStrictEqual([
        { id: "3", price: 30 },
        { id: "2", price: 20 },
      ]);
      expect(movedRowAfterDelete.totalRows).toBe(2);

      yield* engine.publish("orders", order("4", "open", 50, 5));

      const afterSlotReuse = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
          price: { gt: 35 },
        },
        orderBy: [{ field: "price", direction: "desc" }],
      });
      expect(afterSlotReuse.rows).toStrictEqual([{ id: "4", price: 50 }]);
      expect(afterSlotReuse.totalRows).toBe(1);

      yield* engine.patch("orders", "4", { status: "closed", price: 5 });

      const afterPatchOut = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
          price: { gt: 35 },
        },
      });
      expect(afterPatchOut.rows).toStrictEqual([]);
      expect(afterPatchOut.totalRows).toBe(0);

      yield* engine.patch("orders", "4", { status: "open", price: 45 });

      const afterPatchIn = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
          price: { gt: 35 },
        },
      });
      expect(afterPatchIn.rows).toStrictEqual([{ id: "4", price: 45 }]);
      expect(afterPatchIn.totalRows).toBe(1);
    }),
  );

  it.effect("uses bigint column range narrowing for less-than filters", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("positions", [
        position("1", "AAPL", 5n, "10"),
        position("2", "MSFT", 15n, "20"),
      ]);

      const snapshot = yield* engine.snapshot("positions", {
        select: ["id"],
        where: {
          quantity: { lt: 10n },
        },
      });

      expect(snapshot.rows).toStrictEqual([{ id: "1" }]);
      expect(snapshot.totalRows).toBe(1);
    }),
  );

  it.effect("preserves compiled predicate miss behavior for custom storage scanners", () =>
    Effect.gen(function* () {
      const compiled = yield* prepareRawQuery("orders", rawQueryCompilerMetadata(Order), {
        select: ["id"],
        where: {
          status: { eq: "open" },
          customerId: { startsWith: "customer-" },
        },
      });
      const rows = [
        order("1", "closed", 10, 1),
        { ...order("2", "open", 20, 2), customerId: "account-2" },
      ];

      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            const window = rows
              .filter((row) => plan.matches(row))
              .map((row) => ({
                key: row.id,
                row,
              }));
            return {
              keys: window.map((entry) => entry.key),
              window,
              totalRows: window.length,
            };
          },
          version: () => 1,
        },
        compiled,
      );

      expect(evaluation.rows).toStrictEqual([]);
      expect(evaluation.totalRows).toBe(0);
    }),
  );

  it.effect("keeps optional not-equal semantics aligned with public raw snapshots", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("missing-note", "open", 10, 1),
        { ...order("matched-note", "open", 20, 2), note: "hello" },
        { ...order("other-note", "open", 30, 3), note: "bye" },
      ]);

      const snapshot = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          note: { neq: "bye" },
        },
        orderBy: [{ field: "id", direction: "asc" }],
      });

      expect(rowIds(snapshot.rows)).toStrictEqual(["matched-note"]);
      expect(snapshot.totalRows).toBe(1);
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

  it.effect("applies disjoint patches cumulatively against the latest row state", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("a", "open", 10, 1));

      yield* engine.patch("orders", "a", { status: "closed" });
      yield* engine.patch("orders", "a", { price: 99 });

      const fresh = yield* engine.snapshot("orders", { select: orderSelect });
      expect(fresh.version).toBe(3);
      expect(fresh.rows).toStrictEqual([order("a", "closed", 99, 1)]);
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
        acquireSubscriptionHandoff(
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
        acquireSubscriptionHandoff((markAcquired) =>
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

  it.effect("closes acquired subscriptions when topic-store acquisition is interrupted", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
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
        acquireTopicStoreSubscription(store, (_permit, markAcquired) =>
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

  it.effect("honors explicit topic-store subscription handoff options", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      let beforeReturnCount = 0;
      const subscriber: LiveTopicSubscriber = {
        topic: "orders",
        queryId: "query-explicit-handoff-options",
        notify: () => Effect.void,
        queuedEvents: Effect.succeed(0),
        end: Effect.void,
        closeWithStatus: () => Effect.void,
        maxQueueDepth: 0,
        backpressureEvents: 0,
        closed: false,
      };

      yield* acquireTopicStoreSubscription(
        store,
        (permit, markAcquired) =>
          Effect.gen(function* () {
            const subscription = {
              close: () => Effect.void,
            };
            yield* registerTopicStoreSubscription(permit, subscriber);
            yield* markAcquired(subscription);
            return subscription;
          }),
        {
          beforeReturn: Effect.sync(() => {
            beforeReturnCount += 1;
          }),
        },
      );

      const health = yield* collectTopicStoreHealth(store, false);
      expect(beforeReturnCount).toBe(1);
      expect(health.activeSubscriptions).toBe(1);
      yield* closeTopicStoreSubscriptions(store);
    }),
  );

  it.effect("records backpressure close only once for already closed subscribers", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      let finalizeCount = 0;
      const subscriber: LiveTopicSubscriber = {
        topic: "orders",
        queryId: "query-backpressure-idempotent",
        notify: () => Effect.void,
        queuedEvents: Effect.succeed(0),
        end: Effect.void,
        closeWithStatus: () => Effect.void,
        maxQueueDepth: 0,
        backpressureEvents: 0,
        closed: false,
      };
      const finalize = Effect.sync(() => {
        finalizeCount += 1;
      });

      yield* registerTestTopicStoreSubscriber(store, subscriber);
      yield* closeBackpressuredTopicStoreSubscription(store, subscriber, finalize);
      yield* closeBackpressuredTopicStoreSubscription(store, subscriber, finalize);

      const health = yield* collectTopicStoreHealth(store, false);
      expect(finalizeCount).toBe(1);
      expect(subscriber.backpressureEvents).toBe(1);
      expect(health.activeSubscriptions).toBe(0);
      expect(health.backpressureEvents).toBe(1);
    }),
  );

  it.effect("collects topic-store throughput and drains subscribers on normal close", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      let closeStatusCount = 0;
      const subscriber: LiveTopicSubscriber = {
        topic: "orders",
        queryId: "query-normal-close",
        notify: () => Effect.void,
        queuedEvents: Effect.succeed(2),
        end: Effect.void,
        closeWithStatus: () =>
          Effect.sync(() => {
            closeStatusCount += 1;
          }),
        maxQueueDepth: 0,
        backpressureEvents: 0,
        closed: false,
      };

      yield* registerTestTopicStoreSubscriber(store, subscriber);
      yield* publishTopicStoreRow(store, order("1", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      const readyHealth = yield* collectTopicStoreHealth(store, false);
      expect(readyHealth.status).toBe("ready");
      expect(readyHealth.rowCount).toBe(1);
      expect(readyHealth.mutationsPerSecond).toBeGreaterThanOrEqual(0);
      expect(readyHealth.rowsPerSecond).toBeGreaterThanOrEqual(0);
      expect(readyHealth.activeSubscriptions).toBe(1);
      expect(readyHealth.queuedEvents).toBe(2);

      yield* closeTopicStoreSubscriptions(store);

      const closedHealth = yield* collectTopicStoreHealth(store, true);
      expect(closeStatusCount).toBe(1);
      expect(closedHealth.status).toBe("degraded");
      expect(closedHealth.activeSubscriptions).toBe(0);
    }),
  );

  it.effect("acquires topic-store subscriptions through the permit handoff", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      let closed = false;
      const subscription = yield* acquireTopicStoreSubscription(store, (permit, markAcquired) =>
        Effect.gen(function* () {
          expect(permit.store).toBe(store);
          const acquired = {
            close: () =>
              Effect.sync(() => {
                closed = true;
              }),
          };
          yield* markAcquired(acquired);
          return acquired;
        }),
      );

      yield* subscription.close();
      expect(closed).toBe(true);
    }),
  );

  it.effect("exposes bounded row-change batches for active query catch-up", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      const readModel = topicStoreReadModel(store);
      expect(readModel.changesSince(readModel.version())).toStrictEqual([]);
      expect(readModel.changesSince(-1)).toBeUndefined();
      expect(readModel.changesSince(1)).toBeUndefined();

      yield* publishTopicStoreRow(store, order("initial", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      expect(readModel.changesSince(0)).toBeUndefined();

      const compiled = yield* prepareGroupedQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: { rowCount: { aggFunc: "count" } },
        },
      );
      const execution = yield* acquireMaterializedQueryExecution(readModel, "journal-bounds", () =>
        makeIncrementalGroupedQueryExecution(readModel, compiled, () => {}),
      );
      expect(readModel.changesSince(readModel.version())).toStrictEqual([]);

      yield* publishTopicStoreRow(store, order("first-active", "open", 11, 2), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      expect(readModel.changesSince(1)).toStrictEqual([
        {
          version: 2,
          changes: [
            {
              key: "first-active",
              previous: undefined,
              next: order("first-active", "open", 11, 2),
            },
          ],
        },
      ]);

      for (let index = 0; index < 1_025; index += 1) {
        yield* publishTopicStoreRow(
          store,
          order(`journal-${index}`, "open", index, index),
          (topic, message) => InvalidRowError.make({ topic, message }),
        );
      }

      expect(readModel.version()).toBe(1_027);
      expect(readModel.changesSince(0)).toBeUndefined();
      expect(readModel.changesSince(readModel.version())).toStrictEqual([]);
      yield* releaseMaterializedQueryExecution(readModel, "journal-bounds");
      expect(readModel.changesSince(readModel.version() - 1)).toBeUndefined();
      const cursor = execution.createCursor();
      const unchanged = yield* execution.next("released-journal", cursor);
      expect(Option.isNone(unchanged)).toBe(true);
    }),
  );

  it.effect("clears retained row-change journals on active execution clear and overflow", () =>
    Effect.gen(function* () {
      const storage = new ColumnarTopicStore("orders", Order, "id");
      const compiled = yield* prepareGroupedQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: { rowCount: { aggFunc: "count" } },
        },
      );
      yield* acquireMaterializedQueryExecution(storage.readModel, "overflow-journal", () =>
        makeIncrementalGroupedQueryExecution(storage.readModel, compiled, () => {}),
      );
      yield* acquireMaterializedQueryExecution(storage.readModel, "overflow-journal-second", () =>
        makeIncrementalGroupedQueryExecution(storage.readModel, compiled, () => {}),
      );
      yield* releaseMaterializedQueryExecution(storage.readModel, "overflow-journal-second");
      expect(storage.readModel.changesSince(storage.version)).toStrictEqual([]);

      const baseVersion = storage.version;
      storage.setPreparedMany(
        Array.from({ length: 65_537 }, (_value, index) => ({
          key: `row-${index}`,
          row: order(`row-${index}`, "open", index, index),
        })),
      );
      storage.advanceVersion();
      expect(storage.readModel.changesSince(baseVersion)).toBeUndefined();

      const recoveredVersion = storage.version;
      storage.setPrepared({
        key: "after-overflow",
        row: order("after-overflow", "closed", 1, 1),
      });
      storage.advanceVersion();
      expect(storage.readModel.changesSince(recoveredVersion)).toStrictEqual([
        {
          version: recoveredVersion + 1,
          changes: [
            {
              key: "after-overflow",
              previous: undefined,
              next: order("after-overflow", "closed", 1, 1),
            },
          ],
        },
      ]);

      const multiVersionOverflowStart = storage.version;
      for (let batchIndex = 0; batchIndex < 257; batchIndex += 1) {
        storage.setPreparedMany(
          Array.from({ length: 256 }, (_value, rowIndex) => {
            const key = `multi-version-${batchIndex}-${rowIndex}`;
            return {
              key,
              row: order(key, "open", rowIndex, rowIndex),
            };
          }),
        );
        storage.advanceVersion();
      }
      expect(storage.readModel.changesSince(multiVersionOverflowStart)).toBeUndefined();

      yield* clearStoreRawQueryExecutions(storage.readModel);
      expect(yield* activeStoreRawQueryExecutionCount(storage.readModel)).toBe(0);
      storage.setPrepared({
        key: "after-clear",
        row: order("after-clear", "open", 1, 1),
      });
      const afterClearVersion = storage.version;
      storage.advanceVersion();
      expect(storage.readModel.changesSince(afterClearVersion)).toBeUndefined();

      const fallbackStorage = new ColumnarTopicStore("orders", Order, "id");
      fallbackStorage.setPreparedMany(
        Array.from({ length: 65_537 }, (_value, index) => ({
          key: `fallback-${index}`,
          row: order(`fallback-${index}`, "open", index, index),
        })),
      );
      fallbackStorage.advanceVersion();
      yield* acquireMaterializedQueryExecution(fallbackStorage.readModel, "fallback-clear", () =>
        makeIncrementalGroupedQueryExecution(fallbackStorage.readModel, compiled, () => {}),
      );
      yield* clearStoreRawQueryExecutions(fallbackStorage.readModel);
      expect(yield* activeStoreRawQueryExecutionCount(fallbackStorage.readModel)).toBe(0);
    }),
  );

  it.effect("releases retained row-change journals after grouped fallback demotion", () =>
    Effect.gen(function* () {
      const storage = new ColumnarTopicStore("orders", Order, "id");
      const compiled = yield* prepareGroupedQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: { rowCount: { aggFunc: "count" } },
        },
      );
      const execution = yield* acquireMaterializedQueryExecution(
        storage.readModel,
        "demoted-grouped-journal",
        (releaseRetainedChanges) =>
          makeIncrementalGroupedQueryExecution(storage.readModel, compiled, releaseRetainedChanges),
      );
      const cursor = execution.createCursor();

      storage.setPreparedMany(
        Array.from({ length: 65_537 }, (_value, index) => ({
          key: `row-${index}`,
          row: order(`row-${index}`, "open", index, index),
        })),
      );
      storage.advanceVersion();
      yield* execution.next("demoted-grouped-journal", cursor);

      const demotedVersion = storage.version;
      storage.setPrepared({
        key: "after-demotion",
        row: order("after-demotion", "closed", 1, 1),
      });
      storage.advanceVersion();
      expect(storage.readModel.changesSince(demotedVersion)).toBeUndefined();

      yield* releaseMaterializedQueryExecution(storage.readModel, "demoted-grouped-journal");
    }),
  );

  it.effect("drains subscribers and storage on normal topic-store reset", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      let resetStatusCount = 0;
      let resetStatus: StatusEvent | undefined;
      const subscriber: LiveTopicSubscriber = {
        topic: "orders",
        queryId: "query-normal-reset",
        notify: () => Effect.void,
        queuedEvents: Effect.succeed(0),
        end: Effect.void,
        closeWithStatus: (event) =>
          Effect.sync(() => {
            resetStatusCount += 1;
            resetStatus = event;
          }),
        maxQueueDepth: 0,
        backpressureEvents: 0,
        closed: false,
      };

      yield* registerTestTopicStoreSubscriber(store, subscriber);
      yield* publishTopicStoreRow(store, order("1", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      yield* resetTopicStore(store);

      const health = yield* collectTopicStoreHealth(store, false);
      expect(resetStatusCount).toBe(1);
      expect(expectDefined(resetStatus)).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "query-normal-reset",
        status: "closed",
        code: "SubscriptionClosed",
        message: "Subscription closed because the engine reset.",
      });
      expect(health.status).toBe("ready");
      expect(health.rowCount).toBe(0);
      expect(health.activeSubscriptions).toBe(0);
      expect(health.version).toBe(0);
    }),
  );

  it.effect("keeps deleted slots out of scans and handles hostile plans conservatively", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      yield* publishTopicStoreRow(store, order("1", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("2", "open", 20, 2), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("3", "open", 30, 3), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* deleteTopicStoreRow(store, "1");

      const readModel = topicStoreReadModel(store);
      const scannedKeys: Array<string> = [];
      readModel.scanRows((key) => {
        scannedKeys.push(key);
      });
      expect(scannedKeys.toSorted()).toStrictEqual(["2", "3"]);

      const compareByKey = (left: { readonly key: string }, right: { readonly key: string }) =>
        left.key.localeCompare(right.key);
      const compareByKeyDescending = (
        left: { readonly key: string },
        right: { readonly key: string },
      ) => right.key.localeCompare(left.key);
      const matchesOnlySecondRow = (row: object) => fieldValue(row, "id") === "2";
      const missingColumn = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "missing", operator: "eq", value: "anything" }],
          callbackRequired: true,
        },
        orderBy: [],
        matches: matchesOnlySecondRow,
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(missingColumn.keys).toStrictEqual(["2"]);

      const missingOrderColumn = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [{ field: "missing", direction: "asc" }],
        matches: () => true,
        compare: compareByKeyDescending,
        offset: 0,
        limit: undefined,
      });
      expect(missingOrderColumn.keys).toStrictEqual(["3", "2"]);

      const existingOrderColumnCustomCompare = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKeyDescending,
        offset: 0,
        limit: undefined,
      });
      expect(existingOrderColumnCustomCompare.keys).toStrictEqual(["3", "2"]);

      const invalidStorageOrderColumn = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "missing", direction: "asc" }],
        matches: () => true,
        compare: compareByKeyDescending,
        offset: 0,
        limit: undefined,
      });
      expect(invalidStorageOrderColumn.keys).toStrictEqual(["3", "2"]);

      const invalidStorageOrderColumnLimited = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "missing", direction: "asc" }],
        matches: () => true,
        compare: compareByKeyDescending,
        offset: 0,
        limit: 1,
      });
      expect(invalidStorageOrderColumnLimited.keys).toStrictEqual(["3"]);
      expect(invalidStorageOrderColumnLimited.totalRows).toBe(2);

      const multiFieldStorageOrderZeroLimit = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [
          { field: "price", direction: "asc" },
          { field: "updatedAt", direction: "desc" },
        ],
        storageOrderBy: [
          { field: "price", direction: "asc" },
          { field: "updatedAt", direction: "desc" },
        ],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: 0,
      });
      expect(multiFieldStorageOrderZeroLimit.keys).toStrictEqual([]);
      expect(multiFieldStorageOrderZeroLimit.totalRows).toBe(2);

      const negativeLimitPlan = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKeyDescending,
        offset: 0,
        limit: -1,
      });
      expect(negativeLimitPlan.keys).toStrictEqual(["2"]);
      expect(negativeLimitPlan.totalRows).toBe(2);

      const nanLimitPlan = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKeyDescending,
        offset: 0,
        limit: Number.NaN,
      });
      expect(nanLimitPlan.keys).toStrictEqual([]);
      expect(nanLimitPlan.totalRows).toBe(2);

      const infiniteLimitPlan = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKeyDescending,
        offset: 0,
        limit: Number.POSITIVE_INFINITY,
      });
      expect(infiniteLimitPlan.keys).toStrictEqual(["2", "3"]);
      expect(infiniteLimitPlan.totalRows).toBe(2);

      const callbackRequiredStorageOrderPlan = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: true,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: matchesOnlySecondRow,
        compare: compareByKeyDescending,
        offset: 0,
        limit: 10,
      });
      expect(callbackRequiredStorageOrderPlan.keys).toStrictEqual(["2"]);
      expect(callbackRequiredStorageOrderPlan.totalRows).toBe(1);

      const manuallyOrderedEqualExclusiveBounds = readModel.scanRawWindow({
        predicate: {
          filters: [
            { field: "price", operator: "gte", value: 20 },
            { field: "price", operator: "gt", value: 20 },
            { field: "price", operator: "lte", value: 30 },
            { field: "price", operator: "lt", value: 30 },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: 10,
      });
      expect(manuallyOrderedEqualExclusiveBounds.keys).toStrictEqual([]);
      expect(manuallyOrderedEqualExclusiveBounds.totalRows).toBe(0);

      const unsafeRangeHintPlan = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "gt", value: "10" }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: 10,
      });
      expect(unsafeRangeHintPlan.keys).toStrictEqual(["2", "3"]);
      expect(unsafeRangeHintPlan.totalRows).toBe(2);

      const equalityHintPlan = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "eq", value: 20 }],
          callbackRequired: false,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: 10,
      });
      expect(equalityHintPlan.keys).toStrictEqual(["2"]);
      expect(equalityHintPlan.totalRows).toBe(1);

      const unsafeEqualityHintPlan = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "eq", value: "20" }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: 10,
      });
      expect(unsafeEqualityHintPlan.keys).toStrictEqual([]);
      expect(unsafeEqualityHintPlan.totalRows).toBe(0);

      const duplicateInHintPlan = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "in", values: [20, 20, 30] }],
          callbackRequired: false,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: 10,
      });
      expect(duplicateInHintPlan.keys).toStrictEqual(["2", "3"]);
      expect(duplicateInHintPlan.totalRows).toBe(2);

      const emptyInHintPlan = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "in", values: [] }],
          callbackRequired: false,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: 10,
      });
      expect(emptyInHintPlan.keys).toStrictEqual([]);
      expect(emptyInHintPlan.totalRows).toBe(0);

      const unsafeMixedInHintPlan = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "in", values: [20, "30"] }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: 10,
      });
      expect(unsafeMixedInHintPlan.keys).toStrictEqual(["2"]);
      expect(unsafeMixedInHintPlan.totalRows).toBe(1);

      const missingOrderColumnLimitedMisses = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [{ field: "missing", direction: "asc" }],
        matches: matchesOnlySecondRow,
        compare: compareByKeyDescending,
        offset: 0,
        limit: 1,
      });
      expect(missingOrderColumnLimitedMisses.keys).toStrictEqual(["2"]);
      expect(missingOrderColumnLimitedMisses.totalRows).toBe(1);

      const invalidStartsWithPlan = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "customerId", operator: "startsWith", value: 1 }],
          callbackRequired: true,
        },
        orderBy: [],
        matches: matchesOnlySecondRow,
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(invalidStartsWithPlan.keys).toStrictEqual(["2"]);

      const invalidRangePlan = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "gt", value: "10" }],
          callbackRequired: true,
        },
        orderBy: [],
        matches: matchesOnlySecondRow,
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(invalidRangePlan.keys).toStrictEqual(["2"]);

      const nonFiniteRangePlan = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "gt", value: Number.NaN }],
          callbackRequired: true,
        },
        orderBy: [],
        matches: matchesOnlySecondRow,
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(nonFiniteRangePlan.keys).toStrictEqual(["2"]);

      const zeroLimitPlan = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: 0,
      });
      expect(zeroLimitPlan.keys).toStrictEqual([]);
      expect(zeroLimitPlan.totalRows).toBe(2);

      const unsafeWindowEndPlan = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: compareByKey,
        offset: Number.MAX_SAFE_INTEGER,
        limit: 1,
      });
      expect(unsafeWindowEndPlan.keys).toStrictEqual([]);
      expect(unsafeWindowEndPlan.totalRows).toBe(2);
    }),
  );

  it.effect("uses bounded fallback scans for finite windows with stable row-key ties", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      yield* publishTopicStoreRow(store, order("d", "open", 20, 4), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("b", "open", 10, 2), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("a", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("c", "open", 10, 3), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("e", "open", 30, 5), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      const compareByPriceThenKey = (
        left: { readonly key: string; readonly row: object },
        right: { readonly key: string; readonly row: object },
      ) => {
        const priceComparison =
          numericRowField(left.row, "price") - numericRowField(right.row, "price");
        if (priceComparison !== 0) {
          return priceComparison;
        }
        return left.key.localeCompare(right.key);
      };

      const readModel = topicStoreReadModel(store);
      const boundedWindow = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: compareByPriceThenKey,
        offset: 1,
        limit: 3,
      });

      expect(boundedWindow.keys).toStrictEqual(["b", "c", "d"]);
      expect(boundedWindow.window.map((entry) => entry.key)).toStrictEqual(["b", "c", "d"]);
      expect(rowIds(boundedWindow.window.map((entry) => entry.row))).toStrictEqual(["b", "c", "d"]);
      expect(boundedWindow.totalRows).toBe(5);

      const cappedLargeWindow = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: compareByPriceThenKey,
        offset: 1_024,
        limit: 1,
      });

      expect(cappedLargeWindow.keys).toStrictEqual([]);
      expect(cappedLargeWindow.window).toStrictEqual([]);
      expect(cappedLargeWindow.totalRows).toBe(5);
    }),
  );

  it.effect("skips row callbacks for complete column predicate plans", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      yield* publishTopicStoreRow(store, order("missing-note", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(
        store,
        { ...order("matched-note", "open", 20, 2), note: "hello" },
        (topic, message) => InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(
        store,
        { ...order("other-note", "open", 30, 3), note: "bye" },
        (topic, message) => InvalidRowError.make({ topic, message }),
      );

      const readModel = topicStoreReadModel(store);
      const callbackSkipped = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "note", operator: "startsWith", value: "he" }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete column predicates should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });

      expect(callbackSkipped.keys).toStrictEqual(["matched-note"]);
      expect(callbackSkipped.totalRows).toBe(1);

      const optionalNotEqual = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "note", operator: "neq", value: "bye" }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete optional not-equal predicates should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });

      expect(optionalNotEqual.keys).toStrictEqual(["matched-note"]);
      expect(optionalNotEqual.totalRows).toBe(1);
    }),
  );

  it.effect("uses indexed scalar in predicate filters without row callbacks", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      yield* publishTopicStoreRow(store, order("cheap", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("matched", "open", 20, 2), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("expensive", "open", 30, 3), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(
        store,
        { ...order("noted", "open", 40, 4), note: "hello" },
        (topic, message) => InvalidRowError.make({ topic, message }),
      );
      const readModel = topicStoreReadModel(store);
      const indexedNumberIn = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20],
              valueKeys: new Set(["number:20"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("indexed numeric in predicates should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });

      expect(indexedNumberIn.keys).toStrictEqual(["matched"]);
      expect(indexedNumberIn.totalRows).toBe(1);

      const indexedOptionalIn = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "note",
              operator: "in",
              values: ["hello"],
              valueKeys: new Set(["string:5:hello"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("indexed optional in predicates should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });

      expect(indexedOptionalIn.keys).toStrictEqual(["noted"]);
      expect(indexedOptionalIn.totalRows).toBe(1);
    }),
  );

  it.effect("uses scalar predicate candidate scans across row mutations", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      yield* publishTopicStoreRow(store, order("cheap", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("matched", "open", 20, 2), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(
        store,
        { ...order("noted", "open", 40, 4), note: "hello" },
        (topic, message) => InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("cold", "closed", 99, 9), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      const readModel = topicStoreReadModel(store);
      const compareByKey = (left: { readonly key: string }, right: { readonly key: string }) =>
        left.key.localeCompare(right.key);

      const initialPriceMatch = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20],
              valueKeys: new Set(["number:20"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete scalar in predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(initialPriceMatch.keys).toStrictEqual(["matched"]);

      const initialPriceCountOnly = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20],
              valueKeys: new Set(["number:20"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete scalar count-only predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: 0,
      });
      expect(initialPriceCountOnly.keys).toStrictEqual([]);
      expect(initialPriceCountOnly.totalRows).toBe(1);

      const initialRangeGreaterThan = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "gt",
              value: 30,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete range gt predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(initialRangeGreaterThan.keys).toStrictEqual(["cold", "noted"]);

      const initialRangeGreaterThanOrEqual = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "gte",
              value: 40,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete range gte predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(initialRangeGreaterThanOrEqual.keys).toStrictEqual(["cold", "noted"]);

      const initialRangeLessThan = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "lt",
              value: 20,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete range lt predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(initialRangeLessThan.keys).toStrictEqual(["cheap"]);

      const initialRangeLessThanOrEqual = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "lte",
              value: 10,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete range lte predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(initialRangeLessThanOrEqual.keys).toStrictEqual(["cheap"]);

      const initialRangeCountOnly = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "gt",
              value: 30,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete range count-only predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: 0,
      });
      expect(initialRangeCountOnly.keys).toStrictEqual([]);
      expect(initialRangeCountOnly.totalRows).toBe(2);

      const rangeCandidateRejectedBySecondFilter = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "gt",
              value: 30,
            },
            {
              field: "status",
              operator: "in",
              values: ["open"],
              valueKeys: new Set(["string:4:open"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("compound range candidate predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(rangeCandidateRejectedBySecondFilter.keys).toStrictEqual(["noted"]);

      const rangeCandidateRejectedByIncompatibleSecondFilter = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "gt",
              value: 30,
            },
            {
              field: "note",
              operator: "gt",
              value: 30,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("incompatible compound range predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(rangeCandidateRejectedByIncompatibleSecondFilter.keys).toStrictEqual([]);

      const partialPriceBuckets = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20, 999],
              valueKeys: new Set(["number:20", "number:999"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("missing scalar buckets should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(partialPriceBuckets.keys).toStrictEqual(["matched"]);

      const stableTieWindow = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20, 10],
              valueKeys: new Set(["number:20", "number:10"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete scalar in tie windows should not call row callbacks");
        },
        compare: () => 0,
        offset: 0,
        limit: 1,
      });
      expect(stableTieWindow.keys).toStrictEqual(["cheap"]);

      const boundedCandidateRejectedBySecondFilter = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "status",
              operator: "in",
              values: ["closed"],
              valueKeys: new Set(["string:6:closed"]),
            },
            {
              field: "price",
              operator: "in",
              values: [10, 20],
              valueKeys: new Set(["number:10", "number:20"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("bounded scalar candidate scans should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: 1,
      });
      expect(boundedCandidateRejectedBySecondFilter.keys).toStrictEqual([]);

      const boundedCandidateReplacement = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [10, 20],
              valueKeys: new Set(["number:10", "number:20"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("bounded scalar candidate top-k scans should not call row callbacks");
        },
        compare: (left, right) => right.key.localeCompare(left.key),
        offset: 0,
        limit: 1,
      });
      expect(boundedCandidateReplacement.keys).toStrictEqual(["matched"]);

      const notedMatch = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "note",
              operator: "in",
              values: ["hello"],
              valueKeys: new Set(["string:5:hello"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete optional scalar in predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(notedMatch.keys).toStrictEqual(["noted"]);

      const closedStatusMatch = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "status",
              operator: "in",
              values: ["closed"],
              valueKeys: new Set(["string:6:closed"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("selective status predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(closedStatusMatch.keys).toStrictEqual(["cold"]);

      const sameSizeCandidateMatch = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "status",
              operator: "in",
              values: ["closed"],
              valueKeys: new Set(["string:6:closed"]),
            },
            {
              field: "note",
              operator: "in",
              values: ["hello"],
              valueKeys: new Set(["string:5:hello"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("same-size scalar candidates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(sameSizeCandidateMatch.keys).toStrictEqual([]);

      const broadStatusMatch = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "status",
              operator: "in",
              values: ["open"],
              valueKeys: new Set(["string:4:open"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("broad complete scalar predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(broadStatusMatch.keys).toStrictEqual(["cheap", "matched", "noted"]);

      yield* publishTopicStoreRow(store, order("also-matched", "open", 20, 3), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("other", "open", 30, 5), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("missing-note", "open", 50, 6), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      const insertedPriceMatches = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20],
              valueKeys: new Set(["number:20"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("scalar candidate scans should not call row callbacks after append");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(insertedPriceMatches.keys).toStrictEqual(["also-matched", "matched"]);

      const newPriceBucket = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [30],
              valueKeys: new Set(["number:30"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("new scalar candidate buckets should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(newPriceBucket.keys).toStrictEqual(["other"]);

      const insertedRangeMatches = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "gte",
              value: 50,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("range candidate scans should not call row callbacks after append");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(insertedRangeMatches.keys).toStrictEqual(["cold", "missing-note"]);

      const smallerCandidateWins = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "status",
              operator: "in",
              values: ["open"],
              valueKeys: new Set(["string:4:open"]),
            },
            {
              field: "price",
              operator: "eq",
              value: 20,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete scalar predicate candidates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(smallerCandidateWins.keys).toStrictEqual(["also-matched", "matched"]);

      yield* publishTopicStoreRow(store, order("matched", "open", 30, 7), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      const rebuiltPriceMatches = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20],
              valueKeys: new Set(["number:20"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("scalar candidate scans should not call row callbacks after replace");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(rebuiltPriceMatches.keys).toStrictEqual(["also-matched"]);

      const manualObjectEquality = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "eq",
              value: { price: 20 },
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(manualObjectEquality.keys).toStrictEqual([]);

      const missingFieldEq = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "missing",
              operator: "eq",
              value: "anything",
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(missingFieldEq.keys).toStrictEqual([
        "also-matched",
        "cheap",
        "cold",
        "matched",
        "missing-note",
        "noted",
        "other",
      ]);

      const missingFieldIn = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "missing",
              operator: "in",
              values: ["anything"],
              valueKeys: new Set(["string:8:anything"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(missingFieldIn.keys).toStrictEqual([
        "also-matched",
        "cheap",
        "cold",
        "matched",
        "missing-note",
        "noted",
        "other",
      ]);

      const missingFieldRange = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "missing",
              operator: "gt",
              value: 1,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(missingFieldRange.keys).toStrictEqual([
        "also-matched",
        "cheap",
        "cold",
        "matched",
        "missing-note",
        "noted",
        "other",
      ]);

      yield* deleteTopicStoreRow(store, "other");

      const afterDeleteRangeMatch = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "gte",
              value: 30,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("range candidate scans after delete should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(afterDeleteRangeMatch.keys).toStrictEqual([
        "cold",
        "matched",
        "missing-note",
        "noted",
      ]);

      const afterDeletePriceMatch = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "eq",
              value: 30,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("scalar candidate scans after delete should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(afterDeletePriceMatch.keys).toStrictEqual(["matched"]);

      yield* publishTopicStoreRows(
        store,
        [order("bulk-a", "open", 20, 8), order("bulk-b", "closed", 60, 9)],
        (topic, message) => InvalidRowError.make({ topic, message }),
      );

      const afterBulkPriceMatch = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20],
              valueKeys: new Set(["number:20"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error(
            "scalar candidate scans after bulk publish should not call row callbacks",
          );
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(afterBulkPriceMatch.keys).toStrictEqual(["also-matched", "bulk-a"]);

      yield* resetTopicStore(store);

      const afterResetPriceMatch = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20],
              valueKeys: new Set(["number:20"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("scalar candidate scans after reset should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(afterResetPriceMatch.keys).toStrictEqual([]);
    }),
  );

  it.effect("keeps optional column values out of exact range scans without row callbacks", () =>
    Effect.gen(function* () {
      const OptionalPrice = Schema.Struct({
        group: Schema.String,
        id: Schema.String,
        price: Schema.optionalKey(Schema.Finite),
      });
      const store = new TopicStore("optional-prices", OptionalPrice, "id", () => {});
      yield* publishTopicStoreRow(store, { group: "candidate", id: "missing" }, (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(
        store,
        { group: "other", id: "cheap", price: 5 },
        (topic, message) => InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(
        store,
        { group: "other", id: "expensive", price: 20 },
        (topic, message) => InvalidRowError.make({ topic, message }),
      );

      const readModel = topicStoreReadModel(store);
      const callbackSkipped = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "gt", value: 10 }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete range predicates should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });

      expect(callbackSkipped.keys).toStrictEqual(["expensive"]);
      expect(callbackSkipped.totalRows).toBe(1);

      const scalarCandidateRejectedByMissingRangeValue = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "group",
              operator: "in",
              values: ["candidate"],
              valueKeys: new Set(["string:9:candidate"]),
            },
            { field: "price", operator: "gt", value: 0 },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("missing optional range values should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });

      expect(scalarCandidateRejectedByMissingRangeValue.keys).toStrictEqual([]);
      expect(scalarCandidateRejectedByMissingRangeValue.totalRows).toBe(0);
    }),
  );

  it.effect("keeps broad exact scalar predicates correct without row callbacks", () =>
    Effect.gen(function* () {
      const SkewedRow = Schema.Struct({
        id: Schema.String,
        status: Schema.String,
      });
      const rowCount = 20_000;
      const skippedSlots = new Set(Array.from({ length: 4_096 }, (_value, index) => index * 2));
      const rows = Array.from({ length: rowCount }, (_value, index) => ({
        id: `row-${index.toString().padStart(5, "0")}`,
        status: skippedSlots.has(index) ? "skip" : "match",
      }));
      const store = new TopicStore("skewed", SkewedRow, "id", () => {});
      yield* publishTopicStoreRows(store, rows, (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      const readModel = topicStoreReadModel(store);
      const fallbackResult = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "status", operator: "eq", value: "match" }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete skewed predicates should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: 0,
      });

      expect(fallbackResult.keys).toStrictEqual([]);
      expect(fallbackResult.totalRows).toBe(rowCount - skippedSlots.size);
    }),
  );

  it("does not touch non-candidate row entries for exact scalar and range scans", () => {
    const first = order("first", "open", 10, 1);
    const second = order("second", "closed", 20, 2);
    const slots = [
      {
        key: first.id,
        row: first,
      },
      {
        key: second.id,
        row: second,
      },
    ];
    const state = {
      columns: new Map<string, ReadonlyArray<unknown>>([
        ["id", [first.id, second.id]],
        ["price", [first.price, second.price]],
        ["status", [first.status, second.status]],
      ]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: rawQueryCompilerMetadata(Order),
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots,
    };

    const warmRangeIndex = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "price", operator: "gt", value: 0 }],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [{ field: "price", direction: "asc" }],
      storageOrderBy: [{ field: "price", direction: "asc" }],
      matches: () => {
        throw new Error("exact range warmup should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: 2,
    });

    expect(warmRangeIndex.keys).toStrictEqual(["first", "second"]);
    expect(warmRangeIndex.totalRows).toBe(2);

    Object.defineProperty(slots, "0", {
      get: () => {
        throw new Error("non-candidate scalar slot should not be read");
      },
    });

    const scalarCandidate = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "price", operator: "eq", value: 20 }],
        callbackRequired: true,
      },
      orderBy: [],
      matches: () => true,
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });

    expect(scalarCandidate.keys).toStrictEqual(["second"]);
    expect(scalarCandidate.totalRows).toBe(1);

    const rangeCandidate = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "price", operator: "gt", value: 10 }],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [],
      matches: () => {
        throw new Error("exact range candidates should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });

    expect(rangeCandidate.keys).toStrictEqual(["second"]);
    expect(rangeCandidate.totalRows).toBe(1);

    const emptyRange = scanTopicRawWindow(state, {
      predicate: {
        filters: [
          { field: "price", operator: "gt", value: 30 },
          { field: "price", operator: "lt", value: 10 },
        ],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [],
      matches: () => {
        throw new Error("empty exact range candidates should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });

    expect(emptyRange.keys).toStrictEqual([]);
    expect(emptyRange.totalRows).toBe(0);
  });

  it("keeps exact candidate scans aligned with bounded and ordered raw windows", () => {
    const low = position("low", "AAPL", 5n, "10");
    const equal = position("equal", "AAPL", 10n, "10");
    const high = position("high", "MSFT", 20n, "10");
    const missingQuantity = {
      ...position("missing-quantity", "TSLA", 0n, "10"),
      quantity: undefined,
    };
    const rows = [low, equal, high, missingQuantity];
    const slots = rows.map((row) => ({
      key: row.id,
      row,
    }));
    const state = {
      columns: new Map<string, ReadonlyArray<unknown>>([
        ["id", rows.map((row) => row.id)],
        ["symbol", rows.map((row) => row.symbol)],
        ["quantity", rows.map((row) => row.quantity)],
      ]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: rawQueryCompilerMetadata(Position),
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots,
    };

    const boundedReplacement = scanTopicRawWindow(state, {
      predicate: {
        filters: [],
        callbackRequired: true,
      },
      orderBy: [],
      matches: () => true,
      compare: (left, right) =>
        Number(fieldValue(right.row, "quantity")) - Number(fieldValue(left.row, "quantity")),
      offset: 0,
      limit: 1,
    });

    expect(boundedReplacement.keys).toStrictEqual(["high"]);
    expect(boundedReplacement.totalRows).toBe(4);

    const exactBigIntRange = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "quantity", operator: "gte", value: 10n }],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [],
      matches: () => {
        throw new Error("exact BigInt range candidates should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });

    expect(exactBigIntRange.keys).toStrictEqual(["equal", "high"]);
    expect(exactBigIntRange.totalRows).toBe(2);

    const nonExactBigIntRange = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "quantity", operator: "gte", value: 10n }],
        callbackRequired: false,
      },
      orderBy: [],
      matches: () => true,
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });

    expect(nonExactBigIntRange.keys).toStrictEqual(["equal", "high", "missing-quantity"]);
    expect(nonExactBigIntRange.totalRows).toBe(3);

    const nonExactBigIntUpperRange = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "quantity", operator: "lte", value: 10n }],
        callbackRequired: false,
      },
      orderBy: [],
      matches: () => true,
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });

    expect(nonExactBigIntUpperRange.keys).toStrictEqual(["equal", "low", "missing-quantity"]);
    expect(nonExactBigIntUpperRange.totalRows).toBe(3);

    const nonExactBigIntLowerRange = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "quantity", operator: "gt", value: 10n }],
        callbackRequired: false,
      },
      orderBy: [],
      matches: () => true,
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });

    expect(nonExactBigIntLowerRange.keys).toStrictEqual(["high", "missing-quantity"]);
    expect(nonExactBigIntLowerRange.totalRows).toBe(2);

    const orderedCandidate = scanTopicRawWindow(state, {
      predicate: {
        filters: [
          { field: "quantity", operator: "gt", value: 0n },
          {
            field: "symbol",
            operator: "in",
            values: ["MSFT"],
            valueKeys: new Set(["string:4:MSFT"]),
          },
        ],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [{ field: "quantity", direction: "asc" }],
      storageOrderBy: [{ field: "quantity", direction: "asc" }],
      matches: () => {
        throw new Error("ordered exact candidates should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: 2,
    });

    expect(orderedCandidate.keys).toStrictEqual(["high"]);
    expect(orderedCandidate.totalRows).toBe(1);
    expect(state.scalarPredicateIndexes.size).toBe(0);

    const openScalarBucket = selectedPredicateCandidateSlots(
      state,
      [{ field: "symbol", operator: "eq", value: "AAPL" }],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
        maxSlotCount: state.slots.length,
      },
    );

    expect(openScalarBucket?.slots).toStrictEqual([0, 1]);

    const existingBucketOverBudget = selectedPredicateCandidateSlots(
      state,
      [{ field: "symbol", operator: "in", values: ["AAPL", "NVDA"] }],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
        maxSlotCount: 2,
      },
    );

    expect(existingBucketOverBudget).toBeUndefined();

    const existingIndexWithMissingBucketBuildDisabled = selectedPredicateCandidateSlots(
      state,
      [{ field: "symbol", operator: "in", values: ["AAPL", "NVDA"] }],
      {
        allowScalarIndexBuild: false,
        exactRangeCandidates: true,
        maxSlotCount: state.slots.length,
      },
    );

    expect(existingIndexWithMissingBucketBuildDisabled).toBeUndefined();

    const orderedMissingScalarBucket = scanTopicRawWindow(state, {
      predicate: {
        filters: [
          { field: "quantity", operator: "gt", value: 0n },
          { field: "symbol", operator: "eq", value: "NVDA" },
        ],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [{ field: "quantity", direction: "asc" }],
      storageOrderBy: [{ field: "quantity", direction: "asc" }],
      matches: () => {
        throw new Error("ordered missing scalar candidates should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: 2,
    });

    expect(orderedMissingScalarBucket.keys).toStrictEqual([]);
    expect(orderedMissingScalarBucket.totalRows).toBe(0);
    expect(state.scalarPredicateIndexes.has("symbol")).toBe(true);
    const symbolIndex = state.scalarPredicateIndexes.get("symbol")!;
    expect(symbolIndex.buckets.has("string:4:NVDA")).toBe(false);
  });

  it("rejects exact candidates that are not smaller than their scan budget", () => {
    const rows = [order("a", "open", 1, 1), order("b", "closed", 2, 2), order("c", "open", 3, 3)];
    const state = {
      columns: new Map<string, ReadonlyArray<unknown>>([
        ["id", rows.map((row) => row.id)],
        ["price", rows.map((row) => row.price)],
        ["status", [rows[0]!.status, rows[1]!.status, { nonScalar: true }]],
      ]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: rawQueryCompilerMetadata(Order),
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots: rows.map((row) => ({
        key: row.id,
        row,
      })),
    };

    const valueKeysTakePrecedence = scanTopicRawWindow(state, {
      predicate: {
        filters: [
          {
            field: "status",
            operator: "in",
            values: ["closed"],
            valueKeys: new Set([scalarEqualityKey("open")!]),
          },
        ],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [],
      matches: () => {
        throw new Error("exact value-key candidates should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });

    expect(valueKeysTakePrecedence.keys).toStrictEqual(["a"]);
    expect(valueKeysTakePrecedence.totalRows).toBe(1);

    const orderedInWithoutValueKeys = scanTopicRawWindow(state, {
      predicate: {
        filters: [
          {
            field: "status",
            operator: "in",
            values: ["open"],
          },
        ],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [{ field: "status", direction: "asc" }],
      storageOrderBy: [{ field: "status", direction: "asc" }],
      matches: () => {
        throw new Error("ordered exact in candidates should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: 2,
    });

    expect(orderedInWithoutValueKeys.keys).toStrictEqual(["a"]);
    expect(orderedInWithoutValueKeys.totalRows).toBe(1);

    const orderedValueKeysTakePrecedence = scanTopicRawWindow(state, {
      predicate: {
        filters: [
          {
            field: "status",
            operator: "in",
            values: ["closed"],
            valueKeys: new Set([scalarEqualityKey("open")!]),
          },
        ],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [{ field: "status", direction: "asc" }],
      storageOrderBy: [{ field: "status", direction: "asc" }],
      matches: () => {
        throw new Error("ordered exact value-key candidates should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: 2,
    });

    expect(orderedValueKeysTakePrecedence.keys).toStrictEqual(["a"]);
    expect(orderedValueKeysTakePrecedence.totalRows).toBe(1);

    const orderedValueKeySizeMismatch = scanTopicRawWindow(state, {
      predicate: {
        filters: [
          {
            field: "status",
            operator: "in",
            values: [],
            valueKeys: new Set([scalarEqualityKey("open")!]),
          },
        ],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [{ field: "status", direction: "asc" }],
      storageOrderBy: [{ field: "status", direction: "asc" }],
      matches: () => {
        throw new Error("ordered exact value-key mismatch should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: 2,
    });

    expect(orderedValueKeySizeMismatch.keys).toStrictEqual(["a"]);
    expect(orderedValueKeySizeMismatch.totalRows).toBe(1);

    const nonScalarInWithoutValueKeys = selectedPredicateCandidateSlots(
      state,
      [{ field: "status", operator: "in", values: [{ structured: true }] }],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
        maxSlotCount: state.slots.length,
      },
    );

    expect(nonScalarInWithoutValueKeys).toBeUndefined();

    const broadScalar = selectedPredicateCandidateSlots(
      state,
      [{ field: "status", operator: "in", values: ["open", "closed"] }],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
        maxSlotCount: 2,
      },
    );

    expect(broadScalar).toBeUndefined();

    const missingScalarWithNonScalarColumnEntry = selectedPredicateCandidateSlots(
      state,
      [{ field: "status", operator: "in", values: ["missing", "absent"] }],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
        maxSlotCount: state.slots.length,
      },
    );

    expect(missingScalarWithNonScalarColumnEntry?.slots).toStrictEqual([]);
    const statusIndex = state.scalarPredicateIndexes.get("status")!;
    expect(statusIndex.indexedKeys.has(scalarEqualityKey("missing")!)).toBe(false);
    expect(statusIndex.indexedKeys.has(scalarEqualityKey("absent")!)).toBe(false);
    expect(statusIndex.buckets.has(scalarEqualityKey("missing")!)).toBe(false);
    expect(statusIndex.buckets.has(scalarEqualityKey("absent")!)).toBe(false);

    const singleMissingScalar = selectedPredicateCandidateSlots(
      state,
      [{ field: "status", operator: "eq", value: "single-missing" }],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
        maxSlotCount: state.slots.length,
      },
    );

    expect(singleMissingScalar?.slots).toStrictEqual([]);
    expect(statusIndex.indexedKeys.has(scalarEqualityKey("single-missing")!)).toBe(false);
    expect(statusIndex.buckets.has(scalarEqualityKey("single-missing")!)).toBe(false);

    const zeroBudgetCandidate = selectedPredicateCandidateSlots(
      state,
      [{ field: "status", operator: "in", values: [] }],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
        maxSlotCount: 0,
      },
    );

    expect(zeroBudgetCandidate).toBeUndefined();

    const warmPriceIndex = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "price", operator: "gt", value: 0 }],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [{ field: "price", direction: "asc" }],
      storageOrderBy: [{ field: "price", direction: "asc" }],
      matches: () => {
        throw new Error("exact range index warmup should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: 3,
    });

    expect(warmPriceIndex.totalRows).toBe(3);

    const fullRange = selectedPredicateCandidateSlots(
      state,
      [{ field: "price", operator: "gte", value: 1 }],
      {
        allowScalarIndexBuild: false,
        exactRangeCandidates: true,
      },
    );

    expect(fullRange).toBeUndefined();

    const rangeNotSmallerThanBudget = selectedPredicateCandidateSlots(
      state,
      [{ field: "price", operator: "gte", value: 2 }],
      {
        allowScalarIndexBuild: false,
        exactRangeCandidates: true,
        maxSlotCount: 2,
      },
    );

    expect(rangeNotSmallerThanBudget).toBeUndefined();
  });

  it.effect("uses bigint range hints conservatively for manual plans", () =>
    Effect.gen(function* () {
      const store = new TopicStore("positions", Position, "id", () => {});
      yield* publishTopicStoreRow(store, position("low", "AAPL", 5n, "10"), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, position("equal", "AAPL", 10n, "10"), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, position("high", "AAPL", 20n, "10"), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      const readModel = topicStoreReadModel(store);
      const rangeHinted = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "quantity", operator: "gt", value: 10n }],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });

      expect(rangeHinted.keys).toStrictEqual(["high"]);
      expect(rangeHinted.totalRows).toBe(1);
    }),
  );

  it.effect("handles exact not-equal column predicates without row callbacks", () =>
    Effect.gen(function* () {
      const orderStore = new TopicStore("orders", Order, "id", () => {});
      yield* publishTopicStoreRow(orderStore, order("cheap", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(orderStore, order("excluded", "open", 20, 2), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(orderStore, order("expensive", "open", 30, 3), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      const orderReadModel = topicStoreReadModel(orderStore);
      const exactNumberNotEqual = orderReadModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "neq", value: 20 }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("exact numeric not-equal predicates should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });
      expect(exactNumberNotEqual.keys).toStrictEqual(["cheap", "expensive"]);
      expect(exactNumberNotEqual.totalRows).toBe(2);

      const manualRangeHint = orderReadModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "gt", value: 10 }],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });
      expect(manualRangeHint.keys).toStrictEqual(["excluded", "expensive"]);
      expect(manualRangeHint.totalRows).toBe(2);

      const manualNotEqualHint = orderReadModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "neq", value: 20 }],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });
      expect(manualNotEqualHint.keys).toStrictEqual(["cheap", "expensive"]);
      expect(manualNotEqualHint.totalRows).toBe(2);

      const manualGreaterThanOrEqualHint = orderReadModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "gte", value: 20 }],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });
      expect(manualGreaterThanOrEqualHint.keys).toStrictEqual(["excluded", "expensive"]);
      expect(manualGreaterThanOrEqualHint.totalRows).toBe(2);

      const manualLessThanHint = orderReadModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "lt", value: 30 }],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });
      expect(manualLessThanHint.keys).toStrictEqual(["cheap", "excluded"]);
      expect(manualLessThanHint.totalRows).toBe(2);

      const manualLessThanOrEqualHint = orderReadModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "lte", value: 20 }],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });
      expect(manualLessThanOrEqualHint.keys).toStrictEqual(["cheap", "excluded"]);
      expect(manualLessThanOrEqualHint.totalRows).toBe(2);

      const positionStore = new TopicStore("positions", Position, "id", () => {});
      yield* publishTopicStoreRow(
        positionStore,
        position("drop", "DROP", 20n, "10"),
        (topic, message) => InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(
        positionStore,
        position("excluded-price", "AAPL", 20n, "99"),
        (topic, message) => InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(
        positionStore,
        position("excluded-quantity", "AAPL", 10n, "10"),
        (topic, message) => InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(
        positionStore,
        position("keep", "AAPL", 20n, "10"),
        (topic, message) => InvalidRowError.make({ topic, message }),
      );

      const positionReadModel = topicStoreReadModel(positionStore);
      const manualBigDecimalRangeHint = positionReadModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "gt", value: fromStringUnsafe("20") }],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });
      expect(manualBigDecimalRangeHint.keys).toStrictEqual(["excluded-price"]);
      expect(manualBigDecimalRangeHint.totalRows).toBe(1);

      const exactMixedNotEqual = positionReadModel.scanRawWindow({
        predicate: {
          filters: [
            { field: "symbol", operator: "neq", value: "DROP" },
            { field: "quantity", operator: "neq", value: 10n },
            { field: "price", operator: "neq", value: fromStringUnsafe("99") },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("exact mixed not-equal predicates should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });

      expect(exactMixedNotEqual.keys).toStrictEqual(["keep"]);
      expect(exactMixedNotEqual.totalRows).toBe(1);

      const LooseNumber = Schema.Struct({
        id: Schema.String,
        value: Schema.Number,
      });
      const looseNumberStore = new TopicStore("loose-numbers", LooseNumber, "id", () => {});
      yield* publishTopicStoreRow(
        looseNumberStore,
        { id: "nan", value: Number.NaN },
        (topic, message) => InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(looseNumberStore, { id: "real", value: 20 }, (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      const looseNumberReadModel = topicStoreReadModel(looseNumberStore);
      const exactFiniteRange = looseNumberReadModel.scanRawWindow({
        predicate: {
          filters: [{ field: "value", operator: "gt", value: 10 }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("exact finite range predicates should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });

      expect(exactFiniteRange.keys).toStrictEqual(["real"]);
      expect(exactFiniteRange.totalRows).toBe(1);
    }),
  );

  it.effect("does not notify subscribers that were closed after mutation capture", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      let notifyCount = 0;
      const subscriber: LiveTopicSubscriber = {
        topic: "orders",
        queryId: "query-closed-before-notify",
        notify: () =>
          Effect.sync(() => {
            notifyCount += 1;
          }),
        queuedEvents: Effect.succeed(0),
        end: Effect.void,
        closeWithStatus: () => Effect.void,
        maxQueueDepth: 0,
        backpressureEvents: 0,
        closed: false,
      };

      yield* registerTestTopicStoreSubscriber(store, subscriber);
      subscriber.closed = true;
      yield* publishTopicStoreRow(store, order("1", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      expect(notifyCount).toBe(0);
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
      yield* registerTestTopicStoreSubscriber(store, subscriber);
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
      yield* registerTestTopicStoreSubscriber(store, subscriber);
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
      const makeExecution = () => {
        const evaluation = evaluate();
        return {
          incremental: false,
          latest: () => evaluation,
        };
      };

      const execution = yield* acquireMaterializedQueryExecution(
        readModel,
        "empty-materialized",
        makeExecution,
      );
      const cursor = execution.createCursor();
      const unchanged = yield* execution.next("query-unchanged", cursor);

      expect(Option.isNone(unchanged)).toBe(true);
      expect(evaluationCount).toBe(1);
      yield* acquireMaterializedQueryExecution(readModel, "second-materialized", makeExecution);
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
  it("encodes scalar equality keys deterministically", () => {
    expect(scalarEqualityKey(null)).toBe("null");
    expect(scalarEqualityKey("open")).toBe("string:4:open");
    expect(scalarEqualityKey(false)).toBe("boolean:false");
    expect(scalarEqualityKey(true)).toBe("boolean:true");
    expect(scalarEqualityKey(1n)).toBe("bigint:1");
    expect(scalarEqualityKey(-0)).toBe("number:-0");
    expect(scalarEqualityKey(Number.NaN)).toBe("number:NaN");
    expect(scalarEqualityKey(Number.POSITIVE_INFINITY)).toBe("number:Infinity");
    expect(scalarEqualityKey(fromStringUnsafe("1.0"))).toBe("bigDecimal:1");
    expect(scalarEqualityKey({ value: "open" })).toBeUndefined();
  });

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
