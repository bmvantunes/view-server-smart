import { fromStringUnsafe } from "effect/BigDecimal";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import {
  acquireRawQueryExecution,
  activeStoreRawQueryExecutionCount,
  releaseRawQueryExecution,
} from "./active-query";
import { prepareRawQuery } from "./raw-query-compiler";
import { publishTopicStoreRow, TopicStore } from "./topic-store";

const invalidRow = (_topic: string, message: string): Error => new Error(message);

describe("column-live-view-engine active query execution", () => {
  it.effect("reuses execution state for identical compiled raw queries", () =>
    Effect.gen(function* () {
      const rowSchema = Schema.Struct({
        id: Schema.String,
        status: Schema.String,
        score: Schema.Number,
        count: Schema.BigInt,
        value: Schema.BigDecimal,
      });
      const store = new TopicStore("scores", rowSchema, "id", () => {});

      yield* publishTopicStoreRow(
        store,
        {
          id: "1",
          status: "open",
          score: 10,
          count: 1n,
          value: fromStringUnsafe("1.00"),
        },
        invalidRow,
      );
      yield* publishTopicStoreRow(
        store,
        {
          id: "2",
          status: "closed",
          score: 20,
          count: 2n,
          value: fromStringUnsafe("2.00"),
        },
        invalidRow,
      );

      const compiled = yield* prepareRawQuery("scores", store.rawQueryMetadata, {
        select: ["id", "score", "count", "value"],
        where: {
          status: "open",
        },
        orderBy: [
          {
            field: "score",
            direction: "desc",
          },
        ],
      });

      const firstExecution = yield* acquireRawQueryExecution(store, compiled);
      const secondExecution = yield* acquireRawQueryExecution(store, compiled);
      expect(yield* activeStoreRawQueryExecutionCount(store)).toBe(1);

      const firstCursor = firstExecution.createCursor();
      const secondCursor = secondExecution.createCursor();

      const initialFirst = firstExecution.initial("query-a");
      const initialSecond = secondExecution.initial("query-b");
      expect(initialFirst.rows).toStrictEqual(initialSecond.rows);
      expect(initialFirst.totalRows).toBe(1);
      expect(initialFirst.keys).toStrictEqual(["1"]);

      const beforePublishFirst = yield* firstExecution.next("query-a", firstCursor);
      const beforePublishSecond = yield* secondExecution.next("query-b", secondCursor);
      expect(beforePublishFirst._tag).toBe("None");
      expect(beforePublishSecond._tag).toBe("None");

      yield* publishTopicStoreRow(
        store,
        {
          id: "3",
          status: "open",
          score: 5,
          count: 3n,
          value: fromStringUnsafe("3.00"),
        },
        invalidRow,
      );

      const afterPublishFirst = yield* firstExecution.next("query-a", firstCursor);
      const afterPublishSecond = yield* secondExecution.next("query-b", secondCursor);
      expect(afterPublishFirst._tag).toBe("Some");
      expect(afterPublishSecond._tag).toBe("Some");

      yield* releaseRawQueryExecution(store, compiled);
      expect(yield* activeStoreRawQueryExecutionCount(store)).toBe(1);
      const afterRefcountDecrement = yield* acquireRawQueryExecution(store, compiled);
      expect(afterRefcountDecrement.initial("query-c").totalRows).toBe(2);
      expect(yield* activeStoreRawQueryExecutionCount(store)).toBe(1);

      yield* releaseRawQueryExecution(store, compiled);
      yield* releaseRawQueryExecution(store, compiled);
      expect(yield* activeStoreRawQueryExecutionCount(store)).toBe(0);

      const afterRefcountExhausted = yield* acquireRawQueryExecution(store, compiled);
      expect(afterRefcountExhausted.initial("query-d").totalRows).toBe(2);
      expect(yield* activeStoreRawQueryExecutionCount(store)).toBe(1);
      yield* releaseRawQueryExecution(store, compiled);
    }),
  );

  it.effect("shares base evaluation across different projections", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "projection-sharing",
        Schema.Struct({
          id: Schema.String,
          status: Schema.String,
          score: Schema.Number,
        }),
        "id",
        () => {},
      );
      yield* publishTopicStoreRow(store, { id: "a", status: "open", score: 1 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "b", status: "open", score: 2 }, invalidRow);

      const idOnly = yield* prepareRawQuery("projection-sharing", store.rawQueryMetadata, {
        select: ["id"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "score", direction: "desc" }],
      });
      const idAndScore = yield* prepareRawQuery("projection-sharing", store.rawQueryMetadata, {
        select: ["id", "score"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "score", direction: "desc" }],
      });

      const idOnlyExecution = yield* acquireRawQueryExecution(store, idOnly);
      const idAndScoreExecution = yield* acquireRawQueryExecution(store, idAndScore);

      expect(yield* activeStoreRawQueryExecutionCount(store)).toBe(1);
      expect(idOnlyExecution.initial("ids").rows).toStrictEqual([{ id: "b" }, { id: "a" }]);
      expect(idAndScoreExecution.initial("scores").rows).toStrictEqual([
        { id: "b", score: 2 },
        { id: "a", score: 1 },
      ]);

      yield* releaseRawQueryExecution(store, idOnly);
      yield* releaseRawQueryExecution(store, idAndScore);
      expect(yield* activeStoreRawQueryExecutionCount(store)).toBe(0);
    }),
  );

  it.effect("no-ops release when execution cache does not contain a query", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "numbers",
        Schema.Struct({ id: Schema.String, score: Schema.Number }),
        "id",
        () => {},
      );
      const compiled = yield* prepareRawQuery("numbers", store.rawQueryMetadata, {
        select: ["id"],
        where: {
          score: 1,
        },
      });

      yield* releaseRawQueryExecution(store, compiled);
    }),
  );

  it.effect("covers query cache key paths for numeric, bigint, and BigDecimal filters", () =>
    Effect.gen(function* () {
      const numericStore = new TopicStore(
        "numbers",
        Schema.Struct({ id: Schema.String, score: Schema.Number }),
        "id",
        () => {},
      );
      const bigintStore = new TopicStore(
        "bigints",
        Schema.Struct({ id: Schema.String, amount: Schema.BigInt }),
        "id",
        () => {},
      );
      const decimalStore = new TopicStore(
        "decimals",
        Schema.Struct({ id: Schema.String, price: Schema.BigDecimal }),
        "id",
        () => {},
      );

      yield* publishTopicStoreRow(numericStore, { id: "a", score: 10 }, invalidRow);
      yield* publishTopicStoreRow(bigintStore, { id: "b", amount: 5n }, invalidRow);
      yield* publishTopicStoreRow(
        decimalStore,
        { id: "c", price: fromStringUnsafe("1.23") },
        invalidRow,
      );

      const infFilter = yield* prepareRawQuery("numbers", numericStore.rawQueryMetadata, {
        select: ["id", "score"],
        where: {
          score: Number.POSITIVE_INFINITY,
        },
      });
      const nanFilter = yield* prepareRawQuery("numbers", numericStore.rawQueryMetadata, {
        select: ["id", "score"],
        where: {
          score: Number.NaN,
        },
      });
      const zeroFilter = yield* prepareRawQuery("numbers", numericStore.rawQueryMetadata, {
        select: ["id", "score"],
        where: {
          score: 10,
        },
      });
      const positiveZeroFilter = yield* prepareRawQuery("numbers", numericStore.rawQueryMetadata, {
        select: ["id", "score"],
        where: {
          score: 0,
        },
      });
      const negativeZeroFilter = yield* prepareRawQuery("numbers", numericStore.rawQueryMetadata, {
        select: ["id", "score"],
        where: {
          score: -0,
        },
      });
      const offsetFilter = yield* prepareRawQuery("numbers", numericStore.rawQueryMetadata, {
        select: ["id", "score"],
        offset: 1,
      });
      const bigIntFilter = yield* prepareRawQuery("bigints", bigintStore.rawQueryMetadata, {
        select: ["id", "amount"],
        where: {
          amount: 3n,
        },
      });
      const decimalFilter = yield* prepareRawQuery("decimals", decimalStore.rawQueryMetadata, {
        select: ["id", "price"],
        where: {
          price: fromStringUnsafe("1.23"),
        },
      });

      const infExecution = yield* acquireRawQueryExecution(numericStore, infFilter);
      const nanExecution = yield* acquireRawQueryExecution(numericStore, nanFilter);
      const zeroExecution = yield* acquireRawQueryExecution(numericStore, zeroFilter);
      yield* acquireRawQueryExecution(numericStore, positiveZeroFilter);
      yield* acquireRawQueryExecution(numericStore, negativeZeroFilter);
      const offsetExecution = yield* acquireRawQueryExecution(numericStore, offsetFilter);
      const bigintExecution = yield* acquireRawQueryExecution(bigintStore, bigIntFilter);
      const decimalExecution = yield* acquireRawQueryExecution(decimalStore, decimalFilter);

      expect(yield* activeStoreRawQueryExecutionCount(numericStore)).toBe(6);
      expect(yield* activeStoreRawQueryExecutionCount(bigintStore)).toBe(1);
      expect(yield* activeStoreRawQueryExecutionCount(decimalStore)).toBe(1);

      const infCursor = infExecution.createCursor();
      const nanCursor = nanExecution.createCursor();
      const zeroCursor = zeroExecution.createCursor();
      const offsetCursor = offsetExecution.createCursor();
      const bigintCursor = bigintExecution.createCursor();
      const decimalCursor = decimalExecution.createCursor();

      expect(infExecution.initial("q").totalRows).toBe(0);
      expect(nanExecution.initial("q").totalRows).toBe(0);
      expect(zeroExecution.initial("q").totalRows).toBe(1);
      expect(offsetExecution.initial("q").totalRows).toBe(1);
      expect(bigintExecution.initial("q").totalRows).toBe(0);
      expect(decimalExecution.initial("q").totalRows).toBe(1);

      expect((yield* infExecution.next("q", infCursor))._tag).toBe("None");
      expect((yield* nanExecution.next("q", nanCursor))._tag).toBe("None");
      expect((yield* zeroExecution.next("q", zeroCursor))._tag).toBe("None");
      expect((yield* offsetExecution.next("q", offsetCursor))._tag).toBe("None");
      expect((yield* bigintExecution.next("q", bigintCursor))._tag).toBe("None");
      expect((yield* decimalExecution.next("q", decimalCursor))._tag).toBe("None");

      yield* releaseRawQueryExecution(numericStore, infFilter);
      yield* releaseRawQueryExecution(numericStore, nanFilter);
      yield* releaseRawQueryExecution(numericStore, zeroFilter);
      yield* releaseRawQueryExecution(numericStore, positiveZeroFilter);
      yield* releaseRawQueryExecution(numericStore, negativeZeroFilter);
      yield* releaseRawQueryExecution(numericStore, offsetFilter);
      yield* releaseRawQueryExecution(bigintStore, bigIntFilter);
      yield* releaseRawQueryExecution(decimalStore, decimalFilter);
    }),
  );

  it.effect("covers cache keys for nullish, array, object, and boolean filter values", () =>
    Effect.gen(function* () {
      const eventStore = new TopicStore(
        "events",
        Schema.Struct({
          id: Schema.String,
          label: Schema.String,
          tags: Schema.Array(Schema.String),
          metadata: Schema.Struct({
            kind: Schema.String,
            scope: Schema.String,
          }),
          active: Schema.Boolean,
        }),
        "id",
        () => {},
      );

      yield* publishTopicStoreRow(
        eventStore,
        {
          id: "a",
          label: "foo",
          tags: ["open", "closed"],
          metadata: { kind: "test", scope: "global" },
          active: true,
        },
        invalidRow,
      );

      const undefinedFilter = yield* prepareRawQuery("events", eventStore.rawQueryMetadata, {
        select: ["id", "label"],
        where: {
          label: undefined,
        },
      });
      const nullFilter = yield* prepareRawQuery("events", eventStore.rawQueryMetadata, {
        select: ["id", "label"],
        where: {
          label: null,
        },
      });
      const arrayFilter = yield* prepareRawQuery("events", eventStore.rawQueryMetadata, {
        select: ["id", "tags"],
        where: {
          tags: ["open", "closed"],
        },
      });
      const objectFilter = yield* prepareRawQuery("events", eventStore.rawQueryMetadata, {
        select: ["id", "metadata"],
        where: {
          metadata: { kind: "test", scope: "global" },
        },
      });
      const booleanFilter = yield* prepareRawQuery("events", eventStore.rawQueryMetadata, {
        select: ["id", "active"],
        where: {
          active: true,
        },
      });

      const undefinedExecution = yield* acquireRawQueryExecution(eventStore, undefinedFilter);
      const nullExecution = yield* acquireRawQueryExecution(eventStore, nullFilter);
      const arrayExecution = yield* acquireRawQueryExecution(eventStore, arrayFilter);
      const objectExecution = yield* acquireRawQueryExecution(eventStore, objectFilter);
      const booleanExecution = yield* acquireRawQueryExecution(eventStore, booleanFilter);

      expect(yield* activeStoreRawQueryExecutionCount(eventStore)).toBe(5);

      expect(undefinedExecution.initial("query").totalRows).toBe(0);
      expect(nullExecution.initial("query").totalRows).toBe(0);
      expect(arrayExecution.initial("query").totalRows).toBe(1);
      expect(objectExecution.initial("query").totalRows).toBe(1);
      expect(booleanExecution.initial("query").totalRows).toBe(1);

      const undefinedCursor = undefinedExecution.createCursor();
      const nullCursor = nullExecution.createCursor();
      const arrayCursor = arrayExecution.createCursor();
      const objectCursor = objectExecution.createCursor();
      const booleanCursor = booleanExecution.createCursor();

      expect((yield* undefinedExecution.next("query", undefinedCursor))._tag).toBe("None");
      expect((yield* nullExecution.next("query", nullCursor))._tag).toBe("None");
      expect((yield* arrayExecution.next("query", arrayCursor))._tag).toBe("None");
      expect((yield* objectExecution.next("query", objectCursor))._tag).toBe("None");
      expect((yield* booleanExecution.next("query", booleanCursor))._tag).toBe("None");

      yield* releaseRawQueryExecution(eventStore, undefinedFilter);
      yield* releaseRawQueryExecution(eventStore, nullFilter);
      yield* releaseRawQueryExecution(eventStore, arrayFilter);
      yield* releaseRawQueryExecution(eventStore, objectFilter);
      yield* releaseRawQueryExecution(eventStore, booleanFilter);
    }),
  );

  it.effect("does not collide delimiter-bearing field names in cache keys", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "delimiter-fields",
        Schema.Struct({
          id: Schema.String,
          a: Schema.Number,
          b: Schema.Number,
          "a:asc;b": Schema.Number,
        }),
        "id",
        () => {},
      );
      const splitFieldsQuery = yield* prepareRawQuery("delimiter-fields", store.rawQueryMetadata, {
        select: ["id"],
        orderBy: [
          {
            field: "a",
            direction: "asc",
          },
          {
            field: "b",
            direction: "desc",
          },
        ],
      });
      const delimiterFieldQuery = yield* prepareRawQuery(
        "delimiter-fields",
        store.rawQueryMetadata,
        {
          select: ["id"],
          orderBy: [
            {
              field: "a:asc;b",
              direction: "desc",
            },
          ],
        },
      );

      yield* acquireRawQueryExecution(store, splitFieldsQuery);
      yield* acquireRawQueryExecution(store, delimiterFieldQuery);

      expect(yield* activeStoreRawQueryExecutionCount(store)).toBe(2);

      yield* releaseRawQueryExecution(store, splitFieldsQuery);
      yield* releaseRawQueryExecution(store, delimiterFieldQuery);
    }),
  );

  it.effect("covers query cache encoding for non-plain object filter values", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "special",
        Schema.Struct({
          id: Schema.String,
          payload: Schema.Struct({
            value: Schema.BigInt,
            label: Schema.String,
          }),
        }),
        "id",
        () => {},
      );
      const queryPayload: Record<string, unknown> = Object.create(null);
      queryPayload["value"] = 1n;
      queryPayload["label"] = "special";

      const compiled = yield* prepareRawQuery("special", store.rawQueryMetadata, {
        select: ["id", "payload"],
        where: {
          payload: queryPayload,
        },
      });

      const execution = yield* acquireRawQueryExecution(store, compiled);
      const cursor = execution.createCursor();

      expect(yield* activeStoreRawQueryExecutionCount(store)).toBe(1);
      expect(execution.initial("query").rows).toStrictEqual([]);
      expect((yield* execution.next("query", cursor))._tag).toBe("None");

      yield* releaseRawQueryExecution(store, compiled);
      expect(yield* activeStoreRawQueryExecutionCount(store)).toBe(0);
    }),
  );

  it.effect("covers cache key encoding for non-serializable filter values", () =>
    Effect.gen(function* () {
      const firstFunction = () => "first";
      const secondFunction = () => "second";
      const anonymousFunction = (() =>
        function () {
          return "anonymous";
        })();
      const firstSymbol = Symbol("first");
      const secondSymbol = Symbol("second");
      const firstMap = new Map([["marker", "first"]]);
      const secondMap = new Map([["marker", "second"]]);
      const store = new TopicStore(
        "special-non-serializable",
        Schema.Struct({
          id: Schema.String,
          marker: Schema.Unknown,
        }),
        "id",
        () => {},
      );

      const firstFunctionQuery = yield* prepareRawQuery(
        "special-non-serializable",
        store.rawQueryMetadata,
        {
          select: ["id", "marker"],
          where: {
            marker: firstFunction,
          },
        },
      );
      const matchingFirstFunctionQuery = yield* prepareRawQuery(
        "special-non-serializable",
        store.rawQueryMetadata,
        {
          select: ["id", "marker"],
          where: {
            marker: firstFunction,
          },
        },
      );
      const secondFunctionQuery = yield* prepareRawQuery(
        "special-non-serializable",
        store.rawQueryMetadata,
        {
          select: ["id", "marker"],
          where: {
            marker: secondFunction,
          },
        },
      );
      const anonymousFunctionQuery = yield* prepareRawQuery(
        "special-non-serializable",
        store.rawQueryMetadata,
        {
          select: ["id", "marker"],
          where: {
            marker: anonymousFunction,
          },
        },
      );
      const firstSymbolQuery = yield* prepareRawQuery(
        "special-non-serializable",
        store.rawQueryMetadata,
        {
          select: ["id", "marker"],
          where: {
            marker: firstSymbol,
          },
        },
      );
      const secondSymbolQuery = yield* prepareRawQuery(
        "special-non-serializable",
        store.rawQueryMetadata,
        {
          select: ["id", "marker"],
          where: {
            marker: secondSymbol,
          },
        },
      );
      const firstMapQuery = yield* prepareRawQuery(
        "special-non-serializable",
        store.rawQueryMetadata,
        {
          select: ["id", "marker"],
          where: {
            marker: firstMap,
          },
        },
      );
      const secondMapQuery = yield* prepareRawQuery(
        "special-non-serializable",
        store.rawQueryMetadata,
        {
          select: ["id", "marker"],
          where: {
            marker: secondMap,
          },
        },
      );

      const firstFunctionExecution = yield* acquireRawQueryExecution(store, firstFunctionQuery);
      yield* acquireRawQueryExecution(store, matchingFirstFunctionQuery);
      yield* acquireRawQueryExecution(store, secondFunctionQuery);
      yield* acquireRawQueryExecution(store, anonymousFunctionQuery);
      yield* acquireRawQueryExecution(store, firstSymbolQuery);
      yield* acquireRawQueryExecution(store, secondSymbolQuery);
      yield* acquireRawQueryExecution(store, firstMapQuery);
      yield* acquireRawQueryExecution(store, secondMapQuery);

      const cursor = firstFunctionExecution.createCursor();

      expect(yield* activeStoreRawQueryExecutionCount(store)).toBe(7);
      expect(firstFunctionExecution.initial("query").totalRows).toBe(0);
      expect((yield* firstFunctionExecution.next("query", cursor))._tag).toBe("None");

      yield* releaseRawQueryExecution(store, firstFunctionQuery);
      yield* releaseRawQueryExecution(store, matchingFirstFunctionQuery);
      yield* releaseRawQueryExecution(store, secondFunctionQuery);
      yield* releaseRawQueryExecution(store, anonymousFunctionQuery);
      yield* releaseRawQueryExecution(store, firstSymbolQuery);
      yield* releaseRawQueryExecution(store, secondSymbolQuery);
      yield* releaseRawQueryExecution(store, firstMapQuery);
      yield* releaseRawQueryExecution(store, secondMapQuery);
    }),
  );
});
