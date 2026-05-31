import { fromStringUnsafe } from "effect/BigDecimal";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import {
  acquireRawQueryExecution,
  activeStoreRawQueryExecutionCount,
  releaseRawQueryExecution,
} from "./active-query";
import { prepareRawQuery } from "./raw-query-compiler";
import {
  publishTopicStoreRow,
  TopicStore,
  topicStoreRawQueryMetadata,
  topicStoreReadModel,
} from "./topic-store";

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

      const compiled = yield* prepareRawQuery("scores", topicStoreRawQueryMetadata(store), {
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

      const firstExecution = yield* acquireRawQueryExecution(topicStoreReadModel(store), compiled);
      const secondExecution = yield* acquireRawQueryExecution(topicStoreReadModel(store), compiled);
      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store))).toBe(1);

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

      yield* releaseRawQueryExecution(topicStoreReadModel(store), compiled);
      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store))).toBe(1);
      const afterRefcountDecrement = yield* acquireRawQueryExecution(
        topicStoreReadModel(store),
        compiled,
      );
      expect(afterRefcountDecrement.initial("query-c").totalRows).toBe(2);
      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store))).toBe(1);

      yield* releaseRawQueryExecution(topicStoreReadModel(store), compiled);
      yield* releaseRawQueryExecution(topicStoreReadModel(store), compiled);
      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store))).toBe(0);

      const afterRefcountExhausted = yield* acquireRawQueryExecution(
        topicStoreReadModel(store),
        compiled,
      );
      expect(afterRefcountExhausted.initial("query-d").totalRows).toBe(2);
      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store))).toBe(1);
      yield* releaseRawQueryExecution(topicStoreReadModel(store), compiled);
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

      const idOnly = yield* prepareRawQuery(
        "projection-sharing",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id"],
          where: {
            status: "open",
          },
          orderBy: [{ field: "score", direction: "desc" }],
        },
      );
      const idAndScore = yield* prepareRawQuery(
        "projection-sharing",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id", "score"],
          where: {
            status: "open",
          },
          orderBy: [{ field: "score", direction: "desc" }],
        },
      );

      const idOnlyExecution = yield* acquireRawQueryExecution(topicStoreReadModel(store), idOnly);
      const idAndScoreExecution = yield* acquireRawQueryExecution(
        topicStoreReadModel(store),
        idAndScore,
      );

      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store))).toBe(1);
      expect(idOnlyExecution.initial("ids").rows).toStrictEqual([{ id: "b" }, { id: "a" }]);
      expect(idAndScoreExecution.initial("scores").rows).toStrictEqual([
        { id: "b", score: 2 },
        { id: "a", score: 1 },
      ]);

      yield* releaseRawQueryExecution(topicStoreReadModel(store), idOnly);
      yield* releaseRawQueryExecution(topicStoreReadModel(store), idAndScore);
      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store))).toBe(0);
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
      const compiled = yield* prepareRawQuery("numbers", topicStoreRawQueryMetadata(store), {
        select: ["id"],
        where: {
          score: 1,
        },
      });

      yield* releaseRawQueryExecution(topicStoreReadModel(store), compiled);
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

      const infFilter = yield* prepareRawQuery(
        "numbers",
        topicStoreRawQueryMetadata(numericStore),
        {
          select: ["id", "score"],
          where: {
            score: Number.POSITIVE_INFINITY,
          },
        },
      );
      const nanFilter = yield* prepareRawQuery(
        "numbers",
        topicStoreRawQueryMetadata(numericStore),
        {
          select: ["id", "score"],
          where: {
            score: Number.NaN,
          },
        },
      );
      const zeroFilter = yield* prepareRawQuery(
        "numbers",
        topicStoreRawQueryMetadata(numericStore),
        {
          select: ["id", "score"],
          where: {
            score: 10,
          },
        },
      );
      const positiveZeroFilter = yield* prepareRawQuery(
        "numbers",
        topicStoreRawQueryMetadata(numericStore),
        {
          select: ["id", "score"],
          where: {
            score: 0,
          },
        },
      );
      const negativeZeroFilter = yield* prepareRawQuery(
        "numbers",
        topicStoreRawQueryMetadata(numericStore),
        {
          select: ["id", "score"],
          where: {
            score: -0,
          },
        },
      );
      const offsetFilter = yield* prepareRawQuery(
        "numbers",
        topicStoreRawQueryMetadata(numericStore),
        {
          select: ["id", "score"],
          offset: 1,
        },
      );
      const bigIntFilter = yield* prepareRawQuery(
        "bigints",
        topicStoreRawQueryMetadata(bigintStore),
        {
          select: ["id", "amount"],
          where: {
            amount: 3n,
          },
        },
      );
      const decimalFilter = yield* prepareRawQuery(
        "decimals",
        topicStoreRawQueryMetadata(decimalStore),
        {
          select: ["id", "price"],
          where: {
            price: fromStringUnsafe("1.23"),
          },
        },
      );

      const infExecution = yield* acquireRawQueryExecution(
        topicStoreReadModel(numericStore),
        infFilter,
      );
      const nanExecution = yield* acquireRawQueryExecution(
        topicStoreReadModel(numericStore),
        nanFilter,
      );
      const zeroExecution = yield* acquireRawQueryExecution(
        topicStoreReadModel(numericStore),
        zeroFilter,
      );
      yield* acquireRawQueryExecution(topicStoreReadModel(numericStore), positiveZeroFilter);
      yield* acquireRawQueryExecution(topicStoreReadModel(numericStore), negativeZeroFilter);
      const offsetExecution = yield* acquireRawQueryExecution(
        topicStoreReadModel(numericStore),
        offsetFilter,
      );
      const bigintExecution = yield* acquireRawQueryExecution(
        topicStoreReadModel(bigintStore),
        bigIntFilter,
      );
      const decimalExecution = yield* acquireRawQueryExecution(
        topicStoreReadModel(decimalStore),
        decimalFilter,
      );

      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(numericStore))).toBe(6);
      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(bigintStore))).toBe(1);
      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(decimalStore))).toBe(1);

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

      yield* releaseRawQueryExecution(topicStoreReadModel(numericStore), infFilter);
      yield* releaseRawQueryExecution(topicStoreReadModel(numericStore), nanFilter);
      yield* releaseRawQueryExecution(topicStoreReadModel(numericStore), zeroFilter);
      yield* releaseRawQueryExecution(topicStoreReadModel(numericStore), positiveZeroFilter);
      yield* releaseRawQueryExecution(topicStoreReadModel(numericStore), negativeZeroFilter);
      yield* releaseRawQueryExecution(topicStoreReadModel(numericStore), offsetFilter);
      yield* releaseRawQueryExecution(topicStoreReadModel(bigintStore), bigIntFilter);
      yield* releaseRawQueryExecution(topicStoreReadModel(decimalStore), decimalFilter);
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

      const undefinedFilter = yield* prepareRawQuery(
        "events",
        topicStoreRawQueryMetadata(eventStore),
        {
          select: ["id", "label"],
          where: {
            label: undefined,
          },
        },
      );
      const nullFilter = yield* prepareRawQuery("events", topicStoreRawQueryMetadata(eventStore), {
        select: ["id", "label"],
        where: {
          label: null,
        },
      });
      const arrayFilter = yield* prepareRawQuery("events", topicStoreRawQueryMetadata(eventStore), {
        select: ["id", "tags"],
        where: {
          tags: ["open", "closed"],
        },
      });
      const objectFilter = yield* prepareRawQuery(
        "events",
        topicStoreRawQueryMetadata(eventStore),
        {
          select: ["id", "metadata"],
          where: {
            metadata: { kind: "test", scope: "global" },
          },
        },
      );
      const booleanFilter = yield* prepareRawQuery(
        "events",
        topicStoreRawQueryMetadata(eventStore),
        {
          select: ["id", "active"],
          where: {
            active: true,
          },
        },
      );

      const undefinedExecution = yield* acquireRawQueryExecution(
        topicStoreReadModel(eventStore),
        undefinedFilter,
      );
      const nullExecution = yield* acquireRawQueryExecution(
        topicStoreReadModel(eventStore),
        nullFilter,
      );
      const arrayExecution = yield* acquireRawQueryExecution(
        topicStoreReadModel(eventStore),
        arrayFilter,
      );
      const objectExecution = yield* acquireRawQueryExecution(
        topicStoreReadModel(eventStore),
        objectFilter,
      );
      const booleanExecution = yield* acquireRawQueryExecution(
        topicStoreReadModel(eventStore),
        booleanFilter,
      );

      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(eventStore))).toBe(5);

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

      yield* releaseRawQueryExecution(topicStoreReadModel(eventStore), undefinedFilter);
      yield* releaseRawQueryExecution(topicStoreReadModel(eventStore), nullFilter);
      yield* releaseRawQueryExecution(topicStoreReadModel(eventStore), arrayFilter);
      yield* releaseRawQueryExecution(topicStoreReadModel(eventStore), objectFilter);
      yield* releaseRawQueryExecution(topicStoreReadModel(eventStore), booleanFilter);
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
      const splitFieldsQuery = yield* prepareRawQuery(
        "delimiter-fields",
        topicStoreRawQueryMetadata(store),
        {
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
        },
      );
      const delimiterFieldQuery = yield* prepareRawQuery(
        "delimiter-fields",
        topicStoreRawQueryMetadata(store),
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

      yield* acquireRawQueryExecution(topicStoreReadModel(store), splitFieldsQuery);
      yield* acquireRawQueryExecution(topicStoreReadModel(store), delimiterFieldQuery);

      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store))).toBe(2);

      yield* releaseRawQueryExecution(topicStoreReadModel(store), splitFieldsQuery);
      yield* releaseRawQueryExecution(topicStoreReadModel(store), delimiterFieldQuery);
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

      const compiled = yield* prepareRawQuery("special", topicStoreRawQueryMetadata(store), {
        select: ["id", "payload"],
        where: {
          payload: queryPayload,
        },
      });

      const execution = yield* acquireRawQueryExecution(topicStoreReadModel(store), compiled);
      const cursor = execution.createCursor();

      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store))).toBe(1);
      expect(execution.initial("query").rows).toStrictEqual([]);
      expect((yield* execution.next("query", cursor))._tag).toBe("None");

      yield* releaseRawQueryExecution(topicStoreReadModel(store), compiled);
      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store))).toBe(0);
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
        topicStoreRawQueryMetadata(store),
        {
          select: ["id", "marker"],
          where: {
            marker: firstFunction,
          },
        },
      );
      const matchingFirstFunctionQuery = yield* prepareRawQuery(
        "special-non-serializable",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id", "marker"],
          where: {
            marker: firstFunction,
          },
        },
      );
      const secondFunctionQuery = yield* prepareRawQuery(
        "special-non-serializable",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id", "marker"],
          where: {
            marker: secondFunction,
          },
        },
      );
      const anonymousFunctionQuery = yield* prepareRawQuery(
        "special-non-serializable",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id", "marker"],
          where: {
            marker: anonymousFunction,
          },
        },
      );
      const firstSymbolQuery = yield* prepareRawQuery(
        "special-non-serializable",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id", "marker"],
          where: {
            marker: firstSymbol,
          },
        },
      );
      const secondSymbolQuery = yield* prepareRawQuery(
        "special-non-serializable",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id", "marker"],
          where: {
            marker: secondSymbol,
          },
        },
      );
      const firstMapQuery = yield* prepareRawQuery(
        "special-non-serializable",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id", "marker"],
          where: {
            marker: firstMap,
          },
        },
      );
      const secondMapQuery = yield* prepareRawQuery(
        "special-non-serializable",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id", "marker"],
          where: {
            marker: secondMap,
          },
        },
      );

      const firstFunctionExecution = yield* acquireRawQueryExecution(
        topicStoreReadModel(store),
        firstFunctionQuery,
      );
      yield* acquireRawQueryExecution(topicStoreReadModel(store), matchingFirstFunctionQuery);
      yield* acquireRawQueryExecution(topicStoreReadModel(store), secondFunctionQuery);
      yield* acquireRawQueryExecution(topicStoreReadModel(store), anonymousFunctionQuery);
      yield* acquireRawQueryExecution(topicStoreReadModel(store), firstSymbolQuery);
      yield* acquireRawQueryExecution(topicStoreReadModel(store), secondSymbolQuery);
      yield* acquireRawQueryExecution(topicStoreReadModel(store), firstMapQuery);
      yield* acquireRawQueryExecution(topicStoreReadModel(store), secondMapQuery);

      const cursor = firstFunctionExecution.createCursor();

      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store))).toBe(7);
      expect(firstFunctionExecution.initial("query").totalRows).toBe(0);
      expect((yield* firstFunctionExecution.next("query", cursor))._tag).toBe("None");

      yield* releaseRawQueryExecution(topicStoreReadModel(store), firstFunctionQuery);
      yield* releaseRawQueryExecution(topicStoreReadModel(store), matchingFirstFunctionQuery);
      yield* releaseRawQueryExecution(topicStoreReadModel(store), secondFunctionQuery);
      yield* releaseRawQueryExecution(topicStoreReadModel(store), anonymousFunctionQuery);
      yield* releaseRawQueryExecution(topicStoreReadModel(store), firstSymbolQuery);
      yield* releaseRawQueryExecution(topicStoreReadModel(store), secondSymbolQuery);
      yield* releaseRawQueryExecution(topicStoreReadModel(store), firstMapQuery);
      yield* releaseRawQueryExecution(topicStoreReadModel(store), secondMapQuery);
    }),
  );
});
