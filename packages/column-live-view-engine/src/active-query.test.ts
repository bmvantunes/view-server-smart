import { fromStringUnsafe } from "effect/BigDecimal";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Option, Schema } from "effect";
import {
  acquireMaterializedQueryExecution,
  acquireRawQueryExecution,
  activeStoreRawQueryExecutionCount,
  createActiveQueryRegistry,
  releaseMaterializedQueryExecution,
  releaseRawQueryExecution,
} from "./active-query";
import { prepareRawQuery } from "./raw-query-compiler";
import {
  deleteTopicStoreRow,
  patchTopicStoreRow,
  publishTopicStoreRow,
  publishTopicStoreRows,
  resetTopicStore,
  TopicStore,
} from "./topic-store";
import { topicStoreRawQueryMetadata, topicStoreReadModel } from "./topic-store-state";

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

  it.effect("keeps execution caches local to the active query registry", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "registry-isolation",
        Schema.Struct({
          id: Schema.String,
          score: Schema.Number,
        }),
        "id",
        () => {},
      );
      yield* publishTopicStoreRow(store, { id: "a", score: 1 }, invalidRow);

      const compiled = yield* prepareRawQuery(
        "registry-isolation",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id"],
          orderBy: [{ field: "score", direction: "desc" }],
        },
      );
      const readModel = topicStoreReadModel(store);
      const isolatedReadModel = {
        ...readModel,
        activeQueries: createActiveQueryRegistry(),
      };

      yield* acquireRawQueryExecution(readModel, compiled);
      yield* acquireRawQueryExecution(isolatedReadModel, compiled);

      expect(yield* activeStoreRawQueryExecutionCount(readModel)).toBe(1);
      expect(yield* activeStoreRawQueryExecutionCount(isolatedReadModel)).toBe(1);

      yield* releaseRawQueryExecution(readModel, compiled);
      expect(yield* activeStoreRawQueryExecutionCount(readModel)).toBe(0);
      expect(yield* activeStoreRawQueryExecutionCount(isolatedReadModel)).toBe(1);

      yield* releaseRawQueryExecution(isolatedReadModel, compiled);
      expect(yield* activeStoreRawQueryExecutionCount(isolatedReadModel)).toBe(0);
    }),
  );

  it.effect("keeps materialized execution caches local to the active query registry", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "materialized-registry-isolation",
        Schema.Struct({
          id: Schema.String,
        }),
        "id",
        () => {},
      );
      const readModel = topicStoreReadModel(store);
      const isolatedReadModel = {
        ...readModel,
        activeQueries: createActiveQueryRegistry(),
      };

      const emptyEvaluation = (version: number) => ({
        rows: [],
        keys: [],
        window: [],
        totalRows: 0,
        version,
      });

      yield* acquireMaterializedQueryExecution(readModel, "grouped", () => ({
        incremental: false,
        latest: () => emptyEvaluation(readModel.version()),
      }));
      yield* acquireMaterializedQueryExecution(isolatedReadModel, "grouped", () => ({
        incremental: false,
        latest: () => emptyEvaluation(isolatedReadModel.version()),
      }));

      expect(yield* activeStoreRawQueryExecutionCount(readModel)).toBe(1);
      expect(yield* activeStoreRawQueryExecutionCount(isolatedReadModel)).toBe(1);

      yield* releaseMaterializedQueryExecution(readModel, "grouped");
      expect(yield* activeStoreRawQueryExecutionCount(readModel)).toBe(0);
      expect(yield* activeStoreRawQueryExecutionCount(isolatedReadModel)).toBe(1);

      yield* releaseMaterializedQueryExecution(isolatedReadModel, "grouped");
      expect(yield* activeStoreRawQueryExecutionCount(isolatedReadModel)).toBe(0);
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

  it.effect("shares base evaluation across different windows", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "window-sharing",
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
      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 3 }, invalidRow);

      const firstWindow = yield* prepareRawQuery(
        "window-sharing",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id", "score"],
          where: {
            status: "open",
          },
          orderBy: [{ field: "score", direction: "desc" }],
          offset: 0,
          limit: 1,
        },
      );
      const secondWindow = yield* prepareRawQuery(
        "window-sharing",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id"],
          where: {
            status: "open",
          },
          orderBy: [{ field: "score", direction: "desc" }],
          offset: 1,
          limit: 1,
        },
      );

      const firstExecution = yield* acquireRawQueryExecution(
        topicStoreReadModel(store),
        firstWindow,
      );
      expect(firstExecution.initial("first").rows).toStrictEqual([{ id: "c", score: 3 }]);

      yield* releaseRawQueryExecution(topicStoreReadModel(store), secondWindow);
      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store))).toBe(1);
      expect(firstExecution.initial("first-after-unknown-release").rows).toStrictEqual([
        { id: "c", score: 3 },
      ]);

      const secondExecution = yield* acquireRawQueryExecution(
        topicStoreReadModel(store),
        secondWindow,
      );
      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store))).toBe(1);
      expect(secondExecution.initial("second").rows).toStrictEqual([{ id: "b" }]);

      const firstCursor = firstExecution.createCursor();
      const secondCursor = secondExecution.createCursor();

      yield* publishTopicStoreRow(store, { id: "d", status: "open", score: 4 }, invalidRow);

      const firstDelta = yield* firstExecution.next("first", firstCursor);
      const secondDelta = yield* secondExecution.next("second", secondCursor);
      expect(Option.getOrThrow(firstDelta)).toStrictEqual({
        type: "delta",
        topic: "window-sharing",
        queryId: "first",
        fromVersion: 3,
        toVersion: 4,
        operations: [
          {
            type: "remove",
            key: "c",
          },
          {
            type: "insert",
            key: "d",
            row: {
              id: "d",
              score: 4,
            },
            index: 0,
          },
        ],
        totalRows: 4,
      });
      expect(Option.getOrThrow(secondDelta)).toStrictEqual({
        type: "delta",
        topic: "window-sharing",
        queryId: "second",
        fromVersion: 3,
        toVersion: 4,
        operations: [
          {
            type: "remove",
            key: "b",
          },
          {
            type: "insert",
            key: "c",
            row: {
              id: "c",
            },
            index: 0,
          },
        ],
        totalRows: 4,
      });

      yield* releaseRawQueryExecution(topicStoreReadModel(store), firstWindow);
      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store))).toBe(1);
      expect(secondExecution.initial("second-after-release").rows).toStrictEqual([{ id: "c" }]);

      yield* releaseRawQueryExecution(topicStoreReadModel(store), secondWindow);
      expect(yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store))).toBe(0);

      yield* releaseRawQueryExecution(topicStoreReadModel(store), secondWindow);
    }),
  );

  it.effect("updates raw active windows from retained insert-only changes without rescanning", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-insert-incremental",
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
      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 3 }, invalidRow);

      const scanLimits: Array<number | undefined> = [];
      const readModel = topicStoreReadModel(store);
      const observedReadModel = {
        ...readModel,
        scanRawWindow: (plan: Parameters<typeof readModel.scanRawWindow>[0]) => {
          scanLimits.push(plan.limit);
          return readModel.scanRawWindow(plan);
        },
      };

      const compiled = yield* prepareRawQuery(
        "raw-insert-incremental",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id", "score"],
          where: {
            status: "open",
          },
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedReadModel, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["c", "b"]);
      const cursor = execution.createCursor();

      yield* publishTopicStoreRow(store, { id: "d", status: "open", score: 4 }, invalidRow);

      const delta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(delta)).toStrictEqual({
        type: "delta",
        topic: "raw-insert-incremental",
        queryId: "query",
        fromVersion: 3,
        toVersion: 4,
        operations: [
          {
            type: "remove",
            key: "b",
          },
          {
            type: "insert",
            key: "d",
            row: {
              id: "d",
              score: 4,
            },
            index: 0,
          },
        ],
        totalRows: 4,
      });
      expect(scanLimits).toStrictEqual([2]);

      yield* releaseRawQueryExecution(observedReadModel, compiled);
      expect(readModel.changesSince(3)).toBeUndefined();
    }),
  );

  it.effect("updates total rows for lower-ranked insert-only raw changes without rescanning", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-insert-incremental-count-only",
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
      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 3 }, invalidRow);

      const scanLimits: Array<number | undefined> = [];
      const readModel = topicStoreReadModel(store);
      const observedReadModel = {
        ...readModel,
        scanRawWindow: (plan: Parameters<typeof readModel.scanRawWindow>[0]) => {
          scanLimits.push(plan.limit);
          return readModel.scanRawWindow(plan);
        },
      };

      const compiled = yield* prepareRawQuery(
        "raw-insert-incremental-count-only",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id", "score"],
          where: {
            status: "open",
          },
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedReadModel, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["c", "b"]);
      const cursor = execution.createCursor();

      yield* publishTopicStoreRow(store, { id: "d", status: "open", score: 0 }, invalidRow);

      const delta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(delta)).toStrictEqual({
        type: "delta",
        topic: "raw-insert-incremental-count-only",
        queryId: "query",
        fromVersion: 3,
        toVersion: 4,
        operations: [],
        totalRows: 4,
      });
      expect(scanLimits).toStrictEqual([2]);

      yield* releaseRawQueryExecution(observedReadModel, compiled);
    }),
  );

  it.effect("releases retained raw changes when a topic reset clears active queries", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-insert-reset-release",
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
      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 3 }, invalidRow);

      const readModel = topicStoreReadModel(store);
      const compiled = yield* prepareRawQuery(
        "raw-insert-reset-release",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id"],
          where: {
            status: "open",
          },
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      yield* acquireRawQueryExecution(readModel, compiled);
      yield* publishTopicStoreRow(store, { id: "d", status: "open", score: 4 }, invalidRow);
      expect(readModel.changesSince(3)).toBeDefined();

      yield* resetTopicStore(store);
      expect(yield* activeStoreRawQueryExecutionCount(readModel)).toBe(0);

      yield* publishTopicStoreRow(store, { id: "e", status: "open", score: 5 }, invalidRow);
      expect(readModel.changesSince(0)).toBeUndefined();
    }),
  );

  it.effect("falls back to a raw window scan when retained changes replace rows", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-replacement-fallback",
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
      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 3 }, invalidRow);

      const scanLimits: Array<number | undefined> = [];
      const readModel = topicStoreReadModel(store);
      const observedReadModel = {
        ...readModel,
        scanRawWindow: (plan: Parameters<typeof readModel.scanRawWindow>[0]) => {
          scanLimits.push(plan.limit);
          return readModel.scanRawWindow(plan);
        },
      };

      const compiled = yield* prepareRawQuery(
        "raw-replacement-fallback",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id", "score"],
          where: {
            status: "open",
          },
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedReadModel, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["c", "b"]);
      const cursor = execution.createCursor();

      yield* publishTopicStoreRow(store, { id: "b", status: "open", score: 5 }, invalidRow);

      const delta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(delta)).toStrictEqual({
        type: "delta",
        topic: "raw-replacement-fallback",
        queryId: "query",
        fromVersion: 3,
        toVersion: 4,
        operations: [
          {
            type: "move",
            key: "b",
            fromIndex: 1,
            toIndex: 0,
          },
          {
            type: "update",
            key: "b",
            row: {
              id: "b",
              score: 5,
            },
            index: 0,
          },
        ],
        totalRows: 3,
      });
      expect(scanLimits).toStrictEqual([2, 2]);

      yield* releaseRawQueryExecution(observedReadModel, compiled);
    }),
  );

  it.effect("falls back to a raw window scan when retained raw changes are unavailable", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-unavailable-changes-fallback",
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

      const scanLimits: Array<number | undefined> = [];
      const readModel = topicStoreReadModel(store);
      const observedReadModel = {
        ...readModel,
        changesSince: () => undefined,
        scanRawWindow: (plan: Parameters<typeof readModel.scanRawWindow>[0]) => {
          scanLimits.push(plan.limit);
          return readModel.scanRawWindow(plan);
        },
      };

      const compiled = yield* prepareRawQuery(
        "raw-unavailable-changes-fallback",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id", "score"],
          where: {
            status: "open",
          },
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedReadModel, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["b", "a"]);
      const cursor = execution.createCursor();

      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 3 }, invalidRow);

      const delta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(delta).operations).toStrictEqual([
        {
          type: "remove",
          key: "a",
        },
        {
          type: "insert",
          key: "c",
          row: {
            id: "c",
            score: 3,
          },
          index: 0,
        },
      ]);
      expect(scanLimits).toStrictEqual([2, 2]);

      yield* releaseRawQueryExecution(observedReadModel, compiled);
    }),
  );

  it.effect("ignores non-matching insert-only raw changes without rescanning", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-mixed-insert-incremental",
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

      const scanLimits: Array<number | undefined> = [];
      const readModel = topicStoreReadModel(store);
      const observedReadModel = {
        ...readModel,
        scanRawWindow: (plan: Parameters<typeof readModel.scanRawWindow>[0]) => {
          scanLimits.push(plan.limit);
          return readModel.scanRawWindow(plan);
        },
      };

      const compiled = yield* prepareRawQuery(
        "raw-mixed-insert-incremental",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id", "score"],
          where: {
            status: "open",
          },
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedReadModel, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["b", "a"]);
      const cursor = execution.createCursor();

      yield* publishTopicStoreRows(
        store,
        [
          { id: "closed", status: "closed", score: 10 },
          { id: "c", status: "open", score: 3 },
        ],
        invalidRow,
      );

      const delta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(delta).operations).toStrictEqual([
        {
          type: "remove",
          key: "a",
        },
        {
          type: "insert",
          key: "c",
          row: {
            id: "c",
            score: 3,
          },
          index: 0,
        },
      ]);
      expect(scanLimits).toStrictEqual([2]);

      yield* releaseRawQueryExecution(observedReadModel, compiled);
    }),
  );

  it.effect(
    "updates zero-limit raw active queries from insert-only changes without rescanning",
    () =>
      Effect.gen(function* () {
        const store = new TopicStore(
          "raw-zero-limit-incremental",
          Schema.Struct({
            id: Schema.String,
            status: Schema.String,
            score: Schema.Number,
          }),
          "id",
          () => {},
        );
        yield* publishTopicStoreRow(store, { id: "a", status: "open", score: 1 }, invalidRow);

        const scanLimits: Array<number | undefined> = [];
        const readModel = topicStoreReadModel(store);
        const observedReadModel = {
          ...readModel,
          scanRawWindow: (plan: Parameters<typeof readModel.scanRawWindow>[0]) => {
            scanLimits.push(plan.limit);
            return readModel.scanRawWindow(plan);
          },
        };

        const compiled = yield* prepareRawQuery(
          "raw-zero-limit-incremental",
          topicStoreRawQueryMetadata(store),
          {
            select: ["id"],
            where: {
              status: "open",
            },
            limit: 0,
          },
        );

        const execution = yield* acquireRawQueryExecution(observedReadModel, compiled);
        expect(execution.initial("query").totalRows).toBe(1);
        const cursor = execution.createCursor();

        yield* publishTopicStoreRow(store, { id: "b", status: "open", score: 2 }, invalidRow);

        const delta = yield* execution.next("query", cursor);
        expect(Option.getOrThrow(delta)).toStrictEqual({
          type: "delta",
          topic: "raw-zero-limit-incremental",
          queryId: "query",
          fromVersion: 1,
          toVersion: 2,
          operations: [],
          totalRows: 2,
        });
        expect(scanLimits).toStrictEqual([0]);

        yield* releaseRawQueryExecution(observedReadModel, compiled);
      }),
  );

  it.effect("ignores non-matching retained raw updates and deletes without rescanning", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-non-matching-change-incremental",
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
      yield* publishTopicStoreRow(
        store,
        { id: "closed-update", status: "closed", score: 3 },
        invalidRow,
      );
      yield* publishTopicStoreRow(
        store,
        { id: "closed-delete", status: "closed", score: 4 },
        invalidRow,
      );

      const scanLimits: Array<number | undefined> = [];
      const readModel = topicStoreReadModel(store);
      const observedReadModel = {
        ...readModel,
        scanRawWindow: (plan: Parameters<typeof readModel.scanRawWindow>[0]) => {
          scanLimits.push(plan.limit);
          return readModel.scanRawWindow(plan);
        },
      };

      const compiled = yield* prepareRawQuery(
        "raw-non-matching-change-incremental",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id", "score"],
          where: {
            status: "open",
          },
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );
      let compareCount = 0;
      const observedCompiled = {
        ...compiled,
        plan: {
          ...compiled.plan,
          compare: (
            left: Parameters<typeof compiled.plan.compare>[0],
            right: Parameters<typeof compiled.plan.compare>[1],
          ) => {
            compareCount += 1;
            return compiled.plan.compare(left, right);
          },
        },
      };

      const execution = yield* acquireRawQueryExecution(observedReadModel, observedCompiled);
      expect(execution.initial("query").keys).toStrictEqual(["b", "a"]);
      const cursor = execution.createCursor();
      compareCount = 0;

      yield* patchTopicStoreRow(
        store,
        "closed-update",
        {
          score: 30,
        },
        invalidRow,
      );
      yield* deleteTopicStoreRow(store, "closed-delete");

      const delta = yield* execution.next("query", cursor);
      expect(Option.isNone(delta)).toBe(true);
      expect(scanLimits).toStrictEqual([2]);
      expect(compareCount).toBe(0);

      yield* releaseRawQueryExecution(observedReadModel, observedCompiled);
    }),
  );

  it.effect(
    "emits the next visible raw delta from the last delivered version after no-op changes",
    () =>
      Effect.gen(function* () {
        const store = new TopicStore(
          "raw-noop-then-visible-version",
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
        yield* publishTopicStoreRow(
          store,
          { id: "closed", status: "closed", score: 3 },
          invalidRow,
        );

        const readModel = topicStoreReadModel(store);
        const compiled = yield* prepareRawQuery(
          "raw-noop-then-visible-version",
          topicStoreRawQueryMetadata(store),
          {
            select: ["id", "score"],
            where: {
              status: "open",
            },
            orderBy: [{ field: "score", direction: "desc" }],
            limit: 2,
          },
        );

        const execution = yield* acquireRawQueryExecution(readModel, compiled);
        expect(execution.initial("query").keys).toStrictEqual(["b", "a"]);
        const cursor = execution.createCursor();

        yield* patchTopicStoreRow(
          store,
          "closed",
          {
            score: 30,
          },
          invalidRow,
        );

        const noOpDelta = yield* execution.next("query", cursor);
        expect(Option.isNone(noOpDelta)).toBe(true);

        yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 4 }, invalidRow);

        const visibleDelta = yield* execution.next("query", cursor);
        expect(Option.getOrThrow(visibleDelta)).toStrictEqual({
          type: "delta",
          topic: "raw-noop-then-visible-version",
          queryId: "query",
          fromVersion: 3,
          toVersion: 5,
          operations: [
            {
              type: "remove",
              key: "a",
            },
            {
              type: "insert",
              key: "c",
              row: {
                id: "c",
                score: 4,
              },
              index: 0,
            },
          ],
          totalRows: 3,
        });

        yield* releaseRawQueryExecution(readModel, compiled);
      }),
  );

  it.effect(
    "updates zero-limit raw active counts from retained updates and deletes without rescanning",
    () =>
      Effect.gen(function* () {
        const store = new TopicStore(
          "raw-zero-limit-mixed-incremental",
          Schema.Struct({
            id: Schema.String,
            status: Schema.String,
            score: Schema.Number,
          }),
          "id",
          () => {},
        );
        yield* publishTopicStoreRow(store, { id: "a", status: "open", score: 1 }, invalidRow);
        yield* publishTopicStoreRow(store, { id: "b", status: "closed", score: 2 }, invalidRow);
        yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 3 }, invalidRow);
        yield* publishTopicStoreRow(store, { id: "d", status: "closed", score: 4 }, invalidRow);

        const scanLimits: Array<number | undefined> = [];
        const readModel = topicStoreReadModel(store);
        const observedReadModel = {
          ...readModel,
          scanRawWindow: (plan: Parameters<typeof readModel.scanRawWindow>[0]) => {
            scanLimits.push(plan.limit);
            return readModel.scanRawWindow(plan);
          },
        };

        const compiled = yield* prepareRawQuery(
          "raw-zero-limit-mixed-incremental",
          topicStoreRawQueryMetadata(store),
          {
            select: ["id"],
            where: {
              status: "open",
            },
            limit: 0,
          },
        );

        const execution = yield* acquireRawQueryExecution(observedReadModel, compiled);
        expect(execution.initial("query").totalRows).toBe(2);
        const cursor = execution.createCursor();

        yield* publishTopicStoreRow(store, { id: "b", status: "open", score: 2 }, invalidRow);
        yield* publishTopicStoreRow(store, { id: "c", status: "closed", score: 3 }, invalidRow);
        yield* patchTopicStoreRow(
          store,
          "d",
          {
            score: 40,
          },
          invalidRow,
        );
        yield* deleteTopicStoreRow(store, "a");

        const delta = yield* execution.next("query", cursor);
        expect(Option.getOrThrow(delta)).toStrictEqual({
          type: "delta",
          topic: "raw-zero-limit-mixed-incremental",
          queryId: "query",
          fromVersion: 4,
          toVersion: 8,
          operations: [],
          totalRows: 1,
        });
        expect(scanLimits).toStrictEqual([0]);

        yield* releaseRawQueryExecution(observedReadModel, compiled);
      }),
  );

  it.effect("falls back to a raw window scan when retained deletes remove matching rows", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-visible-delete-fallback",
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
      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 3 }, invalidRow);

      const scanLimits: Array<number | undefined> = [];
      const readModel = topicStoreReadModel(store);
      const observedReadModel = {
        ...readModel,
        scanRawWindow: (plan: Parameters<typeof readModel.scanRawWindow>[0]) => {
          scanLimits.push(plan.limit);
          return readModel.scanRawWindow(plan);
        },
      };

      const compiled = yield* prepareRawQuery(
        "raw-visible-delete-fallback",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id", "score"],
          where: {
            status: "open",
          },
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      const execution = yield* acquireRawQueryExecution(observedReadModel, compiled);
      expect(execution.initial("query").keys).toStrictEqual(["c", "b"]);
      const cursor = execution.createCursor();

      yield* deleteTopicStoreRow(store, "c");

      const delta = yield* execution.next("query", cursor);
      expect(Option.getOrThrow(delta)).toStrictEqual({
        type: "delta",
        topic: "raw-visible-delete-fallback",
        queryId: "query",
        fromVersion: 3,
        toVersion: 4,
        operations: [
          {
            type: "remove",
            key: "c",
          },
          {
            type: "insert",
            key: "a",
            row: {
              id: "a",
              score: 1,
            },
            index: 1,
          },
        ],
        totalRows: 2,
      });
      expect(scanLimits).toStrictEqual([2, 2]);

      yield* releaseRawQueryExecution(observedReadModel, compiled);
    }),
  );

  it.effect("shrinks shared base windows immediately when larger windows release", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "window-shrink",
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
      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 3 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "d", status: "open", score: 4 }, invalidRow);

      const scanLimits: Array<number | undefined> = [];
      const readModel = topicStoreReadModel(store);
      const observedReadModel = {
        ...readModel,
        scanRawWindow: (plan: Parameters<typeof readModel.scanRawWindow>[0]) => {
          scanLimits.push(plan.limit);
          return readModel.scanRawWindow(plan);
        },
      };

      const wideWindow = yield* prepareRawQuery(
        "window-shrink",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id"],
          where: {
            status: "open",
          },
          orderBy: [{ field: "score", direction: "desc" }],
          offset: 0,
          limit: 3,
        },
      );
      const narrowWindow = yield* prepareRawQuery(
        "window-shrink",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id"],
          where: {
            status: "open",
          },
          orderBy: [{ field: "score", direction: "desc" }],
          offset: 1,
          limit: 1,
        },
      );

      const wideExecution = yield* acquireRawQueryExecution(observedReadModel, wideWindow);
      expect(wideExecution.initial("wide").keys).toStrictEqual(["d", "c", "b"]);

      const narrowExecution = yield* acquireRawQueryExecution(observedReadModel, narrowWindow);
      expect(narrowExecution.initial("narrow").keys).toStrictEqual(["c"]);

      yield* releaseRawQueryExecution(observedReadModel, wideWindow);
      expect(scanLimits).toStrictEqual([3, 2]);
      expect(narrowExecution.initial("narrow-after-shrink").keys).toStrictEqual(["c"]);

      yield* releaseRawQueryExecution(observedReadModel, narrowWindow);
    }),
  );

  it.effect("compacts unbounded shared base windows when unbounded windows release", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "window-unbounded",
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
      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 3 }, invalidRow);

      const scanLimits: Array<number | undefined> = [];
      const readModel = topicStoreReadModel(store);
      const observedReadModel = {
        ...readModel,
        scanRawWindow: (plan: Parameters<typeof readModel.scanRawWindow>[0]) => {
          scanLimits.push(plan.limit);
          return readModel.scanRawWindow(plan);
        },
      };

      const boundedWindow = yield* prepareRawQuery(
        "window-unbounded",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id"],
          where: {
            status: "open",
          },
          orderBy: [{ field: "score", direction: "desc" }],
          offset: 1,
          limit: 1,
        },
      );
      const unboundedWindow = yield* prepareRawQuery(
        "window-unbounded",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id"],
          where: {
            status: "open",
          },
          orderBy: [{ field: "score", direction: "desc" }],
        },
      );

      const boundedExecution = yield* acquireRawQueryExecution(observedReadModel, boundedWindow);
      expect(boundedExecution.initial("bounded").keys).toStrictEqual(["b"]);

      const unboundedExecution = yield* acquireRawQueryExecution(
        observedReadModel,
        unboundedWindow,
      );
      expect(unboundedExecution.initial("unbounded").keys).toStrictEqual(["c", "b", "a"]);

      yield* releaseRawQueryExecution(observedReadModel, unboundedWindow);
      expect(scanLimits).toStrictEqual([2, undefined, 2]);
      expect(boundedExecution.initial("bounded-after-compact").keys).toStrictEqual(["b"]);

      yield* releaseRawQueryExecution(observedReadModel, boundedWindow);
    }),
  );

  it.effect("does not expand shared base rows for zero-limit windows", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "window-zero-limit",
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

      const scanLimits: Array<number | undefined> = [];
      const readModel = topicStoreReadModel(store);
      const observedReadModel = {
        ...readModel,
        scanRawWindow: (plan: Parameters<typeof readModel.scanRawWindow>[0]) => {
          scanLimits.push(plan.limit);
          return readModel.scanRawWindow(plan);
        },
      };

      const zeroWindow = yield* prepareRawQuery(
        "window-zero-limit",
        topicStoreRawQueryMetadata(store),
        {
          select: ["id"],
          where: {
            status: "open",
          },
          orderBy: [{ field: "score", direction: "desc" }],
          offset: 10,
          limit: 0,
        },
      );

      const zeroExecution = yield* acquireRawQueryExecution(observedReadModel, zeroWindow);
      const initial = zeroExecution.initial("zero");
      expect(scanLimits).toStrictEqual([0]);
      expect(initial.rows).toStrictEqual([]);
      expect(initial.keys).toStrictEqual([]);
      expect(initial.totalRows).toBe(2);

      yield* releaseRawQueryExecution(observedReadModel, zeroWindow);
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

  it.effect("rejects non-plain object filter values before cache-keying", () =>
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

      const invalidPayload = yield* Effect.flip(
        prepareRawQuery("special", topicStoreRawQueryMetadata(store), {
          select: ["id", "payload"],
          where: {
            payload: queryPayload,
          },
        }),
      );
      expect(invalidPayload).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported query value"),
      });
    }),
  );

  it.effect("rejects non-serializable filter values before cache-keying", () =>
    Effect.gen(function* () {
      const firstFunction = () => "first";
      const firstSymbol = Symbol("first");
      const firstMap = new Map([["marker", "first"]]);
      const store = new TopicStore(
        "special-non-serializable",
        Schema.Struct({
          id: Schema.String,
          marker: Schema.Unknown,
        }),
        "id",
        () => {},
      );

      const functionFilter = yield* Effect.flip(
        prepareRawQuery("special-non-serializable", topicStoreRawQueryMetadata(store), {
          select: ["id", "marker"],
          where: {
            marker: firstFunction,
          },
        }),
      );
      expect(functionFilter).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported query value"),
      });

      const symbolFilter = yield* Effect.flip(
        prepareRawQuery("special-non-serializable", topicStoreRawQueryMetadata(store), {
          select: ["id", "marker"],
          where: {
            marker: firstSymbol,
          },
        }),
      );
      expect(symbolFilter).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported query value"),
      });

      const mapFilter = yield* Effect.flip(
        prepareRawQuery("special-non-serializable", topicStoreRawQueryMetadata(store), {
          select: ["id", "marker"],
          where: {
            marker: firstMap,
          },
        }),
      );
      expect(mapFilter).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported query value"),
      });
    }),
  );
});
