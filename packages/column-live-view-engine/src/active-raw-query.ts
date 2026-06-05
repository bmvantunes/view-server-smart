import { Effect, Option } from "effect";
import type { DeltaEvent } from "@view-server/config";
import type { ActiveQueryStoreState, RawQueryExecution } from "./active-query";
import type { CompiledRawQuery } from "./raw-query-compiler";
import {
  rawQueryPlanWindow,
  rawQueryWindowScanPlan,
  type RawQueryPlanWindow,
} from "./raw-query-plan";
import { deltaEvent, deltaOperations, snapshotEvent } from "./query-result";
import type { QueryEvaluation } from "./query-result";
import type { TopicRawWindowScan, TopicRawWindowScanResult } from "./raw-window-scan";

type RowObject = object;

type ActiveQueryBaseExecution = {
  readonly latest: () => ActiveQueryBaseEvaluation<object>;
};

export type RawQueryExecutionSlot = {
  readonly execution: ActiveQueryBaseExecution;
  readonly windows: Map<string, RawQueryExecutionWindowSlot>;
  refs: number;
};

type RawQueryExecutionWindowSlot = {
  readonly window: RawQueryPlanWindow;
  refs: number;
};

type ActiveQueryBaseEvaluation<Row extends RowObject> = TopicRawWindowScanResult<Row> & {
  readonly version: number;
};

const getActiveRawQueryMap = (store: ActiveQueryStoreState): Map<string, RawQueryExecutionSlot> => {
  return store.activeQueries.raw;
};

const getActiveRawQueryEntry = <ResultRow extends RowObject>(
  store: ActiveQueryStoreState,
  compiled: CompiledRawQuery<object, ResultRow>,
): {
  map: Map<string, RawQueryExecutionSlot>;
  key: string;
} => {
  const key = compiled.plan.queryCacheKey;
  const map = getActiveRawQueryMap(store);
  return { map, key };
};

const evaluateBaseQuery = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRawWindowScan<Row> & { readonly version: () => number },
  compiled: CompiledRawQuery<Row, ResultRow>,
  queryWindow: RawQueryPlanWindow = compiled.plan.window,
): ActiveQueryBaseEvaluation<Row> => {
  const version = store.version();
  const scanResult = store.scanRawWindow(rawQueryWindowScanPlan(compiled.plan, queryWindow));
  return {
    ...scanResult,
    version,
  };
};

const projectBaseEvaluation = <Row extends RowObject, ResultRow extends RowObject>(
  compiled: CompiledRawQuery<Row, ResultRow>,
  evaluation: ActiveQueryBaseEvaluation<Row>,
): QueryEvaluation<ResultRow> => {
  const window = evaluation.window.map((entry) => ({
    key: entry.key,
    row: compiled.plan.project(entry.row),
  }));

  return {
    rows: window.map((entry) => entry.row),
    keys: evaluation.keys,
    window,
    totalRows: evaluation.totalRows,
    version: evaluation.version,
  };
};

const projectWindowEvaluation = <Row extends RowObject, ResultRow extends RowObject>(
  compiled: CompiledRawQuery<Row, ResultRow>,
  evaluation: ActiveQueryBaseEvaluation<Row>,
): QueryEvaluation<ResultRow> => {
  const end =
    compiled.plan.window.limit === undefined
      ? undefined
      : compiled.plan.window.offset + compiled.plan.window.limit;
  const sourceWindow = evaluation.window.slice(compiled.plan.window.offset, end);
  const window = sourceWindow.map((entry) => ({
    key: entry.key,
    row: compiled.plan.project(entry.row),
  }));

  return {
    rows: window.map((entry) => entry.row),
    keys: window.map((entry) => entry.key),
    window,
    totalRows: evaluation.totalRows,
    version: evaluation.version,
  };
};

export const evaluateRawQuery = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRawWindowScan<Row> & { readonly version: () => number },
  compiled: CompiledRawQuery<Row, ResultRow>,
): QueryEvaluation<ResultRow> =>
  projectBaseEvaluation(compiled, evaluateBaseQuery(store, compiled));

const leaseRawQueryExecution = <ResultRow extends RowObject>(
  store: ActiveQueryStoreState,
  execution: ActiveQueryBaseExecution,
  compiled: CompiledRawQuery<object, ResultRow>,
): RawQueryExecution<ResultRow> => {
  const latestEvaluation = () => projectWindowEvaluation(compiled, execution.latest());

  return {
    initial: (queryId) => snapshotEvent(store, queryId, latestEvaluation()),
    createCursor: () => ({
      evaluation: latestEvaluation(),
    }),
    next: (queryId, cursor): Effect.Effect<Option.Option<DeltaEvent<ResultRow>>> =>
      Effect.sync(() => {
        const previous = cursor.evaluation;
        const next = latestEvaluation();
        const operations = deltaOperations(previous, next);
        if (operations.length === 0 && previous.totalRows === next.totalRows) {
          return Option.none();
        }
        cursor.evaluation = next;
        return Option.some(deltaEvent(store, queryId, previous.version, next, operations));
      }),
  };
};

const baseWindowForActiveWindows = (
  windows: ReadonlyMap<string, RawQueryExecutionWindowSlot>,
): RawQueryPlanWindow => {
  let limit = 0;
  for (const { window } of windows.values()) {
    if (window.limit === undefined) {
      return rawQueryPlanWindow(0, undefined);
    }
    const windowEnd = window.limit === 0 ? 0 : window.offset + window.limit;
    limit = Math.max(limit, windowEnd);
  }
  return rawQueryPlanWindow(0, limit);
};

const acquireRawQueryWindow = (
  windows: Map<string, RawQueryExecutionWindowSlot>,
  window: RawQueryPlanWindow,
): void => {
  const key = window.cacheKey;
  const existing = windows.get(key);
  if (existing !== undefined) {
    existing.refs += 1;
    return;
  }
  windows.set(key, {
    window,
    refs: 1,
  });
};

const releaseRawQueryWindow = (
  windows: Map<string, RawQueryExecutionWindowSlot>,
  window: RawQueryPlanWindow,
): boolean => {
  const key = window.cacheKey;
  const existing = windows.get(key);
  if (existing === undefined) {
    return false;
  }
  if (existing.refs > 1) {
    existing.refs -= 1;
    return true;
  }
  windows.delete(key);
  return true;
};

const makeRawQueryExecution = Effect.fn("ColumnLiveViewEngine.activeQuery.raw.make")(
  (
    store: ActiveQueryStoreState,
    canonicalCompiled: CompiledRawQuery<object, object>,
    windows: ReadonlyMap<string, RawQueryExecutionWindowSlot>,
  ) =>
    Effect.sync(() => {
      let baseWindow = baseWindowForActiveWindows(windows);
      let snapshot = {
        evaluation: evaluateBaseQuery(store, canonicalCompiled, baseWindow),
        version: store.version(),
      };

      const latest = () => {
        const storeVersion = store.version();
        const nextBaseWindow = baseWindowForActiveWindows(windows);
        if (
          snapshot.version !== storeVersion ||
          nextBaseWindow.offset !== baseWindow.offset ||
          nextBaseWindow.limit !== baseWindow.limit
        ) {
          baseWindow = nextBaseWindow;
          snapshot = {
            evaluation: evaluateBaseQuery(store, canonicalCompiled, baseWindow),
            version: storeVersion,
          };
        }
        return snapshot.evaluation;
      };

      return {
        latest,
      };
    }),
);

export const acquireRawQueryExecution = Effect.fn("ColumnLiveViewEngine.activeQuery.raw.acquire")(
  function* <ResultRow extends RowObject>(
    store: ActiveQueryStoreState,
    compiled: CompiledRawQuery<object, ResultRow>,
  ) {
    const { map, key } = getActiveRawQueryEntry(store, compiled);
    const existing = map.get(key);
    if (existing !== undefined) {
      const entry = existing;
      entry.refs += 1;
      acquireRawQueryWindow(entry.windows, compiled.plan.window);
      return leaseRawQueryExecution(store, entry.execution, compiled);
    }

    const windows = new Map<string, RawQueryExecutionWindowSlot>();
    acquireRawQueryWindow(windows, compiled.plan.window);
    const execution = yield* makeRawQueryExecution(store, compiled, windows);
    map.set(key, {
      execution,
      windows,
      refs: 1,
    });
    return leaseRawQueryExecution(store, execution, compiled);
  },
);

export const releaseRawQueryExecution = Effect.fn("ColumnLiveViewEngine.activeQuery.raw.release")(
  <ResultRow extends RowObject>(
    store: ActiveQueryStoreState,
    compiled: CompiledRawQuery<object, ResultRow>,
  ) =>
    Effect.sync(() => {
      const { map, key } = getActiveRawQueryEntry(store, compiled);
      const existing = map.get(key);
      if (existing === undefined) {
        return undefined;
      }
      const entry = existing;
      if (!releaseRawQueryWindow(entry.windows, compiled.plan.window)) {
        return undefined;
      }
      if (entry.refs > 1) {
        entry.refs -= 1;
        entry.execution.latest();
        return undefined;
      }
      map.delete(key);
      return undefined;
    }),
);

export const clearRawQueryExecutions = Effect.fn("ColumnLiveViewEngine.activeQuery.raw.clearStore")(
  (store: ActiveQueryStoreState) =>
    Effect.sync(() => {
      getActiveRawQueryMap(store).clear();
    }),
);

export const activeRawQueryExecutionCount = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.raw.countStore",
)((store: ActiveQueryStoreState) => Effect.sync(() => getActiveRawQueryMap(store).size));
