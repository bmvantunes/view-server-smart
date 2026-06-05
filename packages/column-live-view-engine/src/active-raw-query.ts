import { Effect, Option } from "effect";
import type { DeltaEvent } from "@view-server/config";
import type { ActiveQueryStoreState, RawQueryExecution } from "./active-query";
import {
  stableQueryValueString,
  type CompiledRawQuery,
  type RuntimeRawQuery,
} from "./raw-query-compiler";
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
  readonly window: CompiledRawQuery<object, object>["window"];
  refs: number;
};

type ActiveQueryBaseEvaluation<Row extends RowObject> = TopicRawWindowScanResult<Row> & {
  readonly version: number;
};

type QueryCacheToken = readonly ["raw", string, string];
type QueryWindowCacheToken = readonly ["window", string, string];

const queryCacheKey = (query: RuntimeRawQuery): string => {
  const orderBy: ReadonlyArray<readonly [string, "asc" | "desc"]> =
    query.orderBy === undefined ? [] : query.orderBy.map((entry) => [entry.field, entry.direction]);
  const token: QueryCacheToken = [
    "raw",
    query.where === undefined ? "" : stableQueryValueString(query.where),
    stableQueryValueString(orderBy),
  ];
  return JSON.stringify(token);
};

const queryWindowCacheKey = (window: CompiledRawQuery<object, object>["window"]): string => {
  const token: QueryWindowCacheToken = [
    "window",
    stableQueryValueString(window.offset),
    stableQueryValueString(window.limit ?? null),
  ];
  return JSON.stringify(token);
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
  const key = queryCacheKey(compiled.query);
  const map = getActiveRawQueryMap(store);
  return { map, key };
};

const evaluateBaseQuery = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRawWindowScan<Row> & { readonly version: () => number },
  compiled: CompiledRawQuery<Row, ResultRow>,
  queryWindow: CompiledRawQuery<Row, ResultRow>["window"] = compiled.window,
): ActiveQueryBaseEvaluation<Row> => {
  const version = store.version();
  const scanResult = store.scanRawWindow({
    predicate: compiled.predicate.plan,
    orderBy: compiled.ordering.plan,
    storageOrderBy: compiled.ordering.plan,
    matches: compiled.predicate.matches,
    compare: compiled.ordering.compare,
    offset: queryWindow.offset,
    limit: queryWindow.limit,
  });
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
    row: compiled.projection.project(entry.row),
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
    compiled.window.limit === undefined
      ? undefined
      : compiled.window.offset + compiled.window.limit;
  const sourceWindow = evaluation.window.slice(compiled.window.offset, end);
  const window = sourceWindow.map((entry) => ({
    key: entry.key,
    row: compiled.projection.project(entry.row),
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
): CompiledRawQuery<object, object>["window"] => {
  let limit = 0;
  for (const { window } of windows.values()) {
    if (window.limit === undefined) {
      return {
        offset: 0,
        limit: undefined,
      };
    }
    const windowEnd = window.limit === 0 ? 0 : window.offset + window.limit;
    limit = Math.max(limit, windowEnd);
  }
  return {
    offset: 0,
    limit,
  };
};

const acquireRawQueryWindow = (
  windows: Map<string, RawQueryExecutionWindowSlot>,
  window: CompiledRawQuery<object, object>["window"],
): void => {
  const key = queryWindowCacheKey(window);
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
  window: CompiledRawQuery<object, object>["window"],
): boolean => {
  const key = queryWindowCacheKey(window);
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
      acquireRawQueryWindow(entry.windows, compiled.window);
      return leaseRawQueryExecution(store, entry.execution, compiled);
    }

    const windows = new Map<string, RawQueryExecutionWindowSlot>();
    acquireRawQueryWindow(windows, compiled.window);
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
      if (!releaseRawQueryWindow(entry.windows, compiled.window)) {
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
