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
import type { TopicRawWindowScan } from "./raw-window-scan";
import type { TopicRowEntry } from "./row-scan";

type RowObject = object;

type ActiveQueryBaseExecution = {
  readonly latest: () => ActiveQueryBaseEvaluation<object>;
};

export type RawQueryExecutionSlot = {
  readonly execution: ActiveQueryBaseExecution;
  readonly releaseRetainedChanges: () => void;
  readonly windows: Map<string, RawQueryExecutionWindowSlot>;
  refs: number;
};

type RawQueryExecutionWindowSlot = {
  readonly window: RawQueryPlanWindow;
  refs: number;
};

type ActiveQueryBaseEvaluation<Row extends RowObject> = {
  readonly keys: ReadonlyArray<string>;
  readonly retainedWindowFilled: boolean;
  readonly totalRows: number;
  readonly version: number;
  readonly window: ReadonlyArray<RetainedWindowEntry<Row>>;
};

type RetainedWindowEntry<Row extends RowObject = RowObject> = TopicRowEntry<Row> & {
  readonly key: string;
  readonly row: Row;
};

type RetainedReplacementResult<Row extends RowObject> = {
  readonly window: ReadonlyArray<RetainedWindowEntry<Row>>;
};

const retainedWindowFilled = (
  window: ReadonlyArray<{ readonly key: string; readonly row: RowObject }>,
  totalRows: number,
  queryWindow: RawQueryPlanWindow,
): boolean =>
  queryWindow.limit === undefined || window.length >= Math.min(totalRows, queryWindow.limit);

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

const insertionIndexForSortedRetainedEntries = <Row extends RowObject>(
  windowEntries: ReadonlyArray<RetainedWindowEntry<Row>>,
  nextEntry: RetainedWindowEntry<Row>,
  compare: (left: RetainedWindowEntry<Row>, right: RetainedWindowEntry<Row>) => number,
): number => {
  let low = 0;
  let high = windowEntries.length;
  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    const middleEntry = windowEntries[middle]!;
    if (compare(nextEntry, middleEntry) < 0) {
      high = middle;
    } else {
      low = middle + 1;
    }
  }
  return low;
};

const evaluateBaseQuery = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRawWindowScan<Row> & { readonly version: () => number },
  compiled: CompiledRawQuery<Row, ResultRow>,
  queryWindow: RawQueryPlanWindow = compiled.plan.window,
): ActiveQueryBaseEvaluation<Row> => {
  const version = store.version();
  const scanResult = store.scanRawWindow(rawQueryWindowScanPlan(compiled.plan, queryWindow));
  const window = scanResult.window.map((entry) => ({
    key: entry.key,
    row: entry.row,
  }));
  return {
    ...scanResult,
    retainedWindowFilled: retainedWindowFilled(window, scanResult.totalRows, queryWindow),
    version,
    window,
  };
};

const replaceRetainedMatchingEntry = <Row extends RowObject>(
  windowEntries: ReadonlyArray<RetainedWindowEntry<Row>>,
  key: string,
  row: Row,
  compare: (left: RetainedWindowEntry<Row>, right: RetainedWindowEntry<Row>) => number,
  retainedLimit: number | undefined,
): RetainedReplacementResult<Row> | undefined => {
  let previousIndex = -1;
  let previousEntry: RetainedWindowEntry<Row> | undefined;
  for (const [index, entry] of windowEntries.entries()) {
    if (entry.key === key) {
      previousIndex = index;
      previousEntry = entry;
      break;
    }
  }
  if (previousEntry === undefined) {
    return undefined;
  }
  const nextEntry: RetainedWindowEntry<Row> = { key, row };
  if (retainedLimit !== undefined && compare(nextEntry, previousEntry) > 0) {
    return undefined;
  }
  const withoutPrevious = [
    ...windowEntries.slice(0, previousIndex),
    ...windowEntries.slice(previousIndex + 1),
  ];
  const insertionIndex = insertionIndexForSortedRetainedEntries(
    withoutPrevious,
    nextEntry,
    compare,
  );
  const replaced = [
    ...withoutPrevious.slice(0, insertionIndex),
    nextEntry,
    ...withoutPrevious.slice(insertionIndex),
  ];
  return {
    window: replaced,
  };
};

const retainedEntrySortComparator = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRawWindowScan<Row>,
  compiled: CompiledRawQuery<Row, ResultRow>,
  queryWindow: RawQueryPlanWindow,
): ((left: RetainedWindowEntry<Row>, right: RetainedWindowEntry<Row>) => number) => {
  const compareSlots = store.compareRawSlots?.(rawQueryWindowScanPlan(compiled.plan, queryWindow));
  const slotForKey = store.slotForKey;
  if (compareSlots === undefined || slotForKey === undefined) {
    return compiled.plan.compare;
  }
  return (left, right) => {
    const leftSlot = slotForKey(left.key);
    const rightSlot = slotForKey(right.key);
    return leftSlot === undefined || rightSlot === undefined
      ? compiled.plan.compare(left, right)
      : compareSlots(leftSlot, rightSlot);
  };
};

const updateBaseEvaluationFromRetainedChanges = (
  store: ActiveQueryStoreState,
  compiled: CompiledRawQuery<object, object>,
  evaluation: ActiveQueryBaseEvaluation<object>,
  queryWindow: RawQueryPlanWindow,
): ActiveQueryBaseEvaluation<object> | undefined => {
  const currentVersion = store.version();
  const batches = store.changesSince(evaluation.version);
  if (batches === undefined) {
    return undefined;
  }

  let totalRows = evaluation.totalRows;
  let windowEntries = evaluation.window;
  let removedRetainedEntry = false;
  const insertedWindowEntries = new Map<string, RetainedWindowEntry>();
  for (const batch of batches) {
    for (const change of batch.changes) {
      const previousMatches =
        change.previous !== undefined && compiled.plan.predicate.matches(change.previous);
      const nextMatches = change.next !== undefined && compiled.plan.predicate.matches(change.next);

      if (queryWindow.limit === 0) {
        if (previousMatches && !nextMatches) {
          totalRows -= 1;
        } else if (!previousMatches && nextMatches) {
          totalRows += 1;
        }
        continue;
      }

      if (change.previous !== undefined) {
        if (previousMatches && nextMatches) {
          const pendingInsertedEntry = insertedWindowEntries.get(change.key);
          if (pendingInsertedEntry !== undefined) {
            insertedWindowEntries.set(change.key, {
              key: change.key,
              row: change.next,
            });
            continue;
          }
          const replacedWindow = replaceRetainedMatchingEntry(
            windowEntries,
            change.key,
            change.next,
            compiled.plan.compare,
            queryWindow.limit,
          );
          if (replacedWindow === undefined) {
            return undefined;
          }
          windowEntries = replacedWindow.window;
          continue;
        }
        if (previousMatches) {
          totalRows -= 1;
          insertedWindowEntries.delete(change.key);
          const removedWindowEntries: ReadonlyArray<RetainedWindowEntry> = windowEntries.filter(
            (entry) => entry.key !== change.key,
          );
          if (removedWindowEntries.length !== windowEntries.length) {
            windowEntries = removedWindowEntries;
            removedRetainedEntry = true;
          }
          continue;
        }
        if (nextMatches) {
          totalRows += 1;
          insertedWindowEntries.set(change.key, {
            key: change.key,
            row: change.next,
          });
        }
        continue;
      }
      if (change.next !== undefined && nextMatches) {
        totalRows += 1;
        insertedWindowEntries.set(change.key, {
          key: change.key,
          row: change.next,
        });
      }
    }
  }

  if (queryWindow.limit === 0) {
    return {
      keys: [],
      retainedWindowFilled: true,
      totalRows,
      version: currentVersion,
      window: [],
    };
  }

  const requiredWindowEntries =
    queryWindow.limit === undefined ? undefined : Math.max(0, queryWindow.limit - 1);
  if (
    requiredWindowEntries !== undefined &&
    windowEntries.length < Math.min(totalRows, requiredWindowEntries)
  ) {
    return undefined;
  }

  if (insertedWindowEntries.size === 0) {
    if (windowEntries === evaluation.window && totalRows === evaluation.totalRows) {
      return {
        ...evaluation,
        retainedWindowFilled: retainedWindowFilled(windowEntries, totalRows, queryWindow),
        version: currentVersion,
      };
    }
    return {
      ...evaluation,
      keys: windowEntries.map((entry) => entry.key),
      retainedWindowFilled: retainedWindowFilled(windowEntries, totalRows, queryWindow),
      totalRows,
      version: currentVersion,
      window: windowEntries,
    };
  }

  if (!evaluation.retainedWindowFilled) {
    return undefined;
  }

  const compareRetainedEntries = retainedEntrySortComparator(store, compiled, queryWindow);
  const window = [...windowEntries, ...insertedWindowEntries.values()].sort(compareRetainedEntries);
  const retainedLimit =
    removedRetainedEntry && requiredWindowEntries !== undefined
      ? requiredWindowEntries
      : queryWindow.limit;
  const limitedWindow = retainedLimit === undefined ? window : window.slice(0, retainedLimit);
  return {
    keys: limitedWindow.map((entry) => entry.key),
    retainedWindowFilled: retainedWindowFilled(limitedWindow, totalRows, queryWindow),
    totalRows,
    version: currentVersion,
    window: limitedWindow,
  };
};

const projectBaseEvaluation = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRawWindowScan<Row>,
  compiled: CompiledRawQuery<Row, ResultRow>,
  evaluation: ActiveQueryBaseEvaluation<Row>,
): QueryEvaluation<ResultRow> => {
  const window = evaluation.window.map((entry) => ({
    key: entry.key,
    row: projectRetainedEntry(store, compiled, entry),
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
  store: TopicRawWindowScan<Row>,
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
    row: projectRetainedEntry(store, compiled, entry),
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
  projectBaseEvaluation(store, compiled, evaluateBaseQuery(store, compiled));

function projectStoreSlot<Row extends RowObject, ResultRow extends RowObject>(
  projectRawRow: (slot: number, selectedFields: ReadonlyArray<string>) => RowObject,
  compiled: CompiledRawQuery<Row, ResultRow>,
  slot: number,
): ResultRow;
function projectStoreSlot(
  projectRawRow: (slot: number, selectedFields: ReadonlyArray<string>) => RowObject,
  compiled: CompiledRawQuery<RowObject, RowObject>,
  slot: number,
): RowObject {
  return projectRawRow(slot, compiled.plan.selectedFields);
}

const projectRetainedEntry = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRawWindowScan<Row>,
  compiled: CompiledRawQuery<Row, ResultRow>,
  entry: RetainedWindowEntry<Row>,
): ResultRow => {
  const slot = store.slotForKey?.(entry.key);
  const projectRawRow = store.projectRawRow;
  return slot === undefined || projectRawRow === undefined
    ? compiled.plan.project(entry.row)
    : projectStoreSlot(projectRawRow, compiled, slot);
};

const leaseRawQueryExecution = <ResultRow extends RowObject>(
  store: ActiveQueryStoreState,
  execution: ActiveQueryBaseExecution,
  compiled: CompiledRawQuery<object, ResultRow>,
): RawQueryExecution<ResultRow> => {
  const latestEvaluation = () => projectWindowEvaluation(store, compiled, execution.latest());

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

const retainedWindowForBaseWindow = (window: RawQueryPlanWindow): RawQueryPlanWindow => {
  if (window.limit === undefined || window.limit === 0) {
    return window;
  }
  return rawQueryPlanWindow(window.offset, window.limit + 1);
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
      let retainedWindow = retainedWindowForBaseWindow(baseWindow);
      let snapshot = {
        evaluation: evaluateBaseQuery(store, canonicalCompiled, retainedWindow),
        version: store.version(),
      };

      const latest = () => {
        const storeVersion = store.version();
        const nextBaseWindow = baseWindowForActiveWindows(windows);
        const windowChanged =
          nextBaseWindow.offset !== baseWindow.offset || nextBaseWindow.limit !== baseWindow.limit;
        if (windowChanged) {
          baseWindow = nextBaseWindow;
          retainedWindow = retainedWindowForBaseWindow(baseWindow);
          snapshot = {
            evaluation: evaluateBaseQuery(store, canonicalCompiled, retainedWindow),
            version: storeVersion,
          };
          return snapshot.evaluation;
        }
        if (snapshot.version !== storeVersion) {
          const incrementalEvaluation = updateBaseEvaluationFromRetainedChanges(
            store,
            canonicalCompiled,
            snapshot.evaluation,
            retainedWindow,
          );
          snapshot = {
            evaluation:
              incrementalEvaluation ?? evaluateBaseQuery(store, canonicalCompiled, retainedWindow),
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
    return yield* Effect.sync(() => {
      store.retainChanges();
      map.set(key, {
        execution,
        releaseRetainedChanges: () => store.releaseChanges(),
        windows,
        refs: 1,
      });
      return leaseRawQueryExecution(store, execution, compiled);
    });
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
      entry.releaseRetainedChanges();
      map.delete(key);
      return undefined;
    }),
);

export const clearRawQueryExecutions = Effect.fn("ColumnLiveViewEngine.activeQuery.raw.clearStore")(
  (store: ActiveQueryStoreState) =>
    Effect.sync(() => {
      const map = getActiveRawQueryMap(store);
      for (const entry of map.values()) {
        entry.releaseRetainedChanges();
      }
      map.clear();
    }),
);

export const activeRawQueryExecutionCount = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.raw.countStore",
)((store: ActiveQueryStoreState) => Effect.sync(() => getActiveRawQueryMap(store).size));
