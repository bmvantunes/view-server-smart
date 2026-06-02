import { Effect, Option } from "effect";
import type { DeltaEvent, SnapshotEvent } from "@view-server/config";
import {
  stableQueryValueString,
  type CompiledRawQuery,
  type RuntimeRawQuery,
} from "./raw-query-compiler";
import { deltaEvent, deltaOperations, snapshotEvent } from "./query-result";
import type { QueryEvaluation, StoredRowOf } from "./query-result";
import type { TopicRowScan } from "./row-scan";

type RowObject = object;

export type ActiveQueryStoreState = TopicRowScan<object> & {
  readonly identity: object;
  readonly topic: string;
};

export type LiveQueryExecutionCursor<ResultRow extends RowObject> = {
  evaluation: QueryEvaluation<ResultRow>;
};

type RawQueryExecutionUpdate<ResultRow extends RowObject> = Effect.Effect<
  Option.Option<DeltaEvent<ResultRow>>,
  never,
  never
>;

export type LiveQueryExecution<ResultRow extends RowObject> = {
  readonly initial: (queryId: string) => SnapshotEvent<ResultRow>;
  readonly createCursor: () => LiveQueryExecutionCursor<ResultRow>;
  readonly next: (
    queryId: string,
    cursor: LiveQueryExecutionCursor<ResultRow>,
  ) => RawQueryExecutionUpdate<ResultRow>;
};

export type RawQueryExecution<ResultRow extends RowObject> = LiveQueryExecution<ResultRow>;

type ActiveQueryBaseExecution = {
  readonly latest: () => ActiveQueryBaseEvaluation<object>;
};

type RawQueryExecutionSlot = {
  readonly execution: ActiveQueryBaseExecution;
  refs: number;
};

type MaterializedQueryExecutionSlot = {
  readonly execution: ActiveMaterializedQueryExecution;
  refs: number;
};

type ActiveQueryBaseEvaluation<Row extends RowObject> = {
  readonly keys: ReadonlyArray<string>;
  readonly window: ReadonlyArray<StoredRowOf<Row>>;
  readonly totalRows: number;
  readonly version: number;
};

type ActiveMaterializedQueryExecution = {
  readonly latest: () => QueryEvaluation<object>;
};

type QueryExecutionCache = WeakMap<object, Map<string, RawQueryExecutionSlot>>;
type MaterializedQueryExecutionCache = WeakMap<object, Map<string, MaterializedQueryExecutionSlot>>;

const activeQueryExecutionCache: QueryExecutionCache = new WeakMap();
const activeMaterializedQueryExecutionCache: MaterializedQueryExecutionCache = new WeakMap();

type QueryCacheToken = readonly ["raw", string, string, string, string];

const queryCacheKey = (query: RuntimeRawQuery): string => {
  const orderBy: ReadonlyArray<readonly [string, "asc" | "desc"]> =
    query.orderBy === undefined ? [] : query.orderBy.map((entry) => [entry.field, entry.direction]);
  const token: QueryCacheToken = [
    "raw",
    query.where === undefined ? "" : stableQueryValueString(query.where),
    stableQueryValueString(orderBy),
    stableQueryValueString(query.offset ?? null),
    stableQueryValueString(query.limit ?? null),
  ];
  return JSON.stringify(token);
};

const getActiveQueryMap = (store: ActiveQueryStoreState): Map<string, RawQueryExecutionSlot> => {
  const existing = activeQueryExecutionCache.get(store.identity);
  if (existing !== undefined) {
    return existing;
  }
  const created = new Map<string, RawQueryExecutionSlot>();
  activeQueryExecutionCache.set(store.identity, created);
  return created;
};

const getActiveMaterializedQueryMap = (
  store: ActiveQueryStoreState,
): Map<string, MaterializedQueryExecutionSlot> => {
  const existing = activeMaterializedQueryExecutionCache.get(store.identity);
  if (existing !== undefined) {
    return existing;
  }
  const created = new Map<string, MaterializedQueryExecutionSlot>();
  activeMaterializedQueryExecutionCache.set(store.identity, created);
  return created;
};

const getActiveQueryEntry = <ResultRow extends RowObject>(
  store: ActiveQueryStoreState,
  compiled: CompiledRawQuery<object, ResultRow>,
): {
  map: Map<string, RawQueryExecutionSlot>;
  key: string;
} => {
  const key = queryCacheKey(compiled.query);
  const map = getActiveQueryMap(store);
  return { map, key };
};

const evaluateBaseQuery = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRowScan<Row>,
  compiled: CompiledRawQuery<Row, ResultRow>,
): ActiveQueryBaseEvaluation<Row> => {
  const storeVersion = store.version();
  const filtered: Array<StoredRowOf<Row>> = [];
  store.scanRows((key, row) => {
    if (compiled.matches(row)) {
      filtered.push({ key, row });
    }
  });
  const ordered = filtered.toSorted(compiled.compare);
  const offset = compiled.offset;
  const window = ordered.slice(
    offset,
    compiled.limit === undefined ? undefined : offset + compiled.limit,
  );

  return {
    keys: window.map((entry) => entry.key),
    window,
    totalRows: filtered.length,
    version: storeVersion,
  };
};

const projectBaseEvaluation = <Row extends RowObject, ResultRow extends RowObject>(
  compiled: CompiledRawQuery<Row, ResultRow>,
  evaluation: ActiveQueryBaseEvaluation<Row>,
): QueryEvaluation<ResultRow> => {
  const window = evaluation.window.map((entry) => ({
    key: entry.key,
    row: compiled.project(entry.row),
  }));

  return {
    rows: window.map((entry) => entry.row),
    keys: evaluation.keys,
    window,
    totalRows: evaluation.totalRows,
    version: evaluation.version,
  };
};

export const evaluateRawQuery = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRowScan<Row>,
  compiled: CompiledRawQuery<Row, ResultRow>,
): QueryEvaluation<ResultRow> =>
  projectBaseEvaluation(compiled, evaluateBaseQuery(store, compiled));

const leaseRawQueryExecution = <ResultRow extends RowObject>(
  store: ActiveQueryStoreState,
  execution: ActiveQueryBaseExecution,
  compiled: CompiledRawQuery<object, ResultRow>,
): RawQueryExecution<ResultRow> => {
  const latestEvaluation = () => projectBaseEvaluation(compiled, execution.latest());

  return {
    initial: (queryId) => snapshotEvent(store, queryId, latestEvaluation()),
    createCursor: () => ({
      evaluation: latestEvaluation(),
    }),
    next: (queryId, cursor) =>
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

const leaseMaterializedQueryExecution = <ResultRow extends RowObject>(
  store: ActiveQueryStoreState,
  execution: ActiveMaterializedQueryExecution,
): LiveQueryExecution<ResultRow> => {
  const latestEvaluation = () => typedQueryEvaluation<ResultRow>(execution.latest());

  return {
    initial: (queryId) => snapshotEvent(store, queryId, latestEvaluation()),
    createCursor: () => ({
      evaluation: latestEvaluation(),
    }),
    next: (queryId, cursor) =>
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

function typedQueryEvaluation<ResultRow extends RowObject>(
  evaluation: QueryEvaluation<object>,
): QueryEvaluation<ResultRow>;
function typedQueryEvaluation(evaluation: QueryEvaluation<object>): QueryEvaluation<object> {
  return evaluation;
}

export const makeRawQueryExecution = Effect.fn("ColumnLiveViewEngine.activeQuery.make")(
  (store: ActiveQueryStoreState, canonicalCompiled: CompiledRawQuery<object, object>) =>
    Effect.sync(() => {
      let snapshot = {
        evaluation: evaluateBaseQuery(store, canonicalCompiled),
        version: store.version(),
      };

      const latest = () => {
        const storeVersion = store.version();
        if (snapshot.version !== storeVersion) {
          snapshot = {
            evaluation: evaluateBaseQuery(store, canonicalCompiled),
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

export const acquireRawQueryExecution = Effect.fn("ColumnLiveViewEngine.activeQuery.acquire")(
  function* <ResultRow extends RowObject>(
    store: ActiveQueryStoreState,
    compiled: CompiledRawQuery<object, ResultRow>,
  ) {
    const { map, key } = getActiveQueryEntry(store, compiled);
    const existing = map.get(key);
    if (existing !== undefined) {
      const entry = existing;
      entry.refs += 1;
      return leaseRawQueryExecution(store, entry.execution, compiled);
    }

    const execution = yield* makeRawQueryExecution(store, compiled);
    map.set(key, {
      execution,
      refs: 1,
    });
    return leaseRawQueryExecution(store, execution, compiled);
  },
);

export const releaseRawQueryExecution = Effect.fn("ColumnLiveViewEngine.activeQuery.release")(
  <ResultRow extends RowObject>(
    store: ActiveQueryStoreState,
    compiled: CompiledRawQuery<object, ResultRow>,
  ) =>
    Effect.sync(() => {
      const { map, key } = getActiveQueryEntry(store, compiled);
      const existing = map.get(key);
      if (existing === undefined) {
        return undefined;
      }
      const entry = existing;
      if (entry.refs > 1) {
        entry.refs -= 1;
        return undefined;
      }
      map.delete(key);
      if (map.size === 0) {
        activeQueryExecutionCache.delete(store.identity);
      }
      return undefined;
    }),
);

export const acquireMaterializedQueryExecution = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.materialized.acquire",
)(function <ResultRow extends RowObject>(
  store: ActiveQueryStoreState,
  cacheKey: string,
  evaluate: () => QueryEvaluation<ResultRow>,
) {
  return Effect.sync(() => {
    const map = getActiveMaterializedQueryMap(store);
    const existing = map.get(cacheKey);
    if (existing !== undefined) {
      const entry = existing;
      entry.refs += 1;
      return leaseMaterializedQueryExecution<ResultRow>(store, entry.execution);
    }

    let snapshot = {
      evaluation: evaluate(),
      version: store.version(),
    };
    const execution: ActiveMaterializedQueryExecution = {
      latest: () => {
        const storeVersion = store.version();
        if (snapshot.version !== storeVersion) {
          snapshot = {
            evaluation: evaluate(),
            version: storeVersion,
          };
        }
        return snapshot.evaluation;
      },
    };
    map.set(cacheKey, {
      execution,
      refs: 1,
    });
    return leaseMaterializedQueryExecution<ResultRow>(store, execution);
  });
});

export const releaseMaterializedQueryExecution = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.materialized.release",
)((store: ActiveQueryStoreState, cacheKey: string) =>
  Effect.sync(() => {
    const map = activeMaterializedQueryExecutionCache.get(store.identity);
    const existing = map?.get(cacheKey);
    if (existing === undefined || map === undefined) {
      return undefined;
    }
    const entry = existing;
    if (entry.refs > 1) {
      entry.refs -= 1;
      return undefined;
    }
    map.delete(cacheKey);
    if (map.size === 0) {
      activeMaterializedQueryExecutionCache.delete(store.identity);
    }
    return undefined;
  }),
);

export const clearStoreRawQueryExecutions = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.clearStore",
)((store: ActiveQueryStoreState) =>
  Effect.sync(() => {
    activeQueryExecutionCache.delete(store.identity);
    activeMaterializedQueryExecutionCache.delete(store.identity);
  }),
);

export const activeStoreRawQueryExecutionCount = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.countStore",
)((store: ActiveQueryStoreState) =>
  Effect.sync(
    () =>
      (activeQueryExecutionCache.get(store.identity)?.size ?? 0) +
      (activeMaterializedQueryExecutionCache.get(store.identity)?.size ?? 0),
  ),
);
