import { Effect, Option } from "effect";
import { isBigDecimal } from "effect/BigDecimal";
import type { DeltaEvent, SnapshotEvent } from "@view-server/config";
import { type CompiledRawQuery, type RuntimeRawQuery } from "./raw-query-compiler";
import type { RawQueryRowStore } from "./raw-query-compiler";
import { deltaEvent, deltaOperations, snapshotEvent } from "./query-result";
import type { QueryEvaluation, StoredRowOf } from "./query-result";
import { isPlainRecord } from "./row-values";

type RowObject = object;

export type ActiveQueryStoreState = RawQueryRowStore<object> & {
  readonly identity: object;
  readonly topic: string;
};

export type RawQueryExecutionCursor<ResultRow extends RowObject> = {
  evaluation: QueryEvaluation<ResultRow>;
};

type RawQueryExecutionUpdate<ResultRow extends RowObject> = Effect.Effect<
  Option.Option<DeltaEvent<ResultRow>>,
  never,
  never
>;

export type RawQueryExecution<ResultRow extends RowObject> = {
  readonly initial: (queryId: string) => SnapshotEvent<ResultRow>;
  readonly createCursor: () => RawQueryExecutionCursor<ResultRow>;
  readonly next: (
    queryId: string,
    cursor: RawQueryExecutionCursor<ResultRow>,
  ) => RawQueryExecutionUpdate<ResultRow>;
};

type ActiveQueryBaseExecution = {
  readonly latest: () => ActiveQueryBaseEvaluation;
};

type RawQueryExecutionSlot = {
  readonly execution: ActiveQueryBaseExecution;
  refs: number;
};

type ActiveQueryBaseEvaluation = {
  readonly keys: ReadonlyArray<string>;
  readonly window: ReadonlyArray<StoredRowOf<object>>;
  readonly totalRows: number;
  readonly version: number;
};

type QueryExecutionCache = WeakMap<object, Map<string, RawQueryExecutionSlot>>;

const activeQueryExecutionCache: QueryExecutionCache = new WeakMap();

const objectIdentities = new WeakMap<object, number>();
const symbolIdentities = new Map<symbol, number>();
let nextObjectIdentity = 0;
let nextSymbolIdentity = 0;

type QueryValueToken =
  | readonly ["null"]
  | readonly ["undefined"]
  | readonly ["boolean", boolean]
  | readonly ["number", string]
  | readonly ["string", string]
  | readonly ["bigint", string]
  | readonly ["bigDecimal", string]
  | readonly ["symbol", number]
  | readonly ["function", string, number]
  | readonly ["array", ReadonlyArray<QueryValueToken>]
  | readonly ["object", ReadonlyArray<readonly [string, QueryValueToken]>]
  | readonly ["nonPlainObject", string, number];

type QueryCacheToken = readonly [
  "raw",
  ReadonlyArray<readonly [string, QueryValueToken]>,
  ReadonlyArray<readonly [string, "asc" | "desc"]>,
  QueryValueToken,
  QueryValueToken,
];

const stableObjectIdentity = (value: object): number => {
  const existing = objectIdentities.get(value);
  if (existing !== undefined) {
    return existing;
  }
  nextObjectIdentity += 1;
  objectIdentities.set(value, nextObjectIdentity);
  return nextObjectIdentity;
};

const stableSymbolIdentity = (value: symbol): number => {
  const existing = symbolIdentities.get(value);
  if (existing !== undefined) {
    return existing;
  }
  nextSymbolIdentity += 1;
  symbolIdentities.set(value, nextSymbolIdentity);
  return nextSymbolIdentity;
};

const stableObjectName = (value: object): string => Object.prototype.toString.call(value);

const stableFunctionName = (value: { readonly name: string }): string =>
  value.name === "" ? "anonymous" : value.name;

const isRecordLike = (value: unknown): value is Record<string, unknown> =>
  isPlainRecord(value) || isBigDecimal(value);

const stableNumberValue = (value: number): string => {
  if (Object.is(value, -0)) {
    return "-0";
  }
  return String(value);
};

const encodeQueryValue = (value: unknown): QueryValueToken => {
  if (isBigDecimal(value)) {
    return ["bigDecimal", value.toString()];
  }
  if (value === null) {
    return ["null"];
  }
  if (Array.isArray(value)) {
    return ["array", value.map(encodeQueryValue)];
  }
  if (isRecordLike(value)) {
    return [
      "object",
      Object.keys(value)
        .toSorted()
        .map((key) => [key, encodeQueryValue(value[key])]),
    ];
  }
  switch (typeof value) {
    case "string":
      return ["string", value];
    case "number":
      return ["number", stableNumberValue(value)];
    case "bigint":
      return ["bigint", value.toString()];
    case "boolean":
      return ["boolean", value];
    case "symbol":
      return ["symbol", stableSymbolIdentity(value)];
    case "function":
      return ["function", stableFunctionName(value), stableObjectIdentity(value)];
    case "object":
      return ["nonPlainObject", stableObjectName(value), stableObjectIdentity(value)];
  }
  return ["undefined"];
};

const canonicalizeWhere = (
  where: Record<string, unknown>,
): ReadonlyArray<readonly [string, QueryValueToken]> =>
  Object.keys(where)
    .toSorted()
    .map((field) => [field, encodeQueryValue(where[field])]);

const queryCacheKey = (query: RuntimeRawQuery): string => {
  const orderBy: ReadonlyArray<readonly [string, "asc" | "desc"]> =
    query.orderBy === undefined ? [] : query.orderBy.map((entry) => [entry.field, entry.direction]);
  const token: QueryCacheToken = [
    "raw",
    query.where === undefined ? [] : canonicalizeWhere(query.where),
    orderBy,
    encodeQueryValue(query.offset),
    encodeQueryValue(query.limit),
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

const evaluateBaseQuery = (
  store: ActiveQueryStoreState,
  compiled: CompiledRawQuery<object, object>,
): ActiveQueryBaseEvaluation => {
  const storeRows = store.rows();
  const storeVersion = store.version();
  const filtered = Array.from(storeRows, ([key, row]) => ({ key, row })).filter((entry) =>
    compiled.matches(entry.row),
  );
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

const projectBaseEvaluation = <ResultRow extends RowObject>(
  compiled: CompiledRawQuery<object, ResultRow>,
  evaluation: ActiveQueryBaseEvaluation,
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

export const clearStoreRawQueryExecutions = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.clearStore",
)((store: ActiveQueryStoreState) =>
  Effect.sync(() => {
    activeQueryExecutionCache.delete(store.identity);
  }),
);

export const activeStoreRawQueryExecutionCount = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.countStore",
)((store: ActiveQueryStoreState) =>
  Effect.sync(() => activeQueryExecutionCache.get(store.identity)?.size ?? 0),
);
