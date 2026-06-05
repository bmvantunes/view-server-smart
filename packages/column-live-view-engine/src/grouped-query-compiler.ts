import { Effect } from "effect";
import { decodeGroupedQuery, type RuntimeGroupedQuery } from "./grouped-query-decoder";
import {
  evaluateGroupedRows,
  groupedCacheKey,
  typedGroupedEvaluation,
} from "./grouped-query-evaluation";
import { type RawQueryCompilerMetadata, prepareRawQuery } from "./raw-query-compiler";
import type { QueryEvaluation } from "./query-result";
import type { TopicRowScan } from "./row-scan";

type RowObject = object;

export type { RuntimeGroupedQuery };

export type CompiledGroupedQuery<Row extends RowObject, ResultRow extends RowObject> = {
  readonly query: RuntimeGroupedQuery;
  readonly cacheKey: string;
  readonly matches: (row: Row) => boolean;
  readonly evaluate: (store: TopicRowScan<Row>) => QueryEvaluation<ResultRow>;
};

export const prepareGroupedQuery = Effect.fn("ColumnLiveViewEngine.groupedQuery.prepare")(
  function* <Row extends RowObject, ResultRow extends RowObject>(
    topic: string,
    metadata: RawQueryCompilerMetadata,
    query: unknown,
  ) {
    const decoded = yield* decodeGroupedQuery(topic, metadata, query);
    const rawFilter = yield* prepareRawQuery<Row, RowObject>(topic, metadata, {
      select: decoded.groupBy,
      ...(decoded.where === undefined ? {} : { where: decoded.where }),
    });
    const { matches } = rawFilter.plan.predicate;
    return {
      query: decoded,
      cacheKey: groupedCacheKey(decoded),
      matches,
      evaluate: (store) =>
        typedGroupedEvaluation<ResultRow>(evaluateGroupedRows(store, decoded, matches)),
    } satisfies CompiledGroupedQuery<Row, ResultRow>;
  },
);

export const evaluateCompiledGroupedQuery = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRowScan<Row>,
  compiled: CompiledGroupedQuery<Row, ResultRow>,
): QueryEvaluation<ResultRow> => compiled.evaluate(store);
