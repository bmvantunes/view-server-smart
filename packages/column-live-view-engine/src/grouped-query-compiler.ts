import { Effect } from "effect";
import { decodeGroupedQuery, type RuntimeGroupedQuery } from "./grouped-query-decoder";
import { evaluateGroupedRows, typedGroupedEvaluation } from "./grouped-query-evaluation";
import { makeGroupedQueryPlan, type GroupedQueryPlan } from "./grouped-query-plan";
import { type RawQueryCompilerMetadata, prepareRawQuery } from "./raw-query-compiler";
import type { QueryEvaluation } from "./query-result";
import type { TopicRowScan } from "./row-scan";

type RowObject = object;

export type { RuntimeGroupedQuery };

export type CompiledGroupedQuery<Row extends RowObject, ResultRow extends RowObject> = {
  readonly plan: GroupedQueryPlan<Row>;
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
    const plan = makeGroupedQueryPlan<Row>(decoded);
    return {
      plan,
      cacheKey: plan.cacheKey,
      matches,
      evaluate: (store) =>
        typedGroupedEvaluation<ResultRow>(evaluateGroupedRows(store, plan, matches)),
    } satisfies CompiledGroupedQuery<Row, ResultRow>;
  },
);

export const evaluateCompiledGroupedQuery = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRowScan<Row>,
  compiled: CompiledGroupedQuery<Row, ResultRow>,
): QueryEvaluation<ResultRow> => compiled.evaluate(store);
