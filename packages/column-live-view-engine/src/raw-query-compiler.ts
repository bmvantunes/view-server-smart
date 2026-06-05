import { Effect } from "effect";
import { compareQueryValue, stableQueryValueString } from "./query-value";
import type { CompiledRawPredicate } from "./raw-predicate-compiler";
import { isRangePlanValue } from "./raw-predicate-plan";
import { makeRawQueryPlan, type RawQueryPlan } from "./raw-query-plan";
import {
  decodeRawQuery,
  InvalidQueryError,
  type RuntimeRawQuery,
  validateRuntimeQuery,
} from "./raw-query-decoder";
import { rawQueryCompilerMetadata, type RawQueryCompilerMetadata } from "./raw-query-metadata";

type RowObject = object;
const compiledRawQueryBrand: unique symbol = Symbol("CompiledRawQuery");

export { rawQueryCompilerMetadata };
export { compareQueryValue, stableQueryValueString };
export { isRangePlanValue };
export { InvalidQueryError };
export type { RawQueryCompilerMetadata, RuntimeRawQuery };

export type CompiledRawQuery<Row extends RowObject, ResultRow extends RowObject> = {
  readonly [compiledRawQueryBrand]: true;
  readonly query: RuntimeRawQuery;
  readonly plan: RawQueryPlan<Row, ResultRow>;
};

export type { CompiledRawPredicate };

const compileRawQuery = <Row extends RowObject, ResultRow extends RowObject>(
  metadata: RawQueryCompilerMetadata,
  query: RuntimeRawQuery,
): CompiledRawQuery<Row, ResultRow> => {
  const plan = makeRawQueryPlan<Row, ResultRow>(metadata, query);
  return {
    [compiledRawQueryBrand]: true,
    query,
    plan,
  };
};

export const prepareRawQuery = Effect.fn("ColumnLiveViewEngine.rawQuery.prepare")(function* <
  Row extends RowObject,
  ResultRow extends RowObject,
>(topic: string, metadata: RawQueryCompilerMetadata, query: unknown) {
  const decoded = yield* decodeRawQuery(topic, metadata, query);
  yield* validateRuntimeQuery(topic, metadata, decoded);
  return compileRawQuery<Row, ResultRow>(metadata, decoded);
});
