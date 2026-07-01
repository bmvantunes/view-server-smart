import type { FieldKey, TopicDefinitions, TopicRow } from "./query-core";
import type { RejectExtraKeys } from "./query-exact";
import type { ExactLiveQuery, ValidateLiveQuery } from "./query-result-contract";
import type {
  NonEmptyRouteBy,
  TopicLeasedSourceDefinition,
  TopicSourceDefinition,
} from "./source-contract";

type RouteFilterShape<Value> = {
  readonly eq: Value;
};

type ExactRouteFilter<Value, Filter> =
  Filter extends RouteFilterShape<infer Eq>
    ? [Eq] extends [Value]
      ? Filter & RejectExtraKeys<Filter, RouteFilterShape<Value>>
      : never
    : never;

type ExactRouteWhere<Row, QueryWhere, RouteBy extends string> = QueryWhere & {
  readonly [Field in Extract<RouteBy, FieldKey<Row>>]-?: Field extends keyof QueryWhere
    ? ExactRouteFilter<Row[Field], QueryWhere[Field]>
    : never;
};

type UnionToIntersection<Union> = (Union extends unknown ? (value: Union) => void : never) extends (
  value: infer Intersection,
) => void
  ? Intersection
  : never;

export type TopicRouteBy<Topics, Topic extends keyof Topics> = Topics[Topic] extends {
  readonly grpcSource: TopicLeasedSourceDefinition<infer RouteBy>;
}
  ? Extract<RouteBy[number], string>
  : Topics[Topic] extends {
        readonly source: TopicLeasedSourceDefinition<infer RouteBy>;
      }
    ? Extract<RouteBy[number], string>
    : never;

export type TopicRouteByTuple<Topics, Topic extends keyof Topics> = Topics[Topic] extends {
  readonly grpcSource: TopicLeasedSourceDefinition<infer RouteBy>;
}
  ? RouteBy extends NonEmptyRouteBy
    ? RouteBy
    : never
  : Topics[Topic] extends {
        readonly source: TopicLeasedSourceDefinition<infer RouteBy>;
      }
    ? RouteBy extends NonEmptyRouteBy
      ? RouteBy
      : never
    : never;

type NoLeasedRouteRequirement = Readonly<Record<never, never>>;

export type ExactLeasedRouteQuery<Row, RouteBy extends string, Query> = [RouteBy] extends [never]
  ? NoLeasedRouteRequirement
  : Query extends {
        readonly where: infer QueryWhere;
      }
    ? {
        readonly where: ExactRouteWhere<Row, QueryWhere, RouteBy>;
      }
    : {
        readonly where: never;
      };

type ExactLeasedRouteQueryForTopic<
  Topics,
  Topic extends keyof Topics,
  Query,
> = Topic extends keyof Topics
  ? ExactLeasedRouteQuery<TopicRow<Topics, Topic>, TopicRouteBy<Topics, Topic>, Query>
  : never;

export type ExactLiveQueryInputForTopic<Topics, Topic extends keyof Topics, Query> = Query &
  ExactLiveQuery<TopicRow<Topics, Topic>, Query> &
  ValidateLiveQuery<Query> &
  UnionToIntersection<ExactLeasedRouteQueryForTopic<Topics, Topic, Query>>;

const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const sourceLeasedRouteBy = (
  source: TopicSourceDefinition | undefined,
): ReadonlyArray<string> | "invalid" | undefined => {
  const candidate: unknown = source;
  if (!isRecord(candidate) || candidate["lifecycle"] !== "leased") {
    return undefined;
  }
  const routeBy = candidate["routeBy"];
  if (
    !Array.isArray(routeBy) ||
    routeBy.length === 0 ||
    !routeBy.every((field) => typeof field === "string")
  ) {
    return "invalid";
  }
  return routeBy;
};

const exactEqFilterIsPresent = (filter: unknown): boolean =>
  isRecord(filter) && Object.keys(filter).length === 1 && Object.hasOwn(filter, "eq");

export const validateLiveQuerySourceRoute = <Topics extends TopicDefinitions>(
  topics: Topics,
  topic: string,
  query: unknown,
): string | undefined => {
  const topicDefinition = topics[topic];
  if (topicDefinition === undefined) {
    return undefined;
  }
  const sourceAwareTopic: {
    readonly source?: TopicSourceDefinition | undefined;
    readonly grpcSource?: TopicSourceDefinition | undefined;
  } = topicDefinition;
  const routeBy = sourceLeasedRouteBy(sourceAwareTopic.grpcSource ?? sourceAwareTopic.source);
  if (routeBy === undefined) {
    return undefined;
  }
  if (routeBy === "invalid") {
    return `Leased topic ${topic} has invalid route metadata.`;
  }
  if (!isRecord(query)) {
    return `Leased topic ${topic} requires a query object.`;
  }
  const where = query["where"];
  if (!isRecord(where)) {
    return `Leased topic ${topic} requires exact equality filters for route fields: ${routeBy.join(", ")}.`;
  }
  for (const field of routeBy) {
    if (!exactEqFilterIsPresent(where[field])) {
      return `Leased topic ${topic} route field ${field} must use an exact eq filter.`;
    }
  }
  return undefined;
};
