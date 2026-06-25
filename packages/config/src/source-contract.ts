export type TopicSourceDefinition = {
  readonly kind: string;
};

export type NonEmptyRouteBy = readonly [string, ...ReadonlyArray<string>];

export type TopicMaterializedSourceDefinition = TopicSourceDefinition & {
  readonly lifecycle: "materialized";
};

export type TopicLeasedSourceDefinition<
  RouteBy extends ReadonlyArray<string> = ReadonlyArray<string>,
> = TopicSourceDefinition & {
  readonly lifecycle: "leased";
  readonly routeBy: RouteBy;
};
