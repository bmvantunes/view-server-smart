import type {
  DescMessage,
  DescMethodServerStreaming,
  DescService,
  MessageInitShape,
  MessageShape,
} from "@bufbuild/protobuf";
import type { Client } from "@connectrpc/connect";
import type { Config, Effect, Stream } from "effect";
import type { FieldKey, RowSchema, TopicRow } from "./query-core";
import type { RejectExtraKeys } from "./query-exact";
import type { TopicRouteByTuple } from "./source-query-contract";
import type {
  NonEmptyRouteBy,
  TopicLeasedSourceDefinition,
  TopicMaterializedSourceDefinition,
} from "./source-contract";

const GrpcTopicSourceTypeId: unique symbol = Symbol("@view-server/config/GrpcTopicSource");
const GrpcFeedDefinitionTypeId: unique symbol = Symbol("@view-server/config/GrpcFeedDefinition");
const GrpcFeedMapTypeId: unique symbol = Symbol("@view-server/config/GrpcFeedMap");
const grpcFeedMapBrand = { [GrpcFeedMapTypeId]: true } as const;

const brandGrpcFeedMap = <Mapping extends (...args: ReadonlyArray<never>) => unknown>(
  mapping: Mapping,
) => Object.assign(mapping, grpcFeedMapBrand);

export type GrpcTopicSourceLifecycle = "materialized" | "leased";

export type GrpcMaterializedTopicSource = TopicMaterializedSourceDefinition & {
  readonly _tag: "GrpcMaterializedTopicSource";
  readonly [GrpcTopicSourceTypeId]: true;
  readonly kind: "grpc";
  readonly lifecycle: "materialized";
};

export type GrpcLeasedTopicSource<RouteBy extends ReadonlyArray<string> = ReadonlyArray<string>> =
  TopicLeasedSourceDefinition<RouteBy> & {
    readonly _tag: "GrpcLeasedTopicSource";
    readonly [GrpcTopicSourceTypeId]: true;
    readonly kind: "grpc";
    readonly lifecycle: "leased";
    readonly routeBy: RouteBy;
  };

export type GrpcTopicSource = GrpcMaterializedTopicSource | GrpcLeasedTopicSource;

type ExactObject<Candidate, Shape> = Candidate & RejectExtraKeys<Candidate, Shape>;

type ExactGrpcLeasedTopicSourceInput<Input> = Input &
  RejectExtraKeys<
    Input,
    {
      readonly routeBy: NonEmptyRouteBy;
    }
  >;

export type GrpcMaterializedTopic<Topics> = Extract<
  {
    readonly [Topic in keyof Topics]: Topics[Topic] extends {
      readonly source: GrpcMaterializedTopicSource;
    }
      ? Topic
      : never;
  }[keyof Topics],
  string
>;

export type GrpcLeasedTopic<Topics> = Extract<
  {
    readonly [Topic in keyof Topics]: [TopicRouteByTuple<Topics, Topic>] extends [never]
      ? never
      : TopicRouteByTuple<Topics, Topic> extends NonEmptyRouteBy
        ? Topic
        : never;
  }[keyof Topics],
  string
>;

type GrpcLeasedRouteBy<Topics, Topic extends GrpcLeasedTopic<Topics>> =
  TopicRouteByTuple<Topics, Topic> extends NonEmptyRouteBy
    ? TopicRouteByTuple<Topics, Topic>
    : never;

type RouteShape<
  Topics extends Record<string, { readonly schema: RowSchema }>,
  Topic extends Extract<keyof Topics, string>,
  RouteBy extends ReadonlyArray<string>,
> = Pick<TopicRow<Topics, Topic>, Extract<RouteBy[number], FieldKey<TopicRow<Topics, Topic>>>>;

type ExactGrpcFeedMap<Input, Row, Mapping extends (input: Input) => Row> = Mapping &
  ((input: Input) => ExactObject<ReturnType<Mapping>, Row>);

type GrpcFeedMapDefinition<Input, Row, Mapping extends (input: Input) => Row> = ExactGrpcFeedMap<
  Input,
  Row,
  Mapping
> & {
  readonly [GrpcFeedMapTypeId]: true;
};

export type GrpcHelper = {
  readonly materialized: () => GrpcMaterializedTopicSource;
  readonly leased: <const Input extends { readonly routeBy: NonEmptyRouteBy }>(
    input: ExactGrpcLeasedTopicSourceInput<Input>,
  ) => GrpcLeasedTopicSource<Input["routeBy"]>;
  readonly connectClient: <
    const Input extends {
      readonly service: DescService;
      readonly baseUrl: GrpcRuntimeValue<string>;
    },
  >(
    input: ExactGrpcConnectClientInput<Input>,
  ) => GrpcConnectClientDefinition<Input["service"]>;
};

export type GrpcRuntimeValue<A> = A | Config.Config<A>;

export type GrpcConnectClientDefinition<Service extends DescService = DescService> = {
  readonly _tag: "GrpcConnectClientDefinition";
  readonly service: Service;
  readonly baseUrl: GrpcRuntimeValue<string>;
  readonly protocol: "grpc";
};

export type GrpcRuntimeClients = Record<string, GrpcConnectClientDefinition>;

export type GrpcClientDefinitionService<ClientDefinition extends GrpcConnectClientDefinition> =
  ClientDefinition extends GrpcConnectClientDefinition<infer Service> ? Service : never;

export type GrpcClientValue<ClientDefinition extends GrpcConnectClientDefinition> = Client<
  GrpcClientDefinitionService<ClientDefinition>
>;

export type GrpcServerStreamingMethodName<ClientDefinition> =
  ClientDefinition extends GrpcConnectClientDefinition<infer Service>
    ? DescService extends Service
      ? string
      : {
          readonly [MethodName in keyof Service["method"]]: Service["method"][MethodName] extends DescMethodServerStreaming<
            infer _Input extends DescMessage,
            infer _Output extends DescMessage
          >
            ? MethodName
            : never;
        }[keyof Service["method"]] &
          string
    : never;

export type GrpcMethodRequest<
  ClientDefinition,
  MethodName extends GrpcServerStreamingMethodName<ClientDefinition>,
> =
  ClientDefinition extends GrpcConnectClientDefinition<infer Service>
    ? DescService extends Service
      ? unknown
      : Service["method"][MethodName] extends DescMethodServerStreaming<
            infer Input extends DescMessage,
            infer _Output extends DescMessage
          >
        ? MessageInitShape<Input>
        : never
    : never;

export type GrpcMethodValue<
  ClientDefinition,
  MethodName extends GrpcServerStreamingMethodName<ClientDefinition>,
> =
  ClientDefinition extends GrpcConnectClientDefinition<infer Service>
    ? DescService extends Service
      ? unknown
      : Service["method"][MethodName] extends DescMethodServerStreaming<
            infer _Input extends DescMessage,
            infer Output extends DescMessage
          >
        ? MessageShape<Output>
        : never
    : never;

export type GrpcFeedSession = {
  readonly id: string | null;
  readonly forwardedHeaders: Readonly<Record<string, string>>;
  readonly systemHeaders: Readonly<Record<string, string>>;
};

export type GrpcFeedAcquireInput<ClientValue, Request, Route> = {
  readonly client: ClientValue;
  readonly request: Request;
  readonly route: Route;
  readonly session: GrpcFeedSession;
};

export type GrpcFeedReleaseInput<ClientValue, Request, Route> = GrpcFeedAcquireInput<
  ClientValue,
  Request,
  Route
>;

export type GrpcFeedMapInput<Value, Route, SchemaValue extends RowSchema> = {
  readonly value: Value;
  readonly route: Route;
  readonly schema: SchemaValue;
};

export type GrpcLeasedFeedDefinition<
  Topics extends Record<string, { readonly schema: RowSchema }>,
  Clients extends GrpcRuntimeClients,
  Topic extends Extract<keyof Topics, string>,
  ClientName extends Extract<keyof Clients, string>,
  RouteBy extends NonEmptyRouteBy,
  MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
  Mapping extends (
    input: GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      RouteShape<Topics, Topic, RouteBy>,
      Topics[Topic]["schema"]
    >,
  ) => TopicRow<Topics, Topic> = (
    input: GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      RouteShape<Topics, Topic, RouteBy>,
      Topics[Topic]["schema"]
    >,
  ) => TopicRow<Topics, Topic>,
> = {
  readonly _tag: "GrpcLeasedFeedDefinition";
  readonly [GrpcFeedDefinitionTypeId]: {
    readonly topic: Topic;
    readonly client: ClientName;
    readonly method: MethodName;
  };
  readonly lifecycle: "leased";
  readonly topic: Topic;
  readonly client: ClientName;
  readonly method: MethodName;
  readonly routeBy: RouteBy;
  readonly request: (
    route: RouteShape<Topics, Topic, RouteBy>,
  ) => GrpcMethodRequest<Clients[ClientName], MethodName>;
  readonly acquire: (
    input: GrpcFeedAcquireInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      RouteShape<Topics, Topic, RouteBy>
    >,
  ) => Stream.Stream<GrpcMethodValue<Clients[ClientName], MethodName>, unknown, never>;
  readonly release?: (
    input: GrpcFeedReleaseInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      RouteShape<Topics, Topic, RouteBy>
    >,
  ) => Effect.Effect<void, unknown, never>;
  readonly map: GrpcFeedMapDefinition<
    GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      RouteShape<Topics, Topic, RouteBy>,
      Topics[Topic]["schema"]
    >,
    TopicRow<Topics, Topic>,
    Mapping
  >;
};

export type GrpcMaterializedFeedDefinition<
  Topics extends Record<string, { readonly schema: RowSchema }>,
  Clients extends GrpcRuntimeClients,
  Topic extends GrpcMaterializedTopic<Topics>,
  ClientName extends Extract<keyof Clients, string>,
  MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
  Mapping extends (
    input: GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      undefined,
      Topics[Topic]["schema"]
    >,
  ) => TopicRow<Topics, Topic> = (
    input: GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      undefined,
      Topics[Topic]["schema"]
    >,
  ) => TopicRow<Topics, Topic>,
> = {
  readonly _tag: "GrpcMaterializedFeedDefinition";
  readonly [GrpcFeedDefinitionTypeId]: {
    readonly topic: Topic;
    readonly client: ClientName;
    readonly method: MethodName;
  };
  readonly lifecycle: "materialized";
  readonly topic: Topic;
  readonly client: ClientName;
  readonly method: MethodName;
  readonly request: () => GrpcMethodRequest<Clients[ClientName], MethodName>;
  readonly acquire: (
    input: GrpcFeedAcquireInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      undefined
    >,
  ) => Stream.Stream<GrpcMethodValue<Clients[ClientName], MethodName>, unknown, never>;
  readonly release?: (
    input: GrpcFeedReleaseInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      undefined
    >,
  ) => Effect.Effect<void, unknown, never>;
  readonly map: GrpcFeedMapDefinition<
    GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      undefined,
      Topics[Topic]["schema"]
    >,
    TopicRow<Topics, Topic>,
    Mapping
  >;
};

type ExactGrpcConnectClientInput<Input> = Input &
  RejectExtraKeys<
    Input,
    {
      readonly service: DescService;
      readonly baseUrl: GrpcRuntimeValue<string>;
    }
  >;

type ExactGrpcLeasedFeedInput<
  Topics extends Record<string, { readonly schema: RowSchema }>,
  Clients extends GrpcRuntimeClients,
  Topic extends Extract<keyof Topics, string>,
  ClientName extends Extract<keyof Clients, string>,
  RouteBy extends TopicRouteByTuple<Topics, Topic>,
  MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
  Mapping extends (
    input: GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      RouteShape<Topics, Topic, RouteBy>,
      Topics[Topic]["schema"]
    >,
  ) => TopicRow<Topics, Topic>,
> = {
  readonly topic: Topic;
  readonly client: ClientName;
  readonly method: MethodName;
  readonly routeBy: RouteBy;
  readonly request: (
    route: RouteShape<Topics, Topic, RouteBy>,
  ) => GrpcMethodRequest<Clients[ClientName], MethodName>;
  readonly acquire: (
    input: GrpcFeedAcquireInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      RouteShape<Topics, Topic, RouteBy>
    >,
  ) => Stream.Stream<GrpcMethodValue<Clients[ClientName], MethodName>, unknown, never>;
  readonly release?: (
    input: GrpcFeedReleaseInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      RouteShape<Topics, Topic, RouteBy>
    >,
  ) => Effect.Effect<void, unknown, never>;
  readonly map: ExactGrpcFeedMap<
    GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      RouteShape<Topics, Topic, RouteBy>,
      Topics[Topic]["schema"]
    >,
    TopicRow<Topics, Topic>,
    Mapping
  >;
};

type ExactGrpcMaterializedFeedInput<
  Topics extends Record<string, { readonly schema: RowSchema }>,
  Clients extends GrpcRuntimeClients,
  Topic extends GrpcMaterializedTopic<Topics>,
  ClientName extends Extract<keyof Clients, string>,
  MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
  Mapping extends (
    input: GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      undefined,
      Topics[Topic]["schema"]
    >,
  ) => TopicRow<Topics, Topic>,
> = {
  readonly topic: Topic;
  readonly client: ClientName;
  readonly method: MethodName;
  readonly request: () => GrpcMethodRequest<Clients[ClientName], MethodName>;
  readonly acquire: (
    input: GrpcFeedAcquireInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      undefined
    >,
  ) => Stream.Stream<GrpcMethodValue<Clients[ClientName], MethodName>, unknown, never>;
  readonly release?: (
    input: GrpcFeedReleaseInput<
      GrpcClientValue<Clients[ClientName]>,
      GrpcMethodRequest<Clients[ClientName], MethodName>,
      undefined
    >,
  ) => Effect.Effect<void, unknown, never>;
  readonly map: ExactGrpcFeedMap<
    GrpcFeedMapInput<
      GrpcMethodValue<Clients[ClientName], MethodName>,
      undefined,
      Topics[Topic]["schema"]
    >,
    TopicRow<Topics, Topic>,
    Mapping
  >;
};

export type GrpcFeedHelper<
  Topics extends Record<string, { readonly schema: RowSchema }>,
  Clients extends GrpcRuntimeClients,
> = {
  readonly leasedFeed: <
    const Topic extends GrpcLeasedTopic<Topics>,
    const ClientName extends Extract<keyof Clients, string>,
    const RouteBy extends GrpcLeasedRouteBy<Topics, Topic>,
    const MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
    const Mapping extends (
      input: GrpcFeedMapInput<
        GrpcMethodValue<Clients[ClientName], MethodName>,
        RouteShape<Topics, Topic, RouteBy>,
        Topics[Topic]["schema"]
      >,
    ) => TopicRow<Topics, Topic>,
  >(
    input: ExactGrpcLeasedFeedInput<
      Topics,
      Clients,
      Topic,
      ClientName,
      RouteBy,
      MethodName,
      Mapping
    >,
  ) => GrpcLeasedFeedDefinition<Topics, Clients, Topic, ClientName, RouteBy, MethodName, Mapping>;
  readonly materializedFeed: <
    const Topic extends GrpcMaterializedTopic<Topics>,
    const ClientName extends Extract<keyof Clients, string>,
    const MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
    const Mapping extends (
      input: GrpcFeedMapInput<
        GrpcMethodValue<Clients[ClientName], MethodName>,
        undefined,
        Topics[Topic]["schema"]
      >,
    ) => TopicRow<Topics, Topic>,
  >(
    input: ExactGrpcMaterializedFeedInput<Topics, Clients, Topic, ClientName, MethodName, Mapping>,
  ) => GrpcMaterializedFeedDefinition<Topics, Clients, Topic, ClientName, MethodName, Mapping>;
};

export type AnyGrpcMaterializedFeedDefinition<
  Topics extends Record<string, { readonly schema: RowSchema }>,
  Clients extends GrpcRuntimeClients,
> = {
  readonly [Topic in GrpcMaterializedTopic<Topics>]: {
    readonly [ClientName in Extract<keyof Clients, string>]: {
      readonly [MethodName in GrpcServerStreamingMethodName<
        Clients[ClientName]
      >]: GrpcMaterializedFeedDefinition<Topics, Clients, Topic, ClientName, MethodName>;
    }[GrpcServerStreamingMethodName<Clients[ClientName]>];
  }[Extract<keyof Clients, string>];
}[GrpcMaterializedTopic<Topics>];

export type AnyGrpcLeasedFeedDefinition<
  Topics extends Record<string, { readonly schema: RowSchema }>,
  Clients extends GrpcRuntimeClients,
> = {
  readonly [Topic in GrpcLeasedTopic<Topics>]: {
    readonly [ClientName in Extract<keyof Clients, string>]: {
      readonly [MethodName in GrpcServerStreamingMethodName<
        Clients[ClientName]
      >]: GrpcLeasedFeedDefinition<
        Topics,
        Clients,
        Topic,
        ClientName,
        GrpcLeasedRouteBy<Topics, Topic>,
        MethodName
      >;
    }[GrpcServerStreamingMethodName<Clients[ClientName]>];
  }[Extract<keyof Clients, string>];
}[GrpcLeasedTopic<Topics>];

export type GrpcFeedDefinition<
  Topics extends Record<string, { readonly schema: RowSchema }>,
  Clients extends GrpcRuntimeClients,
> =
  | AnyGrpcMaterializedFeedDefinition<Topics, Clients>
  | AnyGrpcLeasedFeedDefinition<Topics, Clients>;

export const grpc: GrpcHelper = {
  materialized: () => ({
    _tag: "GrpcMaterializedTopicSource",
    [GrpcTopicSourceTypeId]: true,
    kind: "grpc",
    lifecycle: "materialized",
  }),
  leased: (input) => ({
    _tag: "GrpcLeasedTopicSource",
    [GrpcTopicSourceTypeId]: true,
    kind: "grpc",
    lifecycle: "leased",
    routeBy: input.routeBy,
  }),
  connectClient: (input) => ({
    _tag: "GrpcConnectClientDefinition",
    service: input.service,
    baseUrl: input.baseUrl,
    protocol: "grpc",
  }),
};

export const defineGrpcFeed = <
  const Topics extends Record<string, { readonly schema: RowSchema }>,
  const Clients extends GrpcRuntimeClients,
>(): GrpcFeedHelper<Topics, Clients> => ({
  leasedFeed: (input) => ({
    _tag: "GrpcLeasedFeedDefinition",
    [GrpcFeedDefinitionTypeId]: {
      topic: input.topic,
      client: input.client,
      method: input.method,
    },
    lifecycle: "leased",
    topic: input.topic,
    client: input.client,
    method: input.method,
    routeBy: input.routeBy,
    request: input.request,
    acquire: input.acquire,
    map: brandGrpcFeedMap(input.map),
    ...(input.release === undefined ? {} : { release: input.release }),
  }),
  materializedFeed: (input) => ({
    _tag: "GrpcMaterializedFeedDefinition",
    [GrpcFeedDefinitionTypeId]: {
      topic: input.topic,
      client: input.client,
      method: input.method,
    },
    lifecycle: "materialized",
    topic: input.topic,
    client: input.client,
    method: input.method,
    request: input.request,
    acquire: input.acquire,
    map: brandGrpcFeedMap(input.map),
    ...(input.release === undefined ? {} : { release: input.release }),
  }),
});
