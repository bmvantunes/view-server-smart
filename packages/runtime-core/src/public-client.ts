import type {
  ViewServerLiveClient,
  ViewServerLiveSubscription,
  ViewServerRuntimeLiveClient,
} from "@view-server/client";
import type {
  ExactLiveQueryInputForTopic,
  ExactPatch,
  GroupedQuery,
  LiveQueryRow,
  LiveQueryResult,
  RawQuery,
  TopicDefinitions,
  TopicRouteBy,
  TopicRow,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@view-server/config";
import type { Effect } from "effect";

type RuntimeCorePublicTopic<Topics extends TopicDefinitions> = Extract<
  {
    readonly [Topic in keyof Topics]: [TopicRouteBy<Topics, Topic>] extends [never] ? Topic : never;
  }[keyof Topics],
  string
>;

type RuntimeCoreLeasedTopic<Topics extends TopicDefinitions> = Extract<
  {
    readonly [Topic in keyof Topics]: [TopicRouteBy<Topics, Topic>] extends [never] ? never : Topic;
  }[keyof Topics],
  string
>;

type RuntimeCorePublicReset<Topics extends TopicDefinitions> = [
  RuntimeCoreLeasedTopic<Topics>,
] extends [never]
  ? {
      readonly reset: ViewServerRuntimeClient<Topics>["reset"];
    }
  : {
      readonly reset: (...args: never) => ReturnType<ViewServerRuntimeClient<Topics>["reset"]>;
    };

type RuntimeCorePublicSnapshot<Topics extends TopicDefinitions> = <
  Topic extends RuntimeCorePublicTopic<Topics>,
  const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
>(
  topic: Topic,
  query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
) => Effect.Effect<
  LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
  ViewServerRuntimeError
>;

type RuntimeCorePublicSubscribe<Topics extends TopicDefinitions> = <
  Topic extends RuntimeCorePublicTopic<Topics>,
  const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
>(
  topic: Topic,
  query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
) => Effect.Effect<
  ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
  ViewServerRuntimeError | ViewServerTransportError
>;

type RuntimeCorePublicSubscribeRuntime<Topics extends TopicDefinitions> = <
  Topic extends RuntimeCorePublicTopic<Topics>,
>(
  topic: Topic,
  query: RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
) => Effect.Effect<
  ViewServerLiveSubscription<object>,
  ViewServerRuntimeError | ViewServerTransportError
>;

export type ViewServerRuntimeCorePublicClient<Topics extends TopicDefinitions> = Omit<
  ViewServerRuntimeClient<Topics>,
  "delete" | "patch" | "publish" | "publishMany" | "reset" | "snapshot"
> & {
  readonly publish: <Topic extends RuntimeCorePublicTopic<Topics>>(
    topic: Topic,
    row: TopicRow<Topics, Topic>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly publishMany: <Topic extends RuntimeCorePublicTopic<Topics>>(
    topic: Topic,
    rows: ReadonlyArray<TopicRow<Topics, Topic>>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly patch: <Topic extends RuntimeCorePublicTopic<Topics>, const Patch>(
    topic: Topic,
    key: string,
    patch: Patch & Partial<TopicRow<Topics, Topic>> & ExactPatch<TopicRow<Topics, Topic>, Patch>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly delete: <Topic extends RuntimeCorePublicTopic<Topics>>(
    topic: Topic,
    key: string,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly snapshot: RuntimeCorePublicSnapshot<Topics>;
} & RuntimeCorePublicReset<Topics>;

export type ViewServerRuntimeCorePublicLiveClient<Topics extends TopicDefinitions> = [
  RuntimeCoreLeasedTopic<Topics>,
] extends [never]
  ? ViewServerLiveClient<Topics>
  : Omit<ViewServerLiveClient<Topics>, "subscribe"> & {
      readonly subscribe: RuntimeCorePublicSubscribe<Topics>;
    };

export type ViewServerRuntimeCoreServerLiveClient<Topics extends TopicDefinitions> = [
  RuntimeCoreLeasedTopic<Topics>,
] extends [never]
  ? ViewServerRuntimeLiveClient<Topics>
  : Omit<ViewServerRuntimeLiveClient<Topics>, "subscribe" | "subscribeRuntime"> & {
      readonly subscribe: RuntimeCorePublicSubscribe<Topics>;
      readonly subscribeRuntime: RuntimeCorePublicSubscribeRuntime<Topics>;
    };
