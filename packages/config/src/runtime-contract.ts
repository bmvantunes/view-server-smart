import type { Clock, Config, Effect } from "effect";
import type { ViewServerHealth } from "./health-contract";
import type {
  LiveQuery,
  LiveQueryResult,
  LiveQueryRow,
  TopicRow,
  UseLiveQuery,
  ValidateLiveQuery,
} from "./topic-contract";

export type ViewServerProviderOptions = {
  readonly url: string;
};

export type ViewServerBackpressureError = {
  readonly _tag: "ViewServerBackpressureError";
  readonly code: "BackpressureExceeded";
  readonly message: string;
  readonly topic?: string;
  readonly queryId?: string;
  readonly queuedEvents?: number;
  readonly maxQueueDepth?: number;
};

export type ViewServerRuntimeError =
  | ViewServerBackpressureError
  | {
      readonly _tag: "ViewServerRuntimeError";
      readonly code:
        | "InvalidTopic"
        | "InvalidRow"
        | "SnapshotStale"
        | "RuntimeUnavailable"
        | "RuntimeResetFailed";
      readonly message: string;
      readonly topic?: string;
    };

export type ViewServerTransportError =
  | ViewServerBackpressureError
  | {
      readonly _tag: "ViewServerTransportError";
      readonly code: "TransportError" | "SubscriptionClosed";
      readonly message: string;
      readonly topic?: string;
      readonly queryId?: string;
    };

export type ViewServerInMemoryRuntime<Topics extends object> = {
  readonly publish: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    row: TopicRow<Topics, Topic>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly publishMany: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    rows: ReadonlyArray<TopicRow<Topics, Topic>>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly patch: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    key: string,
    patch: Partial<TopicRow<Topics, Topic>>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly delete: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    key: string,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly snapshot: <
    Topic extends Extract<keyof Topics, string>,
    const Query extends LiveQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: Query & ValidateLiveQuery<Query>,
  ) => Effect.Effect<
    LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError
  >;
  readonly health: () => Effect.Effect<ViewServerHealth<Topics>, ViewServerRuntimeError>;
  readonly reset: () => Effect.Effect<void, ViewServerRuntimeError>;
};

export type ViewServerInMemoryProviderOptions<Topics extends object> = {
  readonly seed?: {
    readonly [Topic in keyof Topics]?: ReadonlyArray<TopicRow<Topics, Topic>>;
  };
  readonly runtime?: ViewServerInMemoryRuntime<Topics>;
  readonly onRuntime?: (runtime: ViewServerInMemoryRuntime<Topics>) => void;
  readonly clock?: Clock.Clock;
};

export type ReactHookContracts<Topics extends object> = {
  readonly useLiveQuery: UseLiveQuery<Topics>;
  readonly useViewServerHealth: () => ViewServerHealth<Topics>;
  readonly useViewServerTestRuntime: () => ViewServerInMemoryRuntime<Topics>;
};

export type RuntimeEnvironmentConfig = {
  readonly websocketPort: Config.Config<number>;
  readonly tcpPublishPort: Config.Config<number>;
};
