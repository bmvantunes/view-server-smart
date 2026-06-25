import type { Config, Effect } from "effect";
import type { ViewServerHealth } from "./health-contract";
import type { ExactLiveQueryInputForTopic } from "./source-query-contract";
import type {
  ExactPatch,
  GroupedQuery,
  LiveQueryRow,
  LiveQueryResult,
  RawQuery,
  TopicRow,
} from "./topic-contract";

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
        | "InvalidQuery"
        | "UnsupportedQuery"
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

type RuntimeSnapshot<Topics extends object> = <
  Topic extends Extract<keyof Topics, string>,
  const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
>(
  topic: Topic,
  query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
) => Effect.Effect<
  LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
  ViewServerRuntimeError
>;

export type ViewServerRuntimeClient<Topics extends object> = {
  readonly publish: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    row: TopicRow<Topics, Topic>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly publishMany: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    rows: ReadonlyArray<TopicRow<Topics, Topic>>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly patch: <Topic extends Extract<keyof Topics, string>, const Patch>(
    topic: Topic,
    key: string,
    patch: Patch & Partial<TopicRow<Topics, Topic>> & ExactPatch<TopicRow<Topics, Topic>, Patch>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly delete: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    key: string,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly snapshot: RuntimeSnapshot<Topics>;
  readonly health: () => Effect.Effect<ViewServerHealth<Topics>, ViewServerRuntimeError>;
  readonly reset: () => Effect.Effect<void, ViewServerRuntimeError>;
};

export type RuntimeEnvironmentConfig = {
  readonly websocketPort: Config.Config<number>;
};
