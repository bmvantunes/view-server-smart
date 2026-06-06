import type {
  DeltaEvent,
  ExactLiveQueryInput,
  GroupedQuery,
  LiveQuery,
  LiveQueryRow,
  RawQuery,
  SnapshotEvent,
  StatusEvent,
  TopicDefinitions,
  TopicRow,
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
  ViewServerHealth,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@view-server/config";
import type { Effect, Stream } from "effect";
import type { AtomRef } from "effect/unstable/reactivity";

type RowWithKey<Row, Key extends string> = string extends Key
  ? Row
  : Row extends { readonly id: string }
    ? Row & { readonly id: Key }
    : Row;

type TopicCanChangeCardinality<Topic extends string> = Topic extends
  | typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC
  | typeof VIEW_SERVER_HEALTH_TOPIC
  ? false
  : true;

type TopicSnapshotEvent<Row, Topic extends string, Key extends string> = Omit<
  SnapshotEvent<Row>,
  "topic" | "keys" | "rows" | "totalRows"
> & {
  readonly topic: Topic;
  readonly keys: Topic extends typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC
    ? readonly ["summary"]
    : ReadonlyArray<Key>;
  readonly rows: Topic extends typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC
    ? readonly [RowWithKey<Row, Key>]
    : ReadonlyArray<Row>;
  readonly totalRows: Topic extends typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC ? 1 : number;
};

type TopicInsertOperation<Row, Topic extends string, Key extends string> =
  TopicCanChangeCardinality<Topic> extends true
    ? Key extends string
      ? {
          readonly type: "insert";
          readonly key: Key;
          readonly row: RowWithKey<Row, Key>;
          readonly index: number;
        }
      : never
    : never;

type TopicRemoveOperation<Topic extends string, Key extends string> =
  TopicCanChangeCardinality<Topic> extends true
    ? {
        readonly type: "remove";
        readonly key: Key;
      }
    : never;

type TopicDeltaOperation<Row, Topic extends string, Key extends string> =
  | TopicInsertOperation<Row, Topic, Key>
  | (Key extends string
      ? {
          readonly type: "update";
          readonly key: Key;
          readonly row: RowWithKey<Row, Key>;
          readonly index: number;
        }
      : never)
  | {
      readonly type: "move";
      readonly key: Key;
      readonly fromIndex: number;
      readonly toIndex: number;
    }
  | TopicRemoveOperation<Topic, Key>;

type TopicDeltaEvent<Row, Topic extends string, Key extends string> = Omit<
  DeltaEvent<Row>,
  "topic" | "operations" | "totalRows"
> & {
  readonly topic: Topic;
  readonly operations: ReadonlyArray<TopicDeltaOperation<Row, Topic, Key>>;
  readonly totalRows: Topic extends typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC ? 1 : number;
};

type TopicStatusEvent<Topic extends string> = StatusEvent & {
  readonly topic: Topic;
};

export type ViewServerStatusEvent<Topic extends string = string> = TopicStatusEvent<Topic>;

export type ViewServerLiveEvent<Row, Topic extends string = string, Key extends string = string> =
  | TopicSnapshotEvent<Row, Topic, Key>
  | TopicDeltaEvent<Row, Topic, Key>
  | TopicStatusEvent<Topic>;

export type ViewServerLiveSubscription<
  Row,
  Topic extends string = string,
  Key extends string = string,
> = {
  readonly events: Stream.Stream<ViewServerLiveEvent<Row, Topic, Key>>;
  readonly close: () => Effect.Effect<void, ViewServerTransportError>;
};

export type ViewServerLiveClient<Topics extends TopicDefinitions> = {
  readonly subscribe: {
    <
      Topic extends Extract<keyof Topics, string>,
      const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
    >(
      topic: Topic,
      query: ExactLiveQueryInput<TopicRow<Topics, Topic>, Query>,
    ): Effect.Effect<
      ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
      ViewServerRuntimeError | ViewServerTransportError
    >;
  };
  readonly subscribeHealthSummary: () => Effect.Effect<
    ViewServerLiveSubscription<
      ViewServerHealthSummaryRow<Topics>,
      typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
      "summary"
    >,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  readonly subscribeHealth: () => Effect.Effect<
    ViewServerLiveSubscription<
      ViewServerHealthTopicRow<Extract<keyof Topics, string>>,
      typeof VIEW_SERVER_HEALTH_TOPIC,
      Extract<keyof Topics, string>
    >,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  readonly health: AtomRef.ReadonlyRef<ViewServerHealth<Topics>>;
  readonly close: Effect.Effect<void>;
};

export type ViewServerRuntimeLiveClient<Topics extends TopicDefinitions> =
  ViewServerLiveClient<Topics> & {
    readonly subscribeRuntime: <Topic extends Extract<keyof Topics, string>>(
      topic: Topic,
      query: LiveQuery<TopicRow<Topics, Topic>>,
    ) => Effect.Effect<
      ViewServerLiveSubscription<object>,
      ViewServerRuntimeError | ViewServerTransportError
    >;
  };
