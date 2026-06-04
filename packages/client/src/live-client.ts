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
  ViewServerHealth,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@view-server/config";
import type { Effect, Stream } from "effect";
import type { AtomRef } from "effect/unstable/reactivity";

export type ViewServerLiveEvent<Row> = SnapshotEvent<Row> | DeltaEvent<Row> | StatusEvent;

export type ViewServerLiveSubscription<Row> = {
  readonly events: Stream.Stream<ViewServerLiveEvent<Row>>;
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
    ViewServerLiveSubscription<ViewServerHealthSummaryRow<Topics>>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  readonly subscribeHealth: () => Effect.Effect<
    ViewServerLiveSubscription<ViewServerHealthTopicRow<Extract<keyof Topics, string>>>,
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
