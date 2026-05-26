import type {
  DeltaEvent,
  ExactRawQuery,
  LiveQueryRow,
  SnapshotEvent,
  StatusEvent,
  TopicDefinitions,
  TopicRow,
  ValidateLiveQuery,
  ViewServerHealth,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@view-server/config";
import type { Effect, Stream } from "effect";
import type * as AtomRef from "effect/unstable/reactivity/AtomRef";

export type ViewServerLiveEvent<Row> = SnapshotEvent<Row> | DeltaEvent<Row> | StatusEvent;

export type ViewServerLiveSubscription<Row> = {
  readonly events: Stream.Stream<ViewServerLiveEvent<Row>>;
  readonly close: () => Effect.Effect<void, ViewServerTransportError>;
};

export type ViewServerLiveClient<Topics extends TopicDefinitions> = {
  readonly subscribe: <
    Topic extends Extract<keyof Topics, string>,
    const Query extends { readonly select: ReadonlyArray<unknown> },
  >(
    topic: Topic,
    query: Query & ExactRawQuery<TopicRow<Topics, Topic>, Query> & ValidateLiveQuery<Query>,
  ) => Effect.Effect<
    ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
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
