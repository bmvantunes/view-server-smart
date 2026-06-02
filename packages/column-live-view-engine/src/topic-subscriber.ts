import type { StatusEvent } from "@view-server/config";
import type { Effect } from "effect";
import type { TopicStore } from "./topic-store";

export type LiveTopicSubscriber = {
  readonly topic: string;
  readonly queryId: string;
  readonly notify: (store: TopicStore) => Effect.Effect<void>;
  readonly queuedEvents: Effect.Effect<number>;
  readonly end: Effect.Effect<void>;
  readonly closeWithStatus: (event: StatusEvent) => Effect.Effect<void>;
  maxQueueDepth: number;
  backpressureEvents: number;
  closed: boolean;
};
