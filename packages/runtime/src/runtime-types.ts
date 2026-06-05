import type { ViewServerLiveClient } from "@view-server/client";
import type {
  RowSchema,
  TopicDefinitions,
  ViewServerHealth,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
} from "@view-server/config";
import type { Effect, Schema } from "effect";

export type ViewServerRuntimeTopicDefinitions = TopicDefinitions &
  Record<
    string,
    {
      readonly schema: RowSchema & Schema.Decoder<object>;
      readonly key: string;
    }
  >;

type RuntimeHttpPath = `/${string}`;

export type ViewServerRuntimeOptions = {
  readonly host?: string;
  readonly websocketPort?: number;
  readonly rpcPath?: RuntimeHttpPath;
  readonly healthPath?: RuntimeHttpPath;
  readonly subscriptionQueueCapacity?: number;
};

export type ViewServerRuntime<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly url: string;
  readonly healthUrl: string;
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly liveClient: ViewServerLiveClient<Topics>;
  readonly health: () => Effect.Effect<ViewServerHealth<Topics>, ViewServerRuntimeError>;
  readonly close: Effect.Effect<void>;
};
