import type { ViewServerRuntimeLiveClient } from "@view-server/client";
import type {
  TopicDefinitions,
  ViewServerHealth,
  ViewServerRuntimeClient,
} from "@view-server/config";
import type { Effect } from "effect";
import type { ViewServerAuth } from "./auth";

export type ViewServerServerRuntime<Topics extends TopicDefinitions> = Pick<
  ViewServerRuntimeClient<Topics>,
  "health"
>;

export type ViewServerWebSocketServerInput<Topics extends TopicDefinitions> = {
  readonly auth?: ViewServerAuth;
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
  readonly runtime: ViewServerServerRuntime<Topics>;
  readonly transport?: {
    readonly clientOpened?: Effect.Effect<void>;
    readonly clientClosed?: Effect.Effect<void>;
    readonly streamOpened?: Effect.Effect<void>;
    readonly streamClosed?: Effect.Effect<void>;
  };
};

export type ViewServerWebSocketServerOptions = {
  readonly host?: string;
  readonly port?: number;
  readonly path?: `/${string}`;
  readonly healthPath?: `/${string}`;
  readonly metricsPath?: `/${string}`;
};

export type ViewServerWebSocketServer = {
  readonly url: string;
  readonly healthUrl: string;
  readonly metricsUrl: string;
  readonly close: Effect.Effect<void>;
};

export type Jsonify<T> = T extends bigint
  ? string
  : T extends string | number | boolean | null
    ? T
    : T extends ReadonlyArray<infer Item>
      ? ReadonlyArray<Jsonify<Item>>
      : T extends object
        ? { readonly [Key in keyof T]: Jsonify<T[Key]> }
        : never;

export type ViewServerHealthHttpJson<Topics extends TopicDefinitions = TopicDefinitions> = Jsonify<
  ViewServerHealth<Topics>
>;
