import * as AtomReact from "@effect/atom-react";
import {
  applyEvent,
  initialClientState,
  liveQueryResultFromAsyncResult,
  stableQueryKey,
  type ViewServerLiveClient,
} from "@view-server/client";
import type {
  ExactRawQuery,
  LiveQueryResult,
  LiveQueryRow,
  TopicDefinitions,
  TopicRow,
  ValidateLiveQuery,
  ViewServerConfig,
  ViewServerHealth,
} from "@view-server/config";
import { Effect, Stream } from "effect";
import * as Atom from "effect/unstable/reactivity/Atom";
import { createContext, useContext, useMemo, type ReactNode } from "react";
import { ViewServerReactConfig } from "./internal";

export type ViewServerReactBindings<Topics extends TopicDefinitions> = {
  readonly [ViewServerReactConfig]: ViewServerConfig<Topics>;
  readonly useLiveQuery: UseLiveQueryHook<Topics>;
  readonly useViewServerHealth: () => ViewServerHealth<Topics>;
  readonly ViewServerProvider: (props: ViewServerProviderProps<Topics>) => ReactNode;
};

export type ViewServerProviderProps<Topics extends TopicDefinitions> = {
  readonly client: ViewServerLiveClient<Topics>;
  readonly children?: ReactNode;
};

export type UseLiveQueryHook<Topics extends TopicDefinitions> = <
  Topic extends Extract<keyof Topics, string>,
  const Query extends { readonly select: ReadonlyArray<unknown> },
>(
  topic: Topic,
  query: Query & ExactRawQuery<TopicRow<Topics, Topic>, Query> & ValidateLiveQuery<Query>,
) => LiveQueryResult<
  LiveQueryRow<
    TopicRow<Topics, Topic>,
    Query & ExactRawQuery<TopicRow<Topics, Topic>, Query> & ValidateLiveQuery<Query>
  >
>;

export const createViewServerReact = <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
): ViewServerReactBindings<Topics> => {
  const ClientContext = createContext<ViewServerLiveClient<Topics> | null>(null);

  const useClient = (): ViewServerLiveClient<Topics> => {
    const client = useContext(ClientContext);
    if (client === null) {
      throw new Error("ViewServerProvider is missing a client.");
    }
    return client;
  };

  function ViewServerProvider(props: ViewServerProviderProps<Topics>): ReactNode {
    return (
      <AtomReact.RegistryProvider>
        <ClientContext.Provider value={props.client}>{props.children}</ClientContext.Provider>
      </AtomReact.RegistryProvider>
    );
  }

  const useLiveQuery: UseLiveQueryHook<Topics> = (topic, query) => {
    const client = useClient();
    type Row = LiveQueryRow<TopicRow<Topics, typeof topic>, typeof query>;
    const queryKey = stableQueryKey(query);
    const liveAtom = useMemo(
      () =>
        Atom.make(
          Stream.scoped(
            Stream.unwrap(
              Effect.gen(function* () {
                const subscription = yield* client.subscribe(topic, query);
                return subscription.events.pipe(
                  Stream.scan(initialClientState<Row>(), applyEvent),
                  Stream.ensuring(subscription.close().pipe(Effect.ignore)),
                );
              }),
            ),
          ),
        ),
      [client, topic, queryKey],
    );
    const result = AtomReact.useAtomValue(liveAtom);
    return liveQueryResultFromAsyncResult<Row>(result);
  };

  const useViewServerHealth = (): ViewServerHealth<Topics> => {
    const client = useClient();
    return AtomReact.useAtomRef(client.health);
  };

  return {
    [ViewServerReactConfig]: config,
    useLiveQuery,
    useViewServerHealth,
    ViewServerProvider,
  };
};
