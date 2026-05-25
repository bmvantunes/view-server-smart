import * as AtomReact from "@effect/atom-react";
import type { DecodableTopicDefinitions } from "@view-server/column-live-view-engine";
import type {
  ExactRawQuery,
  LiveQueryResult,
  LiveQueryRow,
  TopicRow,
  TopicSchema,
  ValidateLiveQuery,
  ViewServerConfig,
  ViewServerHealth,
  ViewServerInMemoryRuntime,
} from "@view-server/config";
import { Cause, Effect, Stream, type Schema } from "effect";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import * as Atom from "effect/unstable/reactivity/Atom";
import { createElement, useMemo, type ReactNode } from "react";
import {
  makeProviderState,
  refreshHealth,
  type ProviderInput,
  type ProviderState,
} from "./in-memory-runtime";
import { applyEvent, initialClientState, liveQueryResult } from "./live-query-state";
import { stableQueryKey } from "./query-key";

type ReactBindings<Topics extends DecodableTopicDefinitions> = {
  readonly ViewServerInMemoryProvider: (props: ViewServerInMemoryProviderProps) => ReactNode;
  readonly useLiveQuery: UseLiveQueryHook<Topics>;
  readonly useViewServerHealth: () => ViewServerHealth<Topics>;
  readonly useViewServerTestRuntime: () => ViewServerInMemoryRuntime<Topics>;
};

export type ViewServerInMemoryProviderProps = {
  readonly children: ReactNode;
  readonly subscriptionQueueCapacity?: number;
};

export type UseLiveQueryHook<Topics extends DecodableTopicDefinitions> = <
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

export const createViewServerReact = <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
): ReactBindings<Topics> => {
  const ProviderAtom = AtomReact.make((input: ProviderInput) =>
    Atom.make((get) => {
      const providerState = makeProviderState(config, input);
      get.addFinalizer(() => {
        Effect.runFork(providerState.engine.close());
      });
      return providerState;
    }),
  );

  const useProviderState = (): ProviderState<Topics> => AtomReact.useAtomValue(ProviderAtom.use());

  function ViewServerInMemoryProvider(props: ViewServerInMemoryProviderProps): ReactNode {
    const { children, ...input } = props;
    return createElement(
      AtomReact.RegistryProvider,
      { defaultIdleTTL: 0 },
      createElement(ProviderAtom.Provider, { value: input }, children),
    );
  }

  const useLiveQuery: UseLiveQueryHook<Topics> = (topic, query) => {
    const providerState = useProviderState();
    type Row = LiveQueryRow<Schema.Schema.Type<TopicSchema<Topics, typeof topic>>, typeof query>;
    const queryKey = stableQueryKey(query);
    const liveAtom = useMemo(
      () =>
        Atom.make(
          Stream.scoped(
            Stream.unwrap(
              Effect.gen(function* () {
                const subscription = yield* providerState.engine.subscribe(topic, query);
                yield* refreshHealth(providerState.engine, providerState.health);
                return subscription.events.pipe(
                  Stream.scan(initialClientState<Row>(), applyEvent),
                  Stream.ensuring(
                    subscription
                      .close()
                      .pipe(
                        Effect.andThen(refreshHealth(providerState.engine, providerState.health)),
                      ),
                  ),
                );
              }),
            ),
          ),
        ),
      [providerState, topic, queryKey],
    );
    const result = AtomReact.useAtomValue(liveAtom);
    const emptyState = () => initialClientState<Row>();
    if (AsyncResult.isFailure(result)) {
      const defect = Cause.squash(result.cause);
      return {
        ...liveQueryResult(emptyState()),
        status: "error",
        statusCode: "TransportError",
        message: String(defect),
      };
    }
    return liveQueryResult(AsyncResult.getOrElse(result, emptyState));
  };

  const useViewServerHealth = (): ViewServerHealth<Topics> => {
    const providerState = useProviderState();
    return AtomReact.useAtomRef(providerState.health);
  };

  const useViewServerTestRuntime = (): ViewServerInMemoryRuntime<Topics> =>
    useProviderState().runtime;

  return {
    ViewServerInMemoryProvider,
    useLiveQuery,
    useViewServerHealth,
    useViewServerTestRuntime,
  };
};
