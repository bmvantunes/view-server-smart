import * as AtomReact from "@effect/atom-react";
import {
  applyEvent,
  initialClientState,
  liveQueryResultFromAsyncResult,
  stableQueryKey,
  type ViewServerLiveClient,
  type ViewServerLiveSubscription,
} from "@view-server/client";
import { makeViewServerClient, type ViewServerClientOptions } from "@view-server/client/remote";
import type {
  ExactRawQuery,
  LiveQueryResult,
  LiveQueryRow,
  TopicDefinitions,
  TopicRow,
  ValidateLiveQuery,
  ViewServerConfig,
  ViewServerHealthConnectionStatus,
  ViewServerHealthDetails,
  ViewServerHealthSummary,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
} from "@view-server/config";
import { Effect, Stream } from "effect";
import * as Atom from "effect/unstable/reactivity/Atom";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import { createContext, useContext, useMemo, type ReactNode } from "react";
import { ViewServerReactClientProvider, ViewServerReactConfig } from "./internal";

export type ViewServerReactBindings<Topics extends TopicDefinitions> = {
  readonly [ViewServerReactConfig]: ViewServerConfig<Topics>;
  readonly [ViewServerReactClientProvider]: (
    props: ViewServerClientProviderProps<Topics>,
  ) => ReactNode;
  readonly useLiveQuery: UseLiveQueryHook<Topics>;
  readonly useViewServerHealth: () => ViewServerHealthDetails<Extract<keyof Topics, string>>;
  readonly useViewServerHealthSummary: () => ViewServerHealthSummary<Topics>;
  readonly ViewServerProvider: (props: ViewServerProviderProps) => ReactNode;
};

type ViewServerClientProviderProps<Topics extends TopicDefinitions> = {
  readonly client: ViewServerLiveClient<Topics>;
  readonly children?: ReactNode;
};

export type ViewServerProviderProps = ViewServerClientOptions & {
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
  const RemoteClientAtom = AtomReact.make((options: ViewServerClientOptions) =>
    Atom.make((get) =>
      Effect.gen(function* () {
        const services = yield* Effect.context();
        const client = yield* makeViewServerClient(config, options);
        get.addFinalizer(() => {
          Effect.runForkWith(services)(client.close);
        });
        return client;
      }),
    ),
  );

  const useClient = (): ViewServerLiveClient<Topics> => {
    const client = useContext(ClientContext);
    if (client === null) {
      throw new Error("ViewServerProvider is missing a client.");
    }
    return client;
  };

  function ViewServerClientProvider(props: ViewServerClientProviderProps<Topics>): ReactNode {
    return (
      <AtomReact.RegistryProvider>
        <ClientContext.Provider value={props.client}>{props.children}</ClientContext.Provider>
      </AtomReact.RegistryProvider>
    );
  }

  function RemoteClientBoundary(props: { readonly children?: ReactNode }): ReactNode {
    const result = AtomReact.useAtomValue(RemoteClientAtom.use());
    if (AsyncResult.isSuccess(result)) {
      return <ClientContext.Provider value={result.value}>{props.children}</ClientContext.Provider>;
    }
    if (AsyncResult.isFailure(result)) {
      throw new Error(String(result.cause));
    }
    return null;
  }

  function ViewServerProvider(props: ViewServerProviderProps): ReactNode {
    const options = {
      url: props.url,
      ...(props.subscriptionBufferSize === undefined
        ? {}
        : { subscriptionBufferSize: props.subscriptionBufferSize }),
    } satisfies ViewServerClientOptions;
    const providerKey = [props.url, String(props.subscriptionBufferSize ?? "")].join(":");
    return (
      <AtomReact.RegistryProvider>
        <RemoteClientAtom.Provider key={providerKey} value={options}>
          <RemoteClientBoundary>{props.children}</RemoteClientBoundary>
        </RemoteClientAtom.Provider>
      </AtomReact.RegistryProvider>
    );
  }

  const useSubscription = <Row,>(
    subscriptionKey: string,
    subscribe: () => Effect.Effect<ViewServerLiveSubscription<Row>, unknown>,
  ): LiveQueryResult<Row> => {
    const liveAtom = useMemo(
      () =>
        Atom.make(
          Stream.scoped(
            Stream.unwrap(
              Effect.gen(function* () {
                const subscription = yield* subscribe();
                return subscription.events.pipe(
                  Stream.scan(initialClientState<Row>(), applyEvent),
                  Stream.ensuring(subscription.close().pipe(Effect.ignore)),
                );
              }),
            ),
          ),
        ),
      [subscriptionKey],
    );
    const result = AtomReact.useAtomValue(liveAtom);
    return liveQueryResultFromAsyncResult<Row>(result);
  };

  const useLiveQuery: UseLiveQueryHook<Topics> = (topic, query) => {
    const client = useClient();
    type Row = LiveQueryRow<TopicRow<Topics, typeof topic>, typeof query>;
    const queryKey = stableQueryKey(query);
    return useSubscription<Row>(`${client.health.key}:query:${topic}:${queryKey}`, () =>
      client.subscribe(topic, query),
    );
  };

  const connectionStatusFromLiveQueryStatus = (
    status: LiveQueryResult<unknown>["status"],
  ): ViewServerHealthConnectionStatus => {
    if (status === "loading") {
      return "connecting";
    }
    if (status === "ready" || status === "stale") {
      return "connected";
    }
    return "disconnected";
  };

  const emptySummary = (
    connectionStatus: ViewServerHealthConnectionStatus,
  ): ViewServerHealthSummary<Topics> => ({
    status: connectionStatus === "connected" ? "starting" : connectionStatus,
    runtimeStatus: "starting",
    connectionStatus,
    unhealthyTopics: [],
    updatedAtNanos: 0n,
    maxKafkaLag: 0n,
  });

  const summaryFromRow = (
    row: ViewServerHealthSummaryRow<Topics>,
    connectionStatus: ViewServerHealthConnectionStatus,
  ): ViewServerHealthSummary<Topics> => ({
    status: connectionStatus === "connected" ? row.runtimeStatus : connectionStatus,
    runtimeStatus: row.runtimeStatus,
    connectionStatus,
    unhealthyTopics: row.unhealthyTopics,
    updatedAtNanos: row.updatedAtNanos,
    maxKafkaLag: row.maxKafkaLag,
  });

  const useViewServerHealthSummary = (): ViewServerHealthSummary<Topics> => {
    const client = useClient();
    const result = useSubscription<ViewServerHealthSummaryRow<Topics>>(
      `${client.health.key}:health-summary`,
      client.subscribeHealthSummary,
    );
    const connectionStatus = connectionStatusFromLiveQueryStatus(result.status);
    const row = result.rows[0];
    return row === undefined
      ? emptySummary(connectionStatus)
      : summaryFromRow(row, connectionStatus);
  };

  const useViewServerHealth = (): ViewServerHealthDetails<Extract<keyof Topics, string>> => {
    const client = useClient();
    const summary = useViewServerHealthSummary();
    const result = useSubscription<ViewServerHealthTopicRow<Extract<keyof Topics, string>>>(
      `${client.health.key}:health`,
      client.subscribeHealth,
    );
    const detailConnectionStatus = connectionStatusFromLiveQueryStatus(result.status);
    const connectionStatus =
      summary.connectionStatus === "connected" ? detailConnectionStatus : summary.connectionStatus;
    const status = connectionStatus === "connected" ? summary.runtimeStatus : connectionStatus;
    const statusCode =
      status !== "ready" && result.statusCode === "Ready" ? undefined : result.statusCode;
    return {
      ...result,
      runtimeStatus: summary.runtimeStatus,
      status,
      statusCode,
      connectionStatus,
    };
  };

  return {
    [ViewServerReactConfig]: config,
    [ViewServerReactClientProvider]: ViewServerClientProvider,
    useLiveQuery,
    useViewServerHealth,
    useViewServerHealthSummary,
    ViewServerProvider,
  };
};
