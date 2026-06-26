import {
  type DecodableTopicDefinitions,
  type ViewServerInMemoryOptions,
} from "@view-server/in-memory";
import {
  createInMemoryViewServerTesting,
  type ViewServerInMemoryTestingInstance,
} from "@view-server/in-memory/testing";
import * as AtomReact from "@effect/atom-react";
import { Effect } from "effect";
import * as Atom from "effect/unstable/reactivity/Atom";
import type { ReactNode } from "react";
import type { ViewServerReactBindings } from "./index";
import { ViewServerReactClientProvider, ViewServerReactConfig } from "./internal";

export type { ViewServerInMemoryOptions } from "@view-server/in-memory";

export type ViewServerInMemoryProviderProps = {
  readonly children?: ReactNode;
};

export type ViewServerInMemoryReactInstance<Topics extends DecodableTopicDefinitions> = {
  readonly ViewServerInMemoryProvider: (props: ViewServerInMemoryProviderProps) => ReactNode;
  readonly client: ViewServerInMemoryTestingInstance<Topics>["client"];
  readonly close: Effect.Effect<void>;
};

const InMemoryLifetimeAtom = AtomReact.make((close: Effect.Effect<void>) =>
  Atom.make((get) => {
    get.addFinalizer(() => {
      Effect.runFork(close);
    });
    return null;
  }),
);

export const createInMemoryViewServerReact = <const Topics extends DecodableTopicDefinitions>(
  react: ViewServerReactBindings<Topics>,
  options: ViewServerInMemoryOptions<Topics> = {},
): ViewServerInMemoryReactInstance<Topics> => {
  const ViewServerClientProvider = react[ViewServerReactClientProvider];
  const inMemory = createInMemoryViewServerTesting(react[ViewServerReactConfig], options);

  function InMemoryLifetimeMount(): null {
    AtomReact.useAtomMount(InMemoryLifetimeAtom.use());
    return null;
  }

  function ViewServerInMemoryProvider(props: ViewServerInMemoryProviderProps): ReactNode {
    return (
      <InMemoryLifetimeAtom.Provider value={inMemory.close}>
        <ViewServerClientProvider client={inMemory.liveClient}>
          <InMemoryLifetimeMount />
          {props.children}
        </ViewServerClientProvider>
      </InMemoryLifetimeAtom.Provider>
    );
  }

  return {
    ViewServerInMemoryProvider,
    client: inMemory.client,
    close: inMemory.close,
  };
};
