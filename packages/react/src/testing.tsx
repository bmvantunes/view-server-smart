import type { ViewServerInMemoryRuntime } from "@view-server/config";
import {
  createInMemoryViewServer,
  type DecodableTopicDefinitions,
  type ViewServerInMemoryOptions,
} from "@view-server/in-memory";
import * as AtomReact from "@effect/atom-react";
import { Effect } from "effect";
import * as Atom from "effect/unstable/reactivity/Atom";
import type { ReactNode } from "react";
import type { ViewServerReactBindings } from "./index";
import { ViewServerReactConfig } from "./internal";

export type { ViewServerInMemoryOptions } from "@view-server/in-memory";

export type ViewServerInMemoryProviderProps = {
  readonly children?: ReactNode;
};

export type ViewServerInMemoryReactInstance<Topics extends DecodableTopicDefinitions> = {
  readonly ViewServerInMemoryProvider: (props: ViewServerInMemoryProviderProps) => ReactNode;
  readonly client: ViewServerInMemoryRuntime<Topics>;
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
  options: ViewServerInMemoryOptions = {},
): ViewServerInMemoryReactInstance<Topics> => {
  const { ViewServerProvider } = react;
  const inMemory = createInMemoryViewServer(react[ViewServerReactConfig], options);

  function InMemoryLifetimeMount(): null {
    AtomReact.useAtomMount(InMemoryLifetimeAtom.use());
    return null;
  }

  function ViewServerInMemoryProvider(props: ViewServerInMemoryProviderProps): ReactNode {
    return (
      <InMemoryLifetimeAtom.Provider value={inMemory.close}>
        <ViewServerProvider client={inMemory.liveClient}>
          <InMemoryLifetimeMount />
          {props.children}
        </ViewServerProvider>
      </InMemoryLifetimeAtom.Provider>
    );
  }

  return {
    ViewServerInMemoryProvider,
    client: inMemory.client,
    close: inMemory.close,
  };
};
