import type { ReactNode } from "react";
import { OrdersApp } from "./app";
import { ViewServerProvider } from "./view-server.config";

export type ExampleRuntimeConfig = {
  readonly VIEW_SERVER_URL: string;
};

export function AppRoot(props: {
  readonly children?: ReactNode;
  readonly config: ExampleRuntimeConfig;
}): ReactNode {
  return (
    <ViewServerProvider url={props.config.VIEW_SERVER_URL}>
      {props.children ?? <OrdersApp />}
    </ViewServerProvider>
  );
}
