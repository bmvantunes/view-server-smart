import { Config } from "effect";
import type { RuntimeEnvironmentConfig } from "./index";

export type { RuntimeEnvironmentConfig } from "./index";

export const runtimeEnvironmentConfig: RuntimeEnvironmentConfig = {
  websocketPort: Config.number("VIEW_SERVER_WEBSOCKET_PORT"),
  tcpPublishPort: Config.number("VIEW_SERVER_TCP_PUBLISH_PORT"),
};

export const runtimeConfig = {
  port: (name: string): Config.Config<number> => Config.number(name),
  kafkaBootstrapServers: (name: string): Config.Config<string> => Config.string(name),
};
