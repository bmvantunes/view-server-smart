import type { TopicDefinitions, ViewServerConfig } from "@view-server/config";
import { viewServerDecodeHealth } from "@view-server/protocol";
import { Effect } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/unstable/http";
import type { ViewServerWebSocketServerInput } from "./server-types";

const jsonStringify = (value: unknown): string =>
  JSON.stringify(value, (_key, nextValue: unknown) =>
    typeof nextValue === "bigint" ? nextValue.toString() : nextValue,
  );

const jsonResponse = (status: number, value: unknown): HttpServerResponse.HttpServerResponse =>
  HttpServerResponse.text(jsonStringify(value), {
    status,
    contentType: "application/json",
  });

export const makeViewServerHealthRoute = <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerWebSocketServerInput<Topics>,
  path: `/${string}`,
) =>
  HttpRouter.add(
    "GET",
    path,
    input.runtime.health().pipe(
      Effect.flatMap((health) => viewServerDecodeHealth(config, health)),
      Effect.match({
        onFailure: (error) => jsonResponse(500, error),
        onSuccess: (health) => jsonResponse(health.status === "ready" ? 200 : 503, health),
      }),
    ),
  );
