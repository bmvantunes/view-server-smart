import type { TopicDefinitions, ViewServerConfig } from "@view-server/config";
import { viewServerDecodeHealth } from "@view-server/protocol";
import { Cause, Effect, Option } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { validateViewServerHttpRequest, viewServerAuthErrorResponse } from "./auth";
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

const failureJsonResponse = (cause: Cause.Cause<unknown>): HttpServerResponse.HttpServerResponse =>
  jsonResponse(500, Cause.findErrorOption(cause).pipe(Option.getOrElse(() => Cause.pretty(cause))));

export const makeViewServerHealthRoute = <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerWebSocketServerInput<Topics>,
  path: `/${string}`,
) =>
  HttpRouter.add(
    "GET",
    path,
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      return yield* validateViewServerHttpRequest(input.auth, request).pipe(
        Effect.matchEffect({
          onFailure: (error) => Effect.succeed(viewServerAuthErrorResponse(error)),
          onSuccess: () =>
            Effect.gen(function* () {
              const health = yield* input.runtime.health();
              return yield* viewServerDecodeHealth(config, health);
            }).pipe(
              Effect.map((health) => jsonResponse(health.status === "ready" ? 200 : 503, health)),
              Effect.catchCause((cause) => Effect.succeed(failureJsonResponse(cause))),
            ),
        }),
      );
    }),
  );
