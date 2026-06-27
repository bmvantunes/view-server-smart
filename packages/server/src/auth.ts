import { Effect, Schema } from "effect";
import type { HttpServerRequest } from "effect/unstable/http";
import { HttpServerResponse } from "effect/unstable/http";

export class ViewServerAuthError extends Schema.TaggedErrorClass<ViewServerAuthError>()(
  "ViewServerAuthError",
  {
    message: Schema.String,
    status: Schema.Union([Schema.Literal(401), Schema.Literal(403)]),
  },
) {}

export type ViewServerSession = {
  readonly id: string | null;
  readonly forwardedHeaders: Readonly<Record<string, string>>;
  readonly systemHeaders: Readonly<Record<string, string>>;
};

export type ViewServerAuthRequest = {
  readonly headers: Readonly<Record<string, string>>;
  readonly method: HttpServerRequest.HttpServerRequest["method"];
  readonly remoteAddress: HttpServerRequest.HttpServerRequest["remoteAddress"];
  readonly url: string;
};

export type ViewServerAuthValidator = (
  request: ViewServerAuthRequest,
) => Effect.Effect<ViewServerSession, ViewServerAuthError>;

export type ViewServerAuth = {
  readonly validateRequest: ViewServerAuthValidator;
};

export const anonymousViewServerSession: ViewServerSession = {
  forwardedHeaders: {},
  id: null,
  systemHeaders: {},
};

export const allowAnonymousViewServerAuth: ViewServerAuth = {
  validateRequest: () => Effect.succeed(anonymousViewServerSession),
};

const requestInput = (request: HttpServerRequest.HttpServerRequest): ViewServerAuthRequest => ({
  headers: request.headers,
  method: request.method,
  remoteAddress: request.remoteAddress,
  url: request.url,
});

export const validateViewServerAuthRequest = Effect.fn("ViewServerServer.auth.validateRequest")(
  function* (auth: ViewServerAuth | undefined, request: ViewServerAuthRequest) {
    const validator = auth ?? allowAnonymousViewServerAuth;
    return yield* validator.validateRequest(request);
  },
);

export const validateViewServerHttpRequest = Effect.fn("ViewServerServer.auth.validate")(function* (
  auth: ViewServerAuth | undefined,
  request: HttpServerRequest.HttpServerRequest,
) {
  return yield* validateViewServerAuthRequest(auth, requestInput(request));
});

const authErrorBody = (error: ViewServerAuthError): string =>
  JSON.stringify({
    _tag: error._tag,
    message: error.message,
  });

export const viewServerAuthErrorResponse = (
  error: ViewServerAuthError,
): HttpServerResponse.HttpServerResponse =>
  HttpServerResponse.text(authErrorBody(error), {
    contentType: "application/json",
    status: error.status,
  });
