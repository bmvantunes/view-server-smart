import { describe, expectTypeOf, it } from "@effect/vitest";
import { Effect } from "effect";
import type {
  ViewServerAuth,
  ViewServerAuthRequest,
  ViewServerSession,
  ViewServerWebSocketServerInput,
  ViewServerWebSocketServerOptions,
} from "./index";

declare const serverInput: ViewServerWebSocketServerInput<never>;

describe("server type contracts", () => {
  it("accepts omitted, empty, client-only, and stream-only transport hooks", () => {
    const auth = {
      validateRequest: (_request: ViewServerAuthRequest) =>
        Effect.succeed({
          forwardedHeaders: {},
          id: "session-1",
          systemHeaders: {},
        }),
    } satisfies ViewServerAuth;
    const noTransportInput = {
      liveClient: serverInput.liveClient,
      runtime: serverInput.runtime,
    } satisfies ViewServerWebSocketServerInput<never>;
    const emptyTransportInput = {
      liveClient: serverInput.liveClient,
      runtime: serverInput.runtime,
      transport: {},
    } satisfies ViewServerWebSocketServerInput<never>;
    const authInput = {
      auth,
      liveClient: serverInput.liveClient,
      runtime: serverInput.runtime,
    } satisfies ViewServerWebSocketServerInput<never>;
    const clientOnlyInput = {
      liveClient: serverInput.liveClient,
      runtime: serverInput.runtime,
      transport: {
        clientOpened: Effect.void,
        clientClosed: Effect.void,
      },
    } satisfies ViewServerWebSocketServerInput<never>;
    const streamOnlyInput = {
      liveClient: serverInput.liveClient,
      runtime: serverInput.runtime,
      transport: {
        streamOpened: Effect.void,
        streamClosed: Effect.void,
      },
    } satisfies ViewServerWebSocketServerInput<never>;

    expectTypeOf(noTransportInput).not.toBeAny();
    expectTypeOf(emptyTransportInput).not.toBeAny();
    expectTypeOf(authInput).toMatchTypeOf<ViewServerWebSocketServerInput<never>>();
    expectTypeOf(authInput.auth.validateRequest).not.toBeAny();
    expectTypeOf(clientOnlyInput).not.toBeAny();
    expectTypeOf(streamOnlyInput).not.toBeAny();
  });

  it("rejects invalid transport hook values", () => {
    const invalidClientHookInput = {
      liveClient: serverInput.liveClient,
      runtime: serverInput.runtime,
      transport: {
        clientOpened: "not an effect",
      },
    };
    const invalidStreamHookInput = {
      liveClient: serverInput.liveClient,
      runtime: serverInput.runtime,
      transport: {
        streamOpened: "not an effect",
      },
    };
    const invalidAuthInput = {
      auth: {
        validateRequest: () => "not an effect",
      },
      liveClient: serverInput.liveClient,
      runtime: serverInput.runtime,
    };

    // @ts-expect-error clientOpened must be an Effect.
    invalidClientHookInput satisfies ViewServerWebSocketServerInput<never>;
    // @ts-expect-error streamOpened must be an Effect.
    invalidStreamHookInput satisfies ViewServerWebSocketServerInput<never>;
    // @ts-expect-error auth validator must return an Effect.
    invalidAuthInput satisfies ViewServerWebSocketServerInput<never>;
  });

  it("keeps session shape explicit", () => {
    const session = {
      forwardedHeaders: {
        authorization: "Bearer forwarded",
      },
      id: "session-1",
      systemHeaders: {
        "x-system": "view-server",
      },
    } satisfies ViewServerSession;

    expectTypeOf(session).toMatchTypeOf<ViewServerSession>();
    expectTypeOf<ViewServerSession["id"]>().toEqualTypeOf<string | null>();
    expectTypeOf<ViewServerSession["forwardedHeaders"][string]>().toEqualTypeOf<string>();
    expectTypeOf<ViewServerSession["systemHeaders"][string]>().toEqualTypeOf<string>();
  });

  it("only accepts concrete slash-prefixed connection paths", () => {
    const options = {
      path: "/rpc",
      healthPath: "/health",
      metricsPath: "/metrics",
    } satisfies ViewServerWebSocketServerOptions;
    const prefixedOptions = {
      path: "/view-server/rpc",
      healthPath: "/view-server/health",
      metricsPath: "/view-server/metrics",
    } satisfies ViewServerWebSocketServerOptions;

    const invalidWildcardRpcPath: ViewServerWebSocketServerOptions = {
      // @ts-expect-error WebSocket RPC path must produce a concrete client URL.
      path: "*",
    };
    const invalidWildcardHealthPath: ViewServerWebSocketServerOptions = {
      // @ts-expect-error health path must produce a concrete fetch URL.
      healthPath: "*",
    };
    const invalidBareRpcPath: ViewServerWebSocketServerOptions = {
      // @ts-expect-error WebSocket RPC path must be slash-prefixed.
      path: "rpc",
    };
    const invalidBareHealthPath: ViewServerWebSocketServerOptions = {
      // @ts-expect-error health path must be slash-prefixed.
      healthPath: "health",
    };
    const invalidWildcardMetricsPath: ViewServerWebSocketServerOptions = {
      // @ts-expect-error metrics path must produce a concrete fetch URL.
      metricsPath: "*",
    };
    const invalidBareMetricsPath: ViewServerWebSocketServerOptions = {
      // @ts-expect-error metrics path must be slash-prefixed.
      metricsPath: "metrics",
    };

    expectTypeOf(options.path).toEqualTypeOf<"/rpc">();
    expectTypeOf(options.healthPath).toEqualTypeOf<"/health">();
    expectTypeOf(options.metricsPath).toEqualTypeOf<"/metrics">();
    expectTypeOf(prefixedOptions.path).toEqualTypeOf<"/view-server/rpc">();
    expectTypeOf(prefixedOptions.healthPath).toEqualTypeOf<"/view-server/health">();
    expectTypeOf(prefixedOptions.metricsPath).toEqualTypeOf<"/view-server/metrics">();
    expectTypeOf(invalidWildcardRpcPath).not.toBeAny();
    expectTypeOf(invalidWildcardHealthPath).not.toBeAny();
    expectTypeOf(invalidBareRpcPath).not.toBeAny();
    expectTypeOf(invalidBareHealthPath).not.toBeAny();
    expectTypeOf(invalidWildcardMetricsPath).not.toBeAny();
    expectTypeOf(invalidBareMetricsPath).not.toBeAny();
  });
});
