import { describe, expectTypeOf, it } from "@effect/vitest";
import { Effect } from "effect";
import type { ViewServerWebSocketServerInput, ViewServerWebSocketServerOptions } from "./index";

declare const serverInput: ViewServerWebSocketServerInput<never>;

describe("server type contracts", () => {
  it("accepts omitted, empty, client-only, and stream-only transport hooks", () => {
    const noTransportInput = {
      liveClient: serverInput.liveClient,
      runtime: serverInput.runtime,
    } satisfies ViewServerWebSocketServerInput<never>;
    const emptyTransportInput = {
      liveClient: serverInput.liveClient,
      runtime: serverInput.runtime,
      transport: {},
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

    // @ts-expect-error clientOpened must be an Effect.
    invalidClientHookInput satisfies ViewServerWebSocketServerInput<never>;
    // @ts-expect-error streamOpened must be an Effect.
    invalidStreamHookInput satisfies ViewServerWebSocketServerInput<never>;
  });

  it("only accepts concrete slash-prefixed connection paths", () => {
    const options = {
      path: "/rpc",
      healthPath: "/health",
    } satisfies ViewServerWebSocketServerOptions;
    const prefixedOptions = {
      path: "/view-server/rpc",
      healthPath: "/view-server/health",
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

    expectTypeOf(options.path).toEqualTypeOf<"/rpc">();
    expectTypeOf(options.healthPath).toEqualTypeOf<"/health">();
    expectTypeOf(prefixedOptions.path).toEqualTypeOf<"/view-server/rpc">();
    expectTypeOf(prefixedOptions.healthPath).toEqualTypeOf<"/view-server/health">();
    expectTypeOf(invalidWildcardRpcPath).not.toBeAny();
    expectTypeOf(invalidWildcardHealthPath).not.toBeAny();
    expectTypeOf(invalidBareRpcPath).not.toBeAny();
    expectTypeOf(invalidBareHealthPath).not.toBeAny();
  });
});
