import { describe, expectTypeOf, it } from "@effect/vitest";
import type { ViewServerWebSocketServerOptions } from "./index";

describe("server type contracts", () => {
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
