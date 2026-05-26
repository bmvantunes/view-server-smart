import { describe, expectTypeOf, it } from "@effect/vitest";
import type * as Protocol from "./index";

describe("@view-server/protocol type contract", () => {
  it("does not export transport-neutral live client contracts", () => {
    expectTypeOf<keyof typeof Protocol>().not.toEqualTypeOf<
      "ViewServerLiveClient" | "ViewServerLiveEvent" | "ViewServerLiveSubscription"
    >();

    // @ts-expect-error live client contracts belong to @view-server/client.
    expectTypeOf<Protocol.ViewServerLiveClient<never>>().toBeNever();
    // @ts-expect-error live event contracts belong to @view-server/client.
    expectTypeOf<Protocol.ViewServerLiveEvent<never>>().toBeNever();
    // @ts-expect-error live subscription contracts belong to @view-server/client.
    expectTypeOf<Protocol.ViewServerLiveSubscription<never>>().toBeNever();
  });
});
