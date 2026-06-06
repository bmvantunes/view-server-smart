import { describe, expectTypeOf, it } from "@effect/vitest";
import { defineViewServerConfig, type ViewServerRuntimeError } from "@view-server/config";
import type { Effect } from "effect";
import { Schema } from "effect";
import type { HttpServerError } from "effect/unstable/http";
import {
  makeViewServerRuntime,
  runViewServerRuntime,
  type ViewServerRuntime,
  type ViewServerRuntimeOptions,
} from "./index";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

const runtimeEffect = makeViewServerRuntime(viewServer);
const runEffect = runViewServerRuntime(viewServer);
declare const runtime: Effect.Success<typeof runtimeEffect>;

describe("runtime type contracts", () => {
  it("preserves configured topic types through runtime clients", () => {
    expectTypeOf(runtime.url).toEqualTypeOf<ViewServerRuntime<typeof viewServer.topics>["url"]>();
    expectTypeOf(runtime.healthUrl).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["healthUrl"]
    >();
    expectTypeOf(runtime.health).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["health"]
    >();
    expectTypeOf(runtime.close).toEqualTypeOf<
      ViewServerRuntime<typeof viewServer.topics>["close"]
    >();
    expectTypeOf<Effect.Success<typeof runEffect>>().toEqualTypeOf<never>();
    expectTypeOf<Effect.Error<typeof runEffect>>().toEqualTypeOf<HttpServerError.ServeError>();

    const publish = runtime.client.publish("orders", {
      id: "order-1",
      price: 10,
    });
    expectTypeOf<Parameters<typeof runtime.client.publish>>().toEqualTypeOf<
      Parameters<ViewServerRuntime<typeof viewServer.topics>["client"]["publish"]>
    >();
    const subscribe = runtime.liveClient.subscribe("orders", {
      select: ["id", "price"],
    });

    expectTypeOf<Effect.Error<typeof publish>>().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf(subscribe).not.toBeAny();

    const invalidPublish = runtime.client.publish("orders", {
      id: "order-1",
      price: 10,
      // @ts-expect-error runtime mutation client rejects fields outside the topic row.
      prcie: 10,
    });

    const invalidSubscribe = runtime.liveClient.subscribe("orders", {
      // @ts-expect-error runtime live client rejects fields outside the topic row.
      select: ["prcie"],
    });
    const invalidTopicPublish = runtime.client.publish(
      // @ts-expect-error runtime mutation client rejects unknown topics.
      "missing",
      {
        id: "order-1",
        price: 10,
      },
    );
    const invalidSnapshot = runtime.client.snapshot("orders", {
      // @ts-expect-error invalid query collapse keeps selected fields from being accepted.
      select: ["id"],
      where: {
        // @ts-expect-error runtime query client rejects unknown filter fields.
        prcie: { gte: 10 },
      },
    });
    const invalidOptions = makeViewServerRuntime(viewServer, {
      // @ts-expect-error runtime options reject string ports.
      websocketPort: "8080",
    });
    const invalidPathOptions = makeViewServerRuntime(viewServer, {
      // @ts-expect-error runtime paths must be absolute HTTP paths.
      rpcPath: "runtime-rpc",
    });
    const invalidHealthPathOptions = makeViewServerRuntime(viewServer, {
      // @ts-expect-error runtime health paths must be absolute HTTP paths.
      healthPath: "runtime-health",
    });
    const invalidWildcardRpcPathOptions = makeViewServerRuntime(viewServer, {
      // @ts-expect-error runtime RPC path must be a concrete slash-prefixed client URL path.
      rpcPath: "*",
    });
    const invalidWildcardHealthPathOptions = makeViewServerRuntime(viewServer, {
      // @ts-expect-error runtime health path must be a concrete slash-prefixed client URL path.
      healthPath: "*",
    });
    expectTypeOf(invalidPublish).not.toBeAny();
    expectTypeOf(invalidSubscribe).not.toBeAny();
    expectTypeOf(invalidTopicPublish).not.toBeAny();
    expectTypeOf(invalidSnapshot).not.toBeAny();
    expectTypeOf(invalidOptions).not.toBeAny();
    expectTypeOf(invalidPathOptions).not.toBeAny();
    expectTypeOf(invalidHealthPathOptions).not.toBeAny();
    expectTypeOf(invalidWildcardRpcPathOptions).not.toBeAny();
    expectTypeOf(invalidWildcardHealthPathOptions).not.toBeAny();
    expectTypeOf<ViewServerRuntimeOptions>().not.toHaveProperty("port");
    expectTypeOf<ViewServerRuntimeOptions>().not.toHaveProperty("path");
  });
});
