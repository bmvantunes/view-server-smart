import { describe, expect, it } from "@effect/vitest";
import { Effect, Exit, Scope, Stream } from "effect";
import type { StatusEvent } from "@view-server/config";
import type { ViewServerLiveEvent } from "./live-client";
import { makeRemoteSubscription } from "./remote-subscription";

type Row = {
  readonly id: string;
};

const snapshot: ViewServerLiveEvent<Row> = {
  type: "snapshot",
  topic: "orders",
  queryId: "query-1",
  version: 1,
  keys: ["order-1"],
  rows: [{ id: "order-1" }],
  totalRows: 1,
};

const failureStatus = (topic: string, error: string): StatusEvent => ({
  type: "status",
  topic,
  queryId: "remote",
  status: "error",
  code: "TransportError",
  message: error,
});

describe("remote subscription", () => {
  it.effect("streams events and closes without explicit lifecycle hooks", () =>
    Effect.gen(function* () {
      const clientScope = yield* Scope.make("parallel");
      const subscription = yield* makeRemoteSubscription({
        clientScope,
        failureStatus,
        source: Stream.make(snapshot),
        subscriptionBufferSize: 2,
        topic: "orders",
      });

      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);
      expect(events[0]).toStrictEqual(snapshot);

      yield* subscription.close();
      yield* Scope.close(clientScope, Exit.void);
    }),
  );

  it.effect("maps source failures to status events and runs lifecycle finalizers", () =>
    Effect.gen(function* () {
      const clientScope = yield* Scope.make("parallel");
      let openCount = 0;
      let closeCount = 0;
      const subscription = yield* makeRemoteSubscription<Row, string>({
        clientScope,
        failureStatus,
        lifecycle: {
          onOpen: Effect.sync(() => {
            openCount += 1;
          }),
          onClose: Effect.sync(() => {
            closeCount += 1;
          }),
        },
        source: Stream.fail("socket closed"),
        subscriptionBufferSize: 2,
        topic: "orders",
      });

      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);
      expect(events[0]).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "remote",
        status: "error",
        code: "TransportError",
        message: "socket closed",
      });
      expect(openCount).toBe(1);
      expect(closeCount).toBe(1);

      yield* Scope.close(clientScope, Exit.void);
    }),
  );

  it.effect("propagates subscription close defects from lifecycle finalizers", () =>
    Effect.gen(function* () {
      const clientScope = yield* Scope.make("parallel");
      let closeCount = 0;
      const subscription = yield* makeRemoteSubscription<Row, string>({
        clientScope,
        failureStatus,
        lifecycle: {
          onOpen: Effect.void,
          onClose: Effect.sync(() => {
            closeCount += 1;
          }).pipe(Effect.andThen(Effect.die("close failed"))),
        },
        source: Stream.make(snapshot),
        subscriptionBufferSize: 2,
        topic: "orders",
      });

      const closeExit = yield* Effect.exit(subscription.close());

      expect(Exit.isFailure(closeExit)).toBe(true);
      expect(closeCount).toBe(1);
      yield* Scope.close(clientScope, Exit.void);
    }),
  );
});
