import { VIEW_SERVER_HEALTH_SUMMARY_TOPIC, VIEW_SERVER_HEALTH_TOPIC } from "@view-server/config";
import { ignoreLoggedTypedFailuresPreserveNonTypedFailures } from "@view-server/effect-utils";
import type {
  TopicDefinitions,
  ViewServerConfig,
  ViewServerHealth,
  ViewServerRuntimeError,
} from "@view-server/config";
import {
  ViewServerRpcs,
  viewServerDecodeHealth,
  viewServerDecodeHealthQuery,
  viewServerDecodeLiveQuery,
  viewServerDecodeTopic,
  viewServerEncodeHealthSummaryEvent,
  viewServerEncodeHealthTopicEvent,
  viewServerEncodeLiveEvent,
} from "@view-server/protocol";
import { Deferred, Effect, Exit, Fiber, Scope, Semaphore, Stream } from "effect";
import type { ViewServerWebSocketServerInput } from "./server-types";

const ignoreSubscriptionCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "RPC subscription close failed.",
);

const makeCoalescedHealthRead = <const Topics extends TopicDefinitions>(
  input: ViewServerWebSocketServerInput<Topics>,
  scope: Scope.Scope,
) => {
  type ActiveRead = {
    readonly fibers: Array<Fiber.Fiber<void>>;
    readonly deferred: Deferred.Deferred<ViewServerHealth<Topics>, ViewServerRuntimeError>;
  };
  type ReadDecision =
    | {
        readonly _tag: "closed";
      }
    | {
        readonly _tag: "leader";
        readonly active: ActiveRead;
      }
    | {
        readonly _tag: "follower";
        readonly deferred: Deferred.Deferred<ViewServerHealth<Topics>, ViewServerRuntimeError>;
      };
  let activeRead: ActiveRead | undefined = undefined;
  let closed = false;
  let scopeFinalizerRegistered = false;
  const stateLock = Semaphore.makeUnsafe(1);

  const completeActiveRead = (
    active: ActiveRead,
    exit: Exit.Exit<ViewServerHealth<Topics>, ViewServerRuntimeError>,
  ) =>
    Effect.uninterruptible(
      stateLock.withPermit(
        Effect.gen(function* () {
          yield* Deferred.done(active.deferred, exit);
          yield* Effect.sync(() => {
            activeRead = [activeRead].find((read) => read !== active);
          });
        }),
      ),
    );
  const interruptActiveRead = Effect.uninterruptible(
    Effect.gen(function* () {
      const fibers = yield* stateLock.withPermit(
        Effect.gen(function* () {
          const active = activeRead;
          closed = true;
          if (active === undefined) {
            const noFibers: Array<Fiber.Fiber<void>> = [];
            return noFibers;
          }
          activeRead = undefined;
          yield* Deferred.done(active.deferred, Exit.interrupt());
          return active.fibers;
        }),
      );
      yield* Effect.forEach(
        fibers,
        (fiber) => Effect.forkDetach(Fiber.interrupt(fiber), { startImmediately: true }),
        { discard: true },
      );
    }),
  );
  const ensureScopeFinalizer = Effect.gen(function* () {
    if (scopeFinalizerRegistered) {
      return;
    }
    scopeFinalizerRegistered = true;
    yield* Scope.addFinalizer(scope, interruptActiveRead);
  });
  const forkActiveRead = (active: ActiveRead) =>
    Effect.suspend(() => input.runtime.health()).pipe(
      Effect.exit,
      Effect.flatMap((exit) => completeActiveRead(active, exit)),
      Effect.forkDetach({ startImmediately: true, uninterruptible: false }),
    );

  return Effect.fn("ViewServerServer.health.readCoalesced")(function* () {
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        yield* ensureScopeFinalizer;
        const read = yield* stateLock.withPermit(
          Effect.gen(function* () {
            if (closed) {
              return {
                _tag: "closed",
              } satisfies ReadDecision;
            }
            if (activeRead !== undefined) {
              return {
                _tag: "follower",
                deferred: activeRead.deferred,
              } satisfies ReadDecision;
            }
            const deferred = yield* Deferred.make<
              ViewServerHealth<Topics>,
              ViewServerRuntimeError
            >();
            const active: ActiveRead = { deferred, fibers: [] };
            activeRead = active;
            const fiber = yield* forkActiveRead(active);
            active.fibers.push(fiber);
            return {
              _tag: "leader",
              active,
            } satisfies ReadDecision;
          }),
        );
        if (read._tag === "closed") {
          return yield* restore(Effect.interrupt);
        }
        if (read._tag === "follower") {
          return yield* restore(Deferred.await(read.deferred));
        }
        return yield* restore(Deferred.await(read.active.deferred));
      }),
    );
  });
};

export const makeViewServerRpcHandlers = <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerWebSocketServerInput<Topics>,
  scope: Scope.Scope,
) => {
  const streamOpened = input.transport?.streamOpened ?? Effect.void;
  const streamClosed = input.transport?.streamClosed ?? Effect.void;
  const readHealth = makeCoalescedHealthRead(input, scope);
  const withTransportLifecycle = <A, E, R>(
    stream: Effect.Effect<Stream.Stream<A, E, R>, E, R>,
  ): Stream.Stream<A, E, R> =>
    Stream.unwrap(
      streamOpened.pipe(
        Effect.andThen(stream),
        Effect.onExit((exit) => (Exit.isFailure(exit) ? streamClosed : Effect.void)),
        Effect.map((openedStream) => openedStream.pipe(Stream.ensuring(streamClosed))),
      ),
    );

  return ViewServerRpcs.of({
    "ViewServer.Health": () =>
      Effect.gen(function* () {
        const health = yield* readHealth();
        return yield* viewServerDecodeHealth(config, health);
      }),
    "ViewServer.Subscribe": (payload) =>
      withTransportLifecycle(
        Effect.gen(function* () {
          if (payload.topic === VIEW_SERVER_HEALTH_SUMMARY_TOPIC) {
            yield* viewServerDecodeHealthQuery(payload.topic, payload.query);
            const subscription = yield* input.liveClient.subscribeHealthSummary();
            return subscription.events.pipe(
              Stream.mapEffect((event) =>
                viewServerEncodeHealthSummaryEvent<Topics>(config, event),
              ),
              Stream.ensuring(subscription.close().pipe(ignoreSubscriptionCloseFailure)),
            );
          }
          if (payload.topic === VIEW_SERVER_HEALTH_TOPIC) {
            yield* viewServerDecodeHealthQuery(payload.topic, payload.query);
            const subscription = yield* input.liveClient.subscribeHealth();
            return subscription.events.pipe(
              Stream.mapEffect((event) => viewServerEncodeHealthTopicEvent<Topics>(config, event)),
              Stream.ensuring(subscription.close().pipe(ignoreSubscriptionCloseFailure)),
            );
          }
          const topic = yield* viewServerDecodeTopic(config, payload.topic);
          const query = yield* viewServerDecodeLiveQuery<Topics, typeof topic>(
            config,
            topic,
            payload.query,
          );
          const subscription = yield* input.liveClient.subscribeRuntime(topic, query);
          return subscription.events.pipe(
            Stream.mapEffect((event) => viewServerEncodeLiveEvent(config, topic, query, event)),
            Stream.ensuring(subscription.close().pipe(ignoreSubscriptionCloseFailure)),
          );
        }),
      ),
  });
};
