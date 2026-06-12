import type {
  ColumnLiveViewEngineHealth,
  DecodableTopicDefinitions,
} from "@view-server/column-live-view-engine";
import type { TransportHealth, ViewServerHealth } from "@view-server/config";
import { Clock, Deferred, Effect, Fiber, Semaphore, type Exit } from "effect";
import type * as Duration from "effect/Duration";
import type { AtomRef } from "effect/unstable/reactivity";

type EngineHealthReader<Topics extends DecodableTopicDefinitions> = {
  readonly health: () => Effect.Effect<ColumnLiveViewEngineHealth<Topics>, never>;
};

export const healthFromEngine = <Topics extends DecodableTopicDefinitions>(
  engineHealth: ColumnLiveViewEngineHealth<Topics>,
  transportHealth: RuntimeCoreTransportHealth<Topics> = defaultRuntimeCoreTransportHealth,
  healthOverlay: RuntimeCoreHealthOverlay<Topics> = defaultRuntimeCoreHealthOverlay,
  nowMillis = 0,
): ViewServerHealth<Topics> => {
  return healthOverlay(
    {
      status: engineHealth.status,
      version: engineHealth.version,
      uptimeMs: 0,
      engine: { topics: engineHealth.topics },
      transport: transportHealth(engineHealth),
    },
    nowMillis,
  );
};

export type RuntimeCoreTransportHealth<Topics extends DecodableTopicDefinitions> = (
  engineHealth: ColumnLiveViewEngineHealth<Topics>,
) => TransportHealth;

export type RuntimeCoreHealthOverlay<Topics extends DecodableTopicDefinitions> = (
  health: ViewServerHealth<Topics>,
  nowMillis: number,
) => ViewServerHealth<Topics>;

export const defaultRuntimeCoreHealthOverlay = <Topics extends DecodableTopicDefinitions>(
  health: ViewServerHealth<Topics>,
  _nowMillis: number,
): ViewServerHealth<Topics> => health;

export const defaultRuntimeCoreTransportHealth = <Topics extends DecodableTopicDefinitions>(
  engineHealth: ColumnLiveViewEngineHealth<Topics>,
): TransportHealth => ({
  activeClients: 0,
  activeStreams: 0,
  activeSubscriptions: engineHealth.activeSubscriptions,
  messagesPerSecond: 0,
  bytesPerSecond: 0,
  queuedMessages: engineHealth.queuedEvents,
  queuedBytes: 0,
  droppedClients: 0,
  backpressureEvents: engineHealth.backpressureEvents,
  reconnects: 0,
  lastError: null,
});

const nextHealthValue = <Topics extends DecodableTopicDefinitions>(
  current: ViewServerHealth<Topics>,
  next: ViewServerHealth<Topics>,
): ViewServerHealth<Topics> => {
  if (current.status === "stopping" && next.status !== "stopping") {
    return current;
  }
  return next;
};

export const readHealth = Effect.fn("ViewServerRuntimeCore.health.read")(function* <
  const Topics extends DecodableTopicDefinitions,
>(
  engine: EngineHealthReader<Topics>,
  health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
  transportHealth: RuntimeCoreTransportHealth<Topics> = defaultRuntimeCoreTransportHealth,
  healthOverlay: RuntimeCoreHealthOverlay<Topics> = defaultRuntimeCoreHealthOverlay,
  shouldInstall: () => boolean = () => true,
  onInstall: () => void = () => undefined,
) {
  const nowMillis = yield* Clock.currentTimeMillis;
  const value = healthFromEngine(yield* engine.health(), transportHealth, healthOverlay, nowMillis);
  yield* Effect.sync(() => {
    if (shouldInstall()) {
      health.update((current) => nextHealthValue(current, value));
      onInstall();
    }
  });
  return health.value;
});

export const makeCoalescedHealthReader = <const Topics extends DecodableTopicDefinitions, E>(
  read: (epoch: number) => Effect.Effect<ViewServerHealth<Topics>, E>,
  currentEpoch: () => number = () => 0,
) => {
  type ActiveRead = {
    readonly deferred: Deferred.Deferred<ViewServerHealth<Topics>, E>;
    readonly epoch: number;
  };
  let activeRead: ActiveRead | undefined = undefined;
  const stateLock = Semaphore.makeUnsafe(1);
  type ReadDecision =
    | {
        readonly _tag: "leader";
        readonly active: ActiveRead;
      }
    | {
        readonly _tag: "follower";
        readonly deferred: Deferred.Deferred<ViewServerHealth<Topics>, E>;
      };

  const completeActiveRead = (active: ActiveRead, exit: Exit.Exit<ViewServerHealth<Topics>, E>) =>
    Effect.uninterruptible(
      stateLock.withPermit(
        Effect.gen(function* () {
          yield* Deferred.done(active.deferred, exit);
          yield* Effect.sync(() => {
            if (activeRead === active) {
              activeRead = undefined;
            }
          });
        }),
      ),
    );

  return Effect.fn("ViewServerRuntimeCore.health.readCoalesced")(function* () {
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const decision = yield* stateLock.withPermit(
          Effect.gen(function* () {
            const epoch = currentEpoch();
            if (activeRead !== undefined && activeRead.epoch === epoch) {
              return {
                _tag: "follower",
                deferred: activeRead.deferred,
              } satisfies ReadDecision;
            }
            const nextRead = yield* Deferred.make<ViewServerHealth<Topics>, E>();
            const active = {
              deferred: nextRead,
              epoch,
            };
            activeRead = active;
            return {
              _tag: "leader",
              active,
            } satisfies ReadDecision;
          }),
        );
        if (decision._tag === "follower") {
          return yield* restore(Deferred.await(decision.deferred));
        }
        return yield* read(decision.active.epoch).pipe(
          Effect.onExit((exit) => completeActiveRead(decision.active, exit)),
        );
      }),
    );
  });
};

export const makeHealthRefreshScheduler = (
  refresh: Effect.Effect<void>,
  cadence: Duration.Input = "1 second",
) => {
  let scheduled = false;
  let pending = false;
  let closed = false;
  let activeFiber: Fiber.Fiber<void> | undefined = undefined;
  let activeToken: SchedulerRunToken | undefined = undefined;
  let nextToken = 0;
  const stateLock = Semaphore.makeUnsafe(1);
  type SchedulerRunToken = {
    readonly id: number;
  };
  type RequestDecision =
    | { readonly _tag: "closed" }
    | { readonly _tag: "pending" }
    | { readonly _tag: "start"; readonly token: SchedulerRunToken };

  const clearActiveRun = (token: SchedulerRunToken) =>
    stateLock.withPermit(
      Effect.sync(() => {
        if (activeToken === token) {
          activeFiber = undefined;
          activeToken = undefined;
          scheduled = false;
          pending = false;
        }
      }),
    );

  const drainRefreshes = Effect.fn("ViewServerRuntimeCore.healthRefreshScheduler.drain")(
    function* () {
      let shouldRefresh = true;
      while (shouldRefresh) {
        yield* stateLock.withPermit(
          Effect.sync(() => {
            pending = false;
          }),
        );
        yield* Effect.sleep(cadence);
        yield* refresh;
        shouldRefresh = yield* stateLock.withPermit(
          Effect.sync(() => {
            if (pending) {
              return true;
            }
            activeFiber = undefined;
            activeToken = undefined;
            scheduled = false;
            return false;
          }),
        );
      }
    },
  );

  const requestRefresh = Effect.fn("ViewServerRuntimeCore.healthRefreshScheduler.request")(
    function* () {
      yield* Effect.uninterruptible(
        stateLock.withPermit(
          Effect.gen(function* () {
            const decision = yield* Effect.sync((): RequestDecision => {
              if (closed) {
                return { _tag: "closed" };
              }
              if (scheduled) {
                pending = true;
                return { _tag: "pending" };
              }
              const token: SchedulerRunToken = { id: nextToken };
              nextToken += 1;
              scheduled = true;
              pending = true;
              activeToken = token;
              return { _tag: "start", token };
            });
            if (decision._tag !== "start") {
              return;
            }
            const { token } = decision;
            const fiber = yield* drainRefreshes().pipe(
              Effect.ensuring(clearActiveRun(token)),
              Effect.forkDetach({ startImmediately: true }),
            );
            yield* Effect.sync(() => {
              activeFiber = fiber;
            });
          }),
        ),
      );
    },
  );

  const close = Effect.fn("ViewServerRuntimeCore.healthRefreshScheduler.close")(function* () {
    yield* Effect.uninterruptible(
      Effect.gen(function* () {
        const fiber = yield* stateLock.withPermit(
          Effect.sync(() => {
            const current = activeFiber;
            closed = true;
            activeFiber = undefined;
            activeToken = undefined;
            scheduled = false;
            pending = false;
            return current;
          }),
        );
        if (fiber !== undefined) {
          yield* Fiber.interrupt(fiber).pipe(Effect.asVoid);
        }
      }),
    );
  });

  return {
    close: close(),
    request: requestRefresh(),
  };
};
