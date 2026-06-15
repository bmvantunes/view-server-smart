import { Effect } from "effect";
import type { HttpServerRequest } from "effect/unstable/http";
import * as Socket from "effect/unstable/socket/Socket";

export type ActiveSocketClosers = Set<Effect.Effect<void, unknown>>;

const logWebSocketShutdownCloseFailure = <R>(
  closeSocket: Effect.Effect<void, unknown, R>,
): Effect.Effect<void, never, R> =>
  closeSocket.pipe(
    Effect.timeoutOption("100 millis"),
    Effect.asVoid,
    Effect.catchCause((cause) => Effect.logWarning("WebSocket shutdown failed.", cause)),
  );

export const closeTrackedSockets = Effect.fn("ViewServerServer.websocket.tracked.close")(function* (
  activeSocketClosers: ActiveSocketClosers,
) {
  const closers = Array.from(activeSocketClosers);
  yield* Effect.forEach(closers, logWebSocketShutdownCloseFailure, {
    concurrency: "unbounded",
    discard: true,
  });
});

export const makeTrackedSocket = (
  socket: Socket.Socket,
  clientOpened: Effect.Effect<void>,
  clientClosed: Effect.Effect<void>,
  activeSocketClosers: ActiveSocketClosers,
): Socket.Socket =>
  new Proxy(socket, {
    get(target, property, receiver) {
      if (property === "runRaw") {
        const runRaw: Socket.Socket["runRaw"] = (handler, options) => {
          let closeWhenOpened = Effect.void;
          const onOpen = Effect.sync(() => {
            closeWhenOpened = clientClosed;
          }).pipe(Effect.andThen(clientOpened), Effect.andThen(options?.onOpen ?? Effect.void));
          const close = Effect.sync(() => closeWhenOpened).pipe(
            Effect.flatMap((closeEffect) => closeEffect),
          );
          return Effect.scoped(
            Effect.gen(function* () {
              const writer = yield* target.writer;
              const closeSocket = writer(new Socket.CloseEvent(1001, "View Server shutting down"));
              yield* Effect.sync(() => {
                activeSocketClosers.add(closeSocket);
              });
              yield* Effect.addFinalizer(() =>
                Effect.sync(() => {
                  activeSocketClosers.delete(closeSocket);
                }),
              );
              return yield* target.runRaw(handler, {
                ...options,
                onOpen,
              });
            }),
          ).pipe(Effect.ensuring(close));
        };
        return runRaw;
      }
      return Reflect.get(target, property, receiver);
    },
  });

export const makeTrackedUpgradeRequest = (
  request: HttpServerRequest.HttpServerRequest,
  clientOpened: Effect.Effect<void>,
  clientClosed: Effect.Effect<void>,
  activeSocketClosers: ActiveSocketClosers,
): HttpServerRequest.HttpServerRequest =>
  new Proxy(request, {
    get(target, property, receiver) {
      if (property === "upgrade") {
        return target.upgrade.pipe(
          Effect.map((socket) =>
            makeTrackedSocket(socket, clientOpened, clientClosed, activeSocketClosers),
          ),
        );
      }
      return Reflect.get(target, property, receiver);
    },
  });
