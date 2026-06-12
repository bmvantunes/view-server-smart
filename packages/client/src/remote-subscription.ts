import { Cause, Effect, Exit, Queue, Scope, Stream } from "effect";
import { constant } from "effect/Function";
import { ignoreLoggedTypedFailuresPreserveNonTypedFailures } from "@view-server/effect-utils";
import type {
  ViewServerLiveEvent,
  ViewServerLiveSubscription,
  ViewServerStatusEvent,
} from "./live-client";

const ignoreRemoteSubscriptionCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring remote subscription close failure.",
);
const ignoreRemoteSubscriptionStreamStartFailure =
  ignoreLoggedTypedFailuresPreserveNonTypedFailures(
    "Ignoring remote subscription stream start failure.",
  );

export type RemoteSubscriptionLifecycle = {
  readonly onOpen: Effect.Effect<void>;
  readonly onClose: Effect.Effect<void>;
};

export type RemoteSubscriptionOptions<
  Row,
  Error,
  Topic extends string = string,
  Key extends string = string,
> = {
  readonly clientScope: Scope.Scope;
  readonly failureStatus: (topic: Topic, error: Error) => ViewServerStatusEvent<Topic>;
  readonly lifecycle?: RemoteSubscriptionLifecycle;
  readonly source: Stream.Stream<ViewServerLiveEvent<Row, Topic, Key>, Error>;
  readonly subscriptionBufferSize: number;
  readonly topic: Topic;
};

export const makeRemoteSubscription = Effect.fn("ViewServerClient.remote.subscription.make")(
  function* <Row, Error, Topic extends string = string, Key extends string = string>({
    clientScope,
    failureStatus,
    lifecycle = {
      onOpen: Effect.void,
      onClose: Effect.void,
    },
    source,
    subscriptionBufferSize,
    topic,
  }: RemoteSubscriptionOptions<Row, Error, Topic, Key>) {
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const scope = yield* Scope.fork(clientScope, "parallel");
        const closeSubscription = Scope.close(scope, Exit.void).pipe(
          ignoreRemoteSubscriptionCloseFailure,
        );
        return yield* restore(
          Effect.gen(function* () {
            const stream = source.pipe(
              Stream.catch((error) => Stream.make(failureStatus(topic, error))),
            );
            const queue = yield* Queue.bounded<ViewServerLiveEvent<Row, Topic, Key>, Cause.Done>(
              subscriptionBufferSize,
            );
            yield* Scope.addFinalizer(
              scope,
              lifecycle.onClose.pipe(ignoreRemoteSubscriptionCloseFailure),
            );
            yield* Stream.runIntoQueue(stream, queue).pipe(
              Effect.forkIn(scope, { startImmediately: true }),
              ignoreRemoteSubscriptionStreamStartFailure,
            );
            yield* lifecycle.onOpen;
            const subscription = {
              events: Stream.fromQueue(queue).pipe(Stream.ensuring(closeSubscription)),
              close: () => closeSubscription,
            } satisfies ViewServerLiveSubscription<Row, Topic, Key>;
            return subscription;
          }),
        ).pipe(Effect.onInterrupt(constant(closeSubscription)));
      }),
    );
  },
);
