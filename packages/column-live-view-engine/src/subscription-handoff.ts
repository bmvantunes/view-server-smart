import { Effect, Exit } from "effect";

type ClosableSubscription = {
  readonly close: () => Effect.Effect<void, never>;
};

export type MarkAcquiredSubscription<Subscription extends ClosableSubscription> = (
  subscription: Subscription,
) => Effect.Effect<void>;

export type SubscriptionHandoffAcquire<
  Subscription extends ClosableSubscription,
  Error,
  Requirements,
> = (
  markAcquired: MarkAcquiredSubscription<Subscription>,
) => Effect.Effect<Subscription, Error, Requirements>;

export type SubscriptionHandoffOptions = {
  readonly beforeReturn?: Effect.Effect<void>;
};

export const closeInterruptedAcquiredSubscription = Effect.fn(
  "ColumnLiveViewEngine.subscriptionHandoff.closeInterruptedAcquired",
)(function* (exit: Exit.Exit<unknown, unknown>, subscription: ClosableSubscription | undefined) {
  if (!Exit.hasInterrupts(exit) || subscription === undefined) {
    return;
  }
  yield* subscription.close();
});

export function acquireSubscriptionHandoff<
  Subscription extends ClosableSubscription,
  Error,
  Requirements,
>(
  acquire: SubscriptionHandoffAcquire<Subscription, Error, Requirements>,
  options: SubscriptionHandoffOptions = {},
): Effect.Effect<Subscription, Error, Requirements> {
  let acquiredSubscription: ClosableSubscription | undefined;
  const markAcquired: MarkAcquiredSubscription<Subscription> = (subscription) =>
    Effect.sync(() => {
      acquiredSubscription = subscription;
    });

  return acquire(markAcquired).pipe(
    Effect.tap(() => options.beforeReturn ?? Effect.void),
    Effect.onExit((exit) => closeInterruptedAcquiredSubscription(exit, acquiredSubscription)),
  );
}
