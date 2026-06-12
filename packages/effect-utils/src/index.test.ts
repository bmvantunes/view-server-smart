import { describe, expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Fiber, Logger, References } from "effect";
import { ignoreLoggedTypedFailuresPreserveNonTypedFailures, runAllFinalizers } from "./index";

const ignoreCloseFailure =
  ignoreLoggedTypedFailuresPreserveNonTypedFailures("Ignoring close failure.");

type CapturedLog = {
  readonly cause: Cause.Cause<unknown>;
  readonly message: unknown;
};

const makeCapturedLogs = () => {
  const logs: Array<CapturedLog> = [];
  const logger = Logger.make<unknown, void>((options) => {
    logs.push({
      cause: options.cause,
      message: options.message,
    });
  });
  return { logger, logs };
};

describe("close policy", () => {
  it.effect("logs and ignores typed close failures", () => {
    const { logger, logs } = makeCapturedLogs();

    return Effect.gen(function* () {
      yield* Effect.fail("typed close failure").pipe(ignoreCloseFailure);

      expect(logs[0]?.message).toStrictEqual(["Ignoring close failure."]);
      expect(Cause.hasFails(logs[0]?.cause ?? Cause.empty)).toBe(true);
      expect(Cause.hasDies(logs[0]?.cause ?? Cause.empty)).toBe(false);
      expect(Cause.hasInterrupts(logs[0]?.cause ?? Cause.empty)).toBe(false);
    }).pipe(
      Effect.provide(Logger.layer([logger])),
      Effect.provideService(References.MinimumLogLevel, "Trace"),
    );
  });

  it.effect("preserves pure close defects", () =>
    Effect.gen(function* () {
      const cause = yield* Effect.die("close defect").pipe(
        ignoreCloseFailure,
        Effect.sandbox,
        Effect.flip,
      );

      expect(Cause.hasDies(cause)).toBe(true);
      expect(Cause.hasFails(cause)).toBe(false);
      expect(Cause.hasInterrupts(cause)).toBe(false);
    }),
  );

  it.effect("preserves pure close interruptions", () =>
    Effect.gen(function* () {
      const interruptedCloseCause = Cause.fromReasons([Cause.makeInterruptReason()]);
      const cause = yield* Effect.failCause(interruptedCloseCause).pipe(
        ignoreCloseFailure,
        Effect.sandbox,
        Effect.flip,
      );

      expect(Cause.hasDies(cause)).toBe(false);
      expect(Cause.hasFails(cause)).toBe(false);
      expect(Cause.hasInterrupts(cause)).toBe(true);
    }),
  );

  it.effect("logs typed close failures while preserving mixed close defects", () => {
    const { logger, logs } = makeCapturedLogs();

    return Effect.gen(function* () {
      const mixedCloseCause = Cause.fromReasons([
        Cause.makeFailReason("typed close failure"),
        Cause.makeDieReason("close defect"),
      ]);
      const cause = yield* Effect.failCause(mixedCloseCause).pipe(
        ignoreCloseFailure,
        Effect.sandbox,
        Effect.flip,
      );

      expect(Cause.hasDies(cause)).toBe(true);
      expect(Cause.hasFails(cause)).toBe(false);
      expect(Cause.hasInterrupts(cause)).toBe(false);
      expect(logs[0]?.message).toStrictEqual(["Ignoring close failure."]);
      expect(Cause.hasFails(logs[0]?.cause ?? Cause.empty)).toBe(true);
      expect(Cause.hasDies(logs[0]?.cause ?? Cause.empty)).toBe(false);
      expect(Cause.hasInterrupts(logs[0]?.cause ?? Cause.empty)).toBe(false);
    }).pipe(
      Effect.provide(Logger.layer([logger])),
      Effect.provideService(References.MinimumLogLevel, "Trace"),
    );
  });

  it.effect("logs typed close failures while preserving mixed close interruptions", () => {
    const { logger, logs } = makeCapturedLogs();

    return Effect.gen(function* () {
      const mixedCloseCause = Cause.fromReasons([
        Cause.makeFailReason("typed close failure"),
        Cause.makeInterruptReason(),
      ]);
      const cause = yield* Effect.failCause(mixedCloseCause).pipe(
        ignoreCloseFailure,
        Effect.sandbox,
        Effect.flip,
      );

      expect(Cause.hasDies(cause)).toBe(false);
      expect(Cause.hasFails(cause)).toBe(false);
      expect(Cause.hasInterrupts(cause)).toBe(true);
      expect(logs[0]?.message).toStrictEqual(["Ignoring close failure."]);
      expect(Cause.hasFails(logs[0]?.cause ?? Cause.empty)).toBe(true);
      expect(Cause.hasDies(logs[0]?.cause ?? Cause.empty)).toBe(false);
      expect(Cause.hasInterrupts(logs[0]?.cause ?? Cause.empty)).toBe(false);
    }).pipe(
      Effect.provide(Logger.layer([logger])),
      Effect.provideService(References.MinimumLogLevel, "Trace"),
    );
  });

  it.effect("runs every finalizer when all finalizers succeed", () =>
    Effect.gen(function* () {
      const closed: Array<string> = [];

      yield* runAllFinalizers([
        Effect.sync(() => {
          closed.push("first");
        }),
        Effect.sync(() => {
          closed.push("second");
        }),
      ]);

      expect(closed).toStrictEqual(["first", "second"]);
    }),
  );

  it.effect("runs every finalizer before returning the combined close failure", () =>
    Effect.gen(function* () {
      const closed: Array<string> = [];
      const mixedCause = Cause.fromReasons([
        Cause.makeFailReason("typed close failure"),
        Cause.makeDieReason("close defect"),
      ]);

      const cause = yield* runAllFinalizers([
        Effect.sync(() => {
          closed.push("first");
        }).pipe(Effect.andThen(Effect.failCause(mixedCause))),
        Effect.sync(() => {
          closed.push("second");
        }),
        Effect.sync(() => {
          closed.push("third");
        }).pipe(Effect.andThen(Effect.fail("second typed failure"))),
      ]).pipe(Effect.sandbox, Effect.flip);

      expect(closed).toStrictEqual(["first", "second", "third"]);
      expect(Cause.hasFails(cause)).toBe(true);
      expect(Cause.hasDies(cause)).toBe(true);
      expect(Cause.hasInterrupts(cause)).toBe(false);
    }),
  );

  it.effect("runs every finalizer before observing caller interruption", () =>
    Effect.gen(function* () {
      const closed: Array<string> = [];
      const firstStarted = yield* Deferred.make<void>();
      const releaseFirst = yield* Deferred.make<void>();

      const closeFiber = yield* runAllFinalizers([
        Effect.gen(function* () {
          closed.push("first");
          yield* Deferred.succeed(firstStarted, undefined);
          yield* Deferred.await(releaseFirst);
        }),
        Effect.sync(() => {
          closed.push("second");
        }),
      ]).pipe(Effect.forkDetach);

      yield* Deferred.await(firstStarted);
      const interruptFiber = yield* Fiber.interrupt(closeFiber).pipe(Effect.forkDetach);
      yield* Deferred.succeed(releaseFirst, undefined);
      yield* Fiber.join(interruptFiber);

      expect(closed).toStrictEqual(["first", "second"]);
    }),
  );
});
