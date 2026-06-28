import { NodeRuntime } from "@effect/platform-node";
import { Clock, Effect, Schedule } from "effect";
import { TcpPublisherExampleError, writeCommand } from "./tcp-client";

const publishNext = (index: number) =>
  writeCommand({
    op: "publish",
    topic: "orders",
    row: {
      id: `tcp-order-${index}`,
      customerId: `tcp-customer-${index}`,
      status: "open",
      price: index * 5,
      region: index % 2 === 0 ? "london" : "usa",
      updatedAt: index,
    },
  }).pipe(
    Effect.tap((response) =>
      response.ok
        ? Effect.void
        : Effect.fail(
            new TcpPublisherExampleError({
              message: `TCP publish failed: ${response.error.message}`,
            }),
          ),
    ),
  );

NodeRuntime.runMain(
  Effect.repeat(
    Effect.gen(function* () {
      const next = yield* Clock.currentTimeMillis;
      yield* publishNext(next);
      yield* Effect.logInfo(`Published tcp-order-${next}`);
    }),
    Schedule.spaced("1 second"),
  ),
);
