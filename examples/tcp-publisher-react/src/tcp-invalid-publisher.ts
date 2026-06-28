import { NodeRuntime } from "@effect/platform-node";
import { Effect } from "effect";
import { TcpPublisherExampleError, writeCommand } from "./tcp-client";

NodeRuntime.runMain(
  Effect.gen(function* () {
    const response = yield* writeCommand({
      op: "publish",
      topic: "orders",
      row: {
        customerId: "invalid-customer",
        status: "open",
        price: "not-a-number",
        region: "usa",
        updatedAt: 1,
      },
    });

    const report = response.ok
      ? Effect.fail(
          new TcpPublisherExampleError({
            message: "Invalid TCP publish unexpectedly succeeded.",
          }),
        )
      : Effect.logInfo(`Invalid TCP publish rejected: ${response.error.message}`);
    yield* report;
  }),
);
