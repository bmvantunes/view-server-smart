import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import * as Net from "node:net";
import { writeCommand } from "./tcp-client";

class TcpClientTestError extends Schema.TaggedErrorClass<TcpClientTestError>()(
  "TcpClientTestError",
  {
    cause: Schema.optional(Schema.Unknown),
    message: Schema.String,
  },
) {}

const ListeningAddress = Schema.Struct({
  port: Schema.Number,
});

const closeServer = (server: Net.Server) =>
  Effect.tryPromise({
    try: () =>
      new Promise<void>((resolve, reject) => {
        server.close((cause) => (cause === undefined ? resolve() : reject(cause)));
      }),
    catch: (cause) =>
      new TcpClientTestError({
        cause,
        message: "Failed to close fragmented acknowledgement TCP server.",
      }),
  });

const makeFragmentedAcknowledgementServer = Effect.acquireRelease(
  Effect.tryPromise({
    try: () =>
      new Promise<Net.Server>((resolve, reject) => {
        const server = Net.createServer((socket) => {
          socket.once("data", () => {
            socket.write('{"ok":');
            setTimeout(() => socket.end("true}\n"), 1);
          });
        });
        server.once("error", reject);
        server.listen(0, "127.0.0.1", () => {
          server.off("error", reject);
          resolve(server);
        });
      }),
    catch: (cause) =>
      new TcpClientTestError({
        cause,
        message: "Failed to start fragmented acknowledgement TCP server.",
      }),
  }),
  (server) => closeServer(server).pipe(Effect.ignore),
);

const command = {
  op: "publish",
  topic: "orders",
  row: {
    id: "order-fragmented-ack",
    customerId: "customer-1",
    status: "open",
    price: 123,
    region: "usa",
    updatedAt: 1,
  },
} satisfies Parameters<typeof writeCommand>[0];

describe("tcp publisher client", () => {
  it.effect("buffers newline-delimited TCP acknowledgements split across packets", () =>
    Effect.gen(function* () {
      const server = yield* makeFragmentedAcknowledgementServer;
      const address = yield* Schema.decodeUnknownEffect(ListeningAddress)(server.address());
      const response = yield* writeCommand(command, { port: address.port });

      expect(response).toStrictEqual({ ok: true });
    }),
  );
});
