import { Effect, Schema } from "effect";
import * as Net from "node:net";

export class TcpPublisherExampleError extends Schema.TaggedErrorClass<TcpPublisherExampleError>()(
  "TcpPublisherExampleError",
  {
    cause: Schema.optional(Schema.Unknown),
    message: Schema.String,
  },
) {}

export type TcpCommand = {
  readonly op: "publish";
  readonly topic: "orders";
  readonly row: {
    readonly id: string;
    readonly customerId: string;
    readonly status: "open";
    readonly price: number;
    readonly region: string;
    readonly updatedAt: number;
  };
};

export type InvalidTcpCommand = {
  readonly op: "publish";
  readonly topic: "orders";
  readonly row: {
    readonly customerId: string;
    readonly status: "open";
    readonly price: string;
    readonly region: string;
    readonly updatedAt: number;
  };
};

export type WriteCommandOptions = {
  readonly host?: string;
  readonly port?: number;
};

const TcpPublishResponse = Schema.Union([
  Schema.Struct({
    ok: Schema.Literal(true),
  }),
  Schema.Struct({
    ok: Schema.Literal(false),
    error: Schema.Struct({
      _tag: Schema.String,
      message: Schema.String,
      phase: Schema.optional(Schema.String),
      topic: Schema.optional(Schema.String),
    }),
  }),
]);

export type TcpPublishResponse = typeof TcpPublishResponse.Type;

const parseTcpPublishResponse = (line: string) =>
  Effect.try({
    try: () => JSON.parse(line),
    catch: (cause) =>
      new TcpPublisherExampleError({
        cause,
        message: "Invalid TCP publish acknowledgement.",
      }),
  });

export const writeCommand = (
  command: TcpCommand | InvalidTcpCommand,
  options: WriteCommandOptions = {},
) =>
  Effect.tryPromise({
    try: () =>
      new Promise<unknown>((resolve, reject) => {
        let isSettled = false;
        let responseBuffer = "";
        const socket = Net.createConnection({
          host: options.host ?? "127.0.0.1",
          port: options.port ?? 8081,
        });
        const writeCommand = () => {
          socket.write(`${JSON.stringify(command)}\n`);
        };
        const cleanup = () => {
          socket.off("data", onData);
          socket.off("connect", writeCommand);
          socket.off("error", fail);
        };
        const finish = (line: string) => {
          if (!isSettled) {
            isSettled = true;
            cleanup();
            socket.end();
            Effect.runPromise(parseTcpPublishResponse(line)).then(resolve, reject);
          }
        };
        const fail = (cause: unknown) => {
          if (!isSettled) {
            isSettled = true;
            cleanup();
            socket.destroy();
            reject(cause);
          }
        };
        const onData = (chunk: Buffer) => {
          responseBuffer += chunk.toString("utf8");
          const newlineIndex = responseBuffer.indexOf("\n");
          if (newlineIndex >= 0) {
            finish(responseBuffer.slice(0, newlineIndex));
          }
        };
        socket.setTimeout(5_000, () =>
          fail(
            new TcpPublisherExampleError({
              message: "Timed out waiting for TCP publish acknowledgement.",
            }),
          ),
        );
        socket.once("error", fail);
        socket.once("connect", writeCommand);
        socket.on("data", onData);
      }),
    catch: (cause) =>
      cause instanceof TcpPublisherExampleError
        ? cause
        : new TcpPublisherExampleError({
            cause,
            message: "TCP publish command failed.",
          }),
  }).pipe(Effect.andThen(Schema.decodeUnknownEffect(TcpPublishResponse)));
