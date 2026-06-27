import type { RowSchema, ViewServerConfig, ViewServerRuntimeClient } from "@view-server/config";
import { Cause, Effect, Exit, Fiber, Result, Schema } from "effect";
import * as Net from "node:net";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export class ViewServerTcpPublishIngressError extends Schema.TaggedErrorClass<ViewServerTcpPublishIngressError>()(
  "ViewServerTcpPublishIngressError",
  {
    cause: Schema.Unknown,
    message: Schema.String,
    phase: Schema.Literals(["configuration", "listen", "decode", "runtime", "backpressure"]),
    topic: Schema.optional(Schema.String),
  },
) {}

export type ViewServerTcpPublishIngressOptions = {
  readonly host?: string;
  readonly maxConnections?: number;
  readonly maxGlobalQueuedCommands?: number;
  readonly maxLineBytes?: number;
  readonly maxQueuedCommands?: number;
  readonly port: number;
  readonly rejectedTopics?: ReadonlySet<string>;
};

export type ViewServerTcpPublishIngress = {
  readonly url: string;
  readonly close: Effect.Effect<void>;
};

type RuntimeMutationEffect = Effect.Effect<unknown, unknown, never>;

const TcpAddress = Schema.Struct({
  address: Schema.String,
  family: Schema.String,
  port: Schema.Number,
});

const TcpJsonObject = Schema.Record(Schema.String, Schema.Json);

const TcpPublishCommandSchema = Schema.Union([
  Schema.Struct({
    op: Schema.Literal("publish"),
    topic: Schema.String,
    row: TcpJsonObject,
  }),
  Schema.Struct({
    op: Schema.Literal("publishMany"),
    topic: Schema.String,
    rows: Schema.Array(TcpJsonObject),
  }),
  Schema.Struct({
    op: Schema.Literal("patch"),
    topic: Schema.String,
    key: Schema.String,
    patch: TcpJsonObject,
  }),
  Schema.Struct({
    op: Schema.Literal("delete"),
    topic: Schema.String,
    key: Schema.String,
  }),
]);

type TcpPublishSocketState = {
  readonly activeFibers: Set<Fiber.Fiber<void, ViewServerTcpPublishIngressError>>;
  buffer: string;
  closed: boolean;
  queuedCommands: number;
  chain: Promise<void>;
};

type TcpPublishServerState = {
  readonly activeChains: Set<Promise<void>>;
  closed: boolean;
  readonly activeFibers: Set<Fiber.Fiber<void, ViewServerTcpPublishIngressError>>;
  queuedCommands: number;
  readonly socketStates: Map<Net.Socket, TcpPublishSocketState>;
  readonly sockets: Set<Net.Socket>;
};

type TcpDestroyableSocket = {
  readonly destroy: () => void;
};

const defaultMaxLineBytes = 1024 * 1024;
const defaultMaxConnections = 1024;
const defaultMaxGlobalQueuedCommands = 1024;
const defaultMaxQueuedCommands = 1024;
const rejectedSocketDestroyTimeoutMs = 1_000;
const strictParseOptions = {
  onExcessProperty: "error",
} as const;

const isTopicDefinitionWithSchema = (value: unknown): value is { readonly schema: RowSchema } =>
  typeof value === "object" && value !== null && Schema.isSchema(Reflect.get(value, "schema"));

const isRuntimeMutationEffect = (value: unknown): value is RuntimeMutationEffect =>
  Effect.isEffect(value);

const tcpDecodeError = (line: string, cause: unknown): ViewServerTcpPublishIngressError =>
  new ViewServerTcpPublishIngressError({
    message: "TCP publish command must be valid JSON.",
    cause: { cause, line },
    phase: "decode",
  });

const parseCommand = Effect.fn("ViewServerRuntime.tcpPublish.command.parse")(function* (
  line: string,
) {
  const value = yield* Effect.try({
    try: (): unknown => JSON.parse(line),
    catch: (cause) => tcpDecodeError(line, cause),
  });
  return yield* Result.match(
    Schema.decodeUnknownResult(TcpPublishCommandSchema)(value, strictParseOptions),
    {
      onSuccess: Effect.succeed,
      onFailure: (cause) =>
        Effect.fail(
          new ViewServerTcpPublishIngressError({
            message: "TCP publish command must match the publish command schema.",
            cause,
            phase: "decode",
          }),
        ),
    },
  );
});

const runtimeCallFailed = (
  method: string,
  topic: string,
  cause: unknown,
): ViewServerTcpPublishIngressError =>
  new ViewServerTcpPublishIngressError({
    message: `Runtime ${method} did not return an Effect for TCP publish topic ${topic}.`,
    cause,
    phase: "runtime",
    topic,
  });

const runRuntimeMutation = Effect.fn("ViewServerRuntime.tcpPublish.runtime.mutate")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  client: ViewServerRuntimeClient<Topics>,
  method: "delete" | "patch" | "publish" | "publishMany",
  topic: string,
  args: ReadonlyArray<unknown>,
) {
  const fn = Reflect.get(client, method);
  if (typeof fn !== "function") {
    return yield* runtimeCallFailed(method, topic, fn);
  }
  const effect = Reflect.apply(fn, client, args);
  if (!isRuntimeMutationEffect(effect)) {
    return yield* runtimeCallFailed(method, topic, effect);
  }
  yield* effect.pipe(
    Effect.asVoid,
    Effect.mapError(
      (cause) =>
        new ViewServerTcpPublishIngressError({
          message: `TCP publish runtime ${method} failed for topic ${topic}.`,
          cause,
          phase: "runtime",
          topic,
        }),
    ),
  );
});

const topicSchema = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  topic: string,
): Effect.Effect<RowSchema, ViewServerTcpPublishIngressError> => {
  const topicDefinition = Reflect.get(config.topics, topic);
  return isTopicDefinitionWithSchema(topicDefinition)
    ? Effect.succeed(topicDefinition.schema)
    : Effect.fail(
        new ViewServerTcpPublishIngressError({
          message: `TCP publish cannot find View Server topic ${topic}.`,
          cause: topic,
          phase: "decode",
          topic,
        }),
      );
};

const validateTcpRow = Effect.fn("ViewServerRuntime.tcpPublish.row.validate")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(config: ViewServerConfig<Topics>, topic: string, row: unknown) {
  const schema = yield* topicSchema(config, topic);
  return yield* Schema.decodeUnknownEffect(schema)(row, strictParseOptions).pipe(
    Effect.mapError(
      (cause) =>
        new ViewServerTcpPublishIngressError({
          message: `TCP publish row did not match View Server topic ${topic}.`,
          cause,
          phase: "decode",
          topic,
        }),
    ),
  );
});

const validateTcpRows = Effect.fn("ViewServerRuntime.tcpPublish.rows.validate")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(config: ViewServerConfig<Topics>, topic: string, rows: ReadonlyArray<unknown>) {
  return yield* Effect.forEach(rows, (row) => validateTcpRow(config, topic, row));
});

const validateTcpPatch = Effect.fn("ViewServerRuntime.tcpPublish.patch.validate")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(config: ViewServerConfig<Topics>, topic: string, patch: Record<string, unknown>) {
  const schema = yield* topicSchema(config, topic);
  const decodedPatch: Record<string, unknown> = {};
  for (const [field, value] of Object.entries(patch)) {
    const fieldSchema = schema.fields[field];
    if (fieldSchema === undefined) {
      return yield* new ViewServerTcpPublishIngressError({
        message: `TCP publish patch did not match View Server topic ${topic}.`,
        cause: { field },
        phase: "decode",
        topic,
      });
    }
    decodedPatch[field] = yield* Schema.decodeUnknownEffect(fieldSchema)(
      value,
      strictParseOptions,
    ).pipe(
      Effect.mapError(
        (cause) =>
          new ViewServerTcpPublishIngressError({
            message: `TCP publish patch did not match View Server topic ${topic}.`,
            cause,
            phase: "decode",
            topic,
          }),
      ),
    );
  }
  return decodedPatch;
});

const handleCommand = Effect.fn("ViewServerRuntime.tcpPublish.command.handle")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  config: ViewServerConfig<Topics>,
  client: ViewServerRuntimeClient<Topics>,
  options: ViewServerTcpPublishIngressOptions,
  line: string,
) {
  const command = yield* parseCommand(line);
  yield* ensureTopicCanBeMutated(command.topic, options);
  if (command.op === "publish") {
    const row = yield* validateTcpRow(config, command.topic, command.row);
    yield* runRuntimeMutation(client, "publish", command.topic, [command.topic, row]);
    return;
  }
  if (command.op === "publishMany") {
    const rows = yield* validateTcpRows(config, command.topic, command.rows);
    yield* runRuntimeMutation(client, "publishMany", command.topic, [command.topic, rows]);
    return;
  }
  if (command.op === "patch") {
    const patch = yield* validateTcpPatch(config, command.topic, command.patch);
    yield* runRuntimeMutation(client, "patch", command.topic, [command.topic, command.key, patch]);
    return;
  }
  yield* topicSchema(config, command.topic);
  yield* runRuntimeMutation(client, "delete", command.topic, [command.topic, command.key]);
});

const ensureTopicCanBeMutated = (
  topic: string,
  options: ViewServerTcpPublishIngressOptions,
): Effect.Effect<void, ViewServerTcpPublishIngressError> =>
  options.rejectedTopics?.has(topic) === true
    ? Effect.fail(
        new ViewServerTcpPublishIngressError({
          message: `TCP publish cannot mutate source-owned View Server topic ${topic}.`,
          cause: topic,
          phase: "runtime",
          topic,
        }),
      )
    : Effect.void;

const wireError = (cause: Cause.Cause<ViewServerTcpPublishIngressError>): object => {
  const failure = Cause.findErrorOption(cause);
  if (failure._tag === "Some") {
    return {
      ok: false,
      error: {
        _tag: failure.value._tag,
        message: failure.value.message,
        phase: failure.value.phase,
        ...(failure.value.topic === undefined ? {} : { topic: failure.value.topic }),
      },
    };
  }
  return {
    ok: false,
    error: {
      _tag: "ViewServerTcpPublishIngressError",
      message: "TCP publish command failed with an untyped cause.",
      phase: "runtime",
    },
  };
};

const wireSuccess = (): object => ({ ok: true });

const jsonLine = (value: object): string => `${JSON.stringify(value)}\n`;

const endTcpError = (
  socket: Net.Socket,
  state: TcpPublishSocketState,
  error: ViewServerTcpPublishIngressError,
): void => {
  state.closed = true;
  interruptSocketFibers(state);
  socket.end(jsonLine(tcpErrorPayload(error)));
};

/** @internal Package-local test hook; not exported from @view-server/runtime. */
export const writeTcpJsonLineOrClose = (
  write: (chunk: string) => boolean,
  end: () => void,
  state: TcpPublishSocketState,
  value: object,
): void => {
  if (!write(jsonLine(value))) {
    state.closed = true;
    interruptSocketFibers(state);
    end();
  }
};

/** @internal Package-local test hook; not exported from @view-server/runtime. */
export const rejectTcpSocketWhenClosed = (
  closed: boolean,
  socket: TcpDestroyableSocket,
): boolean => {
  if (closed) {
    socket.destroy();
    return true;
  }
  return false;
};

const tcpErrorPayload = (error: ViewServerTcpPublishIngressError): object => ({
  ok: false,
  error: {
    _tag: error._tag,
    message: error.message,
    phase: error.phase,
    topic: error.topic,
  },
});

const tcpQueueExceededError = (maxQueuedCommands: number): ViewServerTcpPublishIngressError =>
  new ViewServerTcpPublishIngressError({
    message: `TCP publish command queue exceeded ${maxQueuedCommands} commands.`,
    cause: { maxQueuedCommands },
    phase: "backpressure",
  });

const tcpGlobalQueueExceededError = (
  maxGlobalQueuedCommands: number,
): ViewServerTcpPublishIngressError =>
  new ViewServerTcpPublishIngressError({
    message: `TCP publish global command queue exceeded ${maxGlobalQueuedCommands} commands.`,
    cause: { maxGlobalQueuedCommands },
    phase: "backpressure",
  });

const tcpLineExceededError = (maxLineBytes: number): ViewServerTcpPublishIngressError =>
  new ViewServerTcpPublishIngressError({
    message: `TCP publish command exceeded ${maxLineBytes} bytes.`,
    cause: { maxLineBytes },
    phase: "backpressure",
  });

const tcpPartialLineExceededError = (maxLineBytes: number): ViewServerTcpPublishIngressError =>
  new ViewServerTcpPublishIngressError({
    message: `TCP publish command exceeded ${maxLineBytes} bytes without a newline.`,
    cause: { maxLineBytes },
    phase: "backpressure",
  });

const tcpConnectionExceededError = (maxConnections: number): ViewServerTcpPublishIngressError =>
  new ViewServerTcpPublishIngressError({
    message: `TCP publish connection count exceeded ${maxConnections} sockets.`,
    cause: { maxConnections },
    phase: "backpressure",
  });

const endRejectedSocket = (
  socket: Net.Socket,
  state: TcpPublishServerState,
  error: ViewServerTcpPublishIngressError,
): void => {
  state.sockets.add(socket);
  socket.on("error", socket.destroy.bind(socket));
  socket.on("close", () => state.sockets.delete(socket));
  socket.setTimeout(rejectedSocketDestroyTimeoutMs);
  socket.once("timeout", socket.destroy.bind(socket));
  socket.end(jsonLine(tcpErrorPayload(error)), socket.destroy.bind(socket));
};

const executeLine = async <const Topics extends ViewServerRuntimeTopicDefinitions>(
  socket: Net.Socket,
  state: TcpPublishSocketState,
  serverState: TcpPublishServerState,
  config: ViewServerConfig<Topics>,
  client: ViewServerRuntimeClient<Topics>,
  options: ViewServerTcpPublishIngressOptions,
  line: string,
): Promise<void> => {
  try {
    if (state.closed || serverState.closed) {
      return;
    }
    const fiber = Effect.runFork(handleCommand(config, client, options, line));
    serverState.activeFibers.add(fiber);
    state.activeFibers.add(fiber);
    const exit = await Effect.runPromise(Fiber.await(fiber));
    serverState.activeFibers.delete(fiber);
    state.activeFibers.delete(fiber);
    if (state.closed || serverState.closed) {
      return;
    }
    if (Exit.isSuccess(exit)) {
      writeTcpJsonLineOrClose(
        socket.write.bind(socket),
        socket.end.bind(socket),
        state,
        wireSuccess(),
      );
      return;
    }
    writeTcpJsonLineOrClose(
      socket.write.bind(socket),
      socket.end.bind(socket),
      state,
      wireError(exit.cause),
    );
  } finally {
    state.queuedCommands -= 1;
    serverState.queuedCommands -= 1;
  }
};

const enqueueLine = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  socket: Net.Socket,
  state: TcpPublishSocketState,
  serverState: TcpPublishServerState,
  config: ViewServerConfig<Topics>,
  client: ViewServerRuntimeClient<Topics>,
  options: ViewServerTcpPublishIngressOptions,
  line: string,
): void => {
  const maxQueuedCommands = options.maxQueuedCommands ?? defaultMaxQueuedCommands;
  const maxGlobalQueuedCommands = options.maxGlobalQueuedCommands ?? defaultMaxGlobalQueuedCommands;
  if (state.closed || serverState.closed) {
    return;
  }
  if (state.queuedCommands >= maxQueuedCommands) {
    endTcpError(socket, state, tcpQueueExceededError(maxQueuedCommands));
    return;
  }
  if (serverState.queuedCommands >= maxGlobalQueuedCommands) {
    endTcpError(socket, state, tcpGlobalQueueExceededError(maxGlobalQueuedCommands));
    return;
  }
  state.queuedCommands += 1;
  serverState.queuedCommands += 1;
  const previousChain = state.chain;
  const chain = (async () => {
    await Promise.allSettled([previousChain]);
    await Promise.allSettled([
      executeLine(socket, state, serverState, config, client, options, line),
    ]);
  })();
  state.chain = chain;
  serverState.activeChains.add(chain);
  const cleanup = () => {
    serverState.activeChains.delete(chain);
    if (state.closed) {
      serverState.socketStates.delete(socket);
    }
  };
  void chain.then(cleanup);
};

const interruptSocketFibers = (state: TcpPublishSocketState): void => {
  if (state.activeFibers.size > 0) {
    Effect.runFork(Effect.forEach(state.activeFibers, Fiber.interrupt, { discard: true }));
  }
};

const installSocketHandler = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  socket: Net.Socket,
  state: TcpPublishSocketState,
  serverState: TcpPublishServerState,
  config: ViewServerConfig<Topics>,
  client: ViewServerRuntimeClient<Topics>,
  options: ViewServerTcpPublishIngressOptions,
): void => {
  socket.setEncoding("utf8");
  socket.on("data", (chunk: string) => {
    if (state.closed || serverState.closed) {
      return;
    }
    const nextBuffer = state.buffer + chunk;
    const maxLineBytes = options.maxLineBytes ?? defaultMaxLineBytes;
    const lines = nextBuffer.split("\n");
    const partialLine = String(lines.pop());
    for (const line of lines) {
      if (Buffer.byteLength(line, "utf8") > maxLineBytes) {
        endTcpError(socket, state, tcpLineExceededError(maxLineBytes));
        return;
      }
      const trimmed = line.trim();
      if (trimmed.length > 0) {
        enqueueLine(socket, state, serverState, config, client, options, trimmed);
      }
    }
    state.buffer = partialLine;
    if (Buffer.byteLength(state.buffer, "utf8") > maxLineBytes) {
      endTcpError(socket, state, tcpPartialLineExceededError(maxLineBytes));
    }
  });
};

const closeTcpServer = (server: Net.Server, state: TcpPublishServerState): Effect.Effect<void> =>
  Effect.gen(function* () {
    if (state.closed) {
      return;
    }
    state.closed = true;
    for (const socketState of state.socketStates.values()) {
      socketState.closed = true;
    }
    const serverClosed = new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
    for (const socket of state.sockets) {
      socket.destroy();
    }
    yield* Effect.forEach(state.activeFibers, Fiber.interrupt, { discard: true });
    yield* Effect.promise(() => Promise.allSettled(state.activeChains));
    yield* Effect.promise(() => serverClosed);
  });

const validateTcpPublishOptions = (
  options: ViewServerTcpPublishIngressOptions,
): Effect.Effect<void, ViewServerTcpPublishIngressError> => {
  if (!Number.isSafeInteger(options.port) || options.port < 0 || options.port > 65535) {
    return Effect.fail(
      new ViewServerTcpPublishIngressError({
        message: "TCP publish port must be a safe integer between 0 and 65535.",
        cause: options.port,
        phase: "configuration",
      }),
    );
  }
  if (
    options.maxLineBytes !== undefined &&
    (!Number.isSafeInteger(options.maxLineBytes) || options.maxLineBytes <= 0)
  ) {
    return Effect.fail(
      new ViewServerTcpPublishIngressError({
        message: "TCP publish maxLineBytes must be a positive safe integer.",
        cause: options.maxLineBytes,
        phase: "configuration",
      }),
    );
  }
  if (
    options.maxConnections !== undefined &&
    (!Number.isSafeInteger(options.maxConnections) || options.maxConnections <= 0)
  ) {
    return Effect.fail(
      new ViewServerTcpPublishIngressError({
        message: "TCP publish maxConnections must be a positive safe integer.",
        cause: options.maxConnections,
        phase: "configuration",
      }),
    );
  }
  if (
    options.maxQueuedCommands !== undefined &&
    (!Number.isSafeInteger(options.maxQueuedCommands) || options.maxQueuedCommands <= 0)
  ) {
    return Effect.fail(
      new ViewServerTcpPublishIngressError({
        message: "TCP publish maxQueuedCommands must be a positive safe integer.",
        cause: options.maxQueuedCommands,
        phase: "configuration",
      }),
    );
  }
  if (
    options.maxGlobalQueuedCommands !== undefined &&
    (!Number.isSafeInteger(options.maxGlobalQueuedCommands) || options.maxGlobalQueuedCommands <= 0)
  ) {
    return Effect.fail(
      new ViewServerTcpPublishIngressError({
        message: "TCP publish maxGlobalQueuedCommands must be a positive safe integer.",
        cause: options.maxGlobalQueuedCommands,
        phase: "configuration",
      }),
    );
  }
  return Effect.void;
};

/** @internal Package-local test hook; not exported from @view-server/runtime. */
export const installTcpPublishAcceptedSocket = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  socket: Net.Socket,
  state: TcpPublishServerState,
  config: ViewServerConfig<Topics>,
  client: ViewServerRuntimeClient<Topics>,
  options: ViewServerTcpPublishIngressOptions,
): void => {
  if (rejectTcpSocketWhenClosed(state.closed, socket)) {
    return;
  }
  const maxConnections = options.maxConnections ?? defaultMaxConnections;
  if (state.sockets.size >= maxConnections) {
    endRejectedSocket(socket, state, tcpConnectionExceededError(maxConnections));
    return;
  }
  const socketState: TcpPublishSocketState = {
    activeFibers: new Set(),
    buffer: "",
    chain: Promise.resolve(),
    closed: false,
    queuedCommands: 0,
  };
  state.sockets.add(socket);
  state.socketStates.set(socket, socketState);
  socket.on("error", socket.destroy.bind(socket));
  socket.on("close", () => {
    socketState.closed = true;
    state.sockets.delete(socket);
    interruptSocketFibers(socketState);
    void socketState.chain.then(() => state.socketStates.delete(socket));
  });
  installSocketHandler(socket, socketState, state, config, client, options);
};

export const makeViewServerTcpPublishIngress = Effect.fn("ViewServerRuntime.tcpPublish.make")(
  function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
    config: ViewServerConfig<Topics>,
    client: ViewServerRuntimeClient<Topics>,
    options: ViewServerTcpPublishIngressOptions,
  ) {
    yield* validateTcpPublishOptions(options);
    const host = options.host ?? "127.0.0.1";
    const state: TcpPublishServerState = {
      activeChains: new Set(),
      activeFibers: new Set(),
      closed: false,
      queuedCommands: 0,
      socketStates: new Map(),
      sockets: new Set(),
    };
    const server = Net.createServer((socket) => {
      installTcpPublishAcceptedSocket(socket, state, config, client, options);
    });
    yield* Effect.callback<void, ViewServerTcpPublishIngressError>((resume) => {
      server.once("error", (cause) => {
        resume(
          Effect.fail(
            new ViewServerTcpPublishIngressError({
              message: "TCP publish server failed to listen.",
              cause,
              phase: "listen",
            }),
          ),
        );
      });
      server.listen({ host, port: options.port }, () => {
        resume(Effect.void);
      });
      return closeTcpServer(server, state);
    });
    const address = Schema.decodeUnknownSync(TcpAddress)(server.address());
    return {
      url: `tcp://${address.address}:${address.port}`,
      close: closeTcpServer(server, state),
    };
  },
);
