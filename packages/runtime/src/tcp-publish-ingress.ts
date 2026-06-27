import type {
  RowSchema,
  ViewServerConfig,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
} from "@view-server/config";
import { Cause, Effect, Exit, Fiber, Result, Schema, SchemaAST } from "effect";
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
type TcpPublishCommandError = ViewServerRuntimeError | ViewServerTcpPublishIngressError;

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
  readonly activeFibers: Set<Fiber.Fiber<void, TcpPublishCommandError>>;
  buffer: string;
  closed: boolean;
  queuedCommands: number;
  chain: Promise<void>;
};

type TcpPublishServerState = {
  readonly activeChains: Set<Promise<void>>;
  closed: boolean;
  readonly activeFibers: Set<Fiber.Fiber<void, TcpPublishCommandError>>;
  queuedCommands: number;
  readonly socketStates: Map<Net.Socket, TcpPublishSocketState>;
  readonly sockets: Set<Net.Socket>;
};

type TcpDestroyableSocket = {
  readonly destroy: () => void;
};

type TcpErrorHandlingServer = {
  readonly on: (event: "error", listener: (cause: Error) => void) => unknown;
};

type TcpResponseSocket = {
  readonly destroyed: boolean;
  readonly off: (event: "close" | "error", listener: () => void) => unknown;
  readonly once: (event: "close" | "error", listener: () => void) => unknown;
  readonly write: (chunk: string, callback: () => void) => boolean;
};

type TcpFieldSchema = NonNullable<RowSchema["fields"][string]>;
type TcpSuspendedSchema = {
  readonly ast: SchemaAST.Suspend;
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

const isPlainRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && Object.getPrototypeOf(value) === Object.prototype;

const isSuspendSchema = (schema: TcpFieldSchema): schema is TcpFieldSchema & TcpSuspendedSchema =>
  SchemaAST.isSuspend(schema.ast);

const isViewServerRuntimeError = (value: unknown): value is ViewServerRuntimeError => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const tag = Reflect.get(value, "_tag");
  const code = Reflect.get(value, "code");
  const message = Reflect.get(value, "message");
  return (
    (tag === "ViewServerRuntimeError" || tag === "ViewServerBackpressureError") &&
    typeof code === "string" &&
    typeof message === "string"
  );
};

const tcpDecodeError = (line: string, cause: unknown): ViewServerTcpPublishIngressError =>
  new ViewServerTcpPublishIngressError({
    message: "TCP publish command must be valid JSON.",
    cause: { cause, line },
    phase: "decode",
  });

const parseCommand = Effect.fn("ViewServerRuntime.tcpPublish.command.parse")(function* (
  line: string,
) {
  const value = yield* Schema.decodeUnknownEffect(Schema.UnknownFromJsonString)(
    line,
    strictParseOptions,
  ).pipe(Effect.mapError((cause) => tcpDecodeError(line, cause)));
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
      (cause): TcpPublishCommandError =>
        isViewServerRuntimeError(cause)
          ? cause
          : new ViewServerTcpPublishIngressError({
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

const tcpDecodeSchemaError = (
  topic: string,
  phase: "patch" | "row",
  cause: unknown,
): ViewServerTcpPublishIngressError =>
  new ViewServerTcpPublishIngressError({
    message: `TCP publish ${phase} did not match View Server topic ${topic}.`,
    cause,
    phase: "decode",
    topic,
  });

const tcpFieldSchemaFromAst = (ast: SchemaAST.AST): TcpFieldSchema => Schema.make(ast);

const setDecodedField = (record: Record<string, unknown>, field: string, value: unknown): void => {
  Object.defineProperty(record, field, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
};

const nestedTcpFieldAst = (ast: SchemaAST.Objects, field: string): SchemaAST.AST | undefined =>
  ast.propertySignatures.find((property) => property.name === field)?.type;

const arrayElementAst = (
  ast: SchemaAST.Arrays,
  index: number,
  length: number,
): SchemaAST.AST | undefined => {
  if (index < ast.elements.length) {
    return ast.elements[index];
  }
  if (ast.rest.length === 0) {
    return undefined;
  }
  if (ast.rest.length === 1) {
    return ast.rest[0];
  }
  const trailingCount = ast.rest.length - 1;
  const firstTrailingIndex = length - trailingCount;
  if (index >= firstTrailingIndex) {
    const trailingIndex = index - firstTrailingIndex + 1;
    return ast.rest[trailingIndex];
  }
  return ast.rest[0];
};

const decodeTcpFieldForRuntimeInternal: (
  schema: TcpFieldSchema,
  topic: string,
  phase: "patch" | "row",
  value: unknown,
) => Effect.Effect<unknown, ViewServerTcpPublishIngressError> = Effect.fn(
  "ViewServerRuntime.tcpPublish.field.decode.internal",
)(function* (schema, topic, phase, value) {
  const rawDecode = yield* Effect.exit(
    Schema.decodeUnknownEffect(schema)(value, strictParseOptions),
  );
  if (Exit.isSuccess(rawDecode)) {
    // Transform schemas such as BigIntFromString must keep the JSON input shape for the engine path.
    return value;
  }
  if (isSuspendSchema(schema)) {
    const decodedValue = yield* decodeTcpFieldForRuntimeInternal(
      Schema.make(schema.ast.thunk()),
      topic,
      phase,
      value,
    );
    yield* Schema.decodeUnknownEffect(schema)(decodedValue, strictParseOptions).pipe(
      Effect.asVoid,
      Effect.mapError((cause) => tcpDecodeSchemaError(topic, phase, cause)),
    );
    return decodedValue;
  }
  if (SchemaAST.isObjects(schema.ast) && isPlainRecord(value)) {
    const decodedValue: Record<string, unknown> = {};
    const indexSignature = schema.ast.indexSignatures[0];
    for (const [field, fieldValue] of Object.entries(value)) {
      const fieldAst = nestedTcpFieldAst(schema.ast, field) ?? indexSignature?.type;
      if (fieldAst === undefined) {
        return yield* tcpDecodeSchemaError(topic, phase, { field });
      }
      setDecodedField(
        decodedValue,
        field,
        yield* decodeTcpFieldForRuntimeInternal(
          tcpFieldSchemaFromAst(fieldAst),
          topic,
          phase,
          fieldValue,
        ),
      );
    }
    yield* Schema.decodeUnknownEffect(schema)(decodedValue, strictParseOptions).pipe(
      Effect.asVoid,
      Effect.mapError((cause) => tcpDecodeSchemaError(topic, phase, cause)),
    );
    return decodedValue;
  }
  if (SchemaAST.isArrays(schema.ast) && Array.isArray(value)) {
    const decodedValue: Array<unknown> = Array.from(value);
    for (const [index, item] of value.entries()) {
      const elementAst = arrayElementAst(schema.ast, index, value.length);
      if (elementAst !== undefined) {
        decodedValue[index] = yield* decodeTcpFieldForRuntimeInternal(
          tcpFieldSchemaFromAst(elementAst),
          topic,
          phase,
          item,
        );
      }
    }
    yield* Schema.decodeUnknownEffect(schema)(decodedValue, strictParseOptions).pipe(
      Effect.asVoid,
      Effect.mapError((cause) => tcpDecodeSchemaError(topic, phase, cause)),
    );
    return decodedValue;
  }
  if (SchemaAST.isUnion(schema.ast)) {
    for (const member of schema.ast.types) {
      const memberDecode = yield* Effect.exit(
        decodeTcpFieldForRuntimeInternal(tcpFieldSchemaFromAst(member), topic, phase, value).pipe(
          Effect.flatMap((decodedValue) =>
            Schema.decodeUnknownEffect(schema)(decodedValue, strictParseOptions).pipe(
              Effect.as(decodedValue),
            ),
          ),
        ),
      );
      if (Exit.isSuccess(memberDecode)) {
        return memberDecode.value;
      }
    }
  }
  return yield* Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(
    value,
    strictParseOptions,
  ).pipe(Effect.mapError((cause) => tcpDecodeSchemaError(topic, phase, cause)));
});

const decodeTcpFieldForRuntime = Effect.fn("ViewServerRuntime.tcpPublish.field.decode")(function* (
  schema: TcpFieldSchema,
  topic: string,
  phase: "patch" | "row",
  value: unknown,
) {
  return yield* decodeTcpFieldForRuntimeInternal(schema, topic, phase, value);
});

const decodeTcpRow = Effect.fn("ViewServerRuntime.tcpPublish.row.decode")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(config: ViewServerConfig<Topics>, topic: string, row: Record<string, unknown>) {
  const schema = yield* topicSchema(config, topic);
  const decodedRow: Record<string, unknown> = {};
  for (const [field, value] of Object.entries(row)) {
    const fieldSchema = Object.entries(schema.fields).find(
      ([fieldName]) => fieldName === field,
    )?.[1];
    if (fieldSchema === undefined) {
      return yield* tcpDecodeSchemaError(topic, "row", { field });
    }
    setDecodedField(
      decodedRow,
      field,
      yield* decodeTcpFieldForRuntime(fieldSchema, topic, "row", value),
    );
  }
  yield* Schema.decodeUnknownEffect(schema)(decodedRow, strictParseOptions).pipe(
    Effect.asVoid,
    Effect.mapError((cause) => tcpDecodeSchemaError(topic, "row", cause)),
  );
  return decodedRow;
});

const decodeTcpRows = Effect.fn("ViewServerRuntime.tcpPublish.rows.decode")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(config: ViewServerConfig<Topics>, topic: string, rows: ReadonlyArray<Record<string, unknown>>) {
  return yield* Effect.forEach(rows, (row) => decodeTcpRow(config, topic, row));
});

const decodeTcpPatch = Effect.fn("ViewServerRuntime.tcpPublish.patch.decode")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(config: ViewServerConfig<Topics>, topic: string, patch: Record<string, unknown>) {
  const schema = yield* topicSchema(config, topic);
  const decodedPatch: Record<string, unknown> = {};
  for (const [field, value] of Object.entries(patch)) {
    const fieldSchema = Object.entries(schema.fields).find(
      ([fieldName]) => fieldName === field,
    )?.[1];
    if (fieldSchema === undefined) {
      return yield* tcpDecodeSchemaError(topic, "patch", { field });
    }
    setDecodedField(
      decodedPatch,
      field,
      yield* decodeTcpFieldForRuntime(fieldSchema, topic, "patch", value),
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
    const row = yield* decodeTcpRow(config, command.topic, command.row);
    yield* runRuntimeMutation(client, "publish", command.topic, [command.topic, row]);
    return;
  }
  if (command.op === "publishMany") {
    const rows = yield* decodeTcpRows(config, command.topic, command.rows);
    yield* runRuntimeMutation(client, "publishMany", command.topic, [command.topic, rows]);
    return;
  }
  if (command.op === "patch") {
    const patch = yield* decodeTcpPatch(config, command.topic, command.patch);
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

const wireError = (cause: Cause.Cause<TcpPublishCommandError>): object => {
  const failure = Cause.findErrorOption(cause);
  if (failure._tag === "Some") {
    if (isViewServerRuntimeError(failure.value)) {
      return {
        ok: false,
        error: {
          _tag: failure.value._tag,
          code: failure.value.code,
          message: failure.value.message,
          ...(failure.value.topic === undefined ? {} : { topic: failure.value.topic }),
        },
      };
    }
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
  socket.setTimeout(rejectedSocketDestroyTimeoutMs);
  socket.once("timeout", socket.destroy.bind(socket));
  socket.end(jsonLine(tcpErrorPayload(error)), socket.destroy.bind(socket));
};

/** @internal Package-local test hook; not exported from @view-server/runtime. */
export const writeTcpJsonLine = (
  socket: TcpResponseSocket,
  state: TcpPublishSocketState,
  value: object,
): Promise<void> => {
  if (state.closed || socket.destroyed) {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    let settled = false;
    const settle = () => {
      if (settled) {
        return;
      }
      settled = true;
      socket.off("close", settle);
      socket.off("error", settle);
      resolve();
    };
    socket.once("close", settle);
    socket.once("error", settle);
    socket.write(jsonLine(value), settle);
  });
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
      await writeTcpJsonLine(socket, state, wireSuccess());
      return;
    }
    await writeTcpJsonLine(socket, state, wireError(exit.cause));
  } finally {
    state.queuedCommands -= 1;
    serverState.queuedCommands -= 1;
  }
};

/** @internal Package-local test hook; not exported from @view-server/runtime. */
export const tcpPublishUrl = (address: {
  readonly address: string;
  readonly port: number;
}): string => {
  const host = address.address.includes(":") ? `[${address.address}]` : address.address;
  return `tcp://${host}:${address.port}`;
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

/** @internal Package-local test hook; not exported from @view-server/runtime. */
export const installTcpServerSteadyStateErrorHandler = (
  server: TcpErrorHandlingServer,
  close: Effect.Effect<void>,
): void => {
  server.on("error", (cause) => {
    Effect.runFork(
      Effect.logWarning("TCP publish server emitted an error after listen; closing ingress.").pipe(
        Effect.annotateLogs({ cause }),
        Effect.andThen(close),
      ),
    );
  });
};

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
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const close = closeTcpServer(server, state);
        yield* restore(
          Effect.callback<void, ViewServerTcpPublishIngressError>((resume) => {
            const onStartupError = (cause: Error) => {
              resume(
                Effect.fail(
                  new ViewServerTcpPublishIngressError({
                    message: "TCP publish server failed to listen.",
                    cause,
                    phase: "listen",
                  }),
                ),
              );
            };
            server.once("error", onStartupError);
            server.listen({ host, port: options.port }, () => {
              server.off("error", onStartupError);
              installTcpServerSteadyStateErrorHandler(server, close);
              resume(Effect.void);
            });
            return close;
          }),
        );
        const address = Schema.decodeUnknownSync(TcpAddress)(server.address());
        return {
          url: tcpPublishUrl(address),
          close,
        };
      }),
    );
  },
);
