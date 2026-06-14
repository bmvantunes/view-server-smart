import { Effect, Schema } from "effect";

export type JsonFieldSchema = Schema.Codec<unknown, unknown, never, never>;

type JsonFieldCodecErrors<E> = {
  readonly invalid: (message: string) => E;
  readonly notJsonSafe: (message: string) => E;
};

type JsonFieldCodecContext<E> = {
  readonly invalid: (message: string) => E;
  readonly invalidMessage: (message: string) => string;
};

type JsonFieldEncodeContext<E> = JsonFieldCodecContext<E> & {
  readonly notJsonSafe: (message: string) => E;
  readonly notJsonSafeMessage: (message: string) => string;
};

type NamedJsonFieldCodecContext<E> = {
  readonly field: string;
  readonly invalid: (message: string) => E;
  readonly invalidPrefix: string;
};

type NamedJsonFieldEncodeContext<E> = NamedJsonFieldCodecContext<E> & {
  readonly notJsonSafePrefix: string;
};

export const encodeJsonFieldValue = Effect.fn("ViewServerProtocol.jsonField.encode")(function* <E>(
  schema: JsonFieldSchema,
  value: unknown,
  errors: JsonFieldCodecErrors<E>,
) {
  const encoded = yield* Schema.encodeUnknownEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.mapError((error) => errors.invalid(error.message)),
  );
  return yield* Schema.decodeUnknownEffect(Schema.Json)(encoded).pipe(
    Effect.mapError((error) => errors.notJsonSafe(error.message)),
  );
});

export const decodeJsonFieldValue = Effect.fn("ViewServerProtocol.jsonField.decode")(function* <E>(
  schema: JsonFieldSchema,
  value: unknown,
  errors: Pick<JsonFieldCodecErrors<E>, "invalid">,
) {
  return yield* Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.mapError((error) => errors.invalid(error.message)),
  );
});

export const encodeContextualJsonFieldValue = Effect.fn(
  "ViewServerProtocol.jsonField.contextual.encode",
)(function* <E>(schema: JsonFieldSchema, value: unknown, context: JsonFieldEncodeContext<E>) {
  return yield* encodeJsonFieldValue(schema, value, {
    invalid: (message) => context.invalid(context.invalidMessage(message)),
    notJsonSafe: (message) => context.notJsonSafe(context.notJsonSafeMessage(message)),
  });
});

export const decodeContextualJsonFieldValue = Effect.fn(
  "ViewServerProtocol.jsonField.contextual.decode",
)(function* <E>(schema: JsonFieldSchema, value: unknown, context: JsonFieldCodecContext<E>) {
  return yield* decodeJsonFieldValue(schema, value, {
    invalid: (message) => context.invalid(context.invalidMessage(message)),
  });
});

export const encodeNamedJsonFieldValue = Effect.fn("ViewServerProtocol.jsonField.named.encode")(
  function* <E>(
    schema: JsonFieldSchema,
    value: unknown,
    { field, invalid, invalidPrefix, notJsonSafePrefix }: NamedJsonFieldEncodeContext<E>,
  ) {
    return yield* encodeContextualJsonFieldValue(schema, value, {
      invalid,
      invalidMessage: (message) => `${invalidPrefix} ${field}: ${message}`,
      notJsonSafe: invalid,
      notJsonSafeMessage: (message) => `${notJsonSafePrefix} ${field} is not JSON-safe: ${message}`,
    });
  },
);

export const decodeNamedJsonFieldValue = Effect.fn("ViewServerProtocol.jsonField.named.decode")(
  function* <E>(
    schema: JsonFieldSchema,
    value: unknown,
    { field, invalid, invalidPrefix }: NamedJsonFieldCodecContext<E>,
  ) {
    return yield* decodeContextualJsonFieldValue(schema, value, {
      invalid,
      invalidMessage: (message) => `${invalidPrefix} ${field}: ${message}`,
    });
  },
);
