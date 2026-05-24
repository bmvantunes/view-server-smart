import { Schema } from "effect";
import type { InvalidQueryError } from "./raw-query-compiler";

export class InvalidTopicError extends Schema.TaggedErrorClass<InvalidTopicError>()(
  "InvalidTopicError",
  {
    topic: Schema.String,
    message: Schema.String,
  },
) {}

export class InvalidRowError extends Schema.TaggedErrorClass<InvalidRowError>()("InvalidRowError", {
  topic: Schema.String,
  message: Schema.String,
}) {}

export class UnsupportedQueryError extends Schema.TaggedErrorClass<UnsupportedQueryError>()(
  "UnsupportedQueryError",
  {
    topic: Schema.String,
    message: Schema.String,
  },
) {}

export class EngineClosedError extends Schema.TaggedErrorClass<EngineClosedError>()(
  "EngineClosedError",
  {
    message: Schema.String,
  },
) {}

export type ColumnLiveViewEngineError =
  | InvalidTopicError
  | InvalidRowError
  | UnsupportedQueryError
  | InvalidQueryError
  | EngineClosedError;
