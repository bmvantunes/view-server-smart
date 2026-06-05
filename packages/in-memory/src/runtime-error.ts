import {
  InvalidQueryError,
  InvalidRowError,
  InvalidTopicError,
  type ColumnLiveViewEngineError,
} from "@view-server/column-live-view-engine";
import type { ViewServerRuntimeError } from "@view-server/config";

export const engineErrorToRuntimeError = (
  error: ColumnLiveViewEngineError,
): ViewServerRuntimeError => {
  if (error instanceof InvalidTopicError) {
    return {
      _tag: "ViewServerRuntimeError",
      code: "InvalidTopic",
      message: error.message,
      topic: error.topic,
    };
  }
  if (error instanceof InvalidRowError) {
    return {
      _tag: "ViewServerRuntimeError",
      code: "InvalidRow",
      message: error.message,
      topic: error.topic,
    };
  }
  if (error instanceof InvalidQueryError) {
    return {
      _tag: "ViewServerRuntimeError",
      code: "InvalidQuery",
      message: error.message,
      topic: error.topic,
    };
  }
  return {
    _tag: "ViewServerRuntimeError",
    code: "RuntimeUnavailable",
    message: error.message,
  };
};
