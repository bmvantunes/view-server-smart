import type { LiveQueryResult } from "@view-server/config";
import { Cause, Option } from "effect";
import { initialClientState, liveQueryResult } from "./live-query-state";

const isFailureLike = (
  value: unknown,
): value is { readonly code: string; readonly message: string } => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  if (!("code" in value) || !("message" in value)) {
    return false;
  }
  return typeof value.code === "string" && typeof value.message === "string";
};

const statusCodeFromFailureCode = (
  code: string,
): NonNullable<LiveQueryResult<never>["statusCode"]> => {
  if (
    code === "Ready" ||
    code === "SnapshotStale" ||
    code === "SubscriptionClosed" ||
    code === "TransportError" ||
    code === "BackpressureExceeded" ||
    code === "InvalidTopic" ||
    code === "InvalidRow" ||
    code === "InvalidQuery" ||
    code === "UnsupportedQuery" ||
    code === "RuntimeUnavailable" ||
    code === "RuntimeResetFailed"
  ) {
    return code;
  }
  return "TransportError";
};

export const liveQueryFailureResult = <Row>(cause: Cause.Cause<unknown>): LiveQueryResult<Row> => {
  const failure = Cause.findErrorOption(cause);
  if (Option.isSome(failure) && isFailureLike(failure.value)) {
    return {
      ...liveQueryResult(initialClientState<Row>()),
      status: "error",
      statusCode: statusCodeFromFailureCode(failure.value.code),
      message: failure.value.message,
    };
  }
  const defect = Cause.squash(cause);
  return {
    ...liveQueryResult(initialClientState<Row>()),
    status: "error",
    statusCode: "TransportError",
    message: String(defect),
  };
};
