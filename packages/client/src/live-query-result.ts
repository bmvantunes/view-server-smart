import type { LiveQueryResult } from "@view-server/config";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import { liveQueryFailureResult } from "./live-query-error";
import { initialClientState, liveQueryResult, type ClientState } from "./live-query-state";

export const liveQueryResultFromAsyncResult = <Row>(
  result: AsyncResult.AsyncResult<ClientState<Row>, unknown>,
): LiveQueryResult<Row> => {
  if (AsyncResult.isFailure(result)) {
    return liveQueryFailureResult<Row>(result.cause);
  }
  if (AsyncResult.isSuccess(result)) {
    return liveQueryResult(result.value);
  }
  return liveQueryResult(initialClientState<Row>());
};
