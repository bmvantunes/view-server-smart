// Generated-style protobuf fixture for runtime Kafka e2e tests.
import type { Message } from "@bufbuild/protobuf";
import { fileDesc, messageDesc } from "@bufbuild/protobuf/codegenv2";
import type { GenFile, GenMessage } from "@bufbuild/protobuf/codegenv2";

/**
 * Describes the file viewserver/runtime/test.proto.
 */
export const file_viewserver_runtime_test: GenFile = fileDesc(
  "Ch12aWV3c2VydmVyL3J1bnRpbWUvdGVzdC5wcm90bxIXdmlld3NlcnZlci5ydW50aW1lLnRlc3QiLAoKT3JkZXJWYWx1ZRIRCgtjdXN0b21lcl9pZBgBKAkSCwoFcHJpY2UYAigBIhoKCE9yZGVyS2V5Eg4KCG9yZGVyX2lkGAEoCWIGcHJvdG8z",
);

/**
 * @generated from message viewserver.runtime.test.OrderValue
 */
export type OrderValue = Message<"viewserver.runtime.test.OrderValue"> & {
  readonly customerId: string;
  readonly price: number;
};

/**
 * @generated from message viewserver.runtime.test.OrderValue
 */
export type OrderValueJson = {
  readonly customerId?: string;
  readonly price?: number;
};

/**
 * Describes the message viewserver.runtime.test.OrderValue.
 * Use `create(OrderValueSchema)` to create a new message.
 */
export const OrderValueSchema: GenMessage<OrderValue, { jsonType: OrderValueJson }> = messageDesc(
  file_viewserver_runtime_test,
  0,
);

/**
 * @generated from message viewserver.runtime.test.OrderKey
 */
export type OrderKey = Message<"viewserver.runtime.test.OrderKey"> & {
  readonly orderId: string;
};

/**
 * @generated from message viewserver.runtime.test.OrderKey
 */
export type OrderKeyJson = {
  readonly orderId?: string;
};

/**
 * Describes the message viewserver.runtime.test.OrderKey.
 * Use `create(OrderKeySchema)` to create a new message.
 */
export const OrderKeySchema: GenMessage<OrderKey, { jsonType: OrderKeyJson }> = messageDesc(
  file_viewserver_runtime_test,
  1,
);
