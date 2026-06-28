import { create, toBinary } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import type { Message } from "@bufbuild/protobuf";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";

export type OrderValueMessage = Message<"viewserver.example.OrderValue"> & {
  readonly customerId: string;
  readonly status: "open" | "closed" | "cancelled";
  readonly price: number;
  readonly updatedAt: number;
};

export type OrderRouteMessage = Message<"viewserver.example.OrderRoute"> & {
  readonly strategyId: string;
  readonly region: string;
};

const base64FromBytes = (bytes: Uint8Array) =>
  globalThis.btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(""));

const protoFile = fileDesc(
  base64FromBytes(
    toBinary(
      FileDescriptorProtoSchema,
      create(FileDescriptorProtoSchema, {
        name: "viewserver/example.proto",
        package: "viewserver.example",
        syntax: "proto3",
        messageType: [
          {
            name: "OrderValue",
            field: [
              { name: "customer_id", number: 1, type: FieldDescriptorProto_Type.STRING },
              { name: "status", number: 2, type: FieldDescriptorProto_Type.STRING },
              { name: "price", number: 3, type: FieldDescriptorProto_Type.DOUBLE },
              { name: "updated_at", number: 4, type: FieldDescriptorProto_Type.DOUBLE },
            ],
          },
          {
            name: "OrderRoute",
            field: [
              { name: "strategy_id", number: 1, type: FieldDescriptorProto_Type.STRING },
              { name: "region", number: 2, type: FieldDescriptorProto_Type.STRING },
            ],
          },
        ],
        service: [
          {
            name: "OrdersService",
            method: [
              {
                name: "StreamOrders",
                inputType: ".viewserver.example.OrderRoute",
                outputType: ".viewserver.example.OrderValue",
                serverStreaming: true,
              },
            ],
          },
        ],
      }),
    ),
  ),
);

export const orderValueSchema = messageDesc<OrderValueMessage>(protoFile, 0);
export const orderRouteSchema = messageDesc<OrderRouteMessage>(protoFile, 1);
export const ordersService = serviceDesc<{
  readonly streamOrders: {
    readonly input: typeof orderRouteSchema;
    readonly output: typeof orderValueSchema;
    readonly methodKind: "server_streaming";
  };
}>(protoFile, 0);
