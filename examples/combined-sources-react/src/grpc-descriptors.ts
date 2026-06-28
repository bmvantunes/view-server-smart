import { create, toBinary } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import type { Message } from "@bufbuild/protobuf";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";

export type OrderValueMessage = Message<"viewserver.combined.OrderValue"> & {
  readonly customerId: string;
  readonly status: "open" | "closed" | "cancelled";
  readonly price: number;
  readonly updatedAt: number;
};

export type OrderRouteMessage = Message<"viewserver.combined.OrderRoute"> & {
  readonly strategyId: string;
  readonly region: string;
};

export type StrategyValueMessage = Message<"viewserver.combined.StrategyValue"> & {
  readonly strategyId: string;
  readonly region: string;
  readonly status: "active" | "paused";
  readonly notional: number;
  readonly updatedAt: number;
};

export type StrategyRequestMessage = Message<"viewserver.combined.StrategyRequest"> & {
  readonly universe: string;
};

const base64FromBytes = (bytes: Uint8Array) =>
  globalThis.btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(""));

const protoFile = fileDesc(
  base64FromBytes(
    toBinary(
      FileDescriptorProtoSchema,
      create(FileDescriptorProtoSchema, {
        name: "viewserver/combined.proto",
        package: "viewserver.combined",
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
          {
            name: "StrategyValue",
            field: [
              { name: "strategy_id", number: 1, type: FieldDescriptorProto_Type.STRING },
              { name: "region", number: 2, type: FieldDescriptorProto_Type.STRING },
              { name: "status", number: 3, type: FieldDescriptorProto_Type.STRING },
              { name: "notional", number: 4, type: FieldDescriptorProto_Type.DOUBLE },
              { name: "updated_at", number: 5, type: FieldDescriptorProto_Type.DOUBLE },
            ],
          },
          {
            name: "StrategyRequest",
            field: [{ name: "universe", number: 1, type: FieldDescriptorProto_Type.STRING }],
          },
        ],
        service: [
          {
            name: "CombinedService",
            method: [
              {
                name: "StreamOrders",
                inputType: ".viewserver.combined.OrderRoute",
                outputType: ".viewserver.combined.OrderValue",
                serverStreaming: true,
              },
              {
                name: "StreamStrategies",
                inputType: ".viewserver.combined.StrategyRequest",
                outputType: ".viewserver.combined.StrategyValue",
                serverStreaming: true,
              },
            ],
          },
        ],
      }),
    ),
  ),
);

const orderValueSchema = messageDesc<OrderValueMessage>(protoFile, 0);
const orderRouteSchema = messageDesc<OrderRouteMessage>(protoFile, 1);
const strategyValueSchema = messageDesc<StrategyValueMessage>(protoFile, 2);
const strategyRequestSchema = messageDesc<StrategyRequestMessage>(protoFile, 3);

export const combinedService = serviceDesc<{
  readonly streamOrders: {
    readonly input: typeof orderRouteSchema;
    readonly output: typeof orderValueSchema;
    readonly methodKind: "server_streaming";
  };
  readonly streamStrategies: {
    readonly input: typeof strategyRequestSchema;
    readonly output: typeof strategyValueSchema;
    readonly methodKind: "server_streaming";
  };
}>(protoFile, 0);
