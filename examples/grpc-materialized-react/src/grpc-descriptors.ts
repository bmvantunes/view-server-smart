import { create, toBinary } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import type { Message } from "@bufbuild/protobuf";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";

export type StrategyValueMessage = Message<"viewserver.example.StrategyValue"> & {
  readonly strategyId: string;
  readonly region: string;
  readonly status: "active" | "paused";
  readonly notional: number;
  readonly updatedAt: number;
};

export type StrategyRequestMessage = Message<"viewserver.example.StrategyRequest"> & {
  readonly universe: string;
};

const base64FromBytes = (bytes: Uint8Array) =>
  globalThis.btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(""));

const protoFile = fileDesc(
  base64FromBytes(
    toBinary(
      FileDescriptorProtoSchema,
      create(FileDescriptorProtoSchema, {
        name: "viewserver/materialized-example.proto",
        package: "viewserver.example",
        syntax: "proto3",
        messageType: [
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
            name: "StrategiesService",
            method: [
              {
                name: "StreamStrategies",
                inputType: ".viewserver.example.StrategyRequest",
                outputType: ".viewserver.example.StrategyValue",
                serverStreaming: true,
              },
            ],
          },
        ],
      }),
    ),
  ),
);

export const strategyValueSchema = messageDesc<StrategyValueMessage>(protoFile, 0);
export const strategyRequestSchema = messageDesc<StrategyRequestMessage>(protoFile, 1);
export const strategiesService = serviceDesc<{
  readonly streamStrategies: {
    readonly input: typeof strategyRequestSchema;
    readonly output: typeof strategyValueSchema;
    readonly methodKind: "server_streaming";
  };
}>(protoFile, 0);
