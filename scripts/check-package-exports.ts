import type { ViewServerLiveClient } from "@view-server/client";
import * as clientPackage from "@view-server/client";
import type { ColumnLiveViewEngine } from "@view-server/column-live-view-engine";
import * as enginePackage from "@view-server/column-live-view-engine";
import type { ViewServerHealth } from "@view-server/config/health";
import type { KafkaMappingInput } from "@view-server/config/kafka";
import type { LiveSubscription } from "@view-server/config/live-protocol";
import type { RawQuery } from "@view-server/config/query";
import type { RuntimeEnvironmentConfig } from "@view-server/config/runtime";
import { createInMemoryViewServer } from "@view-server/in-memory";
import * as inMemoryPackage from "@view-server/in-memory";
import { createViewServerReact } from "@view-server/react";
import { createInMemoryViewServerReact } from "@view-server/react/testing";
import * as configPackage from "@view-server/config";
import * as healthPackage from "@view-server/config/health";
import * as kafkaPackage from "@view-server/config/kafka";
import * as liveProtocolPackage from "@view-server/config/live-protocol";
import * as queryPackage from "@view-server/config/query";
import * as runtimePackage from "@view-server/config/runtime";
import * as reactPackage from "@view-server/react";
import * as reactTestingPackage from "@view-server/react/testing";

const requireExport = (moduleName: string, moduleValue: object, exportName: string) => {
  if (!(exportName in moduleValue)) {
    throw new Error(`${moduleName} is missing export ${exportName}`);
  }
};

const rejectExport = (moduleName: string, moduleValue: object, exportName: string) => {
  if (exportName in moduleValue) {
    throw new Error(`${moduleName} unexpectedly exports ${exportName}`);
  }
};

const requireModule = (moduleName: string, moduleValue: unknown) => {
  if (typeof moduleValue !== "object" || moduleValue === null) {
    throw new Error(`${moduleName} did not resolve to an ES module namespace object`);
  }
};

requireExport("@view-server/config", configPackage, "defineViewServerConfig");
requireExport("@view-server/config", configPackage, "defineProto");
requireExport("@view-server/config", configPackage, "defineKafkaTopic");
requireModule("@view-server/config/query", queryPackage);
requireModule("@view-server/config/health", healthPackage);
requireModule("@view-server/config/live-protocol", liveProtocolPackage);
requireExport("@view-server/config/kafka", kafkaPackage, "defineProto");
requireExport("@view-server/config/kafka", kafkaPackage, "defineKafkaTopic");
requireExport("@view-server/config/runtime", runtimePackage, "runtimeConfig");
requireExport("@view-server/config/runtime", runtimePackage, "runtimeEnvironmentConfig");
requireExport("@view-server/client", clientPackage, "stableQueryKey");
requireExport("@view-server/client", clientPackage, "applyEvent");
requireExport("@view-server/column-live-view-engine", enginePackage, "createColumnLiveViewEngine");
requireExport("@view-server/column-live-view-engine", enginePackage, "InvalidTopicError");
requireExport("@view-server/in-memory", inMemoryPackage, "createInMemoryViewServer");
requireExport("@view-server/in-memory", inMemoryPackage, "makeInMemoryViewServer");
rejectExport("@view-server/in-memory", inMemoryPackage, "readHealth");
rejectExport("@view-server/in-memory", inMemoryPackage, "refreshHealth");
rejectExport("@view-server/in-memory", inMemoryPackage, "makeHealthRefreshScheduler");
requireExport("@view-server/react", reactPackage, "createViewServerReact");
rejectExport("@view-server/react", reactPackage, "createInMemoryViewServerReact");
requireExport("@view-server/react/testing", reactTestingPackage, "createInMemoryViewServerReact");

const _clientType: ViewServerLiveClient<Record<string, never>> | undefined = undefined;
const _engineType: ColumnLiveViewEngine<Record<string, never>> | undefined = undefined;
const _runtimeConfigType: RuntimeEnvironmentConfig | undefined = undefined;
const _queryType: RawQuery<{ readonly id: string }> | undefined = undefined;
const _healthType: ViewServerHealth<{ readonly orders: { readonly id: string } }> | undefined =
  undefined;
const _subscriptionType: LiveSubscription<{ readonly id: string }> | undefined = undefined;
const _mappingInputType:
  | KafkaMappingInput<
      { readonly orders: { readonly schema: never } },
      "orders",
      "usa",
      never,
      undefined
    >
  | undefined = undefined;

void _engineType;
void _clientType;
void _runtimeConfigType;
void _queryType;
void _healthType;
void _subscriptionType;
void _mappingInputType;
void createInMemoryViewServer;
void createViewServerReact;
void createInMemoryViewServerReact;
