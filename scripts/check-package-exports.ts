import type { ViewServerLiveClient } from "@view-server/client";
import * as clientPackage from "@view-server/client";
import * as clientRemotePackage from "@view-server/client/remote";
import type { ColumnLiveViewEngine } from "@view-server/column-live-view-engine";
import * as enginePackage from "@view-server/column-live-view-engine";
import * as effectUtilsPackage from "@view-server/effect-utils";
import type { ViewServerHealth } from "@view-server/config/health";
import type { KafkaMappingInput } from "@view-server/config/kafka";
import type { LiveSubscription } from "@view-server/config/live-protocol";
import type { RawQuery } from "@view-server/config/query";
import type { RuntimeEnvironmentConfig } from "@view-server/config/runtime";
import { createInMemoryViewServer } from "@view-server/in-memory";
import * as inMemoryPackage from "@view-server/in-memory";
import * as protocolPackage from "@view-server/protocol";
import type { ViewServerWireEvent } from "@view-server/protocol";
import { createViewServerReact } from "@view-server/react";
import { createInMemoryViewServerReact } from "@view-server/react/testing";
import { createViewServerRuntime } from "@view-server/runtime";
import type { ViewServerRuntime } from "@view-server/runtime";
import { createViewServerRuntimeCore } from "@view-server/runtime-core";
import * as runtimeCorePackage from "@view-server/runtime-core";
import { createViewServerWebSocketServer } from "@view-server/server";
import type { ViewServerHealthHttpJson, ViewServerWebSocketServer } from "@view-server/server";
import * as configPackage from "@view-server/config";
import * as healthPackage from "@view-server/config/health";
import * as kafkaPackage from "@view-server/config/kafka";
import * as liveProtocolPackage from "@view-server/config/live-protocol";
import * as queryPackage from "@view-server/config/query";
import * as runtimePackage from "@view-server/config/runtime";
import * as reactPackage from "@view-server/react";
import * as reactTestingPackage from "@view-server/react/testing";
import * as runtimeRootPackage from "@view-server/runtime";
import * as serverPackage from "@view-server/server";

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

const requireResolvablePackageExport = (specifier: string) => {
  try {
    return import.meta.resolve(specifier);
  } catch (error) {
    throw new Error(`${specifier} should resolve as a public package export`, { cause: error });
  }
};

const rejectResolvablePackageExport = (specifier: string) => {
  const unexpectedMessage = `${specifier} unexpectedly resolves as a public package export`;
  try {
    const resolved = import.meta.resolve(specifier);
    throw new Error(`${unexpectedMessage}: ${resolved}`);
  } catch (error) {
    if (error instanceof Error && error.message.startsWith(unexpectedMessage)) {
      throw error;
    }
  }
};

const publicPackageExports = [
  "@view-server/client",
  "@view-server/client/remote",
  "@view-server/column-live-view-engine",
  "@view-server/config",
  "@view-server/config/health",
  "@view-server/config/kafka",
  "@view-server/config/live-protocol",
  "@view-server/config/query",
  "@view-server/config/runtime",
  "@view-server/effect-utils",
  "@view-server/in-memory",
  "@view-server/protocol",
  "@view-server/react",
  "@view-server/react/testing",
  "@view-server/runtime",
  "@view-server/runtime-core",
  "@view-server/server",
];

const forbiddenDeepImportSuffixes = ["dist/index.js", "internal", "src/index"];

const forbiddenPackageSubpathDeepImports = publicPackageExports.flatMap((specifier) =>
  forbiddenDeepImportSuffixes.map((suffix) => `${specifier}/${suffix}`),
);

const forbiddenPackageDeepImports = [
  ...forbiddenPackageSubpathDeepImports,
  "@view-server/client/src/index",
  "@view-server/client/dist/index.js",
  "@view-server/client/src/live-client",
  "@view-server/client/src/remote-client",
  "@view-server/client/remote/client",
  "@view-server/client/remote/internal",
  "@view-server/column-live-view-engine/src/index",
  "@view-server/column-live-view-engine/dist/index.js",
  "@view-server/column-live-view-engine/src/topic-store-state",
  "@view-server/column-live-view-engine/topic-store-state",
  "@view-server/config/src/index",
  "@view-server/config/dist/index.js",
  "@view-server/config/dist/topic-contract.js",
  "@view-server/config/src/topic-contract",
  "@view-server/config/query/raw-query-contract",
  "@view-server/config/query/src/topic-contract",
  "@view-server/effect-utils/src/index",
  "@view-server/effect-utils/dist/index.js",
  "@view-server/in-memory/src/index",
  "@view-server/in-memory/dist/index.js",
  "@view-server/protocol/src/index",
  "@view-server/protocol/dist/index.js",
  "@view-server/protocol/src/protocol-json-field-codec",
  "@view-server/protocol/protocol-json-field-codec",
  "@view-server/protocol/src/protocol-row-codec",
  "@view-server/protocol/protocol-row-codec",
  "@view-server/react/src/index",
  "@view-server/react/dist/index.js",
  "@view-server/react/dist/testing.js",
  "@view-server/react/src/testing",
  "@view-server/react/testing/internal",
  "@view-server/runtime/src/internal",
  "@view-server/runtime/dist/index.js",
  "@view-server/runtime/internal",
  "@view-server/runtime-core/src/health",
  "@view-server/runtime-core/dist/index.js",
  "@view-server/runtime-core/health",
  "@view-server/server/src/rpc-handlers",
  "@view-server/server/dist/index.js",
  "@view-server/server/rpc-handlers",
];

for (const specifier of publicPackageExports) {
  requireResolvablePackageExport(specifier);
}

for (const specifier of forbiddenPackageDeepImports) {
  rejectResolvablePackageExport(specifier);
}

requireExport("@view-server/config", configPackage, "defineViewServerConfig");
requireExport("@view-server/config", configPackage, "defineKafkaTopic");
requireExport("@view-server/config", configPackage, "kafka");
requireModule("@view-server/config/query", queryPackage);
requireModule("@view-server/config/health", healthPackage);
requireModule("@view-server/config/live-protocol", liveProtocolPackage);
requireExport("@view-server/config/kafka", kafkaPackage, "defineKafkaTopic");
requireExport("@view-server/config/kafka", kafkaPackage, "kafka");
requireExport("@view-server/config/runtime", runtimePackage, "runtimeConfig");
requireExport("@view-server/config/runtime", runtimePackage, "runtimeEnvironmentConfig");
requireExport("@view-server/client", clientPackage, "stableQueryKey");
requireExport("@view-server/client", clientPackage, "applyEvent");
rejectExport("@view-server/client", clientPackage, "makeViewServerClient");
rejectExport("@view-server/client", clientPackage, "createViewServerClient");
rejectExport("@view-server/client", clientPackage, "ViewServerRpcs");
rejectExport(
  "@view-server/client",
  clientPackage,
  "ignoreLoggedTypedFailuresPreserveNonTypedFailures",
);
requireExport("@view-server/client/remote", clientRemotePackage, "makeViewServerClient");
requireExport("@view-server/client/remote", clientRemotePackage, "createViewServerClient");
requireExport(
  "@view-server/effect-utils",
  effectUtilsPackage,
  "ignoreLoggedTypedFailuresPreserveNonTypedFailures",
);
requireExport("@view-server/protocol", protocolPackage, "ViewServerRpcs");
requireExport("@view-server/protocol", protocolPackage, "ViewServerWireRowSchema");
requireExport("@view-server/column-live-view-engine", enginePackage, "createColumnLiveViewEngine");
requireExport("@view-server/column-live-view-engine", enginePackage, "InvalidTopicError");
requireExport("@view-server/in-memory", inMemoryPackage, "createInMemoryViewServer");
requireExport("@view-server/in-memory", inMemoryPackage, "makeInMemoryViewServer");
rejectExport("@view-server/in-memory", inMemoryPackage, "readHealth");
rejectExport("@view-server/in-memory", inMemoryPackage, "refreshHealth");
rejectExport("@view-server/in-memory", inMemoryPackage, "makeHealthRefreshScheduler");
requireExport("@view-server/runtime-core", runtimeCorePackage, "createViewServerRuntimeCore");
requireExport("@view-server/runtime-core", runtimeCorePackage, "makeViewServerRuntimeCore");
rejectExport("@view-server/runtime-core", runtimeCorePackage, "readHealth");
rejectExport("@view-server/runtime-core", runtimeCorePackage, "refreshHealth");
rejectExport("@view-server/runtime-core", runtimeCorePackage, "makeHealthRefreshScheduler");
requireExport("@view-server/react", reactPackage, "createViewServerReact");
rejectExport("@view-server/react", reactPackage, "createInMemoryViewServerReact");
requireExport("@view-server/react/testing", reactTestingPackage, "createInMemoryViewServerReact");
requireExport("@view-server/runtime", runtimeRootPackage, "makeViewServerRuntime");
requireExport("@view-server/runtime", runtimeRootPackage, "createViewServerRuntime");
requireExport("@view-server/runtime", runtimeRootPackage, "runViewServerRuntime");
requireExport("@view-server/server", serverPackage, "makeViewServerWebSocketServer");
requireExport("@view-server/server", serverPackage, "createViewServerWebSocketServer");

const _clientType: ViewServerLiveClient<Record<string, never>> | undefined = undefined;
const _engineType: ColumnLiveViewEngine<Record<string, never>> | undefined = undefined;
const _runtimeConfigType: RuntimeEnvironmentConfig | undefined = undefined;
const _queryType: RawQuery<{ readonly id: string }> | undefined = undefined;
const _healthType: ViewServerHealth<{ readonly orders: { readonly id: string } }> | undefined =
  undefined;
const _subscriptionType: LiveSubscription<{ readonly id: string }> | undefined = undefined;
const _serverType: ViewServerWebSocketServer | undefined = undefined;
const _runtimeType: ViewServerRuntime<never> | undefined = undefined;
const _healthHttpJsonType: ViewServerHealthHttpJson | undefined = undefined;
const _wireEventType: ViewServerWireEvent | undefined = undefined;
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
void _serverType;
void _runtimeType;
void _healthHttpJsonType;
void _wireEventType;
void _mappingInputType;
void createInMemoryViewServer;
void createViewServerRuntimeCore;
void createViewServerReact;
void createInMemoryViewServerReact;
void createViewServerRuntime;
void createViewServerWebSocketServer;
