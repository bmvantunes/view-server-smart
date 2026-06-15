import { readdirSync, readFileSync } from "node:fs";
import { dirname, isAbsolute, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const packageSourceRoot = (name: string): string => join(repoRoot, "packages", name, "src");
const engineSourceRoot = join(repoRoot, "packages", "column-live-view-engine", "src");
const topicStoreFile = join(engineSourceRoot, "topic-store.ts");
const topicStoreHealthFile = join(engineSourceRoot, "topic-store-health.ts");
const topicStoreLifecycleFile = join(engineSourceRoot, "topic-store-lifecycle.ts");
const topicStoreMutationFile = join(engineSourceRoot, "topic-store-mutation.ts");
const topicStoreQueryFile = join(engineSourceRoot, "topic-store-query.ts");
const topicStoreStateFile = join(engineSourceRoot, "topic-store-state.ts");
const topicStoreSubscriptionFile = join(engineSourceRoot, "topic-store-subscription.ts");

const restrictedTopicStoreHelpers = [
  {
    name: "makeTopicStoreSubscriptionPermit",
    pattern: /\bmakeTopicStoreSubscriptionPermit\b/,
    allowedPaths: new Set([topicStoreStateFile, topicStoreSubscriptionFile]),
  },
  {
    name: "topicStoreRawQueryMetadata",
    pattern: /\btopicStoreRawQueryMetadata\b/,
    allowedPaths: new Set([topicStoreQueryFile, topicStoreStateFile]),
  },
  {
    name: "topicStoreReadModel",
    pattern: /\btopicStoreReadModel\b/,
    allowedPaths: new Set([topicStoreQueryFile, topicStoreStateFile]),
  },
  {
    name: "topicStoreState",
    pattern: /\btopicStoreState\b/,
    allowedPaths: new Set([topicStoreMutationFile, topicStoreStateFile]),
  },
] as const;

export const sourceFiles = (directory: string): ReadonlyArray<string> => {
  const entries = readdirSync(directory, { withFileTypes: true });
  const files: Array<string> = [];

  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      files.push(...sourceFiles(path));
      continue;
    }
    if (entry.isFile() && (entry.name.endsWith(".ts") || entry.name.endsWith(".tsx"))) {
      files.push(path);
    }
  }

  return files;
};

const isTestFile = (path: string): boolean =>
  path.endsWith(".test.ts") ||
  path.endsWith(".test.tsx") ||
  path.endsWith(".test-d.ts") ||
  path.endsWith(".bench.ts") ||
  path.endsWith(".bench.tsx");

type RestrictedPackageImport = {
  readonly allowedRelativePathSpecifiers?: ReadonlyMap<string, ReadonlySet<string>>;
  readonly allowedSpecifiers?: ReadonlySet<string>;
  readonly forbiddenSpecifiers: ReadonlySet<string>;
  readonly message: string;
  readonly packageName: string;
};

const isViewServerSpecifier = (specifier: string): boolean =>
  specifier === "@view-server" || specifier.startsWith("@view-server/");

export const sourceWithoutComments = (contents: string): string => {
  let output = "";
  let index = 0;
  let quote: '"' | "'" | "`" | undefined;
  let lineComment = false;
  let blockComment = false;

  while (index < contents.length) {
    const character = contents.charAt(index);
    const nextCharacter = contents.charAt(index + 1);

    if (lineComment) {
      if (character === "\n") {
        lineComment = false;
        output += character;
      }
      index += 1;
      continue;
    }

    if (blockComment) {
      if (character === "\n") {
        output += character;
      }
      if (character === "*" && nextCharacter === "/") {
        blockComment = false;
        index += 2;
        continue;
      }
      index += 1;
      continue;
    }

    if (quote !== undefined) {
      output += character;
      if (character === "\\") {
        output += nextCharacter;
        index += 2;
        continue;
      }
      if (character === quote) {
        quote = undefined;
      }
      index += 1;
      continue;
    }

    if (character === "/" && nextCharacter === "/") {
      lineComment = true;
      index += 2;
      continue;
    }

    if (character === "/" && nextCharacter === "*") {
      blockComment = true;
      index += 2;
      continue;
    }

    if (character === '"' || character === "'" || character === "`") {
      quote = character;
    }
    output += character;
    index += 1;
  }

  return output;
};

const importedViewServerSpecifiers = (contents: string): ReadonlyArray<string> =>
  importSpecifiersFromSource(contents).filter(isViewServerSpecifier);

const specifierMatches = (specifier: string, packageSpecifier: string): boolean =>
  specifier === packageSpecifier || specifier.startsWith(`${packageSpecifier}/`);

const isImportQuote = (character: string): character is '"' | "'" | "`" =>
  character === '"' || character === "'" || character === "`";

const identifierCharacterPattern = /[A-Za-z0-9_$]/;

const isIdentifierCharacter = (character: string | undefined): boolean =>
  character !== undefined && identifierCharacterPattern.test(character);

const sourceHasKeywordAt = (contents: string, index: number, keyword: string): boolean =>
  contents.startsWith(keyword, index) &&
  !isIdentifierCharacter(contents[index - 1]) &&
  !isIdentifierCharacter(contents[index + keyword.length]);

const skipWhitespace = (contents: string, index: number): number => {
  let nextIndex = index;
  while (nextIndex < contents.length && /\s/.test(contents.charAt(nextIndex))) {
    nextIndex += 1;
  }
  return nextIndex;
};

const readQuotedSpecifier = (
  contents: string,
  index: number,
): { readonly nextIndex: number; readonly specifier: string } | undefined => {
  const quote = contents.charAt(index);
  if (!isImportQuote(quote)) {
    return undefined;
  }

  let specifier = "";
  let nextIndex = index + 1;
  while (nextIndex < contents.length) {
    const character = contents.charAt(nextIndex);
    const nextCharacter = contents.charAt(nextIndex + 1);
    if (character === "\\") {
      specifier += character;
      specifier += nextCharacter;
      nextIndex += 2;
      continue;
    }
    if (character === quote) {
      return {
        nextIndex: nextIndex + 1,
        specifier,
      };
    }
    specifier += character;
    nextIndex += 1;
  }

  return undefined;
};

const skipQuotedLiteral = (contents: string, index: number): number =>
  readQuotedSpecifier(contents, index)?.nextIndex ?? index + 1;

const readTemplateExpression = (
  contents: string,
  index: number,
): { readonly expression: string; readonly nextIndex: number } | undefined => {
  let depth = 1;
  let nextIndex = index;

  while (nextIndex < contents.length) {
    const character = contents.charAt(nextIndex);
    if (isImportQuote(character)) {
      nextIndex = skipQuotedLiteral(contents, nextIndex);
      continue;
    }
    if (character === "{") {
      depth += 1;
    }
    if (character === "}") {
      depth -= 1;
      if (depth === 0) {
        return {
          expression: contents.slice(index, nextIndex),
          nextIndex: nextIndex + 1,
        };
      }
    }
    nextIndex += 1;
  }

  return undefined;
};

const templateExpressionImportSpecifiers = (
  contents: string,
  index: number,
): { readonly nextIndex: number; readonly specifiers: ReadonlyArray<string> } => {
  const specifiers: Array<string> = [];
  let nextIndex = index + 1;

  while (nextIndex < contents.length) {
    const character = contents.charAt(nextIndex);
    const nextCharacter = contents.charAt(nextIndex + 1);
    if (character === "\\") {
      nextIndex += 2;
      continue;
    }
    if (character === "`") {
      return {
        nextIndex: nextIndex + 1,
        specifiers,
      };
    }
    if (character === "$" && nextCharacter === "{") {
      const expression = readTemplateExpression(contents, nextIndex + 2);
      if (expression === undefined) {
        return {
          nextIndex: contents.length,
          specifiers,
        };
      }
      specifiers.push(...importSpecifiersFromSource(expression.expression));
      nextIndex = expression.nextIndex;
      continue;
    }
    nextIndex += 1;
  }

  return {
    nextIndex,
    specifiers,
  };
};

export const importSpecifiersFromSource = (contents: string): ReadonlyArray<string> => {
  const source = sourceWithoutComments(contents);
  const specifiers: Array<string> = [];
  let index = 0;

  while (index < source.length) {
    const character = source.charAt(index);
    if (character === "`") {
      const template = templateExpressionImportSpecifiers(source, index);
      specifiers.push(...template.specifiers);
      index = template.nextIndex;
      continue;
    }
    if (character === '"' || character === "'") {
      index = skipQuotedLiteral(source, index);
      continue;
    }

    if (sourceHasKeywordAt(source, index, "from")) {
      const specifier = readQuotedSpecifier(source, skipWhitespace(source, index + "from".length));
      if (specifier !== undefined) {
        specifiers.push(specifier.specifier);
        index = specifier.nextIndex;
        continue;
      }
    }

    if (sourceHasKeywordAt(source, index, "import")) {
      const afterImport = skipWhitespace(source, index + "import".length);
      const sideEffectSpecifier = readQuotedSpecifier(source, afterImport);
      if (sideEffectSpecifier !== undefined) {
        specifiers.push(sideEffectSpecifier.specifier);
        index = sideEffectSpecifier.nextIndex;
        continue;
      }
      if (source[afterImport] === "(") {
        const dynamicSpecifier = readQuotedSpecifier(
          source,
          skipWhitespace(source, afterImport + 1),
        );
        if (dynamicSpecifier !== undefined) {
          specifiers.push(dynamicSpecifier.specifier);
          index = dynamicSpecifier.nextIndex;
          continue;
        }
      }
    }

    index += 1;
  }

  return specifiers;
};

export const packageImportViolationsFor = ({
  contents,
  relativePath,
  restriction,
}: {
  readonly contents: string;
  readonly relativePath: string;
  readonly restriction: RestrictedPackageImport;
}): ReadonlyArray<string> =>
  importedViewServerSpecifiers(contents).flatMap((specifier) => {
    if (!approvedPublicViewServerSpecifiers.has(specifier)) {
      return [
        `${relativePath} imports ${specifier}: View Server imports must use approved package exports.`,
      ];
    }

    const isAllowed = (() => {
      const relativePathAllowedSpecifiers =
        restriction.allowedRelativePathSpecifiers?.get(relativePath);
      return (
        relativePathAllowedSpecifiers?.has(specifier) === true ||
        restriction.allowedSpecifiers?.has(specifier) === true
      );
    })();

    if (isAllowed) {
      return [];
    }

    const isForbidden = Array.from(restriction.forbiddenSpecifiers).some((forbiddenSpecifier) =>
      specifierMatches(specifier, forbiddenSpecifier),
    );

    return isForbidden
      ? [`${relativePath} imports ${specifier}: ${restriction.message}`]
      : [];
  });

const relativeImportSpecifiers = (contents: string): ReadonlyArray<string> =>
  importSpecifiersFromSource(contents).filter((specifier) => specifier.startsWith("."));

const approvedPublicViewServerSpecifiers = new Set([
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
]);

const isInsideDirectory = (parentDirectory: string, childPath: string): boolean => {
  const relativeChildPath = relative(parentDirectory, childPath);
  return (
    relativeChildPath === "" ||
    (!relativeChildPath.startsWith("..") && !isAbsolute(relativeChildPath))
  );
};

export const packageRelativeImportViolationsFor = ({
  contents,
  packageRoot,
  path,
}: {
  readonly contents: string;
  readonly packageRoot: string;
  readonly path: string;
}): ReadonlyArray<string> =>
  relativeImportSpecifiers(contents)
    .map((specifier) => ({
      resolvedPath: resolve(dirname(path), specifier),
      specifier,
    }))
    .filter(({ resolvedPath }) => !isInsideDirectory(packageRoot, resolvedPath))
    .map(
      ({ specifier }) =>
        `${relative(packageRoot, path)} imports ${specifier}: relative imports must not cross package seams.`,
    );

export const toPosixRelativePath = (path: string): string => path.replaceAll("\\", "/");

const restrictedTopicStoreStateExports = [
  {
    label: "namespace import",
    pattern: /import\s+\*\s+as\s+\w+\s+from\s+["']\.\/topic-store-state["']/,
  },
  {
    label: "wildcard re-export",
    pattern: /export\s+\*\s+from\s+["']\.\/topic-store-state["']/,
  },
  {
    label: "namespace re-export",
    pattern: /export\s+\*\s+as\s+\w+\s+from\s+["']\.\/topic-store-state["']/,
  },
  {
    label: "subscription permit factory re-export",
    pattern:
      /export\s+\{[^}]*\bmakeTopicStoreSubscriptionPermit\b[^}]*\}\s+from\s+["']\.\/topic-store-state["']/s,
  },
  {
    label: "local subscription permit factory re-export",
    pattern: /export\s+\{[^}]*\bmakeTopicStoreSubscriptionPermit\b[^}]*\}/s,
  },
  {
    label: "raw query metadata helper re-export",
    pattern:
      /export\s+\{[^}]*\btopicStoreRawQueryMetadata\b[^}]*\}\s+from\s+["']\.\/topic-store-state["']/s,
  },
  {
    label: "local raw query metadata helper re-export",
    pattern: /export\s+\{[^}]*\btopicStoreRawQueryMetadata\b[^}]*\}/s,
  },
  {
    label: "read model helper re-export",
    pattern:
      /export\s+\{[^}]*\btopicStoreReadModel\b[^}]*\}\s+from\s+["']\.\/topic-store-state["']/s,
  },
  {
    label: "local read model helper re-export",
    pattern: /export\s+\{[^}]*\btopicStoreReadModel\b[^}]*\}/s,
  },
  {
    label: "state helper re-export",
    pattern:
      /export\s+\{[^}]*\btopicStoreState\b[^}]*\}\s+from\s+["']\.\/topic-store-state["']/s,
  },
  {
    label: "local state helper re-export",
    pattern: /export\s+\{[^}]*\btopicStoreState\b[^}]*\}/s,
  },
] as const;

export const topicStoreStateExportViolationsForFile = ({
  contents,
  path,
}: {
  readonly contents: string;
  readonly path: string;
}): ReadonlyArray<string> => {
  if (path === topicStoreStateFile) {
    return [];
  }

  const violations: Array<string> = [];
  for (const restriction of restrictedTopicStoreStateExports) {
    if (restriction.pattern.test(contents)) {
      violations.push(`${relative(repoRoot, path)} has a restricted ${restriction.label}`);
    }
  }
  return violations;
};

export const topicStoreHelperViolationsForFile = ({
  contents,
  path,
}: {
  readonly contents: string;
  readonly path: string;
}): ReadonlyArray<string> => {
  const violations: Array<string> = [];

  for (const helper of restrictedTopicStoreHelpers) {
    if (!helper.allowedPaths.has(path) && helper.pattern.test(contents)) {
      violations.push(`${relative(repoRoot, path)} uses ${helper.name}`);
    }
  }

  return violations;
};

export const collectEngineSeamViolations = () => {
  const helperViolations: Array<string> = [];
  const stateExportViolations: Array<string> = [];

  for (const path of sourceFiles(engineSourceRoot)) {
    if (isTestFile(path)) {
      continue;
    }

    const contents = readFileSync(path, "utf8");
    helperViolations.push(...topicStoreHelperViolationsForFile({ contents, path }));
    stateExportViolations.push(...topicStoreStateExportViolationsForFile({ contents, path }));
  }

  return {
    helperViolations,
    stateExportViolations,
  };
};

export const topicStoreHelperViolationMessage = (violations: ReadonlyArray<string>): string =>
  [
    "Production engine modules must not use restricted TopicStore state helpers.",
    "Route query/read-model behavior through TopicStore helper operations instead.",
    ...violations.map((path) => `- ${path}`),
  ].join("\n");

export const topicStoreStateExportViolationMessage = (
  violations: ReadonlyArray<string>,
): string =>
  [
    "Production engine modules must not re-export restricted TopicStore state internals.",
    ...violations.map((path) => `- ${path}`),
  ].join("\n");

export const assertNoEngineSeamViolations = ({
  helperViolations,
  stateExportViolations,
}: {
  readonly helperViolations: ReadonlyArray<string>;
  readonly stateExportViolations: ReadonlyArray<string>;
}) => {
  if (helperViolations.length > 0) {
    throw new Error(topicStoreHelperViolationMessage(helperViolations));
  }
  if (stateExportViolations.length > 0) {
    throw new Error(topicStoreStateExportViolationMessage(stateExportViolations));
  }
};

assertNoEngineSeamViolations(collectEngineSeamViolations());

const viewServerPackages = {
  client: "@view-server/client",
  config: "@view-server/config",
  effectUtils: "@view-server/effect-utils",
  engine: "@view-server/column-live-view-engine",
  inMemory: "@view-server/in-memory",
  protocol: "@view-server/protocol",
  react: "@view-server/react",
  runtime: "@view-server/runtime",
  runtimeCore: "@view-server/runtime-core",
  server: "@view-server/server",
} as const;

const allViewServerPackages = new Set(Object.values(viewServerPackages));

const restrictedPackageImports: ReadonlyArray<RestrictedPackageImport> = [
  {
    packageName: "config",
    forbiddenSpecifiers: allViewServerPackages,
    message: "Config contracts must stay at the bottom of the dependency graph.",
  },
  {
    packageName: "effect-utils",
    forbiddenSpecifiers: allViewServerPackages,
    message: "Effect utility helpers must stay independent of View Server packages.",
  },
  {
    packageName: "protocol",
    allowedSpecifiers: new Set([viewServerPackages.config]),
    forbiddenSpecifiers: allViewServerPackages,
    message: "Protocol may depend on config contracts only.",
  },
  {
    packageName: "client",
    allowedSpecifiers: new Set([
      viewServerPackages.config,
      viewServerPackages.effectUtils,
      viewServerPackages.protocol,
    ]),
    forbiddenSpecifiers: allViewServerPackages,
    message: "Client code must not depend on runtime, server, React, in-memory, or engine code.",
  },
  {
    packageName: "column-live-view-engine",
    allowedSpecifiers: new Set([viewServerPackages.config]),
    forbiddenSpecifiers: allViewServerPackages,
    message: "The engine must stay transport/runtime independent.",
  },
  {
    packageName: "runtime-core",
    allowedSpecifiers: new Set([
      viewServerPackages.client,
      viewServerPackages.config,
      viewServerPackages.effectUtils,
      viewServerPackages.engine,
    ]),
    forbiddenSpecifiers: allViewServerPackages,
    message: "Runtime core may compose client contracts, config, effect utils, and engine only.",
  },
  {
    packageName: "runtime",
    allowedSpecifiers: new Set([
      viewServerPackages.client,
      viewServerPackages.config,
      viewServerPackages.effectUtils,
      viewServerPackages.runtimeCore,
      viewServerPackages.server,
    ]),
    forbiddenSpecifiers: allViewServerPackages,
    message: "Production runtime must compose runtime-core/server directly.",
  },
  {
    packageName: "in-memory",
    allowedSpecifiers: new Set([
      viewServerPackages.client,
      viewServerPackages.config,
      viewServerPackages.runtimeCore,
    ]),
    forbiddenSpecifiers: allViewServerPackages,
    message: "The in-memory Adapter must use runtime-core instead of reaching into lower layers.",
  },
  {
    packageName: "server",
    allowedSpecifiers: new Set([
      viewServerPackages.client,
      viewServerPackages.config,
      viewServerPackages.effectUtils,
      viewServerPackages.protocol,
    ]),
    forbiddenSpecifiers: allViewServerPackages,
    message: "Server code may depend on protocol/client contracts, not runtime or React adapters.",
  },
  {
    allowedRelativePathSpecifiers: new Map([
      ["src/testing.tsx", new Set([viewServerPackages.inMemory])],
    ]),
    packageName: "react",
    allowedSpecifiers: new Set([
      viewServerPackages.client,
      `${viewServerPackages.client}/remote`,
      viewServerPackages.config,
      viewServerPackages.effectUtils,
    ]),
    forbiddenSpecifiers: allViewServerPackages,
    message:
      "React bindings may use client transports but must not import runtime, server, engine, or in-memory outside the testing entrypoint.",
  },
] as const;

export const packageImportViolationsForFile = ({
  contents,
  packageRoot,
  path,
  restriction,
}: {
  readonly contents: string;
  readonly packageRoot: string;
  readonly path: string;
  readonly restriction: RestrictedPackageImport;
}): ReadonlyArray<string> => {
  const violations: Array<string> = [];

  for (const violation of packageRelativeImportViolationsFor({
    contents,
    packageRoot,
    path,
  })) {
    violations.push(`packages/${restriction.packageName}/${violation}`);
  }

  for (const violation of packageImportViolationsFor({
    contents,
    relativePath: toPosixRelativePath(relative(packageRoot, path)),
    restriction,
  })) {
    violations.push(`packages/${restriction.packageName}/${violation}`);
  }

  return violations;
};

export const collectPackageImportViolations = (): ReadonlyArray<string> => {
  const violations: Array<string> = [];

  for (const restriction of restrictedPackageImports) {
    for (const path of sourceFiles(packageSourceRoot(restriction.packageName))) {
      if (isTestFile(path)) {
        continue;
      }
      violations.push(
        ...packageImportViolationsForFile({
          contents: readFileSync(path, "utf8"),
          packageRoot: join(repoRoot, "packages", restriction.packageName),
          path,
          restriction,
        }),
      );
    }
  }

  return violations;
};

export const packageImportViolationMessage = (violations: ReadonlyArray<string>): string =>
  [
    "Package architecture seam violations found.",
    ...violations.map((path) => `- ${path}`),
  ].join("\n");

export const assertNoPackageImportViolations = (violations: ReadonlyArray<string>) => {
  if (violations.length === 0) {
    return;
  }
  throw new Error(packageImportViolationMessage(violations));
};

assertNoPackageImportViolations(collectPackageImportViolations());
