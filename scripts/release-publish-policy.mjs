export const expectedPublishRepository = "bmvantunes/effect-view-server";
export const internalPackageScope = "@effect-view-server/";
export const publicPackageName = "effect-view-server";

const cloneJson = (value) => structuredClone(value);

const omitInternalDependencies = (dependencies) =>
  dependencies === undefined
    ? undefined
    : Object.fromEntries(
        Object.entries(dependencies).filter(([name]) => !name.startsWith(internalPackageScope)),
      );

const definedEntries = (entries) =>
  Object.fromEntries(entries.filter(([, value]) => value !== undefined));

export const packageTagName = (version) => `${publicPackageName}@${version}`;

export const stagedPackageTagName = (version) => `${packageTagName(version)}-staged`;

export const stagePublishCommandArguments = (stageDirectory) => [
  "stage",
  "publish",
  stageDirectory,
  "--provenance",
  "--access",
  "public",
];

const escapedRegExp = (value) => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

export const classifyStagePublishDuplicateOutput = ({ stderr, stdout, version }) => {
  const output = `${stdout}\n${stderr}`;
  const versionPattern = escapedRegExp(version);

  const hasVersion = new RegExp(versionPattern, "i").test(output);
  if (!hasVersion) {
    return {
      _tag: "Unknown",
    };
  }

  if (/previously published|cannot publish over|cannot modify pre-existing version/i.test(output)) {
    return {
      _tag: "AlreadyPublished",
    };
  }

  if (/already exists|already staged|already pending|staged version/i.test(output)) {
    if (/already exists/i.test(output) && !/already staged|already pending|staged version/i.test(output)) {
      return {
        _tag: "DuplicateVersion",
      };
    }

    return {
      _tag: "AlreadyStaged",
    };
  }

  return {
    _tag: "Unknown",
  };
};

export const stripSourceMapReference = (contents) =>
  contents.replace(/(?:\n)?\/\/# sourceMappingURL=.*(?:\n|$)/g, "\n");

const hasInternalWorkspaceReference = (file) =>
  file.relativePath === "package.json"
    ? file.contents.includes(internalPackageScope)
    : /(?:from\s+["']|import\s*(?:\(\s*)?["']|require\s*\(\s*["'])@effect-view-server\//.test(
        file.contents,
      );

export const sanitizePublicPackageJson = (packageJson) =>
  definedEntries([
    ["name", publicPackageName],
    ["version", packageJson.version],
    ["description", packageJson.description],
    ["keywords", cloneJson(packageJson.keywords)],
    ["homepage", packageJson.homepage],
    ["bugs", cloneJson(packageJson.bugs)],
    ["license", packageJson.license],
    ["repository", cloneJson(packageJson.repository)],
    ["type", packageJson.type],
    ["sideEffects", packageJson.sideEffects],
    ["exports", cloneJson(packageJson.exports)],
    ["engines", cloneJson(packageJson.engines)],
    ["files", ["dist", "README.md"]],
    [
      "publishConfig",
      {
        ...cloneJson(packageJson.publishConfig ?? {}),
        access: "public",
        provenance: true,
      },
    ],
    ["dependencies", omitInternalDependencies(packageJson.dependencies)],
    ["peerDependencies", omitInternalDependencies(packageJson.peerDependencies)],
    ["peerDependenciesMeta", cloneJson(packageJson.peerDependenciesMeta)],
  ]);

export const publishedFileViolations = (files) =>
  files.flatMap((file) => [
    ...(file.relativePath.endsWith(".map") ? [`${file.relativePath} is a source map`] : []),
    ...(file.contents.includes("sourceMappingURL")
      ? [`${file.relativePath} references a source map`]
      : []),
    ...(hasInternalWorkspaceReference(file)
      ? [`${file.relativePath} references ${internalPackageScope}`]
      : []),
  ]);

export const internalPublishViolations = (workspacePackages) =>
  workspacePackages
    .filter((workspacePackage) => workspacePackage.name !== publicPackageName)
    .filter((workspacePackage) => workspacePackage.private !== true)
    .map((workspacePackage) => workspacePackage.name);

export const isTrustedPublishEnvironment = (env) =>
  env.GITHUB_ACTIONS === "true" &&
  (env.GITHUB_EVENT_NAME === "push" || env.GITHUB_EVENT_NAME === "workflow_dispatch") &&
  env.GITHUB_REF === "refs/heads/main" &&
  env.GITHUB_REPOSITORY === expectedPublishRepository;

export const oidcPublishEnvironmentViolations = (env) =>
  ["ACTIONS_ID_TOKEN_REQUEST_URL", "ACTIONS_ID_TOKEN_REQUEST_TOKEN"]
    .filter((name) => env[name] === undefined || env[name] === "")
    .map((name) => `${name} is required for npm trusted publishing.`);

export const publishDecision = ({ env, version, workspacePackages }) => {
  if (version === "0.0.0") {
    return {
      _tag: "Skip",
      message: `Skipping npm publish for ${publicPackageName}@0.0.0.`,
    };
  }

  const violations = internalPublishViolations(workspacePackages);
  if (violations.length > 0) {
    return {
      _tag: "Refuse",
      message: `Refusing to publish because ${violations.join(", ")} ${
        violations.length === 1 ? "is" : "are"
      } not private.`,
    };
  }

  if (!isTrustedPublishEnvironment(env)) {
    return {
      _tag: "Refuse",
      message: "Refusing npm publish outside the trusted main-branch GitHub Actions context.",
    };
  }

  return {
    _tag: "Publish",
  };
};
