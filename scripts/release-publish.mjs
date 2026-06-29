import { spawnSync } from "node:child_process";
import { cpSync, existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import {
  canRecoverMissingStagedMarker,
  classifyStagePublishDuplicateOutput,
  packageTagName,
  oidcPublishEnvironmentViolations,
  publishedFileViolations,
  publicPackageName,
  publishDecision,
  sanitizePublicPackageJson,
  stagedPackageTagName,
  stagePublishCommandArguments,
  stripSourceMapReference,
} from "./release-publish-policy.mjs";

const packageUrl = new URL("../packages/effect-view-server/package.json", import.meta.url);
const packageJson = JSON.parse(readFileSync(packageUrl, "utf8"));
const finalizeVersion =
  process.argv[2] === "--finalize-version" && process.argv[3] !== undefined ? process.argv[3] : undefined;
const version = finalizeVersion ?? packageJson.version;
const workspacePackages = [];
const workspacePackageDirectories = ["apps", "examples", "packages", "tools"];

for (const directory of workspacePackageDirectories) {
  const directoryUrl = new URL(`../${directory}/`, import.meta.url);

  if (!existsSync(directoryUrl)) {
    continue;
  }

  for (const entry of readdirSync(directoryUrl, { withFileTypes: true })) {
    if (!entry.isDirectory()) {
      continue;
    }

    const workspacePackageUrl = new URL(`${entry.name}/package.json`, directoryUrl);

    if (!existsSync(workspacePackageUrl)) {
      continue;
    }

    workspacePackages.push(JSON.parse(readFileSync(workspacePackageUrl, "utf8")));
  }
}

const decision = publishDecision({
  env: process.env,
  version,
  workspacePackages,
});

if (decision._tag === "Skip") {
  process.stdout.write(`${decision.message}\n`);
  process.exit(0);
}

if (decision._tag === "Refuse") {
  process.stderr.write(`${decision.message}\n`);
  process.exit(1);
}

const commandResult = (command, args, options = {}) => {
  const result = spawnSync(command, args, options);

  if (result.error !== undefined) {
    throw result.error;
  }

  return result;
};

const run = (command, args, options = {}) => {
  const result = commandResult(command, args, {
    stdio: "inherit",
    ...options,
  });

  if (result.status !== 0) {
    throw Object.assign(new Error(`${command} ${args.join(" ")} failed.`), {
      exitCode: result.status ?? 1,
    });
  }
};

const collectPublishedFiles = (directory, baseDirectory = directory) => {
  const files = [];

  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);

    if (entry.isDirectory()) {
      files.push(...collectPublishedFiles(path, baseDirectory));
      continue;
    }

    files.push({
      relativePath: relative(baseDirectory, path).replaceAll("\\", "/"),
      contents: readFileSync(path, "utf8"),
    });
  }

  return files;
};

const stripPublishedSourceMapReferences = (directory) => {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);

    if (entry.isDirectory()) {
      stripPublishedSourceMapReferences(path);
      continue;
    }

    if (!path.endsWith(".js") && !path.endsWith(".d.ts")) {
      continue;
    }

    writeFileSync(path, stripSourceMapReference(readFileSync(path, "utf8")));
  }
};

const assertCleanPublishedFiles = (stageDirectory) => {
  const violations = publishedFileViolations(collectPublishedFiles(stageDirectory));

  if (violations.length > 0) {
    throw new Error(
      [
        "Refusing npm stage publish because the staged package contains private workspace artifacts.",
        ...violations.map((violation) => `- ${violation}`),
      ].join("\n"),
    );
  }
};

const isPackageAlreadyCreated = () => {
  const result = commandResult("npm", ["view", publicPackageName, "name", "--json"], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  });

  return result.status === 0 && JSON.parse(result.stdout) === publicPackageName;
};

const isVersionAlreadyPublished = () => {
  const result = commandResult(
    "npm",
    ["view", `${publicPackageName}@${version}`, "version", "--json"],
    {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    },
  );

  return result.status === 0 && JSON.parse(result.stdout) === version;
};

const publishedVersionGitHead = () => {
  const result = commandResult(
    "npm",
    ["view", `${publicPackageName}@${version}`, "gitHead", "--json"],
    {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    },
  );

  if (result.status !== 0) {
    return undefined;
  }

  if (result.stdout.trim() === "") {
    return undefined;
  }

  const gitHead = JSON.parse(result.stdout);

  return typeof gitHead === "string" && gitHead.length > 0 ? gitHead : undefined;
};

const runStagePublish = (stageDirectory) => {
  const result = commandResult("npm", stagePublishCommandArguments(stageDirectory), {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });

  process.stdout.write(result.stdout);
  process.stderr.write(result.stderr);

  if (result.status === 0) {
    return {
      _tag: "Staged",
    };
  }

  const duplicate = classifyStagePublishDuplicateOutput({
    stderr: result.stderr,
    stdout: result.stdout,
    version,
  });

  if (duplicate._tag !== "Unknown") {
    return duplicate;
  }

  throw Object.assign(new Error(`npm ${stagePublishCommandArguments(stageDirectory).join(" ")} failed.`), {
    exitCode: result.status ?? 1,
  });
};

const gitRefTarget = (ref) => {
  const target = commandResult("git", ["rev-parse", "--quiet", "--verify", `${ref}^{}`], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  });

  return target.status === 0 ? target.stdout.trim() : undefined;
};

const gitRefObject = (ref) => {
  const target = commandResult("git", ["rev-parse", "--quiet", "--verify", ref], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  });

  return target.status === 0 ? target.stdout.trim() : undefined;
};

const gitTagExists = (tagName) => gitRefTarget(`refs/tags/${tagName}`) !== undefined;

const pushGitTag = (tagName, expectedRemoteObject) => {
  run(
    "git",
    expectedRemoteObject === undefined
      ? ["push", "origin", `refs/tags/${tagName}`]
      : ["push", `--force-with-lease=refs/tags/${tagName}:${expectedRemoteObject}`, "origin", `refs/tags/${tagName}`],
  );
};

const ensureGitTag = (tagName, targetRef = "HEAD", options = {}) => {
  const expectedTarget = gitRefTarget(targetRef);

  if (expectedTarget === undefined) {
    throw new Error(`Cannot create ${tagName} because ${targetRef} does not resolve to a git object.`);
  }

  const existingTarget = gitRefTarget(`refs/tags/${tagName}`);
  const existingObject = gitRefObject(`refs/tags/${tagName}`);

  if (existingTarget !== undefined) {
    if (existingTarget !== expectedTarget) {
      if (options.allowMove !== true) {
        throw new Error(`${tagName} already points at ${existingTarget}, expected ${expectedTarget}.`);
      }

      run("git", ["tag", "-f", "-a", tagName, expectedTarget, "-m", tagName]);
      pushGitTag(tagName, existingObject);
      return;
    }

    return;
  }

  run("git", ["tag", "-a", tagName, expectedTarget, "-m", tagName]);
  pushGitTag(tagName, undefined);
};

const ensurePublishedVersionTag = () => {
  const stagedTagName = stagedPackageTagName(version);
  const targetRef = gitTagExists(stagedTagName) ? `refs/tags/${stagedTagName}` : publishedVersionGitHead();

  if (targetRef === undefined) {
    throw new Error(
      `Cannot create ${packageTagName(version)} because ${stagedTagName} does not exist and npm did not report a gitHead for ${publicPackageName}@${version}.`,
    );
  }

  ensureGitTag(packageTagName(version), targetRef);
};

let exitCode = 0;
const stageDirectory =
  finalizeVersion === undefined ? mkdtempSync(join(tmpdir(), "effect-view-server-publish-")) : undefined;

try {
  if (finalizeVersion !== undefined) {
    if (!isVersionAlreadyPublished()) {
      throw new Error(`${publicPackageName}@${version} is not published yet; approve the npm stage first.`);
    }

    process.stdout.write(`${publicPackageName}@${version} is published; ensuring git tag.\n`);
    ensurePublishedVersionTag();
  } else {
    const distUrl = new URL("../packages/effect-view-server/dist/", import.meta.url);
    const distDirectory = join(stageDirectory, "dist");

    cpSync(distUrl, distDirectory, {
      recursive: true,
      filter: (source) => !source.endsWith(".map"),
    });
    stripPublishedSourceMapReferences(distDirectory);
    cpSync(new URL("../README.md", import.meta.url), join(stageDirectory, "README.md"));
    writeFileSync(
      join(stageDirectory, "package.json"),
      `${JSON.stringify(sanitizePublicPackageJson(packageJson), null, 2)}\n`,
    );

    assertCleanPublishedFiles(stageDirectory);

    if (isVersionAlreadyPublished()) {
      process.stdout.write(`${publicPackageName}@${version} is already published; ensuring git tag.\n`);
      ensurePublishedVersionTag();
    } else {
      if (!isPackageAlreadyCreated()) {
        throw new Error(
          `${publicPackageName} must exist on npm before staged publishing can be used. Publish the first version manually, then rerun this workflow.`,
        );
      }

      const oidcViolations = oidcPublishEnvironmentViolations(process.env);
      if (oidcViolations.length > 0) {
        throw new Error(
          [
            "Refusing npm stage publish because GitHub Actions OIDC is unavailable.",
            ...oidcViolations.map((violation) => `- ${violation}`),
          ].join("\n"),
        );
      }

      const stagedTagName = stagedPackageTagName(version);
      const stageResult = runStagePublish(stageDirectory);
      const duplicateVersionIsPublished =
        (stageResult._tag === "AlreadyPublished" || stageResult._tag === "DuplicateVersion") &&
        isVersionAlreadyPublished();

      if (duplicateVersionIsPublished) {
        process.stdout.write(
          `${publicPackageName}@${version} is already published; ensuring public git tag.\n`,
        );
        ensurePublishedVersionTag();
      } else if (stageResult._tag === "Staged") {
        ensureGitTag(stagedTagName, "HEAD", {
          allowMove: true,
        });
      } else if (stageResult._tag === "AlreadyStaged" || stageResult._tag === "AlreadyPublished") {
        if (!gitTagExists(stagedTagName)) {
          if (!canRecoverMissingStagedMarker(process.env)) {
            throw new Error(
              `npm reported ${publicPackageName}@${version} as already staged, but ${stagedTagName} is missing. Refusing to recreate it from an unrelated workflow HEAD; rerun the failed staging workflow attempt or reject the npm stage and restage.`,
            );
          }

          process.stdout.write(
            `npm reported ${publicPackageName}@${version} as already staged on a retried workflow; recreating missing ${stagedTagName} marker.\n`,
          );
          ensureGitTag(stagedTagName, "HEAD", {
            allowMove: true,
          });
        } else {
          process.stdout.write(`${publicPackageName}@${version} is already staged; keeping ${stagedTagName}.\n`);
        }
      } else if (stageResult._tag === "DuplicateVersion") {
        throw new Error(
          `npm reported a duplicate ${publicPackageName}@${version}, but npm view does not report it as published. Refusing to guess whether a stage exists.`,
        );
      }
    }
  }
} catch (error) {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  exitCode =
    typeof error === "object" &&
    error !== null &&
    "exitCode" in error &&
    typeof error.exitCode === "number"
      ? error.exitCode
      : 1;
} finally {
  if (stageDirectory !== undefined) {
    rmSync(stageDirectory, {
      force: true,
      recursive: true,
    });
  }
}

process.exit(exitCode);
