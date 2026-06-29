import { describe, expect, it } from "@effect/vitest";
import {
  internalPublishViolations,
  packageTagName,
  publishedFileViolations,
  publicPackageName,
  publishDecision,
  sanitizePublicPackageJson,
  stripSourceMapReference,
} from "./release-publish-policy.mjs";

const trustedEnvironment = {
  GITHUB_ACTIONS: "true",
  GITHUB_EVENT_NAME: "push",
  GITHUB_REF: "refs/heads/main",
  GITHUB_REPOSITORY: "bmvantunes/effect-view-server",
};

const workspacePackages = [
  {
    name: publicPackageName,
    private: false,
  },
  {
    name: "@effect-view-server/client",
    private: true,
  },
  {
    name: "@effect-view-server/runtime",
    private: true,
  },
];

const publicPackageJson = {
  name: publicPackageName,
  version: "1.2.3",
  description: "Typed Effect View Server.",
  keywords: ["effect", "view-server"],
  homepage: "https://github.com/bmvantunes/effect-view-server#readme",
  bugs: {
    url: "https://github.com/bmvantunes/effect-view-server/issues",
  },
  license: "MIT",
  repository: {
    type: "git",
    url: "git+https://github.com/bmvantunes/effect-view-server.git",
    directory: "packages/effect-view-server",
  },
  type: "module",
  exports: {
    "./client": {
      types: "./dist/client.d.ts",
      import: "./dist/client.js",
    },
  },
  files: ["dist"],
  publishConfig: {
    provenance: true,
  },
  scripts: {
    build: "vp pack",
  },
  dependencies: {
    "@effect-view-server/client": "workspace:*",
    effect: "4.0.0-beta.91",
  },
  devDependencies: {
    "@effect-view-server/runtime": "workspace:*",
    vitest: "catalog:",
  },
  peerDependencies: {
    react: "19.2.6",
  },
  peerDependenciesMeta: {
    react: {
      optional: true,
    },
  },
};

describe("release publish policy", () => {
  it("skips the placeholder workspace version before checking the GitHub context", () => {
    expect(
      publishDecision({
        env: {},
        version: "0.0.0",
        workspacePackages,
      }),
    ).toStrictEqual({
      _tag: "Skip",
      message: "Skipping npm publish for effect-view-server@0.0.0.",
    });
  });

  it("refuses to publish outside the trusted main branch GitHub Actions context", () => {
    expect(
      publishDecision({
        env: {
          ...trustedEnvironment,
          GITHUB_REF: "refs/heads/feature/release",
        },
        version: "1.2.3",
        workspacePackages,
      }),
    ).toStrictEqual({
      _tag: "Refuse",
      message: "Refusing npm publish outside the trusted main-branch GitHub Actions context.",
    });
  });

  it("refuses to publish when an internal workspace package is public", () => {
    const unsafeWorkspacePackages = [
      ...workspacePackages,
      {
        name: "@effect-view-server/server",
        private: false,
      },
    ];

    expect(internalPublishViolations(unsafeWorkspacePackages)).toStrictEqual([
      "@effect-view-server/server",
    ]);
    expect(
      publishDecision({
        env: trustedEnvironment,
        version: "1.2.3",
        workspacePackages: unsafeWorkspacePackages,
      }),
    ).toStrictEqual({
      _tag: "Refuse",
      message: "Refusing to publish because @effect-view-server/server is not private.",
    });
  });

  it("refuses to publish when an example workspace package is public", () => {
    expect(
      publishDecision({
        env: trustedEnvironment,
        version: "1.2.3",
        workspacePackages: [
          ...workspacePackages,
          {
            name: "@effect-view-server/example-kafka-react",
            private: false,
          },
        ],
      }),
    ).toStrictEqual({
      _tag: "Refuse",
      message: "Refusing to publish because @effect-view-server/example-kafka-react is not private.",
    });
  });

  it("formats plural internal package publish violations", () => {
    expect(
      publishDecision({
        env: trustedEnvironment,
        version: "1.2.3",
        workspacePackages: [
          ...workspacePackages,
          {
            name: "@effect-view-server/server",
            private: false,
          },
          {
            name: "@effect-view-server/runtime",
            private: false,
          },
        ],
      }),
    ).toStrictEqual({
      _tag: "Refuse",
      message:
        "Refusing to publish because @effect-view-server/server, @effect-view-server/runtime are not private.",
    });
  });

  it("allows publish only for the trusted release context with private internals", () => {
    expect(
      publishDecision({
        env: trustedEnvironment,
        version: "1.2.3",
        workspacePackages,
      }),
    ).toStrictEqual({
      _tag: "Publish",
    });
  });

  it("sanitizes the public package manifest before staging the npm artifact", () => {
    expect(sanitizePublicPackageJson(publicPackageJson)).toStrictEqual({
      name: "effect-view-server",
      version: "1.2.3",
      description: "Typed Effect View Server.",
      keywords: ["effect", "view-server"],
      homepage: "https://github.com/bmvantunes/effect-view-server#readme",
      bugs: {
        url: "https://github.com/bmvantunes/effect-view-server/issues",
      },
      license: "MIT",
      repository: {
        type: "git",
        url: "git+https://github.com/bmvantunes/effect-view-server.git",
        directory: "packages/effect-view-server",
      },
      type: "module",
      exports: {
        "./client": {
          types: "./dist/client.d.ts",
          import: "./dist/client.js",
        },
      },
      files: ["dist"],
      publishConfig: {
        access: "public",
        provenance: true,
      },
      dependencies: {
        effect: "4.0.0-beta.91",
      },
      peerDependencies: {
        react: "19.2.6",
      },
      peerDependenciesMeta: {
        react: {
          optional: true,
        },
      },
    });
  });

  it("omits undefined optional manifest fields from the staged npm artifact", () => {
    expect(
      sanitizePublicPackageJson({
        name: publicPackageName,
        version: "1.2.3",
        type: "module",
        exports: {
          "./client": {
            types: "./dist/client.d.ts",
            import: "./dist/client.js",
          },
        },
      }),
    ).toStrictEqual({
      name: "effect-view-server",
      version: "1.2.3",
      type: "module",
      exports: {
        "./client": {
          types: "./dist/client.d.ts",
          import: "./dist/client.js",
        },
      },
      files: ["dist"],
      publishConfig: {
        access: "public",
        provenance: true,
      },
    });
  });

  it("accepts staged files without source maps or internal workspace references", () => {
    expect(
      publishedFileViolations([
        {
          relativePath: "dist/client.js",
          contents: 'const id = Symbol("@effect-view-server/config/KafkaCodecValue");',
        },
      ]),
    ).toStrictEqual([]);
  });

  it("strips source map references from staged runtime files", () => {
    expect(stripSourceMapReference("export const ok = 1;\n//# sourceMappingURL=client.js.map\n"))
      .toStrictEqual("export const ok = 1;\n");
    expect(stripSourceMapReference("//# sourceMappingURL=client.d.ts.map")).toStrictEqual("\n");
  });

  it("rejects staged source maps and internal workspace references", () => {
    expect(
      publishedFileViolations([
        {
          relativePath: "dist/client.js",
          contents: "export const ok = 1;",
        },
        {
          relativePath: "dist/client.js.map",
          contents: "{}",
        },
        {
          relativePath: "dist/runtime.js",
          contents: "export const ok = 1;\n//# sourceMappingURL=runtime.js.map\n",
        },
        {
          relativePath: "package.json",
          contents: '"@effect-view-server/client":"0.0.0"',
        },
        {
          relativePath: "dist/client.d.ts",
          contents: 'import type { Client } from "@effect-view-server/client";',
        },
        {
          relativePath: "dist/internal.js",
          contents: 'import "@effect-view-server/runtime-core";',
        },
      ]),
    ).toStrictEqual([
      "dist/client.js.map is a source map",
      "dist/runtime.js references a source map",
      "package.json references @effect-view-server/",
      "dist/client.d.ts references @effect-view-server/",
      "dist/internal.js references @effect-view-server/",
    ]);
  });

  it("uses the public package name as the release git tag", () => {
    expect(packageTagName("1.2.3")).toStrictEqual("effect-view-server@1.2.3");
  });
});
