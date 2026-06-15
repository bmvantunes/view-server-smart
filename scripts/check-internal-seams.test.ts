import { describe, expect, it } from "@effect/vitest";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import {
  assertNoPackageImportViolations,
  assertNoPackageExportViolations,
  assertNoEngineSeamViolations,
  collectEngineSeamViolations,
  collectPackageExportViolations,
  collectPackageImportViolations,
  importSpecifiersFromSource,
  libraryPackEntrypointPaths,
  packageExportSpecifiersForManifest,
  packageExportViolationMessage,
  packageExportViolationsForManifest,
  packedEntrypointsFromViteConfigContents,
  packedPackageEntrypointsForPackage,
  packageImportViolationsFor,
  packageImportViolationsForFile,
  packageImportViolationMessage,
  packageRelativeImportViolationsFor,
  sourceFiles,
  sourceEntrypointForPackEntry,
  sourceEntrypointForRelativeDistEntrypoint,
  sourceWithoutComments,
  staleApprovedPackageExportViolations,
  topicStoreHelperViolationMessage,
  topicStoreHelperViolationsForFile,
  topicStoreStateExportViolationMessage,
  topicStoreStateExportViolationsForFile,
  toPosixRelativePath,
} from "./check-internal-seams";

const makeDirectory = () => mkdtempSync(join(tmpdir(), "view-server-internal-seams-"));

describe("internal seam checker", () => {
  it("scans TypeScript and TSX source files recursively", () => {
    const directory = makeDirectory();
    const nested = join(directory, "nested");
    mkdirSync(nested);
    writeFileSync(join(directory, "index.ts"), "");
    writeFileSync(join(directory, "component.tsx"), "");
    writeFileSync(join(directory, "ignore.js"), "");
    writeFileSync(join(nested, "testing.tsx"), "");

    expect(sourceFiles(directory).map((path) => basename(path)).sort()).toStrictEqual([
      "component.tsx",
      "index.ts",
      "testing.tsx",
    ]);
  });

  it("collects engine seam violations for restricted helpers and re-exports", () => {
    const engineFile = join(
      process.cwd(),
      "packages",
      "column-live-view-engine",
      "src",
      "engine.ts",
    );
    const indexFile = join(
      process.cwd(),
      "packages",
      "column-live-view-engine",
      "src",
      "index.ts",
    );

    expect(
      topicStoreHelperViolationsForFile({
        contents: "const helper = topicStoreState;",
        path: engineFile,
      }),
    ).toStrictEqual(["packages/column-live-view-engine/src/engine.ts uses topicStoreState"]);
    expect(
      topicStoreStateExportViolationsForFile({
        contents: 'export { topicStoreState } from "./topic-store-state";',
        path: indexFile,
      }),
    ).toStrictEqual([
      "packages/column-live-view-engine/src/index.ts has a restricted state helper re-export",
      "packages/column-live-view-engine/src/index.ts has a restricted local state helper re-export",
    ]);
  });

  it("allows restricted engine helpers in their owning files", () => {
    const topicStoreStateFile = join(
      process.cwd(),
      "packages",
      "column-live-view-engine",
      "src",
      "topic-store-state.ts",
    );

    expect(
      topicStoreHelperViolationsForFile({
        contents: "const helper = topicStoreState;",
        path: topicStoreStateFile,
      }),
    ).toStrictEqual([]);
  });

  it("formats and throws engine seam violation summaries", () => {
    const helperViolations = ["packages/column-live-view-engine/src/engine.ts uses topicStoreState"];
    const stateExportViolations = [
      "packages/column-live-view-engine/src/index.ts has a restricted state helper re-export",
    ];

    expect(topicStoreHelperViolationMessage(helperViolations)).toStrictEqual(
      [
        "Production engine modules must not use restricted TopicStore state helpers.",
        "Route query/read-model behavior through TopicStore helper operations instead.",
        "- packages/column-live-view-engine/src/engine.ts uses topicStoreState",
      ].join("\n"),
    );
    expect(topicStoreStateExportViolationMessage(stateExportViolations)).toStrictEqual(
      [
        "Production engine modules must not re-export restricted TopicStore state internals.",
        "- packages/column-live-view-engine/src/index.ts has a restricted state helper re-export",
      ].join("\n"),
    );
    expect(() =>
      assertNoEngineSeamViolations({
        helperViolations,
        stateExportViolations: [],
      }),
    ).toThrowError("Production engine modules must not use restricted TopicStore state helpers.");
    expect(() =>
      assertNoEngineSeamViolations({
        helperViolations: [],
        stateExportViolations,
      }),
    ).toThrowError("Production engine modules must not re-export restricted TopicStore state internals.");
    expect(
      assertNoEngineSeamViolations({
        helperViolations: [],
        stateExportViolations: [],
      }),
    ).toStrictEqual(undefined);
  });

  it("keeps the current engine source free of internal seam violations", () => {
    expect(collectEngineSeamViolations()).toStrictEqual({
      helperViolations: [],
      stateExportViolations: [],
    });
  });

  it("reports restricted package imports including subexports and dynamic imports", () => {
    const restriction = {
      forbiddenSpecifiers: new Set([
        "@view-server/in-memory",
        "@view-server/runtime",
        "@view-server/server",
      ]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'import "@view-server/runtime";',
          'import { createInMemoryViewServer } from "@view-server/in-memory";',
          'const runtime = import("@view-server/runtime/internal");',
          "const server = import(`@view-server/server`);",
          'import type { ViewServerLiveClient } from "@view-server/client";',
        ].join("\n"),
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @view-server/runtime: React production must stay transport-neutral.",
      "src/index.tsx imports @view-server/in-memory: React production must stay transport-neutral.",
      "src/index.tsx imports @view-server/runtime/internal: View Server imports must use approved package exports.",
      "src/index.tsx imports @view-server/server: React production must stay transport-neutral.",
    ]);
  });

  it("reports restricted package imports with escaped quoted specifiers", () => {
    const restriction = {
      forbiddenSpecifiers: new Set([
        "@view-server/runtime",
        "@view-server/server",
        "@view-server/protocol",
      ]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'import "\\u0040view-server/runtime";',
          'const server = require("\\x40view-server/server");',
          'const protocol = import.meta.resolve("\\u{40}view-server/protocol");',
        ].join("\n"),
        relativePath: "src/index.ts",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.ts imports @view-server/runtime: React production must stay transport-neutral.",
      "src/index.ts imports @view-server/server: React production must stay transport-neutral.",
      "src/index.ts imports @view-server/protocol: React production must stay transport-neutral.",
    ]);
  });

  it("does not report member APIs named import", () => {
    expect(
      importSpecifiersFromSource(
        [
          'registry.import("@view-server/runtime");',
          'registry?.import("@view-server/server");',
          'this.import("@view-server/protocol");',
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("ignores malformed escaped quoted specifiers", () => {
    expect(
      importSpecifiersFromSource(
        [
          'import "\\u{}view-server/runtime";',
          'import "\\u{40view-server/missing-brace";',
          'import "\\u{110000}view-server/out-of-range";',
          'import "\\u{zz}view-server/server";',
          'const protocol = require("\\u12zzview-server/protocol");',
          'const client = import.meta.resolve("\\xzzview-server/client");',
          'const unfinished = require("\\',
        ].join("\n"),
      ),
    ).toStrictEqual([
      "u{}view-server/runtime",
      "u{40view-server/missing-brace",
      "u{110000}view-server/out-of-range",
      "u{zz}view-server/server",
      "u12zzview-server/protocol",
      "xzzview-server/client",
    ]);
  });

  it("reports restricted CommonJS package imports", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'const runtime = require("@view-server/runtime");',
          'const client = require("@view-server/client");',
        ].join("\n"),
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("reports restricted createRequire package imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          'const runtime = createRequire(import.meta.url)("@view-server/runtime");',
          'const server = createRequire(import.meta.url).resolve("@view-server/server");',
          'const protocol = createRequire(import.meta.url).resolve.call(require, "@view-server/protocol");',
          'const client = (createRequire(import.meta.url)).resolve("@view-server/client");',
          'const config = (createRequire(import.meta.url))["resolve"]("@view-server/config");',
          'const inMemory = (createRequire(import.meta.url)).resolve.call(require, "@view-server/in-memory");',
          'function resolveRuntime() { return (createRequire(import.meta.url)).resolve("@view-server/runtime/return"); }',
        ].join("\n"),
      ),
    ).toStrictEqual([
      "@view-server/runtime",
      "@view-server/server",
      "@view-server/protocol",
      "@view-server/client",
      "@view-server/config",
      "@view-server/in-memory",
      "@view-server/runtime/return",
    ]);
  });

  it("does not report member APIs named createRequire", () => {
    expect(
      importSpecifiersFromSource(
        [
          'factory.createRequire(import.meta.url)("@view-server/runtime");',
          'this.#createRequire(import.meta.url)("@view-server/server");',
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("ignores malformed createRequire package imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          "const factory = createRequire;",
          "const inertFactory = createRequire(import.meta.url);",
          "const inertResolve = createRequire(import.meta.url).resolve;",
          "const dynamicResolve = createRequire(import.meta.url).resolve(packageName);",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("reports restricted CommonJS package resolution", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'const runtime = require.resolve("@view-server/runtime");',
          'const client = require.resolve("@view-server/client");',
        ].join("\n"),
        relativePath: "src/index.ts",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.ts imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("does not hide CommonJS imports inside generic calls", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'const runtime = loader<Runtime>(require("@view-server/runtime"));',
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("does not hide CommonJS imports after TypeScript angle-bracket assertions", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'const cast = <Runtime>value; const runtime = require("@view-server/runtime");',
        relativePath: "src/index.ts",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.ts imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("does not hide CommonJS imports after less-than expressions", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'const ok = a < b; const runtime = require("@view-server/runtime");',
        relativePath: "src/index.ts",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.ts imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("ignores require identifiers that are not literal calls", () => {
    expect(importSpecifiersFromSource("const label = require;")).toStrictEqual([]);
  });

  it("ignores require calls without quoted specifiers", () => {
    expect(importSpecifiersFromSource("const runtime = require(packageName);")).toStrictEqual([]);
  });

  it("ignores require resolve calls without quoted specifiers", () => {
    expect(importSpecifiersFromSource("const runtime = require.resolve(packageName);")).toStrictEqual(
      [],
    );
  });

  it("ignores require resolve property reads", () => {
    expect(importSpecifiersFromSource("const runtime = require.resolve;")).toStrictEqual([]);
  });

  it("detects optional CommonJS literal calls", () => {
    expect(
      importSpecifiersFromSource(
        [
          'const runtime = require?.("@view-server/runtime");',
          'const server = require.resolve?.("@view-server/server");',
          'const client = require?.resolve?.("@view-server/client");',
          'const spacedRuntime = require ?. ("@view-server/runtime");',
          'const spacedServer = require . resolve ?. ("@view-server/server");',
          'const spacedClient = require ?. resolve("@view-server/client");',
          'const bracketRuntime = require?.["resolve"]("@view-server/runtime");',
          'const bracketServer = require["resolve"]?.("@view-server/server");',
          'const parenthesizedResolveRuntime = (require).resolve("@view-server/runtime/resolve");',
          'const parenthesizedBracketResolveRuntime = (require)["resolve"]("@view-server/runtime/bracket-resolve");',
          'const parenthesizedRuntime = (require)("@view-server/runtime");',
          'function loadRuntime() { return (require)("@view-server/runtime/return"); }',
          'const voidRuntime = void (require)("@view-server/runtime/void");',
          'for (;;) (require)("@view-server/runtime/for");',
          'if (ok) noop(); else (require)("@view-server/runtime/else");',
          'if (path.endsWith(")")) (require)("@view-server/runtime/if-string");',
          'if (path.match(/[)]/)) (require)("@view-server/runtime/if-regex");',
          'if (ok /* ) */) (require)("@view-server/runtime/if-block-comment");',
          'if (ok // )\n) (require)("@view-server/runtime/if-line-comment");',
          'do (require)("@view-server/runtime/do"); while (ok);',
          'const sequenceRuntime = (0, require)("@view-server/runtime/sequence");',
          'const logicalRuntime = (false || require)("@view-server/runtime/logical");',
          'const fallbackLogicalRuntime = (require || fallback)("@view-server/runtime/logical-left");',
          'const parenthesizedFallbackLogicalRuntime = ((require) || fallback)("@view-server/runtime/parenthesized-logical-left");',
          'const nullishRuntime = (require ?? fallback)("@view-server/runtime/nullish-left");',
          'const nullishFallbackWithParenRuntime = (require ?? fallback(")"))("@view-server/runtime/nullish-fallback-paren");',
          'const nullishFallbackRegexRuntime = (require ?? /[)]/)("@view-server/runtime/nullish-fallback-regex");',
          'const fallbackNullishRuntime = (fallback ?? require)("@view-server/runtime/nullish-right");',
          'const parenthesizedFallbackNullishRuntime = (fallback ?? (require))("@view-server/runtime/parenthesized-nullish-right");',
          'const ternaryRuntime = (condition ? require : fallback)("@view-server/runtime/ternary-then");',
          'const ternaryFallbackWithParenRuntime = (condition ? require : fallback({ text: ")" }))("@view-server/runtime/ternary-fallback-paren");',
          'const fallbackTernaryRuntime = (condition ? fallback : require)("@view-server/runtime/ternary-else");',
          'const nestedSequenceRuntime = ((0, require))("@view-server/runtime/nested-sequence");',
          'const sequenceCalledRuntime = (0, require).call(undefined, "@view-server/runtime/sequence-call");',
          'const sequenceBoundRuntime = (0, require).bind(undefined)("@view-server/runtime/sequence-bind");',
          'const nestedRuntime = ((require))("@view-server/runtime/nested");',
          'const calledRuntime = require.call(undefined, "@view-server/runtime/call");',
          'const calledServer = (require).call(undefined, "@view-server/server/call");',
          'const boundRuntime = require.bind(undefined)("@view-server/runtime/bind");',
          'const boundArgumentRuntime = require.bind(undefined, "@view-server/runtime/bind-argument")();',
          'const parenthesizedBoundRuntime = (require).bind(undefined)("@view-server/runtime/parenthesized-bind");',
          'const regexBoundRuntime = require.bind(/,/)("@view-server/runtime/regex-bind");',
          'const appliedRuntime = require.apply(undefined, ["@view-server/runtime/apply"]);',
          'const extraAppliedRuntime = require.apply(undefined, ["@view-server/runtime/extra-apply", extra]);',
          'const regexAppliedRuntime = require.apply(/,/, ["@view-server/runtime/regex-apply"]);',
          'const nestedApplyRuntime = require.apply(fn("ignored", value), ["@view-server/runtime/nested-apply"]);',
          'const regexRuntime = require.call(/,/, "@view-server/runtime/regex");',
          'const quoteRegexRuntime = require.call(/"/, "@view-server/runtime/quote-regex");',
          'const escapedRuntime = requ\\u0069re("@view-server/runtime/escaped");',
          'const escapedBraceRuntime = requ\\u{69}re("@view-server/runtime/escaped-brace");',
        ].join("\n"),
      ),
    ).toStrictEqual([
      "@view-server/runtime",
      "@view-server/server",
      "@view-server/client",
      "@view-server/runtime",
      "@view-server/server",
      "@view-server/client",
      "@view-server/runtime",
      "@view-server/server",
      "@view-server/runtime/resolve",
      "@view-server/runtime/bracket-resolve",
      "@view-server/runtime",
      "@view-server/runtime/return",
      "@view-server/runtime/void",
      "@view-server/runtime/for",
      "@view-server/runtime/else",
      "@view-server/runtime/if-string",
      "@view-server/runtime/if-regex",
      "@view-server/runtime/if-block-comment",
      "@view-server/runtime/if-line-comment",
      "@view-server/runtime/do",
      "@view-server/runtime/sequence",
      "@view-server/runtime/logical",
      "@view-server/runtime/logical-left",
      "@view-server/runtime/parenthesized-logical-left",
      "@view-server/runtime/nullish-left",
      "@view-server/runtime/nullish-fallback-paren",
      "@view-server/runtime/nullish-fallback-regex",
      "@view-server/runtime/nullish-right",
      "@view-server/runtime/parenthesized-nullish-right",
      "@view-server/runtime/ternary-then",
      "@view-server/runtime/ternary-fallback-paren",
      "@view-server/runtime/ternary-else",
      "@view-server/runtime/nested-sequence",
      "@view-server/runtime/sequence-call",
      "@view-server/runtime/sequence-bind",
      "@view-server/runtime/nested",
      "@view-server/runtime/call",
      "@view-server/server/call",
      "@view-server/runtime/bind",
      "@view-server/runtime/bind-argument",
      "@view-server/runtime/parenthesized-bind",
      "@view-server/runtime/regex-bind",
      "@view-server/runtime/apply",
      "@view-server/runtime/extra-apply",
      "@view-server/runtime/regex-apply",
      "@view-server/runtime/nested-apply",
      "@view-server/runtime/regex",
      "@view-server/runtime/quote-regex",
      "@view-server/runtime/escaped",
      "@view-server/runtime/escaped-brace",
    ]);
  });

  it("ignores malformed optional CommonJS accessors", () => {
    expect(
      importSpecifiersFromSource(
        [
          "require;",
          "const inertSequence = (0, require);",
          "const inertSequenceCall = (0, require).call(undefined);",
          'const localLoader = (require && localLoader)("@view-server/runtime");',
          "const inertParenthesizedNullish = (fallback ?? (require));",
          "const inertNestedParenthesizedNullish = ((fallback ?? (require)));",
          'const unfinishedNullishWrapper = (require ?? fallback("@view-server/runtime");',
          'const unfinishedTernaryWrapper = (condition ? require : fallback("@view-server/runtime");',
          'const runtime = require ? ("@view-server/runtime") : undefined;',
          'const server = require ? resolveCandidate("@view-server/server") : undefined;',
          'const client = module ? requireCandidate("@view-server/client") : undefined;',
          'const runtimeCandidate = module.load("@view-server/runtime");',
          'const serverCandidate = module?.load("@view-server/server");',
          'const parenthesizedRuntimeCandidate = (require).load("@view-server/runtime");',
          'const parenthesizedServerCandidate = (module).load("@view-server/server");',
          'const protocolCandidate = require["load"]("@view-server/protocol");',
          'const missingBracket = require["resolve"("@view-server/runtime");',
          'const malformedEscapedRequire = requ\\u{zz}re("@view-server/runtime");',
          'const malformedCodePointRequire = requ\\u{110000}re("@view-server/runtime");',
          'const malformedFixedEscapedRequire = requ\\u00zzre("@view-server/server");',
          'const indirectLoader = makeLoader(require)("@view-server/runtime");',
          'const indirectResolver = makeResolver(require.resolve)("@view-server/runtime");',
          'const indirectParenthesizedResolver = makeResolver((require).resolve)("@view-server/runtime");',
          'const indirectParenthesizedModuleRequire = makeResolver((module).require)("@view-server/runtime");',
          'const indirectParenthesizedImportMetaResolve = makeResolver((import.meta).resolve).call(undefined, "@view-server/runtime");',
          'const indirectImportMetaResolver = makeResolver(import.meta.resolve).call(undefined, "@view-server/runtime");',
          'const malformedControl = if ok) (require)("@view-server/runtime");',
          'const malformedOpenControl = if (ok (require)("@view-server/runtime");',
          "const bindProperty = require.bind;",
          "const inertBind = require.bind(undefined);",
          "const unfinishedBind = require.bind(undefined",
          "const applyProperty = require.apply;",
          "const parenthesizedApplyProperty = (require).apply;",
          "const unfinishedNoCommaApply = require.apply(undefined",
          'const stringApply = require.apply(undefined, "@view-server/runtime");',
          'const missingApplyArgument = require.apply(undefined);',
          "const dynamicApply = require.apply(undefined, packageName);",
          "const emptyApply = require.apply(undefined, []);",
          "const unfinishedEmptyApply = require.apply(undefined, [",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
    expect(importSpecifiersFromSource("const unfinished = (require ?? fallback")).toStrictEqual([]);
    expect(importSpecifiersFromSource("const unfinished = (condition ? require : fallback")).toStrictEqual(
      [],
    );
  });

  it("detects bracketed CommonJS package resolution", () => {
    expect(
      importSpecifiersFromSource(
        [
          'const runtime = require["resolve"]("@view-server/runtime");',
          "const server = require['resolve']('@view-server/server');",
          'const parenthesizedRuntime = (require.resolve)("@view-server/runtime/parenthesized");',
          'const parenthesizedBaseRuntime = ((require).resolve)("@view-server/runtime/parenthesized-base");',
          'for (const item of items) (require.resolve)("@view-server/server/for-of");',
          'for await (const item of items) (require.resolve)("@view-server/server/for-await");',
          'while (path.endsWith(")")) (require.resolve)("@view-server/runtime/while-string");',
          'const ternaryParenthesizedResolve = (condition ? (require.resolve) : fallback)("@view-server/runtime/ternary-parenthesized-resolve");',
          'const sequenceServer = (0, require.resolve)("@view-server/server/sequence");',
          'const sequenceCalledServer = (0, require.resolve).call(require, "@view-server/server/sequence-call");',
          'const calledRuntime = require.resolve.call(require, "@view-server/runtime/call");',
          'const regexRuntime = require.resolve.call(/,/, "@view-server/runtime/regex");',
          'const appliedServer = require.resolve.apply(require, ["@view-server/server/apply", { paths: [] }]);',
          'const escapedServer = require.res\\u006flve("@view-server/server/escaped");',
        ].join("\n"),
      ),
    ).toStrictEqual([
      "@view-server/runtime",
      "@view-server/server",
      "@view-server/runtime/parenthesized",
      "@view-server/runtime/parenthesized-base",
      "@view-server/server/for-of",
      "@view-server/server/for-await",
      "@view-server/runtime/while-string",
      "@view-server/runtime/ternary-parenthesized-resolve",
      "@view-server/server/sequence",
      "@view-server/server/sequence-call",
      "@view-server/runtime/call",
      "@view-server/runtime/regex",
      "@view-server/server/apply",
      "@view-server/server/escaped",
    ]);
  });

  it("detects import meta package resolution", () => {
    expect(
      importSpecifiersFromSource(
        [
          'const runtime = import.meta.resolve("@view-server/runtime");',
          'const server = import.meta.resolve?.("@view-server/server");',
          'const protocol = import.meta["resolve"]("@view-server/protocol");',
          'const client = (import.meta.resolve)("@view-server/client");',
          'async function resolveRuntime() { return await (import.meta.resolve)("@view-server/runtime/await"); }',
          'const sequenceClient = (0, import.meta.resolve)("@view-server/client/sequence");',
          'const nestedSequenceClient = ((0, import.meta.resolve))("@view-server/client/nested-sequence");',
          'const sequenceCalledClient = (0, import.meta.resolve).call(import.meta, "@view-server/client/sequence-call");',
          'const ternaryParenthesizedCalledClient = (condition ? fallback : (import.meta.resolve)).call(import.meta, "@view-server/client/ternary-parenthesized-call");',
          'const config = (import.meta["resolve"])?.("@view-server/config");',
          'const parenthesizedBaseRuntime = (import.meta).resolve("@view-server/runtime/parenthesized-base");',
          'const parenthesizedBaseServer = (import.meta)["resolve"]("@view-server/server/parenthesized-base");',
          'const rpc = import.meta.resolve.call(import.meta, "@view-server/protocol/rpc");',
          'const health = (import.meta.resolve).call(import.meta, "@view-server/protocol/health");',
          'const runtimeAgain = import.meta.resolve.call(getMeta("ignored", import.meta), "@view-server/runtime/internal");',
          'const regexClient = import.meta.resolve.call(/,/, "@view-server/client/regex");',
          'const appliedClient = import.meta.resolve.apply(/,/, ["@view-server/client/apply"]);',
          'const boundArgumentClient = import.meta.resolve.bind(import.meta, "@view-server/client/bind-argument")();',
          'const nestedRuntime = ((import.meta.resolve))("@view-server/runtime/nested");',
        ].join("\n"),
      ),
    ).toStrictEqual([
      "@view-server/runtime",
      "@view-server/server",
      "@view-server/protocol",
      "@view-server/client",
      "@view-server/runtime/await",
      "@view-server/client/sequence",
      "@view-server/client/nested-sequence",
      "@view-server/client/sequence-call",
      "@view-server/client/ternary-parenthesized-call",
      "@view-server/config",
      "@view-server/runtime/parenthesized-base",
      "@view-server/server/parenthesized-base",
      "@view-server/protocol/rpc",
      "@view-server/protocol/health",
      "@view-server/runtime/internal",
      "@view-server/client/regex",
      "@view-server/client/apply",
      "@view-server/client/bind-argument",
      "@view-server/runtime/nested",
    ]);
  });

  it("ignores malformed import meta package resolution", () => {
    expect(
      importSpecifiersFromSource(
        [
          'const runtime = import.meta.load("@view-server/runtime");',
          'const server = import.metadata.resolve("@view-server/server");',
          'const protocol = import ? meta.resolve("@view-server/protocol") : undefined;',
          "const client = import.meta.resolve(packageName);",
          "const clientResolver = (import.meta.resolve);",
          "const config = import.meta.resolve.call;",
          'const effectUtils = import.meta.resolve.call("ignored");',
          'const serverPackage = import.meta.resolve.call("ignored", packageName);',
          'const runtimePackage = import.meta.resolve.call("quoted, comma");',
          "const unterminatedCall = import.meta.resolve.call(import.meta",
          'const falseRuntime = registry.import.meta.resolve("@view-server/runtime");',
          'const falseServer = registry.import.meta.resolve.call(registry, "@view-server/server");',
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("does not report import meta resolution specifiers from call context arguments", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'const fs = import.meta.resolve.call(["ignored", "@view-server/runtime"], "node:fs");',
        relativePath: "src/index.ts",
        restriction,
      }),
    ).toStrictEqual([]);
  });

  it("detects Node module.require literal calls", () => {
    expect(
      importSpecifiersFromSource(
        [
          'const runtime = module.require("@view-server/runtime");',
          'const server = module?.require("@view-server/server");',
          'const protocol = module?.require?.("@view-server/protocol");',
          'const client = module.require("@view-server/client");',
          'const spacedRuntime = module ?. require("@view-server/runtime");',
          'const spacedServer = module?. require("@view-server/server");',
          'const spacedProtocol = module ?. require ?. ("@view-server/protocol");',
          'const bracketRuntime = module["require"]("@view-server/runtime");',
          'const bracketServer = module?.["require"]("@view-server/server");',
          'const bracketProtocol = module["require"]?.("@view-server/protocol");',
          'const parenthesizedBaseRuntime = (module).require("@view-server/runtime/base");',
          'const parenthesizedBaseServer = (module)["require"]("@view-server/server/base");',
          'const parenthesizedRuntime = (module.require)("@view-server/runtime/parenthesized");',
          'if (ok) (module.require)("@view-server/runtime/if");',
          'if (isEnabled()) (module.require)("@view-server/runtime/if-call");',
          'const calledRuntime = module.require.call(module, "@view-server/runtime/call");',
          'const boundRuntime = module.require.bind(module)("@view-server/runtime/bind");',
          'const boundArgumentRuntime = module.require.bind(module, "@view-server/runtime/bind-argument")();',
          'const appliedRuntime = module.require.apply(module, ["@view-server/runtime/apply"]);',
          'const regexRuntime = module.require.call(/,/, "@view-server/runtime/regex");',
          'const escapedProtocol = module.requ\\u0069re("@view-server/protocol/escaped");',
        ].join("\n"),
      ),
    ).toStrictEqual([
      "@view-server/runtime",
      "@view-server/server",
      "@view-server/protocol",
      "@view-server/client",
      "@view-server/runtime",
      "@view-server/server",
      "@view-server/protocol",
      "@view-server/runtime",
      "@view-server/server",
      "@view-server/protocol",
      "@view-server/runtime/base",
      "@view-server/server/base",
      "@view-server/runtime/parenthesized",
      "@view-server/runtime/if",
      "@view-server/runtime/if-call",
      "@view-server/runtime/call",
      "@view-server/runtime/bind",
      "@view-server/runtime/bind-argument",
      "@view-server/runtime/apply",
      "@view-server/runtime/regex",
      "@view-server/protocol/escaped",
    ]);
  });

  it("ignores Node module.require calls without quoted specifiers", () => {
    expect(importSpecifiersFromSource("const runtime = module.require(packageName);")).toStrictEqual(
      [],
    );
  });

  it("ignores member APIs named module.require", () => {
    expect(
      importSpecifiersFromSource(
        [
          'loader.module.require("@view-server/runtime");',
          'this.#module.require("@view-server/server");',
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("ignores private member APIs named require", () => {
    expect(
      importSpecifiersFromSource('class Loader { load() { return this.#require("@view-server/runtime"); } }'),
    ).toStrictEqual([]);
  });

  it("ignores interpolated CommonJS template specifiers", () => {
    expect(
      importSpecifiersFromSource(
        [
          "const packageName = 'runtime';",
          "const runtime = require(`@external/${packageName}`);",
          "const resolved = require.resolve(`@external/${packageName}`);",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("reports interpolated View Server CommonJS template specifiers conservatively", () => {
    expect(
      importSpecifiersFromSource(
        [
          "const packageName = 'runtime';",
          "const runtime = require(`@view-server/${packageName}`);",
          "const resolved = require.resolve(`@view-server/${packageName}`);",
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/${packageName}", "@view-server/${packageName}"]);
  });

  it("ignores comments while scanning template expressions for CommonJS imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          "const value = `${// }",
          'require("@view-server/runtime")',
          "}`;",
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/runtime"]);
    expect(importSpecifiersFromSource("const unfinished = `${// }")).toStrictEqual([]);
    expect(importSpecifiersFromSource("const unfinished = `${/* }")).toStrictEqual([]);
  });

  it("does not treat regex literal slash pairs as comments in template expressions", () => {
    expect(
      importSpecifiersFromSource(
        'const value = `${/\\\\//.test(path) && require("@view-server/runtime")}`;',
      ),
    ).toStrictEqual(["@view-server/runtime"]);
    expect(
      importSpecifiersFromSource(
        'const value = `${"source" in /\\\\// && require("@view-server/server")}`;',
      ),
    ).toStrictEqual(["@view-server/server"]);
  });

  it("does not treat regex literal slash pairs as comments before CommonJS imports", () => {
    expect(
      importSpecifiersFromSource(
        'const value = /\\\\//.test(path) && require("@view-server/runtime");',
      ),
    ).toStrictEqual(["@view-server/runtime"]);
    expect(
      importSpecifiersFromSource(
        'const value = /[/]/gi.test(path) && require("@view-server/server");',
      ),
    ).toStrictEqual(["@view-server/server"]);
    expect(
      importSpecifiersFromSource(
        'if ("source" in /\\\\// && require("@view-server/protocol")) {}',
      ),
    ).toStrictEqual(["@view-server/protocol"]);
    expect(
      importSpecifiersFromSource(
        'if (path) /\\\\//.test(path) && require("@view-server/client");',
      ),
    ).toStrictEqual(["@view-server/client"]);
    expect(
      importSpecifiersFromSource(
        'if (fn({ value: true })) /[//]/.test(path) && require("@view-server/runtime");',
      ),
    ).toStrictEqual(["@view-server/runtime"]);
    expect(
      importSpecifiersFromSource(
        [
          "if (",
          "  path",
          ') /[//]/.test(path) && require("@view-server/client");',
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/client"]);
    expect(
      importSpecifiersFromSource(
        'for (;;) /[//]/.test(path) && require("@view-server/server");',
      ),
    ).toStrictEqual(["@view-server/server"]);
    expect(
      importSpecifiersFromSource(
        'for (const value of /\\\\//) require("@view-server/protocol");',
      ),
    ).toStrictEqual(["@view-server/protocol"]);
  });

  it("detects no-substitution CommonJS template specifiers", () => {
    expect(
      importSpecifiersFromSource(
        [
          "const runtime = require(`@view-server/runtime`);",
          "const server = require.resolve(`@view-server/server`);",
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/runtime", "@view-server/server"]);
  });

  it("ignores member APIs named require", () => {
    expect(
      importSpecifiersFromSource(
        [
          'validator.require("@view-server/runtime");',
          'this.require("@view-server/server");',
          'loader?.require("@view-server/client");',
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("does not hide CommonJS imports after self-closing JSX", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'const node = <Panel />; const runtime = require("@view-server/runtime");',
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("ignores comments while scanning JSX expressions for CommonJS imports", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: '<Panel value={/* } */ require("@view-server/runtime")} />',
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
    expect(
      packageImportViolationsFor({
        contents: ['<Panel value={// }', 'require("@view-server/runtime")} />'].join("\n"),
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
    expect(packageImportViolationsFor({
      contents: "<Panel value={// }",
      relativePath: "src/index.tsx",
      restriction,
    })).toStrictEqual([]);
  });

  it("does not treat regex literal slash pairs as comments in JSX expressions", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: '<Panel value={/\\\\//.test(path) && require("@view-server/runtime")} />',
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
    expect(
      packageImportViolationsFor({
        contents: '<Panel value={"source" in /\\\\// && require("@view-server/runtime")} />',
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("rejects deep imports even when the package root is allowed", () => {
    const restriction = {
      allowedSpecifiers: new Set(["@view-server/client"]),
      forbiddenSpecifiers: new Set(["@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'import type { ViewServerLiveClient } from "@view-server/client";',
          'import { makeViewServerClient } from "@view-server/client/remote/internal";',
        ].join("\n"),
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @view-server/client/remote/internal: View Server imports must use approved package exports.",
    ]);
  });

  it("rejects approved subexports that are not explicitly allowed for a package", () => {
    const restriction = {
      allowedSpecifiers: new Set(["@view-server/client"]),
      forbiddenSpecifiers: new Set(["@view-server/client"]),
      message: "Server code may depend on client contracts only.",
      packageName: "server",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'import type { ViewServerLiveClient } from "@view-server/client";',
          'import { makeViewServerClient } from "@view-server/client/remote";',
        ].join("\n"),
        relativePath: "src/index.ts",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.ts imports @view-server/client/remote: Server code may depend on client contracts only.",
    ]);
  });

  it("allows intentionally carved testing entrypoints", () => {
    const restriction = {
      allowedRelativePathSpecifiers: new Map([
        ["src/testing.tsx", new Set(["@view-server/in-memory"])],
      ]),
      forbiddenSpecifiers: new Set(["@view-server/in-memory"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'import { createInMemoryViewServer } from "@view-server/in-memory";',
        relativePath: "src/testing.tsx",
        restriction,
      }),
    ).toStrictEqual([]);
  });

  it("matches relative path carveouts across path separators", () => {
    const restriction = {
      allowedRelativePathSpecifiers: new Map([
        ["src/testing.tsx", new Set(["@view-server/in-memory"])],
      ]),
      forbiddenSpecifiers: new Set(["@view-server/in-memory"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'import { createInMemoryViewServer } from "@view-server/in-memory";',
        relativePath: toPosixRelativePath("src\\testing.tsx"),
        restriction,
      }),
    ).toStrictEqual([]);
  });

  it("does not allow testing entrypoint carveouts to hide unrelated forbidden packages", () => {
    const restriction = {
      allowedRelativePathSpecifiers: new Map([
        ["src/testing.tsx", new Set(["@view-server/in-memory"])],
      ]),
      forbiddenSpecifiers: new Set(["@view-server/in-memory", "@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'import { createInMemoryViewServer } from "@view-server/in-memory";',
          'import { createViewServerRuntime } from "@view-server/runtime";',
        ].join("\n"),
        relativePath: "src/testing.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/testing.tsx imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("rejects relative imports that escape a package root", () => {
    const packageRoot = join("/repo", "packages", "react");
    const path = join(packageRoot, "src", "index.tsx");

    expect(
      packageRelativeImportViolationsFor({
        contents: [
          'import { local } from "./internal";',
          'import { createViewServerRuntime } from "../../runtime/src/index";',
        ].join("\n"),
        packageRoot,
        path,
      }),
    ).toStrictEqual([
      "src/index.tsx imports ../../runtime/src/index: relative imports must not cross package seams.",
    ]);
  });

  it("collects relative and package import violations for a package file", () => {
    const packageRoot = join("/repo", "packages", "react");
    const path = join(packageRoot, "src", "index.tsx");

    expect(
      packageImportViolationsForFile({
        contents: [
          'import { local } from "./internal";',
          'import { createViewServerRuntime } from "@view-server/runtime";',
          'import { server } from "../../server/src/index";',
        ].join("\n"),
        packageRoot,
        path,
        restriction: {
          allowedSpecifiers: new Set(["@view-server/client"]),
          forbiddenSpecifiers: new Set(["@view-server/runtime"]),
          message: "React production must stay transport-neutral.",
          packageName: "react",
        },
      }),
    ).toStrictEqual([
      "packages/react/src/index.tsx imports ../../server/src/index: relative imports must not cross package seams.",
      "packages/react/src/index.tsx imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("formats and throws package import violation summaries", () => {
    const violations = ["packages/react/src/index.tsx imports @view-server/runtime: no"];

    expect(packageImportViolationMessage(violations)).toStrictEqual(
      [
        "Package architecture seam violations found.",
        "- packages/react/src/index.tsx imports @view-server/runtime: no",
      ].join("\n"),
    );
    expect(() => assertNoPackageImportViolations(violations)).toThrowError(
      "Package architecture seam violations found.",
    );
    expect(assertNoPackageImportViolations([])).toStrictEqual(undefined);
  });

  it("keeps the current repository free of package import violations", () => {
    expect(collectPackageImportViolations()).toStrictEqual([]);
  }, 30000);

  it("collects package export specifiers from package manifests", () => {
    expect(
      packageExportSpecifiersForManifest(
        JSON.stringify({
          name: "@view-server/example",
          exports: {
            ".": {
              import: "./dist/index.js",
              types: "./dist/index.d.ts",
            },
            "./testing": {
              import: "./dist/testing.js",
              types: "./dist/testing.d.ts",
            },
          },
        }),
      ),
    ).toStrictEqual(["@view-server/example", "@view-server/example/testing"]);
  });

  it("parses Vite+ libraryPack entrypoint declarations", () => {
    expect(libraryPackEntrypointPaths('export default { pack: libraryPack("src/index.ts") };')).toStrictEqual([
      "src/index.ts",
    ]);
    expect(
      libraryPackEntrypointPaths(
        [
          "export default {",
          "  pack: libraryPack([",
          '    "src/index.ts",',
          '    // "src/internal.ts",',
          '    "src/testing.tsx",',
          "  ]),",
          "};",
        ].join("\n"),
      ),
    ).toStrictEqual(["src/index.ts", "src/testing.tsx"]);
    expect(libraryPackEntrypointPaths("export default { fmt: {} };")).toStrictEqual([]);
  });

  it("normalizes safe libraryPack source entrypoints only", () => {
    expect(sourceEntrypointForPackEntry("src/index.ts")).toStrictEqual("index");
    expect(sourceEntrypointForPackEntry("src/testing.tsx")).toStrictEqual("testing");
    expect(sourceEntrypointForPackEntry("generated/index.ts")).toStrictEqual(undefined);
    expect(sourceEntrypointForPackEntry("src/index.js")).toStrictEqual(undefined);
    expect(sourceEntrypointForPackEntry("src/../index.ts")).toStrictEqual(undefined);
  });

  it("collects packed entrypoints from Vite+ config contents", () => {
    expect(
      Array.from(
        packedEntrypointsFromViteConfigContents(
          [
            "export default {",
            "  pack: libraryPack([",
            '    "generated/index.ts",',
            '    "src/index.ts",',
            '    "src/feature.tsx",',
            '    "src/../escape.ts",',
            "  ]),",
            "};",
          ].join("\n"),
        ),
      ).sort(),
    ).toStrictEqual(["feature", "index"]);
  });

  it("collects packed entrypoints from package Vite+ config files", () => {
    expect(Array.from(packedPackageEntrypointsForPackage("react")).sort()).toStrictEqual(["index", "testing"]);
    expect(Array.from(packedPackageEntrypointsForPackage("missing-package"))).toStrictEqual([]);
  });

  it("resolves package source entrypoint files without normalizing missing files into existence", () => {
    expect(sourceEntrypointForRelativeDistEntrypoint("react", "testing")?.endsWith("src/testing.tsx")).toStrictEqual(
      true,
    );
    expect(sourceEntrypointForRelativeDistEntrypoint("react", "missing")).toStrictEqual(undefined);
  });

  it("collects root conditional package export maps as the root specifier", () => {
    expect(
      packageExportSpecifiersForManifest(
        JSON.stringify({
          name: "@view-server/client",
          exports: {
            import: "./dist/index.js",
            types: "./dist/index.d.ts",
          },
        }),
      ),
    ).toStrictEqual(["@view-server/client"]);
  });

  it("collects types-only root conditional package export maps as the root specifier", () => {
    expect(
      packageExportSpecifiersForManifest(
        JSON.stringify({
          name: "@view-server/client",
          exports: {
            types: "./dist/index.d.ts",
          },
        }),
      ),
    ).toStrictEqual(["@view-server/client"]);
  });

  it("collects default-only root conditional package export maps as the root specifier", () => {
    expect(
      packageExportSpecifiersForManifest(
        JSON.stringify({
          name: "@view-server/client",
          exports: {
            default: "./dist/index.js",
          },
        }),
      ),
    ).toStrictEqual(["@view-server/client"]);
  });

  it("keeps non-subpath keys readable in mixed package export maps", () => {
    expect(
      packageExportSpecifiersForManifest(
        JSON.stringify({
          name: "@view-server/example",
          exports: {
            ".": {
              import: "./dist/index.js",
              types: "./dist/index.d.ts",
            },
            types: "./dist/index.d.ts",
          },
        }),
      ),
    ).toStrictEqual(["@view-server/example", "@view-server/example/types"]);
  });

  it("accepts root conditional package export maps for packed entries", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/client",
          exports: {
            import: "./dist/index.js",
            types: "./dist/index.d.ts",
          },
        }),
        packageDirectoryName: "client",
      }),
    ).toStrictEqual([]);
  });

  it("accepts packed TSX package export entrypoints", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": {
              browser: ["./dist/testing.js", null],
              default: "./dist/testing.js",
              import: "./dist/testing.js",
              node: null,
              types: "./dist/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([]);
  });

  it("accepts packed package export fallback arrays", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": {
              import: [null, "./dist/testing.js"],
              types: [null, "./dist/testing.d.ts"],
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([]);
  });

  it("accepts packed conditional objects inside package export fallback arrays", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": [
              {
                import: "./dist/testing.js",
                types: "./dist/testing.d.ts",
              },
            ],
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([]);
  });

  it("accepts packed default conditional package export entrypoints", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": {
              default: "./dist/testing.js",
              types: "./dist/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([]);
  });

  it("accepts packed nested types conditional package export entrypoints", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": {
              default: "./dist/testing.js",
              types: {
                default: "./dist/testing.d.ts",
              },
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([]);
  });

  it("accepts packed versioned TypeScript package export entrypoints", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": {
              import: "./dist/testing.js",
              "types@>=5.2": "./dist/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([]);
  });

  it("accepts packed nested runtime conditional package export entrypoints", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": {
              node: {
                import: "./dist/testing.js",
              },
              types: "./dist/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([]);
  });

  it("accepts packed default conditional objects inside package export fallback arrays", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": [
              {
                browser: null,
                default: "./dist/testing.js",
                types: "./dist/testing.d.ts",
              },
            ],
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([]);
  });

  it("collects root string package export maps as the root specifier", () => {
    expect(
      packageExportSpecifiersForManifest(
        JSON.stringify({
          exports: "./dist/index.js",
          name: "@view-server/example",
        }),
      ),
    ).toStrictEqual(["@view-server/example"]);
  });

  it("collects root fallback array package export maps as the root specifier", () => {
    expect(
      packageExportSpecifiersForManifest(
        JSON.stringify({
          exports: ["./dist/index.js"],
          name: "@view-server/example",
        }),
      ),
    ).toStrictEqual(["@view-server/example"]);
  });

  it("ignores package export violations from unsupported top-level export shapes", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([]);
  });

  it("ignores package export specifiers when the manifest has no package name", () => {
    expect(
      packageExportSpecifiersForManifest(
        JSON.stringify({
          exports: {
            ".": {
              import: "./dist/index.js",
              types: "./dist/index.d.ts",
            },
          },
        }),
      ),
    ).toStrictEqual([]);
  });

  it("collects package export specifiers from string targets", () => {
    expect(
      packageExportSpecifiersForManifest(
        JSON.stringify({
          name: "@view-server/example",
          exports: {
            ".": "./dist/index.js",
            "./array": ["./dist/array.js"],
            "./null": null,
          },
        }),
      ),
    ).toStrictEqual(["@view-server/example", "@view-server/example/array", "@view-server/example/null"]);
  });

  it("reports unsupported package export map targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./array": ["./dist/array.js"],
            "./null": null,
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json exports @view-server/react/array: add intentional public specifier approval or remove the export.",
      "packages/react/package.json export ./array points at ./dist/array.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./array has no types target.",
      "packages/react/package.json exports @view-server/react/null: add intentional public specifier approval or remove the export.",
      "packages/react/package.json export ./null has no import target.",
    ]);
  });

  it("reports package string exports through the same approval and source checks", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./internal": "./dist/internal.js",
            "./missing": "./dist/missing.js",
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json exports @view-server/react/internal: add intentional public specifier approval or remove the export.",
      "packages/react/package.json export ./internal points at ./dist/internal.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./internal has no types target.",
      "packages/react/package.json exports @view-server/react/missing: add intentional public specifier approval or remove the export.",
      "packages/react/package.json export ./missing points at ./dist/missing.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./missing has no types target.",
    ]);
  });

  it("reports root string package exports through the same target checks", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: "./dist/index.js",
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual(["packages/react/package.json export . has no types target."]);
  });

  it("reports root fallback array package exports through the same target checks", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: ["./dist/live-query-state.js"],
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export . points at ./dist/live-query-state.js without a matching packed src entrypoint.",
      "packages/react/package.json export . has no types target.",
    ]);
  });

  it("reports types-only fallback array exports without runtime import targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": [
              {
                types: "./dist/testing.d.ts",
              },
            ],
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual(["packages/react/package.json export ./testing has no import target."]);
  });

  it("does not allow nested types import conditions to satisfy runtime import targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": {
              types: {
                import: "./dist/testing.d.ts",
              },
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual(["packages/react/package.json export ./testing has no import target."]);
  });

  it("reports package exports that are not approved public specifiers", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./internal": {
              import: "./dist/internal.js",
              types: "./dist/internal.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json exports @view-server/react/internal: add intentional public specifier approval or remove the export.",
      "packages/react/package.json export ./internal points at ./dist/internal.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./internal points at ./dist/internal.d.ts without a matching packed src entrypoint.",
    ]);
  });

  it("reports package exports from nameless manifests using the package directory label", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          exports: {
            ".": {
              import: "./dist/index.js",
              types: "./dist/index.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json exports packages/react: add intentional public specifier approval or remove the export.",
    ]);
  });

  it("reports package exports without import targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": {
              types: "./dist/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual(["packages/react/package.json export ./testing has no import target."]);
  });

  it("reports package exports without types targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": {
              import: "./dist/testing.js",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual(["packages/react/package.json export ./testing has no types target."]);
  });

  it("reports package exports without matching source entrypoints", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": {
              import: "./dist/missing.js",
              types: "./dist/missing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing points at ./dist/missing.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./testing points at ./dist/missing.d.ts without a matching packed src entrypoint.",
    ]);
  });

  it("reports unpacked import and types fallback array targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": {
              import: ["./dist/live-query-state.js"],
              types: ["./dist/live-query-state.d.ts"],
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing points at ./dist/live-query-state.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./testing points at ./dist/live-query-state.d.ts without a matching packed src entrypoint.",
    ]);
  });

  it("reports unpacked conditional package export targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": {
              browser: {
                default: "./dist/live-query-state.js",
              },
              default: "./dist/internal.js",
              import: "./dist/testing.js",
              node: ["./dist/internal.js"],
              types: "./dist/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing condition browser.default points at ./dist/live-query-state.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./testing condition default points at ./dist/internal.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./testing condition node[0] points at ./dist/internal.js without a matching packed src entrypoint.",
    ]);
  });

  it("reports package exports without a Vite+ pack config", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/client",
          exports: {
            ".": {
              import: "./dist/index.js",
              types: "./dist/index.d.ts",
            },
          },
        }),
        packageDirectoryName: "missing-package",
      }),
    ).toStrictEqual([
      "packages/missing-package/package.json export . points at ./dist/index.js without a matching packed src entrypoint.",
      "packages/missing-package/package.json export . points at ./dist/index.d.ts without a matching packed src entrypoint.",
    ]);
  });

  it("reports package exports with import targets outside dist entrypoints", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": {
              import: "./generated/testing.js",
              types: "./generated/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing points at ./generated/testing.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./testing points at ./generated/testing.d.ts without a matching packed src entrypoint.",
    ]);
  });

  it("reports package exports with traversal in dist entrypoint targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": {
              import: "./dist/../src/testing.js",
              types: "./dist/../src/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing points at ./dist/../src/testing.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./testing points at ./dist/../src/testing.d.ts without a matching packed src entrypoint.",
    ]);
  });

  it("reports package exports that target unpacked source entrypoints", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": {
              import: "./dist/live-query-state.js",
              types: "./dist/live-query-state.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing points at ./dist/live-query-state.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./testing points at ./dist/live-query-state.d.ts without a matching packed src entrypoint.",
    ]);
  });

  it("does not report mismatch noise when the import target is unpacked", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": {
              import: "./dist/live-query-state.js",
              types: "./dist/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing points at ./dist/live-query-state.js without a matching packed src entrypoint.",
    ]);
  });

  it("reports package exports with declaration targets that do not match import targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": {
              import: "./dist/testing.js",
              types: "./dist/index.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing types target ./dist/index.d.ts does not match import target ./dist/testing.js.",
    ]);
  });

  it("reports package exports with runtime condition targets that do not match import targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": {
              browser: "./dist/index.js",
              import: "./dist/testing.js",
              types: "./dist/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing condition browser target ./dist/index.js does not match import target ./dist/testing.js.",
    ]);
  });

  it("reports package exports with versioned declaration targets that do not match import targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@view-server/react",
          exports: {
            "./testing": {
              import: "./dist/testing.js",
              types: "./dist/testing.d.ts",
              "types@>=5.2": "./dist/index.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing condition types@>=5.2 target ./dist/index.d.ts does not match import target ./dist/testing.js.",
    ]);
  });

  it("reports stale approved public package export specifiers", () => {
    expect(
      staleApprovedPackageExportViolations({
        approvedSpecifiers: new Set(["@view-server/client", "@view-server/client/missing"]),
        exportedSpecifiers: new Set(["@view-server/client"]),
      }),
    ).toStrictEqual([
      "@view-server/client/missing is approved as public but is not exported by any package.json.",
    ]);
  });

  it("formats and throws package export violation summaries", () => {
    const violations = [
      "packages/react/package.json exports @view-server/react/internal: remove it.",
    ];

    expect(packageExportViolationMessage(violations)).toStrictEqual(
      [
        "Package public export violations found.",
        "- packages/react/package.json exports @view-server/react/internal: remove it.",
      ].join("\n"),
    );
    expect(() => assertNoPackageExportViolations(violations)).toThrowError(
      "Package public export violations found.",
    );
    expect(assertNoPackageExportViolations([])).toStrictEqual(undefined);
  });

  it("keeps the current repository free of package export violations", () => {
    expect(collectPackageExportViolations()).toStrictEqual([]);
  });

  it("ignores import-like text in comments", () => {
    expect(
      sourceWithoutComments(
        [
          'import { client } from "@view-server/client";',
          '// import { runtime } from "@view-server/runtime";',
          '/* import { server } from "@view-server/server"; */',
          'const example = "import from comment-like string";',
        ].join("\n"),
      ),
    ).toStrictEqual(
      [
        'import { client } from "@view-server/client";',
        "",
        "",
        'const example = "import from comment-like string";',
      ].join("\n"),
    );
  });

  it("keeps regex literals while stripping comments", () => {
    expect(
      sourceWithoutComments(
        [
          'const value = /\\\\//.test(path) && require("@view-server/runtime");',
          'const unfinished = /unterminated',
          '// require("@view-server/server");',
        ].join("\n"),
      ),
    ).toStrictEqual(
      [
        'const value = /\\\\//.test(path) && require("@view-server/runtime");',
        "const unfinished = /unterminated",
        "",
      ].join("\n"),
    );
    expect(sourceWithoutComments("const unfinished = /unterminated")).toStrictEqual(
      "const unfinished = /unterminated",
    );
  });

  it("does not treat import-like text inside strings as imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          'const message = "Do not import from \\"@view-server/runtime\\"";',
          "const docs = `import { server } from \"@view-server/server\"`;",
          'import { client } from "@view-server/client";',
          "const runtime = import(`@view-server/runtime`);",
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/client", "@view-server/runtime"]);
  });

  it("does not treat import-like JSX text as imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return <p>Install from \"@view-server/runtime\" and import from \"@view-server/server\".</p>;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("does not treat import-like JSX fragment text as imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return <>Install from \"@view-server/runtime\" and import from \"@view-server/server\".</>;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("does not treat import-like JSX text after logical operators as imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return condition && <p>Install from \"@view-server/runtime\".</p>;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return fallback || <>Install from \"@view-server/server\".</>;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("does not treat import-like JSX text inside underscore or dollar components as imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return <_Panel>Install from \"@view-server/runtime\".</_Panel>;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return <$Panel>Install from \"@view-server/server\".</$Panel>;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("does not treat import-like JSX text after nested self-closing children as imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return <Panel><Icon />Install from \"@view-server/runtime\".</Panel>;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("does not strip code after JSX text that looks like a block comment", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return <p>/*</p>;",
          "}",
          'const runtime = require("@view-server/runtime");',
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/runtime"]);
  });

  it("does not strip code after JSX text that looks like a line comment", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return <p>//</p>;",
          "}",
          'const runtime = require("@view-server/runtime");',
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/runtime"]);
  });

  it("detects imports inside JSX expressions", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function Loader() {",
          "  return <Panel>{import(\"@view-server/runtime\")}</Panel>;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/runtime"]);
  });

  it("detects imports inside self-closing JSX expressions", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function Loader() {",
          "  return <Panel value={import(\"@view-server/runtime\")} />;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/runtime"]);
  });

  it("handles unfinished JSX tag expressions conservatively", () => {
    expect(importSpecifiersFromSource("export const node = <Panel value={import(")).toStrictEqual(
      [],
    );
  });

  it("handles unfinished JSX tags conservatively", () => {
    expect(importSpecifiersFromSource("export const node = <Panel")).toStrictEqual([]);
  });

  it("handles unfinished JSX child expressions conservatively", () => {
    expect(importSpecifiersFromSource("export const node = <Panel>{import(")).toStrictEqual([]);
  });

  it("handles unfinished JSX roots conservatively", () => {
    expect(importSpecifiersFromSource("export const node = <Panel>")).toStrictEqual([]);
  });

  it("detects imports inside template literal expressions", () => {
    expect(
      importSpecifiersFromSource(
        [
          "const text = `plain import { server } from \"@view-server/server\"`;",
          "const runtime = `${await import(\"@view-server/runtime\")}`;",
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/runtime"]);
  });

  it("handles unfinished template literal expressions conservatively", () => {
    expect(importSpecifiersFromSource("const text = `${await import(")).toStrictEqual([]);
  });

  it("handles unfinished plain template literals conservatively", () => {
    expect(importSpecifiersFromSource("const text = `unterminated")).toStrictEqual([]);
  });

  it("ignores unterminated quoted import specifiers", () => {
    expect(importSpecifiersFromSource('const broken = import("@view-server/runtime')).toStrictEqual(
      [],
    );
  });

  it("ignores from keywords that are not followed by quoted specifiers", () => {
    expect(importSpecifiersFromSource("from notAString")).toStrictEqual([]);
  });
});
