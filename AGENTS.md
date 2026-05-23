<!--VITE PLUS START-->

# Using Vite+, the Unified Toolchain for the Web

This project is using Vite+, a unified toolchain built on top of Vite, Rolldown, Vitest, tsdown, Oxlint, Oxfmt, and Vite Task. Vite+ wraps runtime management, package management, and frontend tooling in a single global CLI called `vp`. Vite+ is distinct from Vite, and it invokes Vite through `vp dev` and `vp build`. Run `vp help` to print a list of commands and `vp <command> --help` for information about a specific command.

Docs are local at `node_modules/vite-plus/docs` or online at https://viteplus.dev/guide/.

## Review Checklist

- [ ] Run `vp install` after pulling remote changes and before getting started.
- [ ] Run `vp check` and `vp test` to format, lint, type check and test changes.
- [ ] Check if there are `vite.config.ts` tasks or `package.json` scripts necessary for validation, run via `vp run <script>`.
- [ ] If setup, runtime, or package-manager behavior looks wrong, run `vp env doctor` and include its output when asking for help.

<!--VITE PLUS END-->

## Agent Skills

- Use `.agents/skills/effect-ts/SKILL.md` for all Effect-related implementation and review work. The repository has `.repos/effect` available for source-level checks.
- Use `.agents/skills/vitest/SKILL.md` for tests, coverage, fixtures, and especially type tests. For type-heavy packages, read `.agents/skills/vitest/references/advanced-type-testing.md` before reviewing or writing tests.
- Use `.agents/skills/vite/SKILL.md` for `vite.config.ts`, build, library packaging, and Vite/Vite+ integration work.
- Use `.agents/skills/improve-codebase-architecture/SKILL.md` for architecture reviews. Use its vocabulary: Module, Interface, Implementation, Depth, Seam, Adapter, Leverage, Locality.
- Use `.agents/skills/grill-me/SKILL.md` when a design decision is ambiguous enough that implementation should pause and the decision tree should be clarified first.
- Implementation agents should not skip Effect skill research for services, layers, streams, resource management, typed errors, or tests.
- Type-level regressions are product bugs. Prefer dedicated `.test-d.ts` files or explicit `expectTypeOf` / `assertType` coverage for public generic APIs, plus negative `@ts-expect-error` assertions for rejected calls.
- Review agents should explicitly state whether findings are blocking or non-blocking.
