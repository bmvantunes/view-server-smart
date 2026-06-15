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

- Use `.agents/skills/effect-ts/SKILL.md` for all Effect-related implementation and review work. The repository tracks `.repos/effect` as a submodule for source-level Effect checks.
- Use `.agents/skills/vitest/SKILL.md` for tests, coverage, fixtures, and especially type tests. For type-heavy packages, read `.agents/skills/vitest/references/advanced-type-testing.md` before reviewing or writing tests.
- Use `.agents/skills/vite/SKILL.md` for `vite.config.ts`, build, library packaging, and Vite/Vite+ integration work.
- Use `.agents/skills/improve-codebase-architecture/SKILL.md` for architecture reviews. Use its vocabulary: Module, Interface, Implementation, Depth, Seam, Adapter, Leverage, Locality.
- Use `.agents/skills/grill-me/SKILL.md` when a design decision is ambiguous enough that implementation should pause and the decision tree should be clarified first.
- Implementation agents should not skip Effect skill research for services, layers, streams, resource management, typed errors, or tests.
- Type-level regressions are product bugs. Prefer dedicated `.test-d.ts` files or explicit `expectTypeOf` coverage for public generic APIs, plus negative `@ts-expect-error` assertions for rejected calls.
- Review agents should explicitly state whether findings are blocking or non-blocking.

## Project Non-Negotiables

- Performance and type safety are product features. Do not trade either away silently.
- Public APIs must remain fully typed end-to-end. If a user can pass an invalid topic, field, aggregate, mapping result, or query shape without a type error, that is a bug.
- Runtime behavior must match the public types. Do not make type-level promises that the implementation only satisfies through casts.
- Do not add compatibility layers for APIs nobody uses yet. This project is still allowed to break its own API to make the right design.
- Do not mention or preserve old implementation history in product docs or new APIs unless explicitly requested.

## Effect Rules

- Use Effect v4 beta APIs. When in doubt, check `.repos/effect` first.
- Use `Effect.gen` for workflows.
- Use named `Effect.fn` for reusable operations and important runtime paths so spans/diagnostics are useful.
- Prefer plain `return value` for plain values inside `Effect.gen`; prefer `return yield* effect` when the final value comes from an Effect.
- Do not use `return yield* Effect.succeed(value)` as a style pattern. It is only acceptable as a narrow inference workaround with a comment explaining why.
- Use `Effect.scoped`, `Scope`, `Layer`, and finalizers for resource ownership. Do not hand-roll lifecycle cleanup when Effect has a scoped primitive.
- Only call `Effect.run*` at runtime edges, test edges, or tiny adapter boundaries. Do not normalize `runSync` inside reusable modules.
- Use typed errors. Do not throw for expected domain/runtime/transport failures.
- Use schemas at external boundaries: RPC, HTTP, Kafka, TCP, browser storage, and package-public decoding.
- Use `Clock` for time. Do not use `Date`, `new Date()`, or `Date.now()` in implementation code.
- Strict Effect LSP must pass before a task is considered done.

## Type Safety Rules

- Do not use `as any`, `as unknown`, or `as never`.
- Avoid casts in general. Do not add `as` casts to implementation or public API code unless the user explicitly approves the exact seam. If a cast is unavoidable at a boundary, keep it in the smallest adapter seam and add tests proving the runtime value actually satisfies the claimed type.
- Internal `as const` is allowed and often useful. Public APIs must not require users to write `as const` to get correct topic, select, where, orderBy, aggregate, or hook result inference.
- Do not use `object` / `Record<string, unknown>` internally when the topic row type is known. Keep generics alive through the implementation.
- Public generic APIs require type tests for valid inference and invalid usage.
- Mapping functions must type both input and output. Extra returned fields must fail.
- Query typing must reject invalid select fields, where fields/operators, order fields, group fields, aggregate fields, and aggregate aliases.
- `select` is required for raw queries. `aggregates` is required for grouped queries. Do not allow implicit “all columns”.
- Values that always exist at runtime should not be optional in public types. For example, `totalRows` should be present, with `0` as the empty value.
- Count-like aggregates should not lose precision. Prefer `bigint` for counts/count-distinct unless a selected design explicitly says otherwise.

## React Rules

- React components in tests and examples should be `function Component() { ... }`, not arrow component constants.
- Production React must be transport-agnostic. Hooks consume a provider/client seam; hooks must not know whether the backend is in-memory, RPC, WebSocket, HTTP, or something else.
- `@view-server/react` production code must not depend on `@view-server/in-memory`.
- In-memory behavior must use the same shared runtime core and Column Live View Engine as production. Only transport/ingress Adapters may differ.
- In-memory React helpers belong under `@view-server/react/testing`.
- Provider ownership must be explicit. Generic providers receiving caller-owned clients must not close those clients. Testing/in-memory providers that create clients may own cleanup.
- Do not use `useEffect` for Effect runtime integration when `@effect/atom-react` / Effect reactivity primitives are the right fit.
- Do not use `act`, `flushSync`, manual `react-dom/client`, Testing Library, `getByTestId`, or `data-testid`.
- Browser tests must use Vitest Browser Mode and `vitest-browser-react`.
- Prefer role/text locators and exact or anchored assertions. Avoid substring assertions that can pass with stale or extra UI.

## Test Rules

- Use `@effect/vitest` for tests. Do not import directly from `vitest` unless there is no equivalent and the exception is documented.
- Use `expect` / `expectTypeOf` assertions, not `assert.*`. This intentionally overrides the Effect skill preference so server, engine, React, and type tests use one assertion style.
- Use `toStrictEqual`, not `toEqual`, for structural equality. Exact shape matters for public contracts and wire/runtime events.
- Prefer full-object `toStrictEqual` assertions when the value under test is already an object and its shape is stable. Do not manufacture wrapper objects just to assert unrelated scalar values; use direct scalar assertions for scalars.
- Use the Vitest skill before changing type tests, browser tests, coverage, or Vite/Vite+ config.
- Tests should be dumb and explicit. Avoid branching assertions such as `if (_tag === ...) { expect(...) }`.
- Do not use `try` / `catch` / `finally` in tests. Keep tests linear; put explicit cleanup at the end or use fixtures/helpers.
- Prefer e2e-style tests for the column live view engine: publish rows, subscribe, assert snapshot, publish/patch/delete, assert deltas and convergence.
- Do not add narrow implementation unit tests that would block future SIMD/Rust/native rewrites unless the behavior is a stable public contract.
- Type tests must cover both good and bad examples, including `@ts-expect-error` negative cases.
- React browser coverage should run in Chromium, Firefox, and WebKit when React behavior changes.
- All changed package tests must pass with 100% statement, branch, function, and line coverage before a task is considered done.
- Do not remove coverage requirements to make CI pass.
- Do not add coverage ignore comments. `c8 ignore`, `v8 ignore`, and `istanbul ignore` are forbidden; add tests or refactor unreachable code instead.

## Runtime And Transport Rules

- Preserve typed errors over RPC/transport boundaries. `InvalidTopic`, `InvalidRow`, `InvalidQuery`, `UnsupportedQuery`, `BackpressureExceeded`, and `SubscriptionClosed` must not be collapsed into generic `TransportError`.
- `TransportError` is only for actual transport failures.
- Do not put health refreshes or health RPC calls on the live-event hot path.
- Health should be cached/cadenced unless a caller explicitly asks for a fresh runtime health read.
- Server health RPCs should read from the runtime health source, not from a possibly stale client atom.
- Long-lived streams must have clear finalizers. If a consumer stops reading, unmounts, disconnects, or closes a client, server subscriptions must be released.
- Client `close` must close active subscriptions/scopes before or while disposing the transport runtime.
- The production wire protocol is Effect RPC over WebSocket using NDJSON serialization unless the user explicitly approves a different protocol.
- Do not switch production transport serialization to JSON, MessagePack, SSE, raw WebSocket frames, HTTP streams, or custom protocols without a dedicated benchmark and an explicit architecture decision.
- Do not use `Schema.Unknown` for typed row/event payloads over RPC unless tests prove JSON-unsafe values such as `bigint` and BigDecimal-like values roundtrip correctly.
- Keep protocol schemas in a neutral shared module when both client and server use them. Server packages must not import browser transport code through a client root export.
- Effect RPC is acceptable for the production slice, but firehose data-path overhead must remain measurable and swappable behind the same React hook/provider seam.

## Performance Rules

- Avoid O(n^2) hot paths unless benchmarked and justified.
- Prefer indexed lookup/maps over repeated scans when memory tradeoffs are acceptable.
- Benchmark write paths when adding read-path indexes, vectors, caches, or materialized state. A faster read path that destroys publish/patch/delete throughput is not a win.
- Do not clone or materialize per subscriber when work can be shared by topic/query/window/plan.
- Hot paths should avoid unnecessary allocations, schema decoding, object spreading, and health snapshots.
- Health counters should update cheaply and publish at a bounded cadence.
- Benchmarks must state whether they are localhost CPU/GC stress, browser stress, network-shaped, or production-like.
- `pnpm run bench:baseline:smoke` is the smoke performance-regression gate. It must compare fresh Vitest benchmark artifacts against `benchmarks/baselines/smoke.json`; use `pnpm run bench:baseline:smoke:update` only when an accepted performance change intentionally moves the baseline.
- `pnpm run bench:baseline:kafka-ingest` is the real Kafka ingest smoke gate. It starts Apache Kafka via `compose.yaml`, uses `@platformatic/kafka`, and compares JSON/protobuf ingest plus a 2k-message mixed burst against `benchmarks/baselines/kafka-ingest.json`.
- Do not run competing benchmark suites in parallel when comparing results.

## Package And Architecture Rules

- Keep Modules deep. If deleting a module makes complexity disappear, it is probably pass-through and should not exist.
- Use real Seams only where there are multiple Implementations or a clear near-term Adapter boundary.
- Production engine modules must not import `topicStoreReadModel` or `topicStoreRawQueryMetadata`. Route read-model/query behavior through Topic Store helper operations so current row-oriented storage internals and future Columnar Topic Store internals stay local to the Topic Store Module.
- Keep package direction clean:
  - config/contracts: public types and pure contracts.
  - column live view engine: query compilation, store, snapshots, deltas, subscriptions, engine health.
  - client: framework-neutral client contracts and remote client adapters.
  - server: server/runtime adapters.
  - react: React bindings only.
  - react/testing: React test helpers.
  - runtime-core: shared engine-backed runtime Module; owns runtime client, live client, health snapshots, and lifecycle.
  - in-memory: in-process Adapter over runtime-core for tests, demos, Storybook, and browser benchmarks.
  - runtime: production composition of runtime-core plus server and future Kafka/TCP/gRPC ingress Adapters.
- Do not make production packages depend on testing packages or in-memory implementations.
- Package export checks must cover approved root exports, approved subexports, and rejected deep/internal subpaths. A package seam is not enforced if `@view-server/package/src/...`, `@view-server/package/dist/...`, or unapproved nested subexports can resolve.

## Common Blockers

These issues block merge until fixed or explicitly accepted by the user:

- Typed runtime/query errors are converted into `TransportError`.
- Production Effect RPC WebSocket uses JSON or MessagePack instead of NDJSON without an explicit decision.
- RPC wire schemas erase configured topic schemas and fail or silently corrupt `bigint` / BigDecimal-like values.
- A stream or subscription can leak when the consumer stops reading without calling an explicit close method.
- A client/server close path does not release active subscriptions.
- Server health reads a cached client atom instead of fresh runtime health.
- Health refresh or health RPC is called per live event.
- Browser tests import directly from `vitest`, Testing Library, or use `act`, `flushSync`, `getByTestId`, or `data-testid`.
- Tests use `assert.*` instead of `expect` / `expectTypeOf`.
- Tests use `toEqual` instead of `toStrictEqual`.
- Tests use `try`, `catch`, or `finally` blocks instead of linear assertions and explicit cleanup.
- Tests use substring assertions where stale/extra rows could still pass.
- Implementation or tests add coverage ignore comments such as `c8 ignore`, `v8 ignore`, or `istanbul ignore`.
- Implementation adds casts, especially broad casts (`as any`, `as unknown`, `as never`), or hides type erasure behind casts.
- Public APIs require consumers to add `as const` for correct inference.
- Production React depends on in-memory runtime code.
- Production runtime imports the in-memory Adapter instead of runtime-core.
- Browser/remote client exposes publish, publishMany, reset, or other admin mutation RPCs.
- A package imports through a root export that also pulls in the wrong runtime/platform adapter.
- An unapproved deep import or nested subexport resolves across a package seam.
- Public API type tests are missing for new generic inference or rejection behavior.
- Runtime fields that are always present are modeled as optional.
- A benchmark comparison runs multiple candidates concurrently or omits process/child failure checks.

## Before Saying Done

0 - implement the feature request and review [plans/](plans/) before merging.
1 - run effect, vitest, and improve-codebase-architecture reviews with three separate agents.
2 - wait for all review agents to report.
3 - if any issues are found, fix them, commit/push, and return to step 1 until all agents agree there are no problems.
4 - open a Pull Request on GitHub.
5 - wait for Codex Cloud review.
6 - if Codex Cloud reports issues, fix them, commit/push, and return to step 1.
7 - once all reviews are clean, merge the Pull Request and proceed to the next request.
8 - if you changed a public API, add Vitest type tests for inference and rejection behavior (including `@ts-expect-error` cases where applicable).

- Run the relevant focused tests first, then the package-level tests.
- Run `vp check`.
- Run strict Effect diagnostics.
- Run type tests for changed public APIs.
- Run browser tests across Chromium, Firefox, and WebKit for React changes.
- Run package export checks when package exports change.
- Scan for forbidden patterns:
  - direct `vitest` imports
  - Testing Library imports
  - `assert.`
  - `.toEqual(`
  - `act(` / `flushSync`
  - `getByTestId` / `data-testid`
  - unapproved `as` casts, especially `as any` / `as unknown` / `as never`
  - `new Date(` / `Date.now(`
- `return yield* Effect.succeed`
  - `c8 ignore` / `v8 ignore` / `istanbul ignore`
  - removed browser/admin RPC symbols such as `ViewServer.Publish`, `ViewServer.PublishMany`, `ViewServer.Reset`, or `ViewServerPublish`
- If reviewers find blockers, fix them and run the review loop again. Do not open or merge a PR while known blockers remain.
