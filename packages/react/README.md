# @effect-view-server/react

Production code should import from `@effect-view-server/react`. That entry depends only on
`@effect-view-server/client`, `@effect-view-server/config`, Effect, and React.

Tests can import `createInMemoryViewServerReact` from `@effect-view-server/react/testing`.
That testing subpath intentionally uses `@effect-view-server/in-memory` as a package
devDependency in this repository and an optional peer for external consumers. It must
be created from the same `createViewServerReact(...)` binding object used by the app
hooks, so the test provider and hook contexts cannot drift apart.
