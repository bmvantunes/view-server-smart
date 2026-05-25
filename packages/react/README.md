# @view-server/react

Production code should import from `@view-server/react`. That entry depends only on
`@view-server/client`, `@view-server/config`, Effect, and React.

Tests can import `createInMemoryViewServerReact` from `@view-server/react/testing`.
That testing subpath intentionally uses `@view-server/in-memory` as a package
devDependency and bundles the in-memory runtime into the testing entry. It must be
created from the same `createViewServerReact(...)` binding object used by the app
hooks, so the test provider and hook contexts cannot drift apart.
