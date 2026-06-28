# In-Memory React Example

TanStack Start app using `createInMemoryViewServerReact(viewServerReact)`.

Run:

```bash
vp run @view-server/example-in-memory-react#test
vp run @view-server/example-in-memory-react#dev
```

This example demonstrates:

- `useLiveQuery` with explicit `select`, filters, sorting, and grouped aggregates.
- `useViewServerHealthSummary`.
- Publishing rows through the in-memory client while the UI uses the same hook as
  production.
