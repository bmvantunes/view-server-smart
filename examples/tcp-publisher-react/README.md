# TCP Publisher React Example

TanStack Start app showing an external TCP publisher feeding View Server.

Run:

```bash
vp run @view-server/example-tcp-publisher-react#runtime
vp run @view-server/example-tcp-publisher-react#publisher
vp run @view-server/example-tcp-publisher-react#publisher:invalid
vp run @view-server/example-tcp-publisher-react#dev
```

This example demonstrates:

- `tcpPublishPort` runtime ingress.
- External process publishing one row per second.
- Schema-safe TCP publish commands before runtime mutation.
- Typed schema rejection for invalid TCP rows.
- React receiving updates through normal WebSocket `useLiveQuery`.
