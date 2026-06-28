# SSR React Example

TanStack Start app showing the current SSR-safe contract.

Run:

```bash
vp run @view-server/example-ssr-react#build
vp run @view-server/example-ssr-react#dev
```

The shell runs without a View Server runtime. To try the optional live panel, start a compatible
runtime on `ws://127.0.0.1:8080/rpc`, then click `Connect live data` in the browser.

This example demonstrates:

- The page shell can be server-rendered.
- The View Server WebSocket provider is optional and mounted only in the browser.
- Live query hooks stay behind the browser-only provider boundary.
