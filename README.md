# mcp-aggregator-proxy

A tiny, dependency-free **stdio MCP aggregator** written in Go. An MCP client (e.g. Claude Code) spawns it once over stdio; it connects out to several downstream MCP servers — over **Streamable HTTP** or **stdio** — merges their tools under a `<server>__` prefix, and forwards `tools/call` to the right one.

## Why

MCP clients freeze their tool catalog at connection time and give up on any server that's down at startup. Because the proxy is spawned over stdio it is **always up**, so:

- Downstreams that are offline at startup (e.g. an IDE not launched yet) can be attached **later** via the `reload` tool — it re-probes them and emits `notifications/tools/list_changed`, and the client hot-attaches the tools **without a restart**.
- One stable endpoint fronts many servers; one server being down never breaks the others (graceful degradation).

## Build

Requires Go (stdlib only — no modules to fetch, compiles offline). Stamp the build so the `version` tool can identify the running binary:

```sh
go build -ldflags "-X main.buildVersion=$(date +%Y%m%d-%H%M%S)" -o mcp-proxy.exe .
```

(`buildVersion` defaults to `dev` if unstamped.) Self-test OAuth discovery + dynamic registration for a resource URL without a browser:

```sh
mcp-proxy.exe -oauth-test https://mcp.sentry.dev/mcp
```

## Configure

Copy `downstreams.example.json` to `downstreams.json` (gitignored — it holds secrets) and edit. Each entry:

| field | applies to | meaning |
|---|---|---|
| `name` | all | unique; becomes the `<name>__` tool prefix |
| `transport` | all | `http` (Streamable HTTP) or `stdio` |
| `url` | http | endpoint URL |
| `command` / `args` / `env` | stdio | process to spawn |
| `headers` | http | extra HTTP headers, e.g. `{"Authorization":"Bearer <token>"}` |
| `oauth` | http | `true` → obtain/attach a bearer token via the OAuth flow |
| `disabled` | all | skip this entry |

Top-level `callTimeoutSeconds` bounds forwarded `tools/call`.

By default the proxy reads `downstreams.json` next to the executable; override with `-config <path>`. Logs go to stderr and `mcp-proxy.log` (never stdout — that's the protocol channel); override with `-log <path>`.

## Tools the proxy exposes

Besides the prefixed downstream tools (`<server>__<tool>`):

- **`version`** — this process's build stamp, PID, start time, binary path (handy to confirm which build a session is running).
- **`reload`** — re-read `downstreams.json` **and** re-probe all downstreams; always emits `list_changed` so the client re-fetches. Call after launching an IDE or editing config.
- **`status`** — per-downstream UP/DOWN + tool counts.
- **`add_server` / `remove_server`** — add/remove a downstream dynamically (persists to `downstreams.json`, reconciles live).
- **`authenticate {name}`** — run the OAuth browser sign-in for an `oauth:true` downstream.

## OAuth

Full client (`oauth.go`): RFC 9728 protected-resource + RFC 8414 authorization-server metadata discovery, RFC 7591 dynamic client registration, authorization-code + PKCE (S256) via a one-time loopback-redirect browser sign-in, refresh-token renewal. Tokens persist in `oauth-tokens.json` (gitignored) and auto-refresh before expiry. For servers that accept a static token instead, use `headers` with `Authorization: Bearer …`.

## Wire into Claude Code

In `~/.claude.json`, a single stdio entry replaces the individual server entries:

```json
{
  "mcpServers": {
    "mcp-proxy": {
      "type": "stdio",
      "command": "C:\\path\\to\\mcp-proxy.exe"
    }
  }
}
```

Tools then appear as `mcp__mcp-proxy__<server>__<tool>`.

## Notes

- **Windows browser launch:** OAuth opens the URL via `rundll32 url.dll,FileProtocolHandler` — *not* `cmd /c start`, which would split the URL at the first `&` and break the redirect.
- A running process keeps the binary image it was spawned with, so after rebuilding, restart the client session (or spawn a fresh proxy) to pick up the new build — compare `version` across sessions to tell which is stale.
