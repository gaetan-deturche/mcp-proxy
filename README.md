# mcp-aggregator-proxy

A tiny, dependency-free **MCP aggregator** written in Go. An MCP client (e.g. Claude Code) reaches it over **stdio** (client spawns it) or **Streamable HTTP** (client connects to a persistent listener — recommended, see below); it connects out to several downstream MCP servers — over **Streamable HTTP** or **stdio** — merges their tools under a `<server>__` prefix, and forwards `tools/call` to the right one.

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

Two transport modes.

### stdio (default) — client spawns the proxy

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

Simple, but the proxy's lifetime is tied to the client-owned stdin pipe: when the client recycles its MCP connection it closes that pipe, the proxy hits EOF and exits, and if the client fails to re-spawn (observed mid-session) every tool disappears until a full restart.

### HTTP (recommended) — client connects to a persistent proxy

Run the proxy once as a background listener and have the client *connect* instead of spawn, so a connection recycle is just a reconnect — downstream connections and OAuth tokens stay warm, and it survives client/session restarts.

```sh
mcp-proxy.exe -http 127.0.0.1:6390     # serves Streamable HTTP at /mcp
```

```json
{
  "mcpServers": {
    "mcp-proxy": {
      "type": "http",
      "url": "http://127.0.0.1:6390/mcp"
    }
  }
}
```

`POST /mcp` handles JSON-RPC requests; `GET /mcp` is the SSE stream that carries `notifications/tools/list_changed` (so `reload` self-healing still works); `GET /health` returns `ok`.

**Autostart at logon (Windows, no admin):** `start-proxy-hidden.vbs` launches `mcp-proxy.exe` sitting next to it, with no console window (it self-locates via its own script dir, so keep the two together). Two ways to run it at logon — use **one**, not both (a second instance can't bind the port and exits):

- **Task Scheduler:** run `register-startup-task.bat` from an interactive shell — it registers a per-user ONLOGON task pointing at the VBS (paths resolved from `%~dp0`, nothing hardcoded) and starts the proxy immediately. Note: creating the task may be blocked by EDR/policy in non-interactive contexts.
- **Startup folder:** drop a *shortcut* to `start-proxy-hidden.vbs` in `%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\`. Use a shortcut, not a copy — the VBS resolves `mcp-proxy.exe` relative to its own location, so a copy in the Startup folder would look for the exe there.

Tools then appear as `mcp__mcp-proxy__<server>__<tool>` in either mode.

## Notes

- **Windows browser launch:** OAuth opens the URL via `rundll32 url.dll,FileProtocolHandler` — *not* `cmd /c start`, which would split the URL at the first `&` and break the redirect.
- A running process keeps the binary image it was spawned with, so after rebuilding, restart the client session (or spawn a fresh proxy) to pick up the new build — compare `version` across sessions to tell which is stale.
