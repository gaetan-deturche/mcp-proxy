// mcp-aggregator-proxy: a lightweight stdio MCP server that fronts several
// downstream MCP servers (Streamable HTTP or stdio), merges their tools under a
// "<server>__" prefix, and forwards tools/call to the right one.
//
// Why it exists: Claude Code freezes its tool catalog at session start and gives
// up on any MCP server that is down at that moment. This proxy is spawned by
// Claude over stdio, so it is ALWAYS up; downstreams that are offline at startup
// (e.g. an IDE not launched yet) are attached later via the `reload` tool, which
// re-probes them and emits notifications/tools/list_changed so Claude refreshes
// live — no session restart needed.
//
// Transparency: tool objects pass through verbatim (schema/annotations intact);
// only the "name" field is rewritten with the prefix. tools/call results are
// forwarded raw. The only added surface is the `reload` and `status` tools.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	proxyName       = "mcp-aggregator-proxy"
	proxyVersion    = "0.2.0"
	protocolVersion = "2025-03-26"
)

// buildVersion is stamped at compile time via `-ldflags "-X main.buildVersion=..."`
// (a per-build timestamp). It lets you confirm, via the `version` tool, exactly
// which binary a given Claude session's proxy process is running — different
// sessions can run different builds because a live process keeps the image it
// was spawned with even after the on-disk binary is replaced.
var (
	buildVersion = "dev"
	startedAt    = time.Now()
)

// ---------- JSON-RPC ----------

type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type rpcMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

func mustJSON(v interface{}) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage("null")
	}
	return b
}

func parseID(raw json.RawMessage) (int64, bool) {
	var n int64
	if json.Unmarshal(raw, &n) == nil {
		return n, true
	}
	return 0, false
}

// ---------- config ----------

type dsConfig struct {
	Name      string            `json:"name"`
	Transport string            `json:"transport"` // "http" (Streamable HTTP) | "stdio"
	URL       string            `json:"url,omitempty"`
	Command   string            `json:"command,omitempty"`
	Args      []string          `json:"args,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"` // extra HTTP headers (e.g. {"Authorization":"Bearer <token>"})
	OAuth     bool              `json:"oauth,omitempty"`   // http only: obtain/attach a bearer token via the OAuth flow
	Disabled  bool              `json:"disabled,omitempty"`
}

// dsSig is a stable signature of a downstream's connection-relevant config, used
// to decide on hot-reload whether an existing downstream can be reused as-is or
// must be torn down and recreated.
func dsSig(dc dsConfig) string {
	b, _ := json.Marshal(struct {
		T, U, C string
		A       []string
		E, H    map[string]string
		O       bool
	}{dc.Transport, dc.URL, dc.Command, dc.Args, dc.Env, dc.Headers, dc.OAuth})
	return string(b)
}

func newDownstream(dc dsConfig) downstream {
	if strings.EqualFold(dc.Transport, "stdio") {
		return newStdioDownstream(dc)
	}
	return newHTTPDownstream(dc)
}

type proxyConfig struct {
	CallTimeoutSeconds int        `json:"callTimeoutSeconds,omitempty"`
	Downstreams        []dsConfig `json:"downstreams"`
}

// ---------- downstream interface ----------

type downstream interface {
	Name() string
	Connect(ctx context.Context) error
	Request(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, *rpcError, error)
	Notify(ctx context.Context, method string, params json.RawMessage) error
	Connected() bool
	Close()
}

func initParams() json.RawMessage {
	return mustJSON(map[string]interface{}{
		"protocolVersion": protocolVersion,
		"capabilities":    map[string]interface{}{},
		"clientInfo":      map[string]string{"name": proxyName, "version": proxyVersion},
	})
}

// ---------- Streamable HTTP downstream ----------

type httpDownstream struct {
	cfg       dsConfig
	client    *http.Client
	mu        sync.Mutex
	sessionID string
	conn      bool
	idc       int64
}

func newHTTPDownstream(cfg dsConfig) *httpDownstream {
	return &httpDownstream{cfg: cfg, client: &http.Client{}}
}

func (h *httpDownstream) Name() string { return h.cfg.Name }

func (h *httpDownstream) Connected() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.conn
}

func (h *httpDownstream) Close() {}

func (h *httpDownstream) nextID() json.RawMessage {
	n := atomic.AddInt64(&h.idc, 1)
	return json.RawMessage(strconv.FormatInt(n, 10))
}

func (h *httpDownstream) Connect(ctx context.Context) error {
	h.mu.Lock()
	h.sessionID = ""
	h.conn = false
	h.mu.Unlock()
	_, rerr, err := h.Request(ctx, "initialize", initParams())
	if err != nil {
		return err
	}
	if rerr != nil {
		return fmt.Errorf("initialize error %d: %s", rerr.Code, rerr.Message)
	}
	h.mu.Lock()
	h.conn = true
	h.mu.Unlock()
	_ = h.Notify(ctx, "notifications/initialized", nil)
	return nil
}

func (h *httpDownstream) Request(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, *rpcError, error) {
	msg := rpcMessage{JSONRPC: "2.0", ID: h.nextID(), Method: method, Params: params}
	resp, err := h.post(ctx, msg, false)
	if err != nil {
		return nil, nil, err
	}
	if resp == nil {
		return nil, nil, errors.New("no response")
	}
	if resp.Error != nil {
		return nil, resp.Error, nil
	}
	return resp.Result, nil, nil
}

func (h *httpDownstream) Notify(ctx context.Context, method string, params json.RawMessage) error {
	msg := rpcMessage{JSONRPC: "2.0", Method: method, Params: params}
	_, err := h.post(ctx, msg, true)
	return err
}

func (h *httpDownstream) post(ctx context.Context, msg rpcMessage, isNotify bool) (*rpcMessage, error) {
	body, _ := json.Marshal(msg)
	req, err := http.NewRequestWithContext(ctx, "POST", h.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("MCP-Protocol-Version", protocolVersion)
	for k, v := range h.cfg.Headers {
		req.Header.Set(k, v)
	}
	if h.cfg.OAuth && oauthStore != nil {
		tok, err := oauthStore.accessToken(h.cfg.Name)
		if err != nil {
			return nil, fmt.Errorf("oauth: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	h.mu.Lock()
	sid := h.sessionID
	h.mu.Unlock()
	if sid != "" {
		req.Header.Set("Mcp-Session-Id", sid)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if s := resp.Header.Get("Mcp-Session-Id"); s != "" {
		h.mu.Lock()
		h.sessionID = s
		h.mu.Unlock()
	}
	if resp.StatusCode == http.StatusAccepted {
		return nil, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if isNotify {
		return nil, nil
	}
	if strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		return readSSEResponse(resp.Body)
	}
	var m rpcMessage
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

// readSSEResponse reads an SSE stream and returns the first JSON-RPC message
// carrying a result or error (the response to our single POSTed request).
func readSSEResponse(r io.Reader) (*rpcMessage, error) {
	br := bufio.NewReaderSize(r, 1<<20)
	var data strings.Builder
	flush := func() (*rpcMessage, bool) {
		if data.Len() == 0 {
			return nil, false
		}
		var m rpcMessage
		ok := json.Unmarshal([]byte(data.String()), &m) == nil && (m.Result != nil || m.Error != nil)
		data.Reset()
		if ok {
			return &m, true
		}
		return nil, false
	}
	for {
		line, err := br.ReadString('\n')
		if len(line) > 0 {
			line = strings.TrimRight(line, "\r\n")
			switch {
			case strings.HasPrefix(line, "data:"):
				data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
			case line == "":
				if m, ok := flush(); ok {
					return m, nil
				}
			}
		}
		if err != nil {
			if m, ok := flush(); ok {
				return m, nil
			}
			if err == io.EOF {
				return nil, errors.New("sse stream closed without a response")
			}
			return nil, err
		}
	}
}

// ---------- stdio downstream ----------

type stdioDownstream struct {
	cfg     dsConfig
	mu      sync.Mutex
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	pending map[int64]chan rpcMessage
	pmu     sync.Mutex
	writeMu sync.Mutex
	idc     int64
	conn    bool
	started bool
}

func newStdioDownstream(cfg dsConfig) *stdioDownstream {
	return &stdioDownstream{cfg: cfg, pending: map[int64]chan rpcMessage{}}
}

func (s *stdioDownstream) Name() string { return s.cfg.Name }

func (s *stdioDownstream) Connected() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn
}

func (s *stdioDownstream) Close() {
	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
	}
}

func (s *stdioDownstream) start() error {
	cmd := exec.Command(s.cfg.Command, s.cfg.Args...)
	env := os.Environ()
	for k, v := range s.cfg.Env {
		env = append(env, k+"="+v)
	}
	cmd.Env = env
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	s.mu.Lock()
	s.cmd = cmd
	s.stdin = stdin
	s.started = true
	s.mu.Unlock()
	go s.readLoop(stdout)
	return nil
}

func (s *stdioDownstream) readLoop(r io.Reader) {
	br := bufio.NewReaderSize(r, 1<<20)
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			var m rpcMessage
			if json.Unmarshal(line, &m) == nil && len(m.ID) > 0 {
				if id, ok := parseID(m.ID); ok {
					s.pmu.Lock()
					ch := s.pending[id]
					delete(s.pending, id)
					s.pmu.Unlock()
					if ch != nil {
						ch <- m
					}
				}
			}
		}
		if err != nil {
			s.mu.Lock()
			s.conn = false
			s.started = false
			s.mu.Unlock()
			return
		}
	}
}

func (s *stdioDownstream) Connect(ctx context.Context) error {
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if !started {
		if err := s.start(); err != nil {
			return err
		}
	}
	_, rerr, err := s.Request(ctx, "initialize", initParams())
	if err != nil {
		return err
	}
	if rerr != nil {
		return fmt.Errorf("initialize error %d: %s", rerr.Code, rerr.Message)
	}
	s.mu.Lock()
	s.conn = true
	s.mu.Unlock()
	_ = s.Notify(ctx, "notifications/initialized", nil)
	return nil
}

func (s *stdioDownstream) Request(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, *rpcError, error) {
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if !started {
		return nil, nil, errors.New("process not started")
	}
	n := atomic.AddInt64(&s.idc, 1)
	ch := make(chan rpcMessage, 1)
	s.pmu.Lock()
	s.pending[n] = ch
	s.pmu.Unlock()
	msg := rpcMessage{JSONRPC: "2.0", ID: json.RawMessage(strconv.FormatInt(n, 10)), Method: method, Params: params}
	b, _ := json.Marshal(msg)
	b = append(b, '\n')
	s.writeMu.Lock()
	_, werr := s.stdin.Write(b)
	s.writeMu.Unlock()
	if werr != nil {
		s.pmu.Lock()
		delete(s.pending, n)
		s.pmu.Unlock()
		return nil, nil, werr
	}
	select {
	case m := <-ch:
		if m.Error != nil {
			return nil, m.Error, nil
		}
		return m.Result, nil, nil
	case <-ctx.Done():
		s.pmu.Lock()
		delete(s.pending, n)
		s.pmu.Unlock()
		return nil, nil, ctx.Err()
	}
}

func (s *stdioDownstream) Notify(ctx context.Context, method string, params json.RawMessage) error {
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if !started {
		return errors.New("process not started")
	}
	msg := rpcMessage{JSONRPC: "2.0", Method: method, Params: params}
	b, _ := json.Marshal(msg)
	b = append(b, '\n')
	s.writeMu.Lock()
	_, err := s.stdin.Write(b)
	s.writeMu.Unlock()
	return err
}

// ---------- registry ----------

type route struct {
	ds   string
	orig string
}

type registry struct {
	mu          sync.RWMutex
	configPath  string
	desired     []dsConfig // mirror of the config file (incl. disabled), for rewrites
	dss         map[string]downstream
	sigs        map[string]string // name -> dsSig, to detect config changes on reload
	order       []string
	tools       []json.RawMessage // exposed tool objects (incl. management tools)
	routes      map[string]route  // exposedName -> downstream/originalName
	lastReload  time.Time
	callTimeout time.Duration
}

// applyConfig re-reads the config file from disk and reconciles the live
// downstream set: new entries are created, removed/disabled ones are closed, and
// entries whose connection config changed are recreated. This is what makes the
// downstream list hot-reloadable — edit downstreams.json (or use add_server /
// remove_server) then call reload, no proxy restart needed.
func (r *registry) applyConfig() error {
	b, err := os.ReadFile(r.configPath)
	if err != nil {
		return err
	}
	var cfg proxyConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return err
	}
	if cfg.CallTimeoutSeconds <= 0 {
		cfg.CallTimeoutSeconds = 120
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.desired = cfg.Downstreams
	r.callTimeout = time.Duration(cfg.CallTimeoutSeconds) * time.Second

	want := map[string]dsConfig{}
	var order []string
	for _, dc := range cfg.Downstreams {
		if dc.Disabled || dc.Name == "" {
			continue
		}
		t := strings.ToLower(dc.Transport)
		if t != "stdio" && t != "http" && t != "streamable" && t != "streamable-http" && t != "streamablehttp" {
			log.Printf("downstream %q: unknown transport %q, skipping", dc.Name, dc.Transport)
			continue
		}
		want[dc.Name] = dc
		order = append(order, dc.Name)
	}
	// Drop downstreams that disappeared or whose config changed.
	for name, ds := range r.dss {
		if dc, ok := want[name]; !ok || dsSig(dc) != r.sigs[name] {
			ds.Close()
			delete(r.dss, name)
			delete(r.sigs, name)
		}
	}
	// Create newly added (or changed-and-removed-above) downstreams.
	for _, name := range order {
		if _, ok := r.dss[name]; !ok {
			r.dss[name] = newDownstream(want[name])
			r.sigs[name] = dsSig(want[name])
		}
	}
	r.order = order
	return nil
}

// writeConfig persists the current desired config back to downstreams.json.
func (r *registry) writeConfig() error {
	r.mu.RLock()
	cfg := proxyConfig{CallTimeoutSeconds: int(r.callTimeout / time.Second), Downstreams: r.desired}
	path := r.configPath
	r.mu.RUnlock()
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0644)
}

// upsertConfig adds or replaces a downstream entry in the desired config.
func (r *registry) upsertConfig(dc dsConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.desired {
		if r.desired[i].Name == dc.Name {
			r.desired[i] = dc
			return
		}
	}
	r.desired = append(r.desired, dc)
}

// removeConfig deletes a downstream entry from the desired config; returns false
// if no such entry existed.
func (r *registry) removeConfig(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.desired[:0]
	found := false
	for _, d := range r.desired {
		if d.Name == name {
			found = true
			continue
		}
		out = append(out, d)
	}
	r.desired = append([]dsConfig(nil), out...)
	return found
}

func baseTools() []json.RawMessage {
	return []json.RawMessage{versionToolDef(), reloadToolDef(), statusToolDef(), addServerToolDef(), removeServerToolDef(), authenticateToolDef()}
}

func versionToolDef() json.RawMessage {
	return json.RawMessage(`{"name":"version","description":"Report THIS proxy process's build stamp, PID, start time and binary path. Use it to confirm which build a given Claude session's proxy is running — different sessions can run different binaries (a live process keeps its spawned image even after the .exe is rebuilt on disk).","inputSchema":{"type":"object","properties":{}}}`)
}

func sanitize(name string) string {
	var sb strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('_')
		}
	}
	return sb.String()
}

func reloadToolDef() json.RawMessage {
	return json.RawMessage(`{"name":"reload","description":"Re-read the proxy config file (downstreams.json) AND re-probe every downstream MCP server: pick up newly added/removed servers, reconnect ones that came online, drop ones that went offline, and refresh the tool list. Call this after editing the config or after starting/stopping an IDE so tools appear/disappear without restarting the Claude session.","inputSchema":{"type":"object","properties":{}}}`)
}

func statusToolDef() json.RawMessage {
	return json.RawMessage(`{"name":"status","description":"Report the connection status and tool count of each downstream MCP server fronted by this proxy.","inputSchema":{"type":"object","properties":{}}}`)
}

func addServerToolDef() json.RawMessage {
	return json.RawMessage(`{"name":"add_server","description":"Dynamically add (or replace) a downstream MCP server and connect to it immediately — no proxy restart. Persists to downstreams.json. For HTTP/Streamable servers pass url (and optional headers, e.g. for auth); for stdio servers pass command (and optional args/env).","inputSchema":{"type":"object","properties":{"name":{"type":"string","description":"Unique downstream name; used as the '<name>__' tool prefix."},"transport":{"type":"string","enum":["http","stdio"],"description":"http = Streamable HTTP; stdio = spawn a local process."},"url":{"type":"string","description":"Endpoint URL (http transport)."},"command":{"type":"string","description":"Executable path (stdio transport)."},"args":{"type":"array","items":{"type":"string"},"description":"Process args (stdio)."},"env":{"type":"object","additionalProperties":{"type":"string"},"description":"Extra environment variables (stdio)."},"headers":{"type":"object","additionalProperties":{"type":"string"},"description":"Extra HTTP headers (http), e.g. {\"Authorization\":\"Bearer <token>\"}."}},"required":["name","transport"]}}`)
}

func removeServerToolDef() json.RawMessage {
	return json.RawMessage(`{"name":"remove_server","description":"Dynamically remove a downstream MCP server (disconnect + drop its tools) and persist the removal to downstreams.json. No proxy restart.","inputSchema":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}}`)
}

type dsProbe struct {
	tools  []json.RawMessage
	routes map[string]route
	line   string
}

// reload probes all downstreams concurrently and rebuilds the tool table.
// Returns whether the exposed tool set changed plus a human-readable status.
func (r *registry) reload(ctx context.Context) (bool, string) {
	if err := r.applyConfig(); err != nil {
		log.Printf("reload: cannot apply config: %v", err)
	}
	r.mu.RLock()
	order := append([]string(nil), r.order...)
	dss := r.dss
	r.mu.RUnlock()

	probes := make([]dsProbe, len(order))
	var wg sync.WaitGroup
	for i, name := range order {
		wg.Add(1)
		go func(i int, name string) {
			defer wg.Done()
			ds := dss[name]
			cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			if !ds.Connected() {
				if err := ds.Connect(cctx); err != nil {
					probes[i] = dsProbe{line: fmt.Sprintf("  %-18s DOWN  (%v)", name, err)}
					return
				}
			}
			res, rerr, err := ds.Request(cctx, "tools/list", mustJSON(map[string]interface{}{}))
			if err != nil || rerr != nil {
				// Likely a stale session (the IDE/server restarted while we still
				// thought we were connected). Force a fresh initialize and retry
				// once before giving up — this is what makes "reload" recover an
				// IDE restart without a remove/re-add.
				if cerr := ds.Connect(cctx); cerr != nil {
					probes[i] = dsProbe{line: fmt.Sprintf("  %-18s DOWN  (reconnect failed: %v)", name, cerr)}
					return
				}
				res, rerr, err = ds.Request(cctx, "tools/list", mustJSON(map[string]interface{}{}))
				if err != nil || rerr != nil {
					emsg := "tools/list failed after reconnect"
					if err != nil {
						emsg = err.Error()
					}
					probes[i] = dsProbe{line: fmt.Sprintf("  %-18s DOWN  (%s)", name, emsg)}
					return
				}
			}
			var tl struct {
				Tools []json.RawMessage `json:"tools"`
			}
			if json.Unmarshal(res, &tl) != nil {
				probes[i] = dsProbe{line: fmt.Sprintf("  %-18s UP but tools/list malformed", name)}
				return
			}
			pfx := sanitize(name)
			pr := dsProbe{routes: map[string]route{}}
			for _, t := range tl.Tools {
				var obj map[string]json.RawMessage
				if json.Unmarshal(t, &obj) != nil {
					continue
				}
				var orig string
				if json.Unmarshal(obj["name"], &orig) != nil {
					continue
				}
				exposed := pfx + "__" + orig
				obj["name"] = mustJSON(exposed)
				rebuilt, err := json.Marshal(obj)
				if err != nil {
					continue
				}
				pr.tools = append(pr.tools, rebuilt)
				pr.routes[exposed] = route{ds: name, orig: orig}
			}
			pr.line = fmt.Sprintf("  %-18s UP    (%d tools)", name, len(pr.tools))
			probes[i] = pr
		}(i, name)
	}
	wg.Wait()

	newTools := baseTools()
	newRoutes := map[string]route{}
	var sb strings.Builder
	for _, p := range probes {
		if p.line != "" {
			sb.WriteString(p.line + "\n")
		}
		newTools = append(newTools, p.tools...)
		for k, v := range p.routes {
			newRoutes[k] = v
		}
	}

	r.mu.Lock()
	old := toolNameSet(r.tools)
	r.tools = newTools
	r.routes = newRoutes
	r.lastReload = time.Now()
	r.mu.Unlock()

	changed := !sameSet(old, toolNameSet(newTools))
	header := fmt.Sprintf("%s build=%s pid=%d\nDownstream MCP status:\n", proxyName, buildVersion, os.Getpid())
	return changed, header + sb.String()
}

func toolNameSet(tools []json.RawMessage) map[string]bool {
	s := map[string]bool{}
	for _, t := range tools {
		var obj struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(t, &obj) == nil {
			s[obj.Name] = true
		}
	}
	return s
}

func sameSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func (r *registry) snapshotTools() []json.RawMessage {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]json.RawMessage(nil), r.tools...)
}

func (r *registry) lookup(exposed string) (downstream, string, bool) {
	r.mu.RLock()
	rt, ok := r.routes[exposed]
	ds := r.dss[rt.ds]
	r.mu.RUnlock()
	if !ok {
		return nil, "", false
	}
	return ds, rt.orig, true
}

// ---------- stdio server (towards Claude) ----------

var (
	stdoutMu sync.Mutex
	out      = bufio.NewWriter(os.Stdout)
)

func writeMessage(m rpcMessage) {
	m.JSONRPC = "2.0"
	emit(m)
}

// emit delivers a fully-formed message to the active client transport. Default
// is stdio (stdout); HTTP mode swaps in the router's sink at startup (httpserve.go).
var emit = emitStdout

func emitStdout(m rpcMessage) {
	b, _ := json.Marshal(m)
	stdoutMu.Lock()
	out.Write(b)
	out.WriteByte('\n')
	out.Flush()
	stdoutMu.Unlock()
}

func respondResult(id json.RawMessage, v interface{}) {
	writeMessage(rpcMessage{ID: id, Result: mustJSON(v)})
}

func respondRaw(id json.RawMessage, raw json.RawMessage) {
	writeMessage(rpcMessage{ID: id, Result: raw})
}

func respondError(id json.RawMessage, code int, msg string) {
	writeMessage(rpcMessage{ID: id, Error: &rpcError{Code: code, Message: msg}})
}

func sendListChanged() {
	writeMessage(rpcMessage{Method: "notifications/tools/list_changed"})
}

func toolText(text string, isErr bool) json.RawMessage {
	r := map[string]interface{}{"content": []map[string]string{{"type": "text", "text": text}}}
	if isErr {
		r["isError"] = true
	}
	return mustJSON(r)
}

type server struct {
	reg *registry
}

func (s *server) handle(msg rpcMessage) {
	// Notifications (no id) — nothing to answer.
	if len(msg.ID) == 0 {
		if msg.Method == "notifications/initialized" {
			// Session is live: probe downstreams and announce whatever is up.
			go func() {
				if changed, _ := s.reg.reload(context.Background()); changed {
					sendListChanged()
				}
			}()
		}
		return
	}

	switch msg.Method {
	case "initialize":
		respondResult(msg.ID, map[string]interface{}{
			"protocolVersion": protocolVersion,
			"capabilities":    map[string]interface{}{"tools": map[string]interface{}{"listChanged": true}},
			"serverInfo":      map[string]string{"name": proxyName, "version": proxyVersion},
			"instructions":    "Aggregating proxy over several MCP servers. Tools are exposed as '<server>__<tool>'. Call 'reload' to re-probe downstream servers (e.g. after launching an IDE) and refresh the tool list; call 'status' to see what's connected.",
		})
	case "ping":
		respondResult(msg.ID, map[string]interface{}{})
	case "tools/list":
		respondResult(msg.ID, map[string]interface{}{"tools": s.reg.snapshotTools()})
		s.maybeRefresh()
	case "tools/call":
		s.handleCall(msg)
	case "resources/list":
		respondResult(msg.ID, map[string]interface{}{"resources": []interface{}{}})
	case "prompts/list":
		respondResult(msg.ID, map[string]interface{}{"prompts": []interface{}{}})
	default:
		respondError(msg.ID, -32601, "method not found: "+msg.Method)
	}
}

// maybeRefresh re-probes downstreams in the background, rate-limited, so the
// tool list self-heals when an IDE comes up even without an explicit reload.
func (s *server) maybeRefresh() {
	s.reg.mu.RLock()
	stale := time.Since(s.reg.lastReload) > 5*time.Second
	s.reg.mu.RUnlock()
	if !stale {
		return
	}
	go func() {
		if changed, _ := s.reg.reload(context.Background()); changed {
			sendListChanged()
		}
	}()
}

func (s *server) handleCall(msg rpcMessage) {
	var p struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(msg.Params, &p); err != nil {
		respondError(msg.ID, -32602, "invalid params: "+err.Error())
		return
	}

	switch p.Name {
	case "version":
		exe, _ := os.Executable()
		info := fmt.Sprintf("%s\n  build:    %s\n  proxyVer: %s\n  pid:      %d\n  started:  %s\n  exe:      %s",
			proxyName, buildVersion, proxyVersion, os.Getpid(), startedAt.Format(time.RFC3339), exe)
		respondRaw(msg.ID, toolText(info, false))
		return
	case "reload":
		// Always emit list_changed on an EXPLICIT reload, not just on a detected
		// delta. The client's cached tool manifest can desync from our view (a
		// missed earlier notification, or our r.tools already populated by the
		// auto-refresh so `changed` is false) — and `reload` is the user's
		// recovery lever, so it must force the client to re-fetch tools/list
		// unconditionally. (Auto-refresh on tools/list still emits only on change.)
		_, status := s.reg.reload(context.Background())
		sendListChanged()
		respondRaw(msg.ID, toolText(status, false))
		return
	case "status":
		_, status := s.reg.reload(context.Background())
		respondRaw(msg.ID, toolText(status, false))
		return
	case "add_server":
		var dc dsConfig
		if err := json.Unmarshal(p.Arguments, &dc); err != nil || dc.Name == "" || dc.Transport == "" {
			respondRaw(msg.ID, toolText("add_server requires at least 'name' and 'transport' (and url for http, or command for stdio).", true))
			return
		}
		s.reg.upsertConfig(dc)
		if err := s.reg.writeConfig(); err != nil {
			respondRaw(msg.ID, toolText("failed to persist config: "+err.Error(), true))
			return
		}
		changed, status := s.reg.reload(context.Background())
		if changed {
			sendListChanged()
		}
		respondRaw(msg.ID, toolText("Added downstream "+dc.Name+".\n"+status, false))
		return
	case "remove_server":
		var a struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(p.Arguments, &a); err != nil || a.Name == "" {
			respondRaw(msg.ID, toolText("remove_server requires 'name'.", true))
			return
		}
		existed := s.reg.removeConfig(a.Name)
		if err := s.reg.writeConfig(); err != nil {
			respondRaw(msg.ID, toolText("failed to persist config: "+err.Error(), true))
			return
		}
		changed, status := s.reg.reload(context.Background())
		if changed {
			sendListChanged()
		}
		verb := "Removed"
		if !existed {
			verb = "No such downstream;"
		}
		respondRaw(msg.ID, toolText(verb+" "+a.Name+".\n"+status, false))
		return
	case "authenticate":
		var a struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(p.Arguments, &a); err != nil || a.Name == "" {
			respondRaw(msg.ID, toolText("authenticate requires 'name'.", true))
			return
		}
		s.reg.mu.RLock()
		var url string
		for _, d := range s.reg.desired {
			if d.Name == a.Name {
				url = d.URL
			}
		}
		s.reg.mu.RUnlock()
		if url == "" {
			respondRaw(msg.ID, toolText("no HTTP downstream named "+a.Name+" (authenticate is for http/oauth servers).", true))
			return
		}
		authCtx, cancel := context.WithTimeout(context.Background(), 200*time.Second)
		rec, err := authenticateOAuth(authCtx, a.Name, url)
		cancel()
		if err != nil {
			respondRaw(msg.ID, toolText("Authentication failed for "+a.Name+": "+err.Error(), true))
			return
		}
		oauthStore.put(a.Name, rec)
		changed, status := s.reg.reload(context.Background())
		if changed {
			sendListChanged()
		}
		respondRaw(msg.ID, toolText("Authenticated "+a.Name+" (scopes: "+strings.Join(rec.Scopes, " ")+").\n"+status, false))
		return
	}

	ds, orig, ok := s.reg.lookup(p.Name)
	if !ok {
		respondError(msg.ID, -32602, "unknown tool: "+p.Name+" (call 'reload' if the owning server was just started)")
		return
	}
	if !ds.Connected() {
		respondRaw(msg.ID, toolText(fmt.Sprintf("Downstream %q is not connected. Start the server/IDE, then call 'reload'.", ds.Name()), true))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.reg.callTimeout)
	defer cancel()
	fwd := mustJSON(map[string]interface{}{"name": orig, "arguments": p.Arguments})
	res, rerr, err := ds.Request(ctx, "tools/call", fwd)
	if err != nil {
		respondRaw(msg.ID, toolText(fmt.Sprintf("Call to %s failed: %v", ds.Name(), err), true))
		return
	}
	if rerr != nil {
		respondError(msg.ID, rerr.Code, rerr.Message)
		return
	}
	respondRaw(msg.ID, res)
}

func (s *server) serve() {
	br := bufio.NewReaderSize(os.Stdin, 1<<20)
	for {
		line, err := br.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			var msg rpcMessage
			if json.Unmarshal(line, &msg) == nil {
				go s.handle(msg)
			} else {
				log.Printf("dropping unparseable line (%d bytes)", len(line))
			}
		}
		if err != nil {
			if err != io.EOF {
				log.Printf("stdin read error: %v", err)
			}
			return
		}
	}
}

// ---------- main ----------

func main() {
	exe, _ := os.Executable()
	exeDir := filepath.Dir(exe)
	configPath := flag.String("config", filepath.Join(exeDir, "downstreams.json"), "path to downstreams.json")
	logPath := flag.String("log", filepath.Join(exeDir, "mcp-proxy.log"), "path to log file (stderr always)")
	httpAddr := flag.String("http", "", "if set (e.g. 127.0.0.1:6390), serve MCP over Streamable HTTP instead of stdio — lets the client connect (survives session recycles) rather than spawn")
	oauthTest := flag.String("oauth-test", "", "internal: test OAuth discovery+registration for a resource URL, then exit")
	flag.Parse()

	if *oauthTest != "" {
		log.SetOutput(os.Stderr)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		meta, err := discoverOAuth(ctx, *oauthTest)
		if err != nil {
			fmt.Fprintln(os.Stderr, "discover FAILED:", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "discover OK\n  auth=%s\n  token=%s\n  reg=%s\n  scopes=%v\n  resource=%s\n", meta.AuthEP, meta.TokenEP, meta.RegEP, meta.Scopes, meta.Resource)
		cid, err := registerClient(ctx, meta.RegEP, "http://127.0.0.1:49160/callback", meta.Scopes)
		if err != nil {
			fmt.Fprintln(os.Stderr, "register FAILED:", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "register OK\n  client_id=%s\n", cid)
		return
	}

	oauthStore = newTokenStore(filepath.Join(exeDir, "oauth-tokens.json"))

	// Logs go to stderr (+ a file). NEVER stdout — that's the protocol channel.
	var logw io.Writer = os.Stderr
	if f, err := os.OpenFile(*logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err == nil {
		logw = io.MultiWriter(os.Stderr, f)
	}
	log.SetOutput(logw)
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[mcp-proxy] ")

	cfgBytes, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("cannot read config %s: %v", *configPath, err)
	}
	var cfg proxyConfig
	if err := json.Unmarshal(cfgBytes, &cfg); err != nil {
		log.Fatalf("cannot parse config %s: %v", *configPath, err)
	}
	if cfg.CallTimeoutSeconds <= 0 {
		cfg.CallTimeoutSeconds = 120
	}

	reg := &registry{
		configPath:  *configPath,
		dss:         map[string]downstream{},
		sigs:        map[string]string{},
		routes:      map[string]route{},
		tools:       baseTools(),
		callTimeout: time.Duration(cfg.CallTimeoutSeconds) * time.Second,
	}
	if err := reg.applyConfig(); err != nil {
		log.Fatalf("cannot apply config: %v", err)
	}

	// In HTTP mode, redirect the message sink to the router BEFORE any startup
	// notification can fire, so emit is set consistently (no data race on the
	// global) and list_changed reaches SSE subscribers rather than stdout.
	var router *httpRouter
	if *httpAddr != "" {
		router = newHTTPRouter()
		emit = router.emit
	}

	// Probe downstreams at startup so the first tools/list is populated; runs in
	// the background so initialize stays instant. Completion emits list_changed.
	go func() {
		if changed, status := reg.reload(context.Background()); changed {
			sendListChanged()
			log.Printf("startup reload complete\n%s", status)
		}
	}()

	srv := &server{reg: reg}
	if *httpAddr != "" {
		log.Printf("%s %s starting (HTTP mode on %s); fronting %d downstream(s): %s", proxyName, proxyVersion, *httpAddr, len(reg.order), strings.Join(reg.order, ", "))
		if err := serveHTTP(srv, router, *httpAddr); err != nil {
			log.Fatalf("http server error: %v", err)
		}
		return
	}

	log.Printf("%s %s starting (stdio mode); fronting %d downstream(s): %s", proxyName, proxyVersion, len(reg.order), strings.Join(reg.order, ", "))
	srv.serve()

	for _, name := range reg.order {
		reg.dss[name].Close()
	}
	log.Printf("stdin closed; exiting")
}

var _ = sort.Strings
