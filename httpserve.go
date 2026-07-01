// Streamable-HTTP inbound transport (towards the MCP client).
//
// Why: in stdio mode the client (Claude Code) SPAWNS the proxy and owns the
// write end of its stdin; when the client recycles its MCP connection it closes
// that pipe, the proxy hits EOF and exits, and if the client fails to re-spawn
// (observed mid-session) every tool vanishes until a full restart. In HTTP mode
// the proxy is a persistent listener the client CONNECTS to, so a recycle is
// just a reconnect to a still-running endpoint — downstream connections and
// OAuth tokens stay warm, and it survives client/session restarts.
//
// The handlers reuse the existing message pipeline verbatim: server.handle()
// still calls respond*/sendListChanged -> writeMessage -> emit. In HTTP mode
// emit is the router's sink, which routes responses (id present) back to the
// waiting POST and fans notifications (id absent) out to open SSE streams.
package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

// httpRouter bridges the id-addressed request/response pipeline to HTTP: it
// hands each response back to the specific in-flight POST that produced it, and
// broadcasts server-initiated notifications to every open GET/SSE stream.
type httpRouter struct {
	mu      sync.Mutex
	waiters map[string]chan rpcMessage // keyed by JSON-RPC id (raw bytes as string)
	subs    map[chan rpcMessage]struct{}
}

func newHTTPRouter() *httpRouter {
	return &httpRouter{
		waiters: map[string]chan rpcMessage{},
		subs:    map[chan rpcMessage]struct{}{},
	}
}

func (h *httpRouter) register(id string) chan rpcMessage {
	ch := make(chan rpcMessage, 1)
	h.mu.Lock()
	h.waiters[id] = ch
	h.mu.Unlock()
	return ch
}

func (h *httpRouter) unregister(id string) {
	h.mu.Lock()
	delete(h.waiters, id)
	h.mu.Unlock()
}

// emit is installed as the global message sink (var emit) in HTTP mode.
func (h *httpRouter) emit(m rpcMessage) {
	if len(m.ID) > 0 {
		key := string(m.ID)
		h.mu.Lock()
		ch := h.waiters[key]
		delete(h.waiters, key)
		h.mu.Unlock()
		if ch != nil {
			ch <- m
		} else {
			log.Printf("http: no waiter for response id %s (dropped)", key)
		}
		return
	}
	// Server-initiated notification (e.g. tools/list_changed): fan out to streams.
	h.mu.Lock()
	for ch := range h.subs {
		select {
		case ch <- m:
		default: // slow/stuck subscriber: drop rather than block the whole proxy
		}
	}
	h.mu.Unlock()
}

func (h *httpRouter) subscribe() chan rpcMessage {
	ch := make(chan rpcMessage, 32)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *httpRouter) unsubscribe(ch chan rpcMessage) {
	h.mu.Lock()
	delete(h.subs, ch)
	h.mu.Unlock()
}

func serveHTTP(srv *server, router *httpRouter, addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			handleHTTPPost(srv, router, w, r)
		case http.MethodGet:
			handleHTTPGet(router, w, r)
		case http.MethodDelete:
			// Session teardown: the proxy keeps no per-session state, so ack.
			w.WriteHeader(http.StatusOK)
		default:
			w.Header().Set("Allow", "GET, POST, DELETE")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	// Trivial liveness endpoint (handy for the startup task / a browser check).
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok\n")
	})

	hs := &http.Server{Addr: addr, Handler: mux}
	return hs.ListenAndServe()
}

func handleHTTPPost(srv *server, router *httpRouter, w http.ResponseWriter, r *http.Request) {
	var msg rpcMessage
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(&msg); err != nil {
		http.Error(w, "invalid json-rpc: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Client notification or response (no id): dispatch, nothing to return.
	if len(msg.ID) == 0 {
		go srv.handle(msg)
		w.WriteHeader(http.StatusAccepted)
		return
	}

	key := string(msg.ID)
	ch := router.register(key)
	defer router.unregister(key)

	go srv.handle(msg) // emits its response via router.emit -> ch

	select {
	case resp := <-ch:
		resp.JSONRPC = "2.0"
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	case <-r.Context().Done():
		// client hung up; router.unregister (deferred) cleans the waiter
	case <-time.After(150 * time.Second):
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(rpcMessage{
			JSONRPC: "2.0", ID: msg.ID,
			Error: &rpcError{Code: -32000, Message: "proxy timeout waiting for handler"},
		})
	}
}

// handleHTTPGet opens the SSE stream the client uses to receive server-initiated
// messages (notifications/tools/list_changed), keeping `reload` self-healing
// working over HTTP just as it does over stdio.
func handleHTTPGet(router *httpRouter, w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ch := router.subscribe()
	defer router.unsubscribe(ch)

	ka := time.NewTicker(25 * time.Second)
	defer ka.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ka.C:
			io.WriteString(w, ": keepalive\n\n")
			flusher.Flush()
		case m := <-ch:
			m.JSONRPC = "2.0"
			b, _ := json.Marshal(m)
			io.WriteString(w, "data: ")
			w.Write(b)
			io.WriteString(w, "\n\n")
			flusher.Flush()
		}
	}
}
