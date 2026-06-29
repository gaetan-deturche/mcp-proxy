// OAuth 2.1 client for downstream MCP servers that require authorization
// (e.g. Sentry). Implements the MCP authorization flow: RFC 9728 protected-
// resource metadata discovery, RFC 8414 authorization-server metadata, RFC 7591
// dynamic client registration, and authorization-code + PKCE via a one-time
// loopback-redirect browser sign-in, with refresh-token renewal.
//
// Tokens are persisted to oauth-tokens.json next to the binary and attached as
// "Authorization: Bearer" on every request to an oauth downstream.
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// oauthStore is the process-wide token store, set in main().
var oauthStore *tokenStore

type tokenRecord struct {
	ClientID     string    `json:"client_id"`
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token,omitempty"`
	ExpiresAt    time.Time `json:"expires_at"`
	AuthEP       string    `json:"authorization_endpoint"`
	TokenEP      string    `json:"token_endpoint"`
	RegEP        string    `json:"registration_endpoint,omitempty"`
	Resource     string    `json:"resource"`
	Scopes       []string  `json:"scopes,omitempty"`
}

type tokenStore struct {
	path string
	mu   sync.Mutex
	recs map[string]tokenRecord
}

func newTokenStore(path string) *tokenStore {
	ts := &tokenStore{path: path, recs: map[string]tokenRecord{}}
	if b, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(b, &ts.recs)
	}
	return ts
}

func (ts *tokenStore) save() {
	b, _ := json.MarshalIndent(ts.recs, "", "  ")
	_ = os.WriteFile(ts.path, b, 0600)
}

func (ts *tokenStore) get(name string) (tokenRecord, bool) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	r, ok := ts.recs[name]
	return r, ok
}

func (ts *tokenStore) put(name string, r tokenRecord) {
	ts.mu.Lock()
	ts.recs[name] = r
	ts.save()
	ts.mu.Unlock()
}

// accessToken returns a valid bearer token for name, refreshing if near expiry.
func (ts *tokenStore) accessToken(name string) (string, error) {
	rec, ok := ts.get(name)
	if !ok || rec.AccessToken == "" {
		return "", errors.New("not authenticated; run the 'authenticate' tool")
	}
	if time.Now().Before(rec.ExpiresAt.Add(-60 * time.Second)) {
		return rec.AccessToken, nil
	}
	if rec.RefreshToken == "" {
		return "", errors.New("token expired and no refresh token; run 'authenticate'")
	}
	nr, err := refreshToken(rec)
	if err != nil {
		return "", fmt.Errorf("token refresh failed (%v); run 'authenticate'", err)
	}
	ts.put(name, nr)
	return nr.AccessToken, nil
}

// ---------- discovery ----------

type oauthMeta struct {
	AuthEP   string
	TokenEP  string
	RegEP    string
	Resource string
	Scopes   []string
}

func httpGetJSON(ctx context.Context, u string, v interface{}) error {
	req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("GET %s -> %d %s", u, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

// discoverOAuth resolves OAuth endpoints for an MCP resource URL (RFC 9728 + 8414).
func discoverOAuth(ctx context.Context, resourceURL string) (*oauthMeta, error) {
	prmURL := probeResourceMetadataURL(ctx, resourceURL)
	if prmURL == "" {
		u, err := url.Parse(resourceURL)
		if err != nil {
			return nil, err
		}
		prmURL = u.Scheme + "://" + u.Host + "/.well-known/oauth-protected-resource" + u.Path
	}
	var prm struct {
		Resource             string   `json:"resource"`
		AuthorizationServers []string `json:"authorization_servers"`
		ScopesSupported      []string `json:"scopes_supported"`
	}
	if err := httpGetJSON(ctx, prmURL, &prm); err != nil {
		return nil, fmt.Errorf("protected-resource metadata: %w", err)
	}
	if len(prm.AuthorizationServers) == 0 {
		return nil, errors.New("no authorization_servers in resource metadata")
	}
	as := strings.TrimRight(prm.AuthorizationServers[0], "/")
	var asm struct {
		AuthorizationEndpoint string   `json:"authorization_endpoint"`
		TokenEndpoint         string   `json:"token_endpoint"`
		RegistrationEndpoint  string   `json:"registration_endpoint"`
		ScopesSupported       []string `json:"scopes_supported"`
	}
	err1 := httpGetJSON(ctx, as+"/.well-known/oauth-authorization-server", &asm)
	if err1 != nil || asm.TokenEndpoint == "" {
		if err2 := httpGetJSON(ctx, as+"/.well-known/openid-configuration", &asm); err2 != nil {
			return nil, fmt.Errorf("authorization-server metadata: %v / %v", err1, err2)
		}
	}
	scopes := prm.ScopesSupported
	if len(scopes) == 0 {
		scopes = asm.ScopesSupported
	}
	res := prm.Resource
	if res == "" {
		res = resourceURL
	}
	return &oauthMeta{AuthEP: asm.AuthorizationEndpoint, TokenEP: asm.TokenEndpoint, RegEP: asm.RegistrationEndpoint, Resource: res, Scopes: scopes}, nil
}

// probeResourceMetadataURL reads the resource_metadata pointer from the 401
// WWW-Authenticate header (RFC 9728), returning "" if absent.
func probeResourceMetadataURL(ctx context.Context, resourceURL string) string {
	req, _ := http.NewRequestWithContext(ctx, "POST", resourceURL, strings.NewReader(`{"jsonrpc":"2.0","id":0,"method":"ping"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	wa := resp.Header.Get("WWW-Authenticate")
	const k = `resource_metadata="`
	if i := strings.Index(wa, k); i >= 0 {
		rest := wa[i+len(k):]
		if j := strings.Index(rest, `"`); j >= 0 {
			return rest[:j]
		}
	}
	return ""
}

// ---------- dynamic client registration ----------

func registerClient(ctx context.Context, regEP, redirectURI string, scopes []string) (string, error) {
	if regEP == "" {
		return "", errors.New("no registration_endpoint (dynamic client registration unsupported)")
	}
	body, _ := json.Marshal(map[string]interface{}{
		"client_name":                "mcp-aggregator-proxy",
		"redirect_uris":              []string{redirectURI},
		"grant_types":                []string{"authorization_code", "refresh_token"},
		"response_types":             []string{"code"},
		"token_endpoint_auth_method": "none",
		"scope":                      strings.Join(scopes, " "),
	})
	req, _ := http.NewRequestWithContext(ctx, "POST", regEP, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	log.Printf("OAuth DCR: redirect=%s status=%d resp=%.300s", redirectURI, resp.StatusCode, strings.TrimSpace(string(rb)))
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("register %d: %s", resp.StatusCode, strings.TrimSpace(string(rb)))
	}
	var rr struct {
		ClientID string `json:"client_id"`
	}
	if err := json.Unmarshal(rb, &rr); err != nil {
		return "", err
	}
	if rr.ClientID == "" {
		return "", errors.New("registration returned no client_id")
	}
	return rr.ClientID, nil
}

// ---------- PKCE + browser authorization-code flow ----------

func randB64(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func s256(v string) string {
	h := sha256.Sum256([]byte(v))
	return base64.RawURLEncoding.EncodeToString(h[:])
}

func openBrowser(u string) {
	// Windows: open via rundll32/ShellExecute so the URL is passed as a SINGLE
	// argument. Do NOT use `cmd /c start` — cmd.exe treats '&' as a command
	// separator, and Go does not quote '&', so the OAuth URL gets truncated at
	// the first '&' (dropping redirect_uri and everything after client_id),
	// which makes Sentry report "Invalid redirect URI".
	_ = exec.Command("rundll32", "url.dll,FileProtocolHandler", u).Start()
}

// authenticateOAuth runs the full flow for an MCP resource URL, opening a browser
// for the user to sign in, and returns a stored token record.
func authenticateOAuth(ctx context.Context, name, resourceURL string) (tokenRecord, error) {
	meta, err := discoverOAuth(ctx, resourceURL)
	if err != nil {
		return tokenRecord{}, err
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return tokenRecord{}, err
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port
	redirect := fmt.Sprintf("http://127.0.0.1:%d/callback", port)

	clientID, err := registerClient(ctx, meta.RegEP, redirect, meta.Scopes)
	if err != nil {
		return tokenRecord{}, err
	}

	verifier := randB64(48)
	state := randB64(16)
	q := url.Values{}
	q.Set("response_type", "code")
	q.Set("client_id", clientID)
	q.Set("redirect_uri", redirect)
	q.Set("code_challenge", s256(verifier))
	q.Set("code_challenge_method", "S256")
	q.Set("state", state)
	if len(meta.Scopes) > 0 {
		q.Set("scope", strings.Join(meta.Scopes, " "))
	}
	if meta.Resource != "" {
		q.Set("resource", meta.Resource)
	}
	authURL := meta.AuthEP + "?" + q.Encode()

	codeCh := make(chan string, 1)
	errCh := make(chan error, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("OAuth(%s) callback hit: %s", name, r.URL.RequestURI())
		if e := r.URL.Query().Get("error"); e != "" {
			desc := r.URL.Query().Get("error_description")
			log.Printf("OAuth(%s) callback error: %s — %s", name, e, desc)
			fmt.Fprintf(w, "Authorization failed: %s — %s. You can close this tab.", e, desc)
			errCh <- fmt.Errorf("authorization error: %s: %s", e, desc)
			return
		}
		if r.URL.Query().Get("state") != state {
			http.Error(w, "state mismatch", http.StatusBadRequest)
			errCh <- errors.New("state mismatch")
			return
		}
		fmt.Fprint(w, "Authentication complete. You can close this tab and return to Claude.")
		codeCh <- r.URL.Query().Get("code")
	})
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)
	defer srv.Shutdown(context.Background())

	log.Printf("OAuth(%s): opening browser:\n%s", name, authURL)
	openBrowser(authURL)

	log.Printf("OAuth(%s): client_id=%s redirect=%s — waiting for browser authorization", name, clientID, redirect)
	var code string
	select {
	case code = <-codeCh:
	case err = <-errCh:
		return tokenRecord{}, err
	case <-time.After(180 * time.Second):
		return tokenRecord{}, errors.New("timed out waiting for browser authorization")
	case <-ctx.Done():
		return tokenRecord{}, ctx.Err()
	}

	log.Printf("OAuth(%s): code received, exchanging at %s", name, meta.TokenEP)
	rec, err := exchangeCode(ctx, meta, clientID, code, verifier, redirect)
	if err != nil {
		log.Printf("OAuth(%s): token exchange FAILED: %v", name, err)
		return tokenRecord{}, err
	}
	log.Printf("OAuth(%s): token exchange OK (expires %s)", name, rec.ExpiresAt.Format(time.RFC3339))
	rec.Scopes = meta.Scopes
	return rec, nil
}

func exchangeCode(ctx context.Context, meta *oauthMeta, clientID, code, verifier, redirect string) (tokenRecord, error) {
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", redirect)
	form.Set("client_id", clientID)
	form.Set("code_verifier", verifier)
	if meta.Resource != "" {
		form.Set("resource", meta.Resource)
	}
	return tokenPost(ctx, meta.AuthEP, meta.TokenEP, meta.RegEP, meta.Resource, clientID, form)
}

func refreshToken(rec tokenRecord) (tokenRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", rec.RefreshToken)
	form.Set("client_id", rec.ClientID)
	if rec.Resource != "" {
		form.Set("resource", rec.Resource)
	}
	nr, err := tokenPost(ctx, rec.AuthEP, rec.TokenEP, rec.RegEP, rec.Resource, rec.ClientID, form)
	if err != nil {
		return tokenRecord{}, err
	}
	if nr.RefreshToken == "" { // some servers omit a new refresh token; keep the old one
		nr.RefreshToken = rec.RefreshToken
	}
	if len(nr.Scopes) == 0 {
		nr.Scopes = rec.Scopes
	}
	return nr, nil
}

func tokenPost(ctx context.Context, authEP, tokenEP, regEP, resource, clientID string, form url.Values) (tokenRecord, error) {
	req, _ := http.NewRequestWithContext(ctx, "POST", tokenEP, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return tokenRecord{}, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return tokenRecord{}, fmt.Errorf("token endpoint %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var tr struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
	}
	if err := json.Unmarshal(b, &tr); err != nil {
		return tokenRecord{}, err
	}
	if tr.AccessToken == "" {
		return tokenRecord{}, errors.New("no access_token in token response")
	}
	return tokenRecord{
		ClientID:     clientID,
		AccessToken:  tr.AccessToken,
		RefreshToken: tr.RefreshToken,
		ExpiresAt:    time.Now().Add(time.Duration(max(tr.ExpiresIn, 300)) * time.Second),
		AuthEP:       authEP,
		TokenEP:      tokenEP,
		RegEP:        regEP,
		Resource:     resource,
	}, nil
}

func authenticateToolDef() json.RawMessage {
	return json.RawMessage(`{"name":"authenticate","description":"Run the OAuth sign-in flow for a downstream MCP server that requires it (e.g. sentry, marked \"oauth\": true). Opens your browser to authorize; on success the token is stored and auto-refreshed, and the server's tools are attached without a restart.","inputSchema":{"type":"object","properties":{"name":{"type":"string","description":"Downstream name to authenticate (e.g. sentry)."}},"required":["name"]}}`)
}
