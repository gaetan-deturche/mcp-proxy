// Interactive session-token sign-in for the Mattermost stdio downstream.
// Our Mattermost (v11.7) has Personal Access Tokens disabled, so the MCP server
// authenticates with a *session token* from POST /api/v4/users/login (returned
// in the "Token" response header). This mirrors the OAuth flow's UX: authenticate
// opens a loopback sign-in page in the browser; the user submits email + password
// (+ MFA code if their account uses it); the proxy calls /users/login server-side,
// reads the Token header, and returns it to the caller, which writes it into the
// downstream's MM_ACCESS_TOKEN env and respawns the child. The password is used
// once server-side and never stored.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

// authenticateMattermost opens a loopback login page, waits for the user to sign
// in, and returns a fresh Mattermost session token. The server base URL is read
// from the downstream's MM_SERVER_URL env.
func authenticateMattermost(ctx context.Context, cfg dsConfig) (string, error) {
	server := strings.TrimRight(cfg.Env["MM_SERVER_URL"], "/")
	if server == "" {
		return "", errors.New("downstream has no MM_SERVER_URL env")
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port
	pageURL := fmt.Sprintf("http://127.0.0.1:%d/", port)

	tokenCh := make(chan string, 1)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, renderLogin(server, ""))
	})
	mux.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		loginID := strings.TrimSpace(r.FormValue("login_id"))
		password := r.FormValue("password")
		mfa := strings.TrimSpace(r.FormValue("mfa"))
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if loginID == "" || password == "" {
			io.WriteString(w, renderLogin(server, "Email and password are required."))
			return
		}
		token, lerr := mattermostLogin(ctx, server, loginID, password, mfa)
		if lerr != nil {
			// Re-render the form so the user can retry without reopening.
			io.WriteString(w, renderLogin(server, lerr.Error()))
			return
		}
		io.WriteString(w, renderResult("Signed in. You can close this tab and return to Claude."))
		tokenCh <- token
	})

	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)
	defer srv.Shutdown(context.Background())

	log.Printf("mattermost auth: opening login page %s", pageURL)
	openBrowser(pageURL)

	select {
	case token := <-tokenCh:
		return token, nil
	case <-time.After(190 * time.Second):
		return "", errors.New("timed out waiting for Mattermost sign-in")
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// mattermostLogin calls POST /api/v4/users/login and returns the session token
// from the "Token" response header. The MFA code is included when provided.
func mattermostLogin(ctx context.Context, server, loginID, password, mfa string) (string, error) {
	body := map[string]string{"login_id": loginID, "password": password}
	if mfa != "" {
		body["token"] = mfa
	}
	b, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, "POST", server+"/api/v4/users/login", strings.NewReader(string(b)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		var mmErr struct {
			Message string `json:"message"`
			ID      string `json:"id"`
		}
		rb, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		_ = json.Unmarshal(rb, &mmErr)
		msg := mmErr.Message
		if msg == "" {
			msg = strings.TrimSpace(string(rb))
		}
		if strings.Contains(mmErr.ID, "mfa") {
			msg += " — enter your current MFA code above and sign in again."
		}
		return "", fmt.Errorf("login failed (%d): %s", resp.StatusCode, msg)
	}
	token := resp.Header.Get("Token")
	if token == "" {
		return "", errors.New("login succeeded but no Token header was returned")
	}
	return token, nil
}

func renderLogin(server, errMsg string) string {
	page := strings.ReplaceAll(loginPageHTML, "{{SERVER}}", html.EscapeString(server))
	errHTML := ""
	if errMsg != "" {
		errHTML = `<p class="err">` + html.EscapeString(errMsg) + `</p>`
	}
	return strings.ReplaceAll(page, "{{ERR}}", errHTML)
}

func renderResult(msg string) string {
	return strings.ReplaceAll(resultPageHTML, "{{MSG}}", html.EscapeString(msg))
}

const loginPageHTML = `<!doctype html>
<html><head><meta charset="utf-8"><title>Mattermost sign-in</title>
<style>
body{font-family:system-ui,Segoe UI,Arial,sans-serif;background:#1b1d22;color:#e8e8e8;display:flex;min-height:100vh;align-items:center;justify-content:center;margin:0}
.card{background:#24272e;padding:28px 32px;border-radius:10px;box-shadow:0 8px 30px rgba(0,0,0,.4);width:320px}
h1{font-size:18px;margin:0 0 4px}
.sub{font-size:12px;color:#9aa0aa;margin:0 0 14px;word-break:break-all}
.err{color:#ff6b6b;font-size:12px;margin:8px 0 0}
label{display:block;font-size:12px;color:#c3c7cf;margin:12px 0 4px}
.hint{color:#9aa0aa}
input{width:100%;box-sizing:border-box;padding:9px 10px;border:1px solid #3a3f49;border-radius:6px;background:#1b1d22;color:#e8e8e8;font-size:14px}
button{width:100%;margin-top:18px;padding:10px;border:0;border-radius:6px;background:#1c6cff;color:#fff;font-size:14px;cursor:pointer}
button:hover{background:#155ad6}
</style></head>
<body><form class="card" method="post" action="/submit">
<h1>Sign in to Mattermost</h1>
<p class="sub">{{SERVER}}</p>
{{ERR}}
<label>Email</label><input name="login_id" type="email" autocomplete="username" autofocus>
<label>Password</label><input name="password" type="password" autocomplete="current-password">
<label>MFA code <span class="hint">(only if your account uses MFA)</span></label><input name="mfa" type="text" inputmode="numeric" autocomplete="one-time-code">
<button type="submit">Sign in</button>
</form></body></html>`

const resultPageHTML = `<!doctype html>
<html><head><meta charset="utf-8"><title>Mattermost sign-in</title>
<style>
body{font-family:system-ui,Segoe UI,Arial,sans-serif;background:#1b1d22;color:#e8e8e8;display:flex;min-height:100vh;align-items:center;justify-content:center;margin:0}
.card{background:#24272e;padding:28px 32px;border-radius:10px;max-width:360px;text-align:center}
p{font-size:15px;margin:0}
</style></head>
<body><div class="card"><p>{{MSG}}</p></div></body></html>`
