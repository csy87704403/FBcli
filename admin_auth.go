package main

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	adminSessionCookie = "freebuff_admin_session"
	adminSessionTTL    = 12 * time.Hour
	loginAttemptWindow = 10 * time.Minute
	loginBlockDuration = 5 * time.Minute
	maxLoginFailures   = 5
)

type loginFailures struct {
	Count        int
	WindowStart  time.Time
	BlockedUntil time.Time
}

type adminAuthenticator struct {
	mu       sync.Mutex
	username string
	password [32]byte
	sessions map[string]time.Time
	failures map[string]loginFailures
}

func newAdminAuthenticator(username, password string) *adminAuthenticator {
	return &adminAuthenticator{
		username: username,
		password: sha256.Sum256([]byte(password)),
		sessions: make(map[string]time.Time),
		failures: make(map[string]loginFailures),
	}
}

func (a *adminAuthenticator) validCredentials(username, password string) bool {
	userHash := sha256.Sum256([]byte(username))
	expectedUserHash := sha256.Sum256([]byte(a.username))
	passwordHash := sha256.Sum256([]byte(password))
	return subtle.ConstantTimeCompare(userHash[:], expectedUserHash[:]) == 1 &&
		subtle.ConstantTimeCompare(passwordHash[:], a.password[:]) == 1
}

func requestIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

func (a *adminAuthenticator) isBlocked(ip string, now time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	failure := a.failures[ip]
	return now.Before(failure.BlockedUntil)
}

func (a *adminAuthenticator) recordFailure(ip string, now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	failure := a.failures[ip]
	if failure.WindowStart.IsZero() || now.Sub(failure.WindowStart) > loginAttemptWindow {
		failure = loginFailures{WindowStart: now}
	}
	failure.Count++
	if failure.Count >= maxLoginFailures {
		failure.BlockedUntil = now.Add(loginBlockDuration)
	}
	a.failures[ip] = failure
}

func (a *adminAuthenticator) createSession(ip string, now time.Time) (string, error) {
	buffer := make([]byte, 32)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	token := hex.EncodeToString(buffer)
	a.mu.Lock()
	delete(a.failures, ip)
	for existing, expiry := range a.sessions {
		if !now.Before(expiry) {
			delete(a.sessions, existing)
		}
	}
	a.sessions[token] = now.Add(adminSessionTTL)
	a.mu.Unlock()
	return token, nil
}

func (a *adminAuthenticator) validSession(r *http.Request) bool {
	cookie, err := r.Cookie(adminSessionCookie)
	if err != nil || cookie.Value == "" {
		return false
	}
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	expiry, ok := a.sessions[cookie.Value]
	if !ok || !now.Before(expiry) {
		delete(a.sessions, cookie.Value)
		return false
	}
	return true
}

func secureRequest(r *http.Request) bool {
	return r.TLS != nil || strings.EqualFold(strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")), "https")
}

func setAdminSessionCookie(w http.ResponseWriter, r *http.Request, token string, maxAge int) {
	http.SetCookie(w, &http.Cookie{
		Name: adminSessionCookie, Value: token, Path: "/", MaxAge: maxAge,
		HttpOnly: true, Secure: secureRequest(r), SameSite: http.SameSiteStrictMode,
	})
}

func (a *adminAuthenticator) login(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	ip := requestIP(r)
	if a.isBlocked(ip, now) {
		writeError(w, http.StatusTooManyRequests, "登录失败次数过多，请稍后重试", "login_rate_limited")
		return
	}
	var credentials struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&credentials) != nil {
		writeError(w, http.StatusBadRequest, "登录请求无效", "invalid_request")
		return
	}
	if !a.validCredentials(credentials.Username, credentials.Password) {
		a.recordFailure(ip, now)
		writeError(w, http.StatusUnauthorized, "用户名或密码错误", "invalid_credentials")
		return
	}
	token, err := a.createSession(ip, now)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "无法创建登录会话", "session_failed")
		return
	}
	setAdminSessionCookie(w, r, token, int(adminSessionTTL.Seconds()))
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *adminAuthenticator) logout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie(adminSessionCookie); err == nil {
		a.mu.Lock()
		delete(a.sessions, cookie.Value)
		a.mu.Unlock()
	}
	setAdminSessionCookie(w, r, "", -1)
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *adminAuthenticator) require(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.validSession(r) {
			next(w, r)
			return
		}
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		writeError(w, http.StatusUnauthorized, "请先登录管理后台", "admin_authentication_required")
	}
}

const adminLoginPage = `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Freebuff 管理后台登录</title><style>body{margin:0;background:#0d1117;color:#c9d1d9;font:14px system-ui;display:grid;place-items:center;min-height:100vh}.box{width:min(360px,calc(100% - 30px));background:#161b22;border:1px solid #30363d;border-radius:9px;padding:22px}h1{font-size:19px;color:#58a6ff}input,button{width:100%;box-sizing:border-box;border-radius:6px;padding:10px;margin-top:10px}input{background:#0d1117;color:#c9d1d9;border:1px solid #30363d}button{border:0;background:#238636;color:#fff;cursor:pointer}.error{color:#f85149;min-height:20px}</style></head><body><form class="box" id="login"><h1>Freebuff CLI Gateway</h1><p>管理后台登录</p><input id="user" autocomplete="username" placeholder="用户名" required autofocus><input id="password" type="password" autocomplete="current-password" placeholder="密码" required><p class="error" id="error"></p><button>登录</button></form><script>document.querySelector('#login').onsubmit=async e=>{e.preventDefault();const error=document.querySelector('#error');error.textContent='';const r=await fetch('/api/admin/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:document.querySelector('#user').value,password:document.querySelector('#password').value})});if(r.ok){location.href='/';return}let d={};try{d=await r.json()}catch{}error.textContent=d?.error?.message||'登录失败'};</script></body></html>`
