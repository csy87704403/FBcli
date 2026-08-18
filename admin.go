package main

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const officialAccessTierURL = "https://freebuff.com/api/web/access-tier"

type egressTierStatus struct {
	Tier      string    `json:"tier"`
	Detail    string    `json:"detail,omitempty"`
	CheckedAt time.Time `json:"checked_at,omitempty"`
	LatencyMS int64     `json:"latency_ms,omitempty"`
}

type proxyNode struct {
	Addr          string    `json:"addr"`
	Mode          string    `json:"mode,omitempty"`
	TierCheckedAt time.Time `json:"tier_checked_at,omitempty"`
	Detail        string    `json:"detail,omitempty"`
	LatencyMS     int64     `json:"latency_ms,omitempty"`
	Alive         bool      `json:"alive"`
	LastChecked   time.Time `json:"last_checked,omitempty"`
	LastOK        time.Time `json:"last_ok,omitempty"`
	CooldownUntil time.Time `json:"cooldown_until,omitempty"`
	FailCount     int       `json:"fail_count,omitempty"`
}

type usageRecord struct {
	At              time.Time `json:"at"`
	Model           string    `json:"model"`
	APIKey          string    `json:"api_key"`
	EstimatedTokens int       `json:"estimated_tokens"`
	Success         bool      `json:"success"`
	DurationMS      int64     `json:"duration_ms"`
}

type gatewayState struct {
	APIKeys              []string                         `json:"api_keys"`
	Accounts             []accountConfig                  `json:"accounts,omitempty"`
	AccountSessions      map[string]accountSessionBinding `json:"account_sessions,omitempty"`
	ConversationSessions map[string]sessionBinding        `json:"conversation_sessions,omitempty"`
	Proxies              []proxyNode                      `json:"proxies"`
	SelectedProxy        string                           `json:"selected_proxy,omitempty"`
	EgressTier           egressTierStatus                 `json:"egress_tier,omitempty"`
	Usage                []usageRecord                    `json:"usage,omitempty"`
}

type stateStore struct {
	mu    sync.RWMutex
	path  string
	state gatewayState
}

func newStateStore(path, legacyConfig, legacyPool string) (*stateStore, error) {
	store := &stateStore{path: path}
	data, err := os.ReadFile(path)
	if err == nil {
		if decodeErr := json.Unmarshal(data, &store.state); decodeErr != nil {
			backup, backupErr := os.ReadFile(path + ".bak")
			if backupErr != nil || json.Unmarshal(backup, &store.state) != nil {
				return nil, decodeErr
			}
			if saveErr := store.saveLocked(); saveErr != nil {
				return nil, saveErr
			}
		}
		if prunePersistedSessionBindings(&store.state, time.Now().Add(-sessionBindingTTL)) {
			if err := store.saveLocked(); err != nil {
				return nil, err
			}
		}
		return store, nil
	}
	if os.IsNotExist(err) {
		if backup, backupErr := os.ReadFile(path + ".bak"); backupErr == nil {
			if decodeErr := json.Unmarshal(backup, &store.state); decodeErr != nil {
				return nil, decodeErr
			}
			if saveErr := store.saveLocked(); saveErr != nil {
				return nil, saveErr
			}
			return store, nil
		}
	}
	if !os.IsNotExist(err) {
		return nil, err
	}
	if legacyConfig != "" {
		var legacy struct {
			APIKeys []string `json:"API_KEYS"`
		}
		if data, readErr := os.ReadFile(legacyConfig); readErr == nil {
			_ = json.Unmarshal(data, &legacy)
			store.state.APIKeys = uniqueStrings(legacy.APIKeys)
		}
	}
	if legacyPool != "" {
		if data, readErr := os.ReadFile(legacyPool); readErr == nil {
			_ = json.Unmarshal(data, &store.state.Proxies)
			for index := range store.state.Proxies {
				store.state.Proxies[index].Alive = false
				store.state.Proxies[index].LatencyMS = 0
				store.state.Proxies[index].LastChecked = time.Time{}
				store.state.Proxies[index].Detail = "stale_import"
			}
		}
	}
	if err := store.saveLocked(); err != nil {
		return nil, err
	}
	return store, nil
}

func prunePersistedSessionBindings(state *gatewayState, cutoff time.Time) bool {
	changed := false
	for sessionID, binding := range state.AccountSessions {
		if binding.Updated.Before(cutoff) {
			delete(state.AccountSessions, sessionID)
			changed = true
		}
	}
	for history, binding := range state.ConversationSessions {
		if binding.Updated.Before(cutoff) {
			delete(state.ConversationSessions, history)
			changed = true
		}
	}
	return changed
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]bool)
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
}

func (s *stateStore) saveLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	if previous, readErr := os.ReadFile(s.path); readErr == nil && json.Valid(previous) {
		if err := atomicWriteFile(s.path+".bak", previous, 0o600); err != nil {
			return fmt.Errorf("write state backup: %w", err)
		}
	}
	if err := atomicWriteFile(s.path, data, 0o600); err != nil {
		return fmt.Errorf("write state: %w", err)
	}
	return nil
}

func atomicWriteFile(path string, data []byte, mode os.FileMode) error {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, filepath.Base(path)+".tmp-")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(mode); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func (s *stateStore) selectedProxy() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.SelectedProxy
}

type adminService struct {
	store     *stateStore
	server    *server
	auth      *adminAuthenticator
	runtimeMu sync.RWMutex
	lastError string
	lastAt    time.Time
}

func newAdminService(store *stateStore, server *server, username, password string) *adminService {
	return &adminService{store: store, server: server, auth: newAdminAuthenticator(username, password)}
}

func (a *adminService) register(mux *http.ServeMux) {
	mux.HandleFunc("GET /login", a.loginPage)
	mux.HandleFunc("POST /api/admin/login", a.auth.login)
	mux.HandleFunc("POST /api/admin/logout", a.auth.require(a.auth.logout))
	mux.HandleFunc("GET /", a.auth.require(a.index))
	mux.HandleFunc("GET /webui.js", a.auth.require(a.script))
	mux.HandleFunc("GET /api/webui/status", a.auth.require(a.status))
	mux.HandleFunc("GET /api/webui/tokens", a.auth.require(a.accounts))
	mux.HandleFunc("POST /api/webui/accounts/login", a.auth.require(a.startAccountLogin))
	mux.HandleFunc("GET /api/webui/accounts/login/status", a.auth.require(a.accountLoginStatus))
	mux.HandleFunc("POST /api/webui/accounts/login/cancel", a.auth.require(a.cancelAccountLogin))
	mux.HandleFunc("POST /api/webui/accounts/toggle", a.auth.require(a.toggleAccount))
	mux.HandleFunc("GET /api/webui/accounts/config", a.auth.require(a.accountConfig))
	mux.HandleFunc("POST /api/webui/accounts/config", a.auth.require(a.saveAccountConfig))
	mux.HandleFunc("GET /api/webui/apis", a.auth.require(a.listAPIKeys))
	mux.HandleFunc("POST /api/webui/apis/create", a.auth.require(a.createAPIKey))
	mux.HandleFunc("POST /api/webui/apis/delete", a.auth.require(a.deleteAPIKey))
	mux.HandleFunc("GET /api/webui/usage", a.auth.require(a.usage))
	mux.HandleFunc("GET /api/webui/proxy/pool", a.auth.require(a.proxyPool))
	mux.HandleFunc("POST /api/webui/proxy/pool", a.auth.require(a.saveProxyPool))
	mux.HandleFunc("POST /api/webui/proxy/refresh", a.auth.require(a.refreshProxies))
	mux.HandleFunc("POST /api/webui/proxy/select", a.auth.require(a.selectProxy))
	mux.HandleFunc("POST /api/webui/proxy/egress-tier", a.auth.require(a.refreshEgressTier))
}

func (a *adminService) loginPage(w http.ResponseWriter, r *http.Request) {
	if a.auth.validSession(r) {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(adminLoginPage))
}

func (a *adminService) requireAPIKey(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		a.store.mu.RLock()
		keys := append([]string(nil), a.store.state.APIKeys...)
		a.store.mu.RUnlock()
		if len(keys) == 0 {
			writeError(w, http.StatusServiceUnavailable, "gateway has no API key configured", "api_key_not_configured")
			return
		}
		provided := strings.TrimSpace(r.Header.Get("X-API-Key"))
		if auth := strings.TrimSpace(r.Header.Get("Authorization")); strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			provided = strings.TrimSpace(auth[7:])
		}
		for _, key := range keys {
			if provided == key {
				r.Header.Set("X-Gateway-API-Key", key)
				next(w, r)
				return
			}
		}
		writeError(w, http.StatusUnauthorized, "invalid API key", "authentication_error")
	}
}

func apiKeyFromRequest(r *http.Request) string {
	if value := r.Header.Get("X-Gateway-API-Key"); value != "" {
		return value
	}
	return "未鉴权"
}

func (a *adminService) recordUsage(model, apiKey string, inputChars, outputChars int, success bool, duration time.Duration, requestErr error) {
	record := usageRecord{
		At: time.Now(), Model: model, APIKey: apiKey, Success: success,
		EstimatedTokens: (inputChars + outputChars + 3) / 4, DurationMS: duration.Milliseconds(),
	}
	a.store.mu.Lock()
	a.store.state.Usage = append(a.store.state.Usage, record)
	if len(a.store.state.Usage) > 2000 {
		a.store.state.Usage = append([]usageRecord(nil), a.store.state.Usage[len(a.store.state.Usage)-2000:]...)
	}
	_ = a.store.saveLocked()
	a.store.mu.Unlock()
	a.runtimeMu.Lock()
	a.lastAt = time.Now()
	if requestErr != nil {
		a.lastError = requestErr.Error()
	} else {
		a.lastError = ""
	}
	a.runtimeMu.Unlock()
}

func (a *adminService) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(webUIPage))
}

func (a *adminService) script(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(webUIScript))
}

func (a *adminService) status(w http.ResponseWriter, _ *http.Request) {
	a.store.mu.RLock()
	keys, proxies, selected := len(a.store.state.APIKeys), len(a.store.state.Proxies), a.store.state.SelectedProxy
	egressTier := a.store.state.EgressTier
	a.store.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"accounts": a.server.accounts.count(), "api_keys": keys, "proxies": proxies, "selected_proxy": selected,
		"active_sessions": a.server.sessions.count(), "cli_running": a.server.accounts.running(), "egress_tier": egressTier,
	})
}

func (a *adminService) accounts(w http.ResponseWriter, _ *http.Request) {
	a.store.mu.RLock()
	selected := a.store.state.SelectedProxy
	a.store.mu.RUnlock()
	writeJSON(w, http.StatusOK, a.server.accounts.snapshots(selected))
}

func (a *adminService) accountConfig(w http.ResponseWriter, _ *http.Request) {
	bundle, err := a.server.accounts.exportPortableAccounts()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error(), "account_config_export_failed")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	writeJSON(w, http.StatusOK, bundle)
}

func (a *adminService) saveAccountConfig(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	var bundle portableAccountBundle
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&bundle); err != nil {
		writeError(w, http.StatusBadRequest, "invalid account configuration JSON", "invalid_account_config")
		return
	}
	updated, added, err := a.server.accounts.importPortableAccounts(bundle)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "account_config_save_failed")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "updated": updated, "added": added})
}

func (a *adminService) startAccountLogin(w http.ResponseWriter, _ *http.Request) {
	attempt, err := a.server.accounts.startLogin()
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "login_start_failed")
		return
	}
	writeJSON(w, http.StatusAccepted, attempt)
}

func (a *adminService) accountLoginStatus(w http.ResponseWriter, r *http.Request) {
	attempt, found := a.server.accounts.loginStatus(strings.TrimSpace(r.URL.Query().Get("id")))
	if !found {
		writeError(w, http.StatusNotFound, "login attempt not found", "not_found")
		return
	}
	writeJSON(w, http.StatusOK, attempt)
}

func (a *adminService) cancelAccountLogin(w http.ResponseWriter, r *http.Request) {
	var request struct {
		ID string `json:"id"`
	}
	if json.NewDecoder(r.Body).Decode(&request) != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request", "invalid_request")
		return
	}
	if err := a.server.accounts.cancelLogin(strings.TrimSpace(request.ID)); err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "login_cancel_failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *adminService) toggleAccount(w http.ResponseWriter, r *http.Request) {
	var request struct {
		ID      string `json:"id"`
		Enabled bool   `json:"enabled"`
	}
	if json.NewDecoder(r.Body).Decode(&request) != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request", "invalid_request")
		return
	}
	if err := a.server.accounts.toggle(strings.TrimSpace(request.ID), request.Enabled); err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "account_update_failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *adminService) listAPIKeys(w http.ResponseWriter, _ *http.Request) {
	a.store.mu.RLock()
	keys := append([]string(nil), a.store.state.APIKeys...)
	a.store.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{"keys": keys})
}

func randomAPIKey() string {
	var raw [18]byte
	_, _ = cryptorand.Read(raw[:])
	return "sk-fb-" + hex.EncodeToString(raw[:])
}

func (a *adminService) createAPIKey(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Key string `json:"key"`
	}
	_ = json.NewDecoder(r.Body).Decode(&request)
	request.Key = strings.TrimSpace(request.Key)
	if request.Key == "" {
		request.Key = randomAPIKey()
	}
	a.store.mu.Lock()
	a.store.state.APIKeys = uniqueStrings(append(a.store.state.APIKeys, request.Key))
	err := a.store.saveLocked()
	a.store.mu.Unlock()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error(), "state_write_failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"key": request.Key})
}

func (a *adminService) deleteAPIKey(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Key string `json:"key"`
	}
	if json.NewDecoder(r.Body).Decode(&request) != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request", "invalid_request")
		return
	}
	a.store.mu.Lock()
	filtered := a.store.state.APIKeys[:0]
	for _, key := range a.store.state.APIKeys {
		if key != request.Key {
			filtered = append(filtered, key)
		}
	}
	a.store.state.APIKeys = filtered
	err := a.store.saveLocked()
	a.store.mu.Unlock()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error(), "state_write_failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *adminService) usage(w http.ResponseWriter, r *http.Request) {
	days := 1
	_, _ = fmt.Sscanf(r.URL.Query().Get("days"), "%d", &days)
	if days < 1 || days > 365 {
		days = 1
	}
	cutoff := time.Now().Add(-time.Duration(days) * 24 * time.Hour)
	byModel, byKey := map[string]int{}, map[string]int{}
	total, successful := 0, 0
	a.store.mu.RLock()
	for _, record := range a.store.state.Usage {
		if record.At.Before(cutoff) {
			continue
		}
		total++
		if record.Success {
			successful++
		}
		byModel[record.Model] += record.EstimatedTokens
		byKey[record.APIKey] += record.EstimatedTokens
	}
	a.store.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"by_model": byModel, "by_key": byKey, "requests": total, "successful": successful,
		"note": "Token 为本地字符数估算，不代表官方额度",
	})
}

func (a *adminService) proxyPool(w http.ResponseWriter, _ *http.Request) {
	a.store.mu.RLock()
	nodes := append([]proxyNode(nil), a.store.state.Proxies...)
	selected := a.store.state.SelectedProxy
	egressTier := a.store.state.EgressTier
	a.store.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{"items": nodes, "selected": selected, "egress_tier": egressTier})
}

func probeOfficialEgressTier(ctx context.Context, proxyAddr, endpoint string) egressTierStatus {
	status := egressTierStatus{Tier: "unknown", CheckedAt: time.Now()}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if strings.TrimSpace(proxyAddr) != "" {
		parsed, err := url.Parse(proxyAddr)
		if err != nil {
			status.Detail = "invalid_proxy"
			return status
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			status.Detail = "tier_probe_requires_http_proxy"
			return status
		}
		transport.Proxy = http.ProxyURL(parsed)
	}
	client := &http.Client{Transport: transport, Timeout: 15 * time.Second}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		status.Detail = "request_error"
		return status
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("x-fb-timezone", "UTC")
	request.Header.Set("x-fb-tz-offset", "0")
	request.Header.Set("x-fb-languages", "en-US")
	started := time.Now()
	response, err := client.Do(request)
	status.LatencyMS = time.Since(started).Milliseconds()
	if err != nil {
		status.Detail = "tier_probe_failed"
		return status
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		status.Detail = fmt.Sprintf("tier_probe_http_%d", response.StatusCode)
		return status
	}
	var body struct {
		AccessTier string `json:"accessTier"`
	}
	if json.NewDecoder(response.Body).Decode(&body) != nil {
		status.Detail = "tier_probe_invalid"
		return status
	}
	tier := strings.ToLower(strings.TrimSpace(body.AccessTier))
	if tier != "full" && tier != "limited" {
		status.Detail = "tier_probe_missing"
		return status
	}
	status.Tier = tier
	status.Detail = "official_public_tier"
	return status
}

func (a *adminService) updateEgressTier(ctx context.Context) egressTierStatus {
	proxyAddr := a.store.selectedProxy()
	status := probeOfficialEgressTier(ctx, proxyAddr, officialAccessTierURL)
	a.store.mu.Lock()
	a.store.state.EgressTier = status
	_ = a.store.saveLocked()
	a.store.mu.Unlock()
	return status
}

func (a *adminService) refreshEgressTier(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.updateEgressTier(r.Context()))
}

func (a *adminService) startEgressTierMonitor(ctx context.Context) {
	go func() {
		a.updateEgressTier(ctx)
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.updateEgressTier(ctx)
			}
		}
	}()
}

func normalizeProxy(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("proxy address is empty")
	}
	if !strings.Contains(value, "://") {
		value = "http://" + value
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Hostname() == "" || parsed.Port() == "" {
		return "", fmt.Errorf("invalid proxy address: %s", value)
	}
	switch parsed.Scheme {
	case "http", "https", "socks5":
	default:
		return "", fmt.Errorf("unsupported proxy scheme: %s", parsed.Scheme)
	}
	return parsed.String(), nil
}

func (a *adminService) saveProxyPool(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Items []string `json:"items"`
	}
	if json.NewDecoder(r.Body).Decode(&request) != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request", "invalid_request")
		return
	}
	unique := map[string]bool{}
	nodes := make([]proxyNode, 0, len(request.Items))
	for _, item := range request.Items {
		addr, err := normalizeProxy(item)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), "invalid_proxy")
			return
		}
		if !unique[addr] {
			unique[addr] = true
			nodes = append(nodes, proxyNode{Addr: addr, Mode: "unknown"})
		}
	}
	a.store.mu.Lock()
	previous := make(map[string]proxyNode, len(a.store.state.Proxies))
	for _, node := range a.store.state.Proxies {
		previous[node.Addr] = node
	}
	for index := range nodes {
		if old, ok := previous[nodes[index].Addr]; ok {
			nodes[index] = old
		}
	}
	a.store.state.Proxies = nodes
	clearedProxy := false
	if !unique[a.store.state.SelectedProxy] {
		a.store.state.SelectedProxy = ""
		clearedProxy = true
	}
	err := a.store.saveLocked()
	a.store.mu.Unlock()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error(), "state_write_failed")
		return
	}
	if clearedProxy {
		a.server.accounts.setProxy("")
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "count": len(nodes)})
}

func checkProxy(node proxyNode) proxyNode {
	started := time.Now()
	parsed, err := url.Parse(node.Addr)
	if err == nil {
		connection, dialErr := net.DialTimeout("tcp", parsed.Host, 2500*time.Millisecond)
		err = dialErr
		if connection != nil {
			_ = connection.Close()
		}
	}
	now := time.Now()
	node.LastChecked = now
	node.LatencyMS = time.Since(started).Milliseconds()
	node.Alive = err == nil
	if err == nil {
		node.LastOK = now
		node.FailCount = 0
		node.Detail = "tcp_ok"
	} else {
		node.FailCount++
		node.Detail = "offline"
	}
	return node
}

func (a *adminService) refreshProxies(w http.ResponseWriter, _ *http.Request) {
	a.store.mu.RLock()
	nodes := append([]proxyNode(nil), a.store.state.Proxies...)
	a.store.mu.RUnlock()
	var wait sync.WaitGroup
	limit := make(chan struct{}, 12)
	for index := range nodes {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			limit <- struct{}{}
			nodes[index] = checkProxy(nodes[index])
			<-limit
		}(index)
	}
	wait.Wait()
	sort.SliceStable(nodes, func(i, j int) bool {
		if nodes[i].Alive != nodes[j].Alive {
			return nodes[i].Alive
		}
		return nodes[i].LatencyMS < nodes[j].LatencyMS
	})
	a.store.mu.Lock()
	a.store.state.Proxies = nodes
	err := a.store.saveLocked()
	a.store.mu.Unlock()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error(), "state_write_failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": nodes})
}

func (a *adminService) selectProxy(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Addr string `json:"addr"`
	}
	if json.NewDecoder(r.Body).Decode(&request) != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request", "invalid_request")
		return
	}
	request.Addr = strings.TrimSpace(request.Addr)
	a.store.mu.Lock()
	valid := request.Addr == ""
	for _, node := range a.store.state.Proxies {
		if node.Addr == request.Addr && node.Alive {
			valid = true
			break
		}
	}
	if !valid {
		a.store.mu.Unlock()
		writeError(w, http.StatusBadRequest, "only a reachable proxy can be selected", "proxy_unavailable")
		return
	}
	a.store.state.SelectedProxy = request.Addr
	err := a.store.saveLocked()
	a.store.mu.Unlock()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error(), "state_write_failed")
		return
	}
	a.server.accounts.setProxy(request.Addr)
	status := a.updateEgressTier(r.Context())
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "selected": request.Addr, "egress_tier": status})
}
