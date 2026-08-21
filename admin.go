package main

import (
	"bufio"
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const officialAccessTierURL = "https://freebuff.com/api/web/access-tier"

const ipCapProxyCooldown = 10 * time.Minute

type egressTierStatus struct {
	Tier      string    `json:"tier"`
	Detail    string    `json:"detail,omitempty"`
	CheckedAt time.Time `json:"checked_at,omitempty"`
	LatencyMS int64     `json:"latency_ms,omitempty"`
}

type proxyNode struct {
	Addr          string    `json:"addr"`
	Name          string    `json:"name,omitempty"`
	Mode          string    `json:"mode,omitempty"`
	TierCheckedAt time.Time `json:"tier_checked_at,omitempty"`
	TierLatencyMS int64     `json:"tier_latency_ms,omitempty"`
	TierDetail    string    `json:"tier_detail,omitempty"`
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
	APIKeys               []string                         `json:"api_keys"`
	Accounts              []accountConfig                  `json:"accounts,omitempty"`
	DefaultAccountChecked bool                             `json:"default_account_checked,omitempty"`
	AccountSessions       map[string]accountSessionBinding `json:"account_sessions,omitempty"`
	ConversationSessions  map[string]sessionBinding        `json:"conversation_sessions,omitempty"`
	Proxies               []proxyNode                      `json:"proxies"`
	SelectedProxy         string                           `json:"selected_proxy,omitempty"`
	EgressTier            egressTierStatus                 `json:"egress_tier,omitempty"`
	Usage                 []usageRecord                    `json:"usage,omitempty"`
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
	store           *stateStore
	server          *server
	auth            *adminAuthenticator
	mihomoConfig    string
	runtimeMu       sync.RWMutex
	proxyScanMu     sync.Mutex
	proxyFailoverMu sync.Mutex
	lastError       string
	lastAt          time.Time
}

func newAdminService(store *stateStore, server *server, username, password, mihomoConfig string) *adminService {
	return &adminService{store: store, server: server, auth: newAdminAuthenticator(username, password), mihomoConfig: mihomoConfig}
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
	mux.HandleFunc("POST /api/webui/accounts/delete", a.auth.require(a.deleteAccount))
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
	mux.HandleFunc("POST /api/webui/proxy/scan-full", a.auth.require(a.scanFullProxies))
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

func (a *adminService) deleteAccount(w http.ResponseWriter, r *http.Request) {
	var request struct {
		ID string `json:"id"`
	}
	if json.NewDecoder(r.Body).Decode(&request) != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request", "invalid_request")
		return
	}
	if err := a.server.accounts.remove(strings.TrimSpace(request.ID)); err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "account_delete_failed")
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
	nodes = a.enrichProxyNames(nodes)
	writeJSON(w, http.StatusOK, map[string]any{"items": nodes, "selected": selected, "egress_tier": egressTier})
}

func yamlScalar(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 2 && value[0] == '"' {
		if decoded, err := strconv.Unquote(value); err == nil {
			return decoded
		}
	}
	if len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'' {
		return strings.ReplaceAll(value[1:len(value)-1], "''", "'")
	}
	if index := strings.Index(value, " #"); index >= 0 {
		value = value[:index]
	}
	return strings.TrimSpace(value)
}

func loadMihomoListenerNames(path string) (map[string]string, error) {
	file, err := os.Open(strings.TrimSpace(path))
	if err != nil {
		return nil, err
	}
	defer file.Close()
	names := make(map[string]string)
	scanner := bufio.NewScanner(file)
	inListeners := false
	port, proxy := "", ""
	flush := func() {
		if port != "" && proxy != "" {
			names[port] = proxy
		}
		port, proxy = "", ""
	}
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if !inListeners {
			inListeners = trimmed == "listeners:"
			continue
		}
		if trimmed != "" && len(line) > 0 && line[0] != ' ' && line[0] != '\t' {
			break
		}
		if strings.HasPrefix(trimmed, "- name:") {
			flush()
			continue
		}
		if strings.HasPrefix(trimmed, "port:") {
			port = yamlScalar(strings.TrimSpace(strings.TrimPrefix(trimmed, "port:")))
		}
		if strings.HasPrefix(trimmed, "proxy:") {
			proxy = yamlScalar(strings.TrimSpace(strings.TrimPrefix(trimmed, "proxy:")))
		}
	}
	flush()
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return names, nil
}

func (a *adminService) enrichProxyNames(nodes []proxyNode) []proxyNode {
	if strings.TrimSpace(a.mihomoConfig) == "" {
		return nodes
	}
	names, err := loadMihomoListenerNames(a.mihomoConfig)
	if err != nil {
		log.Printf("load Mihomo listener names: %v", err)
		return nodes
	}
	for index := range nodes {
		parsed, err := url.Parse(nodes[index].Addr)
		if err != nil {
			continue
		}
		nodes[index].Name = names[parsed.Port()]
	}
	return nodes
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

func tierRank(mode string) int {
	switch strings.ToLower(mode) {
	case "full":
		return 0
	case "limited":
		return 1
	default:
		return 2
	}
}

func scanOfficialProxyTiers(ctx context.Context, nodes []proxyNode, endpoint string, concurrency int) []proxyNode {
	if concurrency < 1 {
		concurrency = 1
	}
	var wait sync.WaitGroup
	limit := make(chan struct{}, concurrency)
	for index := range nodes {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			limit <- struct{}{}
			status := probeOfficialEgressTier(ctx, nodes[index].Addr, endpoint)
			<-limit
			nodes[index].Mode = status.Tier
			nodes[index].TierCheckedAt = status.CheckedAt
			nodes[index].TierLatencyMS = status.LatencyMS
			nodes[index].TierDetail = status.Detail
			nodes[index].LastChecked = status.CheckedAt
			nodes[index].Alive = status.Tier == "full" || status.Tier == "limited"
			if nodes[index].Alive {
				nodes[index].LastOK = status.CheckedAt
				nodes[index].FailCount = 0
			} else {
				nodes[index].FailCount++
			}
		}(index)
	}
	wait.Wait()
	sort.SliceStable(nodes, func(i, j int) bool {
		left, right := tierRank(nodes[i].Mode), tierRank(nodes[j].Mode)
		if left != right {
			return left < right
		}
		return nodes[i].TierLatencyMS < nodes[j].TierLatencyMS
	})
	return nodes
}

func selectScannedProxy(nodes []proxyNode, previousSelected string, forceLowest bool) (string, egressTierStatus, int) {
	selected := previousSelected
	fullCount := 0
	var fastestFull *proxyNode
	var current *proxyNode
	for index := range nodes {
		node := &nodes[index]
		if node.Mode == "full" {
			fullCount++
			if fastestFull == nil {
				fastestFull = node
			}
		}
		if node.Addr == previousSelected {
			current = node
		}
	}
	if fastestFull != nil && (forceLowest || current == nil || current.Mode != "full") {
		selected = fastestFull.Addr
		current = fastestFull
	}
	if current == nil || current.Addr != selected {
		current = nil
		for index := range nodes {
			if nodes[index].Addr == selected {
				current = &nodes[index]
				break
			}
		}
	}
	if current == nil {
		return selected, egressTierStatus{Tier: "unknown", Detail: "selected_proxy_not_in_pool", CheckedAt: time.Now()}, fullCount
	}
	return selected, egressTierStatus{
		Tier: current.Mode, Detail: current.TierDetail, CheckedAt: current.TierCheckedAt, LatencyMS: current.TierLatencyMS,
	}, fullCount
}

func (a *adminService) scanProxyPool(ctx context.Context, forceLowest bool) ([]proxyNode, string, egressTierStatus, int, error) {
	a.proxyScanMu.Lock()
	defer a.proxyScanMu.Unlock()

	a.store.mu.RLock()
	nodes := append([]proxyNode(nil), a.store.state.Proxies...)
	previousSelected := a.store.state.SelectedProxy
	a.store.mu.RUnlock()
	if len(nodes) == 0 {
		return nil, "", egressTierStatus{}, 0, fmt.Errorf("IP 池为空，请先保存节点")
	}

	nodes = scanOfficialProxyTiers(ctx, nodes, officialAccessTierURL, 8)
	nodes = a.enrichProxyNames(nodes)
	selected, selectedTier, fullCount := selectScannedProxy(nodes, previousSelected, forceLowest)
	a.store.mu.Lock()
	a.store.state.Proxies = nodes
	a.store.state.SelectedProxy = selected
	a.store.state.EgressTier = selectedTier
	err := a.store.saveLocked()
	a.store.mu.Unlock()
	if err != nil {
		return nil, "", egressTierStatus{}, 0, err
	}
	if a.server != nil && a.server.accounts != nil {
		a.server.accounts.reconcileFullProxies(nodes)
	}
	return nodes, selected, selectedTier, fullCount, nil
}

func (a *adminService) scanFullProxies(w http.ResponseWriter, r *http.Request) {
	nodes, selected, selectedTier, fullCount, err := a.scanProxyPool(r.Context(), true)
	if err != nil {
		status, code := http.StatusInternalServerError, "state_write_failed"
		if strings.Contains(err.Error(), "IP 池为空") {
			status, code = http.StatusBadRequest, "proxy_pool_empty"
		}
		writeError(w, status, err.Error(), code)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": nodes, "selected": selected, "full_count": fullCount, "egress_tier": selectedTier,
	})
}

func (a *adminService) startEgressTierMonitor(ctx context.Context) {
	go func() {
		scan := func() {
			_, selected, tier, fullCount, err := a.scanProxyPool(ctx, false)
			if err != nil {
				log.Printf("automatic FULL proxy scan failed: %v", err)
				return
			}
			log.Printf("automatic FULL proxy scan completed: full=%d selected=%s tier=%s", fullCount, selected, tier.Tier)
		}
		scan()
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				scan()
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

func (a *adminService) rotateProxyAfterIPCap(accountIDs ...string) bool {
	a.proxyFailoverMu.Lock()
	defer a.proxyFailoverMu.Unlock()
	a.proxyScanMu.Lock()
	defer a.proxyScanMu.Unlock()

	now := time.Now()
	a.store.mu.Lock()
	current := a.store.state.SelectedProxy
	accountID := ""
	if len(accountIDs) > 0 {
		accountID = strings.TrimSpace(accountIDs[0])
		for _, account := range a.store.state.Accounts {
			if account.ID == accountID && account.Proxy != "" {
				current = account.Proxy
				break
			}
		}
	}
	if current == "" {
		a.store.mu.Unlock()
		return false
	}
	for index := range a.store.state.Proxies {
		if a.store.state.Proxies[index].Addr == current {
			a.store.state.Proxies[index].CooldownUntil = now.Add(ipCapProxyCooldown)
			a.store.state.Proxies[index].Detail = "upstream_ip_capped"
			break
		}
	}
	best := -1
	for index := range a.store.state.Proxies {
		node := &a.store.state.Proxies[index]
		if node.Addr == current || node.Mode != "full" || !node.Alive || node.CooldownUntil.After(now) {
			continue
		}
		if best < 0 || node.TierLatencyMS < a.store.state.Proxies[best].TierLatencyMS {
			best = index
		}
	}
	if best < 0 {
		_ = a.store.saveLocked()
		a.store.mu.Unlock()
		return false
	}
	next := a.store.state.Proxies[best].Addr
	if accountID == "" {
		a.store.state.SelectedProxy = next
		a.store.state.EgressTier = egressTierStatus{
			Tier: a.store.state.Proxies[best].Mode, Detail: a.store.state.Proxies[best].TierDetail,
			CheckedAt: a.store.state.Proxies[best].TierCheckedAt, LatencyMS: a.store.state.Proxies[best].TierLatencyMS,
		}
	}
	err := a.store.saveLocked()
	a.store.mu.Unlock()
	if err != nil {
		log.Printf("persist IP-cap proxy failover: %v", err)
		return false
	}
	if a.server != nil && a.server.accounts != nil {
		if accountID != "" {
			if !a.server.accounts.setAccountProxy(accountID, next) {
				return false
			}
		} else {
			a.server.accounts.setProxy(next)
		}
	}
	log.Printf("rotated Freebuff exit after ip_capped: account=%s %s -> %s", accountID, current, next)
	return true
}

// rotateProxyAfterAdmission changes only the failing account's exit.  A
// controller rejection is scoped to an account+exit pair, so other accounts
// keep their own working CLI session and tool chain.
func (a *adminService) rotateProxyAfterAdmission(accountID string) bool {
	if a == nil || a.server == nil || a.server.accounts == nil || accountID == "" {
		return false
	}
	a.proxyFailoverMu.Lock()
	defer a.proxyFailoverMu.Unlock()
	now := time.Now()
	a.store.mu.Lock()
	current := ""
	for _, account := range a.store.state.Accounts {
		if account.ID == accountID {
			current = account.Proxy
			break
		}
	}
	if current == "" {
		current = a.store.state.SelectedProxy
	}
	best := -1
	for index := range a.store.state.Proxies {
		node := &a.store.state.Proxies[index]
		if node.Addr == current {
			continue
		}
		if node.Mode != "full" || !node.Alive || node.CooldownUntil.After(now) {
			continue
		}
		if best == -1 || node.TierLatencyMS < a.store.state.Proxies[best].TierLatencyMS {
			best = index
		}
	}
	next := ""
	if best >= 0 {
		next = a.store.state.Proxies[best].Addr
	}
	err := a.store.saveLocked()
	a.store.mu.Unlock()
	if err != nil {
		log.Printf("persist admission failover: %v", err)
	}
	if next == "" {
		return false
	}
	return a.server.accounts.setAccountProxy(accountID, next)
}
