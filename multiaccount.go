package main

import (
	"bufio"
	"bytes"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

type accountConfig struct {
	ID                     string               `json:"id"`
	Label                  string               `json:"label"`
	ConfigDir              string               `json:"config_dir"`
	Proxy                  string               `json:"proxy,omitempty"`
	AdmissionProxy         string               `json:"admission_proxy,omitempty"`
	AdmissionCooldownUntil time.Time            `json:"admission_cooldown_until,omitempty"`
	AdmissionCooldowns     map[string]time.Time `json:"admission_cooldowns,omitempty"`
	Enabled                bool                 `json:"enabled"`
	AddedAt                time.Time            `json:"added_at"`
}

type accountCredential struct {
	Label         string
	Email         string
	Authenticated bool
}

func parseAccountCredential(data []byte) accountCredential {
	result := accountCredential{Label: "官方 CLI 账号"}
	var root map[string]json.RawMessage
	if json.Unmarshal(data, &root) != nil {
		return result
	}
	var credentials struct {
		Name      string `json:"name"`
		Email     string `json:"email"`
		AuthToken string `json:"authToken"`
	}
	if json.Unmarshal(root["default"], &credentials) != nil {
		return result
	}
	result.Authenticated = strings.TrimSpace(credentials.AuthToken) != ""
	result.Email = strings.TrimSpace(credentials.Email)
	if result.Email != "" {
		result.Label = result.Email
	} else if name := strings.TrimSpace(credentials.Name); name != "" {
		result.Label = name
	}
	return result
}

func readAccountCredential(configDir string) accountCredential {
	data, err := os.ReadFile(filepath.Join(configDir, "credentials.json"))
	if err != nil {
		return accountCredential{Label: "官方 CLI 账号"}
	}
	return parseAccountCredential(data)
}

type portableAccount struct {
	ID          string          `json:"id,omitempty"`
	Label       string          `json:"label"`
	Enabled     bool            `json:"enabled"`
	Credentials json.RawMessage `json:"credentials"`
}

type portableAccountBundle struct {
	Version  int               `json:"version"`
	Accounts []portableAccount `json:"accounts"`
}

func ensureDefaultAccount(store *stateStore, configDir string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.state.DefaultAccountChecked || len(store.state.Accounts) > 0 {
		return nil
	}
	credential := readAccountCredential(configDir)
	if !credential.Authenticated {
		return nil
	}
	store.state.DefaultAccountChecked = true
	store.state.Accounts = []accountConfig{{
		ID: "account-default", Label: credential.Label, ConfigDir: configDir,
		Enabled: true, AddedAt: time.Now(),
	}}
	return store.saveLocked()
}

type accountRuntime struct {
	config            accountConfig
	client            *cliClient
	active            int
	activeModel       string
	lastUsed          time.Time
	lastError         string
	cooldownUntil     time.Time
	admissionCooldown map[string]time.Time
	reconfiguring     bool
}

const (
	admissionCooldown = 10 * time.Minute
	accountCooldown   = 5 * time.Minute
)

type accountSessionBinding struct {
	AccountID string    `json:"account_id"`
	Updated   time.Time `json:"updated_at"`
}

type loginAttempt struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	URL       string    `json:"url,omitempty"`
	Error     string    `json:"error,omitempty"`
	ConfigDir string    `json:"-"`
	StartedAt time.Time `json:"started_at"`
	cmd       *exec.Cmd
}

type accountManager struct {
	mu              sync.Mutex
	configuring     bool
	store           *stateStore
	cliPath         string
	loginCLIPath    string
	cwd             string
	accountsRoot    string
	runtimes        map[string]*accountRuntime
	sessionAccounts map[string]accountSessionBinding
	loginAttempts   map[string]*loginAttempt
}

func newAccountManager(store *stateStore, cliPath, loginCLIPath, cwd, accountsRoot string) *accountManager {
	manager := &accountManager{
		store: store, cliPath: cliPath, loginCLIPath: loginCLIPath, cwd: cwd, accountsRoot: accountsRoot,
		runtimes: make(map[string]*accountRuntime), sessionAccounts: make(map[string]accountSessionBinding),
		loginAttempts: make(map[string]*loginAttempt),
	}
	store.mu.RLock()
	configs := append([]accountConfig(nil), store.state.Accounts...)
	proxy := store.state.SelectedProxy
	persistedBindings := make(map[string]accountSessionBinding, len(store.state.AccountSessions))
	for sessionID, binding := range store.state.AccountSessions {
		persistedBindings[sessionID] = binding
	}
	store.mu.RUnlock()
	for _, config := range configs {
		if config.Proxy == "" {
			config.Proxy = proxy
		}
		cooldowns := make(map[string]time.Time)
		for proxy, until := range config.AdmissionCooldowns {
			if until.After(time.Now()) {
				cooldowns[proxy] = until
			}
		}
		if config.AdmissionProxy != "" && config.AdmissionCooldownUntil.After(time.Now()) {
			cooldowns[config.AdmissionProxy] = config.AdmissionCooldownUntil
		}
		manager.runtimes[config.ID] = &accountRuntime{
			config:            config,
			client:            &cliClient{path: cliPath, cwd: cwd, configDir: config.ConfigDir, proxy: config.Proxy},
			admissionCooldown: cooldowns,
		}
	}
	cutoff := time.Now().Add(-sessionBindingTTL)
	for sessionID, binding := range persistedBindings {
		if binding.Updated.After(cutoff) && manager.runtimes[binding.AccountID] != nil {
			manager.sessionAccounts[sessionID] = binding
		}
	}
	go manager.reapIdleProcesses()
	return manager
}

func (m *accountManager) reapIdleProcesses() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		var clients []*cliClient
		cutoff := time.Now().Add(-sessionBindingTTL)
		bindingsChanged := false
		m.mu.Lock()
		for accountID, runtime := range m.runtimes {
			runtime.client.recoverExpiredToolCall(time.Now())
			hasBinding := false
			for _, binding := range m.sessionAccounts {
				if binding.AccountID == accountID {
					hasBinding = true
					break
				}
			}
			if runtime.active == 0 && !hasBinding && !runtime.lastUsed.IsZero() && time.Since(runtime.lastUsed) >= time.Minute {
				clients = append(clients, runtime.client)
			}
		}
		for sessionID, binding := range m.sessionAccounts {
			if binding.Updated.Before(cutoff) {
				delete(m.sessionAccounts, sessionID)
				bindingsChanged = true
			}
		}
		m.mu.Unlock()
		for _, client := range clients {
			client.stop()
		}
		if bindingsChanged {
			m.store.mu.Lock()
			for sessionID, binding := range m.store.state.AccountSessions {
				if binding.Updated.Before(cutoff) {
					delete(m.store.state.AccountSessions, sessionID)
				}
			}
			err := m.store.saveLocked()
			m.store.mu.Unlock()
			if err != nil {
				fmt.Fprintf(os.Stderr, "prune account session bindings: %v\n", err)
			}
		}
	}
}

func modelAffinityRank(activeModel, requestedModel string) int {
	if activeModel == requestedModel {
		return 0
	}
	if activeModel == "" {
		return 1
	}
	return 2
}

func (m *accountManager) acquire(sessionID, requestedModel string) (*cliClient, string, error) {
	rawSessionID := sessionID
	cliSessionID := scopedSessionID(requestedModel, rawSessionID)
	sessionID = accountSessionKey(rawSessionID)
	m.mu.Lock()
	if m.configuring {
		m.mu.Unlock()
		return nil, "", errors.New("account configuration is being updated")
	}
	now := time.Now()
	if binding, found := m.sessionAccounts[sessionID]; found && binding.Updated.After(now.Add(-sessionBindingTTL)) {
		runtime := m.runtimes[binding.AccountID]
		if runtime == nil || runtime.reconfiguring || !runtime.config.Enabled || !readAccountCredential(runtime.config.ConfigDir).Authenticated {
			m.mu.Unlock()
			return nil, "", errors.New("conversation account is unavailable")
		}
		admissionUntil := runtime.admissionCooldown[runtime.config.Proxy]
		if runtime.cooldownUntil.After(now) || admissionUntil.After(now) {
			m.mu.Unlock()
			until := runtime.cooldownUntil
			if admissionUntil.After(until) {
				until = admissionUntil
			}
			return nil, "", fmt.Errorf("conversation account is cooling down until %s", until.Format(time.RFC3339))
		}
		if runtime.active > 0 {
			m.mu.Unlock()
			return nil, "", errors.New("conversation account is busy")
		}
		runtime.client.recoverExpiredToolCall(now)
		if pendingSession := runtime.client.pendingSession(); pendingSession != "" && pendingSession != cliSessionID {
			m.mu.Unlock()
			return nil, "", errors.New("conversation account is waiting for another session's tool result")
		}
		runtime.active++
		binding.Updated = now
		m.sessionAccounts[sessionID] = binding
		m.mu.Unlock()
		m.persistAccountSession(sessionID, binding, true)
		return runtime.client, binding.AccountID, nil
	}
	delete(m.sessionAccounts, sessionID)
	var selected *accountRuntime
	for _, runtime := range m.runtimes {
		runtime.client.recoverExpiredToolCall(now)
		if runtime.reconfiguring || !runtime.config.Enabled || runtime.active > 0 || runtime.cooldownUntil.After(now) || runtime.admissionCooldown[runtime.config.Proxy].After(now) {
			continue
		}
		if !readAccountCredential(runtime.config.ConfigDir).Authenticated {
			continue
		}
		if pendingSession := runtime.client.pendingSession(); pendingSession != "" && pendingSession != cliSessionID {
			continue
		}
		if selected == nil || runtime.active < selected.active ||
			(runtime.active == selected.active && modelAffinityRank(runtime.activeModel, requestedModel) < modelAffinityRank(selected.activeModel, requestedModel)) ||
			(runtime.active == selected.active && modelAffinityRank(runtime.activeModel, requestedModel) == modelAffinityRank(selected.activeModel, requestedModel) && runtime.lastUsed.Before(selected.lastUsed)) {
			selected = runtime
		}
	}
	if selected == nil {
		m.mu.Unlock()
		return nil, "", errors.New("no authenticated account is currently available")
	}
	selected.active++
	binding := accountSessionBinding{AccountID: selected.config.ID, Updated: now}
	m.sessionAccounts[sessionID] = binding
	m.mu.Unlock()
	if err := m.persistAccountSession(sessionID, binding, true); err != nil {
		m.mu.Lock()
		if current := m.sessionAccounts[sessionID]; current == binding {
			delete(m.sessionAccounts, sessionID)
		}
		if selected.active > 0 {
			selected.active--
		}
		m.mu.Unlock()
		return nil, "", fmt.Errorf("persist account session binding: %w", err)
	}
	return selected.client, selected.config.ID, nil
}

func accountSessionKey(sessionID string) string {
	sum := sha256.Sum256([]byte(sessionID))
	return hex.EncodeToString(sum[:16])
}

func (m *accountManager) persistAccountSession(sessionID string, binding accountSessionBinding, save bool) error {
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	if m.store.state.AccountSessions == nil {
		m.store.state.AccountSessions = make(map[string]accountSessionBinding)
	}
	m.store.state.AccountSessions[sessionID] = binding
	if save {
		return m.store.saveLocked()
	}
	return nil
}

func (m *accountManager) finish(accountID, model, sessionID string, requestErr error) {
	m.mu.Lock()
	runtime := m.runtimes[accountID]
	if runtime == nil {
		m.mu.Unlock()
		return
	}
	if runtime.active > 0 {
		runtime.active--
	}
	runtime.lastUsed = time.Now()
	if requestErr == nil {
		runtime.activeModel = model
		runtime.lastError = ""
		m.mu.Unlock()
		return
	}
	delete(m.sessionAccounts, accountSessionKey(sessionID))
	runtime.lastError = requestErr.Error()
	lower := strings.ToLower(runtime.lastError)
	if strings.Contains(lower, "rate_limit") || strings.Contains(lower, "rate limit") ||
		strings.Contains(lower, "budget") || strings.Contains(lower, "quota") || strings.Contains(lower, "429") {
		runtime.cooldownUntil = time.Now().Add(15 * time.Minute)
	} else if strings.Contains(lower, "not authenticated") || strings.Contains(lower, "unauthorized") || strings.Contains(lower, "401") {
		runtime.cooldownUntil = time.Now().Add(30 * time.Minute)
	}
	m.mu.Unlock()
	m.store.mu.Lock()
	delete(m.store.state.AccountSessions, accountSessionKey(sessionID))
	err := m.store.saveLocked()
	m.store.mu.Unlock()
	if err != nil {
		fmt.Fprintf(os.Stderr, "remove failed account session binding: %v\n", err)
	}
}

func isSessionAdmissionFailure(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "session admission returned banned") ||
		strings.Contains(lower, "session admission returned") && strings.Contains(lower, "banned")
}

func (m *accountManager) markAdmissionFailure(accountID string) {
	now := time.Now()
	m.mu.Lock()
	runtime := m.runtimes[accountID]
	var config accountConfig
	if runtime != nil {
		if runtime.admissionCooldown == nil {
			runtime.admissionCooldown = make(map[string]time.Time)
		}
		until := now.Add(admissionCooldown)
		runtime.admissionCooldown[runtime.config.Proxy] = until
		runtime.config.AdmissionProxy = runtime.config.Proxy
		runtime.config.AdmissionCooldownUntil = until
		if runtime.config.AdmissionCooldowns == nil {
			runtime.config.AdmissionCooldowns = make(map[string]time.Time)
		}
		runtime.config.AdmissionCooldowns[runtime.config.Proxy] = until
		config = runtime.config
		runtime.lastError = "Freebuff session admission returned banned"
	}
	m.mu.Unlock()
	if runtime == nil {
		return
	}
	m.store.mu.Lock()
	for index := range m.store.state.Accounts {
		if m.store.state.Accounts[index].ID == accountID {
			m.store.state.Accounts[index].AdmissionProxy = config.AdmissionProxy
			m.store.state.Accounts[index].AdmissionCooldownUntil = config.AdmissionCooldownUntil
			m.store.state.Accounts[index].AdmissionCooldowns = config.AdmissionCooldowns
			break
		}
	}
	err := m.store.saveLocked()
	m.store.mu.Unlock()
	if err != nil {
		fmt.Fprintf(os.Stderr, "persist admission cooldown: %v\n", err)
	}
}

func (m *accountManager) coolAccount(accountID string) {
	m.mu.Lock()
	if runtime := m.runtimes[accountID]; runtime != nil {
		runtime.cooldownUntil = time.Now().Add(accountCooldown)
	}
	m.mu.Unlock()
}

func (m *accountManager) setAccountProxy(accountID, proxy string) bool {
	m.mu.Lock()
	runtime := m.runtimes[accountID]
	if runtime == nil || runtime.reconfiguring || runtime.active > 0 || runtime.client.pendingSession() != "" {
		m.mu.Unlock()
		return false
	}
	if runtime.config.Proxy == proxy {
		m.mu.Unlock()
		return true
	}
	previousProxy := runtime.config.Proxy
	runtime.reconfiguring = true
	runtime.config.Proxy = proxy
	client := runtime.client
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		if current := m.runtimes[accountID]; current != nil {
			current.reconfiguring = false
		}
		m.mu.Unlock()
	}()

	m.store.mu.Lock()
	for index := range m.store.state.Accounts {
		if m.store.state.Accounts[index].ID == accountID {
			m.store.state.Accounts[index].Proxy = proxy
			break
		}
	}
	err := m.store.saveLocked()
	if err != nil {
		for index := range m.store.state.Accounts {
			if m.store.state.Accounts[index].ID == accountID {
				m.store.state.Accounts[index].Proxy = previousProxy
				break
			}
		}
	}
	m.store.mu.Unlock()
	if err != nil {
		fmt.Fprintf(os.Stderr, "persist account proxy: %v\n", err)
		m.mu.Lock()
		if current := m.runtimes[accountID]; current != nil {
			current.config.Proxy = previousProxy
		}
		m.mu.Unlock()
		return false
	}
	client.setProxy(proxy)
	return true
}

// reconcileFullProxies gives idle accounts independent low-latency FULL exits.
// It never moves an active or tool-waiting account, because that would break a
// caller's current tool chain.
func (m *accountManager) reconcileFullProxies(nodes []proxyNode) {
	now := time.Now()
	full := make([]proxyNode, 0, len(nodes))
	for _, node := range nodes {
		if node.Mode == "full" && node.Alive && !node.CooldownUntil.After(now) {
			full = append(full, node)
		}
	}
	if len(full) == 0 {
		return
	}
	sort.SliceStable(full, func(i, j int) bool { return full[i].TierLatencyMS < full[j].TierLatencyMS })
	m.mu.Lock()
	accountIDs := make([]string, 0, len(m.runtimes))
	for accountID, runtime := range m.runtimes {
		if runtime.active == 0 && runtime.client.pendingSession() == "" {
			accountIDs = append(accountIDs, accountID)
		}
	}
	m.mu.Unlock()
	sort.Strings(accountIDs)
	for index, accountID := range accountIDs {
		m.setAccountProxy(accountID, full[index%len(full)].Addr)
	}
}

func (m *accountManager) setProxy(proxy string) {
	m.mu.Lock()
	clients := make([]*cliClient, 0, len(m.runtimes))
	for _, runtime := range m.runtimes {
		runtime.config.Proxy = proxy
		clients = append(clients, runtime.client)
	}
	m.mu.Unlock()
	m.store.mu.Lock()
	for index := range m.store.state.Accounts {
		m.store.state.Accounts[index].Proxy = proxy
	}
	err := m.store.saveLocked()
	m.store.mu.Unlock()
	if err != nil {
		fmt.Fprintf(os.Stderr, "persist global account proxy: %v\n", err)
	}
	for _, client := range clients {
		client.setProxy(proxy)
	}
}

func (m *accountManager) running() bool {
	m.mu.Lock()
	clients := make([]*cliClient, 0, len(m.runtimes))
	for _, runtime := range m.runtimes {
		clients = append(clients, runtime.client)
	}
	m.mu.Unlock()
	for _, client := range clients {
		if client.running() {
			return true
		}
	}
	return false
}

func (m *accountManager) stopAll() {
	m.mu.Lock()
	clients := make([]*cliClient, 0, len(m.runtimes))
	for _, runtime := range m.runtimes {
		clients = append(clients, runtime.client)
	}
	var loginProcesses []*os.Process
	for _, attempt := range m.loginAttempts {
		if attempt.cmd != nil && attempt.cmd.Process != nil && attempt.Status != "completed" && attempt.Status != "failed" {
			attempt.Status = "cancelled"
			loginProcesses = append(loginProcesses, attempt.cmd.Process)
		}
	}
	m.mu.Unlock()
	for _, client := range clients {
		client.stop()
	}
	for _, process := range loginProcesses {
		_ = process.Kill()
	}
}

func (m *accountManager) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.runtimes)
}

func (m *accountManager) snapshots(selectedProxy string) []map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]map[string]any, 0, len(m.runtimes))
	boundSessions := make(map[string]int)
	for _, binding := range m.sessionAccounts {
		boundSessions[binding.AccountID]++
	}
	for _, runtime := range m.runtimes {
		credential := readAccountCredential(runtime.config.ConfigDir)
		cliOpen, cliPID, cliStartedAt := runtime.client.processStatus()
		status := "待机"
		if !credential.Authenticated {
			status = "未登录"
		} else if !runtime.config.Enabled {
			status = "已停用"
		} else if runtime.cooldownUntil.After(time.Now()) {
			status = "冷却中"
		} else if runtime.client.running() || runtime.active > 0 {
			status = "运行中"
		}
		result = append(result, map[string]any{
			"id": runtime.config.ID, "label": credential.Label, "authenticated": credential.Authenticated,
			"enabled": runtime.config.Enabled, "status": status, "current_exit": firstNonEmpty(runtime.config.Proxy, selectedProxy),
			"active_sessions": boundSessions[runtime.config.ID], "active_requests": runtime.active,
			"cli_open": cliOpen, "cli_pid": cliPID, "cli_started_at": cliStartedAt,
			"models": len(gatewayModels), "last_error": runtime.lastError,
			"active_model": runtime.activeModel,
			"last_at":      runtime.lastUsed, "cooldown_until": runtime.cooldownUntil,
			"admission_proxy": runtime.config.AdmissionProxy, "admission_cooldown_until": runtime.config.AdmissionCooldownUntil,
		})
	}
	return result
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (m *accountManager) exportPortableAccounts() (portableAccountBundle, error) {
	m.store.mu.RLock()
	configs := append([]accountConfig(nil), m.store.state.Accounts...)
	m.store.mu.RUnlock()
	bundle := portableAccountBundle{Version: 1, Accounts: make([]portableAccount, 0, len(configs))}
	for _, config := range configs {
		data, err := os.ReadFile(filepath.Join(config.ConfigDir, "credentials.json"))
		if err != nil {
			return portableAccountBundle{}, fmt.Errorf("read credentials for %s: %w", config.Label, err)
		}
		if !json.Valid(data) {
			return portableAccountBundle{}, fmt.Errorf("credentials for %s are invalid JSON", config.Label)
		}
		bundle.Accounts = append(bundle.Accounts, portableAccount{
			ID: config.ID, Label: config.Label, Enabled: config.Enabled, Credentials: append(json.RawMessage(nil), data...),
		})
	}
	return bundle, nil
}

type portableAccountWrite struct {
	config      accountConfig
	credentials json.RawMessage
	isNew       bool
}

var portableAccountIDPattern = regexp.MustCompile(`^account-[A-Za-z0-9_-]+$`)

func savePortableCredentials(path string, credentials json.RawMessage) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	if existing, err := os.ReadFile(path); err == nil && json.Valid(existing) {
		if err := atomicWriteFile(path+".bak", existing, 0o600); err != nil {
			return fmt.Errorf("backup credentials: %w", err)
		}
	}
	var formatted bytes.Buffer
	if err := json.Indent(&formatted, credentials, "", "  "); err != nil {
		return err
	}
	formatted.WriteByte('\n')
	return atomicWriteFile(path, []byte(formatted.String()), 0o600)
}

func (m *accountManager) importPortableAccounts(bundle portableAccountBundle) (int, int, error) {
	if bundle.Version != 1 {
		return 0, 0, errors.New("unsupported account configuration version")
	}
	if len(bundle.Accounts) == 0 || len(bundle.Accounts) > 50 {
		return 0, 0, errors.New("account configuration must contain 1 to 50 accounts")
	}
	m.mu.Lock()
	if m.configuring {
		m.mu.Unlock()
		return 0, 0, errors.New("account configuration is already being updated")
	}
	for _, runtime := range m.runtimes {
		if runtime.active > 0 {
			m.mu.Unlock()
			return 0, 0, errors.New("cannot save account configuration while requests are active")
		}
	}
	m.configuring = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.configuring = false
		m.mu.Unlock()
	}()

	m.store.mu.RLock()
	configs := append([]accountConfig(nil), m.store.state.Accounts...)
	selectedProxy := m.store.state.SelectedProxy
	m.store.mu.RUnlock()
	byEmail := make(map[string]int, len(configs))
	usedIDs := make(map[string]bool, len(configs))
	for index, config := range configs {
		credential := readAccountCredential(config.ConfigDir)
		if credential.Email != "" {
			byEmail[strings.ToLower(credential.Email)] = index
		}
		usedIDs[config.ID] = true
	}
	seenEmails := make(map[string]bool, len(bundle.Accounts))
	writes := make([]portableAccountWrite, 0, len(bundle.Accounts))
	newCount := 0
	for _, item := range bundle.Accounts {
		if !json.Valid(item.Credentials) {
			return 0, 0, errors.New("account credentials must be valid JSON")
		}
		credential := parseAccountCredential(item.Credentials)
		if !credential.Authenticated || credential.Email == "" {
			return 0, 0, errors.New("every account must contain an email and authToken")
		}
		emailKey := strings.ToLower(credential.Email)
		if seenEmails[emailKey] {
			return 0, 0, fmt.Errorf("duplicate account email: %s", credential.Email)
		}
		seenEmails[emailKey] = true
		if index, exists := byEmail[emailKey]; exists {
			config := configs[index]
			config.Label = credential.Label
			config.Enabled = item.Enabled
			configs[index] = config
			writes = append(writes, portableAccountWrite{config: config, credentials: item.Credentials})
			continue
		}
		id := strings.TrimSpace(item.ID)
		if !portableAccountIDPattern.MatchString(id) || usedIDs[id] {
			for {
				id = newAccountID()
				if !usedIDs[id] {
					break
				}
			}
		}
		usedIDs[id] = true
		config := accountConfig{
			ID: id, Label: credential.Label, ConfigDir: filepath.Join(m.accountsRoot, id), Enabled: item.Enabled, AddedAt: time.Now(),
		}
		configs = append(configs, config)
		byEmail[emailKey] = len(configs) - 1
		writes = append(writes, portableAccountWrite{config: config, credentials: item.Credentials, isNew: true})
		newCount++
	}
	for _, write := range writes {
		if err := savePortableCredentials(filepath.Join(write.config.ConfigDir, "credentials.json"), write.credentials); err != nil {
			return 0, 0, fmt.Errorf("save credentials for %s: %w", write.config.Label, err)
		}
	}
	m.store.mu.Lock()
	m.store.state.Accounts = configs
	err := m.store.saveLocked()
	m.store.mu.Unlock()
	if err != nil {
		return 0, 0, err
	}
	var stopClients []*cliClient
	m.mu.Lock()
	for _, write := range writes {
		if runtime := m.runtimes[write.config.ID]; runtime != nil {
			runtime.config = write.config
			stopClients = append(stopClients, runtime.client)
			continue
		}
		if write.config.Proxy == "" {
			write.config.Proxy = selectedProxy
		}
		m.runtimes[write.config.ID] = &accountRuntime{
			config:            write.config,
			client:            &cliClient{path: m.cliPath, cwd: m.cwd, configDir: write.config.ConfigDir, proxy: write.config.Proxy},
			admissionCooldown: make(map[string]time.Time),
		}
	}
	m.mu.Unlock()
	for _, client := range stopClients {
		client.stop()
	}
	return len(writes), newCount, nil
}

func (m *accountManager) toggle(accountID string, enabled bool) error {
	m.store.mu.Lock()
	found := false
	for index := range m.store.state.Accounts {
		if m.store.state.Accounts[index].ID == accountID {
			m.store.state.Accounts[index].Enabled = enabled
			found = true
			break
		}
	}
	if !found {
		m.store.mu.Unlock()
		return errors.New("account not found")
	}
	err := m.store.saveLocked()
	m.store.mu.Unlock()
	if err != nil {
		return err
	}
	m.mu.Lock()
	runtime := m.runtimes[accountID]
	if runtime != nil {
		runtime.config.Enabled = enabled
	}
	m.mu.Unlock()
	if runtime != nil && !enabled {
		runtime.client.stop()
	}
	return nil
}

// remove stops and deregisters an account without deleting its credential directory.
// Credentials are intentionally retained so an operator can recover them manually.
func (m *accountManager) remove(accountID string) error {
	if accountID == "" {
		return errors.New("account id is required")
	}
	m.mu.Lock()
	if m.configuring {
		m.mu.Unlock()
		return errors.New("account configuration is being updated")
	}
	runtime := m.runtimes[accountID]
	if runtime == nil {
		m.mu.Unlock()
		return errors.New("account not found")
	}
	if runtime.active > 0 {
		m.mu.Unlock()
		return errors.New("cannot remove account while requests are active")
	}
	m.configuring = true
	delete(m.runtimes, accountID)
	for sessionID, binding := range m.sessionAccounts {
		if binding.AccountID == accountID {
			delete(m.sessionAccounts, sessionID)
		}
	}
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.configuring = false
		m.mu.Unlock()
	}()

	// Stop first so no already-idle process can continue using the removed account.
	runtime.client.stop()
	m.store.mu.Lock()
	previousAccounts := append([]accountConfig(nil), m.store.state.Accounts...)
	previousBindings := make(map[string]accountSessionBinding, len(m.store.state.AccountSessions))
	for sessionID, binding := range m.store.state.AccountSessions {
		previousBindings[sessionID] = binding
	}
	previousDefaultCheck := m.store.state.DefaultAccountChecked
	configs := make([]accountConfig, 0, len(m.store.state.Accounts)-1)
	for _, config := range m.store.state.Accounts {
		if config.ID != accountID {
			configs = append(configs, config)
		}
	}
	m.store.state.Accounts = configs
	m.store.state.DefaultAccountChecked = true
	for sessionID, binding := range m.store.state.AccountSessions {
		if binding.AccountID == accountID {
			delete(m.store.state.AccountSessions, sessionID)
		}
	}
	err := m.store.saveLocked()
	if err != nil {
		m.store.state.Accounts = previousAccounts
		m.store.state.AccountSessions = previousBindings
		m.store.state.DefaultAccountChecked = previousDefaultCheck
	}
	m.store.mu.Unlock()
	if err != nil {
		// Keep the runtime usable if state persistence fails; the next restart
		// reloads the last known-good state file.
		m.mu.Lock()
		m.runtimes[accountID] = runtime
		m.mu.Unlock()
		return err
	}
	return nil
}

func newAccountID() string {
	var raw [8]byte
	_, _ = cryptorand.Read(raw[:])
	return "account-" + hex.EncodeToString(raw[:])
}

func (m *accountManager) registerAccount(configDir string) error {
	credential := readAccountCredential(configDir)
	if !credential.Authenticated {
		return errors.New("login completed without account credentials")
	}
	config := accountConfig{ID: filepath.Base(configDir), Label: credential.Label, ConfigDir: configDir, Enabled: true, AddedAt: time.Now()}
	m.store.mu.Lock()
	for _, existing := range m.store.state.Accounts {
		if existing.ConfigDir == configDir {
			m.store.mu.Unlock()
			return nil
		}
	}
	m.store.state.Accounts = append(m.store.state.Accounts, config)
	err := m.store.saveLocked()
	proxy := m.store.state.SelectedProxy
	m.store.mu.Unlock()
	if err != nil {
		return err
	}
	m.mu.Lock()
	config.Proxy = proxy
	m.runtimes[config.ID] = &accountRuntime{config: config, client: &cliClient{path: m.cliPath, cwd: m.cwd, configDir: configDir, proxy: proxy}, admissionCooldown: make(map[string]time.Time)}
	m.mu.Unlock()
	return nil
}

var ansiPattern = regexp.MustCompile(`\x1b\[[0-9;?]*[ -/]*[@-~]`)
var urlPattern = regexp.MustCompile(`https?://[^\s]+`)

func cleanLoginURL(line string) string {
	line = ansiPattern.ReplaceAllString(line, "")
	candidate := urlPattern.FindString(line)
	candidate = strings.TrimRight(candidate, ".,;)]}\"'")
	parsed, err := url.Parse(candidate)
	if err != nil || parsed.Host == "" {
		return ""
	}
	return candidate
}

func (m *accountManager) startLogin() (loginAttempt, error) {
	if strings.TrimSpace(m.loginCLIPath) == "" {
		return loginAttempt{}, errors.New("login CLI is not configured")
	}
	id := newAccountID()
	configDir := filepath.Join(m.accountsRoot, id)
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		return loginAttempt{}, err
	}
	cmd := exec.Command(m.loginCLIPath, "login")
	cmd.Dir = m.cwd
	cmd.Env = cliEnvironment(m.store.selectedProxy(), configDir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return loginAttempt{}, err
	}
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return loginAttempt{}, err
	}
	attempt := &loginAttempt{ID: id, Status: "starting", ConfigDir: configDir, StartedAt: time.Now(), cmd: cmd}
	m.mu.Lock()
	m.loginAttempts[id] = attempt
	m.mu.Unlock()
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if loginURL := cleanLoginURL(scanner.Text()); loginURL != "" {
				m.mu.Lock()
				attempt.URL = loginURL
				attempt.Status = "waiting"
				m.mu.Unlock()
			}
		}
		waitErr := cmd.Wait()
		registerErr := m.registerAccount(configDir)
		m.mu.Lock()
		defer m.mu.Unlock()
		if attempt.Status == "cancelled" {
			return
		}
		if registerErr == nil {
			attempt.Status = "completed"
			return
		}
		attempt.Status = "failed"
		if waitErr != nil {
			attempt.Error = waitErr.Error()
		} else {
			attempt.Error = registerErr.Error()
		}
	}()
	return *attempt, nil
}

func (m *accountManager) cancelLogin(id string) error {
	m.mu.Lock()
	attempt := m.loginAttempts[id]
	if attempt == nil {
		m.mu.Unlock()
		return errors.New("login attempt not found")
	}
	if attempt.Status == "completed" || attempt.Status == "failed" || attempt.Status == "cancelled" {
		m.mu.Unlock()
		return nil
	}
	attempt.Status = "cancelled"
	cmd := attempt.cmd
	m.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	return nil
}

func (m *accountManager) loginStatus(id string) (loginAttempt, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	attempt := m.loginAttempts[id]
	if attempt == nil {
		return loginAttempt{}, false
	}
	return *attempt, true
}
