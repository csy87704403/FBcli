package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOfficialTierProbeDoesNotSendAccountCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("X-API-Key") != "" {
			t.Fatalf("tier probe sent account credentials: %#v", r.Header)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"accessTier":"full"}`))
	}))
	defer server.Close()

	status := probeOfficialEgressTier(context.Background(), "", server.URL)
	if status.Tier != "full" || status.Detail != "official_public_tier" || status.CheckedAt.IsZero() {
		t.Fatalf("unexpected tier status: %#v", status)
	}
}

func TestOfficialTierScanSortsFullNodesByOfficialLatency(t *testing.T) {
	proxy := func(tier string, delay time.Duration) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
				t.Fatalf("tier scan sent account credentials: %#v", r.Header)
			}
			time.Sleep(delay)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"accessTier":"` + tier + `"}`))
		}))
	}
	slowFull := proxy("full", 50*time.Millisecond)
	defer slowFull.Close()
	limited := proxy("limited", 0)
	defer limited.Close()
	fastFull := proxy("full", 0)
	defer fastFull.Close()

	nodes := scanOfficialProxyTiers(context.Background(), []proxyNode{
		{Addr: slowFull.URL}, {Addr: limited.URL}, {Addr: fastFull.URL},
	}, "http://official.test/api/web/access-tier", 3)
	if len(nodes) != 3 || nodes[0].Addr != fastFull.URL || nodes[0].Mode != "full" || nodes[1].Addr != slowFull.URL || nodes[2].Mode != "limited" {
		t.Fatalf("unexpected tier order: %#v", nodes)
	}
	for _, node := range nodes {
		if !node.Alive || node.TierCheckedAt.IsZero() || node.TierDetail != "official_public_tier" {
			t.Fatalf("tier metadata missing: %#v", node)
		}
	}
}

func TestAutomaticProxySelectionKeepsCurrentFullNode(t *testing.T) {
	nodes := []proxyNode{
		{Addr: "fast", Mode: "full", TierLatencyMS: 10},
		{Addr: "current", Mode: "full", TierLatencyMS: 50},
	}
	selected, _, fullCount := selectScannedProxy(nodes, "current", false)
	if selected != "current" || fullCount != 2 {
		t.Fatalf("automatic scan changed a healthy FULL exit: selected=%s full=%d", selected, fullCount)
	}
	selected, _, _ = selectScannedProxy(nodes, "current", true)
	if selected != "fast" {
		t.Fatalf("manual scan did not choose the fastest FULL exit: %s", selected)
	}
}

func TestAutomaticProxySelectionReplacesNonFullNode(t *testing.T) {
	nodes := []proxyNode{
		{Addr: "fast", Mode: "full", TierLatencyMS: 10},
		{Addr: "current", Mode: "limited", TierLatencyMS: 5},
	}
	selected, tier, _ := selectScannedProxy(nodes, "current", false)
	if selected != "fast" || tier.Tier != "full" {
		t.Fatalf("automatic scan kept a non-FULL exit: selected=%s tier=%s", selected, tier.Tier)
	}
}

func TestRotateProxyAfterIPCapChoosesFastestOtherFullNode(t *testing.T) {
	dir := t.TempDir()
	store := &stateStore{path: filepath.Join(dir, "state.json"), state: gatewayState{
		SelectedProxy: "current",
		Proxies: []proxyNode{
			{Addr: "current", Mode: "full", Alive: true, TierLatencyMS: 10},
			{Addr: "slow", Mode: "full", Alive: true, TierLatencyMS: 50},
			{Addr: "fast", Mode: "full", Alive: true, TierLatencyMS: 20},
			{Addr: "limited", Mode: "limited", Alive: true, TierLatencyMS: 1},
		},
	}}
	if err := store.saveLocked(); err != nil {
		t.Fatal(err)
	}
	admin := &adminService{store: store}
	if !admin.rotateProxyAfterIPCap() {
		t.Fatal("IP-cap failover did not select a replacement")
	}
	if store.state.SelectedProxy != "fast" {
		t.Fatalf("selected proxy = %q, want fast", store.state.SelectedProxy)
	}
	if !store.state.Proxies[0].CooldownUntil.After(time.Now()) || store.state.Proxies[0].Detail != "upstream_ip_capped" {
		t.Fatalf("current proxy was not cooled down: %#v", store.state.Proxies[0])
	}
}

func TestLoadMihomoListenerNames(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	config := "listeners:\n  - name: freebuff-node-53\n    type: mixed\n    port: 10053\n    proxy: \"🇸🇬VIP新加坡6\"\n  - name: freebuff-node-54\n    port: 10054\n    proxy: '🇺🇸美国2'\n"
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	names, err := loadMihomoListenerNames(path)
	if err != nil {
		t.Fatal(err)
	}
	if names["10053"] != "🇸🇬VIP新加坡6" || names["10054"] != "🇺🇸美国2" {
		t.Fatalf("unexpected listener names: %#v", names)
	}
}

func TestLegacyImportKeepsTierButInvalidatesReachability(t *testing.T) {
	dir := t.TempDir()
	legacyConfig := filepath.Join(dir, "config.json")
	legacyPool := filepath.Join(dir, "pool.json")
	if err := os.WriteFile(legacyConfig, []byte(`{"API_KEYS":["sk-old"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacyPool, []byte(`[{"addr":"http://127.0.0.1:10001","mode":"full","alive":true,"latency_ms":12,"detail":"official_web_tier"}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := newStateStore(filepath.Join(dir, "state.json"), legacyConfig, legacyPool)
	if err != nil {
		t.Fatal(err)
	}
	if len(store.state.APIKeys) != 1 || store.state.APIKeys[0] != "sk-old" {
		t.Fatalf("unexpected imported keys: %#v", store.state.APIKeys)
	}
	node := store.state.Proxies[0]
	if node.Mode != "full" || node.Alive || node.LatencyMS != 0 || node.Detail != "stale_import" {
		t.Fatalf("legacy reachability was trusted: %#v", node)
	}
}

func TestStateSaveCreatesValidBackupAndLeavesNoTemporaryFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	store := &stateStore{path: path, state: gatewayState{APIKeys: []string{"old-key"}}}
	if err := store.saveLocked(); err != nil {
		t.Fatal(err)
	}
	store.state.APIKeys = []string{"new-key"}
	if err := store.saveLocked(); err != nil {
		t.Fatal(err)
	}
	var backup gatewayState
	data, err := os.ReadFile(path + ".bak")
	if err != nil || json.Unmarshal(data, &backup) != nil {
		t.Fatalf("backup is unreadable: %v", err)
	}
	if len(backup.APIKeys) != 1 || backup.APIKeys[0] != "old-key" {
		t.Fatalf("unexpected backup state: %#v", backup.APIKeys)
	}
	temporaryFiles, err := filepath.Glob(filepath.Join(dir, "*.tmp-*"))
	if err != nil || len(temporaryFiles) != 0 {
		t.Fatalf("temporary files remain: %#v, %v", temporaryFiles, err)
	}
}

func TestStateLoadRecoversCorruptedPrimaryFromBackup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	store := &stateStore{path: path, state: gatewayState{APIKeys: []string{"recover-me"}}}
	if err := store.saveLocked(); err != nil {
		t.Fatal(err)
	}
	store.state.APIKeys = []string{"newer-state"}
	if err := store.saveLocked(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"broken"`), 0o600); err != nil {
		t.Fatal(err)
	}
	recovered, err := newStateStore(path, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered.state.APIKeys) != 1 || recovered.state.APIKeys[0] != "recover-me" {
		t.Fatalf("backup was not recovered: %#v", recovered.state.APIKeys)
	}
	data, err := os.ReadFile(path)
	if err != nil || !json.Valid(data) {
		t.Fatalf("primary state was not repaired: %v", err)
	}
}

func TestAPIKeyMiddleware(t *testing.T) {
	admin := &adminService{store: &stateStore{state: gatewayState{APIKeys: []string{"sk-test"}}}}
	handler := admin.requireAPIKey(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })

	withoutKey := httptest.NewRecorder()
	handler(withoutKey, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if withoutKey.Code != http.StatusUnauthorized {
		t.Fatalf("without key status = %d", withoutKey.Code)
	}

	withKey := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	request.Header.Set("Authorization", "Bearer sk-test")
	handler(withKey, request)
	if withKey.Code != http.StatusNoContent {
		t.Fatalf("with key status = %d", withKey.Code)
	}
}

func TestAPIKeyMiddlewareRejectsUnconfiguredGateway(t *testing.T) {
	admin := &adminService{store: &stateStore{state: gatewayState{}}}
	handler := admin.requireAPIKey(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured gateway status = %d", recorder.Code)
	}
}

func TestAdminLoginSessionAndLogout(t *testing.T) {
	auth := newAdminAuthenticator("admin", "correct horse battery staple")
	login := httptest.NewRequest(http.MethodPost, "/api/admin/login", strings.NewReader(`{"username":"admin","password":"correct horse battery staple"}`))
	login.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()
	auth.login(recorder, login)
	if recorder.Code != http.StatusOK || len(recorder.Result().Cookies()) != 1 {
		t.Fatalf("login failed: status=%d cookies=%d", recorder.Code, len(recorder.Result().Cookies()))
	}
	cookie := recorder.Result().Cookies()[0]
	if !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode {
		t.Fatalf("unsafe session cookie: %#v", cookie)
	}

	protected := auth.require(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	request := httptest.NewRequest(http.MethodGet, "/api/webui/status", nil)
	request.AddCookie(cookie)
	protectedRecorder := httptest.NewRecorder()
	protected(protectedRecorder, request)
	if protectedRecorder.Code != http.StatusNoContent {
		t.Fatalf("authenticated request status = %d", protectedRecorder.Code)
	}

	logoutRequest := httptest.NewRequest(http.MethodPost, "/api/admin/logout", nil)
	logoutRequest.AddCookie(cookie)
	auth.logout(httptest.NewRecorder(), logoutRequest)
	rejected := httptest.NewRecorder()
	protected(rejected, request)
	if rejected.Code != http.StatusUnauthorized {
		t.Fatalf("logged-out request status = %d", rejected.Code)
	}
}

func TestAdminLoginRateLimit(t *testing.T) {
	auth := newAdminAuthenticator("admin", "secret")
	for attempt := 0; attempt < maxLoginFailures; attempt++ {
		request := httptest.NewRequest(http.MethodPost, "/api/admin/login", strings.NewReader(`{"username":"admin","password":"wrong"}`))
		request.RemoteAddr = "127.0.0.1:45678"
		auth.login(httptest.NewRecorder(), request)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/admin/login", strings.NewReader(`{"username":"admin","password":"secret"}`))
	request.RemoteAddr = "127.0.0.1:45678"
	recorder := httptest.NewRecorder()
	auth.login(recorder, request)
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("rate-limited login status = %d", recorder.Code)
	}
}

func TestNormalizeProxy(t *testing.T) {
	value, err := normalizeProxy("127.0.0.1:10001")
	if err != nil || value != "http://127.0.0.1:10001" {
		t.Fatalf("normalize result = %q, %v", value, err)
	}
	if _, err := normalizeProxy("file:///tmp/proxy"); err == nil {
		t.Fatal("unsupported proxy scheme accepted")
	}
}
