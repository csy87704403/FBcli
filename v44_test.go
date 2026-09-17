package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestServerWithChild(t *testing.T) (*server, *cliClient) {
	t.Helper()
	t.Setenv("FREEBUFF_TEST_JSONL_CHILD", "1")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	numAccounts := 10
	runtimes := make(map[string]*accountRuntime, numAccounts)
	accountsList := make([]accountConfig, numAccounts)
	var firstClient *cliClient
	for i := 1; i <= numAccounts; i++ {
		accID := fmt.Sprintf("acc-%d", i)
		configDir := t.TempDir()
		writeTestCredential(t, configDir, fmt.Sprintf("test%d@example.com", i))
		c := &cliClient{path: executable, cwd: t.TempDir(), configDir: configDir}
		t.Cleanup(func() { c.stop() })
		if i == 1 {
			firstClient = c
		}
		cfg := accountConfig{ID: accID, ConfigDir: configDir, Enabled: true}
		accountsList[i-1] = cfg
		runtimes[accID] = &accountRuntime{
			client:            c,
			config:            cfg,
			admissionCooldown: make(map[string]time.Time),
		}
	}
	store := &stateStore{
		path: filepath.Join(t.TempDir(), "state.json"),
		state: gatewayState{
			APIKeys:         []string{"test-key"},
			Accounts:        accountsList,
			AccountSessions: make(map[string]accountSessionBinding),
		},
	}
	accounts := &accountManager{
		store:           store,
		runtimes:        runtimes,
		sessionAccounts: make(map[string]accountSessionBinding),
	}
	s := &server{
		accounts:  accounts,
		sessions:  newConversationRouter(store),
		namespace: "test-ns:",
		admin:     &adminService{store: store},
	}
	return s, firstClient
}

// 1. Session ID conflicts and OpenAI user independence
func TestSessionIDHeaderAndBodyConflict(t *testing.T) {
	s, _ := newTestServerWithChild(t)

	// Conflict: header != body => 400
	reqBody := `{"session_id":"body-sess-1","model":"` + defaultModel + `","messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqBody))
	req.Header.Set("X-Freebuff-Session-ID", "header-sess-2")
	rec := httptest.NewRecorder()
	s.chatCompletions(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on session_id conflict, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "conflicts with X-Freebuff-Session-ID") {
		t.Fatalf("unexpected error message: %s", rec.Body.String())
	}

	// Matching header and body => succeeds
	reqBodyMatch := `{"session_id":"same-sess","model":"` + defaultModel + `","messages":[{"role":"user","content":"hi"}]}`
	reqMatch := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqBodyMatch))
	reqMatch.Header.Set("X-Freebuff-Session-ID", "same-sess")
	recMatch := httptest.NewRecorder()
	s.chatCompletions(recMatch, reqMatch)
	if recMatch.Code != http.StatusOK {
		t.Fatalf("expected 200 on matching session IDs, got %d: %s", recMatch.Code, recMatch.Body.String())
	}

	// OpenAI user is not a session ID
	reqBodyUser := `{"model":"` + defaultModel + `","user":"openai-user-123","messages":[{"role":"user","content":"hi"}]}`
	reqUser := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqBodyUser))
	recUser := httptest.NewRecorder()
	s.chatCompletions(recUser, reqUser)
	if recUser.Code != http.StatusOK {
		t.Fatalf("expected 200 on request with user field, got %d", recUser.Code)
	}
	var parsed chatRequest
	_ = json.Unmarshal([]byte(reqBodyUser), &parsed)
	if explicitSessionID(parsed, "") != "" {
		t.Fatalf("user field was incorrectly treated as session ID: %q", explicitSessionID(parsed, ""))
	}
}

// 2. max_tokens distinctions: omitted vs explicit 0, null, negative, invalid type, >= contextLimit
func TestMaxTokensValidation(t *testing.T) {
	s, _ := newTestServerWithChild(t)
	t.Setenv("FREEBUFF_CONTEXT_LIMIT", "1000")
	t.Setenv("FREEBUFF_CONTEXT_RESERVE", "100")

	// Explicit null => 400
	reqNull := `{"model":"` + defaultModel + `","max_tokens":null,"messages":[{"role":"user","content":"hi"}]}`
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqNull))
	rec := httptest.NewRecorder()
	s.chatCompletions(rec, r)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("max_tokens null expected 400, got %d: %s", rec.Code, rec.Body.String())
	}

	// Explicit 0 => 400
	reqZero := `{"model":"` + defaultModel + `","max_tokens":0,"messages":[{"role":"user","content":"hi"}]}`
	r = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqZero))
	rec = httptest.NewRecorder()
	s.chatCompletions(rec, r)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("max_tokens 0 expected 400, got %d: %s", rec.Code, rec.Body.String())
	}

	// Explicit negative => 400
	reqNeg := `{"model":"` + defaultModel + `","max_tokens":-10,"messages":[{"role":"user","content":"hi"}]}`
	r = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqNeg))
	rec = httptest.NewRecorder()
	s.chatCompletions(rec, r)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("max_tokens negative expected 400, got %d: %s", rec.Code, rec.Body.String())
	}

	// Invalid type string => 400
	reqStr := `{"model":"` + defaultModel + `","max_tokens":"many","messages":[{"role":"user","content":"hi"}]}`
	r = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqStr))
	rec = httptest.NewRecorder()
	s.chatCompletions(rec, r)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("max_tokens string expected 400, got %d: %s", rec.Code, rec.Body.String())
	}

	// >= contextLimit (1000) => 400
	reqOversize := `{"model":"` + defaultModel + `","max_tokens":1000,"messages":[{"role":"user","content":"hi"}]}`
	r = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqOversize))
	rec = httptest.NewRecorder()
	s.chatCompletions(rec, r)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("max_tokens >= context expected 400, got %d: %s", rec.Code, rec.Body.String())
	}

	// Omitted max_tokens => succeeds (200)
	reqOmitted := `{"model":"` + defaultModel + `","messages":[{"role":"user","content":"hi"}]}`
	r = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqOmitted))
	rec = httptest.NewRecorder()
	s.chatCompletions(rec, r)
	if rec.Code != http.StatusOK {
		t.Fatalf("omitted max_tokens expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Valid positive max_tokens => succeeds (200)
	reqValid := `{"model":"` + defaultModel + `","max_tokens":50,"messages":[{"role":"user","content":"hi"}]}`
	r = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqValid))
	rec = httptest.NewRecorder()
	s.chatCompletions(rec, r)
	if rec.Code != http.StatusOK {
		t.Fatalf("valid max_tokens expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Struct compatibility check
	var reqDecoded chatRequest
	if err := json.Unmarshal([]byte(`{"max_tokens":123}`), &reqDecoded); err != nil || reqDecoded.MaxTokens != 123 || !reqDecoded.MaxTokensPresent {
		t.Fatalf("struct unmarshal failed: %#v, err=%v", reqDecoded, err)
	}
	var reqOmitDecoded chatRequest
	if err := json.Unmarshal([]byte(`{}`), &reqOmitDecoded); err != nil || reqOmitDecoded.MaxTokens != 0 || reqOmitDecoded.MaxTokensPresent {
		t.Fatalf("struct unmarshal omitted failed: %#v, err=%v", reqOmitDecoded, err)
	}
}

// 3. Unknown model returns 404 model_not_found before admission & catalog tests
func TestUnknownModelReturns404AndCatalogValidation(t *testing.T) {
	s, _ := newTestServerWithChild(t)

	// Unknown model => 404 model_not_found
	reqBody := `{"model":"claude-3-7-sonnet","messages":[{"role":"user","content":"hi"}]}`
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqBody))
	rec := httptest.NewRecorder()
	s.chatCompletions(rec, r)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown model, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "model_not_found") {
		t.Fatalf("expected model_not_found error code: %s", rec.Body.String())
	}

	// Verify catalog reading via loadHeadlessCatalog using our child fixture
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	// Success case
	if err := loadHeadlessCatalog(executable); err != nil {
		t.Fatalf("loadHeadlessCatalog failed: %v", err)
	}
	if !knownModel(defaultModel) || !knownModel(glmV52Model) || !knownModel(fableModel) {
		t.Fatalf("catalog did not load expected models")
	}

	// Failure case: child returns error
	t.Setenv("FREEBUFF_TEST_CATALOG_FAIL", "1")
	if err := loadHeadlessCatalog(executable); err == nil {
		t.Fatal("expected error when headless catalog fails")
	}
	t.Setenv("FREEBUFF_TEST_CATALOG_FAIL", "")

	// Failure case: missing default model
	t.Setenv("FREEBUFF_TEST_CATALOG_NO_DEFAULT", "1")
	if err := loadHeadlessCatalog(executable); err == nil || !strings.Contains(err.Error(), "default model") {
		t.Fatalf("expected missing default model error, got: %v", err)
	}
	t.Setenv("FREEBUFF_TEST_CATALOG_NO_DEFAULT", "")

	// Failure case: duplicate model ID
	t.Setenv("FREEBUFF_TEST_CATALOG_DUPLICATE", "1")
	if err := loadHeadlessCatalog(executable); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate model error, got: %v", err)
	}
	t.Setenv("FREEBUFF_TEST_CATALOG_DUPLICATE", "")

	// Failure case: blank model ID
	t.Setenv("FREEBUFF_TEST_CATALOG_BLANK", "1")
	if err := loadHeadlessCatalog(executable); err == nil || !strings.Contains(err.Error(), "blank") {
		t.Fatalf("expected blank model error, got: %v", err)
	}
	t.Setenv("FREEBUFF_TEST_CATALOG_BLANK", "")

	// Allowlist subset test
	t.Setenv("FREEBUFF_TEST_CATALOG_ALLOWLIST", defaultModel+","+gptLunaModel)
	if err := loadHeadlessCatalog(executable); err != nil {
		t.Fatalf("loadHeadlessCatalog allowlist failed: %v", err)
	}
	if !knownModel(defaultModel) || !knownModel(gptLunaModel) || knownModel(glmV52Model) {
		t.Fatalf("allowlist filtering did not apply as expected")
	}
	t.Setenv("FREEBUFF_TEST_CATALOG_ALLOWLIST", "")

	// Custom FREEBUFF_DEFAULT_MODEL in catalog and HTTP fallback
	t.Setenv("FREEBUFF_DEFAULT_MODEL", fableModel)
	if err := loadHeadlessCatalog(executable); err != nil {
		t.Fatalf("loadHeadlessCatalog with custom default failed: %v", err)
	}
	reqNoModel := `{"messages":[{"role":"user","content":"hello without model"}]}`
	rNoModel := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqNoModel))
	recNoModel := httptest.NewRecorder()
	s.chatCompletions(recNoModel, rNoModel)
	if recNoModel.Code != http.StatusOK {
		t.Fatalf("expected 200 with custom default model, got %d: %s", recNoModel.Code, recNoModel.Body.String())
	}
	t.Setenv("FREEBUFF_DEFAULT_MODEL", "")

	// Restore default catalog
	_ = loadHeadlessCatalog(executable)
}

// 4. Stats: request_count, success_count, message_count across header, body, and auto paths,
// preserved across observe and implicit reset, no mutation on invalid params.
func TestSessionStatsTrackingAndPreservation(t *testing.T) {
	s, _ := newTestServerWithChild(t)

	// Test 1: Header path
	reqHeader := `{"model":"` + defaultModel + `","messages":[{"role":"user","content":"m1"},{"role":"assistant","content":"a1"},{"role":"user","content":"m2"}]}`
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqHeader))
	r.Header.Set("X-Freebuff-Session-ID", "header-stats-sess")
	rec := httptest.NewRecorder()
	s.chatCompletions(rec, r)
	if rec.Code != http.StatusOK {
		t.Fatalf("header path chat failed: %d %s", rec.Code, rec.Body.String())
	}

	obs, found := s.sessions.sessionStatus("header-stats-sess")
	if !found || obs.RequestCount != 1 || obs.SuccessCount != 1 || obs.MessageCount != 3 {
		t.Fatalf("header path stats incorrect: %#v", obs)
	}

	// Test 2: Body path
	reqBody := `{"session_id":"body-stats-sess","model":"` + defaultModel + `","messages":[{"role":"user","content":"m1"}]}`
	r = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqBody))
	rec = httptest.NewRecorder()
	s.chatCompletions(rec, r)
	if rec.Code != http.StatusOK {
		t.Fatalf("body path chat failed: %d %s", rec.Code, rec.Body.String())
	}
	obsBody, found := s.sessions.sessionStatus("body-stats-sess")
	if !found || obsBody.RequestCount != 1 || obsBody.SuccessCount != 1 || obsBody.MessageCount != 1 {
		t.Fatalf("body path stats incorrect: %#v", obsBody)
	}

	// Test 3: Auto path
	reqAuto := `{"model":"` + defaultModel + `","messages":[{"role":"user","content":"auto-m1"},{"role":"assistant","content":"auto-a1"}]}`
	r = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqAuto))
	rec = httptest.NewRecorder()
	s.chatCompletions(rec, r)
	if rec.Code != http.StatusOK {
		t.Fatalf("auto path chat failed: %d %s", rec.Code, rec.Body.String())
	}
	var respAuto map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &respAuto)
	autoID, _ := respAuto["id"].(string)
	if autoID == "" {
		t.Fatal("no auto chat completion id")
	}

	// Query auto session from router
	sessionIDs := s.sessions.sessionIDs()
	var createdAutoID string
	for _, id := range sessionIDs {
		if strings.HasPrefix(id, "auto-") {
			createdAutoID = id
			break
		}
	}
	if createdAutoID == "" {
		t.Fatal("auto session was not tracked in router")
	}
	obsAuto, found := s.sessions.sessionStatus(createdAutoID)
	if !found || obsAuto.RequestCount != 1 || obsAuto.SuccessCount != 1 || obsAuto.MessageCount != 2 {
		t.Fatalf("auto session stats incorrect: %#v", obsAuto)
	}

	// Test 4: Invalid parameters must NOT mutate state or increment request_count
	priorObs := obs
	invalidReq := `{"model":"unknown-model-xxx","messages":[{"role":"user","content":"m1"}]}`
	r = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(invalidReq))
	r.Header.Set("X-Freebuff-Session-ID", "header-stats-sess")
	rec = httptest.NewRecorder()
	s.chatCompletions(rec, r)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
	currentObs, _ := s.sessions.sessionStatus("header-stats-sess")
	if currentObs.RequestCount != priorObs.RequestCount || currentObs.SuccessCount != priorObs.SuccessCount {
		t.Fatalf("invalid local request mutated session stats: was %#v, now %#v", priorObs, currentObs)
	}

	// Test 5: Counters preserved across observeExternal and implicit reset
	// Set large history drop threshold
	s.sessions.observeExternal("header-stats-sess", 100, false)
	afterObserve, _ := s.sessions.sessionStatus("header-stats-sess")
	if afterObserve.RequestCount != 1 || afterObserve.SuccessCount != 1 || afterObserve.MessageCount != 100 {
		t.Fatalf("observeExternal did not preserve counters: %#v", afterObserve)
	}

	// Trigger implicit reset via message drop (100 -> 2 messages)
	dropReq := `{"model":"` + defaultModel + `","messages":[{"role":"user","content":"new start"}]}`
	r = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(dropReq))
	r.Header.Set("X-Freebuff-Session-ID", "header-stats-sess")
	rec = httptest.NewRecorder()
	s.chatCompletions(rec, r)
	if rec.Code != http.StatusOK {
		t.Fatalf("drop req failed: %d %s", rec.Code, rec.Body.String())
	}
	afterDrop, _ := s.sessions.sessionStatus("header-stats-sess")
	// Should have incremented request count (was 1, now 2) and success count (was 1, now 2)
	if afterDrop.RequestCount != 2 || afterDrop.SuccessCount != 2 || afterDrop.MessageCount != 1 {
		t.Fatalf("counters lost during implicit reset: %#v", afterDrop)
	}
	if afterDrop.LastResetReason != "history_drop" {
		t.Fatalf("expected last_reset_reason 'history_drop', got %q", afterDrop.LastResetReason)
	}
}

// 5. Owning session API exposes actual native generation, rebuild reason, upstream instance ID.
// Start event includes instance_id, consumed without fabricating IDs, increment generation only on native rebuild.
func TestInstancePropagationAndGenerationLifecycle(t *testing.T) {
	t.Setenv("FREEBUFF_TEST_CUSTOM_INSTANCE", "inst-real-upstream-888")
	s, client := newTestServerWithChild(t)
	defer client.stop()

	// 1. Initial turn
	req1 := `{"model":"` + defaultModel + `","messages":[{"role":"user","content":"first prompt"}]}`
	r1 := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(req1))
	r1.Header.Set("X-Freebuff-Session-ID", "gen-sess-1")
	rec1 := httptest.NewRecorder()
	s.chatCompletions(rec1, r1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("req1 failed: %d %s", rec1.Code, rec1.Body.String())
	}

	// Inspect owning session API
	statusReq := httptest.NewRequest("GET", "/v1/sessions/gen-sess-1", nil)
	statusReq.SetPathValue("id", "gen-sess-1")
	statusRec := httptest.NewRecorder()
	s.sessionStatus(statusRec, statusReq)
	if statusRec.Code != http.StatusOK {
		t.Fatalf("GET /v1/sessions/gen-sess-1 failed: %d %s", statusRec.Code, statusRec.Body.String())
	}
	var statusData map[string]any
	if err := json.Unmarshal(statusRec.Body.Bytes(), &statusData); err != nil {
		t.Fatal(err)
	}

	if statusData["instance_id"] != "inst-real-upstream-888" {
		t.Fatalf("expected instance_id 'inst-real-upstream-888', got %#v", statusData["instance_id"])
	}
	if statusData["generation"] != float64(1) {
		t.Fatalf("expected generation 1 on initial turn, got %#v", statusData["generation"])
	}

	// 2. Normal continuation turn: generation must NOT increment
	req2 := `{"model":"` + defaultModel + `","messages":[{"role":"user","content":"first prompt"},{"role":"assistant","content":"ACK"},{"role":"user","content":"second prompt"}]}`
	r2 := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(req2))
	r2.Header.Set("X-Freebuff-Session-ID", "gen-sess-1")
	rec2 := httptest.NewRecorder()
	s.chatCompletions(rec2, r2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("req2 failed: %d %s", rec2.Code, rec2.Body.String())
	}

	statusRec2 := httptest.NewRecorder()
	s.sessionStatus(statusRec2, statusReq)
	var statusData2 map[string]any
	_ = json.Unmarshal(statusRec2.Body.Bytes(), &statusData2)
	if statusData2["generation"] != float64(1) {
		t.Fatalf("generation incremented on normal continuation: %#v", statusData2["generation"])
	}

	// 3. History edit (native rebuild): generation must increment
	reqEdit := `{"model":"` + defaultModel + `","messages":[{"role":"user","content":"EDITED FIRST PROMPT"},{"role":"assistant","content":"ACK"},{"role":"user","content":"second prompt"}]}`
	rEdit := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqEdit))
	rEdit.Header.Set("X-Freebuff-Session-ID", "gen-sess-1")
	recEdit := httptest.NewRecorder()
	s.chatCompletions(recEdit, rEdit)
	if recEdit.Code != http.StatusOK {
		t.Fatalf("reqEdit failed: %d %s", recEdit.Code, recEdit.Body.String())
	}

	statusRec3 := httptest.NewRecorder()
	s.sessionStatus(statusRec3, statusReq)
	var statusData3 map[string]any
	_ = json.Unmarshal(statusRec3.Body.Bytes(), &statusData3)
	if statusData3["generation"] != float64(2) {
		t.Fatalf("generation did not increment on history edit: %#v", statusData3["generation"])
	}
	if statusData3["rebuild_reason"] != "history_edit" {
		t.Fatalf("rebuild_reason not set to history_edit: %#v", statusData3["rebuild_reason"])
	}

	// 4. Model change: generation must increment
	reqModel := `{"model":"` + gptLunaModel + `","messages":[{"role":"user","content":"new question on luna"}]}`
	rModel := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqModel))
	rModel.Header.Set("X-Freebuff-Session-ID", "gen-sess-1")
	recModel := httptest.NewRecorder()
	s.chatCompletions(recModel, rModel)
	if recModel.Code != http.StatusOK {
		t.Fatalf("reqModel failed: %d %s", recModel.Code, recModel.Body.String())
	}

	statusRec4 := httptest.NewRecorder()
	s.sessionStatus(statusRec4, statusReq)
	var statusData4 map[string]any
	_ = json.Unmarshal(statusRec4.Body.Bytes(), &statusData4)
	if statusData4["generation"] != float64(3) {
		t.Fatalf("generation did not increment on model change: %#v", statusData4["generation"])
	}
}

// 6. Actual child reset
func TestActualChildReset(t *testing.T) {
	_, client := newTestServerWithChild(t)
	defer client.stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Initial chat to start the child process
	first := []chatMessage{{Role: "user", Content: "hello child"}}
	_, err := client.chat(ctx, defaultModel, "child-sess", "hello child", nil, nil, nil, nil, first)
	if err != nil {
		t.Fatalf("client.chat failed: %v", err)
	}

	// Call resetExternalSession
	if err := client.resetExternalSession("child-sess"); err != nil {
		t.Fatalf("resetExternalSession failed: %v", err)
	}

	// Verify session was cleared
	if client.sessions[scopedSessionID(defaultModel, "child-sess")] {
		t.Fatal("session was not removed from cliClient.sessions")
	}
}

// 7. Pending tool call expiry and recovery
func TestPendingToolCallExpiryAndRecovery(t *testing.T) {
	client := &cliClient{}
	now := time.Now()
	client.pending = &pendingToolCall{
		SessionID:  "pending-sess",
		ToolCallID: "call-1",
		CreatedAt:  now.Add(-10 * time.Minute),
	}
	if !client.recoverExpiredToolCall(now) {
		t.Fatal("expected recoverExpiredToolCall to detect expired tool")
	}
	if client.pending != nil {
		t.Fatal("expected pending state to be cleared after recovery")
	}
	// Expired record moved into expiredTools
	call, found := client.expiredTools["call-1"]
	if !found {
		t.Fatal("expired tool not found in expiredTools map")
	}
	if call.ExpiredAt.IsZero() {
		t.Fatal("expired tool did not have ExpiredAt stamped")
	}
}

// 8. Concurrency: concurrent requests and status checks
func TestConcurrentChatAndStatusRequests(t *testing.T) {
	s, _ := newTestServerWithChild(t)

	var wg sync.WaitGroup
	errCh := make(chan error, 20)
	var okCount, busyCount int64

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sessID := fmt.Sprintf("concurrent-sess-%d", idx%3)
			reqBody := fmt.Sprintf(`{"model":"%s","messages":[{"role":"user","content":"ping %d"}]}`, defaultModel, idx)
			req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqBody))
			req.Header.Set("X-Freebuff-Session-ID", sessID)
			rec := httptest.NewRecorder()
			s.chatCompletions(rec, req)
			switch rec.Code {
			case http.StatusOK:
				atomic.AddInt64(&okCount, 1)
			case http.StatusTooManyRequests:
				atomic.AddInt64(&busyCount, 1)
				if !strings.Contains(rec.Body.String(), "session_busy") {
					errCh <- fmt.Errorf("concurrent chat %d got 429 but not session_busy: %s", idx, rec.Body.String())
					return
				}
			default:
				errCh <- fmt.Errorf("concurrent chat %d failed with unexpected code %d: %s", idx, rec.Code, rec.Body.String())
				return
			}

			// Concurrently query session status
			statReq := httptest.NewRequest("GET", "/v1/sessions/"+sessID, nil)
			statReq.SetPathValue("id", sessID)
			statRec := httptest.NewRecorder()
			s.sessionStatus(statRec, statReq)
			if statRec.Code != http.StatusOK {
				errCh <- fmt.Errorf("concurrent status %d failed with code %d", idx, statRec.Code)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	if atomic.LoadInt64(&okCount) == 0 {
		t.Fatal("expected at least one successful chat completion")
	}
	if atomic.LoadInt64(&busyCount) == 0 {
		t.Fatal("expected contention to reject overlapping execution with 429 session_busy")
	}
}

// 9. UnmarshalJSON UseNumber precision test for 9007199254740993 (2^53 + 1)
func TestChatRequestUnmarshalUseNumberPrecision(t *testing.T) {
	rawJSON := `{"model":"` + defaultModel + `","messages":[{"role":"user","content":[{"type":"text","text":"hello","value":9007199254740993}]}]}`
	var req chatRequest
	if err := json.Unmarshal([]byte(rawJSON), &req); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(req.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(req.Messages))
	}
	contentList, ok := req.Messages[0].Content.([]any)
	if !ok || len(contentList) != 1 {
		t.Fatalf("expected []any content, got %#v", req.Messages[0].Content)
	}
	part, ok := contentList[0].(map[string]any)
	if !ok {
		t.Fatalf("expected map part, got %#v", contentList[0])
	}
	numVal, ok := part["value"].(json.Number)
	if !ok {
		t.Fatalf("expected json.Number for 9007199254740993, got %T (%v)", part["value"], part["value"])
	}
	if numVal.String() != "9007199254740993" {
		t.Fatalf("precision lost: expected '9007199254740993', got %q", numVal.String())
	}
	intVal, err := numVal.Int64()
	if err != nil || intVal != 9007199254740993 {
		t.Fatalf("expected int64 9007199254740993, got %d (err: %v)", intVal, err)
	}

	// Also verify via HTTP server chatCompletions handler
	s, _ := newTestServerWithChild(t)
	httpReq := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(rawJSON))
	httpRec := httptest.NewRecorder()
	s.chatCompletions(httpRec, httpReq)
	if httpRec.Code != http.StatusOK {
		t.Fatalf("expected 200 via HTTP with large integer, got %d: %s", httpRec.Code, httpRec.Body.String())
	}
}

// 10. HTTP-level tool_call roundtrip and result resolution using TestMain child fixture
func TestHTTPToolCallAndResultResolution(t *testing.T) {
	t.Setenv("FREEBUFF_TEST_TRIGGER_TOOL", "1")
	s, _ := newTestServerWithChild(t)

	sessID := "tool-e2e-sess-1"

	// 1. First turn: user prompt + tools definition => Expect 200 with tool_calls
	reqBody1 := `{
		"model":"` + defaultModel + `",
		"messages":[{"role":"user","content":"lookup something"}],
		"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}]
	}`
	r1 := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqBody1))
	r1.Header.Set("X-Freebuff-Session-ID", sessID)
	rec1 := httptest.NewRecorder()
	s.chatCompletions(rec1, r1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first turn failed: %d %s", rec1.Code, rec1.Body.String())
	}

	var resp1 struct {
		Choices []struct {
			Message struct {
				Role      string           `json:"role"`
				ToolCalls []openAIToolCall `json:"tool_calls"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(rec1.Body.Bytes(), &resp1); err != nil {
		t.Fatalf("unmarshal turn 1 response: %v", err)
	}
	if len(resp1.Choices) == 0 || resp1.Choices[0].FinishReason != "tool_calls" {
		t.Fatalf("expected finish_reason 'tool_calls', got %#v", resp1)
	}
	if len(resp1.Choices[0].Message.ToolCalls) == 0 {
		t.Fatalf("expected tool_calls in message, got none")
	}
	toolCall := resp1.Choices[0].Message.ToolCalls[0]
	if toolCall.ID != "call-test-fixture-1" || toolCall.Function.Name != "lookup" {
		t.Fatalf("unexpected tool call: %#v", toolCall)
	}

	// Verify session status indicates pending_tool
	statusReq := httptest.NewRequest("GET", "/v1/sessions/"+sessID, nil)
	statusReq.SetPathValue("id", sessID)
	statusRec := httptest.NewRecorder()
	s.sessionStatus(statusRec, statusReq)
	var statData map[string]any
	_ = json.Unmarshal(statusRec.Body.Bytes(), &statData)
	if statData["pending_tool"] != true {
		t.Fatalf("expected pending_tool true after tool_call turn, got %#v", statData["pending_tool"])
	}

	// 2. Second turn: tool result callback => Expect 200 with resolved text
	reqBody2 := fmt.Sprintf(`{
		"model":"%s",
		"messages":[
			{"role":"user","content":"lookup something"},
			{"role":"assistant","tool_calls":[{"id":"%s","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"test\"}"}}]},
			{"role":"tool","tool_call_id":"%s","content":"lookup result: matched document 42"}
		]
	}`, defaultModel, toolCall.ID, toolCall.ID)

	r2 := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(reqBody2))
	r2.Header.Set("X-Freebuff-Session-ID", sessID)
	rec2 := httptest.NewRecorder()
	s.chatCompletions(rec2, r2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("second turn failed: %d %s", rec2.Code, rec2.Body.String())
	}

	var resp2 struct {
		Choices []struct {
			Message struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("unmarshal turn 2 response: %v", err)
	}
	if len(resp2.Choices) == 0 || resp2.Choices[0].FinishReason != "stop" {
		t.Fatalf("expected finish_reason 'stop', got %#v", resp2)
	}
	if !strings.Contains(resp2.Choices[0].Message.Content, "matched document 42") {
		t.Fatalf("expected assistant content to contain tool output, got: %q", resp2.Choices[0].Message.Content)
	}

	// Verify session status indicates pending_tool is now cleared
	statusRec2 := httptest.NewRecorder()
	s.sessionStatus(statusRec2, statusReq)
	var statData2 map[string]any
	_ = json.Unmarshal(statusRec2.Body.Bytes(), &statData2)
	if statData2["pending_tool"] != false {
		t.Fatalf("expected pending_tool false after resolution, got %#v", statData2["pending_tool"])
	}
}
