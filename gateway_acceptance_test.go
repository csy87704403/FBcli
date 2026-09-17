package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOversizedContextFailsBeforeCLIRequest(t *testing.T) {
	t.Setenv("FREEBUFF_CONTEXT_LIMIT", "10000")
	t.Setenv("FREEBUFF_CONTEXT_RESERVE", "0")
	service := &server{sessions: newConversationRouter()}
	body, err := json.Marshal(chatRequest{Messages: []chatMessage{{Role: "user", Content: strings.Repeat("x", 50000)}}})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
	response := httptest.NewRecorder()
	service.chatCompletions(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413: %s", response.Code, response.Body.String())
	}
	var result struct {
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Error.Type != "context_exceeded" || !strings.Contains(result.Error.Message, "estimated context") {
		t.Fatalf("unexpected error: %#v", result.Error)
	}
}

func TestExternalSessionStatusTracksContextEstimate(t *testing.T) {
	router := newConversationRouter()
	router.observeExternal("hermes-main", 10, false)
	router.setExternalTokenEstimate("hermes-main", 1234)
	first, ok := router.sessionStatus("hermes-main")
	if !ok || first.MessageCount != 10 || first.EstimatedTokens != 1234 || first.Created.IsZero() {
		t.Fatalf("first observation = %#v, found=%t", first, ok)
	}
	router.observeExternal("hermes-main", 12, false)
	second, _ := router.sessionStatus("hermes-main")
	if !second.Created.Equal(first.Created) {
		t.Fatalf("session creation timestamp changed: %s to %s", first.Created, second.Created)
	}
}

func TestSessionCreateReturnsQueryableID(t *testing.T) {
	service := &server{sessions: newConversationRouter()}
	response := httptest.NewRecorder()
	service.sessionCreate(response, httptest.NewRequest(http.MethodPost, "/v1/sessions", nil))
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", response.Code)
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil || created.ID == "" {
		t.Fatalf("invalid created session: %#v, %v", created, err)
	}
	if _, ok := service.sessions.sessionStatus(created.ID); !ok {
		t.Fatal("created session is not queryable")
	}
}

func TestAmbiguousHistoryStillRoutesExactToolResult(t *testing.T) {
	router := newConversationRouter()
	request := chatRequest{Messages: []chatMessage{{Role: "user", Content: "same opening"}, {Role: "assistant", Content: "same answer"}, {Role: "user", Content: "run a tool"}}}
	router.bind(request, cliChatResult{Text: "one"}, sessionSelection{ID: "one", Automatic: true})
	call := openAIToolCall{ID: "call-exact", Type: "function"}
	call.Function.Name = "terminal"
	call.Function.Arguments = `{}`
	router.bind(request, cliChatResult{ToolCalls: []openAIToolCall{call}}, sessionSelection{ID: "two", Automatic: true})
	request.Messages = append(request.Messages, chatMessage{Role: "assistant", ToolCalls: []openAIToolCall{call}}, chatMessage{Role: "tool", ToolCallID: call.ID, Content: "210"})
	if selected := router.resolve(request, ""); selected.ID != "two" {
		t.Fatalf("tool result routed to %q instead of its owner", selected.ID)
	}
	key, _ := historyKey(request)
	if router.byHistory[key].ID != "" {
		t.Fatal("ambiguous history must remain unbound")
	}
}

func TestResetRemovesRawAutomaticSessionRoutes(t *testing.T) {
	router := newConversationRouter()
	router.byHistory["history"] = sessionBinding{ID: "auto-target"}
	router.byFallback["fallback"] = sessionBinding{ID: "auto-target"}
	router.byToolCall["call"] = sessionBinding{ID: "auto-target"}
	router.byHistory["other"] = sessionBinding{ID: "auto-other"}
	router.resetSession("auto-target")
	if len(router.byHistory) != 1 || router.byHistory["other"].ID != "auto-other" || len(router.byFallback) != 0 || len(router.byToolCall) != 0 {
		t.Fatal("reset retained target routes or removed an unrelated session")
	}
}

func TestExpiredAbandonedToolReleasesAccountAndRejectsLateResult(t *testing.T) {
	t.Setenv("FREEBUFF_TOOL_TIMEOUT", "1s")
	now := time.Now()
	key := accountSessionKey("abandoned")
	binding := accountSessionBinding{AccountID: "one", Updated: now.Add(-2 * time.Hour)}
	store := &stateStore{path: filepath.Join(t.TempDir(), "state.json"), state: gatewayState{AccountSessions: map[string]accountSessionBinding{key: binding}}}
	client := &cliClient{pending: &pendingToolCall{SessionID: scopedSessionID(defaultModel, "abandoned"), ToolCallID: "late-tool", CreatedAt: now.Add(-2 * time.Hour)}}
	manager := &accountManager{store: store, runtimes: map[string]*accountRuntime{"one": {client: client}}, sessionAccounts: map[string]accountSessionBinding{key: binding}}
	manager.reapIdleProcessesOnce(now)
	if client.pendingSession() != "" {
		t.Fatal("expired tool still holds the account")
	}
	if err := manager.toolResultError("abandoned", defaultModel, "late-tool"); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("late result error: %v", err)
	}
	if err := manager.resetSession("abandoned"); err != nil {
		t.Fatal(err)
	}
	client.recoverExpiredToolCall(now.Add(31 * time.Minute))
	if len(client.expiredTools) != 0 {
		t.Fatal("expiration records did not age out")
	}
}

func TestModelAndProcessSpecificContextOwnership(t *testing.T) {
	client := &cliClient{sessions: map[string]bool{scopedSessionID(defaultModel, "one"): true}}
	if !client.canResume(scopedSessionID(defaultModel, "one")) {
		t.Fatal("existing context was lost")
	}
	if client.canResume(scopedSessionID(mimoModel, "one")) || client.canResume(scopedSessionID(defaultModel, "two")) {
		t.Fatal("unrelated context was treated as resumable")
	}
	client.stop()
	if client.canResume(scopedSessionID(defaultModel, "one")) {
		t.Fatal("old process context survived restart")
	}
}

func TestInvalidOutputBudgetRejectedBeforeSessionMutation(t *testing.T) {
	t.Setenv("FREEBUFF_CONTEXT_LIMIT", "1000")
	for _, maximum := range []int{-1, 1000, 1001} {
		body, _ := json.Marshal(chatRequest{MaxTokens: maximum, Messages: []chatMessage{{Role: "user", Content: "hello"}}})
		response := httptest.NewRecorder()
		(&server{}).chatCompletions(response, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(body))))
		if response.Code != 400 {
			t.Fatalf("max_tokens=%d status=%d", maximum, response.Code)
		}
	}
	if contextInputLimit(chatRequest{MaxTokens: 1000}) != 750 {
		t.Fatal("budget helper must cap reserve independently of HTTP validation")
	}
}
