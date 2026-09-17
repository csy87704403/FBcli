package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestStreamWhitespacePreserved(t *testing.T) {
	var output strings.Builder
	b := streamTextBuffer{emit: func(s string) { output.WriteString(s) }}
	for _, chunk := range []string{"\n", "def f():", "\n", "    ", "return", " ", "42", "\n"} {
		b.push(chunk)
	}
	b.finish(false)
	if output.String() != "\ndef f():\n    return 42\n" {
		t.Fatalf("corrupted code %q", output.String())
	}
	output.Reset()
	b = streamTextBuffer{emit: func(s string) { output.WriteString(s) }}
	b.push("\n\n")
	b.finish(true)
	if output.Len() != 0 {
		t.Fatal("blank content emitted before a tool-only response")
	}
}

func TestClientHistoryMustExtendExactly(t *testing.T) {
	old := []chatMessage{{Role: "user", Content: "remember RED"}, {Role: "assistant", Content: "OK"}, {Role: "user", Content: "continue"}}
	prior := messageHashes(old)
	next := append(append([]chatMessage{}, old...), chatMessage{Role: "assistant", Content: "yes"}, chatMessage{Role: "user", Content: "next"})
	if !historyExtends(prior, messageHashes(next)) {
		t.Fatal("valid continuation rejected")
	}
	for _, changed := range [][]chatMessage{old, {{Role: "user", Content: "new conversation"}}, {{Role: "user", Content: "remember BLUE"}, old[1], old[2], next[3], next[4]}} {
		if historyExtends(prior, messageHashes(changed)) {
			t.Fatal("reset/edited history accepted as continuation")
		}
	}
}

func integrityServer(t *testing.T) *server {
	t.Helper()
	store := &stateStore{path: filepath.Join(t.TempDir(), "state.json"), state: gatewayState{APIKeys: []string{"key-a", "key-b"}}}
	return &server{admin: &adminService{store: store}, sessions: newConversationRouter(), accounts: &accountManager{store: store, runtimes: map[string]*accountRuntime{}, sessionAccounts: map[string]accountSessionBinding{}}}
}

func TestAuthenticatedSessionIsolation(t *testing.T) {
	s := integrityServer(t)
	selected := map[string]*server{}
	handler := s.tenantHandler(func(tenant *server, w http.ResponseWriter, r *http.Request) { selected[apiKeyFromRequest(r)] = tenant })
	for _, key := range []string{"key-a", "key-b"} {
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer "+key)
		handler(httptest.NewRecorder(), r)
	}
	a, b := selected["key-a"], selected["key-b"]
	if a == b || a.accountSession("hermes-main") == b.accountSession("hermes-main") {
		t.Fatal("keys share a namespace")
	}
	a.sessions.observeExternal("hermes-main", 3, false)
	if _, found := b.sessions.sessionStatus("hermes-main"); found {
		t.Fatal("cross-key session visible")
	}
	b.sessions.resetSession("hermes-main")
	if _, found := a.sessions.sessionStatus("hermes-main"); !found {
		t.Fatal("cross-key reset affected owner")
	}
	a.sessions.byToolCall["call-a"] = sessionBinding{ID: "hermes-main"}
	if _, found := b.sessions.byToolCall["call-a"]; found {
		t.Fatal("cross-key tool callback visible")
	}
	r := httptest.NewRequest("GET", "/v1/sessions/hermes-main", nil)
	r.SetPathValue("id", "hermes-main")
	w := httptest.NewRecorder()
	b.sessionStatus(w, r)
	if w.Code != 404 {
		t.Fatalf("cross-key GET returned %d", w.Code)
	}
}

func TestStreamAdmissionFailureUsesHTTPError(t *testing.T) {
	s := integrityServer(t)
	body, _ := json.Marshal(chatRequest{Model: defaultModel, Stream: true, Messages: []chatMessage{{Role: "user", Content: "hello"}}})
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	s.chatCompletions(w, r)
	if w.Code != 503 || !strings.Contains(w.Header().Get("Content-Type"), "application/json") || strings.Contains(w.Body.String(), "[DONE]") {
		t.Fatalf("unexpected response: %d %s %s", w.Code, w.Header(), w.Body.String())
	}
}
