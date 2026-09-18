package main

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestGatewayVersionIsReleaseTag(t *testing.T) {
	if !regexp.MustCompile(`^v[1-9][0-9]*$`).MatchString(gatewayVersion) {
		t.Fatalf("gateway version %q must be a release tag", gatewayVersion)
	}
}

func TestAccountQuotaErrorClassification(t *testing.T) {
	for _, message := range []string{
		"free session rate_limited",
		"Freebuff session admission returned spend_limited",
		"upstream quota exhausted",
		"model budget exhausted",
	} {
		if !isAccountQuotaError(fmt.Errorf("%s", message)) {
			t.Fatalf("expected quota error: %q", message)
		}
	}
	if isAccountQuotaError(fmt.Errorf("free session admission returned banned")) {
		t.Fatal("banned is not a quota error")
	}
}

func TestGatewayModelListMatchesOfficialFreebuffPicker(t *testing.T) {
	models := gatewayModelList()
	want := []string{deepSeekProModel, defaultModel, gptLunaModel, miniMaxModel, mimoModel}
	if len(models) != len(want) {
		t.Fatalf("model count = %d, want %d: %#v", len(models), len(want), models)
	}
	for index, model := range models {
		if model["id"] != want[index] || model["x_freebuff_admission"] != "official" {
			t.Fatalf("model %d = %#v, want %q", index, model, want[index])
		}
		if model["context_length"] != defaultContextLimit || model["x_freebuff_context_reserve"] != defaultContextReserve {
			t.Fatalf("model %d missing context metadata: %#v", index, model)
		}
	}
}

func TestGatewayModelListUsesGatewayBudgetMetadata(t *testing.T) {
	t.Setenv("FREEBUFF_CONTEXT_LIMIT", "10000")
	t.Setenv("FREEBUFF_CONTEXT_RESERVE", "500")
	model := gatewayModelList()[0]
	if model["context_length"] != 10000 || model["x_freebuff_context_reserve"] != 500 {
		t.Fatalf("model budget metadata = %#v", model)
	}
	if model["max_tokens"] != 2500 {
		t.Fatal("advertised output budget must follow the gateway context cap")
	}
}

func TestEstimateContextTokensIncludesTools(t *testing.T) {
	request := chatRequest{Messages: []chatMessage{{Role: "user", Content: strings.Repeat("x", 400)}}, Tools: []openAITool{{Type: "function", Function: openAIFunction{Name: "inspect", Parameters: []byte(`{"type":"object"}`)}}}}
	if got := estimateContextTokens(request); got < 100 {
		t.Fatalf("estimated tokens = %d, want tool and message payload included", got)
	}
}

func TestEstimateContextTokensProtectsNonASCII(t *testing.T) {
	request := chatRequest{Messages: []chatMessage{{Role: "user", Content: strings.Repeat("中", 400)}}}
	if got := estimateContextTokens(request); got < 280 || got > 310 {
		t.Fatalf("CJK estimate = %d, want approximately 0.7 tokens per character plus framing", got)
	}
}

func TestConversationRouterDetectsMessageDrop(t *testing.T) {
	router := newConversationRouter()
	if router.observeExternal("hermes-main", 869, false) {
		t.Fatal("first observation cannot be a reset")
	}
	if !router.observeExternal("hermes-main", 2, false) {
		t.Fatal("expected large message drop to trigger reset")
	}
	if router.observeExternal("hermes-main", 1, true) {
		t.Fatal("tool result should not trigger implicit reset")
	}
}

func TestSessionIdleTTLIsConfigurable(t *testing.T) {
	t.Setenv("FREEBUFF_SESSION_IDLE_TTL", "45m")
	if got := sessionIdleTTL(); got != 45*time.Minute {
		t.Fatalf("session idle ttl = %s, want 45m", got)
	}
}

func TestAccountFailoverDefaultsEnabledAndCanDisable(t *testing.T) {
	if !accountFailoverEnabled() {
		t.Fatal("account failover should default to enabled")
	}
	t.Setenv("FREEBUFF_ACCOUNT_FAILOVER", "false")
	if accountFailoverEnabled() {
		t.Fatal("account failover should be disableable")
	}
}

func TestPromptFromMessages(t *testing.T) {
	messages := []chatMessage{
		{Role: "system", Content: "Be concise."},
		{Role: "assistant", Content: "old"},
		{Role: "user", Content: "hello"},
	}
	want := "System instructions:\nBe concise.\n\nUser message:\nhello"
	if got := promptFromMessages(messages); got != want {
		t.Fatalf("prompt = %q, want %q", got, want)
	}
}

func TestConversationRouterRestoresBoundSession(t *testing.T) {
	router := newConversationRouter()
	first := chatRequest{Model: defaultModel, Messages: []chatMessage{
		{Role: "system", Content: "runtime context A"},
		{Role: "user", Content: []any{
			map[string]any{"type": "text", "text": "inspect this image"},
			map[string]any{"type": "image_url", "image_url": map[string]any{"url": "data:image/png;base64,aGk="}},
		}},
	}}
	initial := router.resolve(first, "")
	router.bind(first, cliChatResult{Text: "image answer"}, initial)
	resumed := chatRequest{Model: defaultModel, Messages: []chatMessage{
		{Role: "system", Content: "runtime context B"},
		{Role: "user", Content: "inspect this image\n\n[Image attached at: C:\\tmp\\image.png]\n[screenshot]"},
		{Role: "assistant", Content: "image answer"},
		{Role: "user", Content: "what was in it?"},
	}}
	if got := router.resolve(resumed, "").ID; got != initial.ID {
		t.Fatalf("resumed session = %q, want %q", got, initial.ID)
	}
}

func TestConversationRouterSeparatesSameOpening(t *testing.T) {
	router := newConversationRouter()
	first := chatRequest{Model: defaultModel, Messages: []chatMessage{{Role: "user", Content: "same opening"}}}
	one := router.resolve(first, "")
	two := router.resolve(first, "")
	if one.ID == two.ID {
		t.Fatal("identical opening requests shared a session id")
	}
	router.bind(first, cliChatResult{Text: "answer one"}, one)
	router.bind(first, cliChatResult{Text: "answer two"}, two)
	continueOne := chatRequest{Model: defaultModel, Messages: []chatMessage{
		{Role: "user", Content: "same opening"}, {Role: "assistant", Content: "answer one"}, {Role: "user", Content: "next"},
	}}
	continueTwo := chatRequest{Model: defaultModel, Messages: []chatMessage{
		{Role: "user", Content: "same opening"}, {Role: "assistant", Content: "answer two"}, {Role: "user", Content: "next"},
	}}
	if got := router.resolve(continueOne, "").ID; got != one.ID {
		t.Fatalf("first continuation = %q, want %q", got, one.ID)
	}
	if got := router.resolve(continueTwo, "").ID; got != two.ID {
		t.Fatalf("second continuation = %q, want %q", got, two.ID)
	}
}

func TestConversationRouterConcurrentOpeningsUnique(t *testing.T) {
	router := newConversationRouter()
	request := chatRequest{Model: defaultModel, Messages: []chatMessage{{Role: "user", Content: "same opening"}}}
	const count = 64
	ids := make(chan string, count)
	var wait sync.WaitGroup
	for range count {
		wait.Add(1)
		go func() {
			defer wait.Done()
			ids <- router.resolve(request, "").ID
		}()
	}
	wait.Wait()
	close(ids)
	seen := make(map[string]struct{}, count)
	for id := range ids {
		if _, exists := seen[id]; exists {
			t.Fatalf("concurrent opening reused session id %q", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != count {
		t.Fatalf("unique sessions = %d, want %d", len(seen), count)
	}
}

func TestConversationRouterFailsClosedOnAmbiguousHistory(t *testing.T) {
	router := newConversationRouter()
	first := chatRequest{Model: defaultModel, Messages: []chatMessage{{Role: "user", Content: "same opening"}}}
	one := router.resolve(first, "")
	two := router.resolve(first, "")
	router.bind(first, cliChatResult{Text: "same answer"}, one)
	router.bind(first, cliChatResult{Text: "same answer"}, two)
	continuation := chatRequest{Model: defaultModel, Messages: []chatMessage{
		{Role: "user", Content: "same opening"}, {Role: "assistant", Content: "same answer"}, {Role: "user", Content: "next"},
	}}
	got := router.resolve(continuation, "").ID
	if got == one.ID || got == two.ID {
		t.Fatal("ambiguous history reused an existing session")
	}
}

func TestConversationRouterExplicitIDWins(t *testing.T) {
	router := newConversationRouter()
	request := chatRequest{Model: defaultModel, User: "request-user", Messages: []chatMessage{{Role: "user", Content: "hello"}}}
	if got := router.resolve(request, "agent-session").ID; got != "agent-session" {
		t.Fatalf("header session id = %q", got)
	}
	if got := router.resolve(request, "").ID; got == "user-request-user" {
		t.Fatalf("OpenAI user field was incorrectly used as a session id: %q", got)
	}
}

func TestConversationRouterReusesRecentHermesFallback(t *testing.T) {
	router := newConversationRouter()
	first := chatRequest{Model: defaultModel, User: "hermes-user-1", Messages: []chatMessage{{Role: "user", Content: "first task"}}}
	selection := router.resolve(first, "", defaultModel)
	if selection.FallbackKey == "" {
		t.Fatal("Hermes fallback key was not created")
	}
	router.bind(first, cliChatResult{Text: "done"}, selection)
	next := chatRequest{Model: defaultModel, User: "hermes-user-1", Messages: []chatMessage{{Role: "user", Content: "follow up without history"}}}
	resumed := router.resolve(next, "", defaultModel)
	if resumed.ID != selection.ID || resumed.FallbackKey == "" {
		t.Fatalf("short Hermes fallback did not resume: %#v", resumed)
	}
}

func TestConversationRouterHermesFallbackIsModelScoped(t *testing.T) {
	router := newConversationRouter()
	first := chatRequest{Model: defaultModel, User: "hermes-user-1", Messages: []chatMessage{{Role: "user", Content: "first task"}}}
	selection := router.resolve(first, "", defaultModel)
	router.bind(first, cliChatResult{Text: "done"}, selection)
	other := router.resolve(chatRequest{Model: mimoModel, User: "hermes-user-1", Messages: []chatMessage{{Role: "user", Content: "mimo task"}}}, "", mimoModel)
	if other.ID == selection.ID {
		t.Fatal("fallback crossed model boundary")
	}
}

func TestScopedSessionIDSeparatesModelsButKeepsSameModel(t *testing.T) {
	if scopedSessionID(defaultModel, "agent") == scopedSessionID(mimoModel, "agent") {
		t.Fatal("different models shared the same CLI session id")
	}
	if scopedSessionID(defaultModel, "agent") != scopedSessionID(defaultModel, "agent") {
		t.Fatal("same model did not keep a stable CLI session id")
	}
}

func TestConversationRouterRestoresHermesToolResultByCallID(t *testing.T) {
	router := newConversationRouter()
	first := chatRequest{Model: defaultModel, Messages: []chatMessage{{Role: "user", Content: "inspect files"}}}
	selection := router.resolve(first, "")
	call := openAIToolCall{ID: "call-hermes-1"}
	call.Function.Name = "search_files"
	router.bind(first, cliChatResult{ToolCalls: []openAIToolCall{call}}, selection)
	toolResult := chatRequest{Model: defaultModel, Messages: []chatMessage{
		{Role: "user", Content: "inspect files"},
		{Role: "assistant", ToolCalls: []openAIToolCall{call}},
		{Role: "tool", ToolCallID: call.ID, Content: "[]"},
	}}
	if got := router.resolve(toolResult, "").ID; got != selection.ID {
		t.Fatalf("Hermes tool result session = %q, want %q", got, selection.ID)
	}
}

func TestConversationRouterCapsPendingToolBindings(t *testing.T) {
	router := newConversationRouter()
	request := chatRequest{Model: defaultModel, Messages: []chatMessage{{Role: "user", Content: "inspect files"}}}
	selection := router.resolve(request, "")
	calls := make([]openAIToolCall, maxToolCallBindings+1)
	for index := range calls {
		calls[index].ID = fmt.Sprintf("call-%d", index)
	}
	router.bind(request, cliChatResult{ToolCalls: calls}, selection)
	if got := len(router.byToolCall); got != maxToolCallBindings {
		t.Fatalf("pending tool bindings = %d, want %d", got, maxToolCallBindings)
	}
}

func TestUpstreamErrorStatus(t *testing.T) {
	tests := []struct {
		message string
		status  int
		typeID  string
	}{
		{"free session rate_limited", 429, "upstream_rate_limited"},
		{"upstream quota exhausted", 429, "upstream_rate_limited"},
		{"service_overloaded", 503, "upstream_unavailable"},
		{"context deadline exceeded", 504, "upstream_timeout"},
	}
	for _, test := range tests {
		status, typeID := upstreamErrorStatus(test.message)
		if status != test.status || typeID != test.typeID {
			t.Fatalf("%q = (%d, %q), want (%d, %q)", test.message, status, typeID, test.status, test.typeID)
		}
	}
}

func TestMultimodalContentConvertsDataURL(t *testing.T) {
	messages := []chatMessage{{Role: "user", Content: []any{
		map[string]any{"type": "text", "text": "inspect"},
		map[string]any{"type": "image_url", "image_url": map[string]any{"url": "data:image/png;base64,aGk="}},
	}}}
	content, err := multimodalContent(messages, "inspect")
	if err != nil {
		t.Fatal(err)
	}
	if len(content) != 2 || content[1]["mediaType"] != "image/png" || content[1]["image"] != "aGk=" {
		t.Fatalf("unexpected content: %#v", content)
	}
}

func TestMultimodalContentRejectsRemoteURL(t *testing.T) {
	messages := []chatMessage{{Role: "user", Content: []any{
		map[string]any{"type": "text", "text": "inspect"},
		map[string]any{"type": "image_url", "image_url": map[string]any{"url": "https://example.com/a.png"}},
	}}}
	if _, err := multimodalContent(messages, "inspect"); err == nil {
		t.Fatal("expected remote image URL to be rejected")
	}
}
