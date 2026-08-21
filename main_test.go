package main

import (
	"fmt"
	"sync"
	"testing"
)

func TestGatewayModelListIncludesDeepSeekAndMiMo(t *testing.T) {
	models := gatewayModelList()
	if len(models) != 2 || models[0]["id"] != defaultModel || models[1]["id"] != mimoModel {
		t.Fatalf("unexpected model catalog: %#v", models)
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
	if got := router.resolve(request, "").ID; got != "user-request-user" {
		t.Fatalf("user session id = %q", got)
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
