package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAgentToolsEstimateAndStructuredBudgetError(t *testing.T) {
	t.Setenv("FREEBUFF_CONTEXT_LIMIT", "131072")
	t.Setenv("FREEBUFF_CONTEXT_RESERVE", "8192")
	req := chatRequest{Model: defaultModel, MaxTokens: 8192,
		Messages: []chatMessage{{Role: "user", Content: strings.Repeat("a", 14500)}},
		Tools:    []openAITool{{Type: "function", Function: openAIFunction{Name: "test_tool", Description: strings.Repeat("b", 63200), Parameters: json.RawMessage(`{"type":"object"}`)}}},
	}
	messages, tools := contextTokenParts(req)
	if messages < 4000 || messages > 4200 || tools < 15800 || tools > 16100 {
		t.Fatalf("incorrect split: messages=%d tools=%d", messages, tools)
	}
	if estimateContextTokens(req) > 21000 {
		t.Fatal("ASCII-heavy request is overcharged")
	}
	service, _ := newTestServerWithChild(t)
	acceptedBody, _ := json.Marshal(req)
	accepted := httptest.NewRecorder()
	service.chatCompletions(accepted, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(acceptedBody))))
	if accepted.Code != 200 {
		t.Fatalf("tool-heavy input rejected: status=%d %.300s", accepted.Code, accepted.Body.String())
	}
	req.Messages[0].Content = strings.Repeat("a", 500000)
	body, _ := json.Marshal(req)
	response := httptest.NewRecorder()
	(&server{}).chatCompletions(response, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(body))))
	if response.Code != 413 {
		t.Fatalf("status=%d", response.Code)
	}
	var result struct {
		Error struct {
			Estimated int `json:"estimated_input_tokens"`
			Limit     int `json:"input_limit"`
			Reduce    int `json:"must_reduce_by"`
			Messages  int `json:"messages_tokens_estimate"`
			Tools     int `json:"tools_tokens_estimate"`
		}
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	e := result.Error
	if e.Limit != 122880 || e.Reduce != e.Estimated-e.Limit || e.Messages+e.Tools != e.Estimated {
		t.Fatalf("inconsistent error: %s", response.Body.String())
	}
	metadata := gatewayModelList()[0]
	if metadata["input_limit_estimate"] != 122880 {
		t.Fatalf("metadata=%v", metadata)
	}
}

func TestMixedTextEstimator(t *testing.T) {
	for _, tc := range []struct {
		text         string
		weight, want int
	}{
		{strings.Repeat("a", 1000), 28, 280},
		{strings.Repeat("a", 1000), 25, 250},
		{strings.Repeat("中", 1000), 28, 700},
		{strings.Repeat("a中", 1000), 28, 980},
		{"", 28, 0},
	} {
		if got := estimateTextTokens([]byte(tc.text), tc.weight); got != tc.want {
			t.Fatalf("got %d want %d", got, tc.want)
		}
	}
}
