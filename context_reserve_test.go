package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCappedContextReserve(t *testing.T) {
	t.Setenv("FREEBUFF_CONTEXT_LIMIT", "131072")
	t.Setenv("FREEBUFF_CONTEXT_RESERVE", "8192")
	for _, tc := range []struct{ output, input int }{
		{0, 122880}, {50, 131022}, {8192, 122880},
		{32768, 98304}, {57344, 98304}, {65536, 98304},
	} {
		req := chatRequest{MaxTokens: tc.output}
		if got := contextInputLimit(req); got != tc.input {
			t.Fatalf("output=%d input=%d want=%d", tc.output, got, tc.input)
		}
		if req.MaxTokens != tc.output {
			t.Fatal("budget computation mutated request")
		}
	}
	if gatewayModelList()[0]["max_tokens"] != 32768 {
		t.Fatal("incorrect advertised output budget")
	}
}

func TestHeavyAgentContextAdmission(t *testing.T) {
	t.Setenv("FREEBUFF_CONTEXT_LIMIT", "131072")
	t.Setenv("FREEBUFF_CONTEXT_RESERVE", "8192")
	s, _ := newTestServerWithChild(t)
	for _, tc := range []struct{ chars, status int }{{72520, 200}, {100000, 413}} {
		req := chatRequest{
			Model: defaultModel, MaxTokens: 65536,
			Messages: []chatMessage{
				{Role: "system", Content: strings.Repeat("x", tc.chars)},
				{Role: "user", Content: "hello"},
			},
		}
		body, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		rec := httptest.NewRecorder()
		s.chatCompletions(rec, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(body))))
		if rec.Code != tc.status {
			t.Fatalf("chars=%d status=%d want=%d: %.400s", tc.chars, rec.Code, tc.status, rec.Body.String())
		}
		if tc.status == 413 && (!strings.Contains(rec.Body.String(), "input limit 98304") || !strings.Contains(rec.Body.String(), "reserve 32768")) {
			t.Fatalf("wrong budget diagnostic: %s", rec.Body.String())
		}
	}
}
