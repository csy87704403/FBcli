package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// Local JSONL fixture: no credentials, network or real model are involved.
func TestMain(m *testing.M) {
	if os.Getenv("FREEBUFF_TEST_JSONL_CHILD") == "1" {
		fmt.Println(`{"type":"ready"}`)
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			var request map[string]any
			if json.Unmarshal(scanner.Bytes(), &request) != nil {
				continue
			}
			response, _ := json.Marshal(map[string]any{"type": "result", "id": request["id"], "text": request["prompt"]})
			fmt.Println(string(response))
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestHistoryEditRebuildsActualCLIProcess(t *testing.T) {
	t.Setenv("FREEBUFF_TEST_JSONL_CHILD", "1")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	client := &cliClient{path: executable, cwd: t.TempDir()}
	defer client.stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first := []chatMessage{{Role: "user", Content: "remember JADE"}}
	call := func(messages []chatMessage) string {
		result, err := client.chat(ctx, defaultModel, "same-session", promptFromMessages(messages), nil, nil, nil, nil, messages)
		if err != nil {
			t.Fatal(err)
		}
		return result.Text
	}
	call(first)
	originalPID := client.processPID
	next := append(append([]chatMessage{}, first...), chatMessage{Role: "assistant", Content: "ACK"}, chatMessage{Role: "user", Content: "continue"})
	if got := call(next); got != "continue" || client.processPID != originalPID {
		t.Fatalf("normal history not reused: %q", got)
	}
	edited := append([]chatMessage{}, next...)
	edited[0].Content = "remember COPPER"
	got := call(edited)
	if !strings.Contains(got, "COPPER") || strings.Contains(got, "JADE") || client.processPID == originalPID {
		t.Fatalf("edited history not rebuilt: %q", got)
	}
	editedPID := client.processPID
	got = call([]chatMessage{{Role: "user", Content: "new session"}})
	if got != "new session" || client.processPID == editedPID {
		t.Fatalf("short reset not rebuilt: %q", got)
	}
}
