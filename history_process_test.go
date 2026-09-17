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
	if len(os.Args) > 1 && os.Args[1] == "--catalog" {
		if os.Getenv("FREEBUFF_TEST_CATALOG_FAIL") == "1" {
			os.Exit(1)
		}
		if os.Getenv("FREEBUFF_TEST_CATALOG_NO_DEFAULT") == "1" {
			fmt.Println(`["other-model"]`)
			os.Exit(0)
		}
		if os.Getenv("FREEBUFF_TEST_CATALOG_DUPLICATE") == "1" {
			fmt.Printf("[\"%s\",\"%s\"]\n", defaultModel, defaultModel)
			os.Exit(0)
		}
		if os.Getenv("FREEBUFF_TEST_CATALOG_BLANK") == "1" {
			fmt.Printf("[\"%s\",\"   \"]\n", defaultModel)
			os.Exit(0)
		}
		catalog := []string{
			deepSeekProModel,
			defaultModel,
			gptLunaModel,
			miniMaxModel,
			mimoModel,
			glmV52Model,
			fableModel,
		}
		if customDef := strings.TrimSpace(os.Getenv("FREEBUFF_DEFAULT_MODEL")); customDef != "" {
			found := false
			for _, m := range catalog {
				if m == customDef {
					found = true
					break
				}
			}
			if !found {
				catalog = append(catalog, customDef)
			}
		}
		if allow := os.Getenv("FREEBUFF_TEST_CATALOG_ALLOWLIST"); allow != "" {
			var subset []string
			for _, part := range strings.Split(allow, ",") {
				p := strings.TrimSpace(part)
				if p == "" {
					continue
				}
				found := false
				for _, m := range catalog {
					if m == p {
						found = true
						subset = append(subset, m)
						break
					}
				}
				if !found {
					fmt.Fprintf(os.Stderr, "unknown model in allowlist: %s\n", p)
					os.Exit(1)
				}
			}
			hasDef := false
			defModel := defaultModel
			if configured := strings.TrimSpace(os.Getenv("FREEBUFF_DEFAULT_MODEL")); configured != "" {
				defModel = configured
			}
			for _, m := range subset {
				if m == defModel {
					hasDef = true
					break
				}
			}
			if !hasDef {
				fmt.Fprintf(os.Stderr, "allowlist excludes default model %s\n", defModel)
				os.Exit(1)
			}
			data, _ := json.Marshal(subset)
			fmt.Println(string(data))
			os.Exit(0)
		}
		data, _ := json.Marshal(catalog)
		fmt.Println(string(data))
		os.Exit(0)
	}
	if os.Getenv("FREEBUFF_TEST_JSONL_CHILD") == "1" {
		fmt.Println(`{"type":"ready"}`)
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(make([]byte, 4096), 8<<20)
		pendingRequestID := ""
		for scanner.Scan() {
			var request map[string]any
			if json.Unmarshal(scanner.Bytes(), &request) != nil {
				continue
			}
			reqType, _ := request["type"].(string)
			reqID, _ := request["id"].(string)
			if reqType == "reset" {
				res, _ := json.Marshal(map[string]any{"type": "reset_ok", "id": reqID})
				fmt.Println(string(res))
				continue
			}
			if reqType == "chat" {
				instanceID := "inst-test-001"
				if customInst := os.Getenv("FREEBUFF_TEST_CUSTOM_INSTANCE"); customInst != "" {
					instanceID = customInst
				}
				start, _ := json.Marshal(map[string]any{
					"type":        "start",
					"id":          reqID,
					"instance_id": instanceID,
					"model":       request["model"],
				})
				fmt.Println(string(start))
				if os.Getenv("FREEBUFF_TEST_TRIGGER_TOOL") == "1" {
					pendingRequestID = reqID
					toolCall, _ := json.Marshal(map[string]any{
						"type":         "tool_call",
						"id":           reqID,
						"tool_call_id": "call-test-fixture-1",
						"name":         "lookup",
						"arguments":    json.RawMessage(`{"q":"test"}`),
					})
					fmt.Println(string(toolCall))
					continue
				}
			}
			text := request["prompt"]
			if reqType == "tool_result" {
				reqID = pendingRequestID
				pendingRequestID = ""
				text = "tool executed: " + fmt.Sprint(request["content"])
			}
			response, _ := json.Marshal(map[string]any{"type": "result", "id": reqID, "text": text})
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
