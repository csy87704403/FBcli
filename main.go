package main

import (
	"bufio"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultModel = "deepseek/deepseek-v4-flash"
	mimoModel    = "mimo/mimo-v2.5"
)

var gatewayModels = []string{defaultModel, mimoModel}

func gatewayModelList() []map[string]any {
	models := make([]map[string]any, 0, len(gatewayModels))
	for _, model := range gatewayModels {
		models = append(models, map[string]any{"id": model, "object": "model", "owned_by": "freebuff-cli"})
	}
	return models
}

type chatMessage struct {
	Role       string           `json:"role"`
	Content    any              `json:"content"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
	ToolCalls  []openAIToolCall `json:"tool_calls,omitempty"`
	Name       string           `json:"name,omitempty"`
}

type openAIFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

type openAITool struct {
	Type     string         `json:"type"`
	Function openAIFunction `json:"function"`
}

type openAIToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
	Stream   bool          `json:"stream"`
	Tools    []openAITool  `json:"tools"`
	User     string        `json:"user"`
}

type cliEvent struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Text       string          `json:"text"`
	Message    string          `json:"message"`
	ToolCallID string          `json:"tool_call_id"`
	Name       string          `json:"name"`
	Arguments  json.RawMessage `json:"arguments"`
}

type cliChatResult struct {
	Text      string
	ToolCalls []openAIToolCall
}

type sessionSelection struct {
	ID        string
	Automatic bool
}

type sessionBinding struct {
	ID      string    `json:"id"`
	Updated time.Time `json:"updated_at"`
}

type conversationRouter struct {
	mu        sync.Mutex
	byHistory map[string]sessionBinding
	store     *stateStore
}

type pendingToolCall struct {
	RequestID  string
	SessionID  string
	ToolCallID string
	ToolName   string
}

type cliClient struct {
	path             string
	cwd              string
	configDir        string
	mu               sync.Mutex
	processMu        sync.RWMutex
	cmd              *exec.Cmd
	stdin            io.WriteCloser
	scanner          *bufio.Scanner
	pending          *pendingToolCall
	proxy            string
	processPID       int
	processStartedAt time.Time
}

func (c *cliClient) start() error {
	if c.cmd != nil && c.cmd.Process != nil {
		return nil
	}
	cmd := exec.Command(c.path)
	cmd.Dir = c.cwd
	cmd.Env = cliEnvironment(c.proxy, c.configDir)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	if !scanner.Scan() {
		_ = cmd.Process.Kill()
		return errors.New("headless CLI exited before ready")
	}
	var ready cliEvent
	if err := json.Unmarshal(scanner.Bytes(), &ready); err != nil || ready.Type != "ready" {
		_ = cmd.Process.Kill()
		return fmt.Errorf("invalid headless CLI ready event: %s", scanner.Text())
	}
	c.cmd, c.stdin, c.scanner = cmd, stdin, scanner
	c.processMu.Lock()
	c.processPID = cmd.Process.Pid
	c.processStartedAt = time.Now()
	c.processMu.Unlock()
	return nil
}

func cliEnvironment(proxy, configDir string) []string {
	keys := map[string]bool{"http_proxy": true, "https_proxy": true, "all_proxy": true, "freebuff_config_dir": true}
	env := make([]string, 0, len(os.Environ())+3)
	for _, item := range os.Environ() {
		name, _, _ := strings.Cut(item, "=")
		if !keys[strings.ToLower(name)] {
			env = append(env, item)
		}
	}
	if proxy != "" {
		env = append(env, "HTTP_PROXY="+proxy, "HTTPS_PROXY="+proxy, "ALL_PROXY="+proxy)
	}
	if configDir != "" {
		env = append(env, "FREEBUFF_CONFIG_DIR="+configDir)
	}
	return env
}

func (c *cliClient) setProxy(proxy string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.proxy == proxy {
		return
	}
	c.proxy = proxy
	c.resetProcess()
}

func (c *cliClient) running() bool {
	if !c.mu.TryLock() {
		return true
	}
	defer c.mu.Unlock()
	return c.cmd != nil && c.cmd.Process != nil
}

func (c *cliClient) processStatus() (bool, int, time.Time) {
	c.processMu.RLock()
	defer c.processMu.RUnlock()
	return c.processPID > 0, c.processPID, c.processStartedAt
}

func (c *cliClient) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetProcess()
}

func (c *cliClient) resetProcess() {
	if c.stdin != nil {
		_ = c.stdin.Close()
	}
	if c.cmd != nil && c.cmd.Process != nil {
		_ = c.cmd.Process.Kill()
		_, _, _ = c.cmd.ProcessState, c.cmd.Process, c.cmd.Wait()
	}
	c.cmd, c.stdin, c.scanner = nil, nil, nil
	c.processMu.Lock()
	c.processPID = 0
	c.processStartedAt = time.Time{}
	c.processMu.Unlock()
}

func externalTools(tools []openAITool) []map[string]any {
	result := make([]map[string]any, 0, len(tools))
	for _, tool := range tools {
		if tool.Type != "function" || strings.TrimSpace(tool.Function.Name) == "" {
			continue
		}
		parameters := any(map[string]any{"type": "object"})
		if len(tool.Function.Parameters) > 0 {
			parameters = tool.Function.Parameters
		}
		result = append(result, map[string]any{
			"name": tool.Function.Name, "description": tool.Function.Description,
			"parameters": parameters,
		})
	}
	return result
}

func newToolCall(event cliEvent) openAIToolCall {
	arguments := strings.TrimSpace(string(event.Arguments))
	if arguments == "" || arguments == "null" {
		arguments = "{}"
	}
	call := openAIToolCall{ID: event.ToolCallID, Type: "function"}
	call.Function.Name = event.Name
	call.Function.Arguments = arguments
	return call
}

func (c *cliClient) chat(ctx context.Context, model, sessionID, prompt string, content []map[string]any, tools []openAITool, toolResult *chatMessage, onDelta func(string)) (cliChatResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.start(); err != nil {
		return cliChatResult{}, err
	}
	id := ""
	var request map[string]any
	if toolResult != nil {
		if c.pending == nil {
			return cliChatResult{}, errors.New("no pending tool call for tool result")
		}
		if c.pending.SessionID != sessionID || c.pending.ToolCallID != toolResult.ToolCallID {
			return cliChatResult{}, errors.New("tool result does not match the pending session and tool_call_id")
		}
		id = c.pending.RequestID
		request = map[string]any{
			"id": fmt.Sprintf("tool-result-%d", time.Now().UnixNano()), "type": "tool_result",
			"tool_call_id": toolResult.ToolCallID, "content": contentValue(toolResult.Content),
		}
	} else {
		if c.pending != nil {
			return cliChatResult{}, errors.New("another external tool call is waiting for its result")
		}
		id = fmt.Sprintf("req-%d", time.Now().UnixNano())
		request = map[string]any{
			"id": id, "type": "chat", "model": model,
			"session_id": sessionID, "prompt": prompt, "cwd": c.cwd,
			"tools": externalTools(tools),
		}
		if len(content) > 0 {
			request["message_content"] = content
		}
	}
	data, _ := json.Marshal(request)
	if _, err := c.stdin.Write(append(data, '\n')); err != nil {
		c.resetProcess()
		c.pending = nil
		return cliChatResult{}, err
	}

	var streamed strings.Builder
	for c.scanner.Scan() {
		if err := ctx.Err(); err != nil {
			c.resetProcess()
			c.pending = nil
			return cliChatResult{}, err
		}
		var event cliEvent
		if err := json.Unmarshal(c.scanner.Bytes(), &event); err != nil {
			continue
		}
		if event.ID != id {
			continue
		}
		switch event.Type {
		case "delta":
			streamed.WriteString(event.Text)
			if onDelta != nil {
				onDelta(event.Text)
			}
		case "result":
			c.pending = nil
			if event.Text != "" {
				return cliChatResult{Text: event.Text}, nil
			}
			return cliChatResult{Text: streamed.String()}, nil
		case "tool_call":
			call := newToolCall(event)
			c.pending = &pendingToolCall{
				RequestID: id, SessionID: sessionID, ToolCallID: call.ID, ToolName: call.Function.Name,
			}
			return cliChatResult{ToolCalls: []openAIToolCall{call}}, nil
		case "error":
			c.pending = nil
			return cliChatResult{}, errors.New(event.Message)
		}
	}
	err := c.scanner.Err()
	c.resetProcess()
	c.pending = nil
	if err != nil {
		return cliChatResult{}, err
	}
	return cliChatResult{}, errors.New("headless CLI closed its output")
}

func contentValue(content any) any {
	if text, ok := content.(string); ok {
		var value any
		if json.Unmarshal([]byte(text), &value) == nil {
			return value
		}
	}
	return content
}

func contentText(content any) string {
	switch value := content.(type) {
	case string:
		return value
	case []any:
		var parts []string
		for _, item := range value {
			object, ok := item.(map[string]any)
			if !ok {
				continue
			}
			kind, _ := object["type"].(string)
			if kind != "text" && kind != "input_text" {
				continue
			}
			if text, _ := object["text"].(string); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	default:
		return ""
	}
}

func imageURL(item map[string]any) string {
	value := item["image_url"]
	if value == nil {
		value = item["url"]
	}
	if text, ok := value.(string); ok {
		return text
	}
	if object, ok := value.(map[string]any); ok {
		text, _ := object["url"].(string)
		return text
	}
	return ""
}

func multimodalContent(messages []chatMessage, prompt string) ([]map[string]any, error) {
	var userContent []any
	for index := len(messages) - 1; index >= 0; index-- {
		if messages[index].Role != "user" {
			continue
		}
		userContent, _ = messages[index].Content.([]any)
		break
	}
	if len(userContent) == 0 {
		return nil, nil
	}
	result := []map[string]any{{"type": "text", "text": prompt}}
	for _, rawItem := range userContent {
		item, ok := rawItem.(map[string]any)
		if !ok {
			continue
		}
		kind, _ := item["type"].(string)
		if kind != "image_url" && kind != "input_image" && kind != "image" {
			continue
		}
		dataURL := imageURL(item)
		metadata, encoded, found := strings.Cut(strings.TrimPrefix(dataURL, "data:"), ",")
		if !found || !strings.HasSuffix(strings.ToLower(metadata), ";base64") {
			return nil, errors.New("only base64 data: image URLs are supported")
		}
		mediaType := strings.TrimSuffix(metadata, ";base64")
		if !strings.HasPrefix(strings.ToLower(mediaType), "image/") {
			return nil, errors.New("multimodal content must use an image media type")
		}
		if _, err := base64.StdEncoding.DecodeString(encoded); err != nil {
			return nil, errors.New("invalid base64 image data")
		}
		result = append(result, map[string]any{"type": "image", "image": encoded, "mediaType": mediaType})
	}
	if len(result) == 1 {
		return nil, nil
	}
	return result, nil
}

func promptFromMessages(messages []chatMessage) string {
	var instructions []string
	lastUser := ""
	for _, message := range messages {
		text := strings.TrimSpace(contentText(message.Content))
		if text == "" {
			continue
		}
		switch message.Role {
		case "system", "developer":
			instructions = append(instructions, text)
		case "user":
			lastUser = text
		}
	}
	if lastUser == "" {
		return ""
	}
	if len(instructions) == 0 {
		return lastUser
	}
	return "System instructions:\n" + strings.Join(instructions, "\n\n") + "\n\nUser message:\n" + lastUser
}

func lastToolResult(messages []chatMessage) *chatMessage {
	if len(messages) == 0 {
		return nil
	}
	last := messages[len(messages)-1]
	if last.Role != "tool" || strings.TrimSpace(last.ToolCallID) == "" {
		return nil
	}
	return &last
}

func addToolContract(prompt string, tools []openAITool) string {
	if len(tools) == 0 {
		return prompt
	}
	return "External tools are executed by the API caller. Use only the supplied tool names, do not execute equivalent commands or files locally, and wait for each tool result before continuing.\n\n" + prompt
}

func sessionSeedText(content any) string {
	text := strings.TrimSpace(contentText(content))
	for _, marker := range []string{"\n\n[Image attached at:", "\n[Image attached at:"} {
		if index := strings.Index(text, marker); index >= 0 {
			return strings.TrimSpace(text[:index])
		}
	}
	return text
}

func explicitSessionID(request chatRequest, header string) string {
	if value := strings.TrimSpace(header); value != "" {
		return value
	}
	if value := strings.TrimSpace(request.User); value != "" {
		return "user-" + value
	}
	return ""
}

func newAutoSessionID() string {
	var random [8]byte
	if _, err := cryptorand.Read(random[:]); err == nil {
		return "auto-" + hex.EncodeToString(random[:])
	}
	return fmt.Sprintf("auto-%d", time.Now().UnixNano())
}

func firstUserText(messages []chatMessage) string {
	for _, message := range messages {
		if message.Role == "user" {
			return sessionSeedText(message.Content)
		}
	}
	return ""
}

func assistantFingerprint(message chatMessage) string {
	if text := strings.TrimSpace(contentText(message.Content)); text != "" {
		return "text\x00" + text
	}
	if len(message.ToolCalls) > 0 {
		encoded, _ := json.Marshal(message.ToolCalls)
		return "tools\x00" + string(encoded)
	}
	return ""
}

func historyKey(request chatRequest) (string, bool) {
	firstUser := firstUserText(request.Messages)
	if firstUser == "" {
		return "", false
	}
	foundUser := false
	firstAssistant := ""
	for _, message := range request.Messages {
		if message.Role == "user" {
			foundUser = true
			continue
		}
		if foundUser && message.Role == "assistant" {
			firstAssistant = assistantFingerprint(message)
			if firstAssistant != "" {
				break
			}
		}
	}
	if firstAssistant == "" {
		return "", false
	}
	model := strings.TrimSpace(request.Model)
	if model == "" {
		model = defaultModel
	}
	var seed strings.Builder
	seed.WriteString(model)
	seed.WriteString("\x00user\x00" + firstUser)
	seed.WriteString("\x00assistant\x00" + firstAssistant)
	sum := sha256.Sum256([]byte(seed.String()))
	return hex.EncodeToString(sum[:16]), true
}

func historyKeyForResult(request chatRequest, result cliChatResult) (string, bool) {
	if _, found := historyKey(request); found {
		return "", false
	}
	assistant := chatMessage{Role: "assistant", Content: result.Text, ToolCalls: result.ToolCalls}
	request.Messages = append(append([]chatMessage(nil), request.Messages...), assistant)
	return historyKey(request)
}

const sessionBindingTTL = 24 * time.Hour

func newConversationRouter(stores ...*stateStore) *conversationRouter {
	router := &conversationRouter{byHistory: make(map[string]sessionBinding)}
	if len(stores) == 0 || stores[0] == nil {
		return router
	}
	router.store = stores[0]
	cutoff := time.Now().Add(-sessionBindingTTL)
	router.store.mu.RLock()
	for key, binding := range router.store.state.ConversationSessions {
		if binding.Updated.After(cutoff) {
			router.byHistory[key] = binding
		}
	}
	router.store.mu.RUnlock()
	go router.reapExpired()
	return router
}

func (router *conversationRouter) persist(key string, binding sessionBinding, save bool) {
	if router.store == nil {
		return
	}
	router.store.mu.Lock()
	if router.store.state.ConversationSessions == nil {
		router.store.state.ConversationSessions = make(map[string]sessionBinding)
	}
	router.store.state.ConversationSessions[key] = binding
	var err error
	if save {
		err = router.store.saveLocked()
	}
	router.store.mu.Unlock()
	if err != nil {
		log.Printf("persist conversation binding: %v", err)
	}

}

func (router *conversationRouter) reapExpired() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-sessionBindingTTL)
		changed := false
		router.mu.Lock()
		for key, binding := range router.byHistory {
			if binding.Updated.Before(cutoff) {
				delete(router.byHistory, key)
				changed = true
			}
		}
		router.mu.Unlock()
		if !changed || router.store == nil {
			continue
		}
		router.store.mu.Lock()
		for key, binding := range router.store.state.ConversationSessions {
			if binding.Updated.Before(cutoff) {
				delete(router.store.state.ConversationSessions, key)
			}
		}
		err := router.store.saveLocked()
		router.store.mu.Unlock()
		if err != nil {
			log.Printf("prune conversation bindings: %v", err)
		}
	}
}

func (router *conversationRouter) resolve(request chatRequest, header string) sessionSelection {
	if explicit := explicitSessionID(request, header); explicit != "" {
		return sessionSelection{ID: explicit}
	}
	key, found := historyKey(request)
	if !found {
		return sessionSelection{ID: newAutoSessionID(), Automatic: true}
	}
	router.mu.Lock()
	defer router.mu.Unlock()
	if binding, ok := router.byHistory[key]; ok && binding.ID != "" {
		binding.Updated = time.Now()
		router.byHistory[key] = binding
		router.persist(key, binding, false)
		return sessionSelection{ID: binding.ID, Automatic: true}
	}
	return sessionSelection{ID: newAutoSessionID(), Automatic: true}
}

func (router *conversationRouter) bind(request chatRequest, result cliChatResult, selection sessionSelection) {
	if !selection.Automatic {
		return
	}
	key, found := historyKey(request)
	if !found {
		key, found = historyKeyForResult(request, result)
	}
	if !found {
		return
	}
	router.mu.Lock()
	if existing, ok := router.byHistory[key]; ok && existing.ID != selection.ID {
		binding := sessionBinding{Updated: time.Now()}
		router.byHistory[key] = binding
		router.mu.Unlock()
		router.persist(key, binding, true)
		return
	}
	binding := sessionBinding{ID: selection.ID, Updated: time.Now()}
	router.byHistory[key] = binding
	if len(router.byHistory) <= 4096 {
		router.mu.Unlock()
		router.persist(key, binding, true)
		return
	}
	var oldestKey string
	var oldest time.Time
	for candidate, binding := range router.byHistory {
		if oldestKey == "" || binding.Updated.Before(oldest) {
			oldestKey, oldest = candidate, binding.Updated
		}
	}
	delete(router.byHistory, oldestKey)
	router.mu.Unlock()
	if router.store != nil {
		router.store.mu.Lock()
		delete(router.store.state.ConversationSessions, oldestKey)
		router.store.mu.Unlock()
	}
	router.persist(key, binding, true)
}

func (router *conversationRouter) count() int {
	router.mu.Lock()
	defer router.mu.Unlock()
	return len(router.byHistory)
}

type server struct {
	accounts *accountManager
	sessions *conversationRouter
	admin    *adminService
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message, code string) {
	writeJSON(w, status, map[string]any{"error": map[string]any{"message": message, "type": code}})
}

func (s *server) chatCompletions(w http.ResponseWriter, r *http.Request) {
	startedAt := time.Now()
	var request chatRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, 8<<20))
	decoder.UseNumber()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request", "invalid_request")
		return
	}
	toolResult := lastToolResult(request.Messages)
	prompt := addToolContract(promptFromMessages(request.Messages), request.Tools)
	if prompt == "" && toolResult == nil {
		writeError(w, http.StatusBadRequest, "a user message is required", "invalid_request")
		return
	}
	content, err := multimodalContent(request.Messages, prompt)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid_image")
		return
	}
	model := strings.TrimSpace(request.Model)
	if model == "" {
		model = defaultModel
	}
	id := fmt.Sprintf("chatcmpl-cli-%d", time.Now().UnixNano())
	created := time.Now().Unix()
	selection := s.sessions.resolve(request, r.Header.Get("X-Freebuff-Session-ID"))
	client, accountID, err := s.accounts.acquire(selection.ID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error(), "account_unavailable")
		return
	}
	inputChars := len(prompt)

	if request.Stream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher, _ := w.(http.Flusher)
		started := false
		outputChars := 0
		onDelta := func(delta string) {
			outputChars += len([]rune(delta))
			payload := map[string]any{
				"id": id, "object": "chat.completion.chunk", "created": created, "model": model,
				"choices": []map[string]any{{"index": 0, "delta": map[string]any{"content": delta}, "finish_reason": nil}},
			}
			if !started {
				payload["choices"] = []map[string]any{{"index": 0, "delta": map[string]any{"role": "assistant", "content": delta}, "finish_reason": nil}}
				started = true
			}
			data, _ := json.Marshal(payload)
			fmt.Fprintf(w, "data: %s\n\n", data)
			if flusher != nil {
				flusher.Flush()
			}
		}
		result, err := client.chat(r.Context(), model, selection.ID, prompt, content, request.Tools, toolResult, onDelta)
		s.accounts.finish(accountID, err)
		if err != nil {
			s.admin.recordUsage(model, apiKeyFromRequest(r), inputChars, outputChars, false, time.Since(startedAt), err)
			data, _ := json.Marshal(map[string]any{"error": map[string]any{"message": err.Error(), "type": "cli_request_failed"}})
			fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", data)
			return
		}
		s.admin.recordUsage(model, apiKeyFromRequest(r), inputChars, outputChars, true, time.Since(startedAt), nil)
		s.sessions.bind(request, result, selection)
		finishReason := "stop"
		if len(result.ToolCalls) > 0 {
			finishReason = "tool_calls"
			toolChunk, _ := json.Marshal(map[string]any{
				"id": id, "object": "chat.completion.chunk", "created": created, "model": model,
				"choices": []map[string]any{{"index": 0, "delta": map[string]any{
					"role": "assistant", "tool_calls": streamToolCalls(result.ToolCalls),
				}, "finish_reason": nil}},
			})
			fmt.Fprintf(w, "data: %s\n\n", toolChunk)
		}
		finish, _ := json.Marshal(map[string]any{
			"id": id, "object": "chat.completion.chunk", "created": created, "model": model,
			"choices": []map[string]any{{"index": 0, "delta": map[string]any{}, "finish_reason": finishReason}},
		})
		fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", finish)
		return
	}

	result, err := client.chat(r.Context(), model, selection.ID, prompt, content, request.Tools, toolResult, nil)
	s.accounts.finish(accountID, err)
	if err != nil {
		s.admin.recordUsage(model, apiKeyFromRequest(r), inputChars, 0, false, time.Since(startedAt), err)
		writeError(w, http.StatusBadGateway, err.Error(), "cli_request_failed")
		return
	}
	s.admin.recordUsage(model, apiKeyFromRequest(r), inputChars, len([]rune(result.Text)), true, time.Since(startedAt), nil)
	s.sessions.bind(request, result, selection)
	message := map[string]any{"role": "assistant", "content": result.Text}
	finishReason := "stop"
	if len(result.ToolCalls) > 0 {
		message["content"] = nil
		message["tool_calls"] = result.ToolCalls
		finishReason = "tool_calls"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": id, "object": "chat.completion", "created": created, "model": model,
		"choices": []map[string]any{{"index": 0, "message": message, "finish_reason": finishReason}},
	})
}

func streamToolCalls(calls []openAIToolCall) []map[string]any {
	result := make([]map[string]any, 0, len(calls))
	for index, call := range calls {
		result = append(result, map[string]any{
			"index": index, "id": call.ID, "type": "function", "function": call.Function,
		})
	}
	return result
}

func main() {
	listen := flag.String("listen", "127.0.0.1:16882", "listen address")
	adminListen := flag.String("admin-listen", "127.0.0.1:16883", "local management UI listen address")
	cliPath := flag.String("cli", os.Getenv("FREEBUFF_HEADLESS_BIN"), "headless Freebuff CLI executable")
	cwd := flag.String("cwd", ".", "isolated CLI working directory")
	statePath := flag.String("state", "", "gateway state file (default: beside executable)")
	loginCLIPath := flag.String("login-cli", "", "patched official Freebuff CLI used for isolated account login")
	accountsRoot := flag.String("accounts-root", "", "isolated account directories")
	defaultAccountConfig := flag.String("default-account-config", "", "existing official CLI config directory to import once")
	legacyConfig := flag.String("legacy-config", "", "optional old gateway config to import once")
	legacyPool := flag.String("legacy-pool", "", "optional old proxy pool to import once")
	mihomoConfig := flag.String("mihomo-config", os.Getenv("FREEBUFF_MIHOMO_CONFIG"), "Mihomo config used to map listener ports to node names")
	flag.Parse()
	if strings.TrimSpace(*cliPath) == "" {
		log.Fatal("set -cli or FREEBUFF_HEADLESS_BIN")
	}
	adminUser := strings.TrimSpace(os.Getenv("FREEBUFF_ADMIN_USER"))
	adminPassword := os.Getenv("FREEBUFF_ADMIN_PASSWORD")
	if adminUser == "" || adminPassword == "" {
		log.Fatal("set FREEBUFF_ADMIN_USER and FREEBUFF_ADMIN_PASSWORD")
	}
	if *statePath == "" {
		executable, _ := os.Executable()
		*statePath = filepath.Join(filepath.Dir(executable), "freebuff-cli-gateway-state.json")
	}
	if *accountsRoot == "" {
		*accountsRoot = filepath.Join(filepath.Dir(*statePath), "accounts")
	}
	if *defaultAccountConfig == "" {
		home, _ := os.UserHomeDir()
		*defaultAccountConfig = filepath.Join(home, ".config", "manicode")
	}
	store, err := newStateStore(*statePath, *legacyConfig, *legacyPool)
	if err != nil {
		log.Fatalf("load gateway state: %v", err)
	}
	if err := ensureDefaultAccount(store, *defaultAccountConfig); err != nil {
		log.Fatalf("import default CLI account: %v", err)
	}
	accounts := newAccountManager(store, *cliPath, *loginCLIPath, *cwd, *accountsRoot)
	service := &server{accounts: accounts, sessions: newConversationRouter(store)}
	service.admin = newAdminService(store, service, adminUser, adminPassword, *mihomoConfig)
	monitorContext, stopMonitor := context.WithCancel(context.Background())
	defer stopMonitor()
	service.admin.startEgressTierMonitor(monitorContext)
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "backend": "freebuff-cli-jsonl"})
	})
	apiMux.HandleFunc("GET /v1/models", service.admin.requireAPIKey(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": gatewayModelList()})
	}))
	apiMux.HandleFunc("POST /v1/chat/completions", service.admin.requireAPIKey(service.chatCompletions))
	adminMux := http.NewServeMux()
	service.admin.register(adminMux)
	apiServer := &http.Server{Addr: *listen, Handler: apiMux, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute, MaxHeaderBytes: 1 << 20}
	adminServer := &http.Server{Addr: *adminListen, Handler: adminMux, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute, MaxHeaderBytes: 1 << 20}
	log.Printf("Freebuff model API listening on http://%s", *listen)
	log.Printf("Freebuff management UI listening on http://%s", *adminListen)
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	serverErrors := make(chan error, 2)
	go func() { serverErrors <- apiServer.ListenAndServe() }()
	go func() { serverErrors <- adminServer.ListenAndServe() }()
	select {
	case <-shutdown:
		stopMonitor()
		accounts.stopAll()
		_ = apiServer.Close()
		_ = adminServer.Close()
	case err := <-serverErrors:
		stopMonitor()
		accounts.stopAll()
		_ = apiServer.Close()
		_ = adminServer.Close()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}
}
