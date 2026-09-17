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
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

const (
	defaultModel     = "deepseek/deepseek-v4-flash"
	deepSeekProModel = "deepseek/deepseek-v4-pro"
	gptLunaModel     = "openai/gpt-5.6-luna"
	miniMaxModel     = "minimax/minimax-m3"
	mimoModel        = "mimo/mimo-v2.5"

	// The CLI may wait while an external agent executes a tool. Keep the default
	// generous, while allowing deployments to tune it without rebuilding.
	staleToolResultTimeout = 5 * time.Minute
	maxExternalToolCalls   = 96
	maxRepeatedToolCalls   = 12
	maxGatewayRequestTime  = 12 * time.Minute
	headlessStartTimeout   = 30 * time.Second
	headlessResetTimeout   = 10 * time.Second
	defaultContextLimit    = 131072
	defaultContextReserve  = 8192
)

// gatewayModels mirrors the public Freebuff CLI picker. It is a catalog, not a
// promise that every model is currently joinable: Freebuff may temporarily
// close a model, exhaust an account's entitlement, or restrict an account to
// the limited tier. Keep the catalog stable so a temporary model_unavailable
// response (notably V4 Flash) never makes clients forget that model exists.
// The official session admission remains the authority for each actual chat.
var gatewayModels = []string{
	deepSeekProModel,
	defaultModel,
	gptLunaModel,
	miniMaxModel,
	mimoModel,
}

func gatewayModelList() []map[string]any {
	models := make([]map[string]any, 0, len(gatewayModels))
	for _, model := range gatewayModels {
		models = append(models, map[string]any{
			"id": model, "object": "model", "owned_by": "freebuff-cli",
			"x_freebuff_admission": "official",
			// These are gateway safeguards, not claims about the upstream model's
			// native context window or output limit.
			"context_length":             contextLimit(),
			"x_freebuff_context_reserve": contextReserve(),
			"capabilities":               []string{"chat", "tools", "streaming"},
		})
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
	Model     string        `json:"model"`
	Messages  []chatMessage `json:"messages"`
	Stream    bool          `json:"stream"`
	Tools     []openAITool  `json:"tools"`
	User      string        `json:"user"`
	MaxTokens int           `json:"max_tokens,omitempty"`
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

type cliScanResult struct {
	line []byte
	err  error
}

type cliChatResult struct {
	Text      string
	ToolCalls []openAIToolCall
}

type sessionSelection struct {
	ID          string
	Automatic   bool
	FallbackKey string
}

type sessionBinding struct {
	ID      string    `json:"id"`
	Updated time.Time `json:"updated_at"`
}

type conversationRouter struct {
	mu         sync.Mutex
	byHistory  map[string]sessionBinding
	byToolCall map[string]sessionBinding
	byFallback map[string]sessionBinding
	external   map[string]sessionObservation
	store      *stateStore
}

type sessionObservation struct {
	MessageCount    int
	EstimatedTokens int
	Created         time.Time
	Updated         time.Time
}

type pendingToolCall struct {
	RequestID  string
	SessionID  string
	ToolCallID string
	ToolName   string
	CreatedAt  time.Time
	ExpiredAt  time.Time
}

type cliClient struct {
	histories           map[string][][32]byte
	path                string
	cwd                 string
	configDir           string
	mu                  sync.Mutex
	processMu           sync.RWMutex
	cmd                 *exec.Cmd
	stdin               io.WriteCloser
	scanner             *bufio.Scanner
	events              chan cliScanResult
	scanStop            chan struct{}
	pending             *pendingToolCall
	expiredTools        map[string]pendingToolCall
	sessions            map[string]bool
	toolCalls           int
	lastToolCall        string
	repeatedToolCall    int
	proxy               string
	proxyRestartPending bool
	processPID          int
	processStartedAt    time.Time
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
	readyResult := make(chan error, 1)
	go func() {
		if !scanner.Scan() {
			readyResult <- errors.New("headless CLI exited before ready")
			return
		}
		var ready cliEvent
		if err := json.Unmarshal(scanner.Bytes(), &ready); err != nil || ready.Type != "ready" {
			readyResult <- fmt.Errorf("invalid headless CLI ready event: %s", scanner.Text())
			return
		}
		readyResult <- nil
	}()
	select {
	case err := <-readyResult:
		if err != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			return err
		}
	case <-time.After(headlessStartTimeout):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return errors.New("headless CLI did not become ready before timeout")
	}
	c.cmd, c.stdin, c.scanner = cmd, stdin, scanner
	c.events = make(chan cliScanResult, 256)
	c.scanStop = make(chan struct{})
	events, stop := c.events, c.scanStop
	go func() {
		defer close(events)
		for scanner.Scan() {
			line := append([]byte(nil), scanner.Bytes()...)
			select {
			case events <- cliScanResult{line: line}:
			case <-stop:
				return
			}
		}
		select {
		case events <- cliScanResult{err: scanner.Err()}:
		case <-stop:
		}
	}()
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
	if c.pending != nil {
		c.proxyRestartPending = true
		return
	}
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

// resetExternalSession removes only the requested run from the headless CLI.
// Keeping the account process alive is safe when other sessions use it.
func (c *cliClient) resetExternalSession(externalSessionID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cmd == nil || c.cmd.Process == nil || c.stdin == nil || c.events == nil {
		return nil
	}
	timeout := time.NewTimer(headlessResetTimeout)
	defer timeout.Stop()
	for _, model := range gatewayModels {
		delete(c.sessions, scopedSessionID(model, externalSessionID))
		id := fmt.Sprintf("reset-%d", time.Now().UnixNano())
		request := map[string]any{"id": id, "type": "reset", "session_id": scopedSessionID(model, externalSessionID)}
		data, _ := json.Marshal(request)
		if _, err := c.stdin.Write(append(data, '\n')); err != nil {
			return err
		}
		for {
			var scanned cliScanResult
			var ok bool
			select {
			case <-timeout.C:
				c.resetProcess()
				return errors.New("headless CLI reset timed out")
			case scanned, ok = <-c.events:
				if !ok {
					return errors.New("headless CLI closed its output while resetting session")
				}
			}
			if scanned.err != nil {
				return scanned.err
			}
			var event cliEvent
			if err := json.Unmarshal(scanned.line, &event); err != nil || event.ID != id {
				continue
			}
			if event.Type == "reset_ok" {
				break
			}
			if event.Type == "error" {
				return errors.New(event.Message)
			}
		}
	}
	return nil
}

func (c *cliClient) clearToolState() {
	c.pending = nil
	c.toolCalls = 0
	c.lastToolCall = ""
	c.repeatedToolCall = 0
}

func (c *cliClient) clearPendingTool() {
	c.pending = nil
}

func toolResultTimeout() time.Duration {
	if raw := strings.TrimSpace(os.Getenv("FREEBUFF_TOOL_TIMEOUT")); raw != "" {
		if value, err := time.ParseDuration(raw); err == nil && value >= time.Second {
			return value
		}
		log.Printf("[warn] invalid FREEBUFF_TOOL_TIMEOUT=%q; using %s", raw, staleToolResultTimeout)
	}
	return staleToolResultTimeout
}

func toolCallLimit(name string, fallback int) int {
	if raw := strings.TrimSpace(os.Getenv(name)); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil && value > 0 {
			return value
		}
		log.Printf("[warn] invalid %s=%q; using %d", name, raw, fallback)
	}
	return fallback
}

func (c *cliClient) resetProcess() {
	if c.scanStop != nil {
		close(c.scanStop)
		c.scanStop = nil
	}
	if c.stdin != nil {
		_ = c.stdin.Close()
	}
	if c.cmd != nil && c.cmd.Process != nil {
		_ = c.cmd.Process.Kill()
		_, _, _ = c.cmd.ProcessState, c.cmd.Process, c.cmd.Wait()
	}
	c.cmd, c.stdin, c.scanner, c.events = nil, nil, nil, nil
	c.sessions = nil
	c.histories = nil
	c.clearToolState()
	c.processMu.Lock()
	c.processPID = 0
	c.processStartedAt = time.Time{}
	c.processMu.Unlock()
}

func (c *cliClient) pendingSession() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending == nil {
		return ""
	}
	return c.pending.SessionID
}

func (c *cliClient) recoverExpiredToolCall(now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, call := range c.expiredTools {
		if now.Sub(call.ExpiredAt) > 30*time.Minute {
			delete(c.expiredTools, id)
		}
	}
	if c.pending == nil || now.Sub(c.pending.CreatedAt) < toolResultTimeout() {
		return false
	}
	c.pending.ExpiredAt = now
	log.Printf("[warn] tool call expired: session=%s tool_call_id=%s age=%s", c.pending.SessionID, c.pending.ToolCallID, now.Sub(c.pending.CreatedAt).Round(time.Second))
	if c.expiredTools == nil {
		c.expiredTools = make(map[string]pendingToolCall)
	}
	if len(c.expiredTools) >= maxToolCallBindings {
		var oldest string
		for id, call := range c.expiredTools {
			if oldest == "" || call.ExpiredAt.Before(c.expiredTools[oldest].ExpiredAt) {
				oldest = id
			}
		}
		delete(c.expiredTools, oldest)
	}
	c.expiredTools[c.pending.ToolCallID] = *c.pending
	c.resetProcess()
	return true
}

func (c *cliClient) canResume(sessionID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessions[sessionID] && (c.pending == nil || c.pending.SessionID == sessionID)
}

func (c *cliClient) setPendingToolCall(requestID, sessionID string, call openAIToolCall) error {
	fingerprint := call.Function.Name + "\n" + call.Function.Arguments
	c.toolCalls++
	if fingerprint == c.lastToolCall {
		c.repeatedToolCall++
	} else {
		c.lastToolCall = fingerprint
		c.repeatedToolCall = 1
	}
	if c.toolCalls > toolCallLimit("FREEBUFF_MAX_TOOL_CALLS", maxExternalToolCalls) || c.repeatedToolCall > toolCallLimit("FREEBUFF_MAX_REPEATED_TOOL_CALLS", maxRepeatedToolCalls) {
		c.resetProcess()
		return errors.New("external tool-call loop detected; the CLI session was reset")
	}
	c.pending = &pendingToolCall{
		RequestID: requestID, SessionID: sessionID, ToolCallID: call.ID,
		ToolName: call.Function.Name, CreatedAt: time.Now(),
	}
	return nil
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

func (c *cliClient) chat(ctx context.Context, model, sessionID, prompt string, content []map[string]any, tools []openAITool, toolResult *chatMessage, onDelta func(string), histories ...[]chatMessage) (cliChatResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var incoming [][32]byte
	if len(histories) > 0 {
		incoming = messageHashes(histories[0])
	}
	if toolResult == nil && c.sessions[sessionID] && !historyExtends(c.histories[sessionID], incoming) {
		// Client history is authoritative: edits, compaction and /new invalidate
		// native history even when the message count has not changed.
		c.resetProcess()
	}
	id := ""
	var request map[string]any
	if toolResult != nil {
		if call, found := c.expiredTools[toolResult.ToolCallID]; found {
			return cliChatResult{}, fmt.Errorf("tool call %s expired after %s, retry the full conversation turn", call.ToolCallID, call.ExpiredAt.Sub(call.CreatedAt).Round(time.Second))
		}
		if c.pending == nil {
			return cliChatResult{}, errors.New("no pending tool call for tool result")
		}
		if !c.pending.ExpiredAt.IsZero() {
			age := time.Since(c.pending.CreatedAt).Round(time.Second)
			toolCallID := c.pending.ToolCallID
			log.Printf("[warn] rejecting expired tool result: session=%s tool_call_id=%s age=%s", sessionID, toolCallID, age)
			// The headless process is still blocked on the old request. Once the
			// caller is told to restart the turn, discard that process as well so
			// its stale request cannot poison the next conversation.
			c.resetProcess()
			return cliChatResult{}, fmt.Errorf("tool call %s expired after %s, retry the full conversation turn", toolCallID, age)
		}
		if c.pending.SessionID != sessionID || c.pending.ToolCallID != toolResult.ToolCallID {
			if c.pending.SessionID == sessionID {
				c.resetProcess()
			}
			return cliChatResult{}, errors.New("tool result does not match the pending session and tool_call_id")
		}
		id = c.pending.RequestID
		request = map[string]any{
			"id": fmt.Sprintf("tool-result-%d", time.Now().UnixNano()), "type": "tool_result",
			"tool_call_id": toolResult.ToolCallID, "content": contentValue(toolResult.Content),
		}
	} else {
		if c.pending != nil {
			if c.pending.SessionID != sessionID {
				// The account scheduler must never route a second session into a
				// pending tool chain. If it still happens, the child state is no
				// longer trustworthy; reset it so this account cannot poison every
				// later request until an operator restarts the gateway.
				return cliChatResult{}, errors.New("conversation session conflict: another session is waiting for a tool result")
			}
			if !c.pending.ExpiredAt.IsZero() {
				log.Printf("[warn] rebuilding expired tool session: session=%s tool_call_id=%s", sessionID, c.pending.ToolCallID)
				c.resetProcess()
			}
			if c.pending != nil {
				// The same Agent resumed without returning the requested tool result.
				// Its local run cannot safely continue, so start a clean process.
				c.resetProcess()
			}
		}
		id = fmt.Sprintf("req-%d", time.Now().UnixNano())
		if !c.sessions[sessionID] && len(histories) > 0 && len(histories[0]) > 1 {
			prompt = fullPromptFromMessages(histories[0], tools)
			var err error
			content, err = multimodalContent(histories[0], prompt)
			if err != nil {
				return cliChatResult{}, err
			}
		}
		request = map[string]any{
			"id": id, "type": "chat", "model": model,
			"session_id": sessionID, "prompt": prompt, "cwd": c.cwd,
			"tools": externalTools(tools),
		}
		if len(content) > 0 {
			request["message_content"] = content
		}
	}
	if err := c.start(); err != nil {
		return cliChatResult{}, err
	}
	data, _ := json.Marshal(request)
	if _, err := c.stdin.Write(append(data, '\n')); err != nil {
		c.resetProcess()
		c.pending = nil
		return cliChatResult{}, err
	}

	var streamed strings.Builder
	for {
		select {
		case <-ctx.Done():
			c.resetProcess()
			return cliChatResult{}, ctx.Err()
		case scanned, ok := <-c.events:
			if !ok {
				c.resetProcess()
				return cliChatResult{}, errors.New("headless CLI closed its output")
			}
			if scanned.err != nil {
				c.resetProcess()
				return cliChatResult{}, scanned.err
			}
			var event cliEvent
			if err := json.Unmarshal(scanned.line, &event); err != nil {
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
				c.rememberHistory(sessionID, incoming)
				if c.sessions == nil {
					c.sessions = make(map[string]bool)
				}
				c.sessions[sessionID] = true
				if toolResult != nil {
					c.clearPendingTool()
				} else {
					c.clearToolState()
				}
				log.Printf("[info] cli result: session=%s request_id=%s tool_pending=%t", sessionID, id, c.pending != nil)
				if c.proxyRestartPending {
					c.proxyRestartPending = false
					c.resetProcess()
				}
				if event.Text != "" {
					return cliChatResult{Text: event.Text}, nil
				}
				return cliChatResult{Text: streamed.String()}, nil
			case "tool_call":
				c.rememberHistory(sessionID, incoming)
				call := newToolCall(event)
				if err := c.setPendingToolCall(id, sessionID, call); err != nil {
					return cliChatResult{}, err
				}
				if c.sessions == nil {
					c.sessions = make(map[string]bool)
				}
				c.sessions[sessionID] = true
				return cliChatResult{ToolCalls: []openAIToolCall{call}}, nil
			case "error":
				c.clearToolState()
				c.resetProcess()
				return cliChatResult{}, errors.New(event.Message)
			}
		}
	}
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

func contextLimit() int {
	if raw := strings.TrimSpace(os.Getenv("FREEBUFF_CONTEXT_LIMIT")); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil && value > 0 {
			return value
		}
		log.Printf("[warn] invalid FREEBUFF_CONTEXT_LIMIT=%q; using %d", raw, defaultContextLimit)
	}
	return defaultContextLimit
}

func contextReserve() int {
	if raw := strings.TrimSpace(os.Getenv("FREEBUFF_CONTEXT_RESERVE")); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil && value >= 0 {
			return value
		}
		log.Printf("[warn] invalid FREEBUFF_CONTEXT_RESERVE=%q; using %d", raw, defaultContextReserve)
	}
	return defaultContextReserve
}

// estimateContextTokens intentionally errs high enough to protect the runtime
// when callers omit tokenizer metadata. JSON size captures tool schemas and
// tool results, which plain text-only estimates would miss.
func estimateContextTokens(request chatRequest) int {
	data, _ := json.Marshal(struct {
		Messages []chatMessage `json:"messages"`
		Tools    []openAITool  `json:"tools,omitempty"`
	}{request.Messages, request.Tools})
	if len(data) == 0 {
		return 0
	}
	byByte := (len(data) + 3) / 4
	byRune := utf8.RuneCount(data)
	if byRune > byByte {
		return byRune
	}
	return byByte
}

func contextInputLimit(request chatRequest) int {
	reserve := contextReserve()
	if request.MaxTokens > 0 {
		reserve = request.MaxTokens
	}
	limit := contextLimit() - reserve
	if limit < 1 {
		return 0
	}
	return limit
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

// Rebuild context when failover selects a fresh CLI session. Normal turns use
// the native session history and continue sending only the current prompt.
func fullPromptFromMessages(messages []chatMessage, tools []openAITool) string {
	var transcript []string
	for _, message := range messages {
		role := strings.TrimSpace(message.Role)
		if role == "" {
			continue
		}
		if text := strings.TrimSpace(contentText(message.Content)); text != "" {
			transcript = append(transcript, strings.ToUpper(role)+":\n"+text)
		}
		if len(message.ToolCalls) > 0 {
			encoded, _ := json.Marshal(message.ToolCalls)
			transcript = append(transcript, "ASSISTANT_TOOL_CALLS:\n"+string(encoded))
		}
		if role == "tool" && message.ToolCallID != "" {
			transcript = append(transcript, "TOOL_CALL_ID: "+message.ToolCallID)
		}
	}
	if len(transcript) == 0 {
		return ""
	}
	return addToolContract("Conversation history from the API caller. Continue the same conversation and preserve this context.\n\n"+strings.Join(transcript, "\n\n"), tools)
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
	// OpenAI's `user` is an end-user label, not a conversation identifier.
	// Hermes commonly sends a stable user value across unrelated tasks; using it
	// as a session key would merge those tasks and repeatedly disturb the
	// official CLI admission/session state. Callers that have a real stable
	// conversation id should use X-Freebuff-Session-ID.
	return ""
}

func newAutoSessionID() string {
	var random [8]byte
	if _, err := cryptorand.Read(random[:]); err == nil {
		return "auto-" + hex.EncodeToString(random[:])
	}
	return fmt.Sprintf("auto-%d", time.Now().UnixNano())
}

func scopedSessionID(model, raw string) string {
	model = strings.TrimSpace(model)
	if model == "" {
		model = defaultModel
	}
	sum := sha256.Sum256([]byte(model + "\x00" + raw))
	return "session-" + hex.EncodeToString(sum[:16])
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

const (
	// A remote agent may keep an active conversation for a while, but retaining
	// bindings for a whole day creates stale CLI context and artificial load.
	sessionBindingTTL       = 30 * time.Minute
	shortSessionFallbackTTL = 2 * time.Minute
	maxToolCallBindings     = 512
	maxHistoryBindings      = 512
)

func sessionIdleTTL() time.Duration {
	if raw := strings.TrimSpace(os.Getenv("FREEBUFF_SESSION_IDLE_TTL")); raw != "" {
		if value, err := time.ParseDuration(raw); err == nil && value >= time.Minute {
			return value
		}
		log.Printf("[warn] invalid FREEBUFF_SESSION_IDLE_TTL=%q; using %s", raw, sessionBindingTTL)
	}
	return sessionBindingTTL
}

func newConversationRouter(stores ...*stateStore) *conversationRouter {
	router := &conversationRouter{
		byHistory:  make(map[string]sessionBinding),
		byToolCall: make(map[string]sessionBinding),
		byFallback: make(map[string]sessionBinding),
		external:   make(map[string]sessionObservation),
	}
	if len(stores) == 0 || stores[0] == nil {
		return router
	}
	router.store = stores[0]
	cutoff := time.Now().Add(-sessionIdleTTL())
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

func sessionDropRatio() float64 {
	if raw := strings.TrimSpace(os.Getenv("FREEBUFF_SESSION_DROP_RATIO")); raw != "" {
		if value, err := strconv.ParseFloat(raw, 64); err == nil && value > 0 && value < 1 {
			return value
		}
		log.Printf("[warn] invalid FREEBUFF_SESSION_DROP_RATIO=%q; using 0.5", raw)
	}
	return 0.5
}

func sessionDropMinimum() int {
	if raw := strings.TrimSpace(os.Getenv("FREEBUFF_SESSION_DROP_MIN_MESSAGES")); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil && value > 0 {
			return value
		}
		log.Printf("[warn] invalid FREEBUFF_SESSION_DROP_MIN_MESSAGES=%q; using 100", raw)
	}
	return 100
}

// observeExternal records client-side history size and detects an implicit
// reset (Hermes /new keeps the same X-Freebuff-Session-ID).
func (router *conversationRouter) observeExternal(id string, messageCount int, toolResult bool) bool {
	id = strings.TrimSpace(id)
	if id == "" {
		return false
	}
	now := time.Now()
	router.mu.Lock()
	previous, found := router.external[id]
	created := previous.Created
	if created.IsZero() {
		created = now
	}
	router.external[id] = sessionObservation{MessageCount: messageCount, EstimatedTokens: previous.EstimatedTokens, Created: created, Updated: now}
	router.mu.Unlock()
	if toolResult || !found || previous.MessageCount < sessionDropMinimum() || messageCount >= previous.MessageCount {
		return false
	}
	if float64(messageCount) > float64(previous.MessageCount)*sessionDropRatio() {
		return false
	}
	log.Printf("[warn] client session reset detected: session=%s messages=%d->%d", id, previous.MessageCount, messageCount)
	return true
}

func (router *conversationRouter) setExternalTokenEstimate(id string, estimatedTokens int) {
	router.mu.Lock()
	if observation, ok := router.external[id]; ok {
		observation.EstimatedTokens = estimatedTokens
		router.external[id] = observation
	}
	router.mu.Unlock()
}

func (router *conversationRouter) externalResetNeeded(id string, messageCount int, toolResult bool) bool {
	id = strings.TrimSpace(id)
	if id == "" || toolResult {
		return false
	}
	router.mu.Lock()
	previous, found := router.external[id]
	router.mu.Unlock()
	if !found || previous.MessageCount < sessionDropMinimum() || messageCount >= previous.MessageCount {
		return false
	}
	if float64(messageCount) > float64(previous.MessageCount)*sessionDropRatio() {
		return false
	}
	log.Printf("[warn] client session reset detected: session=%s messages=%d->%d", id, previous.MessageCount, messageCount)
	return true
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

func shortFallbackKey(request chatRequest, model string) string {
	user := strings.TrimSpace(request.User)
	if user == "" {
		return ""
	}
	if model = strings.TrimSpace(model); model == "" {
		model = defaultModel
	}
	sum := sha256.Sum256([]byte("short-fallback\x00" + model + "\x00" + user))
	return hex.EncodeToString(sum[:16])
}

func (router *conversationRouter) reapExpired() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-sessionIdleTTL())
		changed := false
		router.mu.Lock()
		for key, binding := range router.byHistory {
			if binding.Updated.Before(cutoff) {
				delete(router.byHistory, key)
				changed = true
			}
		}
		for toolCallID, binding := range router.byToolCall {
			if binding.Updated.Before(cutoff) {
				delete(router.byToolCall, toolCallID)
			}
		}
		for key, binding := range router.byFallback {
			if binding.Updated.Before(time.Now().Add(-shortSessionFallbackTTL)) {
				delete(router.byFallback, key)
			}
		}
		for id, observation := range router.external {
			if observation.Updated.Before(cutoff) {
				delete(router.external, id)
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

func (router *conversationRouter) resolve(request chatRequest, header string, models ...string) sessionSelection {
	model := defaultModel
	if len(models) > 0 && strings.TrimSpace(models[0]) != "" {
		model = models[0]
	}
	if explicit := explicitSessionID(request, header); explicit != "" {
		return sessionSelection{ID: explicit}
	}
	if toolResult := lastToolResult(request.Messages); toolResult != nil {
		router.mu.Lock()
		if binding, ok := router.byToolCall[toolResult.ToolCallID]; ok && binding.ID != "" {
			binding.Updated = time.Now()
			router.byToolCall[toolResult.ToolCallID] = binding
			router.mu.Unlock()
			return sessionSelection{ID: binding.ID, Automatic: true}
		}
		router.mu.Unlock()
	}
	key, found := historyKey(request)
	if !found {
		fallbackKey := shortFallbackKey(request, model)
		if fallbackKey != "" {
			router.mu.Lock()
			if binding, ok := router.byFallback[fallbackKey]; ok && binding.ID != "" && binding.Updated.After(time.Now().Add(-shortSessionFallbackTTL)) {
				router.mu.Unlock()
				return sessionSelection{ID: binding.ID, Automatic: true, FallbackKey: fallbackKey}
			}
			router.mu.Unlock()
		}
		return sessionSelection{ID: newAutoSessionID(), Automatic: true, FallbackKey: fallbackKey}
	}
	router.mu.Lock()
	defer router.mu.Unlock()
	if binding, ok := router.byHistory[key]; ok && binding.ID != "" {
		binding.Updated = time.Now()
		router.byHistory[key] = binding
		router.persist(key, binding, true)
		return sessionSelection{ID: binding.ID, Automatic: true, FallbackKey: shortFallbackKey(request, model)}
	}
	return sessionSelection{ID: newAutoSessionID(), Automatic: true, FallbackKey: shortFallbackKey(request, model)}
}

func (router *conversationRouter) discardFallback(key string) {
	if key == "" {
		return
	}
	router.mu.Lock()
	delete(router.byFallback, key)
	router.mu.Unlock()
}

func (router *conversationRouter) completeToolResult(toolCallID string) {
	if toolCallID == "" {
		return
	}
	router.mu.Lock()
	delete(router.byToolCall, toolCallID)
	router.mu.Unlock()
}

func (router *conversationRouter) resetSession(sessionID string) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return
	}
	removedHistory := make([]string, 0)
	router.mu.Lock()
	delete(router.external, sessionID)
	for _, model := range gatewayModels {
		internal := scopedSessionID(model, sessionID)
		for key, binding := range router.byHistory {
			if binding.ID == sessionID || binding.ID == internal {
				delete(router.byHistory, key)
				removedHistory = append(removedHistory, key)
			}
		}
		for key, binding := range router.byFallback {
			if binding.ID == sessionID || binding.ID == internal {
				delete(router.byFallback, key)
			}
		}
		for toolCallID, binding := range router.byToolCall {
			if binding.ID == sessionID || binding.ID == internal {
				delete(router.byToolCall, toolCallID)
			}
		}
	}
	router.mu.Unlock()
	if router.store == nil || len(removedHistory) == 0 {
		return
	}
	router.store.mu.Lock()
	for _, key := range removedHistory {
		delete(router.store.state.ConversationSessions, key)
	}
	if err := router.store.saveLocked(); err != nil {
		log.Printf("persist session reset: %v", err)
	}
	router.store.mu.Unlock()
}

func (router *conversationRouter) sessionStatus(sessionID string) (sessionObservation, bool) {
	router.mu.Lock()
	defer router.mu.Unlock()
	observation, ok := router.external[strings.TrimSpace(sessionID)]
	return observation, ok
}

func (router *conversationRouter) sessionIDs() []string {
	router.mu.Lock()
	defer router.mu.Unlock()
	result := make([]string, 0, len(router.external))
	for id := range router.external {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

// forget removes all automatic routes pointing at a failed CLI session.  A
// restarted headless process no longer owns that in-memory run, so retaining
// the route would only make the next request resume broken context.
func (router *conversationRouter) forget(sessionID string) {
	if sessionID == "" {
		return
	}
	router.mu.Lock()
	removedHistory := make([]string, 0)
	for key, binding := range router.byHistory {
		if binding.ID == sessionID {
			delete(router.byHistory, key)
			removedHistory = append(removedHistory, key)
		}
	}
	for toolCallID, binding := range router.byToolCall {
		if binding.ID == sessionID {
			delete(router.byToolCall, toolCallID)
		}
	}
	for key, binding := range router.byFallback {
		if binding.ID == sessionID {
			delete(router.byFallback, key)
		}
	}
	router.mu.Unlock()
	if router.store == nil || len(removedHistory) == 0 {
		return
	}
	router.store.mu.Lock()
	for _, key := range removedHistory {
		delete(router.store.state.ConversationSessions, key)
	}
	err := router.store.saveLocked()
	router.store.mu.Unlock()
	if err != nil {
		log.Printf("remove failed conversation routes: %v", err)
	}
}

func (router *conversationRouter) bind(request chatRequest, result cliChatResult, selection sessionSelection) {
	if !selection.Automatic {
		return
	}
	key, found := historyKey(request)
	if !found {
		key, found = historyKeyForResult(request, result)
	}
	if !found && len(result.ToolCalls) == 0 && selection.FallbackKey == "" {
		return
	}
	router.mu.Lock()
	ambiguousHistory := false
	if found {
		if existing, ok := router.byHistory[key]; ok && existing.ID != selection.ID {
			// The same shortened history can belong to more than one conversation.
			// Fail closed rather than attaching one agent run to another's context.
			ambiguousHistory = true
		}
	}
	binding := sessionBinding{ID: selection.ID, Updated: time.Now()}
	historyBinding := binding
	if ambiguousHistory {
		historyBinding.ID = ""
	}
	if selection.FallbackKey != "" {
		router.byFallback[selection.FallbackKey] = binding
	}
	if found {
		router.byHistory[key] = historyBinding
	}
	for _, toolCall := range result.ToolCalls {
		if toolCall.ID != "" {
			router.byToolCall[toolCall.ID] = binding
		}
	}
	for len(router.byToolCall) > maxToolCallBindings {
		var oldestID string
		var oldest time.Time
		for toolCallID, candidate := range router.byToolCall {
			if oldestID == "" || candidate.Updated.Before(oldest) {
				oldestID, oldest = toolCallID, candidate.Updated
			}
		}
		delete(router.byToolCall, oldestID)
	}
	if len(router.byHistory) <= maxHistoryBindings {
		router.mu.Unlock()
		if found {
			router.persist(key, historyBinding, true)
		}
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
	router.persist(key, historyBinding, true)
}

func (router *conversationRouter) count() int {
	router.mu.Lock()
	defer router.mu.Unlock()
	return len(router.byHistory)
}

type server struct {
	tenantsMu sync.Mutex
	tenants   map[string]*server
	namespace string
	accounts  *accountManager
	sessions  *conversationRouter
	admin     *adminService
}

func (s *server) sessionStatus(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeError(w, http.StatusBadRequest, "session id is required", "invalid_request")
		return
	}
	observation, found := s.sessions.sessionStatus(id)
	account := s.accounts.sessionStatus(s.accountSession(id))
	if !found && account == nil {
		writeError(w, http.StatusNotFound, "session not found", "session_not_found")
		return
	}
	result := map[string]any{"id": id, "message_count": observation.MessageCount, "estimated_tokens": observation.EstimatedTokens, "created_at": observation.Created, "last_active_at": observation.Updated}
	for key, value := range account {
		result[key] = value
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) sessionList(w http.ResponseWriter, _ *http.Request) {
	result := make([]map[string]any, 0)
	for _, id := range s.sessions.sessionIDs() {
		observation, _ := s.sessions.sessionStatus(id)
		item := map[string]any{"id": id, "message_count": observation.MessageCount, "estimated_tokens": observation.EstimatedTokens, "created_at": observation.Created, "last_active_at": observation.Updated}
		for key, value := range s.accounts.sessionStatus(s.accountSession(id)) {
			item[key] = value
		}
		result = append(result, item)
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": result})
}

func (s *server) sessionCreate(w http.ResponseWriter, _ *http.Request) {
	id := newAutoSessionID()
	s.sessions.observeExternal(id, 0, false)
	writeJSON(w, http.StatusCreated, map[string]any{"id": id})
}

func (s *server) sessionReset(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeError(w, http.StatusBadRequest, "session id is required", "invalid_request")
		return
	}
	if err := s.accounts.resetSession(s.accountSession(id)); err != nil {
		writeError(w, http.StatusConflict, err.Error(), "session_conflict")
		return
	}
	s.sessions.resetSession(id)
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "reset": true})
}

func (s *server) sessionDelete(w http.ResponseWriter, r *http.Request) {
	s.sessionReset(w, r)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message, code string) {
	writeJSON(w, status, map[string]any{"error": map[string]any{"message": message, "type": code}})
}

func upstreamErrorStatus(message string) (int, string) {
	lower := strings.ToLower(message)
	switch {
	case strings.Contains(lower, "no authenticated account"), strings.Contains(lower, "cooling down"), strings.Contains(lower, "conversation account is unavailable"):
		return http.StatusServiceUnavailable, "account_unavailable"
	case strings.Contains(lower, "tool call") && strings.Contains(lower, "expired"):
		return http.StatusConflict, "tool_call_expired"
	case strings.Contains(lower, "session conflict") || strings.Contains(lower, "waiting for a tool result"):
		return http.StatusConflict, "session_conflict"
	case strings.Contains(lower, "conversation account is busy"):
		return http.StatusTooManyRequests, "session_busy"
	case strings.Contains(lower, "rate_limit"), strings.Contains(lower, "rate limit"),
		strings.Contains(lower, "quota"), strings.Contains(lower, "budget"), strings.Contains(lower, "429"):
		return http.StatusTooManyRequests, "upstream_rate_limited"
	case strings.Contains(lower, "timeout"), strings.Contains(lower, "deadline exceeded"):
		return http.StatusGatewayTimeout, "upstream_timeout"
	case strings.Contains(lower, "service_overloaded"), strings.Contains(lower, "ip_capped"),
		strings.Contains(lower, "temporarily unavailable"):
		return http.StatusServiceUnavailable, "upstream_unavailable"
	default:
		return http.StatusBadGateway, "upstream_error"
	}
}

func isIPCapError(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "ip_capped")
}

// Account quota failures are safe to retry on another authenticated account
// only for a fresh user turn. Tool-result callbacks must never be replayed:
// doing so could execute an external tool twice. finish() already removes the
// failed binding and places the account in cooldown before acquire() selects
// the next account.
func isAccountQuotaError(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "rate_limited") ||
		strings.Contains(lower, "rate limit") ||
		strings.Contains(lower, "spend_limited") ||
		strings.Contains(lower, "quota") ||
		strings.Contains(lower, "budget")
}

func isRecoverableCLIError(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "another external tool call is waiting") ||
		strings.Contains(lower, "headless cli closed its output") ||
		strings.Contains(lower, "headless cli exited before ready") ||
		strings.Contains(lower, "headless cli did not become ready") ||
		strings.Contains(lower, "model_locked") || strings.Contains(lower, "session is locked to") ||
		strings.Contains(lower, "broken pipe") ||
		strings.Contains(lower, "unexpected eof")
}

func shouldAccountFailover(err error) bool {
	return isSessionAdmissionFailure(err) || isAccountQuotaError(err) || isRecoverableCLIError(err)
}

func (s *server) chatWithIPCapFailover(ctx context.Context, model string, selection sessionSelection, prompt string, content []map[string]any, messages []chatMessage, tools []openAITool, toolResult *chatMessage, onDelta func(string), canRetry func() bool) (cliChatResult, error) {
	selection.ID = s.accountSession(selection.ID)
	client, accountID, err := s.accounts.acquire(selection.ID, model)
	if err != nil {
		return cliChatResult{}, err
	}
	result, err := client.chat(ctx, model, scopedSessionID(model, selection.ID), prompt, content, tools, toolResult, onDelta, messages)
	s.accounts.finish(accountID, model, selection.ID, err)
	if err == nil || (canRetry != nil && !canRetry()) {
		return result, err
	}
	if isSessionAdmissionFailure(err) {
		// "banned" is an account-level admission failure. finish() already
		// cooled the account for 30 minutes; changing its exit here would
		// needlessly churn the proxy pool and must not make it retry immediately.
		s.accounts.markAdmissionFailure(accountID)
	} else if isIPCapError(err) {
		if s.admin == nil || !s.admin.rotateProxyAfterIPCap(accountID) {
			return result, err
		}
	} else if isAccountQuotaError(err) {
		// The failed account is cooled by finish(). A fresh turn can safely
		// continue on another account; a tool result cannot be replayed.
		if toolResult != nil || !accountFailoverEnabled() {
			return result, err
		}
		s.accounts.coolAccount(accountID)
	} else if isRecoverableCLIError(err) {
		// chat() already terminated its broken process in the output/start paths.
		// A waiting external-tool state needs an explicit reset before retrying a
		// plain chat request; never replay a tool-result callback.
		if toolResult != nil {
			return result, err
		}
		client.stop()
	} else {
		return result, err
	}
	if shouldAccountFailover(err) && !accountFailoverEnabled() {
		return result, err
	}
	client, accountID, retryAcquireErr := s.accounts.acquire(selection.ID, model)
	if retryAcquireErr != nil {
		return cliChatResult{}, retryAcquireErr
	}
	// A failed account loses its native CLI context. Fresh turns can be safely
	// replayed on the replacement account, but must carry the complete caller
	// history or the conversation would silently start from the latest prompt.
	if toolResult == nil && len(messages) > 1 {
		prompt = fullPromptFromMessages(messages, tools)
		if rebuilt, rebuildErr := multimodalContent(messages, prompt); rebuildErr == nil {
			content = rebuilt
		} else {
			s.accounts.finish(accountID, model, selection.ID, rebuildErr)
			return cliChatResult{}, rebuildErr
		}
	}
	result, err = client.chat(ctx, model, scopedSessionID(model, selection.ID), prompt, content, tools, toolResult, onDelta, messages)
	s.accounts.finish(accountID, model, selection.ID, err)
	return result, err
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
	externalSession := strings.TrimSpace(r.Header.Get("X-Freebuff-Session-ID"))
	if request.MaxTokens < 0 || request.MaxTokens >= contextLimit() {
		writeError(w, http.StatusBadRequest, "max_tokens must be positive and smaller than the context limit when supplied", "invalid_request")
		return
	}
	if request.MaxTokens == 0 && contextReserve() >= contextLimit() {
		writeError(w, http.StatusServiceUnavailable, "configured context reserve leaves no input budget", "invalid_context_configuration")
		return
	}
	resetRequested := strings.EqualFold(strings.TrimSpace(r.Header.Get("X-Freebuff-Session-Reset")), "1") ||
		strings.EqualFold(strings.TrimSpace(r.Header.Get("X-Freebuff-Session-Reset")), "true")
	estimatedTokens := estimateContextTokens(request)
	inputLimit := contextInputLimit(request)
	if estimatedTokens > inputLimit {
		message := fmt.Sprintf("estimated context %d tokens exceeds input limit %d (context limit %d, reserve %d); compact or reset the session", estimatedTokens, inputLimit, contextLimit(), contextReserve())
		log.Printf("[warn] context exceeded: session=%s messages=%d estimated_tokens=%d limit=%d", externalSession, len(request.Messages), estimatedTokens, inputLimit)
		writeError(w, http.StatusRequestEntityTooLarge, message, "context_exceeded")
		return
	}
	if externalSession != "" {
		implicitReset := s.sessions.externalResetNeeded(externalSession, len(request.Messages), lastToolResult(request.Messages) != nil)
		if resetRequested || implicitReset {
			if err := s.accounts.resetSession(s.accountSession(externalSession)); err != nil {
				writeError(w, http.StatusConflict, err.Error(), "session_conflict")
				return
			}
			s.sessions.resetSession(externalSession)
			// resetSession removes the old observation; retain the current
			// request count so the next turn starts from a known baseline.
		}
		s.sessions.observeExternal(externalSession, len(request.Messages), lastToolResult(request.Messages) != nil)
		s.sessions.setExternalTokenEstimate(externalSession, estimatedTokens)
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
	selection := s.sessions.resolve(request, externalSession, model)
	if toolResult != nil {
		if err := s.accounts.toolResultError(s.accountSession(selection.ID), model, toolResult.ToolCallID); err != nil {
			status, code := upstreamErrorStatus(err.Error())
			writeError(w, status, err.Error(), code)
			return
		}
	}
	log.Printf("[info] chat request: id=%s session=%s model=%s stream=%t tool_result=%t messages=%d", id, selection.ID, model, request.Stream, toolResult != nil, len(request.Messages))
	if selection.FallbackKey != "" && !s.accounts.sessionIdle(s.accountSession(selection.ID), model) {
		s.sessions.discardFallback(selection.FallbackKey)
		selection = sessionSelection{ID: newAutoSessionID(), Automatic: true, FallbackKey: shortFallbackKey(request, model)}
	}
	if toolResult == nil && !s.accounts.sessionIdle(s.accountSession(selection.ID), model) && len(request.Messages) > 1 {
		prompt = fullPromptFromMessages(request.Messages, request.Tools)
		content, err = multimodalContent(request.Messages, prompt)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), "invalid_image")
			return
		}
		// A fresh account process cannot accept a tool result from the old one.
		toolResult = nil
	}
	inputChars := len(prompt)
	requestContext, cancel := context.WithTimeout(r.Context(), maxGatewayRequestTime)
	defer cancel()

	if request.Stream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher, _ := w.(http.Flusher)
		started := false
		outputChars := 0
		emitDelta := func(delta string) {
			if delta == "" {
				return
			}
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
		textBuffer := streamTextBuffer{emit: emitDelta}
		result, err := s.chatWithIPCapFailover(requestContext, model, selection, prompt, content, request.Messages, request.Tools, toolResult, textBuffer.push, func() bool { return toolResult == nil && outputChars == 0 && textBuffer.leading == "" })
		if err != nil {
			s.sessions.forget(selection.ID)
			s.admin.recordUsage(model, apiKeyFromRequest(r), inputChars, outputChars, false, time.Since(startedAt), err)
			status, errorType := upstreamErrorStatus(err.Error())
			if !started {
				w.Header().Del("Content-Type")
				if status == http.StatusTooManyRequests {
					w.Header().Set("Retry-After", "5")
				}
				writeError(w, status, err.Error(), errorType)
				return
			}
			data, _ := json.Marshal(map[string]any{"error": map[string]any{"message": err.Error(), "type": errorType}})
			fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", data)
			return
		}
		if toolResult != nil {
			s.sessions.completeToolResult(toolResult.ToolCallID)
		}
		if !started && result.Text != "" && len(result.ToolCalls) == 0 {
			textBuffer.leading = ""
			emitDelta(result.Text)
		}
		textBuffer.finish(len(result.ToolCalls) > 0)
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

	result, err := s.chatWithIPCapFailover(requestContext, model, selection, prompt, content, request.Messages, request.Tools, toolResult, nil, func() bool { return toolResult == nil })
	if err != nil {
		s.sessions.forget(selection.ID)
		if strings.Contains(err.Error(), "no authenticated account") || strings.Contains(err.Error(), "cooling down") {
			writeError(w, http.StatusServiceUnavailable, err.Error(), "account_unavailable")
			return
		}
		s.admin.recordUsage(model, apiKeyFromRequest(r), inputChars, 0, false, time.Since(startedAt), err)
		status, errorType := upstreamErrorStatus(err.Error())
		if status == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", "5")
		}
		writeError(w, status, err.Error(), errorType)
		return
	}
	if toolResult != nil {
		s.sessions.completeToolResult(toolResult.ToolCallID)
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
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "backend": "freebuff-cli-jsonl", "version": gatewayVersion})
	})
	apiMux.HandleFunc("GET /v1/models", service.admin.requireAPIKey(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": gatewayModelList()})
	}))
	apiMux.HandleFunc("GET /v1/sessions", service.tenantHandler((*server).sessionList))
	apiMux.HandleFunc("POST /v1/sessions", service.tenantHandler((*server).sessionCreate))
	apiMux.HandleFunc("GET /v1/sessions/{id}", service.tenantHandler((*server).sessionStatus))
	apiMux.HandleFunc("POST /v1/sessions/{id}/reset", service.tenantHandler((*server).sessionReset))
	apiMux.HandleFunc("DELETE /v1/sessions/{id}", service.tenantHandler((*server).sessionDelete))
	apiMux.HandleFunc("POST /v1/chat/completions", service.tenantHandler((*server).chatCompletions))
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
