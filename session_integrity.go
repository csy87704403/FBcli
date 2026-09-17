package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
)

func messageHashes(messages []chatMessage) [][32]byte {
	result := make([][32]byte, len(messages))
	for i, message := range messages {
		data, _ := json.Marshal(message)
		result[i] = sha256.Sum256(data)
	}
	return result
}

// Delay only leading blank chunks until their meaning is known. Blanks within
// text are always preserved; an otherwise empty tool response needs no content.
type streamTextBuffer struct {
	leading string
	started bool
	emit    func(string)
}

func (b *streamTextBuffer) push(delta string) {
	if delta == "" {
		return
	}
	if !b.started && strings.TrimSpace(delta) == "" {
		b.leading += delta
		return
	}
	if !b.started {
		delta = b.leading + delta
		b.leading = ""
		b.started = true
	}
	b.emit(delta)
}
func (b *streamTextBuffer) finish(toolCalls bool) {
	if !toolCalls && b.leading != "" {
		b.emit(b.leading)
	}
	b.leading = ""
}

func historyExtends(previous, incoming [][32]byte) bool {
	if len(previous) == 0 || len(incoming) <= len(previous) {
		return false
	}
	for i := range previous {
		if previous[i] != incoming[i] {
			return false
		}
	}
	return true
}

func (c *cliClient) rememberHistory(id string, hashes [][32]byte) {
	if c.histories == nil {
		c.histories = make(map[string][][32]byte)
	}
	c.histories[id] = hashes
}

func (s *server) accountSession(id string) string { return s.namespace + id }

// Each authenticated key owns its routing tables. Account scheduling remains
// shared, but bindings use a non-secret key namespace. On restart the caller's
// messages rebuild native history; no unscoped legacy route is imported.
func (s *server) tenantHandler(handler func(*server, http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return s.admin.requireAPIKey(func(w http.ResponseWriter, r *http.Request) {
		digest := sha256.Sum256([]byte(apiKeyFromRequest(r)))
		namespace := hex.EncodeToString(digest[:]) + ":"
		s.tenantsMu.Lock()
		if s.tenants == nil {
			s.tenants = make(map[string]*server)
		}
		tenant := s.tenants[namespace]
		if tenant == nil {
			tenant = &server{accounts: s.accounts, admin: s.admin, sessions: newConversationRouter(), namespace: namespace}
			go tenant.sessions.reapExpired()
			s.tenants[namespace] = tenant
		}
		s.tenantsMu.Unlock()
		handler(tenant, w, r)
	})
}
