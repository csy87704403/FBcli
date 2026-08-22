package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPortableAccountBundleImportsIntoFreshAccountRoot(t *testing.T) {
	sourceDir := t.TempDir()
	sourceAccountDir := filepath.Join(sourceDir, "source-account")
	writeTestCredential(t, sourceAccountDir, "portable@example.com")
	sourceStore := &stateStore{path: filepath.Join(sourceDir, "state.json"), state: gatewayState{Accounts: []accountConfig{{
		ID: "account-portable", Label: "portable@example.com", ConfigDir: sourceAccountDir, Enabled: true, AddedAt: time.Now(),
	}}}}
	source := newAccountManager(sourceStore, "headless", "", sourceDir, filepath.Join(sourceDir, "accounts"))
	bundle, err := source.exportPortableAccounts()
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Accounts) != 1 || !strings.Contains(string(bundle.Accounts[0].Credentials), `"authToken":"token"`) {
		t.Fatalf("exported credentials are incomplete: %#v", bundle)
	}

	targetDir := t.TempDir()
	targetStore := &stateStore{path: filepath.Join(targetDir, "state.json")}
	target := newAccountManager(targetStore, "headless", "", targetDir, filepath.Join(targetDir, "accounts"))
	updated, added, err := target.importPortableAccounts(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if updated != 1 || added != 1 || len(targetStore.state.Accounts) != 1 {
		t.Fatalf("unexpected import result: updated=%d added=%d accounts=%d", updated, added, len(targetStore.state.Accounts))
	}
	config := targetStore.state.Accounts[0]
	if filepath.Dir(config.ConfigDir) != filepath.Join(targetDir, "accounts") {
		t.Fatalf("source machine path was imported: %s", config.ConfigDir)
	}
	data, err := os.ReadFile(filepath.Join(config.ConfigDir, "credentials.json"))
	if err != nil || !json.Valid(data) || !readAccountCredential(config.ConfigDir).Authenticated {
		t.Fatalf("imported credentials are invalid: %v", err)
	}
}

func TestPortableAccountImportRejectsActiveRequests(t *testing.T) {
	dir := t.TempDir()
	accountDir := filepath.Join(dir, "one")
	writeTestCredential(t, accountDir, "one@example.com")
	store := &stateStore{path: filepath.Join(dir, "state.json"), state: gatewayState{Accounts: []accountConfig{{ID: "account-one", ConfigDir: accountDir, Enabled: true}}}}
	manager := newAccountManager(store, "headless", "", dir, filepath.Join(dir, "accounts"))
	manager.runtimes["account-one"].active = 1
	bundle, err := manager.exportPortableAccounts()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := manager.importPortableAccounts(bundle); err == nil || !strings.Contains(err.Error(), "requests are active") {
		t.Fatalf("active import was not rejected: %v", err)
	}
}

func TestRemoveAccountDeregistersWithoutDeletingCredentials(t *testing.T) {
	dir := t.TempDir()
	accountDir := filepath.Join(dir, "account-one")
	writeTestCredential(t, accountDir, "remove@example.com")
	store := &stateStore{path: filepath.Join(dir, "state.json"), state: gatewayState{
		Accounts:        []accountConfig{{ID: "account-one", ConfigDir: accountDir, Enabled: true}},
		AccountSessions: map[string]accountSessionBinding{"session": {AccountID: "account-one", Updated: time.Now()}},
	}}
	manager := newAccountManager(store, "headless", "", dir, filepath.Join(dir, "accounts"))
	manager.sessionAccounts["session"] = accountSessionBinding{AccountID: "account-one", Updated: time.Now()}
	if err := manager.remove("account-one"); err != nil {
		t.Fatal(err)
	}
	if manager.count() != 0 || len(store.state.Accounts) != 0 || len(store.state.AccountSessions) != 0 {
		t.Fatalf("account was not fully deregistered: runtimes=%d accounts=%d bindings=%d", manager.count(), len(store.state.Accounts), len(store.state.AccountSessions))
	}
	if _, err := os.Stat(filepath.Join(accountDir, "credentials.json")); err != nil {
		t.Fatalf("credentials were unexpectedly removed: %v", err)
	}
}

func TestDefaultAccountIsNotReimportedAfterRemoval(t *testing.T) {
	dir := t.TempDir()
	defaultDir := filepath.Join(dir, "default")
	writeTestCredential(t, defaultDir, "default@example.com")
	store := &stateStore{path: filepath.Join(dir, "state.json")}
	if err := ensureDefaultAccount(store, defaultDir); err != nil {
		t.Fatal(err)
	}
	manager := newAccountManager(store, "headless", "", dir, filepath.Join(dir, "accounts"))
	if err := manager.remove("account-default"); err != nil {
		t.Fatal(err)
	}
	if err := ensureDefaultAccount(store, defaultDir); err != nil {
		t.Fatal(err)
	}
	if len(store.state.Accounts) != 0 {
		t.Fatal("removed default account was imported again")
	}
}

func writeTestCredential(t *testing.T, dir, email string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	content := `{"default":{"name":"Test","email":"` + email + `","authToken":"token"}}`
	if err := os.WriteFile(filepath.Join(dir, "credentials.json"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestAccountSchedulerBalancesAndPinsSessions(t *testing.T) {
	dir := t.TempDir()
	oneDir, twoDir := filepath.Join(dir, "one"), filepath.Join(dir, "two")
	writeTestCredential(t, oneDir, "one@example.com")
	writeTestCredential(t, twoDir, "two@example.com")
	store := &stateStore{path: filepath.Join(dir, "state.json"), state: gatewayState{Accounts: []accountConfig{
		{ID: "one", ConfigDir: oneDir, Enabled: true},
		{ID: "two", ConfigDir: twoDir, Enabled: true},
	}}}
	manager := newAccountManager(store, "headless", "", dir, filepath.Join(dir, "accounts"))

	_, firstAccount, err := manager.acquire("session-one", defaultModel)
	if err != nil {
		t.Fatal(err)
	}
	_, secondAccount, err := manager.acquire("session-two", defaultModel)
	if err != nil {
		t.Fatal(err)
	}
	if firstAccount == secondAccount {
		t.Fatalf("two concurrent sessions selected the same account: %s", firstAccount)
	}
	manager.finish(firstAccount, defaultModel, "session-one", nil)
	manager.finish(secondAccount, defaultModel, "session-two", nil)

	_, resumedAccount, err := manager.acquire("session-one", defaultModel)
	if err != nil {
		t.Fatal(err)
	}
	manager.finish(resumedAccount, defaultModel, "session-one", nil)
	if resumedAccount != firstAccount {
		t.Fatalf("session moved accounts: got %s, want %s", resumedAccount, firstAccount)
	}
}

func TestAccountSchedulerPrefersEmptyAccountForDifferentModel(t *testing.T) {
	dir := t.TempDir()
	oneDir, twoDir := filepath.Join(dir, "one"), filepath.Join(dir, "two")
	writeTestCredential(t, oneDir, "one@example.com")
	writeTestCredential(t, twoDir, "two@example.com")
	store := &stateStore{path: filepath.Join(dir, "state.json"), state: gatewayState{Accounts: []accountConfig{
		{ID: "one", ConfigDir: oneDir, Enabled: true},
		{ID: "two", ConfigDir: twoDir, Enabled: true},
	}}}
	manager := newAccountManager(store, "headless", "", dir, filepath.Join(dir, "accounts"))

	_, deepSeekAccount, err := manager.acquire("deepseek-session", defaultModel)
	if err != nil {
		t.Fatal(err)
	}
	manager.finish(deepSeekAccount, defaultModel, "deepseek-session", nil)
	_, mimoAccount, err := manager.acquire("mimo-session", mimoModel)
	if err != nil {
		t.Fatal(err)
	}
	manager.finish(mimoAccount, mimoModel, "mimo-session", nil)
	if mimoAccount == deepSeekAccount {
		t.Fatalf("MiMo reused the DeepSeek-locked account while an empty account was available: %s", mimoAccount)
	}
}

func TestAccountSchedulerSkipsAccountWaitingForAnotherToolResult(t *testing.T) {
	dir := t.TempDir()
	oneDir, twoDir := filepath.Join(dir, "one"), filepath.Join(dir, "two")
	writeTestCredential(t, oneDir, "one@example.com")
	writeTestCredential(t, twoDir, "two@example.com")
	store := &stateStore{path: filepath.Join(dir, "state.json"), state: gatewayState{Accounts: []accountConfig{
		{ID: "one", ConfigDir: oneDir, Enabled: true},
		{ID: "two", ConfigDir: twoDir, Enabled: true},
	}}}
	manager := newAccountManager(store, "headless", "", dir, filepath.Join(dir, "accounts"))
	manager.runtimes["one"].client.pending = &pendingToolCall{SessionID: "blocked-session", CreatedAt: time.Now()}

	_, accountID, err := manager.acquire("new-session", defaultModel)
	if err != nil {
		t.Fatal(err)
	}
	manager.finish(accountID, defaultModel, "new-session", nil)
	if accountID != "two" {
		t.Fatalf("new session used account waiting for another tool result: %s", accountID)
	}
}

func TestAccountSchedulerUsesOneActiveRequestPerAccount(t *testing.T) {
	dir := t.TempDir()
	oneDir, twoDir := filepath.Join(dir, "one"), filepath.Join(dir, "two")
	writeTestCredential(t, oneDir, "one@example.com")
	writeTestCredential(t, twoDir, "two@example.com")
	store := &stateStore{path: filepath.Join(dir, "state.json"), state: gatewayState{Accounts: []accountConfig{
		{ID: "one", ConfigDir: oneDir, Enabled: true}, {ID: "two", ConfigDir: twoDir, Enabled: true},
	}}}
	manager := newAccountManager(store, "headless", "", dir, filepath.Join(dir, "accounts"))
	_, first, err := manager.acquire("session-one", defaultModel)
	if err != nil {
		t.Fatal(err)
	}
	_, second, err := manager.acquire("session-two", defaultModel)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("concurrent requests used one account: %s", first)
	}
}

func TestAccountSessionIdleRejectsActiveSession(t *testing.T) {
	dir := t.TempDir()
	accountDir := filepath.Join(dir, "one")
	writeTestCredential(t, accountDir, "one@example.com")
	store := &stateStore{path: filepath.Join(dir, "state.json"), state: gatewayState{Accounts: []accountConfig{{ID: "one", ConfigDir: accountDir, Enabled: true}}}}
	manager := newAccountManager(store, "headless", "", dir, filepath.Join(dir, "accounts"))
	_, accountID, err := manager.acquire("hermes-short", defaultModel)
	if err != nil {
		t.Fatal(err)
	}
	if manager.sessionIdle("hermes-short", defaultModel) {
		t.Fatal("active account session was considered idle")
	}
	manager.finish(accountID, defaultModel, "hermes-short", nil)
	if !manager.sessionIdle("hermes-short", defaultModel) {
		t.Fatal("completed account session was not considered idle")
	}
}

func TestAdmissionCooldownIsScopedToAccountExit(t *testing.T) {
	dir := t.TempDir()
	accountDir := filepath.Join(dir, "one")
	writeTestCredential(t, accountDir, "one@example.com")
	store := &stateStore{path: filepath.Join(dir, "state.json"), state: gatewayState{Accounts: []accountConfig{{ID: "one", ConfigDir: accountDir, Proxy: "old", Enabled: true}}}}
	manager := newAccountManager(store, "headless", "", dir, filepath.Join(dir, "accounts"))
	manager.markAdmissionFailure("one")
	if _, _, err := manager.acquire("session", defaultModel); err == nil {
		t.Fatal("admission-cooled account+exit was selected")
	}
	if !manager.setAccountProxy("one", "new") {
		t.Fatal("could not switch account exit")
	}
	if _, _, err := manager.acquire("session", defaultModel); err != nil {
		t.Fatalf("new exit remained incorrectly cooled: %v", err)
	}
}

func TestToolLoopIsResetInsteadOfPersisting(t *testing.T) {
	client := &cliClient{}
	call := openAIToolCall{ID: "call-1"}
	call.Function.Name = "read_file"
	call.Function.Arguments = `{"path":"a.txt"}`
	for i := 0; i < maxRepeatedToolCalls; i++ {
		if err := client.setPendingToolCall("request", "session", call); err != nil {
			t.Fatalf("tool call %d unexpectedly failed: %v", i+1, err)
		}
	}
	if err := client.setPendingToolCall("request", "session", call); err == nil {
		t.Fatal("repeated tool loop was not rejected")
	}
	if client.pending != nil || client.toolCalls != 0 {
		t.Fatal("tool state remained after loop reset")
	}
}

func TestExpiredToolCallIsReset(t *testing.T) {
	client := &cliClient{pending: &pendingToolCall{SessionID: "session", CreatedAt: time.Now().Add(-staleToolResultTimeout)}}
	if !client.recoverExpiredToolCall(time.Now()) {
		t.Fatal("expired tool call was not reset")
	}
	if client.pending != nil {
		t.Fatal("expired tool call remained pending")
	}
}

func TestLimitedAccountEntersCooldown(t *testing.T) {
	dir := t.TempDir()
	accountDir := filepath.Join(dir, "one")
	writeTestCredential(t, accountDir, "one@example.com")
	store := &stateStore{path: filepath.Join(dir, "state.json"), state: gatewayState{Accounts: []accountConfig{{ID: "one", ConfigDir: accountDir, Enabled: true}}}}
	manager := newAccountManager(store, "headless", "", dir, filepath.Join(dir, "accounts"))
	_, accountID, err := manager.acquire("session", defaultModel)
	if err != nil {
		t.Fatal(err)
	}
	manager.finish(accountID, defaultModel, "session", os.ErrDeadlineExceeded)
	if !manager.runtimes[accountID].cooldownUntil.IsZero() {
		t.Fatal("transport error incorrectly cooled down the account")
	}
	_, accountID, err = manager.acquire("limited-session", defaultModel)
	if err != nil {
		t.Fatal(err)
	}
	manager.finish(accountID, defaultModel, "limited-session", &testError{"free session rate_limited"})
	if !manager.runtimes[accountID].cooldownUntil.After(time.Now()) {
		t.Fatal("rate-limited account did not enter cooldown")
	}
}

func TestFailedRequestReleasesAccountSessionBinding(t *testing.T) {
	dir := t.TempDir()
	accountDir := filepath.Join(dir, "one")
	writeTestCredential(t, accountDir, "one@example.com")
	store := &stateStore{path: filepath.Join(dir, "state.json"), state: gatewayState{Accounts: []accountConfig{{ID: "one", ConfigDir: accountDir, Enabled: true}}, AccountSessions: make(map[string]accountSessionBinding)}}
	manager := newAccountManager(store, "headless", "", dir, filepath.Join(dir, "accounts"))
	_, accountID, err := manager.acquire("failed-session", defaultModel)
	if err != nil {
		t.Fatal(err)
	}
	manager.finish(accountID, defaultModel, "failed-session", &testError{"upstream quota exhausted"})
	if _, found := manager.sessionAccounts[accountSessionKey("failed-session")]; found {
		t.Fatal("failed request kept its account session binding")
	}
	if !manager.runtimes[accountID].cooldownUntil.After(time.Now()) {
		t.Fatal("quota failure did not cool down the account")
	}
}

type testError struct{ message string }

func (e *testError) Error() string { return e.message }

func TestCLIEnvironmentIsolatesConfigDirectory(t *testing.T) {
	env := cliEnvironment("", `E:\accounts\one`)
	found := false
	for _, item := range env {
		if strings.EqualFold(item, `FREEBUFF_CONFIG_DIR=E:\accounts\one`) {
			found = true
		}
	}
	if !found {
		t.Fatal("FREEBUFF_CONFIG_DIR missing from child environment")
	}
}

func TestCleanLoginURL(t *testing.T) {
	line := "\x1b[36mhttps://freebuff.com/cli/login?code=abc\x1b[0m"
	if got := cleanLoginURL(line); got != "https://freebuff.com/cli/login?code=abc" {
		t.Fatalf("login URL = %q", got)
	}
}

func TestAccountSessionBindingSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	oneDir, twoDir := filepath.Join(dir, "one"), filepath.Join(dir, "two")
	writeTestCredential(t, oneDir, "one@example.com")
	writeTestCredential(t, twoDir, "two@example.com")
	statePath := filepath.Join(dir, "state.json")
	store := &stateStore{path: statePath, state: gatewayState{Accounts: []accountConfig{
		{ID: "one", ConfigDir: oneDir, Enabled: true},
		{ID: "two", ConfigDir: twoDir, Enabled: true},
	}}}
	if err := store.saveLocked(); err != nil {
		t.Fatal(err)
	}
	firstManager := newAccountManager(store, "headless", "", dir, filepath.Join(dir, "accounts"))
	_, expectedAccount, err := firstManager.acquire("private-session-id", defaultModel)
	if err != nil {
		t.Fatal(err)
	}
	firstManager.finish(expectedAccount, defaultModel, "private-session-id", nil)

	reloadedStore, err := newStateStore(statePath, "", "")
	if err != nil {
		t.Fatal(err)
	}
	secondManager := newAccountManager(reloadedStore, "headless", "", dir, filepath.Join(dir, "accounts"))
	_, actualAccount, err := secondManager.acquire("private-session-id", defaultModel)
	if err != nil {
		t.Fatal(err)
	}
	secondManager.finish(actualAccount, defaultModel, "private-session-id", nil)
	if actualAccount != expectedAccount {
		t.Fatalf("session account after restart = %s, want %s", actualAccount, expectedAccount)
	}
	data, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "private-session-id") {
		t.Fatal("raw session id was persisted")
	}
}

func TestConversationBindingSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	store := &stateStore{path: filepath.Join(dir, "state.json")}
	firstRouter := newConversationRouter(store)
	request := chatRequest{Model: defaultModel, Messages: []chatMessage{{Role: "user", Content: "private opening"}}}
	selection := firstRouter.resolve(request, "")
	firstRouter.bind(request, cliChatResult{Text: "private answer"}, selection)

	reloadedStore, err := newStateStore(store.path, "", "")
	if err != nil {
		t.Fatal(err)
	}
	secondRouter := newConversationRouter(reloadedStore)
	continued := request
	continued.Messages = append(continued.Messages, chatMessage{Role: "assistant", Content: "private answer"}, chatMessage{Role: "user", Content: "continue"})
	if got := secondRouter.resolve(continued, "").ID; got != selection.ID {
		t.Fatalf("conversation session after restart = %s, want %s", got, selection.ID)
	}
	data, err := os.ReadFile(store.path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "private opening") || strings.Contains(string(data), "private answer") {
		t.Fatal("conversation text was persisted")
	}
}

func TestPrunePersistedSessionBindings(t *testing.T) {
	now := time.Now()
	state := gatewayState{
		AccountSessions: map[string]accountSessionBinding{
			"old":   {AccountID: "one", Updated: now.Add(-25 * time.Hour)},
			"fresh": {AccountID: "one", Updated: now},
		},
		ConversationSessions: map[string]sessionBinding{
			"old":   {ID: "old", Updated: now.Add(-25 * time.Hour)},
			"fresh": {ID: "fresh", Updated: now},
		},
	}
	if !prunePersistedSessionBindings(&state, now.Add(-sessionBindingTTL)) {
		t.Fatal("prune reported no changes")
	}
	if _, found := state.AccountSessions["old"]; found {
		t.Fatal("old account binding was retained")
	}
	if _, found := state.ConversationSessions["old"]; found {
		t.Fatal("old conversation binding was retained")
	}
	if _, found := state.AccountSessions["fresh"]; !found {
		t.Fatal("fresh account binding was removed")
	}
	if _, found := state.ConversationSessions["fresh"]; !found {
		t.Fatal("fresh conversation binding was removed")
	}
}
