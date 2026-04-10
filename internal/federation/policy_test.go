//nolint:gosec // test file: G306
package federation_test

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/federation"
	"github.com/wu/keyop-messenger/internal/testutil"
)

// ---- AtomicPolicy -----------------------------------------------------------

func TestAllowForwardExactMatch(t *testing.T) {
	ap := federation.NewAtomicPolicy(federation.ForwardPolicy{
		Forward: []string{"orders", "billing"},
	})
	assert.True(t, ap.AllowForward("orders"))
	assert.True(t, ap.AllowForward("billing"))
	assert.False(t, ap.AllowForward("shipping"))
}

func TestAllowReceiveExactMatch(t *testing.T) {
	ap := federation.NewAtomicPolicy(federation.ForwardPolicy{
		Receive: []string{"events"},
	})
	assert.True(t, ap.AllowReceive("events"))
	assert.False(t, ap.AllowReceive("orders"))
}

func TestAllowForwardEmptyList(t *testing.T) {
	ap := federation.NewAtomicPolicy(federation.ForwardPolicy{})
	// Forward: empty list means "don't forward to anyone".
	assert.False(t, ap.AllowForward("anything"))
	// Receive: empty list means "accept from anyone" (no allowlist = unrestricted).
	assert.True(t, ap.AllowReceive("anything"))
}

func TestAtomicPolicySwapRace(_ *testing.T) {
	ap := federation.NewAtomicPolicy(federation.ForwardPolicy{
		Forward: []string{"ch1"},
	})

	const readers = 50
	const iterations = 1000

	var wg sync.WaitGroup
	wg.Add(readers + 1)

	// Writer: swap policy repeatedly.
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			ap.Store(federation.ForwardPolicy{Forward: []string{"ch1", "ch2"}})
			ap.Store(federation.ForwardPolicy{Forward: []string{"ch1"}})
		}
	}()

	// Readers: call AllowForward concurrently — must not race.
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = ap.AllowForward("ch1")
				_ = ap.AllowForward("ch2")
			}
		}()
	}

	wg.Wait()
}

// ---- HubConfig / IsPeerAllowed ------------------------------------------

func TestIsPeerAllowed(t *testing.T) {
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{
			{Name: "billing-host"},
			{Name: "orders-host"},
		},
	}
	assert.True(t, cfg.IsPeerAllowed("billing-host"))
	assert.True(t, cfg.IsPeerAllowed("orders-host"))
	assert.False(t, cfg.IsPeerAllowed("unknown-host"))
	assert.False(t, cfg.IsPeerAllowed(""))
}

func TestIsPeerAllowedEmpty(t *testing.T) {
	cfg := federation.HubConfig{}
	assert.False(t, cfg.IsPeerAllowed("anyone"))
}

// ---- PolicyWatcher ----------------------------------------------------------

// fakeHub records ApplyPolicy calls.
type fakeHub struct {
	mu      sync.Mutex
	applied []federation.HubConfig
}

func (f *fakeHub) ApplyPolicy(cfg federation.HubConfig) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, cfg)
}

func (f *fakeHub) applyCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.applied)
}

func (f *fakeHub) lastApplied() (federation.HubConfig, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.applied) == 0 {
		return federation.HubConfig{}, false
	}
	return f.applied[len(f.applied)-1], true
}

// fakeAuditLogger records Log calls.
type fakeAuditLogger struct {
	mu     sync.Mutex
	events []audit.Event
}

func (f *fakeAuditLogger) Log(ev audit.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, ev)
	return nil
}

func (f *fakeAuditLogger) Close() error { return nil }

func (f *fakeAuditLogger) hasEvent(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ev := range f.events {
		if ev.Event == name {
			return true
		}
	}
	return false
}

const validConfig = `
allowed_peers:
  - name: billing-host
`

const validConfig2 = `
allowed_peers:
  - name: billing-host
  - name: orders-host
`

const invalidConfig = `
allowed_peers:
  - name: ""
`

func writeConfig(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

func TestPolicyWatcherReload(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "hub.yaml")
	writeConfig(t, cfgPath, validConfig)

	hub := &fakeHub{}
	auditLog := &fakeAuditLogger{}
	log := &testutil.FakeLogger{}

	pw, err := federation.NewPolicyWatcher(cfgPath, hub, auditLog, log)
	require.NoError(t, err)
	defer func() { _ = pw.Close() }()

	// Overwrite with updated config to trigger reload.
	writeConfig(t, cfgPath, validConfig2)

	// Wait up to 1 second for ApplyPolicy to be called.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if hub.applyCount() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.GreaterOrEqual(t, hub.applyCount(), 1, "ApplyPolicy should have been called")

	cfg, ok := hub.lastApplied()
	require.True(t, ok)
	assert.True(t, cfg.IsPeerAllowed("orders-host"), "updated config should include orders-host")
	assert.True(t, auditLog.hasEvent(audit.EventPolicyReloaded))
}

func TestPolicyWatcherInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "hub.yaml")
	writeConfig(t, cfgPath, validConfig)

	hub := &fakeHub{}
	auditLog := &fakeAuditLogger{}
	log := &testutil.FakeLogger{}

	pw, err := federation.NewPolicyWatcher(cfgPath, hub, auditLog, log)
	require.NoError(t, err)
	defer func() { _ = pw.Close() }()

	// Write an invalid config (peer with empty addr).
	writeConfig(t, cfgPath, invalidConfig)

	// Wait up to 1 second for the reload attempt.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if auditLog.hasEvent(audit.EventPolicyReloadFailed) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	assert.True(t, auditLog.hasEvent(audit.EventPolicyReloadFailed), "should log reload failure")
	assert.Equal(t, 0, hub.applyCount(), "ApplyPolicy must not be called on invalid config")
	assert.True(t, log.HasError("policy reload failed"), "should log error")
}

func TestPolicyWatcherClose(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "hub.yaml")
	writeConfig(t, cfgPath, validConfig)

	pw, err := federation.NewPolicyWatcher(cfgPath, &fakeHub{}, &fakeAuditLogger{}, &testutil.FakeLogger{})
	require.NoError(t, err)

	done := make(chan struct{})
	go func() { _ = pw.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() timed out")
	}
}
