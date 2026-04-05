//go:build integration

package messenger

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/keyop/keyop-messenger/internal/federation"
	"github.com/keyop/keyop-messenger/internal/tlsutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// integrationTLS generates a CA and two per-instance PEM certs for the given
// names, writes them under dir, and returns helper closures.
func integrationTLS(
	t *testing.T,
	dir string,
	names ...string,
) (caFile string, certFor, keyFor func(string) string) {
	t.Helper()
	caCert, caKey, err := tlsutil.GenerateCA(30)
	require.NoError(t, err)

	caFile = filepath.Join(dir, "ca.crt")
	require.NoError(t, os.WriteFile(caFile, caCert, 0o600))

	certPaths := map[string]string{}
	keyPaths := map[string]string{}
	for _, name := range names {
		cert, key, err2 := tlsutil.GenerateInstance(caCert, caKey, name, 30)
		require.NoError(t, err2)
		cp := filepath.Join(dir, name+".crt")
		kp := filepath.Join(dir, name+".key")
		require.NoError(t, os.WriteFile(cp, cert, 0o600))
		require.NoError(t, os.WriteFile(kp, key, 0o600))
		certPaths[name] = cp
		keyPaths[name] = kp
	}
	return caFile,
		func(n string) string { return certPaths[n] },
		func(n string) string { return keyPaths[n] }
}

// newHubMessenger creates a Messenger in hub mode listening on 127.0.0.1:0.
func newHubMessenger(
	t *testing.T,
	name, dir, caFile, certFile, keyFile string,
	hubCfg HubConfig,
) *Messenger {
	t.Helper()
	hubCfg.Enabled = true
	if hubCfg.ListenAddr == "" {
		hubCfg.ListenAddr = "127.0.0.1:0"
	}
	cfg := &Config{
		Name: name,
		Storage: StorageConfig{
			DataDir:    dir,
			SyncPolicy: SyncPolicyNone,
		},
		Hub: hubCfg,
		TLS: TLSConfig{Cert: certFile, Key: keyFile, CA: caFile},
	}
	cfg.ApplyDefaults()
	m, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// newClientMessenger creates a Messenger in client-only mode dialling hubAddr.
func newClientMessenger(
	t *testing.T,
	name, dir, caFile, certFile, keyFile, hubAddr string,
) *Messenger {
	t.Helper()
	cfg := &Config{
		Name: name,
		Storage: StorageConfig{
			DataDir:    filepath.Join(dir, name),
			SyncPolicy: SyncPolicyNone,
		},
		Client: ClientConfig{
			Enabled: true,
			Hubs:    []ClientHubRef{{Addr: hubAddr}},
		},
		TLS: TLSConfig{Cert: certFile, Key: keyFile, CA: caFile},
	}
	cfg.ApplyDefaults()
	m, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// hubLocalAddr returns "localhost:PORT" for a Messenger hub listener.
func hubLocalAddr(t *testing.T, m *Messenger) string {
	t.Helper()
	addr := m.HubAddr()
	require.NotEmpty(t, addr)
	_, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	return net.JoinHostPort("localhost", port)
}

// ---- TestIntegrationHubTwoClients -------------------------------------------

// TestIntegrationHubTwoClients verifies that two clients can simultaneously
// connect to a hub and that messages from each client are independently
// delivered to hub subscribers.
func TestIntegrationHubTwoClients(t *testing.T) {
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "client-a", "client-b")

	hubM := newHubMessenger(t, "hub", filepath.Join(dir, "hub"), caFile,
		certFor("localhost"), keyFor("localhost"),
		HubConfig{
			AllowedClients: []AllowedClient{{Name: "client-a"}, {Name: "client-b"}},
		},
	)
	hubAddr := hubLocalAddr(t, hubM)

	clientA := newClientMessenger(t, "client-a", dir, caFile, certFor("client-a"), keyFor("client-a"), hubAddr)
	clientB := newClientMessenger(t, "client-b", dir, caFile, certFor("client-b"), keyFor("client-b"), hubAddr)

	received := make(chan Message, 10)
	require.NoError(t, hubM.Subscribe(context.Background(), "events", "hub-sub",
		func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		},
	))

	// Allow both clients to establish connections before publishing.
	time.Sleep(200 * time.Millisecond)

	require.NoError(t, clientA.Publish(context.Background(), "events", "test.Evt",
		map[string]any{"from": "a"}))
	require.NoError(t, clientB.Publish(context.Background(), "events", "test.Evt",
		map[string]any{"from": "b"}))

	origins := map[string]bool{}
	deadline := time.After(3 * time.Second)
	for len(origins) < 2 {
		select {
		case msg := <-received:
			origins[msg.Origin] = true
		case <-deadline:
			t.Fatalf("only received from origins: %v (wanted both client-a and client-b)", origins)
		}
	}
	assert.True(t, origins["client-a"], "expected message from client-a")
	assert.True(t, origins["client-b"], "expected message from client-b")
}

// ---- TestIntegrationRingDedup -----------------------------------------------

// TestIntegrationRingDedup verifies that the shared per-Messenger dedup LRU
// prevents duplicate delivery when the same envelope arrives via two
// simultaneous forwarding paths.
//
// Topology:
//   - Hub2: hub listener, AllowedClients=["hub1"]
//   - Hub1: client with TWO independent connections to Hub2 (same address)
//
// When Hub1 publishes M on "ring-events":
//   - Hub1.clients[0].Sender → sends M to Hub2 via connection A
//   - Hub1.clients[1].Sender → sends M to Hub2 via connection B
//
// Hub2 receives M via two concurrent PeerReceivers that share a single dedup
// LRU, ensuring exactly one local write and one subscriber delivery.
func TestIntegrationRingDedup(t *testing.T) {
	dir := t.TempDir()
	// hub2 cert must use CN="localhost" so TLS hostname verification passes when
	// hub1 dials "localhost:PORT".  hub1 uses CN="hub1" so hub2's allowlist check
	// can identify it by name.
	caFile, certFor, keyFor := integrationTLS(t, dir, "hub1", "localhost")

	// Hub2: hub, accepts connections from hub1 (identified by its TLS CN).
	hub2M := newHubMessenger(t, "hub2", filepath.Join(dir, "hub2"), caFile,
		certFor("localhost"), keyFor("localhost"),
		HubConfig{AllowedClients: []AllowedClient{{Name: "hub1"}}},
	)
	hub2Addr := hubLocalAddr(t, hub2M)

	// Hub1: client with TWO connections to Hub2 — creates dual forwarding paths.
	hub1Cfg := &Config{
		Name: "hub1",
		Storage: StorageConfig{
			DataDir:    filepath.Join(dir, "hub1"),
			SyncPolicy: SyncPolicyNone,
		},
		Client: ClientConfig{
			Enabled: true,
			Hubs: []ClientHubRef{
				{Addr: hub2Addr},
				{Addr: hub2Addr},
			},
		},
		TLS: TLSConfig{
			Cert: certFor("hub1"),
			Key:  keyFor("hub1"),
			CA:   caFile,
		},
	}
	hub1Cfg.ApplyDefaults()
	hub1M, err := New(hub1Cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = hub1M.Close() })

	// Subscribe on Hub2 to count deliveries.
	var deliveryCount atomic.Int32
	require.NoError(t, hub2M.Subscribe(context.Background(), "ring-events", "h2-sub",
		func(_ context.Context, _ Message) error {
			deliveryCount.Add(1)
			return nil
		},
	))

	// Allow both client connections to fully establish before publishing.
	time.Sleep(300 * time.Millisecond)

	// Publish from Hub1. Both clients[0] and clients[1] enqueue the same envelope.
	require.NoError(t, hub1M.Publish(context.Background(), "ring-events", "test.Evt",
		map[string]any{"n": 1}))

	// Wait for Hub2 to receive the message.
	deadline := time.After(3 * time.Second)
	for deliveryCount.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("Hub2 did not receive message within 3s")
		case <-time.After(20 * time.Millisecond):
		}
	}

	// Wait extra time for any duplicate delivery (via the second path) to surface.
	time.Sleep(300 * time.Millisecond)

	assert.Equal(t, int32(1), deliveryCount.Load(),
		"Hub2 should receive the message exactly once despite dual forwarding paths (dedup)")
}

// ---- TestIntegrationPolicyHotReload -----------------------------------------

// writeFedCfg marshals a federation.HubConfig to YAML and overwrites path.
func writeFedCfg(t *testing.T, path string, cfg federation.HubConfig) {
	t.Helper()
	data, err := yaml.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o644))
}

// TestIntegrationPolicyHotReload verifies that a PolicyWatcher hot-reload
// propagates new forwarding channel lists to active peer connections without
// restart or interruption.
//
//   - Hub1: hub mode, Hub2 in PeerHubs initially forwarding only "channel-a"
//   - Hub2: client mode connecting to Hub1
//   - After reload: forwarding list expands to include "channel-b"
//   - Publish on "channel-b" should be delivered to Hub2 after reload.
func TestIntegrationPolicyHotReload(t *testing.T) {
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "hub2")

	// Hub1 starts with forward policy for channel-a only.
	hub1M := newHubMessenger(t, "hub1", filepath.Join(dir, "hub1"), caFile,
		certFor("localhost"), keyFor("localhost"),
		HubConfig{
			// addr="localhost" matches Hub2's cert CN when Hub2 connects.
			PeerHubs: []PeerHubConfig{
				{Addr: "localhost", Forward: []string{"channel-a"}},
			},
		},
	)
	hub1Addr := hubLocalAddr(t, hub1M)

	// Hub2 connects to Hub1 as a client.
	hub2M := newClientMessenger(t, "hub2", dir, caFile,
		certFor("localhost"), keyFor("localhost"), hub1Addr)

	received := make(chan string, 20)
	require.NoError(t, hub2M.Subscribe(context.Background(), "channel-a", "sub",
		func(_ context.Context, msg Message) error { received <- "a"; return nil }))
	require.NoError(t, hub2M.Subscribe(context.Background(), "channel-b", "sub",
		func(_ context.Context, msg Message) error { received <- "b"; return nil }))

	// Allow connection to establish.
	time.Sleep(200 * time.Millisecond)

	// Confirm channel-a is forwarded before reload.
	require.NoError(t, hub1M.Publish(context.Background(), "channel-a", "test.Evt",
		map[string]any{"x": 1}))
	select {
	case ch := <-received:
		require.Equal(t, "a", ch)
	case <-time.After(2 * time.Second):
		t.Fatal("channel-a message not received before policy reload")
	}

	// Write initial config file and start PolicyWatcher.
	cfgPath := filepath.Join(dir, "hub1-policy.yaml")
	writeFedCfg(t, cfgPath, federation.HubConfig{
		PeerHubs: []federation.PeerHubConfig{
			{Addr: "localhost", Forward: []string{"channel-a"}},
		},
	})
	pw, err := federation.NewPolicyWatcher(cfgPath, hub1M.hub, hub1M.auditL, hub1M.log)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pw.Close() })

	// Hot-reload: add channel-b to the forward list.
	writeFedCfg(t, cfgPath, federation.HubConfig{
		PeerHubs: []federation.PeerHubConfig{
			{Addr: "localhost", Forward: []string{"channel-a", "channel-b"}},
		},
	})

	// Wait for PolicyWatcher to pick up the file change (fsnotify).
	time.Sleep(300 * time.Millisecond)

	// Publish on channel-b — should now be forwarded after reload.
	require.NoError(t, hub1M.Publish(context.Background(), "channel-b", "test.Evt",
		map[string]any{"x": 2}))
	select {
	case ch := <-received:
		assert.Equal(t, "b", ch, "channel-b should be forwarded after hot-reload")
	case <-time.After(3 * time.Second):
		t.Fatal("channel-b message not received after policy hot-reload")
	}
}

// ---- TestIntegrationBackpressure --------------------------------------------

// TestIntegrationBackpressure verifies at-least-once delivery when the receiving
// hub uses a durability policy (SyncPolicyAlways) that makes each write slower,
// creating realistic backpressure in the federation path.  All published messages
// must be received with no loss.
func TestIntegrationBackpressure(t *testing.T) {
	const numMessages = 50

	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "publisher")

	// Hub2 uses SyncPolicyAlways to slow down local writes (simulates a slow disk).
	hub2Dir := filepath.Join(dir, "hub2")
	hub2Cfg := &Config{
		Name: "hub2",
		Storage: StorageConfig{
			DataDir:    hub2Dir,
			SyncPolicy: SyncPolicyAlways,
		},
		Hub: HubConfig{
			Enabled:        true,
			ListenAddr:     "127.0.0.1:0",
			AllowedClients: []AllowedClient{{Name: "publisher"}},
		},
		TLS: TLSConfig{
			Cert: certFor("localhost"),
			Key:  keyFor("localhost"),
			CA:   caFile,
		},
	}
	hub2Cfg.ApplyDefaults()
	hub2M, err := New(hub2Cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = hub2M.Close() })
	hub2Addr := hubLocalAddr(t, hub2M)

	// Publisher connects to Hub2 as client.
	pubM := newClientMessenger(t, "publisher", dir, caFile,
		certFor("publisher"), keyFor("publisher"), hub2Addr)

	received := make(chan struct{}, numMessages)
	require.NoError(t, hub2M.Subscribe(context.Background(), "backpressure", "sub",
		func(_ context.Context, _ Message) error {
			received <- struct{}{}
			return nil
		},
	))

	// Allow connection.
	time.Sleep(200 * time.Millisecond)

	ctx := context.Background()
	for i := 0; i < numMessages; i++ {
		require.NoError(t, pubM.Publish(ctx, "backpressure", "test.Evt",
			map[string]any{"i": i}))
	}

	// All messages must arrive; timeout generous for SyncPolicyAlways.
	deadline := time.After(30 * time.Second)
	got := 0
	for got < numMessages {
		select {
		case <-received:
			got++
		case <-deadline:
			t.Fatalf("backpressure: received %d/%d messages before timeout", got, numMessages)
		}
	}
	assert.Equal(t, numMessages, got, "all messages must be delivered with no loss")
}

// ---- TestIntegrationCompactionDuringSubscribers -----------------------------

// TestIntegrationCompactionDuringSubscribers verifies that triggering compaction
// while an active subscriber is mid-stream does not cause message loss or
// corrupt the subscriber's offset tracking.
func TestIntegrationCompactionDuringSubscribers(t *testing.T) {
	const (
		phase1Count = 100 // messages published before compaction
		phase2Count = 50  // messages published after compaction
		total       = phase1Count + phase2Count
	)

	dir := t.TempDir()
	// Very low compaction threshold so the compactor will delete sealed segments.
	cfg := &Config{
		Name: "compact-test",
		Storage: StorageConfig{
			DataDir:               dir,
			SyncPolicy:            SyncPolicyNone,
			CompactionThresholdMB: 1, // 1 MB; in practice will never trigger automatically but lets MaybeCompact run
		},
	}
	cfg.ApplyDefaults()
	m, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	// Use struct{} to avoid JSON round-trip type issues: JSON numbers in
	// map[string]any are decoded as float64, not int.
	received := make(chan struct{}, total+10)
	require.NoError(t, m.Subscribe(context.Background(), "compact-ch", "sub",
		func(_ context.Context, _ Message) error {
			received <- struct{}{}
			return nil
		},
	))

	ctx := context.Background()

	// Phase 1: publish and fully consume the first batch.
	for i := 0; i < phase1Count; i++ {
		require.NoError(t, m.Publish(ctx, "compact-ch", "test.Evt", map[string]any{"i": i}))
	}

	got := 0
	deadline := time.After(5 * time.Second)
	for got < phase1Count {
		select {
		case <-received:
			got++
		case <-deadline:
			t.Fatalf("phase 1: received %d/%d before timeout", got, phase1Count)
		}
	}

	// Trigger compaction manually while the subscriber is still active.
	m.runCompaction()

	// Phase 2: publish more messages after compaction.
	for i := 0; i < phase2Count; i++ {
		require.NoError(t, m.Publish(ctx, "compact-ch", "test.Evt", map[string]any{"i": i}))
	}

	// Collect phase 2 messages.
	deadline2 := time.After(5 * time.Second)
	for got < total {
		select {
		case <-received:
			got++
		case <-deadline2:
			t.Fatalf("phase 2: received %d/%d before timeout", got, total)
		}
	}

	assert.Equal(t, total, got, "all messages before and after compaction must be delivered")
	assert.Empty(t, received, "no extra deliveries expected")
}

// Compile-time assertion: ensure the integration test file imports are used.
var _ = fmt.Sprintf
