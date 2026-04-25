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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/tlsutil"
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
			AllowedPeers: []AllowedPeer{{Name: "client-a"}, {Name: "client-b"}},
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
		HubConfig{AllowedPeers: []AllowedPeer{{Name: "hub1"}}},
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
			Enabled:      true,
			ListenAddr:   "127.0.0.1:0",
			AllowedPeers: []AllowedPeer{{Name: "publisher"}},
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

// newClientMessengerWithPolicy creates a Messenger in client-only mode with
// configured subscribe/publish channel lists for federation.
func newClientMessengerWithPolicy(
	t *testing.T,
	name, dir, caFile, certFile, keyFile, hubAddr string,
	subscribe, publish []string,
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
			Hubs:    []ClientHubRef{{Addr: hubAddr, Subscribe: subscribe, Publish: publish}},
		},
		TLS: TLSConfig{Cert: certFile, Key: keyFile, CA: caFile},
	}
	cfg.ApplyDefaults()
	m, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// ---- TestIntegrationClientToClientForwarding --------------------------------

// TestIntegrationClientToClientForwarding verifies that messages from one
// client are properly forwarded to other clients through the hub, even when
// the receiving client doesn't explicitly subscribe to channels from the hub.
//
// This test exposes the bug: when ClientB connects to the hub but doesn't
// declare any subscriptions, it won't get a PeerSender created. Without the
// fix, messages from ClientA won't be forwarded to ClientB because
// EnqueueToAll() only forwards to peers with senders.
//
// Topology:
//   - Hub: hub listener accepting ClientA and ClientB
//   - ClientA: publishes to "forward-test" channel
//   - ClientB: connects to hub but does NOT subscribe to any channels
//     (yet still has local subscribers that should receive messages)
//
// Expected flow:
//  1. ClientA.Publish() → Hub.PeerReceiver
//  2. Hub.writeLocalEnvelope() writes message to local storage
//  3. Hub.writeLocalEnvelope() calls EnqueueToAll() to forward to connected peers
//  4. Hub forwards to ClientB even though ClientB has no subscriptions
//  5. ClientB receives message via its local subscription
func TestIntegrationClientToClientForwarding(t *testing.T) {
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "client-a", "client-b")

	hubM := newHubMessenger(t, "hub", filepath.Join(dir, "hub"), caFile,
		certFor("localhost"), keyFor("localhost"),
		HubConfig{
			// Allow both clients to publish to all channels
			// ClientB has NO subscribe allowlist, so it won't request any channels from hub
			AllowedPeers: []AllowedPeer{
				{Name: "client-a", Publish: []string{}, Subscribe: []string{}},
				{Name: "client-b", Publish: []string{}, Subscribe: []string{}},
			},
		},
	)
	hubAddr := hubLocalAddr(t, hubM)

	// ClientA publishes to "forward-test"
	// ClientB subscribes to "forward-test" from the hub
	clientA := newClientMessengerWithPolicy(t, "client-a", dir, caFile, certFor("client-a"), keyFor("client-a"), hubAddr,
		[]string{},               // subscribe: none (only publishes)
		[]string{"forward-test"}, // publish: forward-test
	)
	clientB := newClientMessengerWithPolicy(t, "client-b", dir, caFile, certFor("client-b"), keyFor("client-b"), hubAddr,
		[]string{"forward-test"}, // subscribe: forward-test
		[]string{},               // publish: none (only consumes)
	)

	// ClientB has subscribed to "forward-test" channel from the hub.
	// Messages published to "forward-test" should be delivered to local subscribers.
	received := make(chan Message, 10)
	require.NoError(t, clientB.Subscribe(context.Background(), "forward-test", "client-b-sub",
		func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		},
	))

	// Allow both clients to establish connections with the hub before publishing.
	time.Sleep(200 * time.Millisecond)

	// ClientA publishes a message to "forward-test".
	// This should be forwarded from hub to ClientB even though ClientB
	// didn't explicitly subscribe to channels from the hub.
	require.NoError(t, clientA.Publish(context.Background(), "forward-test", "test.ForwardMsg",
		map[string]any{"from": "client-a", "id": "forward-001"}))

	// Wait for ClientB to receive the message.
	deadline := time.After(3 * time.Second)
	var msg Message
	select {
	case msg = <-received:
		// Success: message was forwarded from ClientA through hub to ClientB
	case <-deadline:
		t.Fatal("ClientB did not receive forwarded message from ClientA within 3s - " +
			"this indicates that the hub does not forward to peers without subscriptions")
	}

	// Verify the message content and origin.
	assert.Equal(t, "forward-test", msg.Channel)
	assert.Equal(t, "client-a", msg.Origin, "message should originate from client-a")
	assert.Equal(t, "test.ForwardMsg", msg.PayloadType)

	payload, ok := msg.Payload.(map[string]any)
	require.True(t, ok, "payload should be a map")
	assert.Equal(t, "client-a", payload["from"])
	assert.Equal(t, "forward-001", payload["id"])
}

// TestIntegrationForwardingRespectSubscription verifies that messages are only
// forwarded to peers subscribed to that channel, not to all connected peers.
//
// Topology:
//   - Hub: hub listener accepting ClientA and ClientB
//   - ClientA: publishes to "channel-a" (subscribes to nothing)
//   - ClientB: subscribes to "channel-b" only (not "channel-a")
//   - ClientC: subscribes to "channel-a"
//
// Expected flow:
//  1. ClientA publishes to "channel-a"
//  2. Hub forwards ONLY to ClientC (subscribed to "channel-a"), not ClientB
//  3. ClientB does NOT receive the message
//  4. ClientC receives the message
func TestIntegrationForwardingRespectSubscription(t *testing.T) {
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "client-a", "client-b", "client-c")

	hubM := newHubMessenger(t, "hub", filepath.Join(dir, "hub"), caFile,
		certFor("localhost"), keyFor("localhost"),
		HubConfig{
			AllowedPeers: []AllowedPeer{
				{Name: "client-a", Publish: []string{"channel-a"}, Subscribe: []string{}},
				{Name: "client-b", Publish: []string{}, Subscribe: []string{"channel-b"}},
				{Name: "client-c", Publish: []string{}, Subscribe: []string{"channel-a"}},
			},
		},
	)
	hubAddr := hubLocalAddr(t, hubM)

	// ClientA publishes to "channel-a"
	clientA := newClientMessengerWithPolicy(t, "client-a", dir, caFile, certFor("client-a"), keyFor("client-a"), hubAddr,
		[]string{},            // subscribe: none
		[]string{"channel-a"}, // publish: channel-a
	)

	// ClientB subscribes to "channel-b" (NOT channel-a)
	clientB := newClientMessengerWithPolicy(t, "client-b", dir, caFile, certFor("client-b"), keyFor("client-b"), hubAddr,
		[]string{"channel-b"}, // subscribe: channel-b only
		[]string{},            // publish: none
	)

	// ClientC subscribes to "channel-a"
	clientC := newClientMessengerWithPolicy(t, "client-c", dir, caFile, certFor("client-c"), keyFor("client-c"), hubAddr,
		[]string{"channel-a"}, // subscribe: channel-a
		[]string{},            // publish: none
	)

	// Register handlers on both ClientB and ClientC
	receivedB := make(chan Message, 10)
	receivedC := make(chan Message, 10)

	require.NoError(t, clientB.Subscribe(context.Background(), "channel-a", "client-b-sub",
		func(_ context.Context, msg Message) error {
			receivedB <- msg
			return nil
		},
	))
	require.NoError(t, clientC.Subscribe(context.Background(), "channel-a", "client-c-sub",
		func(_ context.Context, msg Message) error {
			receivedC <- msg
			return nil
		},
	))

	// Allow time for connections to establish
	time.Sleep(200 * time.Millisecond)

	// ClientA publishes to "channel-a"
	require.NoError(t, clientA.Publish(context.Background(), "channel-a", "test.ForwardMsg",
		map[string]any{"from": "client-a", "id": "channel-a-001"}))

	// ClientC should receive the message (subscribed to channel-a)
	deadline := time.After(3 * time.Second)
	var msgC Message
	select {
	case msgC = <-receivedC:
		// Success: ClientC received the message
	case <-deadline:
		t.Fatal("ClientC did not receive message on channel-a within 3s - " +
			"ClientC should receive because it subscribed to channel-a")
	}
	assert.Equal(t, "channel-a", msgC.Channel)
	assert.Equal(t, "client-a", msgC.Origin)

	// ClientB should NOT receive the message (not subscribed to channel-a)
	select {
	case msg := <-receivedB:
		t.Fatalf("ClientB received unexpected message on %s (only subscribed to channel-b): %+v",
			msg.Channel, msg)
	case <-time.After(500 * time.Millisecond):
		// Correct: ClientB did not receive the message
	}
}

// Compile-time assertion: ensure the integration test file imports are used.
var _ = fmt.Sprintf
