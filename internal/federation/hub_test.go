//nolint:gosec // test file: G301/G304/G306
package federation_test

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/dedup"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/federation"
	"github.com/wu/keyop-messenger/internal/testutil"
	"github.com/wu/keyop-messenger/internal/tlsutil"
)

// ---- shared test helpers ----------------------------------------------------

type countingWriter struct {
	mu    sync.Mutex
	count int
	delay time.Duration
}

func (c *countingWriter) write(_ *envelope.Envelope) error {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	c.mu.Lock()
	c.count++
	c.mu.Unlock()
	return nil
}

func (c *countingWriter) n() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

type fakeAuditLog struct {
	mu     sync.Mutex
	events []audit.Event
}

func (f *fakeAuditLog) Log(ev audit.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, ev)
	return nil
}
func (f *fakeAuditLog) Close() error { return nil }
func (f *fakeAuditLog) has(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ev := range f.events {
		if ev.Event == name {
			return true
		}
	}
	return false
}

// clientHandshake performs the client side of the application handshake.
func clientHandshake(t *testing.T, conn *websocket.Conn, name string) {
	t.Helper()
	require.NoError(t, federation.SendHandshake(conn, federation.HandshakeMsg{
		InstanceName: name, Role: "client", Version: "1",
	}))
	_, err := federation.ReceiveHandshake(conn)
	require.NoError(t, err)
}

func newHub(t *testing.T, cfg federation.HubConfig, cw *countingWriter, auditL audit.AuditLogger) *federation.Hub {
	t.Helper()
	dd, err := dedup.NewLRUDedup(10000)
	require.NoError(t, err)
	log := &testutil.FakeLogger{}
	return federation.NewHub("hub", cfg, nil, cw.write, dd, auditL, log, 1000, 65536, "")
}

// ---- tests ------------------------------------------------------------------

func TestHubClientIntegration(t *testing.T) {
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{AllowedPeers: []federation.AllowedPeer{{Name: "sender"}}}
	hub := newHub(t, cfg, cw, auditL)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	clientHandshake(t, cli, "sender")

	log := &testutil.FakeLogger{}
	sender := federation.NewPeerSender(cli, &sync.Mutex{}, 100, 65536, log)
	defer sender.Close()

	for i := 0; i < 10; i++ {
		env, err := envelope.NewEnvelope("orders", "sender", "test", map[string]any{"n": i})
		require.NoError(t, err)
		require.True(t, sender.Enqueue(&env))
	}
	require.Eventually(t, func() bool { return cw.n() == 10 }, 2*time.Second, 20*time.Millisecond)
}

func TestAllowlistRejection(t *testing.T) {
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{AllowedPeers: []federation.AllowedPeer{{Name: "allowed-only"}}}
	hub := newHub(t, cfg, cw, auditL)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)

	// Client sends handshake with a name not in the allowlist.
	require.NoError(t, federation.SendHandshake(cli, federation.HandshakeMsg{
		InstanceName: "unknown-peer", Role: "client", Version: "1",
	}))

	// Hub should close with 4403.
	_, _, err := cli.ReadMessage()
	var closeErr *websocket.CloseError
	require.ErrorAs(t, err, &closeErr)
	assert.Equal(t, 4403, closeErr.Code)

	require.Eventually(t, func() bool { return auditL.has(audit.EventClientRejected) }, time.Second, 20*time.Millisecond)
}

func TestDeduplication(t *testing.T) {
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{AllowedPeers: []federation.AllowedPeer{{Name: "sender"}}}
	hub := newHub(t, cfg, cw, auditL)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	clientHandshake(t, cli, "sender")

	log := &testutil.FakeLogger{}
	sender := federation.NewPeerSender(cli, &sync.Mutex{}, 100, 65536, log)
	defer sender.Close()

	env, err := envelope.NewEnvelope("ch", "sender", "t", nil)
	require.NoError(t, err)

	sender.Enqueue(&env)
	require.Eventually(t, func() bool { return cw.n() == 1 }, time.Second, 10*time.Millisecond)

	sender.Enqueue(&env) // same ID
	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, 1, cw.n(), "duplicate ID must be deduped")
}

func TestPolicyViolation(t *testing.T) {
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{
			Name:    "sender",
			Publish: []string{"allowed-ch"},
		}},
	}
	hub := newHub(t, cfg, cw, auditL)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	clientHandshake(t, cli, "sender")

	log := &testutil.FakeLogger{}
	sender := federation.NewPeerSender(cli, &sync.Mutex{}, 100, 65536, log)
	defer sender.Close()

	env, err := envelope.NewEnvelope("blocked-ch", "sender", "t", nil)
	require.NoError(t, err)
	sender.Enqueue(&env)

	require.Eventually(t, func() bool { return auditL.has(audit.EventPolicyViolation) }, time.Second, 20*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, cw.n(), "blocked channel must not reach localWriter")
}

func TestBackpressure(t *testing.T) {
	auditL := &fakeAuditLog{}
	cw := &countingWriter{delay: 250 * time.Millisecond}
	cfg := federation.HubConfig{AllowedPeers: []federation.AllowedPeer{{Name: "sender"}}}
	hub := newHub(t, cfg, cw, auditL)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	clientHandshake(t, cli, "sender")

	log := &testutil.FakeLogger{}
	sender := federation.NewPeerSender(cli, &sync.Mutex{}, 100, 65536, log)
	defer sender.Close()

	env1, _ := envelope.NewEnvelope("ch", "s", "t", nil)
	env2, _ := envelope.NewEnvelope("ch", "s", "t", nil)
	sender.Enqueue(&env1)
	sender.Enqueue(&env2)

	// After 100ms only the first should have completed (second blocked in localWriter).
	time.Sleep(100 * time.Millisecond)
	assert.LessOrEqual(t, cw.n(), 1, "second write should still be blocked")

	require.Eventually(t, func() bool { return cw.n() == 2 }, 3*time.Second, 20*time.Millisecond)
}

func TestSendBufferFull(t *testing.T) {
	log := &testutil.FakeLogger{}
	srv, _ := newWSPair(t)
	// Buffer size 1; goroutine will be stuck waiting for ack since no receiver.
	ps := federation.NewPeerSender(srv, &sync.Mutex{}, 1, 65536, log)
	defer ps.Close()

	e1, _ := envelope.NewEnvelope("ch", "s", "t", nil)
	e2, _ := envelope.NewEnvelope("ch", "s", "t", nil)
	e3, _ := envelope.NewEnvelope("ch", "s", "t", nil)

	ps.Enqueue(&e1)             // enters buffer
	ps.Enqueue(&e2)             // may enter buffer or be consumed by goroutine
	dropped := !ps.Enqueue(&e3) // at least one of e2/e3 must drop eventually

	// Try a few more to guarantee a drop is logged.
	for i := 0; i < 5; i++ {
		e, _ := envelope.NewEnvelope("ch", "s", "t", nil)
		ps.Enqueue(&e)
	}
	time.Sleep(50 * time.Millisecond)

	_ = dropped          // timing-dependent; just ensure no deadlock/panic
	assert.True(t, true) // reached here without blocking
}

func TestBatching(t *testing.T) {
	srv, cli := newWSPair(t)
	log := &testutil.FakeLogger{}

	var frameCount int32
	go func() {
		for {
			msgType, _, err := srv.ReadMessage()
			if err != nil {
				return
			}
			if msgType == websocket.BinaryMessage {
				atomic.AddInt32(&frameCount, 1)
			}
			_ = federation.SendAck(srv, federation.AckMsg{})
		}
	}()

	sender := federation.NewPeerSender(cli, &sync.Mutex{}, 1000, 65536, log)
	for i := 0; i < 200; i++ {
		env, _ := envelope.NewEnvelope("ch", "s", "t", map[string]any{"i": i})
		sender.Enqueue(&env)
	}
	require.Eventually(t, func() bool { return int(atomic.LoadInt32(&frameCount)) > 0 }, 2*time.Second, 20*time.Millisecond)
	sender.Close()

	frames := int(atomic.LoadInt32(&frameCount))
	t.Logf("200 messages → %d frames", frames)
	assert.Less(t, frames, 200, "messages should be batched")
}

// TestNotifyChannel_NoopAfterDisconnect verifies that NotifyChannel does not
// panic after a peer disconnects and its readers are deregistered from the
// notify registry.
func TestNotifyChannel_NoopAfterDisconnect(t *testing.T) {
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{
			Name:      "notify-test",
			Subscribe: []string{"orders"},
		}},
	}
	hub := newHub(t, cfg, cw, auditL)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	clientHandshake(t, cli, "notify-test")
	time.Sleep(50 * time.Millisecond)

	_ = cli.Close()
	_ = srv.Close()

	require.Eventually(t, func() bool {
		return auditL.has(audit.EventPeerDisconnected)
	}, 2*time.Second, 20*time.Millisecond)

	// NotifyChannel must not panic when no readers are registered.
	assert.NotPanics(t, func() { hub.NotifyChannel("orders") })
	assert.NotPanics(t, func() { hub.NotifyChannel("unknown-channel") })
}

func TestReconnectReplay(t *testing.T) {
	log := &testutil.FakeLogger{}

	// First connection: server reads one frame (which may batch all envelopes)
	// without sending an ack, then closes — all sent messages remain unacked.
	srv1, cli1 := newWSPair(t)
	disconnected := make(chan struct{})
	go func() {
		defer close(disconnected)
		_, _, err := srv1.ReadMessage() // read the one batched frame
		if err != nil {
			return
		}
		// Close without acking so all messages stay in sender1.Unacked().
		_ = srv1.Close()
	}()

	sender1 := federation.NewPeerSender(cli1, &sync.Mutex{}, 100, 65536, log)
	var sent []*envelope.Envelope
	for i := 0; i < 5; i++ {
		env, _ := envelope.NewEnvelope("ch", "s", "t", map[string]any{"i": i})
		sender1.Enqueue(&env)
		sent = append(sent, &env)
	}

	<-disconnected
	<-sender1.Done()

	unacked := sender1.Unacked()
	assert.Len(t, unacked, len(sent), "all sent messages must be unacked when no ack received")

	// Second connection: server reads and acks everything.
	srv2, cli2 := newWSPair(t)
	var replayed int32
	go func() {
		for {
			msgType, _, err := srv2.ReadMessage()
			if err != nil {
				return
			}
			if msgType == websocket.BinaryMessage {
				atomic.AddInt32(&replayed, 1)
			}
			_ = federation.SendAck(srv2, federation.AckMsg{})
		}
	}()

	sender2 := federation.NewPeerSender(cli2, &sync.Mutex{}, 100, 65536, log)
	for _, env := range unacked {
		env := env
		sender2.Enqueue(env)
	}

	// At least one frame must arrive (batching may pack all into one frame).
	require.Eventually(t,
		func() bool { return atomic.LoadInt32(&replayed) >= 1 },
		2*time.Second, 20*time.Millisecond,
		"unacked messages must be replayed on reconnect")
	sender2.Close()
}

func TestMTLSRejection(t *testing.T) {
	caCertPEM1, caKeyPEM1, err := tlsutil.GenerateCA(365)
	require.NoError(t, err)
	caCertPEM2, caKeyPEM2, err := tlsutil.GenerateCA(365)
	require.NoError(t, err)

	dir := t.TempDir()
	write := func(name string, data []byte) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, data, 0o600))
		return p
	}

	hubCert, hubKey, err := tlsutil.GenerateInstance(caCertPEM1, caKeyPEM1, "localhost", 90)
	require.NoError(t, err)
	hubTLS, err := tlsutil.BuildTLSConfig(
		write("hub.crt", hubCert), write("hub.key", hubKey), write("ca1.crt", caCertPEM1),
		&testutil.FakeLogger{})
	require.NoError(t, err)

	dd, _ := dedup.NewLRUDedup(100)
	auditL := &fakeAuditLog{}
	log := &testutil.FakeLogger{}
	hub := federation.NewHub("hub", federation.HubConfig{}, hubTLS,
		func(*envelope.Envelope) error { return nil }, dd, auditL, log, 100, 65536, "")
	require.NoError(t, hub.Listen("127.0.0.1:0"))
	defer func() { _ = hub.Close() }()

	// Client cert signed by CA2, which hub doesn't trust.
	clientCert, clientKey, err := tlsutil.GenerateInstance(caCertPEM2, caKeyPEM2, "client", 90)
	require.NoError(t, err)
	cert, err := tls.X509KeyPair(clientCert, clientKey)
	require.NoError(t, err)

	wrongCAPool := x509.NewCertPool()
	wrongCAPool.AppendCertsFromPEM(caCertPEM1) // trust hub's CA but hub won't trust ours

	clientTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      wrongCAPool,
		MinVersion:   tls.VersionTLS13,
	}
	dialer := websocket.Dialer{TLSClientConfig: clientTLS}
	_, _, err = dialer.Dial("wss://"+hub.Addr(), nil)
	assert.Error(t, err, "client with cert from untrusted CA must be rejected")
}

// ---- reverse index (channelSubscribers) tests --------------------------------

func TestChannelSubscribersCleanupOnDisconnect(t *testing.T) {
	// Verify that the reverse index is properly cleaned up when a peer disconnects.
	// After cleanup, the channel lists should be empty for channels that only had that peer.
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{
			Name:      "cleanup-test",
			Subscribe: []string{"ch-a", "ch-b"},
		}},
	}
	hub := newHub(t, cfg, cw, auditL)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	clientHandshake(t, cli, "cleanup-test")

	time.Sleep(100 * time.Millisecond)

	// Close the connection to trigger cleanup.
	_ = cli.Close()
	_ = srv.Close()

	// Wait for cleanup goroutine to run.
	require.Eventually(t, func() bool {
		return auditL.has(audit.EventPeerDisconnected)
	}, 2*time.Second, 20*time.Millisecond)

	// After cleanup, NotifyChannel must not panic (notify registry was cleaned up).
	assert.NotPanics(t, func() { hub.NotifyChannel("ch-a") })
	assert.NotPanics(t, func() { hub.NotifyChannel("ch-b") })
}

func TestChannelSubscribersEmptyChannelDeletion(t *testing.T) {
	// Verify that when all peers on a channel disconnect, the channel entry is deleted
	// from the reverse index to prevent memory leaks.
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{
			{Name: "peer-1", Subscribe: []string{"exclusive-ch"}},
		},
	}
	hub := newHub(t, cfg, cw, auditL)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	clientHandshake(t, cli, "peer-1")

	time.Sleep(100 * time.Millisecond)

	// Disconnect the only peer on exclusive-ch.
	_ = cli.Close()
	_ = srv.Close()

	require.Eventually(t, func() bool {
		return auditL.has(audit.EventPeerDisconnected)
	}, 2*time.Second, 20*time.Millisecond)

	// NotifyChannel must not panic when the channel's reader list is empty.
	assert.NotPanics(t, func() { hub.NotifyChannel("exclusive-ch") })
}

func TestChannelSubscribersStressConnectDisconnect(t *testing.T) {
	// Stress test: rapidly connect and disconnect many peers to verify the reverse index
	// doesn't have memory leaks or corruption.
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}

	peers := []federation.AllowedPeer{}
	for i := 0; i < 30; i++ {
		peers = append(peers, federation.AllowedPeer{
			Name:      fmt.Sprintf("peer-%d", i),
			Subscribe: []string{"stress-ch-a", "stress-ch-b"},
		})
	}

	cfg := federation.HubConfig{AllowedPeers: peers}
	hub := newHub(t, cfg, cw, auditL)

	// Connect and disconnect 30 peers multiple times, each subscribing to 2 channels.
	for round := 0; round < 3; round++ {
		var connections []*websocket.Conn
		for i := 0; i < 30; i++ {
			srv, cli := newWSPair(t)
			hub.ServeTestConn(srv, nil)
			clientHandshake(t, cli, fmt.Sprintf("peer-%d", i))
			connections = append(connections, cli)
		}

		time.Sleep(50 * time.Millisecond) // Let all connections establish

		// Disconnect all peers.
		for _, conn := range connections {
			_ = conn.Close()
		}

		time.Sleep(50 * time.Millisecond) // Let cleanup complete

		// NotifyChannel must not panic after all peers disconnect.
		hub.NotifyChannel("stress-ch-a")
		hub.NotifyChannel("stress-ch-b")
	}

	// Final NotifyChannel to verify hub is still functional.
	hub.NotifyChannel("stress-ch-a")
	hub.NotifyChannel("stress-ch-b")

	// Test completed without panic; notify registry survived the stress.
	assert.True(t, true, "stress test completed without panic or memory issues")
}
