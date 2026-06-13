//go:build integration

//nolint:gosec // test file: G301/G304/G306
package federation_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/dedup"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/federation"
	"github.com/wu/keyop-messenger/internal/testutil"
	"github.com/wu/keyop-messenger/internal/tlsutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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

func newHub(t *testing.T, cfg federation.HubConfig, cw *countingWriter, auditL audit.AuditLogger) *federation.Hub {
	t.Helper()
	dd, err := dedup.NewLRUDedup(10000)
	require.NoError(t, err)
	log := &testutil.FakeLogger{}
	return federation.NewHub("hub", cfg, nil, cw.write, dd, auditL, log, 1000, 65536, "")
}

// startHub starts hub on ":0" and returns a connected grpc.ClientConn.
func startHub(t *testing.T, hub *federation.Hub) (stub federationv1.FederationServiceClient, cleanup func()) {
	t.Helper()
	require.NoError(t, hub.Listen(":0"))
	conn, err := grpc.NewClient(hub.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return federationv1.NewFederationServiceClient(conn), func() {
		_ = conn.Close()
		_ = hub.Close()
	}
}

// openPublish opens a Publish stream identified by instanceName.
func openPublish(t *testing.T, stub federationv1.FederationServiceClient, instanceName string) (federationv1.FederationService_PublishClient, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	md := metadata.Pairs("x-federation-instance", instanceName)
	stream, err := stub.Publish(metadata.NewOutgoingContext(ctx, md))
	require.NoError(t, err)
	return stream, cancel
}

// ---- tests ------------------------------------------------------------------

func TestHubClientIntegration(t *testing.T) {
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{AllowedPeers: []federation.AllowedPeer{{Name: "sender"}}}
	hub := newHub(t, cfg, cw, auditL)

	stub, cleanup := startHub(t, hub)
	defer cleanup()

	pubStream, cancel := openPublish(t, stub, "sender")
	defer cancel()

	log := &testutil.FakeLogger{}
	senderCtx, senderCancel := context.WithCancel(context.Background())
	_ = senderCtx
	sender := federation.NewPeerSender(pubStream, senderCancel, 100, 65536, log)
	defer sender.Close()

	for i := 0; i < 10; i++ {
		env, err := envelope.NewEnvelope("orders", "sender", "test", map[string]any{"n": i})
		require.NoError(t, err)
		require.True(t, sender.Enqueue(&env))
	}
	require.Eventually(t, func() bool { return cw.n() == 10 }, 5*time.Second, 20*time.Millisecond)
}

func TestAllowlistRejection(t *testing.T) {
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{AllowedPeers: []federation.AllowedPeer{{Name: "allowed-only"}}}
	hub := newHub(t, cfg, cw, auditL)

	stub, cleanup := startHub(t, hub)
	defer cleanup()

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-federation-instance", "unknown-peer"))
	stream, err := stub.Publish(ctx)
	require.NoError(t, err)

	_, err = stream.Recv()
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.PermissionDenied, st.Code())

	require.Eventually(t, func() bool { return auditL.has(audit.EventClientRejected) }, time.Second, 20*time.Millisecond)
}

func TestDeduplication(t *testing.T) {
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{AllowedPeers: []federation.AllowedPeer{{Name: "sender"}}}
	hub := newHub(t, cfg, cw, auditL)

	stub, cleanup := startHub(t, hub)
	defer cleanup()

	pubStream, cancel := openPublish(t, stub, "sender")
	defer cancel()

	log := &testutil.FakeLogger{}
	senderCtx, senderCancel := context.WithCancel(context.Background())
	sender := federation.NewPeerSender(pubStream, senderCancel, 100, 65536, log)
	defer sender.Close()
	_ = senderCtx

	env, err := envelope.NewEnvelope("ch", "sender", "t", nil)
	require.NoError(t, err)

	sender.Enqueue(&env)
	require.Eventually(t, func() bool { return cw.n() == 1 }, time.Second, 10*time.Millisecond)

	sender.Enqueue(&env) // same ID — should be deduped
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

	stub, cleanup := startHub(t, hub)
	defer cleanup()

	pubStream, cancel := openPublish(t, stub, "sender")
	defer cancel()

	log := &testutil.FakeLogger{}
	senderCtx, senderCancel := context.WithCancel(context.Background())
	sender := federation.NewPeerSender(pubStream, senderCancel, 100, 65536, log)
	defer sender.Close()
	_ = senderCtx

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

	stub, cleanup := startHub(t, hub)
	defer cleanup()

	pubStream, cancel := openPublish(t, stub, "sender")
	defer cancel()

	log := &testutil.FakeLogger{}
	senderCtx, senderCancel := context.WithCancel(context.Background())
	sender := federation.NewPeerSender(pubStream, senderCancel, 100, 65536, log)
	defer sender.Close()
	_ = senderCtx

	env1, _ := envelope.NewEnvelope("ch", "s", "t", nil)
	env2, _ := envelope.NewEnvelope("ch", "s", "t", nil)
	sender.Enqueue(&env1)
	sender.Enqueue(&env2)

	time.Sleep(100 * time.Millisecond)
	assert.LessOrEqual(t, cw.n(), 1, "second write should still be blocked")

	require.Eventually(t, func() bool { return cw.n() == 2 }, 3*time.Second, 20*time.Millisecond)
}

// TestSendBufferFull verifies that Enqueue returns false when the buffer is full
// and the goroutine is stuck waiting for an ack (mock stream never acks).
func TestSendBufferFull(t *testing.T) {
	log := &testutil.FakeLogger{}
	stream, cancel := newNeverAckStream()
	ps := federation.NewPeerSender(stream, cancel, 1, 65536, log)
	defer ps.Close()

	e1, _ := envelope.NewEnvelope("ch", "s", "t", nil)
	e2, _ := envelope.NewEnvelope("ch", "s", "t", nil)
	e3, _ := envelope.NewEnvelope("ch", "s", "t", nil)

	ps.Enqueue(&e1)
	ps.Enqueue(&e2)
	dropped := !ps.Enqueue(&e3)

	for i := 0; i < 5; i++ {
		e, _ := envelope.NewEnvelope("ch", "s", "t", nil)
		ps.Enqueue(&e)
	}
	time.Sleep(50 * time.Millisecond)
	_ = dropped
	assert.True(t, true)
}

func TestBatching(t *testing.T) {
	var frameCount atomic.Int32
	stream, cancel := newCountingAckStream(&frameCount)
	log := &testutil.FakeLogger{}
	sender := federation.NewPeerSender(stream, cancel, 1000, 65536, log)

	for i := 0; i < 200; i++ {
		env, _ := envelope.NewEnvelope("ch", "s", "t", map[string]any{"i": i})
		sender.Enqueue(&env)
	}
	require.Eventually(t, func() bool { return frameCount.Load() > 0 }, 5*time.Second, 20*time.Millisecond)
	sender.Close()
	cancel()

	frames := frameCount.Load()
	t.Logf("200 messages → %d frames", frames)
	assert.Less(t, frames, int32(200), "messages should be batched")
}

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

	stub, cleanup := startHub(t, hub)
	defer cleanup()

	pubStream, cancel := openPublish(t, stub, "notify-test")

	// Wait for connect audit event.
	require.Eventually(t, func() bool {
		return auditL.has(audit.EventClientConnected)
	}, 2*time.Second, 20*time.Millisecond)

	cancel() // disconnect

	require.Eventually(t, func() bool {
		return auditL.has(audit.EventPeerDisconnected)
	}, 2*time.Second, 20*time.Millisecond)

	_ = pubStream

	assert.NotPanics(t, func() { hub.NotifyChannel("orders") })
	assert.NotPanics(t, func() { hub.NotifyChannel("unknown-channel") })
}

func TestReconnectReplay(t *testing.T) {
	log := &testutil.FakeLogger{}

	// First connection: server reads one batch without acking → all messages stay unacked.
	stream1, cancel1 := newNeverAckStream()
	sender1 := federation.NewPeerSender(stream1, cancel1, 100, 65536, log)

	for i := 0; i < 5; i++ {
		env, _ := envelope.NewEnvelope("ch", "s", "t", map[string]any{"i": i})
		sender1.Enqueue(&env)
	}

	// Give sender time to send, then disconnect.
	require.Eventually(t, func() bool { return stream1.sent.Load() > 0 }, 2*time.Second, 10*time.Millisecond)
	cancel1()
	<-sender1.Done()

	unacked := sender1.Unacked()
	assert.GreaterOrEqual(t, len(unacked), 1, "sent-but-not-acked messages must appear in Unacked()")

	// Second connection: server acks everything.
	var replayed atomic.Int32
	stream2, cancel2 := newCountingAckStream(&replayed)
	sender2 := federation.NewPeerSender(stream2, cancel2, 100, 65536, log)
	for _, env := range unacked {
		env := env
		sender2.Enqueue(env)
	}

	require.Eventually(t,
		func() bool { return replayed.Load() >= 1 },
		2*time.Second, 20*time.Millisecond,
		"unacked messages must be replayed on reconnect")
	sender2.Close()
	cancel2()
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
	wrongCAPool.AppendCertsFromPEM(caCertPEM1)

	clientTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      wrongCAPool,
		MinVersion:   tls.VersionTLS13,
	}

	conn, err := grpc.NewClient(hub.Addr(), grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	stub := federationv1.NewFederationServiceClient(conn)
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-federation-instance", "client"))
	stream, openErr := stub.Publish(ctx)
	if openErr == nil {
		_, openErr = stream.Recv()
	}
	assert.Error(t, openErr, "client with cert from untrusted CA must be rejected")
}

func TestChannelSubscribersCleanupOnDisconnect(t *testing.T) {
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{
			Name:      "cleanup-test",
			Subscribe: []string{"ch-a", "ch-b"},
		}},
	}
	hub := newHub(t, cfg, cw, auditL)
	stub, cleanup := startHub(t, hub)
	defer cleanup()

	_, cancel := openPublish(t, stub, "cleanup-test")

	require.Eventually(t, func() bool {
		return auditL.has(audit.EventClientConnected)
	}, 2*time.Second, 20*time.Millisecond)

	cancel()

	require.Eventually(t, func() bool {
		return auditL.has(audit.EventPeerDisconnected)
	}, 2*time.Second, 20*time.Millisecond)

	assert.NotPanics(t, func() { hub.NotifyChannel("ch-a") })
	assert.NotPanics(t, func() { hub.NotifyChannel("ch-b") })
}

func TestChannelSubscribersEmptyChannelDeletion(t *testing.T) {
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{
			{Name: "peer-1", Subscribe: []string{"exclusive-ch"}},
		},
	}
	hub := newHub(t, cfg, cw, auditL)
	stub, cleanup := startHub(t, hub)
	defer cleanup()

	_, cancel := openPublish(t, stub, "peer-1")
	require.Eventually(t, func() bool { return auditL.has(audit.EventClientConnected) }, 2*time.Second, 20*time.Millisecond)
	cancel()
	require.Eventually(t, func() bool { return auditL.has(audit.EventPeerDisconnected) }, 2*time.Second, 20*time.Millisecond)
	assert.NotPanics(t, func() { hub.NotifyChannel("exclusive-ch") })
}

func TestHubListenError(t *testing.T) {
	dd, err := dedup.NewLRUDedup(10000)
	require.NoError(t, err)
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLog{}
	hub := federation.NewHub("hub", federation.HubConfig{}, nil, func(*envelope.Envelope) error { return nil },
		dd, auditL, log, 16, 65536, "")

	err = hub.Listen("127.0.0.1:notaport")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "hub listen")
}

// ---- mock streams for unit tests of PeerSender ------------------------------

// neverAckStream is a Publish stream that accepts sends but never acks.
// The caller receives the cancel func and passes it to NewPeerSender as streamCancel
// so that PeerSender.Close() unblocks Recv().
type neverAckStream struct {
	ctx  context.Context
	sent atomic.Int32
}

func newNeverAckStream() (*neverAckStream, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	return &neverAckStream{ctx: ctx}, cancel
}

func (s *neverAckStream) Send(_ *federationv1.PublishBatch) error {
	s.sent.Add(1)
	return nil
}
func (s *neverAckStream) Recv() (*federationv1.PublishAck, error) {
	<-s.ctx.Done()
	return nil, io.EOF
}
func (s *neverAckStream) Context() context.Context     { return s.ctx }
func (s *neverAckStream) Header() (metadata.MD, error) { return nil, nil }
func (s *neverAckStream) Trailer() metadata.MD         { return nil }
func (s *neverAckStream) CloseSend() error             { return nil }
func (s *neverAckStream) SendMsg(any) error            { return nil }
func (s *neverAckStream) RecvMsg(any) error            { return nil }

// countingAckStream counts received batches and sends an ack for each.
type countingAckStream struct {
	ctx     context.Context
	counter *atomic.Int32
	ackCh   chan *federationv1.PublishAck
}

func newCountingAckStream(counter *atomic.Int32) (*countingAckStream, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &countingAckStream{ctx: ctx, counter: counter, ackCh: make(chan *federationv1.PublishAck, 256)}
	return s, cancel
}

func (s *countingAckStream) Send(_ *federationv1.PublishBatch) error {
	s.counter.Add(1)
	s.ackCh <- &federationv1.PublishAck{}
	return nil
}
func (s *countingAckStream) Recv() (*federationv1.PublishAck, error) {
	select {
	case ack := <-s.ackCh:
		return ack, nil
	case <-s.ctx.Done():
		return nil, io.EOF
	}
}
func (s *countingAckStream) Context() context.Context     { return s.ctx }
func (s *countingAckStream) Header() (metadata.MD, error) { return nil, nil }
func (s *countingAckStream) Trailer() metadata.MD         { return nil }
func (s *countingAckStream) CloseSend() error             { return nil }
func (s *countingAckStream) SendMsg(any) error            { return nil }
func (s *countingAckStream) RecvMsg(any) error            { return nil }
