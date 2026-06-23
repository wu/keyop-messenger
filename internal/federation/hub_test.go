//go:build integration

//nolint:gosec // test file: G301/G304/G306
package federation_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"sync"
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

// writeBatch adapts the counting writer to the batch localWriter signature,
// applying the same per-record count and delay so backpressure tests behave
// identically.
func (c *countingWriter) writeBatch(envs []*envelope.Envelope) error {
	for range envs {
		if err := c.write(nil); err != nil {
			return err
		}
	}
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
	return federation.NewHub(cfg, nil, cw.writeBatch, dd, auditL, log, 1000, 65536, "")
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

	sender := newTestSender(pubStream)

	for i := 0; i < 10; i++ {
		env, err := envelope.NewEnvelope("orders", "sender", "test", map[string]any{"n": i})
		require.NoError(t, err)
		require.NoError(t, sender.Send(&env))
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

	sender := newTestSender(pubStream)

	env, err := envelope.NewEnvelope("ch", "sender", "t", nil)
	require.NoError(t, err)

	require.NoError(t, sender.Send(&env))
	require.Eventually(t, func() bool { return cw.n() == 1 }, time.Second, 10*time.Millisecond)

	require.NoError(t, sender.Send(&env)) // same ID — should be deduped
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

	sender := newTestSender(pubStream)

	env, err := envelope.NewEnvelope("blocked-ch", "sender", "t", nil)
	require.NoError(t, err)
	require.NoError(t, sender.Send(&env))

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

	sender := newTestSender(pubStream)

	env1, _ := envelope.NewEnvelope("ch", "s", "t", nil)
	env2, _ := envelope.NewEnvelope("ch", "s", "t", nil)
	// Drive sends from a goroutine so the test can observe partial progress
	// while the hub is still applying the artificial write delay.
	go func() {
		_ = sender.Send(&env1)
		_ = sender.Send(&env2)
	}()

	time.Sleep(100 * time.Millisecond)
	assert.LessOrEqual(t, cw.n(), 1, "second write should still be blocked")

	require.Eventually(t, func() bool { return cw.n() == 2 }, 3*time.Second, 20*time.Millisecond)
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
	hub := federation.NewHub(federation.HubConfig{}, hubTLS,
		func([]*envelope.Envelope) error { return nil }, dd, auditL, log, 100, 65536, "")
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
	hub := federation.NewHub(federation.HubConfig{}, nil, func([]*envelope.Envelope) error { return nil },
		dd, auditL, log, 16, 65536, "")

	err = hub.Listen("127.0.0.1:notaport")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "hub listen")
}
