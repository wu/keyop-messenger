//go:build integration

//nolint:gosec // test file
package federation

import (
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/dedup"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
	"google.golang.org/grpc"
)

// recordingAudit is a thread-safe audit logger that records all events for assertion.
type recordingAudit struct {
	mu     sync.Mutex
	events []audit.Event
}

func (r *recordingAudit) Log(ev audit.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
	return nil
}

func (r *recordingAudit) Close() error { return nil }

func (r *recordingAudit) hasEventWithDetail(name, substr string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.events {
		if e.Event == name && strings.Contains(e.Detail, substr) {
			return true
		}
	}
	return false
}

func (r *recordingAudit) findEventWithDetail(name, substr string) (audit.Event, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.events {
		if e.Event == name && strings.Contains(e.Detail, substr) {
			return e, true
		}
	}
	return audit.Event{}, false
}

// newClientWithChannel constructs a Client wired to a temporary data dir with
// one configured outbound publishChannel. Returns the client and a channelFeeder
// that writes to the local channel file and notifies the client's reader.
func newClientWithChannel(t *testing.T, auditL audit.AuditLogger, log *testutil.FakeLogger, channel string) (*Client, *channelFeeder) {
	t.Helper()
	dd, _ := dedup.NewLRUDedup(100)
	dataDir := t.TempDir()
	client := NewClient(
		"test-client", nil, NewAtomicPolicy(ForwardPolicy{}),
		nil,
		nil, // localBatchWriter
		dd, auditL, log,
		65536,
		50*time.Millisecond, 500*time.Millisecond, 0.0,
		nil, []string{channel}, dataDir,
	)
	feeder := newChannelFeeder(t, dataDir, channel, client)
	return client, feeder
}

// TestClientDisconnect_AuditsPeerDisconnected verifies that ConnectWithReconnect
// emits a peer_disconnected audit event when the hub closes without acking.
func TestClientDisconnect_AuditsPeerDisconnected(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	auditL := &recordingAudit{}

	// Server receives a batch but closes without acking.
	addr := startMockServer(t, &mockFedServer{
		publishFn: func(stream grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error {
			_, _ = stream.Recv() // receive but don't ack
			return nil
		},
	})

	client, feeder := newClientWithChannel(t, auditL, log, "test-ch")
	defer client.Close()

	err := client.ConnectWithReconnect(addr)
	require.NoError(t, err)

	env, envErr := envelope.NewEnvelope("test-ch", "origin", "test.v1", nil)
	require.NoError(t, envErr)
	feeder.publish(t, &env)

	require.Eventually(t, func() bool {
		return auditL.hasEventWithDetail(audit.EventPeerDisconnected, "unacked_bytes=")
	}, 5*time.Second, 20*time.Millisecond)

	evt, found := auditL.findEventWithDetail(audit.EventPeerDisconnected, "unacked_bytes=")
	require.True(t, found)
	assert.NotEmpty(t, evt.PeerAddr)
	assert.Contains(t, evt.Detail, "unacked_bytes=")
}

// TestClientReconnect_AuditsFailedAttempts verifies that failed reconnect
// attempts are logged as peer_connected events with attempt= and err= in Detail.
func TestClientReconnect_AuditsFailedAttempts(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	auditL := &recordingAudit{}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()

	grpcSrv := grpc.NewServer()
	federationv1.RegisterFederationServiceServer(grpcSrv, &mockFedServer{
		publishFn: func(stream grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error {
			_, _ = stream.Recv() // receive but close without acking
			return nil
		},
	})
	go grpcSrv.Serve(lis) //nolint:errcheck

	client, feeder := newClientWithChannel(t, auditL, log, "test-ch")
	defer client.Close()

	require.NoError(t, client.ConnectWithReconnect(addr))

	env, envErr := envelope.NewEnvelope("test-ch", "origin", "test.v1", nil)
	require.NoError(t, envErr)
	feeder.publish(t, &env)

	// Wait for the disconnect event.
	require.Eventually(t, func() bool {
		return auditL.hasEventWithDetail(audit.EventPeerDisconnected, "unacked_bytes=")
	}, 5*time.Second, 20*time.Millisecond)

	// Stop the server so subsequent reconnect attempts fail.
	grpcSrv.Stop()

	require.Eventually(t, func() bool {
		return auditL.hasEventWithDetail(audit.EventPeerConnected, "attempt=")
	}, 5*time.Second, 20*time.Millisecond)

	evt, found := auditL.findEventWithDetail(audit.EventPeerConnected, "attempt=")
	require.True(t, found)
	assert.Contains(t, evt.Detail, "attempt=")
	assert.Contains(t, evt.Detail, "err=")
}
