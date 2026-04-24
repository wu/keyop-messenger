//nolint:gosec // test file
package federation

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/dedup"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
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

// hasEventWithDetail reports whether any recorded event with the given name has
// Detail containing substr.
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

// findEventWithDetail returns the first event matching name whose Detail contains
// substr, or a zero value if none is found.
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

// mockHubHandshakeInternal performs the server side of the application handshake.
func mockHubHandshakeInternal(conn *websocket.Conn) {
	_ = conn.ReadJSON(&HandshakeMsg{})
	_ = conn.WriteJSON(HandshakeMsg{InstanceName: "hub", Role: "hub", Version: "1"})
}

// newSenderOnlyClient builds a sender-only Client (no localWriter) with short
// reconnect timings suitable for these tests.
func newSenderOnlyClient(auditL audit.AuditLogger, log *testutil.FakeLogger) *Client {
	dd, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})
	return NewClient(
		"test-client",
		nil,
		policy,
		nil, // no localWriter → sender reads acks directly from conn
		dd,
		auditL,
		log,
		100,
		65536,
		50*time.Millisecond,  // reconnectBase: short for tests
		500*time.Millisecond, // reconnectMax
		0.0,                  // no jitter for deterministic timing
		nil, nil,
	)
}

// TestClientDisconnect_AuditsPeerDisconnected verifies that ConnectWithReconnect
// emits a peer_disconnected audit event (with "unacked=" in Detail) when the hub
// closes without acking an in-flight message.
//
// The sender's Done() channel only fires when it is actively sending and
// encounters an error (write failure or ack channel closed). We trigger this
// by enqueueing a message so the sender blocks waiting for an ack, then the
// server closes the connection without sending one.
func TestClientDisconnect_AuditsPeerDisconnected(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	auditL := &recordingAudit{}

	srv := mockWsServer(t, func(conn *websocket.Conn) {
		defer func() { _ = conn.Close() }()
		mockHubHandshakeInternal(conn)
		// Read the binary message frame but close without sending an ack.
		// This causes the client sender to detect the disconnect.
		_, _, _ = conn.ReadMessage()
	})
	defer srv.Close()

	client := newSenderOnlyClient(auditL, log)
	defer client.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	err := client.ConnectWithReconnect(strings.TrimPrefix(wsURL, "ws://"))
	require.NoError(t, err)

	// Enqueue a message so the sender sends it and then blocks waiting for an ack.
	// When the server closes without acking, sender.Done() fires and the reconnect
	// loop emits the peer_disconnected event.
	env, envErr := envelope.NewEnvelope("test-ch", "origin", "test.v1", nil)
	require.NoError(t, envErr)
	sender := client.Sender()
	require.NotNil(t, sender)
	sender.Enqueue(&env)

	require.Eventually(t, func() bool {
		return auditL.hasEventWithDetail(audit.EventPeerDisconnected, "unacked=")
	}, 2*time.Second, 20*time.Millisecond, "peer_disconnected audit event should be emitted on disconnect")

	evt, found := auditL.findEventWithDetail(audit.EventPeerDisconnected, "unacked=")
	require.True(t, found)
	assert.NotEmpty(t, evt.PeerAddr, "peer_disconnected should record the hub address in PeerAddr")
	assert.Contains(t, evt.Detail, "unacked=")
}

// TestClientReconnect_AuditsFailedAttempts verifies that each failed reconnect
// attempt is logged as a peer_connected audit event with "attempt=" and "err="
// in the Detail field.
func TestClientReconnect_AuditsFailedAttempts(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	auditL := &recordingAudit{}

	// Server accepts the initial connection, reads a message, then closes.
	srv := mockWsServer(t, func(conn *websocket.Conn) {
		defer func() { _ = conn.Close() }()
		mockHubHandshakeInternal(conn)
		_, _, _ = conn.ReadMessage() // read the message but close without acking
	})

	client := newSenderOnlyClient(auditL, log)
	defer client.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	addr := strings.TrimPrefix(wsURL, "ws://")

	err := client.ConnectWithReconnect(addr)
	require.NoError(t, err)

	// Trigger the disconnect so the reconnect loop starts.
	env, envErr := envelope.NewEnvelope("test-ch", "origin", "test.v1", nil)
	require.NoError(t, envErr)
	client.Sender().Enqueue(&env)

	// Wait for the first disconnect event before stopping the server.
	require.Eventually(t, func() bool {
		return auditL.hasEventWithDetail(audit.EventPeerDisconnected, "unacked=")
	}, 2*time.Second, 20*time.Millisecond)

	// Stop the server so all subsequent reconnect attempts fail.
	srv.Close()

	// Failed reconnect attempts are logged as peer_connected events with attempt=N err=...
	require.Eventually(t, func() bool {
		return auditL.hasEventWithDetail(audit.EventPeerConnected, "attempt=")
	}, 2*time.Second, 20*time.Millisecond, "failed reconnect attempts should be audited")

	evt, found := auditL.findEventWithDetail(audit.EventPeerConnected, "attempt=")
	require.True(t, found)
	assert.Contains(t, evt.Detail, "attempt=", "Detail should include the attempt number")
	assert.Contains(t, evt.Detail, "err=", "Detail should include the dial error message")
}
