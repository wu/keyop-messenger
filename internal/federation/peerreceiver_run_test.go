package federation_test

import (
	"fmt"
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
)

// ---- helpers ----------------------------------------------------------------

// sendBinaryFrame encodes records with WriteFrame and sends them as a single
// WebSocket binary message on conn.
func sendBinaryFrame(t *testing.T, conn *websocket.Conn, records [][]byte) {
	t.Helper()
	w, err := conn.NextWriter(websocket.BinaryMessage)
	require.NoError(t, err)
	require.NoError(t, federation.WriteFrame(w, records))
	require.NoError(t, w.Close())
}

// readAck reads the next ack text-frame from conn with a 2-second deadline.
func readAck(t *testing.T, conn *websocket.Conn) federation.AckMsg {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	ack, err := federation.ReceiveAck(conn)
	require.NoError(t, err)
	_ = conn.SetReadDeadline(time.Time{})
	return ack
}

// startReceiver creates a PeerReceiver on conn and registers pr.Close via
// t.Cleanup. fakeAuditLog and FakeLogger are defined in sibling test files.
func startReceiver(
	t *testing.T,
	conn *websocket.Conn,
	policy *federation.AtomicPolicy,
	writer func(*envelope.Envelope) error,
	log *testutil.FakeLogger,
	auditL *fakeAuditLog,
	maxBatchBytes int,
) *federation.PeerReceiver {
	t.Helper()
	dd, err := dedup.NewLRUDedup(1000)
	require.NoError(t, err)
	pr := federation.NewPeerReceiver(
		conn, &sync.Mutex{}, policy, dd, writer,
		auditL, log, "test-peer", maxBatchBytes,
	)
	t.Cleanup(pr.Close)
	return pr
}

// marshalEnv marshals env and fails the test if it cannot be marshalled.
func marshalEnv(t *testing.T, env envelope.Envelope) []byte {
	t.Helper()
	raw, err := envelope.Marshal(env)
	require.NoError(t, err)
	return raw
}

// ---- tests ------------------------------------------------------------------

// TestPeerReceiver_OversizedRecordSkipped verifies that a record whose serialized
// size exceeds maxBatchBytes is NOT delivered to localWriter but an ack is still
// sent so the sender can advance its window past the undeliverable message.
func TestPeerReceiver_OversizedRecordSkipped(t *testing.T) {
	srv, cli := newWSPair(t)
	log := &testutil.FakeLogger{}

	var writeCount atomic.Int64
	startReceiver(t, cli,
		nil, // no policy
		func(*envelope.Envelope) error { writeCount.Add(1); return nil },
		log, &fakeAuditLog{},
		50, // per-record cap of 50 B; any real envelope exceeds this
	)

	env := makeTestEnv(t, "orders", "oversized-1")
	raw := marshalEnv(t, env)
	require.Greaterf(t, len(raw), 50,
		"marshaled envelope (%d B) must exceed maxBatchBytes=50 for this test to be meaningful", len(raw))

	sendBinaryFrame(t, srv, [][]byte{raw})
	ack := readAck(t, srv)

	// Ack must carry the skipped record's ID so the sender advances past it.
	assert.Equal(t, env.ID, ack.LastID)
	// The oversized record must not have been handed to localWriter.
	assert.Equal(t, int64(0), writeCount.Load())
	assert.True(t, log.HasError("record too large, skipping"))
}

// TestPeerReceiver_InboundPolicyViolation verifies that a message on a channel
// not permitted by the receive policy is discarded without calling localWriter,
// is logged and audited, but an ack is still sent to advance the sender's window.
func TestPeerReceiver_InboundPolicyViolation(t *testing.T) {
	srv, cli := newWSPair(t)
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLog{}

	// Only "allowed" is in the receive policy; "blocked" must be rejected.
	policy := federation.NewAtomicPolicy(federation.ForwardPolicy{
		Receive: []string{"allowed"},
	})

	var writeCount atomic.Int64
	startReceiver(t, cli, policy,
		func(*envelope.Envelope) error { writeCount.Add(1); return nil },
		log, auditL, 65536)

	env := makeTestEnv(t, "blocked", "policy-1")
	sendBinaryFrame(t, srv, [][]byte{marshalEnv(t, env)})
	ack := readAck(t, srv)

	// Sender window must still advance.
	assert.Equal(t, env.ID, ack.LastID)
	// Message must not reach local storage.
	assert.Equal(t, int64(0), writeCount.Load())
	assert.True(t, log.HasWarn("receive policy violation"))
	// Audit entry must record an inbound policy violation.
	assert.True(t, auditL.has(audit.EventPolicyViolation))
}

// TestPeerReceiver_LocalWriteFailure verifies the documented intentional behaviour:
// when localWriter returns an error the message is silently dropped from local
// storage but an ack is still sent to advance the sender's window, preventing
// the sender from replaying the same message indefinitely.
func TestPeerReceiver_LocalWriteFailure(t *testing.T) {
	srv, cli := newWSPair(t)
	log := &testutil.FakeLogger{}

	startReceiver(t, cli, nil,
		func(*envelope.Envelope) error { return fmt.Errorf("storage unavailable") },
		log, &fakeAuditLog{}, 65536)

	env := makeTestEnv(t, "orders", "write-fail-1")
	sendBinaryFrame(t, srv, [][]byte{marshalEnv(t, env)})
	ack := readAck(t, srv)

	// Ack must still be sent with the message ID so the sender's window advances.
	assert.Equal(t, env.ID, ack.LastID)
	// The write failure must be logged.
	assert.True(t, log.HasError("receiver local write"))
}
