package federation_test

import (
	"context"
	"fmt"
	"io"
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
	"google.golang.org/grpc/metadata"
)

// ---- mock Subscribe client stream -------------------------------------------

// mockSubClientStream drives a PeerReceiver without a real gRPC connection.
// The test pushes HubBatch messages via Push(); acks sent by the receiver
// appear on AckCh.
type mockSubClientStream struct {
	ctx    context.Context
	cancel context.CancelFunc
	msgCh  chan *federationv1.HubBatch
	AckCh  chan *federationv1.SubscribeFrame
}

func newMockSubClientStream() *mockSubClientStream {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // cancel stored in struct
	return &mockSubClientStream{
		ctx:    ctx,
		cancel: cancel,
		msgCh:  make(chan *federationv1.HubBatch, 16),
		AckCh:  make(chan *federationv1.SubscribeFrame, 16),
	}
}

func (m *mockSubClientStream) Recv() (*federationv1.HubBatch, error) {
	select {
	case msg, ok := <-m.msgCh:
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
}

func (m *mockSubClientStream) Send(frame *federationv1.SubscribeFrame) error {
	select {
	case m.AckCh <- frame:
		return nil
	case <-m.ctx.Done():
		return m.ctx.Err()
	}
}

func (m *mockSubClientStream) Push(batch *federationv1.EnvelopeBatch) {
	m.msgCh <- &federationv1.HubBatch{Payload: &federationv1.HubBatch_Batch{Batch: batch}}
}

func (m *mockSubClientStream) Close() { close(m.msgCh) }

func (m *mockSubClientStream) Context() context.Context     { return m.ctx }
func (m *mockSubClientStream) Header() (metadata.MD, error) { return nil, nil }
func (m *mockSubClientStream) Trailer() metadata.MD         { return nil }
func (m *mockSubClientStream) CloseSend() error             { return nil }
func (m *mockSubClientStream) SendMsg(any) error            { return nil }
func (m *mockSubClientStream) RecvMsg(any) error            { return nil }

// ---- helpers ----------------------------------------------------------------

func startReceiver(
	t *testing.T,
	stream *mockSubClientStream,
	policy *federation.AtomicPolicy,
	writer func(*envelope.Envelope) error,
	log *testutil.FakeLogger,
	auditL *fakeAuditLog,
	maxBatchBytes int,
) *federation.PeerReceiver {
	t.Helper()
	dd, err := dedup.NewLRUDedup(1000)
	require.NoError(t, err)
	// Use stream.cancel so that pr.Close() → streamCancel() unblocks stream.Recv().
	pr := federation.NewPeerReceiver(stream, stream.cancel, policy, dd, writer,
		auditL, log, "test-peer", maxBatchBytes)
	t.Cleanup(pr.Close)
	return pr
}

func marshalEnv(t *testing.T, env envelope.Envelope) []byte {
	t.Helper()
	raw, err := envelope.Marshal(env)
	require.NoError(t, err)
	return raw
}

// ---- tests ------------------------------------------------------------------

// TestPeerReceiver_OversizedRecordSkipped verifies that a record exceeding
// maxBatchBytes is skipped but still acked so the sender advances past it.
func TestPeerReceiver_OversizedRecordSkipped(t *testing.T) {
	stream := newMockSubClientStream()
	defer stream.cancel()
	log := &testutil.FakeLogger{}

	var writeCount atomic.Int64
	startReceiver(t, stream, nil,
		func(*envelope.Envelope) error { writeCount.Add(1); return nil },
		log, &fakeAuditLog{},
		50, // per-record cap of 50 B; any real envelope exceeds this
	)

	env := makeTestEnv(t, "orders", "oversized-1")
	raw := marshalEnv(t, env)
	require.Greaterf(t, len(raw), 50,
		"marshaled envelope (%d B) must exceed maxBatchBytes=50 for this test to be meaningful", len(raw))

	stream.Push(&federationv1.EnvelopeBatch{Records: [][]byte{raw}})

	// Receiver must send an ack with the oversized record's ID.
	var ack *federationv1.SubscribeFrame
	require.Eventually(t, func() bool {
		select {
		case ack = <-stream.AckCh:
			return true
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)

	require.NotNil(t, ack.GetAck())
	assert.Equal(t, env.ID, ack.GetAck().LastId)
	assert.Equal(t, int64(0), writeCount.Load())
	assert.True(t, log.HasError("record too large, skipping"))
}

// TestPeerReceiver_InboundPolicyViolation verifies that a message on a blocked
// channel is discarded, logged, audited, but still acked.
func TestPeerReceiver_InboundPolicyViolation(t *testing.T) {
	stream := newMockSubClientStream()
	defer stream.cancel()
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLog{}

	policy := federation.NewAtomicPolicy(federation.ForwardPolicy{
		Receive: []string{"allowed"},
	})

	var writeCount atomic.Int64
	startReceiver(t, stream, policy,
		func(*envelope.Envelope) error { writeCount.Add(1); return nil },
		log, auditL, 65536)

	env := makeTestEnv(t, "blocked", "policy-1")
	stream.Push(&federationv1.EnvelopeBatch{Records: [][]byte{marshalEnv(t, env)}})

	var ack *federationv1.SubscribeFrame
	require.Eventually(t, func() bool {
		select {
		case ack = <-stream.AckCh:
			return true
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)

	require.NotNil(t, ack.GetAck())
	assert.Equal(t, env.ID, ack.GetAck().LastId)
	assert.Equal(t, int64(0), writeCount.Load())
	assert.True(t, log.HasWarn("receive policy violation"))
	assert.True(t, auditL.has(audit.EventPolicyViolation))
}

// TestPeerReceiver_LocalWriteFailure verifies that a localWriter error still
// results in an ack so the sender's window advances.
func TestPeerReceiver_LocalWriteFailure(t *testing.T) {
	stream := newMockSubClientStream()
	defer stream.cancel()
	log := &testutil.FakeLogger{}

	startReceiver(t, stream, nil,
		func(*envelope.Envelope) error { return fmt.Errorf("storage unavailable") },
		log, &fakeAuditLog{}, 65536)

	env := makeTestEnv(t, "orders", "write-fail-1")
	stream.Push(&federationv1.EnvelopeBatch{Records: [][]byte{marshalEnv(t, env)}})

	var ack *federationv1.SubscribeFrame
	require.Eventually(t, func() bool {
		select {
		case ack = <-stream.AckCh:
			return true
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)

	require.NotNil(t, ack.GetAck())
	assert.Equal(t, env.ID, ack.GetAck().LastId)
	assert.True(t, log.HasError("receiver local write"))
}
