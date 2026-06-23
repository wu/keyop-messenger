package federation_test

import (
	"context"
	"fmt"
	"io"
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
	pr := federation.NewPeerReceiver(stream, stream.cancel, policy, dd, writer, nil,
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

// fakeBatchWriter records the batched local-commit calls made on the durable
// receive path. When err is non-nil it fails the commit and records nothing.
type fakeBatchWriter struct {
	mu      sync.Mutex
	calls   int
	written []*envelope.Envelope
	err     error
}

func (f *fakeBatchWriter) write(envs []*envelope.Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.calls++
	f.written = append(f.written, envs...)
	return nil
}

func (f *fakeBatchWriter) snapshot() (int, []*envelope.Envelope) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*envelope.Envelope, len(f.written))
	copy(out, f.written)
	return f.calls, out
}

// startReceiverBatch builds a PeerReceiver on the durable batched path, with a
// non-nil localBatchWriter. The single-record localWriter is a no-op (unused on
// this path).
func startReceiverBatch(
	t *testing.T,
	stream *mockSubClientStream,
	batchWriter func([]*envelope.Envelope) error,
	log *testutil.FakeLogger,
	auditL *fakeAuditLog,
) *federation.PeerReceiver {
	t.Helper()
	dd, err := dedup.NewLRUDedup(1000)
	require.NoError(t, err)
	pr := federation.NewPeerReceiver(stream, stream.cancel, nil, dd,
		func(*envelope.Envelope) error { return nil }, batchWriter,
		auditL, log, "test-peer", 65536)
	t.Cleanup(pr.Close)
	return pr
}

func recvAck(t *testing.T, stream *mockSubClientStream) *federationv1.SubscribeFrame {
	t.Helper()
	var ack *federationv1.SubscribeFrame
	require.Eventually(t, func() bool {
		select {
		case ack = <-stream.AckCh:
			return true
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)
	return ack
}

// TestPeerReceiver_Durable_CommitsAndAcks verifies the durable path commits a
// whole batch with one WriteBatch call (not one per record), audits each
// forward, and acks with the last record's ID after the commit.
func TestPeerReceiver_Durable_CommitsAndAcks(t *testing.T) {
	stream := newMockSubClientStream()
	defer stream.cancel()
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLog{}
	bw := &fakeBatchWriter{}

	startReceiverBatch(t, stream, bw.write, log, auditL)

	envs := []envelope.Envelope{
		makeTestEnv(t, "orders", "d-1"),
		makeTestEnv(t, "orders", "d-2"),
		makeTestEnv(t, "orders", "d-3"),
	}
	recs := make([][]byte, len(envs))
	for i, e := range envs {
		recs[i] = marshalEnv(t, e)
	}
	stream.Push(&federationv1.EnvelopeBatch{Records: recs})

	ack := recvAck(t, stream)
	require.NotNil(t, ack.GetAck())
	assert.Equal(t, envs[2].ID, ack.GetAck().LastId, "ack should carry the last committed record's ID")

	calls, written := bw.snapshot()
	assert.Equal(t, 1, calls, "the batch must be committed in a single WriteBatch call")
	require.Len(t, written, 3)
	for i := range envs {
		assert.Equal(t, envs[i].ID, written[i].ID, "record %d out of order", i)
	}
	assert.Equal(t, 3, auditL.count(audit.EventForward), "each committed record should be audited")
}

// TestPeerReceiver_Durable_DedupSuppressesRepeat verifies an ID committed in one
// batch is suppressed on a later batch and not committed a second time.
func TestPeerReceiver_Durable_DedupSuppressesRepeat(t *testing.T) {
	stream := newMockSubClientStream()
	defer stream.cancel()
	bw := &fakeBatchWriter{}

	startReceiverBatch(t, stream, bw.write, &testutil.FakeLogger{}, &fakeAuditLog{})

	env := makeTestEnv(t, "orders", "dup-1")
	rec := marshalEnv(t, env)

	stream.Push(&federationv1.EnvelopeBatch{Records: [][]byte{rec}})
	require.NotNil(t, recvAck(t, stream).GetAck())

	// Re-push the same ID: it must be skipped, not re-committed.
	stream.Push(&federationv1.EnvelopeBatch{Records: [][]byte{rec}})
	require.NotNil(t, recvAck(t, stream).GetAck())

	_, written := bw.snapshot()
	require.Len(t, written, 1, "a duplicate ID must not be committed a second time")
	assert.Equal(t, env.ID, written[0].ID)
}

// TestPeerReceiver_Durable_CommitFailureNoAck verifies that when the commit
// fails, the batch is NOT acked and the receiver exits — so the sender resends
// from its un-advanced offset. Because dedup is recorded only after a successful
// commit, the redelivery is not suppressed.
func TestPeerReceiver_Durable_CommitFailureNoAck(t *testing.T) {
	stream := newMockSubClientStream()
	defer stream.cancel()
	log := &testutil.FakeLogger{}
	bw := &fakeBatchWriter{err: fmt.Errorf("fsync failed")}

	pr := startReceiverBatch(t, stream, bw.write, log, &fakeAuditLog{})

	stream.Push(&federationv1.EnvelopeBatch{Records: [][]byte{marshalEnv(t, makeTestEnv(t, "orders", "fail-1"))}})

	// The receiver goroutine must exit (run() returns on the commit error).
	select {
	case <-pr.Done():
	case <-time.After(time.Second):
		t.Fatal("receiver did not exit after commit failure")
	}

	// No ack must have been sent: the sender keeps its offset and will resend.
	select {
	case <-stream.AckCh:
		t.Fatal("receiver must not ack a batch whose commit failed")
	default:
	}
	assert.True(t, log.HasError("batch commit failed"))
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
