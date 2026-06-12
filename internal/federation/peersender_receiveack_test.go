package federation

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
	"google.golang.org/grpc/metadata"
)

// ---- mock Publish stream for unit tests -------------------------------------

// mockPublishStream is a simple in-process Publish stream backed by channels.
// The test goroutine reads batches from recvCh and writes acks to ackCh.
type mockPublishStream struct {
	ctx    context.Context
	cancel context.CancelFunc
	recvCh chan *federationv1.PublishBatch
	ackCh  chan *federationv1.PublishAck
}

func newMockPublishStream() *mockPublishStream {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // cancel stored in struct
	return &mockPublishStream{
		ctx:    ctx,
		cancel: cancel,
		recvCh: make(chan *federationv1.PublishBatch, 16),
		ackCh:  make(chan *federationv1.PublishAck, 16),
	}
}

func (m *mockPublishStream) Send(batch *federationv1.PublishBatch) error {
	select {
	case m.recvCh <- batch:
		return nil
	case <-m.ctx.Done():
		return io.EOF
	}
}

func (m *mockPublishStream) Recv() (*federationv1.PublishAck, error) {
	select {
	case ack, ok := <-m.ackCh:
		if !ok {
			return nil, io.EOF
		}
		return ack, nil
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
}

func (m *mockPublishStream) Context() context.Context     { return m.ctx }
func (m *mockPublishStream) Header() (metadata.MD, error) { return nil, nil }
func (m *mockPublishStream) Trailer() metadata.MD         { return nil }
func (m *mockPublishStream) CloseSend() error             { return nil }
func (m *mockPublishStream) SendMsg(any) error            { return nil }
func (m *mockPublishStream) RecvMsg(any) error            { return nil }

// ---- tests ------------------------------------------------------------------

// TestPeerSender_AckAdvancesWindow verifies that receiving a PublishAck with
// LastId clears the unacked window up to that ID.
func TestPeerSender_AckAdvancesWindow(t *testing.T) {
	t.Parallel()
	stream := newMockPublishStream()
	log := &testutil.FakeLogger{}

	ps := NewPeerSender(stream, stream.cancel, 100, 65536, log)
	defer ps.Close()

	env1, _ := envelope.NewEnvelope("ch", "s", "t", nil)
	env2, _ := envelope.NewEnvelope("ch", "s", "t", nil)

	// Enqueue one at a time to force separate batches: wait for first to
	// be sent before enqueueing the second.
	ps.Enqueue(&env1)
	require.Eventually(t, func() bool { return len(stream.recvCh) > 0 }, time.Second, 5*time.Millisecond)
	<-stream.recvCh
	// Ack the first batch.
	stream.ackCh <- &federationv1.PublishAck{LastId: env1.ID}

	// Now enqueue second.
	ps.Enqueue(&env2)
	require.Eventually(t, func() bool { return len(stream.recvCh) > 0 }, time.Second, 5*time.Millisecond)
	<-stream.recvCh
	stream.ackCh <- &federationv1.PublishAck{LastId: env2.ID}

	require.Eventually(t, func() bool {
		return ps.LastAckedID() == env2.ID
	}, time.Second, 5*time.Millisecond)

	assert.Equal(t, env2.ID, ps.LastAckedID())
}

// TestPeerSender_StreamClose_ExitsSender verifies that closing ackCh (stream EOF)
// causes the sender goroutine to exit and Done() fires.
func TestPeerSender_StreamClose_ExitsSender(t *testing.T) {
	t.Parallel()
	stream := newMockPublishStream()
	log := &testutil.FakeLogger{}

	ps := NewPeerSender(stream, stream.cancel, 100, 65536, log)

	env, _ := envelope.NewEnvelope("ch", "s", "t", nil)
	ps.Enqueue(&env)

	// Wait for batch to be sent then close the ack channel.
	require.Eventually(t, func() bool { return len(stream.recvCh) > 0 }, time.Second, 5*time.Millisecond)
	close(stream.ackCh) // simulate stream EOF

	select {
	case <-ps.Done():
		// expected: sender exited cleanly
	case <-time.After(2 * time.Second):
		t.Fatal("PeerSender did not exit after stream closed")
	}
}

// TestPeerSender_ContextCancel_ExitsSender verifies that cancelling the stream
// context (via Close) causes the sender goroutine to exit.
func TestPeerSender_ContextCancel_ExitsSender(t *testing.T) {
	t.Parallel()
	stream := newMockPublishStream()
	log := &testutil.FakeLogger{}

	ps := NewPeerSender(stream, stream.cancel, 100, 65536, log)

	// Cancel the stream context directly (simulates hub-side close).
	stream.cancel()

	select {
	case <-ps.Done():
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("PeerSender did not exit after context cancelled")
	}
}
