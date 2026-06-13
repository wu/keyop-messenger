package federation

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/storage"
	"github.com/wu/keyop-messenger/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// mockSubServerStream implements grpc.BidiStreamingServer[SubscribeFrame, HubBatch]
// for coordinator unit tests. The test goroutine reads batches from sendCh and
// can deliver acks by writing to ackCh (or by closing ackCh).
type mockSubServerStream struct {
	ctx    context.Context
	cancel context.CancelFunc
	sendCh chan *federationv1.HubBatch
	ackCh  chan *federationv1.SubscribeFrame
}

func newMockSubServerStream() *mockSubServerStream {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // cancel stored in struct
	return &mockSubServerStream{
		ctx:    ctx,
		cancel: cancel,
		sendCh: make(chan *federationv1.HubBatch, 16),
		ackCh:  make(chan *federationv1.SubscribeFrame, 16),
	}
}

func (m *mockSubServerStream) Send(batch *federationv1.HubBatch) error {
	select {
	case m.sendCh <- batch:
		return nil
	case <-m.ctx.Done():
		return m.ctx.Err()
	}
}

func (m *mockSubServerStream) Recv() (*federationv1.SubscribeFrame, error) {
	select {
	case f, ok := <-m.ackCh:
		if !ok {
			return nil, context.Canceled
		}
		return f, nil
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
}

func (m *mockSubServerStream) Context() context.Context { return m.ctx }

// grpc.ServerStream methods (no-ops for tests)
func (m *mockSubServerStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockSubServerStream) SendHeader(metadata.MD) error { return nil }
func (m *mockSubServerStream) SetTrailer(metadata.MD)       {}
func (m *mockSubServerStream) SendMsg(any) error            { return nil }
func (m *mockSubServerStream) RecvMsg(any) error            { return nil }

// Ensure mockSubServerStream satisfies the interface at compile time.
var _ grpc.BidiStreamingServer[federationv1.SubscribeFrame, federationv1.HubBatch] = (*mockSubServerStream)(nil)

// ---- tests ------------------------------------------------------------------

// TestClientCoordinator_SendBatch_AckFlow verifies that a batch is sent over
// the stream and doneCh is closed once the ack arrives.
func TestClientCoordinator_SendBatch_AckFlow(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	stream := newMockSubServerStream()
	ackCh := make(chan struct{}, 4)

	cc := newClientCoordinator(stream, ackCh, 65536, log, nil)
	cc.start()
	defer cc.close()

	rawLine := []byte(`{"v":1,"id":"abc","channel":"events","origin":"test","ts":"2024-01-01T00:00:00Z","payload_type":"t","payload":{}}`)
	doneCh := make(chan struct{})
	req := sendReq{
		channel:   "events",
		rawLines:  [][]byte{rawLine},
		newOffset: int64(len(rawLine) + 1),
		doneCh:    doneCh,
	}

	// Deliver ack to coordinator before it can block waiting.
	go func() {
		// Wait for the coordinator to send the batch first.
		select {
		case <-stream.sendCh:
		case <-time.After(2 * time.Second):
			return
		}
		ackCh <- struct{}{}
	}()

	select {
	case cc.requestCh <- req:
	case <-time.After(time.Second):
		t.Fatal("timed out submitting request")
	}

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for doneCh to close")
	}
}

// TestClientCoordinator_SequentialDelivery verifies two batches are delivered in order.
func TestClientCoordinator_SequentialDelivery(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	stream := newMockSubServerStream()
	ackCh := make(chan struct{}, 8)

	cc := newClientCoordinator(stream, ackCh, 65536, log, nil)
	cc.start()
	defer cc.close()

	line1 := []byte(`{"v":1,"id":"id1","channel":"ch","origin":"o","ts":"2024-01-01T00:00:00Z","payload_type":"t","payload":{}}`)
	line2 := []byte(`{"v":1,"id":"id2","channel":"ch","origin":"o","ts":"2024-01-01T00:00:00Z","payload_type":"t","payload":{}}`)

	done1 := make(chan struct{})
	done2 := make(chan struct{})

	// Goroutine that acks batches in order as they arrive.
	go func() {
		// Batch 1 arrives → ack → batch 2 arrives → ack.
		<-stream.sendCh
		ackCh <- struct{}{}
		<-stream.sendCh
		ackCh <- struct{}{}
	}()

	require.NoError(t, submitReq(t, cc, sendReq{
		channel: "ch", rawLines: [][]byte{line1},
		newOffset: int64(len(line1) + 1), doneCh: done1,
	}))
	require.NoError(t, submitReq(t, cc, sendReq{
		channel: "ch", rawLines: [][]byte{line2},
		newOffset: int64(len(line1)+1) + int64(len(line2)+1), doneCh: done2,
	}))

	select {
	case <-done1:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for done1")
	}
	select {
	case <-done2:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for done2")
	}
}

// submitReq sends a sendReq to cc.requestCh with a short timeout.
func submitReq(t *testing.T, cc *clientCoordinator, req sendReq) error {
	t.Helper()
	select {
	case cc.requestCh <- req:
		return nil
	case <-time.After(time.Second):
		t.Fatal("timed out submitting request to coordinator")
		return nil
	}
}

// TestClientCoordinator_Close_Idempotent verifies close() does not panic when
// called more than once.
func TestClientCoordinator_Close_Idempotent(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	stream := newMockSubServerStream()
	ackCh := make(chan struct{}, 4)

	cc := newClientCoordinator(stream, ackCh, 65536, log, nil)
	cc.start()

	cc.close()
	assert.NotPanics(t, func() { cc.close() })
}

// TestClientCoordinator_AckChannelClosed stops coordinator when ackCh is closed.
func TestClientCoordinator_AckChannelClosed(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	stream := newMockSubServerStream()
	ackCh := make(chan struct{}, 4)

	cc := newClientCoordinator(stream, ackCh, 65536, log, nil)
	cc.start()

	rawLine := []byte(`{"v":1,"id":"z","channel":"ch","origin":"o","ts":"2024-01-01T00:00:00Z","payload_type":"t","payload":{}}`)
	doneCh := make(chan struct{})
	go func() {
		cc.requestCh <- sendReq{
			channel: "ch", rawLines: [][]byte{rawLine},
			newOffset: int64(len(rawLine) + 1), doneCh: doneCh,
		}
	}()

	// Wait for batch to arrive at stream, then close ackCh.
	select {
	case <-stream.sendCh:
	case <-time.After(2 * time.Second):
		t.Fatal("batch never sent")
	}
	close(ackCh)

	select {
	case <-cc.done:
	case <-time.After(2 * time.Second):
		t.Fatal("coordinator did not exit after ackCh closed")
	}
	cc.close()
}

// TestClientCoordinator_WithReaders verifies integration with channelReader.
func TestClientCoordinator_WithReaders(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	log := &testutil.FakeLogger{}

	channelDir := dir + "/channels/feed"
	offsetDir := dir + "/subscribers/feed"

	stream := newMockSubServerStream()
	ackCh := make(chan struct{}, 4)

	placeholder := make(chan sendReq, 1)
	reader, err := newChannelReader("peer1", "feed", channelDir, offsetDir, "fed-", 65536, placeholder, log)
	require.NoError(t, err)

	cc := newClientCoordinator(stream, ackCh, 65536, log, []*channelReader{reader})
	cc.start()
	defer cc.close()

	env1 := makeEnvelope(t, "feed", "m1")
	writeTestSegment(t, channelDir, 0, []envelope.Envelope{env1})

	// Route the ack when the batch arrives at the stream.
	go func() {
		select {
		case <-stream.sendCh:
			ackCh <- struct{}{}
		case <-time.After(2 * time.Second):
		}
	}()

	reader.notify()

	require.Eventually(t, func() bool {
		offset, readErr := storage.ReadOffset(offsetDir + "/fed-peer1.offset")
		return readErr == nil && offset > 0
	}, 2*time.Second, 10*time.Millisecond, "offset should be advanced after delivery")
}
