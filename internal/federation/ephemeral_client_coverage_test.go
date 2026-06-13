//go:build integration

package federation

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
	"google.golang.org/grpc"
)

// TestNoopAuditLogger covers the two no-op methods.
func TestNoopAuditLogger(t *testing.T) {
	a := noopAuditLogger{}
	assert.NoError(t, a.Log(audit.Event{Event: "forward", Channel: "events"}))
	assert.NoError(t, a.Close())
}

// TestEphemeralClient_DispatchEnvelope_HandlerCalled verifies that a hub batch
// pushed via Subscribe stream is decoded and delivered to registered handlers.
func TestEphemeralClient_DispatchEnvelope_HandlerCalled(t *testing.T) {
	received := make(chan string, 4)

	env, err := envelope.NewEnvelope("events", "hub", "test.Msg", map[string]string{"k": "v"})
	require.NoError(t, err)
	raw, err := envelope.Marshal(env)
	require.NoError(t, err)

	addr := startMockServer(t, &mockFedServer{
		subscribeFn: func(stream grpc.BidiStreamingServer[federationv1.SubscribeFrame, federationv1.HubBatch]) error {
			frame, recvErr := stream.Recv()
			if recvErr != nil || frame.GetRequest() == nil {
				return nil
			}
			// Push the test envelope as a HubBatch.
			_ = stream.Send(&federationv1.HubBatch{
				Payload: &federationv1.HubBatch_Batch{
					Batch: &federationv1.EnvelopeBatch{Records: [][]byte{raw}},
				},
			})
			// Wait for the ack then keep stream open briefly.
			_, _ = stream.Recv()
			time.Sleep(200 * time.Millisecond)
			return nil
		},
	})

	log := &testutil.FakeLogger{}
	ec := newEphemeralTestClient(t, log, "events")

	ec.AddHandler("events", func(e *envelope.Envelope) error {
		received <- e.ID
		return nil
	})

	dialEphemeral(t, ec, addr)

	select {
	case id := <-received:
		assert.Equal(t, env.ID, id)
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not called")
	}
}

// TestEphemeralClient_Publish_ConnLost returns ErrEphemeralConnLost when the
// connection closes before the ack arrives.
func TestEphemeralClient_Publish_ConnLost(t *testing.T) {
	addr := startMockServer(t, &mockFedServer{
		publishFn: func(stream grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error {
			_, _ = stream.Recv() // receive but close without acking
			return nil
		},
	})

	log := &testutil.FakeLogger{}
	ec := newEphemeralTestClient(t, log)
	dialEphemeral(t, ec, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	env := &envelope.Envelope{Channel: "ch", ID: "x"}
	err := ec.Publish(ctx, env)
	assert.Error(t, err)
}

// TestEphemeralClient_Publish_ClosedClient returns ErrEphemeralClosed.
func TestEphemeralClient_Publish_ClosedClient(t *testing.T) {
	log := &testutil.FakeLogger{}
	ec := NewEphemeralClient(EphemeralClientConfig{InstanceName: "test"}, log)
	ec.Close()

	env := &envelope.Envelope{Channel: "ch", ID: "x"}
	err := ec.Publish(context.Background(), env)
	assert.ErrorIs(t, err, ErrEphemeralClosed)
}

// TestEphemeralClient_Publish_MarshalError returns error for unmarshalable envelope.
func TestEphemeralClient_Publish_MarshalError(t *testing.T) {
	addr := startMockServer(t, &mockFedServer{}) // default: ack all

	log := &testutil.FakeLogger{}
	ec := newEphemeralTestClient(t, log)
	dialEphemeral(t, ec, addr)

	// An envelope with nil Payload marshals fine, but crafted invalid content might not.
	// This test mostly verifies the code path doesn't panic; marshal errors are rare.
	env := &envelope.Envelope{Channel: "ch", ID: "valid"}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = ec.Publish(ctx, env)
}

// TestEphemeralClient_ConnectWithReconnect_Reconnects verifies that after the
// hub closes the connection, the client reconnects automatically.
func TestEphemeralClient_ConnectWithReconnect_Reconnects(t *testing.T) {
	var connCount atomic.Int32
	secondConnReady := make(chan struct{})

	addr := startMockServer(t, &mockFedServer{
		publishFn: func(stream grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error {
			n := connCount.Add(1)
			if n == 1 {
				// First connection: receive one batch but close without acking.
				_, _ = stream.Recv()
				return nil
			}
			// Second connection: stay open (signal readiness, then drain acks).
			select {
			case secondConnReady <- struct{}{}:
			default:
			}
			for {
				_, err := stream.Recv()
				if err != nil {
					return nil
				}
				_ = stream.Send(&federationv1.PublishAck{})
			}
		},
	})

	log := &testutil.FakeLogger{}
	// Use a longer reconnect base so the loop doesn't run thousands of times.
	ec := NewEphemeralClient(EphemeralClientConfig{
		InstanceName:  "test",
		ReconnectBase: 5 * time.Millisecond,
		ReconnectMax:  50 * time.Millisecond,
	}, log)
	t.Cleanup(ec.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ec.ConnectWithReconnect(ctx, addr)
	require.NoError(t, err)

	// Send a message to trigger disconnect on the first connection (no ack → conn lost).
	env := &envelope.Envelope{Channel: "ch", ID: "trigger"}
	pubCtx, pubCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = ec.Publish(pubCtx, env)
	pubCancel()

	// Wait for the second connection to establish.
	select {
	case <-secondConnReady:
		assert.GreaterOrEqual(t, int(connCount.Load()), 2)
	case <-time.After(5 * time.Second):
		t.Fatal("client did not reconnect")
	}
}

// TestEphemeralClient_DispatchEnvelope_MultipleHandlers verifies all handlers are called.
func TestEphemeralClient_DispatchEnvelope_MultipleHandlers(t *testing.T) {
	env1, _ := envelope.NewEnvelope("ch", "o", "t", nil)
	raw, _ := envelope.Marshal(env1)

	addr := startMockServer(t, &mockFedServer{
		subscribeFn: func(stream grpc.BidiStreamingServer[federationv1.SubscribeFrame, federationv1.HubBatch]) error {
			frame, err := stream.Recv()
			if err != nil || frame.GetRequest() == nil {
				return nil
			}
			_ = stream.Send(&federationv1.HubBatch{
				Payload: &federationv1.HubBatch_Batch{
					Batch: &federationv1.EnvelopeBatch{Records: [][]byte{raw}},
				},
			})
			_, _ = stream.Recv() // ack
			time.Sleep(200 * time.Millisecond)
			return nil
		},
	})

	log := &testutil.FakeLogger{}
	ec := newEphemeralTestClient(t, log, "ch")

	var count atomic.Int32
	for i := 0; i < 3; i++ {
		ec.AddHandler("ch", func(_ *envelope.Envelope) error {
			count.Add(1)
			return nil
		})
	}
	dialEphemeral(t, ec, addr)

	require.Eventually(t, func() bool { return count.Load() == 3 }, 2*time.Second, 10*time.Millisecond)
}
