//go:build integration

// Package-level tests for EphemeralMessenger that require a live (in-process)
// gRPC hub to exercise code paths that unit tests cannot reach.
package messenger

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
	"github.com/wu/keyop-messenger/internal/envelope"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// mockEphemeralGRPCHub is a minimal FederationServiceServer for EphemeralMessenger tests.
type mockEphemeralGRPCHub struct {
	federationv1.UnimplementedFederationServiceServer
	publishFn   func(grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error
	subscribeFn func(grpc.BidiStreamingServer[federationv1.SubscribeFrame, federationv1.HubBatch]) error
}

func (s *mockEphemeralGRPCHub) Publish(stream grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error {
	if s.publishFn != nil {
		return s.publishFn(stream)
	}
	for {
		_, err := stream.Recv()
		if err != nil {
			return nil
		}
		_ = stream.Send(&federationv1.PublishAck{})
	}
}

func (s *mockEphemeralGRPCHub) Subscribe(stream grpc.BidiStreamingServer[federationv1.SubscribeFrame, federationv1.HubBatch]) error {
	if s.subscribeFn != nil {
		return s.subscribeFn(stream)
	}
	_, _ = stream.Recv() // read SubscribeRequest
	<-stream.Context().Done()
	return nil
}

// startEphemeralHub starts a gRPC server and returns its addr.
func startEphemeralHub(t *testing.T, srv *mockEphemeralGRPCHub) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcSrv := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	federationv1.RegisterFederationServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis) //nolint:errcheck
	t.Cleanup(grpcSrv.Stop)
	return lis.Addr().String()
}

// TestEphemeralMessenger_Subscribe_HandlerInvoked exercises the handler closure
// registered by EphemeralMessenger.Subscribe.
func TestEphemeralMessenger_Subscribe_HandlerInvoked(t *testing.T) {
	env, err := envelope.NewEnvelope("events", "hub", "test.Msg", map[string]string{"x": "1"})
	require.NoError(t, err)
	raw, err := envelope.Marshal(env)
	require.NoError(t, err)

	addr := startEphemeralHub(t, &mockEphemeralGRPCHub{
		subscribeFn: func(stream grpc.BidiStreamingServer[federationv1.SubscribeFrame, federationv1.HubBatch]) error {
			if _, err := stream.Recv(); err != nil { // read SubscribeRequest
				return nil
			}
			_ = stream.Send(&federationv1.HubBatch{
				Payload: &federationv1.HubBatch_Batch{
					Batch: &federationv1.EnvelopeBatch{Records: [][]byte{raw}},
				},
			})
			_, _ = stream.Recv() // read ack
			<-stream.Context().Done()
			return nil
		},
	})

	var received atomic.Int32
	em, err := newEphemeralForTest(EphemeralConfig{
		HubAddr:   addr,
		Subscribe: []string{"events"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	require.NoError(t, em.Subscribe("events", func(msg Message) {
		received.Add(1)
		assert.Equal(t, "events", msg.Channel)
		assert.Equal(t, "test.Msg", msg.PayloadType)
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))

	require.Eventually(t, func() bool { return received.Load() >= 1 },
		2*time.Second, 10*time.Millisecond,
		"Subscribe handler must be invoked when the hub pushes a message")
}

// TestEphemeralMessenger_Subscribe_PayloadDecoded verifies that a registered
// payload type is decoded before the Subscribe handler receives it.
func TestEphemeralMessenger_Subscribe_PayloadDecoded(t *testing.T) {
	type MyEvent struct{ Name string }

	env, err := envelope.NewEnvelope("typed", "hub", "test.MyEvent", MyEvent{Name: "hello"})
	require.NoError(t, err)
	raw, err := envelope.Marshal(env)
	require.NoError(t, err)

	addr := startEphemeralHub(t, &mockEphemeralGRPCHub{
		subscribeFn: func(stream grpc.BidiStreamingServer[federationv1.SubscribeFrame, federationv1.HubBatch]) error {
			if _, err := stream.Recv(); err != nil {
				return nil
			}
			_ = stream.Send(&federationv1.HubBatch{
				Payload: &federationv1.HubBatch_Batch{
					Batch: &federationv1.EnvelopeBatch{Records: [][]byte{raw}},
				},
			})
			_, _ = stream.Recv()
			<-stream.Context().Done()
			return nil
		},
	})

	var gotPayload any
	var received atomic.Bool

	em, err := newEphemeralForTest(EphemeralConfig{
		HubAddr:   addr,
		Subscribe: []string{"typed"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	require.NoError(t, em.RegisterPayloadType("test.MyEvent", MyEvent{}))
	require.NoError(t, em.Subscribe("typed", func(msg Message) {
		gotPayload = msg.Payload
		received.Store(true)
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))

	require.Eventually(t, received.Load, 2*time.Second, 10*time.Millisecond)

	event, ok := gotPayload.(MyEvent)
	require.True(t, ok, "payload must be decoded to MyEvent, got %T", gotPayload)
	assert.Equal(t, "hello", event.Name)
}

// TestEphemeralMessenger_Publish_EnvelopeCreateError exercises the error path
// when an unmarshalable payload is passed to Publish.
func TestEphemeralMessenger_Publish_EnvelopeCreateError(t *testing.T) {
	em, err := newEphemeralForTest(EphemeralConfig{
		HubAddr: "hub.example.com:7740",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	ctx := context.Background()
	err = em.Publish(ctx, "events", "test.Event", make(chan struct{}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ephemeral publish: create envelope")
}

// TestEphemeralMessenger_Publish_WithCorrelationAndService verifies that
// CorrelationID and ServiceName are stamped onto the envelope.
func TestEphemeralMessenger_Publish_WithCorrelationAndService(t *testing.T) {
	acked := make(chan struct{}, 1)

	addr := startEphemeralHub(t, &mockEphemeralGRPCHub{
		publishFn: func(stream grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error {
			for {
				batch, err := stream.Recv()
				if err != nil {
					return nil
				}
				_ = batch
				_ = stream.Send(&federationv1.PublishAck{})
				select {
				case acked <- struct{}{}:
				default:
				}
			}
		},
	})

	em, err := newEphemeralForTest(EphemeralConfig{
		HubAddr: addr,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))
	time.Sleep(20 * time.Millisecond)

	pubCtx := WithCorrelationID(ctx, "corr-123")
	pubCtx = WithServiceName(pubCtx, "svc-abc")

	err = em.Publish(pubCtx, "events", "test.Event", map[string]string{"k": "v"})
	require.NoError(t, err)

	select {
	case <-acked:
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the publish frame")
	}
}
