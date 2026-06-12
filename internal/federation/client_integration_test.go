//go:build integration

package federation

import (
	"io"
	"net"
	"sync/atomic"
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

// fakeAuditLogger2 for client tests (unexported; used across client test files).
type fakeAuditLogger2 struct{}

func (f *fakeAuditLogger2) Log(_ audit.Event) error { return nil }
func (f *fakeAuditLogger2) Close() error            { return nil }

// mockFedServer implements FederationServiceServer for client integration tests.
// The publishFn handler is invoked for every incoming Publish stream.
// If publishFn is nil, all batches are acked immediately.
type mockFedServer struct {
	federationv1.UnimplementedFederationServiceServer
	publishFn   func(grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error
	subscribeFn func(grpc.BidiStreamingServer[federationv1.SubscribeFrame, federationv1.HubBatch]) error
}

func (s *mockFedServer) Publish(stream grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error {
	if s.publishFn != nil {
		return s.publishFn(stream)
	}
	for {
		_, err := stream.Recv()
		if err != nil {
			return nil
		}
		if sendErr := stream.Send(&federationv1.PublishAck{}); sendErr != nil {
			return sendErr
		}
	}
}

func (s *mockFedServer) Subscribe(stream grpc.BidiStreamingServer[federationv1.SubscribeFrame, federationv1.HubBatch]) error {
	if s.subscribeFn != nil {
		return s.subscribeFn(stream)
	}
	// Default: read SubscribeRequest then block until stream ends.
	_, _ = stream.Recv()
	<-stream.Context().Done()
	return nil
}

// startMockServer starts a gRPC server with srv registered and returns its addr.
func startMockServer(t *testing.T, srv *mockFedServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcSrv := grpc.NewServer()
	federationv1.RegisterFederationServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis) //nolint:errcheck
	t.Cleanup(grpcSrv.Stop)
	return lis.Addr().String()
}

// newTestClient creates a sender-only Client suitable for integration tests.
func newTestClient(log *testutil.FakeLogger, subscribeChannels []string) *Client {
	dd, _ := dedup.NewLRUDedup(100)
	return NewClient(
		"test-client", nil, NewAtomicPolicy(ForwardPolicy{}),
		func(_ *envelope.Envelope) error { return nil },
		dd, &fakeAuditLogger2{}, log,
		100, 65536,
		100*time.Millisecond, 500*time.Millisecond, 0.1,
		subscribeChannels, nil,
	)
}

// ---- tests ------------------------------------------------------------------

func TestClient_Dial_Success(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	connected := make(chan struct{}, 1)
	addr := startMockServer(t, &mockFedServer{
		publishFn: func(stream grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error {
			connected <- struct{}{}
			for {
				_, err := stream.Recv()
				if err != nil {
					return nil
				}
				_ = stream.Send(&federationv1.PublishAck{})
			}
		},
	})

	client := newTestClient(log, nil)
	defer client.Close()

	sender, err := client.Dial(addr)
	assert.NoError(t, err)
	assert.NotNil(t, sender)

	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("server never saw connection")
	}
	sender.Close()
}

func TestClient_Sender_AfterDial(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	addr := startMockServer(t, &mockFedServer{})

	client := newTestClient(log, nil)
	defer client.Close()

	assert.Nil(t, client.Sender())

	sender, err := client.Dial(addr)
	require.NoError(t, err)
	assert.Equal(t, sender, client.Sender())
	sender.Close()
}

func TestClient_ConnectWithReconnect_InitialSuccess(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	var connCount atomic.Int32
	addr := startMockServer(t, &mockFedServer{
		publishFn: func(stream grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error {
			connCount.Add(1)
			for {
				_, err := stream.Recv()
				if err != nil {
					return nil
				}
				_ = stream.Send(&federationv1.PublishAck{})
			}
		},
	})

	client := newTestClient(log, nil)
	defer client.Close()

	err := client.ConnectWithReconnect(addr)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	assert.GreaterOrEqual(t, connCount.Load(), int32(1))
}

func TestClient_Close_StopsReconnectLoop(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	addr := startMockServer(t, &mockFedServer{})

	client := newTestClient(log, nil)

	err := client.ConnectWithReconnect(addr)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	client.Close()
	elapsed := time.Since(start)
	assert.Less(t, elapsed, 2*time.Second)
}

func TestClient_Dial_WithoutLocalWriter(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	addr := startMockServer(t, &mockFedServer{})

	dd, _ := dedup.NewLRUDedup(100)
	client := NewClient(
		"sender-client", nil, NewAtomicPolicy(ForwardPolicy{}),
		nil, // no localWriter
		dd, &fakeAuditLogger2{}, log,
		100, 65536,
		500*time.Millisecond, 60*time.Second, 0.2,
		nil, nil,
	)
	defer client.Close()

	sender, err := client.Dial(addr)
	assert.NoError(t, err)
	assert.NotNil(t, sender)
	sender.Close()
}

func TestClient_Dial_WithSubscriptions(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	var receivedSubscribe []string
	gotSubscribe := make(chan struct{}, 1)

	addr := startMockServer(t, &mockFedServer{
		subscribeFn: func(stream grpc.BidiStreamingServer[federationv1.SubscribeFrame, federationv1.HubBatch]) error {
			frame, err := stream.Recv()
			if err != nil {
				return nil
			}
			if req := frame.GetRequest(); req != nil {
				receivedSubscribe = req.Subscribe
				gotSubscribe <- struct{}{}
			}
			<-stream.Context().Done()
			return nil
		},
	})

	dd, _ := dedup.NewLRUDedup(100)
	client := NewClient(
		"test-client", nil, NewAtomicPolicy(ForwardPolicy{}),
		func(_ *envelope.Envelope) error { return nil },
		dd, &fakeAuditLogger2{}, log,
		100, 65536,
		500*time.Millisecond, 60*time.Second, 0.2,
		[]string{"chan1", "chan2", "chan3"}, nil,
	)
	defer client.Close()

	sender, err := client.Dial(addr)
	require.NoError(t, err)

	select {
	case <-gotSubscribe:
		assert.Equal(t, []string{"chan1", "chan2", "chan3"}, receivedSubscribe)
	case <-time.After(2 * time.Second):
		t.Fatal("server never received SubscribeRequest")
	}
	sender.Close()
}

func TestClient_Dial_NoLastID(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	var receivedLastID string
	gotSubscribe := make(chan struct{}, 1)

	addr := startMockServer(t, &mockFedServer{
		subscribeFn: func(stream grpc.BidiStreamingServer[federationv1.SubscribeFrame, federationv1.HubBatch]) error {
			frame, err := stream.Recv()
			if err != nil {
				return nil
			}
			if req := frame.GetRequest(); req != nil {
				receivedLastID = req.LastId
				gotSubscribe <- struct{}{}
			}
			<-stream.Context().Done()
			return nil
		},
	})

	dd, _ := dedup.NewLRUDedup(100)
	client := NewClient(
		"test-client", nil, NewAtomicPolicy(ForwardPolicy{}),
		func(_ *envelope.Envelope) error { return nil },
		dd, &fakeAuditLogger2{}, log,
		100, 65536,
		500*time.Millisecond, 60*time.Second, 0.2,
		[]string{"events"}, nil,
	)
	defer client.Close()

	sender, err := client.Dial(addr)
	require.NoError(t, err)

	select {
	case <-gotSubscribe:
		assert.Empty(t, receivedLastID, "client must not send LastID on fresh connect")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for subscribe request")
	}
	sender.Close()
}

func TestClient_Publish_Batching(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	var batchCount atomic.Int32
	gotMessages := make(chan struct{}, 1)

	addr := startMockServer(t, &mockFedServer{
		publishFn: func(stream grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error {
			for {
				batch, err := stream.Recv()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return nil
				}
				batchCount.Add(1)
				_ = batch
				_ = stream.Send(&federationv1.PublishAck{})
				select {
				case gotMessages <- struct{}{}:
				default:
				}
			}
		},
	})

	client := newTestClient(log, nil)
	defer client.Close()

	sender, err := client.Dial(addr)
	require.NoError(t, err)
	defer sender.Close()

	for i := 0; i < 5; i++ {
		env, _ := envelope.NewEnvelope("ch", "src", "t", map[string]any{"i": i})
		sender.Enqueue(&env)
	}

	require.Eventually(t, func() bool {
		select {
		case <-gotMessages:
			return true
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond)
}

func TestClient_PeerConnected_AuditEvent(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	auditL := &recordingAudit{}

	addr := startMockServer(t, &mockFedServer{})

	dd, _ := dedup.NewLRUDedup(100)
	client := NewClient(
		"test-client", nil, NewAtomicPolicy(ForwardPolicy{}),
		nil, dd, auditL, log,
		100, 65536,
		500*time.Millisecond, 60*time.Second, 0.2,
		nil, nil,
	)
	defer client.Close()

	sender, err := client.Dial(addr)
	require.NoError(t, err)
	defer sender.Close()

	require.Eventually(t, func() bool {
		auditL.mu.Lock()
		defer auditL.mu.Unlock()
		for _, e := range auditL.events {
			if e.Event == audit.EventPeerConnected {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond)
}
