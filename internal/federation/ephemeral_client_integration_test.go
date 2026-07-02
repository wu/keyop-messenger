//go:build integration

package federation

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newEphemeralTestClient creates an EphemeralClient with minimal reconnect delay.
func newEphemeralTestClient(t *testing.T, log *testutil.FakeLogger, subscribe ...string) *EphemeralClient {
	t.Helper()
	ec := NewEphemeralClient(EphemeralClientConfig{
		InstanceName:  "test",
		ReconnectBase: 1 * time.Millisecond,
		ReconnectMax:  10 * time.Millisecond,
		Subscribe:     subscribe,
	}, log)
	t.Cleanup(ec.Close)
	return ec
}

func dialEphemeral(t *testing.T, ec *EphemeralClient, addr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, ec.Connect(ctx, addr))
	time.Sleep(20 * time.Millisecond)
}

// TestEphemeralClient_Dispatch_MessageHandlers verifies multiple handlers registered.
func TestEphemeralClient_Dispatch_MessageHandlers(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	ec := NewEphemeralClient(EphemeralClientConfig{
		InstanceName: "em-client",
		Subscribe:    []string{"events", "alerts"},
	}, log)
	defer ec.Close()

	handlerCount := atomic.Int32{}
	for i := 0; i < 5; i++ {
		ec.AddHandler("events", func(_ *envelope.Envelope) error {
			handlerCount.Add(1)
			return nil
		})
	}
	for i := 0; i < 3; i++ {
		ec.AddHandler("alerts", func(_ *envelope.Envelope) error {
			handlerCount.Add(1)
			return nil
		})
	}

	assert.Equal(t, int32(0), handlerCount.Load())
}

// TestEphemeralClient_WriteLoop_BatchesMessages verifies messages are batched.
func TestEphemeralClient_WriteLoop_BatchesMessages(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	receivedFrames := atomic.Int32{}
	addr := startMockServer(t, &mockFedServer{
		publishFn: func(stream grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error {
			for {
				_, err := stream.Recv()
				if err != nil {
					return nil
				}
				receivedFrames.Add(1)
				_ = stream.Send(&federationv1.PublishAck{})
			}
		},
	})

	ec := NewEphemeralClient(EphemeralClientConfig{
		InstanceName:   "em-client",
		MaxBatchBytes:  1024,
		WriteQueueSize: 10,
	}, log)
	defer ec.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ec.Connect(ctx, addr)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		env := &envelope.Envelope{
			Channel: "test",
			ID:      "id-" + string(rune(i+48)),
			Payload: []byte(`{}`),
		}
		pubCtx, pubCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = ec.Publish(pubCtx, env)
		pubCancel()
	}

	time.Sleep(200 * time.Millisecond)
	assert.Greater(t, receivedFrames.Load(), int32(0))
}

// TestEphemeralClient_ReconnectStopsOnFatalRejection verifies that when the hub
// rejects the client's identity (PermissionDenied), the reconnect loop invokes
// OnFatal with the error and stops retrying instead of looping forever.
func TestEphemeralClient_ReconnectStopsOnFatalRejection(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	var pubAttempts atomic.Int32
	reject := func() error {
		return status.Error(codes.PermissionDenied, "not in allowlist")
	}
	addr := startMockServer(t, &mockFedServer{
		publishFn: func(_ grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error {
			pubAttempts.Add(1)
			return reject()
		},
		subscribeFn: func(_ grpc.BidiStreamingServer[federationv1.SubscribeFrame, federationv1.HubBatch]) error {
			return reject()
		},
	})

	fatalCh := make(chan error, 1)
	ec := NewEphemeralClient(EphemeralClientConfig{
		InstanceName:  "rejected-client",
		Subscribe:     []string{"events"},
		ReconnectBase: 1 * time.Millisecond,
		ReconnectMax:  5 * time.Millisecond,
		OnFatal: func(err error) {
			select {
			case fatalCh <- err:
			default:
			}
		},
	}, log)
	t.Cleanup(ec.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, ec.ConnectWithReconnect(ctx, addr))

	select {
	case err := <-fatalCh:
		require.Error(t, err)
		assert.Equal(t, codes.PermissionDenied, status.Code(err),
			"OnFatal should receive the hub's PermissionDenied status")
	case <-time.After(3 * time.Second):
		t.Fatal("OnFatal was not called on a PermissionDenied rejection")
	}

	// The loop must have stopped: no further Publish connection attempts after the
	// fatal error, even after several reconnect-backoff intervals elapse.
	settled := pubAttempts.Load()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, settled, pubAttempts.Load(),
		"reconnect must stop after a fatal rejection, not keep retrying")
}

// TestEphemeralClient_Close_StopsAllGoroutines verifies close completes quickly.
func TestEphemeralClient_Close_StopsAllGoroutines(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	ec := NewEphemeralClient(EphemeralClientConfig{InstanceName: "em-client"}, log)

	for i := 0; i < 3; i++ {
		ec.AddHandler("test", func(_ *envelope.Envelope) error { return nil })
	}

	start := time.Now()
	ec.Close()
	elapsed := time.Since(start)
	assert.Less(t, elapsed, 500*time.Millisecond)
}

// TestEphemeralClient_Publish_BlocksUntilAck verifies publish blocks for ack.
func TestEphemeralClient_Publish_BlocksUntilAck(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	ackDelay := 100 * time.Millisecond
	addr := startMockServer(t, &mockFedServer{
		publishFn: func(stream grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error {
			_, err := stream.Recv()
			if err != nil {
				return nil
			}
			time.Sleep(ackDelay)
			_ = stream.Send(&federationv1.PublishAck{LastId: "msg1"})
			// drain remaining
			for {
				_, err := stream.Recv()
				if err != nil {
					return nil
				}
				_ = stream.Send(&federationv1.PublishAck{})
			}
		},
	})

	ec := NewEphemeralClient(EphemeralClientConfig{InstanceName: "em-client"}, log)
	defer ec.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ec.Connect(ctx, addr)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	env := &envelope.Envelope{Channel: "test", ID: "msg1"}
	pubCtx, pubCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pubCancel()

	start := time.Now()
	err = ec.Publish(pubCtx, env)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.GreaterOrEqual(t, elapsed, ackDelay-20*time.Millisecond)
}

// TestEphemeralClient_PublishConcurrent verifies concurrent publishes all get acked.
func TestEphemeralClient_PublishConcurrent(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	addr := startMockServer(t, &mockFedServer{}) // default: ack everything

	ec := NewEphemeralClient(EphemeralClientConfig{
		InstanceName:   "em-client",
		WriteQueueSize: 100,
	}, log)
	defer ec.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ec.Connect(ctx, addr)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	const n = 10
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			env := &envelope.Envelope{
				Channel: "test",
				ID:      fmt.Sprintf("concurrent-%d", i),
			}
			pubCtx, pubCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer pubCancel()
			errs <- ec.Publish(pubCtx, env)
		}()
	}

	for i := 0; i < n; i++ {
		assert.NoError(t, <-errs)
	}
}

// TestEphemeralClient_WriteLoop_BoundsBatchBytes verifies that when many items
// pile into the write queue, the write loop splits them into frames so that no
// multi-record PublishBatch exceeds the configured MaxBatchBytes, and that no
// item is dropped. Regression for ingest/egress ignoring MaxBatchBytes.
func TestEphemeralClient_WriteLoop_BoundsBatchBytes(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	const maxBatch = 1024

	var mu sync.Mutex
	var maxMultiRecordBytes int
	totalRecords := atomic.Int64{}

	addr := startMockServer(t, &mockFedServer{
		publishFn: func(stream grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error {
			for {
				batch, err := stream.Recv()
				if err != nil {
					return nil
				}
				sum := 0
				for _, rec := range batch.Records {
					sum += len(rec)
				}
				if len(batch.Records) > 1 {
					mu.Lock()
					if sum > maxMultiRecordBytes {
						maxMultiRecordBytes = sum
					}
					mu.Unlock()
				}
				totalRecords.Add(int64(len(batch.Records)))
				_ = stream.Send(&federationv1.PublishAck{})
			}
		},
	})

	ec := NewEphemeralClient(EphemeralClientConfig{
		InstanceName:   "em-client",
		MaxBatchBytes:  maxBatch,
		WriteQueueSize: 100,
	}, log)
	defer ec.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, ec.Connect(ctx, addr))
	time.Sleep(50 * time.Millisecond)

	// Each record (~300-byte payload) is comfortably under maxBatch on its own,
	// so the only way to exceed maxBatch is to pack several into one frame.
	const n = 24
	pad := strings.Repeat("x", 300)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			env, err := envelope.NewEnvelope("test", "em-client", "test.Type",
				map[string]any{"id": fmt.Sprintf("m-%d", i), "pad": pad})
			if err != nil {
				errs <- err
				return
			}
			pubCtx, pubCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer pubCancel()
			errs <- ec.Publish(pubCtx, &env)
		}()
	}

	for i := 0; i < n; i++ {
		assert.NoError(t, <-errs)
	}

	assert.Equal(t, int64(n), totalRecords.Load(), "every published item must be delivered")
	mu.Lock()
	defer mu.Unlock()
	assert.LessOrEqual(t, maxMultiRecordBytes, maxBatch,
		"no multi-record batch may exceed MaxBatchBytes")
	assert.Greater(t, maxMultiRecordBytes, 0,
		"test must actually exercise multi-record batching")
}
