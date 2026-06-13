//go:build integration

package federation

import (
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
	"github.com/wu/keyop-messenger/internal/dedup"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
	"google.golang.org/grpc"
)

// newReconnectClient builds a sender-only Client with explicit reconnect timing.
func newReconnectClient(
	t *testing.T,
	log *testutil.FakeLogger,
	reconnBase, reconnMax time.Duration,
	jitter float64,
) *Client {
	t.Helper()
	dd, err := dedup.NewLRUDedup(1000)
	require.NoError(t, err)
	c := NewClient(
		"test-client", nil, NewAtomicPolicy(ForwardPolicy{}),
		nil,
		dd, &fakeAuditLogger2{}, log,
		100, 65536,
		reconnBase, reconnMax, jitter,
		nil, nil,
	)
	t.Cleanup(c.Close)
	return c
}

// TestClient_ConnectWithReconnect_ReplayUnacked verifies that messages in the
// unacked window are replayed on the next successful reconnect.
func TestClient_ConnectWithReconnect_ReplayUnacked(t *testing.T) {
	var connIdx atomic.Int32
	replayed := make(chan string, 20)

	addr := startMockServer(t, &mockFedServer{
		publishFn: func(stream grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error {
			idx := int(connIdx.Add(1))
			if idx == 1 {
				// First connection: read one batch but never ack — simulate mid-transfer failure.
				_, _ = stream.Recv()
				return nil
			}
			// Subsequent connections: ack everything and capture IDs.
			for {
				batch, err := stream.Recv()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return nil
				}
				var lastID string
				for _, rec := range batch.Records {
					env, unmarshalErr := envelope.Unmarshal(rec)
					if unmarshalErr == nil {
						select {
						case replayed <- env.ID:
						default:
						}
						lastID = env.ID
					}
				}
				_ = stream.Send(&federationv1.PublishAck{LastId: lastID})
			}
		},
	})

	client := newReconnectClient(t, &testutil.FakeLogger{}, 0, 0, 0)
	require.NoError(t, client.ConnectWithReconnect(addr))

	sender := client.Sender()
	require.NotNil(t, sender)
	var sentIDs []string
	for i := 0; i < 3; i++ {
		env, _ := envelope.NewEnvelope("orders", "test", "t", map[string]int{"n": i})
		sender.Enqueue(&env)
		sentIDs = append(sentIDs, env.ID)
	}

	var gotIDs []string
	require.Eventually(t, func() bool {
		for {
			select {
			case id := <-replayed:
				gotIDs = append(gotIDs, id)
			default:
				return len(gotIDs) >= len(sentIDs)
			}
		}
	}, 5*time.Second, 5*time.Millisecond)

	assert.ElementsMatch(t, sentIDs, gotIDs[:len(sentIDs)])
	assert.GreaterOrEqual(t, int(connIdx.Load()), 2)
}

// TestClient_ConnectWithReconnect_BackoffOnFailedAttempts verifies that the inner
// retry loop backs off and eventually recovers.
func TestClient_ConnectWithReconnect_BackoffOnFailedAttempts(t *testing.T) {
	const rejectCount = 2

	var connIdx atomic.Int32
	log := &testutil.FakeLogger{}
	replayed := make(chan string, 10)

	addr := startMockServer(t, &mockFedServer{
		publishFn: func(stream grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error {
			idx := int(connIdx.Add(1))
			if idx == 1 {
				// First connection: receive without acking (to trigger disconnect and unacked window).
				_, _ = stream.Recv()
				return nil
			}
			if idx <= rejectCount+1 {
				// Reject connections 2..N: return immediately without acking.
				return nil
			}
			// Final connection: receive replayed batches and ack.
			for {
				batch, err := stream.Recv()
				if err != nil {
					return nil
				}
				var lastID string
				for _, rec := range batch.Records {
					env, _ := envelope.Unmarshal(rec)
					select {
					case replayed <- env.ID:
					default:
					}
					lastID = env.ID
				}
				_ = stream.Send(&federationv1.PublishAck{LastId: lastID})
			}
		},
	})

	client := newReconnectClient(t, log, 5*time.Millisecond, 50*time.Millisecond, 0)
	require.NoError(t, client.ConnectWithReconnect(addr))

	env, _ := envelope.NewEnvelope("ch", "test", "t", nil)
	client.Sender().Enqueue(&env)
	sentID := env.ID

	var gotID string
	require.Eventually(t, func() bool {
		select {
		case id := <-replayed:
			gotID = id
			return true
		default:
			return false
		}
	}, 10*time.Second, 5*time.Millisecond)

	assert.Equal(t, sentID, gotID)
	assert.True(t, log.HasWarn("client disconnected, reconnecting"))
}
