package federation

import (
	"bytes"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/dedup"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
)

// ---- helpers ----------------------------------------------------------------

// newReconnectClient builds a sender-only Client with explicit reconnect timing
// and registers it for cleanup. fakeAuditLogger2 is defined in
// client_integration_test.go (same package).
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
		nil, // sender-only: no inbound localWriter
		dd, &fakeAuditLogger2{}, log,
		100, 65536,
		reconnBase, reconnMax, jitter,
		nil, nil,
	)
	t.Cleanup(c.Close)
	return c
}

// srvAddr strips the "http://" scheme from an httptest.Server URL to produce
// the host:port form that Client.dial expects.
func srvAddr(rawURL string) string {
	return strings.TrimPrefix(rawURL, "http://")
}

// ---- tests ------------------------------------------------------------------

// TestClient_ConnectWithReconnect_ReplayUnacked verifies that messages in the
// sender's unacked window are replayed in full on the next successful reconnect,
// exercising the replay loop at lines 221–223 of client.go and the outer
// disconnect-detect / backoff-sleep path (lines 154–185).
func TestClient_ConnectWithReconnect_ReplayUnacked(t *testing.T) {
	var connIdx atomic.Int32
	replayed := make(chan string, 20)

	srv := mockFedServer(t, func(conn *Conn) {
		idx := int(connIdx.Add(1))
		defer func() { _ = conn.Close() }()

		// All connections begin with the standard hub handshake.
		if _, err := ReceiveHandshake(conn); err != nil {
			return
		}
		if err := SendHandshake(conn, HandshakeMsg{
			InstanceName: "hub", Role: "hub", Version: "1",
		}); err != nil {
			return
		}

		if idx == 1 {
			// First connection: consume the first binary frame the sender delivers
			// but never ack it, then close to simulate a mid-transfer failure.
			// PeerSender.run() is blocked at receiveAck(); closing the connection
			// returns an error there and fires sender.Done().
			for {
				msgType, _, err := conn.ReadMessage()
				if err != nil || msgType == MsgTypeBinary {
					return // close without sending any ack
				}
			}
		}

		// Second connection: receive replayed frames, ack each batch, and forward
		// the decoded IDs to the test goroutine.
		for {
			msgType, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if msgType != MsgTypeBinary {
				continue
			}
			records, _, _ := ReadFrame(bytes.NewReader(data), 65536)
			var lastID string
			for _, rec := range records {
				if env, unmarshalErr := envelope.Unmarshal(rec); unmarshalErr == nil {
					select {
					case replayed <- env.ID:
					default:
					}
					lastID = env.ID
				}
			}
			_ = SendAck(conn, AckMsg{LastID: lastID})
		}
	})
	defer srv.Close()

	// Zero backoff so the reconnect is instant.
	client := newReconnectClient(t, &testutil.FakeLogger{}, 0, 0, 0)
	require.NoError(t, client.ConnectWithReconnect(srvAddr(srv.URL)))

	// Enqueue 3 messages on the first sender. The sender delivers them in a
	// batch and then blocks at receiveAck() — when connection 1 closes, that
	// unblocks receiveAck() with an error and fires sender.Done().
	sender := client.Sender()
	require.NotNil(t, sender)
	var sentIDs []string
	for i := 0; i < 3; i++ {
		env, envErr := envelope.NewEnvelope("orders", "test", "t", map[string]int{"n": i})
		require.NoError(t, envErr)
		sender.Enqueue(&env)
		sentIDs = append(sentIDs, env.ID)
	}

	// Collect the IDs that arrive on the second connection.
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
	}, 5*time.Second, 5*time.Millisecond,
		"all sent messages must be replayed on the second connection")

	assert.ElementsMatch(t, sentIDs, gotIDs[:len(sentIDs)],
		"replayed IDs must exactly match the originally sent IDs")
	assert.GreaterOrEqual(t, int(connIdx.Load()), 2,
		"at least two connections must have been made")
}

// TestClient_ConnectWithReconnect_BackoffOnFailedAttempts verifies the inner
// retry loop at lines 189–218 of client.go: when consecutive dial attempts
// fail, each failure is logged at Error level, the backoff doubles, and the
// loop eventually recovers once the hub becomes reachable.
func TestClient_ConnectWithReconnect_BackoffOnFailedAttempts(t *testing.T) {
	const rejectCount = 2 // reconnect attempts to reject before accepting

	var connIdx atomic.Int32
	log := &testutil.FakeLogger{}
	replayed := make(chan string, 10)

	srv := mockFedServer(t, func(conn *Conn) {
		idx := int(connIdx.Add(1))
		defer func() { _ = conn.Close() }()

		if idx == 1 {
			// First connection: full handshake, consume the binary frame without
			// acking, then close. This puts the message into sender.Unacked() and
			// fires sender.Done() (receiveAck returns an error on connection close).
			if _, err := ReceiveHandshake(conn); err != nil {
				return
			}
			_ = SendHandshake(conn, HandshakeMsg{InstanceName: "hub", Role: "hub", Version: "1"})
			for {
				msgType, _, err := conn.ReadMessage()
				if err != nil || msgType == MsgTypeBinary {
					return
				}
			}
		}

		if idx <= rejectCount+1 {
			// Rejection: receive the client handshake, then close without sending a
			// hub handshake response so that ReceiveHandshake inside Client.dial
			// returns an error.
			_, _ = ReceiveHandshake(conn)
			return
		}

		// Successful reconnect: full handshake, receive the replayed message, ack.
		if _, err := ReceiveHandshake(conn); err != nil {
			return
		}
		_ = SendHandshake(conn, HandshakeMsg{InstanceName: "hub", Role: "hub", Version: "1"})
		for {
			msgType, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if msgType != MsgTypeBinary {
				continue
			}
			records, _, _ := ReadFrame(bytes.NewReader(data), 65536)
			var lastID string
			for _, rec := range records {
				if env, unmarshalErr := envelope.Unmarshal(rec); unmarshalErr == nil {
					select {
					case replayed <- env.ID:
					default:
					}
					lastID = env.ID
				}
			}
			_ = SendAck(conn, AckMsg{LastID: lastID})
		}
	})
	defer srv.Close()

	// Non-zero backoff so that the doubling logic at line 206 executes with real
	// (non-zero) values and the inner backoff sleeps actually fire.
	client := newReconnectClient(t, log, 5*time.Millisecond, 50*time.Millisecond, 0)
	require.NoError(t, client.ConnectWithReconnect(srvAddr(srv.URL)))

	// Enqueue one message to unblock PeerSender so it will detect the disconnect.
	sender := client.Sender()
	require.NotNil(t, sender)
	env, err := envelope.NewEnvelope("ch", "test", "t", map[string]string{"k": "v"})
	require.NoError(t, err)
	sender.Enqueue(&env)
	sentID := env.ID

	// Wait for the replayed message to arrive on the final successful connection.
	var gotID string
	require.Eventually(t, func() bool {
		select {
		case id := <-replayed:
			gotID = id
			return true
		default:
			return false
		}
	}, 5*time.Second, 5*time.Millisecond,
		"message must be replayed on the final successful connection after retries")

	assert.Equal(t, sentID, gotID, "replayed ID must match the originally sent ID")
	assert.True(t, log.HasError("client reconnect failed"),
		"each failed reconnect attempt must be logged at Error level")
	assert.True(t, log.HasWarn("client disconnected, reconnecting"),
		"initial disconnect must be logged at Warn level")
}
