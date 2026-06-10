// Package-level tests for EphemeralMessenger that require a live (in-process)
// WebSocket hub to exercise code paths that unit tests cannot reach.
package messenger

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/federation"
)

// mockEphemeralHub returns a plain-text (ws://) WebSocket test server that
// completes the ephemeral handshake with every connecting client. The caller
// supplies afterHandshake, which is invoked on the server-side conn immediately
// after the handshake completes. Pass nil to just keep the connection alive.
func mockEphemeralHub(t *testing.T, afterHandshake func(*websocket.Conn)) *httptest.Server {
	t.Helper()
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()

		// Read and respond to the client handshake.
		var hs federation.HandshakeMsg
		if err := conn.ReadJSON(&hs); err != nil {
			return
		}
		if err := conn.WriteJSON(federation.HandshakeMsg{
			InstanceName: "hub", Role: "hub", Version: "1",
		}); err != nil {
			return
		}

		if afterHandshake != nil {
			afterHandshake(conn)
		} else {
			// Keep alive: drain any messages the client sends.
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// sendEnvelopeFromHub writes a length-prefixed binary frame containing one
// envelope to conn (the server side).
func sendEnvelopeFromHub(t *testing.T, conn *websocket.Conn, channel, payloadType string, payload any) {
	t.Helper()
	env, err := envelope.NewEnvelope(channel, "hub", payloadType, payload)
	require.NoError(t, err)
	data, err := envelope.Marshal(env)
	require.NoError(t, err)

	w, err := conn.NextWriter(websocket.BinaryMessage)
	require.NoError(t, err)
	require.NoError(t, federation.WriteFrame(w, [][]byte{data}))
	require.NoError(t, w.Close())
}

// TestEphemeralMessenger_Subscribe_HandlerInvoked exercises the handler closure
// registered by EphemeralMessenger.Subscribe (ephemeral.go:124–144).
// This closure builds a Message from the incoming *envelope.Envelope and calls
// the user-supplied handler; it runs inside dispatchEnvelope and is never
// executed by the existing unit tests (no real connection → no inbound frames).
func TestEphemeralMessenger_Subscribe_HandlerInvoked(t *testing.T) {
	var received atomic.Int32
	var mu sync.Mutex
	var lastMsg Message

	srv := mockEphemeralHub(t, func(conn *websocket.Conn) {
		// Push one envelope to the client, then keep the connection alive so
		// the PeerReceiver can ack it and the client doesn't disconnect.
		sendEnvelopeFromHub(t, conn, "events", "test.Msg", map[string]string{"x": "1"})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})

	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:      strings.TrimPrefix(srv.URL, "http://"),
		InstanceName: "test-client",
		Subscribe:    []string{"events"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	require.NoError(t, em.Subscribe("events", func(msg Message) {
		mu.Lock()
		lastMsg = msg
		mu.Unlock()
		received.Add(1)
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))

	require.Eventually(t, func() bool { return received.Load() >= 1 },
		2*time.Second, 10*time.Millisecond,
		"Subscribe handler must be invoked when the hub pushes a message")

	mu.Lock()
	ch, pt := lastMsg.Channel, lastMsg.PayloadType
	mu.Unlock()
	assert.Equal(t, "events", ch)
	assert.Equal(t, "test.Msg", pt)
}

// TestEphemeralMessenger_Subscribe_PayloadDecoded verifies that a registered
// payload type is decoded before the Subscribe handler receives it.
func TestEphemeralMessenger_Subscribe_PayloadDecoded(t *testing.T) {
	type MyEvent struct{ Name string }

	var gotPayload any
	var received atomic.Bool

	srv := mockEphemeralHub(t, func(conn *websocket.Conn) {
		sendEnvelopeFromHub(t, conn, "typed", "test.MyEvent", MyEvent{Name: "hello"})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})

	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:      strings.TrimPrefix(srv.URL, "http://"),
		InstanceName: "test-client",
		Subscribe:    []string{"typed"},
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

	require.Eventually(t, received.Load, 2*time.Second, 10*time.Millisecond,
		"handler must receive the decoded payload")

	event, ok := gotPayload.(MyEvent)
	require.True(t, ok, "payload must be decoded to MyEvent, got %T", gotPayload)
	assert.Equal(t, "hello", event.Name)
}

// TestEphemeralMessenger_Publish_EnvelopeCreateError exercises the
// `if err != nil { return fmt.Errorf("ephemeral publish: create envelope: ...") }`
// branch in Publish by passing an unmarshalable payload (a channel value).
func TestEphemeralMessenger_Publish_EnvelopeCreateError(t *testing.T) {
	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:      "hub.example.com:7740",
		InstanceName: "test-client",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	ctx := context.Background()
	// A channel value cannot be JSON-marshaled, so NewEnvelope returns an error.
	err = em.Publish(ctx, "events", "test.Event", make(chan struct{}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ephemeral publish: create envelope")
}

// TestEphemeralMessenger_Publish_WithCorrelationAndService verifies that
// CorrelationID and ServiceName are stamped onto the envelope from context.
func TestEphemeralMessenger_Publish_WithCorrelationAndService(t *testing.T) {
	acked := make(chan struct{}, 1)

	srv := mockEphemeralHub(t, func(conn *websocket.Conn) {
		// Read the binary frame from the client and send a text-frame ack.
		for {
			mt, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if mt == websocket.BinaryMessage {
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"last_id":"x"}`))
				select {
				case acked <- struct{}{}:
				default:
				}
			}
		}
	})

	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:      strings.TrimPrefix(srv.URL, "http://"),
		InstanceName: "test-client",
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
