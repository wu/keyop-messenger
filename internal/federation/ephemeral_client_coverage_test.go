package federation

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
)

// ---- shared helpers ---------------------------------------------------------

// doEphemeralHandshake completes the ephemeral client/hub handshake on conn.
func doEphemeralHandshake(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	var hs HandshakeMsg
	require.NoError(t, conn.ReadJSON(&hs))
	require.NoError(t, conn.WriteJSON(HandshakeMsg{
		InstanceName: "hub", Role: "hub", Version: "1",
	}))
}

// keepAlive drains messages from conn until it closes.
func keepAlive(conn *websocket.Conn) {
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

// pushFrame sends envs to the client as a single length-prefixed binary frame.
func pushFrame(t *testing.T, conn *websocket.Conn, envs ...*envelope.Envelope) {
	t.Helper()
	records := make([][]byte, 0, len(envs))
	for _, env := range envs {
		data, err := envelope.Marshal(*env)
		require.NoError(t, err)
		records = append(records, data)
	}
	w, err := conn.NextWriter(websocket.BinaryMessage)
	require.NoError(t, err)
	require.NoError(t, WriteFrame(w, records))
	require.NoError(t, w.Close())
}

// newEphemeralTestClient creates an EphemeralClient with minimal reconnect
// delay (so reconnect tests complete quickly) and registers Close via cleanup.
func newEphemeralTestClient(t *testing.T, log *testutil.FakeLogger) *EphemeralClient {
	t.Helper()
	ec := NewEphemeralClient(EphemeralClientConfig{
		InstanceName:  "test",
		ReconnectBase: 1 * time.Millisecond,
		ReconnectMax:  10 * time.Millisecond,
	}, log)
	t.Cleanup(ec.Close)
	return ec
}

// dialEphemeral connects ec to the mock server at srv.URL and waits briefly.
func dialEphemeral(t *testing.T, ec *EphemeralClient, srvURL string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, ec.Connect(ctx, strings.TrimPrefix(srvURL, "http://")))
	time.Sleep(20 * time.Millisecond)
}

// ---- noopAuditLogger --------------------------------------------------------

// TestNoopAuditLogger covers the two no-op methods that are invoked by
// PeerReceiver for every inbound message forwarded to the client.
func TestNoopAuditLogger(t *testing.T) {
	a := noopAuditLogger{}
	assert.NoError(t, a.Log(audit.Event{Event: "forward", Channel: "events"}))
	assert.NoError(t, a.Close())
}

// ---- dispatchEnvelope -------------------------------------------------------

// TestEphemeralClient_DispatchEnvelope_HandlerCalled verifies that a binary
// frame pushed by the hub is decoded and delivered to every registered handler.
// It also covers noopAuditLogger.Log, which PeerReceiver calls on EventForward.
func TestEphemeralClient_DispatchEnvelope_HandlerCalled(t *testing.T) {
	received := make(chan string, 4)

	srv := mockWsServer(t, func(conn *websocket.Conn) {
		defer func() { _ = conn.Close() }()
		doEphemeralHandshake(t, conn)

		env, err := envelope.NewEnvelope("events", "hub", "test.Msg", map[string]string{"k": "v"})
		require.NoError(t, err)
		pushFrame(t, conn, &env)

		keepAlive(conn)
	})
	defer srv.Close()

	log := &testutil.FakeLogger{}
	ec := newEphemeralTestClient(t, log)
	ec.AddHandler("events", func(env *envelope.Envelope) error {
		select {
		case received <- env.Channel:
		default:
		}
		return nil
	})

	dialEphemeral(t, ec, srv.URL)

	select {
	case ch := <-received:
		assert.Equal(t, "events", ch)
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not called within 2 s")
	}
}

// TestEphemeralClient_DispatchEnvelope_HandlerError verifies that a handler
// returning an error is logged at Warn but does not stop delivery of subsequent
// messages.
func TestEphemeralClient_DispatchEnvelope_HandlerError(t *testing.T) {
	var callCount atomic.Int32

	srv := mockWsServer(t, func(conn *websocket.Conn) {
		defer func() { _ = conn.Close() }()
		doEphemeralHandshake(t, conn)

		for i := 0; i < 2; i++ {
			env, err := envelope.NewEnvelope("events", "hub", "test.Msg", map[string]int{"n": i})
			require.NoError(t, err)
			pushFrame(t, conn, &env)
			time.Sleep(5 * time.Millisecond) // give writeLoop time to ack before next frame
		}

		keepAlive(conn)
	})
	defer srv.Close()

	log := &testutil.FakeLogger{}
	ec := newEphemeralTestClient(t, log)
	ec.AddHandler("events", func(_ *envelope.Envelope) error {
		callCount.Add(1)
		return fmt.Errorf("deliberate handler error")
	})

	dialEphemeral(t, ec, srv.URL)

	require.Eventually(t, func() bool { return callCount.Load() >= 2 },
		2*time.Second, 10*time.Millisecond, "both messages must be delivered despite handler errors")
	assert.True(t, log.HasWarn("ephemeral: handler error"),
		"handler error must be logged at Warn level")
}

// ---- writeLoop error paths --------------------------------------------------

// TestEphemeralClient_Publish_CloseWhileWaitingForAck verifies that calling
// Close() while a Publish is blocked waiting for a hub ack returns
// ErrEphemeralClosed (writeLoop path: select case <-c.stop after write).
func TestEphemeralClient_Publish_CloseWhileWaitingForAck(t *testing.T) {
	frameReceived := make(chan struct{}, 1)

	srv := mockWsServer(t, func(conn *websocket.Conn) {
		defer func() { _ = conn.Close() }()
		doEphemeralHandshake(t, conn)

		// Read the frame but never ack — Publish blocks indefinitely.
		for {
			mt, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if mt == websocket.BinaryMessage {
				select {
				case frameReceived <- struct{}{}:
				default:
				}
				keepAlive(conn) // drain remaining reads without acking
				return
			}
		}
	})
	defer srv.Close()

	ec := newEphemeralTestClient(t, &testutil.FakeLogger{})
	dialEphemeral(t, ec, srv.URL)

	env := &envelope.Envelope{Channel: "ch", ID: "msg1"}
	errCh := make(chan error, 1)
	go func() {
		pubCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		errCh <- ec.Publish(pubCtx, env)
	}()

	// Ensure the frame has reached the server before closing.
	select {
	case <-frameReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the binary frame")
	}

	ec.Close() // signals ErrEphemeralClosed to the waiting Publish

	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, ErrEphemeralClosed)
	case <-time.After(2 * time.Second):
		t.Fatal("Publish did not unblock after Close()")
	}
}

// TestEphemeralClient_Publish_ConnLostWhileWaitingForAck verifies that when
// the connection closes before the hub acks, Publish returns ErrEphemeralConnLost
// (writeLoop path: select case <-connDead after write, and ackCh !ok).
func TestEphemeralClient_Publish_ConnLostWhileWaitingForAck(t *testing.T) {
	srv := mockWsServer(t, func(conn *websocket.Conn) {
		defer func() { _ = conn.Close() }()
		doEphemeralHandshake(t, conn)

		// Read the frame, then close without acking.
		for {
			mt, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if mt == websocket.BinaryMessage {
				return // close → PeerReceiver exits → connDead fires
			}
		}
	})
	defer srv.Close()

	ec := newEphemeralTestClient(t, &testutil.FakeLogger{})
	dialEphemeral(t, ec, srv.URL)

	env := &envelope.Envelope{Channel: "ch", ID: "msg1"}
	pubCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := ec.Publish(pubCtx, env)
	assert.ErrorIs(t, err, ErrEphemeralConnLost)
}

// TestEphemeralClient_Publish_ContextCancelledWhileWaiting verifies that
// cancelling the context while Publish waits for an ack returns a wrapped
// context.Canceled error (Publish second-select ctx.Done() path).
func TestEphemeralClient_Publish_ContextCancelledWhileWaiting(t *testing.T) {
	srv := mockWsServer(t, func(conn *websocket.Conn) {
		defer func() { _ = conn.Close() }()
		doEphemeralHandshake(t, conn)
		// Read the frame but never ack; keep connection alive.
		for {
			mt, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if mt == websocket.BinaryMessage {
				keepAlive(conn)
				return
			}
		}
	})
	defer srv.Close()

	ec := newEphemeralTestClient(t, &testutil.FakeLogger{})
	dialEphemeral(t, ec, srv.URL)

	env := &envelope.Envelope{Channel: "ch", ID: "msg2"}
	pubCtx, pubCancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() { errCh <- ec.Publish(pubCtx, env) }()

	// Give writeLoop time to dequeue and send the frame.
	time.Sleep(50 * time.Millisecond)
	pubCancel() // cancel while Publish is at the second select

	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Publish did not return after context cancellation")
	}
}

// ---- reconnectLoop ----------------------------------------------------------

// TestEphemeralClient_ConnectWithReconnect_Reconnects verifies that when the
// hub closes the connection, reconnectLoop detects connLost and reconnects,
// logging both the disconnect warning and the success info.
func TestEphemeralClient_ConnectWithReconnect_Reconnects(t *testing.T) {
	var connIdx atomic.Int32
	log := &testutil.FakeLogger{}

	srv := mockWsServer(t, func(conn *websocket.Conn) {
		idx := int(connIdx.Add(1))
		defer func() { _ = conn.Close() }()
		doEphemeralHandshake(t, conn)

		if idx == 1 {
			// First connection: close quickly to trigger the reconnect loop.
			time.Sleep(10 * time.Millisecond)
			return
		}
		// Second connection: stay alive.
		keepAlive(conn)
	})
	defer srv.Close()

	ec := newEphemeralTestClient(t, log)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, ec.ConnectWithReconnect(ctx, strings.TrimPrefix(srv.URL, "http://")))

	require.Eventually(t, func() bool { return int(connIdx.Load()) >= 2 },
		3*time.Second, 10*time.Millisecond,
		"must establish at least two connections")

	assert.True(t, log.HasWarn("ephemeral: connection lost, reconnecting"))
	assert.True(t, log.HasInfo("ephemeral: reconnected"))
}

// TestEphemeralClient_ConnectWithReconnect_FailedAttemptThenSuccess verifies
// that when a reconnect attempt fails (server rejects it), reconnectLoop logs
// "reconnect failed", doubles the backoff, and eventually succeeds.
func TestEphemeralClient_ConnectWithReconnect_FailedAttemptThenSuccess(t *testing.T) {
	var connIdx atomic.Int32
	log := &testutil.FakeLogger{}

	srv := mockWsServer(t, func(conn *websocket.Conn) {
		idx := int(connIdx.Add(1))
		defer func() { _ = conn.Close() }()

		if idx == 1 {
			// First connection: complete handshake, close after a moment.
			doEphemeralHandshake(t, conn)
			time.Sleep(10 * time.Millisecond)
			return
		}
		if idx == 2 {
			// Rejection: consume the client handshake but close without responding,
			// so ReceiveHandshake inside startConn returns an error.
			var hs HandshakeMsg
			_ = conn.ReadJSON(&hs)
			return
		}
		// Third connection: successful reconnect.
		doEphemeralHandshake(t, conn)
		keepAlive(conn)
	})
	defer srv.Close()

	ec := newEphemeralTestClient(t, log)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, ec.ConnectWithReconnect(ctx, strings.TrimPrefix(srv.URL, "http://")))

	require.Eventually(t, func() bool { return int(connIdx.Load()) >= 3 },
		3*time.Second, 10*time.Millisecond,
		"must navigate two failures and then reconnect successfully")

	assert.True(t, log.HasError("ephemeral: reconnect failed"),
		"failed reconnect attempt must be logged at Error")
	assert.True(t, log.HasInfo("ephemeral: reconnected"))
}

// TestEphemeralClient_ConnectWithReconnect_CloseStopsLoop verifies that calling
// Close() while connected terminates the reconnect loop immediately via the
// outer select case <-c.stop path.
func TestEphemeralClient_ConnectWithReconnect_CloseStopsLoop(t *testing.T) {
	srv := mockWsServer(t, func(conn *websocket.Conn) {
		defer func() { _ = conn.Close() }()
		doEphemeralHandshake(t, conn)
		keepAlive(conn)
	})
	defer srv.Close()

	ec := newEphemeralTestClient(t, &testutil.FakeLogger{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, ec.ConnectWithReconnect(ctx, strings.TrimPrefix(srv.URL, "http://")))
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	ec.Close()
	assert.Less(t, time.Since(start), 2*time.Second, "Close must complete promptly")
}

// TestEphemeralClient_ConnectWithReconnect_CloseInterruptsBackoff verifies
// that Close() interrupts the backoff sleep in the reconnect loop's inner
// retry loop (select case <-c.stop while time.After is pending).
func TestEphemeralClient_ConnectWithReconnect_CloseInterruptsBackoff(t *testing.T) {
	var connIdx atomic.Int32

	srv := mockWsServer(t, func(conn *websocket.Conn) {
		idx := int(connIdx.Add(1))
		defer func() { _ = conn.Close() }()

		if idx == 1 {
			doEphemeralHandshake(t, conn)
			time.Sleep(10 * time.Millisecond)
			return // trigger reconnect
		}
		// Subsequent connections: reject to force a long backoff sleep.
		var hs HandshakeMsg
		_ = conn.ReadJSON(&hs)
	})
	defer srv.Close()

	ec := NewEphemeralClient(EphemeralClientConfig{
		InstanceName:  "test",
		ReconnectBase: 200 * time.Millisecond, // long enough that Close() interrupts it
		ReconnectMax:  1 * time.Second,
	}, &testutil.FakeLogger{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, ec.ConnectWithReconnect(ctx, strings.TrimPrefix(srv.URL, "http://")))

	// Wait until the rejection has been seen (connection 2 attempted).
	require.Eventually(t, func() bool { return int(connIdx.Load()) >= 2 },
		2*time.Second, 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond) // let the backoff sleep begin

	// Close() must unblock from the backoff sleep promptly.
	start := time.Now()
	ec.Close()
	assert.Less(t, time.Since(start), 1*time.Second,
		"Close must interrupt the backoff sleep")
}
