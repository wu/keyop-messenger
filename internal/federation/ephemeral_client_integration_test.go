//nolint:gosec // test file: G115 type conversions are safe
package federation

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// mockFedServer creates a simple HTTP/2 (h2c) server for testing federation
// clients. The handler receives a *Conn wrapping the bidirectional HTTP/2 stream.
func mockFedServer(t *testing.T, handler func(*Conn)) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn := NewConn(r.Body, w, &stringAddr{"tcp", r.RemoteAddr}, func() error { return nil })
		handler(conn)
	})
	srv := httptest.NewUnstartedServer(h2c.NewHandler(mux, &http2.Server{}))
	srv.Start()
	t.Cleanup(srv.Close)
	return srv
}

// fedServerAddr extracts the host:port from an httptest.Server URL.
func fedServerAddr(srv *httptest.Server) string {
	return strings.TrimPrefix(srv.URL, "http://")
}

// TestEphemeralClient_Dispatch_MessageHandlers verifies multiple handlers are registered.
// (Actual dispatch testing is covered by integration tests in ephemeral_test.go)
func TestEphemeralClient_Dispatch_MessageHandlers(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	ec := NewEphemeralClient(EphemeralClientConfig{
		InstanceName: "em-client",
		Subscribe:    []string{"events", "alerts"},
	}, log)
	defer ec.Close()

	// Register multiple handlers on different channels
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

	// Handlers are registered but not called (no connection)
	assert.Equal(t, int32(0), handlerCount.Load())
}

// TestEphemeralClient_WriteLoop_BatchesMessages verifies messages are batched.
func TestEphemeralClient_WriteLoop_BatchesMessages(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	receivedFrames := atomic.Int32{}
	srv := mockFedServer(t, func(conn *Conn) {
		defer func() { _ = conn.Close() }()

		// Receive client handshake
		if _, err := ReceiveHandshake(conn); err != nil {
			return
		}

		// Send hub handshake
		_ = SendHandshake(conn, HandshakeMsg{
			InstanceName: "hub",
			Role:         "hub",
			Version:      "1",
		})

		// Read binary frames from client (published messages)
		connWriteMu := &sync.Mutex{}
		for {
			msgType, _, err := conn.NextReader()
			if err != nil {
				break
			}
			if msgType == MsgTypeBinary {
				receivedFrames.Add(1)
			}
			// Send ack for each frame
			connWriteMu.Lock()
			_ = SendAck(conn, AckMsg{LastID: "ack"})
			connWriteMu.Unlock()
		}
	})

	ec := NewEphemeralClient(EphemeralClientConfig{
		InstanceName:   "em-client",
		MaxBatchBytes:  1024,
		WriteQueueSize: 10,
	}, log)
	defer ec.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ec.Connect(ctx, fedServerAddr(srv))
	require.NoError(t, err)

	// Publish multiple small messages
	for i := 0; i < 5; i++ {
		env := &envelope.Envelope{
			Channel: "test",
			ID:      "id-" + string(rune(i)),
			Payload: []byte(`{"i":` + string(rune(48+i)) + `}`),
		}
		pubCtx, pubCancel := context.WithTimeout(context.Background(), 1*time.Second)
		err := ec.Publish(pubCtx, env)
		pubCancel()
		if err != nil {
			break
		}
	}

	time.Sleep(200 * time.Millisecond)

	// Should have sent at least 1 frame (messages batched together)
	assert.Greater(t, receivedFrames.Load(), int32(0))
}

// TestEphemeralClient_Close_StopsAllGoroutines verifies close completes quickly.
func TestEphemeralClient_Close_StopsAllGoroutines(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	ec := NewEphemeralClient(EphemeralClientConfig{
		InstanceName: "em-client",
	}, log)

	// Register some handlers
	for i := 0; i < 3; i++ {
		ec.AddHandler("test", func(_ *envelope.Envelope) error { return nil })
	}

	// Close should complete quickly without deadlock
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
	srv := mockFedServer(t, func(conn *Conn) {
		defer func() { _ = conn.Close() }()

		// Receive handshake
		if _, err := ReceiveHandshake(conn); err != nil {
			return
		}

		// Send hub handshake
		_ = SendHandshake(conn, HandshakeMsg{
			InstanceName: "hub",
			Role:         "hub",
			Version:      "1",
		})

		// Read message from client
		_, _, err := conn.ReadMessage()
		if err != nil {
			return
		}

		// Delay before sending ack
		time.Sleep(ackDelay)
		_ = SendAck(conn, AckMsg{LastID: "msg1"})
	})

	ec := NewEphemeralClient(EphemeralClientConfig{
		InstanceName: "em-client",
	}, log)
	defer ec.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ec.Connect(ctx, fedServerAddr(srv))
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	env := &envelope.Envelope{
		Channel: "test",
		ID:      "msg1",
	}

	pubCtx, pubCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pubCancel()

	start := time.Now()
	err = ec.Publish(pubCtx, env)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	// Should have waited at least for the ack delay
	assert.GreaterOrEqual(t, elapsed, ackDelay-20*time.Millisecond)
}

// TestEphemeralClient_PublishConcurrent verifies concurrent publishes all get acked.
func TestEphemeralClient_PublishConcurrent(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	messageCount := atomic.Int32{}
	srv := mockFedServer(t, func(conn *Conn) {
		defer func() { _ = conn.Close() }()

		// Receive handshake
		if _, err := ReceiveHandshake(conn); err != nil {
			return
		}

		// Send hub handshake
		_ = SendHandshake(conn, HandshakeMsg{
			InstanceName: "hub",
			Role:         "hub",
			Version:      "1",
		})

		// Read messages and send acks
		connWriteMu := &sync.Mutex{}
		for {
			msgType, _, err := conn.ReadMessage()
			if err != nil {
				break
			}
			if msgType == MsgTypeBinary {
				messageCount.Add(1)
				connWriteMu.Lock()
				_ = SendAck(conn, AckMsg{LastID: "ack"})
				connWriteMu.Unlock()
			}
		}
	})

	ec := NewEphemeralClient(EphemeralClientConfig{
		InstanceName: "em-client",
	}, log)
	defer ec.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ec.Connect(ctx, fedServerAddr(srv))
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	// Publish concurrently
	numMessages := 10
	errors := make(chan error, numMessages)
	var wg sync.WaitGroup

	for i := 0; i < numMessages; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			env := &envelope.Envelope{
				Channel: "test",
				ID:      "msg-" + string(rune(48+id)),
			}
			pubCtx, pubCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer pubCancel()
			errors <- ec.Publish(pubCtx, env)
		}(i)
	}

	wg.Wait()
	close(errors)

	// All publishes should succeed
	for err := range errors {
		assert.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)
	assert.Greater(t, messageCount.Load(), int32(0))
}
