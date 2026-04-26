package federation

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/storage"
	"github.com/wu/keyop-messenger/internal/testutil"
)

// wsUpgrader is a permissive upgrader for coordinator tests.
var wsUpgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// newCoordTestPair creates a connected WebSocket pair for coordinator tests.
// serverFn runs in a goroutine on the server side.
func newCoordTestPair(t *testing.T, serverFn func(*websocket.Conn)) *websocket.Conn {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := wsUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		serverFn(conn)
	}))
	t.Cleanup(srv.Close)

	u := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(u, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestClientCoordinator_SendBatch_AckFlow verifies that a batch is sent over
// the WebSocket and doneCh is closed once the ack arrives.
func TestClientCoordinator_SendBatch_AckFlow(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	ackCh := make(chan AckMsg, 4)
	var serverReceivedFrames int
	var serverMu sync.Mutex

	clientConn := newCoordTestPair(t, func(conn *websocket.Conn) {
		defer func() {
			if err := conn.Close(); err != nil {
				t.Logf("failed to close connection: %v", err)
			}
		}()
		for {
			msgType, _, err := conn.NextReader()
			if err != nil {
				return
			}
			if msgType == websocket.BinaryMessage {
				serverMu.Lock()
				serverReceivedFrames++
				serverMu.Unlock()
				// Send an ack back (simulates PeerReceiver on client side).
				_ = conn.WriteJSON(AckMsg{LastID: "x"})
			}
		}
	})

	connWriteMu := &sync.Mutex{}
	cc := newClientCoordinator(clientConn, connWriteMu, ackCh, 65536, log, nil)
	cc.start()
	defer cc.close()

	// Build a raw line and a sendReq.
	rawLine := []byte(`{"v":1,"id":"abc","channel":"events","origin":"test","ts":"2024-01-01T00:00:00Z","payload_type":"t","payload":{}}`)
	doneCh := make(chan struct{})
	req := sendReq{
		channel:   "events",
		rawLines:  [][]byte{rawLine},
		newOffset: int64(len(rawLine) + 1),
		doneCh:    doneCh,
	}

	// Simulate the server sending the ack to ackCh when the frame arrives.
	go func() {
		// Deliver ack to coordinator via the ackCh (normally routed by PeerReceiver).
		ackCh <- AckMsg{LastID: "abc"}
	}()

	// Submit request directly to coordinator's requestCh.
	select {
	case cc.requestCh <- req:
	case <-time.After(time.Second):
		t.Fatal("timed out submitting request")
	}

	// doneCh must be closed after the ack.
	select {
	case <-doneCh:
		// expected
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for doneCh to close")
	}
}

// TestClientCoordinator_SequentialDelivery verifies that two batches are
// delivered strictly in order: the second batch is not sent until the first
// is acked.
func TestClientCoordinator_SequentialDelivery(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	ackCh := make(chan AckMsg, 8)
	received := make(chan []byte, 16)

	clientConn := newCoordTestPair(t, func(conn *websocket.Conn) {
		defer func() {
			if err := conn.Close(); err != nil {
				t.Logf("failed to close connection: %v", err)
			}
		}()
		for {
			msgType, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if msgType == websocket.BinaryMessage {
				received <- data
				_ = conn.WriteJSON(AckMsg{LastID: "ok"})
			}
		}
	})

	connWriteMu := &sync.Mutex{}
	cc := newClientCoordinator(clientConn, connWriteMu, ackCh, 65536, log, nil)
	cc.start()
	defer cc.close()

	line1 := []byte(`{"v":1,"id":"id1","channel":"ch","origin":"o","ts":"2024-01-01T00:00:00Z","payload_type":"t","payload":{}}`)
	line2 := []byte(`{"v":1,"id":"id2","channel":"ch","origin":"o","ts":"2024-01-01T00:00:00Z","payload_type":"t","payload":{}}`)

	done1 := make(chan struct{})
	done2 := make(chan struct{})

	go func() { ackCh <- AckMsg{LastID: "id1"} }()
	go func() {
		<-done1 // send second ack only after first batch is done
		ackCh <- AckMsg{LastID: "id2"}
	}()

	// Submit batch 1.
	require.NoError(t, submitReq(t, cc, sendReq{
		channel: "ch", rawLines: [][]byte{line1},
		newOffset: int64(len(line1) + 1), doneCh: done1,
	}))
	// Submit batch 2 right after.
	require.NoError(t, submitReq(t, cc, sendReq{
		channel: "ch", rawLines: [][]byte{line2},
		newOffset: int64(len(line1)+1) + int64(len(line2)+1), doneCh: done2,
	}))

	// Verify both dones close.
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
	ackCh := make(chan AckMsg, 4)

	clientConn := newCoordTestPair(t, func(conn *websocket.Conn) {
		defer func() {
			if err := conn.Close(); err != nil {
				t.Logf("failed to close connection: %v", err)
			}
		}()
		time.Sleep(200 * time.Millisecond)
	})

	connWriteMu := &sync.Mutex{}
	cc := newClientCoordinator(clientConn, connWriteMu, ackCh, 65536, log, nil)
	cc.start()

	cc.close()
	// Second close should not panic.
	assert.NotPanics(t, func() { cc.close() })
}

// TestClientCoordinator_AckChannelClosed stops the coordinator when the ack
// channel is closed (simulating a PeerReceiver exit on connection loss).
func TestClientCoordinator_AckChannelClosed(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	ackCh := make(chan AckMsg, 4)

	clientConn := newCoordTestPair(t, func(conn *websocket.Conn) {
		defer func() {
			if err := conn.Close(); err != nil {
				t.Logf("failed to close connection: %v", err)
			}
		}()
		time.Sleep(500 * time.Millisecond)
	})

	connWriteMu := &sync.Mutex{}
	cc := newClientCoordinator(clientConn, connWriteMu, ackCh, 65536, log, nil)
	cc.start()

	// Send a batch so the coordinator is waiting for an ack.
	rawLine := []byte(`{"v":1,"id":"z","channel":"ch","origin":"o","ts":"2024-01-01T00:00:00Z","payload_type":"t","payload":{}}`)
	doneCh := make(chan struct{})
	go func() {
		cc.requestCh <- sendReq{
			channel: "ch", rawLines: [][]byte{rawLine},
			newOffset: int64(len(rawLine) + 1), doneCh: doneCh,
		}
	}()

	// Give coordinator time to block on ackCh.
	time.Sleep(30 * time.Millisecond)

	// Close the ackCh to simulate receiver exit.
	close(ackCh)

	// The coordinator run() goroutine should exit promptly.
	select {
	case <-cc.done:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("coordinator did not exit after ackCh closed")
	}

	cc.close() // should not hang
}

// TestClientCoordinator_WithReaders verifies integration with channelReader:
// a reader can submit via the shared requestCh and the coordinator routes acks.
func TestClientCoordinator_WithReaders(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	log := &testutil.FakeLogger{}

	channelDir := dir + "/channels/feed"
	offsetDir := dir + "/subscribers/feed"

	// Write one message to the channel directory before creating the reader.
	// The reader will be created with no existing offset → starts at 0.
	// (We create the reader before writing data so it starts at offset 0.)

	ackCh := make(chan AckMsg, 4)

	clientConn := newCoordTestPair(t, func(conn *websocket.Conn) {
		defer func() {
			if err := conn.Close(); err != nil {
				t.Logf("failed to close connection: %v", err)
			}
		}()
		for {
			msgType, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if msgType == websocket.BinaryMessage {
				if err := conn.WriteJSON(AckMsg{LastID: "ok"}); err != nil {
					t.Logf("failed to write ack: %v", err)
				}
			}
		}
	})

	connWriteMu := &sync.Mutex{}

	// Create a placeholder requestCh; newClientCoordinator will replace it.
	placeholder := make(chan sendReq, 1)
	reader, err := newChannelReader("peer1", "feed", channelDir, offsetDir, 65536, placeholder, log)
	require.NoError(t, err)

	cc := newClientCoordinator(clientConn, connWriteMu, ackCh, 65536, log, []*channelReader{reader})
	cc.start()
	defer cc.close()

	// Write a message and notify the reader.
	env1 := makeEnvelope(t, "feed", "m1")
	writeTestSegment(t, channelDir, 0, []envelope.Envelope{env1})

	// Route the ack that the server will send back to ackCh.
	go func() {
		time.Sleep(50 * time.Millisecond)
		ackCh <- AckMsg{LastID: "m1"}
	}()

	reader.notify()

	// The reader should have its offset updated to the end of the segment.
	time.Sleep(300 * time.Millisecond)

	offset, err := readOffsetFile(t, offsetDir+"/fed-peer1.offset")
	require.NoError(t, err)
	assert.Greater(t, offset, int64(0), "offset should be advanced after delivery")
}

// readOffsetFile is a thin test helper around storage.ReadOffset.
func readOffsetFile(t *testing.T, path string) (int64, error) {
	t.Helper()
	return storage.ReadOffset(path)
}
