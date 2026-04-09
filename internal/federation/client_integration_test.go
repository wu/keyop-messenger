package federation

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/dedup"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
)

// fakeAuditLogger2 for client tests
type fakeAuditLogger2 struct{}

func (f *fakeAuditLogger2) Log(_ audit.Event) error { return nil }
func (f *fakeAuditLogger2) Close() error            { return nil }

// TestClient_Dial_Success connects to a mock hub server.
func TestClient_Dial_Success(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	srv := mockWsServer(t, func(conn *websocket.Conn) {
		defer func() { _ = conn.Close() }()

		// Receive client handshake
		var hs HandshakeMsg
		err := conn.ReadJSON(&hs)
		require.NoError(t, err)
		assert.Equal(t, "test-client", hs.InstanceName)
		assert.Equal(t, "client", hs.Role)

		// Send hub handshake
		_ = conn.WriteJSON(HandshakeMsg{
			InstanceName: "hub",
			Role:         "hub",
			Version:      "1",
		})

		// Keep connection open
		time.Sleep(100 * time.Millisecond)
	})
	defer srv.Close()

	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})
	client := NewClient(
		"test-client",
		nil,
		policy,
		func(_ *envelope.Envelope) error { return nil },
		dedupL,
		&fakeAuditLogger2{},
		log,
		100,
		65536,
		500*time.Millisecond,
		60*time.Second,
		0.2,
		[]string{"events"},
	)
	defer client.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	sender, err := client.Dial(strings.TrimPrefix(wsURL, "ws://"))

	assert.NoError(t, err)
	assert.NotNil(t, sender)
	time.Sleep(50 * time.Millisecond)
	sender.Close()
}

// TestClient_Sender_AfterDial returns the connected sender.
func TestClient_Sender_AfterDial(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	srv := mockWsServer(t, func(conn *websocket.Conn) {
		defer func() { _ = conn.Close() }()
		_ = conn.ReadJSON(&HandshakeMsg{})
		_ = conn.WriteJSON(HandshakeMsg{
			InstanceName: "hub",
			Role:         "hub",
			Version:      "1",
		})
		time.Sleep(100 * time.Millisecond)
	})
	defer srv.Close()

	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})
	client := NewClient(
		"test-client",
		nil,
		policy,
		func(_ *envelope.Envelope) error { return nil },
		dedupL,
		&fakeAuditLogger2{},
		log,
		100,
		65536,
		500*time.Millisecond,
		60*time.Second,
		0.2,
		[]string{},
	)
	defer client.Close()

	// Before dial, Sender should be nil
	assert.Nil(t, client.Sender())

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	sender, err := client.Dial(strings.TrimPrefix(wsURL, "ws://"))
	require.NoError(t, err)

	// After dial, Sender should return the same sender
	assert.Equal(t, sender, client.Sender())
	time.Sleep(50 * time.Millisecond)
	sender.Close()
}

// TestClient_ConnectWithReconnect_InitialSuccess connects and sets up reconnect loop.
func TestClient_ConnectWithReconnect_InitialSuccess(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	connectionCount := atomic.Int32{}
	srv := mockWsServer(t, func(conn *websocket.Conn) {
		connectionCount.Add(1)
		defer func() { _ = conn.Close() }()
		_ = conn.ReadJSON(&HandshakeMsg{})
		_ = conn.WriteJSON(HandshakeMsg{
			InstanceName: "hub",
			Role:         "hub",
			Version:      "1",
		})
		time.Sleep(50 * time.Millisecond)
	})
	defer srv.Close()

	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})
	client := NewClient(
		"test-client",
		nil,
		policy,
		func(_ *envelope.Envelope) error { return nil },
		dedupL,
		&fakeAuditLogger2{},
		log,
		100,
		65536,
		100*time.Millisecond, // short reconnect base for testing
		500*time.Millisecond,
		0.1,
		[]string{},
	)
	defer client.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	err := client.ConnectWithReconnect(strings.TrimPrefix(wsURL, "ws://"))
	require.NoError(t, err)

	// Give reconnect loop time to attempt a reconnection after the first disconnects
	time.Sleep(300 * time.Millisecond)

	// Should have made at least the initial connection
	assert.GreaterOrEqual(t, connectionCount.Load(), int32(1))
}

// TestClient_Close_StopsReconnectLoop verifies close stops background reconnect.
func TestClient_Close_StopsReconnectLoop(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	srv := mockWsServer(t, func(conn *websocket.Conn) {
		defer func() { _ = conn.Close() }()
		_ = conn.ReadJSON(&HandshakeMsg{})
		_ = conn.WriteJSON(HandshakeMsg{
			InstanceName: "hub",
			Role:         "hub",
			Version:      "1",
		})
		time.Sleep(50 * time.Millisecond)
	})
	defer srv.Close()

	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})
	client := NewClient(
		"test-client",
		nil,
		policy,
		func(_ *envelope.Envelope) error { return nil },
		dedupL,
		&fakeAuditLogger2{},
		log,
		100,
		65536,
		100*time.Millisecond,
		500*time.Millisecond,
		0.1,
		[]string{},
	)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	err := client.ConnectWithReconnect(strings.TrimPrefix(wsURL, "ws://"))
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	// Close should complete quickly without hanging
	start := time.Now()
	client.Close()
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 2*time.Second)
}

// TestClient_Dial_WithoutLocalWriter creates sender-only client.
func TestClient_Dial_WithoutLocalWriter(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	srv := mockWsServer(t, func(conn *websocket.Conn) {
		defer func() { _ = conn.Close() }()
		_ = conn.ReadJSON(&HandshakeMsg{})
		_ = conn.WriteJSON(HandshakeMsg{
			InstanceName: "hub",
			Role:         "hub",
			Version:      "1",
		})
		time.Sleep(100 * time.Millisecond)
	})
	defer srv.Close()

	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})
	client := NewClient(
		"sender-client",
		nil,
		policy,
		nil, // no localWriter
		dedupL,
		&fakeAuditLogger2{},
		log,
		100,
		65536,
		500*time.Millisecond,
		60*time.Second,
		0.2,
		[]string{}, // no subscriptions
	)
	defer client.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	sender, err := client.Dial(strings.TrimPrefix(wsURL, "ws://"))

	assert.NoError(t, err)
	assert.NotNil(t, sender)
	time.Sleep(50 * time.Millisecond)
	sender.Close()
}

// TestClient_Dial_WithSubscriptions sends subscribe list in handshake.
func TestClient_Dial_WithSubscriptions(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	var receivedSubscribe []string
	srv := mockWsServer(t, func(conn *websocket.Conn) {
		defer func() { _ = conn.Close() }()
		var hs HandshakeMsg
		_ = conn.ReadJSON(&hs)
		receivedSubscribe = hs.Subscribe
		_ = conn.WriteJSON(HandshakeMsg{
			InstanceName: "hub",
			Role:         "hub",
			Version:      "1",
		})
		time.Sleep(100 * time.Millisecond)
	})
	defer srv.Close()

	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})
	client := NewClient(
		"test-client",
		nil,
		policy,
		func(_ *envelope.Envelope) error { return nil },
		dedupL,
		&fakeAuditLogger2{},
		log,
		100,
		65536,
		500*time.Millisecond,
		60*time.Second,
		0.2,
		[]string{"chan1", "chan2", "chan3"},
	)
	defer client.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	sender, err := client.Dial(strings.TrimPrefix(wsURL, "ws://"))
	require.NoError(t, err)

	// Verify subscribe channels were sent
	assert.Equal(t, []string{"chan1", "chan2", "chan3"}, receivedSubscribe)
	time.Sleep(50 * time.Millisecond)
	sender.Close()
}

// TestClient_Dial_WithReplayID sends last_id in handshake.
func TestClient_Dial_WithReplayID(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	var receivedLastID string
	srv := mockWsServer(t, func(conn *websocket.Conn) {
		defer func() { _ = conn.Close() }()
		var hs HandshakeMsg
		_ = conn.ReadJSON(&hs)
		receivedLastID = hs.LastID
		_ = conn.WriteJSON(HandshakeMsg{
			InstanceName: "hub",
			Role:         "hub",
			Version:      "1",
		})
		time.Sleep(100 * time.Millisecond)
	})
	defer srv.Close()

	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})
	client := NewClient(
		"test-client",
		nil,
		policy,
		func(_ *envelope.Envelope) error { return nil },
		dedupL,
		&fakeAuditLogger2{},
		log,
		100,
		65536,
		500*time.Millisecond,
		60*time.Second,
		0.2,
		[]string{},
	)
	defer client.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	// Dial with internal method that accepts lastID
	sender, err := client.dial(strings.TrimPrefix(wsURL, "ws://"), "previous-msg-id")
	require.NoError(t, err)

	// Verify last_id was sent
	assert.Equal(t, "previous-msg-id", receivedLastID)
	time.Sleep(50 * time.Millisecond)
	sender.Close()
}
