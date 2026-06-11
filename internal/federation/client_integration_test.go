package federation

import (
	"sync/atomic"
	"testing"
	"time"

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

// mockHubHandshake performs the server-side application handshake on a *Conn.
func mockHubHandshake(conn *Conn) error {
	if _, err := ReceiveHandshake(conn); err != nil {
		return err
	}
	return SendHandshake(conn, HandshakeMsg{
		InstanceName: "hub",
		Role:         "hub",
		Version:      "1",
	})
}

// TestClient_Dial_Success connects to a mock hub server.
func TestClient_Dial_Success(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	srv := mockFedServer(t, func(conn *Conn) {
		defer func() { _ = conn.Close() }()

		// Receive client handshake
		hs, err := ReceiveHandshake(conn)
		require.NoError(t, err)
		assert.Equal(t, "test-client", hs.InstanceName)
		assert.Equal(t, "client", hs.Role)

		// Send hub handshake
		_ = SendHandshake(conn, HandshakeMsg{
			InstanceName: "hub",
			Role:         "hub",
			Version:      "1",
		})

		// Keep connection open
		time.Sleep(100 * time.Millisecond)
	})

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
		[]string{"events"}, nil,
	)
	defer client.Close()

	sender, err := client.Dial(fedServerAddr(srv))

	assert.NoError(t, err)
	assert.NotNil(t, sender)
	time.Sleep(50 * time.Millisecond)
	sender.Close()
}

// TestClient_Sender_AfterDial returns the connected sender.
func TestClient_Sender_AfterDial(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	srv := mockFedServer(t, func(conn *Conn) {
		defer func() { _ = conn.Close() }()
		_ = mockHubHandshake(conn)
		time.Sleep(100 * time.Millisecond)
	})

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
		[]string{}, nil,
	)
	defer client.Close()

	// Before dial, Sender should be nil
	assert.Nil(t, client.Sender())

	sender, err := client.Dial(fedServerAddr(srv))
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
	srv := mockFedServer(t, func(conn *Conn) {
		connectionCount.Add(1)
		defer func() { _ = conn.Close() }()
		_ = mockHubHandshake(conn)
		time.Sleep(50 * time.Millisecond)
	})

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
		[]string{}, nil,
	)
	defer client.Close()

	err := client.ConnectWithReconnect(fedServerAddr(srv))
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

	srv := mockFedServer(t, func(conn *Conn) {
		defer func() { _ = conn.Close() }()
		_ = mockHubHandshake(conn)
		time.Sleep(50 * time.Millisecond)
	})

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
		[]string{}, nil,
	)

	err := client.ConnectWithReconnect(fedServerAddr(srv))
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

	srv := mockFedServer(t, func(conn *Conn) {
		defer func() { _ = conn.Close() }()
		_ = mockHubHandshake(conn)
		time.Sleep(100 * time.Millisecond)
	})

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
		nil,
	)
	defer client.Close()

	sender, err := client.Dial(fedServerAddr(srv))

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
	srv := mockFedServer(t, func(conn *Conn) {
		defer func() { _ = conn.Close() }()
		hs, err := ReceiveHandshake(conn)
		if err != nil {
			return
		}
		receivedSubscribe = hs.Subscribe
		_ = SendHandshake(conn, HandshakeMsg{
			InstanceName: "hub",
			Role:         "hub",
			Version:      "1",
		})
		time.Sleep(100 * time.Millisecond)
	})

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
		[]string{"chan1", "chan2", "chan3"}, nil,
	)
	defer client.Close()

	sender, err := client.Dial(fedServerAddr(srv))
	require.NoError(t, err)

	// Verify subscribe channels were sent
	assert.Equal(t, []string{"chan1", "chan2", "chan3"}, receivedSubscribe)
	time.Sleep(50 * time.Millisecond)
	sender.Close()
}

// TestClient_Dial_NoLastID verifies the client does not send LastID in the handshake.
// The hub now tracks delivery progress server-side via offset files.
func TestClient_Dial_NoLastID(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}

	var receivedLastID string
	srv := mockFedServer(t, func(conn *Conn) {
		defer func() { _ = conn.Close() }()
		hs, err := ReceiveHandshake(conn)
		if err != nil {
			return
		}
		receivedLastID = hs.LastID
		_ = SendHandshake(conn, HandshakeMsg{
			InstanceName: "hub",
			Role:         "hub",
			Version:      "1",
		})
		time.Sleep(100 * time.Millisecond)
	})

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
		[]string{}, nil,
	)
	defer client.Close()

	sender, err := client.Dial(fedServerAddr(srv))
	require.NoError(t, err)

	assert.Empty(t, receivedLastID, "client must not send LastID; hub tracks progress server-side")
	time.Sleep(50 * time.Millisecond)
	sender.Close()
}
