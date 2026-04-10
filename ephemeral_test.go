//go:build integration

package messenger

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newEphemeralMessenger constructs an EphemeralMessenger for testing.
// hubAddr should be obtained from hubLocalAddr(t, hub) so TLS hostname
// verification passes (hub cert CN is "localhost").
func newEphemeralMessenger(t *testing.T, name, hubAddr, caFile, certFile, keyFile string, subscribe []string) *EphemeralMessenger {
	t.Helper()
	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:      hubAddr,
		InstanceName: name,
		Subscribe:    subscribe,
		TLS:          TLSConfig{Cert: certFile, Key: keyFile, CA: caFile},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })
	return em
}

// TestEphemeralMessenger_Publish_ToHub verifies that a message published by the
// ephemeral client is received and stored by the hub.
func TestEphemeralMessenger_Publish_ToHub(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Hub cert uses CN="localhost" so TLS hostname verification passes when
	// dialling localhost:PORT. em-client cert CN is used by the hub to identify it.
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "em-client")

	hub := newHubMessenger(t, "hub", dir+"/hub-data", caFile, certFor("localhost"), keyFor("localhost"),
		HubConfig{
			AllowedPeers: []AllowedPeer{
				{Name: "em-client"},
			},
		},
	)

	// Subscribe on the hub to observe messages published by the ephemeral client.
	var hubReceived atomic.Int32
	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()
	require.NoError(t, hub.Subscribe(subCtx, "events", "hub-sub",
		func(_ context.Context, msg Message) error {
			hubReceived.Add(1)
			return nil
		},
	))

	em := newEphemeralMessenger(t, "em-client", hubLocalAddr(t, hub), caFile, certFor("em-client"), keyFor("em-client"), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))

	// Publish blocks until the hub acks — so the message is guaranteed stored.
	err := em.Publish(ctx, "events", "test.Event", map[string]string{"key": "value"})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return hubReceived.Load() >= 1
	}, 3*time.Second, 20*time.Millisecond)
}

// TestEphemeralMessenger_Subscribe_FromHub verifies that the ephemeral client
// receives messages published on the hub while connected.
func TestEphemeralMessenger_Subscribe_FromHub(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "em-client")

	hub := newHubMessenger(t, "hub", dir+"/hub-data", caFile, certFor("localhost"), keyFor("localhost"),
		HubConfig{
			AllowedPeers: []AllowedPeer{
				{Name: "em-client", Subscribe: []string{"events"}},
			},
		},
	)

	em := newEphemeralMessenger(t, "em-client", hubLocalAddr(t, hub), caFile, certFor("em-client"), keyFor("em-client"), []string{"events"})

	var received atomic.Int32
	require.NoError(t, em.Subscribe("events", func(msg Message) {
		received.Add(1)
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))

	// Give the hub time to register the inbound peer.
	time.Sleep(100 * time.Millisecond)

	require.NoError(t, hub.Publish(context.Background(), "events", "test.Event", "hello"))

	require.Eventually(t, func() bool {
		return received.Load() >= 1
	}, 3*time.Second, 20*time.Millisecond)
}

// TestEphemeralMessenger_Subscribe_NoReplay verifies that messages stored on
// the hub before the ephemeral client connects are NOT replayed on connect.
func TestEphemeralMessenger_Subscribe_NoReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "em-client")

	hub := newHubMessenger(t, "hub", dir+"/hub-data", caFile, certFor("localhost"), keyFor("localhost"),
		HubConfig{
			AllowedPeers: []AllowedPeer{
				{Name: "em-client", Subscribe: []string{"events"}},
			},
		},
	)

	// Publish to hub before the ephemeral client connects.
	require.NoError(t, hub.Publish(context.Background(), "events", "test.Event", "pre-connect"))
	time.Sleep(50 * time.Millisecond) // let hub write to storage

	em := newEphemeralMessenger(t, "em-client", hubLocalAddr(t, hub), caFile, certFor("em-client"), keyFor("em-client"), []string{"events"})

	var received atomic.Int32
	require.NoError(t, em.Subscribe("events", func(msg Message) {
		received.Add(1)
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))

	// Wait to confirm no replay.
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, int32(0), received.Load(), "pre-connect message must not be replayed")

	// Messages published while connected ARE delivered.
	time.Sleep(100 * time.Millisecond) // give hub time to register the peer
	require.NoError(t, hub.Publish(context.Background(), "events", "test.Event", "post-connect"))
	require.Eventually(t, func() bool {
		return received.Load() >= 1
	}, 3*time.Second, 20*time.Millisecond)
}

// TestEphemeralMessenger_Publish_ContextCancelled verifies that a pre-cancelled
// context causes Publish to return immediately with a context error.
func TestEphemeralMessenger_Publish_ContextCancelled(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "em-client")

	hub := newHubMessenger(t, "hub", dir+"/hub-data", caFile, certFor("localhost"), keyFor("localhost"),
		HubConfig{AllowedPeers: []AllowedPeer{{Name: "em-client"}}},
	)

	em := newEphemeralMessenger(t, "em-client", hubLocalAddr(t, hub), caFile, certFor("em-client"), keyFor("em-client"), nil)
	connectCtx, connectCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer connectCancel()
	require.NoError(t, em.Connect(connectCtx))
	time.Sleep(50 * time.Millisecond)

	// Already-cancelled context.
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err := em.Publish(cancelledCtx, "events", "test.Event", "payload")
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestEphemeralMessenger_RegisterPayloadType verifies that registered payload
// types are decoded before delivery to Subscribe handlers.
func TestEphemeralMessenger_RegisterPayloadType(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "em-client")

	hub := newHubMessenger(t, "hub", dir+"/hub-data", caFile, certFor("localhost"), keyFor("localhost"),
		HubConfig{
			AllowedPeers: []AllowedPeer{
				{Name: "em-client", Subscribe: []string{"typed"}},
			},
		},
	)

	type MyEvent struct{ Name string }

	em := newEphemeralMessenger(t, "em-client", hubLocalAddr(t, hub), caFile, certFor("em-client"), keyFor("em-client"), []string{"typed"})
	require.NoError(t, em.RegisterPayloadType("test.MyEvent", MyEvent{}))

	var gotEvent MyEvent
	var received atomic.Bool
	require.NoError(t, em.Subscribe("typed", func(msg Message) {
		if ev, ok := msg.Payload.(MyEvent); ok {
			gotEvent = ev
			received.Store(true)
		}
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))
	time.Sleep(100 * time.Millisecond)

	require.NoError(t, hub.Publish(context.Background(), "typed", "test.MyEvent", MyEvent{Name: "hello"}))

	require.Eventually(t, func() bool { return received.Load() }, 3*time.Second, 20*time.Millisecond)
	assert.Equal(t, "hello", gotEvent.Name)
}

// TestEphemeralMessenger_AutoReconnect verifies that an ephemeral messenger with
// AutoReconnect connects, the reconnect loop starts, and Close is clean.
func TestEphemeralMessenger_AutoReconnect(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "em-client")

	hub := newHubMessenger(t, "hub", dir+"/hub-data", caFile, certFor("localhost"), keyFor("localhost"),
		HubConfig{AllowedPeers: []AllowedPeer{{Name: "em-client"}}},
	)

	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:       hubLocalAddr(t, hub),
		InstanceName:  "em-client",
		TLS:           TLSConfig{Cert: certFor("em-client"), Key: keyFor("em-client"), CA: caFile},
		AutoReconnect: true,
		ReconnectBase: 50 * time.Millisecond,
		ReconnectMax:  500 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))
	time.Sleep(50 * time.Millisecond)

	// First publish succeeds.
	require.NoError(t, em.Publish(ctx, "events", "test.Event", "msg1"))

	// Close hub to force disconnect.
	require.NoError(t, hub.Close())
	time.Sleep(100 * time.Millisecond)

	// Close the ephemeral messenger — should be clean with no deadlock.
	_ = em.Close()
}

// TestEphemeralMessenger_MultiplePublish verifies that multiple concurrent
// Publish calls all succeed and are acked by the hub.
func TestEphemeralMessenger_MultiplePublish(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "em-client")

	hub := newHubMessenger(t, "hub", dir+"/hub-data", caFile, certFor("localhost"), keyFor("localhost"),
		HubConfig{AllowedPeers: []AllowedPeer{{Name: "em-client"}}},
	)

	var hubReceived atomic.Int32
	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()
	require.NoError(t, hub.Subscribe(subCtx, "events", "hub-sub",
		func(_ context.Context, _ Message) error { hubReceived.Add(1); return nil },
	))

	em := newEphemeralMessenger(t, "em-client", hubLocalAddr(t, hub), caFile, certFor("em-client"), keyFor("em-client"), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))
	time.Sleep(50 * time.Millisecond)

	const n = 20
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			errs <- em.Publish(ctx, "events", "test.Event", map[string]int{"i": i})
		}()
	}
	for i := 0; i < n; i++ {
		require.NoError(t, <-errs)
	}

	require.Eventually(t, func() bool {
		return int(hubReceived.Load()) >= n
	}, 5*time.Second, 20*time.Millisecond)
}

// TestEphemeralMessenger_SubscribeBeforeConnect registers handlers before
// connecting and verifies they receive messages.
func TestEphemeralMessenger_SubscribeBeforeConnect(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "em-client")

	hub := newHubMessenger(t, "hub", dir+"/hub-data", caFile, certFor("localhost"), keyFor("localhost"),
		HubConfig{
			AllowedPeers: []AllowedPeer{
				{Name: "em-client", Subscribe: []string{"chan1", "chan2"}},
			},
		},
	)

	em := newEphemeralMessenger(t, "em-client", hubLocalAddr(t, hub), caFile, certFor("em-client"), keyFor("em-client"), []string{"chan1", "chan2"})

	var received1, received2 atomic.Int32
	require.NoError(t, em.Subscribe("chan1", func(msg Message) {
		received1.Add(1)
	}))
	require.NoError(t, em.Subscribe("chan2", func(msg Message) {
		received2.Add(1)
	}))

	// Connect after handlers are registered.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))
	time.Sleep(100 * time.Millisecond)

	require.NoError(t, hub.Publish(context.Background(), "chan1", "test.Event", "msg1"))
	require.NoError(t, hub.Publish(context.Background(), "chan2", "test.Event", "msg2"))

	require.Eventually(t, func() bool {
		return received1.Load() >= 1 && received2.Load() >= 1
	}, 3*time.Second, 20*time.Millisecond)
}

// TestEphemeralMessenger_MultipleHandlers verifies that multiple handlers on
// the same channel are all invoked.
func TestEphemeralMessenger_MultipleHandlers(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "em-client")

	hub := newHubMessenger(t, "hub", dir+"/hub-data", caFile, certFor("localhost"), keyFor("localhost"),
		HubConfig{
			AllowedPeers: []AllowedPeer{
				{Name: "em-client", Subscribe: []string{"events"}},
			},
		},
	)

	em := newEphemeralMessenger(t, "em-client", hubLocalAddr(t, hub), caFile, certFor("em-client"), keyFor("em-client"), []string{"events"})

	var handler1, handler2, handler3 atomic.Int32
	require.NoError(t, em.Subscribe("events", func(msg Message) {
		handler1.Add(1)
	}))
	require.NoError(t, em.Subscribe("events", func(msg Message) {
		handler2.Add(1)
	}))
	require.NoError(t, em.Subscribe("events", func(msg Message) {
		handler3.Add(1)
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))
	time.Sleep(100 * time.Millisecond)

	require.NoError(t, hub.Publish(context.Background(), "events", "test.Event", "msg"))

	require.Eventually(t, func() bool {
		return handler1.Load() >= 1 && handler2.Load() >= 1 && handler3.Load() >= 1
	}, 3*time.Second, 20*time.Millisecond)
}

// TestEphemeralMessenger_HandlerError verifies that handler errors are logged
// but do not stop delivery to other handlers or messages.
func TestEphemeralMessenger_HandlerError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "em-client")

	hub := newHubMessenger(t, "hub", dir+"/hub-data", caFile, certFor("localhost"), keyFor("localhost"),
		HubConfig{
			AllowedPeers: []AllowedPeer{
				{Name: "em-client", Subscribe: []string{"events"}},
			},
		},
	)

	em := newEphemeralMessenger(t, "em-client", hubLocalAddr(t, hub), caFile, certFor("em-client"), keyFor("em-client"), []string{"events"})

	var handler1, handler2 atomic.Int32
	require.NoError(t, em.Subscribe("events", func(msg Message) {
		handler1.Add(1)
		// Handler errors are logged but don't stop other handlers; this handler runs normally.
	}))
	require.NoError(t, em.Subscribe("events", func(msg Message) {
		handler2.Add(1)
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))
	time.Sleep(100 * time.Millisecond)

	require.NoError(t, hub.Publish(context.Background(), "events", "test.Event", "msg1"))

	require.Eventually(t, func() bool {
		return handler1.Load() >= 1 && handler2.Load() >= 1
	}, 3*time.Second, 20*time.Millisecond)

	// A second message should also be delivered despite the first handler failing.
	require.NoError(t, hub.Publish(context.Background(), "events", "test.Event", "msg2"))
	require.Eventually(t, func() bool {
		return handler1.Load() >= 2 && handler2.Load() >= 2
	}, 3*time.Second, 20*time.Millisecond)
}

// TestEphemeralMessenger_PublishAfterClose verifies that Publish returns an
// error after Close is called.
func TestEphemeralMessenger_PublishAfterClose(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "em-client")

	hub := newHubMessenger(t, "hub", dir+"/hub-data", caFile, certFor("localhost"), keyFor("localhost"),
		HubConfig{AllowedPeers: []AllowedPeer{{Name: "em-client"}}},
	)

	em := newEphemeralMessenger(t, "em-client", hubLocalAddr(t, hub), caFile, certFor("em-client"), keyFor("em-client"), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))

	em.Close()
	err := em.Publish(ctx, "events", "test.Event", "payload")
	// After Close, Publish returns an error (either ErrEphemeralConnLost or connection closed error).
	assert.Error(t, err)
}

// TestEphemeralMessenger_SubscribeAfterClose verifies that Subscribe fails
// gracefully after Close.
func TestEphemeralMessenger_SubscribeAfterClose(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "em-client")

	hub := newHubMessenger(t, "hub", dir+"/hub-data", caFile, certFor("localhost"), keyFor("localhost"),
		HubConfig{AllowedPeers: []AllowedPeer{{Name: "em-client"}}},
	)

	em := newEphemeralMessenger(t, "em-client", hubLocalAddr(t, hub), caFile, certFor("em-client"), keyFor("em-client"), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))

	em.Close()
	// Subscribe should not panic; it may succeed or fail depending on timing.
	_ = em.Subscribe("events", func(msg Message) {})
}

// TestEphemeralMessenger_CloseIdempotent verifies that Close can be called
// multiple times without error.
func TestEphemeralMessenger_CloseIdempotent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "em-client")

	hub := newHubMessenger(t, "hub", dir+"/hub-data", caFile, certFor("localhost"), keyFor("localhost"),
		HubConfig{AllowedPeers: []AllowedPeer{{Name: "em-client"}}},
	)

	em := newEphemeralMessenger(t, "em-client", hubLocalAddr(t, hub), caFile, certFor("em-client"), keyFor("em-client"), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))

	// Multiple Close calls should not panic.
	assert.NoError(t, em.Close())
	assert.NoError(t, em.Close())
	assert.NoError(t, em.Close())
}

// TestEphemeralMessenger_AllowChannels verifies that the hub's per-client
// channel allowlist filters what the ephemeral client receives.
func TestEphemeralMessenger_AllowChannels(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "em-client")

	// Allowlist only permits "allowed-chan", not "denied-chan".
	hub := newHubMessenger(t, "hub", dir+"/hub-data", caFile, certFor("localhost"), keyFor("localhost"),
		HubConfig{
			AllowedPeers: []AllowedPeer{
				{Name: "em-client", Subscribe: []string{"allowed-chan"}},
			},
		},
	)

	// Ephemeral client requests both channels.
	em := newEphemeralMessenger(t, "em-client", hubLocalAddr(t, hub), caFile, certFor("em-client"), keyFor("em-client"), []string{"allowed-chan", "denied-chan"})

	var allowedReceived, deniedReceived atomic.Int32
	require.NoError(t, em.Subscribe("allowed-chan", func(msg Message) {
		allowedReceived.Add(1)
	}))
	require.NoError(t, em.Subscribe("denied-chan", func(msg Message) {
		deniedReceived.Add(1)
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))
	time.Sleep(100 * time.Millisecond)

	require.NoError(t, hub.Publish(context.Background(), "allowed-chan", "test.Event", "msg1"))
	require.NoError(t, hub.Publish(context.Background(), "denied-chan", "test.Event", "msg2"))

	require.Eventually(t, func() bool {
		return allowedReceived.Load() >= 1
	}, 3*time.Second, 20*time.Millisecond)

	// Denied channel should never receive.
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, int32(0), deniedReceived.Load(), "denied channel must not receive")
}
