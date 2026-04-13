//go:build integration

package messenger

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMessenger_ServiceName_PublishAndSubscribe verifies that service name
// set via WithServiceName is stamped on the envelope and delivered to subscribers.
func TestMessenger_ServiceName_PublishAndSubscribe(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := New(testConfig(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	// Use a channel to safely receive the message from the handler goroutine.
	received := make(chan Message, 1)
	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()

	require.NoError(t, m.Subscribe(subCtx, "events", "sub-1",
		func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		},
	))

	// Publish with a service name set in context.
	serviceName := "my-service"
	pubCtx := WithServiceName(context.Background(), serviceName)
	pubCtx, cancel := context.WithTimeout(pubCtx, 5*time.Second)
	defer cancel()

	require.NoError(t, m.Publish(pubCtx, "events", "test.Event", map[string]string{"key": "value"}))

	// Wait for the message to be delivered.
	select {
	case msg := <-received:
		assert.Equal(t, serviceName, msg.ServiceName)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// TestMessenger_ServiceName_Empty verifies that messages published without
// a service name have an empty ServiceName field.
func TestMessenger_ServiceName_Empty(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := New(testConfig(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	// Use a channel to safely receive the message from the handler goroutine.
	received := make(chan Message, 1)
	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()

	require.NoError(t, m.Subscribe(subCtx, "events", "sub-1",
		func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		},
	))

	// Publish WITHOUT setting service name in context.
	pubCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, m.Publish(pubCtx, "events", "test.Event", map[string]string{"key": "value"}))

	// Verify the service name is empty.
	select {
	case msg := <-received:
		assert.Equal(t, "", msg.ServiceName)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// TestMessenger_ServiceName_MultipleMessages verifies that multiple messages
// published with the same context carry the same service name.
func TestMessenger_ServiceName_MultipleMessages(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := New(testConfig(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	// Use a channel to safely receive messages from the handler goroutine.
	received := make(chan Message, 2)
	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()

	require.NoError(t, m.Subscribe(subCtx, "events", "sub-1",
		func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		},
	))

	// Publish two messages with the same service name.
	serviceName := "test-service"
	pubCtx := WithServiceName(context.Background(), serviceName)
	pubCtx, pubCancel := context.WithTimeout(pubCtx, 5*time.Second)
	defer pubCancel()

	require.NoError(t, m.Publish(pubCtx, "events", "test.Event", map[string]string{"msg": "1"}))
	require.NoError(t, m.Publish(pubCtx, "events", "test.Event", map[string]string{"msg": "2"}))

	// Collect both messages and verify they have the same service name.
	var msg1, msg2 Message
	timeout := time.After(2 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case msg := <-received:
			if i == 0 {
				msg1 = msg
			} else {
				msg2 = msg
			}
		case <-timeout:
			t.Fatalf("timeout waiting for message %d", i+1)
		}
	}

	assert.Equal(t, serviceName, msg1.ServiceName)
	assert.Equal(t, serviceName, msg2.ServiceName)
}

// TestMessenger_ServiceNameAndCorrelationID verifies that both service name
// and correlation ID can be set in the same context and both are delivered.
func TestMessenger_ServiceNameAndCorrelationID(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := New(testConfig(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	// Use a channel to safely receive the message from the handler goroutine.
	received := make(chan Message, 1)
	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()

	require.NoError(t, m.Subscribe(subCtx, "events", "sub-1",
		func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		},
	))

	// Publish with both service name and correlation ID.
	serviceName := "test-svc"
	correlationID := "corr-456"
	pubCtx := WithServiceName(context.Background(), serviceName)
	pubCtx = WithCorrelationID(pubCtx, correlationID)
	pubCtx, cancel := context.WithTimeout(pubCtx, 5*time.Second)
	defer cancel()

	require.NoError(t, m.Publish(pubCtx, "events", "test.Event", map[string]string{"key": "value"}))

	// Wait for the message and verify both fields.
	select {
	case msg := <-received:
		assert.Equal(t, serviceName, msg.ServiceName)
		assert.Equal(t, correlationID, msg.CorrelationID)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// TestEphemeralMessenger_ServiceName verifies that EphemeralMessenger also
// stamps service names from context on published messages.
func TestEphemeralMessenger_ServiceName(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "em-client")

	hub := newHubMessenger(t, "hub", dir+"/hub-data", caFile, certFor("localhost"), keyFor("localhost"),
		HubConfig{
			AllowedPeers: []AllowedPeer{
				{Name: "em-client"},
			},
		},
	)

	// Use a channel to safely receive the message from the hub subscriber.
	received := make(chan Message, 1)
	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()
	require.NoError(t, hub.Subscribe(subCtx, "events", "hub-sub",
		func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		},
	))

	em := newEphemeralMessenger(t, "em-client", hubLocalAddr(t, hub), caFile, certFor("em-client"), keyFor("em-client"), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))

	// Publish with service name set in context.
	serviceName := "ephemeral-service"
	pubCtx := WithServiceName(context.Background(), serviceName)
	pubCtx, pubCancel := context.WithTimeout(pubCtx, 5*time.Second)
	defer pubCancel()

	require.NoError(t, em.Publish(pubCtx, "events", "test.Event", map[string]string{"key": "value"}))

	// Wait for the message to arrive at the hub and verify service name.
	select {
	case msg := <-received:
		assert.Equal(t, serviceName, msg.ServiceName)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message from ephemeral client")
	}
}

// TestEphemeralMessenger_CorrelationID_Now_Delivered verifies that the fix
// to EphemeralMessenger.Subscribe now correctly delivers CorrelationID to handlers.
func TestEphemeralMessenger_CorrelationID_Now_Delivered(t *testing.T) {
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

	// Ephemeral client subscribes to the channel.
	received := make(chan Message, 1)
	em := newEphemeralMessenger(t, "em-client", hubLocalAddr(t, hub), caFile, certFor("em-client"), keyFor("em-client"), []string{"events"})
	require.NoError(t, em.Subscribe("events", func(msg Message) {
		received <- msg
	}))

	// Connect to hub.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))

	// Give the hub time to register the inbound peer.
	time.Sleep(100 * time.Millisecond)

	// Hub publishes a message with correlation ID.
	hubCtx := WithCorrelationID(context.Background(), "test-corr-123")
	hubCtx, hubCancel := context.WithTimeout(hubCtx, 5*time.Second)
	defer hubCancel()
	require.NoError(t, hub.Publish(hubCtx, "events", "test.Event", map[string]string{"key": "value"}))

	// Wait for the message and verify correlation ID is delivered (after the fix).
	select {
	case msg := <-received:
		assert.Equal(t, "test-corr-123", msg.CorrelationID, "CorrelationID should now be delivered in EphemeralMessenger")
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message from hub")
	}
}
