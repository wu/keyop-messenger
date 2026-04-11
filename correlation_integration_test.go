//go:build integration

package messenger

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMessenger_CorrelationID_PublishAndSubscribe verifies that correlation ID
// set via WithCorrelationID is stamped on the envelope and delivered to subscribers.
func TestMessenger_CorrelationID_PublishAndSubscribe(t *testing.T) {
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

	// Publish with a correlation ID set in context.
	correlationID := "test-correlation-id-123"
	pubCtx := WithCorrelationID(context.Background(), correlationID)
	pubCtx, cancel := context.WithTimeout(pubCtx, 5*time.Second)
	defer cancel()

	require.NoError(t, m.Publish(pubCtx, "events", "test.Event", map[string]string{"key": "value"}))

	// Wait for the message to be delivered.
	select {
	case msg := <-received:
		assert.Equal(t, correlationID, msg.CorrelationID)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// TestMessenger_CorrelationID_Empty verifies that messages published without
// a correlation ID have an empty CorrelationID field.
func TestMessenger_CorrelationID_Empty(t *testing.T) {
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

	// Publish WITHOUT setting correlation ID in context.
	pubCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, m.Publish(pubCtx, "events", "test.Event", map[string]string{"key": "value"}))

	// Verify the correlation ID is empty.
	select {
	case msg := <-received:
		assert.Equal(t, "", msg.CorrelationID)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// TestMessenger_CorrelationID_MultipleMessages verifies that multiple messages
// published with the same context carry the same correlation ID.
func TestMessenger_CorrelationID_MultipleMessages(t *testing.T) {
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

	// Publish two messages with the same correlation ID.
	correlationID := "test-corr-multi"
	pubCtx := WithCorrelationID(context.Background(), correlationID)
	pubCtx, pubCancel := context.WithTimeout(pubCtx, 5*time.Second)
	defer pubCancel()

	require.NoError(t, m.Publish(pubCtx, "events", "test.Event", map[string]string{"msg": "1"}))
	require.NoError(t, m.Publish(pubCtx, "events", "test.Event", map[string]string{"msg": "2"}))

	// Collect both messages and verify they have the same correlation ID.
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

	assert.Equal(t, correlationID, msg1.CorrelationID)
	assert.Equal(t, correlationID, msg2.CorrelationID)
}

// TestEphemeralMessenger_CorrelationID verifies that EphemeralMessenger also
// stamps correlation IDs from context on published messages.
func TestEphemeralMessenger_CorrelationID(t *testing.T) {
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

	// Publish with correlation ID set in context.
	correlationID := "ephemeral-corr-id"
	pubCtx := WithCorrelationID(context.Background(), correlationID)
	pubCtx, pubCancel := context.WithTimeout(pubCtx, 5*time.Second)
	defer pubCancel()

	require.NoError(t, em.Publish(pubCtx, "events", "test.Event", map[string]string{"key": "value"}))

	// Wait for the message to arrive at the hub and verify correlation ID.
	select {
	case msg := <-received:
		assert.Equal(t, correlationID, msg.CorrelationID)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message from ephemeral client")
	}
}
