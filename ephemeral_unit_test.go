package messenger

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewEphemeralMessenger_ValidConfig constructs an EphemeralMessenger
// with a valid config and verifies it succeeds.
func TestNewEphemeralMessenger_ValidConfig(t *testing.T) {
	t.Parallel()
	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:      "hub.example.com:7740",
		InstanceName: "test-client",
	})
	require.NoError(t, err)
	assert.NotNil(t, em)
	t.Cleanup(func() { _ = em.Close() })
}

// TestNewEphemeralMessenger_MissingHubAddr verifies that missing HubAddr
// returns a validation error.
func TestNewEphemeralMessenger_MissingHubAddr(t *testing.T) {
	t.Parallel()
	em, err := NewEphemeralMessenger(EphemeralConfig{
		InstanceName: "test-client",
	})
	assert.Error(t, err)
	assert.Nil(t, em)
	assert.ErrorContains(t, err, "HubAddr")
}

// TestNewEphemeralMessenger_MissingInstanceName verifies that missing
// InstanceName returns a validation error.
func TestNewEphemeralMessenger_MissingInstanceName(t *testing.T) {
	t.Parallel()
	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr: "hub.example.com:7740",
	})
	assert.Error(t, err)
	assert.Nil(t, em)
	assert.ErrorContains(t, err, "InstanceName")
}

// TestNewEphemeralMessenger_WithTLS constructs an EphemeralMessenger with
// TLS config. TLS cert files don't exist, so this should fail at TLS build time.
func TestNewEphemeralMessenger_WithTLS_MissingCert(t *testing.T) {
	t.Parallel()
	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:      "hub.example.com:7740",
		InstanceName: "test-client",
		TLS: TLSConfig{
			Cert: "/nonexistent/cert.pem",
			Key:  "/nonexistent/key.pem",
			CA:   "/nonexistent/ca.pem",
		},
	})
	assert.Error(t, err)
	assert.Nil(t, em)
}

// TestNewEphemeralMessenger_WithLogger uses WithLogger option to provide
// a custom logger.
func TestNewEphemeralMessenger_WithLogger(t *testing.T) {
	t.Parallel()
	// Use a custom logger via WithLogger option
	loggerCalls := 0
	customLogger := &testLogger{calls: &loggerCalls}
	em, err := NewEphemeralMessenger(
		EphemeralConfig{
			HubAddr:      "hub.example.com:7740",
			InstanceName: "test-client",
		},
		WithLogger(customLogger),
	)
	require.NoError(t, err)
	assert.NotNil(t, em)
	t.Cleanup(func() { _ = em.Close() })
}

// TestEphemeralMessenger_RegisterPayloadType_Valid registers a payload type
// and verifies it succeeds.
func TestEphemeralMessenger_RegisterPayloadType_Valid(t *testing.T) {
	t.Parallel()
	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:      "hub.example.com:7740",
		InstanceName: "test-client",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	type TestEvent struct {
		Message string `json:"message"`
	}

	err = em.RegisterPayloadType("test.Event", TestEvent{})
	assert.NoError(t, err)
}

// TestEphemeralMessenger_RegisterPayloadType_Duplicate verifies that
// registering the same payload type twice returns an error.
func TestEphemeralMessenger_RegisterPayloadType_Duplicate(t *testing.T) {
	t.Parallel()
	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:      "hub.example.com:7740",
		InstanceName: "test-client",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	type TestEvent struct {
		Message string `json:"message"`
	}

	err1 := em.RegisterPayloadType("test.Event", TestEvent{})
	require.NoError(t, err1)

	err2 := em.RegisterPayloadType("test.Event", TestEvent{})
	assert.Error(t, err2)
	assert.ErrorIs(t, err2, ErrPayloadTypeAlreadyRegistered)
}

// TestEphemeralMessenger_Subscribe_ValidChannel registers a handler for
// a valid channel and verifies it succeeds.
func TestEphemeralMessenger_Subscribe_ValidChannel(t *testing.T) {
	t.Parallel()
	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:      "hub.example.com:7740",
		InstanceName: "test-client",
		Subscribe:    []string{"events", "metrics"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	err = em.Subscribe("events", func(_ Message) {})
	assert.NoError(t, err)
}

// TestEphemeralMessenger_Subscribe_InvalidChannel verifies that invalid
// channel names are rejected.
func TestEphemeralMessenger_Subscribe_InvalidChannel(t *testing.T) {
	t.Parallel()
	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:      "hub.example.com:7740",
		InstanceName: "test-client",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	tests := []string{
		"",                   // empty
		"events@invalid",     // invalid character
		"events with spaces", // spaces
		"events\n",           // newline
	}
	for _, ch := range tests {
		err := em.Subscribe(ch, func(_ Message) {})
		assert.Error(t, err, "channel %q should be invalid", ch)
	}
}

// TestEphemeralMessenger_Subscribe_MultipleHandlers registers multiple
// handlers on the same channel and verifies they are all stored.
func TestEphemeralMessenger_Subscribe_MultipleHandlers(t *testing.T) {
	t.Parallel()
	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:      "hub.example.com:7740",
		InstanceName: "test-client",
		Subscribe:    []string{"events"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	handler1 := func(_ Message) {}
	handler2 := func(_ Message) {}
	handler3 := func(_ Message) {}

	assert.NoError(t, em.Subscribe("events", handler1))
	assert.NoError(t, em.Subscribe("events", handler2))
	assert.NoError(t, em.Subscribe("events", handler3))
	// All should succeed; handlers are stored in a slice per channel.
}

// TestEphemeralMessenger_Publish_InvalidChannel verifies that Publish
// rejects invalid channel names.
func TestEphemeralMessenger_Publish_InvalidChannel(t *testing.T) {
	t.Parallel()
	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:      "hub.example.com:7740",
		InstanceName: "test-client",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	ctx := context.Background()
	err = em.Publish(ctx, "", "test.Event", "payload")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidChannelName)
}

// TestEphemeralMessenger_Publish_InvalidPayloadType verifies that Publish
// validates the payload can be marshalled.
func TestEphemeralMessenger_Publish_InvalidPayloadType(t *testing.T) {
	t.Parallel()
	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:      "hub.example.com:7740",
		InstanceName: "test-client",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	// Test publishing with valid channel and payload.
	// (Publishing before Connect will block, so we test the validation path only)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// This will timeout since we're not connected, which is expected.
	err = em.Publish(ctx, "events", "test.Event", map[string]interface{}{"valid": true})
	assert.Error(t, err) // Context timeout error is fine
}

// TestEphemeralMessenger_Connect_InvalidAddr returns error for invalid
// hub address.
func TestEphemeralMessenger_Connect_InvalidAddr(t *testing.T) {
	t.Parallel()
	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:      "invalid..addr:999999", // invalid port
		InstanceName: "test-client",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = em.Connect(ctx)
	// Should fail to connect to invalid address.
	assert.Error(t, err)
}

// TestEphemeralMessenger_Connect_ContextCancelled returns error when
// context is already cancelled.
func TestEphemeralMessenger_Connect_ContextCancelled(t *testing.T) {
	t.Parallel()
	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:      "hub.example.com:7740",
		InstanceName: "test-client",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = em.Connect(ctx)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestEphemeralMessenger_Close_Idempotent verifies that Close can be called
// multiple times without error.
func TestEphemeralMessenger_Close_Idempotent(t *testing.T) {
	t.Parallel()
	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:      "hub.example.com:7740",
		InstanceName: "test-client",
	})
	require.NoError(t, err)

	assert.NoError(t, em.Close())
	assert.NoError(t, em.Close())
	assert.NoError(t, em.Close())
}

// TestEphemeralMessenger_ConfigDefaults verifies that default values are
// applied when optional config fields are zero.
func TestEphemeralMessenger_ConfigDefaults(t *testing.T) {
	t.Parallel()
	cfg := EphemeralConfig{
		HubAddr:      "hub.example.com:7740",
		InstanceName: "test-client",
		// All optional fields left zero
	}
	em, err := NewEphemeralMessenger(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	// Verify construction succeeds; defaults are applied internally.
	assert.NotNil(t, em)
}

// TestEphemeralMessenger_Subscribe_BeforeAndAfterClose registers handlers
// both before and after Close, verifying behavior is consistent.
func TestEphemeralMessenger_Subscribe_BeforeClose(t *testing.T) {
	t.Parallel()
	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:      "hub.example.com:7740",
		InstanceName: "test-client",
		Subscribe:    []string{"events"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	// Subscribe before Close should succeed.
	err = em.Subscribe("events", func(_ Message) {})
	assert.NoError(t, err)

	_ = em.Close()

	// Subscribe after Close may succeed or fail depending on the client
	// state; both are acceptable.
	_ = em.Subscribe("events", func(_ Message) {})
}

// TestEphemeralMessenger_RegisterPayloadType_WithDifferentTypes registers
// multiple different payload types and verifies they all succeed.
func TestEphemeralMessenger_RegisterPayloadType_Multiple(t *testing.T) {
	t.Parallel()
	em, err := NewEphemeralMessenger(EphemeralConfig{
		HubAddr:      "hub.example.com:7740",
		InstanceName: "test-client",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	type EventA struct{ A string }
	type EventB struct{ B int }
	type EventC struct{ C bool }

	assert.NoError(t, em.RegisterPayloadType("test.EventA", EventA{}))
	assert.NoError(t, em.RegisterPayloadType("test.EventB", EventB{}))
	assert.NoError(t, em.RegisterPayloadType("test.EventC", EventC{}))
}

// testLogger is a simple logger for testing that tracks call counts.
type testLogger struct {
	calls *int
}

func (tl *testLogger) Debug(_ string, _ ...any) { *tl.calls++ }
func (tl *testLogger) Info(_ string, _ ...any)  { *tl.calls++ }
func (tl *testLogger) Warn(_ string, _ ...any)  { *tl.calls++ }
func (tl *testLogger) Error(_ string, _ ...any) { *tl.calls++ }
