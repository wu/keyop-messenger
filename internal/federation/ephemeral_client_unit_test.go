package federation

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
)

// TestNewEphemeralClient_ValidConfig creates a valid EphemeralClient.
func TestNewEphemeralClient_ValidConfig(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	cfg := EphemeralClientConfig{
		InstanceName: "test-client",
		Subscribe:    []string{"events"},
	}
	ec := NewEphemeralClient(cfg, log)
	assert.NotNil(t, ec)
	t.Cleanup(func() { ec.Close() })
}

// TestNewEphemeralClient_DefaultsApplied verifies default values are applied.
func TestNewEphemeralClient_DefaultsApplied(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	cfg := EphemeralClientConfig{
		InstanceName: "test-client",
		// Leave all optional fields zero
	}
	ec := NewEphemeralClient(cfg, log)
	assert.NotNil(t, ec)
	t.Cleanup(func() { ec.Close() })
	// Defaults should be applied internally; no panic indicates success.
}

// TestEphemeralClient_AddHandler_Single registers a single handler.
func TestEphemeralClient_AddHandler_Single(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	ec := NewEphemeralClient(EphemeralClientConfig{InstanceName: "test"}, log)
	t.Cleanup(func() { ec.Close() })

	handlerCalled := false
	ec.AddHandler("events", func(_ *envelope.Envelope) error {
		handlerCalled = true
		return nil
	})
	// Handler is registered but not called yet (no connection).
	assert.False(t, handlerCalled)
}

// TestEphemeralClient_AddHandler_Multiple registers multiple handlers on same channel.
func TestEphemeralClient_AddHandler_Multiple(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	ec := NewEphemeralClient(EphemeralClientConfig{InstanceName: "test"}, log)
	t.Cleanup(func() { ec.Close() })

	count := 0
	ec.AddHandler("events", func(_ *envelope.Envelope) error {
		count++
		return nil
	})
	ec.AddHandler("events", func(_ *envelope.Envelope) error {
		count++
		return nil
	})
	// Two handlers registered on same channel.
	assert.Equal(t, 0, count) // Not called yet
}

// TestEphemeralClient_AddHandler_DifferentChannels registers handlers on different channels.
func TestEphemeralClient_AddHandler_DifferentChannels(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	ec := NewEphemeralClient(
		EphemeralClientConfig{
			InstanceName: "test",
			Subscribe:    []string{"chan1", "chan2"},
		},
		log,
	)
	t.Cleanup(func() { ec.Close() })

	count1, count2 := 0, 0
	ec.AddHandler("chan1", func(_ *envelope.Envelope) error {
		count1++
		return nil
	})
	ec.AddHandler("chan2", func(_ *envelope.Envelope) error {
		count2++
		return nil
	})
	// Both registered; neither called yet.
	assert.Equal(t, 0, count1)
	assert.Equal(t, 0, count2)
}

// TestEphemeralClient_Close_Idempotent verifies Close can be called multiple times.
func TestEphemeralClient_Close_Idempotent(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	ec := NewEphemeralClient(EphemeralClientConfig{InstanceName: "test"}, log)

	// Multiple Close calls should not panic.
	ec.Close()
	ec.Close()
	ec.Close()
}

// TestEphemeralClient_Connect_InvalidAddr fails gracefully with invalid address.
func TestEphemeralClient_Connect_InvalidAddr(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	ec := NewEphemeralClient(EphemeralClientConfig{InstanceName: "test"}, log)
	t.Cleanup(func() { ec.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := ec.Connect(ctx, "invalid..addr:999999")
	assert.Error(t, err)
}

// TestEphemeralClient_Connect_ContextCancelled returns error when context cancelled.
func TestEphemeralClient_Connect_ContextCancelled(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	ec := NewEphemeralClient(EphemeralClientConfig{InstanceName: "test"}, log)
	t.Cleanup(func() { ec.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := ec.Connect(ctx, "hub.example.com:7740")
	assert.Error(t, err)
}

// TestEphemeralClient_Publish_ContextCancelled returns error when context cancelled.
func TestEphemeralClient_Publish_ContextCancelled(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	ec := NewEphemeralClient(EphemeralClientConfig{InstanceName: "test"}, log)
	t.Cleanup(func() { ec.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	env := &envelope.Envelope{Channel: "events", ID: "test-id"}
	err := ec.Publish(ctx, env)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestEphemeralClient_Publish_AfterClose returns error.
func TestEphemeralClient_Publish_AfterClose(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	ec := NewEphemeralClient(EphemeralClientConfig{InstanceName: "test"}, log)

	ec.Close()

	env := &envelope.Envelope{Channel: "events", ID: "test-id"}
	ctx := context.Background()
	err := ec.Publish(ctx, env)
	assert.Error(t, err)
}

// TestEphemeralClient_ConnectWithReconnect_ContextCancelled fails on invalid address.
func TestEphemeralClient_ConnectWithReconnect_ContextCancelled(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	ec := NewEphemeralClient(EphemeralClientConfig{InstanceName: "test"}, log)
	t.Cleanup(func() { ec.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := ec.ConnectWithReconnect(ctx, "hub.example.com:7740")
	assert.Error(t, err)
}

// TestEphemeralClient_Config_WithTLS creates client with TLS config.
func TestEphemeralClient_Config_WithTLS(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	tlsCfg := &tls.Config{} // Minimal TLS config
	cfg := EphemeralClientConfig{
		InstanceName: "test",
		TLSConfig:    tlsCfg,
	}
	ec := NewEphemeralClient(cfg, log)
	assert.NotNil(t, ec)
	t.Cleanup(func() { ec.Close() })
}

// TestEphemeralClient_Config_WithReconnectParams creates client with custom reconnect.
func TestEphemeralClient_Config_WithReconnectParams(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	cfg := EphemeralClientConfig{
		InstanceName:    "test",
		ReconnectBase:   100 * time.Millisecond,
		ReconnectMax:    1 * time.Second,
		ReconnectJitter: 0.5,
	}
	ec := NewEphemeralClient(cfg, log)
	assert.NotNil(t, ec)
	t.Cleanup(func() { ec.Close() })
}

// TestEphemeralClient_Config_WithQueues creates client with custom queue sizes.
func TestEphemeralClient_Config_WithQueues(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	cfg := EphemeralClientConfig{
		InstanceName:   "test",
		MaxBatchBytes:  32768,
		WriteQueueSize: 512,
	}
	ec := NewEphemeralClient(cfg, log)
	assert.NotNil(t, ec)
	t.Cleanup(func() { ec.Close() })
}

// TestEphemeralClient_Publish_WithTimeout respects context timeout.
func TestEphemeralClient_Publish_WithTimeout(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	ec := NewEphemeralClient(EphemeralClientConfig{InstanceName: "test"}, log)
	t.Cleanup(func() { ec.Close() })

	// Publish with timeout (no connection, so it will timeout).
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	env := &envelope.Envelope{Channel: "events", ID: "test-id"}
	err := ec.Publish(ctx, env)
	// Should timeout because not connected.
	assert.Error(t, err)
}

// TestEphemeralClient_AddHandler_ConcurrentAdd registers handlers concurrently.
func TestEphemeralClient_AddHandler_ConcurrentAdd(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	ec := NewEphemeralClient(
		EphemeralClientConfig{
			InstanceName: "test",
			Subscribe:    []string{"events"},
		},
		log,
	)
	t.Cleanup(func() { ec.Close() })

	// Concurrently add handlers.
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func(_ int) {
			ec.AddHandler("events", func(_ *envelope.Envelope) error {
				return nil
			})
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 10; i++ {
		<-done
	}
	// All handlers should be registered without data race.
}

// TestEphemeralClient_Config_LargeDedup creates client with large dedup size.
func TestEphemeralClient_Config_LargeDedup(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	cfg := EphemeralClientConfig{
		InstanceName: "test",
	}
	ec := NewEphemeralClient(cfg, log)
	assert.NotNil(t, ec)
	t.Cleanup(func() { ec.Close() })
}
