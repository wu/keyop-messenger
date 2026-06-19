package messenger

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testConfig returns a minimal valid Config for a temporary data directory.
func testConfig(dataDir string) *Config {
	cfg := &Config{
		Storage: StorageConfig{
			DataDir: dataDir,
		},
	}
	cfg.ApplyDefaults()
	return cfg
}

// newForTest constructs a Messenger with a stable test identity. Tests should
// prefer this over calling New directly so they exercise the same TLS-bypass
// path that production code never uses.
func newForTest(dataDir string, opts ...Option) (*Messenger, error) {
	opts = append(opts, WithTestIdentity("test-instance"))
	return New(testConfig(dataDir), opts...)
}

// newEphemeralForTest constructs an EphemeralMessenger with a stable test
// identity. Like newForTest, this is the right helper for tests that don't
// provision real TLS certificates.
func newEphemeralForTest(cfg EphemeralConfig, opts ...Option) (*EphemeralMessenger, error) {
	opts = append(opts, WithTestIdentity("test-client"))
	return NewEphemeralMessenger(cfg, opts...)
}

// TestEndToEnd verifies the full Publish → handler path in a single instance.
func TestEndToEnd(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	type OrderCreated struct {
		OrderID string `json:"order_id"`
	}
	require.NoError(t, m.RegisterPayloadType("test.OrderCreated", OrderCreated{}))

	received := make(chan Message, 1)
	require.NoError(t, m.Subscribe(context.Background(), "orders", "sub1",
		func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		}))

	require.NoError(t, m.Publish(context.Background(), "orders", "test.OrderCreated",
		OrderCreated{OrderID: "abc123"}))

	select {
	case msg := <-received:
		assert.Equal(t, "orders", msg.Channel)
		assert.Equal(t, "test-instance", msg.Origin)
		assert.Equal(t, "test.OrderCreated", msg.PayloadType)
		oc, ok := msg.Payload.(OrderCreated)
		require.True(t, ok, "payload should be OrderCreated, got %T", msg.Payload)
		assert.Equal(t, "abc123", oc.OrderID)
		assert.False(t, msg.Timestamp.IsZero())
	case <-time.After(500 * time.Millisecond):
		t.Fatal("handler not called within 500ms")
	}
}

// TestAtLeastOnce verifies that a restarted subscriber resumes from its
// persisted offset and does not replay already-delivered messages.
func TestAtLeastOnce(t *testing.T) {
	dir := t.TempDir()

	// First instance: subscribe, publish, verify delivery, then close.
	m1, err := newForTest(dir)
	require.NoError(t, err)

	delivered := make(chan struct{}, 10)
	require.NoError(t, m1.Subscribe(context.Background(), "events", "sub1",
		func(_ context.Context, _ Message) error {
			delivered <- struct{}{}
			return nil
		}))

	require.NoError(t, m1.Publish(context.Background(), "events", "test.Evt",
		map[string]any{"x": 1}))

	select {
	case <-delivered:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("first delivery not received")
	}
	require.NoError(t, m1.Close())

	// Second instance: same subscriberID, no new publishes — expect no deliveries.
	m2, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m2.Close() })

	require.NoError(t, m2.Subscribe(context.Background(), "events", "sub1",
		func(_ context.Context, _ Message) error {
			delivered <- struct{}{}
			return nil
		}))

	time.Sleep(150 * time.Millisecond)
	assert.Empty(t, delivered, "no phantom deliveries expected after restart")
}

// TestMultipleSubscribers verifies that each subscriber independently receives
// all published messages on the same channel.
func TestMultipleSubscribers(t *testing.T) {
	const numSubs = 3
	const numMsgs = 5

	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	channels := make([]chan Message, numSubs)
	for i := range channels {
		channels[i] = make(chan Message, numMsgs)
		idx := i
		require.NoError(t, m.Subscribe(context.Background(), "orders",
			fmt.Sprintf("sub%d", idx),
			func(_ context.Context, msg Message) error {
				channels[idx] <- msg
				return nil
			}))
	}

	for i := 0; i < numMsgs; i++ {
		require.NoError(t, m.Publish(context.Background(), "orders", "test.Evt",
			map[string]any{"i": i}))
	}

	for i, ch := range channels {
		got := 0
		deadline := time.After(500 * time.Millisecond)
		for got < numMsgs {
			select {
			case <-ch:
				got++
			case <-deadline:
				t.Fatalf("subscriber %d: received %d/%d messages", i, got, numMsgs)
			}
		}
	}
}

// TestCloseIdempotent verifies that Close can be called more than once safely.
func TestCloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)

	assert.NoError(t, m.Close())
	assert.NoError(t, m.Close())
}

// TestPublishAfterClose verifies that Publish returns ErrMessengerClosed.
func TestPublishAfterClose(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	require.NoError(t, m.Close())

	err = m.Publish(context.Background(), "ch", "test.T", nil)
	assert.ErrorIs(t, err, ErrMessengerClosed)
}

// TestInvalidChannelName verifies that invalid channel names are rejected.
func TestInvalidChannelName(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	cases := []struct {
		name string
		ch   string
	}{
		{"empty", ""},
		{"spaces", "invalid channel"},
		{"slash", "foo/bar"},
		{"too long", string(make([]byte, 256))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := m.Publish(context.Background(), tc.ch, "test.T", nil)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidChannelName,
				"expected ErrInvalidChannelName for channel %q", tc.ch)
		})
	}
}

// TestCtxCancellation verifies that cancelling the Subscribe context stops the
// subscriber goroutine and allows Close to return promptly.
func TestCtxCancellation(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	require.NoError(t, m.Subscribe(ctx, "events", "sub1",
		func(_ context.Context, _ Message) error { return nil }))

	cancel() // cancel the subscription's context

	// Close must return within 1 second; the ctx-watcher goroutine should have
	// already called sub.Stop() so m.wg.Wait() completes quickly.
	done := make(chan error, 1)
	go func() { done <- m.Close() }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close() did not return within 1 second after ctx cancellation")
	}
}

// TestUnsubscribe verifies that Unsubscribe stops the subscriber and removes
// the offset file.
func TestUnsubscribe(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	defer func() { _ = m.Close() }()

	channel := "unsub-test"
	subID := "sub1"

	// 1. Subscribe and publish a message to ensure offset file is created.
	delivered := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, m.Subscribe(ctx, channel, subID, func(_ context.Context, _ Message) error {
		delivered <- struct{}{}
		return nil
	}))

	require.NoError(t, m.Publish(context.Background(), channel, "test.Evt", map[string]any{"x": 1}))

	select {
	case <-delivered:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("delivery not received")
	}

	// Verify offset file exists.
	offsetFile := filepath.Join(dir, "subscribers", channel, subID+".offset")
	_, err = os.Stat(offsetFile)
	assert.NoError(t, err, "offset file should exist")

	// 2. Unsubscribe.
	require.NoError(t, m.Unsubscribe(channel, subID))

	// Verify offset file is removed.
	_, err = os.Stat(offsetFile)
	assert.True(t, os.IsNotExist(err), "offset file should be removed after Unsubscribe")

	// 3. Verify further publishes are not delivered to the stopped subscriber.
	require.NoError(t, m.Publish(context.Background(), channel, "test.Evt", map[string]any{"x": 2}))
	time.Sleep(100 * time.Millisecond)
	assert.Empty(t, delivered, "no further deliveries expected after Unsubscribe")
}

// TestValidateChannelName unit-tests the public validation function.
func TestValidateChannelName(t *testing.T) {
	valid := []string{
		"orders", "order-events", "order.events", "order_events",
		"OrderCreated", "a", strings.Repeat("a", 255),
	}
	for _, ch := range valid {
		assert.NoError(t, ValidateChannelName(ch), "expected valid: %q", ch)
	}

	invalid := []string{
		"", "has space", "has/slash", "has@at", strings.Repeat("a", 256),
	}
	for _, ch := range invalid {
		assert.ErrorIs(t, ValidateChannelName(ch), ErrInvalidChannelName,
			"expected invalid: %q", ch)
	}
}

// TestRegisterPayloadTypeDuplicate verifies the duplicate-registration error.
func TestRegisterPayloadTypeDuplicate(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	type Evt struct{}
	require.NoError(t, m.RegisterPayloadType("test.Evt", Evt{}))
	err = m.RegisterPayloadType("test.Evt", Evt{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrPayloadTypeAlreadyRegistered)
}

// TestInstanceNameFromTestIdentity verifies that WithTestIdentity sets the
// identity returned by InstanceName when no TLS config is present.
func TestInstanceNameFromTestIdentity(t *testing.T) {
	dir := t.TempDir()
	m, err := New(testConfig(dir), WithTestIdentity("my-messenger-instance"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	assert.Equal(t, "my-messenger-instance", m.InstanceName())
}

// TestInstanceName_LocalOnlyUsesHostname verifies that a local-only instance
// (no hub, no client, no TLS, no test identity) successfully constructs and
// derives its identity from the OS hostname.
func TestInstanceName_LocalOnlyUsesHostname(t *testing.T) {
	dir := t.TempDir()
	m, err := New(testConfig(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	hostname, hostErr := os.Hostname()
	require.NoError(t, hostErr)
	assert.Equal(t, hostname, m.InstanceName())
}

// TestInstanceName_FederationRequiresTLS verifies that enabling federation
// (hub or client) without configuring TLS fails fast at construction.
func TestInstanceName_FederationRequiresTLS(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.Hub.Enabled = true
	cfg.Hub.ListenAddr = "127.0.0.1:0"

	_, err := New(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "federation requires TLS")
}

// TestNew_ErrorPathDoesNotLeakAuditWriter verifies that when construction fails
// after the audit writer has been started, New closes it rather than leaking its
// goroutine. A leaked writer keeps creating files under the data dir and races
// t.TempDir() cleanup (the "directory not empty" failure seen on arm64). We
// assert the goroutine count returns to its pre-New baseline.
func TestNew_ErrorPathDoesNotLeakAuditWriter(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.Hub.Enabled = true
	cfg.Hub.ListenAddr = "127.0.0.1:0" // federation enabled but no TLS -> fails after audit writer starts

	before := runtime.NumGoroutine()

	_, err := New(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "federation requires TLS")

	// With the audit writer closed, no goroutine is left behind. Poll in this
	// goroutine (testify's Eventually would spawn its own goroutine and skew the
	// count); without the fix the writer blocks on its channel forever and the
	// count stays above baseline.
	after := runtime.NumGoroutine()
	for i := 0; i < 200 && after > before; i++ {
		time.Sleep(5 * time.Millisecond)
		runtime.GC()
		after = runtime.NumGoroutine()
	}
	assert.LessOrEqual(t, after, before,
		"audit writer goroutine leaked after New returned an error")
}
