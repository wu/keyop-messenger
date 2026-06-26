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
	"github.com/wu/keyop-messenger/internal/envelope"
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

// payloadRegistrar is satisfied by both *Messenger and *EphemeralMessenger.
type payloadRegistrar interface {
	RegisterPayloadType(typeStr string, prototype any) error
}

// registerMapTypes registers each typeStr to decode into map[string]any — the
// payload shape used by delivery tests and benchmarks that publish a bare map
// rather than a concrete struct. Required because unregistered payload types are
// no longer delivered (the durable subscriber dead-letters them; the ephemeral
// subscriber skips them); a subscriber only receives a type it has registered.
// Takes testing.TB so it is usable from both tests and benchmarks.
func registerMapTypes(tb testing.TB, r payloadRegistrar, types ...string) {
	tb.Helper()
	for _, ty := range types {
		require.NoError(tb, r.RegisterPayloadType(ty, map[string]any{}))
	}
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

// TestPublishBatch_EndToEnd verifies every message in a batch is delivered in
// order with its fields intact.
func TestPublishBatch_EndToEnd(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	type Evt struct {
		N int `json:"n"`
	}
	require.NoError(t, m.RegisterPayloadType("test.Evt", Evt{}))

	const n = 20
	received := make(chan Message, n)
	require.NoError(t, m.Subscribe(context.Background(), "events", "sub1",
		func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		}))

	msgs := make([]BatchMessage, n)
	for i := 0; i < n; i++ {
		msgs[i] = BatchMessage{PayloadType: "test.Evt", Payload: Evt{N: i}}
	}
	require.NoError(t, m.PublishBatch(context.Background(), "events", msgs))

	for i := 0; i < n; i++ {
		select {
		case msg := <-received:
			assert.Equal(t, "events", msg.Channel)
			assert.Equal(t, "test-instance", msg.Origin)
			assert.Equal(t, "test.Evt", msg.PayloadType)
			evt, ok := msg.Payload.(Evt)
			require.True(t, ok, "payload should be Evt, got %T", msg.Payload)
			assert.Equal(t, i, evt.N, "message %d delivered out of order", i)
		case <-time.After(time.Second):
			t.Fatalf("only %d of %d batched messages delivered", i, n)
		}
	}
}

// TestPublishBatch_MixedTypes verifies a single batch can carry different
// payload types on the same channel, each decoded to its registered prototype.
func TestPublishBatch_MixedTypes(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	type Alert struct {
		Level string `json:"level"`
	}
	type Metric struct {
		V float64 `json:"v"`
	}
	require.NoError(t, m.RegisterPayloadType("test.Alert", Alert{}))
	require.NoError(t, m.RegisterPayloadType("test.Metric", Metric{}))

	received := make(chan Message, 3)
	require.NoError(t, m.Subscribe(context.Background(), "events", "sub1",
		func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		}))

	require.NoError(t, m.PublishBatch(context.Background(), "events", []BatchMessage{
		{PayloadType: "test.Alert", Payload: Alert{Level: "warn"}},
		{PayloadType: "test.Metric", Payload: Metric{V: 0.5}},
		{PayloadType: "test.Alert", Payload: Alert{Level: "crit"}},
	}))

	want := []struct {
		typ string
		val any
	}{
		{"test.Alert", Alert{Level: "warn"}},
		{"test.Metric", Metric{V: 0.5}},
		{"test.Alert", Alert{Level: "crit"}},
	}
	for i, w := range want {
		select {
		case msg := <-received:
			assert.Equal(t, w.typ, msg.PayloadType, "message %d type", i)
			assert.Equal(t, w.val, msg.Payload, "message %d payload", i)
		case <-time.After(time.Second):
			t.Fatalf("only %d of %d mixed-type messages delivered", i, len(want))
		}
	}
}

// TestPublishBatch_Empty verifies an empty batch is a no-op that delivers nothing.
func TestPublishBatch_Empty(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	delivered := make(chan struct{}, 1)
	require.NoError(t, m.Subscribe(context.Background(), "events", "sub1",
		func(_ context.Context, _ Message) error {
			delivered <- struct{}{}
			return nil
		}))

	require.NoError(t, m.PublishBatch(context.Background(), "events", nil))
	require.NoError(t, m.PublishBatch(context.Background(), "events", []BatchMessage{}))

	time.Sleep(150 * time.Millisecond)
	assert.Empty(t, delivered, "empty batch must deliver nothing")
}

// TestPublishBatch_Guards verifies PublishBatch enforces the same channel-name
// and closed-messenger guards as Publish.
func TestPublishBatch_Guards(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)

	require.ErrorIs(t, m.PublishBatch(context.Background(), "bad name",
		[]BatchMessage{{PayloadType: "test.T", Payload: 1}}), ErrInvalidChannelName)

	require.NoError(t, m.Close())
	require.ErrorIs(t, m.PublishBatch(context.Background(), "ch",
		[]BatchMessage{{PayloadType: "test.T", Payload: 1}}), ErrMessengerClosed)
}

// TestPublishToDeadLetterRejected verifies the reserved ".dead-letter" suffix is
// rejected on the publish paths but still subscribable for monitoring.
func TestPublishToDeadLetterRejected(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	defer func() { _ = m.Close() }()

	ctx := context.Background()

	require.ErrorIs(t, m.Publish(ctx, "orders.dead-letter", "test.T", 1),
		ErrReservedChannelName)
	require.ErrorIs(t, m.PublishBatch(ctx, "orders.dead-letter",
		[]BatchMessage{{PayloadType: "test.T", Payload: 1}}), ErrReservedChannelName)

	// A nested suffix is rejected too.
	require.ErrorIs(t, m.Publish(ctx, "a.b.dead-letter", "test.T", 1),
		ErrReservedChannelName)

	// Publishing to a normal channel is unaffected.
	require.NoError(t, m.Publish(ctx, "orders", "test.T", 1))

	// Subscribing to a dead-letter channel (for monitoring/reprocessing) is allowed.
	require.NoError(t, m.Subscribe(ctx, "orders.dead-letter", "dlq-monitor",
		func(context.Context, Message) error { return nil }))
}

// TestChannelRetention verifies dead-letter channels get a bounded default
// retention when none is globally configured, and otherwise inherit the
// configured storage retention like any other channel.
func TestChannelRetention(t *testing.T) {
	// No global retention: regular channels keep until consumed (0); dead-letter
	// channels fall back to the bounded default.
	m, err := newForTest(t.TempDir())
	require.NoError(t, err)
	defer func() { _ = m.Close() }()

	assert.Equal(t, time.Duration(0), m.channelRetention("orders"))
	assert.Equal(t, defaultDeadLetterRetention, m.channelRetention("orders.dead-letter"))

	// Global retention configured: every channel, dead-letter included, inherits it.
	cfg := testConfig(t.TempDir())
	cfg.Storage.RetentionAge = Duration{48 * time.Hour}
	m2, err := New(cfg, WithTestIdentity("test-instance"))
	require.NoError(t, err)
	defer func() { _ = m2.Close() }()

	assert.Equal(t, 48*time.Hour, m2.channelRetention("orders"))
	assert.Equal(t, 48*time.Hour, m2.channelRetention("orders.dead-letter"))
}

// TestChannelRollThreshold verifies dead-letter channels roll at the smaller
// dead-letter segment size while regular channels use the main threshold, and
// that both honour explicit config.
func TestChannelRollThreshold(t *testing.T) {
	const mb = 1024 * 1024

	// Defaults: 256 MB regular, 64 MB dead-letter.
	m, err := newForTest(t.TempDir())
	require.NoError(t, err)
	defer func() { _ = m.Close() }()

	assert.Equal(t, int64(256*mb), m.channelRollThreshold("orders"))
	assert.Equal(t, int64(64*mb), m.channelRollThreshold("orders.dead-letter"))

	// Explicit config is honoured for both channel kinds.
	cfg := testConfig(t.TempDir())
	cfg.Storage.CompactionThresholdMB = 32
	cfg.Storage.DeadLetterCompactionThresholdMB = 8
	m2, err := New(cfg, WithTestIdentity("test-instance"))
	require.NoError(t, err)
	defer func() { _ = m2.Close() }()

	assert.Equal(t, int64(32*mb), m2.channelRollThreshold("orders"))
	assert.Equal(t, int64(8*mb), m2.channelRollThreshold("orders.dead-letter"))
}

// TestSubscribe_PerSubscriptionRetryOverride verifies WithMaxRetries overrides the
// instance default for a single subscription: with WithMaxRetries(0) the failing
// handler is invoked exactly once, whereas the instance default of 5 would invoke
// it again after the first backoff (~100ms).
func TestSubscribe_PerSubscriptionRetryOverride(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir) // instance default max_retries = 5
	require.NoError(t, err)
	defer func() { _ = m.Close() }()
	registerMapTypes(t, m, "test.T")

	ctx := context.Background()
	calls := make(chan struct{}, 16)

	require.NoError(t, m.Subscribe(ctx, "events", "sub",
		func(_ context.Context, _ Message) error {
			calls <- struct{}{}
			return fmt.Errorf("always fail")
		},
		WithMaxRetries(0),
	))

	require.NoError(t, m.Publish(ctx, "events", "test.T", map[string]any{"n": 1}))

	// First (and, with WithMaxRetries(0), only) attempt.
	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("handler was not invoked")
	}

	// The instance default of 5 would retry after ~100ms; the override must suppress it.
	select {
	case <-calls:
		t.Fatal("handler was retried despite WithMaxRetries(0)")
	case <-time.After(400 * time.Millisecond):
	}
}

// TestAtLeastOnce verifies that a restarted subscriber resumes from its
// persisted offset and does not replay already-delivered messages.
func TestAtLeastOnce(t *testing.T) {
	dir := t.TempDir()

	// First instance: subscribe, publish, verify delivery, then close.
	m1, err := newForTest(dir)
	require.NoError(t, err)
	registerMapTypes(t, m1, "test.Evt")

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
	registerMapTypes(t, m, "test.Evt")

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
	registerMapTypes(t, m, "test.Evt")

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

// TestWriteLocalEnvelope_RejectsInvalidChannel verifies the federation inbound
// write path validates the (attacker-influenced) channel name before it becomes
// a filesystem path via channelDir -> filepath.Join. The receive policy is not a
// sufficient guard (an empty publish allowlist makes AllowReceive accept any
// channel), so a buggy/compromised peer could otherwise traverse outside the
// channel tree or inject into a reserved ".dead-letter" channel.
func TestWriteLocalEnvelope_RejectsInvalidChannel(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	cases := []struct {
		name    string
		channel string
		wantErr error
	}{
		{"path traversal", "../escape", ErrInvalidChannelName},
		{"nested traversal", "../../etc/passwd", ErrInvalidChannelName},
		{"slash", "a/b", ErrInvalidChannelName},
		{"empty", "", ErrInvalidChannelName},
		{"dead-letter suffix", "orders.dead-letter", ErrReservedChannelName},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env, nerr := envelope.NewEnvelope(tc.channel, "peer", "test.Evt", map[string]string{"k": "v"})
			require.NoError(t, nerr)
			writeErr := m.writeLocalEnvelope(&env)
			require.Error(t, writeErr, "invalid channel must be rejected")
			assert.ErrorIs(t, writeErr, tc.wantErr)
		})
	}

	// "../escape" would resolve to dir/escape after filepath.Join cleaning; it
	// must not have been created.
	_, escErr := os.Stat(filepath.Join(dir, "escape"))
	assert.True(t, os.IsNotExist(escErr), "path-traversal write must not escape the channel tree")
	_, dlErr := os.Stat(filepath.Join(dir, "channels", "orders.dead-letter"))
	assert.True(t, os.IsNotExist(dlErr), "dead-letter channel must not be created")
}

// TestWriteLocalEnvelopeBatch_SkipsInvalidGroupKeepsValid verifies that an
// invalid channel name in a federated batch drops only that channel group and
// does NOT fail the whole batch — failing would make the sender resend forever.
// Valid groups in the same batch are still committed.
func TestWriteLocalEnvelopeBatch_SkipsInvalidGroupKeepsValid(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	mk := func(channel, id string) *envelope.Envelope {
		env, nerr := envelope.NewEnvelope(channel, "peer", "test.Evt", map[string]string{"id": id})
		require.NoError(t, nerr)
		return &env
	}

	// Federation batches arrive grouped by channel: one valid group followed by a
	// path-traversal group and a dead-letter group.
	envs := []*envelope.Envelope{
		mk("good", "1"),
		mk("good", "2"),
		mk("../escape", "3"),
		mk("orders.dead-letter", "4"),
	}

	require.NoError(t, m.writeLocalEnvelopeBatch(envs),
		"batch with an invalid group must not fail the whole batch")

	// Valid group landed.
	_, goodErr := os.Stat(filepath.Join(dir, "channels", "good"))
	require.NoError(t, goodErr, "valid channel must be written")
	m.mu.RLock()
	cs := m.channels["good"]
	m.mu.RUnlock()
	require.NotNil(t, cs, "valid channel state must exist")
	assert.Equal(t, int64(2), cs.publishCount.Load(), "both valid records committed")

	// Invalid groups created nothing.
	_, escErr := os.Stat(filepath.Join(dir, "escape"))
	assert.True(t, os.IsNotExist(escErr), "path-traversal channel must not be written")
	_, dlErr := os.Stat(filepath.Join(dir, "channels", "orders.dead-letter"))
	assert.True(t, os.IsNotExist(dlErr), "dead-letter channel must not be written")
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
