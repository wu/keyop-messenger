package messenger

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/tlsutil"
)

// testConfig returns a minimal valid Config for a temporary data directory.
func testConfig(dataDir string) *Config {
	cfg := &Config{
		Name: "test-instance",
		Storage: StorageConfig{
			DataDir:    dataDir,
			SyncPolicy: SyncPolicyNone,
		},
	}
	cfg.ApplyDefaults()
	return cfg
}

// TestEndToEnd verifies the full Publish → handler path in a single instance.
func TestEndToEnd(t *testing.T) {
	dir := t.TempDir()
	m, err := New(testConfig(dir))
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
	m1, err := New(testConfig(dir))
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
	m2, err := New(testConfig(dir))
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
	m, err := New(testConfig(dir))
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

// TestFederationEndToEnd publishes on a client Messenger and asserts that a
// subscriber on the hub Messenger receives the message.
func TestFederationEndToEnd(t *testing.T) {
	dir := t.TempDir()

	// Generate a shared CA and per-instance certificates.
	// The hub cert uses "localhost" as its name so that the DNS SAN matches the
	// hostname the client will dial (TLS does not accept IP addresses unless an
	// IP SAN is present, and GenerateInstance only sets a DNS SAN).
	caCert, caKey, err := tlsutil.GenerateCA(90)
	require.NoError(t, err)

	hubCert, hubKey, err := tlsutil.GenerateInstance(caCert, caKey, "localhost", 90)
	require.NoError(t, err)

	clientCert, clientKey, err := tlsutil.GenerateInstance(caCert, caKey, "test-client", 90)
	require.NoError(t, err)

	writePEM := func(name string, data []byte) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, data, 0o600))
		return p
	}
	caFile := writePEM("ca.crt", caCert)

	// Hub Messenger — listen on 127.0.0.1:0 (OS assigns port).
	hubDir := filepath.Join(dir, "hub-data")
	hubCfg := &Config{
		Name: "test-hub",
		Storage: StorageConfig{
			DataDir:    hubDir,
			SyncPolicy: SyncPolicyNone,
		},
		Hub: HubConfig{
			Enabled:      true,
			ListenAddr:   "127.0.0.1:0",
			AllowedPeers: []AllowedPeer{{Name: "test-client"}},
		},
		TLS: TLSConfig{
			Cert: writePEM("hub.crt", hubCert),
			Key:  writePEM("hub.key", hubKey),
			CA:   caFile,
		},
	}
	hubCfg.ApplyDefaults()

	hubM, err := New(hubCfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = hubM.Close() })

	// hub.Addr() returns "127.0.0.1:PORT"; translate to "localhost:PORT" so that
	// TLS hostname verification passes against the DNS SAN "localhost".
	boundAddr := hubM.HubAddr()
	require.NotEmpty(t, boundAddr, "hub must be listening")
	_, port, err := net.SplitHostPort(boundAddr)
	require.NoError(t, err)
	hubAddr := net.JoinHostPort("localhost", port)

	// Client Messenger.
	clientDir := filepath.Join(dir, "client-data")
	clientCfg := &Config{
		Name: "test-client",
		Storage: StorageConfig{
			DataDir:    clientDir,
			SyncPolicy: SyncPolicyNone,
		},
		Client: ClientConfig{
			Enabled: true,
			Hubs:    []ClientHubRef{{Addr: hubAddr}},
		},
		TLS: TLSConfig{
			Cert: writePEM("client.crt", clientCert),
			Key:  writePEM("client.key", clientKey),
			CA:   caFile,
		},
	}
	clientCfg.ApplyDefaults()

	clientM, err := New(clientCfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientM.Close() })

	// Subscribe on the hub for the "orders" channel.
	received := make(chan Message, 1)
	require.NoError(t, hubM.Subscribe(context.Background(), "orders", "hub-sub",
		func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		}))

	// Publish from the client.
	require.NoError(t, clientM.Publish(context.Background(), "orders", "test.Order",
		map[string]any{"id": "xyz"}))

	select {
	case msg := <-received:
		assert.Equal(t, "orders", msg.Channel)
		assert.Equal(t, "test-client", msg.Origin)
	case <-time.After(2 * time.Second):
		t.Fatal("hub subscriber did not receive message from client within 2s")
	}
}

// TestCloseIdempotent verifies that Close can be called more than once safely.
func TestCloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	m, err := New(testConfig(dir))
	require.NoError(t, err)

	assert.NoError(t, m.Close())
	assert.NoError(t, m.Close())
}

// TestPublishAfterClose verifies that Publish returns ErrMessengerClosed.
func TestPublishAfterClose(t *testing.T) {
	dir := t.TempDir()
	m, err := New(testConfig(dir))
	require.NoError(t, err)
	require.NoError(t, m.Close())

	err = m.Publish(context.Background(), "ch", "test.T", nil)
	assert.ErrorIs(t, err, ErrMessengerClosed)
}

// TestInvalidChannelName verifies that invalid channel names are rejected.
func TestInvalidChannelName(t *testing.T) {
	dir := t.TempDir()
	m, err := New(testConfig(dir))
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
	m, err := New(testConfig(dir))
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
	m, err := New(testConfig(dir))
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
	m, err := New(testConfig(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	type Evt struct{}
	require.NoError(t, m.RegisterPayloadType("test.Evt", Evt{}))
	err = m.RegisterPayloadType("test.Evt", Evt{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrPayloadTypeAlreadyRegistered)
}

// TestInstanceName verifies that InstanceName returns the configured instance name.
func TestInstanceName(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.Name = "my-messenger-instance"

	m, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	assert.Equal(t, "my-messenger-instance", m.InstanceName())
}

// TestInstanceNameDefault verifies that InstanceName returns the default name when not explicitly set.
func TestInstanceNameDefault(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Storage: StorageConfig{
			DataDir:    dir,
			SyncPolicy: SyncPolicyNone,
		},
	}
	cfg.ApplyDefaults()

	m, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	// After ApplyDefaults, Name should be set to the hostname
	assert.NotEmpty(t, m.InstanceName())
}
