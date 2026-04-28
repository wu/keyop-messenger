package messenger

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/tlsutil"
)

// BenchmarkPublishThroughput measures the sustained write rate for Publish on a
// single channel with no subscribers and no federation.
func BenchmarkPublishThroughput(b *testing.B) {
	dir := b.TempDir()
	cfg := &Config{
		Name:    "bench",
		Storage: StorageConfig{DataDir: dir},
	}
	cfg.ApplyDefaults()
	m, err := New(cfg)
	require.NoError(b, err)
	b.Cleanup(func() { _ = m.Close() })

	ctx := context.Background()
	payload := map[string]any{"key": "value", "num": 42}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := m.Publish(ctx, "bench", "bench.Evt", payload); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSubscribeLatency measures the time from when Publish is called to
// when the subscriber handler is invoked (same-process LocalNotifier fast path).
// The reported ns/op is the end-to-end delivery latency per message.
func BenchmarkSubscribeLatency(b *testing.B) {
	dir := b.TempDir()
	cfg := &Config{
		Name:    "bench",
		Storage: StorageConfig{DataDir: dir},
	}
	cfg.ApplyDefaults()
	m, err := New(cfg)
	require.NoError(b, err)
	b.Cleanup(func() { _ = m.Close() })

	notified := make(chan time.Time, 1)
	require.NoError(b, m.Subscribe(context.Background(), "bench", "sub",
		func(_ context.Context, _ Message) error {
			notified <- time.Now()
			return nil
		},
	))

	ctx := context.Background()
	payload := map[string]any{"k": "v"}

	b.ResetTimer()
	var totalNs int64
	for i := 0; i < b.N; i++ {
		sent := time.Now()
		if err := m.Publish(ctx, "bench", "bench.Evt", payload); err != nil {
			b.Fatal(err)
		}
		recv := <-notified
		totalNs += recv.Sub(sent).Nanoseconds()
	}
	b.ReportMetric(float64(totalNs)/float64(b.N), "ns/delivery")
}

// BenchmarkFederationRoundTrip measures the time from publishing on a client
// Messenger to the message being received by a subscriber on the hub Messenger.
// The reported ns/op is the end-to-end federation round-trip latency per message.
func BenchmarkFederationRoundTrip(b *testing.B) {
	dir := b.TempDir()

	caCert, caKey, err := tlsutil.GenerateCA(30)
	require.NoError(b, err)
	hubCert, hubKey, err := tlsutil.GenerateInstance(caCert, caKey, "localhost", 30)
	require.NoError(b, err)
	clientCert, clientKey, err := tlsutil.GenerateInstance(caCert, caKey, "bench-client", 30)
	require.NoError(b, err)

	writePEM := func(name string, data []byte) string {
		p := filepath.Join(dir, name)
		require.NoError(b, os.WriteFile(p, data, 0o600))
		return p
	}
	caFile := writePEM("ca.crt", caCert)

	hubCfg := &Config{
		Name: "bench-hub",
		Storage: StorageConfig{
			DataDir: filepath.Join(dir, "hub"),
		},
		Hub: HubConfig{
			Enabled:      true,
			ListenAddr:   "127.0.0.1:0",
			AllowedPeers: []AllowedPeer{{Name: "bench-client"}},
		},
		TLS: TLSConfig{
			Cert: writePEM("hub.crt", hubCert),
			Key:  writePEM("hub.key", hubKey),
			CA:   caFile,
		},
	}
	hubCfg.ApplyDefaults()
	hubM, err := New(hubCfg)
	require.NoError(b, err)
	b.Cleanup(func() { _ = hubM.Close() })

	_, port, err := net.SplitHostPort(hubM.HubAddr())
	require.NoError(b, err)

	clientCfg := &Config{
		Name: "bench-client",
		Storage: StorageConfig{
			DataDir: filepath.Join(dir, "client"),
		},
		Client: ClientConfig{
			Enabled: true,
			Hubs:    []ClientHubRef{{Addr: net.JoinHostPort("localhost", port)}},
		},
		TLS: TLSConfig{
			Cert: writePEM("client.crt", clientCert),
			Key:  writePEM("client.key", clientKey),
			CA:   caFile,
		},
	}
	clientCfg.ApplyDefaults()
	clientM, err := New(clientCfg)
	require.NoError(b, err)
	b.Cleanup(func() { _ = clientM.Close() })

	// Allow the federation connection to establish before benchmarking.
	time.Sleep(150 * time.Millisecond)

	notified := make(chan time.Time, 1)
	require.NoError(b, hubM.Subscribe(context.Background(), "bench", "hub-sub",
		func(_ context.Context, _ Message) error {
			select {
			case notified <- time.Now():
			default:
			}
			return nil
		},
	))

	ctx := context.Background()
	payload := map[string]any{"k": "v"}

	b.ResetTimer()
	var totalNs int64
	for i := 0; i < b.N; i++ {
		sent := time.Now()
		if err := clientM.Publish(ctx, "bench", "bench.Evt", payload); err != nil {
			b.Fatal(err)
		}
		recv := <-notified
		totalNs += recv.Sub(sent).Nanoseconds()
	}
	b.ReportMetric(float64(totalNs)/float64(b.N), "ns/roundtrip")
}
