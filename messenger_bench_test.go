package messenger

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/tlsutil"
)

// BenchmarkPublishThroughput measures the sustained write rate for Publish on a
// single channel with no subscribers and no federation, with different sync settings.
func BenchmarkPublishThroughput(b *testing.B) {
	tests := []struct {
		name               string
		syncIntervalMS     int
		offsetFlushInterMS int
	}{
		{"SyncImmediate", 0, 0},
		{"SyncBatched200ms", 200, 200},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			dir := b.TempDir()
			cfg := &Config{
				Storage: StorageConfig{
					DataDir:               dir,
					SyncIntervalMS:        tt.syncIntervalMS,
					OffsetFlushIntervalMS: tt.offsetFlushInterMS,
				},
			}
			cfg.ApplyDefaults()
			m, err := New(cfg, WithTestIdentity("test-instance"))
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
		})
	}
}

// BenchmarkSubscribeLatency measures the time from when Publish is called to
// when the subscriber handler is invoked (same-process LocalNotifier fast path).
// The reported ns/op is the end-to-end delivery latency per message.
func BenchmarkSubscribeLatency(b *testing.B) {
	tests := []struct {
		name               string
		syncIntervalMS     int
		offsetFlushInterMS int
	}{
		{"SyncImmediate", 0, 0},
		{"SyncBatched200ms", 200, 200},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			dir := b.TempDir()
			cfg := &Config{
				Storage: StorageConfig{
					DataDir:               dir,
					SyncIntervalMS:        tt.syncIntervalMS,
					OffsetFlushIntervalMS: tt.offsetFlushInterMS,
				},
			}
			cfg.ApplyDefaults()
			m, err := New(cfg, WithTestIdentity("test-instance"))
			require.NoError(b, err)
			b.Cleanup(func() { _ = m.Close() })
			registerMapTypes(b, m, "bench.Evt")

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
		})
	}
}

// BenchmarkFederationRoundTrip measures the time from publishing on a client
// Messenger to the message being received by a subscriber on the hub Messenger.
// The reported ns/op is the end-to-end federation round-trip latency per message.
func BenchmarkFederationRoundTrip(b *testing.B) {
	tests := []struct {
		name               string
		syncIntervalMS     int
		offsetFlushInterMS int
	}{
		{"SyncImmediate", 0, 0},
		{"SyncBatched200ms", 200, 200},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			clientM, hubM := newFederationPair(b, tt.syncIntervalMS, tt.offsetFlushInterMS)

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
		})
	}
}

// federationBatchSize is the number of messages published per PublishBatch in
// the throughput benchmark. Publishing in batches gives the client one fsync per
// batch and writes the records contiguously, so the client's outbound reader
// ships them as a multi-record wire batch and the hub commits each with a single
// WriteBatch — the path that receive-side batching optimises. A serial,
// wait-per-message round trip keeps every batch at size 1 and cannot show it.
const federationBatchSize = 50

// BenchmarkFederationThroughput measures sustained client→hub federation
// throughput with the client publishing in batches of federationBatchSize. The
// reported ns/op is per message. Run it before and after a receive-side change
// to see the effect of batched hub commits.
func BenchmarkFederationThroughput(b *testing.B) {
	tests := []struct {
		name               string
		syncIntervalMS     int
		offsetFlushInterMS int
	}{
		{"SyncImmediate", 0, 0},
		{"SyncBatched200ms", 200, 200},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			clientM, hubM := newFederationPair(b, tt.syncIntervalMS, tt.offsetFlushInterMS)

			var received atomic.Int64
			done := make(chan struct{})
			var once sync.Once
			require.NoError(b, hubM.Subscribe(context.Background(), "bench", "hub-sub",
				func(_ context.Context, _ Message) error {
					if received.Add(1) == int64(b.N) {
						once.Do(func() { close(done) })
					}
					return nil
				}))

			ctx := context.Background()
			batch := make([]BatchMessage, federationBatchSize)
			for i := range batch {
				batch[i] = BatchMessage{PayloadType: "bench.Evt", Payload: map[string]any{"k": "v"}}
			}

			b.ResetTimer()
			for sent := 0; sent < b.N; {
				n := federationBatchSize
				if rem := b.N - sent; rem < n {
					n = rem
				}
				if err := clientM.PublishBatch(ctx, "bench", batch[:n]); err != nil {
					b.Fatal(err)
				}
				sent += n
			}
			select {
			case <-done:
			case <-time.After(60 * time.Second):
				b.Fatalf("only %d of %d messages received", received.Load(), b.N)
			}
			b.StopTimer()
		})
	}
}

// newFederationPair builds a connected client+hub Messenger pair sharing one CA,
// with the given sync settings, and registers cleanups. The client publishes to
// channel "bench"; the hub is the receiving side.
func newFederationPair(b *testing.B, syncIntervalMS, offsetFlushInterMS int) (clientM, hubM *Messenger) {
	b.Helper()
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
		Storage: StorageConfig{
			DataDir:               filepath.Join(dir, "hub"),
			SyncIntervalMS:        syncIntervalMS,
			OffsetFlushIntervalMS: offsetFlushInterMS,
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
	hubM, err = New(hubCfg)
	require.NoError(b, err)
	b.Cleanup(func() { _ = hubM.Close() })
	registerMapTypes(b, hubM, "bench.Evt")

	_, port, err := net.SplitHostPort(hubM.HubAddr())
	require.NoError(b, err)

	clientCfg := &Config{
		Storage: StorageConfig{
			DataDir:               filepath.Join(dir, "client"),
			SyncIntervalMS:        syncIntervalMS,
			OffsetFlushIntervalMS: offsetFlushInterMS,
		},
		Client: ClientConfig{
			Enabled: true,
			Hubs:    []ClientHubRef{{Addr: net.JoinHostPort("127.0.0.1", port), Publish: []string{"bench"}}},
		},
		TLS: TLSConfig{
			Cert: writePEM("client.crt", clientCert),
			Key:  writePEM("client.key", clientKey),
			CA:   caFile,
		},
	}
	clientCfg.ApplyDefaults()
	clientM, err = New(clientCfg)
	require.NoError(b, err)
	b.Cleanup(func() { _ = clientM.Close() })

	// Allow the federation connection to establish before benchmarking.
	time.Sleep(150 * time.Millisecond)

	return clientM, hubM
}
