package messenger

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// throughputRun is the shared body for both throughput tests.
func throughputRun(t *testing.T, count int, flushIntervalMS int) {
	t.Helper()
	// 16-byte data value; the full serialised envelope is larger (UUID,
	// timestamp, JSON framing, etc.).
	payload := map[string]any{"data": "0123456789abcdef"}

	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.Storage.OffsetFlushIntervalMS = flushIntervalMS
	m, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	var received atomic.Int64
	done := make(chan struct{})

	require.NoError(t, m.Subscribe(context.Background(), "tput", "sub",
		func(_ context.Context, _ Message) error {
			if received.Add(1) == int64(count) {
				close(done)
			}
			return nil
		},
	))

	start := time.Now()

	ctx := context.Background()
	for i := 0; i < count; i++ {
		if err := m.Publish(ctx, "tput", "test.Evt", payload); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatalf("timed out: received %d/%d messages", received.Load(), count)
	}

	elapsed := time.Since(start)
	t.Logf("%d messages, offset_flush_interval=%dms → %s → %.0f msg/s",
		count, flushIntervalMS, elapsed.Round(time.Millisecond), float64(count)/elapsed.Seconds())
}

// TestThroughputFlushEveryMessage measures end-to-end throughput with the
// strictest at-least-once setting: the offset file is fsynced after every
// delivered message (OffsetFlushIntervalMS=0).
//
// Run with:
//
//	go test -v -run TestThroughput -count=1
func TestThroughputFlushEveryMessage(t *testing.T) {
	throughputRun(t, 1_000, 0)
}

// TestThroughputFlush1s measures end-to-end throughput with offset flushes
// batched to at most once per second (OffsetFlushIntervalMS=1000). On a crash
// the subscriber may replay up to ~1 second of already-delivered messages.
//
// Run with:
//
//	go test -v -run TestThroughput -count=1
func TestThroughputFlush1s(t *testing.T) {
	throughputRun(t, 1_000, 1000)
}
