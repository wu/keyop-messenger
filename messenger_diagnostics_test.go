package messenger

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDiagnosticStats_UnknownChannel verifies the zero value is returned for a
// channel that was never published to or subscribed.
func TestDiagnosticStats_UnknownChannel(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	assert.Equal(t, DiagnosticStats{}, m.DiagnosticStats("nonexistent", "sub1"))
}

// TestDiagnosticStats_UnknownSubscriber verifies the zero value is returned when
// the channel exists but the subscriber ID does not.
func TestDiagnosticStats_UnknownSubscriber(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	require.NoError(t, m.Subscribe(context.Background(), "orders", "sub1",
		func(_ context.Context, _ Message) error { return nil }))

	assert.Equal(t, DiagnosticStats{}, m.DiagnosticStats("orders", "ghost"))
}

// TestDiagnosticStats_HappyPath verifies that after a message is delivered the
// snapshot reflects real subscriber and notifier activity.
func TestDiagnosticStats_HappyPath(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	delivered := make(chan struct{}, 1)
	require.NoError(t, m.Subscribe(context.Background(), "orders", "sub1",
		func(_ context.Context, _ Message) error {
			delivered <- struct{}{}
			return nil
		}))

	require.NoError(t, m.Publish(context.Background(), "orders", "test.Evt",
		map[string]any{"x": 1}))

	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("handler not called within 1s")
	}

	// The handler ran, but the subscriber updates its counters and offsets after
	// the handler returns. Poll briefly for the snapshot to settle.
	var stats DiagnosticStats
	require.Eventually(t, func() bool {
		stats = m.DiagnosticStats("orders", "sub1")
		return stats.Dispatched >= 1 && stats.CurrentOffset > 0
	}, time.Second, 10*time.Millisecond, "stats did not reflect delivery: %+v", stats)

	assert.GreaterOrEqual(t, stats.ProcessCalls, int64(1))
	assert.GreaterOrEqual(t, stats.NotifySent, int64(1))
	assert.GreaterOrEqual(t, stats.CurrentOffset, stats.FlushedOffset,
		"in-memory cursor should be at or ahead of the on-disk cursor")
}
