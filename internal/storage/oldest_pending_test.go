//nolint:gosec // test file: G301/G304/G306
package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOldestPendingTimestamp verifies that the timestamp of the record at a
// given offset is read back exactly, and that a caught-up offset (== stream end)
// reports no pending record.
func TestOldestPendingTimestamp(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "events")
	require.NoError(t, os.MkdirAll(channelDir, 0o755))

	t1 := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 1, 1, 10, 1, 0, 0, time.UTC)
	n1 := writeTestEnvelope(t, channelDir, makeEnvAt(t, "events", t1, map[string]any{"v": 1}))
	writeTestEnvelope(t, channelDir, makeEnvAt(t, "events", t2, map[string]any{"v": 2}))

	// At offset 0 the oldest pending record is the first envelope.
	ts, ok, err := OldestPendingTimestamp(channelDir, 0)
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, ts.Equal(t1), "want %v, got %v", t1, ts)

	// At the start of the second record the oldest pending is the second envelope.
	ts, ok, err = OldestPendingTimestamp(channelDir, n1)
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, ts.Equal(t2), "want %v, got %v", t2, ts)

	// At stream end there is nothing pending.
	end, err := ChannelStreamEnd(channelDir)
	require.NoError(t, err)
	_, ok, err = OldestPendingTimestamp(channelDir, end)
	require.NoError(t, err)
	assert.False(t, ok, "caught-up offset should report no pending record")
}

// TestOldestPendingTimestamp_EmptyChannel verifies a channel with no segments
// reports no pending record rather than erroring.
func TestOldestPendingTimestamp_EmptyChannel(t *testing.T) {
	dir := t.TempDir()
	_, ok, err := OldestPendingTimestamp(filepath.Join(dir, "nope"), 0)
	require.NoError(t, err)
	assert.False(t, ok)
}
