package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
)

// writeSegment creates a segment file in channelDir starting at startOffset
// and containing n copies of a fixed JSONL line. Returns the byte size written.
func writeSegment(t *testing.T, channelDir string, startOffset int64, n int) int64 {
	t.Helper()
	require.NoError(t, os.MkdirAll(channelDir, 0o755))
	path := filepath.Join(channelDir, segmentName(startOffset))
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	line := []byte(`{"v":1,"channel":"orders","id":"x","ts":"2026-01-01T00:00:00Z","origin":"h","payload_type":"t","payload":{}}` + "\n")
	for i := 0; i < n; i++ {
		_, err := f.Write(line)
		require.NoError(t, err)
	}
	info, err := f.Stat()
	require.NoError(t, err)
	return info.Size()
}

func newTestCompactor(t *testing.T) (*Compactor, string, string) {
	t.Helper()
	base := t.TempDir()
	channelDir := filepath.Join(base, "orders")
	offsetDir := filepath.Join(base, "offsets")
	require.NoError(t, os.MkdirAll(offsetDir, 0o755))
	return NewCompactor(offsetDir, 0, &testutil.FakeLogger{}), channelDir, offsetDir
}

// TestCompactor_NoSegmentsToDelete verifies MaybeCompact is a no-op when only
// the active segment exists.
func TestCompactor_NoSegmentsToDelete(t *testing.T) {
	c, channelDir, offsetDir := newTestCompactor(t)

	size := writeSegment(t, channelDir, 0, 10)
	require.NoError(t, WriteOffset(filepath.Join(offsetDir, "sub1.offset"), size))
	c.RegisterSubscriber("sub1")

	require.NoError(t, c.MaybeCompact(channelDir))

	segs, err := listSegments(channelDir)
	require.NoError(t, err)
	assert.Len(t, segs, 1, "single active segment must not be deleted")
}

// TestCompactor_DeletesFullyConsumedSegment verifies that a sealed segment
// whose entire content is before minOffset is deleted.
func TestCompactor_DeletesFullyConsumedSegment(t *testing.T) {
	c, channelDir, offsetDir := newTestCompactor(t)

	// Segment 0: 10 lines.
	seg0Size := writeSegment(t, channelDir, 0, 10)
	// Segment 1 (active): 5 lines.
	writeSegment(t, channelDir, seg0Size, 5)

	// Subscriber has consumed all of segment 0.
	require.NoError(t, WriteOffset(filepath.Join(offsetDir, "sub1.offset"), seg0Size))
	c.RegisterSubscriber("sub1")

	require.NoError(t, c.MaybeCompact(channelDir))

	segs, err := listSegments(channelDir)
	require.NoError(t, err)
	require.Len(t, segs, 1, "consumed sealed segment must be deleted")
	assert.Equal(t, seg0Size, segs[0].startOffset, "remaining segment must be segment 1")
}

// TestCompactor_PreservesUnconsumedSegments verifies that segments not yet
// fully consumed are left intact.
func TestCompactor_PreservesUnconsumedSegments(t *testing.T) {
	c, channelDir, offsetDir := newTestCompactor(t)

	seg0Size := writeSegment(t, channelDir, 0, 10)
	writeSegment(t, channelDir, seg0Size, 5)

	// Subscriber is still in segment 0 (hasn't finished it).
	require.NoError(t, WriteOffset(filepath.Join(offsetDir, "sub1.offset"), seg0Size/2))
	c.RegisterSubscriber("sub1")

	require.NoError(t, c.MaybeCompact(channelDir))

	segs, err := listSegments(channelDir)
	require.NoError(t, err)
	assert.Len(t, segs, 2, "unconsumed segment must be preserved")
}

// TestCompactor_MultipleSubscribersUsesMinOffset verifies that compaction
// respects the slowest subscriber.
func TestCompactor_MultipleSubscribersUsesMinOffset(t *testing.T) {
	c, channelDir, offsetDir := newTestCompactor(t)

	seg0Size := writeSegment(t, channelDir, 0, 10)
	writeSegment(t, channelDir, seg0Size, 5)

	// sub1 has consumed segment 0; sub2 has not.
	require.NoError(t, WriteOffset(filepath.Join(offsetDir, "sub1.offset"), seg0Size))
	require.NoError(t, WriteOffset(filepath.Join(offsetDir, "sub2.offset"), seg0Size/2))
	c.RegisterSubscriber("sub1")
	c.RegisterSubscriber("sub2")

	require.NoError(t, c.MaybeCompact(channelDir))

	segs, err := listSegments(channelDir)
	require.NoError(t, err)
	assert.Len(t, segs, 2, "segment must be preserved because sub2 has not consumed it")
}

// TestCompactor_DeletesMultipleSegments verifies deletion of several
// fully-consumed sealed segments in one pass.
func TestCompactor_DeletesMultipleSegments(t *testing.T) {
	c, channelDir, offsetDir := newTestCompactor(t)

	seg0Size := writeSegment(t, channelDir, 0, 5)
	seg1Size := writeSegment(t, channelDir, seg0Size, 5)
	seg2Start := seg0Size + seg1Size
	writeSegment(t, channelDir, seg2Start, 5) // active

	// Subscriber has consumed segments 0 and 1.
	require.NoError(t, WriteOffset(filepath.Join(offsetDir, "sub1.offset"), seg2Start))
	c.RegisterSubscriber("sub1")

	require.NoError(t, c.MaybeCompact(channelDir))

	segs, err := listSegments(channelDir)
	require.NoError(t, err)
	assert.Len(t, segs, 1, "both consumed sealed segments must be deleted")
	assert.Equal(t, seg2Start, segs[0].startOffset)
}

// TestCompactor_DeregisterRemovesOffsetFile verifies that deregistering a
// subscriber deletes its offset file.
func TestCompactor_DeregisterRemovesOffsetFile(t *testing.T) {
	c, _, offsetDir := newTestCompactor(t)

	offsetPath := filepath.Join(offsetDir, "sub1.offset")
	require.NoError(t, WriteOffset(offsetPath, 42))
	c.RegisterSubscriber("sub1")

	require.NoError(t, c.DeregisterSubscriber("sub1"))
	assert.False(t, OffsetFileExists(offsetPath))

	min, err := c.MinOffset()
	require.NoError(t, err)
	assert.Equal(t, int64(0), min)
}

// TestCompactor_LagWarning verifies that a subscriber lagging beyond the
// configured threshold causes a warning to be logged.
func TestCompactor_LagWarning(t *testing.T) {
	base := t.TempDir()
	channelDir := filepath.Join(base, "orders")
	offsetDir := filepath.Join(base, "offsets")
	require.NoError(t, os.MkdirAll(offsetDir, 0o755))
	log := &testutil.FakeLogger{}

	const maxLag = int64(100)
	c := NewCompactor(offsetDir, maxLag, log)

	size := writeSegment(t, channelDir, 0, 20) // > 100 bytes
	require.NoError(t, WriteOffset(filepath.Join(offsetDir, "sub1.offset"), 0))
	c.RegisterSubscriber("sub1")
	require.Greater(t, size, maxLag)

	require.NoError(t, c.MaybeCompact(channelDir))
	assert.True(t, log.HasWarn("subscriber is lagging"), "lag warning must be logged")
}

// TestCompactor_NoPauseNeeded verifies that concurrent writes to the active
// segment and compaction of sealed segments do not interfere — the writer keeps
// appending to the active segment while old ones are deleted.
func TestCompactor_NoPauseNeeded(t *testing.T) {
	base := t.TempDir()
	channelDir := filepath.Join(base, "orders")
	offsetDir := filepath.Join(base, "offsets")
	require.NoError(t, os.MkdirAll(offsetDir, 0o755))

	// Create two sealed segments and one active via a real writer with rolling.
	const maxSeg = 300
	writer, err := NewChannelWriter(channelDir, maxSeg, SyncPolicyNone, 0, nil, nil)
	require.NoError(t, err)

	for i := 0; i < 15; i++ {
		env, err := envelope.NewEnvelope("orders", "host", "com.test.T", map[string]int{"i": i})
		require.NoError(t, err)
		require.NoError(t, writer.Write(&env))
	}

	segs, err := listSegments(channelDir)
	require.NoError(t, err)
	require.Greater(t, len(segs), 1, "test requires multiple segments")

	// Mark subscriber as having consumed all sealed segments.
	active := segs[len(segs)-1]
	require.NoError(t, WriteOffset(filepath.Join(offsetDir, "sub1.offset"), active.startOffset))
	c := NewCompactor(offsetDir, 0, nil)
	c.RegisterSubscriber("sub1")

	// Compact while the writer is still running.
	require.NoError(t, c.MaybeCompact(channelDir))

	// Writer must still work after compaction.
	env, err := envelope.NewEnvelope("orders", "host", "com.test.T", map[string]int{"i": 99})
	require.NoError(t, err)
	require.NoError(t, writer.Write(&env))
	require.NoError(t, writer.Close())

	// Active segment must still exist and contain data.
	segsAfter, err := listSegments(channelDir)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(segsAfter), 1)
	assert.Greater(t, segsAfter[len(segsAfter)-1].size, int64(0))
}
