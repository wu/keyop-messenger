package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/keyop/keyop-messenger/internal/envelope"
	"github.com/keyop/keyop-messenger/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakePauseable implements pauseableWriter by calling fn() directly without
// any actual writer goroutine. Used in tests that don't exercise the writer.
type fakePauseable struct{ err error }

func (f *fakePauseable) PauseAndSwap(fn func() error) error {
	if f.err != nil {
		return f.err
	}
	return fn()
}

// writeLines appends raw JSONL lines to path (one per call).
func writeLines(t *testing.T, path string, lines []string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer f.Close()
	for _, l := range lines {
		_, err := fmt.Fprintln(f, l)
		require.NoError(t, err)
	}
}

// makeLines returns n identical JSONL lines of roughly lineBytes bytes each.
func makeLines(n, lineBytes int) []string {
	payload := strings.Repeat("x", lineBytes-20) // rough padding
	line := fmt.Sprintf(`{"v":1,"data":%q}`, payload)
	lines := make([]string, n)
	for i := range lines {
		lines[i] = line
	}
	return lines
}

func newCompactor(t *testing.T) (*Compactor, string, string) {
	t.Helper()
	dir := t.TempDir()
	channelPath := filepath.Join(dir, "orders.jsonl")
	offsetDir := filepath.Join(dir, "offsets")
	require.NoError(t, os.MkdirAll(offsetDir, 0o755))
	log := &testutil.FakeLogger{}
	c := NewCompactor(offsetDir, 0, log)
	return c, channelPath, offsetDir
}

// TestCompactor_NoCompactionBelowThreshold ensures MaybeCompact is a no-op
// when the minimum subscriber offset is at or below the threshold.
func TestCompactor_NoCompactionBelowThreshold(t *testing.T) {
	c, channelPath, offsetDir := newCompactor(t)

	writeLines(t, channelPath, makeLines(10, 50))
	info, err := os.Stat(channelPath)
	require.NoError(t, err)
	fileSize := info.Size()

	// Subscriber has consumed half the file.
	half := fileSize / 2
	require.NoError(t, WriteOffset(filepath.Join(offsetDir, "sub1.offset"), half))
	c.RegisterSubscriber("sub1")

	// Threshold is larger than minOffset — no compaction.
	require.NoError(t, c.MaybeCompact(channelPath, fileSize, &fakePauseable{}))

	info2, err := os.Stat(channelPath)
	require.NoError(t, err)
	assert.Equal(t, fileSize, info2.Size(), "file must be unchanged")
}

// TestCompactor_Compacts verifies that when minOffset exceeds the threshold
// the file is compacted and subscriber offsets are adjusted.
func TestCompactor_Compacts(t *testing.T) {
	c, channelPath, offsetDir := newCompactor(t)

	lines := makeLines(10, 50)
	writeLines(t, channelPath, lines)

	info, err := os.Stat(channelPath)
	require.NoError(t, err)
	fileSize := info.Size()

	// Both subscribers have consumed the full file.
	require.NoError(t, WriteOffset(filepath.Join(offsetDir, "sub1.offset"), fileSize))
	require.NoError(t, WriteOffset(filepath.Join(offsetDir, "sub2.offset"), fileSize))
	c.RegisterSubscriber("sub1")
	c.RegisterSubscriber("sub2")

	// Threshold of 0 → always compact if minOffset > 0.
	require.NoError(t, c.MaybeCompact(channelPath, 0, &fakePauseable{}))

	info2, err := os.Stat(channelPath)
	require.NoError(t, err)
	assert.Equal(t, int64(0), info2.Size(), "compacted file must be empty")

	// Both subscriber offsets must have been reset to 0.
	off1, err := ReadOffset(filepath.Join(offsetDir, "sub1.offset"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), off1)

	off2, err := ReadOffset(filepath.Join(offsetDir, "sub2.offset"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), off2)
}

// TestCompactor_PartialCompaction verifies that when the minimum offset falls
// in the middle of the file, only the consumed prefix is removed and the
// remainder is preserved byte-for-byte.
func TestCompactor_PartialCompaction(t *testing.T) {
	c, channelPath, offsetDir := newCompactor(t)

	// Write 5 lines; record the offset after line 3 (the compaction boundary).
	first := makeLines(3, 50)
	writeLines(t, channelPath, first)

	boundary, err := os.Stat(channelPath)
	require.NoError(t, err)
	boundaryOffset := boundary.Size()

	rest := makeLines(2, 50)
	writeLines(t, channelPath, rest)

	info, err := os.Stat(channelPath)
	require.NoError(t, err)
	fullSize := info.Size()
	remaining := fullSize - boundaryOffset

	// Subscriber consumed up to the boundary.
	require.NoError(t, WriteOffset(filepath.Join(offsetDir, "sub1.offset"), boundaryOffset))
	c.RegisterSubscriber("sub1")

	require.NoError(t, c.MaybeCompact(channelPath, 0, &fakePauseable{}))

	info2, err := os.Stat(channelPath)
	require.NoError(t, err)
	assert.Equal(t, remaining, info2.Size(), "compacted file must contain only the unconsumed tail")

	// Subscriber offset must be adjusted to 0 (start of compacted file).
	off, err := ReadOffset(filepath.Join(offsetDir, "sub1.offset"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), off)
}

// TestCompactor_MultipleSubscribers verifies that MinOffset correctly returns
// the minimum across all registered subscribers and that compaction respects it.
func TestCompactor_MultipleSubscribers(t *testing.T) {
	c, channelPath, offsetDir := newCompactor(t)

	lines := makeLines(10, 50)
	writeLines(t, channelPath, lines)

	info, err := os.Stat(channelPath)
	require.NoError(t, err)
	fileSize := info.Size()

	// sub1 is at the end, sub2 is only halfway.
	require.NoError(t, WriteOffset(filepath.Join(offsetDir, "sub1.offset"), fileSize))
	require.NoError(t, WriteOffset(filepath.Join(offsetDir, "sub2.offset"), fileSize/2))
	c.RegisterSubscriber("sub1")
	c.RegisterSubscriber("sub2")

	minOff, err := c.MinOffset()
	require.NoError(t, err)
	assert.Equal(t, fileSize/2, minOff, "min offset must be sub2's offset")

	require.NoError(t, c.MaybeCompact(channelPath, 0, &fakePauseable{}))

	info2, err := os.Stat(channelPath)
	require.NoError(t, err)
	// File should be ~half the original size.
	assert.Equal(t, fileSize-fileSize/2, info2.Size())

	// sub1's offset was fileSize; after removing fileSize/2 it becomes fileSize/2.
	off1, err := ReadOffset(filepath.Join(offsetDir, "sub1.offset"))
	require.NoError(t, err)
	assert.Equal(t, fileSize-fileSize/2, off1)

	// sub2's offset was fileSize/2; it becomes 0.
	off2, err := ReadOffset(filepath.Join(offsetDir, "sub2.offset"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), off2)
}

// TestCompactor_DeregisterRemovesOffsetFile verifies that deregistering a
// subscriber removes its offset file and excludes it from future min offsets.
func TestCompactor_DeregisterRemovesOffsetFile(t *testing.T) {
	c, _, offsetDir := newCompactor(t)

	offsetPath := filepath.Join(offsetDir, "sub1.offset")
	require.NoError(t, WriteOffset(offsetPath, 42))
	c.RegisterSubscriber("sub1")

	require.NoError(t, c.DeregisterSubscriber("sub1"))
	assert.False(t, OffsetFileExists(offsetPath), "offset file must be removed")

	// MinOffset with no subscribers returns 0.
	min, err := c.MinOffset()
	require.NoError(t, err)
	assert.Equal(t, int64(0), min)
}

// TestCompactor_LagWarning verifies that a subscriber lagging beyond the
// configured threshold causes a warning to be logged.
func TestCompactor_LagWarning(t *testing.T) {
	dir := t.TempDir()
	channelPath := filepath.Join(dir, "orders.jsonl")
	offsetDir := filepath.Join(dir, "offsets")
	require.NoError(t, os.MkdirAll(offsetDir, 0o755))
	log := &testutil.FakeLogger{}

	const maxLag = int64(100)
	c := NewCompactor(offsetDir, maxLag, log)

	lines := makeLines(10, 50)
	writeLines(t, channelPath, lines)
	info, err := os.Stat(channelPath)
	require.NoError(t, err)

	// Subscriber at offset 0 in a file larger than maxLag.
	require.NoError(t, WriteOffset(filepath.Join(offsetDir, "sub1.offset"), 0))
	c.RegisterSubscriber("sub1")
	require.Greater(t, info.Size(), maxLag, "test requires file larger than maxLag")

	// threshold == fileSize so no compaction actually happens; we just want the warning.
	require.NoError(t, c.MaybeCompact(channelPath, info.Size(), &fakePauseable{}))

	assert.True(t, log.HasWarn("subscriber is lagging"), "lag warning must be logged")
}

// TestCompactor_WriterPauseAndResume verifies that writes issued concurrently
// with MaybeCompact succeed after the compaction completes, and that the
// writer correctly appends to the new (compacted) file.
func TestCompactor_WriterPauseAndResume(t *testing.T) {
	dir := t.TempDir()
	channelPath := filepath.Join(dir, "orders.jsonl")
	offsetDir := filepath.Join(dir, "offsets")
	require.NoError(t, os.MkdirAll(offsetDir, 0o755))

	// Use a real writer so PauseAndSwap exercises the goroutine mechanism.
	writer, err := NewChannelWriter(channelPath, SyncPolicyNone, 0, nil, nil)
	require.NoError(t, err)
	defer writer.Close()

	// Write 3 envelopes to establish content.
	for i := 0; i < 3; i++ {
		env, err := envelope.NewEnvelope("orders", "test", "test.T", map[string]int{"n": i})
		require.NoError(t, err)
		require.NoError(t, writer.Write(&env))
	}

	info, err := os.Stat(channelPath)
	require.NoError(t, err)
	fileSize := info.Size()

	// Mark the subscriber as having consumed everything.
	require.NoError(t, WriteOffset(filepath.Join(offsetDir, "sub1.offset"), fileSize))
	c := NewCompactor(offsetDir, 0, nil)
	c.RegisterSubscriber("sub1")

	// Compact (threshold=0 → triggers since minOffset == fileSize > 0).
	require.NoError(t, c.MaybeCompact(channelPath, 0, writer))

	info2, err := os.Stat(channelPath)
	require.NoError(t, err)
	assert.Equal(t, int64(0), info2.Size(), "file must be empty after full compaction")

	// The writer must still work after the compaction (fd was reopened).
	env, err := envelope.NewEnvelope("orders", "test", "test.T", map[string]int{"n": 99})
	require.NoError(t, err)
	require.NoError(t, writer.Write(&env), "write after compaction must succeed")

	info3, err := os.Stat(channelPath)
	require.NoError(t, err)
	assert.Greater(t, info3.Size(), int64(0), "post-compaction write must appear in the file")
}

// TestSubscriber_ReloadsOffsetAfterCompaction verifies that a subscriber
// whose in-memory offset exceeds the compacted file size reloads from disk and
// continues reading from the correct position.
func TestSubscriber_ReloadsOffsetAfterCompaction(t *testing.T) {
	dir := t.TempDir()
	channelPath := filepath.Join(dir, "orders.jsonl")
	offsetDir := filepath.Join(dir, "offsets")

	notifier := NewLocalNotifier()
	dlWriter := &testutil.FakeChannelWriter{}

	// Write 3 envelopes; the subscriber will start from current EOF.
	for i := 0; i < 3; i++ {
		writeTestEnvelope(t, channelPath, makeEnv(t, "orders", map[string]int{"n": i}))
	}

	info, err := os.Stat(channelPath)
	require.NoError(t, err)
	fullSize := info.Size()

	// Create subscriber — starts at EOF (skips existing messages).
	sub, err := NewSubscriber("s", channelPath, offsetDir, mapDecoder{}, 0, dlWriter, nil)
	require.NoError(t, err)
	sub.retryDelay = func(int) time.Duration { return 0 }

	// Simulate compaction: truncate the file to empty and update the offset
	// file to 0 (what the compactor would write). The subscriber's in-memory
	// offset is still fullSize.
	require.NoError(t, os.WriteFile(channelPath, nil, 0o644))
	require.NoError(t, WriteOffset(filepath.Join(offsetDir, "s.offset"), 0))

	// Write one new envelope to the now-empty file.
	writeTestEnvelope(t, channelPath, makeEnv(t, "orders", map[string]int{"n": 99}))

	received := make(chan *envelope.Envelope, 5)
	sub.Start(notifier.C(), func(env *envelope.Envelope, _ any) error {
		received <- env
		return nil
	})
	t.Cleanup(sub.Stop)

	notifier.Notify()

	msgs := collectN(t, received, 1, testTimeout)
	require.Len(t, msgs, 1, "subscriber must deliver the post-compaction message")
	_ = fullSize // used to set up the scenario
}
