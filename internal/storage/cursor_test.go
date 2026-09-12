//nolint:gosec // test file: G301/G304/G306
package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeCursorSegment writes raw bytes as the segment starting at startOffset.
func writeCursorSegment(t *testing.T, channelDir string, startOffset int64, data string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(channelDir, 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(channelDir, segmentName(startOffset)), []byte(data), 0o600))
}

// committedAll bounds a cursor at the channel's current stream end — correct for
// a fixture written in one go, where every byte on disk is a complete record.
// Tests that care about the bound itself supply their own.
func committedAll(channelDir string) func() int64 {
	return func() int64 {
		end, err := ChannelStreamEnd(channelDir)
		if err != nil {
			return 0
		}
		return end
	}
}

// drainCursor collects every record the cursor yields in one pass.
func drainCursor(t *testing.T, c *Cursor, from int64) []Record {
	t.Helper()
	require.NoError(t, c.Reset(from))
	var out []Record
	for {
		rec, ok, err := c.Next()
		require.NoError(t, err)
		if !ok {
			return out
		}
		// Bytes are only valid until the next call, so copy before continuing.
		out = append(out, Record{
			Bytes:      append([]byte(nil), rec.Bytes...),
			Offset:     rec.Offset,
			NextOffset: rec.NextOffset,
			Segment:    rec.Segment,
		})
	}
}

func recordBodies(recs []Record) []string {
	out := make([]string, 0, len(recs))
	for _, r := range recs {
		out = append(out, string(r.Bytes))
	}
	return out
}

func TestCursor_ReadsCompleteRecords(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "ch")
	writeCursorSegment(t, dir, 0, "aaa\nbb\ncccc\n")

	c := NewCursor(dir, committedAll(dir), CursorOpts{})
	t.Cleanup(func() { _ = c.Close() })

	recs := drainCursor(t, c, 0)
	assert.Equal(t, []string{"aaa", "bb", "cccc"}, recordBodies(recs))
	assert.Equal(t, int64(0), recs[0].Offset)
	assert.Equal(t, int64(4), recs[0].NextOffset)
	assert.Equal(t, int64(4), recs[1].Offset)
	assert.Equal(t, int64(12), c.Offset())
}

// TestCursor_PartialTailIsNotARecord is the invariant the 2026-09-11 incident
// came down to: the bytes past the last newline belong to a record the writer is
// still appending, and consuming them loses that record and mis-frames the rest.
func TestCursor_PartialTailIsNotARecord(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "ch")
	writeCursorSegment(t, dir, 0, "aaa\npart")

	c := NewCursor(dir, committedAll(dir), CursorOpts{})
	t.Cleanup(func() { _ = c.Close() })

	recs := drainCursor(t, c, 0)
	assert.Equal(t, []string{"aaa"}, recordBodies(recs))
	assert.Equal(t, int64(4), c.Offset(), "position holds at the record boundary")

	// The writer finishes the record; it is delivered whole on the next pass.
	writeCursorSegment(t, dir, 0, "aaa\npartial\n")
	recs = drainCursor(t, c, 4)
	assert.Equal(t, []string{"partial"}, recordBodies(recs))
}

func TestCursor_ResumesFromOffset(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "ch")
	writeCursorSegment(t, dir, 0, "aaa\nbbb\nccc\n")

	c := NewCursor(dir, committedAll(dir), CursorOpts{})
	t.Cleanup(func() { _ = c.Close() })

	assert.Equal(t, []string{"bbb", "ccc"}, recordBodies(drainCursor(t, c, 4)))
}

// TestCursor_SkipsGapBetweenSegments covers a sealed segment whose end does not
// meet the next segment's start, which compaction and rolling can produce.
func TestCursor_SkipsGapBetweenSegments(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "ch")
	writeCursorSegment(t, dir, 0, "aaa\n")
	writeCursorSegment(t, dir, 100, "bbb\n") // gap: 4..100

	c := NewCursor(dir, committedAll(dir), CursorOpts{})
	t.Cleanup(func() { _ = c.Close() })

	recs := drainCursor(t, c, 0)
	assert.Equal(t, []string{"aaa", "bbb"}, recordBodies(recs))
	assert.Equal(t, int64(100), recs[1].Offset, "the gap is stepped over")
	assert.Equal(t, int64(104), c.Offset())
}

func TestCursor_StartsInLaterSegment(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "ch")
	writeCursorSegment(t, dir, 0, "aaa\n")
	writeCursorSegment(t, dir, 4, "bbb\nccc\n")

	c := NewCursor(dir, committedAll(dir), CursorOpts{})
	t.Cleanup(func() { _ = c.Close() })

	assert.Equal(t, []string{"ccc"}, recordBodies(drainCursor(t, c, 8)))
}

// TestCursor_SkipsOversizedRecord: a record the consumer cannot carry is stepped
// over so one poison message cannot wedge the channel.
func TestCursor_SkipsOversizedRecord(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "ch")
	big := strings.Repeat("x", 64)
	writeCursorSegment(t, dir, 0, "aaa\n"+big+"\nbbb\n")

	var skips []SkipInfo
	c := NewCursor(dir, committedAll(dir), CursorOpts{
		MaxRecordBytes: 16,
		OnSkip:         func(s SkipInfo) { skips = append(skips, s) },
	})
	t.Cleanup(func() { _ = c.Close() })

	assert.Equal(t, []string{"aaa", "bbb"}, recordBodies(drainCursor(t, c, 0)))
	require.Len(t, skips, 1)
	assert.Equal(t, SkipOversized, skips[0].Reason)
	assert.Equal(t, 64, skips[0].Len)
	assert.Equal(t, int64(4), skips[0].Offset)
	assert.Equal(t, int64(69), skips[0].NextOffset)
}

// TestCursor_SkipsUnscannableRecord: a record larger than the scan buffer cannot
// be held at all, so its end is located by scanning for the next newline.
func TestCursor_SkipsUnscannableRecord(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "ch")
	huge := strings.Repeat("x", 4096)
	writeCursorSegment(t, dir, 0, "aaa\n"+huge+"\nbbb\n")

	var skips []SkipInfo
	c := NewCursor(dir, committedAll(dir), CursorOpts{
		ScanLimit:      64,
		InitialBufSize: 16,
		OnSkip:         func(s SkipInfo) { skips = append(skips, s) },
	})
	t.Cleanup(func() { _ = c.Close() })

	assert.Equal(t, []string{"aaa", "bbb"}, recordBodies(drainCursor(t, c, 0)),
		"records on both sides of the unscannable one are still delivered")
	require.Len(t, skips, 1)
	assert.Equal(t, SkipUnscannable, skips[0].Reason)
	assert.Equal(t, int64(4), skips[0].Offset)
	assert.Equal(t, int64(4+4096+1), skips[0].NextOffset)
}

// TestCursor_UnscannableWithoutNewlineHolds distinguishes a record too large to
// read from one still being written: an unterminated giant must not be skipped,
// or the message is lost the moment the writer finishes it.
func TestCursor_UnscannableWithoutNewlineHolds(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "ch")
	writeCursorSegment(t, dir, 0, "aaa\n"+strings.Repeat("x", 4096))

	var skips []SkipInfo
	c := NewCursor(dir, committedAll(dir), CursorOpts{
		ScanLimit:      64,
		InitialBufSize: 16,
		OnSkip:         func(s SkipInfo) { skips = append(skips, s) },
	})
	t.Cleanup(func() { _ = c.Close() })

	assert.Equal(t, []string{"aaa"}, recordBodies(drainCursor(t, c, 0)))
	assert.Empty(t, skips, "an unterminated record is in flight, not oversized")
	assert.Equal(t, int64(4), c.Offset(), "position holds before it")
}

// TestCursor_BoundedByCommittedEnd: records already on disk but not yet
// committed are deferred, not skipped.
func TestCursor_BoundedByCommittedEnd(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "ch")
	writeCursorSegment(t, dir, 0, "aaa\nbbb\n")

	var end atomic.Int64
	end.Store(4)
	c := NewCursor(dir, end.Load, CursorOpts{})
	t.Cleanup(func() { _ = c.Close() })

	assert.Equal(t, []string{"aaa"}, recordBodies(drainCursor(t, c, 0)))
	assert.Equal(t, int64(4), c.Offset())

	end.Store(8)
	assert.Equal(t, []string{"bbb"}, recordBodies(drainCursor(t, c, 4)))
}

// TestCursor_CommittedEndBoundsAcrossSegments verifies the bound is not lost
// when the cursor moves from one segment to the next.
func TestCursor_CommittedEndBoundsAcrossSegments(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "ch")
	writeCursorSegment(t, dir, 0, "aaa\n")
	writeCursorSegment(t, dir, 4, "bbb\nccc\n")

	var end atomic.Int64
	end.Store(8) // through "bbb" only
	c := NewCursor(dir, end.Load, CursorOpts{})
	t.Cleanup(func() { _ = c.Close() })

	assert.Equal(t, []string{"aaa", "bbb"}, recordBodies(drainCursor(t, c, 0)))
	assert.Equal(t, int64(8), c.Offset(), "must not read into the next segment past the bound")
}

// TestCursor_NothingCommittedYieldsNothing pins the contract that replaced the
// old "unknown committed end → read to EOF" fallback. A channel with nothing
// committed is not a special case needing a fallback: there is nothing its
// readers may see, and saying so is the whole point of the bound.
func TestCursor_NothingCommittedYieldsNothing(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "ch")
	writeCursorSegment(t, dir, 0, "aaa\nbbb\n")

	c := NewCursor(dir, func() int64 { return 0 }, CursorOpts{})
	t.Cleanup(func() { _ = c.Close() })

	assert.Empty(t, drainCursor(t, c, 0),
		"records on disk are not readable until the writer commits them")
	assert.Equal(t, int64(0), c.Offset(), "and the position does not move")
}

func TestCursor_EmptyAndMissingChannel(t *testing.T) {
	t.Parallel()
	missingDir := filepath.Join(t.TempDir(), "absent")
	missing := NewCursor(missingDir, committedAll(missingDir), CursorOpts{})
	t.Cleanup(func() { _ = missing.Close() })
	assert.Empty(t, drainCursor(t, missing, 0))

	dir := filepath.Join(t.TempDir(), "ch")
	writeCursorSegment(t, dir, 0, "")
	empty := NewCursor(dir, committedAll(dir), CursorOpts{})
	t.Cleanup(func() { _ = empty.Close() })
	assert.Empty(t, drainCursor(t, empty, 0))
}

// TestCursor_ReusesScanBuffer pins the reason a cursor is owned by its reader:
// the scan buffer is allocated once, not per pass.
func TestCursor_ReusesScanBuffer(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ch")
	writeCursorSegment(t, dir, 0, "aaa\nbbb\n")

	c := NewCursor(dir, committedAll(dir), CursorOpts{})
	t.Cleanup(func() { _ = c.Close() })
	drainCursor(t, c, 0) // first pass allocates nothing extra to measure

	allocs := testing.AllocsPerRun(20, func() {
		require.NoError(t, c.Reset(0))
		for {
			if _, ok, err := c.Next(); err != nil || !ok {
				require.NoError(t, err)
				return
			}
		}
	})
	assert.Less(t, allocs, float64(defaultScanInitialBufSize/2),
		"a pass must not re-allocate the scan buffer")
}

// TestCursor_NextDoesNotAllocatePerRecord pins the zero-copy contract that
// keeps records off the heap: Bytes points into the cursor's scan buffer, so
// iterating costs no allocation per record. A consumer that retains the bytes
// must copy them — see Record.Bytes.
func TestCursor_NextDoesNotAllocatePerRecord(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ch")
	var sb strings.Builder
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&sb, "record-%03d\n", i)
	}
	writeCursorSegment(t, dir, 0, sb.String())

	c := NewCursor(dir, committedAll(dir), CursorOpts{})
	t.Cleanup(func() { _ = c.Close() })
	require.NoError(t, c.Reset(0))
	_, _, err := c.Next() // open the segment outside the measured loop
	require.NoError(t, err)

	allocs := testing.AllocsPerRun(50, func() {
		if _, ok, err := c.Next(); err != nil || !ok {
			require.NoError(t, err)
			require.NoError(t, c.Reset(0))
			_, _, _ = c.Next()
		}
	})
	assert.Less(t, allocs, 1.0, "reading a record must not allocate")
}

func TestCursor_ResetRelistsSegments(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "ch")
	writeCursorSegment(t, dir, 0, "aaa\n")

	c := NewCursor(dir, committedAll(dir), CursorOpts{})
	t.Cleanup(func() { _ = c.Close() })
	assert.Equal(t, []string{"aaa"}, recordBodies(drainCursor(t, c, 0)))

	// A segment that did not exist during the first pass.
	writeCursorSegment(t, dir, 4, "bbb\n")
	assert.Equal(t, []string{"bbb"}, recordBodies(drainCursor(t, c, 4)))
}

func TestCursor_ReportsScanError(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "ch")
	writeCursorSegment(t, dir, 0, "aaa\n")
	c := NewCursor(dir, committedAll(dir), CursorOpts{})
	t.Cleanup(func() { _ = c.Close() })
	require.NoError(t, c.Reset(0))

	// Remove read permission so opening the segment fails mid-pass.
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	require.NoError(t, os.Chmod(filepath.Join(dir, segmentName(0)), 0o000))
	t.Cleanup(func() { _ = os.Chmod(filepath.Join(dir, segmentName(0)), 0o600) })

	_, ok, err := c.Next()
	require.Error(t, err)
	assert.False(t, ok)
	assert.Contains(t, err.Error(), "open segment")
}

func TestCursor_OptionDefaults(t *testing.T) {
	t.Parallel()
	c := NewCursor("dir", nil, CursorOpts{})
	assert.Equal(t, defaultScanLimit, c.opts.ScanLimit)
	assert.Equal(t, defaultScanInitialBufSize, c.opts.InitialBufSize)

	// An initial buffer larger than the ceiling would make Scanner's effective
	// maximum the buffer size, silently raising the limit.
	c = NewCursor("dir", nil, CursorOpts{ScanLimit: 100, InitialBufSize: 1000})
	assert.Equal(t, 100, c.opts.InitialBufSize)
	assert.Len(t, c.buf, 100)
}

// TestOffsetPastNextNewline covers the helper the cursor uses to step over a
// record too large to scan: it must find the byte just past the next newline,
// and report not-found when the record has no terminating newline yet, which
// means it is still being written rather than oversized.
func TestOffsetPastNextNewline(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	complete := filepath.Join(dir, "complete.jsonl")
	require.NoError(t, os.WriteFile(complete, []byte("aaaa\nbbbb\n"), 0o600))

	off, found, err := offsetPastNextNewline(complete, 0, 0)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, int64(5), off)

	// With a non-zero segment start the global offset is shifted accordingly.
	off, found, err = offsetPastNextNewline(complete, 0, 100)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, int64(105), off)

	// Starting mid-file finds the following record boundary.
	off, found, err = offsetPastNextNewline(complete, 5, 0)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, int64(10), off)

	partial := filepath.Join(dir, "partial.jsonl")
	require.NoError(t, os.WriteFile(partial, []byte("no newline yet"), 0o600))
	_, found, err = offsetPastNextNewline(partial, 0, 0)
	require.NoError(t, err)
	assert.False(t, found)
}
