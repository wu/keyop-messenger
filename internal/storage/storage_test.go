package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
)

func TestChannelStreamEnd_NonexistentDirectory(t *testing.T) {
	end, err := ChannelStreamEnd(filepath.Join(t.TempDir(), "no-such-channel"))
	require.NoError(t, err)
	assert.Zero(t, end)
}

func TestChannelStreamEnd_EmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "ch")
	require.NoError(t, os.MkdirAll(channelDir, 0o750))

	end, err := ChannelStreamEnd(channelDir)
	require.NoError(t, err)
	assert.Zero(t, end)
}

func TestChannelStreamEnd_AfterWrite(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "ch")

	env, err := envelope.NewEnvelope("ch", "origin", "test.T", map[string]any{"v": 1})
	require.NoError(t, err)
	writeTestEnvelope(t, channelDir, env)

	end, err := ChannelStreamEnd(channelDir)
	require.NoError(t, err)
	assert.Positive(t, end)
}

func TestChannelStreamEnd_GrowsWithMoreWrites(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "ch")

	env1, _ := envelope.NewEnvelope("ch", "origin", "test.T", map[string]any{"v": 1})
	writeTestEnvelope(t, channelDir, env1)
	end1, err := ChannelStreamEnd(channelDir)
	require.NoError(t, err)

	env2, _ := envelope.NewEnvelope("ch", "origin", "test.T", map[string]any{"v": 2})
	writeTestEnvelope(t, channelDir, env2)
	end2, err := ChannelStreamEnd(channelDir)
	require.NoError(t, err)

	assert.Greater(t, end2, end1)
}

// TestLastRecordBoundary covers the single definition of "where does the
// complete data end": the offset one byte past the last '\n'.
func TestLastRecordBoundary(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		data string
		want int64
	}{
		{"empty file", "", 0},
		{"ends on a boundary", "a\nb\n", 4},
		{"partial tail", "a\nb\npart", 4},
		{"single complete record", "a\n", 2},
		{"no newline at all", "partial-only", 0},
		{"trailing newline only", "\n", 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := lastRecordBoundary(strings.NewReader(tc.data), int64(len(tc.data)))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestLastRecordBoundary_WalksBackMultipleChunks verifies the backward scan
// crosses its 4 KiB chunk boundary rather than giving up after one read.
func TestLastRecordBoundary_WalksBackMultipleChunks(t *testing.T) {
	t.Parallel()
	data := "a\n" + strings.Repeat("x", 10*1024) // one record, then a long partial tail
	got, err := lastRecordBoundary(strings.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	assert.Equal(t, int64(2), got)
}

// writeSegmentFile writes raw bytes as the segment starting at startOffset.
func writeSegmentFile(t *testing.T, channelDir string, startOffset int64, data string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(channelDir, 0o750))
	path := filepath.Join(channelDir, segmentName(startOffset))
	require.NoError(t, os.WriteFile(path, []byte(data), 0o600))
}

// TestChannelCommittedEnd verifies that positioning never lands inside a record,
// which ChannelStreamEnd (file-size arithmetic) does not guarantee.
func TestChannelCommittedEnd(t *testing.T) {
	t.Parallel()

	t.Run("no segments", func(t *testing.T) {
		t.Parallel()
		end, err := ChannelCommittedEnd(filepath.Join(t.TempDir(), "absent"))
		require.NoError(t, err)
		assert.Equal(t, int64(0), end)
	})

	t.Run("segment ends on a boundary", func(t *testing.T) {
		t.Parallel()
		dir := filepath.Join(t.TempDir(), "ch")
		writeSegmentFile(t, dir, 0, "aaaa\nbbbb\n")
		end, err := ChannelCommittedEnd(dir)
		require.NoError(t, err)
		assert.Equal(t, int64(10), end)
	})

	t.Run("partial tail is excluded", func(t *testing.T) {
		t.Parallel()
		dir := filepath.Join(t.TempDir(), "ch")
		writeSegmentFile(t, dir, 0, "aaaa\nbbbb\npart")
		end, err := ChannelCommittedEnd(dir)
		require.NoError(t, err)
		assert.Equal(t, int64(10), end, "must stop before the unterminated record")

		streamEnd, err := ChannelStreamEnd(dir)
		require.NoError(t, err)
		assert.Equal(t, int64(14), streamEnd, "ChannelStreamEnd counts bytes, including the partial")
	})

	t.Run("active segment holds only a partial record", func(t *testing.T) {
		t.Parallel()
		dir := filepath.Join(t.TempDir(), "ch")
		writeSegmentFile(t, dir, 0, "aaaa\n")
		writeSegmentFile(t, dir, 5, "part")
		end, err := ChannelCommittedEnd(dir)
		require.NoError(t, err)
		assert.Equal(t, int64(5), end, "boundary is the previous segment's end")
	})

	t.Run("freshly rolled empty segment", func(t *testing.T) {
		t.Parallel()
		dir := filepath.Join(t.TempDir(), "ch")
		writeSegmentFile(t, dir, 0, "aaaa\n")
		writeSegmentFile(t, dir, 5, "")
		end, err := ChannelCommittedEnd(dir)
		require.NoError(t, err)
		assert.Equal(t, int64(5), end)
	})
}

// TestChannelCommittedEnd_MatchesPostRecoveryEnd pins the property that lets a
// reader position correctly before a channel's writer (and therefore its crash
// recovery) has been created: the committed end equals the file size that
// recovery will leave behind.
func TestChannelCommittedEnd_MatchesPostRecoveryEnd(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "ch")
	writeSegmentFile(t, dir, 0, "aaaa\nbbbb\nhalf-writ")
	path := filepath.Join(dir, segmentName(0))

	before, err := ChannelCommittedEnd(dir)
	require.NoError(t, err)

	info, err := os.Stat(path)
	require.NoError(t, err)
	recovered, err := truncatePartialTrailing(path, info.Size(), &testutil.FakeLogger{})
	require.NoError(t, err)

	assert.Equal(t, recovered, before, "committed end must equal the post-recovery size")

	after, err := ChannelCommittedEnd(dir)
	require.NoError(t, err)
	assert.Equal(t, before, after, "recovery must not move the committed end")
}
