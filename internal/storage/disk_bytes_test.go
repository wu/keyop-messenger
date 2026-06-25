//nolint:gosec // test file: G301/G304/G306
package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChannelDiskBytes verifies the on-disk footprint is the sum of segment
// sizes and, unlike the monotonic stream end, shrinks when a consumed segment is
// removed by compaction (here simulated by deleting the oldest segment file).
func TestChannelDiskBytes(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "orders")

	seg0 := writeSegment(t, channelDir, 0, 5)
	seg1 := writeSegment(t, channelDir, seg0, 3)

	disk, err := ChannelDiskBytes(channelDir)
	require.NoError(t, err)
	assert.Equal(t, seg0+seg1, disk, "disk bytes should sum all segment sizes")

	streamEnd, err := ChannelStreamEnd(channelDir)
	require.NoError(t, err)
	assert.Equal(t, seg0+seg1, streamEnd)

	// Compaction removes the fully-consumed oldest segment.
	require.NoError(t, os.Remove(filepath.Join(channelDir, segmentName(0))))

	disk, err = ChannelDiskBytes(channelDir)
	require.NoError(t, err)
	assert.Equal(t, seg1, disk, "disk bytes should drop after the oldest segment is compacted away")

	// Stream end is monotonic: it still reflects the last segment's end and does
	// not shrink.
	streamEnd, err = ChannelStreamEnd(channelDir)
	require.NoError(t, err)
	assert.Equal(t, seg0+seg1, streamEnd, "stream end must not shrink after compaction")
}

// TestChannelDiskBytes_Empty verifies a channel with no segments reports 0.
func TestChannelDiskBytes_Empty(t *testing.T) {
	disk, err := ChannelDiskBytes(filepath.Join(t.TempDir(), "nope"))
	require.NoError(t, err)
	assert.Zero(t, disk)
}
