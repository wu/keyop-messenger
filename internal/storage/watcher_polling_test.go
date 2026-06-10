//nolint:gosec // test file: G301/G304/G306
package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- helpers ----------------------------------------------------------------

// makeChannelDir creates channelDir and registers cleanup.
func makeChannelDir(t *testing.T) (channelDir string) {
	t.Helper()
	channelDir = filepath.Join(t.TempDir(), "orders")
	require.NoError(t, os.MkdirAll(channelDir, 0o755))
	return channelDir
}

// pollingWatcher returns a channelWatcher backed by a fakeFsnotify whose Add
// always fails, forcing every Watch call to fall back to startPolling.
// fakeFsnotify and newFakeFsnotify are defined in watcher_test.go.
func pollingWatcher(t *testing.T) *channelWatcher {
	t.Helper()
	fake := newFakeFsnotify()
	fake.addErr = fmt.Errorf("watch not supported")
	w := newChannelWatcherFromBackend(fake, nil)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

// writeSegFile writes content to segFile, creating it if necessary.
func writeSegFile(t *testing.T, segFile string, content []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(segFile, content, 0o644))
}

// ---- tests ------------------------------------------------------------------

// TestWatcher_Poll_NewSegmentAdded verifies that the polling goroutine detects
// a newly created segment file in a previously empty channel directory.
// This exercises the changed := len(segs) != lastSegCount TRUE branch
// (count 0 → 1) that the existing PollingFallback test does not reach
// (it modifies an existing file, so count stays equal and changed is set
// only via mtime/size).
func TestWatcher_Poll_NewSegmentAdded(t *testing.T) {
	channelDir := makeChannelDir(t)
	// No segment file yet — poll initialises with lastSegCount = 0.

	w := pollingWatcher(t)
	ch, err := w.Watch(channelDir)
	require.NoError(t, err)

	// Allow the polling goroutine time to snapshot the empty directory.
	time.Sleep(20 * time.Millisecond)

	// Create the first segment file: count changes from 0 to 1.
	writeSegFile(t, filepath.Join(channelDir, segmentName(0)), []byte("data\n"))

	assert.True(t, receiveWithin(t, ch, 500*time.Millisecond),
		"polling must fire a notification when a segment file is created")
}

// TestWatcher_Poll_SegmentRemoved verifies that the polling goroutine fires a
// notification when the only segment file is deleted, exercising the path
// where changed is true (count 1 → 0) but len(segs) == 0 so the stat-update
// block inside if changed { if len(segs) > 0 { ... } } is skipped.
func TestWatcher_Poll_SegmentRemoved(t *testing.T) {
	channelDir := makeChannelDir(t)
	segPath := filepath.Join(channelDir, segmentName(0))
	writeSegFile(t, segPath, []byte("hello\n"))

	w := pollingWatcher(t)
	ch, err := w.Watch(channelDir)
	require.NoError(t, err)

	// Allow the polling goroutine to snapshot the directory (lastSegCount = 1).
	time.Sleep(20 * time.Millisecond)

	// Remove the only segment file: count drops from 1 to 0.
	require.NoError(t, os.Remove(segPath))

	assert.True(t, receiveWithin(t, ch, 500*time.Millisecond),
		"polling must fire a notification when a segment file is removed")
}

// TestWatcher_StartPolling_AlreadyPolling verifies the idempotency guard in
// startPolling: a second call for a path that is already being polled returns
// immediately without starting a second goroutine.
func TestWatcher_StartPolling_AlreadyPolling(t *testing.T) {
	channelDir := makeChannelDir(t)
	segPath := filepath.Join(channelDir, segmentName(0))
	writeSegFile(t, segPath, nil)

	w := pollingWatcher(t)

	// Watch calls startPolling(abs) internally because Add fails.
	ch, err := w.Watch(channelDir)
	require.NoError(t, err)

	abs, err := filepath.Abs(channelDir)
	require.NoError(t, err)

	// Second call: polled[abs] is already true, so the guard fires and returns.
	w.startPolling(abs)

	// The single polling goroutine must still work correctly.
	time.Sleep(20 * time.Millisecond)
	writeSegFile(t, segPath, []byte("new data\n"))
	assert.True(t, receiveWithin(t, ch, 500*time.Millisecond),
		"watcher must still deliver notifications after the guard was hit")
}

// TestWatcher_Poll_RecoversAfterDirectoryError verifies that the polling
// goroutine survives a transient directory-read error — the `continue` path
// inside if err != nil in poll — and resumes normal delivery once the
// directory becomes readable again.
func TestWatcher_Poll_RecoversAfterDirectoryError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("permission-based test is not meaningful when running as root")
	}

	channelDir := makeChannelDir(t)
	segPath := filepath.Join(channelDir, segmentName(0))
	writeSegFile(t, segPath, []byte("initial\n"))

	w := pollingWatcher(t)
	ch, err := w.Watch(channelDir)
	require.NoError(t, err)

	// Allow the polling goroutine to take its initial snapshot.
	time.Sleep(20 * time.Millisecond)

	// Remove read permission so os.ReadDir returns EACCES — not os.IsNotExist —
	// causing listSegments to return a non-nil error and poll to hit continue.
	require.NoError(t, os.Chmod(channelDir, 0o200))
	t.Cleanup(func() { _ = os.Chmod(channelDir, 0o755) })

	// Wait for at least one tick (100 ms interval) so the continue path runs.
	time.Sleep(150 * time.Millisecond)

	// Restore permissions and write new data so the next tick detects the change.
	require.NoError(t, os.Chmod(channelDir, 0o755))
	writeSegFile(t, segPath, []byte("after-error\n"))

	assert.True(t, receiveWithin(t, ch, 500*time.Millisecond),
		"polling must resume normally after a transient directory-read error")
}
