//nolint:gosec // test file: G301/G304/G306
package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- fakeFsnotify -----------------------------------------------------------

type fakeFsnotify struct {
	events chan fsnotify.Event
	errors chan error
	addErr error // if non-nil, Add returns this error
}

func newFakeFsnotify() *fakeFsnotify {
	return &fakeFsnotify{
		events: make(chan fsnotify.Event, 16),
		errors: make(chan error, 4),
	}
}

func (f *fakeFsnotify) Add(_ string) error            { return f.addErr }
func (f *fakeFsnotify) Close() error                  { close(f.events); return nil }
func (f *fakeFsnotify) Events() <-chan fsnotify.Event { return f.events }
func (f *fakeFsnotify) Errors() <-chan error          { return f.errors }

// ---- helpers ----------------------------------------------------------------

func receiveWithin(t *testing.T, ch <-chan struct{}, d time.Duration) bool {
	t.Helper()
	select {
	case <-ch:
		return true
	case <-time.After(d):
		return false
	}
}

// ---- tests ------------------------------------------------------------------

func TestWatcher_Integration(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "orders")
	require.NoError(t, os.MkdirAll(channelDir, 0o755))
	segPath := filepath.Join(channelDir, "00000000000000000000.jsonl")
	require.NoError(t, os.WriteFile(segPath, nil, 0o644))

	w, err := NewChannelWatcher(nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ch, err := w.Watch(channelDir)
	require.NoError(t, err)

	f, err := os.OpenFile(segPath, os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = f.WriteString(`{"v":1}` + "\n")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	assert.True(t, receiveWithin(t, ch, 500*time.Millisecond),
		"expected notification within 500ms of file write")
}

func TestWatcher_Coalescing(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "orders")
	require.NoError(t, os.MkdirAll(channelDir, 0o755))
	segPath := filepath.Join(channelDir, "00000000000000000000.jsonl")
	require.NoError(t, os.WriteFile(segPath, nil, 0o644))

	w, err := NewChannelWatcher(nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ch, err := w.Watch(channelDir)
	require.NoError(t, err)

	// Write 100 times rapidly.
	f, err := os.OpenFile(segPath, os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		_, _ = f.WriteString("{}\n")
	}
	require.NoError(t, f.Close())

	// Let events settle, then drain.
	time.Sleep(200 * time.Millisecond)
	count := 0
	for {
		select {
		case <-ch:
			count++
		default:
			goto done
		}
	}
done:
	assert.Greater(t, count, 0, "at least one notification must arrive")
	assert.Less(t, count, 100, "coalescing must produce fewer notifications than writes")
}

func TestWatcher_PollingFallback(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "orders")
	require.NoError(t, os.MkdirAll(channelDir, 0o755))
	segPath := filepath.Join(channelDir, "00000000000000000000.jsonl")
	require.NoError(t, os.WriteFile(segPath, nil, 0o644))

	// Inject a backend whose Add always fails — triggers polling fallback.
	fake := newFakeFsnotify()
	fake.addErr = fmt.Errorf("watch not supported")
	w := newChannelWatcherFromBackend(fake, nil)
	t.Cleanup(func() { _ = w.Close() })

	ch, err := w.Watch(channelDir)
	require.NoError(t, err)

	// Give the polling goroutine time to take its initial stat (sets lastMod),
	// then modify the file so the next tick sees an mtime change.
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, os.WriteFile(segPath, []byte("hello\n"), 0o644))

	assert.True(t, receiveWithin(t, ch, 500*time.Millisecond),
		"polling fallback must emit notification on file change")
}

func TestWatcher_ErrorTriggersPollingFallback(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "orders")
	require.NoError(t, os.MkdirAll(channelDir, 0o755))
	segPath := filepath.Join(channelDir, "00000000000000000000.jsonl")
	require.NoError(t, os.WriteFile(segPath, nil, 0o644))

	// Use a backend whose Add succeeds (so no polling starts at Watch time),
	// but then inject an error to simulate inotify exhaustion.
	fake := newFakeFsnotify()
	w := newChannelWatcherFromBackend(fake, nil)
	t.Cleanup(func() { _ = w.Close() })

	absDir, err := filepath.Abs(channelDir)
	require.NoError(t, err)

	ch, err := w.Watch(absDir)
	require.NoError(t, err)

	// Inject a watcher error — run() should start polling for all watched paths.
	fake.errors <- fmt.Errorf("inotify: too many open files")

	// Give the error handler time to spawn the polling goroutine.
	time.Sleep(20 * time.Millisecond)

	// Modify the segment file — the polling goroutine must detect it.
	require.NoError(t, os.WriteFile(segPath, []byte("hello\n"), 0o644))

	assert.True(t, receiveWithin(t, ch, 500*time.Millisecond),
		"polling fallback must emit notification after fsnotify error")
}

func TestWatcher_MultipleSubscribers(t *testing.T) {
	fake := newFakeFsnotify()
	w := newChannelWatcherFromBackend(fake, nil)
	t.Cleanup(func() { _ = w.Close() })

	// Watch a channel directory; inject fsnotify events with the segment file path.
	channelDir := "/tmp/orders"
	segPath := filepath.Join(channelDir, "00000000000000000000.jsonl")
	ch1, err := w.Watch(channelDir)
	require.NoError(t, err)
	ch2, err := w.Watch(channelDir)
	require.NoError(t, err)

	// Inject an event via the fake backend with the segment file path.
	// The updated run() fans out to filepath.Dir(segPath) == channelDir.
	fake.events <- fsnotify.Event{Name: segPath, Op: fsnotify.Write}

	assert.True(t, receiveWithin(t, ch1, 200*time.Millisecond), "subscriber 1 must be notified")
	assert.True(t, receiveWithin(t, ch2, 200*time.Millisecond), "subscriber 2 must be notified")
}

func TestWatcher_PathNormalization(t *testing.T) {
	// Create the temp dir relative to cwd so filepath.Rel can produce a
	// relative path (t.TempDir lands under /var/... on macOS, which is
	// unreachable from ".").
	dir, err := os.MkdirTemp(".", "testwatcher-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	channelDir := filepath.Join(dir, "orders")
	require.NoError(t, os.MkdirAll(channelDir, 0o755))
	segPath := filepath.Join(channelDir, "00000000000000000000.jsonl")
	require.NoError(t, os.WriteFile(segPath, nil, 0o644))

	absDir, err := filepath.Abs(channelDir)
	require.NoError(t, err)
	absSegPath := filepath.Join(absDir, "00000000000000000000.jsonl")

	fake := newFakeFsnotify()
	w := newChannelWatcherFromBackend(fake, nil)
	t.Cleanup(func() { _ = w.Close() })

	// Watch once via the absolute path, once via the relative path — must
	// share one subs entry (both notified by a single event).
	ch1, err := w.Watch(absDir)
	require.NoError(t, err)
	ch2, err := w.Watch(channelDir) // relative-ish (via filepath.Abs internally)
	require.NoError(t, err)

	// Inject event with the segment file path; run() fans out to parent dir.
	fake.events <- fsnotify.Event{Name: absSegPath, Op: fsnotify.Write}

	assert.True(t, receiveWithin(t, ch1, 200*time.Millisecond), "absolute-path subscriber must be notified")
	assert.True(t, receiveWithin(t, ch2, 200*time.Millisecond), "relative-path subscriber must be notified")
}

func TestWatcher_Close_Idempotent(t *testing.T) {
	w, err := NewChannelWatcher(nil)
	require.NoError(t, err)
	assert.NoError(t, w.Close())
	assert.NoError(t, w.Close())
}

func TestLocalNotifier_SendAndReceive(t *testing.T) {
	n := NewLocalNotifier()

	n.Notify()
	select {
	case <-n.C():
	default:
		t.Fatal("expected notification on C()")
	}
}

func TestLocalNotifier_Coalescing(t *testing.T) {
	n := NewLocalNotifier()

	// Multiple Notify calls must not block and must collapse to one token.
	n.Notify()
	n.Notify()
	n.Notify()

	count := 0
	for {
		select {
		case <-n.C():
			count++
		default:
			goto done
		}
	}
done:
	assert.Equal(t, 1, count, "multiple Notify calls must produce exactly one token")
}
