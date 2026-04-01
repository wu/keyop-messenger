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
	path := filepath.Join(dir, "orders.jsonl")
	require.NoError(t, os.WriteFile(path, nil, 0o644))

	w, err := NewChannelWatcher(nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ch, err := w.Watch(path)
	require.NoError(t, err)

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = f.WriteString(`{"v":1}` + "\n")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	assert.True(t, receiveWithin(t, ch, 500*time.Millisecond),
		"expected notification within 500ms of file write")
}

func TestWatcher_Coalescing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "orders.jsonl")
	require.NoError(t, os.WriteFile(path, nil, 0o644))

	w, err := NewChannelWatcher(nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ch, err := w.Watch(path)
	require.NoError(t, err)

	// Write 100 times rapidly.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
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
	path := filepath.Join(dir, "orders.jsonl")
	require.NoError(t, os.WriteFile(path, nil, 0o644))

	// Inject a backend whose Add always fails — triggers polling fallback.
	fake := newFakeFsnotify()
	fake.addErr = fmt.Errorf("watch not supported")
	w := newChannelWatcherFromBackend(fake, nil)
	t.Cleanup(func() { _ = w.Close() })

	ch, err := w.Watch(path)
	require.NoError(t, err)

	// Give the polling goroutine time to take its initial stat (sets lastMod),
	// then modify the file so the next tick sees an mtime change.
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, os.WriteFile(path, []byte("hello\n"), 0o644))

	assert.True(t, receiveWithin(t, ch, 500*time.Millisecond),
		"polling fallback must emit notification on file change")
}

func TestWatcher_ErrorTriggersPollingFallback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "orders.jsonl")
	require.NoError(t, os.WriteFile(path, nil, 0o644))

	// Use a backend whose Add succeeds (so no polling starts at Watch time),
	// but then inject an error to simulate inotify exhaustion.
	fake := newFakeFsnotify()
	w := newChannelWatcherFromBackend(fake, nil)
	t.Cleanup(func() { _ = w.Close() })

	abs, err := filepath.Abs(path)
	require.NoError(t, err)

	ch, err := w.Watch(abs)
	require.NoError(t, err)

	// Inject a watcher error — run() should start polling for all watched paths.
	fake.errors <- fmt.Errorf("inotify: too many open files")

	// Give the error handler time to spawn the polling goroutine.
	time.Sleep(20 * time.Millisecond)

	// Modify the file — the polling goroutine must detect it.
	require.NoError(t, os.WriteFile(path, []byte("hello\n"), 0o644))

	assert.True(t, receiveWithin(t, ch, 500*time.Millisecond),
		"polling fallback must emit notification after fsnotify error")
}

func TestWatcher_MultipleSubscribers(t *testing.T) {
	fake := newFakeFsnotify()
	w := newChannelWatcherFromBackend(fake, nil)
	t.Cleanup(func() { _ = w.Close() })

	path := "/tmp/orders.jsonl"
	ch1, err := w.Watch(path)
	require.NoError(t, err)
	ch2, err := w.Watch(path)
	require.NoError(t, err)

	// Inject an event via the fake backend.
	fake.events <- fsnotify.Event{Name: path, Op: fsnotify.Write}

	assert.True(t, receiveWithin(t, ch1, 200*time.Millisecond), "subscriber 1 must be notified")
	assert.True(t, receiveWithin(t, ch2, 200*time.Millisecond), "subscriber 2 must be notified")
}

func TestWatcher_PathNormalization(t *testing.T) {
	// Create the temp dir relative to cwd so filepath.Rel can produce a
	// relative path (t.TempDir lands under /var/... on macOS, which is
	// unreachable from ".").
	dir, err := os.MkdirTemp(".", "testwatcher-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })

	path := filepath.Join(dir, "orders.jsonl")
	require.NoError(t, os.WriteFile(path, nil, 0o644))

	abs, err := filepath.Abs(path)
	require.NoError(t, err)

	fake := newFakeFsnotify()
	w := newChannelWatcherFromBackend(fake, nil)
	t.Cleanup(func() { _ = w.Close() })

	// Watch once via the absolute path, once via the relative path — must
	// share one subs entry (both notified by a single event).
	ch1, err := w.Watch(abs)
	require.NoError(t, err)
	ch2, err := w.Watch(path) // relative-ish (via filepath.Abs internally)
	require.NoError(t, err)

	fake.events <- fsnotify.Event{Name: abs, Op: fsnotify.Write}

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
