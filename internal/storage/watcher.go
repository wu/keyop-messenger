package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// ChannelWatcher abstracts fsnotify vs polling so subscriber code is testable.
type ChannelWatcher interface {
	Watch(path string) (<-chan struct{}, error)
	Close() error
}

// LocalNotifier provides an in-process notification channel that bypasses the
// filesystem watcher, giving same-process subscribers lower latency. Use
// LocalNotifier.Notify as the notifyFn argument to NewChannelWriter; have the
// subscriber select on LocalNotifier.C().
type LocalNotifier struct {
	ch chan struct{}
}

// NewLocalNotifier creates a LocalNotifier with a capacity-1 channel.
func NewLocalNotifier() *LocalNotifier {
	return &LocalNotifier{ch: make(chan struct{}, 1)}
}

// Notify sends a non-blocking notification. Safe to call from any goroutine.
func (n *LocalNotifier) Notify() {
	select {
	case n.ch <- struct{}{}:
	default:
	}
}

// C returns the read-only channel that subscribers should select on.
func (n *LocalNotifier) C() <-chan struct{} { return n.ch }

// fsnotifyBackend abstracts *fsnotify.Watcher so tests can inject a fake.
type fsnotifyBackend interface {
	Add(path string) error
	Close() error
	Events() <-chan fsnotify.Event
	Errors() <-chan error
}

// realFsnotify wraps *fsnotify.Watcher (whose Events/Errors are fields, not
// methods) to satisfy fsnotifyBackend.
type realFsnotify struct{ w *fsnotify.Watcher }

func (r *realFsnotify) Add(path string) error         { return r.w.Add(path) }
func (r *realFsnotify) Close() error                  { return r.w.Close() }
func (r *realFsnotify) Events() <-chan fsnotify.Event { return r.w.Events }
func (r *realFsnotify) Errors() <-chan error          { return r.w.Errors }

type channelWatcher struct {
	mu        sync.Mutex
	fw        fsnotifyBackend
	subs      map[string][]chan struct{} // path → notification channels
	polled    map[string]bool            // paths with an active polling goroutine
	stopCh    chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
	log       logger
}

// NewChannelWatcher creates a ChannelWatcher backed by fsnotify. log may be nil.
//
//nolint:revive // unexported-return is acceptable for unexported implementation of exported interface
func NewChannelWatcher(log logger) (*channelWatcher, error) {
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("create fsnotify watcher: %w", err)
	}
	return newChannelWatcherFromBackend(&realFsnotify{fw}, log), nil
}

// newChannelWatcherFromBackend is the testable constructor.
func newChannelWatcherFromBackend(fw fsnotifyBackend, log logger) *channelWatcher {
	if log == nil {
		log = nopLogger{}
	}
	w := &channelWatcher{
		fw:     fw,
		subs:   make(map[string][]chan struct{}),
		polled: make(map[string]bool),
		stopCh: make(chan struct{}),
		log:    log,
	}
	w.wg.Add(1)
	go w.run()
	return w
}

// Watch registers path and returns a coalesced notification channel. Rapid
// filesystem events are collapsed into a single send (capacity-1 channel).
// The path is normalized with filepath.Abs so that relative paths and absolute
// paths referring to the same file share a single watch. If fsnotify.Add fails,
// a 100ms polling goroutine is started as a fallback.
func (w *channelWatcher) Watch(path string) (<-chan struct{}, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve path %q: %w", path, err)
	}

	ch := make(chan struct{}, 1)

	w.mu.Lock()
	_, alreadyWatching := w.subs[abs]
	w.subs[abs] = append(w.subs[abs], ch)
	w.mu.Unlock()

	if !alreadyWatching {
		if err := w.fw.Add(abs); err != nil {
			w.log.Warn("fsnotify watch failed, falling back to polling",
				"path", abs, "err", err)
			w.startPolling(abs)
		}
	}

	return ch, nil
}

// Close stops all watches and waits for internal goroutines to exit. Safe to
// call concurrently and more than once.
func (w *channelWatcher) Close() error {
	var fwErr error
	w.closeOnce.Do(func() {
		close(w.stopCh)
		fwErr = w.fw.Close()
	})
	w.wg.Wait()
	return fwErr
}

// run processes fsnotify events and fans out to registered subscribers.
func (w *channelWatcher) run() {
	defer w.wg.Done()
	for {
		select {
		case event, ok := <-w.fw.Events():
			if !ok {
				return
			}
			// Try the exact event path first (file-level watch), then the parent
			// directory (directory-level watch — callers register the channel dir
			// and fsnotify fires events with the path of the changed file within it).
			w.fanOut(event.Name)
			w.fanOut(filepath.Dir(event.Name))
		case err, ok := <-w.fw.Errors():
			if !ok {
				return
			}
			w.log.Warn("fsnotify error, enabling polling fallback for all watched paths",
				"err", err)
			// The watcher may have lost events (e.g. inotify queue overflow)
			// or may be permanently broken. Start polling for every path that
			// isn't already covered by a polling goroutine.
			w.mu.Lock()
			paths := make([]string, 0, len(w.subs))
			for path := range w.subs {
				if !w.polled[path] {
					paths = append(paths, path)
				}
			}
			w.mu.Unlock()
			for _, path := range paths {
				w.startPolling(path)
			}
		case <-w.stopCh:
			return
		}
	}
}

// startPolling starts a polling goroutine for path if one is not already
// running. Safe to call concurrently.
func (w *channelWatcher) startPolling(path string) {
	w.mu.Lock()
	if w.polled[path] {
		w.mu.Unlock()
		return
	}
	w.polled[path] = true
	w.mu.Unlock()
	w.wg.Add(1)
	go w.poll(path)
}

// poll watches path by statting every 100ms, used when fsnotify.Add fails.
// path may be a channel directory (segment model) or a flat file.
// For a directory, the active (last) segment's mtime/size is checked.
func (w *channelWatcher) poll(path string) {
	defer w.wg.Done()

	// Snapshot initial state to avoid a spurious notification on first tick.
	initSegs, _ := listSegments(path)
	var lastSegCount = len(initSegs)
	var lastMod time.Time
	var lastSize int64
	if len(initSegs) > 0 {
		if info, err := os.Stat(initSegs[len(initSegs)-1].path); err == nil {
			lastMod = info.ModTime()
			lastSize = info.Size()
		}
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			segs, err := listSegments(path)
			if err != nil {
				continue
			}
			changed := len(segs) != lastSegCount
			if !changed && len(segs) > 0 {
				info, err := os.Stat(segs[len(segs)-1].path)
				if err == nil {
					mod, size := info.ModTime(), info.Size()
					changed = mod != lastMod || size != lastSize
				}
			}
			if changed {
				lastSegCount = len(segs)
				if len(segs) > 0 {
					if info, err := os.Stat(segs[len(segs)-1].path); err == nil {
						lastMod = info.ModTime()
						lastSize = info.Size()
					}
				}
				w.fanOut(path)
			}
		case <-w.stopCh:
			return
		}
	}
}

// fanOut sends a non-blocking notification to every subscriber for path.
func (w *channelWatcher) fanOut(path string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, ch := range w.subs[path] {
		select {
		case ch <- struct{}{}:
		default: // coalesce: notification already pending
		}
	}
}
