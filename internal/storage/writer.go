//nolint:gosec // G301/G302/G304/G315: data file operations with trusted paths
package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/wu/keyop-messenger/internal/envelope"
)

// SyncPolicy controls when channel file writes are flushed to stable storage.
type SyncPolicy string

// SyncPolicy constants.
const (
	SyncPolicyNone     SyncPolicy = "none"
	SyncPolicyPeriodic SyncPolicy = "periodic"
	SyncPolicyAlways   SyncPolicy = "always"
)

// ChannelWriter appends envelopes to a channel's segment files.
// Write blocks until confirmed (rendezvous — no in-memory queue).
type ChannelWriter interface {
	Write(env *envelope.Envelope) error
	Close() error
}

// ErrWriterClosed is returned by Write after Close has been called.
var ErrWriterClosed = errors.New("channel writer is closed")

// pipeBuf is the conservative POSIX PIPE_BUF floor used to decide whether an
// advisory flock is needed to guarantee write atomicity.
const pipeBuf = 4096

// fileWriter is the minimal interface the writer goroutine requires.
// *os.File satisfies it; tests may inject a fake.
type fileWriter interface {
	Write(b []byte) (int, error)
	Sync() error
	Close() error
	Fd() uintptr
}

// segmentFactory abstracts creating and opening segment files so tests can
// inject fakes without touching the real filesystem.
type segmentFactory interface {
	openSegment(path string) (fileWriter, error)
	createSegment(path string) (fileWriter, error)
}

type osSegmentFactory struct{}

func (osSegmentFactory) openSegment(path string) (fileWriter, error) {
	return os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
}

func (osSegmentFactory) createSegment(path string) (fileWriter, error) {
	return os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
}

type writeRequest struct {
	data []byte
	done chan<- error
}

type channelWriter struct {
	channelDir      string
	maxSegmentBytes int64 // 0 = unlimited (single segment)
	sf              segmentFactory
	requests        chan writeRequest
	stopCh          chan struct{}
	doneCh          chan struct{}
	closeOnce       sync.Once
	log             logger
}

// NewChannelWriter creates channelDir if needed and starts the writer goroutine.
// maxSegmentBytes controls when the writer rolls to a new segment file; 0 means
// never roll (all data goes into one segment). notifyFn is called after each
// successful write; it may be nil. log may be nil.
//
//nolint:revive // unexported-return is acceptable for unexported implementation of exported interface
func NewChannelWriter(channelDir string, maxSegmentBytes int64, policy SyncPolicy, syncInterval time.Duration, notifyFn func(), log logger) (*channelWriter, error) {
	if err := os.MkdirAll(channelDir, 0o755); err != nil {
		return nil, fmt.Errorf("create channel directory %q: %w", channelDir, err)
	}
	return newChannelWriterWithFactory(channelDir, maxSegmentBytes, osSegmentFactory{}, policy, syncInterval, notifyFn, log), nil
}

// newChannelWriterWithFactory is the testable constructor. channelDir may be
// empty when the segmentFactory handles all file operations (e.g. a fake).
func newChannelWriterWithFactory(channelDir string, maxSegmentBytes int64, sf segmentFactory, policy SyncPolicy, syncInterval time.Duration, notifyFn func(), log logger) *channelWriter {
	if log == nil {
		log = nopLogger{}
	}
	w := &channelWriter{
		channelDir:      channelDir,
		maxSegmentBytes: maxSegmentBytes,
		sf:              sf,
		requests:        make(chan writeRequest),
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
		log:             log,
	}
	go w.run(policy, syncInterval, notifyFn)
	return w
}

// Write marshals env and hands it to the writer goroutine, blocking until the
// write is confirmed or the writer is closed.
func (w *channelWriter) Write(env *envelope.Envelope) error {
	data, err := envelope.Marshal(*env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	data = append(data, '\n')

	done := make(chan error, 1)
	select {
	case w.requests <- writeRequest{data: data, done: done}:
		return <-done
	case <-w.stopCh:
		return ErrWriterClosed
	}
}

// Close signals the writer goroutine to stop and waits for it to exit.
// Safe to call concurrently and more than once.
func (w *channelWriter) Close() error {
	w.closeOnce.Do(func() { close(w.stopCh) })
	<-w.doneCh
	return nil
}

// run is the writer goroutine. It opens or creates the active segment on
// startup, handles writes with segment rolling, and runs the periodic fsync
// tick when configured.
func (w *channelWriter) run(policy SyncPolicy, syncInterval time.Duration, notifyFn func()) {
	defer close(w.doneCh)

	f, segStart, segSize, err := w.openActive()
	if err != nil {
		w.log.Error("open active segment on startup", "err", err)
		return
	}
	defer func() { _ = f.Close() }()

	var tickCh <-chan time.Time
	if policy == SyncPolicyPeriodic && syncInterval > 0 {
		t := time.NewTicker(syncInterval)
		defer t.Stop()
		tickCh = t.C
	}

	for {
		select {
		case req := <-w.requests:
			// Roll to a new segment if the current one would exceed the limit.
			if w.maxSegmentBytes > 0 && segSize > 0 &&
				segSize+int64(len(req.data)) > w.maxSegmentBytes {
				if err := f.Sync(); err != nil {
					w.log.Warn("fsync before segment roll", "err", err)
				}
				_ = f.Close()
				newStart := segStart + segSize
				newPath := filepath.Join(w.channelDir, segmentName(newStart))
				newF, err := w.sf.createSegment(newPath)
				if err != nil {
					w.log.Error("create new segment", "offset", newStart, "err", err)
					req.done <- fmt.Errorf("create new segment: %w", err)
					return
				}
				f = newF
				segStart = newStart
				segSize = 0
			}
			w.doWrite(f, req, &segSize, policy, notifyFn)

		case <-tickCh:
			if err := f.Sync(); err != nil {
				w.log.Warn("periodic fsync failed", "err", err)
			}

		case <-w.stopCh:
			return
		}
	}
}

// openActive returns the active segment file (highest start offset) ready for
// appending, along with its start offset and current byte size. If no segments
// exist yet, the first segment is created.
func (w *channelWriter) openActive() (fileWriter, int64, int64, error) {
	segs, err := listSegments(w.channelDir)
	if err != nil {
		return nil, 0, 0, err
	}
	if len(segs) == 0 {
		path := filepath.Join(w.channelDir, segmentName(0))
		f, err := w.sf.createSegment(path)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("create initial segment: %w", err)
		}
		return f, 0, 0, nil
	}
	active := segs[len(segs)-1]
	f, err := w.sf.openSegment(active.path)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("open active segment %q: %w", active.path, err)
	}
	return f, active.startOffset, active.size, nil
}

// doWrite performs the write with retry-on-error (backpressure) and optional
// fsync. It signals req.done exactly once before returning.
func (w *channelWriter) doWrite(f fileWriter, req writeRequest, segSize *int64, policy SyncPolicy, notifyFn func()) {
	needLock := len(req.data) > pipeBuf

	for {
		if needLock {
			if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
				w.log.Warn("flock LOCK_EX failed", "err", err)
			}
		}
		_, err := f.Write(req.data)
		if needLock {
			if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
				w.log.Warn("flock LOCK_UN failed", "err", err)
			}
		}
		if err == nil {
			break
		}
		// Retry on write error (disk full, transient I/O). Honour Close() to
		// avoid spinning forever on shutdown.
		select {
		case <-time.After(10 * time.Millisecond):
		case <-w.stopCh:
			req.done <- fmt.Errorf("write aborted: writer closed during retry")
			return
		}
	}

	*segSize += int64(len(req.data))

	if policy == SyncPolicyAlways {
		if err := f.Sync(); err != nil {
			w.log.Error("fsync failed", "err", err)
			req.done <- fmt.Errorf("fsync channel file: %w", err)
			return
		}
	}

	if notifyFn != nil {
		notifyFn()
	}
	req.done <- nil
}
