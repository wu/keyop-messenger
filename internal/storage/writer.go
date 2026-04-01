package storage

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/keyop/keyop-messenger/internal/envelope"
)

// SyncPolicy controls when channel file writes are flushed to stable storage.
// The constants mirror those in the root messenger package; the storage package
// defines its own type to avoid an import cycle. Conversion is a trivial cast.
type SyncPolicy string

const (
	SyncPolicyNone     SyncPolicy = "none"
	SyncPolicyPeriodic SyncPolicy = "periodic"
	SyncPolicyAlways   SyncPolicy = "always"
)

// ChannelWriter appends envelopes to a channel file.
// Write blocks until the write is confirmed (rendezvous — no in-memory queue).
type ChannelWriter interface {
	Write(env *envelope.Envelope) error
	Close() error
}

// ErrWriterClosed is returned by Write after Close has been called.
var ErrWriterClosed = errors.New("channel writer is closed")

// pipeBuf is the conservative POSIX PIPE_BUF floor used to decide whether an
// advisory flock is needed to guarantee write atomicity.
const pipeBuf = 4096

// fileWriter is the minimal interface the writer goroutine requires from the
// underlying file. *os.File satisfies it; tests may inject a fake.
type fileWriter interface {
	Write(b []byte) (int, error)
	Sync() error
	Close() error
	Fd() uintptr
}

type writeRequest struct {
	data []byte
	done chan<- error // buffered (cap 1) — goroutine never blocks on send
}

type channelWriter struct {
	requests  chan writeRequest // unbuffered — rendezvous with writer goroutine
	stopCh    chan struct{}
	doneCh    chan struct{}
	closeOnce sync.Once
	log       logger
}

// NewChannelWriter opens channelPath (O_APPEND|O_CREATE|O_WRONLY) and starts
// the writer goroutine. notifyFn is called after each successful write; it may
// be nil. log may be nil, in which case warnings are discarded.
func NewChannelWriter(channelPath string, policy SyncPolicy, syncInterval time.Duration, notifyFn func(), log logger) (*channelWriter, error) {
	f, err := os.OpenFile(channelPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open channel file %q: %w", channelPath, err)
	}
	return newChannelWriterFromFile(f, policy, syncInterval, notifyFn, log), nil
}

// newChannelWriterFromFile is the testable constructor that accepts an
// arbitrary fileWriter instead of a real *os.File.
func newChannelWriterFromFile(f fileWriter, policy SyncPolicy, syncInterval time.Duration, notifyFn func(), log logger) *channelWriter {
	if log == nil {
		log = nopLogger{}
	}
	w := &channelWriter{
		requests: make(chan writeRequest), // unbuffered — rendezvous
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
		log:      log,
	}
	go w.run(f, policy, syncInterval, notifyFn)
	return w
}

// Write marshals env and hands it to the writer goroutine, blocking until the
// write is confirmed or the writer is closed.
func (w *channelWriter) Write(env *envelope.Envelope) error {
	data, err := envelope.Marshal(*env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	// Append newline to form a JSONL record.
	data = append(data, '\n')

	done := make(chan error, 1)
	select {
	case w.requests <- writeRequest{data: data, done: done}:
		return <-done
	case <-w.stopCh:
		return ErrWriterClosed
	}
}

// Close signals the writer goroutine to stop and waits for it to exit. Safe to
// call concurrently and more than once.
func (w *channelWriter) Close() error {
	w.closeOnce.Do(func() { close(w.stopCh) })
	<-w.doneCh
	return nil
}

// run is the writer goroutine. It owns the file descriptor for the lifetime of
// the channelWriter.
func (w *channelWriter) run(f fileWriter, policy SyncPolicy, syncInterval time.Duration, notifyFn func()) {
	defer close(w.doneCh)
	defer f.Close()

	var tickCh <-chan time.Time
	if policy == SyncPolicyPeriodic && syncInterval > 0 {
		t := time.NewTicker(syncInterval)
		defer t.Stop()
		tickCh = t.C
	}

	for {
		select {
		case req := <-w.requests:
			w.doWrite(f, req, policy, notifyFn)
		case <-tickCh: // nil channel never fires when policy != SyncPolicyPeriodic
			if err := f.Sync(); err != nil {
				w.log.Warn("periodic fsync failed", "err", err)
			}
		case <-w.stopCh:
			return
		}
	}
}

// doWrite performs the write with retry-on-error (backpressure) and optional
// fsync. It signals req.done exactly once before returning.
func (w *channelWriter) doWrite(f fileWriter, req writeRequest, policy SyncPolicy, notifyFn func()) {
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

		// Retry on any write error (disk full, transient I/O error).
		// Also honour a concurrent Close() so the goroutine does not
		// spin forever when the process is shutting down.
		select {
		case <-time.After(10 * time.Millisecond):
		case <-w.stopCh:
			req.done <- fmt.Errorf("write aborted: writer closed during retry")
			return
		}
	}

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
