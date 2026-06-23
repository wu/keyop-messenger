//nolint:gosec // G301/G302/G304/G315: data file operations with trusted paths
package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/wu/keyop-messenger/internal/envelope"
)

// ChannelWriter appends envelopes to a channel's segment files.
//
// Write blocks until the write is confirmed or aborted. The return value is a
// definitive signal:
//
//   - nil: the record was successfully written. With sync_interval_ms=0 it
//     was also fsync'd; otherwise it is in the OS buffer cache and will be
//     fsync'd on the next tick.
//   - non-nil: the record was NOT written. The caller may safely retry.
//
// If ctx is cancelled while the writer is in its disk-full retry loop, the
// retry is abandoned and ctx.Err() is returned without a write. ctx is not
// checked once the record has been handed to the OS — once write(2) returns
// success the data is in the kernel and the writer will see it through to
// fsync regardless of caller cancellation.
type ChannelWriter interface {
	Write(ctx context.Context, env *envelope.Envelope) error
	WriteBatch(ctx context.Context, envs []*envelope.Envelope) error
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
	// records is non-nil for a batch request: each element is one newline-
	// terminated record to append as a single durable unit. When set, data is
	// unused. A batch shares one fsync and one notify across all its records.
	records [][]byte
	done    chan<- error
	// ctx is the caller's context. The writer goroutine watches it during the
	// disk-full retry loop and abandons the write (signalling done with
	// ctx.Err()) if the caller gives up.
	ctx context.Context
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
// never roll (all data goes into one segment). syncIntervalMS controls fsync behavior:
// 0 syncs after every write (strictest), > 0 syncs periodically at that interval.
// notifyFn is called after each successful write; it may be nil. log may be nil.
//
//nolint:revive // unexported-return is acceptable for unexported implementation of exported interface
func NewChannelWriter(channelDir string, maxSegmentBytes int64, syncIntervalMS int, notifyFn func(), log logger) (*channelWriter, error) {
	if err := os.MkdirAll(channelDir, 0o755); err != nil {
		return nil, fmt.Errorf("create channel directory %q: %w", channelDir, err)
	}
	return newChannelWriterWithFactory(channelDir, maxSegmentBytes, osSegmentFactory{}, syncIntervalMS, notifyFn, log), nil
}

// newChannelWriterWithFactory is the testable constructor. channelDir may be
// empty when the segmentFactory handles all file operations (e.g. a fake).
func newChannelWriterWithFactory(channelDir string, maxSegmentBytes int64, sf segmentFactory, syncIntervalMS int, notifyFn func(), log logger) *channelWriter {
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
	go w.run(syncIntervalMS, notifyFn)
	return w
}

// Write marshals env and hands it to the writer goroutine, blocking until the
// write completes or is abandoned. The writer goroutine watches the same ctx
// during its disk-full retry loop and signals done with ctx.Err() if the
// caller gives up — so the return value is a definitive written/not-written
// signal (see the ChannelWriter docs).
func (w *channelWriter) Write(ctx context.Context, env *envelope.Envelope) error {
	data, err := envelope.Marshal(*env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	data = append(data, '\n')

	done := make(chan error, 1)
	select {
	case w.requests <- writeRequest{data: data, done: done, ctx: ctx}:
	case <-w.stopCh:
		return ErrWriterClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	// The writer goroutine owns the request and is responsible for signalling
	// done exactly once — including ctx.Err() if it abandons the write.
	return <-done
}

// WriteBatch marshals and appends all envs as a single durable unit: one fsync
// and one subscriber notification cover the whole batch (when sync_interval_ms
// is 0), amortising the per-record fsync cost. Ordering within the batch is
// preserved, and no individual record is ever split across a segment boundary.
//
// The return value follows the same written/not-written contract as Write: nil
// means every record was durably committed; a non-nil error means the batch was
// not fully committed and the caller may safely retry it. A retry may duplicate
// any record-aligned prefix that reached disk before the failure, which is
// consistent with at-least-once delivery. An empty batch is a no-op.
func (w *channelWriter) WriteBatch(ctx context.Context, envs []*envelope.Envelope) error {
	if len(envs) == 0 {
		return nil
	}
	records := make([][]byte, 0, len(envs))
	for _, env := range envs {
		data, err := envelope.Marshal(*env)
		if err != nil {
			return fmt.Errorf("marshal envelope: %w", err)
		}
		records = append(records, append(data, '\n'))
	}

	done := make(chan error, 1)
	select {
	case w.requests <- writeRequest{records: records, done: done, ctx: ctx}:
	case <-w.stopCh:
		return ErrWriterClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	return <-done
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
// tick when syncIntervalMS > 0.
func (w *channelWriter) run(syncIntervalMS int, notifyFn func()) {
	defer close(w.doneCh)

	f, segStart, segSize, err := w.openActive()
	if err != nil {
		w.log.Error("open active segment on startup", "err", err)
		return
	}
	defer func() { _ = f.Close() }()

	var tickCh <-chan time.Time
	if syncIntervalMS > 0 {
		t := time.NewTicker(time.Duration(syncIntervalMS) * time.Millisecond)
		defer t.Stop()
		tickCh = t.C
	}

	for {
		select {
		case req := <-w.requests:
			var fatal error
			if req.records != nil {
				f, segStart, segSize, fatal = w.doWriteBatch(f, segStart, segSize, req, syncIntervalMS, notifyFn)
			} else {
				f, segStart, segSize, fatal = w.doWriteSingle(f, segStart, segSize, req, syncIntervalMS, notifyFn)
			}
			// A fatal error is an unrecoverable segment-create failure; the
			// goroutine cannot continue writing. doWrite* has already signalled
			// req.done with the error.
			if fatal != nil {
				return
			}

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
//
// If the active segment's last byte is not '\n', that's evidence of a previous
// crash mid-write: the file invariant (every byte belongs to a \n-terminated
// record) has been violated. Truncate the trailing partial bytes so subsequent
// writes appended at end-of-file produce valid records. Without this, the next
// message would concatenate with the garbage and fail to unmarshal in the
// subscriber, silently dropping the first post-crash message.
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
	size, err := truncatePartialTrailing(active.path, active.size, w.log)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("truncate partial trailing in %q: %w", active.path, err)
	}
	f, err := w.sf.openSegment(active.path)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("open active segment %q: %w", active.path, err)
	}
	return f, active.startOffset, size, nil
}

// truncatePartialTrailing inspects the last byte of path. If it's already '\n'
// the file is intact and the function is a no-op. Otherwise it scans backward
// in 4 KiB chunks to find the most recent '\n' and truncates the file to one
// byte after it. If no '\n' is found anywhere, the file is truncated to zero.
// Returns the post-truncation file size.
func truncatePartialTrailing(path string, currentSize int64, log logger) (int64, error) {
	if currentSize == 0 {
		return 0, nil
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return 0, fmt.Errorf("open for recovery: %w", err)
	}
	defer func() { _ = f.Close() }()

	var last [1]byte
	if _, err := f.ReadAt(last[:], currentSize-1); err != nil {
		return 0, fmt.Errorf("read tail byte: %w", err)
	}
	if last[0] == '\n' {
		return currentSize, nil
	}

	const chunk = 4096
	pos := currentSize
	for pos > 0 {
		n := int64(chunk)
		if pos < n {
			n = pos
		}
		pos -= n
		buf := make([]byte, n)
		if _, err := f.ReadAt(buf, pos); err != nil {
			return 0, fmt.Errorf("read chunk at %d: %w", pos, err)
		}
		if i := bytes.LastIndexByte(buf, '\n'); i >= 0 {
			newSize := pos + int64(i) + 1
			if err := f.Truncate(newSize); err != nil {
				return 0, fmt.Errorf("truncate to %d: %w", newSize, err)
			}
			log.Warn("recovered partial trailing bytes after crash",
				"path", path, "old_size", currentSize, "new_size", newSize,
				"dropped_bytes", currentSize-newSize)
			return newSize, nil
		}
	}
	// No '\n' anywhere — the entire file is partial garbage.
	if err := f.Truncate(0); err != nil {
		return 0, fmt.Errorf("truncate to 0: %w", err)
	}
	log.Warn("recovered fully-corrupt segment after crash",
		"path", path, "dropped_bytes", currentSize)
	return 0, nil
}

// rollIfNeeded rolls to a new segment when appending recLen bytes to the current
// segment would exceed maxSegmentBytes. It fsyncs and closes the current segment
// and creates the next one, so no record is ever split across a boundary (a
// segment always holds whole records starting at its start offset). It returns
// the (possibly unchanged) file, start offset, and size. A non-nil error means
// the next segment could not be created and the writer goroutine must exit.
func (w *channelWriter) rollIfNeeded(f fileWriter, segStart, segSize int64, recLen int) (fileWriter, int64, int64, error) {
	if w.maxSegmentBytes <= 0 || segSize == 0 || segSize+int64(recLen) <= w.maxSegmentBytes {
		return f, segStart, segSize, nil
	}
	if err := f.Sync(); err != nil {
		w.log.Warn("fsync before segment roll", "err", err)
	}
	_ = f.Close()
	newStart := segStart + segSize
	newPath := filepath.Join(w.channelDir, segmentName(newStart))
	newF, err := w.sf.createSegment(newPath)
	if err != nil {
		w.log.Error("create new segment", "offset", newStart, "err", err)
		return nil, 0, 0, fmt.Errorf("create new segment: %w", err)
	}
	return newF, newStart, 0, nil
}

// writeRecord appends data to f with retry-on-error (backpressure) and an
// advisory flock for records larger than PIPE_BUF. It performs no fsync or
// notify — callers batch those so a multi-record write shares one fsync.
//
// A non-nil error means the record was NOT written: either Close() was called
// or the caller's ctx was cancelled while retrying a transient write error
// (disk full, I/O). In both cases write(2) never succeeded, so the caller's
// "not written" contract holds.
func (w *channelWriter) writeRecord(ctx context.Context, f fileWriter, data []byte) error {
	needLock := len(data) > pipeBuf
	for {
		if needLock {
			if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
				w.log.Warn("flock LOCK_EX failed", "err", err)
			}
		}
		_, err := f.Write(data)
		if needLock {
			if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
				w.log.Warn("flock LOCK_UN failed", "err", err)
			}
		}
		if err == nil {
			return nil
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-w.stopCh:
			return fmt.Errorf("write aborted: writer closed during retry")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// doWriteSingle writes one record (req.data): it rolls the segment first if the
// record would overflow it, appends, then fsyncs (when syncIntervalMS == 0) and
// notifies. It signals req.done exactly once. The returned error is non-nil only
// for a fatal segment-create failure, which makes the writer goroutine exit;
// ordinary write/fsync failures are reported via req.done with a nil returned
// error so the goroutine keeps serving subsequent requests.
func (w *channelWriter) doWriteSingle(f fileWriter, segStart, segSize int64, req writeRequest, syncIntervalMS int, notifyFn func()) (fileWriter, int64, int64, error) {
	nf, ns, nsz, err := w.rollIfNeeded(f, segStart, segSize, len(req.data))
	if err != nil {
		req.done <- err
		return f, segStart, segSize, err
	}
	f, segStart, segSize = nf, ns, nsz

	if err := w.writeRecord(req.ctx, f, req.data); err != nil {
		req.done <- err
		return f, segStart, segSize, nil
	}
	segSize += int64(len(req.data))

	if syncIntervalMS == 0 {
		if err := f.Sync(); err != nil {
			w.log.Error("fsync failed", "err", err)
			req.done <- fmt.Errorf("fsync channel file: %w", err)
			return f, segStart, segSize, nil
		}
	}
	if notifyFn != nil {
		notifyFn()
	}
	req.done <- nil
	return f, segStart, segSize, nil
}

// doWriteBatch writes every record in req.records as a single durable unit: each
// record is appended in order (rolling segments at boundaries so no record is
// split across files), then a single fsync (when syncIntervalMS == 0) and a
// single notify cover the whole batch. req.done is signalled exactly once.
//
// As with doWriteSingle, a non-nil returned error is fatal (segment-create
// failure) and exits the writer goroutine; a write/fsync failure mid-batch is
// reported via req.done with a nil returned error. On a mid-batch write failure
// the records already appended may remain on disk; the caller treated the batch
// as not-acked and retries the whole batch, so at-least-once holds (the retry
// may duplicate the persisted prefix).
func (w *channelWriter) doWriteBatch(f fileWriter, segStart, segSize int64, req writeRequest, syncIntervalMS int, notifyFn func()) (fileWriter, int64, int64, error) {
	for _, rec := range req.records {
		nf, ns, nsz, err := w.rollIfNeeded(f, segStart, segSize, len(rec))
		if err != nil {
			req.done <- err
			return f, segStart, segSize, err
		}
		f, segStart, segSize = nf, ns, nsz

		if err := w.writeRecord(req.ctx, f, rec); err != nil {
			req.done <- err
			return f, segStart, segSize, nil
		}
		segSize += int64(len(rec))
	}

	if syncIntervalMS == 0 {
		if err := f.Sync(); err != nil {
			w.log.Error("fsync failed", "err", err)
			req.done <- fmt.Errorf("fsync channel file: %w", err)
			return f, segStart, segSize, nil
		}
	}
	if notifyFn != nil {
		notifyFn()
	}
	req.done <- nil
	return f, segStart, segSize, nil
}
