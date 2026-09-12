package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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

	// CommittedEnd returns the offset just past the last complete record this
	// writer has written: the furthest point a reader may safely read to. It is
	// a fact about records, unlike the file size, which counts the bytes of a
	// record still being appended. Readers bound their scans by it so they never
	// see a partial record at all.
	//
	// It is valid from construction (seeded from the channel's existing committed
	// end) and advances only after a record is fully written, and fsynced when
	// the channel syncs per write.
	CommittedEnd() int64

	// Failed is closed when the writer goroutine has stopped, whether from Close
	// or from a fatal error. After it closes, FatalErr says which.
	Failed() <-chan struct{}

	// FatalErr returns the error that stopped the writer goroutine, or nil if it
	// stopped because of Close. A non-nil value means this channel can no longer
	// be written to for the life of the process: the segment ends in bytes that
	// could not be rolled back, or the active segment could not be opened. Only
	// startup recovery can clear that, so the process must be restarted.
	FatalErr() error

	Close() error
}

// ErrWriterClosed is returned by Write after Close has been called.
var ErrWriterClosed = errors.New("channel writer is closed")

// errCorruptSegment signals that a partial write could not be rolled back by
// truncation, so the active segment now ends in unrecoverable partial bytes.
// The writer goroutine must exit on this error: continuing to append (O_APPEND)
// would concatenate the next record onto the garbage. On restart, openActive ->
// truncatePartialTrailing removes the trailing partial bytes and writing resumes
// cleanly.
var errCorruptSegment = errors.New("channel segment corrupt: partial write not rolled back")

// fileWriter is the minimal interface the writer goroutine requires.
// *os.File satisfies it; tests may inject a fake.
type fileWriter interface {
	Write(b []byte) (int, error)
	Truncate(size int64) error
	Sync() error
	Close() error
}

// segmentFactory abstracts creating and opening segment files so tests can
// inject fakes without touching the real filesystem.
type segmentFactory interface {
	openSegment(path string) (fileWriter, error)
	createSegment(path string) (fileWriter, error)
}

type osSegmentFactory struct{}

func (osSegmentFactory) openSegment(path string) (fileWriter, error) {
	return os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644) // #nosec G302 G304 -- trusted, library-constructed segment path; 0o644 is intentional
}

func (osSegmentFactory) createSegment(path string) (fileWriter, error) {
	return os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644) // #nosec G302 G304 -- trusted, library-constructed segment path; 0o644 is intentional
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

	// fatalErr holds the error that stopped the writer goroutine, set before
	// doneCh closes so any observer of doneCh sees it.
	fatalErr atomic.Pointer[error]

	// committedEnd is the offset just past the last complete record written to
	// this channel. Seeded synchronously at construction so it is valid before
	// the writer goroutine has started, then owned by that goroutine, which
	// stores to it after each successful write and always before notifying.
	// Readers load it to bound their scans; see ChannelWriter.CommittedEnd.
	committedEnd atomic.Int64
}

// CommittedEnd implements ChannelWriter.
func (w *channelWriter) CommittedEnd() int64 { return w.committedEnd.Load() }

// Failed implements ChannelWriter.
func (w *channelWriter) Failed() <-chan struct{} { return w.doneCh }

// FatalErr implements ChannelWriter.
func (w *channelWriter) FatalErr() error {
	if p := w.fatalErr.Load(); p != nil {
		return *p
	}
	return nil
}

// fail records the error that is about to stop the writer goroutine. It must be
// called before the goroutine returns, so that doneCh closing implies the error
// is already visible.
func (w *channelWriter) fail(err error) {
	w.fatalErr.CompareAndSwap(nil, &err)
}

// NewChannelWriter creates channelDir if needed and starts the writer goroutine.
// maxSegmentBytes controls when the writer rolls to a new segment file; 0 means
// never roll (all data goes into one segment). syncIntervalMS controls fsync behavior:
// 0 syncs after every write (strictest), > 0 syncs periodically at that interval.
// notifyFn is called after each successful write; it may be nil. log may be nil.
//
//nolint:revive // unexported-return is acceptable for unexported implementation of exported interface
func NewChannelWriter(channelDir string, maxSegmentBytes int64, syncIntervalMS int, notifyFn func(), log logger) (*channelWriter, error) {
	if err := os.MkdirAll(channelDir, 0o755); err != nil { // #nosec G301 -- 0o755 is appropriate for shared data directories
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
	// Seed before starting the goroutine: a reader may attach the instant this
	// constructor returns, and a zero committed end would send it back to
	// reading to EOF. The channel's existing committed end is what run() will
	// arrive at anyway, since openActive truncates to the same boundary.
	if channelDir != "" {
		if end, err := ChannelCommittedEnd(channelDir); err == nil {
			w.committedEnd.Store(end)
		} else {
			log.Warn("seed committed end", "dir", channelDir, "err", err)
		}
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

	// Report an unrecoverable failure in preference to "closed": a caller that
	// is told the writer is closed will retry or move on, where the truth is that
	// this channel cannot be written again until the process restarts. Checked
	// before the select because Close also fires there, and a closed stopCh and a
	// closed doneCh would otherwise be chosen between at random.
	if err := w.FatalErr(); err != nil {
		return err
	}

	done := make(chan error, 1)
	select {
	case w.requests <- writeRequest{data: data, done: done, ctx: ctx}:
	case <-w.doneCh:
		// The writer goroutine has stopped. Without this case the send would
		// block until the caller's context expired — a silent per-channel wedge
		// rather than a reported failure.
		if err := w.FatalErr(); err != nil {
			return err
		}
		return ErrWriterClosed
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

	// Report an unrecoverable failure in preference to "closed": a caller that
	// is told the writer is closed will retry or move on, where the truth is that
	// this channel cannot be written again until the process restarts. Checked
	// before the select because Close also fires there, and a closed stopCh and a
	// closed doneCh would otherwise be chosen between at random.
	if err := w.FatalErr(); err != nil {
		return err
	}

	done := make(chan error, 1)
	select {
	case w.requests <- writeRequest{records: records, done: done, ctx: ctx}:
	case <-w.doneCh:
		// The writer goroutine has stopped. Without this case the send would
		// block until the caller's context expired — a silent per-channel wedge
		// rather than a reported failure.
		if err := w.FatalErr(); err != nil {
			return err
		}
		return ErrWriterClosed
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
		w.fail(err) // openActive already names what failed
		return
	}
	defer func() { _ = f.Close() }()
	// Authoritative after recovery: openActive has truncated any partial tail.
	w.committedEnd.Store(segStart + segSize)

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
			// A fatal error is an unrecoverable segment failure; the goroutine
			// cannot continue writing. doWrite* has already signalled req.done
			// with the error.
			if fatal != nil {
				w.fail(fatal)
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

// lastRecordBoundary returns the offset one byte past the last '\n' at or before
// size — the end of the last complete record in the file. It returns size when
// the file already ends on a record boundary, and 0 when there is no '\n' at all
// (every byte belongs to a record that was never finished).
//
// This is the one definition of "where does the complete data end" in the
// package. Recovery truncates to it; ChannelCommittedEnd positions new readers
// at it. File size is not a substitute: it counts bytes present, including those
// of a record the writer is mid-append on or was cut off mid-write by a crash.
func lastRecordBoundary(r io.ReaderAt, size int64) (int64, error) {
	if size == 0 {
		return 0, nil
	}

	var last [1]byte
	if _, err := r.ReadAt(last[:], size-1); err != nil {
		return 0, fmt.Errorf("read tail byte: %w", err)
	}
	if last[0] == '\n' {
		return size, nil
	}

	const chunk = 4096
	pos := size
	for pos > 0 {
		n := int64(chunk)
		if pos < n {
			n = pos
		}
		pos -= n
		buf := make([]byte, n)
		if _, err := r.ReadAt(buf, pos); err != nil {
			return 0, fmt.Errorf("read chunk at %d: %w", pos, err)
		}
		if i := bytes.LastIndexByte(buf, '\n'); i >= 0 {
			return pos + int64(i) + 1, nil
		}
	}
	return 0, nil
}

// truncatePartialTrailing truncates path to its last record boundary, dropping
// a partial record left behind by a crash. It is a no-op when the file already
// ends on a boundary. Returns the post-truncation file size.
func truncatePartialTrailing(path string, currentSize int64, log logger) (int64, error) {
	if currentSize == 0 {
		return 0, nil
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0) // #nosec G304 -- segment path is a trusted, library-constructed data file path
	if err != nil {
		return 0, fmt.Errorf("open for recovery: %w", err)
	}
	defer func() { _ = f.Close() }()

	newSize, err := lastRecordBoundary(f, currentSize)
	if err != nil {
		return 0, err
	}
	if newSize == currentSize {
		return currentSize, nil // already ends on a record boundary
	}
	if err := f.Truncate(newSize); err != nil {
		return 0, fmt.Errorf("truncate to %d: %w", newSize, err)
	}
	if newSize == 0 {
		log.Warn("recovered fully-corrupt segment after crash",
			"path", path, "dropped_bytes", currentSize)
		return 0, nil
	}
	log.Warn("recovered partial trailing bytes after crash",
		"path", path, "old_size", currentSize, "new_size", newSize,
		"dropped_bytes", currentSize-newSize)
	return newSize, nil
}

// RecoverChannel truncates any partial trailing record in a channel's active
// segment and returns the channel's committed end — the offset just past its
// last complete record.
//
// It is the single entry point for making a channel's log valid to read. Call it
// for every channel at startup, before any reader exists: a reader bounded by a
// committed end never observes a partial record, but only if something has
// removed the one a crash left behind. Returns 0 for a channel with no segments.
func RecoverChannel(channelDir string, log logger) (int64, error) {
	if log == nil {
		log = nopLogger{}
	}
	segs, err := listSegments(channelDir)
	if err != nil {
		return 0, err
	}
	if len(segs) == 0 {
		return 0, nil
	}
	active := segs[len(segs)-1]
	size, err := truncatePartialTrailing(active.path, active.size, log)
	if err != nil {
		return 0, fmt.Errorf("recover %q: %w", active.path, err)
	}
	return active.startOffset + size, nil
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

// writeRecord appends data to f with retry-on-error (backpressure). It performs
// no fsync or notify — callers batch those so a multi-record write shares one
// fsync. Each data_dir is owned by a single process and every append to a
// channel is serialized through this one writer goroutine, so no cross-writer
// locking is needed: the OS O_APPEND guarantee plus single-goroutine serial
// access is sufficient regardless of record size.
//
// A non-nil error means the record was NOT written: either Close() was called
// or the caller's ctx was cancelled while retrying a transient write error
// (disk full, I/O). In both cases the record is absent from the file, so the
// caller's "not written" contract holds.
//
// origSize is the file's byte length before this record. A transient error may
// arrive with a partial write — n bytes of data already on disk where
// 0 < n < len(data). Because the segment is opened O_APPEND, a naive retry would
// append the full slice again, producing a concatenated [partial][full]\n line.
// A subscriber's bufio.Scanner reads that whole line, envelope.Unmarshal fails,
// and the subscriber advances its offset past it — silently dropping the message
// and breaking at-least-once delivery on a merely transient disk condition. So
// on a partial write we truncate back to origSize before retrying, restoring the
// record boundary. If truncation itself fails the segment is left corrupt and we
// return errCorruptSegment, which makes the writer goroutine exit.
func (w *channelWriter) writeRecord(ctx context.Context, f fileWriter, origSize int64, data []byte) error {
	for {
		n, err := f.Write(data)
		if err == nil {
			return nil
		}
		if n > 0 {
			if truncErr := f.Truncate(origSize); truncErr != nil {
				w.log.Error("truncate after partial write failed; segment corrupt",
					"err", truncErr, "orig_size", origSize, "partial_bytes", n)
				return fmt.Errorf("%w: truncate after %d-byte partial write: %v", errCorruptSegment, n, truncErr)
			}
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

	if err := w.writeRecord(req.ctx, f, segSize, req.data); err != nil {
		req.done <- err
		// A corrupt segment is fatal: the writer goroutine must exit so startup
		// recovery can trim the trailing partial bytes before any further append.
		if errors.Is(err, errCorruptSegment) {
			return f, segStart, segSize, err
		}
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
	// Store before notifying: the notifier coalesces on a capacity-1 channel, so
	// a reader woken by this signal must be guaranteed to see this store. The
	// reverse order lets it wake, read a stale end, deliver nothing, and find the
	// notification already consumed.
	w.committedEnd.Store(segStart + segSize)
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

		if err := w.writeRecord(req.ctx, f, segSize, rec); err != nil {
			req.done <- err
			// A corrupt segment is fatal: exit the writer goroutine so startup
			// recovery can trim the trailing partial bytes before any further
			// append (which would otherwise concatenate onto the garbage).
			if errors.Is(err, errCorruptSegment) {
				return f, segStart, segSize, err
			}
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
	// Store before notifying; see doWriteSingle.
	w.committedEnd.Store(segStart + segSize)
	if notifyFn != nil {
		notifyFn()
	}
	req.done <- nil
	return f, segStart, segSize, nil
}
