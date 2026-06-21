package storage

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wu/keyop-messenger/internal/envelope"
)

// ErrRetryLater signals a transient downstream failure. When a HandlerFunc
// returns it (directly or wrapped), the subscriber does NOT advance its offset
// and does NOT dead-letter the message; it stops processing the current batch
// and re-attempts from the same offset on the next notify or poll. The durable
// channel log buffers the backlog, and offset-aware compaction will not delete
// data behind the unadvanced offset. Unlike an ordinary handler error, it is
// not counted against the retry budget.
var ErrRetryLater = errors.New("transient downstream failure; retry later")

// scanCompleteLines is like bufio.ScanLines but never returns a partial line
// at EOF. ScanLines's behaviour of emitting an unterminated final token is a
// feature for text-file consumers and a bug for a streaming append-only log:
// when the subscriber races a concurrent writer, Linux's regular-file read
// can observe an extended i_size before the page cache is fully populated,
// yielding bytes that lack the trailing newline. With ScanLines, those bytes
// get dispatched as a "line", fail unmarshal, and the subscriber advances
// past them — losing the real message that the writer is mid-flight on.
// scanCompleteLines instead stalls until the newline arrives, which the next
// poll or notify will surface.
func scanCompleteLines(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[0:i], nil
	}
	// No newline. Wait for more data even at EOF — partial bytes here are an
	// in-flight write, not a truly unterminated final line. The subscriber's
	// next processAvailable will re-read from the same offset and pick up the
	// complete line.
	return 0, nil, nil
}

// defaultRetryDelay returns exponential backoff delay for retry attempt n (1-based).
// Starts at 100ms, doubles each attempt, capped at 5s.
func defaultRetryDelay(attempt int) time.Duration {
	const (
		base       = 100 * time.Millisecond
		maxBackoff = 5 * time.Second
	)
	d := time.Duration(float64(base) * math.Pow(2, float64(attempt-1)))
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}

// HandlerFunc processes a decoded message. A non-nil return or a panic
// triggers the retry / dead-letter logic.
type HandlerFunc func(env *envelope.Envelope, payload any) error

// Subscriber reads envelopes from a channel's segment files, dispatches them
// to a handler, and persists a global byte offset for at-least-once delivery.
type Subscriber struct {
	id         string
	channelDir string
	offsetPath string
	reg        payloadDecoder
	maxRetries int
	dlWriter   ChannelWriter
	log        logger

	mu     sync.Mutex
	offset int64 // in-memory cursor; may be ahead of flushedOffset

	// flushInterval is the minimum time between offset file flushes.
	// 0 flushes after every message (strictest at-least-once).
	flushInterval time.Duration
	// flushedOffset tracks what has actually been written to disk. Atomic so
	// external Stats() callers can read it safely. Writes are still only from
	// the subscriber goroutine.
	flushedOffset atomic.Int64
	// lastFlush is only accessed from the subscriber goroutine — no mutex needed.
	lastFlush time.Time

	stopCh    chan struct{}
	doneCh    chan struct{}
	closeOnce sync.Once

	// retryDelay returns the backoff duration before retry attempt n (1-based).
	// Defaults to defaultRetryDelay; tests may override to zero for speed.
	retryDelay func(attempt int) time.Duration

	// writeOffsetFn persists an offset; defaults to WriteOffset.
	// Tests may inject a failing stub to exercise the pause-on-failure path.
	writeOffsetFn func(path string, offset int64) error

	// consecutiveOffsetErrs counts successive WriteOffset failures.
	// Only accessed from the subscriber goroutine — no mutex needed.
	consecutiveOffsetErrs int

	// Diagnostic counters for investigating lost-wakeup and offset-skew bugs.
	// All updated only from the subscriber goroutine and accessed via Stats().
	wakesByNotify    atomic.Int64
	wakesByPoll      atomic.Int64
	processCalls     atomic.Int64
	dispatched       atomic.Int64
	unmarshalSkipped atomic.Int64
	decodeSkipped    atomic.Int64
	retryLaterPauses atomic.Int64
}

// SubscriberStats returns a snapshot of the diagnostic counters. Intended for
// tests and observability hooks investigating delivery anomalies.
type SubscriberStats struct {
	WakesByNotify    int64
	WakesByPoll      int64
	ProcessCalls     int64
	Dispatched       int64
	UnmarshalSkipped int64
	DecodeSkipped    int64
	RetryLaterPauses int64
	CurrentOffset    int64
	FlushedOffset    int64
}

// Stats returns a snapshot of the diagnostic counters.
func (s *Subscriber) Stats() SubscriberStats {
	s.mu.Lock()
	off := s.offset
	s.mu.Unlock()
	return SubscriberStats{
		WakesByNotify:    s.wakesByNotify.Load(),
		WakesByPoll:      s.wakesByPoll.Load(),
		ProcessCalls:     s.processCalls.Load(),
		Dispatched:       s.dispatched.Load(),
		UnmarshalSkipped: s.unmarshalSkipped.Load(),
		DecodeSkipped:    s.decodeSkipped.Load(),
		RetryLaterPauses: s.retryLaterPauses.Load(),
		CurrentOffset:    off,
		FlushedOffset:    s.flushedOffset.Load(),
	}
}

// NewSubscriber constructs a Subscriber and initialises its offset file.
// If no offset file exists the subscriber starts from the current end of the
// channel (new subscriber, skips history). Otherwise it resumes from the
// persisted offset (restarting subscriber). log may be nil.
func NewSubscriber(
	id, channelDir, offsetDir string,
	reg payloadDecoder,
	maxRetries int,
	dlWriter ChannelWriter,
	log logger,
	flushInterval time.Duration,
) (*Subscriber, error) {
	if log == nil {
		log = nopLogger{}
	}
	//nolint:gosec // G301: 0o755 is appropriate for shared data directories
	if err := os.MkdirAll(offsetDir, 0o755); err != nil {
		return nil, fmt.Errorf("create offset dir %q: %w", offsetDir, err)
	}
	offsetPath := filepath.Join(offsetDir, id+".offset")

	var offset int64
	if OffsetFileExists(offsetPath) {
		var err error
		offset, err = ReadOffset(offsetPath)
		if err != nil {
			return nil, fmt.Errorf("read offset for subscriber %q: %w", id, err)
		}
	} else {
		// New subscriber: start from the current end to avoid replaying history.
		segs, err := listSegments(channelDir)
		if err != nil {
			return nil, fmt.Errorf("list segments for subscriber %q: %w", id, err)
		}
		if len(segs) > 0 {
			active := segs[len(segs)-1]
			offset = active.startOffset + active.size
		}
		if err := WriteOffset(offsetPath, offset); err != nil {
			return nil, fmt.Errorf("write initial offset for subscriber %q: %w", id, err)
		}
	}

	s := &Subscriber{
		id:            id,
		channelDir:    channelDir,
		offsetPath:    offsetPath,
		reg:           reg,
		maxRetries:    maxRetries,
		dlWriter:      dlWriter,
		log:           log,
		offset:        offset,
		flushInterval: flushInterval,
		lastFlush:     time.Now(),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
		retryDelay:    defaultRetryDelay,
		writeOffsetFn: WriteOffset,
	}
	s.flushedOffset.Store(offset)
	return s, nil
}

// Start launches the subscriber goroutine. notifyC should come from
// ChannelWatcher.Watch or LocalNotifier.C(). Call Stop to wait for exit.
func (s *Subscriber) Start(notifyC <-chan struct{}, handler HandlerFunc) {
	go s.run(notifyC, handler)
}

// Offset returns the subscriber's current in-memory read position in the
// channel stream. May be slightly ahead of the persisted offset when a
// flush interval is configured.
func (s *Subscriber) Offset() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offset
}

// Stop signals the goroutine to exit and blocks until it does.
// Safe to call more than once.
func (s *Subscriber) Stop() {
	s.closeOnce.Do(func() { close(s.stopCh) })
	<-s.doneCh
}

func (s *Subscriber) run(notifyC <-chan struct{}, handler HandlerFunc) {
	defer close(s.doneCh)
	// On clean shutdown flush any in-memory offset that hasn't reached disk yet,
	// so a restart doesn't needlessly replay already-delivered messages.
	defer s.flushOnStop()

	// Periodic poll guards against lost-wakeup races: notifyC has capacity 1, so
	// if multiple writes happen while the subscriber is inside processAvailable,
	// only one notification survives. If the subscriber's scanner observes EOF
	// before those writes are visible, it could otherwise sleep forever despite
	// pending data. processAvailable is idempotent (offset tracking skips
	// already-delivered messages), so a periodic re-check is safe and cheap.
	const pollInterval = time.Second
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	s.processAvailable(handler)
	for {
		select {
		case <-notifyC:
			s.wakesByNotify.Add(1)
			s.processAvailable(handler)
		case <-ticker.C:
			s.wakesByPoll.Add(1)
			s.processAvailable(handler)
		case <-s.stopCh:
			return
		}
	}
}

// flushOnStop writes the in-memory offset to disk if it is ahead of the last
// flushed value. Called once from the subscriber goroutine as it exits.
func (s *Subscriber) flushOnStop() {
	s.mu.Lock()
	offset := s.offset
	s.mu.Unlock()
	if offset != s.flushedOffset.Load() {
		s.flushToDisk(offset)
	}
}

// processAvailable reads all complete lines across all segment files starting
// from the subscriber's current global offset and dispatches them.
func (s *Subscriber) processAvailable(handler HandlerFunc) {
	s.processCalls.Add(1)
	segs, err := listSegments(s.channelDir)
	if err != nil {
		s.log.Error("list channel segments", "dir", s.channelDir, "err", err)
		return
	}
	if len(segs) == 0 {
		return
	}

	s.mu.Lock()
	offset := s.offset
	s.mu.Unlock()

	// If offset writes have been failing repeatedly, attempt a probe write
	// before processing more messages.
	if s.consecutiveOffsetErrs >= maxConsecutiveOffsetErrs {
		if err := s.writeOffsetFn(s.offsetPath, offset); err != nil {
			s.log.Error("offset writes still failing; pausing delivery to limit duplicates",
				"consecutive_failures", s.consecutiveOffsetErrs, "err", err)
			return
		}
		s.consecutiveOffsetErrs = 0
		s.flushedOffset.Store(offset)
		s.lastFlush = time.Now()
	}

	const maxLineSize = 10 * 1024 * 1024 // 10 MiB per line

	for i, seg := range segs {
		segEnd := seg.startOffset + seg.size
		if segEnd <= offset {
			continue // subscriber is already past this entire segment
		}

		f, err := os.Open(seg.path)
		if err != nil {
			s.log.Error("open segment", "path", seg.path, "err", err)
			return
		}

		localOffset := offset - seg.startOffset
		if _, err := f.Seek(localOffset, io.SeekStart); err != nil {
			s.log.Error("seek in segment", "path", seg.path, "local_offset", localOffset, "err", err)
			_ = f.Close()
			return
		}

		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 64*1024), maxLineSize)
		scanner.Split(scanCompleteLines)
		for scanner.Scan() {
			line := scanner.Bytes()
			nextOffset := offset + int64(len(line)) + 1 // +1 for '\n'

			env, err := envelope.Unmarshal(line)
			if err != nil {
				s.unmarshalSkipped.Add(1)
				s.log.Error("unmarshal envelope", "err", err, "line_len", len(line), "next_offset", nextOffset)
				s.advanceOffset(nextOffset)
				offset = nextOffset
				continue
			}

			payload, err := s.reg.Decode(env.PayloadType, env.Payload)
			if err != nil {
				s.decodeSkipped.Add(1)
				s.log.Error("decode payload", "type", env.PayloadType, "err", err)
				s.advanceOffset(nextOffset)
				offset = nextOffset
				continue
			}

			s.dispatched.Add(1)
			if s.dispatch(handler, &env, payload, nextOffset) {
				// Transient downstream failure (ErrRetryLater): leave the offset
				// unadvanced and halt the batch. The next notify/poll re-reads
				// from the same offset; the durable log buffers the backlog.
				_ = f.Close()
				return
			}
			offset = nextOffset
		}
		if err := scanner.Err(); err != nil {
			s.log.Error("scan segment", "path", seg.path, "err", err)
		}
		_ = f.Close()

		// If there is a next segment, advance to its start offset. This handles
		// the gap between a sealed segment's end and the next segment's start
		// (in the normal case these are equal, so this is a no-op on offset).
		if i+1 < len(segs) {
			offset = segs[i+1].startOffset
		}
	}
}

// dispatch calls handler with up to maxRetries+1 total attempts with
// exponential backoff. If Stop is called during a backoff the function returns
// immediately without advancing the offset (message re-delivered on restart).
//
// It returns retryLater=true when the handler signalled a transient downstream
// failure via ErrRetryLater. In that case the message is neither retried to
// exhaustion nor dead-lettered, and the offset is left unadvanced, so the caller
// halts the batch and re-attempts from the same position on the next wake-up.
func (s *Subscriber) dispatch(handler HandlerFunc, env *envelope.Envelope, payload any, nextOffset int64) (retryLater bool) {
	var lastErr error
	for attempt := 0; attempt < s.maxRetries+1; attempt++ {
		if attempt > 0 {
			delay := s.retryDelay(attempt)
			if delay > 0 {
				select {
				case <-time.After(delay):
				case <-s.stopCh:
					return false
				}
			}
		}
		lastErr = s.callHandler(handler, env, payload)
		if lastErr == nil {
			s.advanceOffset(nextOffset)
			return false
		}
		if errors.Is(lastErr, ErrRetryLater) {
			// Transient failure: pause without dead-lettering or advancing, and
			// do not count it against the retry budget.
			s.retryLaterPauses.Add(1)
			return true
		}
	}

	if strings.HasSuffix(env.Channel, ".dead-letter") {
		s.log.Error("dead-letter handler failed, skipping",
			"channel", env.Channel, "id", env.ID, "err", lastErr)
	} else {
		s.sendToDeadLetter(env, lastErr)
	}
	s.advanceOffset(nextOffset)
	return false
}

// callHandler invokes the handler and converts any panic into an error.
func (s *Subscriber) callHandler(handler HandlerFunc, env *envelope.Envelope, payload any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return handler(env, payload)
}

// sendToDeadLetter publishes a DeadLetterPayload envelope to the dead-letter channel.
func (s *Subscriber) sendToDeadLetter(orig *envelope.Envelope, lastErr error) {
	dlPayload := envelope.DeadLetterPayload{
		Original:  *orig,
		Retries:   s.maxRetries,
		LastError: lastErr.Error(),
		FailedAt:  time.Now().UTC().Round(0),
	}
	dlEnv, err := envelope.NewEnvelope(
		orig.Channel+".dead-letter",
		orig.Origin,
		"com.keyop.messenger.DeadLetterPayload",
		dlPayload,
	)
	if err != nil {
		s.log.Error("build dead-letter envelope", "err", err)
		return
	}
	if err := s.dlWriter.Write(context.Background(), &dlEnv); err != nil {
		s.log.Error("write dead-letter", "err", err)
	}
}

// maxConsecutiveOffsetErrs is the number of successive WriteOffset failures
// after which delivery is paused.
const maxConsecutiveOffsetErrs = 3

// advanceOffset updates the in-memory offset and flushes to disk either
// immediately (flushInterval == 0) or when the flush interval has elapsed.
// The in-memory offset is always updated so the subscriber cursor advances
// even when the disk flush is deferred.
func (s *Subscriber) advanceOffset(newOffset int64) {
	s.mu.Lock()
	s.offset = newOffset
	s.mu.Unlock()

	if s.flushInterval > 0 && time.Since(s.lastFlush) < s.flushInterval {
		return // defer flush; will be written on the next interval or on stop
	}
	s.flushToDisk(newOffset)
}

// flushToDisk writes newOffset to the offset file. Only called from the
// subscriber goroutine.
func (s *Subscriber) flushToDisk(newOffset int64) {
	if err := s.writeOffsetFn(s.offsetPath, newOffset); err != nil {
		s.consecutiveOffsetErrs++
		s.log.Error("persist offset",
			"consecutive_failures", s.consecutiveOffsetErrs, "err", err)
		return
	}
	s.consecutiveOffsetErrs = 0
	s.flushedOffset.Store(newOffset)
	s.lastFlush = time.Now()
}
