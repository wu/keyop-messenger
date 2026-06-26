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
	"github.com/wu/keyop-messenger/internal/latencyhist"
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

// Default exponential-backoff parameters used between handler retry attempts
// when the messenger does not configure them explicitly.
const (
	defaultRetryBase = 100 * time.Millisecond
	defaultRetryMax  = 5 * time.Second
)

// retryBackoff returns the delay before retry attempt n (1-based):
// base * 2^(attempt-1), capped at max. The cap comparison is done in float space
// before converting to a Duration, so a very large attempt count cannot overflow
// int64 and wrap to a negative (immediate-retry) delay.
func retryBackoff(base, maxDelay time.Duration, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	ns := float64(base) * math.Pow(2, float64(attempt-1))
	if ns >= float64(maxDelay) {
		return maxDelay
	}
	return time.Duration(ns)
}

// defaultRetryDelay is the backoff schedule used when no override is configured.
func defaultRetryDelay(attempt int) time.Duration {
	return retryBackoff(defaultRetryBase, defaultRetryMax, attempt)
}

// makeRetryDelay builds a backoff function from base and max; non-positive values
// fall back to the package defaults (100ms base, 5s cap).
func makeRetryDelay(base, maxDelay time.Duration) func(attempt int) time.Duration {
	if base <= 0 {
		base = defaultRetryBase
	}
	if maxDelay <= 0 {
		maxDelay = defaultRetryMax
	}
	return func(attempt int) time.Duration { return retryBackoff(base, maxDelay, attempt) }
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
	compactionDrops  atomic.Int64
	startupSkipped   atomic.Int64
	oversizedSkipped atomic.Int64

	// Latency aggregates for successfully consumed messages (running count +
	// summed nanos, so callers derive an average). consumeAge is the end-to-end
	// delivery age (now − envelope timestamp); handlerLatency is the time spent in
	// the handler alone. Recorded once per successful dispatch. The *Window
	// histograms feed the recent p50/p90/p99 percentiles for the same two stages.
	consumeAgeSumNs     atomic.Int64
	consumeAgeCount     atomic.Int64
	handlerLatencySumNs atomic.Int64
	handlerLatencyCount atomic.Int64
	consumeWindow       *latencyhist.Window
	handlerWindow       *latencyhist.Window

	// maxAge, when > 0, enables one-time startup freshness filtering: on first
	// start the subscriber fast-forwards its offset past buffered messages older
	// than maxAge. Set via SetMaxAge before Start; read only from the subscriber
	// goroutine thereafter.
	maxAge time.Duration
}

// maxLineSize bounds a single scanned record (10 MiB); records larger than this
// are skipped by scanSegment so one oversized payload cannot wedge delivery.
// scanInitialBufSize is the scanner's starting buffer. bufio.Scanner's effective
// maximum token size is max(maxLineSize, cap(initial buffer)), so the two values
// set the record-size ceiling together. Both are vars rather than consts only so
// tests can shrink them to exercise the oversized-record path without writing a
// real 10 MiB record; production never mutates them.
var (
	maxLineSize        = 10 * 1024 * 1024
	scanInitialBufSize = 64 * 1024
)

// SubscriberStats returns a snapshot of the diagnostic counters. Intended for
// tests and observability hooks investigating delivery anomalies.
type SubscriberStats struct {
	WakesByNotify       int64
	WakesByPoll         int64
	ProcessCalls        int64
	Dispatched          int64
	UnmarshalSkipped    int64
	DecodeSkipped       int64
	RetryLaterPauses    int64
	CompactionDrops     int64
	StartupSkippedBytes int64
	OversizedSkipped    int64
	CurrentOffset       int64
	FlushedOffset       int64
}

// Stats returns a snapshot of the diagnostic counters.
func (s *Subscriber) Stats() SubscriberStats {
	s.mu.Lock()
	off := s.offset
	s.mu.Unlock()
	return SubscriberStats{
		WakesByNotify:       s.wakesByNotify.Load(),
		WakesByPoll:         s.wakesByPoll.Load(),
		ProcessCalls:        s.processCalls.Load(),
		Dispatched:          s.dispatched.Load(),
		UnmarshalSkipped:    s.unmarshalSkipped.Load(),
		DecodeSkipped:       s.decodeSkipped.Load(),
		RetryLaterPauses:    s.retryLaterPauses.Load(),
		CompactionDrops:     s.compactionDrops.Load(),
		StartupSkippedBytes: s.startupSkipped.Load(),
		OversizedSkipped:    s.oversizedSkipped.Load(),
		CurrentOffset:       off,
		FlushedOffset:       s.flushedOffset.Load(),
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
		consumeWindow: latencyhist.NewWindow(),
		handlerWindow: latencyhist.NewWindow(),
	}
	s.flushedOffset.Store(offset)
	return s, nil
}

// Start launches the subscriber goroutine. notifyC should come from
// LocalNotifier.C(). Call Stop to wait for exit.
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

// ConsumeAgeAggregate returns the running latency aggregate for end-to-end
// delivery age of successfully consumed messages: the summed age in nanoseconds
// and the sample count. The mean is sumNanos/count (guard count == 0).
func (s *Subscriber) ConsumeAgeAggregate() (sumNanos, count int64) {
	return s.consumeAgeSumNs.Load(), s.consumeAgeCount.Load()
}

// HandlerLatencyAggregate returns the running latency aggregate for time spent
// inside the handler for successfully consumed messages: the summed duration in
// nanoseconds and the sample count.
func (s *Subscriber) HandlerLatencyAggregate() (sumNanos, count int64) {
	return s.handlerLatencySumNs.Load(), s.handlerLatencyCount.Load()
}

// ConsumeWindowBuckets returns this subscriber's consume-age histogram buckets
// over the current trailing window, for merging across subscribers before
// computing instance-wide percentiles.
func (s *Subscriber) ConsumeWindowBuckets() []int64 {
	return s.consumeWindow.LiveBuckets()
}

// HandlerWindowBuckets returns this subscriber's handler-latency histogram
// buckets over the current trailing window.
func (s *Subscriber) HandlerWindowBuckets() []int64 {
	return s.handlerWindow.LiveBuckets()
}

// Stop signals the goroutine to exit and blocks until it does.
// Safe to call more than once.
func (s *Subscriber) Stop() {
	s.closeOnce.Do(func() { close(s.stopCh) })
	<-s.doneCh
}

// SetMaxAge enables one-time startup freshness filtering. When d > 0, the first
// time the subscriber runs it fast-forwards its offset past any buffered messages
// older than d, so delivery begins near real time instead of replaying a stale
// backlog. Filtering happens only at startup — steady-state messages are never
// skipped. Must be called before Start.
func (s *Subscriber) SetMaxAge(d time.Duration) { s.maxAge = d }

// SetRetryBackoff configures the exponential-backoff schedule applied between
// handler retry attempts: delay = base * 2^(attempt-1), capped at max. Non-positive
// values fall back to the defaults (100ms base, 5s cap). Must be called before Start.
// A longer schedule widens the per-message retry window before the (order-breaking)
// dead-letter; it also lengthens head-of-line blocking for this subscriber.
func (s *Subscriber) SetRetryBackoff(base, maxDelay time.Duration) {
	s.retryDelay = makeRetryDelay(base, maxDelay)
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

	// One-time startup freshness filtering, before any delivery.
	s.applyStartupMaxAge()

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

// applyStartupMaxAge fast-forwards the offset past messages older than s.maxAge.
// It runs once, from the subscriber goroutine, before delivery begins. A failure
// to scan is non-fatal: the subscriber falls back to delivering the full backlog.
func (s *Subscriber) applyStartupMaxAge() {
	if s.maxAge <= 0 {
		return
	}
	cutoff := time.Now().Add(-s.maxAge)

	s.mu.Lock()
	offset := s.offset
	s.mu.Unlock()

	newOffset, err := fastForwardToCutoff(s.channelDir, offset, cutoff)
	if err != nil {
		s.log.Error("startup max-age fast-forward failed; delivering full backlog",
			"subscriber", s.id, "err", err)
		return
	}
	if newOffset > offset {
		skipped := newOffset - offset
		s.startupSkipped.Add(skipped)
		s.log.Warn("startup max-age: skipping stale backlog",
			"subscriber", s.id, "max_age", s.maxAge.String(),
			"old_offset", offset, "new_offset", newOffset, "skipped_bytes", skipped)
		s.advanceOffset(newOffset)
	}
}

// fastForwardToCutoff returns the offset of the first record at or after cutoff,
// scanning forward from startOffset. Whole sealed segments older than the cutoff
// (by file mtime) are skipped without reading their contents; the boundary
// segment is scanned line-by-line by envelope timestamp. If every record is older
// than cutoff, the channel's current stream end is returned.
func fastForwardToCutoff(channelDir string, startOffset int64, cutoff time.Time) (int64, error) {
	segs, err := listSegments(channelDir)
	if err != nil {
		return startOffset, err
	}
	newOffset := startOffset
	for i := 0; i < len(segs); i++ {
		seg := segs[i]
		segEnd := seg.startOffset + seg.size
		if segEnd <= newOffset {
			continue // entirely behind our current position
		}
		// A sealed segment whose last write predates the cutoff cannot contain a
		// fresh record (a record's timestamp is <= its write time), so skip it
		// wholesale without reading. The active segment (i == len-1) always gets
		// the line-level scan since its mtime is current.
		if i < len(segs)-1 && seg.modTime.Before(cutoff) {
			newOffset = segEnd
			continue
		}
		from := newOffset
		if seg.startOffset > from {
			from = seg.startOffset
		}
		found, pos, err := firstOffsetAtOrAfter(seg, from, cutoff)
		if err != nil {
			return startOffset, err
		}
		if found {
			return pos, nil
		}
		newOffset = pos // whole segment was stale; check the next one
	}
	return newOffset, nil
}

// firstOffsetAtOrAfter scans seg from the global offset `from` and returns the
// offset of the first record whose timestamp is at or after cutoff. If no such
// record exists, it returns found=false and the offset of the segment's end.
func firstOffsetAtOrAfter(seg segmentInfo, from int64, cutoff time.Time) (bool, int64, error) {
	f, err := os.Open(seg.path)
	if err != nil {
		return false, 0, err
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Seek(from-seg.startOffset, io.SeekStart); err != nil {
		return false, 0, err
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, scanInitialBufSize), maxLineSize)
	scanner.Split(scanCompleteLines)

	pos := from
	for scanner.Scan() {
		line := scanner.Bytes()
		env, uerr := envelope.Unmarshal(line)
		if uerr == nil && !env.Ts.Before(cutoff) {
			return true, pos, nil // first record at or after the cutoff
		}
		pos += int64(len(line)) + 1 // skip this stale (or unparseable) record
	}
	if err := scanner.Err(); err != nil {
		return false, 0, err
	}
	return false, pos, nil
}

// OldestPendingTimestamp returns the publish timestamp of the single record at
// the given global byte offset — the oldest message not yet consumed by a
// subscriber parked there. ok is false when no record exists at offset: the
// subscriber is caught up (offset == stream end) or the offset fell outside the
// surviving segments after compaction. It is best-effort; a read or parse error
// is returned so the caller can log and fall back to "unknown".
func OldestPendingTimestamp(channelDir string, offset int64) (time.Time, bool, error) {
	segs, err := listSegments(channelDir)
	if err != nil {
		return time.Time{}, false, err
	}
	for _, seg := range segs {
		// The record at `offset` lives in the segment whose half-open byte range
		// [startOffset, startOffset+size) contains it.
		if offset < seg.startOffset || offset >= seg.startOffset+seg.size {
			continue
		}
		return readTimestampAt(seg, offset)
	}
	return time.Time{}, false, nil
}

// readTimestampAt reads the one record beginning at the given global offset
// within seg and returns its envelope timestamp.
func readTimestampAt(seg segmentInfo, offset int64) (time.Time, bool, error) {
	f, err := os.Open(seg.path)
	if err != nil {
		return time.Time{}, false, err
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Seek(offset-seg.startOffset, io.SeekStart); err != nil {
		return time.Time{}, false, err
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, scanInitialBufSize), maxLineSize)
	scanner.Split(scanCompleteLines)
	if scanner.Scan() {
		env, uerr := envelope.Unmarshal(scanner.Bytes())
		if uerr != nil {
			return time.Time{}, false, uerr
		}
		return env.Ts, true, nil
	}
	if err := scanner.Err(); err != nil {
		return time.Time{}, false, err
	}
	return time.Time{}, false, nil // no complete record at offset
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

	// If retention compaction deleted segments below our offset, the earliest
	// surviving segment now starts past where we are. Fast-forward to its start
	// to avoid a negative seek (which would wedge delivery) and report the gap of
	// permanently-dropped messages.
	if earliest := segs[0].startOffset; offset < earliest {
		dropped := earliest - offset
		s.log.Warn("subscriber offset undercut by retention compaction; skipping dropped messages",
			"subscriber", s.id, "old_offset", offset, "new_offset", earliest, "dropped_bytes", dropped)
		s.compactionDrops.Add(1)
		offset = earliest
		s.advanceOffset(offset)
	}

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

	for i, seg := range segs {
		segEnd := seg.startOffset + seg.size
		if segEnd <= offset {
			continue // subscriber is already past this entire segment
		}

		newOffset, stop := s.scanSegment(seg, handler, offset)
		offset = newOffset
		if stop {
			return
		}

		// If there is a next segment, advance to its start offset. This handles
		// the gap between a sealed segment's end and the next segment's start
		// (in the normal case these are equal, so this is a no-op on offset).
		if i+1 < len(segs) {
			offset = segs[i+1].startOffset
		}
	}
}

// scanSegment reads and dispatches complete records from seg starting at the
// subscriber's current global offset. It returns the updated offset and whether
// delivery should halt for now (an ErrRetryLater pause, an unrecoverable read
// error, or an in-flight partial write at the tail).
//
// A record larger than maxLineSize cannot be returned by the scanner, which
// would otherwise wedge the subscriber on that offset forever. Such records are
// skipped: the offset advances past them with an error log and the
// oversizedSkipped counter is incremented, mirroring how unmarshal/decode
// failures are skipped. Because a scanner is single-use after returning
// ErrTooLong, the segment is re-opened to resume scanning past the skipped
// record.
func (s *Subscriber) scanSegment(seg segmentInfo, handler HandlerFunc, offset int64) (int64, bool) {
	for {
		f, err := os.Open(seg.path)
		if err != nil {
			s.log.Error("open segment", "path", seg.path, "err", err)
			return offset, true
		}

		localOffset := offset - seg.startOffset
		if _, err := f.Seek(localOffset, io.SeekStart); err != nil {
			s.log.Error("seek in segment", "path", seg.path, "local_offset", localOffset, "err", err)
			_ = f.Close()
			return offset, true
		}

		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, scanInitialBufSize), maxLineSize)
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
				return offset, true
			}
			offset = nextOffset
		}
		serr := scanner.Err()
		_ = f.Close()

		if serr == nil {
			return offset, false
		}
		if !errors.Is(serr, bufio.ErrTooLong) {
			s.log.Error("scan segment", "path", seg.path, "err", serr)
			return offset, false
		}

		// The record at offset exceeds maxLineSize. Measure it by reading to the
		// next newline so we can advance past it instead of stalling forever.
		recLen, rerr := recordLenAt(seg.path, offset-seg.startOffset)
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				// The oversized record has no terminating newline yet: it is an
				// in-flight write, not a complete poison record. Wait for the
				// next poll rather than skipping a message mid-flight.
				return offset, true
			}
			s.log.Error("measure oversized record; halting to avoid skipping data",
				"subscriber", s.id, "path", seg.path, "offset", offset, "err", rerr)
			return offset, true
		}

		s.oversizedSkipped.Add(1)
		s.log.Error("record exceeds max line size; skipping",
			"subscriber", s.id, "path", seg.path, "offset", offset,
			"max_line_size", maxLineSize, "record_len", recLen)
		offset += recLen
		s.advanceOffset(offset)
		// Loop: re-open the segment and resume scanning after the skipped record.
	}
}

// recordLenAt returns the byte length, including the trailing newline, of the
// record beginning at localOffset within the segment file. It is used to skip a
// record too large for the scanner buffer. It returns io.EOF (with the bytes
// read so far) when no terminating newline is present, which indicates an
// in-flight write rather than a complete record.
func recordLenAt(path string, localOffset int64) (int64, error) {
	//nolint:gosec // G304: segment path is a trusted, library-constructed data file path
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Seek(localOffset, io.SeekStart); err != nil {
		return 0, err
	}
	r := bufio.NewReader(f)
	var n int64
	for {
		b, err := r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return n, io.EOF
			}
			return 0, err
		}
		n++
		if b == '\n' {
			return n, nil
		}
	}
}

// dispatch calls handler with up to maxRetries+1 total attempts with
// exponential backoff. If Stop is called during a backoff the function returns
// immediately without advancing the offset (message re-delivered on restart).
//
// The retry loop — including the backoff sleeps — runs inline in this subscriber's
// own delivery goroutine, so the subscriber does not advance to any later message on
// the channel until the current one succeeds or is dead-lettered. For an ordered
// stream of state-change events this head-of-line blocking is usually the desired
// behaviour: N+1 is not processed before N. It is per subscriber — other subscribers
// on the channel have their own goroutine and offset and are unaffected. The cost is
// latency: a consistently-failing message stalls this subscriber for the whole retry
// window (~3.1s at the default maxRetries of 5) before dead-lettering. See the
// SubscribersConfig.MaxRetries docs and DESIGN §5.5 for tuning the window and the
// ErrRetryLater alternative (which preserves order without ever dead-lettering).
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
		handlerStart := time.Now()
		lastErr = s.callHandler(handler, env, payload)
		if lastErr == nil {
			// Record latency only for successfully consumed messages, so failed
			// and retried attempts don't skew the average. Consume age spans the
			// publisher and consumer clocks; clamp skew-induced negatives to zero.
			handlerDur := time.Since(handlerStart)
			s.handlerLatencySumNs.Add(int64(handlerDur))
			s.handlerLatencyCount.Add(1)
			s.handlerWindow.Observe(handlerDur)
			age := time.Since(env.Ts)
			if age < 0 {
				age = 0
			}
			s.consumeAgeSumNs.Add(int64(age))
			s.consumeAgeCount.Add(1)
			s.consumeWindow.Observe(age)
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
