package storage

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/wu/keyop-messenger/internal/envelope"
)

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
	// flushedOffset and lastFlush track what has actually been written to disk.
	// Both are only accessed from the subscriber goroutine — no mutex needed.
	flushedOffset int64
	lastFlush     time.Time

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

	return &Subscriber{
		id:            id,
		channelDir:    channelDir,
		offsetPath:    offsetPath,
		reg:           reg,
		maxRetries:    maxRetries,
		dlWriter:      dlWriter,
		log:           log,
		offset:        offset,
		flushInterval: flushInterval,
		flushedOffset: offset,
		lastFlush:     time.Now(),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
		retryDelay:    defaultRetryDelay,
		writeOffsetFn: WriteOffset,
	}, nil
}

// Start launches the subscriber goroutine. notifyC should come from
// ChannelWatcher.Watch or LocalNotifier.C(). Call Stop to wait for exit.
func (s *Subscriber) Start(notifyC <-chan struct{}, handler HandlerFunc) {
	go s.run(notifyC, handler)
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
	s.processAvailable(handler)
	for {
		select {
		case <-notifyC:
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
	if offset != s.flushedOffset {
		s.flushToDisk(offset)
	}
}

// processAvailable reads all complete lines across all segment files starting
// from the subscriber's current global offset and dispatches them.
func (s *Subscriber) processAvailable(handler HandlerFunc) {
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
		s.flushedOffset = offset
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
		for scanner.Scan() {
			line := scanner.Bytes()
			nextOffset := offset + int64(len(line)) + 1 // +1 for '\n'

			env, err := envelope.Unmarshal(line)
			if err != nil {
				s.log.Error("unmarshal envelope", "err", err)
				s.advanceOffset(nextOffset)
				offset = nextOffset
				continue
			}

			payload, err := s.reg.Decode(env.PayloadType, env.Payload)
			if err != nil {
				s.log.Error("decode payload", "type", env.PayloadType, "err", err)
				s.advanceOffset(nextOffset)
				offset = nextOffset
				continue
			}

			s.dispatch(handler, &env, payload, nextOffset)
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
func (s *Subscriber) dispatch(handler HandlerFunc, env *envelope.Envelope, payload any, nextOffset int64) {
	var lastErr error
	for attempt := 0; attempt < s.maxRetries+1; attempt++ {
		if attempt > 0 {
			delay := s.retryDelay(attempt)
			if delay > 0 {
				select {
				case <-time.After(delay):
				case <-s.stopCh:
					return
				}
			}
		}
		if lastErr = s.callHandler(handler, env, payload); lastErr == nil {
			s.advanceOffset(nextOffset)
			return
		}
	}

	if strings.HasSuffix(env.Channel, ".dead-letter") {
		s.log.Error("dead-letter handler failed, skipping",
			"channel", env.Channel, "id", env.ID, "err", lastErr)
	} else {
		s.sendToDeadLetter(env, lastErr)
	}
	s.advanceOffset(nextOffset)
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
	if err := s.dlWriter.Write(&dlEnv); err != nil {
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
	s.flushedOffset = newOffset
	s.lastFlush = time.Now()
}
