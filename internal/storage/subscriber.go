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

	"github.com/keyop/keyop-messenger/internal/envelope"
)

// defaultRetryDelay returns an exponential backoff delay for the given retry
// attempt number (1-based: attempt 1 is the first retry after the initial
// failure). The delay starts at 100ms and doubles each attempt, capped at 5s.
func defaultRetryDelay(attempt int) time.Duration {
	const (
		base = 100 * time.Millisecond
		cap  = 5 * time.Second
	)
	d := time.Duration(float64(base) * math.Pow(2, float64(attempt-1)))
	if d > cap {
		return cap
	}
	return d
}

// HandlerFunc processes a decoded message. A non-nil return value or a panic
// is treated as a delivery failure and triggers the retry / dead-letter logic.
type HandlerFunc func(env *envelope.Envelope, payload any) error

// Subscriber reads envelopes from a channel file, dispatches them to a
// handler, and persists a byte-offset so delivery can resume after restart.
type Subscriber struct {
	id          string
	channelPath string
	offsetPath  string
	reg         payloadDecoder
	maxRetries  int
	dlWriter    ChannelWriter
	log         logger

	mu     sync.Mutex
	offset int64

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
// If no offset file exists the subscriber starts from the current end-of-file
// (new subscriber, skips history). Otherwise it resumes from the persisted
// offset (restarting subscriber). log may be nil.
func NewSubscriber(
	id, channelPath, offsetDir string,
	reg payloadDecoder,
	maxRetries int,
	dlWriter ChannelWriter,
	log logger,
) (*Subscriber, error) {
	if log == nil {
		log = nopLogger{}
	}
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
		// New subscriber: start from current EOF to avoid replaying history.
		if info, err := os.Stat(channelPath); err == nil {
			offset = info.Size()
		}
		if err := WriteOffset(offsetPath, offset); err != nil {
			return nil, fmt.Errorf("write initial offset for subscriber %q: %w", id, err)
		}
	}

	return &Subscriber{
		id:            id,
		channelPath:   channelPath,
		offsetPath:    offsetPath,
		reg:           reg,
		maxRetries:    maxRetries,
		dlWriter:      dlWriter,
		log:           log,
		offset:        offset,
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
	// Drain any messages that arrived before Start was called.
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

// processAvailable seeks to the current offset and dispatches all complete
// lines available in the channel file.
func (s *Subscriber) processAvailable(handler HandlerFunc) {
	f, err := os.Open(s.channelPath)
	if err != nil {
		if !os.IsNotExist(err) {
			s.log.Error("open channel file", "path", s.channelPath, "err", err)
		}
		return
	}
	defer f.Close()

	s.mu.Lock()
	offset := s.offset
	s.mu.Unlock()

	// After compaction the channel file is shorter than our in-memory offset.
	// Detect this and reload from the offset file (which the compactor already
	// updated) so we seek to the correct position in the new file.
	if info, statErr := f.Stat(); statErr == nil && offset > info.Size() {
		if diskOff, err := ReadOffset(s.offsetPath); err == nil {
			s.mu.Lock()
			s.offset = diskOff
			s.mu.Unlock()
			offset = diskOff
		}
	}

	// If offset writes have been failing repeatedly, attempt a probe write
	// before processing more messages. Pausing here prevents unbounded
	// duplicate delivery on restart. If the probe succeeds the counter is
	// reset and processing continues normally; if it fails we bail and wait
	// for the next notification to try again.
	if s.consecutiveOffsetErrs >= maxConsecutiveOffsetErrs {
		if err := s.writeOffsetFn(s.offsetPath, offset); err != nil {
			s.log.Error("offset writes still failing; pausing delivery to limit duplicates",
				"consecutive_failures", s.consecutiveOffsetErrs, "err", err)
			return
		}
		s.consecutiveOffsetErrs = 0
	}

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		s.log.Error("seek channel file", "offset", offset, "err", err)
		return
	}

	const maxLineSize = 10 * 1024 * 1024 // 10 MiB
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), maxLineSize)
	for scanner.Scan() {
		line := scanner.Bytes()
		nextOffset := offset + int64(len(line)) + 1 // +1 for the '\n'

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
		s.log.Error("scan channel file", "err", err)
	}
}

// dispatch calls handler with up to maxRetries+1 total attempts. Between each
// retry the backoff delay from s.retryDelay is observed; if Stop is called
// during a backoff sleep the function returns immediately without advancing the
// offset (the message will be re-delivered on next start). On failure after all
// attempts, routes to the dead-letter channel — unless the channel is itself a
// dead-letter channel, in which case the error is logged and delivery advances.
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

	// All attempts exhausted.
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

// maxConsecutiveOffsetErrs is the number of successive offset-write failures
// after which the subscriber pauses delivery to prevent excessive duplicates
// on restart. Delivery resumes automatically once a probe write succeeds.
const maxConsecutiveOffsetErrs = 3

// advanceOffset persists newOffset to disk and updates the in-memory value.
// On failure the in-memory offset is left unchanged (conservative: the message
// will be re-delivered on restart) and consecutiveOffsetErrs is incremented.
// On success the counter is reset.
func (s *Subscriber) advanceOffset(newOffset int64) {
	if err := s.writeOffsetFn(s.offsetPath, newOffset); err != nil {
		s.consecutiveOffsetErrs++
		s.log.Error("persist offset",
			"consecutive_failures", s.consecutiveOffsetErrs, "err", err)
		return
	}
	s.consecutiveOffsetErrs = 0
	s.mu.Lock()
	s.offset = newOffset
	s.mu.Unlock()
}
