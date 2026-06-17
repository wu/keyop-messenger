//nolint:gosec // test file: G301/G304/G306
package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
)

// ---- helpers ----------------------------------------------------------------

type mapDecoder struct{}

func (mapDecoder) Decode(_ string, raw json.RawMessage) (any, error) {
	var v map[string]any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return v, nil
}

// activeSegmentPath returns the path to the active (only) segment in
// channelDir, creating the directory and segment file if they don't exist yet.
func activeSegmentPath(t *testing.T, channelDir string) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(channelDir, 0o755))
	segs, err := listSegments(channelDir)
	require.NoError(t, err)
	if len(segs) > 0 {
		return segs[len(segs)-1].path
	}
	// Create the first segment.
	path := filepath.Join(channelDir, segmentName(0))
	require.NoError(t, os.WriteFile(path, nil, 0o644))
	return path
}

// writeTestEnvelope marshals env and appends it as a JSONL line to the active
// segment in channelDir.
func writeTestEnvelope(t *testing.T, channelDir string, env envelope.Envelope) {
	t.Helper()
	segPath := activeSegmentPath(t, channelDir)
	data, err := envelope.Marshal(env)
	require.NoError(t, err)
	f, err := os.OpenFile(segPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	_, err = fmt.Fprintf(f, "%s\n", data)
	require.NoError(t, err)
}

// makeEnv creates a test envelope for the given channel and payload.
func makeEnv(t *testing.T, channel string, payload any) envelope.Envelope {
	t.Helper()
	env, err := envelope.NewEnvelope(channel, "test-host", "com.test.Msg", payload)
	require.NoError(t, err)
	return env
}

// collectN waits for n envelopes from ch with a timeout.
func collectN(t *testing.T, ch <-chan *envelope.Envelope, n int, timeout time.Duration) []*envelope.Envelope {
	t.Helper()
	out := make([]*envelope.Envelope, 0, n)
	deadline := time.After(timeout)
	for i := 0; i < n; i++ {
		select {
		case env := <-ch:
			out = append(out, env)
		case <-deadline:
			t.Fatalf("timeout waiting for message %d/%d", i+1, n)
		}
	}
	return out
}

// newTestSub creates a Subscriber backed by a FakeChannelWatcher.
// channelDir is the channel directory (not a file path).
func newTestSub(
	t *testing.T,
	id, channelDir, offsetDir string,
	maxRetries int,
) (*Subscriber, <-chan struct{}, *testutil.FakeChannelWriter) {
	t.Helper()
	watcher := &testutil.FakeChannelWatcher{}
	notifyC, err := watcher.Watch(channelDir)
	require.NoError(t, err)
	dlWriter := &testutil.FakeChannelWriter{}
	log := &testutil.FakeLogger{}
	sub, err := NewSubscriber(id, channelDir, offsetDir, mapDecoder{}, maxRetries, dlWriter, log, 0)
	require.NoError(t, err)
	sub.retryDelay = func(int) time.Duration { return 0 }
	return sub, notifyC, dlWriter
}

const testTimeout = 2 * time.Second

// ---- tests ------------------------------------------------------------------

func TestSubscriber_Offset(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "ch")
	offsetDir := filepath.Join(dir, "offsets")

	sub, notifyC, _ := newTestSub(t, "s", channelDir, offsetDir, 0)

	assert.Zero(t, sub.Offset(), "offset should be zero before any messages")

	writeTestEnvelope(t, channelDir, makeEnv(t, "ch", map[string]any{"n": 1}))
	writeTestEnvelope(t, channelDir, makeEnv(t, "ch", map[string]any{"n": 2}))

	received := make(chan struct{}, 10)
	sub.Start(notifyC, func(_ *envelope.Envelope, _ any) error {
		received <- struct{}{}
		return nil
	})
	t.Cleanup(sub.Stop)

	for i := 0; i < 2; i++ {
		select {
		case <-received:
		case <-time.After(testTimeout):
			t.Fatalf("message %d not delivered", i+1)
		}
	}

	streamEnd, err := ChannelStreamEnd(channelDir)
	require.NoError(t, err)
	assert.Equal(t, streamEnd, sub.Offset(), "offset should match stream end after full delivery")
}

func TestSubscriber_HappyPath(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "orders")
	offsetDir := filepath.Join(dir, "offsets")

	// Create subscriber before writing — new subscriber starts at offset 0
	// when the directory does not yet exist.
	sub, notifyC, _ := newTestSub(t, "s", channelDir, offsetDir, 3)

	for i := 0; i < 3; i++ {
		writeTestEnvelope(t, channelDir, makeEnv(t, "orders", map[string]int{"n": i}))
	}

	received := make(chan *envelope.Envelope, 10)
	sub.Start(notifyC, func(env *envelope.Envelope, _ any) error {
		received <- env
		return nil
	})
	t.Cleanup(sub.Stop)

	msgs := collectN(t, received, 3, testTimeout)
	assert.Len(t, msgs, 3)
	assert.Equal(t, "orders", msgs[0].Channel)
}

func TestSubscriber_AtLeastOnce(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "orders")
	offsetDir := filepath.Join(dir, "offsets")

	// First run: process 3 messages.
	sub1, notifyC1, _ := newTestSub(t, "s", channelDir, offsetDir, 3)

	for i := 0; i < 3; i++ {
		writeTestEnvelope(t, channelDir, makeEnv(t, "orders", map[string]int{"n": i}))
	}

	var firstRunCount atomic.Int64
	received1 := make(chan *envelope.Envelope, 10)
	sub1.Start(notifyC1, func(env *envelope.Envelope, _ any) error {
		firstRunCount.Add(1)
		received1 <- env
		return nil
	})
	collectN(t, received1, 3, testTimeout)
	sub1.Stop()

	assert.Equal(t, int64(3), firstRunCount.Load())

	// Second run with same id: must not re-deliver already-processed messages.
	sub2, notifyC2, _ := newTestSub(t, "s", channelDir, offsetDir, 3)

	var secondRunCount atomic.Int64
	sub2.Start(notifyC2, func(_ *envelope.Envelope, _ any) error {
		secondRunCount.Add(1)
		return nil
	})
	// Give the subscriber time to process (it should find nothing new).
	time.Sleep(100 * time.Millisecond)
	sub2.Stop()

	assert.Equal(t, int64(0), secondRunCount.Load(),
		"restarted subscriber must not re-deliver already-processed messages")
}

func TestSubscriber_Retry(t *testing.T) {
	const maxRetries = 3
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "orders")
	offsetDir := filepath.Join(dir, "offsets")

	sub, notifyC, dlWriter := newTestSub(t, "s", channelDir, offsetDir, maxRetries)
	writeTestEnvelope(t, channelDir, makeEnv(t, "orders", map[string]string{"k": "v"}))

	var callCount atomic.Int64
	done := make(chan struct{})
	sub.Start(notifyC, func(_ *envelope.Envelope, _ any) error {
		n := callCount.Add(1)
		if n < int64(maxRetries) { // fail on first maxRetries-1 calls
			return fmt.Errorf("transient error")
		}
		close(done) // succeed on the maxRetries-th call
		return nil
	})
	t.Cleanup(sub.Stop)

	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("handler never succeeded")
	}
	sub.Stop()

	assert.Equal(t, int64(maxRetries), callCount.Load(),
		"handler must be called exactly maxRetries times")
	assert.Empty(t, dlWriter.Written(),
		"successful delivery must not produce a dead-letter message")
}

func TestSubscriber_DeadLetter(t *testing.T) {
	const maxRetries = 3
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "orders")
	offsetDir := filepath.Join(dir, "offsets")

	sub, notifyC, dlWriter := newTestSub(t, "s", channelDir, offsetDir, maxRetries)

	// Write 2 messages; both should fail and go to dead-letter.
	writeTestEnvelope(t, channelDir, makeEnv(t, "orders", map[string]string{"k": "a"}))
	writeTestEnvelope(t, channelDir, makeEnv(t, "orders", map[string]string{"k": "b"}))

	var deliveries atomic.Int64
	sub.Start(notifyC, func(_ *envelope.Envelope, _ any) error {
		deliveries.Add(1)
		return fmt.Errorf("always fails")
	})
	t.Cleanup(sub.Stop)

	// Wait until both messages have been dead-lettered.
	require.Eventually(t, func() bool {
		return len(dlWriter.Written()) == 2
	}, testTimeout, 10*time.Millisecond, "expected 2 dead-letter messages")

	sub.Stop()

	assert.Equal(t, int64(2*(maxRetries+1)), deliveries.Load(),
		"each message must be attempted maxRetries+1 times before dead-lettering")

	// Verify the dead-letter payload structure.
	dl := dlWriter.Written()[0]
	assert.Equal(t, "orders.dead-letter", dl.Channel)
	assert.Equal(t, "com.keyop.messenger.DeadLetterPayload", dl.PayloadType)
}

func TestSubscriber_PanicRecovery(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "orders")
	offsetDir := filepath.Join(dir, "offsets")

	// Use a LocalNotifier so we can push a notification after writing the second message.
	notifier := NewLocalNotifier()
	dlWriter := &testutil.FakeChannelWriter{}
	sub, err := NewSubscriber("s", channelDir, offsetDir, mapDecoder{}, 1, dlWriter, &testutil.FakeLogger{}, 0)
	require.NoError(t, err)
	sub.retryDelay = func(int) time.Duration { return 0 }

	writeTestEnvelope(t, channelDir, makeEnv(t, "orders", map[string]string{"k": "v"}))

	sub.Start(notifier.C(), func(_ *envelope.Envelope, _ any) error {
		panic("deliberate test panic")
	})
	t.Cleanup(sub.Stop)

	// The goroutine must survive the panic and route to dead-letter.
	require.Eventually(t, func() bool {
		return len(dlWriter.Written()) == 1
	}, testTimeout, 10*time.Millisecond, "expected dead-letter after panic")

	// Write a second message and notify to confirm the goroutine is still running.
	writeTestEnvelope(t, channelDir, makeEnv(t, "orders", map[string]string{"k": "v2"}))
	notifier.Notify()
	require.Eventually(t, func() bool {
		return len(dlWriter.Written()) == 2
	}, testTimeout, 10*time.Millisecond, "goroutine must continue processing after panic")
}

func TestSubscriber_DeadLetterChannel_NoRecursion(t *testing.T) {
	dir := t.TempDir()
	// Channel dir ends in .dead-letter — subscriber must not DLQ on failure.
	channelDir := filepath.Join(dir, "orders.dead-letter")
	offsetDir := filepath.Join(dir, "offsets")

	watcher := &testutil.FakeChannelWatcher{}
	notifyC, _ := watcher.Watch(channelDir)
	dlWriter := &testutil.FakeChannelWriter{}
	log := &testutil.FakeLogger{}

	sub, err := NewSubscriber("s", channelDir, offsetDir, mapDecoder{}, 1, dlWriter, log, 0)
	require.NoError(t, err)

	writeTestEnvelope(t, channelDir, makeEnv(t, "orders.dead-letter", map[string]string{"k": "v"}))

	var callCount atomic.Int64
	sub.Start(notifyC, func(_ *envelope.Envelope, _ any) error {
		callCount.Add(1)
		return fmt.Errorf("always fails")
	})
	t.Cleanup(sub.Stop)

	// Wait for the message to be processed (attempted and skipped).
	require.Eventually(t, func() bool {
		return callCount.Load() >= 2 // maxRetries+1 = 2 attempts
	}, testTimeout, 10*time.Millisecond)
	sub.Stop()

	assert.Empty(t, dlWriter.Written(),
		"dead-letter channel subscriber must not publish to dead-letter")
	assert.True(t, log.HasError("dead-letter handler failed"),
		"failure on dead-letter channel must be logged as an error")
}

func TestSubscriber_ResumesFromOffset(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "orders")
	offsetDir := filepath.Join(dir, "offsets")

	// Write 2 envelopes before creating the subscriber.
	writeTestEnvelope(t, channelDir, makeEnv(t, "orders", map[string]string{"k": "old1"}))
	writeTestEnvelope(t, channelDir, makeEnv(t, "orders", map[string]string{"k": "old2"}))

	// New subscriber starts at current EOF — old messages are skipped.
	sub, notifyC, _ := newTestSub(t, "s", channelDir, offsetDir, 1)

	writeTestEnvelope(t, channelDir, makeEnv(t, "orders", map[string]string{"k": "new"}))

	received := make(chan *envelope.Envelope, 10)
	sub.Start(notifyC, func(env *envelope.Envelope, _ any) error {
		received <- env
		return nil
	})
	t.Cleanup(sub.Stop)

	msgs := collectN(t, received, 1, testTimeout)
	require.Len(t, msgs, 1)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(msgs[0].Payload, &payload))
	assert.Equal(t, "new", payload["k"], "new subscriber must skip pre-existing messages")
}

func TestSubscriber_LargePayload(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "orders")
	offsetDir := filepath.Join(dir, "offsets")

	// Build a payload that exceeds bufio.Scanner's default 64KB limit.
	largeValue := strings.Repeat("x", 200*1024) // 200 KiB string
	sub, notifyC, _ := newTestSub(t, "s", channelDir, offsetDir, 1)
	writeTestEnvelope(t, channelDir, makeEnv(t, "orders", map[string]string{"blob": largeValue}))

	received := make(chan *envelope.Envelope, 1)
	sub.Start(notifyC, func(env *envelope.Envelope, _ any) error {
		received <- env
		return nil
	})
	t.Cleanup(sub.Stop)

	msgs := collectN(t, received, 1, testTimeout)
	require.Len(t, msgs, 1)
	assert.Equal(t, "orders", msgs[0].Channel)
}

func TestSubscriber_RetryBackoff(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "orders")
	offsetDir := filepath.Join(dir, "offsets")

	watcher := &testutil.FakeChannelWatcher{}
	notifyC, err := watcher.Watch(channelDir)
	require.NoError(t, err)
	dlWriter := &testutil.FakeChannelWriter{}

	sub, err := NewSubscriber("s", channelDir, offsetDir, mapDecoder{}, 2, dlWriter, &testutil.FakeLogger{}, 0)
	require.NoError(t, err)

	const minDelay = 20 * time.Millisecond
	sub.retryDelay = func(int) time.Duration { return minDelay }

	writeTestEnvelope(t, channelDir, makeEnv(t, "orders", map[string]string{"k": "v"}))

	var attempts []time.Time
	sub.Start(notifyC, func(_ *envelope.Envelope, _ any) error {
		attempts = append(attempts, time.Now())
		return fmt.Errorf("always fails")
	})
	t.Cleanup(sub.Stop)

	// Wait for all 3 attempts (maxRetries+1) to be exhausted.
	require.Eventually(t, func() bool {
		return len(dlWriter.Written()) == 1
	}, testTimeout, time.Millisecond, "message must be dead-lettered")
	sub.Stop()

	require.Len(t, attempts, 3, "expected maxRetries+1 attempts")
	// Each gap between consecutive attempts must be >= minDelay.
	for i := 1; i < len(attempts); i++ {
		gap := attempts[i].Sub(attempts[i-1])
		assert.GreaterOrEqual(t, gap, minDelay,
			"gap between attempt %d and %d must be >= minDelay", i, i+1)
	}
}

// TestSubscriber_DeadLetter_WriterError verifies that a dlWriter.Write failure
// is logged and does not prevent the subscriber from advancing its offset (so
// the message is not re-delivered on restart).
func TestSubscriber_DeadLetter_WriterError(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "orders")
	offsetDir := filepath.Join(dir, "offsets")

	watcher := &testutil.FakeChannelWatcher{}
	notifyC, err := watcher.Watch(channelDir)
	require.NoError(t, err)

	dlWriter := &testutil.FakeChannelWriter{}
	dlWriter.SetError(fmt.Errorf("dead-letter storage unavailable"))

	log := &testutil.FakeLogger{}
	sub, err := NewSubscriber("s", channelDir, offsetDir, mapDecoder{}, 0, dlWriter, log, 0)
	require.NoError(t, err)
	sub.retryDelay = func(int) time.Duration { return 0 }

	// Two messages: both fail handler and attempt (and fail) the DL write.
	writeTestEnvelope(t, channelDir, makeEnv(t, "orders", map[string]string{"k": "a"}))
	writeTestEnvelope(t, channelDir, makeEnv(t, "orders", map[string]string{"k": "b"}))

	var deliveries atomic.Int64
	sub.Start(notifyC, func(_ *envelope.Envelope, _ any) error {
		deliveries.Add(1)
		return fmt.Errorf("always fails")
	})
	t.Cleanup(sub.Stop)

	require.Eventually(t, func() bool { return deliveries.Load() >= 2 },
		testTimeout, time.Millisecond, "both messages must be dispatched")
	sub.Stop()

	// DL write failures must be logged.
	assert.True(t, log.HasError("write dead-letter"),
		"dead-letter write failure must be logged at Error level")
	// The error path must not corrupt the DL writer's state.
	assert.Empty(t, dlWriter.Written(),
		"failed DL write must not persist the envelope")
	// Offset must be advanced so the messages are not re-delivered on restart.
	off, readErr := ReadOffset(filepath.Join(offsetDir, "s.offset"))
	require.NoError(t, readErr)
	assert.Greater(t, off, int64(0),
		"offset must advance past messages whose DL write failed")
}

// TestSubscriber_OffsetWriteFailure_ProbeSucceeds verifies the recovery path:
// when the probe write in processAvailable succeeds after accumulated failures,
// consecutiveOffsetErrs is reset to 0 and delivery continues normally.
func TestSubscriber_OffsetWriteFailure_ProbeSucceeds(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "orders")
	offsetDir := filepath.Join(dir, "offsets")

	notifier := NewLocalNotifier()
	dlWriter := &testutil.FakeChannelWriter{}
	log := &testutil.FakeLogger{}

	sub, err := NewSubscriber("s", channelDir, offsetDir, mapDecoder{}, 1, dlWriter, log, 0)
	require.NoError(t, err)
	sub.retryDelay = func(int) time.Duration { return 0 }

	// Fail the first maxConsecutiveOffsetErrs flushes; succeed on everything after.
	var writeCount atomic.Int64
	sub.writeOffsetFn = func(path string, offset int64) error {
		if writeCount.Add(1) <= int64(maxConsecutiveOffsetErrs) {
			return fmt.Errorf("disk full")
		}
		return WriteOffset(path, offset)
	}

	// Write exactly maxConsecutiveOffsetErrs messages so that every offset flush
	// in the initial processAvailable fails, driving consecutiveOffsetErrs to the
	// threshold that arms the probe.
	for i := 0; i < maxConsecutiveOffsetErrs; i++ {
		writeTestEnvelope(t, channelDir, makeEnv(t, "orders", map[string]int{"n": i}))
	}

	received := make(chan *envelope.Envelope, 20)
	sub.Start(notifier.C(), func(env *envelope.Envelope, _ any) error {
		received <- env
		return nil
	})
	t.Cleanup(sub.Stop)

	// Drain initial messages and confirm all offset write attempts are done.
	collectN(t, received, maxConsecutiveOffsetErrs, testTimeout)
	require.Eventually(t, func() bool {
		return writeCount.Load() >= int64(maxConsecutiveOffsetErrs)
	}, testTimeout, time.Millisecond, "all initial offset writes must have been attempted")

	// Write one more message to be delivered after the probe write resets the counter.
	writeTestEnvelope(t, channelDir, makeEnv(t, "orders", map[string]int{"n": maxConsecutiveOffsetErrs}))

	// Notify: the next processAvailable attempts the probe write (call N+1, which
	// succeeds), resets consecutiveOffsetErrs to 0, and then delivers the new message.
	notifier.Notify()
	collectN(t, received, 1, testTimeout)

	// The probe succeeded immediately: the "pausing delivery" error must not appear.
	assert.False(t, log.HasError("offset writes still failing"),
		"successful probe write must not log the pausing-delivery error")
	// The initial flush failures must still have been logged.
	assert.True(t, log.HasError("persist offset"),
		"initial offset write failures must be logged")
}

func TestSubscriber_OffsetWriteFailure_PausesAndResumes(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "orders")
	offsetDir := filepath.Join(dir, "offsets")

	notifier := NewLocalNotifier()
	dlWriter := &testutil.FakeChannelWriter{}
	log := &testutil.FakeLogger{}

	sub, err := NewSubscriber("s", channelDir, offsetDir, mapDecoder{}, 1, dlWriter, log, 0)
	require.NoError(t, err)
	sub.retryDelay = func(int) time.Duration { return 0 }

	// Inject a failing offset writer; recover after recoverAfter calls.
	const recoverAfter = maxConsecutiveOffsetErrs + 1
	var writeCount atomic.Int64
	sub.writeOffsetFn = func(path string, offset int64) error {
		if writeCount.Add(1) <= recoverAfter {
			return fmt.Errorf("disk full")
		}
		return WriteOffset(path, offset)
	}

	// Write 3 messages. Only messages before the pause should be delivered
	// without offset persistence; after the probe write succeeds the remaining
	// message(s) should be delivered.
	for i := 0; i < 3; i++ {
		writeTestEnvelope(t, channelDir, makeEnv(t, "orders", map[string]int{"n": i}))
	}

	received := make(chan *envelope.Envelope, 10)
	sub.Start(notifier.C(), func(env *envelope.Envelope, _ any) error {
		received <- env
		return nil
	})
	t.Cleanup(sub.Stop)

	// Trigger a re-scan so the probe write is attempted and delivery resumes.
	notifier.Notify()

	// All 3 messages must eventually be delivered.
	collectN(t, received, 3, testTimeout)

	// Offset write failures must have been logged as errors.
	assert.True(t, log.HasError("persist offset"),
		"offset write failures must be logged")
}

// ---- scanCompleteLines ------------------------------------------------------

// TestScanCompleteLines verifies the SplitFunc that backs the subscriber's
// scanner. The critical difference from bufio.ScanLines is the EOF-with-partial
// behaviour: instead of returning the partial bytes as a final token (which
// would cause the subscriber to advance past in-flight writes and lose data),
// scanCompleteLines stalls and waits for the writer to complete the line.
func TestScanCompleteLines(t *testing.T) {
	tests := []struct {
		name        string
		data        string
		atEOF       bool
		wantAdvance int
		wantToken   string
		wantNilTok  bool // distinguishes empty string from nil
		wantErr     bool
	}{
		{
			name:        "complete line not at EOF",
			data:        "hello\n",
			atEOF:       false,
			wantAdvance: 6,
			wantToken:   "hello",
		},
		{
			name:        "complete line at EOF",
			data:        "hello\n",
			atEOF:       true,
			wantAdvance: 6,
			wantToken:   "hello",
		},
		{
			name:        "two complete lines: returns first only",
			data:        "first\nsecond\n",
			atEOF:       false,
			wantAdvance: 6,
			wantToken:   "first",
		},
		{
			name:       "partial bytes not at EOF: requests more data",
			data:       "incomplete",
			atEOF:      false,
			wantNilTok: true,
		},
		{
			// The critical case: bufio.ScanLines would return the partial bytes
			// as a final token here. scanCompleteLines must NOT — those bytes
			// belong to an in-flight write the subscriber must not consume.
			name:       "partial bytes AT EOF: still does not emit token",
			data:       "in-flight-write-no-newline-yet",
			atEOF:      true,
			wantNilTok: true,
		},
		{
			name:       "empty input not at EOF: requests more data",
			data:       "",
			atEOF:      false,
			wantNilTok: true,
		},
		{
			name:       "empty input at EOF: terminating signal",
			data:       "",
			atEOF:      true,
			wantNilTok: true,
		},
		{
			// A bare \n is a zero-length line — legal and distinct from "no token".
			name:        "bare newline returns empty line token",
			data:        "\n",
			atEOF:       false,
			wantAdvance: 1,
			wantToken:   "",
		},
		{
			name:        "complete line followed by partial: returns the complete one",
			data:        "ready\nstill-being-written",
			atEOF:       false,
			wantAdvance: 6,
			wantToken:   "ready",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			advance, token, err := scanCompleteLines([]byte(tc.data), tc.atEOF)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantAdvance, advance, "advance")
			if tc.wantNilTok {
				assert.Nil(t, token, "expected nil token (request-more or no-data signal)")
			} else {
				require.NotNil(t, token)
				assert.Equal(t, tc.wantToken, string(token), "token")
			}
		})
	}
}

// TestScanCompleteLines_ScannerStallsOnPartialAtEOF locks in the end-to-end
// contract that the subscriber depends on: when a bufio.Scanner using
// scanCompleteLines reads a stream that ends with an unterminated trailing
// line, the scanner returns false from Scan() after emitting the complete
// lines — instead of emitting the partial as a final token (the bufio.ScanLines
// default that caused the production bug).
func TestScanCompleteLines_ScannerStallsOnPartialAtEOF(t *testing.T) {
	// "line1\nline2\nlin" — two complete lines, then an in-flight partial.
	input := strings.NewReader("line1\nline2\nlin")

	scanner := bufio.NewScanner(input)
	scanner.Split(scanCompleteLines)

	require.True(t, scanner.Scan(), "should emit first complete line")
	assert.Equal(t, "line1", scanner.Text())

	require.True(t, scanner.Scan(), "should emit second complete line")
	assert.Equal(t, "line2", scanner.Text())

	// Critical: the partial "lin" must NOT be returned. With default ScanLines,
	// scanner.Scan() would return true here with Text() = "lin".
	require.False(t, scanner.Scan(),
		"partial trailing bytes at EOF must not be emitted as a token")
	require.NoError(t, scanner.Err(), "scanner must not report an error")
}

// TestScanCompleteLines_ScannerResumesAfterMoreData verifies the recovery
// semantic the subscriber relies on: once the in-flight write completes (the
// next call to processAvailable sees the now-terminated line), a fresh scanner
// over the same data picks up the previously-stalled line.
func TestScanCompleteLines_ScannerResumesAfterMoreData(t *testing.T) {
	// First read: partial trailing line.
	first := strings.NewReader("line1\nlin")
	s1 := bufio.NewScanner(first)
	s1.Split(scanCompleteLines)

	require.True(t, s1.Scan())
	assert.Equal(t, "line1", s1.Text())
	require.False(t, s1.Scan(), "partial line should not be emitted")

	// Second read (simulating the next processAvailable after the writer
	// completed the line): same data plus the rest of the line.
	second := strings.NewReader("line1\nline2-completed\n")
	s2 := bufio.NewScanner(second)
	s2.Split(scanCompleteLines)

	var got []string
	for s2.Scan() {
		got = append(got, s2.Text())
	}
	require.NoError(t, s2.Err())
	assert.Equal(t, []string{"line1", "line2-completed"}, got)
}
