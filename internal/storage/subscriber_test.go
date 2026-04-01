package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/keyop/keyop-messenger/internal/envelope"
	"github.com/keyop/keyop-messenger/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- helpers ----------------------------------------------------------------

// mapDecoder is a minimal payloadDecoder that returns map[string]any for every type.
type mapDecoder struct{}

func (mapDecoder) Decode(_ string, raw json.RawMessage) (any, error) {
	var v map[string]any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return v, nil
}

// writeTestEnvelope marshals env and appends it as a JSONL line to path.
func writeTestEnvelope(t *testing.T, path string, env envelope.Envelope) {
	t.Helper()
	data, err := envelope.Marshal(env)
	require.NoError(t, err)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer f.Close()
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

// newTestSub creates a Subscriber backed by a FakeChannelWatcher, returning
// the subscriber, its notification channel (from watcher.Watch), and a
// FakeChannelWriter for dead-letter assertions.
func newTestSub(
	t *testing.T,
	id, channelPath, offsetDir string,
	maxRetries int,
) (*Subscriber, <-chan struct{}, *testutil.FakeChannelWriter) {
	t.Helper()
	watcher := &testutil.FakeChannelWatcher{}
	notifyC, err := watcher.Watch(channelPath)
	require.NoError(t, err)
	dlWriter := &testutil.FakeChannelWriter{}
	log := &testutil.FakeLogger{}
	sub, err := NewSubscriber(id, channelPath, offsetDir, mapDecoder{}, maxRetries, dlWriter, log)
	require.NoError(t, err)
	sub.retryDelay = func(int) time.Duration { return 0 } // no backoff in tests
	return sub, notifyC, dlWriter
}

const testTimeout = 2 * time.Second

// ---- tests ------------------------------------------------------------------

func TestSubscriber_HappyPath(t *testing.T) {
	dir := t.TempDir()
	channelPath := filepath.Join(dir, "orders.jsonl")
	offsetDir := filepath.Join(dir, "offsets")

	// Create subscriber before writing — new subscriber starts at offset 0
	// when the file does not yet exist.
	sub, notifyC, _ := newTestSub(t, "s", channelPath, offsetDir, 3)

	for i := 0; i < 3; i++ {
		writeTestEnvelope(t, channelPath, makeEnv(t, "orders", map[string]int{"n": i}))
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
	channelPath := filepath.Join(dir, "orders.jsonl")
	offsetDir := filepath.Join(dir, "offsets")

	// First run: process 3 messages.
	sub1, notifyC1, _ := newTestSub(t, "s", channelPath, offsetDir, 3)

	for i := 0; i < 3; i++ {
		writeTestEnvelope(t, channelPath, makeEnv(t, "orders", map[string]int{"n": i}))
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
	sub2, notifyC2, _ := newTestSub(t, "s", channelPath, offsetDir, 3)

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
	channelPath := filepath.Join(dir, "orders.jsonl")
	offsetDir := filepath.Join(dir, "offsets")

	sub, notifyC, dlWriter := newTestSub(t, "s", channelPath, offsetDir, maxRetries)
	writeTestEnvelope(t, channelPath, makeEnv(t, "orders", map[string]string{"k": "v"}))

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
	channelPath := filepath.Join(dir, "orders.jsonl")
	offsetDir := filepath.Join(dir, "offsets")

	sub, notifyC, dlWriter := newTestSub(t, "s", channelPath, offsetDir, maxRetries)

	// Write 2 messages; both should fail and go to dead-letter.
	writeTestEnvelope(t, channelPath, makeEnv(t, "orders", map[string]string{"k": "a"}))
	writeTestEnvelope(t, channelPath, makeEnv(t, "orders", map[string]string{"k": "b"}))

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
	channelPath := filepath.Join(dir, "orders.jsonl")
	offsetDir := filepath.Join(dir, "offsets")

	// Use a LocalNotifier so we can push a notification after writing the second message.
	notifier := NewLocalNotifier()
	dlWriter := &testutil.FakeChannelWriter{}
	sub, err := NewSubscriber("s", channelPath, offsetDir, mapDecoder{}, 1, dlWriter, &testutil.FakeLogger{})
	require.NoError(t, err)
	sub.retryDelay = func(int) time.Duration { return 0 }

	writeTestEnvelope(t, channelPath, makeEnv(t, "orders", map[string]string{"k": "v"}))

	sub.Start(notifier.C(), func(_ *envelope.Envelope, _ any) error {
		panic("deliberate test panic")
	})
	t.Cleanup(sub.Stop)

	// The goroutine must survive the panic and route to dead-letter.
	require.Eventually(t, func() bool {
		return len(dlWriter.Written()) == 1
	}, testTimeout, 10*time.Millisecond, "expected dead-letter after panic")

	// Write a second message and notify to confirm the goroutine is still running.
	writeTestEnvelope(t, channelPath, makeEnv(t, "orders", map[string]string{"k": "v2"}))
	notifier.Notify()
	require.Eventually(t, func() bool {
		return len(dlWriter.Written()) == 2
	}, testTimeout, 10*time.Millisecond, "goroutine must continue processing after panic")
}

func TestSubscriber_DeadLetterChannel_NoRecursion(t *testing.T) {
	dir := t.TempDir()
	// Channel path ends in .dead-letter.jsonl — subscriber must not DLQ on failure.
	channelPath := filepath.Join(dir, "orders.dead-letter.jsonl")
	offsetDir := filepath.Join(dir, "offsets")

	watcher := &testutil.FakeChannelWatcher{}
	notifyC, _ := watcher.Watch(channelPath)
	dlWriter := &testutil.FakeChannelWriter{}
	log := &testutil.FakeLogger{}

	sub, err := NewSubscriber("s", channelPath, offsetDir, mapDecoder{}, 1, dlWriter, log)
	require.NoError(t, err)

	writeTestEnvelope(t, channelPath, makeEnv(t, "orders.dead-letter", map[string]string{"k": "v"}))

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
	channelPath := filepath.Join(dir, "orders.jsonl")
	offsetDir := filepath.Join(dir, "offsets")

	// Write 2 envelopes before creating the subscriber.
	writeTestEnvelope(t, channelPath, makeEnv(t, "orders", map[string]string{"k": "old1"}))
	writeTestEnvelope(t, channelPath, makeEnv(t, "orders", map[string]string{"k": "old2"}))

	// New subscriber starts at current EOF — old messages are skipped.
	sub, notifyC, _ := newTestSub(t, "s", channelPath, offsetDir, 1)

	writeTestEnvelope(t, channelPath, makeEnv(t, "orders", map[string]string{"k": "new"}))

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
	channelPath := filepath.Join(dir, "orders.jsonl")
	offsetDir := filepath.Join(dir, "offsets")

	// Build a payload that exceeds bufio.Scanner's default 64KB limit.
	largeValue := strings.Repeat("x", 200*1024) // 200 KiB string
	sub, notifyC, _ := newTestSub(t, "s", channelPath, offsetDir, 1)
	writeTestEnvelope(t, channelPath, makeEnv(t, "orders", map[string]string{"blob": largeValue}))

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
	channelPath := filepath.Join(dir, "orders.jsonl")
	offsetDir := filepath.Join(dir, "offsets")

	watcher := &testutil.FakeChannelWatcher{}
	notifyC, err := watcher.Watch(channelPath)
	require.NoError(t, err)
	dlWriter := &testutil.FakeChannelWriter{}

	sub, err := NewSubscriber("s", channelPath, offsetDir, mapDecoder{}, 2, dlWriter, &testutil.FakeLogger{})
	require.NoError(t, err)

	const minDelay = 20 * time.Millisecond
	sub.retryDelay = func(int) time.Duration { return minDelay }

	writeTestEnvelope(t, channelPath, makeEnv(t, "orders", map[string]string{"k": "v"}))

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

func TestSubscriber_OffsetWriteFailure_PausesAndResumes(t *testing.T) {
	dir := t.TempDir()
	channelPath := filepath.Join(dir, "orders.jsonl")
	offsetDir := filepath.Join(dir, "offsets")

	notifier := NewLocalNotifier()
	dlWriter := &testutil.FakeChannelWriter{}
	log := &testutil.FakeLogger{}

	sub, err := NewSubscriber("s", channelPath, offsetDir, mapDecoder{}, 1, dlWriter, log)
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
		writeTestEnvelope(t, channelPath, makeEnv(t, "orders", map[string]int{"n": i}))
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
