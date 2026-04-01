package storage

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/keyop/keyop-messenger/internal/envelope"
	"github.com/keyop/keyop-messenger/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- fakeFile ---------------------------------------------------------------

// fakeFile implements fileWriter for testing. Write returns ENOSPC for the
// first failFirst calls, then succeeds. Sync counts are tracked atomically.
type fakeFile struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	syncCount atomic.Int64
	failFirst int // set before goroutine starts; immutable after that
	failsDone int // number of ENOSPC returns issued; protected by mu
	closed    bool
}

func (f *fakeFile) Write(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failsDone < f.failFirst {
		f.failsDone++
		return 0, syscall.ENOSPC
	}
	return f.buf.Write(b)
}

func (f *fakeFile) Sync() error {
	f.syncCount.Add(1)
	return nil
}

func (f *fakeFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

// Fd returns 0 (stdin). Since test envelopes are well under PIPE_BUF, flock
// is never called and this value is never used.
func (f *fakeFile) Fd() uintptr { return 0 }

// FailsDone returns the number of ENOSPC failures issued. Safe to call after
// the channelWriter has been closed (goroutine has exited).
func (f *fakeFile) FailsDone() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failsDone
}

// Lines returns the lines accumulated in the buffer. Safe to call after the
// channelWriter has been closed (goroutine has exited).
func (f *fakeFile) Lines() []string {
	f.mu.Lock()
	data := make([]byte, f.buf.Len())
	copy(data, f.buf.Bytes())
	f.mu.Unlock()

	var lines []string
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines
}

// ---- helpers ----------------------------------------------------------------

func makeTestEnvelope(t *testing.T, orderID string) *envelope.Envelope {
	t.Helper()
	env, err := envelope.NewEnvelope("orders", "host", "com.test.Order",
		map[string]string{"order_id": orderID})
	require.NoError(t, err)
	return &env
}

func newFakeWriter(policy SyncPolicy, syncInterval time.Duration, notifyFn func()) (*channelWriter, *fakeFile, *testutil.FakeLogger) {
	f := &fakeFile{}
	log := &testutil.FakeLogger{}
	w := newChannelWriterFromFile(f, policy, syncInterval, notifyFn, log)
	return w, f, log
}

// ---- tests ------------------------------------------------------------------

func TestWriter_Sequential(t *testing.T) {
	w, f, _ := newFakeWriter(SyncPolicyNone, 0, nil)

	const n = 1000
	for i := 0; i < n; i++ {
		require.NoError(t, w.Write(makeTestEnvelope(t, "ord")))
	}
	require.NoError(t, w.Close())

	lines := f.Lines()
	require.Len(t, lines, n)
	for i, line := range lines {
		var v map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &v), "line %d is not valid JSON", i)
	}
}

func TestWriter_Concurrent(t *testing.T) {
	w, f, _ := newFakeWriter(SyncPolicyNone, 0, nil)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			require.NoError(t, w.Write(makeTestEnvelope(t, "ord")))
		}()
	}
	wg.Wait()
	require.NoError(t, w.Close())

	lines := f.Lines()
	require.Len(t, lines, goroutines)
	for _, line := range lines {
		// Every line must be a complete JSON object — no torn writes.
		var v map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &v))
	}
}

func TestWriter_SyncPolicyAlways(t *testing.T) {
	w, f, _ := newFakeWriter(SyncPolicyAlways, 0, nil)

	const n = 10
	for i := 0; i < n; i++ {
		require.NoError(t, w.Write(makeTestEnvelope(t, "ord")))
	}
	require.NoError(t, w.Close())

	assert.Equal(t, int64(n), f.syncCount.Load(),
		"SyncPolicyAlways must call Sync() once per write")
}

func TestWriter_SyncPolicyPeriodic(t *testing.T) {
	const interval = 50 * time.Millisecond
	w, f, _ := newFakeWriter(SyncPolicyPeriodic, interval, nil)

	// Write one envelope — Sync() must NOT be called synchronously.
	require.NoError(t, w.Write(makeTestEnvelope(t, "ord")))
	assert.Zero(t, f.syncCount.Load(),
		"SyncPolicyPeriodic must not call Sync() on each write")

	// Wait for several ticks; the background ticker must have fired Sync().
	time.Sleep(3 * interval)
	assert.GreaterOrEqual(t, f.syncCount.Load(), int64(1),
		"SyncPolicyPeriodic must call Sync() on the timer tick")

	require.NoError(t, w.Close())
}

func TestWriter_SyncPolicyNone_NoSync(t *testing.T) {
	w, f, _ := newFakeWriter(SyncPolicyNone, 0, nil)
	require.NoError(t, w.Write(makeTestEnvelope(t, "ord")))
	require.NoError(t, w.Close())
	assert.Zero(t, f.syncCount.Load(), "SyncPolicyNone must never call Sync()")
}

func TestWriter_Backpressure(t *testing.T) {
	const failFirst = 5
	f := &fakeFile{failFirst: failFirst}
	w := newChannelWriterFromFile(f, SyncPolicyNone, 0, nil, nil)

	// Write must eventually succeed despite repeated ENOSPC errors.
	require.NoError(t, w.Write(makeTestEnvelope(t, "ord-1")))
	require.NoError(t, w.Close())

	lines := f.Lines()
	require.Len(t, lines, 1, "record must appear in file after retries")
	var v map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &v))

	assert.Equal(t, failFirst, f.FailsDone(),
		"expected %d failed write attempts before success", failFirst)
}

func TestWriter_NotifyFn(t *testing.T) {
	var calls atomic.Int64
	notify := func() { calls.Add(1) }

	w, _, _ := newFakeWriter(SyncPolicyNone, 0, notify)
	require.NoError(t, w.Write(makeTestEnvelope(t, "ord")))
	require.NoError(t, w.Write(makeTestEnvelope(t, "ord")))
	require.NoError(t, w.Close())

	assert.Equal(t, int64(2), calls.Load(),
		"notifyFn must be called once per successful write")
}

func TestWriter_Close_SubsequentWriteErrors(t *testing.T) {
	w, _, _ := newFakeWriter(SyncPolicyNone, 0, nil)
	require.NoError(t, w.Close())

	err := w.Write(makeTestEnvelope(t, "ord"))
	require.ErrorIs(t, err, ErrWriterClosed)
}

func TestWriter_Close_Idempotent(t *testing.T) {
	w, _, _ := newFakeWriter(SyncPolicyNone, 0, nil)
	assert.NoError(t, w.Close())
	assert.NoError(t, w.Close()) // second call must not panic or deadlock
}

// TestWriter_RealFile exercises the full path through a real OS file, verifying
// that N envelopes produce N valid JSONL lines on disk.
func TestWriter_RealFile(t *testing.T) {
	dir := t.TempDir()
	channelPath := dir + "/orders.jsonl"

	w, err := NewChannelWriter(channelPath, SyncPolicyNone, 0, nil, nil)
	require.NoError(t, err)

	const n = 100
	for i := 0; i < n; i++ {
		require.NoError(t, w.Write(makeTestEnvelope(t, "ord")))
	}
	require.NoError(t, w.Close())

	f, err := os.Open(channelPath)
	require.NoError(t, err)
	defer f.Close()

	lineCount := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var v map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &v),
			"line %d is not valid JSON", lineCount)
		lineCount++
	}
	require.NoError(t, scanner.Err())
	assert.Equal(t, n, lineCount)
}

// TestWriter_RealFile_Concurrent writes from 50 goroutines to a real file and
// verifies the race detector and line integrity under the -race flag.
func TestWriter_RealFile_Concurrent(t *testing.T) {
	dir := t.TempDir()
	channelPath := dir + "/orders.jsonl"

	w, err := NewChannelWriter(channelPath, SyncPolicyNone, 0, nil, nil)
	require.NoError(t, err)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			require.NoError(t, w.Write(makeTestEnvelope(t, "ord")))
		}()
	}
	wg.Wait()
	require.NoError(t, w.Close())

	f, err := os.Open(channelPath)
	require.NoError(t, err)
	defer f.Close()

	lineCount := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var v map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &v),
			"line %d is not valid JSON", lineCount)
		lineCount++
	}
	require.NoError(t, scanner.Err())
	assert.Equal(t, goroutines, lineCount)
}
